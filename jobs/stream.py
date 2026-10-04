"""Structured Streaming job: Kafka -> one-minute event-time windows per (wiki, page) -> Cassandra windows and alerts.

Event time is the revision timestamp `ts`; the watermark bounds how late an edit may arrive and still be counted.
Output mode is update, so a window is written as soon as it changes (low latency) and again on every later change;
the writes are upserts on the window key, which is what makes replay after a failure idempotent.
Configuration is by environment variable so the evaluation can sweep it without code changes.
"""
import os

from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, IntegerType, LongType, StringType, StructField, StructType)

from common import KAFKA, TOPIC, session

WATERMARK = os.environ.get("WATERMARK", "60 seconds")        # revision-to-arrival lag was p99 12 s, max 119 s
TRIGGER = os.environ.get("TRIGGER", "10 seconds")            # or "availableNow" to drain the backlog and stop
MIN_EDITORS = int(os.environ.get("MIN_EDITORS", "3"))        # separates real contention from one bot iterating
BOT_DOMINATED = float(os.environ.get("BOT_DOMINATED", "0.5"))  # bot_fraction at or above this labels a window bot-dominated
DEDUP = os.environ.get("DEDUP", "1") == "1"                  # the producer is at-least-once; costs state, see evaluation
CHECKPOINT = os.environ.get("CHECKPOINT", "/checkpoints/stream")
STARTING = os.environ.get("STARTING_OFFSETS", "earliest")    # only used on the first run; a checkpoint overrides it
MAX_OFFSETS = os.environ.get("MAX_OFFSETS_PER_TRIGGER")      # cap per batch, to control the rate in experiments
# ponytail: shuffle partitions are fixed by the first checkpoint; changing this needs a fresh checkpoint
SHUFFLE_PARTITIONS = os.environ.get("SHUFFLE_PARTITIONS", "6")
KEYSPACE = "wiki"
DEFAULT_NS = -1  # the reference job's default threshold row

SCHEMA = StructType([
    StructField("id", LongType()), StructField("wiki", StringType()), StructField("title", StringType()),
    StructField("namespace", IntegerType()), StructField("user", StringType()), StructField("bot", BooleanType()),
    StructField("type", StringType()), StructField("old_len", IntegerType()), StructField("new_len", IntegerType()),
    StructField("ts", StringType()), StructField("arrival", StringType())])

WINDOW_COLS = ["wiki", "day", "window_start", "page", "edit_count", "distinct_editors", "bot_fraction",
               "net_bytes", "last_ts", "written_at"]
ALERT_COLS = ["day", "window_start", "wiki", "page", "edit_count", "distinct_editors", "bot_fraction", "label"]


def parse(kafka_df):
    e = kafka_df.select(F.from_json(F.col("value").cast("string"), SCHEMA).alias("e")).select("e.*")
    return e.withColumn("ts", F.to_timestamp("ts")).withColumn("arrival", F.to_timestamp("arrival")) \
        .filter(F.col("id").isNotNull() & F.col("ts").isNotNull())


def aggregate(events, watermark=WATERMARK, dedup=DEDUP):
    e = events.withWatermark("ts", watermark)
    if dedup:
        e = e.dropDuplicatesWithinWatermark(["id"])
    return e.groupBy("wiki", "namespace", F.col("title").alias("page"), F.window("ts", "1 minute")).agg(
        F.count("*").alias("edit_count"),
        F.size(F.collect_set("user")).alias("distinct_editors"),  # count(distinct) is not allowed in streaming; null users are ignored
        F.avg(F.col("bot").cast("double")).alias("bot_fraction"),
        F.sum(F.col("new_len") - F.col("old_len")).alias("net_bytes"),
        F.max("ts").alias("last_ts"))


def flatten(agg):
    start = F.col("window.start")
    return agg.select("wiki", "namespace", "page", start.alias("window_start"), F.to_date(start).alias("day"),
                      "edit_count", "distinct_editors", "bot_fraction", "net_bytes", "last_ts")


def flag(w, thresholds, min_editors=MIN_EDITORS):
    """Windows whose count exceeds their namespace threshold and that have enough distinct editors, labelled."""
    own = [x for ns, t in thresholds.items() if ns != DEFAULT_NS for x in (F.lit(ns), F.lit(float(t)))]
    default = F.lit(float(thresholds[DEFAULT_NS]))
    limit = F.coalesce(F.create_map(*own)[F.col("namespace")], default) if own else default
    return w.filter((F.col("edit_count") > limit) & (F.col("distinct_editors") >= min_editors)) \
        .withColumn("label", F.when(F.col("bot_fraction") >= BOT_DOMINATED, "bot-dominated").otherwise("human-dominated"))


def read_thresholds(spark):
    rows = spark.read.format("org.apache.spark.sql.cassandra").options(table="thresholds", keyspace=KEYSPACE).load().collect()
    th = {r["namespace"]: r["threshold"] for r in rows}
    assert DEFAULT_NS in th, "wiki.thresholds is empty: run jobs/reference.py first"
    return th


def save(df, table):
    df.write.format("org.apache.spark.sql.cassandra").mode("append").options(table=table, keyspace=KEYSPACE).save()


def make_writer(windows_table, alerts_table, min_editors):
    def write(batch, batch_id):
        batch.persist()  # used twice below; recomputing a micro-batch would read the source again
        w = flatten(batch).withColumn("written_at", F.current_timestamp())
        save(w.select(*WINDOW_COLS), windows_table)
        save(flag(w, read_thresholds(batch.sparkSession), min_editors).select(*ALERT_COLS), alerts_table)
        batch.unpersist()
    return write


def start(spark, checkpoint=CHECKPOINT, windows_table="windows", alerts_table="alerts", trigger=TRIGGER,
          watermark=WATERMARK, min_editors=MIN_EDITORS):
    src = spark.readStream.format("kafka").option("kafka.bootstrap.servers", KAFKA).option("subscribe", TOPIC) \
        .option("startingOffsets", STARTING)
    if MAX_OFFSETS:
        src = src.option("maxOffsetsPerTrigger", MAX_OFFSETS)
    w = aggregate(parse(src.load()), watermark).writeStream.outputMode("update") \
        .foreachBatch(make_writer(windows_table, alerts_table, min_editors)).option("checkpointLocation", checkpoint)
    return (w.trigger(availableNow=True) if trigger == "availableNow" else w.trigger(processingTime=trigger)).start()


def main():
    spark = session("stream", cores=os.environ.get("CORES", "*"), spark__sql__shuffle__partitions=SHUFFLE_PARTITIONS)
    start(spark).awaitTermination()


if __name__ == "__main__":
    main()
