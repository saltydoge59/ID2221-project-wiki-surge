"""Batch reference job (Spark SQL over the Parquet archive): surge thresholds and hourly baselines.

A mean-plus-k-sigma rule is not used: per-page-per-minute counts are zero-inflated and heavy-tailed, so a high
empirical quantile is the reference. Results go to Cassandra (wiki.thresholds, wiki.baseline) and a JSON summary.
"""
import json
import os

from pyspark.sql import functions as F

from common import ARCHIVE, session

Q = float(os.environ.get("QUANTILE", "0.999"))
MIN_PAIRS = int(os.environ.get("MIN_PAIRS", "1000"))   # fewer observed (page, minute) pairs: use the global quantile
DEFAULT_NS = -1                                        # row for namespaces not seen or too sparse
OUT = os.environ.get("RESULTS_DIR", "/results")


def dedup(df):
    return df.dropDuplicates(["id"])  # the producer is at-least-once, so the archive holds repeated ids


def page_minute_counts(df):
    """Edit count per (wiki, namespace, page, minute), only for pairs with at least one edit."""
    return df.groupBy("wiki", "namespace", "title", F.date_trunc("minute", "ts").alias("minute")).count()


def thresholds(pm, q=Q, min_pairs=MIN_PAIRS):
    pct = F.expr("percentile(count, %s)" % q)  # exact, linearly interpolated
    glob = pm.agg(pct).first()[0]
    by_ns = pm.groupBy("namespace").agg(F.count("*").alias("pairs"), pct.alias("ns_q"))
    own = F.col("pairs") >= min_pairs
    rows = by_ns.select(
        "namespace", F.when(own, F.col("ns_q")).otherwise(F.lit(glob)).alias("threshold"),
        "pairs", F.when(own, "namespace").otherwise("global").alias("source"))
    default = pm.sparkSession.createDataFrame([(DEFAULT_NS, float(glob), pm.count(), "global")], rows.schema)
    return rows.unionByName(default).withColumn("quantile", F.lit(float(q))) \
        .withColumn("computed_at", F.current_timestamp()) \
        .select("namespace", "threshold", "quantile", "pairs", "source", "computed_at")


def baseline(df):
    """Typical edit rate and bot share per (wiki, namespace, hour of day UTC).

    The rate divides by the minutes actually present in the archive for that hour of day, so partial first and
    last hours do not deflate it.
    """
    hour = F.hour("ts")
    minutes = df.groupBy(hour.alias("hour")).agg(F.countDistinct(F.date_trunc("minute", "ts")).alias("minutes"))
    return df.groupBy("wiki", "namespace", hour.alias("hour")) \
        .agg(F.count("*").alias("edits"), F.avg(F.col("bot").cast("double")).alias("bot_share")) \
        .join(minutes, "hour").withColumn("rate_per_min", F.col("edits") / F.col("minutes")) \
        .select("wiki", "namespace", "hour", "edits", "rate_per_min", "bot_share")


def summary(raw, df, pm):
    n_raw, n = raw.count(), df.count()
    lo, hi = df.agg(F.min("ts"), F.max("ts")).first()
    lag = df.select((F.col("arrival").cast("double") - F.col("ts").cast("double")).alias("lag"))
    p50, p90, p99 = lag.selectExpr("percentile(lag, array(0.5, 0.9, 0.99))").first()[0]
    pairs = pm.count()
    top3 = [(r["wiki"], r["count"]) for r in df.groupBy("wiki").count().orderBy(F.desc("count")).limit(3).collect()]
    return {
        "rows_raw": n_raw, "rows_deduplicated": n, "duplicate_rows": n_raw - n,
        "ts_min": str(lo), "ts_max": str(hi),
        "edits_per_second": round(n / (hi.timestamp() - lo.timestamp()), 2),
        "lag_seconds": {"p50": p50, "p90": p90, "p99": p99, "max": lag.agg(F.max("lag")).first()[0]},
        "page_minute_pairs": pairs,
        "single_edit_share": round(pm.filter("count = 1").count() / pairs, 4),
        "max_edits_per_page_minute": pm.agg(F.max("count")).first()[0],
        "top3_wikis": top3, "top3_wiki_share": round(sum(c for _, c in top3) / n, 4),
        "bot_share": round(df.filter("bot").count() / n, 4), "null_user_rows": df.filter("user is null").count(),
        "quantile": Q, "min_pairs": MIN_PAIRS,
    }


def write_cassandra(df, table):
    df.write.format("org.apache.spark.sql.cassandra").mode("append").options(table=table, keyspace="wiki").save()


def main():
    spark = session("reference")
    raw = spark.read.parquet(ARCHIVE)
    df = dedup(raw).cache()
    pm = page_minute_counts(df).cache()
    th = thresholds(pm).cache()
    write_cassandra(th, "thresholds")
    write_cassandra(baseline(df), "baseline")
    s = summary(raw, df, pm)
    os.makedirs(OUT, exist_ok=True)
    with open(os.path.join(OUT, "reference_summary.json"), "w") as f:
        json.dump(s, f, indent=2)
    print("REFERENCE summary:", json.dumps(s))
    print("REFERENCE thresholds (top namespaces by pairs):")
    for r in th.orderBy(F.desc("pairs")).limit(8).collect():
        print("   ns=%-4s threshold=%-6.2f pairs=%-8d source=%s" % (r["namespace"], r["threshold"], r["pairs"], r["source"]))
    spark.stop()
    print("REFERENCE OK")


if __name__ == "__main__":
    main()
