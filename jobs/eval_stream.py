"""One measured run of the streaming query, for the evaluation. Driven by eval/run_eval.py; usable by hand too.

Runs jobs/stream.py's query (same parse, aggregate, flag and sink code) with the settings given here, into the scratch
tables windows_eval_<x> / alerts_eval_<x>, and writes a JSON file with:
  batches    per micro-batch: rows, durations, watermark, state rows and memory, rows dropped as late
  summary    throughput (all batches, and without the first, which pays JIT and RocksDB start-up), state, late rows
  task_ms    Spark task run time summed per Kafka partition (REST API; only with --task-times)
  latency_s  written_at - last_ts over the final window rows, and completeness, when asked for them
The query ends on its own with --trigger availableNow, or when <out>.stop appears (or after --duration seconds).
"""
import argparse
import json
import os
import shutil
import time
import urllib.request
from collections import defaultdict

a = argparse.ArgumentParser()
a.add_argument("--topic", required=True)
a.add_argument("--out", required=True)
a.add_argument("--checkpoint", required=True)
a.add_argument("--resume", action="store_true", help="keep the checkpoint (fault injection); default starts fresh")
a.add_argument("--cores", default="4")
a.add_argument("--shuffle-partitions", default="6")
a.add_argument("--kafka-partitions", type=int, default=6, help="partitions of the topic, to find the Kafka-scan stages")
a.add_argument("--trigger", default="availableNow", help="availableNow, or a processing time such as '5 seconds'")
a.add_argument("--watermark", default="60 seconds")
a.add_argument("--max-offsets", help="maxOffsetsPerTrigger: caps a batch, so a backlog is drained in steady steps")
a.add_argument("--sink", choices=["cassandra", "noop"], default="cassandra")
a.add_argument("--tables", default="a", help="suffix of windows_eval_<x> / alerts_eval_<x>")
a.add_argument("--no-dedup", action="store_true")
a.add_argument("--min-editors", type=int, default=2)
a.add_argument("--duration", type=float, default=0, help="stop after this many seconds (0 = until the query ends)")
a.add_argument("--task-times", action="store_true")
a.add_argument("--latency", action="store_true", help="read latency and completeness back from the table at the end")
a.add_argument("--slice", nargs=2, metavar=("START", "END"), help="archive range replayed, to compute completeness")
args = a.parse_args()

# stream.py reads these at import time
os.environ.update({"TOPIC": args.topic, "DEDUP": "0" if args.no_dedup else "1", "SHUFFLE_PARTITIONS": args.shuffle_partitions})
if args.max_offsets:
    os.environ["MAX_OFFSETS_PER_TRIGGER"] = args.max_offsets

from pyspark.sql import functions as F                    # noqa: E402
from pyspark.sql.streaming import StreamingQueryListener  # noqa: E402

import stream                                             # noqa: E402
from common import ARCHIVE, session                       # noqa: E402

STOP = args.out + ".stop"
batches = []


class Collect(StreamingQueryListener):
    def onQueryStarted(self, event):
        print("EVAL started", flush=True)

    def onQueryProgress(self, event):
        p = json.loads(event.progress.json)
        batches.append({
            "batch": p["batchId"], "rows": p["numInputRows"], "t": p["timestamp"],
            "trigger_ms": p["durationMs"].get("triggerExecution", 0), "add_batch_ms": p["durationMs"].get("addBatch", 0),
            "watermark": p.get("eventTime", {}).get("watermark"),
            "state": [{"op": o.get("operatorName"), "rows": o.get("numRowsTotal"), "mem": o.get("memoryUsedBytes"),
                       "dropped": o.get("numRowsDroppedByWatermark", 0)} for o in p.get("stateOperators", [])]})
        print("EVAL batch %d rows=%d ms=%d" % (p["batchId"], p["numInputRows"], p["durationMs"].get("triggerExecution", 0)), flush=True)

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        pass


def rest(path):
    with urllib.request.urlopen("http://localhost:4040/api/v1/applications" + path, timeout=30) as r:
        return json.load(r)


def task_times(parts):
    """Run time per task index over every Kafka-scan stage (parts tasks, shuffle write, no shuffle read)."""
    app = rest("")[0]["id"]
    ms, recs = defaultdict(float), defaultdict(int)
    for st in rest("/%s/stages?status=complete" % app):
        if st["numTasks"] == parts and st.get("shuffleWriteRecords", 0) > 0 and st.get("shuffleReadRecords", 0) == 0:
            for t in rest("/%s/stages/%d/%d/taskList?length=100000" % (app, st["stageId"], st["attemptId"])):
                m = t.get("taskMetrics")
                if m:
                    ms[t["index"]] += m["executorRunTime"]
                    recs[t["index"]] += m["shuffleWriteMetrics"]["recordsWritten"]
    return [ms[i] for i in sorted(ms)], [recs[i] for i in sorted(recs)]


def summarize(bs):
    def rate(sel):
        rows, secs = sum(b["rows"] for b in sel), sum(b["trigger_ms"] for b in sel) / 1000
        return round(rows / secs, 1) if secs else None
    by_op = defaultdict(lambda: {"rows": 0, "mem": 0, "dropped": 0})
    for b in bs:
        for i, o in enumerate(b["state"]):
            d = by_op["%d:%s" % (i, o["op"])]
            d["rows"], d["mem"], d["dropped"] = max(d["rows"], o["rows"] or 0), max(d["mem"], o["mem"] or 0), d["dropped"] + (o["dropped"] or 0)
    return {"batches": len(bs), "rows": sum(b["rows"] for b in bs), "busy_s": round(sum(b["trigger_ms"] for b in bs) / 1000, 2),
            "rows_per_s_busy": rate(bs), "rows_per_s_steady": rate(bs[1:]) if len(bs) > 1 else None,
            "add_batch_ms_median": sorted(b["add_batch_ms"] for b in bs)[len(bs) // 2] if bs else None,
            "state_max": dict(by_op)}


def main():
    spark = session("eval", cores=args.cores, spark__sql__shuffle__partitions=args.shuffle_partitions)
    spark.sparkContext.setLogLevel("ERROR")
    spark.streams.addListener(Collect())
    if not args.resume:
        shutil.rmtree(args.checkpoint, ignore_errors=True)
    for f in (STOP, args.out):
        if os.path.exists(f):
            os.remove(f)
    t0 = time.time()
    q = stream.start(spark, args.checkpoint, "windows_eval_" + args.tables, "alerts_eval_" + args.tables,
                     args.trigger, args.watermark, args.min_editors, args.sink)
    open(args.out + ".ready", "w").write(str(t0))
    while q.isActive and not os.path.exists(STOP) and not (args.duration and time.time() - t0 > args.duration):
        q.awaitTermination(1)
    q.stop()
    time.sleep(3)  # progress events are delivered asynchronously
    wall = time.time() - t0

    out = {"args": vars(args), "wall_s": round(wall, 2), "batches": batches, "summary": summarize(batches)}
    if args.task_times:
        out["task_ms"], out["task_rows"] = task_times(args.kafka_partitions)
    if args.latency or args.slice:
        w = spark.read.format("org.apache.spark.sql.cassandra").options(table="windows_eval_" + args.tables, keyspace="wiki").load().cache()
        if args.latency:
            lat = w.select((F.col("written_at").cast("double") - F.col("last_ts").cast("double")).alias("l"))
            p = lat.selectExpr("percentile_approx(l, array(0.5, 0.9, 0.99)) p", "max(l) m", "count(*) n").first()
            out["latency_s"] = {"p50": p.p[0], "p90": p.p[1], "p99": p.p[2], "max": p.m, "windows": p.n}
        if args.slice:
            start, end = args.slice
            expected = spark.read.parquet(ARCHIVE).filter((F.col("ts") >= F.lit(start).cast("timestamp")) & (F.col("ts") < F.lit(end).cast("timestamp"))) \
                .select("wiki", "id").distinct().count()
            counted = w.agg(F.sum("edit_count")).first()[0] or 0
            out["completeness"] = {"expected_edits": expected, "counted_edits": counted, "share": round(counted / expected, 6) if expected else None}
    with open(args.out, "w") as f:
        json.dump(out, f, indent=1, default=str)
    print("EVAL OK", json.dumps(out["summary"]), flush=True)
    spark.stop()


main()
