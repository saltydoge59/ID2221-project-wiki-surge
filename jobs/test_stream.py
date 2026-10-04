"""Integration test for stream.py: run it over the Kafka backlog, then compare Cassandra with a pure-Python oracle
built from the Parquet archive (the archive is a subset of Kafka, so windows that are complete in both must agree).

The job writes to the scratch tables windows_test and alerts_test, never the real ones. Start from empty tables:
  docker compose exec cassandra cqlsh -e "TRUNCATE wiki.windows_test; TRUNCATE wiki.alerts_test"
  docker compose exec spark /opt/spark/bin/spark-submit /jobs/test_stream.py     (last line: STREAM TESTS OK)

A generous watermark is used so no late edit is dropped; the watermark's own effect is an evaluation experiment.
"""
import shutil
from collections import defaultdict

from pyspark.sql import functions as F

import stream
from common import ARCHIVE, session

CHECKPOINT = "/checkpoints/test_stream"
WT, AT = "windows_test", "alerts_test"
CASS = "org.apache.spark.sql.cassandra"
# The job runs with no editor minimum so alerts exist to compare (on real data multi-editor surges are rare); the
# minimum is then checked by filtering the stored alerts against the oracle at stricter values.


def oracle(rows, thresholds, min_editors):
    """rows: (id, wiki, namespace, title, user, bot, old_len, new_len, ts_epoch). Returns windows and alerts by key."""
    g = {}
    for _, w, ns, t, u, bot, old, new, e in {r[0]: r for r in rows}.values():  # dedup on id
        d = g.setdefault((w, t, e // 60 * 60), {"n": 0, "users": set(), "bots": 0, "net": 0, "ns": ns})
        d["n"] += 1
        d["bots"] += bot
        d["net"] += new - old
        if u is not None:
            d["users"].add(u)
    windows = {k: (d["n"], len(d["users"]), d["bots"] / d["n"], d["net"]) for k, d in g.items()}
    alerts = {}
    for k, d in g.items():
        limit = thresholds.get(d["ns"], thresholds[stream.DEFAULT_NS])
        if d["n"] > limit and len(d["users"]) >= min_editors:
            alerts[k] = (d["n"], len(d["users"]), "bot-dominated" if d["bots"] / d["n"] >= stream.BOT_DOMINATED else "human-dominated")
    return windows, alerts


def test_flag(spark):
    """flag() on hand-made windows: strict threshold, editor minimum, per-namespace limit, default, label boundary."""
    th = {stream.DEFAULT_NS: 10.0, 0: 5.0}
    rows = [  # (wiki, namespace, page, edit_count, distinct_editors, bot_fraction)
        ("w", 0, "over", 6, 2, 0.0),           # > 5 and 2 editors: flagged, human
        ("w", 0, "at-limit", 5, 3, 0.0),       # == 5 is not > 5
        ("w", 0, "one-bot", 90, 1, 1.0),       # huge but a single editor: never a surge
        ("w", 0, "bot", 7, 2, 0.5),            # bot_fraction exactly 0.5 is bot-dominated
        ("w", 0, "mostly-human", 7, 4, 0.49),  # just under: human
        ("w", 14, "default-over", 11, 2, 0.0),   # no row for ns 14: default 10, 11 > 10
        ("w", 14, "default-at", 10, 2, 0.0)]     # == default
    df = spark.createDataFrame(rows, "wiki string, namespace int, page string, edit_count int, "
                                     "distinct_editors int, bot_fraction double")
    got = {r["page"]: r["label"] for r in stream.flag(df, th, 2).collect()}
    assert got == {"over": "human-dominated", "bot": "bot-dominated", "mostly-human": "human-dominated",
                   "default-over": "human-dominated"}, got
    assert {r["page"] for r in stream.flag(df, th, 1).collect()} == set(got) | {"one-bot"}
    print("STREAM flag ok")


def main():
    spark = session("test_stream", cores="4", spark__sql__shuffle__partitions=stream.SHUFFLE_PARTITIONS)
    spark.sparkContext.setLogLevel("ERROR")
    read = lambda t: spark.read.format(CASS).options(table=t, keyspace="wiki").load()
    test_flag(spark)
    for t in (WT, AT):
        assert read(t).limit(1).count() == 0, "scratch table %s is not empty; TRUNCATE it first (see docstring)" % t

    shutil.rmtree(CHECKPOINT, ignore_errors=True)
    stream.start(spark, CHECKPOINT, WT, AT, trigger="availableNow", watermark="30 minutes", min_editors=1).awaitTermination()

    arc = spark.read.parquet(ARCHIVE).select("id", "wiki", "namespace", "title", "user", "bot", "old_len", "new_len",
                                               F.col("ts").cast("long").alias("e"), F.col("arrival").cast("long").alias("a"))
    rows = [tuple(r) for r in arc.collect()]
    # complete windows only: every event of a window arrived (and so was archived) well before the last archived arrival
    hi = max(r[9] for r in rows) - 180
    # ...and whose events all arrived after the archive began: the first producer run's buffer (about 2.5 minutes,
    # 2026-10-04 10:30-10:32 UTC) went to Kafka but was never archived, so earlier windows legitimately differ
    lo = (min(r[9] for r in rows) // 60 + 1) * 60
    inside = lambda start: lo <= start and start + 60 <= hi
    keep = [r[:9] for r in rows]
    th = {r["namespace"]: r["threshold"] for r in read("thresholds").collect()}
    want_w, want_a = oracle(keep, th, 1)
    want_w = {k: v for k, v in want_w.items() if inside(k[2])}
    want_a = {k: v for k, v in want_a.items() if inside(k[2])}
    assert len(want_w) > 1000 and len(want_a) > 0, "oracle is vacuous: %d windows, %d alerts" % (len(want_w), len(want_a))

    got_w = {(r["wiki"], r["page"], r["e"]): (r["edit_count"], r["distinct_editors"], r["bot_fraction"], r["net_bytes"])
             for r in read(WT).select("wiki", "page", F.col("window_start").cast("long").alias("e"), "edit_count",
                                      "distinct_editors", "bot_fraction", "net_bytes").collect() if inside(r["e"])}
    got_a = {(r["wiki"], r["page"], r["e"]): (r["edit_count"], r["distinct_editors"], r["label"])
             for r in read(AT).select("wiki", "page", F.col("window_start").cast("long").alias("e"), "edit_count",
                                      "distinct_editors", "label").collect() if inside(r["e"])}

    missing, extra = set(want_w) - set(got_w), set(got_w) - set(want_w)
    assert not missing and not extra, "windows: %d missing, %d extra, e.g. %s" % (len(missing), len(extra), (sorted(missing) + sorted(extra))[:3])
    bad = [(k, want_w[k], got_w[k]) for k in want_w
           if want_w[k][:2] != got_w[k][:2] or want_w[k][3] != got_w[k][3] or abs(want_w[k][2] - got_w[k][2]) > 1e-9]
    assert not bad, "%d windows differ, e.g. %s" % (len(bad), bad[:3])
    print("STREAM windows ok: %d complete windows equal the oracle (%d duplicate ids in the archive were collapsed)"
          % (len(want_w), len(rows) - len({r[0] for r in rows})))
    assert got_a == want_a, "alerts differ: %d missing, %d extra, %d changed" % (
        len(set(want_a) - set(got_a)), len(set(got_a) - set(want_a)), sum(1 for k in want_a if k in got_a and want_a[k] != got_a[k]))
    print("STREAM alerts ok: %d flagged windows equal the oracle (%d bot-dominated)"
          % (len(want_a), sum(v[2] == "bot-dominated" for v in want_a.values())))
    assert any(v[1] == 1 for v in want_a.values()), "no single-editor alert, so the editor minimum is not exercised"
    for m in (2, 3):
        _, want_m = oracle(keep, th, m)
        want_m = {k: v for k, v in want_m.items() if inside(k[2])}
        assert {k: v for k, v in got_a.items() if v[1] >= m} == want_m, "editor minimum %d disagrees with the oracle" % m
        print("STREAM editor minimum %d ok: %d alerts%s" % (m, len(want_m), "" if want_m else " (none yet: only test_flag covers it)"))

    nulls = read(WT).filter("written_at is null or last_ts is null").limit(1).count()
    assert nulls == 0, "windows written without last_ts/written_at"
    spark.stop()
    print("STREAM TESTS OK")


if __name__ == "__main__":
    main()
