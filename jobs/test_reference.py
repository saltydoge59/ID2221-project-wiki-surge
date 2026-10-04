"""Checks reference.py against a pure-Python oracle: on synthetic rows with known answers, and on a real archive slice.

Run: docker compose exec spark /opt/spark/bin/spark-submit /jobs/test_reference.py   (last line: REFERENCE TESTS OK)
Timestamps travel as epoch seconds so Spark's session timezone cannot hide a bug in either direction.
"""
from collections import Counter, defaultdict

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, IntegerType, LongType, StringType, StructField, StructType

import reference as ref
from common import ARCHIVE, session

SCHEMA = StructType([StructField("id", LongType()), StructField("wiki", StringType()),
                     StructField("namespace", IntegerType()), StructField("title", StringType()),
                     StructField("bot", BooleanType()), StructField("epoch", LongType())])
BASE = 1791115200  # 2026-10-04 12:00:00 UTC, a minute boundary


def pctl(vals, q):  # linear interpolation, the definition Spark's exact percentile() uses
    v = sorted(vals)
    pos = q * (len(v) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


def frame(spark, rows):
    return spark.createDataFrame(rows, SCHEMA).withColumn("ts", F.timestamp_seconds("epoch")) \
        .withColumn("user", F.lit("u")).withColumn("arrival", F.col("ts"))


def check(spark, rows, q, min_pairs):
    """Compare Spark's thresholds and baseline with an oracle computed from the raw (duplicated) rows."""
    uniq = {r[0]: r for r in rows}.values()  # dedup on id
    pairs = Counter((w, n, t, e // 60) for _, w, n, t, _, e in uniq)
    by_ns = defaultdict(list)
    for (w, n, t, m), c in pairs.items():
        by_ns[n].append(c)
    glob = pctl(list(pairs.values()), q)

    df = ref.dedup(frame(spark, rows))
    got = {r["namespace"]: r for r in ref.thresholds(ref.page_minute_counts(df), q, min_pairs).collect()}
    assert set(got) == set(by_ns) | {ref.DEFAULT_NS}, (sorted(got), sorted(by_ns))
    for n, counts in by_ns.items():
        own = len(counts) >= min_pairs
        want = pctl(counts, q) if own else glob
        assert abs(got[n]["threshold"] - want) < 1e-9, (n, got[n]["threshold"], want)
        assert got[n]["source"] == ("namespace" if own else "global") and got[n]["pairs"] == len(counts)
    assert abs(got[ref.DEFAULT_NS]["threshold"] - glob) < 1e-9

    minutes = defaultdict(set)
    edits, bots = Counter(), Counter()
    for _, w, n, t, bot, e in uniq:
        h = (e // 3600) % 24
        minutes[h].add(e // 60)
        edits[(w, n, h)] += 1
        bots[(w, n, h)] += bot
    base = {(r["wiki"], r["namespace"], r["hour"]): r for r in ref.baseline(df).collect()}
    assert set(base) == set(edits)
    for k, c in edits.items():
        assert base[k]["edits"] == c
        assert abs(base[k]["rate_per_min"] - c / len(minutes[k[2]])) < 1e-9, k
        assert abs(base[k]["bot_share"] - bots[k] / c) < 1e-9, k
    return len(pairs), len(edits)


def synthetic():
    rows, i = [], 0

    def add(w, n, title, minute, count, bot=False):
        nonlocal i
        for s in range(count):
            i += 1
            rows.append((i, w, n, title, bot, BASE + minute * 60 + s))

    for p in range(1500):                          # ns 0: 1500 pairs, mostly single edits
        add("enwiki", 0, "p%d" % p, p, 1)
    add("enwiki", 0, "hot", 7, 50, bot=True)       # one surge
    for p in range(9):
        add("enwiki", 0, "warm%d" % p, 20 + p, 3)
    for p in range(10):                            # ns 1: only 10 pairs, below min_pairs, falls back to global
        add("dewiki", 1, "t%d" % p, 30 + p, 2)
    rows += rows[:200]                             # duplicated ids, as after a producer restart
    return rows


def main():
    spark = session("test_reference", cores="2")
    spark.sparkContext.setLogLevel("ERROR")

    pairs, groups = check(spark, synthetic(), 0.999, 1000)
    print("REFERENCE synthetic ok: %d pairs, %d baseline groups" % (pairs, groups))
    # hand-checkable: ns 0 has 1500 + 1 + 9 = 1510 pairs; sorted tail is ..., 1, 3 x9, 50; q=0.999 lands inside the 3s
    th = {r["namespace"]: r for r in ref.thresholds(ref.page_minute_counts(ref.dedup(frame(spark, synthetic()))),
                                                    0.999, 1000).collect()}
    assert th[0]["threshold"] == 3.0 and th[1]["source"] == "global", (th[0], th[1])

    real = spark.read.parquet(ARCHIVE).filter("date = '2026-10-04' and hour = 11") \
        .select("id", "wiki", "namespace", "title", "bot", F.col("ts").cast("long").alias("epoch")).collect()
    rows = [tuple(r) for r in real]
    pairs, groups = check(spark, rows, 0.99, 50)  # q and min_pairs tuned down so a one-hour slice exercises both paths
    print("REFERENCE real slice ok: %d rows (with duplicates), %d pairs, %d baseline groups" % (len(rows), pairs, groups))
    spark.stop()
    print("REFERENCE TESTS OK")


if __name__ == "__main__":
    main()
