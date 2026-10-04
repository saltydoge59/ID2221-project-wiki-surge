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
                     StructField("bot", BooleanType()), StructField("epoch", LongType()),
                     StructField("user", StringType())])
BASE = 1791115200  # 2026-10-04 12:00:00 UTC, a minute boundary


def pctl(vals, q):  # linear interpolation, the definition Spark's exact percentile() uses
    v = sorted(vals)
    pos = q * (len(v) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(v) - 1)
    return v[lo] + (v[hi] - v[lo]) * (pos - lo)


def frame(spark, rows):
    return spark.createDataFrame(rows, SCHEMA).withColumn("ts", F.timestamp_seconds("epoch")) \
        .withColumn("arrival", F.col("ts"))


def check(spark, rows, q, min_pairs, min_editors):
    """Compare Spark's thresholds and baseline with an oracle computed from the raw (duplicated) rows."""
    uniq = list({r[0]: r for r in rows}.values())  # dedup on id
    count, users = Counter(), defaultdict(set)
    for _, w, n, t, _, e, u in uniq:
        count[(w, n, t, e // 60)] += 1
        if u is not None:
            users[(w, n, t, e // 60)].add(u)
    contested = {k: c for k, c in count.items() if len(users[k]) >= min_editors}
    by_ns = defaultdict(list)
    for (w, n, t, m), c in contested.items():
        by_ns[n].append(c)
    glob = pctl(list(contested.values()), q)
    all_ns = {k[1] for k in count}

    df = ref.dedup(frame(spark, rows))
    got = {r["namespace"]: r for r in ref.thresholds(ref.page_minute_counts(df), q, min_pairs, min_editors).collect()}
    assert set(got) == all_ns | {ref.DEFAULT_NS}, (sorted(got), sorted(all_ns))  # every namespace has a row
    for n in all_ns:
        own = len(by_ns[n]) >= min_pairs
        want = pctl(by_ns[n], q) if own else glob
        assert abs(got[n]["threshold"] - want) < 1e-9, (n, got[n]["threshold"], want)
        assert got[n]["source"] == ("namespace" if own else "global") and got[n]["pairs"] == len(by_ns[n]), (n, got[n])
    assert abs(got[ref.DEFAULT_NS]["threshold"] - glob) < 1e-9

    minutes = defaultdict(set)
    edits, bots = Counter(), Counter()
    for _, w, n, t, bot, e, _ in uniq:
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
    return len(contested), len(count), len(edits)


def synthetic():
    rows, i = [], 0

    def add(w, n, title, minute, count, editors=1, bot=False):
        nonlocal i
        for s in range(count):
            i += 1
            rows.append((i, w, n, title, bot, BASE + minute * 60 + s, "u%d" % (s % editors)))

    for p in range(1500):                                  # ns 0: 1500 contested pairs of 2 edits by 2 editors
        add("enwiki", 0, "p%d" % p, p, 2, editors=2)
    add("enwiki", 0, "hot", 7, 50, editors=5, bot=True)    # one contested surge
    for p in range(9):
        add("enwiki", 0, "warm%d" % p, 20 + p, 3, editors=3)
    add("wikidatawiki", 0, "Q1", 40, 80)                   # one bot, 80 edits: must NOT move the quantile
    for p in range(10):                                    # ns 1: 10 contested pairs, below min_pairs: global
        add("dewiki", 1, "t%d" % p, 50 + p, 2, editors=2)
    add("dewiki", 2, "solo", 70, 30)                       # ns 2: a single editor only: no contested pair at all
    rows += rows[:200]                                     # duplicated ids, as after a producer restart
    rows.append((10 ** 6, "enwiki", 0, "hidden", False, BASE + 90 * 60, None))  # hidden editor: not counted
    return rows


def main():
    spark = session("test_reference", cores="2")
    spark.sparkContext.setLogLevel("ERROR")

    contested, pairs, groups = check(spark, synthetic(), 0.999, 1000, 2)
    print("REFERENCE synthetic ok: %d contested of %d pairs, %d baseline groups" % (contested, pairs, groups))
    # hand-checkable: ns 0 contested counts are 2 x1500, 3 x9, 50 (1510 values); q=0.999 sits at position 1507.49,
    # inside the 3s. Counting the 80-edit single-editor window would move it to 26.
    pm = ref.page_minute_counts(ref.dedup(frame(spark, synthetic())))
    th = {r["namespace"]: r for r in ref.thresholds(pm, 0.999, 1000, 2).collect()}
    assert th[0]["threshold"] == 3.0 and th[1]["source"] == "global", (th[0], th[1])
    assert th[2]["pairs"] == 0 and th[2]["source"] == "global", th[2]  # present, with no contested window
    one = {r["namespace"]: r["threshold"] for r in ref.thresholds(pm, 0.999, 1000, 1).collect()}
    assert one[0] != 3.0, "min_editors=1 must let single-editor windows into the quantile"

    real = spark.read.parquet(ARCHIVE).filter("date = '2026-10-04' and hour = 11") \
        .select("id", "wiki", "namespace", "title", "bot", F.col("ts").cast("long").alias("epoch"), "user").collect()
    rows = [tuple(r) for r in real]
    # q and min_pairs tuned down so a one-hour slice exercises both the per-namespace and the fallback path
    contested, pairs, groups = check(spark, rows, 0.99, 20, 2)
    print("REFERENCE real slice ok: %d rows (with duplicates), %d contested of %d pairs, %d baseline groups"
          % (len(rows), contested, pairs, groups))
    spark.stop()
    print("REFERENCE TESTS OK")


if __name__ == "__main__":
    main()
