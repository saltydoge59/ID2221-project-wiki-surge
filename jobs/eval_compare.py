"""Diff the evaluation tables windows_eval_<a|b> and alerts_eval_<a|b>: used by the fault-injection experiment.

  spark-submit /jobs/eval_compare.py a b      prints one line:  COMPARE {"windows": {...}, "alerts": {...}, "identical": bool}
written_at is ignored (it is the write time); every other column must match exactly.
"""
import json
import sys

from pyspark.sql import functions as F

from common import session

ta, tb = sys.argv[1], sys.argv[2]
spark = session("eval_compare", cores="2")
spark.sparkContext.setLogLevel("ERROR")
read = lambda t: spark.read.format("org.apache.spark.sql.cassandra").options(table=t, keyspace="wiki").load()


def diff(a, b, key, cols):
    only_a, only_b = a.join(b, key, "left_anti").count(), b.join(a, key, "left_anti").count()
    j = a.alias("a").join(b.alias("b"), key)
    differs = F.lit(False)
    for c in cols:
        differs = differs | ~F.col("a." + c).eqNullSafe(F.col("b." + c))
    return {"rows_a": a.count(), "rows_b": b.count(), "only_a": only_a, "only_b": only_b, "differing": j.filter(differs).count()}


w = diff(read("windows_eval_" + ta), read("windows_eval_" + tb), ["wiki", "day", "window_start", "page"],
         ["edit_count", "distinct_editors", "bot_fraction", "net_bytes", "last_ts"])
al = diff(read("alerts_eval_" + ta), read("alerts_eval_" + tb), ["day", "window_start", "wiki", "page"],
          ["edit_count", "distinct_editors", "bot_fraction", "label"])
same = lambda d: d["only_a"] == d["only_b"] == d["differing"] == 0 and d["rows_a"] > 0
print("COMPARE " + json.dumps({"windows": w, "alerts": al, "identical": same(w) and al["only_a"] == al["only_b"] == al["differing"] == 0}), flush=True)
spark.stop()
