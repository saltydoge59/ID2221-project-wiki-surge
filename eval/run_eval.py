#!/usr/bin/env python3
"""Evaluation orchestrator. Host side, standard library only; it drives the compose containers.

  python3 eval/run_eval.py throughput|skew|latency|watermark|fault|all [--quick] [--hours N] [--reps R]

  throughput  (a) events/s against Kafka partitions x Spark cores, and with the Cassandra sink vs a no-op sink
  skew        (b) key wiki|title against key wiki: messages and task time per Kafka partition
  latency     (c) revision timestamp -> Cassandra, against the trigger interval (replay at exactly real time)
  watermark   (d) state size, rows dropped as late and completeness, against the watermark length
  fault       (e) kill the driver mid-stream, restart from the checkpoint, diff the tables against an uninterrupted run

Results go to results/eval/<experiment>.csv, raw per-run JSON and logs to results/eval/raw/. Everything runs on replay
topics named ev-* and the scratch tables windows_eval_a/b; the live topic and the real tables are never touched. The
live `stream` service is stopped for the duration (it competes for the CPUs) and restarted afterwards.
"""
import argparse
import csv
import json
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
RES = ROOT / "results" / "eval"
RAW = RES / "raw"
SPARK = ["exec", "-T", "spark", "/opt/spark/bin/spark-submit"]


def dc(*a, check=True, timeout=None, **kw):
    r = subprocess.run(["docker", "compose", *a], cwd=ROOT, capture_output=True, text=True, timeout=timeout, **kw)
    if check and r.returncode:
        raise RuntimeError("docker compose %s failed (%d):\n%s" % (" ".join(a[:6]), r.returncode, (r.stdout + r.stderr)[-1500:]))
    return r


def log(msg):
    print("[%s] %s" % (datetime.now().strftime("%H:%M:%S"), msg), flush=True)


# macOS has a clock that does not run while the machine sleeps; elsewhere the monotonic clock already stops in suspend.
_AWAKE = getattr(time, "CLOCK_UPTIME_RAW", None)
SLEPT_LIMIT_S = 5


def awake_now():
    return time.clock_gettime(_AWAKE) if _AWAKE is not None else time.monotonic()


def guarded(fn, tries=3, clock=time.time, awake=awake_now):
    """Call fn(); if the machine slept during it, discard the result and repeat. A run that spans a sleep measures the
    sleep, not the system (a closed lid once turned a 30 second run into 34 minutes)."""
    for attempt in range(1, tries + 1):
        w0, a0 = clock(), awake()
        failed = None
        try:
            result = fn()
        except Exception as e:
            result, failed = None, e
        slept = (clock() - w0) - (awake() - a0)
        if slept <= SLEPT_LIMIT_S:
            if failed:
                raise failed
            return result
        log("the machine slept for %.0f s during this run; repeating it (attempt %d of %d)" % (slept, attempt, tries))
    raise RuntimeError("the machine kept sleeping: keep the lid open (or the Mac on power with a display) and rerun")


# ---------------------------------------------------------------- plumbing

def iso(t):
    return t.astimezone(timezone.utc).isoformat()


def archive_slice(hours, minutes=0):
    """(start, end) of a slice of the archive that starts at its first complete hour (the first hour is partial)."""
    info = json.loads(next(l for l in dc("run", "--rm", "replay", "--info").stdout.splitlines() if l.startswith("INFO "))[5:])
    start = datetime.fromisoformat(info["first_hour"]) + timedelta(hours=1)
    end = start + timedelta(hours=hours, minutes=minutes)
    if end > datetime.fromisoformat(info["last_hour"]):
        sys.exit("the archive is too short for %sh%sm (it ends in the hour starting %s)" % (hours, minutes, info["last_hour"]))
    return start, end


def topic_reset(topic, partitions):
    kt = ["exec", "-T", "kafka", "/opt/kafka/bin/kafka-topics.sh", "--bootstrap-server", "localhost:9092"]
    assert topic.startswith("ev-"), "refusing to touch topic %r" % topic
    dc(*kt, "--delete", "--if-exists", "--topic", topic, check=False)
    for _ in range(60):
        if topic not in dc(*kt, "--list").stdout.split():
            break
        time.sleep(1)
    for _ in range(20):
        r = dc(*kt, "--create", "--topic", topic, "--partitions", str(partitions), "--replication-factor", "1", check=False)
        if r.returncode == 0:
            return
        time.sleep(1)
    raise RuntimeError("could not create topic " + topic)


def preload(topic, partitions, key, start, end):
    r = dc("run", "--rm", "replay", "--topic", topic, "--partitions", str(partitions), "--recreate", "--key", key,
           "--start", iso(start), "--end", iso(end), timeout=1800)
    out = json.loads(next(l for l in r.stdout.splitlines() if l.startswith("REPLAY "))[7:])
    log("preloaded %s: %d events, %d partitions, key=%s" % (topic, out["sent"], partitions, key))
    return out["sent"]


def truncate(suffix):
    dc("exec", "-T", "cassandra", "cqlsh", "-e", "TRUNCATE wiki.windows_eval_%s; TRUNCATE wiki.alerts_eval_%s" % (suffix, suffix))


def offsets(topic):
    r = dc("exec", "-T", "kafka", "/opt/kafka/bin/kafka-get-offsets.sh", "--bootstrap-server", "localhost:9092", "--topic", topic)
    return [int(l.split(":")[2]) for l in sorted(r.stdout.split(), key=lambda s: int(s.split(":")[1]))]


def spark_args(name, a, ckpt=None):
    return ["/jobs/eval_stream.py", "--out", "/results/eval/raw/%s.json" % name, "--checkpoint", "/checkpoints/" + (ckpt or name)] + [str(x) for x in a]


def run_measured(name, a, timeout=3600, ckpt=None):
    """Run jobs/eval_stream.py to completion; returns its JSON."""
    RAW.mkdir(parents=True, exist_ok=True)
    r = dc(*SPARK, *spark_args(name, a, ckpt), check=False, timeout=timeout)
    (RAW / (name + ".log")).write_text(r.stdout + r.stderr)
    if r.returncode or "EVAL OK" not in r.stdout:
        raise RuntimeError("measured run %s failed, see results/eval/raw/%s.log" % (name, name))
    return json.loads((RAW / (name + ".json")).read_text())


def start_measured(name, a):
    """Start a measured run in the background; returns the Popen. Wait for <name>.json.ready, stop with <name>.json.stop."""
    RAW.mkdir(parents=True, exist_ok=True)
    for suffix in (".json.ready", ".json.stop", ".json"):
        (RAW / (name + suffix)).unlink(missing_ok=True)
    f = open(RAW / (name + ".log"), "w")
    return subprocess.Popen(["docker", "compose", *SPARK, *spark_args(name, a)], cwd=ROOT, stdout=f, stderr=subprocess.STDOUT, text=True)


def wait_for(path, proc, what, timeout=300):
    t0 = time.time()
    while not path.exists():
        if proc.poll() is not None:
            raise RuntimeError("%s: process ended early, see its log" % what)
        if time.time() - t0 > timeout:
            raise RuntimeError("timed out waiting for " + what)
        time.sleep(0.5)


def stop_measured(name, proc, timeout=300):
    (RAW / (name + ".json.stop")).write_text("stop")
    proc.wait(timeout=timeout)
    out = RAW / (name + ".json")
    if proc.returncode or not out.exists():
        raise RuntimeError("measured run %s failed, see results/eval/raw/%s.log" % (name, name))
    return json.loads(out.read_text())


def write_csv(name, rows):
    RES.mkdir(parents=True, exist_ok=True)
    with open(RES / (name + ".csv"), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    log("wrote results/eval/%s.csv (%d rows)" % (name, len(rows)))


def agg_state(summary):
    """State rows and memory of the aggregation operator and of the de-duplication operator."""
    out = {"agg_rows": None, "agg_mem": None, "dedup_rows": None, "dedup_mem": None, "dropped_late": 0}
    for k, v in summary["state_max"].items():
        kind = "dedup" if "dedup" in k.lower() else "agg"
        out[kind + "_rows"], out[kind + "_mem"] = v["rows"], v["mem"]
        out["dropped_late"] += v["dropped"] if kind == "agg" else 0
    return out


# ---------------------------------------------------------------- experiments

def throughput(q):
    start, end = archive_slice(1 if q.quick else q.hours)
    rows, grid_p, grid_c = [], ([1, 6] if q.quick else [1, 2, 4, 6]), ([1, 4] if q.quick else [1, 2, 4, 8])
    for p in grid_p:
        topic = "ev-thr-p%d" % p
        n = preload(topic, p, "page", start, end)
        for sink in ("cassandra", "noop") if p == 6 or q.quick else ("cassandra",):
            for c in grid_c:
                for rep in range(q.reps):
                    name = "thr-%s-p%d-c%d-r%d" % (sink, p, c, rep)
                    log("throughput: sink=%s partitions=%d cores=%d rep=%d" % (sink, p, c, rep))

                    def one():
                        truncate("a")
                        return run_measured(name, ["--topic", topic, "--cores", c, "--shuffle-partitions", p, "--kafka-partitions", p,
                                                   "--max-offsets", max(n // 8, 1), "--sink", sink, "--tables", "a"])
                    r = guarded(one)
                    s = r["summary"]
                    rows.append({"sink": sink, "partitions": p, "cores": c, "rep": rep, "events": s["rows"], "batches": s["batches"],
                                 "busy_s": s["busy_s"], "wall_s": r["wall_s"], "events_per_s": s["rows_per_s_busy"],
                                 "events_per_s_steady": s["rows_per_s_steady"], **{k: v for k, v in agg_state(s).items() if k.startswith("agg")}})
                    write_csv("throughput", rows)


def skew(q):
    start, end = archive_slice(1 if q.quick else q.hours)
    rows, summ = [], []
    for key in ("page", "wiki"):
        topic = "ev-key-" + key
        n = preload(topic, 6, key, start, end)
        counts = offsets(topic)
        for rep in range(q.reps):
            def one():
                truncate("a")
                return run_measured("skew-%s-r%d" % (key, rep), ["--topic", topic, "--cores", 6, "--shuffle-partitions", 6, "--kafka-partitions", 6,
                                                                "--max-offsets", max(n // 8, 1), "--sink", "noop", "--tables", "a", "--task-times"])
            r = guarded(one)
            ms = r["task_ms"]
            for i, c in enumerate(counts):
                rows.append({"key": key, "rep": rep, "partition": i, "messages": c, "task_ms": ms[i] if i < len(ms) else None})
            srt = sorted(ms)
            summ.append({"key": key, "rep": rep, "events": n, "messages_max_over_min": round(max(counts) / max(min(counts), 1), 2),
                         "task_ms_max_over_median": round(srt[-1] / srt[len(srt) // 2], 2) if srt else None,
                         "events_per_s": r["summary"]["rows_per_s_busy"], "events_per_s_steady": r["summary"]["rows_per_s_steady"]})
            write_csv("skew_partitions", rows)
            write_csv("skew_summary", summ)


def latency(q):
    minutes = 2 if q.quick else q.latency_minutes
    start, _ = archive_slice(1)
    end = start + timedelta(minutes=minutes)
    rows = []
    for trig in (["1 second", "10 seconds"] if q.quick else ["1 second", "5 seconds", "10 seconds", "30 seconds", "60 seconds"]):
        topic = "ev-lat"
        name = "lat-" + trig.replace(" ", "")
        log("latency: trigger=%s, %d minutes of archive replayed in real time" % (trig, minutes))

        def one():
            topic_reset(topic, 6)
            truncate("a")
            proc = start_measured(name, ["--topic", topic, "--trigger", trig, "--tables", "a", "--latency", "--cores", 4, "--watermark", "60 seconds"])
            try:
                wait_for(RAW / (name + ".json.ready"), proc, "query start")
                dc("run", "--rm", "replay", "--topic", topic, "--speed", "1", "--start", iso(start), "--end", iso(end), timeout=minutes * 60 + 600)
                time.sleep(float(trig.split()[0]) + 15)  # let the last trigger fire
            finally:
                return stop_measured(name, proc)
        r = guarded(one)
        lat = r["latency_s"]
        rows.append({"trigger": trig, "windows": lat["windows"], "p50_s": round(lat["p50"], 2), "p90_s": round(lat["p90"], 2),
                     "p99_s": round(lat["p99"], 2), "max_s": round(lat["max"], 2), "batches": r["summary"]["batches"],
                     "add_batch_ms_median": r["summary"]["add_batch_ms_median"]})
        write_csv("latency", rows)


def watermark(q):
    speed = 6.0  # replay 6x faster than real time with the trigger shortened by the same factor: event-time lateness as at 1x
    minutes = 12 if q.quick else q.watermark_minutes
    start, _ = archive_slice(1)
    end = start + timedelta(minutes=minutes)
    trigger_ms = int(10000 / speed)  # the equivalent of a 10 second trigger in real time
    rows = []
    cases = [("5 seconds", False), ("60 seconds", False)] if q.quick else \
        [(w, False) for w in ("0 seconds", "5 seconds", "10 seconds", "30 seconds", "60 seconds", "120 seconds")] + [("60 seconds", True)]
    for wm, nodedup in cases:
        topic = "ev-wm"
        name = "wm-%s%s" % (wm.replace(" ", ""), "-nodedup" if nodedup else "")
        log("watermark: %s%s, replay %dx, trigger %d ms, %d event-minutes" % (wm, " no dedup" if nodedup else "", speed, trigger_ms, minutes))
        a = ["--topic", topic, "--trigger", "%d milliseconds" % trigger_ms, "--watermark", wm, "--tables", "a", "--cores", 4,
             "--slice", iso(start), iso(end)] + (["--no-dedup"] if nodedup else [])

        def one():
            topic_reset(topic, 6)
            truncate("a")
            proc = start_measured(name, a)
            try:
                wait_for(RAW / (name + ".json.ready"), proc, "query start")
                dc("run", "--rm", "replay", "--topic", topic, "--speed", speed, "--start", iso(start), "--end", iso(end), timeout=minutes * 60 + 600)
                time.sleep(15)
            finally:
                return stop_measured(name, proc)
        r = guarded(one)
        s, c = r["summary"], r["completeness"]
        rows.append({"watermark": wm, "dedup": not nodedup, "events": c["expected_edits"], "counted": c["counted_edits"], "completeness": c["share"],
                     "batches": s["batches"], **agg_state(s)})
        write_csv("watermark", rows)


def compare(a_suffix, b_suffix):
    r = dc(*SPARK, "/jobs/eval_compare.py", a_suffix, b_suffix, timeout=900)
    return json.loads(next(l for l in r.stdout.splitlines() if l.startswith("COMPARE "))[8:])


def container_count(path):
    r = dc("exec", "-T", "spark", "sh", "-c", "ls %s 2>/dev/null | grep -v '^\\.' | wc -l" % path, check=False)
    return int(r.stdout.strip() or 0)


KILL = ("import os,signal\n"
        "for p in os.listdir('/proc'):\n"
        "    if p.isdigit() and int(p) != os.getpid():\n"
        "        try:\n"
        "            if b'eval_stream.py' in open('/proc/%s/cmdline' % p, 'rb').read(): os.kill(int(p), signal.SIGKILL)\n"
        "        except OSError: pass\n")


def fault(q):
    start, end = archive_slice(1 if q.quick else q.hours)
    topic = "ev-fault"
    n = preload(topic, 6, "page", start, end)
    common = ["--topic", topic, "--cores", 4, "--shuffle-partitions", 6, "--kafka-partitions", 6, "--max-offsets", max(n // 12, 1)]
    truncate("a")
    ref = run_measured("fault-a", common + ["--tables", "a"])
    total = ref["summary"]["batches"]
    log("uninterrupted reference run: %d batches" % total)
    rows = []
    for kill_at in ([3] if q.quick else [2, 5, 8]):
        name = "fault-b-k%d" % kill_at
        truncate("b")
        proc = start_measured(name, common + ["--tables", "b"])
        t0 = time.time()
        while container_count("/checkpoints/%s/commits" % name) < kill_at:
            if proc.poll() is not None or time.time() - t0 > 600:
                raise RuntimeError("run ended before the kill point")
            time.sleep(0.2)
        dc("exec", "-T", "spark", "python3", "-c", KILL, check=False)
        proc.wait(timeout=60)
        offs, commits = container_count("/checkpoints/%s/offsets" % name), container_count("/checkpoints/%s/commits" % name)
        log("killed %s after %d commits (%d offsets planned: %s)" % (name, commits, offs, "a batch was in flight" if offs > commits else "no batch in flight"))
        control = compare("a", "b")  # the interrupted tables must NOT equal the reference: proves the diff can fail
        run_measured(name + "-resume", common + ["--tables", "b", "--resume"], ckpt=name)  # same checkpoint: recovery
        final = compare("a", "b")
        rows.append({"kill_after_commits": kill_at, "batches_in_reference": total, "offsets_planned_at_kill": offs, "commits_at_kill": commits,
                     "in_flight_batch": offs > commits, "interrupted_windows_missing": control["windows"]["only_a"] + control["windows"]["differing"],
                     "windows_a": final["windows"]["rows_a"], "windows_b": final["windows"]["rows_b"],
                     "windows_only_a": final["windows"]["only_a"], "windows_only_b": final["windows"]["only_b"], "windows_differing": final["windows"]["differing"],
                     "alerts_a": final["alerts"]["rows_a"], "alerts_b": final["alerts"]["rows_b"], "alerts_differing": final["alerts"]["only_a"] + final["alerts"]["only_b"] + final["alerts"]["differing"],
                     "identical": final["identical"]})
        write_csv("fault", rows)
        if not final["identical"]:
            log("WARNING: tables differ after recovery")


def main():
    a = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    a.add_argument("experiment", choices=["throughput", "skew", "latency", "watermark", "fault", "all"])
    a.add_argument("--quick", action="store_true", help="small sizes, to check that the harness works")
    a.add_argument("--hours", type=int, default=6, help="archive hours replayed in throughput, skew and fault runs")
    a.add_argument("--latency-minutes", type=int, default=4, help="minutes replayed in real time per trigger setting")
    a.add_argument("--watermark-minutes", type=int, default=30, help="minutes of event time per watermark setting (replayed 6x faster)")
    a.add_argument("--reps", type=int, default=1)
    q = a.parse_args()
    todo = ["throughput", "skew", "latency", "watermark", "fault"] if q.experiment == "all" else [q.experiment]
    was_running = "stream" in dc("--profile", "app", "ps", "--services", "--filter", "status=running").stdout.split()
    if was_running:
        log("stopping the live stream service for the duration")
        dc("--profile", "app", "stop", "stream")
    try:
        for e in todo:
            log("=== " + e)
            globals()[e](q)
    finally:
        if was_running:
            log("restarting the live stream service")
            dc("--profile", "app", "start", "stream", check=False)


if __name__ == "__main__":
    main()
