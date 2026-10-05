"""Replay the Parquet archive into a Kafka topic at a controlled rate.

Used by the evaluation (throughput, key skew, latency, watermark and fault experiments) and by anyone without a live
feed. Records are sent in the live producer's format (same key and value functions), in archive arrival order.

  python replay.py --topic wiki-replay --partitions 6 --rate 2000 --limit 100000      # fixed events per second
  python replay.py --topic wiki-replay --speed 1 --start 2026-10-04T11:00 --end 2026-10-04T12:00
      # paced by the archive's own arrival times; timestamps are shifted so event time tracks the wall clock

--rate 0 (the default) sends as fast as the broker accepts. The live topic is refused: replayed history must never
mix into it.
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from producer import kafka_value, key as page_key

LIVE_TOPIC = "wiki-edits"
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")


def wiki_key(rec):
    return rec["wiki"].encode()


KEYS = {"page": page_key, "wiki": wiki_key}  # 'page' is what the live producer uses; 'wiki' is the skew baseline


def parse_time(s):
    t = datetime.fromisoformat(s)
    return t if t.tzinfo else t.replace(tzinfo=timezone.utc)


def hour_dirs(fs, bucket):
    """[(hour start, [parquet paths])] for archive/date=YYYY-MM-DD/hour=HH/, oldest first."""
    dirs = {}
    for info in fs.get_file_info(pafs.FileSelector(bucket, recursive=True)):
        if info.type == pafs.FileType.File and info.path.endswith(".parquet"):
            parts = info.path.split("/")
            date, hour = parts[-3].split("=")[1], parts[-2].split("=")[1]
            dirs.setdefault(datetime.fromisoformat(date).replace(hour=int(hour), tzinfo=timezone.utc), []).append(info.path)
    return sorted(dirs.items())


def records(fs, bucket, start=None, end=None):
    """Archive records with start <= ts < end, de-duplicated on id, in arrival order.

    Hours are replayed one after another and sorted by arrival inside each. Duplicates share their revision timestamp,
    hence their hour directory, so de-duplicating per directory is exact. An event that arrived just after an hour
    boundary but belongs to the previous hour is replayed a few seconds early (displacement is at most the lag).
    """
    for hour, files in hour_dirs(fs, bucket):
        if (end and hour >= end) or (start and hour + timedelta(hours=1) <= start):
            continue
        rows = pa.concat_tables([pq.read_table(f, filesystem=fs) for f in files]).to_pylist()
        rows = [r for r in rows if (not start or r["ts"] >= start) and (not end or r["ts"] < end)]
        seen = set()
        for r in sorted(rows, key=lambda r: (r["arrival"], r["id"])):
            if (r["wiki"], r["id"]) not in seen:   # id is MediaWiki's per-wiki counter, so the key is (wiki, id)
                seen.add((r["wiki"], r["id"]))
                yield r


def offset(i, rec, first_arrival, rate, speed):
    """Seconds after the start at which event i is due."""
    if speed:
        return (rec["arrival"] - first_arrival).total_seconds() / speed
    return i / rate if rate else 0.0


def ensure_topic(admin, topic, partitions, recreate):
    def listed():
        return admin.list_topics(timeout=15).topics
    if topic in listed():
        if recreate:
            admin.delete_topics([topic])[topic].result(30)
            for _ in range(60):
                if topic not in listed():
                    break
                time.sleep(0.5)
        else:
            have = len(listed()[topic].partitions)
            if have != partitions:
                sys.exit("topic %s has %d partitions, not %d: use --recreate (it only ever deletes a replay topic)"
                         % (topic, have, partitions))
            return
    for attempt in range(20):  # a just-deleted topic can briefly refuse to be created
        try:
            admin.create_topics([NewTopic(topic, num_partitions=partitions, replication_factor=1)])[topic].result(30)
            return
        except Exception as e:
            if attempt == 19:
                raise
            time.sleep(1)


def main(argv=None):
    a = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    a.add_argument("--topic", default="wiki-replay")
    a.add_argument("--partitions", type=int, default=6)
    a.add_argument("--recreate", action="store_true", help="delete and recreate the replay topic first")
    a.add_argument("--key", choices=KEYS, default="page", help="page = wiki|title (live), wiki = wiki only")
    a.add_argument("--rate", type=float, default=0, help="events per second; 0 = unthrottled")
    a.add_argument("--speed", type=float, default=0, help="pace by archive arrival times (1 = real time) and shift timestamps to now")
    a.add_argument("--start", type=parse_time, help="first revision timestamp, ISO, UTC")
    a.add_argument("--end", type=parse_time, help="revision timestamp upper bound (exclusive)")
    a.add_argument("--limit", type=int, default=0, help="stop after this many events")
    a.add_argument("--bucket", default=os.environ.get("BUCKET", "archive"))
    a.add_argument("--info", action="store_true", help="print the archive's hour range as JSON and exit")
    args = a.parse_args(argv)
    if args.topic == LIVE_TOPIC:
        sys.exit("refusing to replay into the live topic %r" % LIVE_TOPIC)
    if args.rate and args.speed:
        sys.exit("--rate and --speed are alternatives")

    fs = pafs.S3FileSystem(endpoint_override=os.environ.get("S3_ENDPOINT", "minio:9000"), scheme="http",
                           access_key=os.environ.get("MINIO_USER", "minioadmin"),
                           secret_key=os.environ.get("MINIO_PASSWORD", "minioadmin"))
    if args.info:
        hours = [h for h, _ in hour_dirs(fs, args.bucket)]
        print("INFO " + json.dumps({"first_hour": hours[0].isoformat(), "last_hour": hours[-1].isoformat(), "hours": len(hours)}))
        return
    ensure_topic(AdminClient({"bootstrap.servers": BOOTSTRAP}), args.topic, args.partitions, args.recreate)
    prod = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 20, "compression.type": "lz4",
                     "queue.buffering.max.messages": 500000})
    keyfn = KEYS[args.key]
    stats = {"acked": 0, "failed": 0}

    def delivered(err, msg):
        stats["failed" if err else "acked"] += 1

    first, shift, t0, sent = None, timedelta(0), None, 0
    for rec in records(fs, args.bucket, args.start, args.end):
        if first is None:
            first, t0 = rec["arrival"], time.time()
            if args.speed:
                shift = datetime.now(timezone.utc) - first
        due = offset(sent, rec, first, args.rate, args.speed)  # from archive time, before the timestamps are shifted
        if args.speed:
            rec = {**rec, "ts": rec["ts"] + shift, "arrival": rec["arrival"] + shift}
        delay = t0 + due - time.time()
        if delay > 0:
            time.sleep(delay)
        while True:
            try:
                prod.produce(args.topic, key=keyfn(rec), value=kafka_value(rec), on_delivery=delivered)
                break
            except BufferError:  # local queue full: let it drain
                prod.poll(0.5)
        prod.poll(0)
        sent += 1
        if args.limit and sent >= args.limit:
            break
    prod.flush()
    elapsed = time.time() - t0 if t0 else 0.0
    out = {"topic": args.topic, "partitions": args.partitions, "key": args.key, "sent": sent, **stats,
           "seconds": round(elapsed, 2), "events_per_second": round(sent / elapsed, 1) if elapsed else 0,
           "rate": args.rate, "speed": args.speed}
    print("REPLAY " + json.dumps(out), flush=True)
    if stats["failed"] or stats["acked"] != sent:
        sys.exit("REPLAY FAILED: %d sent, %d acked, %d failed" % (sent, stats["acked"], stats["failed"]))


if __name__ == "__main__":
    main()
