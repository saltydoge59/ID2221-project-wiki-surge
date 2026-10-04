"""Wikimedia recentchange SSE -> Kafka (keyed by wiki+title) + date/hour-partitioned Parquet in MinIO."""
import json
import os
import time
from datetime import datetime, timezone

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import requests
from confluent_kafka import Producer

URL = "https://stream.wikimedia.org/v2/stream/recentchange"
# Wikimedia asks clients to identify themselves; set CONTACT to an address you are happy to share.
UA = "ID2221-KTH-student-project-group18 (%s)" % os.environ.get("CONTACT", "no-contact-given")
TOPIC = os.environ.get("TOPIC", "wiki-edits")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
FLUSH_SECONDS = int(os.environ.get("FLUSH_SECONDS", "300"))
FLUSH_ROWS = int(os.environ.get("FLUSH_ROWS", "50000"))
STATE = os.environ.get("STATE_FILE", "/state/last_event_id")

SCHEMA = pa.schema([
    ("id", pa.int64()), ("wiki", pa.string()), ("title", pa.string()), ("namespace", pa.int32()),
    ("user", pa.string()), ("bot", pa.bool_()), ("type", pa.string()),
    ("old_len", pa.int32()), ("new_len", pa.int32()),
    ("ts", pa.timestamp("s", tz="UTC")),          # revision timestamp = event time
    ("arrival", pa.timestamp("ms", tz="UTC")),    # meta.dt = when the event entered the stream
])


def project(ev):
    """Keep only edit/new events and the fields the pipeline uses; None for everything else."""
    if ev.get("type") not in ("edit", "new"):
        return None
    length = ev.get("length") or {}
    return {
        "id": ev["id"], "wiki": ev["wiki"], "title": ev["title"], "namespace": ev["namespace"],
        "user": ev.get("user"),  # absent when the editor's name is hidden (revision-deleted); stored as null
        "bot": bool(ev["bot"]), "type": ev["type"],
        "old_len": length.get("old") or 0, "new_len": length.get("new") or 0,
        "ts": datetime.fromtimestamp(ev["timestamp"], timezone.utc),
        "arrival": datetime.fromisoformat(ev["meta"]["dt"].replace("Z", "+00:00")),
    }


def key(rec):
    return ("%s|%s" % (rec["wiki"], rec["title"])).encode()


def kafka_value(rec):
    return json.dumps({**rec, "ts": rec["ts"].isoformat(), "arrival": rec["arrival"].isoformat()}).encode()


def sse(last_id):
    """Yield (event_id, data) pairs; raises on disconnect so the caller can back off and resume."""
    headers = {"User-Agent": UA, "Accept": "text/event-stream"}
    if last_id:
        headers["Last-Event-ID"] = last_id
    with requests.get(URL, headers=headers, stream=True, timeout=(10, 60)) as r:
        r.raise_for_status()
        eid, data = None, []
        for line in r.iter_lines(decode_unicode=True):
            if line.startswith("id:"):
                eid = line[3:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].strip())
            elif line == "" and data:
                yield eid, "".join(data)
                eid, data = None, []


class Archive:
    def __init__(self):
        self.fs = pafs.S3FileSystem(
            endpoint_override=os.environ.get("S3_ENDPOINT", "minio:9000"), scheme="http",
            access_key=os.environ.get("MINIO_USER", "minioadmin"),
            secret_key=os.environ.get("MINIO_PASSWORD", "minioadmin"), allow_bucket_creation=True)
        self.bucket = os.environ.get("BUCKET", "archive")
        self.fs.create_dir(self.bucket)  # no-op if it already exists
        self.buf, self.last_flush, self.n_files = [], time.time(), 0

    def add(self, rec):
        self.buf.append(rec)

    def due(self):
        return len(self.buf) >= FLUSH_ROWS or time.time() - self.last_flush >= FLUSH_SECONDS

    def flush(self):
        by_hour = {}
        for r in self.buf:
            by_hour.setdefault(r["ts"].strftime("date=%Y-%m-%d/hour=%H"), []).append(r)
        for part, rows in by_hour.items():
            path = "%s/%s/part-%d-%05d.parquet" % (self.bucket, part, time.time() * 1000, self.n_files)
            self.n_files += 1
            pq.write_table(pa.Table.from_pylist(rows, schema=SCHEMA), path, filesystem=self.fs)
        self.buf, self.last_flush = [], time.time()


def main():
    prod = Producer({"bootstrap.servers": BOOTSTRAP, "linger.ms": 50, "compression.type": "lz4"})
    arc = Archive()
    # Two resume points: after a process restart the buffer is gone, so resume from the last id the archive
    # holds (state file); on an in-process reconnect the buffer is intact, so resume from the last id seen.
    flushed_id = open(STATE).read().strip() if os.path.exists(STATE) else None
    seen_id, backoff, n, skipped = flushed_id, 1, 0, 0
    while True:
        try:
            for eid, data in sse(seen_id):
                backoff = 1
                try:
                    rec = project(json.loads(data))
                except (KeyError, TypeError, ValueError) as e:
                    # a malformed event must not stall the stream: reconnecting would replay the same event forever
                    rec, skipped = None, skipped + 1
                    print("skipped malformed event %r: %s" % (e, data[:200]), flush=True)
                if rec:
                    prod.produce(TOPIC, key=key(rec), value=kafka_value(rec))
                    arc.add(rec)
                    n += 1
                if eid:
                    seen_id = eid
                prod.poll(0)
                if arc.due():
                    arc.flush()
                    prod.flush()
                    flushed_id = seen_id  # only advance the restart resume point once the archive holds the data
                    if flushed_id:
                        with open(STATE, "w") as f:
                            f.write(flushed_id)
                    print("flushed; total edits=%d skipped=%d" % (n, skipped), flush=True)
        except Exception as e:  # reconnect with exponential backoff; Last-Event-ID avoids holes
            print("stream error: %r; retry in %ds" % (e, backoff), flush=True)
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)


if __name__ == "__main__":
    main()
