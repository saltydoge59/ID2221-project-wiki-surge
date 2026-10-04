# Real-Time Detection of Editing Surges on Wikipedia

ID2221 Data-Intensive Computing, HT26, final project (Task 2), Group 18.
Proposal: `ID2221_project_proposal.pdf`.

A streaming pipeline that detects per-page editing surges on Wikipedia and labels them bot-driven or
human-driven, plus an evaluation of the streaming system itself (watermarks, state size, key skew,
throughput/latency, fault recovery).

```
Wikimedia SSE ──> producer ──┬──> Kafka (wiki-edits, keyed wiki|title) ──> Spark Structured Streaming ──> Cassandra ──> Streamlit
 recentchange                └──> MinIO  archive/date=…/hour=…/*.parquet ──> Spark SQL batch (thresholds), replay
```

## Status

| Part | State |
|---|---|
| Kafka (KRaft), MinIO, topic, producer + Parquet archive | done, collecting |
| Spark + Cassandra in compose, batch thresholds job | in progress |
| Streaming job, replay tool, dashboard, evaluation, report | not started |

## Requirements

Docker with Compose v2 and about 6 GB of RAM for Docker. Nothing else is installed on the host.
No cloud account or credential is needed. Versions are pinned in `docker-compose.yml` and the Dockerfiles.

## Run the collector

```sh
cd Project
CONTACT="you@example.com" docker compose up -d --build   # first build compiles MinIO, a few minutes
docker compose ps                                        # kafka, minio, producer should be Up; init exits 0
docker compose logs -f producer                          # "flushed; total edits=N" every 5 minutes
```

`CONTACT` is optional. Wikimedia asks API clients to identify themselves, and the value goes into the
User-Agent header sent to stream.wikimedia.org. Leave it unset to send `no-contact-given`.

MinIO console: http://localhost:9001 (login `minioadmin` / `minioadmin`, a local-only default; override with
`MINIO_USER` / `MINIO_PASSWORD`). Kafka from the host: `localhost:29092`.

## What you need to do during the collection window

The archive needs about 10 days of continuous collection (started 2026-10-04, stop around 2026-10-15).

1. **Keep the Mac plugged in and Docker Desktop running.** A sleeping Mac pauses the collector. An 11-day
   `caffeinate -dimsu` is already running; if you reboot or it exits, restart it with
   `nohup caffeinate -dimsu -t 600000 >/dev/null 2>&1 &`.
2. **Never run `docker compose down -v`** (or `docker volume prune`). It deletes the Kafka and MinIO volumes,
   which hold the archive. `docker compose down` without `-v` is safe.
3. **Check on it now and then:** `docker compose ps` and the last `flushed` line in the producer log. The
   producer reconnects with backoff and resumes from the last archived event id, so short outages leave no
   hole as long as they are within Wikimedia's replay window (documented as about 7 days; not verified here).
4. **Keep some disk free.** The archive is small (about 0.15 GB/day), but Docker images and build cache are
   large. Check `df -h` if the machine gets near full.
5. **Stop collecting** when the window ends: `docker compose stop producer`. Kafka and MinIO can stay up.

Restarting the producer is safe. It resumes from the last archived event id, so Kafka may receive a few
duplicate messages; the batch job de-duplicates on `id`.

## Data

Records are projected from the `recentchange` feed to edits and new pages only (`type in ('edit','new')`).
Fields: `id, wiki, title, namespace, user, bot, type, old_len, new_len, ts` (revision timestamp, the event
time) and `arrival` (`meta.dt`, when the event entered the stream). The proposal says ten fields; the
archive keeps eleven, because `id` is needed for de-duplication and `arrival` for lag and latency measurement.

Kafka keeps messages for 24 hours. The Parquet archive in MinIO is the durable copy.

## Tests

```sh
docker compose run --rm --no-deps -v "$PWD/producer/test_producer.py:/app/test_producer.py" producer \
  sh -c "pip install -q pytest && python -m pytest -q /app/test_producer.py"
```

## Notes

- MinIO no longer publishes Docker images or binaries, so `minio/Dockerfile` builds a pinned source commit
  (`7aac2a2c`). The upstream repository is archived, which does not matter for a local-only run.
- The producer is at-least-once. After a restart Kafka may see duplicates, and the archive can too if the
  container dies between a Parquet flush and the state-file write. Downstream jobs de-duplicate on `id`.
