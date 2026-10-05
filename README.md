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
| Spark 3.5.7 + Cassandra 5.0.4 in compose, smoke test (Kafka, Parquet, Cassandra round-trip) | done |
| Batch reference job (thresholds and hourly baselines) | done on the partial archive; re-run when collection ends |
| Streaming job (`jobs/stream.py`), tested against an oracle | done; not yet running continuously |
| Replay tool (`producer/replay.py`), tested | done |
| Dashboard (`dashboard/`) and the live `stream` service, tested and checked in a browser | done |
| Evaluation, report | not started |

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

The first `--build` also builds the Spark image (about 3 GB, 15 to 20 minutes on a slow link, because the
AWS SDK jar is 280 MB). Check that Spark can reach everything:

```sh
docker compose exec spark /opt/spark/bin/spark-submit /jobs/smoke.py   # last line must be: SMOKE OK
```

The `NoClassDefFoundError: jnr/posix/POSIXHandler` in its log is harmless (the Cassandra driver falls back to a
Java clock).

## Batch reference job

Computes the surge thresholds and hourly baselines from the archive (de-duplicated on `id`) and writes them to
Cassandra (`wiki.thresholds`, `wiki.baseline`) plus `results/reference_summary.json`. The threshold is the 99.9th
percentile (`QUANTILE`) of edits per (page, minute) per namespace, taken **only over contested windows**: those with
at least `MIN_EDITORS` (2) distinct editors. The biggest windows overall are one bot editing one page (30 to 52
edits), and a threshold set by them would sit above every real multi-editor surge. Namespaces with fewer than 1000
contested pairs (`MIN_PAIRS`) use a pooled value (namespace `-1`). On a few hours of data every namespace falls back
to the pooled value and nothing is flagged; the thresholds become meaningful on the full archive. It is safe to
re-run; rows are upserted.

```sh
docker compose exec spark /opt/spark/bin/spark-submit /jobs/reference.py        # last line: REFERENCE OK
docker compose exec spark /opt/spark/bin/spark-submit /jobs/test_reference.py   # last line: REFERENCE TESTS OK
```

## Streaming job

Reads Kafka, counts edits per (wiki, page) in one-minute event-time windows (event time is the revision timestamp,
`ts`), and upserts every changed window into `wiki.windows` and every flagged window into `wiki.alerts`. A window
is flagged when its edit count is strictly above the namespace threshold from `wiki.thresholds` and it has at
least `MIN_EDITORS` (2, shared with the reference job) distinct editors; it is labelled bot-dominated when the bot fraction is at least 0.5. Run the
reference job first, since the stream reads its thresholds.

```sh
docker compose exec spark /opt/spark/bin/spark-submit /jobs/stream.py          # runs until you stop it (Ctrl-C)
```

Settings are environment variables, for example `docker compose exec -e WATERMARK="2 minutes" -e TRIGGER="5 seconds" spark ...`:
`WATERMARK` (60 seconds), `TRIGGER` (10 seconds, or `availableNow` to drain the backlog and stop), `MIN_EDITORS` (2),
`DEDUP` (1), `CHECKPOINT` (`/checkpoints/stream`), `MAX_OFFSETS_PER_TRIGGER`, `SHUFFLE_PARTITIONS` (6), `CORES`.
Changing `SHUFFLE_PARTITIONS` or the query shape needs a new `CHECKPOINT` path.

Test (runs the job over the Kafka backlog into scratch tables and compares with a pure-Python oracle built from the
archive; about 2 to 4 minutes):

```sh
docker compose exec cassandra cqlsh -e "TRUNCATE wiki.windows_test; TRUNCATE wiki.alerts_test"
docker compose exec spark /opt/spark/bin/spark-submit /jobs/test_stream.py     # last line: STREAM TESTS OK
```

## Live stream and dashboard

```sh
# needs thresholds first: run the reference job once (see above)
docker compose --profile app up -d --build stream dashboard
open http://localhost:8501
```

`stream` runs `jobs/stream.py` continuously (restarts on failure, resumes from its checkpoint). The first start drains
the Kafka backlog (up to 24 hours), so the dashboard catches up over a minute or two. `dashboard` shows, for the
period you pick: a table of flagged windows (top N by edit count), three headline numbers (age of the newest edit that
reached Cassandra, edits in the last 5 minutes, flagged windows), and a line chart of edits per minute for up to 8
wikis, with a table view underneath. Hover or click the chart for a crosshair and tooltip. When nothing is flagged it
lists the busiest multi-editor windows instead. It refreshes every 30 s (`REFRESH=10s docker compose ... up -d`).

- The chart gives each wiki one fixed colour (by rank in the archive), so filtering never repaints a line.
- Light theme only. Colours come from the validated palette in `dashboard/chart.py`.
- The `stream` container uses about 1.6 GB. Stop it before running evaluation experiments so they have the machine:
  `docker compose --profile app stop stream`.
- Dashboard tests (needs Cassandra; fixtures go to the scratch tables, the real ones are only read):

```sh
docker compose --profile app run --rm --no-deps --entrypoint sh -v "$PWD/dashboard:/app" dashboard \
  -c "pip install -q pytest && python -m pytest -q /app/test_queries.py"     # 10 passed
```

## Replay tool

Republishes the archive into a **replay topic** (never the live one) in the live producer's format, for the
evaluation and for anyone without a live feed. Duplicates are removed on `id` and events go out in arrival order.

```sh
docker compose run --rm replay --help
docker compose run --rm replay --topic wiki-replay --recreate --rate 2000 --limit 100000      # fixed events per second
docker compose run --rm replay --topic wiki-replay --rate 0 --start 2026-10-04T11:00 --end 2026-10-04T12:00   # as fast as possible
docker compose run --rm replay --topic wiki-replay --speed 10 --start 2026-10-04T11:00 --end 2026-10-04T11:10 # 10x real time
```

- `--rate N` sends N events per second (0 = unthrottled) and keeps the original timestamps.
- `--speed S` follows the archive's own arrival times at S times real time and shifts `ts` and `arrival` by one
  constant so the replay looks live (watermarks then behave as they would on a live stream).
- `--key page|wiki` chooses the Kafka key: `page` (`wiki|title`) is what the live producer uses; `wiki` is the
  skew baseline for the evaluation. `--partitions N --recreate` rebuilds the replay topic with N partitions.
- Point the streaming job at it with `TOPIC=wiki-replay` (and a fresh `CHECKPOINT` path).
- The tool refuses the topic `wiki-edits`, including with `--recreate`, so replayed history cannot mix into the live
  data or delete it.

## Synthetic backfill

The archive holds 10 days of **synthetic** edits (2026-09-24 10:30 to 2026-10-04 10:30 UTC, 12.76 million events) followed
by the **real** events collected live from 2026-10-04 10:32 UTC onward. Synthetic files are named
`synthetic-<hour>.parquet`, every synthetic title and editor name starts with `SYN_`, and ids start at 10^12, so the
two are never confused. Same schema, same `date=/hour=` layout, so every job treats them alike.
`archive/_synthetic_manifest.json` records the seed, window and what the model was fitted on.

How it is made (`producer/synth.py`): a run-level bootstrap with fresh identities. Every real (page, minute) window is a
template with its edits, offsets, lags, bot flags and distinct-editor count; synthetic minutes draw templates at the
real hour-of-day rate per wiki (with a fitted AR(1) modulation and a small day-to-day factor) and give each a new page,
new editors, a time and an id. Page novelty and editor pools are power-law. `validate` generates 22 hours and compares
them with the real 22 hours on 16 statistics (rate, wiki mix, bot share, single-edit share, bursts, contested windows,
page novelty, lag percentiles, length changes); all are within tolerance.

```sh
docker compose --profile tools run --rm synth validate            # fit, generate 22 h into a scratch bucket, compare: VALIDATE OK
docker compose --profile tools run --rm synth generate --start 2026-09-24T10:30 --end 2026-10-04T10:30
```

`generate` refuses to overwrite synthetic files (`--overwrite` replaces them) and never writes a real file. Limits: the
model is fitted on, and validated against, the same real hours (a Sunday morning to a Monday morning), so it has no
weekday or weekend pattern, and regenerating after more real data has arrived gives a different (equally valid) draw.

## Live collection (optional)

Live collection continues in the background and adds real events after the synthetic ones. Nothing depends on it any
more, but it is worth leaving on.

1. **Keep Docker Desktop running, and the Mac awake while you run experiments.** A sleeping Mac pauses everything,
   including measured runs, whose timings then mean nothing (one throughput run took 34 minutes on 2026-10-05 because
   the lid was closed). Closing the lid sleeps the Mac even with `caffeinate`, unless it is on power with an external
   display. Collection itself survives sleep: the producer resumes from the last event id and catches up, and the archive
   showed no gap across several sleeps.
2. **Never run `docker compose down -v`** (or `docker volume prune`). It deletes the Kafka and MinIO volumes,
   which hold the archive. `docker compose down` without `-v` is safe.
3. **Check on it now and then:** `docker compose ps` does not detect a stalled producer (it showed `Up`
   while the stream looped on an error for several minutes on 2026-10-04). Look at the log instead:
   `docker compose logs --timestamps producer | grep flushed | tail -2` should show a line less than 6 minutes
   old, and `docker compose logs producer | grep -c "stream error"` should stay small. The
   producer reconnects with backoff and resumes from the last archived event id, so short outages leave no
   hole as long as they are within Wikimedia's replay window (documented as about 7 days; not verified here).
4. **Keep some disk free.** The archive is small (about 0.15 GB/day), but Docker images and build cache are
   large. Check `df -h` if the machine gets near full.
5. **Stop collecting** whenever you like: `docker compose stop producer`. Kafka and MinIO can stay up.

Restarting the producer is safe. It resumes from the last archived event id, so Kafka may receive a few
duplicate messages; the batch job de-duplicates on `id`.

## Data

Records are projected from the `recentchange` feed to edits and new pages only (`type in ('edit','new')`).
Fields: `id, wiki, title, namespace, user, bot, type, old_len, new_len, ts` (revision timestamp, the event
time) and `arrival` (`meta.dt`, when the event entered the stream). The proposal says ten fields; the
archive keeps eleven, because `id` is needed for de-duplication and `arrival` for lag and latency measurement.

`user` is null when MediaWiki hides the editor's name (the field is absent from the event); one such event
was seen in the first hour. Events that cannot be parsed are skipped and counted (`skipped=N` in the flush log).

Kafka keeps messages for 24 hours and holds only real events. The Parquet archive in MinIO is the durable copy and holds
both the synthetic backfill and the real events (see Synthetic backfill).

## Tests

```sh
docker compose run --rm --no-deps --entrypoint sh \
  -v "$PWD/producer/test_producer.py:/app/test_producer.py" -v "$PWD/producer/test_replay.py:/app/test_replay.py" replay \
  -c "pip install -q pytest && python -m pytest -q /app/test_producer.py /app/test_replay.py"   # 13 passed
```

## Notes

- MinIO no longer publishes Docker images or binaries, so `minio/Dockerfile` builds a pinned source commit
  (`7aac2a2c`). The upstream repository is archived, which does not matter for a local-only run.
- The producer is at-least-once. After a restart Kafka may see duplicates, and the archive can too if the
  container dies between a Parquet flush and the state-file write. Downstream jobs de-duplicate on `id`.
