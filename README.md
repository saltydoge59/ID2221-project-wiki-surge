# Real-Time Detection of Editing Surges on Wikipedia

ID2221 Data-Intensive Computing, HT26, final project (Task 2), Group 18. Proposal: `ID2221_project_proposal.pdf`.
Report (2 pages): `docs/report.pdf`.

A streaming pipeline that finds per-page editing surges in the Wikimedia `recentchange` feed, labels them bot-dominated
or human-dominated, and measures the streaming system itself: watermarks, state size, key skew, throughput and latency,
and recovery after a failure.

```
Wikimedia SSE -> producer -+-> Kafka wiki-edits (key wiki|title) -> Spark Structured Streaming -> Cassandra -> Streamlit
                           +-> MinIO archive (Parquet, date=/hour=) -> Spark SQL reference job (thresholds), replay tool
```

## Contents

1. [Requirements](#requirements)
2. [Quick start](#quick-start)
3. [About the data](#about-the-data)
4. [Components](#components)
5. [Tests](#tests)
6. [Reproducing the evaluation and the report](#reproducing-the-evaluation-and-the-report)
7. [Operating notes and troubleshooting](#operating-notes-and-troubleshooting)
8. [Repository layout](#repository-layout)

## Requirements

- Docker with Compose v2. Give Docker about 6 GB of RAM and keep about 15 GB of disk free (the images are about 5 GB).
- Python 3 on the host, only for `eval/run_eval.py` (standard library). `eval/plot.py` also needs matplotlib, and
  `docs/build_report.sh` needs pandoc and Google Chrome. Everything else runs in containers.
- Internet access, for the first image builds and for the live Wikimedia feed. No account or credential is needed.

Versions are pinned in `docker-compose.yml` and the Dockerfiles. The Spark 3.5.7, Kafka connector, hadoop-aws and
Cassandra connector versions constrain each other, so bump them together or not at all.

## Quick start

Run these from the repository root, in order. Each step says what to expect.

**1. Build and start the core services** (first build 20 to 30 minutes: it compiles MinIO and downloads about 3 GB of
Spark jars):

```sh
docker compose up -d --build
docker compose ps        # kafka, minio, cassandra, spark, producer: Up. init and cql-init: Exited (0)
```

Optional: `CONTACT="you@example.com" docker compose up -d --build` puts that address in the User-Agent sent to
stream.wikimedia.org, as Wikimedia asks of API clients. Unset, it sends `no-contact-given`.

**2. Check that Spark can reach Kafka, MinIO and Cassandra:**

```sh
docker compose exec spark /opt/spark/bin/spark-submit /jobs/smoke.py    # last line: SMOKE OK
```

(A `NoClassDefFoundError: jnr/posix/POSIXHandler` earlier in that log is harmless.)

**3. Collect real data.** The producer is already running. The archive lives in a Docker volume and is not part of the
repository, so a fresh clone starts empty:

```sh
docker compose logs --timestamps producer | grep flushed | tail -2   # a "flushed; total edits=N" line every 5 minutes
```

Let it collect for a day or more if you can (the synthetic backfill below is fitted on it, and a day covers the
daily rhythm). I have not tried the backfill on less than 22 hours of real data.

**4. Build the synthetic backfill** (ten synthetic days placed before the real data, see [About the data](#about-the-data)):

```sh
docker compose --profile tools run --rm synth validate           # optional: fits, generates 22 h, compares; last line VALIDATE OK
docker compose --profile tools run --rm synth generate --days 10  # last line: SYNTH {"events": ..., "files": ...}
```

**5. Compute the surge thresholds** from the whole archive (also the data the dashboard's wiki list comes from):

```sh
docker compose exec spark /opt/spark/bin/spark-submit /jobs/reference.py    # last line: REFERENCE OK
```

**6. Start the live streaming query and the dashboard:**

```sh
docker compose --profile app up -d --build stream dashboard
open http://localhost:8501          # or visit it in a browser
```

The stream drains the Kafka backlog (up to 24 hours of real events) and then follows the feed, so the dashboard fills
within a minute or two. Flagged windows are rare by design; when none are flagged the dashboard lists the busiest
multi-editor windows instead.

**7. Run the evaluation** (about 1.5 hours; see [below](#reproducing-the-evaluation-and-the-report)):

```sh
python3 eval/run_eval.py all
```

To stop everything without losing data: `docker compose --profile app --profile tools down`. **Never add `-v`** (see
[Operating notes](#operating-notes-and-troubleshooting)).

## About the data

Each archived record has eleven fields: `id, wiki, title, namespace, user, bot, type, old_len, new_len, ts, arrival`.
`ts` is the revision timestamp (the event time) and `arrival` is `meta.dt`, when the edit entered the stream. Only
`type in ('edit','new')` is kept. The proposal lists ten fields; `id` (for de-duplication) and `arrival` (for lag and
latency) make eleven. `user` is null when MediaWiki hides the editor's name (18 edits in the first 1.15 million).

The archive is Parquet in MinIO, partitioned `date=YYYY-MM-DD/hour=HH/`, and has two kinds of file:

| | Real | Synthetic backfill |
|---|---|---|
| Files | `part-*.parquet` | `synthetic-<hour>.parquet` |
| Marks | none | titles and users start with `SYN_`, ids start at 10^12 |
| Content | the live feed from 2026-10-04 10:32 UTC, still growing | 12.76 million events, 2026-09-24 10:30 to 2026-10-04 10:30 UTC |

Same schema and layout, so every job treats them alike. `archive/_synthetic_manifest.json` records the seed, window and
what the model was fitted on. Kafka keeps 24 hours and holds only real events. The producer is at-least-once, so the
archive can hold a few duplicate events. `id` is MediaWiki's per-wiki counter, so two wikis can share a number (13 pairs
in the first 1.3 million edits); the unique key is (`wiki`, `id`), and every downstream job de-duplicates on that.

How the backfill is made (`producer/synth.py`): a run-level bootstrap with fresh identities. Every real (page, minute)
window becomes a template with its edits, offsets, lags, bot flags and number of distinct editors. Synthetic minutes draw
templates at the real hour-of-day rate per wiki (with a fitted AR(1) modulation and a small day-to-day factor) and give
each a new page, new editors, a time and an id. `validate` generates 22 hours and compares them with the real 22 on 16
statistics (rate, wiki mix, bot share, single-edit share, bursts, contested windows, page novelty, lag, length changes).

`generate` never writes or deletes a real file and refuses to overwrite synthetic ones unless you pass `--overwrite`.
`--days N` means N days ending at the start of the first real event's hour; `--start` and `--end` give explicit bounds.
Limits: the model is fitted on a single day of real data, so it has no weekday pattern, and regenerating after more real
data has arrived gives a different, equally valid draw.

## Components

### Producer and archive (`producer/producer.py`)

Reads the SSE stream, keeps edits and new pages, writes each to Kafka (topic `wiki-edits`, 6 partitions, key
`wiki|title`) and to Parquet in MinIO every 5 minutes. It reconnects with exponential backoff and resumes from the SSE
`Last-Event-ID`, so sleeps and disconnections leave no hole. Malformed events are skipped and counted (`skipped=N` in the
flush line). `docker compose ps` does not show a stalled producer, so check the log: the newest `flushed` line should be
less than 6 minutes old. MinIO console: http://localhost:9001 (`minioadmin` / `minioadmin`, local only; override with
`MINIO_USER` and `MINIO_PASSWORD`). Kafka from the host: `localhost:29092`. Cassandra: `localhost:9042`.

### Reference job (`jobs/reference.py`)

Computes the surge thresholds and hourly baselines from the archive (de-duplicated on `wiki` and `id`) into Cassandra
(`wiki.thresholds`, `wiki.baseline`) and `results/reference_summary.json`. The threshold is the 99.9th percentile
(`QUANTILE`) of edits per page-minute per namespace, taken over **contested** windows only: those with at least
`MIN_EDITORS` (2) distinct editors. The largest windows overall are one bot editing one page, and a threshold set by them
would sit above every real multi-editor surge. Namespaces with fewer than 1000 contested windows (`MIN_PAIRS`) use the
pooled value (namespace `-1`). Safe to re-run; rows are upserted.

### Streaming job (`jobs/stream.py`)

Reads Kafka and counts edits per (wiki, page) in one-minute event-time windows, using the revision timestamp as event
time. Every changed window is upserted into `wiki.windows`, and every flagged window into `wiki.alerts`. A window is
flagged when its count is strictly above the namespace threshold and it has at least `MIN_EDITORS` distinct editors; it is
bot-dominated when at least half its edits are bot edits. The `stream` compose service runs it continuously and resumes from
its checkpoint. To run it by hand: `docker compose exec spark /opt/spark/bin/spark-submit /jobs/stream.py`.

Settings are environment variables, for example `docker compose exec -e WATERMARK="2 minutes" -e TRIGGER="5 seconds" spark ...`:

| Variable | Default | Meaning |
|---|---|---|
| `WATERMARK` | 60 seconds | how late an edit may arrive and still be counted |
| `TRIGGER` | 10 seconds | micro-batch interval, or `availableNow` to drain the backlog and stop |
| `MIN_EDITORS` | 2 | distinct editors needed for a window to count as contested |
| `DEDUP` | 1 | de-duplicate on (`wiki`, `id`) (costs state) |
| `CHECKPOINT` | `/checkpoints/stream` | changing `SHUFFLE_PARTITIONS` or the query needs a new path |
| `SHUFFLE_PARTITIONS`, `CORES`, `MAX_OFFSETS_PER_TRIGGER`, `TOPIC` | 6, all, unset, `wiki-edits` | parallelism, batch cap, source topic |

### Dashboard (`dashboard/`, http://localhost:8501)

For the period you pick: a table of flagged windows (top N by edit count), three headline numbers (age of the newest
edit that reached Cassandra, edits in the last 5 minutes, flagged windows), and a line chart of edits per minute for up to
8 wikis with a table view. Hover or click the chart for a tooltip. Each wiki keeps one colour (by rank in the archive), so
filtering never repaints a line. It refreshes every 30 s (`REFRESH=10s docker compose --profile app up -d dashboard`).
Light theme only.

### Replay tool (`producer/replay.py`)

Republishes the archive into a **replay topic** in the live producer's format, for the evaluation and for running without
a live feed. Duplicates are removed on (`wiki`, `id`) and events go out in arrival order.

```sh
docker compose run --rm replay --help
docker compose run --rm replay --topic wiki-replay --recreate --rate 2000 --limit 100000        # fixed events/s
docker compose run --rm replay --topic wiki-replay --start 2026-09-25T11:00 --end 2026-09-25T12:00   # as fast as possible
docker compose run --rm replay --topic wiki-replay --speed 10 --start 2026-09-25T11:00 --end 2026-09-25T11:10  # 10x real time
```

`--speed S` follows the archive's own arrival times at S times real time and shifts `ts` and `arrival` by one constant so
the replay looks live. `--key page|wiki` chooses the Kafka key (`page` is what the producer uses). `--info` prints the
archive's hour range. Point the stream at a replay with `TOPIC=wiki-replay` and a fresh `CHECKPOINT`. The tool refuses the
topic `wiki-edits`, even with `--recreate`.

## Tests

Each command ends with the line shown. Run them with the stack up (`docker compose up -d`).

| What | Command | Expect |
|---|---|---|
| Producer and replay (13 tests) | `docker compose run --rm --no-deps --entrypoint sh -v "$PWD/producer:/app" replay -c "pip install -q pytest && python -m pytest -q /app/test_producer.py /app/test_replay.py"` | `13 passed` |
| Synthetic generator (6) | `docker compose --profile tools run --rm --no-deps --entrypoint sh -v "$PWD/producer:/app" synth -c "pip install -q pytest && python -m pytest -q /app/test_synth.py"` | `6 passed` |
| Reference job | `docker compose exec spark /opt/spark/bin/spark-submit /jobs/test_reference.py` | `REFERENCE TESTS OK` |
| Streaming job (compares against a pure-Python oracle; 2 to 4 minutes; needs real events in Kafka) | `docker compose exec cassandra cqlsh -e "TRUNCATE wiki.windows_test; TRUNCATE wiki.alerts_test"` then `docker compose exec spark /opt/spark/bin/spark-submit /jobs/test_stream.py` | `STREAM TESTS OK` |
| Dashboard (10, reads Cassandra) | `docker compose --profile app run --rm --no-deps --entrypoint sh -v "$PWD/dashboard:/app" dashboard -c "pip install -q pytest && python -m pytest -q /app/test_queries.py"` | `10 passed` |
| Evaluation harness (6, host) | `python3 eval/test_run_eval.py` | `OK` |

The Spark tests print a lot of log lines first; look at the last line. The stream and dashboard tests write only to
scratch tables (`windows_test`, `alerts_test`, `windows_eval_*`), never to the real `windows` and `alerts`.

## Reproducing the evaluation and the report

```sh
python3 eval/run_eval.py all          # about 1.5 hours. One of: throughput skew latency watermark fault. --quick for a few minutes.
python3 eval/plot.py                  # redraws docs/figures/eval.png from the CSVs
docs/build_report.sh                  # renders docs/report.md to docs/report.pdf and fails unless it is exactly 2 pages
```

`eval/run_eval.py` replays archive slices into `ev-*` topics, runs the real query through `jobs/eval_stream.py` with the
settings under test, and writes `results/eval/<experiment>.csv` (raw logs in `results/eval/raw/`, not tracked). It stops
the live `stream` service while it runs and restarts it if it was running. A run that overlaps a laptop sleep is detected
and repeated, but that costs time, so **keep the lid open**.

| Experiment | What it varies | Output |
|---|---|---|
| throughput | Kafka partitions (1, 2, 4, 6) x Spark cores (1, 2, 4, 8), Cassandra vs no-op sink | `throughput.csv` |
| skew | Kafka key `wiki|title` vs `wiki`, 3 repetitions | `skew_summary.csv`, `skew_partitions.csv` |
| latency | trigger interval 1, 5, 10, 30, 60 s, real-time replay | `latency.csv` |
| watermark | watermark 0 to 120 s, and de-duplication off | `watermark.csv` |
| fault | kill after 2, 5, 8 batches, restart from the checkpoint, diff the tables; two uninterrupted controls | `fault.csv` |

Findings and numbers are in `docs/report.pdf` (section 5) and `docs/PLAN.md`. Results depend on the machine: the numbers
committed here come from one 8-CPU laptop with Spark in local mode.

## Operating notes and troubleshooting

- **Never run `docker compose down -v` or `docker volume prune`.** They delete the Kafka, MinIO and checkpoint volumes,
  including the real events, which cannot be collected again. `docker compose down` without `-v` is safe.
- **Sleep.** A sleeping Mac pauses everything, and closing the lid sleeps it even under `caffeinate` unless it is on
  power with an external display. Collection survives: the producer resumes from its last event id and catches up, and
  the archive had no gap across several sleeps. Measured runs do not survive it, which is why the harness repeats them.
- **Memory.** Cassandra uses about 2 GB and the `stream` service 1.4 GB. Stop `stream` before experiments (the harness does).
- **`stream` keeps restarting.** It needs thresholds first: run the reference job (step 5).
- **`docker compose ps` says `Up` but nothing arrives.** Check `docker compose logs --timestamps producer | grep -E "flushed|stream error" | tail`.
  A stream error every 15 minutes is normal (Wikimedia closes long connections); a changing error many times a minute is not.
- **`SELECT count(*)` on `wiki.windows` times out.** The table is large; count a partition or use the driver.
- **Spark cannot write checkpoints.** The `/checkpoints` volume must be owned by `spark`:
  `docker compose exec -u root spark chown spark /checkpoints`.
- **The first build is slow.** MinIO is compiled from a pinned source commit (no official image or binary exists any more)
  and the Spark image downloads a 280 MB AWS SDK jar.
- **Wikimedia feed unreachable.** The producer retries with backoff and the rest of the stack still works on the archive.

## Repository layout

| Path | Purpose |
|---|---|
| `producer/` | `producer.py` (feed to Kafka and archive), `replay.py`, `synth.py` and their tests; Dockerfiles |
| `jobs/` | Spark jobs: `reference.py`, `stream.py`, `eval_stream.py`, `eval_compare.py`, `smoke.py`, `common.py`, tests |
| `dashboard/` | Streamlit app (`app.py`, `queries.py`, `chart.py`) and its tests |
| `eval/` | `run_eval.py` (orchestrator), `plot.py`, `test_run_eval.py` |
| `cassandra/schema.cql` | keyspace `wiki`: `windows`, `alerts`, `thresholds`, `baseline` and scratch tables |
| `minio/`, `spark/` | Dockerfiles (MinIO built from source; Spark with pinned connector jars) |
| `results/eval/` | evaluation CSVs |
| `docs/` | `report.md` and `report.pdf`, `PLAN.md` (progress and results), `figures/`, `build_report.sh` |
| `CLAUDE.md` | working notes for AI assistants (rules, data facts, commands) |
