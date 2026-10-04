# Project: Real-Time Detection of Editing Surges on Wikipedia

ID2221 Task2 (student project, P/F), Group 18. Detects per-page editing surges on Wikipedia, labels them
bot-driven or human-driven, and evaluates the streaming system itself (watermarks, state size, key skew,
throughput/latency, fault recovery). Proposal: `ID2221_project_proposal.pdf`. Course context lives in the
parent `../CLAUDE.md`; this file covers the project only. This folder is its own git repo.

## Deadlines and deliverables

- Submission **2026-10-25**: code plus a **2-page** report (what was done, dataset, methodology, results,
  instructions for running the code). The page limit is hard, so keep findings dense.
- The proposal's four sections map to the report: problem, tools, data, methodology/algorithm.

## Important
- Update this file whenever a key change occurs
- **Remaining work, decisions and the evaluation plan live in `docs/PLAN.md`.** Read it before starting a task,
  and tick off its progress table when a step finishes.

## Architecture

```
Wikimedia SSE ──> producer ──┬──> Kafka (wiki-edits, keyed wiki|title) ──> Spark Structured Streaming ──> Cassandra ──> Streamlit
 recentchange                └──> MinIO archive/date=…/hour=…/*.parquet ──> Spark SQL batch (thresholds), replay
```

- Kafka is the live buffer (24 h retention). The **Parquet archive in MinIO is the durable copy**.
- Everything runs in Docker Compose with pinned versions; nothing is installed on the host.
- Spark runs in **local mode** inside one idle container (`sleep infinity`); run jobs with
  `docker compose exec spark /opt/spark/bin/spark-submit /jobs/<job>.py`. Move to a standalone cluster only if the
  evaluation needs executors.

| Dir | Purpose |
|---|---|
| `producer/` | SSE client; projects events, writes Kafka and Parquet. Tests: `test_producer.py` (pytest, run in the producer image). |
| `jobs/` | Spark jobs. `common.py` is the session factory (s3a to MinIO, Cassandra connector, RocksDB state store, UTC). `smoke.py` checks Kafka, Parquet and a Cassandra round-trip. `reference.py` is the batch thresholds/baseline job; `test_reference.py` checks it against a pure-Python oracle (plain scripts run with spark-submit, no pytest in the Spark image). `stream.py` is the streaming job (`parse`, `aggregate`, `flag` are pure functions the test reuses); `test_stream.py` runs it over the Kafka backlog into scratch tables and compares with an archive-built oracle. |
| `cassandra/schema.cql` | Keyspace `wiki`; `windows` (partition `(wiki, day)`), `alerts` (partition `day`), `thresholds` (per namespace; `-1` = default), `baseline` (per wiki, namespace, hour), `smoke` (scratch). |
| `results/` | Machine-readable outputs for the report (mounted at `/results` in the Spark container). `reference_summary.json` here is from a partial archive and is git-ignored until the final run. |
| `minio/` | Dockerfile that builds MinIO from a pinned source commit (no official images any more). |
| `spark/` | Dockerfile; connector jars resolved at build time into `/opt/ivy` so runs need no network. |

## Status (as of 2026-10-04; README.md holds the live table)

Done and collecting: Kafka (KRaft), MinIO, topic, producer plus archive. Done: Spark and Cassandra in compose;
`jobs/smoke.py` prints `SMOKE OK` (verified 2026-10-04). Done on the partial archive: batch reference job
(`jobs/reference.py`, tested by `jobs/test_reference.py`); re-run it once collection ends. Done: streaming job
(`jobs/stream.py`, tested by `jobs/test_stream.py`); it is not running continuously yet, so the real `windows` and
`alerts` tables are still empty. Not started: replay tool, dashboard, evaluation, report.

Gotchas found while building: run Spark as `/opt/spark/bin/spark-submit` (it is not on PATH in the image);
`ARCHIVE` needs the trailing slash (`s3a://archive/`); the smoke test writes only to `wiki.smoke`, never to
`windows` or `alerts`; the stream test writes only to `windows_test` / `alerts_test` (TRUNCATE them before a run).
`thresholds` and `baseline` are written by `reference.py`. The `/checkpoints` volume must be owned by `spark`
(the Dockerfile does this; an old root-owned volume needs `docker compose exec -u root spark chown spark /checkpoints`).
`windows` gained `last_ts` and `written_at` after creation, via `ALTER TABLE` (schema.cql has the new definition).

## Hard rules for this folder

- **Never run `docker compose down -v`, `docker volume prune`, or anything that removes the `kafka-data`,
  `minio-data` or `producer-state` volumes.** The 10-day collection window (2026-10-04 to about 2026-10-15)
  cannot be re-run, so the archive is irreplaceable. Plain `docker compose down` is safe.
- Do not rebuild or restart `producer`, `kafka` or `minio` casually. Restarting the producer is safe (it resumes
  from the last archived event id) but is at-least-once and can duplicate messages.
- Changing the Parquet schema or partition layout mid-collection would split the archive. Ask first.
- Do not commit credentials. The `minioadmin` default is local-only; real values come from `MINIO_USER` /
  `MINIO_PASSWORD`.

## Data facts that jobs must respect

- Only `type in ('edit','new')` is kept. Fields: `id, wiki, title, namespace, user, bot, type, old_len, new_len,
  ts, arrival`. That is eleven, though the proposal says ten (`id` for dedup, `arrival` for lag).
- `ts` is the event time (revision timestamp); `arrival` is `meta.dt`. Watermarks use `ts`; latency uses `arrival`.
- **De-duplicate on `id`** in every downstream job (producer is at-least-once). The archive already holds
  3,018 duplicate rows from a resume bug fixed on 2026-10-04 (one copy each, 2 at most).
- `user` is **nullable**: MediaWiki omits it when the editor's name is hidden. Distinct-editor counts must
  tolerate nulls (`count(distinct user)` ignores them, which under-counts hidden editors).
- **Surge rule**: threshold = `QUANTILE` (0.999) over *contested* windows (at least `MIN_EDITORS` = 2 distinct
  editors; `common.MIN_EDITORS`, shared by `reference.py` and `stream.py`); a window is flagged when `edit_count` is
  strictly above it and it has at least `MIN_EDITORS` editors. Decided by the user on 2026-10-04 (see `docs/PLAN.md`).
- The archive starts at arrival 10:32:49 UTC on 2026-10-04. Kafka also holds about 2.5 minutes of earlier events
  (the first producer run's buffer, lost when I restarted it before its first flush), so Kafka-derived windows
  before 10:33 have no archive counterpart. Kafka retention is 24 h, so this cannot be backfilled after 10-05 10:30.
- Incident 2026-10-04: a hidden-username event raised `KeyError('user')`, the reconnect replayed the same event,
  and collection stalled for ~4 min (`docker compose ps` still said Up). Fixed by skipping malformed events.
  The archive has no gap over the stall (checked by arrival-time gaps, none over 15 s).
- Kafka key is `wiki|title`, so per-page state is naturally co-partitioned; key skew is an evaluation topic.
- Timezone is pinned to UTC in `common.py`. Spark timestamp handling has bitten the lab before (see the lab's
  `CLAUDE.md`); verify date bounds from the Parquet file itself, not only from a Spark read.

## Commands

- Start: `CONTACT="you@example.com" docker compose up -d --build` (first build compiles MinIO, a few minutes)
- Status / logs: `docker compose ps`; `docker compose logs -f producer` (a `flushed; total edits=N` line every 5 min)
- Reference job: `docker compose exec spark /opt/spark/bin/spark-submit /jobs/reference.py` (`REFERENCE OK`); its tests: `.../test_reference.py` (`REFERENCE TESTS OK`)
- Streaming job: `docker compose exec spark /opt/spark/bin/spark-submit /jobs/stream.py`; test: `.../test_stream.py` (`STREAM TESTS OK`, see README for the TRUNCATE step)
- Smoke test: `docker compose exec spark /opt/spark/bin/spark-submit /jobs/smoke.py` (expects `SMOKE OK`)
- Producer tests: see the command in `README.md` (runs pytest inside the producer image)
- Stop collecting at the end of the window: `docker compose stop producer`
- MinIO console http://localhost:9001; Kafka from host `localhost:29092`; Cassandra `localhost:9042`
- The Mac must stay awake: `nohup caffeinate -dimsu -t 600000 >/dev/null 2>&1 &` if it exited.

## Conventions

- Version pins are deliberate: the Spark 3.5.7, Kafka connector, hadoop-aws 3.3.4 + aws-sdk 1.12.262 and
  spark-cassandra-connector 3.5.1 constrain each other. Bump together or not at all.
- Code style is minimal: short comments that state the *why* (e.g. "day bucketing bounds partition growth"),
  shortcuts marked with `# ponytail:` comments naming the ceiling. Match that.
- Cassandra tables are query-first: partition keys are chosen for the dashboard's single-partition reads.
- Commits use conventional prefixes seen in history: `feat:`, `docs:`.

## Open items and assumptions

- Assumption: the Wikimedia replay window is about 7 days (documented upstream, not verified here).
- Not yet decided or built: surge threshold method (batch job on the archive), bot/human labelling rule,
  window and watermark settings, replay tool design, evaluation experiments. Check `README.md` before assuming.
