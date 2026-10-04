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

## Architecture

```
Wikimedia SSE ──> producer ──┬──> Kafka (wiki-edits, keyed wiki|title) ──> Spark Structured Streaming ──> Cassandra ──> Streamlit
 recentchange                └──> MinIO archive/date=…/hour=…/*.parquet ──> Spark SQL batch (thresholds), replay
```

- Kafka is the live buffer (24 h retention). The **Parquet archive in MinIO is the durable copy**.
- Everything runs in Docker Compose with pinned versions; nothing is installed on the host.
- Spark runs in **local mode** inside one idle container (`sleep infinity`); run jobs with
  `docker compose exec spark spark-submit /jobs/<job>.py`. Move to a standalone cluster only if the
  evaluation needs executors.

| Dir | Purpose |
|---|---|
| `producer/` | SSE client; projects events, writes Kafka and Parquet. `test_producer.py` is the only test file. |
| `jobs/` | Spark jobs. `common.py` is the session factory (s3a to MinIO, Cassandra connector, RocksDB state store, UTC). `smoke.py` checks Kafka, Parquet and a Cassandra round-trip. |
| `cassandra/schema.cql` | Keyspace `wiki`; tables `windows` (partition `(wiki, day)`) and `alerts` (partition `day`). |
| `minio/` | Dockerfile that builds MinIO from a pinned source commit (no official images any more). |
| `spark/` | Dockerfile; connector jars resolved at build time into `/opt/ivy` so runs need no network. |

## Status (as of 2026-10-04; README.md holds the live table)

Done and collecting: Kafka (KRaft), MinIO, topic, producer plus archive. In progress: Spark and Cassandra in
compose, batch thresholds job. Not started: streaming job, replay tool, dashboard, evaluation, report.
`cassandra/`, `jobs/`, `spark/` and the compose changes are **uncommitted** as of this writing.

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
- Incident 2026-10-04: a hidden-username event raised `KeyError('user')`, the reconnect replayed the same event,
  and collection stalled for ~4 min (`docker compose ps` still said Up). Fixed by skipping malformed events.
  The archive has no gap over the stall (checked by arrival-time gaps, none over 15 s).
- Kafka key is `wiki|title`, so per-page state is naturally co-partitioned; key skew is an evaluation topic.
- Timezone is pinned to UTC in `common.py`. Spark timestamp handling has bitten the lab before (see the lab's
  `CLAUDE.md`); verify date bounds from the Parquet file itself, not only from a Spark read.

## Commands

- Start: `CONTACT="you@example.com" docker compose up -d --build` (first build compiles MinIO, a few minutes)
- Status / logs: `docker compose ps`; `docker compose logs -f producer` (a `flushed; total edits=N` line every 5 min)
- Smoke test: `docker compose exec spark spark-submit /jobs/smoke.py` (expects `SMOKE OK`)
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
