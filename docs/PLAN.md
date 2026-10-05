# Plan and progress

Handover document. `CLAUDE.md` holds the standing rules and data facts; `README.md` holds the live status table and
the user's instructions. This file holds the remaining work. Last updated 2026-10-04.

Goal: a fully working pipeline with clear documentation, submitted **2026-10-25** as code plus a **2-page report**
(what was done, dataset, methodology, results, run instructions). The proposal frames the project around the
evaluation (watermarks, state size, key skew, throughput/latency, fault recovery), not detector quality.

## Decisions already made (by the user)

- Archive window about 10 days: started 2026-10-04, stop about 2026-10-15 (`docker compose stop producer`).
  The proposal said two weeks; the report states the shortfall.
- Collector runs on the user's Mac (`caffeinate`, plugged in). Docker was pruned by the user.
- Report is Markdown rendered to a 2-page PDF, kept in the repo.
- MinIO is built from a pinned source commit because no images or binaries are published any more.
- Nothing is pushed to a remote without asking. Commits are local only so far.

## Progress

| # | Step | State |
|---|---|---|
| 1 | Compose stack: Kafka 3.9, MinIO, Cassandra 5.0.4, Spark 3.5.7 with pinned connector jars | done, `jobs/smoke.py` prints `SMOKE OK` |
| 2 | Producer and Parquet archive | done, collecting; archive has 3,018 known duplicate rows (dedupe on `id`) |
| 3 | Batch reference job (Spark SQL) | done and tested; quantile now over contested windows only; re-run after collection ends (about Oct 15) |
| 4 | Streaming job (`jobs/stream.py`) | done and tested against an oracle; surge rule decided (below), quantile to be re-checked on the full archive |
| 5 | Replay tool (`producer/replay.py`) | done, tested against an independent oracle |
| 6 | Dashboard (Streamlit) and live `stream` service | done; looked at in a browser (see below) |
| 7 | Evaluation harness (`eval/run_eval.py`, `jobs/eval_stream.py`, `jobs/eval_compare.py`) | done: all five experiments run on the 10-day archive, CSVs in `results/eval/` (results below) |
| 8 | Tests | producer 13, synth 5, reference, stream, dashboard 10: all pass |
| 9 | README and 2-page report | README partial; report not started |
| 10 | Fresh-eyes review in a separate chat (user's rule for high-stakes work) | not started |

## Remaining steps

3. **Batch reference job** (`jobs/reference.py`, DONE; details kept for the report): read the archive, de-duplicate on `id`, compute the per-page per-minute edit
   count distribution and, per `(wiki, namespace, hour of day UTC)`, the typical rate and bot share. Output a
   high empirical quantile per namespace as the surge threshold (not mean plus k sigma: the counts are
   zero-inflated and heavy-tailed). Decided: thresholds go to Cassandra (`wiki.thresholds`, `wiki.baseline`) and a summary to
   `results/reference_summary.json`. Choices: quantile 0.999 (`QUANTILE`), strict `edit_count > threshold`, pooled
   fallback for namespaces under 1000 pairs (`MIN_PAIRS`), default row `namespace = -1`. Tested against a pure-Python
   oracle on synthetic and real data; two deliberately broken copies fail the test. Re-run on the full archive after
   collection ends and commit the final `results/` (remove the ignore line in `.gitignore`).
4. **Streaming job** (`jobs/stream.py`, DONE; details kept for the report): Kafka source, parse JSON, event time = `ts`, watermark from the measured lag (proposal:
   median 1.7 s, up to 67 s; re-measure from `arrival - ts`), one-minute tumbling window per `(wiki, page)`:
   edit count, distinct editors (`user` is nullable), bot fraction, net bytes (`new_len - old_len`). Flag a window
   when its count exceeds the namespace threshold and distinct editors meet a minimum; label bot-dominated or
   human-dominated by bot fraction. `foreachBatch` upserts into `wiki.windows` and `wiki.alerts` (idempotent, keyed
   as in `cassandra/schema.cql`). Checkpoints go under `/checkpoints`. Verify by replaying a small slice and
   comparing Cassandra against a batch recomputation of the same windows.
5. **Replay tool** (`producer/replay.py`, DONE; details kept for the report): read the archive from MinIO and publish to Kafka at a controlled rate, for the evaluation and
   for graders who cannot use the live feed. Keep the same key (`wiki|title`) and value format as the producer.
6. **Dashboard**: Streamlit reading Cassandra, top-N surging pages plus a per-wiki edit-rate timeline.
   Verify by running it and looking at a screenshot.
7. **Evaluation harness**, scripted, results to CSV:
   - (a) throughput against Kafka partitions and Spark cores
   - (b) key `(wiki,title)` against `wiki`: per-partition message counts and task-time skew
   - (c) end-to-end latency, revision timestamp to visibility in Cassandra, across trigger intervals
   - (d) watermark length against state size (RocksDB metrics from `StreamingQueryProgress`), completeness, latency
   - (e) fault injection: kill the driver mid-stream, restart from the checkpoint, diff the final Cassandra tables
     against an uninterrupted run (expected to match because the writes are idempotent upserts)

   Single host with 8 CPUs and 8 GB for Docker, so say plainly that the grid is single-host. Run configurations
   one at a time. Cassandra heap is capped at 1 GB.
8. **Tests**: window aggregation against a batch oracle, idempotent upsert, recovery diff, thresholds job.
9. **Docs**: README (architecture diagram, quickstart, replay mode, how to reproduce each experiment, pinned
   versions, troubleshooting) and the 2-page report. Re-measure the proposal's numbers (71% of volume in three
   wikis, 93% of pages with one edit per interval, 249 bytes per record, 1.4 M edits per day) and correct them
   in the report where they differ.
10. **Review** the report and README in a fresh chat, then do a clean-clone run-through.

## Decision (user, 2026-10-04): the surge rule

Problem found while testing step 4: within the first 4 hours the biggest windows (30 to 52 edits, wikidatawiki
items) had a single editor, so a 99.9th-percentile threshold over all windows (about 11) was set by one bot and
`MIN_EDITORS = 3` flagged nothing. **Chosen: compute the quantile only over windows with at least 2 distinct
editors, and flag only such windows** (`MIN_EDITORS = 2`, shared in `jobs/common.py`).

Consequence to report honestly: only 489 of 194,068 windows (0.25%) had 2 or more editors in those 4 hours, the
largest with 10 edits. A 99.9th percentile over 489 points is the maximum, so every namespace falls back to the pooled
threshold (10) and no window is flagged on that slice. On the full archive (about 60 times more data) the quantile has
a real tail. By construction a 0.999 quantile flags about 0.1% of contested windows, so expect only a few alerts per
day; `QUANTILE=0.99` would give about ten times more. **Re-check on the full archive and pick the quantile then**
(it is an environment variable; `MIN_PAIRS` decides which namespaces get their own value).

## Replay tool: what was verified, and a preliminary skew number

Verified on 20,000 events of one archive hour: the replay topic holds exactly the de-duplicated archive events
(ids equal an independent oracle), no duplicates, every key on exactly one partition, values equal to the archive.
`--rate 2000` achieved 1,986 events per second; unthrottled it sent 56,000 per second (broker-side limit not yet
found). `--speed 10` replayed 80.8 s of archive in 8.1 s with every event's lag unchanged. A bug found on the way:
pacing was computed after the timestamp shift and would have waited hours; a regression test now covers it.

Preliminary key-skew result for evaluation (b), 20,000 events, 6 partitions (redo properly in step 7):
key `wiki|title` gives partition sizes 3,468 / 3,418 / 3,387 / 3,346 / 3,244 / 3,137 (largest over smallest 1.11);
key `wiki` gives 6,863 / 6,311 / 3,332 / 1,719 / 938 / 837 (8.2). This supports the proposal's choice of key.

## Synthetic backfill (user's decision, 2026-10-05)

Instead of waiting ten days for live collection, the archive was backfilled with 10 synthetic days
(2026-09-24 10:30 to 2026-10-04 10:30 UTC, 12.76 M events) ahead of the real data (from 2026-10-04 10:32). Made by
`producer/synth.py`, a run-level bootstrap with fresh identities fitted on the real 22 hours; `validate` compares 22
synthetic hours with the real 22 on 16 statistics, all within tolerance (rate +8%, top-3 wiki share +1%, bot share 0%,
single-edit windows -0.1%, contested windows -7%, distinct pages per edit +1%, lag p99 -1%). Two bugs were found by
validation and fixed: an editor drawn per edit instead of per burst (contested windows 24x too many), and a new-page
fraction that ignored revisits (distinct pages 22% too high). The reference job on the 13.9 M-row archive gives a
99.9th-percentile threshold of 19 edits per page-minute (25,505 contested windows), where the 4-hour slice could only
say 10. Real data keeps accumulating behind it. Sleeps never lost real data: the producer resumes and catches up.

Evaluation notes: on 2026-10-05 a closed lid put the Mac to sleep mid-experiment and spoiled four of eight quick
throughput rows (one run took 34 minutes). `eval/run_eval.py` now measures sleep (wall clock minus uptime clock) and
repeats any run that overlaps one. Rerun everything after the lid is kept open.

## Evaluation results (2026-10-05)

All on a 6-hour slice of the synthetic backfill (338,430 events, first complete hour of the archive), one Mac with
8 CPUs and 8 GB for Docker, local-mode Spark, one repetition unless stated. Raw per-run output is in
`results/eval/raw/` (git-ignored); the CSVs in `results/eval/` are tracked. Reproduce with
`python3 eval/run_eval.py all` (about 1.5 hours, keep the lid open).

(a) Throughput, events/s, Cassandra sink (`throughput.csv`):

| Kafka partitions | 1 core | 2 cores | 4 cores | 8 cores |
|---|---|---|---|---|
| 1 | 12357 | 12236 | 12861 | 12130 |
| 2 | 11501 | 17329 | 18135 | 17739 |
| 4 | 10498 | 15929 | 19622 | 19683 |
| 6 | 8788 | 13526 | 17045 | 17684 |

Throughput stops growing once cores reach the partition count: one partition is one source task, so 1 partition is flat
at about 12,000 events/s for any core count. The ceiling is about 18,000 to 20,000 events/s with the Cassandra sink. With
the no-op sink at 6 partitions it reaches 15,561 / 25,013 / 29,188 / 31,894 events/s on 1/2/4/8 cores, so Cassandra
writes cost roughly 45% of the achievable throughput at 8 cores. First batches pay start-up cost, so `events_per_s_steady`
(without the first batch) is 15 to 40% higher.

(b) Key skew, 6 partitions, 3 repetitions (`skew_summary.csv`): key `wiki|title` gives partitions within 2% of each
other (largest over smallest 1.02) and task times within 2% of the median; key `wiki` alone gives a 10x spread in message
counts (98,864 / 124,698 / 18,063 / 24,017 / 60,316 / 12,472 in one run) and the slowest task 1.19 to 1.26 times the
median, which costs about 10 to 20% throughput. Task index is not the partition number, so the CSV lists counts and times
sorted independently.

(c) Latency, revision timestamp to Cassandra, real-time replay of 4 minutes per setting (`latency.csv`), median / p99:
1 s trigger 1.95 / 13.5 s; 5 s 4.78 / 15.8; 10 s 6.48 / 19.3; 30 s 16.4 / 33.5; 60 s 31.6 / 61.6. The median is about
half the trigger interval plus the 1.3 s median feed lag; the tail is the feed's own lag tail. `written_at` is stamped
just before the batch's write, so add the median write time (0.9 to 2.0 s).

(d) Watermark, replay 6x faster with the trigger shortened to match (`watermark.csv`):

| watermark | completeness | aggregation state rows | de-duplication state rows |
|---|---|---|---|
| 0 seconds | 99.37% | 1137 | 204 |
| 5 seconds | 99.63% | 1200 | 382 |
| 10 seconds | 99.75% | 1163 | 559 |
| 30 seconds | 99.91% | 1526 | 1257 |
| 60 seconds | 99.98% | 1938 | 2223 |
| 120 seconds | 100.00% | 2952 | 4224 |
| 60 seconds (no de-dup) | 100.00% | 2055 | - |

Completeness is Cassandra's summed edit counts over the distinct edits replayed. Spark's own "rows dropped by watermark"
counter stays at 0 here even though edits are lost, so it cannot be used. De-duplication state (21x growth) is what
scales with the watermark, more than the aggregation state (2.6x).

(e) Fault injection (`fault.csv`): the query was killed with SIGKILL after 2, 5 and 8 committed batches, each time with
the next batch already planned (in flight), then restarted from the same checkpoint. The recovered tables hold the same
310,572 windows as the uninterrupted run, none lost or invented, and the same alerts. 4 to 8 windows differ in value:
when two edits fall in one page-minute near a batch boundary, which one is the last depends on timing, and two
uninterrupted reruns differ from the reference by 4 and 5 windows, so recovery is as exact as the pipeline's own
run-to-run noise. Without the resume, 100,000 to 260,000 windows would be missing, so the check can fail.

Mistakes found and fixed on the way (all in the harness): a `finally: return` that hid a failed replay and recorded an
empty run; kill trials that reused a stale checkpoint and fired before the run began; a skew CSV that paired Spark task
indices with Kafka partitions; and four timings spoiled by a Mac sleeping, now detected and repeated automatically.

## Dashboard: what was verified

Run in a browser against live data: the stream caught up from a stale start (525 s, 413 s, 125 s, then 10 s behind,
matching the 10 s trigger); two real alerts appeared (wikidatawiki items with 14 and 19 edits, 2 editors, 86 to 95%
bots, bot-dominated); the chart's crosshair and tooltip work. Screenshots found one bug, overlapping end labels (the gap
was measured against the largest end value, not the y-range); it is fixed and a test fails on the old rule. The palette
was run through the dataviz validator: all gates pass on white; aqua, yellow and magenta are under 3:1 contrast, which
the legend, sparse direct labels and the table view cover. Deliberate simplifications: light theme only; at most 8 wikis,
each with a permanent colour; no log axis (wikidatawiki and commonswiki dwarf the small wikis).

For step 7: the `stream` service writes to the real tables and uses about 1.6 GB, so stop it before sweeping
configurations, and use fresh checkpoint paths and the replay topic (`TOPIC=wiki-replay`).

## Numbers observed so far (72-minute slice, 2026-10-04; re-measure on the full archive for the report)

| Quantity | Proposal | Observed |
|---|---|---|
| Single-edit share of (page, minute) pairs | 93% | 94.8% |
| Revision-to-arrival lag | median 1.7 s, max 67 s | median 1.3 s, p99 11.8 s, max 118.6 s |
| Top-3 wiki share of edits | 71% | 60.7% (wikidatawiki, commonswiki, enwiki); time-of-day dependent |
| Edit rate | about 16.5 per second | 14.4 per second |
| Bot share of edits | not stated | 32% |

The lag maximum exceeds the proposal's 67 s, so the watermark (step 4) needs an explicit trade-off, not just the
median. `user` is null for 2 rows (hidden editors).

## Timeline

| Dates | Work |
|---|---|
| Oct 4-5 | Steps 1-2 (done) |
| Oct 6-10 | Steps 3-6 on the partial archive |
| Oct 11-17 | Step 7 on the growing archive; collection stops about Oct 15 |
| Oct 18-21 | Final runs on the full archive, step 8, tables and figures |
| Oct 22-24 | Step 9 report and README, step 10 review, clean-clone test |
| Oct 25 | Submit |

## Risks

- A sleeping Mac or stopped Docker pauses collection. `docker compose ps` does not show a stalled producer;
  check the `flushed` log line (see `README.md`).
- Never remove the `kafka-data`, `minio-data` or `producer-state` volumes. The collection cannot be re-run.
- Version coupling between Spark, the Kafka connector, hadoop-aws and the Cassandra connector. Bump together.
- Memory: Kafka, Cassandra and Spark share 8 GB.
- Spark timestamp handling bit the lab before. Check date bounds against the Parquet file, not only a Spark read.

## End-to-end check before submitting

Fresh clone, `docker compose up -d --build`, `SMOKE OK`, replay a sample, the dashboard shows surges, the
evaluation scripts regenerate the result CSVs, the tests pass, and the fault-injection diff is empty.
