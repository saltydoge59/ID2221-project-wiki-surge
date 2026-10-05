-- Day bucketing bounds partition growth; the dashboard reads a time slice with one partition read.
CREATE KEYSPACE IF NOT EXISTS wiki
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS wiki.windows (
  wiki text,
  day date,
  window_start timestamp,
  page text,
  edit_count int,
  distinct_editors int,
  bot_fraction double,
  net_bytes bigint,
  last_ts timestamp,     -- newest revision timestamp in the window
  written_at timestamp,  -- when the batch holding this version was written; written_at - last_ts is the latency
  PRIMARY KEY ((wiki, day), window_start, page)
) WITH CLUSTERING ORDER BY (window_start DESC, page ASC);

-- Written by jobs/reference.py from the archive; read by the streaming job as a static table.
-- namespace = -1 is the default row for namespaces too sparse for their own quantile (-1 is never edited).
CREATE TABLE IF NOT EXISTS wiki.thresholds (
  namespace int PRIMARY KEY,
  threshold double,      -- flag a window when edit_count > threshold
  quantile double,
  pairs bigint,          -- contested (page, minute) pairs behind the quantile; 0 means the namespace has none
  source text,           -- 'namespace' or 'global'
  computed_at timestamp
);

CREATE TABLE IF NOT EXISTS wiki.baseline (
  wiki text,
  namespace int,
  hour int,              -- hour of day, UTC
  edits bigint,
  rate_per_min double,
  bot_share double,
  PRIMARY KEY ((wiki, namespace), hour)
);

-- Scratch table for jobs/smoke.py, so the smoke test never writes into the real tables.
CREATE TABLE IF NOT EXISTS wiki.smoke (k text PRIMARY KEY, v int);

-- Flagged windows. Keyed by day only so the dashboard's top-N across wikis is one partition read.
CREATE TABLE IF NOT EXISTS wiki.alerts (
  day date,
  window_start timestamp,
  wiki text,
  page text,
  edit_count int,
  distinct_editors int,
  bot_fraction double,
  label text,
  PRIMARY KEY ((day), window_start, wiki, page)
) WITH CLUSTERING ORDER BY (window_start DESC, wiki ASC, page ASC);

-- Scratch copies for jobs/test_stream.py (CQL has no CREATE TABLE ... LIKE in 5.0), so tests never touch the real
-- tables. Keep these in step with windows and alerts above.
CREATE TABLE IF NOT EXISTS wiki.windows_test (
  wiki text, day date, window_start timestamp, page text, edit_count int, distinct_editors int,
  bot_fraction double, net_bytes bigint, last_ts timestamp, written_at timestamp,
  PRIMARY KEY ((wiki, day), window_start, page)
) WITH CLUSTERING ORDER BY (window_start DESC, page ASC);

CREATE TABLE IF NOT EXISTS wiki.alerts_test (
  day date, window_start timestamp, wiki text, page text, edit_count int, distinct_editors int,
  bot_fraction double, label text,
  PRIMARY KEY ((day), window_start, wiki, page)
) WITH CLUSTERING ORDER BY (window_start DESC, wiki ASC, page ASC);

-- Evaluation scratch tables (jobs/eval_stream.py, eval/run_eval.py); same layout as windows and alerts.
CREATE TABLE IF NOT EXISTS wiki.windows_eval_a (
  wiki text, day date, window_start timestamp, page text, edit_count int, distinct_editors int,
  bot_fraction double, net_bytes bigint, last_ts timestamp, written_at timestamp,
  PRIMARY KEY ((wiki, day), window_start, page)
) WITH CLUSTERING ORDER BY (window_start DESC, page ASC);

CREATE TABLE IF NOT EXISTS wiki.alerts_eval_a (
  day date, window_start timestamp, wiki text, page text, edit_count int, distinct_editors int,
  bot_fraction double, label text,
  PRIMARY KEY ((day), window_start, wiki, page)
) WITH CLUSTERING ORDER BY (window_start DESC, wiki ASC, page ASC);

-- Evaluation scratch tables (jobs/eval_stream.py, eval/run_eval.py); same layout as windows and alerts.
CREATE TABLE IF NOT EXISTS wiki.windows_eval_b (
  wiki text, day date, window_start timestamp, page text, edit_count int, distinct_editors int,
  bot_fraction double, net_bytes bigint, last_ts timestamp, written_at timestamp,
  PRIMARY KEY ((wiki, day), window_start, page)
) WITH CLUSTERING ORDER BY (window_start DESC, page ASC);

CREATE TABLE IF NOT EXISTS wiki.alerts_eval_b (
  day date, window_start timestamp, wiki text, page text, edit_count int, distinct_editors int,
  bot_fraction double, label text,
  PRIMARY KEY ((day), window_start, wiki, page)
) WITH CLUSTERING ORDER BY (window_start DESC, wiki ASC, page ASC);
