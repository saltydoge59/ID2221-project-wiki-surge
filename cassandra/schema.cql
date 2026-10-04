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
  PRIMARY KEY ((wiki, day), window_start, page)
) WITH CLUSTERING ORDER BY (window_start DESC, page ASC);

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
