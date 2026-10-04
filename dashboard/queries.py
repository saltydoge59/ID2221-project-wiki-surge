"""Read-side Cassandra queries for the dashboard.

Each query reads a time slice of one partition, (wiki, day) for windows and (day) for alerts: the access pattern the
schema was designed for. Table names are parameters so tests can use scratch tables; `now` is a parameter so they can
use a fixed clock. Cassandra returns naive UTC datetimes, so everything here is naive UTC.
"""
import os
from datetime import datetime, timedelta, timezone

import pandas as pd
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import dict_factory

HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
WINDOW_COLS = ["wiki", "window_start", "page", "edit_count", "distinct_editors", "bot_fraction", "net_bytes", "last_ts"]
ALERT_COLS = ["window_start", "wiki", "page", "edit_count", "distinct_editors", "bot_fraction", "label"]


def connect(host=HOST):
    s = Cluster([host]).connect("wiki")
    s.row_factory = dict_factory
    return s


def utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def days(since, until):
    return [since.date() + timedelta(days=i) for i in range((until.date() - since.date()).days + 1)]


def _fetch(s, cql, args):
    out = []
    for ok, res in execute_concurrent_with_args(s, s.prepare(cql), args, raise_on_first_error=True):
        out.extend(res)
    return out


def top_wikis(s, n=8):
    """The n busiest wikis in the archive baseline, busiest first, ties by name. A stable order, so a wiki keeps its
    chart color however the filters change."""
    rows = list(s.execute("SELECT wiki, edits FROM baseline"))
    if not rows:
        return []
    totals = pd.DataFrame(rows).groupby("wiki").edits.sum().reset_index().sort_values(["edits", "wiki"], ascending=[False, True])
    return list(totals.wiki.head(n))


def window_rows(s, wikis, since, now=None, table="windows"):
    now = now or utcnow()
    args = [(w, d, since) for w in wikis for d in days(since, now)]
    cql = "SELECT %s FROM %s WHERE wiki=? AND day=? AND window_start>=?" % (", ".join(WINDOW_COLS), table)
    return pd.DataFrame(_fetch(s, cql, args), columns=WINDOW_COLS)


def edit_rate(w, wikis, since, now):
    """Edits per minute, one column per wiki, one row per minute from `since` to the last complete minute.

    Minutes with no edit are zero, not missing, so a line drops to the axis instead of skipping across a gap. The
    minute still in progress is left out because its count is partial.
    """
    last = pd.Timestamp(now).floor("min") - pd.Timedelta(minutes=1)
    index = pd.date_range(pd.Timestamp(since).ceil("min"), last, freq="min", name="minute")
    if w.empty:
        return pd.DataFrame(0, index=index, columns=list(wikis))
    wide = w.pivot_table(index="window_start", columns="wiki", values="edit_count", aggfunc="sum")
    return wide.reindex(index=index, columns=list(wikis)).fillna(0).astype(int)


def freshness(w, now):
    """Seconds since the newest revision in the slice, or None when there is nothing."""
    return None if w.empty else max(0.0, (now - w.last_ts.max()).total_seconds())


def recent_alerts(s, since, now=None, limit=10, table="alerts"):
    """Flagged windows since `since`, biggest first (newest first among equals)."""
    now = now or utcnow()
    cql = "SELECT %s FROM %s WHERE day=? AND window_start>=?" % (", ".join(ALERT_COLS), table)
    df = pd.DataFrame(_fetch(s, cql, [(d, since) for d in days(since, now)]), columns=ALERT_COLS)
    return df.sort_values(["edit_count", "window_start"], ascending=False).head(limit).reset_index(drop=True)


def contested(w, min_editors, limit=10):
    """Biggest windows with at least `min_editors` editors, flagged or not: what to show while nothing is flagged."""
    c = w[w.distinct_editors >= min_editors]
    return c.sort_values(["edit_count", "window_start"], ascending=False).head(limit).reset_index(drop=True)


def threshold(s):
    """(threshold, quantile) of the default row of wiki.thresholds, or None before the reference job has run."""
    rows = list(s.execute("SELECT threshold, quantile FROM thresholds WHERE namespace=-1"))
    return (rows[0]["threshold"], rows[0]["quantile"]) if rows else None
