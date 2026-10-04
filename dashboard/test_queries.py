"""Tests for the dashboard's queries and a headless run of the app. Needs the compose stack (Cassandra) running.

Run: docker compose --profile app run --rm --no-deps --entrypoint sh -v "$PWD/dashboard:/app" dashboard \
       -c "pip install -q pytest && python -m pytest -q /app/test_queries.py"
Fixtures go into the scratch tables windows_test / alerts_test on a fixed clock in the year 2000, in partitions of
their own, and are deleted afterwards; the real tables are only read.
"""
from datetime import date, datetime, timedelta

import pandas as pd
import pytest

import chart
import queries as q

W, A = "windows_test", "alerts_test"
NOW = datetime(2000, 1, 2, 12, 0, 30)
T = lambda hh, mm, day=2: datetime(2000, 1, day, hh, mm)


@pytest.fixture(scope="module")
def s():
    return q.connect()


@pytest.fixture
def fixture_rows(s):
    ins_w = s.prepare("INSERT INTO %s (wiki, day, window_start, page, edit_count, distinct_editors, bot_fraction, net_bytes, last_ts) "
                      "VALUES (?,?,?,?,?,?,?,?,?)" % W)
    ins_a = s.prepare("INSERT INTO %s (day, window_start, wiki, page, edit_count, distinct_editors, bot_fraction, label) "
                      "VALUES (?,?,?,?,?,?,?,?)" % A)
    win = [  # wiki, window start, page, edits, editors
        ("tw1", T(11, 58), "A", 3, 2), ("tw1", T(11, 58), "B", 2, 1),   # same minute, two pages: 5 edits
        ("tw1", T(11, 55), "A", 2, 2), ("tw1", T(11, 59), "A", 4, 3),
        ("tw1", T(12, 0), "A", 99, 9),                                     # the minute in progress: left out
        ("tw1", T(23, 55, 1), "A", 7, 2), ("tw1", T(0, 5, 2), "A", 8, 2)]  # either side of midnight
    for w, st_, page, n, ed in win:
        s.execute(ins_w, (w, st_.date(), st_, page, n, ed, 0.5, 10, st_ + timedelta(seconds=30)))
    for st_, wiki, page, n, label in [(T(11, 58), "tw1", "X", 12, "human-dominated"), (T(11, 59), "tw1", "Y", 12, "bot-dominated"),
                                      (T(11, 10), "tw2", "Z", 30, "human-dominated"), (T(10, 0), "tw1", "OLD", 99, "human-dominated")]:
        s.execute(ins_a, (st_.date(), st_, wiki, page, n, 2, 0.4, label))
    yield
    for w in ("tw1", "tw2"):
        for d in (date(2000, 1, 1), date(2000, 1, 2)):
            s.execute("DELETE FROM %s WHERE wiki=%%s AND day=%%s" % W, (w, d))
    for d in (date(2000, 1, 1), date(2000, 1, 2)):
        s.execute("DELETE FROM %s WHERE day=%%s" % A, (d,))


def test_edit_rate_sums_pages_fills_gaps_and_drops_the_open_minute(s, fixture_rows):
    since = T(11, 50)
    w = q.window_rows(s, ["tw1", "tw2"], since, NOW, W)
    wide = q.edit_rate(w, ["tw1", "tw2"], since, NOW)
    assert list(wide.columns) == ["tw1", "tw2"]
    assert wide.index[0] == pd.Timestamp(T(11, 50)) and wide.index[-1] == pd.Timestamp(T(11, 59))
    assert wide.tw1[pd.Timestamp(T(11, 58))] == 5 and wide.tw1[pd.Timestamp(T(11, 59))] == 4
    assert wide.tw1[pd.Timestamp(T(11, 55))] == 2 and wide.tw1[pd.Timestamp(T(11, 56))] == 0   # a gap is a zero
    assert (wide.tw2 == 0).all()                                                                # no rows: zeros, not missing
    assert pd.Timestamp(T(12, 0)) not in wide.index                                             # 99 edits in the open minute


def test_a_period_crossing_midnight_reads_both_day_partitions(s, fixture_rows):
    now, since = datetime(2000, 1, 2, 0, 10, 30), T(23, 50, 1)
    wide = q.edit_rate(q.window_rows(s, ["tw1"], since, now, W), ["tw1"], since, now)
    assert wide.tw1[pd.Timestamp(T(23, 55, 1))] == 7 and wide.tw1[pd.Timestamp(T(0, 5, 2))] == 8


def test_recent_alerts_rank_filter_and_limit(s, fixture_rows):
    got = q.recent_alerts(s, T(11, 0), NOW, 10, A)
    assert list(got.page) == ["Z", "Y", "X"]            # 30 first; the two 12s newest first; OLD is before the period
    assert list(q.recent_alerts(s, T(11, 0), NOW, 2, A).page) == ["Z", "Y"]
    assert q.recent_alerts(s, T(11, 59), NOW, 10, A).page.tolist() == ["Y"]


def test_contested_and_freshness(s, fixture_rows):
    w = q.window_rows(s, ["tw1"], T(11, 50), NOW, W)
    c = q.contested(w, 3, 5)
    assert list(zip(c.edit_count, c.distinct_editors)) == [(99, 9), (4, 3)]
    assert q.freshness(w, NOW) == 0.0                     # newest last_ts is 12:00:30 (the open minute) = NOW
    assert q.freshness(w, NOW + timedelta(seconds=45)) == 45.0
    assert q.freshness(w, NOW - timedelta(seconds=45)) == 0.0   # a clock behind the data never goes negative
    assert q.freshness(w.iloc[0:0], NOW) is None


def test_empty_results_have_the_right_shape(s):
    assert q.recent_alerts(s, T(1, 0, 1), datetime(2000, 1, 1, 2, 0), 5, A).empty
    wide = q.edit_rate(q.window_rows(s, ["nobody"], T(1, 0, 1), datetime(2000, 1, 1, 1, 10), W), ["nobody"], T(1, 0, 1), datetime(2000, 1, 1, 1, 10))
    assert list(wide.columns) == ["nobody"] and (wide == 0).all().all()


def test_top_wikis_orders_by_archive_volume(s):
    rows = pd.DataFrame(list(s.execute("SELECT wiki, edits FROM baseline")))
    assert len(rows), "run jobs/reference.py first"
    tot = rows.groupby("wiki").edits.sum().to_dict()
    want = sorted(tot, key=lambda w: (-tot[w], w))[:8]
    assert q.top_wikis(s, 8) == want and len(want) == 8


def test_threshold_row_exists(s):
    th = q.threshold(s)
    assert th and th[0] > 0 and 0 < th[1] < 1


def test_end_labels_skip_a_label_that_would_overlap():
    # on a 0-500 axis, 130 and 118 are about 5 px apart: labelling both stacks the text. The old rule measured the gap
    # against the largest END value (130), kept both, and overlapped them (seen in the browser).
    idx = pd.date_range("2000-01-02 11:00", periods=3, freq="min")
    wide = pd.DataFrame({"commonswiki": [500, 300, 130], "enwiki": [200, 210, 118], "wikidatawiki": [400, 100, 72]}, index=idx)
    assert chart.end_labels(wide) == ["commonswiki", "wikidatawiki"]
    far = wide.assign(enwiki=[200, 210, 60.0], wikidatawiki=[1, 1, 1])     # well separated: all three labelled
    assert chart.end_labels(far) == ["commonswiki", "enwiki", "wikidatawiki"]
    assert chart.end_labels(wide[["enwiki"]]) == ["enwiki"]                # one series: its label
    zeros = wide * 0
    assert chart.end_labels(zeros) == ["commonswiki"]                      # all equal and zero: one label, no division by zero


def test_chart_spec_and_palette():
    assert len(chart.SLOTS) == 8 and len(set(chart.SLOTS)) == 8
    idx = pd.date_range("2000-01-02 11:00", periods=4, freq="min", name="minute")  # the name edit_rate gives it
    few = pd.DataFrame({"a": [1, 2, 3, 4], "b": [4, 3, 2, 1]}, index=idx)
    colors = dict(zip("abcdefgh", chart.SLOTS))
    spec = chart.rate_chart(few, colors).to_dict()          # raises if the Altair spec is invalid
    assert len(spec["layer"]) == 5                           # line, hover rule, hover dots, end dots, end labels
    scale = spec["layer"][0]["encoding"]["color"]["scale"]
    assert scale["domain"] == ["a", "b"] and scale["range"] == chart.SLOTS[:2]
    many = pd.DataFrame({k: [1, 2, 3, 4] for k in "abcde"}, index=idx)
    assert len(chart.rate_chart(many, colors).to_dict()["layer"]) == 3   # five series: legend only, no direct labels
    only_b = chart.rate_chart(few[["b"]], colors).to_dict()["layer"][0]["encoding"]["color"]["scale"]
    assert only_b["range"] == [chart.SLOTS[1]]               # filtering must not repaint a surviving series


def test_the_app_renders_headless():
    from streamlit.testing.v1 import AppTest
    at = AppTest.from_file("/app/app.py", default_timeout=90).run()
    assert not at.exception, [e.value for e in at.exception]
    assert [h.value for h in at.subheader] == ["Surging pages", "Edit rate per wiki"]
    assert len(at.metric) == 3 and len(at.multiselect) == 1
