from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

import replay
from producer import SCHEMA, key, project

H = datetime(2026, 10, 4, 11, tzinfo=timezone.utc)


def ev(i, wiki, title, ts, arrival_lag=1.0):
    t = int(ts.timestamp())
    arrival = (ts + timedelta(seconds=arrival_lag)).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"
    return project({"id": i, "type": "edit", "wiki": wiki, "title": title, "namespace": 0, "user": "u", "bot": False,
                    "length": {"old": 1, "new": 2}, "timestamp": t, "meta": {"dt": arrival}})


def write(root, hour, recs, name="part-1.parquet"):
    d = root / ("date=%s" % hour.strftime("%Y-%m-%d")) / ("hour=%02d" % hour.hour)
    d.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_pylist(recs, schema=SCHEMA), str(d / name))


@pytest.fixture
def archive(tmp_path):
    # hour 11: ids out of arrival order across two files, plus a duplicate of id 2; hour 12 follows
    write(tmp_path, H, [ev(2, "enwiki", "B", H + timedelta(seconds=30)), ev(1, "enwiki", "A", H + timedelta(seconds=10))],
          "part-1.parquet")
    write(tmp_path, H, [ev(2, "enwiki", "B", H + timedelta(seconds=30)), ev(3, "dewiki", "C", H + timedelta(minutes=59))],
          "part-2.parquet")
    h2 = H + timedelta(hours=1)
    write(tmp_path, h2, [ev(4, "enwiki", "A", h2 + timedelta(seconds=5))])
    return str(tmp_path)


def ids(archive, **kw):
    return [r["id"] for r in replay.records(pafs.LocalFileSystem(), archive, **kw)]


def test_arrival_order_and_dedup(archive):
    assert ids(archive) == [1, 2, 3, 4]  # id 2 once; sorted by arrival inside each hour, hours in order


def test_same_id_in_two_wikis_is_two_events(tmp_path):
    # ids are per-wiki counters: deduplicating on id alone dropped real edits (13 in the first 1.3 million)
    write(tmp_path, H, [ev(5, "eswikivoyage", "A", H + timedelta(seconds=1)), ev(5, "mdfwiki", "B", H + timedelta(seconds=2)),
                        ev(5, "mdfwiki", "B", H + timedelta(seconds=2))])      # the last one is a true duplicate
    got = [(r["wiki"], r["id"]) for r in replay.records(pafs.LocalFileSystem(), str(tmp_path))]
    assert got == [("eswikivoyage", 5), ("mdfwiki", 5)]


def test_time_range_prunes_and_filters(archive):
    assert ids(archive, start=H + timedelta(seconds=20), end=H + timedelta(hours=1)) == [2, 3]
    assert ids(archive, start=H + timedelta(hours=1)) == [4]


def test_key_modes():
    r = ev(1, "enwiki", "Page", H)
    assert replay.KEYS["page"](r) == key(r) == b"enwiki|Page"
    assert replay.KEYS["wiki"](r) == b"enwiki"


def test_pacing_offsets():
    a, b = ev(1, "w", "t", H), ev(2, "w", "t", H + timedelta(seconds=10))
    assert replay.offset(50, a, a["arrival"], rate=100, speed=0) == 0.5   # fixed rate: i / rate
    assert replay.offset(5, a, a["arrival"], rate=0, speed=0) == 0.0      # unthrottled
    assert replay.offset(1, b, a["arrival"], rate=0, speed=1) == 10.0     # real time follows arrival timestamps
    assert replay.offset(1, b, a["arrival"], rate=0, speed=10) == 1.0     # ten times faster


def test_speed_mode_paces_by_archive_time_not_by_the_shift(monkeypatch, archive):
    # regression: pacing was computed from the already-shifted arrival, adding the archive's age (hours) as delay
    sleeps, sent = [], []

    class FakeProducer:
        def __init__(self, *a, **k): pass
        def produce(self, topic, key, value, on_delivery): sent.append(value); on_delivery(None, None)
        def poll(self, *a): pass
        def flush(self): pass

    class FakeFs:
        def __init__(self, *a, **k): pass

    monkeypatch.setattr(replay, "Producer", FakeProducer)
    monkeypatch.setattr(replay.pafs, "S3FileSystem", lambda **k: pafs.LocalFileSystem())
    monkeypatch.setattr(replay, "ensure_topic", lambda *a: None)
    monkeypatch.setattr(replay, "AdminClient", lambda *a: None)
    monkeypatch.setattr(replay.time, "sleep", sleeps.append)
    replay.main(["--topic", "t", "--speed", "10", "--bucket", archive, "--start", "2026-10-04T11:00",
                 "--end", "2026-10-04T11:01"])  # ids 1 (+10 s) and 2 (+30 s) fall in the first minute
    replay.main(["--topic", "t", "--speed", "100", "--bucket", archive])
    assert len(sent) == 2 + 4  # first run: ids 1, 2; second run: all four distinct ids
    assert sleeps and max(sleeps) < 60, "paced %s s for archive events minutes apart" % max(sleeps)  # bug: ~10 000 s


def test_refuses_live_topic_and_conflicting_pacing():
    with pytest.raises(SystemExit, match="live topic"):
        replay.main(["--topic", "wiki-edits"])
    with pytest.raises(SystemExit, match="alternatives"):
        replay.main(["--rate", "5", "--speed", "1"])
