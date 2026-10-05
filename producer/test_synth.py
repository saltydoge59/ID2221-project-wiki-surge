"""Tests for synth.py on a toy 'real' dataset (no MinIO): output layout, provenance marks, determinism, boundaries and the
editors-per-window structure that the surge rule depends on."""
from datetime import datetime, timezone

import numpy as np
import pyarrow as pa
import pyarrow.fs as pafs
import pyarrow.parquet as pq
import pytest

import synth

START = int(datetime(2026, 10, 4, 10, 0, tzinfo=timezone.utc).timestamp())
# Parquet has no seconds unit: the producer's timestamp[s] reads back as milliseconds, and so do these files
ON_DISK = synth.SCHEMA.set(9, pa.field("ts", pa.timestamp("ms", tz="UTC")))


def toy(rng, burst_only=False, hours=3):
    """A toy real dataset in load()'s format: 14 wikis, three hours, windows of 1 event, or 5 edits by one editor."""
    rows = []
    for minute in range(hours * 60):
        for _ in range(rng.integers(8, 14)):
            w = rng.integers(0, 14)
            page = rng.integers(0, 4000)
            n = 5 if burst_only else (1 if rng.random() < 0.9 else 2)
            u = rng.integers(0, 300)
            for j in range(n):
                rows.append((w, w * 10000 + page, u, bool(u % 3 == 0), 0, False, 100, 100 + 7 * j, START + minute * 60 + (j * 5) % 60, 1.0 + 0.1 * j))
    c = np.array(rows, dtype=object)
    col = lambda i, t: np.array(c[:, i], dtype=t)
    return {"n": len(rows), "ts": col(8, np.int64), "lag": col(9, float), "wiki_c": col(0, np.int64),
            "wiki_names": np.array(["wiki%d" % i for i in range(14)], dtype=object), "key_c": col(1, np.int64),
            "user_c": col(2, np.int64), "n_users": 300, "bot": col(3, bool), "ns": col(4, np.int32), "isnew": col(5, bool),
            "old": col(6, np.int32), "new": col(7, np.int32)}


@pytest.fixture(scope="module")
def model():
    return synth.fit(toy(np.random.default_rng(1)))


def run(model, tmp_path, name, start, end, seed=7, **kw):
    fs, bucket = pafs.LocalFileSystem(), str(tmp_path / name)
    res = synth.generate(model, fs, bucket, start, end, seed, log=lambda *_: None, **kw)
    files = sorted(i.path for i in fs.get_file_info(pafs.FileSelector(bucket, recursive=True)) if i.path.endswith(".parquet"))
    return res, files


def read_all(files):
    return pa.concat_tables([pq.ParquetFile(f).read() for f in files])


def test_layout_schema_provenance_and_ids(model, tmp_path):
    start, end = START + 600, START + 3 * 3600 + 1200      # not on hour boundaries
    res, files = run(model, tmp_path, "a", start, end)
    assert res["files"] == len(files) == 4
    assert 1500 < res["events"] < 3000                 # toy rate is about 11 events a minute over 190 minutes
    for f in files:
        parts = f.split("/")
        day, hour = parts[-3].split("=")[1], int(parts[-2].split("=")[1])
        assert parts[-1].startswith("synthetic-")
        t = pq.ParquetFile(f).read()
        assert t.schema.equals(ON_DISK)                    # what the real producer's files read back as
        ts = t["ts"].cast(pa.timestamp("s")).cast(pa.int64()).to_pylist()
        assert all(datetime.fromtimestamp(x, timezone.utc).strftime("%Y-%m-%d") == day and datetime.fromtimestamp(x, timezone.utc).hour == hour for x in ts)
    t = read_all(files)
    ts = np.array(t["ts"].cast(pa.timestamp("s")).cast(pa.int64()).to_pylist())
    assert ts.min() >= start and ts.max() < end                               # the end is exclusive, mid-hour
    ids = t["id"].to_numpy()
    assert len(set(ids)) == len(ids) and ids.min() >= synth.ID_BASE           # unique, above every real id
    assert all(x.startswith("SYN_") for x in t["title"].to_pylist())
    assert all(x is None or x.startswith("SYN_") for x in t["user"].to_pylist())
    assert t["wiki"].to_pylist()[0].startswith("wiki")                          # wikis keep their real names


def test_determinism_and_overwrite_guard(model, tmp_path):
    _, f1 = run(model, tmp_path, "d1", START, START + 7200, seed=5)
    _, f2 = run(model, tmp_path, "d2", START, START + 7200, seed=5)
    _, f3 = run(model, tmp_path, "d3", START, START + 7200, seed=6)
    same = lambda a, b: read_all(a).equals(read_all(b))
    assert same(f1, f2) and not same(f1, f3)
    with pytest.raises(SystemExit, match="refusing to overwrite"):
        synth.generate(model, pafs.LocalFileSystem(), str(tmp_path / "d1"), START, START + 7200, 5, log=lambda *_: None)
    synth.generate(model, pafs.LocalFileSystem(), str(tmp_path / "d1"), START, START + 7200, 5, overwrite=True, log=lambda *_: None)


def test_one_editor_per_burst_not_one_per_edit(tmp_path):
    # regression: users were drawn per event, so a 5-edit single-editor burst got 5 editors and became "contested"
    m = synth.fit(toy(np.random.default_rng(2), burst_only=True))
    assert m["multi"].sum() == 0
    _, files = run(m, tmp_path, "b", START, START + 3600, q_new=1.0)        # every window on a brand-new page: no collisions
    t = read_all(files).to_pylist()
    editors = {}
    for r in t:
        editors.setdefault((r["title"], r["ts"].replace(second=0)), set()).add(r["user"])
    sizes = [len(v) for v in editors.values()]
    assert max(sizes) == 1, "a window has %d editors" % max(sizes)
    counts = {}
    for r in t:
        counts[(r["title"], r["ts"].replace(second=0))] = counts.get((r["title"], r["ts"].replace(second=0)), 0) + 1
    assert set(counts.values()) == {5}                                         # every window is a whole template


def test_stock_and_user_ranks_stay_in_range():
    rng = np.random.default_rng(3)
    for v in (50.0, 10_000.0):
        r = synth.stock_rank(rng, np.full(20000, v), 20000)
        assert r.min() >= 1 and r.max() <= v and r.mean() < v / 2           # head-heavy
        u = synth.user_rank(rng, np.full(20000, v), 20000)
        assert u.min() >= 1 and u.max() <= v and np.median(u) < np.sqrt(v) * 2


def test_circular_fill_wraps_midnight():
    v = np.full(24, np.nan)
    v[2], v[22] = 4.0, 8.0
    out = synth.circular_fill(v)
    assert out[2] == 4.0 and out[22] == 8.0 and out[0] == pytest.approx(6.0) and 4.0 < out[23] < 8.0   # hour 0 lies between 22 and 2


def test_days_option_ends_at_the_hour_of_the_first_real_event(monkeypatch, tmp_path, capsys):
    real = toy(np.random.default_rng(4), hours=1)
    real["ts"] = real["ts"] + 1930                    # first real event at 10:32:10, not on an hour boundary
    seen = {}
    monkeypatch.setattr(synth, "s3", lambda: pafs.LocalFileSystem())
    monkeypatch.setattr(synth, "load", lambda fs, bucket, **k: real)
    monkeypatch.setattr(synth, "fit", lambda d: {"real": {"events": 1, "windows": 1, "hours": 1}, "q_new": 0.6})
    monkeypatch.setattr(synth, "generate", lambda m, fs, b, start, end, *a, **k: seen.update(start=start, end=end) or {"events": 0, "files": 0})
    synth.main(["generate", "--bucket", str(tmp_path), "--days", "2", "--dry-run"])
    first = int(real["ts"].min())
    assert seen["end"] == first // 3600 * 3600 and seen["end"] <= first          # never past the first real event
    assert seen["end"] - seen["start"] == 2 * 86400
    with pytest.raises(SystemExit, match="replaces"):
        synth.main(["generate", "--bucket", str(tmp_path), "--days", "2", "--start", "2026-01-01T00:00"])
    with pytest.raises(SystemExit, match="give --days"):
        synth.main(["generate", "--bucket", str(tmp_path)])


def test_load_deduplicates_on_wiki_and_id(tmp_path):
    # regression: np.unique on id alone dropped one of two edits that share an id in different wikis
    rec = lambda i, wiki: {"id": i, "wiki": wiki, "title": "T", "namespace": 0, "user": "u", "bot": False, "type": "edit", "old_len": 1,
                           "new_len": 2, "ts": datetime(2026, 10, 4, 10, 0, tzinfo=timezone.utc), "arrival": datetime(2026, 10, 4, 10, 0, 1, tzinfo=timezone.utc)}
    d = tmp_path / "archive" / "date=2026-10-04" / "hour=10"
    d.mkdir(parents=True)
    pq.write_table(pa.Table.from_pylist([rec(5, "awiki"), rec(5, "bwiki"), rec(5, "awiki")], schema=synth.SCHEMA), str(d / "part-1.parquet"))
    got = synth.load(pafs.LocalFileSystem(), str(tmp_path / "archive"))
    assert got["n"] == 2
