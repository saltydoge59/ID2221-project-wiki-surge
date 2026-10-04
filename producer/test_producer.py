import json

import pyarrow as pa
import pytest

import producer as mod
from producer import SCHEMA, key, kafka_value, project

EDIT = {"id": 1, "type": "edit", "wiki": "enwiki", "title": "Foo", "namespace": 0, "user": "u", "bot": False,
        "length": {"old": 10, "new": 15}, "timestamp": 1791107787, "meta": {"dt": "2026-10-04T09:56:27.191Z"}}


def test_filters_non_edits():
    assert project({**EDIT, "type": "categorize"}) is None
    assert project({**EDIT, "type": "log"}) is None


def test_projection_and_key():
    rec = project(EDIT)
    assert rec["wiki"] == "enwiki" and rec["new_len"] - rec["old_len"] == 5
    assert key(rec) == b"enwiki|Foo"
    assert json.loads(kafka_value(rec))["ts"].startswith("2026-10-04")
    pa.Table.from_pylist([rec], schema=SCHEMA)  # fits the archive schema


def _run_main(monkeypatch, tmp_path, state=None):
    """Run main() against a fake feed that yields A, B then drops; returns the Last-Event-ID of each connect."""
    calls = []

    def fake_sse(last_id):
        calls.append(last_id)
        if len(calls) == 1:
            yield "A", json.dumps(EDIT)
            yield "B", json.dumps({**EDIT, "id": 2})
            raise ConnectionError("dropped")
        raise SystemExit  # second connect: stop the infinite loop

    class Dummy:
        def __init__(self, *a, **k): pass
        def produce(self, *a, **k): pass
        def poll(self, *a): pass
        def flush(self): pass
        def add(self, rec): pass
        def due(self): return False

    path = tmp_path / "state"
    if state:
        path.write_text(state)
    monkeypatch.setattr(mod, "STATE", str(path))
    monkeypatch.setattr(mod, "sse", fake_sse)
    monkeypatch.setattr(mod, "Producer", Dummy)
    monkeypatch.setattr(mod, "Archive", Dummy)
    monkeypatch.setattr(mod.time, "sleep", lambda s: None)
    with pytest.raises(SystemExit):
        mod.main()
    return calls


def test_reconnect_resumes_from_last_seen_id(monkeypatch, tmp_path):
    # buffer is intact on an in-process reconnect, so resuming from the last *flushed* id would duplicate rows
    assert _run_main(monkeypatch, tmp_path) == [None, "B"]


def test_restart_resumes_from_state_file(monkeypatch, tmp_path):
    assert _run_main(monkeypatch, tmp_path, state="X")[0] == "X"


def test_new_page_without_old_length():
    rec = project({**EDIT, "type": "new", "length": {"new": 7}})
    assert rec["old_len"] == 0 and rec["new_len"] == 7
