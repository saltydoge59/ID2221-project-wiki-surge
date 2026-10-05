"""Synthetic backfill of the Parquet archive, fitted to the real data collected so far.

Method: a run-level bootstrap with fresh identities. Every real (page, minute) window is a template: its edits, their
offsets inside the minute, revision-to-arrival lags, bot flags, namespaces, lengths and the number of distinct editors.
A synthetic minute draws templates at the real hour-of-day rate (per wiki, modulated by a fitted AR(1) process on
5-minute bins and a small per-day factor), then gives each a new page, new editors, a time and an id. The distribution of
edits per page-minute, the bot behaviour of bursts and the lag shape therefore come from the real data, not from a
formula. Pages are new (fraction --q-new) or drawn from a power-law stock; editors from power-law pools.

Output is ordinary archive files (same schema, date=/hour= layout) named synthetic-<hour>.parquet, with every title and
user prefixed SYN_, plus a manifest archive/_synthetic_manifest.json. Real files are never written, changed or deleted.

  python synth.py generate --days 10                                              # 10 days ending where the real data begins
  python synth.py generate --start 2026-09-24T10:30 --end 2026-10-04T10:30       # or explicit bounds
  python synth.py validate                                                        # synthetic vs real, same hours of day
  python synth.py generate --bucket synth-scratch ...                             # anywhere else, for tests
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs
import pyarrow.parquet as pq

SCHEMA = pa.schema([
    ("id", pa.int64()), ("wiki", pa.string()), ("title", pa.string()), ("namespace", pa.int32()),
    ("user", pa.string()), ("bot", pa.bool_()), ("type", pa.string()),
    ("old_len", pa.int32()), ("new_len", pa.int32()),
    ("ts", pa.timestamp("s", tz="UTC")), ("arrival", pa.timestamp("ms", tz="UTC"))])
TOP_K = 12            # wikis modelled individually; the rest share one pool
ID_BASE = 10 ** 12    # synthetic ids live above every real id
STOCK_SLOPE = 0.5     # rank-frequency slope of revisited pages (real top ranks: 0.4 to 0.6)
# Page novelty, calibrated to the real 22 h (0.74 distinct pages per edit, 86% of pages edited once): the fraction of
# windows on a brand-new page, and the revisit stock's size in days of a wiki's volume. Solved from the expected number
# of distinct and singleton pages under a power-law stock, then checked by `validate`.
Q_NEW, VSCALE = 0.60, 0.30
# Two independent edits can land on the same revisited page in the same minute and look like a multi-editor window, on top
# of the real multi-editor templates. The templates' rate is scaled down by that share (measured: contested windows
# 0.0029 vs 0.0023 real before scaling, with a stock of 0.3 days).
MULTI_SCALE = 0.7


def s3():
    return pafs.S3FileSystem(endpoint_override=os.environ.get("S3_ENDPOINT", "minio:9000"), scheme="http",
                             access_key=os.environ.get("MINIO_USER", "minioadmin"),
                             secret_key=os.environ.get("MINIO_PASSWORD", "minioadmin"), allow_bucket_creation=True)


def codes(arr):
    d = pc.dictionary_encode(arr)
    d = d.combine_chunks() if isinstance(d, pa.ChunkedArray) else d
    return d.indices.fill_null(-1).to_numpy(zero_copy_only=False).astype(np.int64), np.array(d.dictionary.to_pylist(), dtype=object)


def load(fs, bucket, synthetic=False, start=None, end=None):
    """Archive rows as numpy arrays, de-duplicated on id. synthetic=False reads only real files."""
    files = [i.path for i in fs.get_file_info(pafs.FileSelector(bucket, recursive=True))
             if i.type == pafs.FileType.File and i.path.endswith(".parquet") and (("synthetic-" in i.path) == synthetic)]
    if not files:
        sys.exit("no %s files in %s" % ("synthetic" if synthetic else "real", bucket))
    t = pa.concat_tables([pq.read_table(f, filesystem=fs) for f in files])
    wc0, _ = codes(t["wiki"])                       # id is a per-wiki counter: de-duplicate on (wiki, id)
    _, first = np.unique(wc0 * (1 << 40) + t["id"].to_numpy(), return_index=True)
    t = t.take(np.sort(first))
    ts = t["ts"].cast(pa.timestamp("s")).cast(pa.int64()).to_numpy()
    keep = np.ones(len(ts), bool)
    if start is not None:
        keep &= ts >= start
    if end is not None:
        keep &= ts < end
    t = t.filter(pa.array(keep))
    ts = ts[keep]
    wc, wn = codes(t["wiki"])
    kc, _ = codes(pc.binary_join_element_wise(t["wiki"], t["title"], "|"))
    uc, un = codes(t["user"])
    return {"n": len(ts), "ts": ts, "lag": t["arrival"].cast(pa.timestamp("ms")).cast(pa.int64()).to_numpy() / 1000.0 - ts,
            "wiki_c": wc, "wiki_names": wn, "key_c": kc, "user_c": uc, "n_users": len(un),
            "bot": t["bot"].to_numpy(zero_copy_only=False), "ns": t["namespace"].to_numpy(),
            "isnew": pc.equal(t["type"], "new").to_numpy(zero_copy_only=False),
            "old": t["old_len"].to_numpy(), "new": t["new_len"].to_numpy()}


# ------------------------------------------------------------------ fit

def circular_fill(v):
    """Fill NaN entries of a length-24 array by circular linear interpolation."""
    ok = np.flatnonzero(~np.isnan(v))
    if len(ok) == 0:
        return np.zeros(24)
    x = np.r_[ok - 24, ok, ok + 24]
    return np.interp(np.arange(24), x, np.r_[v[ok], v[ok], v[ok]])


def smooth(v):
    return 0.25 * np.roll(v, 1) + 0.5 * v + 0.25 * np.roll(v, -1)


def fit(d):
    """Templates, hour-of-day rates and modulation from real data."""
    minute = d["ts"] // 60
    m0 = minute.min()
    win = d["key_c"] * (1 << 20) + (minute - m0)
    order = np.lexsort((d["ts"], win))
    take = lambda k: d[k][order]
    win_s, ts_s, uc_s = win[order], d["ts"][order], d["user_c"][order]
    bounds = np.flatnonzero(np.r_[True, win_s[1:] != win_s[:-1]])
    ends = np.r_[bounds[1:], len(win_s)]
    nev = ends - bounds
    pair = np.unique(win_s * (d["n_users"] + 2) + (uc_s + 1))
    eds = np.bincount(np.searchsorted(np.unique(win_s), pair // (d["n_users"] + 2)), minlength=len(bounds))
    # first-seen order of editors inside a window, so a multi-editor template keeps "who edited when"
    ulocal = np.zeros(len(win_s), np.int8)
    for w in np.flatnonzero(eds >= 2):
        seen = {}
        for i in range(bounds[w], ends[w]):
            ulocal[i] = seen.setdefault(uc_s[i], len(seen))
    cnt = np.bincount(d["wiki_c"])
    top = np.argsort(-cnt)[:TOP_K]
    group_of_wiki = np.full(len(cnt), TOP_K, np.int8)
    group_of_wiki[top] = np.arange(TOP_K)
    grp = group_of_wiki[d["wiki_c"][order][bounds]]
    wiki_of_tmpl = d["wiki_c"][order][bounds]
    minute_s = ts_s // 60
    ev = {"off": (ts_s - minute_s * 60).astype(np.int16), "lag": take("lag").astype(np.float32), "bot": take("bot"),
          "isnew": take("isnew"), "old": take("old").astype(np.int32), "new": take("new").astype(np.int32),
          "ns": take("ns").astype(np.int32), "unull": uc_s < 0, "ulocal": ulocal}
    multi = eds >= 2
    # observed minutes per hour of day (the denominator of every rate)
    all_min = np.unique(minute)
    hod_of_min = (all_min // 60) % 24
    M = np.bincount(hod_of_min, minlength=24).astype(float)
    tmin = minute_s[bounds]
    thod = (tmin // 60) % 24
    rates = {}
    for name, sel in (("single", ~multi), ("multi", multi)):
        r = np.full((TOP_K + 1, 24), np.nan)
        for g in range(TOP_K + 1):
            c = np.bincount(thod[sel & (grp == g)], minlength=24).astype(float)
            r[g] = np.where(M > 0, c / np.maximum(M, 1), np.nan)
            r[g] = smooth(circular_fill(r[g]))
        rates[name] = r
    # AR(1) modulation on 5-minute bins, per group (single and multi templates together)
    sigma, phi = np.zeros(TOP_K + 1), np.zeros(TOP_K + 1)
    bins = (tmin - m0) // 5
    nb = int(bins.max()) + 1
    for g in range(TOP_K + 1):
        c = np.bincount(bins[grp == g], minlength=nb).astype(float)
        hod_b = ((m0 + np.arange(nb) * 5) // 60) % 24
        expect = 5 * (rates["single"][g][hod_b] + rates["multi"][g][hod_b])
        ok = (expect > 0.5) & (c > 0)
        lr = np.log(np.clip(c[ok] / expect[ok], 0.05, 20))
        if len(lr) > 20:
            var = max(lr.var() - float(np.mean(1.0 / np.maximum(c[ok], 1))), 0.0)   # drop the Poisson share of the variance
            sigma[g] = var ** 0.5
            phi[g] = float(np.clip(np.corrcoef(lr[:-1], lr[1:])[0, 1], 0.0, 0.95)) if lr.std() > 0 else 0.0
    # editors per template, for pool sizes; page novelty
    tail_w = np.flatnonzero(group_of_wiki == TOP_K)
    tail_cnt = np.array([(wiki_of_tmpl == w).sum() for w in tail_w], float)
    dist_pages = len(np.unique(d["key_c"]))
    model = {
        "ev": ev, "start": bounds.astype(np.int64), "n": nev.astype(np.int32), "group": grp, "multi": multi, "eds": eds.astype(np.int8),
        "rates": rates, "sigma": sigma, "phi": phi,
        "top_names": d["wiki_names"][top], "tail_names": d["wiki_names"][tail_w], "tail_share": tail_cnt / tail_cnt.sum(),
        "q_new": Q_NEW,
        "real": {"events": int(d["n"]), "windows": int(len(bounds)), "hours": round(len(all_min) / 60, 2),
                 "first_ts": int(d["ts"].min()), "last_ts": int(d["ts"].max()), "distinct_pages": int(dist_pages)},
    }
    model["by_group"] = {k: [np.flatnonzero((grp == g) & (multi == (k == "multi"))) for g in range(TOP_K + 1)] for k in ("single", "multi")}
    h_users = np.unique(d["user_c"][(~d["bot"]) & (d["user_c"] >= 0)]).size
    b_users = np.unique(d["user_c"][d["bot"] & (d["user_c"] >= 0)]).size
    model["pool"] = {"human_per_window": h_users / max(int((~ev["bot"][bounds]).sum()), 1), "bot_per_window": b_users / max(int(ev["bot"][bounds].sum()), 1)}
    return model


# ------------------------------------------------------------------ generate

def stock_rank(rng, v, size):
    """Power-law rank in 1..v with slope STOCK_SLOPE (inverse CDF of a continuous Pareto-like density)."""
    a = 1.0 - STOCK_SLOPE
    return np.minimum(np.floor(((v ** a - 1.0) * rng.random(size) + 1.0) ** (1.0 / a)), v).astype(np.int64)


def user_rank(rng, v, size):
    return np.minimum(np.floor(v ** rng.random(size)), v).astype(np.int64)  # slope 1


class Generator:
    def __init__(self, model, seed, q_new=None, vscale=VSCALE, day_sigma=0.07):
        self.m, self.rng = model, np.random.default_rng(seed)
        self.q_new = model["q_new"] if q_new is None else q_new
        self.vscale, self.day_sigma = vscale, day_sigma
        self.new_counter, self.next_id = {}, ID_BASE
        names = list(model["top_names"]) + list(model["tail_names"])
        self.wiki_idx = {w: i for i, w in enumerate(names)}
        daily = {}
        for g in range(TOP_K):
            daily[model["top_names"][g]] = 1440 * (model["rates"]["single"][g].mean() + model["rates"]["multi"][g].mean())
        tail = 1440 * (model["rates"]["single"][TOP_K].mean() + model["rates"]["multi"][TOP_K].mean())
        for w, sh in zip(model["tail_names"], model["tail_share"]):
            daily[w] = tail * sh
        self.daily = daily

    def modulation(self, n_bins):
        m, rng = self.m, self.rng
        x = np.zeros((TOP_K + 1, n_bins))
        for g in range(TOP_K + 1):
            s, p = m["sigma"][g], m["phi"][g]
            e = rng.standard_normal(n_bins) * s * np.sqrt(1 - p * p)
            x[g, 0] = rng.standard_normal() * s
            for b in range(1, n_bins):
                x[g, b] = p * x[g, b - 1] + e[b]
            x[g] -= s * s / 2  # keep the mean multiplier at 1
        return x

    def pick(self, kind, g, minutes_abs, lam_mult):
        """Template indices and their minutes for one group and kind, for the given absolute minutes."""
        m = self.m
        idx = m["by_group"][kind][g]
        if len(idx) == 0:
            return np.zeros(0, np.int64), np.zeros(0, np.int64)
        hod = (minutes_abs // 60) % 24
        k = self.rng.poisson(m["rates"][kind][g][hod] * lam_mult * (MULTI_SCALE if kind == "multi" else 1.0))
        return idx[self.rng.integers(0, len(idx), int(k.sum()))], np.repeat(minutes_abs, k)

    def hour(self, minutes_abs, x, day_mult):
        """Events whose template starts in these minutes: dict of arrays (not yet filtered by ts)."""
        m, rng = self.m, self.rng
        tm, tmin, tg = [], [], []
        for g in range(TOP_K + 1):
            mult = np.exp(x[g][(minutes_abs - self.t0_min) // 5]) * day_mult
            for kind in ("single", "multi"):
                i, mm = self.pick(kind, g, minutes_abs, mult)
                tm.append(i), tmin.append(mm), tg.append(np.full(len(i), g))
        tm, tmin, tg = np.concatenate(tm), np.concatenate(tmin), np.concatenate(tg)
        T = len(tm)
        # wiki per template: a top wiki fixed by its group, a tail wiki drawn by share
        wiki = np.empty(T, object)
        for g in range(TOP_K):
            wiki[tg == g] = m["top_names"][g]
        tail = tg == TOP_K
        wiki[tail] = rng.choice(m["tail_names"], int(tail.sum()), p=m["tail_share"])
        starts, lens = m["start"][tm], m["n"][tm]
        ev_t = np.repeat(np.arange(T), lens)
        src = np.repeat(starts, lens) + (np.arange(lens.sum()) - np.repeat(np.cumsum(lens) - lens, lens))
        e = m["ev"]
        first = e["bot"][starts], e["isnew"][starts]
        # pages: new, or a revisit drawn from a power-law stock sized to the wiki's volume
        force_new = first[1] | (rng.random(T) < self.q_new)
        v = np.array([max(50.0, self.vscale * self.daily[w]) for w in wiki])
        rank = stock_rank(rng, v, T)
        title = np.empty(T, object)
        for i in range(T):
            if force_new[i]:
                c = self.new_counter.get(wiki[i], 0)
                self.new_counter[wiki[i]] = c + 1
                title[i] = "SYN_%s_n%d" % (wiki[i], c)
            else:
                title[i] = "SYN_%s_%d" % (wiki[i], rank[i])
        # editors: bot or human pool of the wiki; a multi-editor template gets distinct editors
        pool = m["pool"]
        hv = np.maximum(20.0, pool["human_per_window"] * 1.5 * v)
        bv = np.maximum(3.0, pool["bot_per_window"] * 1.5 * v)
        ev_wiki, ev_bot = wiki[ev_t], e["bot"][src]
        size = np.where(ev_bot, bv[ev_t], hv[ev_t])
        local = e["ulocal"][src].astype(np.int64)
        # one editor per (template, local editor index): a 30-edit burst is one editor, not thirty
        pid, inv = np.unique(ev_t * 16 + local, return_inverse=True)
        size_pair = np.zeros(len(pid))
        size_pair[inv] = size
        base_pair = user_rank(rng, size_pair, len(pid))
        urank = (base_pair[inv] + local * 7919) % size.astype(np.int64) + 1   # distinct editors inside a window, same pool
        user = np.array(["SYN_%s_%s%d" % (w, "b" if b else "u", r) for w, b, r in zip(ev_wiki, ev_bot, urank)], dtype=object)
        user[e["unull"][src]] = None
        ts = tmin[ev_t] * 60 + e["off"][src].astype(np.int64)
        return {"wiki": ev_wiki, "title": title[ev_t], "user": user, "bot": ev_bot, "isnew": e["isnew"][src], "ns": e["ns"][src],
                "old": e["old"][src], "new": e["new"][src], "ts": ts, "arrival": ts + e["lag"][src].astype(np.float64)}


def to_table(a, first_id):
    n = len(a["ts"])
    order = np.argsort(a["arrival"], kind="stable")
    g = lambda k: a[k][order]
    ts = pa.array(g("ts"), pa.int64()).cast(pa.timestamp("s", tz="UTC"))
    arr = pa.array(np.round(g("arrival") * 1000).astype(np.int64), pa.int64()).cast(pa.timestamp("ms", tz="UTC"))
    return pa.Table.from_arrays([
        pa.array(first_id + np.arange(n), pa.int64()), pa.array(g("wiki").tolist(), pa.string()), pa.array(g("title").tolist(), pa.string()),
        pa.array(g("ns"), pa.int32()), pa.array(g("user").tolist(), pa.string()), pa.array(g("bot"), pa.bool_()),
        pa.array(np.where(g("isnew"), "new", "edit").tolist(), pa.string()), pa.array(g("old"), pa.int32()), pa.array(g("new"), pa.int32()),
        ts, arr], schema=SCHEMA)


def generate(model, fs, bucket, start, end, seed, overwrite=False, q_new=None, vscale=VSCALE, write=True, log=print):
    """Write synthetic-<hour>.parquet for every hour in [start, end); returns summary. start/end: epoch seconds."""
    gen = Generator(model, seed, q_new, vscale)
    rng = gen.rng
    if write:
        fs.create_dir(bucket)  # no-op when it exists
    first_min, last_min = start // 60, (end + 59) // 60
    gen.t0_min = first_min
    x = gen.modulation((last_min - first_min) // 5 + 2)
    carry = {}                      # events spilling into the next hour (offsets reach 59 s, lags are added on arrival only)
    total, files, day_mult = 0, 0, {}
    t0 = time.time()
    for h in range(first_min // 60, (last_min + 59) // 60):
        lo, hi = max(h * 60, first_min), min(h * 60 + 60, last_min)
        if lo >= hi:
            continue
        day = (h * 60) // 1440
        day_mult.setdefault(day, float(np.exp(rng.standard_normal() * gen.day_sigma)))
        a = gen.hour(np.arange(lo, hi), x, day_mult[day])
        keep = (a["ts"] >= start) & (a["ts"] < end)
        a = {k: v[keep] for k, v in a.items()}
        hrs = a["ts"] // 3600
        if len(hrs):
            tbl = to_table({k: v[hrs == h] for k, v in a.items()}, gen.next_id)
            gen.next_id += tbl.num_rows
            total += tbl.num_rows
            if write and tbl.num_rows:
                d = datetime.fromtimestamp(h * 3600, timezone.utc)
                path = "%s/date=%s/hour=%02d/synthetic-%s.parquet" % (bucket, d.strftime("%Y-%m-%d"), d.hour, d.strftime("%Y%m%dT%H"))
                if not overwrite and fs.get_file_info(path).type != pafs.FileType.NotFound:
                    sys.exit("refusing to overwrite %s (use --overwrite)" % path)
                if isinstance(fs, pafs.LocalFileSystem):      # tests only; S3 has no directories to create
                    fs.create_dir(path.rsplit("/", 1)[0])
                pq.write_table(tbl, path, filesystem=fs)
                files += 1
        if (h - first_min // 60) % 24 == 0:
            log("  hour %s: %d events so far (%.0fs)" % (datetime.fromtimestamp(h * 3600, timezone.utc).strftime("%m-%d %H"), total, time.time() - t0))
    return {"events": total, "files": files, "seconds": round(time.time() - t0, 1)}


# ------------------------------------------------------------------ validate

def metrics(d, hours):
    """Summary statistics that the evaluation and the surge rule depend on."""
    minute = d["ts"] // 60
    win = d["key_c"] * (1 << 20) + (minute - minute.min())
    uw, inv, c = np.unique(win, return_inverse=True, return_counts=True)
    pair = np.unique(win * (d["n_users"] + 2) + (d["user_c"] + 1))
    eds = np.bincount(np.searchsorted(uw, pair // (d["n_users"] + 2)), minlength=len(uw))
    cnt = np.bincount(d["wiki_c"])
    top3 = np.sort(cnt)[::-1][:3].sum() / d["n"]
    pages = np.bincount(np.unique(d["key_c"], return_inverse=True)[1])
    lag = d["lag"]
    dl = (d["new"] - d["old"]).astype(float)
    mx = lambda k: int(((c >= k)).sum())
    return {"events_per_s": d["n"] / (hours * 3600), "top3_wiki_share": top3, "bot_share": float(d["bot"].mean()),
            "new_page_share": float(d["isnew"].mean()), "null_user_share": float((d["user_c"] < 0).mean()),
            "single_edit_window_share": float((c == 1).mean()), "windows_ge5_per_h": mx(5) / hours, "windows_ge10_per_h": mx(10) / hours,
            "max_edits_in_window": int(c.max()), "contested_window_share": float((eds >= 2).mean()),
            "multi_editor_ge3_per_h": int(((eds >= 2) & (c >= 3)).sum()) / hours,
            "distinct_pages_per_edit": len(pages) / d["n"], "singleton_page_share": float((pages == 1).mean()),
            "lag_p50": float(np.percentile(lag, 50)), "lag_p90": float(np.percentile(lag, 90)), "lag_p99": float(np.percentile(lag, 99)),
            "delta_len_p50": float(np.percentile(dl, 50)), "delta_len_p90": float(np.percentile(dl, 90))}


TOLERANCE = {"events_per_s": 0.15, "top3_wiki_share": 0.15, "bot_share": 0.15, "new_page_share": 0.15, "single_edit_window_share": 0.01,
             "windows_ge5_per_h": 0.30, "windows_ge10_per_h": 0.40, "contested_window_share": 0.30, "multi_editor_ge3_per_h": 0.40,
             "distinct_pages_per_edit": 0.15, "singleton_page_share": 0.10, "lag_p50": 0.10, "lag_p90": 0.15, "lag_p99": 0.25,
             "delta_len_p50": 0.25, "delta_len_p90": 0.25}


def validate(args):
    fs = s3()
    real = load(fs, args.bucket)
    first, last = int(real["ts"].min()), int(real["ts"].max())
    hours = min(args.hours, (last - first) / 3600)
    end_real = first + int(hours * 3600)
    r = load(fs, args.bucket, start=first, end=end_real)
    model = fit(load(fs, args.bucket))
    # same hours of day, four days earlier, into a scratch bucket
    s_start = first - 4 * 86400
    scratch = args.scratch
    for i in fs.get_file_info(pafs.FileSelector(scratch, recursive=True)) if fs.get_file_info(scratch).type != pafs.FileType.NotFound else []:
        if i.type == pafs.FileType.File and "synthetic-" in i.path:
            fs.delete_file(i.path)      # scratch bucket only
    print("generating %.1f h of synthetic data into %s" % (hours, scratch))
    generate(model, fs, scratch, s_start, s_start + int(hours * 3600), args.seed, overwrite=True, q_new=args.q_new, vscale=args.vscale)
    s = load(fs, scratch, synthetic=True)
    mr, ms = metrics(r, hours), metrics(s, hours)
    bad = 0
    print("%-28s %12s %12s %8s  %s" % ("metric", "real", "synthetic", "rel.diff", "tolerance"))
    for k in mr:
        rel = (ms[k] - mr[k]) / mr[k] if mr[k] else float("nan")
        tol = TOLERANCE.get(k)
        flag = "" if tol is None else ("ok" if abs(rel) <= tol else "FAIL")
        bad += flag == "FAIL"
        print("%-28s %12.4f %12.4f %+8.1f%%  %s" % (k, mr[k], ms[k], 100 * rel, ("+-%d%% %s" % (tol * 100, flag)) if tol else "(info)"))
    print("VALIDATE %s (%d failing)" % ("OK" if not bad else "FAILED", bad))
    return 1 if bad else 0


def main(argv=None):
    a = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = a.add_subparsers(dest="cmd", required=True)
    for name in ("generate", "validate"):
        p = sub.add_parser(name)
        p.add_argument("--bucket", default=os.environ.get("BUCKET", "archive"), help="bucket with the REAL data to fit to (and, for generate, to write to)")
        p.add_argument("--seed", type=int, default=20261004)
        p.add_argument("--q-new", type=float, default=Q_NEW, help="fraction of windows on a brand-new page")
        p.add_argument("--vscale", type=float, default=VSCALE, help="size of the revisited-page stock, in days of the wiki's volume")
    g = sub.choices["generate"]
    g.add_argument("--start", help="ISO, UTC")
    g.add_argument("--end", help="ISO, UTC; must be before the first real event")
    g.add_argument("--days", type=float, help="instead of --start/--end: this many days ending at the start of the first real event's hour")
    g.add_argument("--overwrite", action="store_true", help="replace existing synthetic files for these hours")
    g.add_argument("--dry-run", action="store_true", help="generate and count, write nothing")
    v = sub.choices["validate"]
    v.add_argument("--hours", type=float, default=22)
    v.add_argument("--scratch", default="synth-scratch")
    args = a.parse_args(argv)
    if args.cmd == "validate":
        sys.exit(validate(args))
    fs = s3()
    parse = lambda s: int((datetime.fromisoformat(s) if "+" in s else datetime.fromisoformat(s).replace(tzinfo=timezone.utc)).timestamp())
    real = load(fs, args.bucket)
    if args.days:
        if args.start or args.end:
            sys.exit("--days replaces --start and --end")
        end = int(real["ts"].min()) // 3600 * 3600
        start = end - int(args.days * 86400)
        args.start, args.end = (datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%M") for t in (start, end))
    elif args.start and args.end:
        start, end = parse(args.start), parse(args.end)
    else:
        sys.exit("give --days N, or both --start and --end")
    if end > int(real["ts"].min()):
        sys.exit("--end %s is not before the first real event (%s)" % (args.end, datetime.fromtimestamp(int(real["ts"].min()), timezone.utc)))
    model = fit(real)
    print("fitted on %(events)d real events, %(windows)d page-minute templates, %(hours)s hours" % model["real"])
    res = generate(model, fs, args.bucket, start, end, args.seed, args.overwrite, args.q_new, args.vscale, write=not args.dry_run)
    manifest = {"generated_at": datetime.now(timezone.utc).isoformat(), "seed": args.seed, "start": args.start, "end": args.end, "q_new": model["q_new"] if args.q_new is None else args.q_new,
                "vscale": args.vscale, "fitted_on": model["real"], "result": res, "method": "run-level bootstrap with fresh identities (see producer/synth.py)",
                "marking": "files named synthetic-*.parquet; every title and user starts with SYN_; ids start at 10^12"}
    if not args.dry_run:
        with fs.open_output_stream("%s/_synthetic_manifest.json" % args.bucket) as f:
            f.write(json.dumps(manifest, indent=1, default=str).encode())
    print("SYNTH " + json.dumps(res))


if __name__ == "__main__":
    main()
