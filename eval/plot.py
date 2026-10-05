#!/usr/bin/env python3
"""Draw docs/figures/eval.png from results/eval/*.csv. Needs matplotlib on the host (optional; the CSVs are the results).

Palette: slots 1-4 of the validated categorical palette (dataviz validator, white surface: all gates pass; aqua and yellow
sit under 3:1 contrast, so every line is also labelled directly). One axis per panel, never a second y-axis.
"""
import csv
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

ROOT = Path(__file__).resolve().parent.parent
RES = ROOT / "results" / "eval"
BLUE, ORANGE, AQUA, YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
INK, MUTED, GRID = "#0b0b0b", "#898781", "#e1e0d9"


def rows(name):
    return list(csv.DictReader(open(RES / (name + ".csv"))))


def style(ax, xlabel, ylabel, title):
    ax.set_title(title, fontsize=7, loc="left", color=INK, pad=3)
    ax.set_xlabel(xlabel, fontsize=6, color=MUTED, labelpad=1)
    ax.set_ylabel(ylabel, fontsize=6, color=MUTED, labelpad=1)
    ax.tick_params(labelsize=6, colors=MUTED, length=2, pad=1)
    ax.grid(axis="y", color=GRID, linewidth=0.6)
    ax.set_axisbelow(True)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color(GRID)


def line(ax, x, y, color, label, dy=0, dash=False, dx=4, direct=True, legend=None):
    ax.plot(x, y, color=color, linewidth=1.4, linestyle="--" if dash else "-", marker="o", markersize=2.6,
            markeredgecolor="white", markeredgewidth=0.5, solid_capstyle="round", label=legend)
    if direct:
        ax.annotate(label, (x[-1], y[-1]), xytext=(dx, dy), textcoords="offset points", fontsize=5.6, color=INK, va="center")


fig, axs = plt.subplots(1, 4, figsize=(7.3, 1.85), dpi=300)

# (a) throughput
thr = rows("throughput")
cores = [1, 2, 4, 8]
for p, col, dy in ((1, BLUE, -3), (2, ORANGE, 3), (4, AQUA, 6), (6, YELLOW, -6)):
    y = [float(r["events_per_s"]) / 1000 for c in cores for r in thr if r["sink"] == "cassandra" and int(r["partitions"]) == p and int(r["cores"]) == c]
    line(axs[0], cores, y, col, "", dy, direct=False, legend="%d partition%s" % (p, "" if p == 1 else "s"))
y = [float(r["events_per_s"]) / 1000 for c in cores for r in thr if r["sink"] == "noop" and int(r["cores"]) == c]
line(axs[0], cores, y, MUTED, "no sink, 6 part.", 0, dash=True, legend=None)
axs[0].set_xscale("log", base=2)
axs[0].set_xticks(cores)
axs[0].set_xticklabels(cores)
axs[0].set_ylim(0, 35)
axs[0].set_xlim(0.9, 19)
axs[0].minorticks_off()
axs[0].legend(fontsize=5.4, loc="lower right", frameon=False, labelcolor=INK, handlelength=1.4, borderaxespad=0.2)
style(axs[0], "Spark cores (Cassandra sink)", "k events / s", "(a) throughput")

# (c) latency
lat = rows("latency")
trig = [int(r["trigger"].split()[0]) for r in lat]
line(axs[1], trig, [float(r["p99_s"]) for r in lat], ORANGE, "p99", 0, dx=3)
line(axs[1], trig, [float(r["p50_s"]) for r in lat], BLUE, "median", 0, dx=3)
axs[1].set_xscale("log")
axs[1].set_xticks(trig)
axs[1].set_xticklabels(trig)
axs[1].set_xlim(0.8, 150)
axs[1].minorticks_off()
style(axs[1], "trigger interval (s)", "seconds", "(b) latency")

# (d) completeness and state against the watermark
wm = [r for r in rows("watermark") if r["dedup"] == "True"]
w = [int(r["watermark"].split()[0]) for r in wm]
line(axs[2], w, [100 * float(r["completeness"]) for r in wm], BLUE, "", 0)
axs[2].text(128, 99.23, "axis starts at 99.2%", fontsize=5.2, color=MUTED, ha="right")
style(axs[2], "watermark (s)", "% of edits counted", "(c) completeness")
axs[2].set_xlim(-5, 130)
axs[2].set_ylim(99.2, 100.05)
line(axs[3], w, [int(r["dedup_rows"]) for r in wm], ORANGE, "de-dup", 0, dx=3)
line(axs[3], w, [int(r["agg_rows"]) for r in wm], BLUE, "windows", 0, dx=3)
axs[3].set_xlim(-5, 150)
axs[3].set_ylim(0, 5200)
style(axs[3], "watermark (s)", "state rows", "(d) state size")

fig.tight_layout(pad=0.4, w_pad=0.8)
out = ROOT / "docs" / "figures" / "eval.png"
fig.savefig(out)
print("wrote", out.relative_to(ROOT))
