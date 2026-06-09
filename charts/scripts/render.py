#!/usr/bin/env python3
"""Render AdaptiveRateLimiter behavior charts from CSV timeseries.

Pipeline:
    sbt "charts/run"                 # writes docs/charts/data/*.csv + manifest.csv
    pip install -r requirements.txt
    python charts/scripts/render.py  # writes docs/images/adaptive-rate-limiter/*.png

This script is data-driven: it reads manifest.csv and renders one PNG per listed scenario, so adding scenarios on
the Scala side requires no changes here.
"""

import argparse
import csv
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless: render straight to files
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = REPO_ROOT / "docs" / "charts" / "data"
DEFAULT_OUT_DIR = REPO_ROOT / "docs" / "images" / "adaptive-rate-limiter"

# Distinct color + style per control-loop event kind.
EVENT_STYLES = {
    "worsening": {"color": "#c0392b", "linestyle": "--", "label": "worsening"},
    "recovering": {"color": "#e67e22", "linestyle": "--", "label": "recovering"},
    "recovered": {"color": "#27ae60", "linestyle": "-.", "label": "recovered"},
}


def read_rows(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def load_samples(path):
    elapsed, aimd, admitted, capacity, observed = [], [], [], [], []
    for row in read_rows(path):
        elapsed.append(float(row["elapsed_ms"]) / 1000.0)  # seconds
        aimd.append(float(row["aimd_rps"]))
        admitted.append(float(row["admitted_rps"]))
        capacity.append(float(row["backend_capacity_rps"]))
        observed.append(float(row["observed_failure_ratio"]))
    return elapsed, aimd, admitted, capacity, observed


def load_events(path):
    events = []
    for row in read_rows(path):
        events.append((float(row["elapsed_ms"]) / 1000.0, row["kind"], row.get("detail", "")))
    return events


def render_scenario(entry, data_dir, out_dir):
    elapsed, aimd, admitted, capacity, observed = load_samples(data_dir / entry["samples_file"])
    events = load_events(data_dir / entry["events_file"])

    fig, ax_rate = plt.subplots(figsize=(11, 5))

    # Left axis: rates (requests / second). The AIMD estimate is the controlled variable; the dashed line is the
    # backend's true capacity (the bottleneck the AIMD is discovering); admitted is the measured throughput.
    capacity_line, = ax_rate.plot(
        elapsed, capacity, color="#7f8c8d", linewidth=1.5, linestyle="--", label="backend capacity"
    )
    admitted_line, = ax_rate.plot(
        elapsed, admitted, color="#16a085", linewidth=1.0, alpha=0.6, label="admitted (measured)"
    )
    rate_line, = ax_rate.step(
        elapsed, aimd, where="post", color="#2c3e50", linewidth=2.0, label="AIMD rate estimate"
    )
    ax_rate.set_xlabel("time (s)")
    ax_rate.set_ylabel("rate (requests / second)")
    ax_rate.set_ylim(bottom=0)
    ax_rate.grid(True, alpha=0.3)

    # Right axis: the limiter's sampled failure ratio in [0, 1].
    ax_ratio = ax_rate.twinx()
    observed_line, = ax_ratio.plot(
        elapsed, observed, color="#2980b9", linewidth=1.5, alpha=0.85, label="failure ratio (observed)"
    )
    ax_ratio.set_ylabel("failure ratio")
    ax_ratio.set_ylim(-0.02, 1.02)

    # Vertical markers for control-loop events.
    seen_kinds = set()
    for ts, kind, _detail in events:
        if kind == "rate_change":
            continue
        style = EVENT_STYLES.get(kind)
        if style is None:
            continue
        ax_rate.axvline(ts, color=style["color"], linestyle=style["linestyle"], linewidth=1.2, alpha=0.7)
        seen_kinds.add(kind)

    handles = [rate_line, admitted_line, capacity_line, observed_line]
    handles += [
        Line2D([0], [0], color=EVENT_STYLES[kind]["color"], linestyle=EVENT_STYLES[kind]["linestyle"],
               label=EVENT_STYLES[kind]["label"])
        for kind in EVENT_STYLES
        if kind in seen_kinds
    ]
    ax_rate.legend(handles=handles, loc="upper left", framealpha=0.9, fontsize=9)

    fig.suptitle(entry["scenario"], fontsize=14, fontweight="bold")
    ax_rate.set_title(entry["description"], fontsize=10, color="#555555")
    fig.tight_layout()

    out_path = out_dir / f"{entry['scenario']}.png"
    fig.savefig(out_path, dpi=130)
    plt.close(fig)
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Render AdaptiveRateLimiter charts from CSV timeseries.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="directory containing manifest.csv")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR, help="directory to write PNGs into")
    args = parser.parse_args()

    manifest_path = args.data_dir / "manifest.csv"
    if not manifest_path.exists():
        raise SystemExit(f"manifest not found: {manifest_path} (run `sbt \"charts/run\"` first)")

    args.out_dir.mkdir(parents=True, exist_ok=True)

    for entry in read_rows(manifest_path):
        out_path = render_scenario(entry, args.data_dir, args.out_dir)
        print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
