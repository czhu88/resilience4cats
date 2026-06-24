#!/usr/bin/env python3
"""Render AdmissionController behavior charts from CSV timeseries.

Pipeline:
    sbt "charts/runMain io.mienks.resilience.charts.GenerateAdmissionCharts"  # writes docs/charts/admission-data/*.csv
    pip install -r requirements.txt
    python charts/scripts/render_admission.py  # writes docs/images/admission-controller/*.png

This script is data-driven: it reads manifest.csv and renders one PNG per listed scenario, so adding scenarios on
the Scala side requires no changes here.
"""

import argparse
import csv
import textwrap
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # headless: render straight to files
import matplotlib.pyplot as plt

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DATA_DIR = REPO_ROOT / "docs" / "charts" / "admission-data"
DEFAULT_OUT_DIR = REPO_ROOT / "docs" / "images" / "admission-controller"


def read_rows(path):
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def load_samples(path):
    elapsed, offered, admitted, accepted, capacity, rejection, failure = [], [], [], [], [], [], []
    for row in read_rows(path):
        elapsed.append(float(row["elapsed_ms"]) / 1000.0)  # seconds
        offered.append(float(row["offered_rps"]))
        admitted.append(float(row["admitted_rps"]))
        accepted.append(float(row["accepted_rps"]))
        capacity.append(float(row["capacity_rps"]))
        rejection.append(float(row["rejection_probability"]))
        failure.append(float(row["failure_ratio"]))
    return elapsed, offered, admitted, accepted, capacity, rejection, failure


def wrap_to_axes(ax, text, fontsize):
    """Wrap text so its rendered width matches the axes width (not the full figure)."""
    fig = ax.figure
    fig.canvas.draw()  # realize a renderer so we can measure text/axes extents
    renderer = fig.canvas.get_renderer()
    axes_width = ax.get_window_extent(renderer=renderer).width
    sample = "the quick brown fox jumps over the lazy dog"
    probe = ax.text(0.0, 0.0, sample, fontsize=fontsize, transform=ax.transAxes)
    char_width = probe.get_window_extent(renderer=renderer).width / len(sample)
    probe.remove()
    max_chars = max(20, int(axes_width / char_width))
    return textwrap.fill(text, width=max_chars)


def render_scenario(entry, data_dir, out_dir):
    elapsed, offered, admitted, accepted, capacity, rejection, failure = load_samples(data_dir / entry["samples_file"])

    fig, ax_rate = plt.subplots(figsize=(11, 5))

    # Left axis: rates (requests / second). Client input rate is the load before the gate; client allowed rate is what
    # the controller lets through; backend actual rate is the goodput (allowed minus throttled); capacity (dashed) is
    # the bottleneck.
    capacity_line, = ax_rate.plot(
        elapsed, capacity, color="#7f8c8d", linewidth=1.5, linestyle="--", label="backend capacity"
    )
    offered_line, = ax_rate.plot(
        elapsed, offered, color="#95a5a6", linewidth=1.0, alpha=0.6, label="client input rate"
    )
    admitted_line, = ax_rate.plot(
        elapsed, admitted, color="#2c3e50", linewidth=2.0, label="client allowed rate"
    )
    accepted_line, = ax_rate.plot(
        elapsed, accepted, color="#16a085", linewidth=1.0, alpha=0.7, label="backend actual rate"
    )
    ax_rate.set_xlabel("time (s)")
    ax_rate.set_ylabel("rate (requests / second)")
    ax_rate.set_ylim(bottom=0)
    ax_rate.grid(True, alpha=0.3)

    # Right axis: the controller's failure ratio and rejection probability in [0, 1]. The failure ratio keeps rising
    # with throttling; the rejection probability stays clamped at zero inside the dead zone and only lifts off once the
    # failure ratio crosses 1 - 1/k. The gap between the two lines is the dead zone.
    ax_prob = ax_rate.twinx()
    failure_line, = ax_prob.plot(
        elapsed, failure, color="#8e44ad", linewidth=1.0, alpha=0.6, linestyle=":", label="failure ratio"
    )
    rejection_line, = ax_prob.plot(
        elapsed, rejection, color="#c0392b", linewidth=1.5, alpha=0.85, label="rejection probability"
    )
    ax_prob.set_ylabel("failure ratio / rejection probability")
    ax_prob.set_ylim(-0.02, 1.02)

    handles = [admitted_line, accepted_line, offered_line, capacity_line, failure_line, rejection_line]
    # Legend in a single horizontal row above the plot so it never covers the curves.
    ax_rate.legend(
        handles=handles, loc="lower center", bbox_to_anchor=(0.5, 1.02), ncol=len(handles), frameon=False, fontsize=9
    )

    fig.suptitle(entry["scenario"], fontsize=14, fontweight="bold")
    description = wrap_to_axes(ax_rate, entry["description"], fontsize=10)
    # Lift the title clear of the legend row, with extra room per wrapped line.
    title_pad = 30 + 13 * description.count("\n")
    ax_rate.set_title(description, fontsize=10, color="#555555", pad=title_pad)
    fig.tight_layout()

    out_path = out_dir / f"{entry['scenario']}.png"
    fig.savefig(out_path, dpi=130, bbox_inches="tight")
    plt.close(fig)
    return out_path


def main():
    parser = argparse.ArgumentParser(description="Render AdmissionController charts from CSV timeseries.")
    parser.add_argument("--data-dir", type=Path, default=DEFAULT_DATA_DIR, help="directory containing manifest.csv")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT_DIR, help="directory to write PNGs into")
    args = parser.parse_args()

    manifest_path = args.data_dir / "manifest.csv"
    if not manifest_path.exists():
        raise SystemExit(
            f"manifest not found: {manifest_path} "
            f'(run `sbt "charts/runMain io.mienks.resilience.charts.GenerateAdmissionCharts"` first)'
        )

    args.out_dir.mkdir(parents=True, exist_ok=True)

    for entry in read_rows(manifest_path):
        out_path = render_scenario(entry, args.data_dir, args.out_dir)
        print(f"wrote {out_path}")


if __name__ == "__main__":
    main()
