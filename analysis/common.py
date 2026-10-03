"""Shared loading and styling for the gossip-sim analysis scripts."""

import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

# Reference categorical palette, fixed order (validated: CVD and normal-vision separation pass;
# slots 3-5 are below 3:1 contrast, so every line also gets a marker and a direct label).
PALETTE = ["#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948"]
MARKERS = ["o", "s", "^", "D", "v", "P", "X", "*"]
TEXT = "#0b0b0b"
TEXT_2 = "#52514e"
GRID = "#e4e3df"
SURFACE = "#fcfcfb"


def load_summary(out_dir):
    df = pd.read_csv(Path(out_dir) / "summary.csv")
    if "error" in df.columns:
        bad = df[df["error"].notna()]
        if len(bad):
            print(f"warning: {len(bad)} runs failed:", ", ".join(bad["run_id"]))
        df = df[df["error"].isna()]
    for col in ["rx_bytes", "tx_bytes", "rx_bytes_to_converge", "tx_bytes_to_converge"]:
        df[col.replace("bytes", "mb")] = df[col] / 1e6
    df["offline_d"] = df["offline_s"] / 86400
    return df


def load_samples(out_dir, run_id):
    path = Path(out_dir) / "runs" / run_id / "samples.jsonl"
    rows = [json.loads(line) for line in path.open()]
    df = pd.DataFrame(rows)
    df["frac_routable"] = df["routable_have"] / df["routable_gt"].clip(lower=1)
    df["frac_upds"] = df["upds_fresh"] / df["upds_gt"].clip(lower=1)
    df["rx_mb"] = df["rx"].apply(lambda d: sum(d.values()) / 1e6)
    df["tx_mb"] = df["tx"].apply(lambda d: sum(d.values()) / 1e6)
    return df


def strategy_styles(strategies):
    """Colour follows the strategy, in first-seen order, never cycled."""
    if len(strategies) > len(PALETTE):
        raise SystemExit(f"{len(strategies)} strategies exceed the {len(PALETTE)}-colour palette; facet instead")
    return {s: (PALETTE[i], MARKERS[i]) for i, s in enumerate(strategies)}


def style_axes(ax):
    ax.set_facecolor(SURFACE)
    ax.grid(True, color=GRID, linewidth=0.8)
    ax.set_axisbelow(True)
    for side in ["top", "right"]:
        ax.spines[side].set_visible(False)
    for side in ["left", "bottom"]:
        ax.spines[side].set_color(GRID)
    ax.tick_params(colors=TEXT_2, labelsize=8)
    ax.xaxis.label.set_color(TEXT_2)
    ax.yaxis.label.set_color(TEXT_2)
    ax.title.set_color(TEXT)


def new_grid(n, ncols=3, width=4.6, height=3.2):
    nrows = (n + ncols - 1) // ncols
    fig, axes = plt.subplots(nrows, ncols, figsize=(width * ncols, height * nrows), squeeze=False)
    fig.patch.set_facecolor(SURFACE)
    flat = list(axes.flat)
    for ax in flat[n:]:
        ax.set_visible(False)
    return fig, flat[:n]


def save(fig, out_dir, name):
    path = Path(out_dir) / "plots" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(path, dpi=130, bbox_inches="tight", facecolor=SURFACE)
    plt.close(fig)
    print("wrote", path)
