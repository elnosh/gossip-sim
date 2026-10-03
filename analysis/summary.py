"""Table view of an experiment: one row per run with the headline numbers, as Markdown.

usage: python analysis/summary.py out/matrix [--scenario bootstrap] [--peer-set mixed]
"""

import argparse

from common import load_summary

COLS = ["scenario", "offline_d", "peer_set", "strategy", "t_converged_s", "rx_mb_to_converge", "rx_mb", "tx_mb",
        "frac_routable", "frac_upds", "frac_nodes", "rejected", "closed"]


def main():
    p = argparse.ArgumentParser()
    p.add_argument("out_dir", nargs="?", default="out/matrix")
    p.add_argument("--scenario")
    p.add_argument("--peer-set")
    a = p.parse_args()
    df = load_summary(a.out_dir)
    if a.scenario:
        df = df[df["scenario"] == a.scenario]
    if a.peer_set:
        df = df[df["peer_set"] == a.peer_set]
    df = df[COLS].sort_values(["scenario", "offline_d", "peer_set", "strategy"])
    try:
        print(df.to_markdown(index=False, floatfmt=".3f"))
    except ImportError:  # to_markdown needs the optional `tabulate` package
        print(df.to_string(index=False))


if __name__ == "__main__":
    main()
