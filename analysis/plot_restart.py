"""Restart curves: bytes and completeness as a function of offline duration, one line per strategy,
one panel per peer set. Separate figures per persisted-updates mode and per metric.

usage: python analysis/plot_restart.py out/matrix
"""

import sys

from common import TEXT_2, load_summary, new_grid, save, strategy_styles, style_axes

METRICS = [
    ("rx_mb", "MB received (whole run)", "restart_rx_mb"),
    ("tx_mb", "MB sent (whole run)", "restart_tx_mb"),
    ("frac_routable", "Routable channels at end (fraction)", "restart_routable"),
    ("frac_upds", "Fresh updates at end (fraction)", "restart_upds"),
    ("frac_nodes", "Fresh node announcements at end (fraction)", "restart_nodes"),
]


def main(out_dir):
    df = load_summary(out_dir)
    rs = df[df["scenario"].str.startswith("restart")]
    if rs.empty:
        raise SystemExit("no restart runs in summary")
    strategies = list(dict.fromkeys(df["strategy"]))
    styles = strategy_styles(strategies)
    peer_sets = list(dict.fromkeys(df["peer_set"]))
    for mode in sorted(rs["scenario"].unique()):
        sub = rs[rs["scenario"] == mode]
        for col, ylabel, name in METRICS:
            fig, axes = new_grid(len(peer_sets))
            for ax, ps in zip(axes, peer_sets):
                d = sub[sub["peer_set"] == ps]
                for st in strategies:
                    s = d[d["strategy"] == st].sort_values("offline_d")
                    if s.empty:
                        continue
                    color, marker = styles[st]
                    ax.plot(s["offline_d"], s[col], color=color, marker=marker, markersize=5, linewidth=2, label=st)
                ax.set_xscale("log")
                ax.set_title(ps, fontsize=10, loc="left")
                ax.set_xlabel("offline (days, log scale)")
                ax.set_ylabel(ylabel, fontsize=8)
                if col.startswith("frac"):
                    ax.set_ylim(-0.02, 1.02)
                ax.axvline(14, color=TEXT_2, linewidth=0.8, linestyle=":")
                style_axes(ax)
            handles, labels = axes[0].get_legend_handles_labels()
            fig.legend(handles, labels, loc="upper center", ncol=len(labels), frameon=False, fontsize=8, bbox_to_anchor=(0.5, 1.03))
            fig.suptitle(f"{ylabel} after restart ({mode}); dotted line = 14 days", y=1.07, fontsize=11)
            save(fig, out_dir, f"{name}_{mode}.png")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "out/matrix")
