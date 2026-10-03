"""Bootstrap: routable-channel completeness over time per strategy (one panel per peer set), and
bytes received to converge.

usage: python analysis/plot_bootstrap.py out/matrix
"""

import sys

from common import TEXT_2, load_samples, load_summary, new_grid, save, strategy_styles, style_axes


def main(out_dir):
    df = load_summary(out_dir)
    boot = df[df["scenario"] == "bootstrap"]
    if boot.empty:
        raise SystemExit("no bootstrap runs in summary")
    strategies = list(dict.fromkeys(df["strategy"]))
    styles = strategy_styles(strategies)
    peer_sets = list(dict.fromkeys(boot["peer_set"]))

    for col, ylabel, name in [("frac_routable", "Routable channels (fraction)", "bootstrap_routable"),
                              ("rx_mb", "MB received", "bootstrap_rx_mb")]:
        fig, axes = new_grid(len(peer_sets))
        for ax, ps in zip(axes, peer_sets):
            for st in strategies:
                row = boot[(boot["peer_set"] == ps) & (boot["strategy"] == st)]
                if row.empty:
                    continue
                s = load_samples(out_dir, row.iloc[0]["run_id"])
                color, marker = styles[st]
                ax.plot(s["t_s"], s[col], color=color, linewidth=2, label=st, marker=marker, markevery=max(1, len(s) // 6), markersize=5)
                ax.annotate(f"{s[col].iloc[-1]:.2f}" if col.startswith("frac") else f"{s[col].iloc[-1]:.0f}",
                            (s["t_s"].iloc[-1], s[col].iloc[-1]), fontsize=7, color=TEXT_2,
                            xytext=(3, 0), textcoords="offset points", va="center")
            ax.set_title(ps, fontsize=10, loc="left")
            ax.set_xlabel("simulated time (s)")
            ax.set_ylabel(ylabel, fontsize=8)
            if col.startswith("frac"):
                ax.set_ylim(-0.02, 1.05)
            style_axes(ax)
        handles, labels = axes[0].get_legend_handles_labels()
        fig.legend(handles, labels, loc="upper center", ncol=len(labels), frameon=False, fontsize=8, bbox_to_anchor=(0.5, 1.03))
        fig.suptitle(f"Bootstrap: {ylabel.lower()} over time", y=1.07, fontsize=11)
        save(fig, out_dir, f"{name}.png")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "out/matrix")
