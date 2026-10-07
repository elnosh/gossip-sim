# gossip-sim

A discrete-event simulator for Lightning gossip sync. It runs a real LDK node (`PeerManager`,
`P2PGossipSync`, `NetworkGraph`) in-process against simulated LND, CLN, Eclair and LDK peers, and
measures how fast, and at what bandwidth cost, different graph-sync strategies bring that node's
graph up to date.

Everything runs on a simulated clock: there are no real sockets and no waiting, so a run over a
mainnet-sized graph takes seconds, and the same inputs always produce the same outputs.

## How it works

1. **Corpus.** A graph dump (`describegraph` JSON) is imported once: every channel and
   node gets fresh keys, every announcement and update is re-signed, and the result is cached as a
   binary `.corpus` file next to the input.
2. **Peer views.** Each simulated peer holds its own view of the corpus, derived from its
   implementation's rules (what it keeps, what it prunes) plus optional structured `drops`
   (a fraction of recent announcements, channels near the staleness edge, or random channels).
   The union of all peer views is the ground truth the LDK node is measured against.
3. **Responders.** A peer profile models how that implementation answers `query_channel_range`,
   `query_short_channel_ids` and `gossip_timestamp_filter` (chunk sizes, TLVs, checksums, rate
   limits, which backlog it replays), and which filter it sends us. Constants live in
   `profiles/*.toml`, each with the source file and line it was taken from.
4. **The LDK node** is unmodified LDK apart from the clock hooks. Its encrypted connections run
   over simulated links with latency and bandwidth. The engine mirrors the background processor:
   timer ticks every 10 s, graph pruning 60 s after start and then hourly.
5. **Strategies** decide what the LDK node asks of which peer: which filters to send and whether to
   use range and SCID queries.
6. **Scenarios.** `bootstrap` starts from an empty graph. `restart` starts from a persisted graph
   as it would have been after being offline for a given duration.
7. **Metrics.** Completeness (channels, routable channels, fresh updates, node announcements) and
   wire bytes per message type in each direction are sampled over time. A run ends when it
   converges, goes quiet, or hits its time limit.

## Layout

| Path | What it is |
|---|---|
| `corpus/` | graph import, re-signing, binary cache, per-peer views, ground truth, mock graphs |
| `sim/` | engine, fake socket, links, simulated peers, responder models, strategies, metrics, runner |
| `profiles/*.toml` | responder constants per implementation |
| `experiments/*.toml` | experiment definitions |
| `analysis/*.py` | plots and summary tables (pandas, matplotlib; see `analysis/requirements.txt`) |

LDK comes from a fork, [`elnosh/rust-lightning`, branch `gossip-sim`](https://github.com/elnosh/rust-lightning/tree/gossip-sim),
used as a git dependency. It adds:

1. `--cfg sim_clock` hooks: a thread-local unix clock replacing every `SystemTime::now()` in the
   gossip sync path, a switch to skip gossip signature checks, a public Noise encryptor, and a
   deterministic hasher. No effect on normal builds.
2. BOLT 7 gossip query TLVs: `query_option`, reply `timestamps`/`checksums`, `query_flags`.
3. `P2PGossipSync::with_query_sync` (`routing/query_sync.rs`): the `range_then_scids` strategy
   implemented inside LDK. Opt-in; default behaviour is unchanged.

`.cargo/config.toml` sets `--cfg sim_clock` for the whole workspace.

## Usage

```sh
cargo build --release

# Import a graph (writes graph.json.corpus; `run` and `views` also do this when the cache is missing).
./target/release/gossip-sim import graph.json

# Or generate a mock graph to try things out: <out.json> [nodes] [channels]
./target/release/gossip-sim mock graph.json 500 1500

# Inspect what each peer would hold and the persisted graph sizes, without running.
./target/release/gossip-sim views experiments/smoke.toml

# Run every (strategy, peer set, scenario) combination of an experiment.
./target/release/gossip-sim run experiments/smoke.toml -j 8
./target/release/gossip-sim run experiments/matrix.toml --only bootstrap__mixed__
./target/release/gossip-sim run experiments/matrix.toml --check-determinism
```

`-j` sets the number of threads (default: all cores). `--only` keeps runs whose id contains the
given substring. `--check-determinism` runs the first selected run twice and compares the outputs.
`GOSSIP_SIM_LDK_LOG=gossip|trace|debug|info` prints the LDK node's log to stderr.

### Outputs

For an experiment with `out_dir = "out/<name>"`:

- `out/<name>/runs/<run_id>/samples.jsonl`: completeness, applied/rejected messages, bytes per
  message type and per-peer queues over time (every second at first, then less often).
- `out/<name>/summary.csv`: one row per run with the end reason, time to converge, final
  completeness, and bytes in total and up to convergence, split by message type.

Byte counts are wire bytes including Noise overhead; `rx` is what the LDK node received, `tx` what
it sent.

### Analysis

```sh
pip install -r analysis/requirements.txt
python analysis/plot_bootstrap.py out/matrix      # out/matrix/plots/*.png
python analysis/plot_restart.py out/matrix
python analysis/summary.py out/matrix --scenario bootstrap
```

## Experiment file

An experiment is the cross product of its strategies, peer sets and scenarios.

```toml
corpus = "graph.json"            # the .corpus cache sits next to it
out_dir = "out/matrix"
seed = 1                         # same seed => identical peer views across strategies
strategies = ["baseline", { name = "range_then_scids", query_peers = 1 }]

[run]                            # all optional
duration_s = 7200
quiesce_s = 300                  # stop once no gossip moved for this long and nothing is queued
converge_routable = 0.99
converge_upds = 0.95
reconnect = { down_s = 60 }      # on convergence, drop every peer and reconnect after down_s

[link]                           # per connection, each direction
latency_ms = 40
bandwidth_mbps = 20.0

[[peer_sets]]
name = "mixed"
peers = [{ profile = "lnd" }, { profile = "cln", count = 2 }, { profile = "eclair",
          drops = [{ kind = "recent_announcements", newer_than_secs = 86400, fraction = 0.5 }] }]

[[scenarios]]
kind = "bootstrap"

[[scenarios]]
kind = "restart"
offline = ["1h", "1d", "15d"]
persisted_updates = "absent"     # or "older_version"
```

Profiles are `lnd`, `cln`, `eclair` and `ldk`. Drop kinds are `recent_announcements`
(`newer_than_secs`, `fraction`), `near_stale_edge` (`within_secs`, `fraction`) and
`random_fraction` (`fraction`).

For `restart`, the persisted graph holds the channels and messages timestamped before shutdown.
`persisted_updates` sets what a direction updated after shutdown looks like: `absent` (missing)
or `older_version` (its current update re-signed with an older timestamp).

### Strategies

| Name | What it does |
|---|---|
| `baseline` | current LDK: a wide `gossip_timestamp_filter` to the first peers, a recent one to the rest |
| `filter_since_last_seen` | the filter starts just before the newest update already in the graph |
| `range_then_scids` | `query_channel_range` with timestamps/checksums, then `query_short_channel_ids` for what is missing or newer, then a recent filter |
| `ldk_query_sync` | the same logic as `range_then_scids`, running inside the fork's `P2PGossipSync` |

A strategy is given by name, or as a table with parameters (number of wide-filter or query peers,
checksums, batch size, timeouts, filter lookback, load balancing, and more). See `StrategyParams` in
`sim/src/strategy.rs`.

## Limitations

- **Peer views come from one dump.** They are derived with rules and structured drops, not
  measured. Peers of the same profile hold identical data unless `drops` are set.
- **Restarts are approximated.** `absent` and `older_version` bracket real behaviour; channels
  closed while offline cannot be represented.
- **No live gossip.** Nothing happens after the dump time, peers do not forward new gossip, and
  peer-initiated queries (CLN's seeker, LND's historical sync) are not modelled.
- **Responder models come from reading source**, not measurement. CPU time and peer-side
  deprioritization are not modelled.

## Tests

```sh
cargo test --release
```

`sim/src/responders/tests.rs` checks each responder rule; the LDK responder model is checked
against the real `P2PGossipSync` replies for the same graph. `sim/tests/engine.rs` runs small
end-to-end scenarios.
