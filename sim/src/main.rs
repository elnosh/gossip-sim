//! `gossip-sim` command line: import a graph, run experiments.

use std::path::PathBuf;
use std::process::exit;

fn usage() -> ! {
	eprintln!(
		"usage:\n  gossip-sim import <graph.json>     build and cache the signed corpus\n"
	);
	exit(2)
}

fn main() {
	let args: Vec<String> = std::env::args().skip(1).collect();
	match args.first().map(String::as_str) {
		Some("import") => {
			let Some(path) = args.get(1) else { usage() };
			let path = PathBuf::from(path);
			let started = std::time::Instant::now();
			match corpus::import::import_graph(&path) {
				Ok((c, stats)) => {
					let cache = corpus::import::cache_path(&path);
					c.save(&cache).expect("write corpus cache");
					println!("{stats}");
					println!(
						"dump_time={} max_height={} channels={} updates={} node_anns={}",
						c.meta.dump_time,
						c.meta.max_height,
						c.chans.len(),
						c.update_count(),
						c.nodes.len()
					);
					println!("wrote {} in {:.1}s", cache.display(), started.elapsed().as_secs_f64());
				},
				Err(e) => {
					eprintln!("import failed: {e}");
					exit(1);
				},
			}
		},
		_ => usage(),
	}
}
