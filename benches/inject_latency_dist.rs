//! Latency *distribution* for cross-runtime scheduling, printed as percentiles.
//!
//! Companion to `inject_latency.rs` (which reports criterion means). The mean
//! hides the interesting part: a task in the injector queue waits until a worker
//! next checks it, so the cost shows up in the tail. This harness records every
//! sample and prints p50/p90/p99/max across several runtime load regimes.
//!
//! Regimes (all spawn+await from the block_on thread, i.e. through the injector):
//!   * idle      — workers parked; latency is the cross-thread wake cost.
//!   * saturated — every worker stuck in a long (~100us) poll; latency floored
//!                 by poll granularity (no spare worker, no preemption).
//!   * loaded    — workers busy with many *short* tasks; local queues never
//!                 empty, so the injector is only drained on the periodic
//!                 `global_queue_interval` tick. This is where lowering the
//!                 interval (or a per-worker inbox) can actually help.
//!
//! `harness = false`: plain binary. Run with:
//!     cargo bench --bench inject_latency_dist
//!
//! NOTE: absolute numbers are platform-dependent; run on Linux for figures
//! comparable to a production runtime. The shape is the point.

use std::hint::black_box;
use std::time::{Duration, Instant};

use tokio::runtime::{self, Runtime};

const WORKERS: usize = 4;
const WARMUP: usize = 10_000;
const SAMPLES: usize = 100_000;
/// Long poll duration for the `saturated` regime.
const BUSY_POLL: Duration = Duration::from_micros(100);

#[derive(Clone, Copy)]
enum Load {
    Idle,
    Saturated,
    Loaded,
}

fn rt(global_queue_interval: Option<u32>) -> Runtime {
    let mut b = runtime::Builder::new_multi_thread();
    b.worker_threads(WORKERS).enable_all();
    if let Some(n) = global_queue_interval {
        b.global_queue_interval(n);
    }
    b.build().unwrap()
}

fn cpu_burst(iters: u64) {
    let mut x = 0u64;
    for i in 0..iters {
        x = x.wrapping_add(i).wrapping_mul(2654435761);
    }
    black_box(x);
}

fn report(name: &str, mut lat: Vec<u64>) {
    lat.sort_unstable();
    let at = |q: f64| lat[(((lat.len() - 1) as f64) * q) as usize] as f64 / 1000.0;
    println!(
        "{name:<22} p50={:>8.2}us  p90={:>8.2}us  p99={:>9.2}us  max={:>9.2}us",
        at(0.50),
        at(0.90),
        at(0.99),
        *lat.last().unwrap() as f64 / 1000.0,
    );
}

/// Baseline: spawn+await from a task already running on a worker (local queue).
fn on_worker() {
    let rt = rt(None);
    let lat = rt.block_on(async {
        tokio::spawn(async {
            for _ in 0..WARMUP {
                tokio::spawn(async {}).await.unwrap();
            }
            let mut lat = Vec::with_capacity(SAMPLES);
            for _ in 0..SAMPLES {
                let s = Instant::now();
                tokio::spawn(async {}).await.unwrap();
                lat.push(s.elapsed().as_nanos() as u64);
            }
            lat
        })
        .await
        .unwrap()
    });
    report("on_worker", lat);
}

/// Spawn+await from the block_on thread (not a worker) → through the injector.
fn off_runtime(load: Load, gqi: Option<u32>) {
    let rt = rt(gqi);
    match load {
        Load::Idle => {}
        Load::Saturated => {
            // One long-polling task per worker: each worker is stuck in a
            // ~BUSY_POLL poll, leaving no spare capacity.
            for _ in 0..WORKERS {
                rt.spawn(async {
                    loop {
                        let start = Instant::now();
                        while start.elapsed() < BUSY_POLL {
                            cpu_burst(256);
                        }
                        tokio::task::yield_now().await;
                    }
                });
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Load::Loaded => {
            // Many short tasks: local queues stay non-empty with sub-us polls,
            // so workers cycle quickly but only drain the injector on interval
            // ticks.
            for _ in 0..WORKERS * 4 {
                rt.spawn(async {
                    loop {
                        cpu_burst(400);
                        tokio::task::yield_now().await;
                    }
                });
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }

    for _ in 0..WARMUP {
        rt.block_on(async {
            tokio::spawn(async {}).await.unwrap();
        });
    }
    let mut lat = Vec::with_capacity(SAMPLES);
    for _ in 0..SAMPLES {
        let s = Instant::now();
        rt.block_on(async {
            tokio::spawn(async {}).await.unwrap();
        });
        lat.push(s.elapsed().as_nanos() as u64);
    }

    let load = match load {
        Load::Idle => "idle",
        Load::Saturated => "saturated",
        Load::Loaded => "loaded",
    };
    let gqi = match gqi {
        Some(n) => format!(" gqi={n}"),
        None => String::new(),
    };
    report(&format!("off_runtime {load}{gqi}"), lat);
}

fn main() {
    on_worker();
    off_runtime(Load::Idle, None);
    off_runtime(Load::Saturated, None);
    off_runtime(Load::Saturated, Some(1));
    off_runtime(Load::Loaded, None);
    off_runtime(Load::Loaded, Some(1));
}
