//! Latency *distribution* for cross-runtime scheduling, printed as percentiles.
//!
//! Companion to `inject_latency.rs` (which reports criterion means). The mean
//! hides the interesting part: when the runtime is busy, a task sitting in the
//! injector queue waits until a worker next checks it (every `global_queue_interval`
//! ticks), so the cost shows up in the tail. This harness records every sample
//! and prints p50/p90/p99/max.
//!
//! `harness = false`: this is a plain binary. Run with:
//!     cargo bench --bench inject_latency_dist
//!
//! NOTE: absolute numbers are platform-dependent; run on Linux for figures
//! comparable to a production runtime. The shape (local vs injector, idle vs
//! busy) is the point.

use std::hint::black_box;
use std::time::{Duration, Instant};

use tokio::runtime::{self, Runtime};

const WORKERS: usize = 4;
const WARMUP: usize = 10_000;
const SAMPLES: usize = 100_000;
/// How long each background task polls before yielding, when the runtime is
/// "busy". Longer polls mean workers check the injector less often.
const BUSY_POLL: Duration = Duration::from_micros(100);

fn rt(global_queue_interval: Option<u32>) -> Runtime {
    let mut b = runtime::Builder::new_multi_thread();
    b.worker_threads(WORKERS).enable_all();
    if let Some(n) = global_queue_interval {
        b.global_queue_interval(n);
    }
    b.build().unwrap()
}

fn report(name: &str, mut lat: Vec<u64>) {
    lat.sort_unstable();
    let at = |q: f64| lat[(((lat.len() - 1) as f64) * q) as usize] as f64 / 1000.0;
    println!(
        "{name:<18} p50={:>8.2}us  p90={:>8.2}us  p99={:>9.2}us  max={:>9.2}us",
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

/// Spawn+await from the block_on thread (not a worker), so the task goes through
/// the injector queue. `busy` saturates every worker with a long-polling loop.
fn off_runtime(busy: bool, gqi: Option<u32>) {
    let rt = rt(gqi);
    if busy {
        for _ in 0..WORKERS {
            rt.spawn(async {
                loop {
                    let start = Instant::now();
                    let mut x = 0u64;
                    while start.elapsed() < BUSY_POLL {
                        x = x.wrapping_add(1).wrapping_mul(2654435761);
                        black_box(x);
                    }
                    tokio::task::yield_now().await;
                }
            });
        }
        std::thread::sleep(Duration::from_millis(100));
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
    let label = match (busy, gqi) {
        (false, None) => "off_idle",
        (true, None) => "off_busy",
        (false, Some(_)) => "off_idle gqi=1",
        (true, Some(_)) => "off_busy gqi=1",
    };
    report(label, lat);
}

fn main() {
    on_worker();
    off_runtime(false, None);
    off_runtime(true, None);
    off_runtime(false, Some(1));
    off_runtime(true, Some(1));
}
