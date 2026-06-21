//! Benchmark the *latency* of getting a task scheduled, depending on where the
//! schedule originates.
//!
//! When a task is spawned (or woken) from a runtime worker thread, it lands on
//! that worker's local queue and is picked up almost immediately. When it is
//! spawned/woken from *outside* the runtime — e.g. the `block_on` thread, a
//! `Handle::enter` guard on a foreign thread, or a cross-runtime channel/lock —
//! it goes through the global "injector" queue instead. Workers only drain the
//! injector when their local queue is empty, or once every `global_queue_interval`
//! ticks (see `multi_thread/worker.rs::next_task`), so the schedule-to-run
//! latency is much higher, and worse when the workers are busy.
//!
//! This measures that gap with three scenarios:
//!   * `on_worker`        — spawn+await from a task already on a worker (local queue)
//!   * `off_runtime_idle` — spawn+await from the block_on thread, workers idle
//!   * `off_runtime_busy` — same, but every worker is saturated with local work
//!
//! NOTE: absolute numbers are platform-dependent (park/unpark cost, scheduler
//! timing). The interesting quantity is the *ratio* between `on_worker` and the
//! `off_runtime_*` cases. Run on Linux for figures comparable to a production
//! runtime.

use std::hint::black_box;
use std::time::{Duration, Instant};

use criterion::{criterion_group, criterion_main, Criterion};
use tokio::runtime::{self, Runtime};

/// Worker thread count for the runtime under test. Fixed for reproducibility.
const WORKERS: usize = 4;
/// How long each background task polls before yielding, in the busy scenario.
/// Longer polls mean workers check the injector queue less often.
const BUSY_POLL: Duration = Duration::from_micros(100);

fn rt() -> Runtime {
    runtime::Builder::new_multi_thread()
        .worker_threads(WORKERS)
        .enable_all()
        .build()
        .unwrap()
}

/// A chunk of pure CPU work, used to keep worker threads busy.
fn cpu_burst() {
    let mut x = 0u64;
    for i in 0..20_000u64 {
        x = x.wrapping_add(i).wrapping_mul(2654435761);
    }
    black_box(x);
}

/// Baseline: the spawn+await loop runs *on a worker*, so the spawned task lands
/// on the local queue.
fn on_worker(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("inject_latency/on_worker", |b| {
        b.iter_custom(|iters| {
            rt.block_on(async move {
                // Hop onto a worker once; everything timed below runs there.
                tokio::spawn(async move {
                    let start = Instant::now();
                    for _ in 0..iters {
                        tokio::spawn(async {}).await.unwrap();
                    }
                    start.elapsed()
                })
                .await
                .unwrap()
            })
        });
    });
}

/// The problem case: spawn+await from the `block_on` thread (not a worker), so
/// the task goes through the injector queue. Workers are otherwise idle.
fn off_runtime_idle(c: &mut Criterion) {
    let rt = rt();
    c.bench_function("inject_latency/off_runtime_idle", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                rt.block_on(async {
                    tokio::spawn(async {}).await.unwrap();
                });
            }
            start.elapsed()
        });
    });
}

/// The worst case: same as above, but every worker is saturated with local work
/// (CPU burst + yield), so they only drain the injector on the periodic
/// `global_queue_interval` tick.
fn off_runtime_busy(c: &mut Criterion) {
    let rt = rt();

    // Saturate every worker with a self-rescheduling busy loop that polls for
    // ~BUSY_POLL before yielding, so workers seldom drain the injector.
    for _ in 0..WORKERS {
        rt.spawn(async {
            loop {
                let start = Instant::now();
                while start.elapsed() < BUSY_POLL {
                    cpu_burst();
                }
                tokio::task::yield_now().await;
            }
        });
    }
    // Let the load ramp up before measuring.
    std::thread::sleep(Duration::from_millis(50));

    c.bench_function("inject_latency/off_runtime_busy", |b| {
        b.iter_custom(|iters| {
            let start = Instant::now();
            for _ in 0..iters {
                rt.block_on(async {
                    tokio::spawn(async {}).await.unwrap();
                });
            }
            start.elapsed()
        });
    });
}

criterion_group!(inject_latency, on_worker, off_runtime_idle, off_runtime_busy);
criterion_main!(inject_latency);
