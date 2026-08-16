use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use axum::{
    Router, extract::RawQuery, extract::State, http::StatusCode, routing::get, serve::ListenerExt,
};
use breezydb::storage::frame::FRAME_MAX_SIZE;
use breezydb::{FileStorage, Writer, spawn};

/// Smallest payload the load generators produce. Matches `/frame`.
const MIN_PAYLOAD: usize = 24;

#[tokio::main]
async fn main() -> io::Result<()> {
    let storage = FileStorage::open("data/test.breezy")?;
    let (w, _, h) = spawn(storage);

    let state = AppState {
        w,
        frame: Metrics::default(),
        noop: Metrics::default(),
        bench: Arc::new(tokio::sync::Mutex::new(())),
    };

    tokio::spawn(report(state.frame.clone(), state.noop.clone()));

    let app = Router::new()
        .route("/frame", get(frame_handler))
        .route("/noop", get(noop_handler))
        .route("/bench", get(bench_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("listening on {addr}");

    // Responses here are a few dozen bytes. Without this, Nagle holds them back
    // waiting for a full segment while the peer's delayed ACK waits for data,
    // and the pair stalls until the ~40ms ACK timer fires.
    let listener = tokio::net::TcpListener::bind(addr).await?.tap_io(|tcp| {
        if let Err(e) = tcp.set_nodelay(true) {
            eprintln!("failed to set TCP_NODELAY: {e}");
        }
    });
    axum::serve(listener, app).await.unwrap();

    h.close().await?;

    Ok(())
}

#[derive(Clone)]
struct AppState {
    w: Writer,
    frame: Metrics,
    noop: Metrics,
    /// Held for the length of a `/bench` run. Two concurrent runs would each
    /// measure the other's load.
    bench: Arc<tokio::sync::Mutex<()>>,
}

/// Server-side service time, sampled inside the handler.
///
/// This is the number to hold against the client's average. Whatever gap
/// remains is not in this process: it is the network, the client, or the time
/// requests spend queued in the accept backlog before a handler ever runs.
#[derive(Clone, Default)]
struct Metrics {
    count: Arc<AtomicU64>,
    nanos: Arc<AtomicU64>,
    max_nanos: Arc<AtomicU64>,
}

impl Metrics {
    fn record(&self, d: Duration) {
        let n = d.as_nanos() as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.nanos.fetch_add(n, Ordering::Relaxed);
        self.max_nanos.fetch_max(n, Ordering::Relaxed);
    }

    /// Reads and resets, so each report covers only its own window.
    fn take(&self) -> (u64, Duration, Duration) {
        let count = self.count.swap(0, Ordering::Relaxed);
        let nanos = self.nanos.swap(0, Ordering::Relaxed);
        let max = self.max_nanos.swap(0, Ordering::Relaxed);
        let mean = if count == 0 { 0 } else { nanos / count };
        (count, Duration::from_nanos(mean), Duration::from_nanos(max))
    }
}

async fn report(frame: Metrics, noop: Metrics) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    // The first tick fires immediately, over a window of roughly zero, which
    // reads as a throughput figure about half of what it should be.
    tick.tick().await;
    loop {
        tick.tick().await;
        for (name, m) in [("frame", &frame), ("noop", &noop)] {
            let (count, mean, max) = m.take();
            if count > 0 {
                println!("{name}: {count}/s, mean {mean:.3?}, max {max:.3?}");
            }
        }
    }
}

async fn frame_handler(
    State(s): State<AppState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    let start = Instant::now();

    let size = rand::random_range(MIN_PAYLOAD..4 * 1024);

    let mut data = vec![0u8; size];
    rand::fill(&mut data);

    let res = s.w.append(data).await.map_err(|e| {
        println!("{e}");
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    });

    s.frame.record(start.elapsed());
    res?;

    Ok((StatusCode::CREATED, format!("wrote {} bytes", size,)))
}

/// Same request path, same response shape, no storage. The difference between
/// this route's client-side average and `/frame`'s is what durability costs;
/// this route's own client-side average is everything else.
async fn noop_handler(State(s): State<AppState>) -> (StatusCode, String) {
    let start = Instant::now();
    let size = rand::random_range(MIN_PAYLOAD..4 * 1024);
    s.noop.record(start.elapsed());
    (StatusCode::CREATED, format!("wrote {} bytes", size,))
}

/// Load-generate against [`Writer::append`] from inside the process.
///
/// Exists because every HTTP measurement of this server has been capped by
/// something other than storage — 16k req/s over WiFi, ~60k over loopback, both
/// identical whether the handler wrote anything or not. This route takes hyper,
/// the socket, and the load generator out of the measured path entirely, so the
/// numbers it reports are the engine's own.
///
/// `GET /bench?tasks=500&secs=5&size=4096&rate=0&burst=1&stall=0`
///
/// Without `rate` this is a closed loop: every worker issues its next append
/// the instant the last one is durable, so the writer is saturated end to end
/// and never idles. That shape can only ever show the *cost* of a group-commit
/// linger, never its benefit. `rate` and `burst` open the loop — `rate` caps
/// total appends/sec so gaps appear between batches, and `burst` makes each
/// worker fire that many appends simultaneously.
///
/// `stall` is the shape that actually tests a group-commit linger: each slot
/// fires **one** append, waits `stall` microseconds, then fires the burst. The
/// lone append hits an idle writer and claims a whole `fsync` for itself, so
/// the burst arrives mid-commit and — with no linger — pays a second one. Set
/// `stall` below a commit's duration or the scout finishes first and the shape
/// degenerates back into two independent batches. Scout and burst latencies are
/// reported separately, since a linger trades the former for the latter.
///
/// Watch p99/p999, not mean throughput.
///
/// One run at a time, and it writes real frames — expect the log to grow by
/// roughly `throughput x duration`, which is gigabytes at these rates.
async fn bench_handler(
    State(s): State<AppState>,
    RawQuery(q): RawQuery,
) -> Result<String, (StatusCode, String)> {
    let p = BenchParams::parse(q.as_deref());

    let _guard = Arc::clone(&s.bench)
        .try_lock_owned()
        .map_err(|_| (StatusCode::CONFLICT, "a bench run is already in progress\n".into()))?;

    let dur = Duration::from_secs(p.secs);
    let started = Instant::now();

    let tasks: Vec<_> = (0..p.tasks)
        .map(|i| {
            let w = s.w.clone();
            let max = p.max_size;
            let (rate, burst, fleet, stall) = (p.rate, p.burst, p.tasks, p.stall);
            tokio::spawn(async move {
                // Filled once. Cloning a prefix per append is the same
                // alloc-and-copy the real handler does, minus the CSPRNG, which
                // would otherwise burn CPU the engine needs.
                let mut src = vec![0u8; max];
                rand::fill(&mut src[..]);

                let mut r = Run::default();
                let start = Instant::now();
                let deadline = start + dur;

                if rate == 0 {
                    while Instant::now() < deadline {
                        let size = rand::random_range(MIN_PAYLOAD..max);
                        let data = src[..size].to_vec();

                        let t = Instant::now();
                        match w.append(data).await {
                            Ok(()) => {
                                r.samples.push(t.elapsed().as_nanos() as u64);
                                r.bytes += size as u64;
                            }
                            Err(_) => r.errors += 1,
                        }
                    }
                    return r;
                }

                // Each worker owns `burst` appends every `period`, and the
                // fleet's slots are spread across one period so the whole set
                // does not fire in lockstep. Waves come from `burst`, not from
                // every worker sharing a phase.
                let per_slot = burst + if stall.is_zero() { 0 } else { 1 };
                let period = Duration::from_secs_f64(fleet as f64 * per_slot as f64 / rate as f64);
                let mut next = start + period.mul_f64(i as f64 / fleet as f64);

                while Instant::now() < deadline {
                    let now = Instant::now();
                    if next > now {
                        tokio::time::sleep(next - now).await;
                    } else if now - next > period {
                        // The engine could not keep up with the asked-for rate,
                        // so this slot is already stale. Reported, not hidden:
                        // a missed slot means the run is really a closed loop.
                        r.late += 1;
                    }
                    next += period;
                    if Instant::now() >= deadline {
                        break;
                    }

                    let mut fire = || {
                        let w = w.clone();
                        let size = rand::random_range(MIN_PAYLOAD..max);
                        let data = src[..size].to_vec();
                        tokio::spawn(async move {
                            let t = Instant::now();
                            let ok = w.append(data).await.is_ok();
                            (t.elapsed().as_nanos() as u64, size as u64, ok)
                        })
                    };

                    // The scout: one lone append against an idle writer, which
                    // claims a whole commit slot for itself. Deliberately not
                    // awaited — its `fsync` has to still be running when the
                    // burst lands, or there is nothing for a linger to fix.
                    let scout = (!stall.is_zero()).then(&mut fire);
                    if scout.is_some() {
                        tokio::time::sleep(stall).await;
                    }

                    // Fired concurrently. Issuing them in sequence would just
                    // be a faster closed loop — each append would wait for the
                    // previous commit, and nothing would ever arrive together.
                    let mut fired = Vec::with_capacity(burst);
                    for _ in 0..burst {
                        fired.push(fire());
                    }

                    if let Some(h) = scout {
                        match h.await {
                            Ok((ns, sz, true)) => {
                                r.scouts.push(ns);
                                r.bytes += sz;
                            }
                            _ => r.errors += 1,
                        }
                    }

                    for h in fired {
                        match h.await {
                            Ok((ns, sz, true)) => {
                                r.samples.push(ns);
                                r.bytes += sz;
                            }
                            _ => r.errors += 1,
                        }
                    }
                }

                r
            })
        })
        .collect();

    let mut all = Run::default();
    for t in tasks {
        let r = t
            .await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("bench task: {e}\n")))?;
        all.samples.extend_from_slice(&r.samples);
        all.scouts.extend_from_slice(&r.scouts);
        all.bytes += r.bytes;
        all.errors += r.errors;
        all.late += r.late;
    }

    let elapsed = started.elapsed();
    all.samples.sort_unstable();
    all.scouts.sort_unstable();

    Ok(render(&p, elapsed, &all))
}

/// What one worker (or the whole fleet, once merged) got through.
#[derive(Default)]
struct Run {
    samples: Vec<u64>,
    /// Lone appends that opened a commit ahead of a burst, when `stall` is set.
    /// Kept apart because a linger is supposed to make these *worse* — they pay
    /// the wait — while making everything behind them better.
    scouts: Vec<u64>,
    bytes: u64,
    errors: u64,
    /// Slots that came due before the worker got back to them.
    late: u64,
}

struct BenchParams {
    tasks: usize,
    secs: u64,
    max_size: usize,
    /// Target appends/sec across the whole fleet. `0` is a closed loop.
    rate: u64,
    /// Appends a worker fires simultaneously when its slot comes up.
    burst: usize,
    /// Gap between the lone opening append and the burst behind it. `0`
    /// disables the scout and fires the burst on its own. Set this shorter
    /// than a commit — the point is for the scout's `fsync` to still be
    /// running when the burst lands.
    stall: Duration,
}

impl BenchParams {
    /// Hand-rolled because pulling in `serde` for three integers is not worth a
    /// dependency. Unknown keys and unparseable values fall back to defaults.
    fn parse(q: Option<&str>) -> Self {
        let mut p = Self {
            tasks: 500,
            secs: 5,
            max_size: 4 * 1024,
            rate: 0,
            burst: 1,
            stall: Duration::ZERO,
        };

        for pair in q.unwrap_or_default().split('&').filter(|s| !s.is_empty()) {
            let Some((k, v)) = pair.split_once('=') else {
                continue;
            };
            match k {
                "tasks" => {
                    if let Ok(n) = v.parse() {
                        p.tasks = n;
                    }
                }
                "secs" => {
                    if let Ok(n) = v.parse() {
                        p.secs = n;
                    }
                }
                "size" => {
                    if let Ok(n) = v.parse() {
                        p.max_size = n;
                    }
                }
                "rate" => {
                    if let Ok(n) = v.parse() {
                        p.rate = n;
                    }
                }
                "burst" => {
                    if let Ok(n) = v.parse() {
                        p.burst = n;
                    }
                }
                // Microseconds — a commit is single-digit milliseconds, so this
                // wants sub-millisecond resolution to sit inside one.
                "stall" => {
                    if let Ok(n) = v.parse::<u64>() {
                        p.stall = Duration::from_micros(n.min(10_000));
                    }
                }
                _ => {}
            }
        }

        p.tasks = p.tasks.clamp(1, 4096);
        p.secs = p.secs.clamp(1, 60);
        p.max_size = p.max_size.clamp(MIN_PAYLOAD + 1, FRAME_MAX_SIZE as usize);
        p.burst = p.burst.clamp(1, 1024);
        p
    }
}

/// Nearest-rank percentile over an already-sorted slice.
fn pct(sorted: &[u64], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let i = (((sorted.len() - 1) as f64) * p).round() as usize;
    Duration::from_nanos(sorted[i])
}

fn render(p: &BenchParams, elapsed: Duration, r: &Run) -> String {
    let lat = &r.samples;
    let secs = elapsed.as_secs_f64();
    let count = lat.len() as u64;
    let mean = if count == 0 {
        Duration::ZERO
    } else {
        Duration::from_nanos(lat.iter().sum::<u64>() / count)
    };

    let load = if p.rate == 0 {
        "closed loop (saturating)".to_string()
    } else if p.stall.is_zero() {
        format!(
            "{}/s target, burst {}, {} slots missed",
            p.rate, p.burst, r.late
        )
    } else {
        format!(
            "{}/s target, 1 scout + {:.0?} stall + burst {}, {} slots missed",
            p.rate, p.stall, p.burst, r.late
        )
    };

    // The trade a linger makes is visible only when these two are split: the
    // scout pays the wait, everything behind it skips a whole commit.
    let scouts = if r.scouts.is_empty() {
        String::new()
    } else {
        let n = r.scouts.len() as u64;
        format!(
            "
scout latency (lone append that opens the commit)
  mean      {:.3?}
  p50       {:.3?}
  p99       {:.3?}
  count     {n}
",
            Duration::from_nanos(r.scouts.iter().sum::<u64>() / n),
            pct(&r.scouts, 0.50),
            pct(&r.scouts, 0.99),
        )
    };

    format!(
        "breezydb direct write bench
  tasks         {tasks}
  duration      {secs:.2}s
  payload       {min}..{max} bytes
  load          {load}

throughput
  {rps:.0} req/s
  {mbs:.1} MB/s durable
  {count} appends, {errors} errors
  {gb:.3} GB written

append latency
  mean      {mean:.3?}
  p50       {p50:.3?}
  p90       {p90:.3?}
  p99       {p99:.3?}
  p999      {p999:.3?}
  max       {max_lat:.3?}
{scouts}",
        tasks = p.tasks,
        min = MIN_PAYLOAD,
        max = p.max_size,
        errors = r.errors,
        rps = count as f64 / secs,
        mbs = r.bytes as f64 / secs / (1024.0 * 1024.0),
        gb = r.bytes as f64 / (1024.0 * 1024.0 * 1024.0),
        p50 = pct(lat, 0.50),
        p90 = pct(lat, 0.90),
        p99 = pct(lat, 0.99),
        p999 = pct(lat, 0.999),
        max_lat = pct(lat, 1.0),
    )
}
