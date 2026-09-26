use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{Router, extract::State, http::StatusCode, routing::get, serve::ListenerExt};
use breezydb::{Append, Writer, spawn};
use tokio::sync::Semaphore;

/// Fixed expiry stamped on every generated entry.
const EXP: &str = "2026-12-12T00:00:00Z";

/// `/frame` writes between 1 and `MAX_ENTRIES` entries.
const MAX_ENTRIES: usize = 10;

/// Rough per-entry memory budget for the inflight guard.
const ENTRY_BYTES: u32 = 96;

#[tokio::main]
async fn main() -> io::Result<()> {
    let (w, h) = spawn("data/test.db").await?;

    let state = AppState {
        w,
        inflight: Arc::new(Semaphore::new(256 << 20)),
    };

    let app = Router::new()
        .route("/frame", get(frame_handler))
        .route("/noop", get(noop_handler))
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
    inflight: Arc<Semaphore>,
}

async fn frame_handler(
    State(s): State<AppState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    let _permit = match s
        .inflight
        .try_acquire_many_owned(ENTRY_BYTES * MAX_ENTRIES as u32)
    {
        Ok(permit) => permit,
        Err(_) => {
            return Err((
                StatusCode::INSUFFICIENT_STORAGE,
                "To many requests inflight, no memory left".to_string(),
            ));
        }
    };

    let entries = rand::random_range(1..=MAX_ENTRIES);
    let rows: Vec<Append> = (0..entries)
        .map(|_| {
            let token = rand::random::<u128>();
            Append {
                schema: 1,
                data: format!("{{\"token\":\"{token:032x}\",\"exp\":\"{EXP}\"}}").into_bytes(),
            }
        })
        .collect();

    s.w.append_many(rows)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((StatusCode::CREATED, format!("wrote {entries} entries")))
}

/// Same request path, same response shape, no storage. The difference between
/// this route's client-side average and `/frame`'s is what durability costs;
/// this route's own client-side average is everything else.
async fn noop_handler(State(_): State<AppState>) -> (StatusCode, String) {
    (StatusCode::CREATED, "OK".to_string())
}
