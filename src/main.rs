use std::io;
use std::net::SocketAddr;
use std::time::Instant;

use axum::{Router, extract::State, http::StatusCode, routing::get, serve::ListenerExt};
use breezydb::data::record::Record;
use breezydb::data::transaction::Transaction;
use breezydb::storage::storage::SequenceClock;
use breezydb::{FileStorage, Writer, spawn};

#[tokio::main]
async fn main() -> io::Result<()> {
    let storage = FileStorage::open("data/test.breezy")?;
    let (seq, w, _, h) = spawn(storage);

    let state = AppState { w, seq };

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
    seq: SequenceClock,
}

async fn frame_handler(
    State(s): State<AppState>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    let rec_cnt: usize = rand::random_range(1..20);

    let seq: Vec<u64> = (0..=rec_cnt).into_iter().map(|_| s.seq.get_seq()).collect();

    let mut buf = Vec::with_capacity(64 * 1024);
    let mut len: usize = 0;
    for i in seq.iter() {
        let rec = Record::new(*i, 1, b"Hello from test endpoint.")
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
        buf.resize(len + rec.size(), 0u8);
        len += rec
            .encode(&mut buf[len..])
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    }

    let min_seq = *seq.first().unwrap();
    let max_seq = *seq.last().unwrap();

    s.w.append((min_seq, max_seq), buf)
        .await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((
        StatusCode::CREATED,
        format!("wrote {} bytes with ({min_seq},{max_seq})", len),
    ))
}

/// Same request path, same response shape, no storage. The difference between
/// this route's client-side average and `/frame`'s is what durability costs;
/// this route's own client-side average is everything else.
async fn noop_handler(State(_): State<AppState>) -> (StatusCode, String) {
    (StatusCode::CREATED, "OK".to_string())
}
