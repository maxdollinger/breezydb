use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::body::Bytes;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    routing::{get, post},
    serve::ListenerExt,
};
use breezydb::{FrameOp, Writer, spawn};
use serde_json::{Value, json};
use tokio::sync::Semaphore;

/// Rough per-op memory budget for the inflight guard.
const OP_BYTES: u32 = 96;

/// Ops assumed inflight per request when reserving memory.
const MAX_OPS: usize = 10;

#[tokio::main]
async fn main() -> io::Result<()> {
    let db_path = std::env::var("DB").unwrap_or_else(|_| "data/test.db".to_string());
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(3000);

    let (w, h) = spawn(&db_path).await?;

    let state = AppState {
        w,
        inflight: Arc::new(Semaphore::new(256 << 20)),
    };

    let app = Router::new()
        .route("/tx", post(tx_handler))
        .route("/noop", get(noop_handler))
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], port));
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

/// One request === one frame === one transaction. The body is either a bare
/// array of ops or `{ "ops": [...] }`; see [`FrameOp`] for the op shapes.
async fn tx_handler(State(s): State<AppState>, body: Bytes) -> (StatusCode, Json<Value>) {
    let _permit = match s.inflight.try_acquire_many_owned(OP_BYTES * MAX_OPS as u32) {
        Ok(permit) => permit,
        Err(_) => {
            return (
                StatusCode::INSUFFICIENT_STORAGE,
                Json(json!({ "success": false, "error": "too many requests inflight, no memory left" })),
            );
        }
    };

    let parsed: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(_) => return bad_request("invalid JSON body"),
    };

    let ops_value = if parsed.is_array() {
        parsed
    } else {
        match parsed.get("ops") {
            Some(v) if v.is_array() => v.clone(),
            _ => return bad_request("expected an array of ops, or { ops: [...] }"),
        }
    };

    let ops: Vec<FrameOp> = match serde_json::from_value(ops_value) {
        Ok(ops) => ops,
        Err(e) => return bad_request(format!("invalid ops: {e}")),
    };

    match s.w.write_frame(ops).await {
        Ok(result) => (
            StatusCode::OK,
            Json(json!({
                "success": true,
                "tx": result.tx,
                "results": result.results,
                "bindings": result.bindings,
            })),
        ),
        Err(e) => (
            StatusCode::CONFLICT,
            Json(json!({ "success": false, "error": e.to_string() })),
        ),
    }
}

/// Same request path, same response shape, no storage. The difference between
/// this route's client-side average and `/tx`'s is what durability costs; this
/// route's own client-side average is everything else.
async fn noop_handler(State(_): State<AppState>) -> (StatusCode, String) {
    (StatusCode::CREATED, "OK".to_string())
}

fn bad_request(msg: impl Into<String>) -> (StatusCode, Json<Value>) {
    (
        StatusCode::BAD_REQUEST,
        Json(json!({ "success": false, "error": msg.into() })),
    )
}
