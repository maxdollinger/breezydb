use std::io;
use std::net::SocketAddr;

use axum::{Router, extract::State, http::StatusCode, routing::get};
use breezydb::{FileStorage, Writer, spawn};

#[tokio::main]
async fn main() -> io::Result<()> {
    let storage = FileStorage::open("data/test.breezy")?;
    let (w, _, h) = spawn(storage);

    let app = Router::new()
        .route("/frame", get(frame_handler))
        .with_state(w);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    println!("listening on {addr}");
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await.unwrap();

    h.close().await?;

    Ok(())
}

async fn frame_handler(
    State(w): State<Writer>,
) -> Result<(StatusCode, String), (StatusCode, String)> {
    let size = rand::random_range(24..4 * 1024);

    let mut data = vec![0u8; size];
    rand::fill(&mut data);

    w.append(data).await.map_err(|e| {
        println!("{e}");
        (StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    })?;

    Ok((StatusCode::CREATED, format!("wrote {} bytes", size,)))
}
