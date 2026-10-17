//! End-to-end tests for the EAV write path: frames, per-frame rollback, and
//! group-commit isolation.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};

use breezydb::{FrameOp, Writer, spawn};
use serde_json::{Value, json};

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn scratch() -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("breezydb-test-{}-{n}.db", std::process::id()))
}

/// Deserialize a JSON array of ops, as the HTTP layer does.
fn ops(v: Value) -> Vec<FrameOp> {
    serde_json::from_value(v).expect("valid ops")
}

/// Seed a concept and two attributes the way an operator would out of band.
async fn seed(path: &PathBuf) {
    let db = libsql::Builder::new_local(path).build().await.unwrap();
    let conn = db.connect().unwrap();
    conn.execute_batch(
        "INSERT INTO concept (id, ident) VALUES (1, 'node');
         INSERT INTO concept_ancestor (concept, ancestor) VALUES (1, 1);
         INSERT INTO attr (id, ident, domain, value_type, range, card_one, is_unique, is_identity)
           VALUES (2, 'name', 1, 'string', NULL, 1, 0, 0),
                  (3, 'ref',  1, 'ref',     1,    1, 1, 1);",
    )
    .await
    .unwrap();
}

#[tokio::test]
async fn frame_roundtrip_and_rollback() {
    let path = scratch();
    let (w, h) = spawn(&path).await.unwrap();
    seed(&path).await;

    // entity + assert in one frame.
    let r = w
        .write_frame(ops(json!([
            { "op": "entity", "uuid": "e1", "as": "$e" },
            { "op": "assert", "e": "$e", "a": 2, "v": "Ada" }
        ])))
        .await
        .unwrap();
    assert_eq!(r.tx, 1, "first frame opens tx 1");
    assert_eq!(r.bindings["$e"], 1, "first entity gets id 1");
    assert_eq!(r.results[1]["op"], "assert");

    // A frame that fails mid-way must leave no trace.
    let err = w
        .write_frame(ops(json!([
            { "op": "entity", "uuid": "e2", "as": "$e" },
            { "op": "assert", "e": "$e", "a": 999, "v": "x" }
        ])))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unknown attribute"), "got: {err}");

    // Rolled-back entity id must be reused, proving the insert was reverted.
    let r = w
        .write_frame(ops(json!([{ "op": "entity", "uuid": "e3", "as": "$e" }])))
        .await
        .unwrap();
    assert_eq!(r.bindings["$e"], 2, "rolled-back entity id must be reused");

    // Lookup ref resolves through the identity attribute.
    let r = w
        .write_frame(ops(json!([
            { "op": "assert", "e": 1, "a": 3, "v": 7 },
            { "op": "resolve", "a": 3, "v": 7, "as": "$r" }
        ])))
        .await
        .unwrap();
    assert_eq!(r.bindings["$r"], 1);

    drop(w);
    h.close().await.unwrap();
    let _ = std::fs::remove_file(&path);
}

/// A failing frame must not poison other frames sharing its group commit.
#[tokio::test]
async fn group_commit_isolation() {
    let path = scratch();
    let (w, h) = spawn(&path).await.unwrap();
    seed(&path).await;

    let good = {
        let w: Writer = w.clone();
        async move {
            w.write_frame(ops(json!([
                { "op": "entity", "uuid": "good", "as": "$e" },
                { "op": "assert", "e": "$e", "a": 2, "v": "ok" }
            ])))
            .await
        }
    };
    let bad = {
        let w: Writer = w.clone();
        async move {
            w.write_frame(ops(json!([
                { "op": "entity", "uuid": "bad", "as": "$e" },
                { "op": "assert", "e": "$e", "a": 999, "v": "x" }
            ])))
            .await
        }
    };

    let (good, bad) = tokio::join!(good, bad);
    assert!(good.is_ok(), "valid frame must succeed: {good:?}");
    assert!(bad.is_err(), "invalid frame must fail");

    // The good frame's entity survived; nothing from the bad frame did.
    let db = libsql::Builder::new_local(&path).build().await.unwrap();
    let conn = db.connect().unwrap();
    let mut rows = conn
        .query("SELECT uuid FROM entity ORDER BY id", ())
        .await
        .unwrap();
    let mut uuids = Vec::new();
    while let Some(row) = rows.next().await.unwrap() {
        uuids.push(row.get::<String>(0).unwrap());
    }
    assert_eq!(uuids, vec!["good".to_string()]);

    drop((w, db, conn));
    h.close().await.unwrap();
    let _ = std::fs::remove_file(&path);
}
