//! End-to-end tests for the EAV write path: transactions, per-transaction rollback, and
//! group-commit isolation.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};

use breezydb::{TransactionOp, Writer, spawn};
use rusqlite::Connection;
use serde_json::{Value, json};

static COUNTER: AtomicU32 = AtomicU32::new(0);

fn scratch() -> PathBuf {
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("breezydb-test-{}-{n}.db", std::process::id()))
}

/// Deserialize a JSON array of ops, as the HTTP layer does.
fn ops(v: Value) -> Vec<TransactionOp> {
    serde_json::from_value(v).expect("valid ops")
}

/// Seed a concept and two attributes the way an operator would out of band.
async fn seed(path: &Path) {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch(
        "INSERT INTO concept (id, ident) VALUES (1, 'node');
         INSERT INTO concept_ancestor (concept, ancestor) VALUES (1, 1);
         INSERT INTO attr (id, ident, domain, value_type, range, card_one, is_unique, is_identity)
           VALUES (2, 'name', 1, 'string', NULL, 1, 0, 0),
                  (3, 'ref',  1, 'ref',     1,    1, 1, 1);",
    )
    .unwrap();
}

#[tokio::test]
async fn transaction_roundtrip_and_rollback() {
    let path = scratch();
    let (w, h) = spawn(&path).await.unwrap();
    seed(&path).await;

    // entity + add in one transaction.
    let r = w
        .write_transaction(ops(json!([
            { "op": "entity", "uuid": "e1", "as": "$e" },
            { "op": "add", "e": "$e", "a": 2, "v": "Ada" }
        ])))
        .await
        .unwrap();
    assert_eq!(r.tx, 1, "first transaction opens tx 1");
    assert_eq!(r.bindings["$e"], 1, "first entity gets id 1");
    assert_eq!(r.results[1]["op"], "add");

    // A transaction that fails mid-way must leave no trace.
    let err = w
        .write_transaction(ops(json!([
            { "op": "entity", "uuid": "e2", "as": "$e" },
            { "op": "add", "e": "$e", "a": 999, "v": "x" }
        ])))
        .await
        .unwrap_err();
    assert!(err.to_string().contains("unknown attribute"), "got: {err}");

    // Rolled-back entity id must be reused, proving the insert was reverted.
    let r = w
        .write_transaction(ops(json!([{ "op": "entity", "uuid": "e3", "as": "$e" }])))
        .await
        .unwrap();
    assert_eq!(r.bindings["$e"], 2, "rolled-back entity id must be reused");

    // Lookup ref resolves through the identity attribute.
    let r = w
        .write_transaction(ops(json!([
            { "op": "add", "e": 1, "a": 3, "v": 7 },
            { "op": "resolve", "a": 3, "v": 7, "as": "$r" }
        ])))
        .await
        .unwrap();
    assert_eq!(r.bindings["$r"], 1);

    drop(w);
    h.close().await.unwrap();
    let _ = std::fs::remove_file(&path);
}

/// A failing transaction must not poison other transactions sharing its group commit.
#[tokio::test]
async fn group_commit_isolation() {
    let path = scratch();
    let (w, h) = spawn(&path).await.unwrap();
    seed(&path).await;

    let good = {
        let w: Writer = w.clone();
        async move {
            w.write_transaction(ops(json!([
                { "op": "entity", "uuid": "good", "as": "$e" },
                { "op": "add", "e": "$e", "a": 2, "v": "ok" }
            ])))
            .await
        }
    };
    let bad = {
        let w: Writer = w.clone();
        async move {
            w.write_transaction(ops(json!([
                { "op": "entity", "uuid": "bad", "as": "$e" },
                { "op": "add", "e": "$e", "a": 999, "v": "x" }
            ])))
            .await
        }
    };

    let (good, bad) = tokio::join!(good, bad);
    assert!(good.is_ok(), "valid transaction must succeed: {good:?}");
    assert!(bad.is_err(), "invalid transaction must fail");

    // The good transaction's entity survived; nothing from the bad transaction did.
    let conn = Connection::open(&path).unwrap();
    let mut stmt = conn.prepare("SELECT uuid FROM entity ORDER BY id").unwrap();
    let uuids: Vec<String> = stmt
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    drop(stmt);
    assert_eq!(uuids, vec!["good".to_string()]);

    drop((w, conn));
    h.close().await.unwrap();
    let _ = std::fs::remove_file(&path);
}
