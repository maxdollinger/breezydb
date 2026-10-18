//! The write API: a *transaction* is a list of ops applied as one unit.
//!
//! This is a thin, pure layer over [`WriteTx`]: it resolves entity references,
//! tracks `as` bindings, and shapes the per-op results. All persistence lives
//! in [`crate::storage`].

use std::collections::BTreeMap;
use std::io;

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::storage::WriteTx;

/// An entity reference inside a transaction.
///
/// - `number` — a concrete entity id
/// - `"$name"` — an entity bound earlier in this transaction via `as`
/// - `{ a, v }` — a lookup ref, resolved through the identity attribute
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum EntityRef {
    Id(i64),
    Bind(String),
    Lookup { a: i64, v: Value },
}

/// One operation in a transaction. See the README for the op shapes.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op")]
pub enum TransactionOp {
    /// Create an entity; `uuid` is generated when omitted. `as` binds its id.
    #[serde(rename = "entity")]
    Entity {
        #[serde(default)]
        uuid: Option<String>,
        #[serde(default, rename = "as")]
        bind: Option<String>,
    },
    /// Add a fact `(e, a, v)`.
    #[serde(rename = "add")]
    Add { e: EntityRef, a: i64, v: Value },
    /// Close the live value of a cardinality-one attribute.
    #[serde(rename = "close")]
    Close { e: EntityRef, a: i64 },
    /// Invalidate a specific value of a cardinality-many attribute.
    #[serde(rename = "invalidate")]
    Invalidate { e: EntityRef, a: i64, v: Value },
    /// Resolve a lookup ref to an entity id; `as` binds the result.
    #[serde(rename = "resolve")]
    Resolve {
        a: i64,
        v: Value,
        #[serde(default, rename = "as")]
        bind: Option<String>,
    },
}

/// The outcome of one transaction.
#[derive(Debug, Clone, Serialize)]
pub struct TransactionResult {
    /// id of the `tx` row opened for this transaction.
    pub tx: i64,
    /// Per-op results, in request order.
    pub results: Vec<Value>,
    /// Entities bound by `as`, keyed by binding name.
    pub bindings: BTreeMap<String, i64>,
}

/// Execute a list of ops. The caller has already opened the `tx` row, so every
/// op is stamped with `tx.id`; a throw anywhere aborts the whole transaction.
pub(crate) async fn run_transaction(
    tx: &mut WriteTx<'_>,
    ops: &[TransactionOp],
) -> io::Result<TransactionResult> {
    let mut bindings: BTreeMap<String, i64> = BTreeMap::new();
    let mut results: Vec<Value> = Vec::with_capacity(ops.len());

    // Bindings are `$`-prefixed: a plain id is left as-is, a name is looked up.
    let resolve = |bindings: &BTreeMap<String, i64>, r: &EntityRef| -> io::Result<EntityRef> {
        match r {
            EntityRef::Bind(name) => {
                if !name.starts_with('$') {
                    return Err(err(format!("invalid entity ref: {name:?}")));
                }
                match bindings.get(name) {
                    Some(id) => Ok(EntityRef::Id(*id)),
                    None => Err(err(format!("unknown binding: {name}"))),
                }
            }
            other => Ok(other.clone()),
        }
    };

    // Resolve a concrete ref to an entity id (binding and lookup refs included).
    macro_rules! entity_id {
        ($ref:expr) => {{
            let r = resolve(&bindings, &$ref)?;
            match r {
                EntityRef::Id(id) => id,
                EntityRef::Lookup { a, ref v } => match tx.resolve_ref(a, v).await? {
                    Some(id) => id,
                    None => return Err(err(format!("lookup ref not found: a={a}"))),
                },
                EntityRef::Bind(name) => return Err(err(format!("unknown binding: {name}"))),
            }
        }};
    }

    for op in ops {
        match op {
            TransactionOp::Entity { uuid, bind } => {
                let uuid = uuid
                    .clone()
                    .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
                let id = tx.insert_entity(&uuid).await?;
                if let Some(name) = bind {
                    bindings.insert(name.clone(), id);
                }
                results.push(json!({ "op": "entity", "id": id }));
            }

            TransactionOp::Add { e, a, v } => {
                let e = entity_id!(e);
                let card_one = card_one(tx, *a).await?;
                tx.add_fact(e, *a, v, card_one).await?;
                results.push(json!({ "op": "add", "e": e }));
            }

            TransactionOp::Close { e, a } => {
                let e = entity_id!(e);
                let changes = tx.close_value(e, *a).await?;
                results.push(json!({ "op": "close", "e": e, "changes": changes }));
            }

            TransactionOp::Invalidate { e, a, v } => {
                let e = entity_id!(e);
                let changes = tx.invalidate_value(e, *a, v).await?;
                results.push(json!({ "op": "invalidate", "e": e, "changes": changes }));
            }

            TransactionOp::Resolve { a, v, bind } => {
                let e = match tx.resolve_ref(*a, v).await? {
                    Some(id) => id,
                    None => return Err(err(format!("lookup ref not found: a={a}"))),
                };
                if let Some(name) = bind {
                    bindings.insert(name.clone(), e);
                }
                results.push(json!({ "op": "resolve", "e": e }));
            }
        }
    }

    Ok(TransactionResult {
        tx: tx.id,
        results,
        bindings,
    })
}

/// Reads `attr.card_one` so `add` doesn't make callers supply it.
async fn card_one(tx: &mut WriteTx<'_>, a: i64) -> io::Result<i64> {
    match tx.attr_card_one(a).await? {
        Some(card) => Ok(card),
        None => Err(err(format!("unknown attribute: {a}"))),
    }
}

fn err(msg: impl Into<String>) -> io::Error {
    io::Error::other(msg.into())
}
