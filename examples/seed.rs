//! Seed concepts and attributes out of band.
//!
//! The write API only writes entities and facts; concepts and attributes are
//! provisioned separately. Run this against the same file the server uses,
//! before starting the server:
//!
//! ```sh
//! cargo run --release --example seed -- /tmp/breezy-load.db
//! ```

use breezydb::storage::DB_SCHEMA;
use rusqlite::Connection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "data/test.db".to_string());

    let conn = Connection::open(&path)?;
    conn.execute_batch(DB_SCHEMA)?;
    conn.execute_batch(
        "INSERT OR IGNORE INTO concept (id, ident) VALUES (1, 'node');
         INSERT OR IGNORE INTO concept_ancestor (concept, ancestor) VALUES (1, 1);
         INSERT OR IGNORE INTO attr (id, ident, domain, value_type, range, card_one, is_unique, is_identity)
           VALUES (2, 'node/name', 1, 'string', NULL, 1, 0, 0);",
    )?;

    println!("seeded {path}");
    Ok(())
}