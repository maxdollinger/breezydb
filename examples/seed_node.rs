//! Seed the schema used by the random-fan-out write-capacity test.
//!
//! Creates the `node` concept and the attributes `node/1` .. `node/N` with the
//! ids `2 .. N+1`. The load generator maps attribute id `2 + i` to `node/{i}`.
//!
//! ```sh
//! cargo run --release --example seed_node -- /tmp/breezy-load.db [N]
//! ```

use rusqlite::Connection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "data/test.db".to_string());
    let n: i64 = std::env::args()
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);

    let conn = Connection::open(&path)?;

    // `page_size` only takes effect before the file has any pages, so the seeder
    // must set it too — otherwise the server's pragma lands on a non-empty file
    // and is ignored, and every "page size" experiment silently runs at 4096.
    let page_size = std::env::var("BREEZY_PAGE_SIZE").unwrap_or_else(|_| "4096".to_string());
    conn.execute_batch(&format!("PRAGMA page_size={page_size};"))?;

    // Honor the index experiment so the server and the seed agree on the schema.
    let schema = breezydb::storage::schema_for_env();
    conn.execute_batch(&schema)?;

    // Attribute ids are `2..=n+1` so the generator can address `node/{i}` as id
    // `i+1`. All are cardinality-one strings; each request creates a fresh
    // entity, so there is never a live `(e, a)` conflict across requests.
    let mut sql = String::from(
        "INSERT OR IGNORE INTO concept (id, ident) VALUES (1, 'node');
         INSERT OR IGNORE INTO concept_ancestor (concept, ancestor) VALUES (1, 1);",
    );
    for i in 1..=n {
        sql.push_str(&format!(
            "INSERT OR IGNORE INTO attr
               (id, ident, domain, value_type, range, card_one, is_unique, is_identity)
             VALUES ({}, 'node/{}', 1, 'string', NULL, 1, 0, 0);",
            i + 1,
            i,
        ));
    }
    conn.execute_batch(&sql)?;

    println!(
        "seeded {path}: concept node + {n} attributes (node/1..node/{n}, ids 2..{})",
        n + 1
    );
    Ok(())
}