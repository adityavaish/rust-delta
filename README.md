# Delta Operations (Minimal Delta Lake + Azure Helper)

Lightweight Rust helper focused on the essential Delta Lake operations you actually need: read, write, merge (upsert style), change data feed (CDF), optimize (compact), vacuum (placeholder), and destructive hard delete for quick test resets. Supports both local filesystem paths and Azure Data Lake Gen2 (ABFSS) seamlessly.

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## 🔍 Goals

* Minimal surface area – no extension traits, no extra layers
* Ergonomic async API via a single `DeltaClient`
* Works locally and with `abfss://...` paths (auto handler registration + token auth)
* Interactive REPL for ad‑hoc exploration
* Tested core flows; Azure-dependent tests isolated/ignored when creds absent

## ✨ Feature Summary

| Capability | Method / Command | Notes |
|------------|------------------|-------|
| SQL query | `client.sql()` | Mixed‑case column fallback quoting helper built-in |
| Load + alias | `load_table / load_table_with_alias` | Returns `(DeltaTable, alias)` |
| List tables | `list_tables()` | In‑memory registry |
| Read whole table | `read_table()` | Convenience `SELECT *` |
| Write (append/overwrite) | `write(df, path, WriteMode)` | Auto‑enables CDF on new tables |
| Merge (upsert) | `merge(...)` | Provide match + update/insert maps |
| Optimize | `optimize(path)` | Compaction via DeltaOps |
| Vacuum | `vacuum(path)` | Removes old, unreferenced files (default retention) |
| Change Data Feed | `read_cdf(path, from_version)` | Validates CDF enabled |
| Enable CDF (existing) | `enable_change_data_feed(path)` | Returns explanatory error (not yet implemented) |
| Hard delete | `hard_delete_table(path)` | Recursively removes all files (local & Azure) |
| Unload alias | `unload_table(alias)` | Deregisters from session |

## 🚀 Quick Start

Add to `Cargo.toml` (path or version as appropriate):

```toml
[dependencies]
delta_operations = { path = "." } # or version when published
```text

### Basic Example

```rust
use delta_operations::{DeltaClient, WriteMode};
use datafusion::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = DeltaClient::new().await?;

    // Create a small in-memory dataset via SQL
    let df = client.sql("SELECT * FROM VALUES (1,'Alice'), (2,'Bob') AS t(id, name)").await?;

    // Write (creates table, auto‑enables CDF)
    client.write(df, "./bin/delta/example_table", WriteMode::Overwrite).await?;

    // Read back
    let read_df = client.read_table("./bin/delta/example_table").await?;
    read_df.show().await?;

    Ok(())
}
```text

### Merge (Upsert Style)

```rust
use delta_operations::{DeltaClient, WriteMode};
use datafusion::prelude::*;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = DeltaClient::new().await?;
    let path = "./bin/delta/customers";

    // Seed
    let base = client.sql("SELECT * FROM VALUES (1,'John'), (2,'Jane') AS c(id,name)").await?;
    client.write(base, path, WriteMode::Overwrite).await?;

    // Updates + new rows
    let updates = client.sql("SELECT * FROM VALUES (2,'Jane-Updated'), (3,'Maria') AS u(id,name)").await?;

    // Build expression maps (id + name columns)
    let merge_condition = col("t.id").eq(col("s.id"));
    let match_condition = lit(true); // update all matches
    let update_map = HashMap::from([
        ("name", col("s.name")),
    ]);
    let insert_map = HashMap::from([
        ("id", col("s.id")),
        ("name", col("s.name")),
    ]);

    client.merge(
        updates,
        path,
        merge_condition,
        match_condition,
        Some(update_map),
        Some(insert_map),
        true,  // insert new
        false  // no delete
    ).await?;

    client.read_table(path).await?.show().await?;
    Ok(())
}
```text

### Change Data Feed (CDF)

```rust
let mut client = DeltaClient::new().await?;
let changes = client.read_cdf("./bin/delta/customers", 0).await?;
changes.show().await?;
```text

If CDF isn't enabled you'll get a clear error including a suggested SQL property statement.

## 🖥️ Interactive REPL

Run:

```bash
cargo run --bin delta_operations
```

Commands now include (type `help` inside REPL):

```
load <path> [alias]
tables
read <alias> [limit]
sql <query>
schema <alias>
cdf <path> <fromVersion>
write <alias> <path> <mode>
merge <source_alias> <target_path> <key_col?>
optimize <path>
vacuum <path>
enable_cdf <path>
hard_delete <path>
unload <alias>
exit
```

Example flow inside REPL:

```
>> sql SELECT * FROM VALUES (1,'A'),(2,'B') AS t(id,name)
>> write t ./bin/delta/demo overwrite
>> load ./bin/delta/demo demo
>> read demo 10
>> optimize ./bin/delta/demo
>> cdf ./bin/delta/demo 0
```

## 🔐 Azure Integration

`DeltaClient::new()` automatically:

* Registers Azure storage handlers (`deltalake::azure::register_handlers`)
* Obtains a token via `DefaultAzureCredential`
* Allows seamless use of paths like:

```
abfss://container@account.dfs.core.windows.net/path/to/table
```

You can also inject your own token with `DeltaClient::with_token(token_string)`.

## 🧪 Testing

Run all local tests:

```bash
cargo test
```

Some integration tests depending on real Azure credentials are `ignored`; run explicitly if you supply env credentials.

## ⚠️ Notes & Limitations

* `vacuum` uses default delta-rs retention; no custom retention/dry-run flags yet.
* Enabling CDF on existing tables is not yet implemented here (clear error is returned).
* Merge assumes column symmetry between source and target; add custom predicates/expressions for advanced scenarios.
* Hard delete is destructive – use only for ephemeral development data.

## 📁 Project Structure (Simplified)

```
src/
    lib.rs        # Public exports
    main.rs       # REPL binary
    client.rs     # High-level DeltaClient
    operations.rs # Underlying load/write helpers
    utils.rs      # Path validation & helpers
    error.rs      # Error definitions
examples/
    simple_client.rs
tests/          # Unit + integration tests
```

Removed (historic): `DataFrameExt` trait, `QueryResult`, extra write modes, schema creation convenience.

## 📜 License

MIT – see `LICENSE`.

## 🤝 Contributing

PRs welcome for: enabling CDF mutation, real vacuum, metrics surfacing, richer merge DSL.

## 🗓️ Changelog (Current Track)

Unreleased:

* API slimmed: removed extension trait & legacy wrappers
* Added REPL commands: merge / optimize / vacuum / enable_cdf / hard_delete
* Stable merge + CDF read path

Historical (pre-slim): legacy extension trait & upsert helpers removed.

---
Lean, focused, and practical. If you need a richer abstraction layer, build it on top – this crate aims to stay small.
