//! Minimal merge example using the slim API
use delta_operations::{DeltaClient, WriteMode};
use datafusion::prelude::*;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = DeltaClient::new().await?;
    let path = "./bin/delta/merge_minimal";

    // Seed initial data
    let seed = client.sql("SELECT * FROM VALUES (1,'A'),(2,'B') AS t(id,name)").await?;
    let _ = client.write(seed, path, WriteMode::Overwrite).await?;

    // New + updated data
    let upd = client.sql("SELECT * FROM VALUES (2,'B2'),(3,'C') AS u(id,name)").await?;

    let merge_condition = col("t.id").eq(col("s.id"));
    let match_condition = lit(true);

    let update_map = HashMap::from([
        ("name", col("s.name")),
    ]);
    let insert_map = HashMap::from([
        ("id", col("s.id")),
        ("name", col("s.name")),
    ]);

    client.merge(
        upd,
        path,
        merge_condition,
        match_condition,
        Some(update_map),
        Some(insert_map),
        true,
        false
    ).await?;

    println!("After merge:");
    client.read_table(path).await?.show().await?;
    Ok(())
}
