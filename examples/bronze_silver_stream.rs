//! Bronze -> Silver streaming style example using Change Data Feed.
//! 
//! Demonstrates:
//! 1. Generating ~50k random rows with diverse column types
//! 2. Writing them in batches to a "bronze" Delta table (producer)
//! 3. Concurrent consumer that tails the bronze table's CDF and writes change rows to a "silver" table
//!
//! Both tables are local filesystem paths (no Azure required). CDF is automatically enabled
//! on table creation via the library's write() implementation.

use delta_operations::{DeltaClient, WriteMode};
use datafusion::prelude::*;
use datafusion::arrow::{array::*, datatypes::{Schema, Field, DataType}};
use datafusion::arrow::record_batch::RecordBatch;
use std::sync::{Arc, atomic::{AtomicI64, AtomicBool, Ordering}};
use anyhow::Result; // use anyhow for Send + Sync error type across async tasks
use rand::Rng;
use uuid::Uuid;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::time::{sleep, Duration};
use datafusion::prelude::col;

const TOTAL_ROWS: usize = 50_000;            // Target total rows in bronze
const BATCH_SIZE: usize = 1_000;             // Rows per producer batch
const BRONZE_PATH: &str = "./bin/delta/bronze_stream";
const SILVER_PATH: &str = "./bin/delta/silver_stream";

#[tokio::main]
async fn main() -> Result<()> {
    println!("⚙️  Bronze->Silver CDF Streaming Example");
    println!("====================================\n");

    // Clean up old directories if present (best-effort)
    let _ = std::fs::remove_dir_all(BRONZE_PATH);
    let _ = std::fs::remove_dir_all(SILVER_PATH);

    // INITIAL WRITE (first batch) to create bronze table & enable CDF
    let mut init_client = DeltaClient::with_token("local".to_string());
    println!("🛠️  Creating bronze table with initial {} rows...", BATCH_SIZE);
    let initial_df = generate_random_dataframe(&init_client.session_context, BATCH_SIZE)?;
    let bronze_table = init_client.write(initial_df, BRONZE_PATH, WriteMode::Overwrite).await?;
    let initial_version = bronze_table.version().unwrap_or(0) as i64; // should be 0
    println!("✅ Bronze table created at version {}", initial_version);

    // Shared state
    let last_committed_version = Arc::new(AtomicI64::new(initial_version));
    let producer_done = Arc::new(AtomicBool::new(false));

    // SPAWN PRODUCER (writes remaining batches)
    let producer_last_version = last_committed_version.clone();
    let producer_done_flag = producer_done.clone();
    let producer_handle = tokio::spawn(async move {
        let mut client = DeltaClient::with_token("local".to_string());
        let remaining_rows = TOTAL_ROWS - BATCH_SIZE;
        let mut rows_written = 0usize;
        let mut batch_index = 1;
        while rows_written < remaining_rows {
            let to_write = BATCH_SIZE.min(remaining_rows - rows_written);
            println!("🧪 [Producer] Generating batch {} with {} rows", batch_index, to_write);
            match generate_random_dataframe(&client.session_context, to_write) {
                Ok(df) => {
                    match client.write(df, BRONZE_PATH, WriteMode::Append).await {
                        Ok(t) => {
                            let v = t.version().unwrap_or(0) as i64;
                            producer_last_version.store(v, Ordering::SeqCst);
                            println!("📝 [Producer] Wrote batch {} (table version now {})", batch_index, v);
                        }
                        Err(e) => {
                            eprintln!("❌ [Producer] Write failed: {}", e);
                            break;
                        }
                    }
                }
                Err(e) => {
                    eprintln!("❌ [Producer] Failed to build dataframe: {}", e);
                    break;
                }
            }
            rows_written += to_write;
            batch_index += 1;
            sleep(Duration::from_millis(250)).await; // simulate streaming cadence
        }
        producer_done_flag.store(true, Ordering::SeqCst);
        println!("🏁 [Producer] Completed. Total produced rows (incl initial): {}", TOTAL_ROWS);
    });

    // SPAWN CONSUMER (tails CDF, writes to silver)
    let consumer_last_version = last_committed_version.clone();
    let consumer_producer_done = producer_done.clone();
    let consumer_handle = tokio::spawn(async move {
        let mut client = DeltaClient::with_token("local".to_string());
        let mut start_version: i64 = 0; // inclusive starting version for next CDF read
        let mut silver_exists = false; // whether silver table already created
        let mut idle_loops = 0u32;

        loop {
            let current_last = consumer_last_version.load(Ordering::SeqCst);
            if current_last >= start_version {
                match client.read_cdf(BRONZE_PATH, start_version).await {
                    Ok(cdf_df) => {
                        // Collect to count rows (clone first as write will consume the DataFrame)
                        let count_df = cdf_df.clone();
                        let change_rows = count_df.count().await.unwrap_or(0) as usize;
                        if change_rows > 0 {
                            println!("🔄 [Consumer] CDF (from v{}) -> {} change rows", start_version, change_rows);
                            use std::collections::HashMap;
                            // Identify base (non-metadata) columns
                            let full_schema = cdf_df.schema();
                            let base_cols: Vec<String> = full_schema.fields()
                                .iter()
                                .map(|f| f.name().to_string())
                                .filter(|name| !name.starts_with('_'))
                                .collect();

                            // Split by change type (case-insensitive support)
                            let delete_filter = col("_change_type")
                                .eq(lit("DELETE")).or(col("_change_type").eq(lit("delete")))
                                .or(col("_change_type").eq(lit("UPDATE_PREIMAGE"))).or(col("_change_type").eq(lit("update_preimage")));
                            let upsert_filter = col("_change_type")
                                .eq(lit("INSERT")).or(col("_change_type").eq(lit("insert")))
                                .or(col("_change_type").eq(lit("UPDATE_POSTIMAGE"))).or(col("_change_type").eq(lit("update_postimage")));

                            let deletes_df = match cdf_df.clone().filter(delete_filter) {
                                Ok(df) => df,
                                Err(e) => { eprintln!("⚠️  [Consumer] Delete filter failed: {}", e); continue; }
                            };
                            let upserts_df = match cdf_df.clone().filter(upsert_filter) {
                                Ok(df) => df,
                                Err(e) => { eprintln!("⚠️  [Consumer] Upsert filter failed: {}", e); continue; }
                            };

                            // Prepare projected DataFrames (select only base columns for silver)
                            let base_exprs: Vec<_> = base_cols.iter().map(|c| col(c)).collect();
                            let upserts_base = match upserts_df.select(base_exprs.clone()) {
                                Ok(df) => df,
                                Err(e) => { eprintln!("⚠️  [Consumer] Upsert projection failed: {}", e); continue; }
                            };

                            // Count upsert rows (will drive create/merge operations)
                            let upsert_row_count = match upserts_base.clone().count().await { Ok(c) => c, Err(e) => { eprintln!("⚠️  [Consumer] Count failed: {}", e); 0 } };
                            if upsert_row_count == 0 && !silver_exists {
                                println!("ℹ️  [Consumer] No upsert rows yet to initialize silver; waiting...");
                            } else if !silver_exists {
                                if let Err(e) = client.write(upserts_base.clone(), SILVER_PATH, WriteMode::Overwrite).await {
                                    eprintln!("❌ [Consumer] Failed to create silver table: {}", e);
                                } else {
                                    silver_exists = true;
                                    println!("✅ [Consumer] Silver table created with {} initial rows", upsert_row_count);
                                }
                            } else {
                                // 1. Process deletes via merge (id only)
                                if let Ok(delete_ids_df_full) = deletes_df.select(vec![col("id")]) {
                                    if let Ok(delete_count_df) = delete_ids_df_full.clone().count().await {
                                        if delete_count_df > 0 {
                                            println!("🗑️  [Consumer] Deleting {} ids in silver", delete_count_df);
                                            // Perform delete merge
                                            let merge_condition = col("t.id").eq(col("s.id"));
                                            let match_condition = lit(true); // delete all matched ids
                                            if let Err(e) = client.merge(
                                                delete_ids_df_full,
                                                SILVER_PATH,
                                                merge_condition,
                                                match_condition,
                                                None,
                                                None,
                                                false, // no insert
                                                true   // delete matched
                                            ).await { eprintln!("❌ [Consumer] Delete merge failed: {}", e); }
                                        }
                                    }
                                }

                                // 2. Upserts via merge
                                if upsert_row_count > 0 {
                                        println!("📥 [Consumer] Upserting {} rows into silver", upsert_row_count);
                                        let merge_condition = col("t.id").eq(col("s.id"));
                                        let match_condition = lit(true);
                                        let mut update_expressions: HashMap<&str, Expr> = HashMap::new();
                                        let mut insert_expressions: HashMap<&str, Expr> = HashMap::new();
                                        for c in &base_cols {
                                            if c == "id" { // id not updated
                                                insert_expressions.insert("id", col("s.id"));
                                            } else {
                                                let expr = col(&format!("s.{}", c));
                                                insert_expressions.insert(Box::leak(c.clone().into_boxed_str()), expr.clone());
                                                update_expressions.insert(Box::leak(c.clone().into_boxed_str()), expr);
                                            }
                                        }
                                        if let Err(e) = client.merge(
                                            upserts_base,
                                            SILVER_PATH,
                                            merge_condition,
                                            match_condition,
                                            Some(update_expressions),
                                            Some(insert_expressions),
                                            true,  // insert if not matched
                                            false  // do not delete on matched
                                        ).await { eprintln!("❌ [Consumer] Upsert merge failed: {}", e); }
                                }
                            }
                        }
                        // Advance start version to one past the last version observed
                        start_version = current_last + 1;
                        idle_loops = 0; // work done
                    }
                    Err(e) => {
                        // CDF might not yet be ready for this version (rare); retry later
                        eprintln!("⚠️  [Consumer] CDF read error: {}", e);
                        idle_loops += 1;
                    }
                }
            } else {
                idle_loops += 1;
            }

            if consumer_producer_done.load(Ordering::SeqCst) && start_version > current_last {
                println!("🏁 [Consumer] No new versions and producer done. Exiting loop.");
                break;
            }

            if idle_loops > 40 { // ~10s with 250ms sleep
                println!("⌛ [Consumer] Idle timeout reached.");
                break;
            }

            sleep(Duration::from_millis(250)).await;
        }
        println!("✅ [Consumer] Finished replication.");
    });

    // Wait for both tasks
    let _ = tokio::join!(producer_handle, consumer_handle);

    // VALIDATION
    println!("\n🔍 Validation Phase");
    let mut verify_client = DeltaClient::with_token("local".to_string());
    let bronze_df = verify_client.read_table(BRONZE_PATH).await?; // SELECT *
    let bronze_rows = bronze_df.count().await? as usize;
    println!("📊 Bronze row count: {}", bronze_rows);

    let silver_df = verify_client.read_table(SILVER_PATH).await?;
    let silver_rows = silver_df.count().await? as usize;
    println!("📊 Silver (changes) row count: {}", silver_rows);

    println!("📊 NOTE: Silver row count represents current state after merges (should match bronze)");
    if bronze_rows == silver_rows { println!("🎉 SUCCESS: Bronze and Silver row counts match: {}", bronze_rows); }
    else { println!("❌ MISMATCH: bronze={} silver={}", bronze_rows, silver_rows); }

    println!("\nDone.");
    Ok(())
}

/// Generate a random DataFrame; id is a GUID (string) per row.
fn generate_random_dataframe(ctx: &SessionContext, rows: usize) -> Result<DataFrame> {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Utf8, false),
    // Use Int64 microseconds since epoch instead of Timestamp to avoid delta-rs schema support issues in this minimal setup
    Field::new("event_time", DataType::Int64, false),
        Field::new("category", DataType::Utf8, false),
        Field::new("amount", DataType::Float64, false),
        Field::new("quantity", DataType::Int32, false),
        Field::new("is_active", DataType::Boolean, false),
        Field::new("region", DataType::Utf8, false),
        Field::new("score", DataType::Float32, false),
        Field::new("created_date", DataType::Date32, false),
        Field::new("guid", DataType::Utf8, false),
        Field::new("tags", DataType::Utf8, false),
    ]));

    let mut id_b = StringBuilder::with_capacity(rows, rows * 36);
    let mut ts_b = Int64Builder::with_capacity(rows); // event_time as i64 micros
    let mut cat_b = StringBuilder::with_capacity(rows, rows * 8);
    let mut amt_b = Float64Builder::with_capacity(rows);
    let mut qty_b = Int32Builder::with_capacity(rows);
    let mut act_b = BooleanBuilder::with_capacity(rows);
    let mut region_b = StringBuilder::with_capacity(rows, rows * 6);
    let mut score_b = Float32Builder::with_capacity(rows);
    let mut date_b = Date32Builder::with_capacity(rows);
    let mut guid_b = StringBuilder::with_capacity(rows, rows * 36);
    let mut tags_b = StringBuilder::with_capacity(rows, rows * 10);

    let categories = ["alpha", "beta", "gamma", "delta", "omega"];    
    let regions = ["us", "eu", "apac", "latam", "me"];    
    let tag_sets = ["t1,t2", "t2,t3", "t3,t4", "t4,t5"];    

    let base_time = SystemTime::now().duration_since(UNIX_EPOCH)?.as_micros() as i64;
    let base_days = (SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs() / 86_400) as i32;
    let mut rng = rand::thread_rng();

    for i in 0..rows {
        id_b.append_value(Uuid::new_v4().to_string().as_str());
        ts_b.append_value(base_time + i as i64 * 1000); // spaced by 1ms (stored as i64 micros)
        let cat = categories[rng.gen_range(0..categories.len())];
        cat_b.append_value(cat);
        let amount: f64 = rng.gen_range(0.0..10_000.0);
        amt_b.append_value(amount);
        qty_b.append_value(rng.gen_range(1..1000) as i32);
        act_b.append_value(rng.gen_bool(0.8));
        let region = regions[rng.gen_range(0..regions.len())];
        region_b.append_value(region);
        score_b.append_value(rng.gen_range(0.0..100.0) as f32);
        date_b.append_value(base_days + rng.gen_range(0..30) as i32);
        guid_b.append_value(Uuid::new_v4().to_string().as_str());
        tags_b.append_value(tag_sets[rng.gen_range(0..tag_sets.len())]);
    }

    let batch = RecordBatch::try_new(schema.clone(), vec![
        Arc::new(id_b.finish()),
        Arc::new(ts_b.finish()),
        Arc::new(cat_b.finish()),
        Arc::new(amt_b.finish()),
        Arc::new(qty_b.finish()),
        Arc::new(act_b.finish()),
        Arc::new(region_b.finish()),
        Arc::new(score_b.finish()),
        Arc::new(date_b.finish()),
        Arc::new(guid_b.finish()),
        Arc::new(tags_b.finish()),
    ])?;

    use datafusion::datasource::MemTable;
    let mem_table = MemTable::try_new(schema, vec![vec![batch]])?;
    let df = ctx.read_table(Arc::new(mem_table))?; // synchronous
    Ok(df)
}
