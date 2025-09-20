//! Simple Client Example - Complete Delta Lake Workflow

use delta_operations::{DeltaClient, WriteMode};
use datafusion::prelude::*;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("🚀 Simple Client Example - Complete Delta Lake Workflow");
    println!("======================================================");
    
    // Create client - just a simple DeltaClient, no Arc<Mutex<>>!
    let mut client = DeltaClient::new().await?;
    let path = "./bin/delta/simple_customers";
    
    // ==========================================
    // STEP 0: Clean up - Hard delete existing table to start fresh
    // ==========================================
    println!("\n🗑️  STEP 0: Performing HARD DELETE - completely removing all files...");
    match client.hard_delete_table(path).await {
        Ok(_) => println!("✅ Hard delete completed successfully"),
        Err(e) => println!("⚠️  Note: Hard delete failed (this is expected if table doesn't exist): {}", e),
    }
    
    // ==========================================
    // STEP 1: Create new dataset
    // ==========================================+
    println!("\n📊 STEP 1: Creating new dataset...");
    let initial_customers = client.sql("
        SELECT * FROM VALUES
            (1, 'John Doe', 'john@email.com', 1000.0),
            (2, 'Jane Smith', 'jane@email.com', 2000.0),
            (3, 'Bob Johnson', 'bob@email.com', 1500.0)
        AS customers(id, name, email, balance)
    ").await?;
    
    println!("✅ Created initial dataset with 3 customers");
    initial_customers.clone().show().await?;

    // ==========================================
    // STEP 2: Write to Delta table (with automatic CDF enabling)
    // ==========================================
    println!("\n💾 STEP 2: Writing to Delta table...");
    let written_table = client.write(initial_customers, path, WriteMode::Overwrite).await?;
    println!("✅ Data written to Delta table");
    println!("� Table version: {}", written_table.version().unwrap_or(0));

    // ==========================================
    // STEP 3: Read and show data from Delta table
    // ==========================================
    println!("\n📖 STEP 3: Reading and showing data from Delta table...");
    let (table_after_write, alias1) = client.load_table(path).await?;
    println!("📊 Current table version: {}", table_after_write.version().unwrap_or(0));
    println!("📊 Data in Delta table:");
    client.session_context.sql(&format!("SELECT * FROM {}", alias1)).await?.show().await?;

    // ==========================================
    // STEP 4: Create updates with new dataset
    // ==========================================
    println!("\n📝 STEP 4: Creating updates with new dataset...");
    let update_dataset = client.sql("
        SELECT * FROM VALUES
            (2, 'Jane Smith-Updated', 'jane.updated@email.com', 2500.0),  -- Update existing
            (4, 'Alice Brown', 'alice@email.com', 1800.0),               -- New customer
            (5, 'Charlie Davis', 'charlie@email.com', 2200.0)           -- New customer
        AS updated_customers(id, name, email, balance)
    ").await?;
    
    println!("✅ Created update dataset with 1 update and 2 new customers");
    update_dataset.clone().show().await?;

    // ==========================================
    // STEP 5: Use dataset to update data in Delta table
    // ==========================================
    println!("\n🔄 STEP 5: Using dataset to update data in Delta table...");
    
    // Define merge conditions and expressions
    let merge_condition = col("t.id").eq(col("s.id"));
    let match_condition = lit(true); // Update all matched rows
    
    let update_expressions = HashMap::from([
        ("name", col("s.name")),
        ("email", col("s.email")),
        ("balance", col("s.balance")),
    ]);
    
    let insert_expressions = HashMap::from([
        ("id", col("s.id")),
        ("name", col("s.name")),
        ("email", col("s.email")),
        ("balance", col("s.balance")),
    ]);
    
    let merged_table = client.merge(
        update_dataset,
        path,
        merge_condition,
        match_condition,
        Some(update_expressions),
        Some(insert_expressions),
        true,   // insert if not matched
        false   // don't delete if matched
    ).await?;
    
    println!("✅ Merge operation completed");
    println!("📊 Table version after merge: {}", merged_table.version().unwrap_or(0));

    // ==========================================
    // STEP 6: Read and show data from Delta table (after updates)
    // ==========================================
    println!("\n📖 STEP 6: Reading and showing updated data from Delta table...");
    let (table_after_merge, alias2) = client.load_table(path).await?;
    println!("📊 Current table version: {}", table_after_merge.version().unwrap_or(0));
    println!("📊 Updated data in Delta table:");
    client.session_context.sql(&format!("SELECT * FROM {}", alias2)).await?.show().await?;

    // ==========================================
    // STEP 7: Optimize Delta table
    // ==========================================
    println!("\n⚡ STEP 7: Optimizing Delta table...");
    println!("🔧 Compacting files for better query performance...");
    let optimized_table = client.optimize(path).await?;
    println!("✅ Optimization completed!");
    println!("📊 Optimized table version: {}", optimized_table.version().unwrap_or(0));
    
    // Show state after optimization
    println!("\n📋 State after optimization:");
    let (table_after_optimize, alias3) = client.load_table(path).await?;
    println!("📊 Table version after optimize: {}", table_after_optimize.version().unwrap_or(0));
    client.session_context.sql(&format!("SELECT * FROM {}", alias3)).await?.show().await?;

    // ==========================================
    // STEP 8: Vacuum Delta table
    // ==========================================
    println!("\n🧹 STEP 8: Vacuuming Delta table...");
    println!("�️ Removing unused files and cleaning up storage...");
    let vacuumed_table = client.vacuum(path).await?;
    println!("✅ Vacuum completed!");
    println!("📊 Vacuumed table version: {}", vacuumed_table.version().unwrap_or(0));
    
    // Show final state after vacuum
    println!("\n📋 Final state after vacuum:");
    let (final_table, alias4) = client.load_table(path).await?;
    println!("📊 Final table version: {}", final_table.version().unwrap_or(0));
    client.session_context.sql(&format!("SELECT * FROM {}", alias4)).await?.show().await?;

    // ==========================================
    // STEP 9: Read change data feed (CDF) stream
    // ==========================================
    println!("\n📡 STEP 9: Reading change data feed stream...");
    println!("📊 Reading changes from version 0 onwards...");
    
    match client.read_cdf(path, 0).await {
        Ok(cdf_data) => {
            println!("✅ Change data feed retrieved successfully!");
            
            // Count different types of changes first (clone to preserve original)
            let change_count = cdf_data.clone().count().await.unwrap_or(0);
            println!("📈 Total change records: {}", change_count);
            
            // Then show the data
            println!("📊 Change data feed (shows all modifications):");
            match cdf_data.show().await {
                Ok(_) => println!("✅ CDF data displayed successfully"),
                Err(e) => println!("⚠️ Failed to display CDF data: {}", e),
            }
        }
        Err(e) => {
            println!("⚠️ Change data feed not available (table may not have CDF enabled): {}", e);
            println!("💡 To enable CDF, use: ALTER TABLE <table> SET TBLPROPERTIES (delta.enableChangeDataFeed = true)");
        }
    }

    // ==========================================
    // CLEANUP
    // ==========================================
    println!("\n🧹 Cleaning up table aliases...");
    client.unload_table(&alias1)?;
    client.unload_table(&alias2)?;
    client.unload_table(&alias3)?;
    client.unload_table(&alias4)?;
    println!("✅ All aliases cleaned up successfully");

    println!("\n🎉 Complete Delta Lake workflow demonstration finished!");
    println!("   ✅ Created dataset");
    println!("   ✅ Wrote to Delta table");
    println!("   ✅ Read and displayed data");
    println!("   ✅ Created update dataset");
    println!("   ✅ Updated Delta table via merge");
    println!("   ✅ Read and displayed updated data");
    println!("   ✅ Optimized Delta table");
    println!("   ✅ Vacuumed Delta table");
    println!("   ✅ Read change data feed stream");

    Ok(())
}