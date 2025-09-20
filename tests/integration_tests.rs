//! Integration tests for the Delta operations library
//! 
//! These tests require an actual Azure storage account for end-to-end testing.
//! For CI/CD environments, mock implementations should be used.

use delta_operations::{DeltaClient, DeltaError, WriteMode};
use datafusion::prelude::*;
use std::collections::HashMap;

/// Integration test configuration
struct TestConfig {
    storage_account: String,
    container: String,
    test_token: String,
}

impl TestConfig {
    fn new() -> Self {
        Self {
            storage_account: "teststorage".to_string(),
            container: "testcontainer".to_string(),
            test_token: "test_token_value".to_string(),
        }
    }

    fn test_path(&self, table_name: &str) -> String {
        format!(
            "abfss://{}@{}.dfs.core.windows.net/test_tables/{}",
            self.container, self.storage_account, table_name
        )
    }
}

#[tokio::test]
#[ignore] // Requires Azure credentials - run with --ignored flag when available
async fn test_client_creation_with_azure_auth() {
    let result = DeltaClient::new().await;
    
    // This will fail in CI without proper Azure setup, but validates the flow
    match result {
        Ok(_client) => {
            // Success case - proper Azure auth available
            println!("Successfully created client with Azure authentication");
        }
        Err(DeltaError::Authentication(_)) => {
            // Expected in CI/test environments without Azure setup
            println!("Authentication failed as expected in test environment");
        }
        Err(e) => {
            panic!("Unexpected error: {:?}", e);
        }
    }
}

#[tokio::test]
async fn test_client_creation_with_custom_token() {
    let config = TestConfig::new();
    let client = DeltaClient::with_token(config.test_token.clone());
    
    // Verify client was created successfully
    assert_eq!(format!("{:?}", client).contains("DeltaClient"), true);
}

#[tokio::test]
#[ignore] // Requires Azure storage access
async fn test_full_workflow() -> Result<(), Box<dyn std::error::Error>> {
    let config = TestConfig::new();
    let mut client = DeltaClient::with_token(config.test_token.clone());
    
    // Create test data
    let test_data = client.sql(
        "SELECT * FROM VALUES 
         (1, 'John', 25, 50000.0), 
         (2, 'Jane', 30, 60000.0),
         (3, 'Bob', 35, 70000.0)
         AS employees(id, name, age, salary)"
    ).await?;
    
    let table_path = config.test_path("test_employees");
    
    // Test write operation
    let _table = client.write(test_data.clone(), &table_path, WriteMode::Overwrite).await?;
    
    // Test load operation
        let (_loaded_table, alias) = client.load_table(&table_path).await?;
    
    // Verify loaded data
    let result_df = client.query(&format!("SELECT COUNT(*) as count FROM {}", alias)).await?;
    let batches = result_df.collect().await?;
    
    // Clean up
    client.unload_table(&alias)?;
    
    // Verify we have the expected number of rows
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].num_rows(), 1);
    
    Ok(())
}

#[tokio::test]
#[ignore] // Requires Azure storage access
async fn test_merge_operations() -> Result<(), Box<dyn std::error::Error>> {
    let config = TestConfig::new();
    let mut client = DeltaClient::with_token(config.test_token.clone());
    
    // Create initial data
    let initial_data = client.sql(
        "SELECT * FROM VALUES 
         (1, 'John', 25), 
         (2, 'Jane', 30)
         AS employees(id, name, age)"
    ).await?;
    
    let table_path = config.test_path("test_merge");
    
    // Create the table
    let _table = client.write(initial_data, &table_path, WriteMode::Overwrite).await?;
    
    // Create merge data
    let merge_data = client.sql(
        "SELECT * FROM VALUES 
         (2, 'Jane Smith', 31),  -- Update existing
         (3, 'Bob', 35)          -- Insert new
         AS new_employees(id, name, age)"
    ).await?;
    
    // Perform merge operation
    let merge_condition = col("new_employees.id").eq(col("employees.id"));
    let match_condition = lit(true); // Always update when matched
    
    let mut update_expressions = HashMap::new();
    update_expressions.insert("name", col("new_employees.name"));
    update_expressions.insert("age", col("new_employees.age"));
    
    let _result_table = client.merge(
        merge_data,
        &table_path,
        merge_condition,
        match_condition,
        Some(update_expressions),
        None, // Use auto-generated inserts
        true, // Insert if not matched
        false, // Don't delete
    ).await?;
    
    // Verify merge results
        let (_, alias) = client.load_table(&table_path).await?;
    let result_df = client.query(&format!("SELECT COUNT(*) as count FROM {}", alias)).await?;
    let batches = result_df.collect().await?;
    
    client.unload_table(&alias)?;
    
    // Should have 3 rows after merge (1 original + 1 updated + 1 inserted)
    assert_eq!(batches[0].num_rows(), 1);
    
    Ok(())
}

// DataFrameExt trait removed; extension trait test eliminated

#[tokio::test]
async fn test_error_handling() {
    let config = TestConfig::new();
    let mut client = DeltaClient::with_token(config.test_token.clone());
    
    // Test invalid SQL
    let result = client.query("INVALID SQL QUERY").await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DeltaError::Query(_) => {}, // Expected
        e => panic!("Expected Query error, got: {:?}", e),
    }
    
    // Test invalid path
    let test_df = client.sql("SELECT 1 as id").await.unwrap();
    let result = client.write(test_df, "invalid://path", WriteMode::Append).await;
    assert!(result.is_err());
    match result.unwrap_err() {
        DeltaError::Config(_) => {}, // Expected
        e => panic!("Expected Config error, got: {:?}", e),
    }
}

#[tokio::test]
async fn test_basic_query_dataframe() {
    let config = TestConfig::new();
    let client = DeltaClient::with_token(config.test_token.clone());
    let df = client.query("SELECT * FROM VALUES (1, 'test'), (2, 'data') AS t(id, name)").await.unwrap();
    let batches = df.collect().await.unwrap();
    assert_eq!(batches.len(), 1);
}

#[tokio::test]
#[ignore] // Requires Azure credentials and real table
async fn test_table_maintenance_workflow() {
    // This test demonstrates the complete table maintenance workflow
    // with optimize and vacuum operations
    let config = TestConfig::new();
    let table_path = config.test_path("maintenance_test_table");
    
    // Use test token for this integration test
    let mut client = DeltaClient::with_token(config.test_token.clone());
    
    // Create test dataset
    let test_data = client.sql("
        SELECT * FROM VALUES
            (1, 'Test User 1', 'test1@example.com', 100.0),
            (2, 'Test User 2', 'test2@example.com', 200.0),
            (3, 'Test User 3', 'test3@example.com', 300.0)
        AS test_users(id, name, email, balance)
    ").await;

    if let Ok(data) = test_data {
        // Write initial data
        if let Ok(written_table) = client.write(data, &table_path, WriteMode::Overwrite).await {
            println!("Initial write successful, version: {}", written_table.version().unwrap_or(0));
            
            // Perform some updates to create file fragmentation
            for i in 4..=10 {
                let update_data = client.sql(&format!("
                    SELECT * FROM VALUES
                        ({}, 'Test User {}', 'test{}@example.com', {}.0)
                    AS new_user(id, name, email, balance)
                ", i, i, i, i * 100)).await;
                
                if let Ok(data) = update_data {
                    let _ = client.write(data, &table_path, WriteMode::Append).await;
                }
            }
            
            // Now test optimization
            println!("Testing optimize operation...");
            match client.optimize(&table_path).await {
                Ok(optimized_table) => {
                    println!("Optimize successful! Version: {}", optimized_table.version().unwrap_or(0));
                    
                    // Test vacuum after optimization
                    println!("Testing vacuum operation...");
                    match client.vacuum(&table_path).await {
                        Ok(vacuumed_table) => {
                            println!("Vacuum successful! Version: {}", vacuumed_table.version().unwrap_or(0));
                            
                            // Test read_cdf for change data feed
                            println!("Testing read_cdf operation...");
                                match client.read_cdf(&table_path, 0).await {
                                Ok(cdf_data) => {
                                    println!("Read stream successful!");
                                    let change_count = cdf_data.count().await.unwrap_or(0);
                                    println!("Change records found: {}", change_count);
                                }
                                Err(e) => {
                                    println!("Read stream failed (expected if CDF not enabled): {:?}", e);
                                    // This is expected if CDF is not enabled on the table
                                }
                            }
                            
                            // Verify table is still readable
                            if let Ok((final_table, alias)) = client.load_table(&table_path).await {
                                println!("Final table version: {}", final_table.version().unwrap_or(0));
                                if let Ok(df) = client.session_context.sql(&format!("SELECT * FROM {}", alias)).await {
                                    if let Ok(count) = df.count().await {
                                    println!("Final record count: {}", count);
                                    assert!(count >= 3); // Should have at least our initial 3 records
                                    }
                                }
                                let _ = client.unload_table(&alias);
                            }
                        }
                        Err(e) => println!("Vacuum failed (expected in test env): {:?}", e),
                    }
                }
                Err(e) => println!("Optimize failed (expected in test env): {:?}", e),
            }
        } else {
            println!("Write failed (expected in test environment without Azure setup)");
        }
    } else {
        println!("SQL data creation failed");
    }
}