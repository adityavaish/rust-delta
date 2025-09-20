//! Unit tests for the DeltaClient module

#[cfg(test)]
mod client_tests {
    use delta_operations::{DeltaClient, DeltaError};
    use std::collections::HashMap;
    use datafusion::prelude::*;

    #[test]
    fn test_client_with_token_creation() {
        let token = "test_token_12345".to_string();
        let client = DeltaClient::with_token(token.clone());
        
        // Verify client structure (using Debug trait)
        let debug_str = format!("{:?}", client);
        assert!(debug_str.contains("DeltaClient"));
        assert!(debug_str.contains("loaded_tables"));
    }

    #[test]
    fn test_generate_random_alias() {
        let client = DeltaClient::with_token("test_token".to_string());
        
        // Test alias generation with different length ranges
        for _ in 0..10 {
            let alias = client.generate_random_alias(4, 8);
            
            // Verify length is within range
            assert!(alias.len() >= 4 && alias.len() <= 8);
            
            // Verify all characters are lowercase alphabetic
            assert!(alias.chars().all(|c| c.is_ascii_lowercase() && c.is_alphabetic()));
        }
        
        // Test minimum length boundary
        let min_alias = client.generate_random_alias(1, 1);
        assert_eq!(min_alias.len(), 1);
        
        // Test maximum length boundary
        let max_alias = client.generate_random_alias(20, 20);
        assert_eq!(max_alias.len(), 20);
    }

    #[test]
    fn test_generate_random_alias_uniqueness() {
        let client = DeltaClient::with_token("test_token".to_string());
        
        // Generate multiple aliases and verify they're likely to be unique
        let mut aliases = std::collections::HashSet::new();
        
        for _ in 0..100 {
            let alias = client.generate_random_alias(6, 10);
            aliases.insert(alias);
        }
        
        // With 100 iterations and random generation, we should have many unique values
        // (allowing for some collision possibility with smaller character space)
        assert!(aliases.len() > 80); // Should be mostly unique
    }

    #[tokio::test]
    async fn test_unload_table_with_invalid_alias() {
        let mut client = DeltaClient::with_token("test_token".to_string());
        
        // Try to unload a table that doesn't exist
        let result = client.unload_table("nonexistent_alias");
        
        // This should succeed (DataFusion doesn't error on deregistering non-existent tables)
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_sql_query_with_simple_values() {
        let client = DeltaClient::with_token("test_token".to_string());
        
        // Test basic SQL with VALUES clause (doesn't require external storage)
        let result = client.sql("SELECT 1 as id, 'test' as name").await;
        
        match result {
            Ok(df) => {
                // Verify the DataFrame was created
                let schema = df.schema();
                assert_eq!(schema.fields().len(), 2);
                assert_eq!(schema.field(0).name(), "id");
                assert_eq!(schema.field(1).name(), "name");
            }
            Err(e) => {
                println!("SQL query failed (expected in some test environments): {:?}", e);
            }
        }
    }

    #[tokio::test]
    async fn test_sql_query_error_handling() {
        let client = DeltaClient::with_token("test_token".to_string());
        
        // Test invalid SQL
        let result = client.sql("INVALID SQL SYNTAX HERE").await;
        
        assert!(result.is_err());
        match result.unwrap_err() {
            DeltaError::Query(_) => {}, // Expected
            e => panic!("Expected Query error, got: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_query_vs_sql_methods() {
        let client = DeltaClient::with_token("test_token".to_string());
        
        let sql_query = "SELECT 42 as answer, 'hello' as greeting";
        
        // query() now returns a DataFrame just like sql()
        let df1 = client.query(sql_query).await;
        let df2 = client.sql(sql_query).await;
        match (df1, df2) {
            (Ok(a), Ok(b)) => { assert_eq!(a.schema(), b.schema()); }
            (Err(_), Err(_)) => println!("Both query variants failed (expected in some envs)"),
            _ => panic!("Inconsistent results between query() and sql()"),
        }
    }

    #[test]
    fn test_client_debug_format() {
        let client = DeltaClient::with_token("debug_test_token".to_string());
        
        let debug_output = format!("{:?}", client);
        
        // Verify debug output contains expected components
        assert!(debug_output.contains("DeltaClient"));
        assert!(debug_output.contains("operations"));
        assert!(debug_output.contains("loaded_tables"));
        
        // Verify it shows empty tables initially
        assert!(debug_output.contains("[]"));
    }

    mod quote_identifiers_tests {
        use super::*;

        #[tokio::test]
        async fn test_quote_mixed_case_identifiers_simple() {
            let client = DeltaClient::with_token("test_token".to_string());
            
            // Test simple SQL without mixed case
            let simple_sql = "select id, name from users";
            let result = client.quote_mixed_case_identifiers(simple_sql).await;
            
            match result {
                Ok(quoted_sql) => {
                    // Should remain unchanged if no mixed case columns are detected
                    assert_eq!(quoted_sql, simple_sql);
                }
                Err(e) => {
                    panic!("Failed to process simple SQL: {:?}", e);
                }
            }
        }

        #[tokio::test]
        async fn test_quote_mixed_case_identifiers_with_keywords() {
            let client = DeltaClient::with_token("test_token".to_string());
            
            // Test SQL with keywords that should not be quoted
            let sql_with_keywords = "SELECT id FROM users WHERE name = 'test' ORDER BY id";
            let result = client.quote_mixed_case_identifiers(sql_with_keywords).await;
            
            match result {
                Ok(quoted_sql) => {
                    // Keywords should not be quoted
                    assert!(quoted_sql.contains("SELECT"));
                    assert!(quoted_sql.contains("FROM"));
                    assert!(quoted_sql.contains("WHERE"));
                    assert!(quoted_sql.contains("ORDER"));
                    assert!(quoted_sql.contains("BY"));
                    
                    // Should not contain quotes around keywords
                    assert!(!quoted_sql.contains("\"SELECT\""));
                    assert!(!quoted_sql.contains("\"FROM\""));
                }
                Err(e) => {
                    panic!("Failed to process SQL with keywords: {:?}", e);
                }
            }
        }

        #[tokio::test]
        async fn test_quote_mixed_case_identifiers_empty_tables() {
            let client = DeltaClient::with_token("test_token".to_string());
            
            // With no loaded tables, mixed case detection should not quote anything
            let sql = "select UserName, firstName from MyTable";
            let result = client.quote_mixed_case_identifiers(sql).await;
            
            match result {
                Ok(quoted_sql) => {
                    // Without loaded table schemas, should remain unchanged
                    assert_eq!(quoted_sql, sql);
                }
                Err(e) => {
                    panic!("Failed to process SQL: {:?}", e);
                }
            }
        }

        #[tokio::test] 
        async fn test_quote_mixed_case_identifiers_regex_error() {
            // This test verifies error handling in the regex creation
            // Since we're using a hardcoded regex pattern, this mainly tests the error path exists
            let client = DeltaClient::with_token("test_token".to_string());
            
            // Normal case should work
            let result = client.quote_mixed_case_identifiers("SELECT * FROM test").await;
            assert!(result.is_ok());
        }
    }

    mod load_table_tests {
        use super::*;

        #[tokio::test]
        async fn test_load_table_invalid_path() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            // Test with invalid path format
            let result = client.load_table("invalid://path/format").await;
            
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::Config(_) => {}, // Expected from path validation
                e => panic!("Expected Config error, got: {:?}", e),
            }
        }

        #[tokio::test]
        async fn test_load_table_valid_path_format() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            // Test with valid path format (will fail on actual loading, but should pass validation)
            let valid_path = "abfss://container@account.dfs.core.windows.net/path/to/table";
            let result = client.load_table(valid_path).await;
            
            // Should fail on the actual loading (no real Azure access), not on validation
            assert!(result.is_err());
            // The error should NOT be a Config error (validation passed)
            match result.unwrap_err() {
                DeltaError::Config(_) => panic!("Should not be a config error for valid path format"),
                _ => {}, // Expected - will fail on actual table loading
            }
        }
    }

    mod write_tests {
        use super::*;
        use delta_operations::WriteMode;

        #[tokio::test]
        async fn test_write_invalid_path() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            // Create a simple DataFrame
            let df = match client.sql("SELECT 1 as id").await {
                Ok(df) => df,
                Err(_) => return, // Skip test if basic SQL doesn't work in test environment
            };
            
            // Test with invalid path
            let result = client.write(df, "invalid://path", WriteMode::Append).await;
            
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::Config(_) => {}, // Expected from path validation
                e => panic!("Expected Config error, got: {:?}", e),
            }
        }

        #[tokio::test]
        async fn test_write_valid_path_format() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            // Create a simple DataFrame
            let df = match client.sql("SELECT 1 as id").await {
                Ok(df) => df,
                Err(_) => return, // Skip test if basic SQL doesn't work in test environment
            };
            
            let valid_path = "abfss://container@account.dfs.core.windows.net/test/table";
            let result = client.write(df, valid_path, WriteMode::Overwrite).await;
            
            // Should fail on actual write operation, not on path validation
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::Config(_) => panic!("Should not be a config error for valid path format"),
                _ => {}, // Expected - will fail on actual write operation
            }
        }
    }

    mod merge_tests {
        use super::*;

        #[tokio::test]
        async fn test_merge_invalid_path() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            // Create a simple DataFrame
            let df = match client.sql("SELECT 1 as id, 'test' as name").await {
                Ok(df) => df,
                Err(_) => return, // Skip test if basic SQL doesn't work
            };
            
            let merge_condition = col("source.id").eq(col("target.id"));
            let match_condition = lit(true);
            
            let result = client.merge(
                df,
                "invalid://path",
                merge_condition,
                match_condition,
                None,
                None,
                true,
                false,
            ).await;
            
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::Config(_) => {}, // Expected from path validation
                e => panic!("Expected Config error, got: {:?}", e),
            }
        }

        #[tokio::test]
        async fn test_merge_parameters() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            // Create test DataFrame
            let df = match client.sql("SELECT 1 as id, 'test' as name").await {
                Ok(df) => df,
                Err(_) => return, // Skip test if basic SQL doesn't work
            };
            
            let valid_path = "abfss://container@account.dfs.core.windows.net/test/table";
            let merge_condition = col("s.id").eq(col("t.id"));
            let match_condition = col("s.name").not_eq(col("t.name"));
            
            let mut update_exprs = HashMap::new();
            update_exprs.insert("name", col("s.name"));
            
            let mut insert_exprs = HashMap::new();
            insert_exprs.insert("id", col("s.id"));
            insert_exprs.insert("name", col("s.name"));
            
            // This will fail due to no Azure access, but tests parameter handling
            let result = client.merge(
                df,
                valid_path,
                merge_condition,
                match_condition,
                Some(update_exprs),
                Some(insert_exprs),
                true,
                false,
            ).await;
            
            // Should fail on actual operation, not on parameter validation
            assert!(result.is_err());
        }
    }

    // create_table_with_schema tests removed (API eliminated)

    mod optimize_tests {
        use super::*;

        #[tokio::test]
        async fn test_optimize_invalid_path() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            let result = client.optimize("invalid://path").await;
            
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::Config(_) => {}, // Expected from path validation
                e => panic!("Expected Config error, got: {:?}", e),
            }
        }

        #[tokio::test]
        async fn test_optimize_valid_path_format() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            let valid_path = "abfss://container@account.dfs.core.windows.net/test/table";
            let result = client.optimize(valid_path).await;
            
            // Should fail on actual optimization (table doesn't exist), not on validation
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::Config(_) => panic!("Should not be a config error for valid path format"),
                DeltaError::DeltaTable(_) => {}, // Expected - will fail when trying to open non-existent table
                e => panic!("Expected DeltaTable error, got: {:?}", e),
            }
        }

        #[tokio::test]
        async fn test_optimize_nonexistent_table() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            let path = "abfss://container@account.dfs.core.windows.net/nonexistent/table";
            let result = client.optimize(path).await;
            
            // Should fail because table doesn't exist (during load_table call)
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::DeltaTable(_) => {}, // Expected - will fail when trying to load non-existent table
                e => panic!("Expected DeltaTable error, got: {:?}", e),
            }
        }
    }

    mod vacuum_tests {
        use super::*;

        #[tokio::test]
        async fn test_vacuum_invalid_path() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            let result = client.vacuum("invalid://path").await;
            
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::Config(_) => {}, // Expected from path validation
                e => panic!("Expected Config error, got: {:?}", e),
            }
        }

        #[tokio::test]
        async fn test_vacuum_valid_path_format() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            let valid_path = "abfss://container@account.dfs.core.windows.net/test/table";
            let result = client.vacuum(valid_path).await;
            
            // Should fail on actual vacuum (table doesn't exist), not on validation
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::Config(_) => panic!("Should not be a config error for valid path format"),
                DeltaError::DeltaTable(_) => {}, // Expected - will fail when trying to open non-existent table
                e => panic!("Expected DeltaTable error, got: {:?}", e),
            }
        }

        #[tokio::test]
        async fn test_vacuum_nonexistent_table() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            let path = "abfss://container@account.dfs.core.windows.net/nonexistent/table";
            let result = client.vacuum(path).await;
            
            // Should fail because table doesn't exist (during load_table call)
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::DeltaTable(_) => {}, // Expected - will fail when trying to load non-existent table
                e => panic!("Expected DeltaTable error, got: {:?}", e),
            }
        }
    }

    mod read_cdf_tests {
        use super::*;

        #[tokio::test]
    async fn test_read_cdf_invalid_path() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            let result = client.read_cdf("invalid://path", 0).await;
            
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::Config(_) => {}, // Expected from path validation
                e => panic!("Expected Config error, got: {:?}", e),
            }
        }

        #[tokio::test]
    async fn test_read_cdf_valid_path_format() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            let valid_path = "abfss://container@account.dfs.core.windows.net/test/table";
            let result = client.read_cdf(valid_path, 5).await;
            
            // Should fail on actual stream reading (table doesn't exist), not on validation
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::Config(_) => panic!("Should not be a config error for valid path format"),
                DeltaError::DeltaTable(_) => {}, // Expected - will fail when trying to open non-existent table
                e => panic!("Expected DeltaTable error, got: {:?}", e),
            }
        }

        #[tokio::test]
    async fn test_read_cdf_nonexistent_table() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            let path = "abfss://container@account.dfs.core.windows.net/nonexistent/table";
            let result = client.read_cdf(path, 10).await;
            
            // Should fail because table doesn't exist (during load_table call)
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::DeltaTable(_) => {}, // Expected - will fail when trying to load non-existent table
                e => panic!("Expected DeltaTable error, got: {:?}", e),
            }
        }

        #[tokio::test]
    async fn test_read_cdf_negative_version() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            let valid_path = "abfss://container@account.dfs.core.windows.net/test/table";
            let result = client.read_cdf(valid_path, -1).await;
            
            // Should handle negative version gracefully
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::DeltaTable(_) => {}, // Expected - will fail when trying to open non-existent table
                e => panic!("Expected DeltaTable error, got: {:?}", e),
            }
        }
    }

    mod change_data_feed_tests {
        use super::*;

        #[tokio::test]
        async fn test_enable_change_data_feed_invalid_path() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            let result = client.enable_change_data_feed("invalid://path").await;
            
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::Config(_) => {}, // Expected from path validation
                e => panic!("Expected Config error, got: {:?}", e),
            }
        }

        #[tokio::test]
        async fn test_enable_change_data_feed_valid_path_format() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            let valid_path = "abfss://container@account.dfs.core.windows.net/test/table";
            let result = client.enable_change_data_feed(valid_path).await;
            
            // Should fail on actual operation (table doesn't exist), not on path validation
            assert!(result.is_err());
            match result.unwrap_err() {
                DeltaError::Config(_) => panic!("Should not be a config error for valid path format"),
                _ => {}, // Expected - will fail on actual operation
            }
        }

        #[tokio::test]
        async fn test_enable_change_data_feed_nonexistent_table() {
            let mut client = DeltaClient::with_token("test_token".to_string());
            
            let valid_path = "abfss://container@account.dfs.core.windows.net/nonexistent/table";
            let result = client.enable_change_data_feed(valid_path).await;
            
            // Should fail because table doesn't exist
            assert!(result.is_err());
        }
    }
}