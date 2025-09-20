//! Unit tests for the operations module

#[cfg(test)]
mod operations_tests {
    use delta_operations::operations::{DeltaOperations, WriteMode};
    use datafusion::prelude::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::array::{Int32Array, StringArray};
    use std::sync::Arc;

    fn create_test_operations() -> DeltaOperations {
        DeltaOperations::new("test_token_12345".to_string())
    }

    #[test]
    fn test_delta_operations_creation() {
        let token = "test_token_value".to_string();
        let ops = DeltaOperations::new(token.clone());
        
        // Verify the operations object was created
        let debug_str = format!("{:?}", ops);
        assert!(debug_str.contains("DeltaOperations"));
        assert!(debug_str.contains("test_token_value"));
    }

    #[test]
    fn test_write_mode_conversion() {
        use deltalake::protocol::SaveMode;
        assert!(matches!(SaveMode::from(WriteMode::Append), SaveMode::Append));
        assert!(matches!(SaveMode::from(WriteMode::Overwrite), SaveMode::Overwrite));
    }

    #[test]
    fn test_write_mode_debug() {
        assert_eq!(format!("{:?}", WriteMode::Append), "Append");
        assert_eq!(format!("{:?}", WriteMode::Overwrite), "Overwrite");
    }

    #[test]
    fn test_write_mode_clone_copy() {
        let mode = WriteMode::Append;
        let cloned = mode.clone();
        let copied = mode;
        assert!(matches!(cloned, WriteMode::Append));
        assert!(matches!(copied, WriteMode::Append));
    }

    // QueryResult tests removed

    mod storage_options_tests {
        use super::*;

        #[test]
        fn test_create_storage_options_valid_path() {
            let ops = create_test_operations();
            
            let path = "abfss://container@testaccount.dfs.core.windows.net/path/to/table";
            let result = ops.create_storage_options(path);
            
            assert!(result.is_ok());
            let opts = result.unwrap();
            
            assert_eq!(opts.get("azure_storage_account_name"), Some(&"testaccount".to_string()));
            assert_eq!(opts.get("azure_storage_token"), Some(&"test_token_12345".to_string()));
        }

        #[test]
        fn test_create_storage_options_no_account() {
            let ops = create_test_operations();
            
            let path = "abfss://container/path/to/table"; // No @ symbol
            let result = ops.create_storage_options(path);
            
            assert!(result.is_ok());
            let opts = result.unwrap();
            
            // Should use default account when extraction fails
            assert_eq!(opts.get("azure_storage_account_name"), Some(&"defaultaccount".to_string()));
            assert_eq!(opts.get("azure_storage_token"), Some(&"test_token_12345".to_string()));
        }

        #[test]
        fn test_create_storage_options_different_tokens() {
            let token1 = "token_123".to_string();
            let token2 = "token_456".to_string();
            
            let ops1 = DeltaOperations::new(token1.clone());
            let ops2 = DeltaOperations::new(token2.clone());
            
            let path = "abfss://container@account.dfs.core.windows.net/table";
            
            let opts1 = ops1.create_storage_options(path).unwrap();
            let opts2 = ops2.create_storage_options(path).unwrap();
            
            assert_eq!(opts1.get("azure_storage_token"), Some(&token1));
            assert_eq!(opts2.get("azure_storage_token"), Some(&token2));
        }
    }

    mod load_table_tests {
        use super::*;

        #[tokio::test]
        async fn test_load_table_invalid_path() {
            let ops = create_test_operations();
            
            let result = ops.load_table("invalid://path").await;
            
            // Should fail when trying to load from invalid path
            assert!(result.is_err());
        }

        #[tokio::test]
        async fn test_load_table_valid_path_format() {
            let ops = create_test_operations();
            
            // Valid format but non-existent table
            let path = "abfss://container@account.dfs.core.windows.net/nonexistent/table";
            let result = ops.load_table(path).await;
            
            // Should fail because table doesn't exist, but not due to path format
            assert!(result.is_err());
        }
    }

    mod write_dataframe_tests {
        use super::*;

        fn _create_test_record_batch() -> RecordBatch {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, true),
            ]));
            
            let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
            let name_array = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]));
            
            RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap()
        }

        #[tokio::test]
        async fn test_write_dataframe_empty_data() {
            let ops = create_test_operations();
            let ctx = SessionContext::new();
            
            // Create an empty DataFrame
            let df = ctx.sql("SELECT 1 as id WHERE 1 = 0").await.unwrap(); // Results in empty set
            
            let path = "abfss://container@account.dfs.core.windows.net/test/table";
            let result = ops.write_dataframe(df, path, WriteMode::Overwrite).await;
            
            // Should fail with "No data to write" error
            assert!(result.is_err());
            if let Err(delta_operations::DeltaError::General(msg)) = result {
                assert!(msg.contains("No data to write"));
            } else {
                panic!("Expected General error with 'No data to write' message");
            }
        }

        #[tokio::test]
        async fn test_write_dataframe_with_data() {
            let ops = create_test_operations();
            let ctx = SessionContext::new();
            
            // Create DataFrame with data
            let df = ctx.sql("SELECT * FROM VALUES (1, 'test'), (2, 'data') AS t(id, name)").await.unwrap();
            
            let path = "memory://test/table"; // Use memory scheme for testing
            let result = ops.write_dataframe(df, path, WriteMode::Append).await;
            
            // Should fail due to no actual Azure access, but not due to empty data
            assert!(result.is_err());
            // Should NOT be the "No data to write" error
            if let Err(delta_operations::DeltaError::General(msg)) = &result {
                assert!(!msg.contains("No data to write"));
            }
        }

        #[tokio::test]
        async fn test_write_modes() {
            let ops = create_test_operations();
            let ctx = SessionContext::new();
            
            let df = ctx.sql("SELECT 1 as id, 'test' as name").await.unwrap();
            let path = "memory://test/table"; // Use memory scheme for testing
            
            // Test different write modes (all will fail due to no Azure access, but test the interface)
            let modes = vec![WriteMode::Append, WriteMode::Overwrite];
            
            for mode in modes {
                let result = ops.write_dataframe(df.clone(), path, mode).await;
                assert!(result.is_err()); // Expected to fail without Azure access
            }
        }
    }

    // create_table_with_schema tests removed

    mod edge_cases_tests {
        use super::*;

        #[test]
        fn test_empty_token() {
            let ops = DeltaOperations::new("".to_string());
            let debug_str = format!("{:?}", ops);
            assert!(debug_str.contains("DeltaOperations"));
        }

        #[test]
        fn test_very_long_token() {
            let long_token = "a".repeat(10000);
            let ops = DeltaOperations::new(long_token.clone());
            
            let path = "abfss://container@account.dfs.core.windows.net/table";
            let opts = ops.create_storage_options(path).unwrap();
            
            assert_eq!(opts.get("azure_storage_token"), Some(&long_token));
        }

        #[test]
        fn test_special_characters_in_token() {
            let special_token = "token!@#$%^&*()_+-=[]{}|;':\",./<>?`~".to_string();
            let ops = DeltaOperations::new(special_token.clone());
            
            let path = "abfss://container@account.dfs.core.windows.net/table";
            let opts = ops.create_storage_options(path).unwrap();
            
            assert_eq!(opts.get("azure_storage_token"), Some(&special_token));
        }
    }
}