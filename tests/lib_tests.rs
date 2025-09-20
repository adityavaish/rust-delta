//! Unit tests for the library module and exports

#[cfg(test)]
mod lib_tests {
    // Test that all public exports are accessible
    use delta_operations::{
        DeltaClient, DeltaError, Result,
        DataFrame, DeltaTable,
        WriteMode,
    };
    use datafusion::prelude::*;

    #[test]
    fn test_public_exports_accessible() {
        // Test that we can reference all the main types
        let _: Option<DeltaClient> = None;
        let _: Option<DeltaError> = None;
        let _: Option<WriteMode> = None;
    // Removed QueryResult and DataFrameExt
        
        // Test Result type alias
        let _success: Result<i32> = Ok(42);
        let _failure: Result<i32> = Err(DeltaError::General("test".to_string()));
    }

    #[test]
    fn test_write_mode_export() {
        // Only remaining variants
        let _append = WriteMode::Append;
        let _overwrite = WriteMode::Overwrite;
    }

    #[test]
    fn test_error_types_export() {
        // Test that all error variants are accessible
        let _auth = DeltaError::Authentication("test".to_string());
        let _delta = DeltaError::DeltaTable("test".to_string());
        let _query = DeltaError::Query("test".to_string());
        let _storage = DeltaError::Storage("test".to_string());
        let _config = DeltaError::Config("test".to_string());
        let _general = DeltaError::General("test".to_string());
    }

    #[tokio::test]
    async fn test_client_creation_export() {
        // Test that we can create clients using exported functions
        let _client = DeltaClient::with_token("test_token".to_string());
        
        // Note: DeltaClient::new() would require Azure credentials, so we don't test that here
    }

    #[tokio::test]
    async fn test_dataframe_basic_query() {
        let ctx = SessionContext::new();
        let df = ctx.sql("SELECT 1 as id").await.unwrap();
        assert_eq!(df.schema().fields().len(), 1);
    }

    #[tokio::test]
    async fn test_dataframe_direct_usage() {
        let ctx = SessionContext::new();
        if let Ok(df) = ctx.sql("SELECT 1 as test").await {
            let batches = df.collect().await.unwrap();
            assert_eq!(batches.len(), 1);
        }
    }

    #[tokio::test]
    async fn test_datafusion_types_export() {
        // Test that we can use re-exported DataFusion types
        let ctx = SessionContext::new();
        
        // These should compile if the exports are working
        let _: Option<DataFrame> = None;
        let _: Option<DeltaTable> = None;
        
        // Test basic DataFusion operations work with exports
        if let Ok(_df) = ctx.sql("SELECT 1").await {
            // DataFrame operations should be available
        }
    }

    #[test]
    fn test_result_combinators_with_exports() {
        // Test that Result type works with standard combinators
        let success: Result<i32> = Ok(42);
        let failure: Result<i32> = Err(DeltaError::General("fail".to_string()));
        
        // Test map
        let mapped = success.map(|x| x * 2);
        assert_eq!(mapped.unwrap(), 84);
        
        // Test and_then
        let chained: Result<String> = Ok(21).and_then(|x| Ok(format!("Value: {}", x)));
        assert!(chained.is_ok());
        
        // Test error propagation
        let propagated: Result<i32> = failure.map(|x| x + 1);
        assert!(propagated.is_err());
    }

    mod backward_compatibility_tests {
        use super::*;

        #[tokio::test]
        async fn test_legacy_exports() {
            // Test that legacy exports still work for backward compatibility
            
            // WriteMode should be available both directly and as legacy export
            let _mode1 = WriteMode::Append;
            
            // QueryResult removed; legacy export no longer tested
        }

        #[test]
        fn test_import_patterns() {
            // Test various import patterns that users might use
            
            // Direct imports
            use delta_operations::DeltaClient;
            use delta_operations::WriteMode;
            use delta_operations::DeltaError;
            
            let _client = DeltaClient::with_token("test".to_string());
            let _mode = WriteMode::Overwrite;
            let _error = DeltaError::General("test".to_string());
            
            // Glob import (testing that it doesn't conflict)
            // This would be `use delta_operations::*;` but we can't test that in a module
            // where we've already imported specific items
        }
    }

    mod api_stability_tests {
        use super::*;

        #[test]
        fn test_public_api_surface() {
            // This test documents the expected public API surface
            // Changes here indicate potential breaking changes
            
            // Core client
            let _client = DeltaClient::with_token("test".to_string());
            
            // Error handling
            let _error: Result<()> = Err(DeltaError::General("test".to_string()));
            
            // Write modes
            let _modes = [WriteMode::Append, WriteMode::Overwrite];
        }

        #[test]
        fn test_trait_bounds() {
            // Test that our types implement expected traits
            
            // Debug should be implemented
            let error = DeltaError::General("test".to_string());
            let _debug = format!("{:?}", error);
            
            // Display should be implemented for errors
            let _display = format!("{}", error);
            
            // WriteMode should be Debug, Clone, Copy (still)
            let mode = WriteMode::Append;
            let _cloned = mode.clone();
            let _copied = mode;
            let _debug = format!("{:?}", mode);
        }
    }

    mod documentation_tests {
        use super::*;

        #[tokio::test]
        async fn test_readme_example_compiles() {
            // This test ensures the example from the lib.rs documentation compiles
            // (though it won't run successfully without Azure credentials)
            
            let client = DeltaClient::with_token("test_token".to_string());
            
            // This part should compile even if it fails at runtime
            let df_result = client.sql("SELECT * FROM VALUES (1, 'John'), (2, 'Jane') AS customers(id, name)").await;
            
            match df_result {
                Ok(_df) => {
                    // In a real scenario with Azure access, this would work:
                    // df.save_delta(&client, "abfss://container@account.dfs.core.windows.net/output").await?;
                    println!("DataFrame created successfully");
                }
                Err(_) => {
                    // Expected in test environment without full setup
                    println!("DataFrame creation failed as expected in test environment");
                }
            }
        }

        #[test]
        fn test_type_aliases() {
            // Test that type aliases work as expected
            let _success: Result<String> = Ok("test".to_string());
            let _failure: Result<String> = Err(DeltaError::General("error".to_string()));
            
            // Should be equivalent to std::result::Result<T, DeltaError>
            let _explicit: std::result::Result<String, DeltaError> = Ok("test".to_string());
        }
    }
}