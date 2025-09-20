//! Unit tests for the error module

#[cfg(test)]
mod error_tests {
    use delta_operations::{DeltaError, Result};
    use anyhow::anyhow;
    use datafusion::error::DataFusionError;
    use azure_core::error::Error as AzureError;

    #[test]
    fn test_delta_error_variants() {
        let auth_error = DeltaError::Authentication("Invalid credentials".to_string());
        let delta_error = DeltaError::DeltaTable("Table not found".to_string());
        let query_error = DeltaError::Query("Invalid SQL syntax".to_string());
        let storage_error = DeltaError::Storage("Connection failed".to_string());
        let config_error = DeltaError::Config("Invalid configuration".to_string());
        let general_error = DeltaError::General("Something went wrong".to_string());

        // Test Debug trait
        assert!(format!("{:?}", auth_error).contains("Authentication"));
        assert!(format!("{:?}", delta_error).contains("DeltaTable"));
        assert!(format!("{:?}", query_error).contains("Query"));
        assert!(format!("{:?}", storage_error).contains("Storage"));
        assert!(format!("{:?}", config_error).contains("Config"));
        assert!(format!("{:?}", general_error).contains("General"));
    }

    #[test]
    fn test_delta_error_display() {
        let test_cases = vec![
            (
                DeltaError::Authentication("auth failed".to_string()),
                "Authentication error: auth failed",
            ),
            (
                DeltaError::DeltaTable("table error".to_string()),
                "Delta table error: table error",
            ),
            (
                DeltaError::Query("sql error".to_string()),
                "Query error: sql error",
            ),
            (
                DeltaError::Storage("storage error".to_string()),
                "Storage error: storage error",
            ),
            (
                DeltaError::Config("config error".to_string()),
                "Configuration error: config error",
            ),
            (
                DeltaError::General("general error".to_string()),
                "Error: general error",
            ),
        ];

        for (error, expected_display) in test_cases {
            assert_eq!(format!("{}", error), expected_display);
        }
    }

    #[test]
    fn test_delta_error_std_error_trait() {
        let error = DeltaError::General("test error".to_string());
        
        // Test that it implements std::error::Error
        let _: &dyn std::error::Error = &error;
        
        // Test error description via Display
        assert_eq!(error.to_string(), "Error: test error");
    }

    #[test]
    fn test_result_type_alias() {
        // Test successful result
        let success: Result<i32> = Ok(42);
        assert_eq!(success.unwrap(), 42);

        // Test error result
        let error: Result<i32> = Err(DeltaError::General("test".to_string()));
        assert!(error.is_err());
    }

    #[test]
    fn test_from_anyhow_error() {
        let anyhow_error = anyhow!("This is an anyhow error");
        let delta_error: DeltaError = anyhow_error.into();
        
        match delta_error {
            DeltaError::General(msg) => {
                assert!(msg.contains("This is an anyhow error"));
            }
            _ => panic!("Expected General error variant"),
        }
    }

    #[test]
    fn test_from_datafusion_error() {
        let df_error = DataFusionError::Plan("Invalid plan".to_string());
        let delta_error: DeltaError = df_error.into();
        
        match delta_error {
            DeltaError::Query(msg) => {
                assert!(msg.contains("Invalid plan"));
            }
            _ => panic!("Expected Query error variant"),
        }
    }

    #[test]
    fn test_from_azure_error() {
        // Create a simple Azure error for testing
        let azure_error = AzureError::new(
            azure_core::error::ErrorKind::Credential, 
            "Azure authentication failed"
        );
        let delta_error: DeltaError = azure_error.into();
        
        match delta_error {
            DeltaError::Authentication(msg) => {
                assert!(msg.contains("Azure authentication failed"));
            }
            _ => panic!("Expected Authentication error variant"),
        }
    }

    #[test]
    fn test_error_chaining() {
        fn create_nested_error() -> Result<()> {
            Err(DeltaError::Query("Inner query error".to_string()))
        }
        
        fn outer_function() -> Result<()> {
            create_nested_error().map_err(|e| match e {
                DeltaError::Query(msg) => DeltaError::General(format!("Outer error: {}", msg)),
                other => other,
            })
        }
        
        let result = outer_function();
        assert!(result.is_err());
        
        match result.unwrap_err() {
            DeltaError::General(msg) => {
                assert!(msg.contains("Outer error"));
                assert!(msg.contains("Inner query error"));
            }
            _ => panic!("Expected General error with chained message"),
        }
    }

    #[test]
    fn test_error_propagation() {
        fn function_that_fails() -> Result<i32> {
            Err(DeltaError::Storage("Storage unavailable".to_string()))
        }
        
        fn calling_function() -> Result<String> {
            let _value = function_that_fails()?; // Propagate error
            Ok("success".to_string())
        }
        
        let result = calling_function();
        assert!(result.is_err());
        
        match result.unwrap_err() {
            DeltaError::Storage(msg) => {
                assert_eq!(msg, "Storage unavailable");
            }
            _ => panic!("Expected Storage error"),
        }
    }

    #[test]
    fn test_error_with_empty_messages() {
        let errors = vec![
            DeltaError::Authentication(String::new()),
            DeltaError::DeltaTable(String::new()),
            DeltaError::Query(String::new()),
            DeltaError::Storage(String::new()),
            DeltaError::Config(String::new()),
            DeltaError::General(String::new()),
        ];

        let expected_displays = vec![
            "Authentication error: ",
            "Delta table error: ",
            "Query error: ",
            "Storage error: ",
            "Configuration error: ",
            "Error: ",
        ];

        for (error, expected) in errors.into_iter().zip(expected_displays) {
            assert_eq!(format!("{}", error), expected);
        }
    }

    #[test]
    fn test_error_with_special_characters() {
        let special_msg = "Error with special chars: !@#$%^&*()_+-={}[]|;':\",./<>?`~";
        let error = DeltaError::General(special_msg.to_string());
        
        assert_eq!(format!("{}", error), format!("Error: {}", special_msg));
    }

    #[test]
    fn test_error_with_multiline_message() {
        let multiline_msg = "This is line 1\nThis is line 2\nThis is line 3";
        let error = DeltaError::Query(multiline_msg.to_string());
        
        let displayed = format!("{}", error);
        assert!(displayed.contains("This is line 1"));
        assert!(displayed.contains("This is line 2"));
        assert!(displayed.contains("This is line 3"));
    }

    #[test]
    fn test_error_size() {
        use std::mem;
        
        // Verify that DeltaError has a reasonable size
        let size = mem::size_of::<DeltaError>();
        
        // Should be reasonable (String + discriminant)
        // This is mostly to catch accidental size increases
        assert!(size < 100, "DeltaError size is {} bytes, which seems large", size);
    }

    #[test]
    fn test_result_combinators() {
        let success: Result<i32> = Ok(42);
        let failure: Result<i32> = Err(DeltaError::General("fail".to_string()));
        
        // Test map
        let mapped_success = success.map(|x| x * 2);
        assert_eq!(mapped_success.unwrap(), 84);
        
        let mapped_failure = failure.map(|x| x * 2);
        assert!(mapped_failure.is_err());
        
        // Test and_then
        let chained_success: Result<i32> = Ok(21).and_then(|x: i32| Ok(x * 2));
        assert_eq!(chained_success.unwrap(), 42);
        
        let chained_failure: Result<i32> = Err(DeltaError::General("fail".to_string()))
            .and_then(|x: i32| Ok(x * 2));
        assert!(chained_failure.is_err());
        
        // Test or_else
        let recovered: Result<i32> = Err(DeltaError::General("fail".to_string()))
            .or_else(|_| Ok(100));
        assert_eq!(recovered.unwrap(), 100);
    }

    mod error_conversion_edge_cases {
        use super::*;

        #[test]
        fn test_very_long_error_message() {
            let long_msg = "a".repeat(10000);
            let error = DeltaError::General(long_msg.clone());
            
            let displayed = format!("{}", error);
            assert!(displayed.contains(&long_msg));
        }

        #[test]
        fn test_unicode_error_message() {
            let unicode_msg = "Error with unicode: 🚀 ñ 中文 🎉";
            let error = DeltaError::Authentication(unicode_msg.to_string());
            
            let displayed = format!("{}", error);
            assert!(displayed.contains("🚀"));
            assert!(displayed.contains("中文"));
            assert!(displayed.contains("🎉"));
        }

        #[test]
        fn test_nested_anyhow_error() {
            let inner_error = anyhow!("Inner error");
            let outer_error = anyhow!("Outer error").context(inner_error);
            let delta_error: DeltaError = outer_error.into();
            
            match delta_error {
                DeltaError::General(msg) => {
                    // Should contain information about the error chain
                    assert!(msg.len() > 0);
                }
                _ => panic!("Expected General error"),
            }
        }
    }
}