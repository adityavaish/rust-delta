//! Tests for utility functions previously colocated in `src/utils.rs`
use delta_operations::utils::{extract_account_name, validate_path, validate_path_with_options, format_batches};
use delta_operations::DeltaError;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::array::{Int32Array, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use std::sync::Arc;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::env;

// -------------------- extract_account_name tests --------------------
#[test]
fn test_extract_account_name_valid() {
    let path = "abfss://container@myaccount.dfs.core.windows.net/path/to/table";
    assert_eq!(extract_account_name(path), Some("myaccount"));
}

#[test]
fn test_extract_account_name_different_accounts() {
    let test_cases = vec![
        ("abfss://container@account1.dfs.core.windows.net/table", Some("account1")),
        ("abfss://data@prodaccount.dfs.core.windows.net/delta", Some("prodaccount")),
        ("abfss://test@devaccount.dfs.core.windows.net/test", Some("devaccount")),
    ];
    for (path, expected) in test_cases { assert_eq!(extract_account_name(path), expected); }
}

#[test]
fn test_extract_account_name_no_at_symbol() { assert_eq!(extract_account_name("abfss://container.dfs.core.windows.net/path/to/table"), None); }

#[test]
fn test_extract_account_name_no_dot() { assert_eq!(extract_account_name("abfss://container@myaccountdfscore/path/to/table"), None); }

#[test]
fn test_extract_account_name_empty_string() { assert_eq!(extract_account_name(""), None); }

#[test]
fn test_extract_account_name_at_at_end() { assert_eq!(extract_account_name("abfss://container@"), None); }

#[test]
fn test_extract_account_name_multiple_at_symbols() { assert_eq!(extract_account_name("abfss://container@account@extra.dfs.core.windows.net/path"), Some("account@extra")); }

#[test]
fn test_extract_account_name_special_characters() { assert_eq!(extract_account_name("abfss://container@my-account_123.dfs.core.windows.net/path"), Some("my-account_123")); }

// -------------------- validate_path tests --------------------
fn create_temp_dir(name: &str) -> std::path::PathBuf {
    let mut dir = env::temp_dir();
    dir.push(format!("delta_utils_test_{}", name));
    create_dir_all(&dir).expect("Failed to create temp test directory");
    dir
}

#[test]
fn test_validate_path_valid() {
    let valid_paths = vec![
        "abfss://container@account.dfs.core.windows.net/path/to/table",
        "abfss://data@myaccount.dfs.core.windows.net/delta/table1",
        "abfss://test@dev.dfs.core.windows.net/tables/users",
    ];
    for path in valid_paths { assert!(validate_path(path).is_ok(), "Should be valid: {path}"); }
}

#[test]
fn test_validate_path_invalid_scheme() {
    let invalid_schemes = vec![
        "s3://bucket/path/to/table",
        "hdfs://namenode:port/path",
        "file:///local/path",
        "http://example.com/path",
        "adls://container@account.dfs.core.windows.net/path",
    ];
    for path in invalid_schemes {
    let result = validate_path(path);
        assert!(result.is_err(), "Should be invalid: {path}");
        match result.unwrap_err() { DeltaError::Config(msg) => assert!(msg.contains("must start with 'abfss://'")), _ => panic!("Expected Config error"), }
    }
}

#[test]
fn test_validate_path_missing_at_symbol() {
    for path in [
        "abfss://container.dfs.core.windows.net/path",
        "abfss://account.dfs.core.windows.net/path",
    ] { let res = validate_path(path); assert!(res.is_err()); }
}

#[test]
fn test_validate_path_missing_dfs_domain() {
    for path in [
        "abfss://container@account.blob.core.windows.net/path",
        "abfss://container@account.file.core.windows.net/path",
        "abfss://container@account.com/path",
    ] { let res = validate_path(path); assert!(res.is_err()); }
}

#[test]
fn test_validate_path_edge_cases() {
    for (path, _desc) in [
        ("", "empty"),
        ("abfss://", "scheme only"),
        ("abfss://container@", "missing domain"),
        ("abfss://container@account", "incomplete domain"),
    ] { assert!(validate_path(path).is_err()); }
}

#[test]
fn test_validate_path_case_sensitivity() {
    for path in [
        "ABFSS://container@account.dfs.core.windows.net/path",
        "Abfss://container@account.dfs.core.windows.net/path",
        "abfSS://container@account.dfs.core.windows.net/path",
    ] { assert!(validate_path(path).is_err()); }
}

#[test]
fn test_validate_local_path_existing_dir() {
    let dir = create_temp_dir("existing_dir");
    assert!(validate_path(dir.to_str().unwrap()).is_ok());
}

#[test]
fn test_validate_local_path_existing_file() {
    let dir = create_temp_dir("existing_file");
    let file_path = dir.join("data.txt");
    let mut f = File::create(&file_path).unwrap();
    writeln!(f, "hello").unwrap();
    assert!(validate_path(file_path.to_str().unwrap()).is_ok());
}

#[test]
fn test_validate_local_path_missing() {
    let dir = create_temp_dir("missing");
    let missing = dir.join("does_not_exist");
    if missing.exists() { std::fs::remove_file(&missing).unwrap(); }
    let res = validate_path(missing.to_str().unwrap());
    assert!(res.is_err());
    match res.unwrap_err() { DeltaError::Config(msg) => assert!(msg.contains("Local path does not exist")), other => panic!("Unexpected error: {other:?}") }
}

// -------------------- format_batches tests --------------------
fn create_test_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let id_array = Arc::new(Int32Array::from(vec![1, 2, 3]));
    let name_array = Arc::new(StringArray::from(vec!["Alice", "Bob", "Charlie"]));
    RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap()
}

#[test]
fn test_format_batches_empty() {
    let batches = vec![]; let result = format_batches(&batches).unwrap(); assert_eq!(result, "(no rows)");
}

#[test]
fn test_format_batches_single_batch() {
    let batch = create_test_batch(); let batches = vec![batch]; let result = format_batches(&batches).unwrap();
    assert!(result.contains("Alice") && result.contains("Rows: 3"));
}

#[test]
fn test_format_batches_multiple_batches() {
    let batch1 = create_test_batch();
    let batch2 = {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]));
        let id_array = Arc::new(Int32Array::from(vec![4,5]));
        let name_array = Arc::new(StringArray::from(vec!["David", "Eve"]));
        RecordBatch::try_new(schema, vec![id_array, name_array]).unwrap()
    };
    let batches = vec![batch1, batch2];
    let result = format_batches(&batches).unwrap();
    assert!(result.contains("Alice") && result.contains("David") && result.contains("Rows: 5"));
}

#[test]
fn test_format_batches_empty_batch() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("name", DataType::Utf8, true),
    ]));
    let empty_batch = RecordBatch::new_empty(schema);
    let result = format_batches(&[empty_batch]).unwrap();
    assert!(result.contains("Rows: 0"));
}

#[test]
fn test_format_batches_different_schemas() {
    let batch1 = create_test_batch();
    let batch2 = {
        let schema = Arc::new(Schema::new(vec![ Field::new("value", DataType::Int32, false) ]));
        let value_array = Arc::new(Int32Array::from(vec![100]));
        RecordBatch::try_new(schema, vec![value_array]).unwrap()
    };
    let result = format_batches(&[batch1, batch2]);
    match result { Ok(r) => assert!(r.contains("Rows:")), Err(DeltaError::General(msg)) => assert!(msg.contains("Failed to format batches")), Err(e) => panic!("Unexpected error: {e:?}") }
}

#[test]
fn test_format_batches_large_data() {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("value", DataType::Utf8, true),
    ]));
    let ids: Vec<i32> = (1..=1000).collect();
    let values: Vec<String> = (1..=1000).map(|i| format!("value_{}", i)).collect();
    let id_array = Arc::new(Int32Array::from(ids));
    let value_array = Arc::new(StringArray::from(values));
    let batch = RecordBatch::try_new(schema, vec![id_array, value_array]).unwrap();
    let result = format_batches(&[batch]).unwrap();
    assert!(result.contains("Rows: 1000"));
}

// -------------------- quote_mixed_case_identifiers tests --------------------
#[tokio::test]
async fn test_quote_mixed_case_identifiers_no_loaded_tables() {
    use delta_operations::DeltaClient;
    let client = DeltaClient::with_token("token".to_string());
    let sql = "select id, Name, AMOUNT from t where amount > 10"; // Mixed-case tokens but no schema => unchanged
    let rewritten = client.quote_mixed_case_identifiers(sql).await.unwrap();
    assert_eq!(sql, rewritten);
}

// (A fuller test would require a real table with mixed-case column names; omitted to keep test self-contained.)

// -------------------- integration-style util tests --------------------
#[test]
fn test_extract_and_validate_together() {
    let path = "abfss://container@myaccount.dfs.core.windows.net/path/to/table";
    assert!(validate_path(path).is_ok());
    assert_eq!(extract_account_name(path), Some("myaccount"));
}

#[test]
fn test_path_without_account_still_validates() {
    let path = "abfss://container@.dfs.core.windows.net/path";
    assert!(validate_path(path).is_ok());
    assert_eq!(extract_account_name(path), Some(""));
}

#[test]
fn test_utils_error_types() {
    let result = validate_path("invalid://path");
    assert!(result.is_err());
    match result.unwrap_err() { DeltaError::Config(_) => {}, other => panic!("Expected Config error, got: {other:?}") }
}

// Deprecated alias smoke test (will produce a deprecation warning which is acceptable here)
#[test]
fn test_deprecated_validate_path_alias() {
    let path = "abfss://container@account.dfs.core.windows.net/table";
    let _ = validate_path(path).unwrap();
}

// New tests for validate_path_with_options existence toggle
#[test]
fn test_validate_local_path_missing_no_check() {
    let mut dir = env::temp_dir();
    dir.push("delta_utils_test_missing_optional");
    // Ensure directory does not exist
    if dir.exists() { std::fs::remove_dir_all(&dir).unwrap(); }
    // Should succeed when existence check disabled
    assert!(validate_path_with_options(dir.to_str().unwrap(), false).is_ok());
}

#[test]
fn test_validate_local_path_missing_with_check_still_errors() {
    let mut dir = env::temp_dir();
    dir.push("delta_utils_test_missing_optional_error");
    if dir.exists() { std::fs::remove_dir_all(&dir).unwrap(); }
    let res = validate_path_with_options(dir.to_str().unwrap(), true);
    assert!(res.is_err());
}
