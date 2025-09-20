//! Utility functions for Delta operations

use crate::error::{DeltaError, Result};
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::arrow::record_batch::RecordBatch;
use std::path::Path;

/// Extract account name from an Azure Data Lake Storage path
pub fn extract_account_name(path: &str) -> Option<&str> {
    // Expect pattern: abfss://<container>@<account>.dfs.core.windows.net/...
    let at_pos = path.find('@')?;
    let after_at = &path[at_pos + 1..];
    let dot_pos = after_at.find('.')?;
    Some(&after_at[..dot_pos])
}

/// Format RecordBatches for pretty printing
pub fn format_batches(batches: &[RecordBatch]) -> Result<String> {
    if batches.is_empty() {
        return Ok("(no rows)".to_string());
    }
    
    let formatted = pretty_format_batches(batches)
        .map_err(|e| DeltaError::General(format!("Failed to format batches: {}", e)))?;
    
    let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
    Ok(format!("{}\nRows: {}", formatted, row_count))
}

/// Generic path validation supporting both local filesystem and ABFSS paths.
///
/// Rules:
/// - If the string contains "://" then it is treated as a URL-style path and must be an ABFSS path
/// - ABFSS paths are validated for correct structural format (same logic as before)
/// - Local paths (no scheme) are validated for non-empty & existence on the local filesystem
///
/// Note: We purposefully do not attempt a network call to verify remote ABFSS existence here
/// to keep validation fast and deterministic for unit tests. A future enhancement could add
/// an async existence check behind a feature flag.
/// Validate a path which may be an ABFSS (Azure Data Lake Storage) URI or a local filesystem path.
/// Prefer using this function instead of `validate_path` (deprecated alias).
pub fn validate_path_with_options(path: &str, check_for_existence: bool) -> Result<()> {
    if path.trim().is_empty() {
        return Err(DeltaError::Config("Path cannot be empty".to_string()));
    }

    let has_scheme_like = path.contains("://");

    if has_scheme_like {
        if !path.starts_with("abfss://") {
            return Err(DeltaError::Config(
                "Path must start with 'abfss://' or be a local filesystem path".to_string(),
            ));
        }
        if !path.contains('@') {
            return Err(DeltaError::Config(
                "Path must contain '@' to specify storage account".to_string(),
            ));
        }
        if !path.contains(".dfs.core.windows.net") {
            return Err(DeltaError::Config(
                "Path must contain '.dfs.core.windows.net' for Azure Data Lake Storage".to_string(),
            ));
        }
        return Ok(());
    }

    if check_for_existence {
        let p = Path::new(path);
        if !p.exists() {
            return Err(DeltaError::Config(format!("Local path does not exist: {}", path)));
        }
    }
    Ok(())
}

/// Backwards-compatible default: existence check ON.
pub fn validate_path(path: &str) -> Result<()> { validate_path_with_options(path, true) }