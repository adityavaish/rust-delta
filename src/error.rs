//! Error types for Delta operations

use std::fmt;

/// Result type alias for Delta operations
pub type Result<T> = std::result::Result<T, DeltaError>;

/// Main error type for Delta operations
#[derive(Debug)]
pub enum DeltaError {
    /// Authentication errors
    Authentication(String),
    /// Delta table operation errors
    DeltaTable(String),
    /// SQL query errors
    Query(String),
    /// File system/storage errors
    Storage(String),
    /// Configuration errors
    Config(String),
    /// General errors
    General(String),
}

impl fmt::Display for DeltaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DeltaError::Authentication(msg) => write!(f, "Authentication error: {}", msg),
            DeltaError::DeltaTable(msg) => write!(f, "Delta table error: {}", msg),
            DeltaError::Query(msg) => write!(f, "Query error: {}", msg),
            DeltaError::Storage(msg) => write!(f, "Storage error: {}", msg),
            DeltaError::Config(msg) => write!(f, "Configuration error: {}", msg),
            DeltaError::General(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for DeltaError {}

impl From<anyhow::Error> for DeltaError {
    fn from(err: anyhow::Error) -> Self {
        DeltaError::General(err.to_string())
    }
}

impl From<deltalake::DeltaTableError> for DeltaError {
    fn from(err: deltalake::DeltaTableError) -> Self {
        DeltaError::DeltaTable(err.to_string())
    }
}

impl From<datafusion::error::DataFusionError> for DeltaError {
    fn from(err: datafusion::error::DataFusionError) -> Self {
        DeltaError::Query(err.to_string())
    }
}

impl From<azure_core::error::Error> for DeltaError {
    fn from(err: azure_core::error::Error) -> Self {
        DeltaError::Authentication(err.to_string())
    }
}
