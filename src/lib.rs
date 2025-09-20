//! # Delta Operations Library - Super Simple!
//!
//! The simplest way to work with Delta Lake on Azure Data Lake Storage.
//! 
//! ## Quick Start
//!
//! ```rust,no_run
//! use delta_operations::DeltaClient;
//! use datafusion::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Create client with token
//!     let mut client = DeltaClient::with_token("your_token".to_string());
//! 
//!     // Example: create a small DataFrame
//!     let df = client.sql("SELECT * FROM VALUES (1, 'John'), (2, 'Jane') AS customers(id, name)").await?;
//! 
//!     // Write the DataFrame to a Delta table (overwrite)
//!     let path = "abfss://container@account.dfs.core.windows.net/output/customers";
//!     let _table = client.write(df.clone(), path, delta_operations::WriteMode::Overwrite).await?;
//! 
//!     // Perform a merge (upsert) with new data
//!     let new_data = client.sql("SELECT * FROM VALUES (2, 'Jane Smith'), (3, 'Bob') AS upd(id, name)").await?;
//!     let merge_condition = col("upd.id").eq(col("customers.id"));
//!     let match_condition = lit(true); // always update when matched
//!     let _merged = client.merge(
//!         new_data,
//!         path,
//!         merge_condition,
//!         match_condition,
//!         None,   // auto-generate update expressions (replace all columns)
//!         None,   // auto-generate insert expressions
//!         true,   // insert if not matched
//!         false   // don't delete matched rows
//!     ).await?;
//! 
//!     Ok(())
//! }
//! ```

// Core modules
mod error;
pub mod operations;
pub mod utils;

// Keep the working modules
mod client;
// dataframe module removed (merge via client API directly)

// Simple exports - just what you need!
pub use client::DeltaClient;
pub use error::{DeltaError, Result};

// Useful types
pub use datafusion::prelude::DataFrame;
pub use deltalake::DeltaTable;

pub use operations::WriteMode;

// Additional example:
// Run the minimal merge example:
//   cargo run --example merge_minimal
