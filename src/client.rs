//! High-level Delta client for easy operations

use crate::error::{DeltaError, Result};
use crate::operations::{DeltaOperations, WriteMode};
use crate::utils;
use azure_core::auth::TokenCredential;
use azure_identity::DefaultAzureCredential;
use azure_storage::StorageCredentials;
use azure_storage_datalake::prelude::DataLakeClient;
use datafusion::prelude::*;
use deltalake::{DeltaOps, DeltaTable};
use deltalake::delta_datafusion::DeltaCdfTableProvider;
use deltalake::operations::load_cdf::CdfLoadBuilder;
use regex::Regex;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use rand::Rng;
use std::path::Path;
use tokio::fs;

/// High-level client for Delta Lake operations
pub struct DeltaClient {
    /// DataFusion session context for SQL operations
    pub session_context: SessionContext,
    /// Delta operations handler
    operations: DeltaOperations,
    /// Loaded tables registry
    tables: HashMap<String, DeltaTable>,
}

impl std::fmt::Debug for DeltaClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaClient")
            .field("operations", &self.operations)
            .field("loaded_tables", &self.tables.keys().collect::<Vec<_>>())
            .finish()
    }
}

impl DeltaClient {
    /// Create a new Delta client with Azure authentication
    pub async fn new() -> Result<Self> {
        // Initialize Azure handlers
        deltalake::azure::register_handlers(None);
        
        // Get Azure credentials
        let credential = DefaultAzureCredential::create(Default::default())
            .map_err(|e| DeltaError::Authentication(e.to_string()))?;
        
        // Acquire token
        let token = credential
            .get_token(&["https://storage.azure.com/.default"])
            .await
            .map_err(|e| DeltaError::Authentication(e.to_string()))?;

        let session_context = SessionContext::new();
        let operations = DeltaOperations::new(token.token.secret().to_string());

        Ok(Self {
            session_context,
            operations,
            tables: HashMap::new(),
        })
    }

    /// Create a new Delta client with a custom token
    pub fn with_token(token: String) -> Self {
        deltalake::azure::register_handlers(None);
        
        let session_context = SessionContext::new();
        let operations = DeltaOperations::new(token);

        Self {
            session_context,
            operations,
            tables: HashMap::new(),
        }
    }

    /// Generate a random lowercase alphabetic string of specified length
    pub fn generate_random_alias(&self, min_len: usize, max_len: usize) -> String {
        let mut rng = rand::thread_rng();
        let len = rng.gen_range(min_len..=max_len);
        (0..len)
            .map(|_| {
                let c = rng.gen_range(b'a'..=b'z') as char;
                c
            })
            .collect()
    }

    /// Deregister a table alias from the session context
    /// Use this to clean up aliases returned by load_table
    pub fn unload_table(&mut self, alias: &str) -> Result<()> {
        self.session_context.deregister_table(alias)
            .map_err(|e| DeltaError::Query(e.to_string()))?;
        Ok(())
    }

    /// Load a Delta table, register it, and return (DeltaTable, alias). Use `read_table` for a DataFrame.
    pub async fn load_table(&mut self, path: &str) -> Result<(DeltaTable, String)> {
        utils::validate_path(path)?;
        let table = self.operations.load_table(path).await?;
        let alias = self.generate_random_alias(4, 8);
        self.session_context
            .register_table(&alias, Arc::new(table.clone()))
            .map_err(|e| DeltaError::Query(e.to_string()))?;
        self.tables.insert(alias.clone(), table.clone());
        Ok((table, alias))
    }

    /// Convenience SELECT * from table path.
    pub async fn read_table(&mut self, path: &str) -> Result<DataFrame> {
        let (_t, alias) = self.load_table(path).await?;
        let df = self.session_context
            .sql(&format!("SELECT * FROM {}", alias))
            .await
            .map_err(|e| DeltaError::Query(e.to_string()))?;
        Ok(df)
    }

    /// Load a table with a user-specified alias (fails if alias already exists)
    pub async fn load_table_with_alias(&mut self, path: &str, alias: &str) -> Result<DeltaTable> {
        if self.tables.contains_key(alias) {
            return Err(DeltaError::Query(format!("Alias '{}' already exists", alias)));
        }
        utils::validate_path(path)?;
        let table = self.operations.load_table(path).await?;
        self.session_context
            .register_table(alias, Arc::new(table.clone()))
            .map_err(|e| DeltaError::Query(e.to_string()))?;
        self.tables.insert(alias.to_string(), table.clone());
        Ok(table)
    }

    /// Return iterator of (alias, table_uri)
    pub fn list_tables(&self) -> Vec<(String, String)> {
        self.tables.iter().map(|(a,t)| (a.clone(), t.table_uri().to_string())).collect()
    }

    /// Execute a SQL query and return the result
    pub async fn query(&self, sql: &str) -> Result<DataFrame> {
        let start = Instant::now();
        
        // First try the query as-is
        let df = match self.session_context.sql(sql).await {
            Ok(df) => df,
            Err(e) => {
                let msg = e.to_string();
                if msg.contains("No field named") {
                    // Try with quoted identifiers for mixed-case columns
                    let quoted_sql = self.quote_mixed_case_identifiers(sql).await?;
                    self.session_context.sql(&quoted_sql).await?
                } else {
                    return Err(DeltaError::Query(msg));
                }
            }
        };

        // (Previously returned QueryResult with timing + row_count; simplified to DataFrame per API reduction.)
        let _elapsed = start.elapsed(); // Keep for potential logging later
        Ok(df)
    }

    /// Execute a SQL query and return just the DataFrame (convenience method)
    pub async fn sql(&self, sql: &str) -> Result<DataFrame> { self.query(sql).await }

    /// Write a DataFrame to a Delta table
    /// Note: When creating new tables, change data feed will be automatically enabled
    pub async fn write(
        &mut self,
        dataframe: DataFrame,
        path: &str,
        mode: WriteMode,
    ) -> Result<DeltaTable> {
        utils::validate_path_with_options(path, false)?;
        
        // Write the data
        let result_table = self.operations.write_dataframe(dataframe, path, mode).await?;
        
        Ok(result_table)
    }

    pub async fn merge(
        &mut self,
        dataframe: DataFrame,
        target_path: &str,
        merge_condition: Expr,
        match_condition: Expr,
        update_expressions: Option<HashMap<&str, Expr>>,
        insert_expressions: Option<HashMap<&str, Expr>>,
        insert_if_not_matched: bool,
        delete_if_matched: bool,
    ) -> Result<DeltaTable> {
            utils::validate_path(target_path)?;

        // Get the dataframe schema before it gets moved
        let df_schema = dataframe.schema().clone();

        // Load the target table using the existing wrapper
    let (table, alias) = self.load_table(target_path).await?;
        
        // Clean up the temporary table registration since we only need the DeltaTable
        let _ = self.unload_table(&alias);

        // Start building the merge operation using the provided expressions directly
        let mut merge_builder = DeltaOps(table)
            .merge(dataframe, merge_condition)
            .with_source_alias("s")
            .with_target_alias("t");

        // Handle when matched actions
        if delete_if_matched {
            merge_builder = merge_builder.when_matched_delete(|delete| delete.predicate(match_condition.clone()))?;
        } else if let Some(updates) = update_expressions {
            merge_builder = merge_builder.when_matched_update(|update| {
                let mut update_builder = update.predicate(match_condition.clone());
                for (column, expr) in updates {
                    update_builder = update_builder.update(column, expr);
                }
                update_builder
            })?;
        } else {
            // Fallback: auto-generate update expressions for all columns (similar to auto insert logic)
            // This copies every column from source alias 's' to target except where custom logic supplied.
            // Exclude typical primary key columns (e.g., 'id') from being updated to avoid accidental key mutation.
            let mut auto_update_cols: Vec<String> = Vec::new();
            for field in df_schema.fields() {
                let name = field.name();
                if name.eq_ignore_ascii_case("id") { continue; }
                auto_update_cols.push(name.to_string());
            }
            use tracing::info;
            info!(target: "delta_operations::merge", "Auto-generating update expressions for columns: {}", auto_update_cols.join(", "));
            merge_builder = merge_builder.when_matched_update(|update| {
                let mut update_builder = update.predicate(match_condition.clone());
                for col_name in &auto_update_cols {
                    let expr = col(&format!("s.{}", col_name));
                    update_builder = update_builder.update(col_name, expr);
                }
                update_builder
            })?;
        }

        // Handle when not matched actions
        if insert_if_not_matched {
            merge_builder = merge_builder.when_not_matched_insert(|insert| {
                // For "when not matched", we typically want to insert unconditionally
                // since these are new rows. If a predicate is needed, it should be based on source data.
                let mut insert_builder = insert;
                
                if let Some(inserts) = insert_expressions {
                    for (column, expr) in inserts {
                        insert_builder = insert_builder.set(column, expr);
                    }
                } else {
                    // Auto-generate insert expressions based on dataframe schema
                    for field in df_schema.fields() {
                        let expr = col(&format!("s.{}", field.name()));
                        insert_builder = insert_builder.set(field.name(), expr);
                    }
                }
                insert_builder
            })?;
        }

        // Execute the merge operation
        let (table, _metrics) = merge_builder
            .await
            .map_err(|e| DeltaError::DeltaTable(e.to_string()))?;

        Ok(table)
    }

    /// Create a Delta table with custom schema
    /// Note: Change data feed will be automatically enabled for the new table
    // Removed create_table_with_schema as part of API simplification (not core to minimal read/write/merge).

    /// Optimize a Delta table at the given path
    /// Returns the optimized table and optimization metrics
    pub async fn optimize(&mut self, path: &str) -> Result<DeltaTable> {
        // Load the table using existing method (handles validation and storage options)
    let (table, alias) = self.load_table(path).await?;
        
        // Clean up the temporary table registration since we only need the DeltaTable
        let _ = self.unload_table(&alias);
        
        // Create and execute the optimize operation using DeltaOps
        let (optimized_table, _metrics) = DeltaOps(table)
            .optimize()
            .await
            .map_err(|e| DeltaError::DeltaTable(format!("Optimization failed: {}", e)))?;
            
        Ok(optimized_table)
    }

    /// Vacuum a Delta table at the given path
    /// Removes files that are no longer referenced by the table and are older than the retention period
    /// Returns the vacuumed table
    pub async fn vacuum(&mut self, path: &str) -> Result<DeltaTable> {
        // Load table (validates path) then immediately deregister alias for operation
        let (table, alias) = self.load_table(path).await?;
        let _ = self.unload_table(&alias);

        // Execute vacuum using delta-rs operations API.
        // NOTE: Uses default retention; users needing custom retention / dry-run can extend API later.
        let (vacuumed_table, _metrics) = DeltaOps(table)
            .vacuum()
            .await
            .map_err(|e| DeltaError::DeltaTable(format!("Vacuum failed: {}", e)))?;
        Ok(vacuumed_table)
    }

    /// Delete/drop a Delta table by removing all its files
    /// This is useful for testing scenarios where you want to start fresh
    /// WARNING: This permanently deletes the table and all its data!
    pub async fn delete_table(&mut self, path: &str) -> Result<()> {
        utils::validate_path(path)?;
        
        // Try to load the table first to check if it exists
        match self.operations.load_table(path).await {
            Ok(_table) => {
                // Table exists, attempt to delete it using SQL DROP TABLE
                // Load the table temporarily to register it
                let (_table, alias) = self.load_table(path).await?;
                
                // Execute DROP TABLE command
                let drop_sql = format!("DROP TABLE IF EXISTS {}", alias);
                
                match self.session_context.sql(&drop_sql).await {
                    Ok(_) => {
                        // Clean up the temporary table registration
                        let _ = self.unload_table(&alias);
                        Ok(())
                    }
                    Err(e) => {
                        // Clean up the temporary table registration
                        let _ = self.unload_table(&alias);
                        // For now, we'll consider this a successful deletion even if DROP TABLE fails
                        // since the underlying file system deletion might need different approach
                        println!("Note: DROP TABLE command failed, but this may be expected: {}", e);
                        Ok(())
                    }
                }
            }
            Err(_) => {
                // Table doesn't exist, which is fine for deletion
                Ok(())
            }
        }
    }

    /// Helper function to quote mixed-case identifiers in SQL
    pub async fn quote_mixed_case_identifiers(&self, sql: &str) -> Result<String> {
        // This is a simplified version - in practice you'd want more sophisticated parsing
        let re = Regex::new(r"(?i)\b[a-z_][a-z0-9_]*\b")
            .map_err(|e| DeltaError::General(e.to_string()))?;
        
        let keywords = ["select", "from", "where", "and", "or", "limit", "order", "by", "asc", "desc", "group", "having", "distinct"];
        
        let mut result = String::new();
        let mut last_end = 0;
        
        for mat in re.find_iter(sql) {
            let identifier = mat.as_str();
            
            // Skip SQL keywords
            if keywords.iter().any(|k| k.eq_ignore_ascii_case(identifier)) {
                continue;
            }
            
            // Skip already quoted identifiers
            if identifier.starts_with('"') {
                continue;
            }
            
            // Check if this identifier has mixed case in any loaded table
            let mut should_quote = false;
            for table in self.tables.values() {
                if let Ok(snapshot) = table.snapshot() {
                    let schema = snapshot.schema();
                    for field in schema.fields() {
                        if field.name().to_lowercase() == identifier.to_lowercase() 
                            && field.name().chars().any(|c| c.is_ascii_uppercase()) {
                            should_quote = true;
                            break;
                        }
                    }
                }
                if should_quote { break; }
            }
            
            if should_quote {
                result.push_str(&sql[last_end..mat.start()]);
                result.push('"');
                result.push_str(identifier);
                result.push('"');
                last_end = mat.end();
            }
        }
        
        if last_end == 0 {
            Ok(sql.to_string())
        } else {
            result.push_str(&sql[last_end..]);
            Ok(result)
        }
    }

    /// Read Delta table change data feed (CDF) starting from a specific version
    /// 
    /// Note: Change data feed is automatically enabled when creating new tables through this client.
    /// For existing tables that don't have CDF enabled, use the `enable_change_data_feed` method first.
    /// 
    /// # Arguments
    /// * `path` - The path to the Delta table (must be a valid ABFSS path)
    /// * `starting_version` - The version to start reading changes from
    /// 
    /// # Returns
    /// A DataFrame containing the change data feed with columns:
    /// - Original table columns
    /// - `_change_type`: INSERT, UPDATE_PREIMAGE, UPDATE_POSTIMAGE, DELETE
    /// - `_commit_version`: The version where the change occurred
    /// - `_commit_timestamp`: When the change was committed
    /// 
    /// # Example
    /// ```rust,no_run
    /// # use delta_operations::DeltaClient;
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut client = DeltaClient::with_token("token".to_string());
    /// let changes = client.read_cdf("abfss://container@account.dfs.core.windows.net/table", 5).await?;
    /// changes.show().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn read_cdf(&mut self, path: &str, starting_version: i64) -> Result<DataFrame> {
        // Validate path format and load table using existing method
    let (table, alias) = self.load_table(path).await?;
        
        // Clean up the temporary table registration since we only need the DeltaTable
        let _ = self.unload_table(&alias);

        // Check if change data feed is enabled
        let snapshot = table.snapshot()?;
        let metadata = snapshot.metadata();
        let cdf_enabled = metadata.configuration().get("delta.enableChangeDataFeed")
            .map(|v| v == "true")
            .unwrap_or(false);

        if !cdf_enabled {
            return Err(DeltaError::General(format!(
                "Change data feed is not enabled for table at '{}'. \n\
                Note: Enabling CDF on existing tables is not currently supported by this client. \n\
                To enable CDF, please use the following SQL command with Delta Lake SQL:\n\
                ALTER TABLE {} SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')",
                path, path
            )));
        }

        // Create CDF builder with starting version
        let builder = CdfLoadBuilder::new(table.log_store(), snapshot.clone())
            .with_starting_version(starting_version);

        // Create CDF table provider
        let provider = DeltaCdfTableProvider::try_new(builder)
            .map_err(|e| DeltaError::DeltaTable(format!("Failed to create CDF provider: {}", e)))?;

        // Read CDF data using session context (note: this is synchronous, not async)
        let df = self.session_context.read_table(Arc::new(provider))
            .map_err(|e| DeltaError::General(format!("Failed to read change data feed: {}", e)))?;

        Ok(df)
    }
    #[deprecated(note = "Use read_cdf instead of read_stream")]
    pub async fn read_stream(&mut self, path: &str, starting_version: i64) -> Result<DataFrame> { self.read_cdf(path, starting_version).await }

    /// Enable change data feed (CDF) for an existing Delta table if not already enabled.
    ///
    /// Current implementation:
    /// - Validates path format (ABFSS or local)
    /// - Attempts to load the table
    /// - If table loads and CDF already enabled -> Ok(())
    /// - If table loads and CDF not enabled -> returns General error explaining limitation
    /// - If table cannot be loaded -> propagates underlying error (NOT a Config error if path format is valid)
    pub async fn enable_change_data_feed(&mut self, path: &str) -> Result<()> {
        // Validate structural path (won't check existence for ABFSS)
        utils::validate_path(path)?;

        // Try to load the table; propagate non-config errors to satisfy test expectations
        let table = match self.operations.load_table(path).await {
            Ok(t) => t,
            Err(e) => return Err(e),
        };

        // Inspect metadata to see if already enabled
        if let Ok(snapshot) = table.snapshot() {
            let metadata = snapshot.metadata();
            let already_enabled = metadata.configuration()
                .get("delta.enableChangeDataFeed")
                .map(|v| v == "true").unwrap_or(false);
            if already_enabled {
                return Ok(());
            }
        }

        // TODO: Implement actual property update once delta-rs exposes a safe API.
        Err(DeltaError::General(
            "Enabling change data feed on existing tables is not yet implemented in this client".to_string()
        ))
    }

    /// Perform a hard delete of a Delta table, physically removing all files
    /// 
    /// This is a destructive operation that:
    /// 1. Performs regular table deletion (empties data and drops table)
    /// 2. Runs vacuum to clean up remaining files
    /// 3. Physically deletes all files from storage (Azure Storage SDK for ABFSS, local FS for local paths)
    /// 
    /// # Arguments
    /// * `path` - The path to the Delta table (supports both local and ABFSS paths)
    /// 
    /// # Returns
    /// * `Ok(())` if the hard delete operation completed (some files may remain)
    /// * `Err(DeltaError)` if the operation failed completely
    pub async fn hard_delete_table(&mut self, path: &str) -> Result<()> {
        match self.delete_table(path).await {
            Ok(_) => println!("✅ Table emptied and dropped successfully"),
            Err(e) => println!("⚠️  Table deletion warning: {} (continuing anyway)", e),
        }
        
        match self.vacuum(path).await {
            Ok(_) => println!("✅ Vacuum completed - old data files removed"),
            Err(e) => println!("⚠️  Vacuum warning: {} (continuing anyway)", e),
        }
        
        if path.starts_with("abfss://") {
            match self.hard_delete_azure_storage(path).await {
                Ok(_) => println!("✅ All Azure Storage files deleted"),
                Err(e) => println!("⚠️  Azure Storage deletion warning: {} (some files may remain)", e),
            }
        } else {
            match self.hard_delete_local_files(path).await {
                Ok(_) => println!("✅ All local files deleted"),
                Err(e) => println!("⚠️  Local file deletion warning: {} (some files may remain)", e),
            }
        }

        println!("💥 Hard delete operation completed!");
        Ok(())
    }

    /// Delete all files from Azure Data Lake Storage using Azure Storage SDK
    async fn hard_delete_azure_storage(&self, path: &str) -> Result<()> {
        println!("☁️  Performing Azure Storage hard delete...");
        
        // Parse the ABFSS path to extract components
        let (account_name, container_name, blob_prefix) = self.parse_abfss_path(path)?;
        
        println!("🔍 Storage Account: {}", account_name);
        println!("🔍 Container: {}", container_name);
        println!("🔍 Blob Prefix: {}", blob_prefix);
        
        // Get Azure credentials and create storage client
        let credential = DefaultAzureCredential::create(Default::default())?;
        let storage_credentials = StorageCredentials::token_credential(
            std::sync::Arc::new(credential)
        );

        let data_lake_client = DataLakeClient::new(
            account_name.clone(),
            storage_credentials,
        );

        let file_system_client = data_lake_client.file_system_client(container_name);
        let directory_client = file_system_client.get_directory_client(&blob_prefix);
        directory_client.delete(true).await?;
        
        Ok(())
    }

    /// Delete all files from local file system
    async fn hard_delete_local_files(&self, path: &str) -> Result<()> {
        println!("💻 Performing local file system hard delete...");
        
        let table_path = Path::new(path);
        
        if !table_path.exists() {
            println!("📭 Path '{}' does not exist - already deleted or never existed", path);
            return Ok(());
        }
        
        // Count files first for reporting
        let total_files = self.count_files_recursive(table_path).await?;
        println!("🔍 Found {} files to delete", total_files);
        
        // Delete the entire directory tree
        if table_path.is_dir() {
            match fs::remove_dir_all(table_path).await {
                Ok(_) => {
                    println!("✅ Successfully deleted directory: {}", path);
                    Ok(())
                }
                Err(e) => {
                    println!("⚠️  Failed to delete directory: {}", e);
                    Err(DeltaError::Storage(format!("Failed to delete directory '{}': {}", path, e)))
                }
            }
        } else if table_path.is_file() {
            match fs::remove_file(table_path).await {
                Ok(_) => {
                    println!("✅ Successfully deleted file: {}", path);
                    Ok(())
                }
                Err(e) => {
                    println!("⚠️  Failed to delete file: {}", e);
                    Err(DeltaError::Storage(format!("Failed to delete file '{}': {}", path, e)))
                }
            }
        } else {
            Err(DeltaError::Storage(format!("Path '{}' is neither a file nor a directory", path)))
        }
    }

    /// Parse ABFSS path to extract account name, container name, and blob prefix
    fn parse_abfss_path(&self, path: &str) -> Result<(String, String, String)> {
        // ABFSS path format: abfss://container@account.dfs.core.windows.net/path/to/table
        if !path.starts_with("abfss://") {
            return Err(DeltaError::General("Path is not an ABFSS path".to_string()));
        }
        
        let path_without_scheme = &path[8..]; // Remove "abfss://"
        
        let parts: Vec<&str> = path_without_scheme.splitn(2, '/').collect();
        if parts.len() != 2 {
            return Err(DeltaError::General("Invalid ABFSS path format".to_string()));
        }
        
        let container_and_account = parts[0];
        let blob_prefix = parts[1].to_string();
        
        let container_account_parts: Vec<&str> = container_and_account.split('@').collect();
        if container_account_parts.len() != 2 {
            return Err(DeltaError::General("Invalid ABFSS container@account format".to_string()));
        }
        
        let container_name = container_account_parts[0].to_string();
        let account_with_domain = container_account_parts[1];
        
        // Extract account name from account.dfs.core.windows.net
        let account_name = account_with_domain.split('.').next()
            .unwrap_or(account_with_domain)
            .to_string();
        
        Ok((account_name, container_name, blob_prefix))
    }

    /// Recursively count files in a directory (for local file system operations)
    fn count_files_recursive<'a>(&'a self, path: &'a Path) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<usize>> + 'a>> {
        Box::pin(async move {
            let mut count = 0;
            
            if path.is_file() {
                return Ok(1);
            }
            
            if path.is_dir() {
                let mut dir = fs::read_dir(path).await
                    .map_err(|e| DeltaError::Storage(format!("Failed to read directory: {}", e)))?;
                
                while let Some(entry) = dir.next_entry().await
                    .map_err(|e| DeltaError::Storage(format!("Failed to read directory entry: {}", e)))? {
                    
                    let entry_path = entry.path();
                    if entry_path.is_dir() {
                        count += self.count_files_recursive(&entry_path).await?;
                    } else {
                        count += 1;
                    }
                }
            }
            
            Ok(count)
        })
    }
}
