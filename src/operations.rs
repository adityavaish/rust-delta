use crate::error::{DeltaError, Result};
use datafusion::prelude::DataFrame;
use deltalake::protocol::SaveMode;
use deltalake::{open_table_with_storage_options, DeltaTable, DeltaTableBuilder};
use deltalake::operations::write::WriteBuilder;
use std::collections::HashMap;

/// Minimal write modes for core functionality
#[derive(Debug, Clone, Copy)]
pub enum WriteMode {
    Append,
    Overwrite,
}

impl From<WriteMode> for SaveMode {
    fn from(mode: WriteMode) -> Self {
        match mode {
            WriteMode::Append => SaveMode::Append,
            WriteMode::Overwrite => SaveMode::Overwrite,
            // Unsupported modes intentionally removed
        }
    }
}

/// Operations for Delta tables
#[derive(Debug)]
pub struct DeltaOperations {
    token: String,
}

impl DeltaOperations {
    /// Create a new DeltaOperations instance
    pub fn new(token: String) -> Self {
        Self { token }
    }

    /// Load a Delta table from the given path
    pub async fn load_table(&self, path: &str) -> Result<DeltaTable> {
        let storage_opts = self.create_storage_options(path)?;
        let table = open_table_with_storage_options(path, storage_opts).await?;
        Ok(table)
    }

    /// Write a DataFrame to a Delta table
    pub async fn write_dataframe(
        &self,
        dataframe: DataFrame,
        path: &str,
        mode: WriteMode,
    ) -> Result<DeltaTable> {
        // Collect the dataframe into Arrow record batches
        let batches = dataframe.collect().await?;
        if batches.is_empty() {
            return Err(DeltaError::General("No data to write".to_string()));
        }

        let storage_opts = self.create_storage_options(path)?;
        let save_mode = SaveMode::from(mode);

        // Try to open existing table first, or prepare to create a new one
        let existing_table = open_table_with_storage_options(path, storage_opts.clone()).await.ok();

        let write_builder = if let Some(table) = existing_table {
            // Table exists, use it
            WriteBuilder::new(table.log_store(), table.state)
        } else {
            if matches!(save_mode, SaveMode::ErrorIfExists) {
                return Err(DeltaError::DeltaTable("Table does not exist and mode is 'error'".to_string()));
            }

            // Create new table
            let new_table = DeltaTableBuilder::from_uri(path)
                .with_storage_options(storage_opts)
                .build()?;

            let mut configuration: HashMap<String, Option<String>> = HashMap::new();
            configuration.insert("delta.enableChangeDataFeed".to_string(), Some("true".to_string()));

            WriteBuilder::new(new_table.log_store(), new_table.state)
                .with_configuration(configuration)
        };

        // Write the data
        let result_table = write_builder
            .with_save_mode(save_mode)
            .with_input_batches(batches)
            .await?;

        Ok(result_table)
    }


    /// Create storage options for Azure Data Lake
    pub fn create_storage_options(&self, path: &str) -> Result<HashMap<String, String>> {
        let mut storage_opts = HashMap::new();
        let account = crate::utils::extract_account_name(path).unwrap_or("defaultaccount");
        storage_opts.insert("azure_storage_account_name".to_string(), account.to_string());
        storage_opts.insert("azure_storage_token".to_string(), self.token.clone());
        Ok(storage_opts)
    }
}
