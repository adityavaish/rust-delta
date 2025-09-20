//! DataFrame extension traits for Delta Lake operations

use crate::error::{Result};
use datafusion::prelude::{DataFrame, Expr};
use deltalake::DeltaTable;
use std::collections::HashMap;

/// Extension trait for DataFusion DataFrame with Delta Lake operations
#[async_trait::async_trait]
pub trait DataFrameExt {
    /// Merge this DataFrame into a target Delta table
    async fn merge(
        &self,
        client: &mut crate::DeltaClient,
        target_path: &str,
        merge_condition: Expr,
        match_condition: Expr,
        update_expressions: Option<HashMap<&str, Expr>>,
        insert_expressions: Option<HashMap<&str, Expr>>,
        insert_if_not_matched: bool,
        delete_if_matched: bool,
    ) -> Result<DeltaTable>;
}

#[async_trait::async_trait]
impl DataFrameExt for DataFrame {
    /// Merge this DataFrame into a target Delta table
    async fn merge(
        &self,
        client: &mut crate::DeltaClient,
        target_path: &str,
        merge_condition: Expr,
        match_condition: Expr,
        update_expressions: Option<HashMap<&str, Expr>>,
        insert_expressions: Option<HashMap<&str, Expr>>,
        insert_if_not_matched: bool,
        delete_if_matched: bool,
    ) -> Result<DeltaTable> {
        client.merge(
            self.clone(),
            target_path,
            merge_condition,
            match_condition,
            update_expressions,
            insert_expressions,
            insert_if_not_matched,
            delete_if_matched,
        ).await
    }
}