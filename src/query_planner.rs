//! Custom DataFusion query planner for DuckLake DML operations.
//!
//! DataFusion's default physical planner only handles INSERT INTO via
//! `TableProvider::insert_into()`. DELETE and UPDATE return `not_impl_err!`.
//!
//! This module provides `DuckLakeQueryPlanner` which intercepts DELETE and UPDATE
//! `DmlStatement` nodes and routes them to `DuckLakeTable::delete()` and
//! `DuckLakeTable::update()` respectively, falling through to the default
//! planner for everything else.

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::catalog::TableProvider;
use datafusion::datasource::DefaultTableSource;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::QueryPlanner;
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::dml::DmlStatement;
use datafusion::logical_expr::expr_rewriter::unnormalize_col;
use datafusion::logical_expr::{Expr, Filter, LogicalPlan, Projection, WriteOp};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner};

use crate::table::DuckLakeTable;
use crate::update_exec::UpdateAssignment;

/// Custom query planner that adds DELETE and UPDATE support for DuckLake tables.
///
/// Register with `SessionStateBuilder::with_query_planner(Arc::new(DuckLakeQueryPlanner))`.
///
/// # Example
/// ```no_run
/// use datafusion::prelude::*;
/// use datafusion::execution::session_state::SessionStateBuilder;
/// use datafusion_ducklake::DuckLakeQueryPlanner;
/// use std::sync::Arc;
///
/// let state = SessionStateBuilder::new()
///     .with_default_features()
///     .with_query_planner(Arc::new(DuckLakeQueryPlanner))
///     .build();
/// let ctx = SessionContext::new_with_state(state);
/// // Now ctx.sql("DELETE FROM ...") and ctx.sql("UPDATE ...") work on DuckLake tables
/// ```
#[derive(Debug)]
pub struct DuckLakeQueryPlanner;

#[async_trait]
impl QueryPlanner for DuckLakeQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        match logical_plan {
            LogicalPlan::Dml(DmlStatement {
                target,
                op: WriteOp::Delete,
                input,
                ..
            }) => {
                let table = downcast_ducklake_table(target)?;
                let filters = extract_filters(input);
                table.delete(session_state, &filters).await
            }
            LogicalPlan::Dml(DmlStatement {
                target,
                op: WriteOp::Update,
                input,
                ..
            }) => {
                let table = downcast_ducklake_table(target)?;
                let (assignments, filters) = extract_update_info(input, &table)?;
                table.update(session_state, assignments, &filters).await
            }
            _ => {
                let planner = DefaultPhysicalPlanner::default();
                planner
                    .create_physical_plan(logical_plan, session_state)
                    .await
            }
        }
    }
}

/// Downcast a `TableSource` to `DuckLakeTable`.
fn downcast_ducklake_table(
    target: &Arc<dyn datafusion::logical_expr::TableSource>,
) -> Result<&DuckLakeTable> {
    let source = target
        .as_any()
        .downcast_ref::<DefaultTableSource>()
        .ok_or_else(|| {
            DataFusionError::Plan(
                "DELETE/UPDATE: table source is not a DefaultTableSource".to_string(),
            )
        })?;

    source
        .table_provider
        .as_any()
        .downcast_ref::<DuckLakeTable>()
        .ok_or_else(|| {
            DataFusionError::Plan(
                "DELETE/UPDATE only supported on DuckLake tables. \
                 Use DuckLakeCatalog::with_writer() to enable writes."
                    .to_string(),
            )
        })
}

/// Extract filter expressions from a DELETE logical plan's input.
///
/// The SQL planner creates:
/// - `DELETE FROM t` → input is `TableScan(t)`
/// - `DELETE FROM t WHERE pred` → input is `Filter(pred, TableScan(t))`
///
/// Column references are unnormalized (qualifiers stripped) so they match
/// the unqualified column names in the table schema.
fn extract_filters(plan: &LogicalPlan) -> Vec<Expr> {
    match plan {
        LogicalPlan::Filter(Filter { predicate, .. }) => {
            datafusion::logical_expr::utils::split_conjunction(predicate)
                .into_iter()
                .cloned()
                .map(unnormalize_col)
                .collect()
        }
        _ => vec![],
    }
}

/// Extract UPDATE assignments and WHERE filters from an UPDATE logical plan's input.
///
/// The SQL planner creates:
/// - `UPDATE t SET col=val` → `Project([val AS col, ...unchanged...], TableScan(t))`
/// - `UPDATE t SET col=val WHERE pred` → `Project([val AS col, ...], Filter(pred, TableScan(t)))`
///
/// Each projection expression is either:
/// - A column reference (unchanged column) — `col("name") AS "name"`
/// - A new value expression (changed column) — `lit(42) AS "id"`
///
/// We compare each projection expr to a simple column reference to detect assignments.
fn extract_update_info(
    plan: &LogicalPlan,
    table: &DuckLakeTable,
) -> Result<(Vec<UpdateAssignment>, Vec<Expr>)> {
    // Walk the plan: expect Project(exprs, Filter(pred, Scan)) or Project(exprs, Scan)
    let (projection_exprs, filter_plan) = match plan {
        LogicalPlan::Projection(Projection { expr, input, .. }) => (expr, input.as_ref()),
        _ => {
            return Err(DataFusionError::Plan(
                "UPDATE: expected Projection as top-level input node".to_string(),
            ));
        }
    };

    // Extract filter if present
    let filters = extract_filters(filter_plan);

    // Build assignments by comparing projection exprs to original column references.
    // The SQL planner produces one expr per column, aliased to the column name.
    // Unchanged columns are `Column(qualifier, name) AS name`.
    // Changed columns are `<new_expr> AS name`.
    let schema = table.schema();
    let mut assignments = Vec::new();

    for (i, expr) in projection_exprs.iter().enumerate() {
        if i >= schema.fields().len() {
            break;
        }
        let field = &schema.fields()[i];

        // Unwrap alias to get the inner expression
        let inner = match expr {
            Expr::Alias(alias) => alias.expr.as_ref(),
            other => other,
        };

        // Check if this is just a column reference to the same field (unchanged)
        let is_unchanged = match inner {
            Expr::Column(col) => col.name == *field.name(),
            _ => false,
        };

        if !is_unchanged {
            assignments.push(UpdateAssignment {
                column_index: i,
                expr: unnormalize_col(inner.clone()),
            });
        }
    }

    Ok((assignments, filters))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::logical_expr::{col, lit};

    #[test]
    fn test_extract_filters_empty_plan() {
        // A TableScan (no filter) should return empty filters
        // We can't easily construct a real TableScan in tests, but we can
        // verify the function handles non-Filter plans
        let plan = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(datafusion::common::DFSchema::empty()),
        });
        let filters = extract_filters(&plan);
        assert!(filters.is_empty());
    }

    #[test]
    fn test_extract_filters_single() {
        let inner = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(datafusion::common::DFSchema::empty()),
        });
        let predicate = col("id").eq(lit(1));
        let filter = Filter::try_new(predicate.clone(), Arc::new(inner)).unwrap();
        let plan = LogicalPlan::Filter(filter);

        let filters = extract_filters(&plan);
        assert_eq!(filters.len(), 1);
        assert_eq!(filters[0], predicate);
    }

    #[test]
    fn test_extract_filters_conjunction() {
        let inner = LogicalPlan::EmptyRelation(datafusion::logical_expr::EmptyRelation {
            produce_one_row: false,
            schema: Arc::new(datafusion::common::DFSchema::empty()),
        });
        let predicate = col("id").gt(lit(1)).and(col("name").eq(lit("test")));
        let filter = Filter::try_new(predicate, Arc::new(inner)).unwrap();
        let plan = LogicalPlan::Filter(filter);

        let filters = extract_filters(&plan);
        assert_eq!(filters.len(), 2);
    }
}
