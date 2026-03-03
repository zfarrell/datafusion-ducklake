//! Common CDC (Change Data Capture) projection analysis shared between
//! `table_changes` and `table_deletions` modules.

use std::sync::Arc;

use arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::Result as DataFusionResult;
use datafusion::error::DataFusionError;

/// Result of analyzing a CDC projection request.
///
/// Splits a logical projection (which may include both table columns and
/// virtual CDC columns like snapshot_id and change_type) into its physical
/// components for execution.
pub(crate) struct CdcProjectionAnalysis {
    /// Table column indices to read from Parquet (in original order)
    pub table_indices: Vec<usize>,
    /// Whether snapshot_id is requested
    pub need_snapshot_id: bool,
    /// Whether change_type is requested
    pub need_change_type: bool,
    /// The projected output schema
    pub output_schema: SchemaRef,
    /// Maps from natural column order (table_cols + CDC cols) to projection order.
    /// None when no reordering is needed (columns already in natural order).
    pub reorder_indices: Option<Vec<usize>>,
}

/// Analyze a projection and split into table columns and CDC columns.
///
/// CDC tables append two virtual columns after all table columns:
/// - `snapshot_id` at index `num_table_cols`
/// - `change_type` at index `num_table_cols + 1`
///
/// This function determines which table columns and CDC columns are needed,
/// builds the projected output schema, and computes any reordering needed.
pub(crate) fn analyze_cdc_projection(
    projection: Option<&Vec<usize>>,
    table_schema: &SchemaRef,
    output_schema: &SchemaRef,
) -> DataFusionResult<CdcProjectionAnalysis> {
    let num_table_cols = table_schema.fields().len();
    let snapshot_id_idx = num_table_cols;
    let change_type_idx = num_table_cols + 1;

    match projection {
        None => Ok(CdcProjectionAnalysis {
            table_indices: (0..num_table_cols).collect(),
            need_snapshot_id: true,
            need_change_type: true,
            output_schema: output_schema.clone(),
            reorder_indices: None,
        }),
        Some(indices) => {
            let mut table_indices: Vec<usize> = Vec::new();
            let mut need_snapshot_id = false;
            let mut need_change_type = false;

            for &idx in indices {
                if idx < num_table_cols {
                    table_indices.push(idx);
                } else if idx == snapshot_id_idx {
                    need_snapshot_id = true;
                } else if idx == change_type_idx {
                    need_change_type = true;
                }
            }

            // Build projected output schema in requested order
            let num_output_fields = output_schema.fields().len();
            let fields: Vec<Field> = indices
                .iter()
                .map(|&idx| {
                    if idx >= num_output_fields {
                        Err(DataFusionError::Internal(format!(
                            "Projection index {} out of bounds (schema has {} fields)",
                            idx, num_output_fields
                        )))
                    } else {
                        Ok(output_schema.field(idx).clone())
                    }
                })
                .collect::<DataFusionResult<Vec<_>>>()?;
            let projected_schema = Arc::new(Schema::new(fields));

            // Compute reorder mapping from natural order to projection order.
            // Natural order: [table_cols in table_indices order, snapshot_id?, change_type?]
            let table_idx_pos: std::collections::HashMap<usize, usize> = table_indices
                .iter()
                .enumerate()
                .map(|(pos, &idx)| (idx, pos))
                .collect();
            let snapshot_natural_pos = table_indices.len();
            let change_type_natural_pos =
                table_indices.len() + if need_snapshot_id { 1 } else { 0 };

            let natural_pos_map: Vec<usize> = indices
                .iter()
                .map(|&idx| {
                    if idx < num_table_cols {
                        table_idx_pos[&idx]
                    } else if idx == snapshot_id_idx {
                        snapshot_natural_pos
                    } else {
                        change_type_natural_pos
                    }
                })
                .collect();

            let needs_reorder = natural_pos_map.iter().enumerate().any(|(i, &pos)| i != pos);

            Ok(CdcProjectionAnalysis {
                table_indices,
                need_snapshot_id,
                need_change_type,
                output_schema: projected_schema,
                reorder_indices: if needs_reorder {
                    Some(natural_pos_map)
                } else {
                    None
                },
            })
        },
    }
}
