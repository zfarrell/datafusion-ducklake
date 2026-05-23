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

            // Deduplicate table column indices so the underlying Parquet scan
            // reads each physical column at most once. Duplicate output slots
            // are then materialized via `reorder_indices` (see below). Without
            // this, `SELECT col, col` would push `[0, 0]` to DataFusion's
            // projection and the second occurrence would collapse onto the
            // first when looking up positions, dropping the second output.
            for &idx in indices {
                if idx < num_table_cols {
                    if !table_indices.contains(&idx) {
                        table_indices.push(idx);
                    }
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
            // Build position-by-position to handle duplicate column projections correctly
            // (e.g., SELECT col, col — a HashMap would collapse duplicates).
            let snapshot_natural_pos = table_indices.len();
            let change_type_natural_pos = table_indices.len()
                + if need_snapshot_id {
                    1
                } else {
                    0
                };

            let natural_pos_map: Vec<usize> = indices
                .iter()
                .map(|&idx| {
                    if idx < num_table_cols {
                        // Use position() scan to handle duplicate projections:
                        // each occurrence maps to the corresponding table_indices position.
                        table_indices.iter().position(|&ti| ti == idx).unwrap_or(0)
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

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_schemas() -> (SchemaRef, SchemaRef) {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("id", arrow::datatypes::DataType::Int32, false),
            Field::new("name", arrow::datatypes::DataType::Utf8, true),
        ]));
        let mut output_fields: Vec<Field> = table_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        output_fields.push(Field::new(
            "snapshot_id",
            arrow::datatypes::DataType::Int64,
            false,
        ));
        output_fields.push(Field::new(
            "change_type",
            arrow::datatypes::DataType::Utf8,
            false,
        ));
        let output_schema = Arc::new(Schema::new(output_fields));
        (table_schema, output_schema)
    }

    #[test]
    fn test_cdc_projection_no_projection() {
        let (table_schema, output_schema) = make_test_schemas();
        let result = analyze_cdc_projection(None, &table_schema, &output_schema).unwrap();
        assert_eq!(result.table_indices, vec![0, 1]);
        assert!(result.need_snapshot_id);
        assert!(result.need_change_type);
        assert!(result.reorder_indices.is_none());
    }

    #[test]
    fn test_cdc_projection_duplicate_columns() {
        // R5-S-037: Duplicate column projections (SELECT col, col) must produce
        // two independent output slots that both reference the same underlying
        // physical column. We achieve this by reading the physical column once
        // (deduplicated `table_indices`) and materializing duplicates via
        // `reorder_indices`.
        let (table_schema, output_schema) = make_test_schemas();
        let projection = vec![0, 0]; // id, id
        let result =
            analyze_cdc_projection(Some(&projection), &table_schema, &output_schema).unwrap();

        // Physical scan should read column 0 exactly once.
        assert_eq!(result.table_indices, vec![0]);

        // Output schema should still have two columns (both `id`).
        assert_eq!(result.output_schema.fields().len(), 2);
        assert_eq!(result.output_schema.field(0).name(), "id");
        assert_eq!(result.output_schema.field(1).name(), "id");

        // Reorder must populate two output slots from the single scan column.
        let reorder = result
            .reorder_indices
            .expect("duplicate projection requires reorder");
        assert_eq!(reorder, vec![0, 0]);
    }

    #[test]
    fn test_cdc_projection_duplicate_columns_produces_independent_outputs() {
        // Regression test for the audit-flagged bug: applying the projection
        // analysis to a real batch must yield two independent output columns,
        // both containing the source column's data, even when projected twice.
        use arrow::array::{Int32Array, RecordBatch};

        let (table_schema, output_schema) = make_test_schemas();
        let projection = vec![0, 0]; // SELECT id, id
        let result =
            analyze_cdc_projection(Some(&projection), &table_schema, &output_schema).unwrap();

        // Simulate the scan: deduped table_indices = [0], so scan reads one
        // column. This mimics what `AppendCDCColumnsStream::transform_batch`
        // sees as input.
        let id_array = Arc::new(Int32Array::from(vec![10, 20, 30])) as arrow::array::ArrayRef;
        let scan_schema = Arc::new(Schema::new(vec![table_schema.field(0).clone()]));
        let scan_batch = RecordBatch::try_new(scan_schema, vec![id_array.clone()]).unwrap();

        // Apply reorder the same way `transform_batch` does.
        let reorder = result.reorder_indices.unwrap();
        let columns: Vec<arrow::array::ArrayRef> = reorder
            .iter()
            .map(|&i| scan_batch.column(i).clone())
            .collect();

        let out = RecordBatch::try_new(result.output_schema.clone(), columns).unwrap();

        // Two output columns, both holding the id data independently.
        assert_eq!(out.num_columns(), 2);
        let col0 = out
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let col1 = out
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(col0.values(), &[10, 20, 30]);
        assert_eq!(col1.values(), &[10, 20, 30]);
    }

    #[test]
    fn test_cdc_projection_duplicate_with_cdc_columns() {
        // SELECT id, id, snapshot_id, change_type — duplicates plus CDC cols.
        let (table_schema, output_schema) = make_test_schemas();
        let projection = vec![0, 0, 2, 3];
        let result =
            analyze_cdc_projection(Some(&projection), &table_schema, &output_schema).unwrap();

        assert_eq!(result.table_indices, vec![0]); // deduped
        assert!(result.need_snapshot_id);
        assert!(result.need_change_type);

        // Natural order after dedup: [id, snapshot_id, change_type]
        // Projection order:          [id, id,         snapshot_id, change_type]
        // So reorder maps:           [0,  0,          1,           2]
        let reorder = result.reorder_indices.expect("reorder required");
        assert_eq!(reorder, vec![0, 0, 1, 2]);
    }

    #[test]
    fn test_cdc_projection_out_of_bounds() {
        let (table_schema, output_schema) = make_test_schemas();
        let projection = vec![10]; // out of bounds
        let result = analyze_cdc_projection(Some(&projection), &table_schema, &output_schema);
        assert!(result.is_err());
    }
}
