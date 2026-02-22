//! Type mapping from DuckLake types to Arrow types

use std::collections::HashMap;

use std::sync::Arc;

use crate::metadata_provider::DuckLakeTableColumn;
use crate::{DuckLakeError, Result};
use arrow::datatypes::{DataType, Field, IntervalUnit, Schema, TimeUnit};
use parquet::file::metadata::ParquetMetaData;

/// Convert a DuckLake type string to an Arrow DataType
pub fn ducklake_to_arrow_type(ducklake_type: &str) -> Result<DataType> {
    // Normalize type string (lowercase, remove whitespace)
    let normalized = ducklake_type.trim().to_lowercase();

    // Handle parameterized types first
    if let Some(decimal_params) = parse_decimal(&normalized) {
        return Ok(decimal_params);
    }

    // Handle basic types
    match normalized.as_str() {
        // Boolean
        "boolean" | "bool" => Ok(DataType::Boolean),

        // Integers
        "int8" | "tinyint" => Ok(DataType::Int8),
        "int16" | "smallint" => Ok(DataType::Int16),
        "int32" | "int" | "integer" => Ok(DataType::Int32),
        "int64" | "bigint" | "long" => Ok(DataType::Int64),
        "uint8" | "utinyint" => Ok(DataType::UInt8),
        "uint16" | "usmallint" => Ok(DataType::UInt16),
        "uint32" | "uint" | "uinteger" => Ok(DataType::UInt32),
        "uint64" | "ubigint" => Ok(DataType::UInt64),

        // Floating point
        "float32" | "float" | "real" => Ok(DataType::Float32),
        "float64" | "double" => Ok(DataType::Float64),

        // Temporal types
        "time" => Ok(DataType::Time64(TimeUnit::Microsecond)),
        "date" => Ok(DataType::Date32),
        "timestamp" => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        "timestamptz" | "timestamp with time zone" => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        "timestamp_s" => Ok(DataType::Timestamp(TimeUnit::Second, None)),
        "timestamp_ms" => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
        "timestamp_ns" => Ok(DataType::Timestamp(TimeUnit::Nanosecond, None)),
        "interval" => Ok(DataType::Interval(IntervalUnit::MonthDayNano)),

        // String types
        "varchar" | "text" | "string" => Ok(DataType::Utf8),
        "json" => Ok(DataType::Utf8), // JSON stored as UTF8 string

        // Binary types
        "blob" | "binary" | "bytea" => Ok(DataType::Binary),
        "uuid" => Ok(DataType::FixedSizeBinary(16)),

        // Geometry types (stored as binary WKB format)
        "point" | "linestring" | "polygon" | "multipoint" | "multilinestring" | "multipolygon"
        | "geometrycollection" | "linestring z" | "geometry" => Ok(DataType::Binary),

        // Time with timezone - not directly supported, use string
        "timetz" | "time with time zone" => Ok(DataType::Utf8),

        _ => {
            // Try complex types (preserves case for struct field names)
            if let Some(result) = parse_complex_type(ducklake_type.trim()) {
                result
            } else {
                Err(DuckLakeError::UnsupportedType(ducklake_type.to_string()))
            }
        },
    }
}

/// Convert an Arrow DataType to a DuckLake type string
///
/// This is the reverse of `ducklake_to_arrow_type()`.
pub fn arrow_to_ducklake_type(arrow_type: &DataType) -> Result<String> {
    match arrow_type {
        // Boolean
        DataType::Boolean => Ok("boolean".to_string()),

        // Integers
        DataType::Int8 => Ok("int8".to_string()),
        DataType::Int16 => Ok("int16".to_string()),
        DataType::Int32 => Ok("int32".to_string()),
        DataType::Int64 => Ok("int64".to_string()),
        DataType::UInt8 => Ok("uint8".to_string()),
        DataType::UInt16 => Ok("uint16".to_string()),
        DataType::UInt32 => Ok("uint32".to_string()),
        DataType::UInt64 => Ok("uint64".to_string()),

        // Floating point
        DataType::Float32 => Ok("float32".to_string()),
        DataType::Float64 => Ok("float64".to_string()),

        // Temporal types
        DataType::Date32 | DataType::Date64 => Ok("date".to_string()),
        DataType::Time32(_) | DataType::Time64(_) => Ok("time".to_string()),
        DataType::Timestamp(TimeUnit::Second, None) => Ok("timestamp_s".to_string()),
        DataType::Timestamp(TimeUnit::Millisecond, None) => Ok("timestamp_ms".to_string()),
        DataType::Timestamp(TimeUnit::Microsecond, None) => Ok("timestamp".to_string()),
        DataType::Timestamp(TimeUnit::Nanosecond, None) => Ok("timestamp_ns".to_string()),
        DataType::Timestamp(_, Some(_)) => Ok("timestamptz".to_string()),
        DataType::Interval(_) => Ok("interval".to_string()),

        // String types
        DataType::Utf8 | DataType::LargeUtf8 => Ok("varchar".to_string()),

        // Binary types
        DataType::Binary | DataType::LargeBinary => Ok("blob".to_string()),
        DataType::FixedSizeBinary(16) => Ok("uuid".to_string()),
        DataType::FixedSizeBinary(_) => Ok("blob".to_string()),

        // Decimal types
        DataType::Decimal128(precision, scale) | DataType::Decimal256(precision, scale) => {
            Ok(format!("decimal({}, {})", precision, scale))
        },

        // Null type - map to varchar as there's no direct equivalent
        DataType::Null => Ok("varchar".to_string()),

        // Complex types
        DataType::List(field) | DataType::LargeList(field) => {
            let inner = arrow_to_ducklake_type(field.data_type())?;
            Ok(format!("list({})", inner))
        },
        DataType::FixedSizeList(field, _) => {
            let inner = arrow_to_ducklake_type(field.data_type())?;
            Ok(format!("list({})", inner))
        },
        DataType::Struct(fields) => {
            let field_strs: Result<Vec<String>> = fields
                .iter()
                .map(|f| {
                    let dt = arrow_to_ducklake_type(f.data_type())?;
                    Ok(format!("{} {}", f.name(), dt))
                })
                .collect();
            Ok(format!("struct({})", field_strs?.join(", ")))
        },
        DataType::Map(entries_field, _) => {
            if let DataType::Struct(fields) = entries_field.data_type() {
                if fields.len() == 2 {
                    let key_type = arrow_to_ducklake_type(fields[0].data_type())?;
                    let value_type = arrow_to_ducklake_type(fields[1].data_type())?;
                    Ok(format!("map({}, {})", key_type, value_type))
                } else {
                    Err(DuckLakeError::UnsupportedType(
                        "Invalid MAP structure: expected 2 fields".to_string(),
                    ))
                }
            } else {
                Err(DuckLakeError::UnsupportedType(
                    "Invalid MAP structure: entries must be a struct".to_string(),
                ))
            }
        },

        // Other unsupported types
        other => Err(DuckLakeError::UnsupportedType(format!(
            "Arrow type '{}' has no DuckLake equivalent",
            other
        ))),
    }
}

/// Parse decimal type with precision and scale
/// Format: "decimal(precision, scale)" or "decimal(precision)"
fn parse_decimal(type_str: &str) -> Option<DataType> {
    if !type_str.starts_with("decimal") && !type_str.starts_with("numeric") {
        return None;
    }

    // Extract parameters from parentheses
    let start = type_str.find('(')?;
    let end = type_str.find(')')?;
    let params = &type_str[start + 1..end];

    let parts: Vec<&str> = params.split(',').map(|s| s.trim()).collect();

    match parts.len() {
        1 => {
            // decimal(precision) with scale=0
            let precision: u8 = parts[0].parse().ok()?;
            Some(DataType::Decimal128(precision, 0))
        },
        2 => {
            // decimal(precision, scale)
            let precision: u8 = parts[0].parse().ok()?;
            let scale: i8 = parts[1].parse().ok()?;

            // Use Decimal256 for high precision
            if precision > 38 {
                Some(DataType::Decimal256(precision, scale))
            } else {
                Some(DataType::Decimal128(precision, scale))
            }
        },
        _ => None,
    }
}

/// Parse a complex DuckLake type string to an Arrow DataType.
/// Returns `Some(Ok(DataType))` on success, `Some(Err)` on parse failure,
/// or `None` if the string is not a complex type.
fn parse_complex_type(type_str: &str) -> Option<Result<DataType>> {
    let lower = type_str.to_lowercase();

    // Array suffix notation: TYPE[]
    if type_str.ends_with("[]") {
        let inner = &type_str[..type_str.len() - 2];
        return Some(
            ducklake_to_arrow_type(inner)
                .map(|dt| DataType::List(Arc::new(Field::new("item", dt, true)))),
        );
    }

    // LIST or ARRAY type
    if lower.starts_with("list") {
        if let Some(inner) = extract_type_params(type_str, 4) {
            return Some(
                ducklake_to_arrow_type(inner.trim())
                    .map(|dt| DataType::List(Arc::new(Field::new("item", dt, true)))),
            );
        }
    }
    if lower.starts_with("array") {
        if let Some(inner) = extract_type_params(type_str, 5) {
            return Some(
                ducklake_to_arrow_type(inner.trim())
                    .map(|dt| DataType::List(Arc::new(Field::new("item", dt, true)))),
            );
        }
    }

    // STRUCT type
    if lower.starts_with("struct") {
        if let Some(inner) = extract_type_params(type_str, 6) {
            return Some(parse_struct_fields(inner));
        }
    }

    // MAP type
    if lower.starts_with("map") {
        if let Some(inner) = extract_type_params(type_str, 3) {
            return Some(parse_map_type(inner));
        }
    }

    None
}

/// Extract the content inside matching brackets after a type prefix.
/// Supports both `()` and `<>` notation.
fn extract_type_params(type_str: &str, prefix_len: usize) -> Option<&str> {
    if type_str.len() <= prefix_len {
        return None;
    }
    let rest = type_str[prefix_len..].trim();
    if rest.is_empty() {
        return None;
    }

    let first = rest.as_bytes()[0];
    let (open, close) = match first {
        b'(' => (b'(', b')'),
        b'<' => (b'<', b'>'),
        _ => return None,
    };

    let mut depth: i32 = 0;
    let bytes = rest.as_bytes();
    for i in 0..bytes.len() {
        if bytes[i] == open {
            depth += 1;
        }
        if bytes[i] == close {
            depth -= 1;
        }
        if depth == 0 {
            if i + 1 == bytes.len() {
                return Some(&rest[1..i]);
            }
            return None;
        }
    }
    None
}

/// Parse struct field definitions.
/// Handles both `name type` (parentheses notation) and `name:type` (angle bracket notation).
fn parse_struct_fields(inner: &str) -> Result<DataType> {
    let parts = split_top_level(inner, ',');
    let mut fields = Vec::new();

    for part in parts {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Try colon separator first (angle bracket notation), then space
        let (name, type_str) = if let Some(pos) = find_top_level_char(part, ':') {
            (&part[..pos], &part[pos + 1..])
        } else if let Some(pos) = find_top_level_char(part, ' ') {
            (&part[..pos], &part[pos + 1..])
        } else {
            return Err(DuckLakeError::UnsupportedType(format!(
                "Invalid struct field definition: '{}'",
                part
            )));
        };

        let field_type = ducklake_to_arrow_type(type_str.trim())?;
        fields.push(Field::new(name.trim(), field_type, true));
    }

    if fields.is_empty() {
        return Err(DuckLakeError::UnsupportedType(
            "STRUCT type must have at least one field".to_string(),
        ));
    }

    Ok(DataType::Struct(fields.into()))
}

/// Parse MAP type parameters: `key_type, value_type`.
fn parse_map_type(inner: &str) -> Result<DataType> {
    let parts = split_top_level(inner, ',');
    if parts.len() != 2 {
        return Err(DuckLakeError::UnsupportedType(format!(
            "MAP type requires exactly 2 type parameters (key, value), got {}",
            parts.len()
        )));
    }

    let key_type = ducklake_to_arrow_type(parts[0].trim())?;
    let value_type = ducklake_to_arrow_type(parts[1].trim())?;

    let entries_field = Field::new(
        "entries",
        DataType::Struct(
            vec![
                Field::new("key", key_type, false),
                Field::new("value", value_type, true),
            ]
            .into(),
        ),
        false,
    );

    Ok(DataType::Map(Arc::new(entries_field), false))
}

/// Split a string by a separator, respecting nested brackets.
fn split_top_level(s: &str, sep: char) -> Vec<&str> {
    let mut result = Vec::new();
    let mut depth = 0;
    let mut start = 0;

    for (i, c) in s.char_indices() {
        match c {
            '(' | '<' | '[' => depth += 1,
            ')' | '>' | ']' => depth -= 1,
            _ if c == sep && depth == 0 => {
                result.push(&s[start..i]);
                start = i + c.len_utf8();
            }
            _ => {}
        }
    }
    result.push(&s[start..]);
    result
}

/// Find the first occurrence of a character at the top level (not inside brackets).
fn find_top_level_char(s: &str, target: char) -> Option<usize> {
    let mut depth = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' | '<' | '[' => depth += 1,
            ')' | '>' | ']' => depth -= 1,
            _ if c == target && depth == 0 => return Some(i),
            _ => {}
        }
    }
    None
}

/// Build an Arrow schema from a list of DuckLake table columns
pub fn build_arrow_schema(columns: &[DuckLakeTableColumn]) -> Result<Schema> {
    let fields: Result<Vec<Field>> = columns
        .iter()
        .map(|col| {
            let data_type = ducklake_to_arrow_type(&col.column_type)?;
            Ok(Field::new(&col.column_name, data_type, col.is_nullable))
        })
        .collect();

    Ok(Schema::new(fields?))
}

/// Extract field_id to column_name mapping from Parquet metadata.
/// DuckLake column_id == Parquet field_id, enabling column matching after renames.
pub fn extract_parquet_field_ids(metadata: &ParquetMetaData) -> HashMap<i32, String> {
    let schema_descr = metadata.file_metadata().schema_descr();
    let mut field_id_map = HashMap::new();

    for i in 0..schema_descr.num_columns() {
        let column = schema_descr.column(i);
        let basic_info = column.self_type().get_basic_info();

        if basic_info.has_id() {
            let field_id = basic_info.id();
            let column_name = column.name().to_string();
            field_id_map.insert(field_id, column_name);
        }
    }

    field_id_map
}

/// Build a schema for reading Parquet files with renamed columns.
/// Returns (read_schema, name_mapping) where read_schema uses original Parquet names
/// and name_mapping maps old->new for columns that were renamed.
pub fn build_read_schema_with_field_id_mapping(
    current_columns: &[DuckLakeTableColumn],
    parquet_field_ids: &HashMap<i32, String>,
) -> Result<(Schema, HashMap<String, String>)> {
    let mut name_mapping: HashMap<String, String> = HashMap::new();

    let fields: Result<Vec<Field>> = current_columns
        .iter()
        .map(|col| {
            let data_type = ducklake_to_arrow_type(&col.column_type)?;
            let field_id = col.column_id as i32;

            let (read_name, needs_rename) =
                if let Some(parquet_name) = parquet_field_ids.get(&field_id) {
                    if parquet_name != &col.column_name {
                        (parquet_name.clone(), true) // Column was renamed
                    } else {
                        (col.column_name.clone(), false)
                    }
                } else {
                    (col.column_name.clone(), false) // No field_id, use current name
                };

            if needs_rename {
                name_mapping.insert(read_name.clone(), col.column_name.clone());
            }

            Ok(Field::new(read_name, data_type, col.is_nullable))
        })
        .collect();

    Ok((Schema::new(fields?), name_mapping))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_build_read_schema_with_renamed_columns() {
        // Simulate: column was originally named "user_id", now renamed to "userId"
        let current_columns = vec![
            DuckLakeTableColumn {
                column_id: 1,
                column_name: "userId".to_string(), // Current name (renamed)
                column_type: "int32".to_string(),
                is_nullable: true,
            },
            DuckLakeTableColumn {
                column_id: 2,
                column_name: "name".to_string(), // Not renamed
                column_type: "varchar".to_string(),
                is_nullable: true,
            },
        ];

        // Parquet file has original names
        let mut parquet_field_ids = HashMap::new();
        parquet_field_ids.insert(1, "user_id".to_string()); // Original name
        parquet_field_ids.insert(2, "name".to_string()); // Same name

        let (read_schema, name_mapping) =
            build_read_schema_with_field_id_mapping(&current_columns, &parquet_field_ids).unwrap();

        // Read schema should have original Parquet names
        assert_eq!(read_schema.field(0).name(), "user_id");
        assert_eq!(read_schema.field(1).name(), "name");

        // Name mapping should map old name to new name
        assert_eq!(name_mapping.len(), 1);
        assert_eq!(name_mapping.get("user_id"), Some(&"userId".to_string()));
    }

    #[test]
    fn test_build_read_schema_no_rename_needed() {
        let current_columns = vec![DuckLakeTableColumn {
            column_id: 1,
            column_name: "id".to_string(),
            column_type: "int32".to_string(),
            is_nullable: true,
        }];

        let mut parquet_field_ids = HashMap::new();
        parquet_field_ids.insert(1, "id".to_string()); // Same name

        let (read_schema, name_mapping) =
            build_read_schema_with_field_id_mapping(&current_columns, &parquet_field_ids).unwrap();

        assert_eq!(read_schema.field(0).name(), "id");
        assert!(name_mapping.is_empty()); // No rename needed
    }

    #[test]
    fn test_build_read_schema_no_field_ids() {
        // External file without field_ids
        let current_columns = vec![DuckLakeTableColumn {
            column_id: 1,
            column_name: "id".to_string(),
            column_type: "int32".to_string(),
            is_nullable: true,
        }];

        let parquet_field_ids = HashMap::new(); // No field_ids in Parquet

        let (read_schema, name_mapping) =
            build_read_schema_with_field_id_mapping(&current_columns, &parquet_field_ids).unwrap();

        // Falls back to current column name
        assert_eq!(read_schema.field(0).name(), "id");
        assert!(name_mapping.is_empty());
    }

    #[test]
    fn test_basic_types() {
        assert_eq!(
            ducklake_to_arrow_type("boolean").unwrap(),
            DataType::Boolean
        );
        assert_eq!(ducklake_to_arrow_type("int32").unwrap(), DataType::Int32);
        assert_eq!(ducklake_to_arrow_type("int64").unwrap(), DataType::Int64);
        assert_eq!(
            ducklake_to_arrow_type("float64").unwrap(),
            DataType::Float64
        );
        assert_eq!(ducklake_to_arrow_type("varchar").unwrap(), DataType::Utf8);
        assert_eq!(ducklake_to_arrow_type("blob").unwrap(), DataType::Binary);
    }

    #[test]
    fn test_decimal_types() {
        assert_eq!(
            ducklake_to_arrow_type("decimal(10, 2)").unwrap(),
            DataType::Decimal128(10, 2)
        );
        assert_eq!(
            ducklake_to_arrow_type("decimal(38, 10)").unwrap(),
            DataType::Decimal128(38, 10)
        );
    }

    #[test]
    fn test_temporal_types() {
        assert_eq!(ducklake_to_arrow_type("date").unwrap(), DataType::Date32);
        assert_eq!(
            ducklake_to_arrow_type("timestamp").unwrap(),
            DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    // ==================== LIST type tests ====================

    #[test]
    fn test_list_angle_bracket_notation() {
        let result = ducklake_to_arrow_type("list<int32>").unwrap();
        assert_eq!(
            result,
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );
    }

    #[test]
    fn test_list_parentheses_notation() {
        let result = ducklake_to_arrow_type("list(varchar)").unwrap();
        assert_eq!(
            result,
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
    }

    #[test]
    fn test_array_angle_bracket_notation() {
        let result = ducklake_to_arrow_type("array<varchar>").unwrap();
        assert_eq!(
            result,
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
    }

    #[test]
    fn test_array_suffix_notation() {
        let result = ducklake_to_arrow_type("int32[]").unwrap();
        assert_eq!(
            result,
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );

        let result2 = ducklake_to_arrow_type("varchar[]").unwrap();
        assert_eq!(
            result2,
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true)))
        );
    }

    #[test]
    fn test_list_uppercase() {
        let result = ducklake_to_arrow_type("LIST(INTEGER)").unwrap();
        assert_eq!(
            result,
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );
    }

    // ==================== STRUCT type tests ====================

    #[test]
    fn test_struct_angle_bracket_notation() {
        let result = ducklake_to_arrow_type("struct<a:int32,b:varchar>").unwrap();
        assert_eq!(
            result,
            DataType::Struct(
                vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Utf8, true),
                ]
                .into()
            )
        );
    }

    #[test]
    fn test_struct_parentheses_notation() {
        let result = ducklake_to_arrow_type("struct(a int32, b varchar)").unwrap();
        assert_eq!(
            result,
            DataType::Struct(
                vec![
                    Field::new("a", DataType::Int32, true),
                    Field::new("b", DataType::Utf8, true),
                ]
                .into()
            )
        );
    }

    #[test]
    fn test_struct_preserves_field_names() {
        let result = ducklake_to_arrow_type("STRUCT(userId INTEGER, userName VARCHAR)").unwrap();
        if let DataType::Struct(fields) = &result {
            assert_eq!(fields[0].name(), "userId");
            assert_eq!(fields[1].name(), "userName");
        } else {
            panic!("Expected Struct type");
        }
    }

    #[test]
    fn test_struct_single_field() {
        let result = ducklake_to_arrow_type("struct(x int64)").unwrap();
        assert_eq!(
            result,
            DataType::Struct(vec![Field::new("x", DataType::Int64, true)].into())
        );
    }

    // ==================== MAP type tests ====================

    #[test]
    fn test_map_angle_bracket_notation() {
        let result = ducklake_to_arrow_type("map<varchar,int32>").unwrap();
        let expected = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int32, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn test_map_parentheses_notation() {
        let result = ducklake_to_arrow_type("map(varchar, int64)").unwrap();
        let expected = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int64, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert_eq!(result, expected);
    }

    // ==================== Nested type tests ====================

    #[test]
    fn test_nested_list_in_list() {
        let result = ducklake_to_arrow_type("list(list(int32))").unwrap();
        let inner_list = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        assert_eq!(
            result,
            DataType::List(Arc::new(Field::new("item", inner_list, true)))
        );
    }

    #[test]
    fn test_nested_struct_in_list() {
        let result = ducklake_to_arrow_type("list<struct<a:int32,b:varchar>>").unwrap();
        let inner_struct = DataType::Struct(
            vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ]
            .into(),
        );
        assert_eq!(
            result,
            DataType::List(Arc::new(Field::new("item", inner_struct, true)))
        );
    }

    #[test]
    fn test_nested_list_in_map() {
        let result = ducklake_to_arrow_type("map(varchar, list(int32))").unwrap();
        let inner_list = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let expected = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", inner_list, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert_eq!(result, expected);
    }

    #[test]
    fn test_struct_array_suffix() {
        // struct(a int32, b varchar)[]
        let result = ducklake_to_arrow_type("struct(a int32, b varchar)[]").unwrap();
        let inner_struct = DataType::Struct(
            vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ]
            .into(),
        );
        assert_eq!(
            result,
            DataType::List(Arc::new(Field::new("item", inner_struct, true)))
        );
    }

    #[test]
    fn test_unknown_type_error() {
        // Test completely unknown types also return error
        let result = ducklake_to_arrow_type("completely_unknown_type");
        assert!(result.is_err());
        match result {
            Err(DuckLakeError::UnsupportedType(msg)) => {
                assert_eq!(msg, "completely_unknown_type");
            },
            _ => panic!("Expected UnsupportedType error for unknown type"),
        }
    }

    #[test]
    fn test_arrow_to_ducklake_basic_types() {
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Boolean).unwrap(),
            "boolean"
        );
        assert_eq!(arrow_to_ducklake_type(&DataType::Int8).unwrap(), "int8");
        assert_eq!(arrow_to_ducklake_type(&DataType::Int16).unwrap(), "int16");
        assert_eq!(arrow_to_ducklake_type(&DataType::Int32).unwrap(), "int32");
        assert_eq!(arrow_to_ducklake_type(&DataType::Int64).unwrap(), "int64");
        assert_eq!(arrow_to_ducklake_type(&DataType::UInt8).unwrap(), "uint8");
        assert_eq!(arrow_to_ducklake_type(&DataType::UInt16).unwrap(), "uint16");
        assert_eq!(arrow_to_ducklake_type(&DataType::UInt32).unwrap(), "uint32");
        assert_eq!(arrow_to_ducklake_type(&DataType::UInt64).unwrap(), "uint64");
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Float32).unwrap(),
            "float32"
        );
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Float64).unwrap(),
            "float64"
        );
        assert_eq!(arrow_to_ducklake_type(&DataType::Utf8).unwrap(), "varchar");
        assert_eq!(arrow_to_ducklake_type(&DataType::Binary).unwrap(), "blob");
    }

    #[test]
    fn test_arrow_to_ducklake_temporal_types() {
        assert_eq!(arrow_to_ducklake_type(&DataType::Date32).unwrap(), "date");
        assert_eq!(arrow_to_ducklake_type(&DataType::Date64).unwrap(), "date");
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Time64(TimeUnit::Microsecond)).unwrap(),
            "time"
        );
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Timestamp(TimeUnit::Microsecond, None)).unwrap(),
            "timestamp"
        );
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            ))
            .unwrap(),
            "timestamptz"
        );
    }

    #[test]
    fn test_arrow_to_ducklake_decimal() {
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Decimal128(10, 2)).unwrap(),
            "decimal(10, 2)"
        );
        assert_eq!(
            arrow_to_ducklake_type(&DataType::Decimal256(40, 5)).unwrap(),
            "decimal(40, 5)"
        );
    }

    #[test]
    fn test_arrow_to_ducklake_uuid() {
        assert_eq!(
            arrow_to_ducklake_type(&DataType::FixedSizeBinary(16)).unwrap(),
            "uuid"
        );
        // Non-16 byte fixed size binary becomes blob
        assert_eq!(
            arrow_to_ducklake_type(&DataType::FixedSizeBinary(32)).unwrap(),
            "blob"
        );
    }

    #[test]
    fn test_arrow_to_ducklake_roundtrip() {
        // Verify roundtrip: arrow -> ducklake -> arrow for common types
        let test_types = vec![
            DataType::Boolean,
            DataType::Int32,
            DataType::Int64,
            DataType::Float64,
            DataType::Utf8,
            DataType::Binary,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Decimal128(10, 2),
        ];

        for original in test_types {
            let ducklake = arrow_to_ducklake_type(&original).unwrap();
            let back = ducklake_to_arrow_type(&ducklake).unwrap();
            assert_eq!(original, back, "Roundtrip failed for {:?}", original);
        }
    }

    #[test]
    fn test_complex_type_roundtrip() {
        // List roundtrip
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        let ducklake = arrow_to_ducklake_type(&list_type).unwrap();
        let back = ducklake_to_arrow_type(&ducklake).unwrap();
        assert_eq!(list_type, back, "List roundtrip failed");

        // Struct roundtrip
        let struct_type = DataType::Struct(
            vec![
                Field::new("x", DataType::Float64, true),
                Field::new("y", DataType::Float64, true),
            ]
            .into(),
        );
        let ducklake = arrow_to_ducklake_type(&struct_type).unwrap();
        let back = ducklake_to_arrow_type(&ducklake).unwrap();
        assert_eq!(struct_type, back, "Struct roundtrip failed");

        // Map roundtrip
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int64, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        let ducklake = arrow_to_ducklake_type(&map_type).unwrap();
        let back = ducklake_to_arrow_type(&ducklake).unwrap();
        assert_eq!(map_type, back, "Map roundtrip failed");

        // Nested roundtrip: list of structs
        let nested = DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(
                vec![
                    Field::new("id", DataType::Int32, true),
                    Field::new("name", DataType::Utf8, true),
                ]
                .into(),
            ),
            true,
        )));
        let ducklake = arrow_to_ducklake_type(&nested).unwrap();
        let back = ducklake_to_arrow_type(&ducklake).unwrap();
        assert_eq!(nested, back, "Nested list-of-struct roundtrip failed");
    }

    #[test]
    fn test_arrow_to_ducklake_list() {
        let list_type = DataType::List(Arc::new(Field::new("item", DataType::Int32, true)));
        assert_eq!(arrow_to_ducklake_type(&list_type).unwrap(), "list(int32)");

        let large_list = DataType::LargeList(Arc::new(Field::new("item", DataType::Utf8, true)));
        assert_eq!(
            arrow_to_ducklake_type(&large_list).unwrap(),
            "list(varchar)"
        );
    }

    #[test]
    fn test_arrow_to_ducklake_struct() {
        let struct_type = DataType::Struct(
            vec![
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ]
            .into(),
        );
        assert_eq!(
            arrow_to_ducklake_type(&struct_type).unwrap(),
            "struct(a int32, b varchar)"
        );
    }

    #[test]
    fn test_arrow_to_ducklake_map() {
        let map_type = DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int64, true),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        );
        assert_eq!(
            arrow_to_ducklake_type(&map_type).unwrap(),
            "map(varchar, int64)"
        );
    }

    #[test]
    fn test_build_schema_with_complex_type() {
        let columns = vec![
            DuckLakeTableColumn {
                column_id: 1,
                column_name: "id".to_string(),
                column_type: "int32".to_string(),
                is_nullable: true,
            },
            DuckLakeTableColumn {
                column_id: 2,
                column_name: "data".to_string(),
                column_type: "list<int32>".to_string(),
                is_nullable: true,
            },
        ];

        let schema = build_arrow_schema(&columns).unwrap();
        assert_eq!(schema.fields().len(), 2);
        assert_eq!(
            *schema.field(1).data_type(),
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true)))
        );
    }
}
