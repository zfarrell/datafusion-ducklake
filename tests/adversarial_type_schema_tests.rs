//! Adversarial tests for the type system and schema evolution.
//!
//! These tests are a red-team attack on the DuckLake type parser (`src/types.rs`),
//! the validation logic (`src/metadata_writer_validation.rs`), and the SQLite
//! metadata writer (`src/metadata_writer_sqlite.rs`).
//!
//! Each test attempts to trigger a panic, wrong result, SQL error, or corruption.
//! Tests that expose bugs are marked with `// BUG:` comments.
//! Tests that pass cleanly are marked with `// DEFENDED:` comments.

// ========================================================================
// Part 1: Type parser adversarial tests (pure unit-level, no DB needed)
// ========================================================================

use datafusion_ducklake::metadata_provider::DuckLakeTableColumn;
use datafusion_ducklake::types::{
    arrow_to_ducklake_type, build_arrow_schema, ducklake_to_arrow_type,
};

// ---------- Empty and whitespace inputs ----------

#[test]
fn attack_type_empty_string() {
    // Attack: pass an empty string as a type.
    // Expected: should return an error, not panic.
    let result = ducklake_to_arrow_type("");
    assert!(result.is_err(), "Empty string should be rejected");
    // DEFENDED: returns UnsupportedType("")
}

#[test]
fn attack_type_whitespace_only() {
    // Attack: pass whitespace-only string.
    // After trim().to_lowercase(), this becomes "".
    let result = ducklake_to_arrow_type("   ");
    assert!(result.is_err(), "Whitespace-only should be rejected");
    // DEFENDED: trim reduces to "", falls through to UnsupportedType
}

#[test]
fn attack_type_tab_and_newline() {
    let result = ducklake_to_arrow_type("\t\n\r");
    assert!(result.is_err(), "Tab/newline should be rejected");
    // DEFENDED
}

// ---------- Null-like and special string inputs ----------

#[test]
fn attack_type_null_string() {
    let result = ducklake_to_arrow_type("null");
    assert!(result.is_err(), "'null' is not a valid DuckLake type");
    // DEFENDED: falls through to UnsupportedType
}

#[test]
fn attack_type_none_string() {
    let result = ducklake_to_arrow_type("None");
    assert!(result.is_err());
    // DEFENDED
}

#[test]
fn attack_type_undefined_string() {
    let result = ducklake_to_arrow_type("undefined");
    assert!(result.is_err());
    // DEFENDED
}

// ---------- SQL injection in type names ----------

#[test]
fn attack_type_sql_injection_semicolon() {
    // Attack: type name containing SQL injection payload.
    // This should never reach SQL execution, just the type parser.
    let result = ducklake_to_arrow_type("int; DROP TABLE ducklake_column;--");
    assert!(
        result.is_err(),
        "SQL injection payload should be rejected as unsupported type"
    );
    // DEFENDED: type parser doesn't execute SQL
}

#[test]
fn attack_type_sql_injection_in_decimal() {
    // Attack: inject SQL inside decimal parameters.
    let result = ducklake_to_arrow_type("decimal(10; DROP TABLE x, 2)");
    // This should fail to parse as decimal and be treated as unknown.
    assert!(result.is_err());
    // DEFENDED: parse().ok()? returns None for non-numeric input
}

#[test]
fn attack_type_sql_injection_in_struct_field() {
    // Attack: struct field name contains SQL injection.
    let result = ducklake_to_arrow_type("struct(\"a'; DROP TABLE x--\" varchar)");
    // This might actually parse the quoted field name. The question is whether
    // the name gets through to SQL unescaped.
    // For the type parser itself, this should parse successfully.
    // BUG potential: if this field name reaches SQL without parameterization.
    match result {
        Ok(_) => {
            // The parser accepted it. The name will be used as a Field name.
            // Not a type parser bug per se, but potentially dangerous if the
            // field name reaches SQL in the metadata writer.
        },
        Err(_) => {
            // Parser rejected it. Also fine.
        },
    }
}

// ---------- Unicode and special characters ----------

#[test]
fn attack_type_unicode_type_name() {
    let result = ducklake_to_arrow_type("整数");
    assert!(
        result.is_err(),
        "Chinese characters should be unsupported type"
    );
    // DEFENDED
}

#[test]
fn attack_type_emoji_type_name() {
    let result = ducklake_to_arrow_type("🔥int64🔥");
    assert!(result.is_err());
    // DEFENDED
}

#[test]
fn attack_type_null_byte_in_type() {
    let result = ducklake_to_arrow_type("int\064");
    // \064 is '4' in octal = "int4", which is not a valid type
    // But let's try actual null byte:
    let result2 = ducklake_to_arrow_type("int\0");
    assert!(result2.is_err(), "Null byte in type should be rejected");
    // DEFENDED: just falls through as unsupported
}

#[test]
fn attack_type_backslash_in_type() {
    let result = ducklake_to_arrow_type("int\\64");
    assert!(result.is_err());
    // DEFENDED
}

// ---------- Deeply nested types (stack overflow attack) ----------

#[test]
fn attack_type_deeply_nested_list_5() {
    // 5 levels deep should work fine.
    let result = ducklake_to_arrow_type("list(list(list(list(list(int32)))))");
    assert!(result.is_ok(), "5-level nesting should work: {:?}", result);
    // DEFENDED: works correctly
}

#[test]
fn attack_type_deeply_nested_list_50() {
    // 50 levels deep. Will the recursive parser stack overflow?
    let mut type_str = "int32".to_string();
    for _ in 0..50 {
        type_str = format!("list({})", type_str);
    }
    let result = ducklake_to_arrow_type(&type_str);
    // BUG: No depth limit! This could cause stack overflow with enough nesting.
    // With 50 levels it likely works, but there's no protection against
    // adversarial input with thousands of levels.
    assert!(
        result.is_ok(),
        "50-level nesting should work (but no depth limit is a bug): {:?}",
        result
    );
}

#[test]
fn attack_type_deeply_nested_list_500() {
    // 500 levels deep. Testing recursion limit.
    let mut type_str = "int32".to_string();
    for _ in 0..500 {
        type_str = format!("list({})", type_str);
    }
    let result = ducklake_to_arrow_type(&type_str);
    // BUG: No recursion depth limit. 500 levels may stack overflow on some systems.
    // On most systems this will work, but it's unbounded.
    match result {
        Ok(_) => {
            // BUG: accepted 500-deep nesting with no limit
        },
        Err(_) => {
            // Stack overflow would manifest as a panic, not an Err.
            // If we got here, something else went wrong.
        },
    }
}

#[test]
fn attack_type_deeply_nested_struct_50() {
    // 50-level nested structs.
    let mut type_str = "int32".to_string();
    for i in 0..50 {
        type_str = format!("struct(f{} {})", i, type_str);
    }
    let result = ducklake_to_arrow_type(&type_str);
    // BUG: No depth limit on struct nesting either.
    match result {
        Ok(_) => { /* No depth limit bug */ },
        Err(e) => {
            // Might fail for other reasons
            panic!("50-deep struct nesting failed: {}", e);
        },
    }
}

// ---------- Malformed parentheses and brackets ----------

#[test]
fn attack_type_unmatched_open_paren() {
    let result = ducklake_to_arrow_type("list(int32");
    assert!(result.is_err(), "Unmatched open paren should fail");
    // DEFENDED: extract_type_params returns None
}

#[test]
fn attack_type_unmatched_close_paren() {
    let result = ducklake_to_arrow_type("list int32)");
    assert!(result.is_err(), "Unmatched close paren should fail");
    // DEFENDED: falls through as unsupported type
}

#[test]
fn attack_type_extra_chars_after_close() {
    let result = ducklake_to_arrow_type("list(int32) EXTRA");
    assert!(
        result.is_err(),
        "Extra chars after closing paren should fail"
    );
    // DEFENDED: extract_type_params checks i + 1 == bytes.len()
}

#[test]
fn attack_type_empty_list() {
    let result = ducklake_to_arrow_type("list()");
    assert!(result.is_err(), "Empty list() should fail");
    // The inner type "" after trim is empty, which should fail.
    // DEFENDED: ducklake_to_arrow_type("") returns UnsupportedType
}

#[test]
fn attack_type_empty_struct() {
    let result = ducklake_to_arrow_type("struct()");
    assert!(result.is_err(), "Empty struct() should fail");
    // DEFENDED: parse_struct_fields returns "must have at least one field"
}

#[test]
fn attack_type_empty_map() {
    let result = ducklake_to_arrow_type("map()");
    assert!(result.is_err(), "Empty map() should fail");
    // DEFENDED: parse_map_type returns "requires exactly 2 type parameters"
}

#[test]
fn attack_type_mixed_bracket_styles() {
    // Opening with ( but closing with >
    let result = ducklake_to_arrow_type("list(int32>");
    assert!(result.is_err(), "Mixed brackets should fail");
    // DEFENDED: extract_type_params tracks matching open/close pairs
}

#[test]
fn attack_type_angle_bracket_nesting_mismatch() {
    // Mismatched nesting: list<list(int32)>
    let result = ducklake_to_arrow_type("list<list(int32)>");
    // This is actually valid! The outer uses <>, the inner uses ().
    // extract_type_params only checks the outer pair.
    match result {
        Ok(_) => { /* Mixed notation is supported */ },
        Err(e) => {
            panic!("Mixed angle/paren notation should work: {}", e);
        },
    }
}

// ---------- Decimal edge cases ----------

#[test]
fn attack_type_decimal_zero_precision() {
    let result = ducklake_to_arrow_type("decimal(0, 0)");
    // BUG: Decimal128(0, 0) is not valid in Arrow.
    // Arrow requires precision >= 1 for Decimal128.
    // The parser accepts it without validation.
    match result {
        Ok(dt) => {
            // BUG: Creates Decimal128(0, 0) which is invalid
            assert_eq!(format!("{:?}", dt), "Decimal128(0, 0)");
        },
        Err(_) => { /* Would be correct to reject */ },
    }
}

#[test]
fn attack_type_decimal_max_precision() {
    // u8 max is 255, but Arrow Decimal128 supports max 38, Decimal256 supports max 76.
    let result = ducklake_to_arrow_type("decimal(255, 0)");
    // BUG: Accepts precision=255 and creates Decimal256(255, 0).
    // Arrow Decimal256 only supports precision up to 76.
    match result {
        Ok(dt) => {
            // BUG: Creates Decimal256(255, 0) which exceeds Arrow's max precision
            assert_eq!(format!("{:?}", dt), "Decimal256(255, 0)");
        },
        Err(_) => { /* Would be correct to reject */ },
    }
}

#[test]
fn attack_type_decimal_negative_scale() {
    // i8 allows negative values.
    let result = ducklake_to_arrow_type("decimal(10, -5)");
    // This creates Decimal128(10, -5). Arrow may or may not support negative scale.
    match result {
        Ok(dt) => {
            assert_eq!(format!("{:?}", dt), "Decimal128(10, -5)");
        },
        Err(_) => { /* Rejected, also acceptable */ },
    }
}

#[test]
fn attack_type_decimal_overflow_precision() {
    // u8 max is 255. What about 256?
    let result = ducklake_to_arrow_type("decimal(256, 0)");
    // 256 doesn't fit in u8, so parse::<u8>() returns None, and parse_decimal returns None.
    assert!(result.is_err(), "Precision 256 overflows u8, should fail");
    // DEFENDED: u8 parse fails gracefully
}

#[test]
fn attack_type_decimal_no_params() {
    // "decimal" without parentheses defaults to Decimal128(18,0) matching DuckDB behavior.
    let result = ducklake_to_arrow_type("decimal");
    assert!(
        result.is_ok(),
        "'decimal' without params should default to Decimal128(18,0)"
    );
    assert_eq!(
        result.unwrap(),
        arrow::datatypes::DataType::Decimal128(18, 0)
    );
    // Also check "numeric" bare
    let result = ducklake_to_arrow_type("numeric");
    assert!(
        result.is_ok(),
        "'numeric' without params should default to Decimal128(18,0)"
    );
    assert_eq!(
        result.unwrap(),
        arrow::datatypes::DataType::Decimal128(18, 0)
    );
}

#[test]
fn attack_type_numeric_alias() {
    // "numeric" is supported as an alias for decimal.
    let result = ducklake_to_arrow_type("numeric(10, 2)");
    assert!(result.is_ok(), "numeric should be accepted");
    // DEFENDED: parse_decimal checks for "numeric" prefix
}

#[test]
fn attack_type_decimal_trailing_garbage() {
    // Trailing garbage after closing parenthesis should be rejected.
    let result = ducklake_to_arrow_type("decimal(10,2)extra_garbage");
    assert!(result.is_err(), "trailing garbage after ')' should fail");
    let err_msg = result.unwrap_err().to_string();
    assert!(
        err_msg.contains("trailing"),
        "error should mention trailing characters: {}",
        err_msg
    );
}

// ---------- VARCHAR/CHAR edge cases ----------

#[test]
fn attack_type_varchar_zero_length() {
    let result = ducklake_to_arrow_type("varchar(0)");
    // The parser just checks starts_with("varchar(") and returns Utf8.
    // It doesn't validate the length parameter at all.
    assert!(result.is_ok());
    // DEFENDED: length is ignored, maps to Utf8
}

#[test]
fn attack_type_varchar_negative_length() {
    let result = ducklake_to_arrow_type("varchar(-1)");
    // Still just checks starts_with and returns Utf8.
    assert!(result.is_ok());
    // DEFENDED: length is ignored
}

#[test]
fn attack_type_varchar_sql_injection() {
    let result = ducklake_to_arrow_type("varchar(255); DROP TABLE x;--");
    // R8-S-039: Now correctly rejects trailing content after closing paren.
    assert!(
        result.is_err(),
        "Malformed trailing input should be rejected"
    );
}

// ---------- Struct field parsing edge cases ----------

#[test]
fn attack_type_struct_no_type() {
    // Struct field with name but no type.
    let result = ducklake_to_arrow_type("struct(fieldonly)");
    assert!(result.is_err(), "Struct field without type should fail");
    // DEFENDED: no space or colon found, returns "Invalid struct field definition"
}

#[test]
fn attack_type_struct_empty_field_name() {
    // Struct with empty field name: space before type.
    let result = ducklake_to_arrow_type("struct( int32)");
    // After split by comma: " int32", after trim: "int32".
    // find_top_level_char for ' ' finds position in "int32" — no space, so fails.
    // Actually "int32" has no space. So it falls to "Invalid struct field definition".
    assert!(result.is_err());
    // DEFENDED
}

#[test]
fn attack_type_struct_duplicate_field_names() {
    // Two fields with the same name.
    let result = ducklake_to_arrow_type("struct(a int32, a varchar)");
    // BUG: The parser accepts duplicate field names without any validation!
    // Arrow's Struct type doesn't inherently prevent this, but it's logically wrong.
    assert!(result.is_ok(), "Parser accepts duplicate field names (BUG)");
}

#[test]
fn attack_type_struct_field_name_with_spaces() {
    // Unquoted field name with spaces.
    let result = ducklake_to_arrow_type("struct(my field name int32)");
    // find_top_level_char for ' ' finds "my" as name and "field name int32" as type.
    // "field name int32" won't parse as a valid type.
    assert!(result.is_err());
    // DEFENDED: inner type parse fails
}

#[test]
fn attack_type_struct_quoted_empty_name() {
    // Quoted but empty field name.
    let result = ducklake_to_arrow_type(r#"struct("" varchar)"#);
    // Finds close quote at position 0 (empty name), rest is " varchar".
    match result {
        Ok(dt) => {
            // BUG: Accepts empty field name!
            if let arrow::datatypes::DataType::Struct(fields) = dt {
                assert_eq!(fields[0].name(), "", "Empty field name was accepted (BUG)");
            }
        },
        Err(_) => { /* Would be correct to reject */ },
    }
}

#[test]
fn attack_type_struct_field_name_very_long() {
    // Field name that's 10000 chars long.
    let long_name = "a".repeat(10000);
    let type_str = format!("struct({} int32)", long_name);
    let result = ducklake_to_arrow_type(&type_str);
    // No length validation on field names.
    assert!(result.is_ok(), "Very long field names accepted (no limit)");
    // BUG: No length limit on field names.
}

// ---------- Array suffix edge cases ----------

#[test]
fn attack_type_double_array_suffix() {
    // int32[][] should be list of list.
    let result = ducklake_to_arrow_type("int32[][]");
    // strip_suffix("[]") gives "int32[]", then recursion gives list(int32).
    assert!(
        result.is_ok(),
        "Double array suffix should create nested list"
    );
    // DEFENDED: recursive handling works correctly
}

#[test]
fn attack_type_empty_array_suffix() {
    // Just "[]" with no base type.
    let result = ducklake_to_arrow_type("[]");
    // strip_suffix("[]") gives "", then ducklake_to_arrow_type("") fails.
    assert!(result.is_err(), "Bare [] should fail");
    // DEFENDED: inner type "" is unsupported
}

// ---------- Absurdly long type strings ----------

#[test]
fn attack_type_very_long_unknown_type() {
    // 100KB of garbage.
    let garbage = "x".repeat(100_000);
    let result = ducklake_to_arrow_type(&garbage);
    assert!(result.is_err());
    // DEFENDED: just falls through to UnsupportedType. No crash.
}

#[test]
fn attack_type_very_long_struct_definition() {
    // Struct with 1000 fields.
    let fields: Vec<String> = (0..1000).map(|i| format!("f{} int32", i)).collect();
    let type_str = format!("struct({})", fields.join(", "));
    let result = ducklake_to_arrow_type(&type_str);
    assert!(
        result.is_ok(),
        "1000-field struct should parse (no field count limit)"
    );
    // BUG: No limit on number of struct fields. Memory exhaustion possible.
}

// ---------- Case sensitivity edge cases ----------

#[test]
fn attack_type_mixed_case_decimal() {
    let result = ducklake_to_arrow_type("DECIMAL(10, 2)");
    assert!(result.is_ok(), "DECIMAL should work case-insensitively");
    // DEFENDED: normalized to lowercase before parsing
}

#[test]
fn attack_type_mixed_case_struct() {
    let result = ducklake_to_arrow_type("STRUCT(Name VARCHAR)");
    assert!(result.is_ok(), "STRUCT should work case-insensitively");
    // Field name "Name" should be preserved.
    if let Ok(arrow::datatypes::DataType::Struct(fields)) = &result {
        assert_eq!(
            fields[0].name(),
            "Name",
            "Field name case should be preserved"
        );
    }
    // DEFENDED: lowercase check but preserves original case for field names
}

// ---------- Roundtrip edge cases ----------

#[test]
fn attack_type_roundtrip_large_list() {
    use arrow::datatypes::DataType;
    use arrow::datatypes::Field;
    use std::sync::Arc;
    // LargeList -> ducklake -> arrow should go through list notation
    let large_list = DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true)));
    let ducklake = arrow_to_ducklake_type(&large_list).unwrap();
    assert_eq!(ducklake, "list(int32)");
    let back = ducklake_to_arrow_type(&ducklake).unwrap();
    // BUG: LargeList -> "list(int32)" -> List (not LargeList!)
    // Information loss: LargeList becomes List on roundtrip.
    assert_ne!(
        large_list, back,
        "LargeList != List after roundtrip (info loss BUG)"
    );
}

#[test]
fn attack_type_roundtrip_large_utf8() {
    use arrow::datatypes::DataType;
    // LargeUtf8 -> ducklake -> arrow
    let ducklake = arrow_to_ducklake_type(&DataType::LargeUtf8).unwrap();
    assert_eq!(ducklake, "varchar");
    let back = ducklake_to_arrow_type(&ducklake).unwrap();
    // BUG: LargeUtf8 -> "varchar" -> Utf8 (not LargeUtf8!)
    assert_ne!(
        DataType::LargeUtf8,
        back,
        "LargeUtf8 != Utf8 after roundtrip (info loss BUG)"
    );
}

#[test]
fn attack_type_roundtrip_date64() {
    use arrow::datatypes::DataType;
    // Date64 -> ducklake -> arrow: now roundtrips correctly via "date_ms"
    let ducklake = arrow_to_ducklake_type(&DataType::Date64).unwrap();
    assert_eq!(ducklake, "date_ms");
    let back = ducklake_to_arrow_type(&ducklake).unwrap();
    assert_eq!(DataType::Date64, back, "Date64 should roundtrip losslessly");
}

#[test]
fn attack_type_roundtrip_time32() {
    use arrow::datatypes::{DataType, TimeUnit};
    // Time32(Millisecond) -> "time_ms" -> Time32(Millisecond): roundtrips correctly
    let ducklake = arrow_to_ducklake_type(&DataType::Time32(TimeUnit::Millisecond)).unwrap();
    assert_eq!(ducklake, "time_ms");
    let back = ducklake_to_arrow_type(&ducklake).unwrap();
    assert_eq!(
        DataType::Time32(TimeUnit::Millisecond),
        back,
        "Time32(Millisecond) should roundtrip losslessly"
    );
}

// ---------- build_arrow_schema edge cases ----------

#[test]
fn attack_schema_empty_columns() {
    let columns: Vec<DuckLakeTableColumn> = vec![];
    let schema = build_arrow_schema(&columns).unwrap();
    assert_eq!(schema.fields().len(), 0);
    // Not a bug per se, but empty schema is accepted.
}

#[test]
fn attack_schema_column_with_empty_name() {
    let columns = vec![DuckLakeTableColumn {
        column_id: 1,
        column_name: "".to_string(),
        column_type: "int32".to_string(),
        is_nullable: true,
    }];
    let schema = build_arrow_schema(&columns).unwrap();
    // BUG: Accepts empty column name without validation.
    assert_eq!(schema.field(0).name(), "");
}

#[test]
fn attack_schema_column_with_invalid_type() {
    let columns = vec![DuckLakeTableColumn {
        column_id: 1,
        column_name: "broken".to_string(),
        column_type: "not_a_type".to_string(),
        is_nullable: true,
    }];
    let result = build_arrow_schema(&columns);
    assert!(result.is_err(), "Invalid type should propagate error");
    // DEFENDED: error propagated from ducklake_to_arrow_type
}

#[test]
fn attack_schema_duplicate_column_names() {
    let columns = vec![
        DuckLakeTableColumn {
            column_id: 1,
            column_name: "id".to_string(),
            column_type: "int32".to_string(),
            is_nullable: false,
        },
        DuckLakeTableColumn {
            column_id: 2,
            column_name: "id".to_string(),
            column_type: "varchar".to_string(),
            is_nullable: true,
        },
    ];
    let schema = build_arrow_schema(&columns);
    // BUG: build_arrow_schema does NOT check for duplicate column names!
    // Arrow Schema allows duplicate field names, but it causes ambiguity in queries.
    assert!(
        schema.is_ok(),
        "Duplicate column names accepted (no validation BUG)"
    );
}

// ========================================================================
// Part 2: Schema evolution / validation adversarial tests
// ========================================================================

#[cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]
mod schema_evolution_tests {
    use datafusion_ducklake::metadata_writer::{
        AlterColumnTypeOp, AlterTableOp, ColumnDef, MetadataWriter, WriteMode,
    };
    use datafusion_ducklake::metadata_writer_sqlite::SqliteMetadataWriter;
    use tempfile::TempDir;

    async fn create_test_writer() -> (SqliteMetadataWriter, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let db_path = temp_dir.path().join("adversarial.db");
        let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
        let writer = SqliteMetadataWriter::new_with_init(&conn_str)
            .await
            .unwrap();
        writer
            .set_data_path(temp_dir.path().to_str().unwrap())
            .unwrap();
        (writer, temp_dir)
    }

    fn setup_table(writer: &SqliteMetadataWriter) -> i64 {
        let columns = vec![
            ColumnDef::new("id", "int32", false).unwrap(),
            ColumnDef::new("name", "varchar", true).unwrap(),
        ];
        let result = writer
            .begin_write_transaction("main", "test_tbl", &columns, WriteMode::Replace)
            .unwrap();
        result.table_id
    }

    // ---------- Rename to empty string ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_rename_to_empty_string() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        let op = AlterTableOp::RenameColumn {
            old_name: "name".to_string(),
            new_name: "".to_string(),
        };
        let result = writer.alter_table(table_id, &op);
        // BUG: Renaming to empty string SUCCEEDS! No validation on new_name.
        // This creates a column with an empty name in the catalog.
        match result {
            Ok(_) => {
                let cols = writer.get_active_columns(table_id).unwrap();
                let empty_col = cols.iter().find(|(name, _, _)| name.is_empty());
                assert!(
                    empty_col.is_some(),
                    "BUG CONFIRMED: Column renamed to empty string exists in catalog"
                );
            },
            Err(_) => {
                // Would be correct behavior to reject
            },
        }
    }

    // ---------- Rename to name with SQL-special characters ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_rename_to_sql_keyword() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        // Rename to SQL keyword.
        let op = AlterTableOp::RenameColumn {
            old_name: "name".to_string(),
            new_name: "SELECT".to_string(),
        };
        let result = writer.alter_table(table_id, &op);
        // No validation on SQL keywords. The column name is stored via parameterized query.
        assert!(
            result.is_ok(),
            "Renaming to SQL keyword should work (parameterized queries protect against injection)"
        );
        // DEFENDED: parameterized queries prevent SQL injection.
        // But querying this column later may require quoting.
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_rename_to_name_with_quotes() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        // Name containing single quotes.
        let op = AlterTableOp::RenameColumn {
            old_name: "name".to_string(),
            new_name: "it's_name".to_string(),
        };
        let result = writer.alter_table(table_id, &op);
        // Should succeed since parameterized queries handle quotes.
        assert!(
            result.is_ok(),
            "Single quote in name should be handled by parameterized queries"
        );
        // DEFENDED
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_rename_to_name_with_semicolons() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        let op = AlterTableOp::RenameColumn {
            old_name: "name".to_string(),
            new_name: "x; DROP TABLE ducklake_column; --".to_string(),
        };
        let result = writer.alter_table(table_id, &op);
        // Parameterized queries protect against this.
        assert!(
            result.is_ok(),
            "SQL injection via column name should be safe with parameterized queries"
        );
        // DEFENDED: parameterized queries
    }

    // ---------- Rename to same name (no-op) ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_rename_to_same_name() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        let op = AlterTableOp::RenameColumn {
            old_name: "name".to_string(),
            new_name: "name".to_string(),
        };
        let result = writer.alter_table(table_id, &op);
        // BUG: Renaming a column to its own name succeeds!
        // The validation checks "Column 'name' already exists" by iterating ALL columns.
        // Since "name" is found in the list, this should fail... but does it?
        // The check is: for col in columns { if col.column_name == new_name { return Err } }
        // The target column IS in the list, so it should detect "name" == "name" and error.
        // Actually it SHOULD fail because the validation loop includes the source column.
        match result {
            Ok(_) => {
                // This would be a bug: it created a snapshot for nothing and
                // ended+replaced the column row unnecessarily.
                panic!(
                    "BUG: Rename to same name should have been rejected but succeeded, creating unnecessary snapshot"
                );
            },
            Err(e) => {
                // Correct: validation catches that "name" already exists
                assert!(e.to_string().contains("already exists"), "Error: {}", e);
            },
        }
    }

    // ---------- Add column with very long name ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_add_column_very_long_name() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        let long_name = "a".repeat(10000);
        let op = AlterTableOp::AddColumn {
            column: ColumnDef::new(&long_name, "varchar", true).unwrap(),
        };
        let result = writer.alter_table(table_id, &op);
        // BUG: No length limit on column names. 10000-char name accepted.
        assert!(
            result.is_ok(),
            "10000-char column name accepted (no length limit)"
        );
    }

    // ---------- Add column with empty name ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_add_column_empty_name() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        let op = AlterTableOp::AddColumn {
            column: ColumnDef::new("", "varchar", true).unwrap(),
        };
        let result = writer.alter_table(table_id, &op);
        // BUG: Adding a column with empty name SUCCEEDS.
        // No validation on column name emptiness.
        match result {
            Ok(_) => {
                let cols = writer.get_active_columns(table_id).unwrap();
                let empty_col = cols.iter().find(|(name, _, _)| name.is_empty());
                assert!(
                    empty_col.is_some(),
                    "BUG CONFIRMED: Empty column name added to table"
                );
            },
            Err(_) => {
                // Would be correct
            },
        }
    }

    // ---------- Add column with invalid type ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_add_column_invalid_type() {
        // ColumnDef::new now validates types at construction time, preventing invalid types
        // from ever reaching the catalog
        let result = ColumnDef::new("bad_col", "not_a_real_type", true);
        assert!(
            result.is_err(),
            "Invalid type should be rejected at ColumnDef construction"
        );
    }

    // ---------- Drop all columns (one by one) ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_drop_all_columns_sequentially() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        // Drop "name" column first (should succeed, 2 -> 1 columns).
        let op1 = AlterTableOp::DropColumn {
            column_name: "name".to_string(),
        };
        let result1 = writer.alter_table(table_id, &op1);
        assert!(
            result1.is_ok(),
            "Dropping one of two columns should succeed"
        );

        // Now try to drop "id" (the last column).
        let op2 = AlterTableOp::DropColumn {
            column_name: "id".to_string(),
        };
        let result2 = writer.alter_table(table_id, &op2);
        assert!(result2.is_err(), "Dropping last column should fail");
        // DEFENDED: "Cannot drop column: table only has one column remaining"
    }

    // ---------- Drop non-existent column ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_drop_nonexistent_column() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        let op = AlterTableOp::DropColumn {
            column_name: "nonexistent".to_string(),
        };
        let result = writer.alter_table(table_id, &op);
        assert!(result.is_err());
        // DEFENDED: "Column 'nonexistent' not found in table"
    }

    // ---------- Type promotion edge cases ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_alter_type_varchar_to_int() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        // varchar -> int32 is a narrowing/incompatible change.
        let op = AlterTableOp::AlterColumnType(AlterColumnTypeOp {
            column_name: "name".to_string(),
            new_type: "int32".to_string(),
        });
        let result = writer.alter_table(table_id, &op);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("widening type promotions")
        );
        // DEFENDED
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_alter_type_to_invalid_type() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        // Change type to something completely invalid.
        let op = AlterTableOp::AlterColumnType(AlterColumnTypeOp {
            column_name: "id".to_string(),
            new_type: "garbage_type".to_string(),
        });
        let result = writer.alter_table(table_id, &op);
        // The is_type_promotion_allowed function does exact string matching.
        // "int32" -> "garbage_type" is not in the allowed list, so it fails.
        assert!(result.is_err());
        // DEFENDED: promotion whitelist rejects unknown types
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_alter_type_same_type() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        // Change int32 to int32 (no-op).
        let op = AlterTableOp::AlterColumnType(AlterColumnTypeOp {
            column_name: "id".to_string(),
            new_type: "int32".to_string(),
        });
        let result = writer.alter_table(table_id, &op);
        // is_type_promotion_allowed("int32", "int32") returns false.
        assert!(result.is_err());
        // DEFENDED: same type rejected as not-a-promotion
    }

    // ---------- Rapid schema evolution: add-drop-add same name ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_add_drop_readd_same_column() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        // Step 1: Add "email" column.
        let add_op = AlterTableOp::AddColumn {
            column: ColumnDef::new("email", "varchar", true).unwrap(),
        };
        writer.alter_table(table_id, &add_op).unwrap();

        // Step 2: Drop "email".
        let drop_op = AlterTableOp::DropColumn {
            column_name: "email".to_string(),
        };
        writer.alter_table(table_id, &drop_op).unwrap();

        // Step 3: Re-add "email" with a DIFFERENT type.
        let readd_op = AlterTableOp::AddColumn {
            column: ColumnDef::new("email", "int64", true).unwrap(),
        };
        let result = writer.alter_table(table_id, &readd_op);
        // This should succeed. The dropped column's end_snapshot is set,
        // so the validation only checks active columns.
        assert!(
            result.is_ok(),
            "Re-adding previously dropped column should work"
        );

        // Verify the re-added column has the new type.
        let cols = writer.get_active_columns(table_id).unwrap();
        let email_col = cols.iter().find(|(name, _, _)| name == "email");
        assert!(email_col.is_some());
        assert_eq!(
            email_col.unwrap().1,
            "int64",
            "Re-added column should have new type"
        );
        // DEFENDED: works correctly with snapshot-based soft deletes
    }

    // ---------- Many columns rapidly ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_add_100_columns_rapidly() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        // Add 100 columns in rapid succession.
        for i in 0..100 {
            let op = AlterTableOp::AddColumn {
                column: ColumnDef::new(format!("col_{}", i), "int32", true).unwrap(),
            };
            writer.alter_table(table_id, &op).unwrap();
        }

        let cols = writer.get_active_columns(table_id).unwrap();
        assert_eq!(
            cols.len(),
            102,
            "Should have 2 original + 100 added columns"
        );
        // DEFENDED: works but creates 100 snapshots (one per ALTER TABLE)
        // This is correct but expensive behavior.
    }

    // ---------- Schema evolution: append with type mismatch ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_schema_evolution_append_type_mismatch() {
        let (writer, _temp) = create_test_writer().await;

        // Create table with int32 column.
        let columns = vec![ColumnDef::new("id", "int32", false).unwrap()];
        writer
            .begin_write_transaction("main", "evolve_test", &columns, WriteMode::Replace)
            .unwrap();

        // Try to append with different type for same column.
        let bad_columns = vec![ColumnDef::new("id", "varchar", false).unwrap()];
        let result =
            writer.begin_write_transaction("main", "evolve_test", &bad_columns, WriteMode::Append);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Type changes are not allowed")
        );
        // DEFENDED
    }

    // ---------- Schema evolution: append with non-nullable new column ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_schema_evolution_append_non_nullable_new_col() {
        let (writer, _temp) = create_test_writer().await;

        let columns = vec![ColumnDef::new("id", "int32", false).unwrap()];
        writer
            .begin_write_transaction("main", "evolve_test2", &columns, WriteMode::Replace)
            .unwrap();

        // Append with a new non-nullable column.
        let bad_columns = vec![
            ColumnDef::new("id", "int32", false).unwrap(),
            ColumnDef::new("required_field", "varchar", false).unwrap(), // non-nullable
        ];
        let result =
            writer.begin_write_transaction("main", "evolve_test2", &bad_columns, WriteMode::Append);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("must be nullable"));
        // DEFENDED
    }

    // ---------- Empty table name ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_empty_table_name() {
        let (writer, _temp) = create_test_writer().await;

        let columns = vec![ColumnDef::new("id", "int32", false).unwrap()];
        let result = writer.begin_write_transaction("main", "", &columns, WriteMode::Replace);
        // BUG: Empty table name is accepted! No validation on table name.
        match result {
            Ok(setup) => {
                assert_eq!(
                    setup.table_id, 1,
                    "BUG: Empty table name created successfully"
                );
            },
            Err(_) => {
                // Would be correct
            },
        }
    }

    // ---------- Empty schema name ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_empty_schema_name() {
        let (writer, _temp) = create_test_writer().await;

        let columns = vec![ColumnDef::new("id", "int32", false).unwrap()];
        let result = writer.begin_write_transaction("", "test_tbl", &columns, WriteMode::Replace);
        // BUG: Empty schema name is accepted! No validation on schema name.
        match result {
            Ok(setup) => {
                assert_eq!(
                    setup.schema_id, 1,
                    "BUG: Empty schema name created successfully"
                );
            },
            Err(_) => {
                // Would be correct
            },
        }
    }

    // ---------- Zero columns table ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_create_table_zero_columns() {
        let (writer, _temp) = create_test_writer().await;

        let columns: Vec<ColumnDef> = vec![];
        let result =
            writer.begin_write_transaction("main", "empty_cols", &columns, WriteMode::Replace);
        // BUG: Creating a table with zero columns SUCCEEDS!
        // This creates a table entry with no columns at all.
        match result {
            Ok(setup) => {
                assert!(
                    setup.column_ids.is_empty(),
                    "BUG: Zero-column table created"
                );
                // The table exists but has no columns. Any query on it will
                // produce an empty schema.
            },
            Err(_) => {
                // Would be correct
            },
        }
    }

    // ---------- Duplicate column names at creation ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_create_table_duplicate_columns() {
        let (writer, _temp) = create_test_writer().await;

        let columns = vec![
            ColumnDef::new("id", "int32", false).unwrap(),
            ColumnDef::new("id", "varchar", true).unwrap(),
        ];
        let result =
            writer.begin_write_transaction("main", "dup_test", &columns, WriteMode::Replace);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Duplicate column name")
        );
        // DEFENDED
    }

    // ---------- Column name with newlines ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_column_name_with_newlines() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        let op = AlterTableOp::AddColumn {
            column: ColumnDef::new("line\none", "varchar", true).unwrap(),
        };
        let result = writer.alter_table(table_id, &op);
        // BUG: Newlines in column names are accepted. No sanitization.
        match result {
            Ok(_) => {
                let cols = writer.get_active_columns(table_id).unwrap();
                let newline_col = cols.iter().find(|(name, _, _)| name.contains('\n'));
                assert!(
                    newline_col.is_some(),
                    "BUG: Newline in column name accepted"
                );
            },
            Err(_) => {
                // Would be correct
            },
        }
    }

    // ---------- Column name with null bytes ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_column_name_with_null_byte() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        let op = AlterTableOp::AddColumn {
            column: ColumnDef::new("col\0name", "varchar", true).unwrap(),
        };
        let result = writer.alter_table(table_id, &op);
        // Null bytes in column names: SQLite may or may not accept this.
        match result {
            Ok(_) => {
                // BUG: null byte in column name accepted
            },
            Err(_) => {
                // SQLite or the parameterized query layer rejected it
            },
        }
    }

    // ---------- is_type_promotion_allowed does not normalize types ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_type_promotion_with_aliases() {
        let (writer, _temp) = create_test_writer().await;

        // Create table with "integer" type (alias for int32).
        let columns = vec![ColumnDef::new("id", "integer", false).unwrap()];
        let setup = writer
            .begin_write_transaction("main", "alias_test", &columns, WriteMode::Replace)
            .unwrap();

        // Try to widen "integer" -> "int64".
        let op = AlterTableOp::AlterColumnType(AlterColumnTypeOp {
            column_name: "id".to_string(),
            new_type: "int64".to_string(),
        });
        let result = writer.alter_table(setup.table_id, &op);
        // BUG: is_type_promotion_allowed("integer", "int64") returns false!
        // It only knows "int32" -> "int64", not "integer" -> "int64".
        // Type aliases are NOT normalized before promotion checking.
        match result {
            Ok(_) => {
                // Would mean the bug is fixed
            },
            Err(e) => {
                assert!(
                    e.to_string().contains("widening type promotions"),
                    "BUG CONFIRMED: type alias 'integer' not recognized for promotion: {}",
                    e
                );
            },
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_type_promotion_with_case_mismatch() {
        let (writer, _temp) = create_test_writer().await;

        // Create table with "INT32" type (uppercase).
        let columns = vec![ColumnDef::new("id", "INT32", false).unwrap()];
        let setup = writer
            .begin_write_transaction("main", "case_test", &columns, WriteMode::Replace)
            .unwrap();

        // Try to widen "INT32" -> "int64".
        let op = AlterTableOp::AlterColumnType(AlterColumnTypeOp {
            column_name: "id".to_string(),
            new_type: "int64".to_string(),
        });
        let result = writer.alter_table(setup.table_id, &op);
        // BUG: is_type_promotion_allowed("INT32", "int64") returns false!
        // The function does case-sensitive matching. Uppercase types break promotion.
        match result {
            Ok(_) => {
                // Fixed
            },
            Err(e) => {
                assert!(
                    e.to_string().contains("widening type promotions"),
                    "BUG CONFIRMED: uppercase type 'INT32' not recognized for promotion: {}",
                    e
                );
            },
        }
    }

    // ---------- Table name with special characters ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_table_name_sql_injection() {
        let (writer, _temp) = create_test_writer().await;

        let columns = vec![ColumnDef::new("id", "int32", false).unwrap()];
        let result = writer.begin_write_transaction(
            "main",
            "t'; DROP TABLE ducklake_table; --",
            &columns,
            WriteMode::Replace,
        );
        // Should be safe due to parameterized queries.
        assert!(
            result.is_ok(),
            "SQL injection in table name should be safe with parameterized queries"
        );
        // DEFENDED: parameterized queries
    }

    // ---------- Schema name with SQL injection ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_schema_name_sql_injection() {
        let (writer, _temp) = create_test_writer().await;

        let columns = vec![ColumnDef::new("id", "int32", false).unwrap()];
        let result = writer.begin_write_transaction(
            "s'; DROP TABLE ducklake_schema; --",
            "test_tbl",
            &columns,
            WriteMode::Replace,
        );
        assert!(
            result.is_ok(),
            "SQL injection in schema name should be safe with parameterized queries"
        );
        // DEFENDED
    }

    // ---------- Alter table on dropped table ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_alter_dropped_table() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        // Drop the table.
        writer.drop_table(table_id).unwrap();

        // Try to alter the dropped table.
        let op = AlterTableOp::AddColumn {
            column: ColumnDef::new("new_col", "varchar", true).unwrap(),
        };
        let result = writer.alter_table(table_id, &op);
        // After dropping, all columns have end_snapshot set, so get_active_columns returns [].
        // validate_table_has_columns should catch this.
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("no active columns"),
            "Should detect table is dropped"
        );
        // DEFENDED
    }

    // ---------- Multiple renames creating a chain ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_rename_chain_a_to_b_to_c() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        // Rename name -> temp_name -> final_name.
        writer
            .alter_table(
                table_id,
                &AlterTableOp::RenameColumn {
                    old_name: "name".to_string(),
                    new_name: "temp_name".to_string(),
                },
            )
            .unwrap();
        writer
            .alter_table(
                table_id,
                &AlterTableOp::RenameColumn {
                    old_name: "temp_name".to_string(),
                    new_name: "final_name".to_string(),
                },
            )
            .unwrap();

        let cols = writer.get_active_columns(table_id).unwrap();
        assert_eq!(cols.len(), 2);
        assert_eq!(cols[1].0, "final_name");
        // DEFENDED: rename chain works correctly
    }

    // ---------- Swap column names (requires temp) ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_swap_column_names() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        // Try to swap "id" and "name" columns.
        // Step 1: rename "id" -> "temp_id".
        writer
            .alter_table(
                table_id,
                &AlterTableOp::RenameColumn {
                    old_name: "id".to_string(),
                    new_name: "temp_id".to_string(),
                },
            )
            .unwrap();
        // Step 2: rename "name" -> "id".
        writer
            .alter_table(
                table_id,
                &AlterTableOp::RenameColumn {
                    old_name: "name".to_string(),
                    new_name: "id".to_string(),
                },
            )
            .unwrap();
        // Step 3: rename "temp_id" -> "name".
        writer
            .alter_table(
                table_id,
                &AlterTableOp::RenameColumn {
                    old_name: "temp_id".to_string(),
                    new_name: "name".to_string(),
                },
            )
            .unwrap();

        let cols = writer.get_active_columns(table_id).unwrap();
        assert_eq!(cols.len(), 2);
        // After swap: what was "id"(int32) is now named "name", what was "name"(varchar) is now "id".
        assert_eq!(cols[0].0, "name");
        assert_eq!(cols[0].1, "int32");
        assert_eq!(cols[1].0, "id");
        assert_eq!(cols[1].1, "varchar");
        // DEFENDED: swap works correctly via temp name
    }

    // ---------- Add column then alter its type ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_add_then_alter_type() {
        let (writer, _temp) = create_test_writer().await;
        let table_id = setup_table(&writer);

        // Add int8 column.
        writer
            .alter_table(
                table_id,
                &AlterTableOp::AddColumn {
                    column: ColumnDef::new("score", "int8", true).unwrap(),
                },
            )
            .unwrap();

        // Widen to int64.
        let result = writer.alter_table(
            table_id,
            &AlterTableOp::AlterColumnType(AlterColumnTypeOp {
                column_name: "score".to_string(),
                new_type: "int64".to_string(),
            }),
        );
        assert!(result.is_ok(), "Widening newly added column should work");

        let cols = writer.get_active_columns(table_id).unwrap();
        let score_col = cols.iter().find(|(name, _, _)| name == "score").unwrap();
        assert_eq!(score_col.1, "int64");
        // DEFENDED
    }

    // ---------- Column type stored as-is (case not normalized) ----------

    #[tokio::test(flavor = "multi_thread")]
    async fn attack_column_type_case_preservation() {
        let (writer, _temp) = create_test_writer().await;

        // Store uppercase type. This gets stored as-is in the catalog.
        let columns = vec![ColumnDef::new("id", "INT32", false).unwrap()];
        let setup = writer
            .begin_write_transaction("main", "case_table", &columns, WriteMode::Replace)
            .unwrap();

        let cols = writer.get_active_columns(setup.table_id).unwrap();
        // BUG (minor): The type is stored exactly as provided, including case.
        // "INT32" is stored rather than normalized "int32".
        // ducklake_to_arrow_type handles case-insensitivity on read, so this works,
        // but the stored metadata is inconsistent with DuckLake conventions.
        assert_eq!(cols[0].1, "INT32", "Type stored as-is with original case");
    }
}

// ========================================================================
// Part 3: Type promotion whitelist gaps
// ========================================================================

#[cfg(feature = "write")]
mod type_promotion_tests {
    use datafusion_ducklake::metadata_writer::is_type_promotion_allowed;

    #[test]
    fn attack_promotion_int_to_float() {
        // int32 -> float64 should arguably be allowed (no data loss).
        // But the whitelist doesn't include it.
        // BUG (design): integer-to-float promotion not supported.
        assert!(
            !is_type_promotion_allowed("int32", "float64"),
            "int32->float64 not in whitelist (missing promotion)"
        );
        assert!(
            !is_type_promotion_allowed("int64", "float64"),
            "int64->float64 not in whitelist (missing promotion, precision loss though)"
        );
    }

    #[test]
    fn attack_promotion_decimal_widening() {
        // decimal(10,2) -> decimal(20,2) should be a valid widening.
        // BUG: No decimal promotion rules exist at all!
        assert!(
            !is_type_promotion_allowed("decimal(10, 2)", "decimal(20, 2)"),
            "No decimal widening support"
        );
    }

    #[test]
    fn attack_promotion_varchar_widening() {
        // varchar(100) -> varchar(200) should arguably be allowed.
        // But there are no string widening rules.
        assert!(
            !is_type_promotion_allowed("varchar(100)", "varchar(200)"),
            "No varchar widening support"
        );
    }

    #[test]
    fn attack_promotion_timestamp_units() {
        // timestamp_s -> timestamp_ms -> timestamp -> timestamp_ns
        // Only timestamp -> timestamptz is supported.
        // BUG: No timestamp precision widening.
        assert!(!is_type_promotion_allowed("timestamp_s", "timestamp_ms"));
        assert!(!is_type_promotion_allowed("timestamp_ms", "timestamp"));
        assert!(!is_type_promotion_allowed("timestamp", "timestamp_ns"));
    }

    #[test]
    fn attack_promotion_signed_to_unsigned() {
        // int32 -> uint64 is NOT allowed (correct: signed can be negative).
        assert!(!is_type_promotion_allowed("int32", "uint64"));
        // DEFENDED: correct behavior
    }

    #[test]
    fn attack_promotion_float32_to_float64_aliases() {
        // The promotion table uses "float" -> "double", not "float32" -> "float64".
        // BUG: The canonical type names don't match the promotion table!
        assert!(
            !is_type_promotion_allowed("float32", "float64"),
            "BUG: 'float32' -> 'float64' not in promotion table (only 'float' -> 'double')"
        );
    }

    #[test]
    fn attack_promotion_tinyint_to_int() {
        // Type aliases: "tinyint" is an alias for "int8".
        // BUG: Aliases not normalized before promotion check.
        assert!(
            !is_type_promotion_allowed("tinyint", "bigint"),
            "BUG: aliases 'tinyint' -> 'bigint' not in promotion table"
        );
    }
}
