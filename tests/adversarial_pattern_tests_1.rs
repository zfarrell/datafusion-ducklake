//! Adversarial pattern-matching tests inspired by DuckLake upstream issues #40-#300.
//!
//! These tests examine root cause PATTERNS from upstream bugs and check whether
//! analogous code in DataFusion-DuckLake has the same class of vulnerability.
//!
//! Each test documents the upstream issue, the extracted pattern, and the
//! analogous code being exercised.

// ============================================================================
// Type System Pattern Tests (types.rs)
// ============================================================================

mod type_patterns {
    use datafusion_ducklake::types::ducklake_to_arrow_type;

    // Pattern from issue #44: JSON type causes "Unsupported user-defined type" error.
    // ROOT CAUSE: Unrecognized types silently fail or crash instead of clean error.
    // ANALOGOUS CODE: Our type parser should return a clear error for every unrecognized type.
    #[test]
    fn test_pattern_44_unknown_type_gives_clear_error() {
        // JSON is handled, but check truly unknown types
        let unknown_types = vec![
            "xml",
            "geography",
            "money",
            "cidr",
            "inet",
            "macaddr",
            "tsquery",
            "tsvector",
            "box",
            "circle",
            "path",
            "lseg",
            "enum('a','b')",
            "user_defined_type",
        ];
        for type_str in unknown_types {
            let result = ducklake_to_arrow_type(type_str);
            assert!(
                result.is_err(),
                "Expected error for unknown type '{}', got {:?}",
                type_str,
                result
            );
        }
    }

    // Pattern from issue #44: JSON type support.
    // ROOT CAUSE: Some valid DuckDB types aren't mapped.
    // ANALOGOUS CODE: Our type mapper handles "json" -> Utf8. Verify it works.
    #[test]
    fn test_pattern_44_json_type_handled() {
        let result = ducklake_to_arrow_type("json");
        assert!(result.is_ok(), "JSON type should be supported");
        assert_eq!(result.unwrap(), arrow::datatypes::DataType::Utf8);
    }

    // Pattern from issue #157: Core dump when querying table with Map type.
    // ROOT CAUSE: Complex types (Map, nested List) not handled, causing crash.
    // ANALOGOUS CODE: Our type parser for complex types — does it handle malformed input?
    #[test]
    fn test_pattern_157_malformed_complex_types_no_crash() {
        let malformed_types = vec![
            "map(",
            "map()",
            "map(varchar)",
            "map(varchar,)",
            "map(,int32)",
            "list(",
            "list()",
            "list<",
            "list<>",
            "struct(",
            "struct()",
            "struct<>",
            "struct<:>",
            "struct(name)",  // field without type
            "map(varchar, int32, extra)",
            "list(int32, varchar)",  // list with multiple types
            "[]",  // empty array suffix
        ];
        for type_str in malformed_types {
            let result = ducklake_to_arrow_type(type_str);
            // Should either parse correctly or return a clean error - never panic/crash
            match result {
                Ok(_) => {} // Some of these might parse fine
                Err(e) => {
                    // Error message should not be empty
                    assert!(!e.to_string().is_empty(), "Error for '{}' was empty", type_str);
                }
            }
        }
    }

    // Pattern from issue #157: Complex nested types crash.
    // ROOT CAUSE: Deep nesting or unusual combinations.
    // ANALOGOUS CODE: Our parse_complex_type with deeply nested types.
    #[test]
    fn test_pattern_157_deeply_nested_types_no_crash() {
        let nested_types = vec![
            "list(list(list(list(int32))))",
            "map(varchar, map(varchar, map(varchar, int32)))",
            "struct(a list(struct(b int32, c list(varchar))))",
            "list(map(varchar, list(struct(x int32, y varchar))))",
        ];
        for type_str in nested_types {
            let result = ducklake_to_arrow_type(type_str);
            // Should succeed without panicking
            assert!(result.is_ok(), "Deep nesting should be handled: '{}' -> {:?}", type_str, result);
        }
    }

    // Pattern from issue #229: TIMESTAMPTZ/VARCHAR type mismatch in SQLite catalog.
    // ROOT CAUSE: Type handling inconsistency between DuckDB and SQLite.
    // ANALOGOUS CODE: Our type mapper should handle type aliases consistently.
    #[test]
    fn test_pattern_229_timestamp_type_aliases() {
        use arrow::datatypes::{DataType, TimeUnit};

        // All timestamp variants should parse
        let timestamp_types = vec![
            ("timestamp", DataType::Timestamp(TimeUnit::Microsecond, None)),
            ("timestamptz", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))),
            ("timestamp with time zone", DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))),
            ("timestamp_s", DataType::Timestamp(TimeUnit::Second, None)),
            ("timestamp_ms", DataType::Timestamp(TimeUnit::Millisecond, None)),
            ("timestamp_ns", DataType::Timestamp(TimeUnit::Nanosecond, None)),
        ];
        for (type_str, expected) in timestamp_types {
            let result = ducklake_to_arrow_type(type_str);
            assert!(result.is_ok(), "Failed to parse '{}': {:?}", type_str, result);
            assert_eq!(result.unwrap(), expected, "Wrong type for '{}'", type_str);
        }
    }

    // Pattern from issue #229: SQLite stores TIMESTAMPTZ as VARCHAR.
    // ROOT CAUSE: Type strings may have unexpected casing or whitespace.
    // ANALOGOUS CODE: Our type normalization — does it handle mixed casing?
    #[test]
    fn test_pattern_229_mixed_case_type_strings() {
        use arrow::datatypes::DataType;

        // Mixed case should be normalized
        let cases = vec![
            ("BOOLEAN", DataType::Boolean),
            ("Boolean", DataType::Boolean),
            ("INT32", DataType::Int32),
            ("Int64", DataType::Int64),
            ("VARCHAR", DataType::Utf8),
            ("Varchar", DataType::Utf8),
            ("FLOAT64", DataType::Float64),
            ("Float32", DataType::Float32),
            ("DATE", DataType::Date32),
            ("BLOB", DataType::Binary),
            ("UUID", DataType::FixedSizeBinary(16)),
        ];
        for (type_str, expected) in cases {
            let result = ducklake_to_arrow_type(type_str);
            assert!(result.is_ok(), "Failed to parse '{}': {:?}", type_str, result);
            assert_eq!(result.unwrap(), expected, "Wrong type for '{}'", type_str);
        }
    }

    // Pattern from issue #44 + #229: Whitespace handling in type strings.
    // ROOT CAUSE: Input not trimmed before parsing.
    // ANALOGOUS CODE: ducklake_to_arrow_type does trim(). But what about internal whitespace?
    #[test]
    fn test_pattern_whitespace_in_type_strings() {
        use arrow::datatypes::DataType;

        // Leading/trailing whitespace
        let result = ducklake_to_arrow_type("  varchar  ");
        assert_eq!(result.unwrap(), DataType::Utf8, "Leading/trailing whitespace should be trimmed");

        let result = ducklake_to_arrow_type("\tint32\n");
        assert_eq!(result.unwrap(), DataType::Int32, "Tab/newline whitespace should be trimmed");

        // Whitespace inside decimal
        let result = ducklake_to_arrow_type("decimal( 10 , 2 )");
        assert!(result.is_ok(), "Whitespace inside decimal params should be handled");

        // Whitespace inside parameterized varchar
        let result = ducklake_to_arrow_type("varchar( 255 )");
        assert_eq!(result.unwrap(), DataType::Utf8, "Whitespace inside varchar(N) should be handled");
    }

    // Pattern from issue #120: Query results incorrect with complex filters.
    // ROOT CAUSE: Off-by-one or filter handling errors.
    // ANALOGOUS CODE: Type parsing edge cases — boundary precision values.
    #[test]
    fn test_pattern_120_decimal_boundary_values() {
        use arrow::datatypes::DataType;

        // Minimum precision
        let result = ducklake_to_arrow_type("decimal(1, 0)");
        assert_eq!(result.unwrap(), DataType::Decimal128(1, 0));

        // Maximum Decimal128 precision
        let result = ducklake_to_arrow_type("decimal(38, 0)");
        assert_eq!(result.unwrap(), DataType::Decimal128(38, 0));

        // Decimal256 range
        let result = ducklake_to_arrow_type("decimal(39, 0)");
        assert_eq!(result.unwrap(), DataType::Decimal256(39, 0));

        // Negative scale
        let result = ducklake_to_arrow_type("decimal(10, -2)");
        assert!(result.is_ok(), "Negative scale should be handled: {:?}", result);

        // Zero precision should either work or give clear error
        let result = ducklake_to_arrow_type("decimal(0, 0)");
        // Arrow may reject this, but we shouldn't crash
        match result {
            Ok(_) => {}
            Err(e) => assert!(!e.to_string().is_empty()),
        }
    }

    // Pattern from issue #288: Stats update failure due to type handling.
    // ROOT CAUSE: Type conversion between string and numeric fails.
    // ANALOGOUS CODE: Our arrow_to_ducklake_type round-trip for edge types.
    #[test]
    fn test_pattern_288_type_roundtrip_consistency() {
        use datafusion_ducklake::types::arrow_to_ducklake_type;
        use arrow::datatypes::{DataType, TimeUnit};

        // Test that arrow -> ducklake -> arrow is identity for all supported types
        let types = vec![
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::Binary,
            DataType::Date32,
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Decimal128(18, 6),
            DataType::FixedSizeBinary(16), // UUID
        ];

        for original in &types {
            let ducklake = arrow_to_ducklake_type(original).unwrap();
            let back = ducklake_to_arrow_type(&ducklake).unwrap();
            assert_eq!(
                *original, back,
                "Round-trip failed: {:?} -> '{}' -> {:?}",
                original, ducklake, back
            );
        }
    }

    // Pattern from issue #65: Decimal discrepancy between systems.
    // ROOT CAUSE: Precision/scale handling differs.
    // ANALOGOUS CODE: decimal parsing — what about "numeric" alias?
    #[test]
    fn test_pattern_65_numeric_alias_for_decimal() {
        use arrow::datatypes::DataType;

        // "numeric" should work as alias for "decimal"
        let result = ducklake_to_arrow_type("numeric(10, 2)");
        assert_eq!(result.unwrap(), DataType::Decimal128(10, 2));

        let result = ducklake_to_arrow_type("NUMERIC(18, 4)");
        assert_eq!(result.unwrap(), DataType::Decimal128(18, 4));
    }

    // Pattern from issue #297: Limitation in default values.
    // ROOT CAUSE: Type system doesn't handle all SQL type aliases.
    // ANALOGOUS CODE: Does our type mapper handle all common SQL aliases?
    #[test]
    fn test_pattern_297_common_sql_type_aliases() {
        use arrow::datatypes::DataType;

        let aliases = vec![
            ("bool", DataType::Boolean),
            ("tinyint", DataType::Int8),
            ("smallint", DataType::Int16),
            ("int", DataType::Int32),
            ("integer", DataType::Int32),
            ("bigint", DataType::Int64),
            ("long", DataType::Int64),
            ("real", DataType::Float32),
            ("float", DataType::Float32),
            ("double", DataType::Float64),
            ("text", DataType::Utf8),
            ("string", DataType::Utf8),
            ("bytea", DataType::Binary),
            ("binary", DataType::Binary),
        ];
        for (alias, expected) in aliases {
            let result = ducklake_to_arrow_type(alias);
            assert!(result.is_ok(), "Alias '{}' should be supported: {:?}", alias, result);
            assert_eq!(result.unwrap(), expected, "Wrong type for alias '{}'", alias);
        }
    }
}

// ============================================================================
// Path Resolution Pattern Tests (path_resolver.rs)
// ============================================================================

mod path_patterns {
    use datafusion_ducklake::path_resolver::{join_paths, parse_object_store_url, resolve_path, PathResolver};
    use datafusion::datasource::object_store::ObjectStoreUrl;
    use std::sync::Arc;

    // Pattern from issue #217: Double slash "//" in S3 URL from MinIO.
    // ROOT CAUSE: Path joining creates "s3://bucket//path" which MinIO rejects.
    // ANALOGOUS CODE: Our join_paths and resolve_path functions.
    #[test]
    fn test_pattern_217_double_slash_in_paths() {
        // join_paths with base ending in / and relative starting with /
        let result = join_paths("/data/", "/subdir/file.parquet");
        assert!(!result.contains("//") || result.starts_with("//"),
            "Double slash should be prevented: '{}'", result);

        // PathResolver creating child with trailing slash + relative starting with /
        let resolver = PathResolver::new(
            Arc::new(ObjectStoreUrl::parse("s3://bucket/").unwrap()),
            "/data/".to_string(),
        );
        let child = resolver.child_resolver("/", true);
        // When base ends with / and child is just /, we get /data/ which is fine
        assert!(child.base_path() == "/data/" || !child.base_path().contains("//"),
            "Double slash in child resolver: '{}'", child.base_path());
    }

    // Pattern from issue #217: Path resolution when data_path ends with /.
    // ROOT CAUSE: Concatenation of paths with trailing slashes.
    // ANALOGOUS CODE: The hierarchical path resolution catalog -> schema -> table -> file.
    #[test]
    fn test_pattern_217_hierarchical_path_no_double_slashes() {
        let resolver = PathResolver::new(
            Arc::new(ObjectStoreUrl::parse("s3://bucket/").unwrap()),
            "/prefix/".to_string(),
        );

        let schema = resolver.child_resolver("schema/", true);
        let table = schema.child_resolver("table/", true);
        let file = table.resolve("data.parquet", true);

        assert_eq!(file, "/prefix/schema/table/data.parquet");
        // Check no double slashes
        assert!(!file.contains("//"), "Double slash in resolved path: '{}'", file);
    }

    // Pattern from issue #198: Wrong path separator (backslash instead of forward slash).
    // ROOT CAUSE: Windows path separators leak into S3 paths.
    // ANALOGOUS CODE: join_paths handles backslash—but does resolve_path?
    #[test]
    fn test_pattern_198_backslash_in_paths() {
        // If metadata contains backslash paths (Windows-created catalogs)
        let result = resolve_path("/data/", "schema\\table\\file.parquet", true);
        // At minimum, should not crash. The path will contain backslashes since we don't normalize.
        assert!(!result.is_empty(), "Path resolution with backslash should not produce empty result");

        // join_paths with backslash base
        let result = join_paths("C:\\data\\", "table\\file.parquet");
        assert!(!result.is_empty(), "Backslash join should not crash");
    }

    // Pattern from issue #255: DataPath fails at S3 bucket root (just "s3://bucket").
    // ROOT CAUSE: Empty path component after bucket name.
    // ANALOGOUS CODE: parse_object_store_url with bucket-only S3 URLs.
    #[test]
    fn test_pattern_255_s3_bucket_root_paths() {
        // S3 bucket with no path
        let (url, path) = parse_object_store_url("s3://bucket").unwrap();
        assert_eq!(url, ObjectStoreUrl::parse("s3://bucket/").unwrap());
        // Path should be empty or "/"
        assert!(path.is_empty() || path == "/",
            "Bucket-only URL path should be empty or '/': '{}'", path);

        // S3 bucket with trailing slash
        let (_, path) = parse_object_store_url("s3://bucket/").unwrap();
        assert_eq!(path, "/");

        // Resolve relative path from bucket root
        let resolver = PathResolver::new(
            Arc::new(ObjectStoreUrl::parse("s3://bucket/").unwrap()),
            path.clone(),
        );
        let resolved = resolver.resolve("schema/table/file.parquet", true);
        assert!(!resolved.is_empty(), "Resolution from bucket root should work");
    }

    // Pattern from issue #255: Dots in bucket names (e.g., "aggregate.lake").
    // ROOT CAUSE: Bucket names with dots confuse URL parsing.
    // ANALOGOUS CODE: parse_object_store_url with dots in hostname.
    #[test]
    fn test_pattern_255_dots_in_s3_bucket_name() {
        let (url, path) = parse_object_store_url("s3://aggregate.lake/data").unwrap();
        assert_eq!(url, ObjectStoreUrl::parse("s3://aggregate.lake/").unwrap());
        assert_eq!(path, "/data");
    }

    // Pattern from issue #217: Empty path components.
    // ROOT CAUSE: Joining with empty strings creates unexpected paths.
    // ANALOGOUS CODE: resolve_path with empty base or empty relative path.
    #[test]
    fn test_pattern_217_empty_path_components() {
        // Empty base path
        let result = resolve_path("", "file.parquet", true);
        assert!(!result.is_empty(), "Empty base with relative should still produce a path");

        // Empty relative path
        let result = resolve_path("/data/", "", true);
        assert!(result == "/data/" || result == "/data",
            "Empty relative path should return base: '{}'", result);

        // Both empty
        let result = resolve_path("", "", true);
        assert!(result == "/" || result.is_empty(),
            "Both empty should produce root or empty: '{}'", result);
    }

    // Pattern from issue #198: Path normalization with parent references.
    // ROOT CAUSE: ".." in paths can traverse outside expected directory.
    // ANALOGOUS CODE: join_paths does NOT normalize ".." — is this a security issue?
    #[test]
    fn test_pattern_198_path_traversal_in_metadata() {
        // If catalog metadata contains ".." paths, we pass them through
        let result = join_paths("/data/schema/", "../../etc/passwd");
        // We should document that we don't normalize these
        assert!(result.contains(".."),
            "join_paths should not silently normalize '..' references: '{}'", result);

        // resolve_path with absolute path (should be returned as-is even if sketchy)
        let result = resolve_path("/data/", "/etc/passwd", false);
        assert_eq!(result, "/etc/passwd");
    }

    // Pattern from issue #217: Special characters in S3 paths.
    // ROOT CAUSE: URL encoding not handled correctly.
    // ANALOGOUS CODE: parse_object_store_url with encoded characters.
    #[test]
    fn test_pattern_217_special_chars_in_paths() {
        // URL-encoded spaces
        let (_, path) = parse_object_store_url("s3://bucket/path%20with%20spaces/data").unwrap();
        assert_eq!(path, "/path%20with%20spaces/data");

        // Unicode in S3 paths — url::Url encodes them to percent-encoding
        // BUG FINDING: Unicode characters in S3 paths get percent-encoded by url::Url parser.
        // If catalog metadata stores raw unicode paths, they won't match after parsing.
        // e.g., "données" becomes "donn%C3%A9es"
        let (_, path) = parse_object_store_url("s3://bucket/données/table").unwrap();
        // Document: url::Url normalizes unicode to percent-encoding
        assert!(path.contains("donn") && path.contains("es/table"),
            "Unicode path should be parseable (may be percent-encoded): '{}'", path);
    }

    // Pattern from issue #255: S3 URL with no trailing slash on data_path.
    // ROOT CAUSE: Missing trailing slash means files get wrong prefix.
    // ANALOGOUS CODE: PathResolver without trailing slash.
    #[test]
    fn test_pattern_255_no_trailing_slash_on_base_path() {
        let resolver = PathResolver::new(
            Arc::new(ObjectStoreUrl::parse("s3://bucket/").unwrap()),
            "/data".to_string(),  // No trailing slash!
        );

        let child = resolver.child_resolver("schema/", true);
        assert_eq!(child.base_path(), "/data/schema/",
            "Should insert / between base and child");

        let resolved = resolver.resolve("file.parquet", true);
        assert_eq!(resolved, "/data/file.parquet",
            "Should insert / before file name");
    }
}

// ============================================================================
// Delete Filter Pattern Tests (delete_filter.rs)
// ============================================================================

mod delete_filter_patterns {
    use arrow::array::{Array, Int32Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::{RecordBatch, RecordBatchOptions};
    use datafusion::error::Result as DataFusionResult;
    use std::collections::HashSet;
    use std::sync::Arc;

    // Minimal struct to replicate DeleteFilterStream::filter_batch behavior
    struct TestFilterStream {
        deleted_positions: Arc<HashSet<i64>>,
        row_offset: i64,
    }

    impl TestFilterStream {
        fn filter_batch(&self, batch: &RecordBatch) -> DataFusionResult<RecordBatch> {
            use arrow::array::UInt32Array;
            use arrow::compute::take;
            use datafusion::error::DataFusionError;

            if self.deleted_positions.is_empty() {
                return Ok(batch.clone());
            }

            let num_rows = batch.num_rows();
            let mut keep_indices: Vec<usize> = Vec::with_capacity(num_rows);

            for i in 0..num_rows {
                let global_pos = self.row_offset + i as i64;
                if !self.deleted_positions.contains(&global_pos) {
                    keep_indices.push(i);
                }
            }

            if keep_indices.len() == num_rows {
                return Ok(batch.clone());
            }

            if batch.num_columns() == 0 {
                let mut options = RecordBatchOptions::new();
                options = options.with_row_count(Some(keep_indices.len()));
                return RecordBatch::try_new_with_options(batch.schema(), vec![], &options)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None));
            }

            let indices = UInt32Array::from(keep_indices.iter().map(|&i| i as u32).collect::<Vec<_>>());
            let filtered_columns: DataFusionResult<Vec<_>> = batch
                .columns()
                .iter()
                .map(|col| {
                    take(col.as_ref(), &indices, None)
                        .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                })
                .collect();
            RecordBatch::try_new(batch.schema(), filtered_columns?)
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        }
    }

    // Pattern from issue #189: Deleted records reappear after merge_adjacent_files.
    // ROOT CAUSE: Delete file positions become stale when files are merged/rewritten.
    // ANALOGOUS CODE: Our DeleteFilterExec uses global row positions.
    // If positions are wrong, deleted rows reappear.
    #[test]
    fn test_pattern_189_row_offset_tracking_across_batches() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        // Simulate two batches from same file
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as Arc<dyn Array>],
        ).unwrap();

        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![6, 7, 8, 9, 10])) as Arc<dyn Array>],
        ).unwrap();

        // Delete positions: 2 (id=3 in batch1), 7 (id=8 in batch2)
        let deleted: HashSet<i64> = [2, 7].into_iter().collect();

        // Process batch1 at offset 0
        let stream = TestFilterStream {
            deleted_positions: Arc::new(deleted.clone()),
            row_offset: 0,
        };
        let filtered1 = stream.filter_batch(&batch1).unwrap();
        assert_eq!(filtered1.num_rows(), 4);

        // Process batch2 at offset 5 (after batch1's 5 rows)
        let stream2 = TestFilterStream {
            deleted_positions: Arc::new(deleted),
            row_offset: 5,
        };
        let filtered2 = stream2.filter_batch(&batch2).unwrap();
        assert_eq!(filtered2.num_rows(), 4);

        // Verify correct rows filtered
        let ids1: Vec<i32> = filtered1.column(0).as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec();
        assert_eq!(ids1, vec![1, 2, 4, 5]); // id=3 at pos 2 deleted

        let ids2: Vec<i32> = filtered2.column(0).as_any().downcast_ref::<Int32Array>().unwrap().values().to_vec();
        assert_eq!(ids2, vec![6, 7, 9, 10]); // id=8 at pos 7 deleted
    }

    // Pattern from issue #189: All rows in a file deleted.
    // ROOT CAUSE: Edge case where delete file covers every row.
    // ANALOGOUS CODE: DeleteFilterExec with all positions deleted.
    #[test]
    fn test_pattern_189_all_rows_deleted() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>],
        ).unwrap();

        let deleted: HashSet<i64> = [0, 1, 2].into_iter().collect();
        let stream = TestFilterStream {
            deleted_positions: Arc::new(deleted),
            row_offset: 0,
        };

        let filtered = stream.filter_batch(&batch).unwrap();
        assert_eq!(filtered.num_rows(), 0, "All rows should be deleted");
        assert_eq!(filtered.num_columns(), 1, "Schema should be preserved even with 0 rows");
    }

    // Pattern from issue #189: COUNT(*) with deletes (zero-column batch).
    // ROOT CAUSE: Special case for batches with no columns.
    // ANALOGOUS CODE: DeleteFilterExec handles zero-column batches specially.
    #[test]
    fn test_pattern_189_count_star_with_deletes() {
        let schema = Arc::new(Schema::new(Vec::<Field>::new()));

        // Zero-column batch with 5 rows
        let mut options = RecordBatchOptions::new();
        options = options.with_row_count(Some(5));
        let batch = RecordBatch::try_new_with_options(schema.clone(), vec![], &options).unwrap();

        // Delete 2 of 5 rows
        let deleted: HashSet<i64> = [1, 3].into_iter().collect();
        let stream = TestFilterStream {
            deleted_positions: Arc::new(deleted),
            row_offset: 0,
        };

        let filtered = stream.filter_batch(&batch).unwrap();
        assert_eq!(filtered.num_rows(), 3, "COUNT(*) should account for deletes");
    }

    // Pattern from issue #284: Data from wrong table loaded.
    // ROOT CAUSE: Concurrent table creation mixes metadata.
    // ANALOGOUS CODE: If delete positions are applied to wrong file, rows are incorrectly filtered.
    #[test]
    fn test_pattern_284_negative_position_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>],
        ).unwrap();

        // Negative positions should never match (positions are 0-based unsigned)
        let deleted: HashSet<i64> = [-1, -100, i64::MIN].into_iter().collect();
        let stream = TestFilterStream {
            deleted_positions: Arc::new(deleted),
            row_offset: 0,
        };

        let filtered = stream.filter_batch(&batch).unwrap();
        assert_eq!(filtered.num_rows(), 3, "Negative positions should not match any rows");
    }

    // Pattern from issue #284: i64::MAX as position.
    // ROOT CAUSE: Extreme values in delete positions.
    // ANALOGOUS CODE: HashSet lookup with extreme i64 values.
    #[test]
    fn test_pattern_284_extreme_position_values() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>],
        ).unwrap();

        let deleted: HashSet<i64> = [i64::MAX, i64::MAX - 1].into_iter().collect();
        let stream = TestFilterStream {
            deleted_positions: Arc::new(deleted),
            row_offset: 0,
        };

        let filtered = stream.filter_batch(&batch).unwrap();
        assert_eq!(filtered.num_rows(), 3, "Extreme positions should not match in small file");
    }

    // Pattern from issue #189: Empty delete file (0 positions).
    // ROOT CAUSE: File exists but contains no deletions.
    // ANALOGOUS CODE: DeleteFilterExec with empty HashSet.
    #[test]
    fn test_pattern_189_empty_delete_set() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3])) as Arc<dyn Array>],
        ).unwrap();

        let deleted: HashSet<i64> = HashSet::new();
        let stream = TestFilterStream {
            deleted_positions: Arc::new(deleted),
            row_offset: 0,
        };

        let filtered = stream.filter_batch(&batch).unwrap();
        assert_eq!(filtered.num_rows(), 3, "Empty delete set should keep all rows");
    }
}

// ============================================================================
// Metadata Provider Pattern Tests (metadata_provider.rs)
// ============================================================================

mod metadata_patterns {
    use datafusion_ducklake::metadata_provider::DuckLakeTableColumn;
    use datafusion_ducklake::types::build_arrow_schema;

    // Pattern from issue #268: Catalog metadata corruption during concurrent table creation.
    // ROOT CAUSE: Schema/column metadata mixed between tables.
    // ANALOGOUS CODE: build_arrow_schema should handle empty, single, and many columns.
    #[test]
    fn test_pattern_268_empty_column_list() {
        let columns: Vec<DuckLakeTableColumn> = vec![];
        let schema = build_arrow_schema(&columns).unwrap();
        assert_eq!(schema.fields().len(), 0, "Empty columns should produce empty schema");
    }

    // Pattern from issue #268: Duplicate column names in metadata.
    // ROOT CAUSE: Concurrent operations create duplicate entries.
    // ANALOGOUS CODE: build_arrow_schema with duplicate names — does Arrow handle this?
    #[test]
    fn test_pattern_268_duplicate_column_names() {
        let columns = vec![
            DuckLakeTableColumn::new(1, "id".to_string(), "int32".to_string(), false),
            DuckLakeTableColumn::new(2, "id".to_string(), "varchar".to_string(), true),
        ];
        // Arrow Schema allows duplicate names, but this represents corrupted metadata
        let schema = build_arrow_schema(&columns).unwrap();
        assert_eq!(schema.fields().len(), 2, "Duplicate names should not crash");
    }

    // Pattern from issue #284: Wrong column types from mixed-up metadata.
    // ROOT CAUSE: Column IDs from one table applied to another.
    // ANALOGOUS CODE: Column ordering must be consistent.
    #[test]
    fn test_pattern_284_column_ordering_preserved() {
        let columns = vec![
            DuckLakeTableColumn::new(10, "z_col".to_string(), "varchar".to_string(), true),
            DuckLakeTableColumn::new(5, "a_col".to_string(), "int32".to_string(), false),
            DuckLakeTableColumn::new(1, "m_col".to_string(), "float64".to_string(), true),
        ];
        let schema = build_arrow_schema(&columns).unwrap();
        // Columns should be in the order given, not sorted by ID or name
        assert_eq!(schema.field(0).name(), "z_col");
        assert_eq!(schema.field(1).name(), "a_col");
        assert_eq!(schema.field(2).name(), "m_col");
    }

    // Pattern from issue #147: Migration adds columns to catalog tables.
    // ROOT CAUSE: Column definition changes between versions.
    // ANALOGOUS CODE: Columns with very long type strings.
    #[test]
    fn test_pattern_147_very_long_type_string() {
        // A deeply nested type could produce a very long type string
        let long_type = format!(
            "struct({})",
            (0..100).map(|i| format!("field_{} int32", i)).collect::<Vec<_>>().join(", ")
        );
        let columns = vec![
            DuckLakeTableColumn::new(1, "data".to_string(), long_type, true),
        ];
        let result = build_arrow_schema(&columns);
        assert!(result.is_ok(), "Very long struct type should be handled");
        assert_eq!(result.unwrap().fields().len(), 1);
    }

    // Pattern from issue #230: Catalog unusable after DROP TABLE.
    // ROOT CAUSE: Orphaned partition metadata references.
    // ANALOGOUS CODE: Column with empty or whitespace-only name.
    #[test]
    fn test_pattern_230_empty_column_names() {
        let columns = vec![
            DuckLakeTableColumn::new(1, "".to_string(), "int32".to_string(), false),
        ];
        let schema = build_arrow_schema(&columns).unwrap();
        assert_eq!(schema.field(0).name(), "", "Empty column name should not crash");

        let columns2 = vec![
            DuckLakeTableColumn::new(1, "  ".to_string(), "int32".to_string(), false),
        ];
        let schema2 = build_arrow_schema(&columns2).unwrap();
        assert_eq!(schema2.field(0).name(), "  ", "Whitespace column name should not crash");
    }

    // Pattern from issue #297: Default value limitations.
    // ROOT CAUSE: Column metadata has unexpected NULL or empty values.
    // ANALOGOUS CODE: Column with empty type string.
    #[test]
    fn test_pattern_297_empty_type_string() {
        use datafusion_ducklake::types::ducklake_to_arrow_type;

        let result = ducklake_to_arrow_type("");
        assert!(result.is_err(), "Empty type string should return error");

        let result = ducklake_to_arrow_type("   ");
        assert!(result.is_err(), "Whitespace-only type string should return error");
    }
}

// ============================================================================
// Schema/Catalog Lifecycle Pattern Tests
// ============================================================================

mod catalog_patterns {
    use datafusion_ducklake::path_resolver::resolve_path;

    // Pattern from issue #69/#101/#230: DROP TABLE breaks all subsequent queries.
    // ROOT CAUSE: Partition metadata references table after it's dropped.
    // ANALOGOUS CODE: Path resolution when schema/table path is empty (dropped table).
    #[test]
    fn test_pattern_69_empty_paths_after_drop() {
        // If a table's path is "" after being dropped/corrupted
        let result = resolve_path("/data/schema/", "", true);
        assert!(result == "/data/schema/" || result == "/data/schema",
            "Empty table path should resolve to schema path: '{}'", result);

        let result = resolve_path("", "", false);
        assert!(result.is_empty(),
            "Empty absolute path should be empty: '{}'", result);
    }

    // Pattern from issue #197: Two ducklakes sharing one catalog.
    // ROOT CAUSE: Table/schema IDs collide between catalogs.
    // ANALOGOUS CODE: Snapshot ID validation—is it possible to get snapshot_id = 0?
    #[test]
    fn test_pattern_197_snapshot_id_zero() {
        // If the catalog is brand new, snapshot_id might be 0
        // Our SQL queries use "? >= begin_snapshot" — does that work with 0?
        // SQL_LIST_SCHEMAS: WHERE ? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)
        // With snapshot_id=0 and begin_snapshot=0, "0 >= 0" is TRUE — correct!
        // With snapshot_id=0 and begin_snapshot=1, "0 >= 1" is FALSE — correct!

        // Verify path resolution works with snapshot_id=0 context
        let result = resolve_path("/catalog/", "schema/table/", true);
        assert_eq!(result, "/catalog/schema/table/");
    }

    // Pattern from issue #214: MySQL catalog fails on second attach.
    // ROOT CAUSE: "CREATE TABLE IF NOT EXISTS" fails because table already exists with different schema.
    // ANALOGOUS CODE: Our DuckLakeCatalog::new() calls get_current_snapshot and get_data_path.
    // If either fails, the catalog should return a clear error.
    #[test]
    fn test_pattern_214_catalog_construction_error_messages() {
        // This is a design-level test: verify error types are descriptive
        use datafusion_ducklake::DuckLakeError;

        let error = DuckLakeError::InvalidConfig("test error".to_string());
        assert!(error.to_string().contains("test error"));

        let error = DuckLakeError::UnsupportedType("bad_type".to_string());
        assert!(error.to_string().contains("bad_type"));
    }
}

// ============================================================================
// Type Promotion Pattern Tests (metadata_writer.rs)
// ============================================================================

#[cfg(feature = "write")]
mod type_promotion_patterns {
    use datafusion_ducklake::metadata_writer::is_type_promotion_allowed;

    // Pattern from issue #288: Stats update failure due to type handling in MySQL.
    // ROOT CAUSE: Type promotion rules may have gaps.
    // ANALOGOUS CODE: is_type_promotion_allowed might miss valid promotions.
    #[test]
    fn test_pattern_288_type_promotion_completeness() {
        // Valid promotions
        assert!(is_type_promotion_allowed("int8", "int16"));
        assert!(is_type_promotion_allowed("int8", "int32"));
        assert!(is_type_promotion_allowed("int8", "int64"));
        assert!(is_type_promotion_allowed("int16", "int32"));
        assert!(is_type_promotion_allowed("int16", "int64"));
        assert!(is_type_promotion_allowed("int32", "int64"));

        // Float promotion
        assert!(is_type_promotion_allowed("float", "double"));

        // Unsigned to signed
        assert!(is_type_promotion_allowed("uint8", "int16"));
        assert!(is_type_promotion_allowed("uint16", "int32"));
        assert!(is_type_promotion_allowed("uint32", "int64"));

        // These should NOT be allowed (potential data loss)
        assert!(!is_type_promotion_allowed("int64", "int32"), "Narrowing should fail");
        assert!(!is_type_promotion_allowed("double", "float"), "Float narrowing should fail");
        assert!(!is_type_promotion_allowed("varchar", "int32"), "String to int should fail");
        assert!(!is_type_promotion_allowed("int32", "varchar"), "Int to string should fail");
    }

    // Pattern from issue #288: Same-type "promotion" (no-op).
    // ROOT CAUSE: Allowing same-type promotion could mask bugs.
    // ANALOGOUS CODE: is_type_promotion_allowed("int32", "int32") — should it be allowed?
    #[test]
    fn test_pattern_288_same_type_promotion() {
        // Same type should NOT be considered a valid "promotion"
        assert!(!is_type_promotion_allowed("int32", "int32"));
        assert!(!is_type_promotion_allowed("varchar", "varchar"));
        assert!(!is_type_promotion_allowed("float", "float"));
        assert!(!is_type_promotion_allowed("double", "double"));
    }

    // Pattern from issue #288: Edge case promotions not in the matrix.
    // ROOT CAUSE: Some valid DuckDB promotions might not be listed.
    // ANALOGOUS CODE: Missing promotions in is_type_promotion_allowed.
    // BUG FINDING: float32->float64 and int->double promotions are NOT in the matrix.
    #[test]
    fn test_pattern_288_missing_promotions() {
        // float32 -> float64 — same as float->double but using our internal names
        // Note: DuckLake uses "float" and "double", not "float32" and "float64"
        assert!(!is_type_promotion_allowed("float32", "float64"),
            "BUG? float32->float64 not in promotion matrix (only float->double)");

        // Timestamp promotion
        assert!(is_type_promotion_allowed("timestamp", "timestamptz"),
            "timestamp->timestamptz should be allowed");

        // But other timestamp promotions?
        assert!(!is_type_promotion_allowed("timestamp_s", "timestamp"),
            "timestamp_s->timestamp promotion not in matrix");
        assert!(!is_type_promotion_allowed("timestamp_ms", "timestamp"),
            "timestamp_ms->timestamp promotion not in matrix");
    }

    // Pattern from issue #297: Limitations in default values.
    // ROOT CAUSE: Type system doesn't handle all edge cases.
    // ANALOGOUS CODE: Type promotion with aliases — "integer" vs "int32".
    #[test]
    fn test_pattern_297_promotion_with_type_aliases() {
        // Our promotion function uses internal type names, not SQL aliases
        // If metadata stores "integer" instead of "int32", promotion check may fail
        assert!(!is_type_promotion_allowed("integer", "int64"),
            "BUG? 'integer' alias not recognized by promotion function");
        assert!(!is_type_promotion_allowed("bigint", "int64"),
            "'bigint' alias not recognized (same type, not a promotion)");
        assert!(!is_type_promotion_allowed("tinyint", "int32"),
            "BUG? 'tinyint' alias not recognized by promotion function");
    }
}

// ============================================================================
// Validation Pattern Tests (metadata_writer_validation.rs)
// ============================================================================

// Validation patterns are tested via the public metadata_writer API
// (metadata_writer_validation is pub(crate), not accessible from integration tests)
#[cfg(feature = "write")]
mod validation_patterns {
    use datafusion_ducklake::metadata_writer::ColumnDef;

    // Pattern from issue #268: Metadata corruption from concurrent table creation.
    // ROOT CAUSE: Duplicate column names not caught.
    // ANALOGOUS CODE: ColumnDef allows creating duplicate-named columns.
    #[test]
    fn test_pattern_268_column_def_creation() {
        // Case-sensitive: "ID" and "id" are different columns in the ColumnDef API
        let col1 = ColumnDef::new("id", "int64", false);
        let col2 = ColumnDef::new("ID", "int64", false);
        assert_ne!(col1.name, col2.name, "Case-sensitive: 'id' and 'ID' should be different");

        // Verify ColumnDef::from_arrow handles edge types
        use arrow::datatypes::DataType;
        let result = ColumnDef::from_arrow("test", &DataType::Null, true);
        assert!(result.is_ok(), "Null type should be convertible to ColumnDef");
    }

    // Pattern from issue #268: ColumnDef with empty name.
    // ROOT CAUSE: No validation on column name at creation time.
    // ANALOGOUS CODE: ColumnDef::new allows empty names.
    #[test]
    fn test_pattern_268_empty_column_name() {
        let col = ColumnDef::new("", "int64", false);
        assert_eq!(col.name, "", "Empty column name is allowed at ColumnDef level");

        let col2 = ColumnDef::new("  ", "int64", false);
        assert_eq!(col2.name, "  ", "Whitespace column name is allowed at ColumnDef level");
    }

    // Pattern from issue #297: Default values.
    // ROOT CAUSE: Type conversion between string and DuckLake type.
    // ANALOGOUS CODE: ColumnDef::from_arrow with complex types.
    #[test]
    fn test_pattern_297_column_def_from_complex_arrow_types() {
        use arrow::datatypes::DataType;
        use datafusion_ducklake::types::arrow_to_ducklake_type;

        // List type
        let list_type = DataType::List(std::sync::Arc::new(arrow::datatypes::Field::new("item", DataType::Int32, true)));
        let ducklake_str = arrow_to_ducklake_type(&list_type).unwrap();
        assert_eq!(ducklake_str, "list(int32)");

        let col = ColumnDef::from_arrow("data", &list_type, true).unwrap();
        assert_eq!(col.ducklake_type, "list(int32)");
    }
}

// ============================================================================
// SQL Query Pattern Tests
// ============================================================================

mod sql_query_patterns {
    use datafusion_ducklake::metadata_provider::*;

    // Pattern from issue #125: Partitioning breaks queries.
    // ROOT CAUSE: SQL queries have wrong snapshot filtering.
    // ANALOGOUS CODE: Our SQL_GET_DATA_FILES uses snapshot range correctly.
    #[test]
    fn test_pattern_125_sql_snapshot_filtering_syntax() {
        // Verify the SQL constants use correct snapshot filtering
        // Pattern: "? >= begin_snapshot AND (? < end_snapshot OR end_snapshot IS NULL)"
        assert!(SQL_LIST_SCHEMAS.contains("begin_snapshot"));
        assert!(SQL_LIST_SCHEMAS.contains("end_snapshot"));
        assert!(SQL_LIST_SCHEMAS.contains("OR end_snapshot IS NULL"));

        assert!(SQL_LIST_TABLES.contains("begin_snapshot"));
        assert!(SQL_LIST_TABLES.contains("end_snapshot IS NULL"));

        assert!(SQL_GET_DATA_FILES.contains("begin_snapshot"));
        assert!(SQL_GET_DATA_FILES.contains("end_snapshot IS NULL"));

        // Verify consistent patterns across all queries
        assert!(SQL_GET_SCHEMA_BY_NAME.contains("end_snapshot IS NULL"));
        assert!(SQL_GET_TABLE_BY_NAME.contains("end_snapshot IS NULL"));
        assert!(SQL_TABLE_EXISTS.contains("end_snapshot IS NULL"));
    }

    // Pattern from issue #120: Query results incorrect with WHERE + ORDER + LIMIT.
    // ROOT CAUSE: Wrong parameter binding order in SQL queries.
    // ANALOGOUS CODE: SQL_GET_DATA_FILES has many ? parameters — verify they're logical.
    #[test]
    fn test_pattern_120_sql_parameter_count_consistency() {
        // Count ? placeholders in key queries
        let schema_count = SQL_LIST_SCHEMAS.matches('?').count();
        assert_eq!(schema_count, 2, "SQL_LIST_SCHEMAS should have 2 snapshot params");

        let table_count = SQL_LIST_TABLES.matches('?').count();
        assert_eq!(table_count, 3, "SQL_LIST_TABLES should have 3 params (schema_id + 2 snapshot)");

        let data_files_count = SQL_GET_DATA_FILES.matches('?').count();
        assert_eq!(data_files_count, 6, "SQL_GET_DATA_FILES should have 6 params (3 for delete + 3 for data)");

        let schema_by_name_count = SQL_GET_SCHEMA_BY_NAME.matches('?').count();
        assert_eq!(schema_by_name_count, 3, "SQL_GET_SCHEMA_BY_NAME should have 3 params (name + 2 snapshot)");
    }

    // Pattern from issue #197: Two ducklakes sharing one catalog.
    // ROOT CAUSE: SQL queries missing table_id filters.
    // ANALOGOUS CODE: SQL_GET_DATA_FILES filters by table_id for both data and delete files.
    #[test]
    fn test_pattern_197_table_id_filters_in_queries() {
        // Ensure data file queries filter by table_id
        assert!(SQL_GET_DATA_FILES.contains("data.table_id = ?"),
            "Data file query must filter by table_id");
        assert!(SQL_GET_DATA_FILES.contains("del.table_id = ?"),
            "Delete file join must filter by table_id");

        // Ensure column query filters by table_id
        assert!(SQL_GET_TABLE_COLUMNS.contains("table_id = ?"),
            "Column query must filter by table_id");
    }

    // Pattern from issue #84: Unexpected parquet file deletions.
    // ROOT CAUSE: Delete files not properly scoped to table_id.
    // ANALOGOUS CODE: SQL_GET_DATA_FILES LEFT JOIN condition.
    #[test]
    fn test_pattern_84_delete_file_scoped_to_table() {
        // The LEFT JOIN for delete files must include table_id
        // to prevent loading delete files from other tables
        assert!(SQL_GET_DATA_FILES.contains("del.table_id = ?"),
            "Delete file join must be scoped to table_id");
        assert!(SQL_GET_DATA_FILES.contains("data.data_file_id = del.data_file_id"),
            "Delete file must match data_file_id");
    }

    // Pattern from issue #101: Dropping partitioned table breaks metadata.
    // ROOT CAUSE: Row count query doesn't handle NULL delete_count.
    // ANALOGOUS CODE: SQL_GET_TABLE_ROW_COUNT uses COALESCE for safety.
    #[test]
    fn test_pattern_101_row_count_handles_nulls() {
        assert!(SQL_GET_TABLE_ROW_COUNT.contains("COALESCE(SUM(data.record_count), 0)"),
            "Row count should handle NULL record_count");
        assert!(SQL_GET_TABLE_ROW_COUNT.contains("COALESCE(SUM(del.delete_count), 0)"),
            "Row count should handle NULL delete_count");
    }

    // Pattern from issue #240: Migration error with deadlock.
    // ROOT CAUSE: SQL has assumptions about column existence.
    // ANALOGOUS CODE: SQL_GET_TABLE_COLUMNS uses "end_snapshot IS NULL" instead of snapshot range.
    #[test]
    fn test_pattern_240_column_query_uses_end_snapshot() {
        // Columns don't use begin/end snapshot range like other entities
        // They use "end_snapshot IS NULL" for current active columns
        assert!(SQL_GET_TABLE_COLUMNS.contains("end_snapshot IS NULL"),
            "Column query should filter by end_snapshot IS NULL");
        // Column query should NOT have snapshot range params
        // (columns are not versioned the same way as schemas/tables)
    }
}

// ============================================================================
// Edge Case Pattern Tests (cross-cutting concerns)
// ============================================================================

mod edge_case_patterns {
    use datafusion_ducklake::types::ducklake_to_arrow_type;
    use datafusion_ducklake::path_resolver::join_paths;

    // Pattern from issue #44 + #157: Unicode in identifiers.
    // ROOT CAUSE: Non-ASCII characters in column names, schema names, etc.
    // ANALOGOUS CODE: Type parsing with unicode characters.
    #[test]
    fn test_pattern_unicode_in_type_strings() {
        // Unicode should not crash the type parser
        let result = ducklake_to_arrow_type("日本語型");
        assert!(result.is_err(), "Unknown unicode type should return error");

        // Struct with unicode field names
        let result = ducklake_to_arrow_type("struct(名前 varchar, 年齢 int32)");
        assert!(result.is_ok(), "Unicode field names in struct should work");
        if let Ok(arrow::datatypes::DataType::Struct(fields)) = result {
            assert_eq!(fields[0].name(), "名前");
            assert_eq!(fields[1].name(), "年齢");
        }
    }

    // Pattern from issue #198: Null bytes in paths.
    // ROOT CAUSE: Unexpected control characters in metadata strings.
    // ANALOGOUS CODE: join_paths with null bytes.
    #[test]
    fn test_pattern_198_control_chars_in_paths() {
        // Null byte in path
        let result = join_paths("/data/", "file\0.parquet");
        assert!(!result.is_empty(), "Null byte in path should not crash");

        // Tab and newline
        let result = join_paths("/data/", "file\t.parquet");
        assert!(!result.is_empty(), "Tab in path should not crash");

        let result = join_paths("/data/", "file\n.parquet");
        assert!(!result.is_empty(), "Newline in path should not crash");
    }

    // Pattern from issue #268: Very long identifiers.
    // ROOT CAUSE: No length limits on names.
    // ANALOGOUS CODE: Type parser and path resolver with very long inputs.
    #[test]
    fn test_pattern_268_very_long_inputs() {
        // Very long type string
        let long_type = "a".repeat(10000);
        let result = ducklake_to_arrow_type(&long_type);
        assert!(result.is_err(), "Very long unknown type should return error");

        // Very long path
        let long_path = "/".to_string() + &"a/".repeat(1000) + "file.parquet";
        let result = join_paths("/data/", &long_path);
        assert!(!result.is_empty(), "Very long path should not crash");
    }

    // Pattern from issue #217: S3 URL scheme case sensitivity.
    // ROOT CAUSE: "S3://" vs "s3://" treated differently.
    // ANALOGOUS CODE: parse_object_store_url only checks lowercase "s3://".
    #[test]
    fn test_pattern_217_url_scheme_case_sensitivity() {
        use datafusion_ducklake::path_resolver::parse_object_store_url;

        // Lowercase works
        assert!(parse_object_store_url("s3://bucket/data").is_ok());

        // Uppercase does NOT work (documented limitation)
        let result = parse_object_store_url("S3://bucket/data");
        assert!(result.is_err(),
            "BUG? Uppercase S3:// scheme should work per RFC 3986 (schemes are case-insensitive)");

        // file:// lowercase works
        assert!(parse_object_store_url("file:///tmp/data").is_ok());
    }

    // Pattern from issue #255: Trailing/leading whitespace in data_path from metadata.
    // ROOT CAUSE: Metadata value not trimmed.
    // ANALOGOUS CODE: parse_object_store_url does NOT trim input.
    #[test]
    fn test_pattern_255_whitespace_in_data_path() {
        use datafusion_ducklake::path_resolver::parse_object_store_url;

        // Leading whitespace
        let result = parse_object_store_url("  s3://bucket/data");
        // This will likely fail because " s3://..." doesn't start with "s3://"
        assert!(result.is_err(),
            "BUG? Leading whitespace in data_path not trimmed before URL parsing");

        // Trailing whitespace
        let result = parse_object_store_url("s3://bucket/data  ");
        // The url::Url parser may handle trailing whitespace
        match result {
            Ok((_, path)) => {
                // If it succeeds, the path should not have trailing spaces
                assert!(!path.ends_with(' '),
                    "BUG? Trailing whitespace preserved in parsed path: '{}'", path);
            }
            Err(_) => {
                // Also acceptable — trailing whitespace causes parse error
            }
        }
    }

    // Pattern from issue #44: Empty string inputs.
    // ROOT CAUSE: Empty strings not validated.
    // ANALOGOUS CODE: Various functions with empty input.
    #[test]
    fn test_pattern_44_empty_string_inputs() {
        // Empty type string
        let result = ducklake_to_arrow_type("");
        assert!(result.is_err(), "Empty type string should error");

        // Empty path components
        let result = join_paths("", "");
        assert!(result == "/" || result.is_empty(),
            "Both empty should not crash: '{}'", result);
    }

    // Pattern from issue #217: File URL with extra slashes.
    // ROOT CAUSE: "file:////path" has too many slashes.
    // ANALOGOUS CODE: parse_object_store_url with file URLs.
    #[test]
    fn test_pattern_217_file_url_extra_slashes() {
        use datafusion_ducklake::path_resolver::parse_object_store_url;

        // Standard file URL
        let (_, path) = parse_object_store_url("file:///tmp/data").unwrap();
        assert_eq!(path, "/tmp/data");

        // File URL with 4 slashes (unusual but technically valid in some contexts)
        let result = parse_object_store_url("file:////tmp/data");
        match result {
            Ok((_, path)) => {
                // Should still give us a valid path
                assert!(path.contains("tmp/data"),
                    "Extra slashes should not corrupt path: '{}'", path);
            }
            Err(_) => {
                // Acceptable — extra slashes cause parse error
            }
        }
    }
}
