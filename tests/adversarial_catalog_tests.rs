//! Adversarial catalog tests: security red-team assessment
//!
//! This test suite exercises the DuckLake catalog with adversarial inputs to
//! discover SQL injection, path traversal, catalog corruption, and input
//! validation vulnerabilities.
//!
//! # Findings Summary
//!
//! ## CRITICAL: No SQL Injection Found
//! All metadata writers (SQLite, PostgreSQL, MySQL) use parameterized queries
//! via sqlx `.bind()`. All metadata providers use `?` or `$N` placeholders.
//! No `format!()` or string concatenation is used to build SQL with user input.
//!
//! ## HIGH: Path Traversal via Schema/Table Names (VULN-001)
//! Schema and table names are used directly as filesystem path components
//! (`format!("{}/", schema_name)`) without sanitization. A schema named
//! `../../etc` would resolve to a parent directory traversal in the path
//! hierarchy. See `path_resolver::join_paths()` which does no `../` filtering.
//!
//! ## MEDIUM: No Input Validation on Names (VULN-002)
//! Schema names, table names, and column names accept arbitrary strings:
//! empty strings, null bytes, control characters, extremely long values,
//! SQL keywords, shell metacharacters. No validation is performed at any layer.
//!
//! ## MEDIUM: Catalog Corruption via Direct DB Edit (VULN-003)
//! SQLite catalog files can be directly edited to create inconsistent metadata:
//! invalid column types, mismatched IDs, orphaned records. The read path has
//! no integrity validation beyond basic SQL query success.
//!
//! ## LOW: Type System Accepts Adversarial Strings (VULN-004)
//! The type parser in `types.rs` returns `UnsupportedType` errors for unknown
//! types but does not crash. However, storing a very long or specially crafted
//! type string in the catalog will be passed through to `ducklake_to_arrow_type()`
//! on every query, wasting CPU on parsing.

#![cfg(all(feature = "write-sqlite", feature = "metadata-sqlite"))]

use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use object_store::local::LocalFileSystem;
use tempfile::TempDir;

use datafusion_ducklake::metadata_writer::{ColumnDef, WriteMode};
use datafusion_ducklake::path_resolver::{join_paths, resolve_path};
use datafusion_ducklake::types::ducklake_to_arrow_type;
use datafusion_ducklake::{
    DuckLakeCatalog, DuckLakeTableWriter, MetadataWriter, SqliteMetadataProvider,
    SqliteMetadataWriter,
};

// ============================================================================
// Test helpers
// ============================================================================

fn create_object_store() -> Arc<dyn object_store::ObjectStore> {
    Arc::new(LocalFileSystem::new())
}

async fn create_test_env() -> (Arc<SqliteMetadataWriter>, TempDir) {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().join("test.db");
    let data_path = temp_dir.path().join("data");
    std::fs::create_dir_all(&data_path).unwrap();

    let conn_str = format!("sqlite:{}?mode=rwc", db_path.display());
    let writer = SqliteMetadataWriter::new_with_init(&conn_str)
        .await
        .unwrap();
    writer.set_data_path(data_path.to_str().unwrap()).unwrap();

    (Arc::new(writer), temp_dir)
}

async fn create_read_ctx(temp_dir: &TempDir) -> SessionContext {
    let db_path = temp_dir.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());

    let provider = SqliteMetadataProvider::new(&conn_str).await.unwrap();
    let catalog = DuckLakeCatalog::new(provider).unwrap();

    let ctx = SessionContext::new();
    ctx.register_catalog("ducklake", Arc::new(catalog));
    ctx
}

fn simple_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("val", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1, 2])),
            Arc::new(StringArray::from(vec![Some("a"), Some("b")])),
        ],
    )
    .unwrap()
}

// ============================================================================
// SQL Injection Tests
// ============================================================================
// FINDING: All parameterized. These tests confirm defense-in-depth.

/// VULN-NONE: Attempt SQL injection via schema name.
/// The schema name goes into a parameterized query `.bind(schema_name)`.
/// If injection worked, this would corrupt the catalog or return extra rows.
#[tokio::test(flavor = "multi_thread")]
async fn test_sqli_schema_name_single_quote() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();

    // Classic SQL injection: single quote to break out of string literal
    let result = writer.get_or_create_schema("'; DROP TABLE ducklake_schema; --", None, snap);
    // Should succeed (name stored as literal string, not interpolated)
    assert!(result.is_ok(), "Parameterized query should handle single quotes safely");

    let (schema_id, created) = result.unwrap();
    assert!(created);
    assert!(schema_id > 0);
}

/// VULN-NONE: Attempt SQL injection via table name.
#[tokio::test(flavor = "multi_thread")]
async fn test_sqli_table_name_union_select() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer.get_or_create_schema("main", None, snap).unwrap();

    // UNION-based injection attempt
    let result = writer.get_or_create_table(
        schema_id,
        "' UNION SELECT password FROM users --",
        None,
        snap,
    );
    assert!(result.is_ok(), "UNION injection should be treated as literal table name");
}

/// VULN-NONE: Attempt SQL injection via column names in set_columns.
#[tokio::test(flavor = "multi_thread")]
async fn test_sqli_column_name_injection() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer.get_or_create_schema("main", None, snap).unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "test", None, snap)
        .unwrap();

    let malicious_columns = vec![
        ColumnDef::new("id", "int32", false),
        ColumnDef::new("'; DELETE FROM ducklake_column; --", "varchar", true),
    ];

    let result = writer.set_columns(table_id, &malicious_columns, snap);
    assert!(result.is_ok(), "Column name injection should be stored as literal");
}

/// VULN-NONE: Attempt injection via column type string.
#[tokio::test(flavor = "multi_thread")]
async fn test_sqli_column_type_injection() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer.get_or_create_schema("main", None, snap).unwrap();
    let (table_id, _) = writer
        .get_or_create_table(schema_id, "test", None, snap)
        .unwrap();

    let malicious_columns = vec![ColumnDef::new(
        "col",
        "varchar'); DROP TABLE ducklake_data_file; --",
        true,
    )];

    let result = writer.set_columns(table_id, &malicious_columns, snap);
    assert!(result.is_ok(), "Type string injection should be stored as literal");
}

/// VULN-NONE: SQL injection via begin_write_transaction (end-to-end write path).
#[tokio::test(flavor = "multi_thread")]
async fn test_sqli_begin_write_transaction() {
    let (writer, _temp) = create_test_env().await;

    let columns = vec![ColumnDef::new("id", "int32", false)];
    let result = writer.begin_write_transaction(
        "main'; DROP TABLE ducklake_snapshot; --",
        "test",
        &columns,
        WriteMode::Replace,
    );
    assert!(result.is_ok(), "Write transaction should handle injected schema name");
}

/// VULN-NONE: Stacked query injection attempt.
#[tokio::test(flavor = "multi_thread")]
async fn test_sqli_stacked_queries() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();

    // Attempt stacked query: semicolon followed by malicious SQL
    let result = writer.get_or_create_schema(
        "test; INSERT INTO ducklake_snapshot VALUES (9999, 'hacked', 1, 0, 0); --",
        None,
        snap,
    );
    assert!(result.is_ok());
}

// ============================================================================
// Path Traversal Tests
// ============================================================================
// FINDING: VULN-001 (HIGH) - No sanitization of ../  in paths.

/// VULN-001: Path traversal via schema name used as directory path.
/// When a schema is created, its path is set to `format!("{}/", schema_name)`.
/// With `../`, this traverses the directory hierarchy.
#[test]
fn test_path_traversal_schema_name_dotdot() {
    // Simulates what happens when schema_name = "../../etc"
    // The path becomes "../../etc/" which resolves relative to data_path
    let base = "/data/warehouse/";
    let malicious_schema = "../../etc";

    let resolved = join_paths(base, &format!("{}/", malicious_schema));
    // Path traversal is now correctly rejected
    assert!(resolved.is_err(), "join_paths should reject path traversal with '..' components");

    // In a child_resolver chain, this escapes the data directory
    let schema_path = resolve_path(base, &format!("{}/", malicious_schema), true);
    assert!(schema_path.is_err(), "resolve_path should reject path traversal with '..' components");
}

/// VULN-001: Path traversal via table name.
#[test]
fn test_path_traversal_table_name_dotdot() {
    let schema_base = "/data/warehouse/main/";
    let malicious_table = "../../../tmp/evil";

    let resolved = join_paths(schema_base, &format!("{}/", malicious_table));
    assert!(resolved.is_err(), "join_paths should reject path traversal with '..' components");
}

/// VULN-001: Path traversal with backslashes (Windows-style).
#[test]
fn test_path_traversal_backslash() {
    let base = "/data/warehouse/";
    let malicious = "..\\..\\etc\\passwd";

    let resolved = join_paths(base, malicious);
    // join_paths now rejects paths with '..' traversal components (splits on both / and \)
    assert!(resolved.is_err(), "join_paths should reject Windows-style path traversal with '..' components");
}

/// VULN-001: Path traversal via file path in data file registration.
#[test]
fn test_path_traversal_file_path() {
    let table_base = "/data/warehouse/main/users/";
    let malicious_file = "../../../../etc/shadow";

    let resolved = resolve_path(table_base, malicious_file, true);
    assert!(resolved.is_err(), "resolve_path should reject path traversal with '..' components");
}

/// VULN-001: Null bytes in paths could truncate path on some OS layers.
#[test]
fn test_path_null_byte_injection() {
    let base = "/data/warehouse/";
    let malicious = "schema\0/../../etc/passwd";

    let resolved = join_paths(base, malicious);
    // Null bytes are now correctly rejected
    assert!(resolved.is_err(), "join_paths should reject paths containing null bytes");
}

/// VULN-001: URL-encoded path traversal — must be rejected.
/// S3 and other backends may decode %2e%2e to "..", so we reject it at validation time.
#[test]
fn test_path_traversal_url_encoded() {
    let base = "/data/warehouse/";
    // %2e%2e = ..  (URL-encoded)
    let malicious = "%2e%2e/%2e%2e/etc/passwd";

    let result = join_paths(base, malicious);
    assert!(result.is_err(), "join_paths should reject URL-encoded path traversal (%2e%2e)");
    assert!(result.unwrap_err().to_string().contains("Path traversal"));
}

/// VULN-001: URL-encoded traversal with uppercase hex digits (%2E%2E).
#[test]
fn test_path_traversal_url_encoded_uppercase() {
    let result = join_paths("/data/", "%2E%2E/etc/passwd");
    assert!(result.is_err(), "should reject %2E%2E (uppercase)");
    assert!(result.unwrap_err().to_string().contains("Path traversal"));
}

/// VULN-001: URL-encoded traversal with mixed-case hex digits (%2e%2E, %2E%2e).
#[test]
fn test_path_traversal_url_encoded_mixed_case() {
    let result = join_paths("/data/", "%2e%2E/etc/passwd");
    assert!(result.is_err(), "should reject %2e%2E (mixed case)");
    assert!(result.unwrap_err().to_string().contains("Path traversal"));

    let result = join_paths("/data/", "%2E%2e/secret");
    assert!(result.is_err(), "should reject %2E%2e (mixed case)");
    assert!(result.unwrap_err().to_string().contains("Path traversal"));
}

/// VULN-001: End-to-end path traversal through schema creation and read.
#[tokio::test(flavor = "multi_thread")]
async fn test_path_traversal_e2e_schema_creation() {
    let (writer, temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();

    // Create schema with traversal name - this succeeds because no validation
    let result = writer.get_or_create_schema("../../etc", None, snap);
    assert!(result.is_ok(), "Schema with ../ name should be creatable (no validation)");

    let (schema_id, _) = result.unwrap();

    // Create a table under the malicious schema
    let result = writer.get_or_create_table(schema_id, "passwd", None, snap);
    assert!(result.is_ok());

    // Now read - the schema name becomes a path component
    let ctx = create_read_ctx(&temp).await;
    let schemas = ctx
        .catalog("ducklake")
        .unwrap()
        .schema_names();
    // The malicious schema name is stored and returned
    assert!(schemas.contains(&"../../etc".to_string()));
}

// ============================================================================
// Special Characters in Names Tests
// ============================================================================
// FINDING: VULN-002 (MEDIUM) - No input validation on names.

/// VULN-002: Empty string as schema name.
#[tokio::test(flavor = "multi_thread")]
async fn test_empty_schema_name() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();

    let result = writer.get_or_create_schema("", None, snap);
    // No validation - empty string is accepted
    assert!(result.is_ok(), "Empty schema name accepted (no validation)");
}

/// VULN-002: Empty string as table name.
#[tokio::test(flavor = "multi_thread")]
async fn test_empty_table_name() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer.get_or_create_schema("main", None, snap).unwrap();

    let result = writer.get_or_create_table(schema_id, "", None, snap);
    assert!(result.is_ok(), "Empty table name accepted (no validation)");
}

/// VULN-002: Whitespace-only names.
#[tokio::test(flavor = "multi_thread")]
async fn test_whitespace_only_names() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();

    let result = writer.get_or_create_schema("   ", None, snap);
    assert!(result.is_ok(), "Whitespace-only schema name accepted");

    let result = writer.get_or_create_schema("\t\n\r", None, snap);
    assert!(result.is_ok(), "Tab/newline schema name accepted");
}

/// VULN-002: Names with control characters.
#[tokio::test(flavor = "multi_thread")]
async fn test_control_characters_in_names() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();

    // Null byte
    let result = writer.get_or_create_schema("test\0schema", None, snap);
    assert!(result.is_ok(), "Null byte in schema name accepted");

    // Bell character and other control chars
    let result = writer.get_or_create_schema("test\x07\x08\x1b", None, snap);
    assert!(result.is_ok(), "Control characters in schema name accepted");
}

/// VULN-002: Unicode edge cases in names.
#[tokio::test(flavor = "multi_thread")]
async fn test_unicode_edge_cases() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();

    // Zero-width characters (invisible but distinct)
    let result = writer.get_or_create_schema("main\u{200B}", None, snap);
    assert!(result.is_ok(), "Zero-width space in name accepted");

    // RTL override (text direction attack)
    let result = writer.get_or_create_schema("main\u{202E}nimdA", None, snap);
    assert!(result.is_ok(), "RTL override character accepted");

    // Emoji
    let result = writer.get_or_create_schema("schema_\u{1F4A9}", None, snap);
    assert!(result.is_ok());

    // Combining characters that visually look like other characters
    let result = writer.get_or_create_schema("ma\u{0300}in", None, snap);
    assert!(result.is_ok(), "Combining diacritical marks accepted");
}

/// VULN-002: Extremely long names.
#[tokio::test(flavor = "multi_thread")]
async fn test_extremely_long_names() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();

    // 10KB schema name
    let long_name = "a".repeat(10_000);
    let result = writer.get_or_create_schema(&long_name, None, snap);
    assert!(result.is_ok(), "10KB schema name accepted (no length limit)");

    // 1MB schema name - tests memory and storage limits
    let very_long_name = "x".repeat(1_000_000);
    let result = writer.get_or_create_schema(&very_long_name, None, snap);
    // This may succeed or fail depending on SQLite limits, but should not crash
    // SQLite default SQLITE_MAX_LENGTH is 1 billion bytes, so this should work
    assert!(result.is_ok(), "1MB schema name accepted");
}

/// VULN-002: SQL keywords as names.
#[tokio::test(flavor = "multi_thread")]
async fn test_sql_keywords_as_names() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();

    for keyword in &[
        "SELECT",
        "DROP",
        "TABLE",
        "INSERT",
        "DELETE",
        "UPDATE",
        "CREATE",
        "ALTER",
        "WHERE",
        "FROM",
        "NULL",
        "TRUE",
        "FALSE",
    ] {
        let result = writer.get_or_create_schema(keyword, None, snap);
        assert!(
            result.is_ok(),
            "SQL keyword '{}' should be safe as schema name (parameterized)",
            keyword
        );
    }
}

/// VULN-002: Shell metacharacters in names.
#[tokio::test(flavor = "multi_thread")]
async fn test_shell_metacharacters() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();

    // Characters that could be dangerous if names are used in shell commands
    for name in &[
        "$(whoami)",
        "`id`",
        "; rm -rf /",
        "| cat /etc/passwd",
        "&& echo pwned",
        "schema > /tmp/out",
        "test\nid",
    ] {
        let result = writer.get_or_create_schema(name, None, snap);
        assert!(
            result.is_ok(),
            "Shell metachar '{}' stored safely in catalog",
            name.escape_debug()
        );
    }
}

/// VULN-002: Names that look like file paths.
#[tokio::test(flavor = "multi_thread")]
async fn test_names_as_file_paths() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();

    for name in &[
        "/etc/passwd",
        "C:\\Windows\\System32",
        "~/.ssh/id_rsa",
        "//network/share",
        "\\\\server\\share",
    ] {
        let result = writer.get_or_create_schema(name, None, snap);
        assert!(result.is_ok(), "Path-like name '{}' accepted", name);
    }
}

// ============================================================================
// Catalog Corruption Tests
// ============================================================================
// FINDING: VULN-003 (MEDIUM) - Direct DB manipulation creates inconsistent state.

/// VULN-003: Corrupt catalog by adding invalid column type directly to SQLite.
#[tokio::test(flavor = "multi_thread")]
async fn test_corrupt_catalog_invalid_column_type() {
    let (writer, temp) = create_test_env().await;

    // Write actual data so there's a parquet file
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer
        .write_table("main", "test", &[batch])
        .await
        .unwrap();

    // Now corrupt the catalog by directly modifying the column type in SQLite
    let db_path = temp.path().join("test.db");
    let pool = sqlx::sqlite::SqlitePool::connect(&format!("sqlite:{}", db_path.display()))
        .await
        .unwrap();

    sqlx::query("UPDATE ducklake_column SET column_type = 'NONEXISTENT_GARBAGE_TYPE' WHERE column_name = 'id'")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    // Try to read - should fail with unsupported type error, not crash
    let ctx = create_read_ctx(&temp).await;
    let result = ctx.sql("SELECT * FROM ducklake.main.test").await;
    // This should produce an error about unsupported type, not panic
    assert!(result.is_err(), "Query should fail with corrupted type, not panic");
}

/// VULN-003: Corrupt catalog by setting negative file sizes.
#[tokio::test(flavor = "multi_thread")]
async fn test_corrupt_catalog_negative_file_size() {
    let (writer, temp) = create_test_env().await;

    // Create table with data
    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer
        .write_table("main", "test", &[batch])
        .await
        .unwrap();

    // Corrupt file_size_bytes to negative
    let db_path = temp.path().join("test.db");
    let pool = sqlx::sqlite::SqlitePool::connect(&format!("sqlite:{}", db_path.display()))
        .await
        .unwrap();
    sqlx::query("UPDATE ducklake_data_file SET file_size_bytes = -1")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    // Try to read - negative file size could cause issues in Parquet reader
    let ctx = create_read_ctx(&temp).await;
    let result = ctx.sql("SELECT * FROM ducklake.main.test").await;
    // Should either work (Parquet reads actual file) or error gracefully
    // The key assertion: it must NOT panic
    if let Ok(df) = result {
        // If query plan succeeds, try collecting results
        let _result = df.collect().await; // may error, but should not panic
    }
}

/// VULN-003: Corrupt catalog by setting footer_size larger than file.
#[tokio::test(flavor = "multi_thread")]
async fn test_corrupt_catalog_oversized_footer() {
    let (writer, temp) = create_test_env().await;

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer
        .write_table("main", "test", &[batch])
        .await
        .unwrap();

    // Set footer_size to absurdly large value
    let db_path = temp.path().join("test.db");
    let pool = sqlx::sqlite::SqlitePool::connect(&format!("sqlite:{}", db_path.display()))
        .await
        .unwrap();
    sqlx::query("UPDATE ducklake_data_file SET footer_size = 999999999")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    // Try to read - oversized footer hint could cause issues
    let ctx = create_read_ctx(&temp).await;
    let result = ctx.sql("SELECT * FROM ducklake.main.test").await;
    if let Ok(df) = result {
        let _result = df.collect().await; // should not panic
    }
}

/// VULN-003: Corrupt catalog by removing all columns for a table.
#[tokio::test(flavor = "multi_thread")]
async fn test_corrupt_catalog_no_columns() {
    let (writer, temp) = create_test_env().await;

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer
        .write_table("main", "test", &[batch])
        .await
        .unwrap();

    // Delete all columns from catalog
    let db_path = temp.path().join("test.db");
    let pool = sqlx::sqlite::SqlitePool::connect(&format!("sqlite:{}", db_path.display()))
        .await
        .unwrap();
    sqlx::query("DELETE FROM ducklake_column")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    // Try to read - no columns means empty schema
    let ctx = create_read_ctx(&temp).await;
    let result = ctx.sql("SELECT * FROM ducklake.main.test").await;
    // Should produce a table with no columns or an error, not panic
    if let Ok(df) = result {
        let _result = df.collect().await;
    }
}

/// VULN-003: Corrupt catalog by pointing data_file path to non-existent file.
#[tokio::test(flavor = "multi_thread")]
async fn test_corrupt_catalog_missing_data_file() {
    let (writer, temp) = create_test_env().await;

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer
        .write_table("main", "test", &[batch])
        .await
        .unwrap();

    // Change file path to non-existent file
    let db_path = temp.path().join("test.db");
    let pool = sqlx::sqlite::SqlitePool::connect(&format!("sqlite:{}", db_path.display()))
        .await
        .unwrap();
    sqlx::query("UPDATE ducklake_data_file SET path = 'nonexistent/file.parquet'")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    // Try to read - missing file should error gracefully
    let ctx = create_read_ctx(&temp).await;
    let result = ctx.sql("SELECT * FROM ducklake.main.test").await;
    if let Ok(df) = result {
        let result = df.collect().await;
        assert!(result.is_err(), "Reading missing file should error, not panic");
    }
}

/// VULN-003: Corrupt catalog by creating orphaned data files (no table reference).
#[tokio::test(flavor = "multi_thread")]
async fn test_corrupt_catalog_orphaned_snapshot() {
    let (writer, temp) = create_test_env().await;

    let batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
        vec![Arc::new(Int32Array::from(vec![1]))],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer
        .write_table("main", "test", &[batch])
        .await
        .unwrap();

    // Delete the snapshot records
    let db_path = temp.path().join("test.db");
    let pool = sqlx::sqlite::SqlitePool::connect(&format!("sqlite:{}", db_path.display()))
        .await
        .unwrap();
    sqlx::query("DELETE FROM ducklake_snapshot")
        .execute(&pool)
        .await
        .unwrap();
    pool.close().await;

    // Try to create a new catalog pointing to the corrupted DB
    let db_path = temp.path().join("test.db");
    let conn_str = format!("sqlite:{}", db_path.display());
    let result = SqliteMetadataProvider::new(&conn_str).await;
    // Should fail gracefully when no snapshots exist
    if let Ok(provider) = result {
        let result = DuckLakeCatalog::new(provider);
        // May succeed with snapshot_id=0 or fail - either way, should not panic
        let _ = result;
    }
}

// ============================================================================
// Type System Adversarial Tests
// ============================================================================
// FINDING: VULN-004 (LOW) - Type parser handles adversarial input safely.

/// VULN-004: Very long type strings.
#[test]
fn test_type_parser_very_long_string() {
    let long_type = "a".repeat(100_000);
    let result = ducklake_to_arrow_type(&long_type);
    assert!(result.is_err(), "Unknown type should return error");
}

/// VULN-004: Type string with SQL injection attempt.
#[test]
fn test_type_parser_sql_injection() {
    let result = ducklake_to_arrow_type("varchar'); DROP TABLE --");
    assert!(result.is_err());
}

/// VULN-004: Type string with null bytes.
#[test]
fn test_type_parser_null_bytes() {
    let result = ducklake_to_arrow_type("int32\0varchar");
    assert!(result.is_err());
}

/// VULN-004: Decimal with extreme precision/scale.
#[test]
fn test_type_parser_extreme_decimal() {
    // Very large precision
    let result = ducklake_to_arrow_type("decimal(999999999, 999999999)");
    // Should either return an error or a valid type, never panic
    let _ = result;

    // Negative values
    let result = ducklake_to_arrow_type("decimal(-1, -1)");
    let _ = result;

    // Zero values
    let result = ducklake_to_arrow_type("decimal(0, 0)");
    let _ = result;
}

/// VULN-004: Malformed parameterized types.
#[test]
fn test_type_parser_malformed_params() {
    // Unclosed parenthesis
    let _ = ducklake_to_arrow_type("varchar(");
    let _ = ducklake_to_arrow_type("decimal(10,");
    let _ = ducklake_to_arrow_type("decimal(10, 2");

    // Extra parentheses
    let _ = ducklake_to_arrow_type("varchar(())");
    let _ = ducklake_to_arrow_type("decimal((10), (2))");

    // Non-numeric precision
    let _ = ducklake_to_arrow_type("decimal(abc, def)");
    let _ = ducklake_to_arrow_type("varchar(abc)");
}

/// VULN-004: Empty and whitespace type strings.
#[test]
fn test_type_parser_empty_and_whitespace() {
    let result = ducklake_to_arrow_type("");
    assert!(result.is_err());

    let result = ducklake_to_arrow_type("   ");
    assert!(result.is_err());

    let result = ducklake_to_arrow_type("\t\n\r");
    assert!(result.is_err());
}

/// VULN-004: Nested type strings with adversarial content.
#[test]
fn test_type_parser_deeply_nested() {
    // Deeply nested list types
    let mut nested = "int32".to_string();
    for _ in 0..100 {
        nested = format!("list({})", nested);
    }
    let _ = ducklake_to_arrow_type(&nested);

    // Deeply nested struct
    let mut nested_struct = "struct(a int32)".to_string();
    for i in 0..50 {
        nested_struct = format!("struct(f{} {})", i, nested_struct);
    }
    let _ = ducklake_to_arrow_type(&nested_struct);
}

// ============================================================================
// End-to-end adversarial query tests
// ============================================================================

/// End-to-end: Create table with special characters and query it via SQL.
#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_special_char_table_name_query() {
    let (writer, temp) = create_test_env().await;

    // Create a table with special characters in the name
    let batch = simple_batch();
    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();

    // Table name with quotes and special chars - stored via parameterized queries
    table_writer
        .write_table("main", "test\"table", &[batch])
        .await
        .unwrap();

    // Now try to query it - DataFusion needs the name escaped
    let ctx = create_read_ctx(&temp).await;

    // List tables to verify it was created
    let schema = ctx.catalog("ducklake").unwrap().schema("main").unwrap();
    let tables = schema.table_names();
    assert!(
        tables.contains(&"test\"table".to_string()),
        "Table with double-quote in name should exist"
    );
}

/// End-to-end: Create table with SQL keyword column names and query successfully.
#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_sql_keyword_column_names() {
    let (writer, temp) = create_test_env().await;

    let schema = Arc::new(Schema::new(vec![
        Field::new("select", DataType::Int32, false),
        Field::new("from", DataType::Utf8, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec![Some("test")])),
        ],
    )
    .unwrap();

    let object_store = create_object_store();
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer
        .write_table("main", "keyword_test", &[batch])
        .await
        .unwrap();

    let ctx = create_read_ctx(&temp).await;
    // Query using quoted column names
    let result = ctx
        .sql("SELECT \"select\", \"from\" FROM ducklake.main.keyword_test")
        .await;
    assert!(result.is_ok(), "Querying SQL-keyword column names should work");

    if let Ok(df) = result {
        let batches = df.collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
    }
}

/// End-to-end: Multiple schemas with nearly identical names (unicode confusables).
#[tokio::test(flavor = "multi_thread")]
async fn test_e2e_unicode_confusable_schemas() {
    let (writer, temp) = create_test_env().await;

    let batch = simple_batch();
    let object_store = create_object_store();

    // "main" with regular 'a' and "main" with Cyrillic 'а' (U+0430)
    let table_writer = DuckLakeTableWriter::new(writer.clone(), object_store.clone()).unwrap();
    table_writer
        .write_table("main", "test", &[batch.clone()])
        .await
        .unwrap();

    let table_writer2 = DuckLakeTableWriter::new(writer.clone(), object_store).unwrap();
    table_writer2
        .write_table("m\u{0430}in", "test", &[batch])
        .await
        .unwrap();

    let ctx = create_read_ctx(&temp).await;
    let catalog = ctx.catalog("ducklake").unwrap();
    let schema_names = catalog.schema_names();

    // Both schemas should exist as separate entries
    assert!(schema_names.contains(&"main".to_string()));
    assert!(schema_names.contains(&"m\u{0430}in".to_string()));
    assert_ne!("main", "m\u{0430}in", "These should be distinct strings");
}

// ============================================================================
// Boundary value tests for numeric metadata
// ============================================================================

/// Test boundary values for record_count in data file registration.
#[tokio::test(flavor = "multi_thread")]
async fn test_boundary_record_count_values() {
    let (writer, _temp) = create_test_env().await;

    let columns = vec![ColumnDef::new("id", "int32", false)];
    let setup = writer
        .begin_write_transaction("main", "test", &columns, WriteMode::Replace)
        .unwrap();

    use datafusion_ducklake::metadata_writer::DataFileInfo;

    // Zero record count
    let result = writer.register_data_file(
        setup.table_id,
        setup.snapshot_id,
        &DataFileInfo::new("zero.parquet", 100, 0),
    );
    assert!(result.is_ok(), "Zero record count should be accepted");

    // Negative record count
    let result = writer.register_data_file(
        setup.table_id,
        setup.snapshot_id,
        &DataFileInfo::new("neg.parquet", 100, -1),
    );
    // No validation - negative count accepted
    assert!(result.is_ok(), "Negative record count accepted (no validation)");

    // MAX i64
    let result = writer.register_data_file(
        setup.table_id,
        setup.snapshot_id,
        &DataFileInfo::new("max.parquet", 100, i64::MAX),
    );
    assert!(result.is_ok(), "i64::MAX record count accepted");
}

/// Test boundary values for snapshot IDs.
#[tokio::test(flavor = "multi_thread")]
async fn test_boundary_snapshot_ids() {
    let (writer, _temp) = create_test_env().await;
    let _snap = writer.create_snapshot().unwrap();

    // Use invalid snapshot ID for schema creation
    let result = writer.get_or_create_schema("test", None, -1);
    // Negative snapshot ID - no validation
    assert!(result.is_ok(), "Negative snapshot ID accepted (no validation)");

    let result = writer.get_or_create_schema("test2", None, i64::MAX);
    assert!(result.is_ok(), "i64::MAX snapshot ID accepted");

    let result = writer.get_or_create_schema("test3", None, 0);
    assert!(result.is_ok(), "Zero snapshot ID accepted");
}

// ============================================================================
// Duplicate name tests
// ============================================================================

/// Test creating schemas with identical names (should return existing).
#[tokio::test(flavor = "multi_thread")]
async fn test_duplicate_schema_names() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();

    let (id1, created1) = writer.get_or_create_schema("dupe", None, snap).unwrap();
    assert!(created1);

    let (id2, created2) = writer.get_or_create_schema("dupe", None, snap).unwrap();
    assert!(!created2);
    assert_eq!(id1, id2, "Same schema name should return same ID");
}

/// Test creating tables with identical names in same schema.
#[tokio::test(flavor = "multi_thread")]
async fn test_duplicate_table_names() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();
    let (schema_id, _) = writer.get_or_create_schema("main", None, snap).unwrap();

    let (id1, created1) = writer
        .get_or_create_table(schema_id, "dupe", None, snap)
        .unwrap();
    assert!(created1);

    let (id2, created2) = writer
        .get_or_create_table(schema_id, "dupe", None, snap)
        .unwrap();
    assert!(!created2);
    assert_eq!(id1, id2);
}

/// Test case sensitivity in names.
#[tokio::test(flavor = "multi_thread")]
async fn test_case_sensitivity() {
    let (writer, _temp) = create_test_env().await;
    let snap = writer.create_snapshot().unwrap();

    let (id1, _) = writer.get_or_create_schema("Main", None, snap).unwrap();
    let (id2, _) = writer.get_or_create_schema("main", None, snap).unwrap();
    let (id3, _) = writer.get_or_create_schema("MAIN", None, snap).unwrap();

    // SQLite is case-sensitive for string comparisons by default
    // so these should be different schemas
    assert_ne!(id1, id2, "Main vs main should be different schemas");
    assert_ne!(id2, id3, "main vs MAIN should be different schemas");
}
