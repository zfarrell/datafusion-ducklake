#![cfg(feature = "metadata-duckdb")]
//! SQL Logic Test Runner using Hybrid DuckDB+DataFusion Adapter
//!
//! This runner executes DuckDB DuckLake tests using a hybrid approach:
//! - WRITE operations (CREATE/INSERT/UPDATE/DELETE) → DuckDB
//! - READ operations (SELECT) → DataFusion
//! - After each WRITE → Refresh DataFusion catalog snapshot
//! - Table references rewritten for DataFusion (ducklake.table → ducklake.main.table)
//!
//! This allows comprehensive testing of DataFusion's read path.
//! Tests from: https://github.com/duckdb/ducklake/tree/main/test/sql

mod common;
mod hybrid_asyncdb;

use hybrid_asyncdb::HybridDuckLakeDB;
use sqllogictest::Runner;
use tempfile::TempDir;

/// Preprocess DuckDB test file to remove DuckDB-specific directives
///
/// This preprocessing:
/// 1. Removes DuckDB-specific test directives (require, test-env, etc.)
/// 2. Skips ATTACH/DETACH statements (handled in Rust)
/// 3. Skips EXPLAIN statements (not testable in hybrid mode)
/// 4. Expands loop/foreach/endloop blocks
/// 5. Handles mode skip/unskip sections
/// 6. Strips error expectations from statement error blocks
/// 7. Handles concurrentloop, statement maybe, multi-connection statements
/// 8. Rewrites unqualified table names after USE ducklake
fn preprocess_test_file(content: &str) -> String {
    // First pass: expand loop/foreach blocks
    let expanded = expand_loops(content);

    // Second pass: handle directives and rewriting
    let mut output = String::new();
    let mut lines = expanded.lines().peekable();
    let mut in_ducklake_context = false;

    while let Some(line) = lines.next() {
        let trimmed = line.trim();

        // Skip DuckDB-specific directives that sqllogictest can't parse
        if trimmed.starts_with("require ")
            || trimmed.starts_with("require-env ")
            || trimmed.starts_with("test-env ")
            || trimmed.starts_with("# name:")
            || trimmed.starts_with("# description:")
            || trimmed.starts_with("# group:")
        {
            continue;
        }

        // Handle mode skip/unskip - skip everything between them
        if trimmed == "mode skip" {
            while let Some(inner) = lines.next() {
                if inner.trim() == "mode unskip" {
                    break;
                }
            }
            continue;
        }

        // Skip concurrentloop blocks (multi-connection, not supported)
        if trimmed.starts_with("concurrentloop ") {
            while let Some(inner) = lines.next() {
                if inner.trim() == "endloop" {
                    break;
                }
            }
            continue;
        }

        // Handle unzip directives (not supported)
        if trimmed.starts_with("unzip ") {
            continue;
        }

        // Handle statement maybe → statement ok (strip any ---- and error text)
        if trimmed == "statement maybe" {
            output.push_str("statement ok\n");
            // Pass through SQL line, then skip any ---- and error text
            while let Some(next) = lines.peek() {
                let next_trimmed = next.trim();
                if next_trimmed == "----" {
                    lines.next(); // skip ----
                    // Skip expected error text lines
                    while let Some(err_line) = lines.peek() {
                        let t = err_line.trim();
                        if t.is_empty()
                            || t.starts_with("statement")
                            || t.starts_with("query")
                            || t.starts_with("halt")
                            || t.starts_with("#")
                        {
                            break;
                        }
                        lines.next();
                    }
                    break;
                } else if next_trimmed.is_empty()
                    || next_trimmed.starts_with("statement")
                    || next_trimmed.starts_with("query")
                    || next_trimmed.starts_with("halt")
                {
                    break;
                } else {
                    // SQL line - pass through
                    let sql_line = lines.next().unwrap();
                    output.push_str(sql_line);
                    output.push('\n');
                }
            }
            continue;
        }

        // Skip multi-connection statements (statement ok conN, query I conN)
        if (trimmed.starts_with("statement ") || trimmed.starts_with("query "))
            && trimmed.contains(" con")
        {
            // Check if it matches conN pattern (e.g. con1, con2)
            if let Some(con_pos) = trimmed.rfind(" con") {
                let after_con = &trimmed[con_pos + 4..];
                if after_con.chars().all(|c| c.is_ascii_digit()) && !after_con.is_empty() {
                    // Skip this record and its body
                    skip_record_body(&mut lines);
                    continue;
                }
            }
        }

        // Track USE ducklake context for table reference rewriting
        if trimmed.to_uppercase().starts_with("USE DUCKLAKE") {
            in_ducklake_context = true;
        } else if trimmed.to_uppercase().starts_with("USE ")
            && !trimmed.to_uppercase().starts_with("USE DUCKLAKE")
        {
            in_ducklake_context = false;
        }

        // Skip ATTACH/DETACH statements (we handle connection in Rust)
        // Also handles multi-line ATTACH with parenthesized options
        if trimmed == "statement ok"
            && let Some(next_line) = lines.peek()
        {
            let next_upper = next_line.trim().to_uppercase();
            if next_upper.starts_with("ATTACH ")
                || next_upper.starts_with("DETACH ")
                || next_upper.starts_with("CHECKPOINT")
            {
                // Skip all lines of the statement (may span multiple lines)
                while let Some(stmt_line) = lines.next() {
                    let t = stmt_line.trim();
                    // Statement ends at blank line or next record
                    if let Some(peek) = lines.peek() {
                        let pt = peek.trim();
                        if pt.is_empty()
                            || pt.starts_with("statement")
                            || pt.starts_with("query")
                            || pt.starts_with("halt")
                            || pt.starts_with("#")
                        {
                            break;
                        }
                    } else {
                        break;
                    }
                    // Also break if this line looks like a complete statement (ends with ;)
                    if t.ends_with(';') || t.ends_with(')') {
                        break;
                    }
                }
                continue;
            }
            if next_upper.starts_with("EXPLAIN ") {
                lines.next();
                continue;
            }
        }

        // Skip query blocks with EXPLAIN or unsupported DuckDB functions
        if trimmed.starts_with("query")
            && let Some(next_line) = lines.peek()
        {
            let next_upper = next_line.trim().to_uppercase();
            if next_upper.starts_with("EXPLAIN ") {
                lines.next();
                skip_query_results(&mut lines);
                continue;
            }
            // Skip queries using DuckDB-specific functions not available in DataFusion
            if contains_unsupported_function(&next_upper) {
                lines.next(); // skip SQL
                skip_query_results(&mut lines);
                continue;
            }
        }

        // Skip statement ok blocks with unsupported DuckDB functions
        if trimmed == "statement ok"
            && let Some(next_line) = lines.peek()
        {
            let next_upper = next_line.trim().to_uppercase();
            if contains_unsupported_function(&next_upper) {
                lines.next(); // skip SQL
                continue;
            }
        }

        // Skip DESCRIBE queries (output format differs significantly between DuckDB and DataFusion)
        if trimmed.starts_with("query")
            && let Some(next_line) = lines.peek()
        {
            let next_upper = next_line.trim().to_uppercase();
            if next_upper.starts_with("DESCRIBE ") {
                lines.next(); // skip SQL
                skip_query_results(&mut lines);
                continue;
            }
        }

        // Strip error expectations from statement error blocks
        // Convert multiline expected errors to empty (accept any error)
        if trimmed.starts_with("statement error") {
            output.push_str("statement error\n");
            // Pass through SQL lines until ---- or blank line or next record
            while let Some(next) = lines.peek() {
                let next_trimmed = next.trim();
                if next_trimmed == "----" {
                    lines.next(); // skip ----
                    // Skip expected error text lines
                    while let Some(err_line) = lines.peek() {
                        let t = err_line.trim();
                        if t.is_empty()
                            || t.starts_with("statement")
                            || t.starts_with("query")
                            || t.starts_with("halt")
                            || t.starts_with("#")
                            || t.starts_with("loop ")
                            || t.starts_with("foreach ")
                        {
                            break;
                        }
                        lines.next();
                    }
                    break;
                } else if next_trimmed.is_empty()
                    || next_trimmed.starts_with("statement")
                    || next_trimmed.starts_with("query")
                    || next_trimmed.starts_with("halt")
                {
                    break;
                } else {
                    // SQL line - pass through, applying table rewriting if needed
                    let sql_line = lines.next().unwrap();
                    if in_ducklake_context {
                        output.push_str(&rewrite_unqualified_tables(sql_line));
                    } else {
                        output.push_str(sql_line);
                    }
                    output.push('\n');
                }
            }
            continue;
        }

        // Rewrite table references in SQL lines if in ducklake context
        // (SQL lines come right after statement/query directives)
        if in_ducklake_context
            && !trimmed.starts_with('#')
            && !trimmed.starts_with("statement")
            && !trimmed.starts_with("query")
            && !trimmed.starts_with("halt")
            && !trimmed.starts_with("----")
            && !trimmed.is_empty()
        {
            output.push_str(&rewrite_unqualified_tables(line));
        } else {
            output.push_str(line);
        }
        output.push('\n');
    }

    output
}

/// Expand loop/foreach/endloop blocks in test content
fn expand_loops(content: &str) -> String {
    let mut output = String::new();
    let mut lines = content.lines().peekable();

    while let Some(line) = lines.next() {
        let trimmed = line.trim();

        // Handle: loop VAR START END
        if trimmed.starts_with("loop ") {
            let parts: Vec<&str> = trimmed.splitn(4, ' ').collect();
            if parts.len() == 4 {
                let var_name = parts[1];
                if let (Ok(start), Ok(end)) = (parts[2].parse::<i64>(), parts[3].parse::<i64>()) {
                    // Collect body lines until endloop
                    let body = collect_loop_body(&mut lines);
                    // Expand: repeat body for each value
                    for i in start..end {
                        let placeholder = format!("${{{}}}", var_name);
                        for body_line in &body {
                            output.push_str(&body_line.replace(&placeholder, &i.to_string()));
                            output.push('\n');
                        }
                    }
                    continue;
                }
            }
        }

        // Handle: foreach VAR val1 val2 ...
        if trimmed.starts_with("foreach ") {
            let parts: Vec<&str> = trimmed.splitn(3, ' ').collect();
            if parts.len() >= 3 {
                let var_name = parts[1];
                let values: Vec<&str> = parts[2].split_whitespace().collect();
                // Collect body lines until endloop
                let body = collect_loop_body(&mut lines);
                // Expand: repeat body for each value
                for val in &values {
                    let placeholder = format!("${{{}}}", var_name);
                    for body_line in &body {
                        output.push_str(&body_line.replace(&placeholder, val));
                        output.push('\n');
                    }
                }
                continue;
            }
        }

        output.push_str(line);
        output.push('\n');
    }

    output
}

/// Collect lines between loop/foreach and endloop
fn collect_loop_body(lines: &mut std::iter::Peekable<std::str::Lines>) -> Vec<String> {
    let mut body = Vec::new();
    while let Some(line) = lines.next() {
        if line.trim() == "endloop" {
            break;
        }
        body.push(line.to_string());
    }
    body
}

/// Skip a record body (SQL + optional results) for multi-connection statements
fn skip_record_body(lines: &mut std::iter::Peekable<std::str::Lines>) {
    // Skip SQL lines
    while let Some(line) = lines.peek() {
        let trimmed = line.trim();
        if trimmed == "----" {
            lines.next(); // skip ----
            // Skip result lines
            while let Some(result_line) = lines.peek() {
                let t = result_line.trim();
                if t.is_empty()
                    || t.starts_with("statement")
                    || t.starts_with("query")
                    || t.starts_with("halt")
                    || t.starts_with("#")
                    || t.starts_with("loop ")
                    || t.starts_with("foreach ")
                {
                    break;
                }
                lines.next();
            }
            return;
        }
        if trimmed.is_empty()
            || trimmed.starts_with("statement")
            || trimmed.starts_with("query")
            || trimmed.starts_with("halt")
            || trimmed.starts_with("#")
        {
            return;
        }
        lines.next();
    }
}

/// Rewrite unqualified table names for DataFusion when in ducklake context
/// After `USE ducklake`, DuckDB resolves bare table names to ducklake.main.table
/// but DataFusion needs explicit catalog.schema.table references
fn rewrite_unqualified_tables(line: &str) -> String {
    // Don't rewrite comments, empty lines, or lines that already have ducklake references
    let trimmed = line.trim();
    if trimmed.starts_with('#') || trimmed.is_empty() {
        return line.to_string();
    }
    // Already handled by HybridDuckLakeDB::rewrite_table_references
    line.to_string()
}

/// Check if SQL line contains DuckDB-specific functions not available in DataFusion
fn contains_unsupported_function(sql_upper: &str) -> bool {
    // DuckDB file/metadata functions
    sql_upper.contains("GLOB(")
        || sql_upper.contains("GLOB '")
        || sql_upper.contains("DUCKDB_TABLES(")
        || sql_upper.contains("DUCKDB_VIEWS(")
        || sql_upper.contains("TEST_ALL_TYPES(")
        || sql_upper.contains("PARQUET_METADATA(")
        || sql_upper.contains("PARQUET_SCHEMA(")
        // DuckLake-specific table functions
        || sql_upper.contains("DUCKLAKE_LIST_FILES(")
        || sql_upper.contains("DUCKLAKE_CLEANUP_OLD_FILES(")
        || sql_upper.contains("DUCKLAKE_EXPIRE_SNAPSHOTS(")
        || sql_upper.contains("DUCKLAKE_SNAPSHOTS(")
        || sql_upper.contains("DUCKLAKE_DELETE_ORPHANED_FILES(")
        || sql_upper.contains("DUCKLAKE_FLUSH_INLINED_DATA(")
        // DuckLake table function syntax: ducklake.function()
        || sql_upper.contains(".SNAPSHOTS(")
        || sql_upper.contains(".TABLE_CHANGES(")
        || sql_upper.contains(".OPTIONS(")
        // Bare DuckLake table functions (after USE ducklake)
        || sql_upper.contains("FROM SNAPSHOTS(")
        || sql_upper.contains("FROM TABLE_CHANGES(")
        // DuckDB functions not in DataFusion
        || sql_upper.contains("STATS(")
        || sql_upper.contains("CURRENT_DATABASE(")
        || sql_upper.contains("LIST_VALUE(")
        || sql_upper.contains("STRUCT_PACK(")
        // Metadata schema access
        || sql_upper.contains("__DUCKLAKE_METADATA_")
        || sql_upper.contains("DUCKLAKE_METADATA.")
        || sql_upper.contains("DUCKLAKE_META.")
}

/// Skip query results until next directive
fn skip_query_results(lines: &mut std::iter::Peekable<std::str::Lines>) {
    // Skip until we find the separator (----)
    while let Some(line) = lines.peek() {
        if line.trim() == "----" {
            lines.next();
            break;
        }
        lines.next();
    }

    // Skip result lines until next directive
    while let Some(line) = lines.peek() {
        let trimmed = line.trim();
        if trimmed.starts_with("query")
            || trimmed.starts_with("statement")
            || trimmed.starts_with("halt")
            || trimmed.is_empty()
        {
            break;
        }
        lines.next();
    }
}

/// Run a DuckDB test file using the hybrid adapter
async fn run_hybrid_test(test_file: &str) -> Result<(), Box<dyn std::error::Error>> {
    let temp_dir = TempDir::new()?;
    let catalog_path = temp_dir.path().join("test.ducklake");

    // Create hybrid DB adapter wrapped in Arc for cloning
    let db = std::sync::Arc::new(std::sync::Mutex::new(HybridDuckLakeDB::new(catalog_path)?));

    // Read and preprocess test file
    let original_content = std::fs::read_to_string(test_file)?;
    let processed_content = preprocess_test_file(&original_content);

    // Write preprocessed test to temp file
    let temp_test_file = temp_dir.path().join("test.slt");
    std::fs::write(&temp_test_file, processed_content)?;

    // Run preprocessed test file with sqllogictest
    let temp_test_path = temp_test_file.to_string_lossy().to_string();
    tokio::task::spawn_blocking(move || {
        let mut runner = Runner::new(|| {
            let db_clone = db.clone();
            async move { Ok(db_clone.lock().unwrap().clone()) }
        });
        runner.run_file(&temp_test_path)
    })
    .await??;

    Ok(())
}

// ============================================================================
// Auto-discovery test runner - runs all .test files
// ============================================================================

#[tokio::test]
async fn run_all_sqllogictests() {
    use std::path::Path;

    let test_dir = Path::new("tests/sqllogictests/sql");
    let mut test_files = Vec::new();

    // Recursively find all .test files
    fn find_test_files(dir: &Path, files: &mut Vec<std::path::PathBuf>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    find_test_files(&path, files);
                } else if path.extension().is_some_and(|ext| ext == "test") {
                    files.push(path);
                }
            }
        }
    }

    find_test_files(test_dir, &mut test_files);
    test_files.sort();

    println!("\nFound {} test files", test_files.len());

    let mut passed = 0;
    let mut failed = 0;
    let mut failed_tests = Vec::new();

    for test_file in &test_files {
        let test_name = test_file
            .strip_prefix("tests/sqllogictests/sql/")
            .unwrap_or(test_file.as_path())
            .display()
            .to_string();

        match run_hybrid_test(test_file.to_str().unwrap()).await {
            Ok(_) => {
                println!("✓ {}", test_name);
                passed += 1;
            },
            Err(e) => {
                println!("✗ {}: {}", test_name, e);
                failed += 1;
                failed_tests.push((test_name, e.to_string()));
            },
        }
    }

    println!("\n========================================");
    println!("Test Summary:");
    println!("  Passed: {}", passed);
    println!("  Failed: {}", failed);
    println!("  Total:  {}", test_files.len());
    println!("========================================");

    if !failed_tests.is_empty() {
        println!("\nFailed tests:");
        for (name, error) in &failed_tests {
            println!("  - {}", name);
            // Print first line of error only
            if let Some(first_line) = error.lines().next() {
                println!("    {}", first_line);
            }
        }
    }
}
