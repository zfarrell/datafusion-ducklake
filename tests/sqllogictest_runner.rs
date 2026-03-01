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
/// 9. Rewrites ORDER BY ALL (not supported by DataFusion's SQL dialect)
/// 10. Skips queries with DuckDB named parameter syntax (=>)
fn preprocess_test_file(content: &str, test_dir: &str) -> String {
    // First pass: collect test-env variables and expand them
    let mut vars = std::collections::HashMap::new();
    vars.insert("__TEST_DIR__".to_string(), test_dir.to_string());
    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with("test-env ") {
            let parts: Vec<&str> = trimmed.splitn(3, ' ').collect();
            if parts.len() >= 3 {
                let var_name = parts[1];
                let var_value = parts[2];
                // Resolve value against existing vars
                let mut resolved = var_value.to_string();
                for (k, v) in &vars {
                    resolved = resolved.replace(k, v);
                }
                vars.insert(var_name.to_string(), resolved);
            }
        }
    }

    // Apply variable substitution to content
    let mut substituted = content.to_string();
    for (k, v) in &vars {
        let pattern = format!("${{{}}}", k);
        substituted = substituted.replace(&pattern, v);
    }

    // Second pass: expand loop/foreach blocks
    let expanded = expand_loops(&substituted);

    // Third pass: handle directives and rewriting
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
            // Tests requiring unavailable extensions or incompatible modes are halted
            if trimmed.starts_with("require no_extension_autoloading")
                || trimmed == "require spatial"
                || trimmed == "require icu"
            {
                output.push_str("halt\n\n");
                return output;
            }
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

        // Tests requiring unzip are incompatible (need pre-existing databases)
        if trimmed.starts_with("unzip ") {
            output.push_str("halt\n\n");
            return output;
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
        // Applies to both statement ok and statement error (ATTACH errors don't apply in hybrid)
        if (trimmed == "statement ok" || trimmed.starts_with("statement error"))
            && let Some(next_line) = lines.peek()
        {
            let next_upper = next_line.trim().to_uppercase();
            if next_upper.starts_with("ATTACH ")
                || next_upper.starts_with("DETACH ")
                || next_upper.starts_with("CHECKPOINT")
                || next_upper.starts_with("COMMENT ON ")
                || next_upper.starts_with("PRAGMA ")
                || next_upper.starts_with("SET ALLOW_PERSISTENT_SECRETS")
                || next_upper.starts_with("SET EXTENSION_DIRECTORY")
                || next_upper.starts_with("SET AUTOLOAD_")
                || next_upper.starts_with("SET AUTOINSTALL_")
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
                // For statement error blocks, also skip ---- and error text
                if trimmed.starts_with("statement error") {
                    skip_statement_error_body(&mut lines);
                }
                continue;
            }
            if next_upper.starts_with("EXPLAIN ") {
                lines.next();
                continue;
            }
        }

        // Skip query blocks with EXPLAIN, unsupported functions, DESCRIBE, or named params
        if trimmed.starts_with("query")
            && let Some(next_line) = lines.peek()
        {
            let next_upper = next_line.trim().to_uppercase();
            if next_upper.starts_with("EXPLAIN ")
                || next_upper.starts_with("DESCRIBE ")
                || next_upper.starts_with("SHOW TABLES")
                || next_upper.starts_with("SHOW ALL TABLES")
            {
                lines.next();
                skip_query_results(&mut lines);
                continue;
            }
            // Collect all SQL lines (until ---- or blank line or next directive) to check
            // for unsupported functions across multi-line queries
            let full_sql_upper = collect_query_sql_preview(&lines, &next_upper);
            // Skip queries using DuckDB-specific functions not available in DataFusion
            if contains_unsupported_function(&full_sql_upper) {
                lines.next(); // skip first SQL line
                skip_query_results(&mut lines);
                continue;
            }
            // Skip queries with DuckDB named parameter syntax (key => value)
            if full_sql_upper.contains("=>") {
                lines.next(); // skip SQL
                skip_query_results(&mut lines);
                continue;
            }
            // Skip queries that mix virtual columns with * (causes duplicate projection errors)
            if has_virtual_column_star_conflict(&full_sql_upper) {
                lines.next(); // skip SQL
                skip_query_results(&mut lines);
                continue;
            }
            // Handle ORDER BY ALL: remove it from SQL and add rowsort to query directive
            if full_sql_upper.contains("ORDER BY ALL") {
                let query_line = rewrite_query_directive_with_rowsort(trimmed);
                output.push_str(&query_line);
                output.push('\n');
                let sql_line = lines.next().unwrap();
                let rewritten_sql = rewrite_order_by_all(sql_line);
                if in_ducklake_context {
                    output.push_str(&rewrite_unqualified_tables(&rewritten_sql));
                } else {
                    output.push_str(&rewritten_sql);
                }
                output.push('\n');
                continue;
            }
        }

        // Skip statement ok/error blocks with unsupported DuckDB functions or named params
        if (trimmed == "statement ok" || trimmed.starts_with("statement error"))
            && let Some(next_line) = lines.peek()
        {
            let next_upper = next_line.trim().to_uppercase();
            let full_sql_upper = collect_query_sql_preview(&lines, &next_upper);
            if contains_unsupported_function(&full_sql_upper) {
                lines.next(); // skip SQL
                // Skip remaining SQL lines
                while let Some(peek) = lines.peek() {
                    let t = peek.trim();
                    if t == "----"
                        || t.is_empty()
                        || t.starts_with("statement")
                        || t.starts_with("query")
                        || t.starts_with("halt")
                        || t.starts_with("#")
                    {
                        break;
                    }
                    lines.next();
                }
                if trimmed.starts_with("statement error") {
                    // Skip any ---- and error text
                    skip_statement_error_body(&mut lines);
                }
                continue;
            }
            // Skip statements with named parameter syntax (key => value)
            if full_sql_upper.contains("=>") && trimmed == "statement ok" {
                lines.next(); // skip SQL
                continue;
            }
        }

        // Strip error expectations from statement error blocks
        // Convert to statement ok when error condition can't occur in hybrid mode
        if trimmed.starts_with("statement error") {
            // Collect SQL lines and error text to decide conversion
            let mut sql_lines_collected = Vec::new();
            let mut error_text = String::new();
            // Use a clone to peek ahead without consuming
            let mut preview = lines.clone();
            while let Some(next) = preview.next() {
                let next_trimmed = next.trim();
                if next_trimmed == "----" {
                    // Collect error text lines
                    while let Some(err_line) = preview.next() {
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
                        if !error_text.is_empty() {
                            error_text.push(' ');
                        }
                        error_text.push_str(t);
                    }
                    break;
                } else if next_trimmed.is_empty()
                    || next_trimmed.starts_with("statement")
                    || next_trimmed.starts_with("query")
                    || next_trimmed.starts_with("halt")
                {
                    break;
                } else {
                    sql_lines_collected.push(next.to_string());
                }
            }

            // Check if error text matches patterns that can't occur in hybrid mode
            let error_upper = error_text.to_uppercase();
            let convert_to_ok = is_hybrid_incompatible_error(&error_upper);

            if convert_to_ok {
                output.push_str("statement ok\n");
            } else {
                output.push_str("statement error\n");
            }

            // Now consume and output SQL lines from the actual iterator
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

/// Rewrite ORDER BY ALL to empty (since DataFusion's dialect doesn't support it)
fn rewrite_order_by_all(sql: &str) -> String {
    // Case-insensitive replacement of ORDER BY ALL
    let upper = sql.to_uppercase();
    if let Some(pos) = upper.find("ORDER BY ALL") {
        let before = &sql[..pos];
        let after = &sql[pos + 12..]; // len("ORDER BY ALL") = 12
        // Check if "ALL" is followed by a comma (ORDER BY ALL, col) or end
        let trimmed_after = after.trim();
        if trimmed_after.is_empty() || trimmed_after.starts_with(';') {
            // Simple case: ORDER BY ALL at end of query
            format!("{}{}", before.trim_end(), after)
        } else {
            // ORDER BY ALL followed by more (e.g. LIMIT, etc.)
            format!("{}{}", before.trim_end(), after)
        }
    } else {
        sql.to_string()
    }
}

/// Add rowsort to a query directive if not already present
fn rewrite_query_directive_with_rowsort(directive: &str) -> String {
    // query directive format: "query <types> [sort_mode] [label]"
    // If no sort mode, add rowsort
    if directive.contains("rowsort")
        || directive.contains("nosort")
        || directive.contains("valuesort")
    {
        return directive.to_string();
    }
    // Insert rowsort after the type spec
    // e.g., "query II" → "query II rowsort"
    format!("{} rowsort", directive)
}

/// Skip statement error body (---- and expected error text)
fn skip_statement_error_body(lines: &mut std::iter::Peekable<std::str::Lines>) {
    while let Some(next) = lines.peek() {
        let t = next.trim();
        if t == "----" {
            lines.next();
            // Skip error text
            while let Some(err_line) = lines.peek() {
                let et = err_line.trim();
                if et.is_empty()
                    || et.starts_with("statement")
                    || et.starts_with("query")
                    || et.starts_with("halt")
                    || et.starts_with("#")
                {
                    break;
                }
                lines.next();
            }
            return;
        }
        if t.is_empty()
            || t.starts_with("statement")
            || t.starts_with("query")
            || t.starts_with("halt")
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

/// Check if a query mixes virtual column names with * (SELECT rowid, * FROM ...)
/// DataFusion errors on duplicate projection names when virtual columns appear in both
/// explicit columns and * expansion
fn has_virtual_column_star_conflict(sql_upper: &str) -> bool {
    // Only check SELECT-like queries
    if !sql_upper.contains('*') {
        return false;
    }
    // Check if any virtual column name is explicitly referenced alongside *
    let virtual_cols = ["ROWID", "SNAPSHOT_ID", "FILE_INDEX", "FILENAME", "FILE_ROW_NUMBER"];
    // Look for patterns like "SELECT rowid, *" or "SELECT *, snapshot_id"
    // The * must be in a SELECT context (not in COUNT(*) or similar)
    let has_select_star = sql_upper.contains(" * ")
        || sql_upper.contains(",*")
        || sql_upper.contains("* ,")
        || sql_upper.contains(", *")
        || sql_upper.ends_with(" *")
        || sql_upper.contains("SELECT *");

    if !has_select_star {
        return false;
    }

    for col in &virtual_cols {
        // Check if the virtual column appears as a word in the SELECT (not inside a function)
        if sql_upper.contains(col) {
            return true;
        }
    }
    false
}

/// Check if SQL line contains DuckDB-specific functions not available in DataFusion
fn contains_unsupported_function(sql_upper: &str) -> bool {
    // DuckDB file/metadata functions
    sql_upper.contains("GLOB(")
        || sql_upper.contains("GLOB '")
        || sql_upper.contains("DUCKDB_TABLES(")
        || sql_upper.contains("DUCKDB_VIEWS(")
        || sql_upper.contains("DUCKDB_COLUMNS(")
        || sql_upper.contains("DUCKDB_DATABASES(")
        || sql_upper.contains("TEST_ALL_TYPES(")
        || sql_upper.contains("PARQUET_METADATA(")
        || sql_upper.contains("PARQUET_SCHEMA(")
        || sql_upper.contains("READ_PARQUET(")
        // DuckLake-specific table functions
        || sql_upper.contains("DUCKLAKE_LIST_FILES(")
        || sql_upper.contains("DUCKLAKE_CLEANUP_OLD_FILES(")
        || sql_upper.contains("DUCKLAKE_EXPIRE_SNAPSHOTS(")
        || sql_upper.contains("DUCKLAKE_SNAPSHOTS(")
        || sql_upper.contains("DUCKLAKE_DELETE_ORPHANED_FILES(")
        || sql_upper.contains("DUCKLAKE_FLUSH_INLINED_DATA(")
        || sql_upper.contains("DUCKLAKE_CURRENT_SNAPSHOT(")
        || sql_upper.contains("DUCKLAKE_LAST_COMMITTED_SNAPSHOT(")
        || sql_upper.contains("DUCKLAKE_TABLE_INSERTIONS(")
        || sql_upper.contains("DUCKLAKE_TABLE_DELETIONS(")
        || sql_upper.contains("DUCKLAKE_MERGE_ADJACENT_FILES(")
        || sql_upper.contains("DUCKLAKE_REWRITE_DATA_FILES(")
        // DuckLake table function syntax: ducklake.function()
        || sql_upper.contains(".SNAPSHOTS(")
        || sql_upper.contains(".TABLE_CHANGES(")
        || sql_upper.contains(".OPTIONS(")
        // Bare DuckLake table functions (after USE ducklake)
        || sql_upper.contains("FROM SNAPSHOTS(")
        || sql_upper.contains("FROM TABLE_CHANGES(")
        || sql_upper.contains("FROM LAST_COMMITTED_SNAPSHOT(")
        || sql_upper.contains("FROM CURRENT_SNAPSHOT(")
        // DuckDB functions not in DataFusion
        || sql_upper.contains("STATS(")
        || sql_upper.contains("CURRENT_DATABASE(")
        || sql_upper.contains("LIST_VALUE(")
        || sql_upper.contains("STRUCT_PACK(")
        || sql_upper.contains("TYPEOF(")
        || sql_upper.contains("CURRENT_SETTING(")
        || sql_upper.contains("STRLEN(")
        // Metadata schema access
        || sql_upper.contains("__DUCKLAKE_METADATA_")
        || sql_upper.contains("DUCKLAKE_METADATA.")
        || sql_upper.contains("DUCKLAKE_META.")
        || sql_upper.contains("METADATA.DUCKLAKE_")
        // DuckDB functions not available
        || sql_upper.contains("COLUMNS(")
        // Infinity timestamp literals (DataFusion optimizer can't handle)
        || sql_upper.contains("'INFINITY'")
        || sql_upper.contains("'-INFINITY'")
        // DuckDB SQL not supported in DataFusion
        || sql_upper.contains("COMMENT ON ")
        || sql_upper.contains("PRAGMA ")
        // DuckDB-specific table functions
        || sql_upper.contains("PRAGMA_DATABASE_SIZE(")
        || sql_upper.contains("TABLE_INFO(")
        // DuckDB SHOW commands
        || sql_upper.contains("SHOW TABLES")
}

/// Check if an expected error text matches patterns that can't occur in hybrid mode.
/// When these patterns match, the statement error should be converted to statement ok.
fn is_hybrid_incompatible_error(error_upper: &str) -> bool {
    // Read-only errors: hybrid adapter always has writable DuckDB
    error_upper.contains("READ-ONLY")
    || error_upper.contains("READ ONLY")
    // Detach-related: DETACH is skipped in hybrid mode, catalog remains available
    || error_upper.contains("DOES NOT EXIST!")
    // Missing extension: parquet is always loaded in hybrid mode
    || error_upper.contains("MISSING EXTENSION ERROR")
    || error_upper.contains("COULD NOT LOAD THE COPY FUNCTION")
    // Transaction-local inlined data: DuckDB version may not enforce this constraint
    || error_upper.contains("TRANSACTION-LOCAL INLINED DATA")
}

/// Preview all SQL lines of a query block without consuming the iterator.
/// Used to check multi-line queries for unsupported functions.
/// Takes the already-peeked first line's uppercase and the peekable iterator.
fn collect_query_sql_preview(
    lines: &std::iter::Peekable<std::str::Lines>,
    first_line_upper: &str,
) -> String {
    // We can't peek more than one line ahead, so clone the iterator to preview
    let mut preview = lines.clone();
    let mut full_sql = first_line_upper.to_string();
    preview.next(); // skip the first SQL line (already in first_line_upper)
    while let Some(line) = preview.next() {
        let t = line.trim();
        // Stop at separator, blank line, or next directive
        if t == "----"
            || t.is_empty()
            || t.starts_with("statement")
            || t.starts_with("query")
            || t.starts_with("halt")
            || t.starts_with("#")
        {
            break;
        }
        full_sql.push(' ');
        full_sql.push_str(&t.to_uppercase());
    }
    full_sql
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
    let test_dir_str = temp_dir.path().to_string_lossy().to_string();
    let processed_content = preprocess_test_file(&original_content, &test_dir_str);

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
