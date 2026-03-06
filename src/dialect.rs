/// SQL dialect differences between SQLite, PostgreSQL, and MySQL.
/// Each method returns a SQL fragment or performs a dialect-specific operation.
pub(crate) trait SqlDialect: Send + Sync + 'static {
    /// Parameter placeholder. SQLite/MySQL: "?", Postgres: "$1", "$2", etc.
    fn ph(&self, n: usize) -> std::borrow::Cow<'static, str>;

    /// Quote an identifier. SQLite/Postgres: "col", MySQL: `col`.
    fn quote_id(&self, name: &str) -> String;

    /// Quote a column name only if it's a reserved word in this dialect.
    /// MySQL must quote `key`, `sql`, `type`. Others return as-is.
    ///
    /// # Safety (SQL injection)
    /// Interpolates the result directly into SQL. Only pass known catalog
    /// column names or compile-time constants — never user input.
    fn col(&self, name: &str) -> String;

    /// SQL expression for current timestamp.
    fn now(&self) -> &'static str;

    /// Generate a UUID. Returns (sql_expr_or_placeholder, Option<bind_value>).
    #[cfg(feature = "write")]
    #[allow(dead_code)]
    fn uuid_value(&self) -> (String, Option<String>);

    /// Boolean literal in SQL text. SQLite: "1"/"0", PG/MySQL: "TRUE"/"FALSE".
    fn bool_lit(&self, val: bool) -> &'static str;

    /// CAST to text type. SQLite: TEXT, Postgres: VARCHAR, MySQL: CHAR.
    fn cast_text(&self, expr: &str) -> String;

    /// CAST to integer type. SQLite: no-op, Postgres: BIGINT, MySQL: SIGNED.
    fn cast_int(&self, expr: &str) -> String;

    /// Clamp-to-zero expression. SQLite: MAX(0, expr), PG/MySQL: GREATEST(0, expr).
    fn clamp_zero(&self, expr: &str) -> String;

    /// Upsert clause. Returns the full ON CONFLICT / ON DUPLICATE KEY clause.
    ///
    /// # Safety (SQL injection)
    /// `conflict_col` and `set_cols` are interpolated directly into SQL.
    /// Only pass known catalog column names — never user input.
    fn upsert(&self, conflict_col: &str, set_cols: &[&str]) -> String;

    /// Whether this dialect supports RETURNING on INSERT. SQLite/PG: true, MySQL: false.
    fn supports_returning(&self) -> bool;

    /// FOR UPDATE clause. SQLite: "" (empty), PG/MySQL: " FOR UPDATE".
    fn for_update(&self) -> &'static str;

    /// INSERT-or-ignore syntax.
    ///
    /// # Safety (SQL injection)
    /// `table`, `columns`, and `values` are interpolated directly into SQL.
    /// Only pass known catalog table/column names and placeholders — never user input.
    #[allow(dead_code)]
    fn insert_or_ignore(&self, table: &str, columns: &str, values: &str) -> String;

    /// Whether existence checks use COUNT(*) (true) or SELECT EXISTS (false).
    fn existence_check_is_count(&self) -> bool;

    /// Greatest of two expressions. SQLite: MAX(a, b), PG/MySQL: GREATEST(a, b).
    #[cfg(feature = "write")]
    fn greatest(&self, a: &str, b: &str) -> String;

    /// Read a UUID column as a String. PG: CAST(col AS VARCHAR), others: col as-is.
    #[cfg(feature = "write")]
    fn read_uuid(&self, col: &str) -> String;

    /// Bind placeholder for a UUID value. PG: $n::UUID, others: ph(n).
    #[cfg(feature = "write")]
    fn uuid_ph(&self, n: usize) -> String;

    /// SQL to allocate the next ID for a given entity.
    /// `entity` is "schema_version", "view_id", "column_id", or "partition_id".
    /// `table_id_bind` is an optional extra bind parameter placeholder for per-table scoping.
    /// Returns (sql, needs_table_id_bind).
    /// SQLite: SELECT COALESCE(MAX(col), 0) + 1 FROM table [WHERE table_id = ?]
    /// PG: SELECT nextval('ducklake_{entity}_seq')
    /// MySQL: not used — each backend provides its own next_id async fn.
    #[cfg(feature = "write")]
    fn next_id_sql(&self, entity: &str) -> (String, bool);
}

// --- SQLite ---

#[cfg_attr(not(feature = "write"), allow(dead_code))]
pub(crate) struct SqliteDialect;

impl SqlDialect for SqliteDialect {
    fn ph(&self, _n: usize) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("?")
    }

    fn quote_id(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn col(&self, name: &str) -> String {
        name.to_string()
    }

    fn now(&self) -> &'static str {
        "strftime('%Y-%m-%d %H:%M:%f+00:00','now')"
    }

    #[cfg(feature = "write")]
    fn uuid_value(&self) -> (String, Option<String>) {
        ("?".to_string(), Some(uuid::Uuid::new_v4().to_string()))
    }

    fn bool_lit(&self, val: bool) -> &'static str {
        if val {
            "1"
        } else {
            "0"
        }
    }

    fn cast_text(&self, expr: &str) -> String {
        format!("CAST({expr} AS TEXT)")
    }

    fn cast_int(&self, expr: &str) -> String {
        expr.to_string()
    }

    fn clamp_zero(&self, expr: &str) -> String {
        format!("MAX(0, {expr})")
    }

    fn upsert(&self, conflict_col: &str, set_cols: &[&str]) -> String {
        debug_assert!(!set_cols.is_empty(), "upsert requires at least one column");
        let sets: Vec<String> = set_cols
            .iter()
            .map(|c| format!("{c} = excluded.{c}"))
            .collect();
        format!(
            "ON CONFLICT({conflict_col}) DO UPDATE SET {}",
            sets.join(", ")
        )
    }

    fn supports_returning(&self) -> bool {
        true
    }

    fn for_update(&self) -> &'static str {
        ""
    }

    fn insert_or_ignore(&self, table: &str, columns: &str, values: &str) -> String {
        format!("INSERT OR IGNORE INTO {table} ({columns}) VALUES ({values})")
    }

    fn existence_check_is_count(&self) -> bool {
        true
    }

    #[cfg(feature = "write")]
    fn greatest(&self, a: &str, b: &str) -> String {
        format!("MAX({a}, {b})")
    }

    #[cfg(feature = "write")]
    fn read_uuid(&self, col: &str) -> String {
        col.to_string()
    }

    #[cfg(feature = "write")]
    fn uuid_ph(&self, _n: usize) -> String {
        "?".to_string()
    }

    #[cfg(feature = "write")]
    fn next_id_sql(&self, entity: &str) -> (String, bool) {
        match entity {
            "schema_version" => (
                "SELECT COALESCE(MAX(schema_version), 0) + 1 FROM ducklake_snapshot".to_string(),
                false,
            ),
            "view_id" => (
                "SELECT COALESCE(MAX(view_id), 0) + 1 FROM ducklake_view".to_string(),
                false,
            ),
            "column_id" => (
                format!(
                    "SELECT COALESCE(MAX(column_id), 0) + 1 FROM ducklake_column WHERE table_id = {}",
                    self.ph(1)
                ),
                true,
            ),
            "partition_id" => (
                "SELECT COALESCE(MAX(partition_id), 0) + 1 FROM ducklake_partition_info"
                    .to_string(),
                false,
            ),
            _ => unreachable!("next_id_sql called with unknown entity: {entity}"),
        }
    }
}

// --- PostgreSQL ---

#[cfg_attr(not(feature = "write"), allow(dead_code))]
pub(crate) struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn ph(&self, n: usize) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Owned(format!("${n}"))
    }

    fn quote_id(&self, name: &str) -> String {
        format!("\"{}\"", name.replace('"', "\"\""))
    }

    fn col(&self, name: &str) -> String {
        name.to_string()
    }

    fn now(&self) -> &'static str {
        "CURRENT_TIMESTAMP"
    }

    #[cfg(feature = "write")]
    fn uuid_value(&self) -> (String, Option<String>) {
        ("gen_random_uuid()".to_string(), None)
    }

    fn bool_lit(&self, val: bool) -> &'static str {
        if val {
            "TRUE"
        } else {
            "FALSE"
        }
    }

    fn cast_text(&self, expr: &str) -> String {
        format!("CAST({expr} AS VARCHAR)")
    }

    fn cast_int(&self, expr: &str) -> String {
        format!("CAST({expr} AS BIGINT)")
    }

    fn clamp_zero(&self, expr: &str) -> String {
        format!("GREATEST(0, {expr})")
    }

    fn upsert(&self, conflict_col: &str, set_cols: &[&str]) -> String {
        debug_assert!(!set_cols.is_empty(), "upsert requires at least one column");
        let sets: Vec<String> = set_cols
            .iter()
            .map(|c| format!("{c} = EXCLUDED.{c}"))
            .collect();
        format!(
            "ON CONFLICT({conflict_col}) DO UPDATE SET {}",
            sets.join(", ")
        )
    }

    fn supports_returning(&self) -> bool {
        true
    }

    fn for_update(&self) -> &'static str {
        " FOR UPDATE"
    }

    fn insert_or_ignore(&self, table: &str, columns: &str, values: &str) -> String {
        format!("INSERT INTO {table} ({columns}) VALUES ({values}) ON CONFLICT DO NOTHING")
    }

    fn existence_check_is_count(&self) -> bool {
        false
    }

    #[cfg(feature = "write")]
    fn greatest(&self, a: &str, b: &str) -> String {
        format!("GREATEST({a}, {b})")
    }

    #[cfg(feature = "write")]
    fn read_uuid(&self, col: &str) -> String {
        format!("CAST({col} AS VARCHAR)")
    }

    #[cfg(feature = "write")]
    fn uuid_ph(&self, n: usize) -> String {
        format!("${n}::UUID")
    }

    #[cfg(feature = "write")]
    fn next_id_sql(&self, entity: &str) -> (String, bool) {
        let seq = match entity {
            "schema_version" => "ducklake_schema_version_seq",
            "view_id" => "ducklake_view_id_seq",
            "column_id" => "ducklake_column_id_seq",
            "partition_id" => "ducklake_partition_id_seq",
            _ => unreachable!("next_id_sql called with unknown entity: {entity}"),
        };
        (format!("SELECT nextval('{seq}')"), false)
    }
}

// --- MySQL ---

#[cfg_attr(not(feature = "write"), allow(dead_code))]
pub(crate) struct MySqlDialect;

impl SqlDialect for MySqlDialect {
    fn ph(&self, _n: usize) -> std::borrow::Cow<'static, str> {
        std::borrow::Cow::Borrowed("?")
    }

    fn quote_id(&self, name: &str) -> String {
        format!("`{}`", name.replace('`', "``"))
    }

    fn col(&self, name: &str) -> String {
        match name {
            "key" | "sql" | "type" => format!("`{name}`"),
            _ => name.to_string(),
        }
    }

    fn now(&self) -> &'static str {
        "NOW(6)"
    }

    #[cfg(feature = "write")]
    fn uuid_value(&self) -> (String, Option<String>) {
        ("?".to_string(), Some(uuid::Uuid::new_v4().to_string()))
    }

    fn bool_lit(&self, val: bool) -> &'static str {
        if val {
            "TRUE"
        } else {
            "FALSE"
        }
    }

    fn cast_text(&self, expr: &str) -> String {
        format!("CAST({expr} AS CHAR)")
    }

    fn cast_int(&self, expr: &str) -> String {
        format!("CAST({expr} AS SIGNED)")
    }

    fn clamp_zero(&self, expr: &str) -> String {
        format!("GREATEST(0, {expr})")
    }

    fn upsert(&self, _conflict_col: &str, set_cols: &[&str]) -> String {
        debug_assert!(!set_cols.is_empty(), "upsert requires at least one column");
        let sets: Vec<String> = set_cols
            .iter()
            .map(|c| format!("{c} = VALUES({c})"))
            .collect();
        format!("ON DUPLICATE KEY UPDATE {}", sets.join(", "))
    }

    fn supports_returning(&self) -> bool {
        false
    }

    fn for_update(&self) -> &'static str {
        " FOR UPDATE"
    }

    fn insert_or_ignore(&self, table: &str, columns: &str, values: &str) -> String {
        format!("INSERT IGNORE INTO {table} ({columns}) VALUES ({values})")
    }

    fn existence_check_is_count(&self) -> bool {
        true
    }

    #[cfg(feature = "write")]
    fn greatest(&self, a: &str, b: &str) -> String {
        format!("GREATEST({a}, {b})")
    }

    #[cfg(feature = "write")]
    fn read_uuid(&self, col: &str) -> String {
        col.to_string()
    }

    #[cfg(feature = "write")]
    fn uuid_ph(&self, _n: usize) -> String {
        "?".to_string()
    }

    #[cfg(feature = "write")]
    fn next_id_sql(&self, _entity: &str) -> (String, bool) {
        unreachable!("MySQL uses next_sequence_id() instead of next_id_sql()")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- ph() ---

    #[test]
    fn test_sqlite_ph() {
        let d = SqliteDialect;
        assert_eq!(d.ph(1), "?");
        assert_eq!(d.ph(99), "?");
    }

    #[test]
    fn test_postgres_ph() {
        let d = PostgresDialect;
        assert_eq!(d.ph(1), "$1");
        assert_eq!(d.ph(5), "$5");
    }

    #[test]
    fn test_mysql_ph() {
        let d = MySqlDialect;
        assert_eq!(d.ph(1), "?");
        assert_eq!(d.ph(99), "?");
    }

    // --- quote_id() ---

    #[test]
    fn test_sqlite_quote_id() {
        let d = SqliteDialect;
        assert_eq!(d.quote_id("col"), "\"col\"");
        assert_eq!(d.quote_id("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn test_postgres_quote_id() {
        let d = PostgresDialect;
        assert_eq!(d.quote_id("col"), "\"col\"");
        assert_eq!(d.quote_id("a\"b"), "\"a\"\"b\"");
    }

    #[test]
    fn test_mysql_quote_id() {
        let d = MySqlDialect;
        assert_eq!(d.quote_id("col"), "`col`");
        assert_eq!(d.quote_id("a`b"), "`a``b`");
    }

    // --- col() ---

    #[test]
    fn test_sqlite_col_no_quoting() {
        let d = SqliteDialect;
        assert_eq!(d.col("key"), "key");
        assert_eq!(d.col("sql"), "sql");
        assert_eq!(d.col("name"), "name");
    }

    #[test]
    fn test_postgres_col_no_quoting() {
        let d = PostgresDialect;
        assert_eq!(d.col("key"), "key");
        assert_eq!(d.col("sql"), "sql");
    }

    #[test]
    fn test_mysql_col_quotes_reserved() {
        let d = MySqlDialect;
        assert_eq!(d.col("key"), "`key`");
        assert_eq!(d.col("sql"), "`sql`");
        assert_eq!(d.col("type"), "`type`");
        assert_eq!(d.col("name"), "name");
    }

    // --- bool_lit() ---

    #[test]
    fn test_sqlite_bool_lit() {
        let d = SqliteDialect;
        assert_eq!(d.bool_lit(true), "1");
        assert_eq!(d.bool_lit(false), "0");
    }

    #[test]
    fn test_postgres_bool_lit() {
        let d = PostgresDialect;
        assert_eq!(d.bool_lit(true), "TRUE");
        assert_eq!(d.bool_lit(false), "FALSE");
    }

    #[test]
    fn test_mysql_bool_lit() {
        let d = MySqlDialect;
        assert_eq!(d.bool_lit(true), "TRUE");
        assert_eq!(d.bool_lit(false), "FALSE");
    }

    // --- upsert() ---

    #[test]
    fn test_sqlite_upsert() {
        let d = SqliteDialect;
        let result = d.upsert("id", &["name", "value"]);
        assert_eq!(
            result,
            "ON CONFLICT(id) DO UPDATE SET name = excluded.name, value = excluded.value"
        );
    }

    #[test]
    fn test_postgres_upsert() {
        let d = PostgresDialect;
        let result = d.upsert("id", &["name", "value"]);
        assert_eq!(
            result,
            "ON CONFLICT(id) DO UPDATE SET name = EXCLUDED.name, value = EXCLUDED.value"
        );
    }

    #[test]
    fn test_mysql_upsert() {
        let d = MySqlDialect;
        let result = d.upsert("id", &["name", "value"]);
        assert_eq!(
            result,
            "ON DUPLICATE KEY UPDATE name = VALUES(name), value = VALUES(value)"
        );
    }

    // --- insert_or_ignore() ---

    #[test]
    fn test_sqlite_insert_or_ignore() {
        let d = SqliteDialect;
        assert_eq!(
            d.insert_or_ignore("t", "a, b", "?, ?"),
            "INSERT OR IGNORE INTO t (a, b) VALUES (?, ?)"
        );
    }

    #[test]
    fn test_postgres_insert_or_ignore() {
        let d = PostgresDialect;
        assert_eq!(
            d.insert_or_ignore("t", "a, b", "$1, $2"),
            "INSERT INTO t (a, b) VALUES ($1, $2) ON CONFLICT DO NOTHING"
        );
    }

    #[test]
    fn test_mysql_insert_or_ignore() {
        let d = MySqlDialect;
        assert_eq!(
            d.insert_or_ignore("t", "a, b", "?, ?"),
            "INSERT IGNORE INTO t (a, b) VALUES (?, ?)"
        );
    }

    // --- supports_returning / for_update / existence_check ---

    #[test]
    fn test_supports_returning() {
        assert!(SqliteDialect.supports_returning());
        assert!(PostgresDialect.supports_returning());
        assert!(!MySqlDialect.supports_returning());
    }

    #[test]
    fn test_for_update() {
        assert_eq!(SqliteDialect.for_update(), "");
        assert_eq!(PostgresDialect.for_update(), " FOR UPDATE");
        assert_eq!(MySqlDialect.for_update(), " FOR UPDATE");
    }

    #[test]
    fn test_existence_check_is_count() {
        assert!(SqliteDialect.existence_check_is_count());
        assert!(!PostgresDialect.existence_check_is_count());
        assert!(MySqlDialect.existence_check_is_count());
    }

    // --- cast / clamp ---

    #[test]
    fn test_cast_text() {
        assert_eq!(SqliteDialect.cast_text("x"), "CAST(x AS TEXT)");
        assert_eq!(PostgresDialect.cast_text("x"), "CAST(x AS VARCHAR)");
        assert_eq!(MySqlDialect.cast_text("x"), "CAST(x AS CHAR)");
    }

    #[test]
    fn test_cast_int() {
        assert_eq!(SqliteDialect.cast_int("x"), "x");
        assert_eq!(PostgresDialect.cast_int("x"), "CAST(x AS BIGINT)");
        assert_eq!(MySqlDialect.cast_int("x"), "CAST(x AS SIGNED)");
    }

    #[test]
    fn test_clamp_zero() {
        assert_eq!(SqliteDialect.clamp_zero("x"), "MAX(0, x)");
        assert_eq!(PostgresDialect.clamp_zero("x"), "GREATEST(0, x)");
        assert_eq!(MySqlDialect.clamp_zero("x"), "GREATEST(0, x)");
    }

    #[test]
    fn test_now() {
        assert_eq!(
            SqliteDialect.now(),
            "strftime('%Y-%m-%d %H:%M:%f+00:00','now')"
        );
        assert_eq!(PostgresDialect.now(), "CURRENT_TIMESTAMP");
        assert_eq!(MySqlDialect.now(), "NOW(6)");
    }

    // --- write-feature-gated methods ---

    #[cfg(feature = "write")]
    mod write_tests {
        use super::*;

        #[test]
        fn test_sqlite_next_id_sql() {
            let d = SqliteDialect;
            let (sql, needs_bind) = d.next_id_sql("schema_version");
            assert!(sql.contains("ducklake_snapshot"));
            assert!(!needs_bind);

            let (sql, needs_bind) = d.next_id_sql("column_id");
            assert!(sql.contains("ducklake_column"));
            assert!(needs_bind);
        }

        #[test]
        fn test_postgres_next_id_sql() {
            let d = PostgresDialect;
            let (sql, needs_bind) = d.next_id_sql("schema_version");
            assert!(sql.contains("nextval"));
            assert!(sql.contains("ducklake_schema_version_seq"));
            assert!(!needs_bind);
        }

        #[test]
        fn test_mysql_next_id_sql_returns_placeholder() {
            let d = MySqlDialect;
            let (sql, needs_bind) = d.next_id_sql("schema_version");
            assert_eq!(sql, "SELECT 0");
            assert!(!needs_bind);
        }

        #[test]
        fn test_greatest() {
            assert_eq!(SqliteDialect.greatest("a", "b"), "MAX(a, b)");
            assert_eq!(PostgresDialect.greatest("a", "b"), "GREATEST(a, b)");
            assert_eq!(MySqlDialect.greatest("a", "b"), "GREATEST(a, b)");
        }

        #[test]
        fn test_read_uuid() {
            assert_eq!(SqliteDialect.read_uuid("col"), "col");
            assert_eq!(PostgresDialect.read_uuid("col"), "CAST(col AS VARCHAR)");
            assert_eq!(MySqlDialect.read_uuid("col"), "col");
        }

        #[test]
        fn test_uuid_ph() {
            assert_eq!(SqliteDialect.uuid_ph(1), "?");
            assert_eq!(PostgresDialect.uuid_ph(3), "$3::UUID");
            assert_eq!(MySqlDialect.uuid_ph(1), "?");
        }
    }
}
