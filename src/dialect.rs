/// SQL dialect differences between SQLite, PostgreSQL, and MySQL.
/// Each method returns a SQL fragment or performs a dialect-specific operation.
pub(crate) trait SqlDialect: Send + Sync + 'static {
    /// Parameter placeholder. SQLite/MySQL: "?", Postgres: "$1", "$2", etc.
    fn ph(&self, n: usize) -> String;

    /// Quote an identifier. SQLite/Postgres: "col", MySQL: `col`.
    fn quote_id(&self, name: &str) -> String;

    /// Quote a column name only if it's a reserved word in this dialect.
    /// MySQL must quote `key`, `sql`, `type`. Others return as-is.
    fn col(&self, name: &str) -> String;

    /// SQL expression for current timestamp.
    fn now(&self) -> &'static str;

    /// Generate a UUID. Returns (sql_expr_or_placeholder, Option<bind_value>).
    #[cfg(feature = "write")]
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
    fn upsert(&self, conflict_col: &str, set_cols: &[&str]) -> String;

    /// Whether this dialect supports RETURNING on INSERT. SQLite/PG: true, MySQL: false.
    fn supports_returning(&self) -> bool;

    /// FOR UPDATE clause. SQLite: "" (empty), PG/MySQL: " FOR UPDATE".
    fn for_update(&self) -> &'static str;

    /// INSERT-or-ignore syntax.
    fn insert_or_ignore(&self, table: &str, columns: &str, values: &str) -> String;

    /// Whether existence checks use COUNT(*) (true) or SELECT EXISTS (false).
    fn existence_check_is_count(&self) -> bool;

    /// Greatest of two expressions. SQLite: MAX(a, b), PG/MySQL: GREATEST(a, b).
    #[cfg(feature = "write")]
    fn greatest(&self, a: &str, b: &str) -> String;
}

// --- SQLite ---

pub(crate) struct SqliteDialect;

impl SqlDialect for SqliteDialect {
    fn ph(&self, _n: usize) -> String {
        "?".to_string()
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
}

// --- PostgreSQL ---

pub(crate) struct PostgresDialect;

impl SqlDialect for PostgresDialect {
    fn ph(&self, n: usize) -> String {
        format!("${n}")
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
}

// --- MySQL ---

pub(crate) struct MySqlDialect;

impl SqlDialect for MySqlDialect {
    fn ph(&self, _n: usize) -> String {
        "?".to_string()
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
}
