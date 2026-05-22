//! Session-level configuration extension for DuckLake DML execution.
//!
//! Exposes tunables that the DML execs (currently UPDATE; MERGE/DELETE may opt
//! in later) read at execute time from
//! `TaskContext::session_config().options().extensions`.
//!
//! Today the only knob is [`DuckLakeConfig::max_buffered_rows_per_dml`], a
//! safety valve against unbounded in-memory buffering of `RecordBatch`es
//! during an UPDATE/MERGE that rewrites many rows. The default (10M) matches
//! the prior hard-coded constant in `update_exec.rs` so existing behaviour is
//! preserved; users running large legitimate UPDATEs can raise it via the
//! standard datafusion config mechanism:
//!
//! ```no_run
//! # #[cfg(feature = "write")]
//! # fn example() -> datafusion::error::Result<()> {
//! use datafusion::prelude::SessionContext;
//! use datafusion_ducklake::config::DuckLakeConfig;
//!
//! let cfg = DuckLakeConfig {
//!     max_buffered_rows_per_dml: 100_000_000,
//! };
//! let mut options = datafusion::common::config::ConfigOptions::default();
//! options.extensions.insert(cfg);
//! let session_config =
//!     datafusion::execution::config::SessionConfig::from(options);
//! let _ctx = SessionContext::new_with_config(session_config);
//! # Ok(())
//! # }
//! ```

use std::any::Any;
use std::fmt;

use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::error::{DataFusionError, Result};

/// Session-level configuration for DuckLake DML execution.
#[derive(Debug, Clone)]
pub struct DuckLakeConfig {
    /// Maximum number of rows a single DML exec may buffer in memory across
    /// all matched data files before erroring with
    /// `DataFusionError::ResourcesExhausted`.
    ///
    /// Defaults to `10_000_000`. Raise this for legitimate large UPDATEs;
    /// lower it to be stricter on memory in shared environments.
    ///
    /// The buffer this bounds is the `Vec<RecordBatch>` of *updated rows*
    /// (the SET-applied replacement data that will become the new data file).
    /// It is NOT a bound on the position list used by DELETE, which is a much
    /// smaller `Vec<i64>` per file (~8 B/row).
    pub max_buffered_rows_per_dml: usize,
}

impl Default for DuckLakeConfig {
    fn default() -> Self {
        Self {
            max_buffered_rows_per_dml: 10_000_000,
        }
    }
}

impl ConfigExtension for DuckLakeConfig {
    const PREFIX: &'static str = "ducklake";
}

impl ExtensionOptions for DuckLakeConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> Result<()> {
        match key {
            "max_buffered_rows_per_dml" => {
                self.max_buffered_rows_per_dml = value.parse::<usize>().map_err(|e| {
                    DataFusionError::Configuration(format!(
                        "Invalid value for ducklake.max_buffered_rows_per_dml '{value}': {e}"
                    ))
                })?;
                Ok(())
            },
            other => Err(DataFusionError::Configuration(format!(
                "Unknown ducklake config key: '{other}'"
            ))),
        }
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![ConfigEntry {
            key: format!("{}.max_buffered_rows_per_dml", Self::PREFIX),
            value: Some(self.max_buffered_rows_per_dml.to_string()),
            description: "Maximum number of rows a single DML exec may buffer \
                          in memory before erroring (default: 10_000_000).",
        }]
    }
}

impl fmt::Display for DuckLakeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DuckLakeConfig {{ max_buffered_rows_per_dml: {} }}",
            self.max_buffered_rows_per_dml
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::config::ConfigOptions;

    #[test]
    fn default_value_matches_legacy_constant() {
        let cfg = DuckLakeConfig::default();
        assert_eq!(cfg.max_buffered_rows_per_dml, 10_000_000);
    }

    #[test]
    fn can_register_and_retrieve_from_config_options() {
        let mut opts = ConfigOptions::default();
        opts.extensions.insert(DuckLakeConfig {
            max_buffered_rows_per_dml: 42,
        });
        let got = opts.extensions.get::<DuckLakeConfig>().unwrap();
        assert_eq!(got.max_buffered_rows_per_dml, 42);
    }

    #[test]
    fn set_parses_via_string_path() {
        let mut opts = ConfigOptions::default();
        opts.extensions.insert(DuckLakeConfig::default());
        opts.set("ducklake.max_buffered_rows_per_dml", "5000").unwrap();
        let got = opts.extensions.get::<DuckLakeConfig>().unwrap();
        assert_eq!(got.max_buffered_rows_per_dml, 5000);
    }

    #[test]
    fn set_rejects_unknown_key() {
        let mut opts = ConfigOptions::default();
        opts.extensions.insert(DuckLakeConfig::default());
        let err = opts.set("ducklake.nope", "1").unwrap_err();
        assert!(err.to_string().contains("nope"));
    }

    #[test]
    fn set_rejects_non_numeric_value() {
        let mut opts = ConfigOptions::default();
        opts.extensions.insert(DuckLakeConfig::default());
        let err = opts
            .set("ducklake.max_buffered_rows_per_dml", "not-a-number")
            .unwrap_err();
        assert!(err.to_string().to_lowercase().contains("invalid"));
    }
}
