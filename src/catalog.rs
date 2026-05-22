//! DuckLake catalog provider implementation

use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use crate::Result;
use crate::information_schema::InformationSchemaProvider;
use crate::metadata_provider::MetadataProvider;
use crate::path_resolver::{parse_object_store_url, resolve_path};
use crate::schema::DuckLakeSchema;
use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::datasource::object_store::ObjectStoreUrl;

#[cfg(feature = "write")]
use crate::metadata_writer::MetadataWriter;
#[cfg(feature = "write")]
use datafusion::error::{DataFusionError, Result as DataFusionResult};

/// Configuration for write operations (when write feature is enabled)
#[cfg(feature = "write")]
#[derive(Clone)]
struct WriteConfig {
    /// Metadata writer for catalog operations
    writer: Arc<dyn MetadataWriter>,
    /// Object store for CTAS writes. If None, defaults to LocalFileSystem.
    object_store: Option<Arc<dyn object_store::ObjectStore>>,
}

#[cfg(feature = "write")]
impl std::fmt::Debug for WriteConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteConfig")
            .field("writer", &self.writer)
            .field("has_object_store", &self.object_store.is_some())
            .finish()
    }
}

/// DuckLake catalog provider
///
/// Connects to a DuckLake catalog database and provides access to schemas and tables.
/// Uses dynamic metadata lookup - schemas are queried on-demand from the catalog database.
/// Bound to a specific snapshot ID for query consistency.
#[derive(Debug)]
pub struct DuckLakeCatalog {
    /// Metadata provider for querying catalog
    provider: Arc<dyn MetadataProvider>,
    /// Snapshot ID this catalog is bound to.
    /// Uses AtomicI64 so write operations (register_schema, deregister_schema, etc.)
    /// can update it after creating new snapshots.
    snapshot_id: Arc<AtomicI64>,
    /// Object store URL for resolving file paths (e.g., s3://bucket/ or file:///)
    object_store_url: Arc<ObjectStoreUrl>,
    /// Catalog base path component for resolving relative schema paths (e.g., /prefix/)
    catalog_path: String,
    /// Whether to expose the `rowid` virtual column on tables in this catalog.
    ///
    /// TODO(#22): the actual scan-path wiring is intentionally not present yet —
    /// it conflicts with the fork's `virtual_column_exec` design and is tracked
    /// in #22. This field is plumbed through the constructor so existing callers
    /// (and the `compare_rowid_against_duckdb` / `rowid_lifecycle` examples)
    /// continue to compile.
    row_lineage: bool,
    /// Write configuration (when write feature is enabled)
    #[cfg(feature = "write")]
    write_config: Option<WriteConfig>,
}

impl DuckLakeCatalog {
    /// Create a new DuckLake catalog with a metadata provider
    ///
    /// Gets the current snapshot ID at creation time and binds the catalog to it.
    /// For backward compatibility. For explicit snapshot control, use `with_snapshot()`.
    pub fn new(provider: impl MetadataProvider + 'static) -> Result<Self> {
        let provider = Arc::new(provider) as Arc<dyn MetadataProvider>;
        let snapshot_id = provider.get_current_snapshot()?;
        let data_path = provider.get_data_path()?;
        let (object_store_url, catalog_path) = parse_object_store_url(&data_path)?;

        Ok(Self {
            provider,
            snapshot_id: Arc::new(AtomicI64::new(snapshot_id)),
            object_store_url: Arc::new(object_store_url),
            catalog_path,
            row_lineage: false,
            #[cfg(feature = "write")]
            write_config: None,
        })
    }

    /// Create a catalog bound to a specific snapshot ID
    ///
    /// All schemas and tables returned will use this snapshot, guaranteeing
    /// query consistency even if multiple catalog/schema/table lookups occur
    /// during query planning.
    pub fn with_snapshot(provider: Arc<dyn MetadataProvider>, snapshot_id: i64) -> Result<Self> {
        let data_path = provider.get_data_path()?;
        let (object_store_url, catalog_path) = parse_object_store_url(&data_path)?;

        Ok(Self {
            provider,
            snapshot_id: Arc::new(AtomicI64::new(snapshot_id)),
            object_store_url: Arc::new(object_store_url),
            catalog_path,
            row_lineage: false,
            #[cfg(feature = "write")]
            write_config: None,
        })
    }

    /// Create a catalog with write support.
    ///
    /// This constructor enables write operations (INSERT INTO, CREATE TABLE AS)
    /// by attaching a metadata writer. The catalog will pass the writer to all
    /// schemas and tables it creates.
    ///
    /// # Arguments
    /// * `provider` - Metadata provider for reading catalog metadata
    /// * `writer` - Metadata writer for write operations
    ///
    /// # Example
    /// ```no_run
    /// # async fn example() -> datafusion_ducklake::Result<()> {
    /// use datafusion_ducklake::{DuckLakeCatalog, SqliteMetadataProvider, SqliteMetadataWriter};
    /// use std::sync::Arc;
    ///
    /// let provider = SqliteMetadataProvider::new("sqlite:catalog.db?mode=rwc").await?;
    /// let writer = SqliteMetadataWriter::new("sqlite:catalog.db?mode=rwc").await?;
    ///
    /// let catalog = DuckLakeCatalog::with_writer(Arc::new(provider), Arc::new(writer))?;
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "write")]
    pub fn with_writer(
        provider: Arc<dyn MetadataProvider>,
        writer: Arc<dyn MetadataWriter>,
    ) -> Result<Self> {
        let snapshot_id = provider.get_current_snapshot()?;
        let data_path_str = provider.get_data_path()?;
        let (object_store_url, catalog_path) = parse_object_store_url(&data_path_str)?;

        Ok(Self {
            provider,
            snapshot_id: Arc::new(AtomicI64::new(snapshot_id)),
            object_store_url: Arc::new(object_store_url),
            catalog_path,
            row_lineage: false,
            write_config: Some(WriteConfig {
                writer,
                object_store: None,
            }),
        })
    }

    /// Create a catalog with write support and an explicit object store.
    ///
    /// Like `with_writer()`, but also sets the object store used for CTAS
    /// writes. This is necessary for S3/MinIO/GCS catalogs where data must
    /// be written to the configured object store rather than local disk.
    #[cfg(feature = "write")]
    pub fn with_writer_and_object_store(
        provider: Arc<dyn MetadataProvider>,
        writer: Arc<dyn MetadataWriter>,
        object_store: Arc<dyn object_store::ObjectStore>,
    ) -> Result<Self> {
        let snapshot_id = provider.get_current_snapshot()?;
        let data_path_str = provider.get_data_path()?;
        let (object_store_url, catalog_path) = parse_object_store_url(&data_path_str)?;

        Ok(Self {
            provider,
            snapshot_id: Arc::new(AtomicI64::new(snapshot_id)),
            object_store_url: Arc::new(object_store_url),
            catalog_path,
            row_lineage: false,
            write_config: Some(WriteConfig {
                writer,
                object_store: Some(object_store),
            }),
        })
    }

    /// Enable or disable the `rowid` virtual column for tables in this catalog.
    ///
    /// TODO(#22): the rowid scan-path integration is being reconciled with the
    /// fork's `virtual_column_exec` design in #22. For now this just records
    /// the preference on the catalog so that existing call sites (and the
    /// rowid example binaries) continue to type-check.
    pub fn with_row_lineage(mut self, enabled: bool) -> Self {
        self.row_lineage = enabled;
        self
    }

    /// Get the metadata provider for this catalog
    ///
    /// This is useful when you need to register table functions separately.
    pub fn provider(&self) -> Arc<dyn MetadataProvider> {
        self.provider.clone()
    }

    /// Get the pinned snapshot ID for this catalog.
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id.load(Ordering::Acquire)
    }
}

impl CatalogProvider for DuckLakeCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Deregister (drop) a schema from this catalog.
    ///
    /// If `cascade` is false, fails if the schema contains active tables.
    /// If `cascade` is true, drops all tables in the schema first, then drops the schema.
    /// Returns the dropped schema provider, or None if the schema doesn't exist.
    #[cfg(feature = "write")]
    fn deregister_schema(
        &self,
        name: &str,
        cascade: bool,
    ) -> DataFusionResult<Option<Arc<dyn SchemaProvider>>> {
        let config = self.write_config.as_ref().ok_or_else(|| {
            DataFusionError::Plan(
                "Catalog is read-only. Use DuckLakeCatalog::with_writer() to enable writes."
                    .to_string(),
            )
        })?;

        // Cannot drop information_schema
        if name == "information_schema" {
            return Err(DataFusionError::Plan(
                "Cannot drop information_schema".to_string(),
            ));
        }

        // Look up the schema
        let current_snapshot = self.snapshot_id.load(Ordering::Acquire);
        let schema_meta = self
            .provider
            .get_schema_by_name(name, current_snapshot)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let Some(meta) = schema_meta else {
            // Schema doesn't exist - return None (DataFusion handles IF EXISTS)
            return Ok(None);
        };

        // Check for active tables
        let active_table_ids = config
            .writer
            .list_active_table_ids(meta.schema_id)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        if !active_table_ids.is_empty() && !cascade {
            return Err(DataFusionError::Plan(format!(
                "Cannot drop schema \"{}\" because there are entries that depend on it. Use DROP...CASCADE to drop all dependents.",
                name
            )));
        }

        // Drop the schema (cascade is handled atomically inside drop_schema_inner,
        // which ends all tables, columns, data files, and delete files in one transaction)
        let new_snapshot = config
            .writer
            .drop_schema(meta.schema_id)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Update snapshot so subsequent lookups see the change
        self.snapshot_id.fetch_max(new_snapshot, Ordering::Release);

        // Return the schema provider that was dropped
        let schema_path = resolve_path(&self.catalog_path, &meta.path, meta.path_is_relative)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let schema = DuckLakeSchema::new(
            meta.schema_id,
            meta.schema_name,
            Arc::clone(&self.provider),
            new_snapshot,
            self.object_store_url.clone(),
            schema_path,
        );

        Ok(Some(Arc::new(schema) as Arc<dyn SchemaProvider>))
    }

    /// Register (create) a new schema in this catalog.
    ///
    /// Called by DataFusion for CREATE SCHEMA statements.
    /// Creates the schema in DuckLake metadata via MetadataWriter.
    /// The passed-in schema provider is ignored; a DuckLakeSchema is created instead.
    #[cfg(feature = "write")]
    fn register_schema(
        &self,
        name: &str,
        _schema: Arc<dyn SchemaProvider>,
    ) -> DataFusionResult<Option<Arc<dyn SchemaProvider>>> {
        let config = self.write_config.as_ref().ok_or_else(|| {
            DataFusionError::Plan(
                "Catalog is read-only. Use DuckLakeCatalog::with_writer() to enable writes."
                    .to_string(),
            )
        })?;

        // Validate schema name to prevent path traversal attacks
        crate::schema::validate_schema_name(name)?;

        // Cannot create information_schema
        if name == "information_schema" {
            return Err(DataFusionError::Plan(
                "Cannot create schema 'information_schema': reserved name".to_string(),
            ));
        }

        // Create snapshot and schema in metadata
        let new_snapshot = config
            .writer
            .create_snapshot()
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let (schema_id, _was_created) = config
            .writer
            .get_or_create_schema(name, None, new_snapshot)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        // Update snapshot so subsequent lookups see the new schema
        self.snapshot_id.fetch_max(new_snapshot, Ordering::Release);

        // Build the schema provider
        let schema_path = resolve_path(&self.catalog_path, name, true)
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let mut schema = DuckLakeSchema::new(
            schema_id,
            name,
            Arc::clone(&self.provider),
            new_snapshot,
            self.object_store_url.clone(),
            schema_path,
        )
        .with_writer(Arc::clone(&config.writer))
        .with_catalog_snapshot_id(Arc::clone(&self.snapshot_id));

        if let Some(ref store) = config.object_store {
            schema = schema.with_object_store(Arc::clone(store));
        }

        Ok(Some(Arc::new(schema) as Arc<dyn SchemaProvider>))
    }

    fn schema_names(&self) -> Vec<String> {
        // Start with information_schema
        let mut names = vec!["information_schema".to_string()];

        let snapshot_id = self.snapshot_id.load(Ordering::Acquire);

        // Add data schemas from catalog using the current snapshot_id.
        // Note: DataFusion's CatalogProvider trait returns Vec<String> (not Result),
        // so metadata errors must be logged and swallowed here (R5-S-059).
        let data_schemas = self
            .provider
            .list_schemas(snapshot_id)
            .inspect_err(|e| {
                tracing::error!(
                    error = %e,
                    snapshot_id = %snapshot_id,
                    "Failed to list schemas from catalog"
                )
            })
            .unwrap_or_default()
            .into_iter()
            .map(|s| s.schema_name);

        names.extend(data_schemas);

        // Ensure deterministic order and no duplicates
        names.sort();
        names.dedup();

        names
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let snapshot_id = self.snapshot_id.load(Ordering::Acquire);

        // Handle information_schema specially
        if name == "information_schema" {
            return Some(Arc::new(InformationSchemaProvider::new(
                Arc::clone(&self.provider),
                snapshot_id,
            )));
        }

        // Query database with the current snapshot_id for data schemas
        match self.provider.get_schema_by_name(name, snapshot_id) {
            Ok(Some(meta)) => {
                // Resolve schema path hierarchically using path_resolver utility
                let schema_path =
                    match resolve_path(&self.catalog_path, &meta.path, meta.path_is_relative) {
                        Ok(p) => p,
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                schema_name = %name,
                                "Failed to resolve schema path"
                            );
                            return None;
                        },
                    };

                // Pass the current snapshot_id to schema
                let schema = DuckLakeSchema::new(
                    meta.schema_id,
                    meta.schema_name,
                    Arc::clone(&self.provider),
                    snapshot_id,
                    self.object_store_url.clone(),
                    schema_path,
                );

                // Configure writer and object store if this catalog is writable
                #[cfg(feature = "write")]
                let schema = if let Some(ref config) = self.write_config {
                    let s = schema
                        .with_writer(Arc::clone(&config.writer))
                        .with_catalog_snapshot_id(Arc::clone(&self.snapshot_id));
                    if let Some(ref store) = config.object_store {
                        s.with_object_store(Arc::clone(store))
                    } else {
                        s
                    }
                } else {
                    schema
                };

                Some(Arc::new(schema) as Arc<dyn SchemaProvider>)
            },
            Ok(None) => None,
            Err(e) => {
                tracing::error!(
                    error = %e,
                    schema_name = %name,
                    "Failed to query schema from metadata provider"
                );
                None
            },
        }
    }
}
