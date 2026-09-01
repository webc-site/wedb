use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::fs::create_dir_all;
use std::path::Path;
use std::sync::Arc;

use fjall::config::{BlockSizePolicy, CompressionPolicy};
use fjall::{CompressionType, Database, Keyspace, KeyspaceCreateOptions, PersistMode};

use crate::error::{Error, Result};

#[derive(Clone)]
pub struct FjallEngine {
  db: Arc<Database>,
}

impl Debug for FjallEngine {
  fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
    f.debug_struct("FjallEngine").finish()
  }
}

impl FjallEngine {
  pub fn new(data_path: impl AsRef<Path>, keyspace_li: Vec<String>) -> Result<Self> {
    let path = data_path.as_ref();
    if let Some(parent) = path.parent() {
      create_dir_all(parent)?;
    }

    let mut builder = Database::builder(path);
    let comp_type = CompressionType::None;
    let data_comp_policy = CompressionPolicy::new([
      CompressionType::None,
      CompressionType::None,
      CompressionType::Lz4,
    ]);

    builder = builder.journal_compression(comp_type);

    let db = builder
      .open()
      .map_err(|e| Error::internal_with_source(format!("Failed to open Fjall at {path:?}"), e))?;

    for ks_name in keyspace_li {
      let data_policy = data_comp_policy.clone();
      let opts = match ks_name.as_str() {
        "_log_meta" | "_sm_meta" => KeyspaceCreateOptions::default()
          .data_block_size_policy(BlockSizePolicy::all(4 * 1024))
          .data_block_compression_policy(data_policy),
        "_log_data" => KeyspaceCreateOptions::default()
          .data_block_size_policy(BlockSizePolicy::all(64 * 1024))
          .data_block_compression_policy(data_policy),
        _ => KeyspaceCreateOptions::default()
          .data_block_size_policy(BlockSizePolicy::all(16 * 1024))
          .data_block_compression_policy(data_policy),
      };
      db.keyspace(&ks_name, move || opts).map_err(|e| {
        Error::internal_with_source(format!("Failed to open keyspace '{ks_name}'"), e)
      })?;
    }

    Ok(Self { db: Arc::new(db) })
  }

  pub fn db(&self) -> &Arc<Database> {
    &self.db
  }

  pub fn keyspace(&self, name: &str) -> Result<Keyspace> {
    self
      .db
      .keyspace(name, KeyspaceCreateOptions::default)
      .map_err(|e| Error::internal_with_source(format!("Failed to get keyspace '{name}'"), e))
  }

  pub fn persist(&self) -> Result<()> {
    self
      .db
      .persist(PersistMode::SyncAll)
      .map_err(|e| Error::internal_with_source("Failed to persist database", e))
  }
}
