pub mod key;
pub mod log_store;
pub mod meta;
pub mod snapshot;
pub mod statemachine;

use std::io::Result as IoResult;
use std::path::PathBuf;
use std::sync::Arc;

pub use log_store::FjallLogStore;
pub use meta::StoreMeta;
pub use statemachine::FjallStateMachine;

use self::key::{LOG_DATA_FAMILY, LOG_META_FAMILY, SM_DATA_FAMILY, SM_META_FAMILY};
use crate::engine::FjallEngine;
use crate::error::Result;
use crate::types::{TypeConfig, read_logs_err};

pub fn create_storage_engine(data_dir: &str) -> Result<FjallEngine> {
  let db_path = PathBuf::from(data_dir).join("data");
  let keyspace_li = vec![
    LOG_META_FAMILY.to_string(),
    LOG_DATA_FAMILY.to_string(),
    SM_META_FAMILY.to_string(),
    SM_DATA_FAMILY.to_string(),
  ];
  FjallEngine::new(db_path, keyspace_li)
}

pub async fn create_stores(
  engine: &Arc<FjallEngine>,
  data_dir: PathBuf,
) -> Result<(FjallLogStore<TypeConfig>, FjallStateMachine)> {
  let log_store = FjallLogStore::create(engine.clone())?;
  let state_machine = FjallStateMachine::new(engine.clone(), data_dir).await?;
  Ok((log_store, state_machine))
}

/// 分批清理指定的迭代器中的键（每 batch_size 条提交一次，防止大数据量下单 batch 过大）
pub fn batch_remove_guards<I>(
  db: &fjall::Database,
  keyspace: &fjall::Keyspace,
  iter: I,
  batch_size: usize,
) -> IoResult<usize>
where
  I: IntoIterator<Item = fjall::Guard>,
{
  let mut batch = db.batch();
  let mut count = 0;
  for g in iter {
    let (key, _) = g.into_inner().map_err(read_logs_err)?;
    batch.remove(keyspace, key);
    count += 1;
    if count % batch_size == 0 {
      batch.commit().map_err(read_logs_err)?;
      batch = db.batch();
    }
  }
  if count > 0 && count % batch_size != 0 {
    batch.commit().map_err(read_logs_err)?;
  }
  Ok(count)
}
