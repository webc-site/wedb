//! 范围索引迁移支持：源侧发现 / 快照读取器 / 发布前置门
//! （对标 libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs）
//!
//! C# 的发布本体 PublishMigratedIndex（文件换入 + 引擎恢复 + 存根落盘）在
//! Rust 侧由 wkv `StoreSession::publish_migrated_range_index` 承接（见
//! embed/wkv/src/range_index.rs）；本域负责会话侧编排：迁移键发现、持锁
//! 快照出迁移读取器、以及任意类型键存在性判定。

use std::{
  collections::HashSet,
  fmt::{self, Display, Formatter},
  fs, io,
  path::PathBuf,
};

use wdev::Device;
use wkv::{
  RangeIndexChunkedSerializer, RangeIndexError, RangeIndexManager, RangeIndexMigrationReader,
  StorageBackendType, StoreSession,
};

/// 迁移编排错误（C# 以异常上抛的会话侧失败）
#[derive(Debug, thiserror::Error)]
pub enum MigrationError {
  /// 键或快照源不满足迁移前置条件（C# InvalidOperationException / 状态检查失败）
  #[error("ERR {0}")]
  Invalid(String),
  /// 存储面操作失败（RI 存根读取 / 发布）
  #[error(transparent)]
  Range(#[from] RangeIndexError),
  /// 文件 I/O 失败
  #[error(transparent)]
  Io(#[from] io::Error),
  /// 主存读取失败
  #[error(transparent)]
  Store(#[from] wkv::Error),
}

/// libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:PublishMigratedIndexResult
///
/// 迁移发布结果（活动追踪与 AOF 流重放路径共用）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PublishMigratedIndexResult {
  /// 迁移索引发布成功
  Success,
  /// 同名键已存在且未指定 MIGRATE REPLACE；未做破坏性动作
  SkippedAlreadyExists,
  /// 同名键已存在且指定了 MIGRATE REPLACE，但 RI 键替换尚不支持；未做破坏性动作
  SkippedReplaceNotSupported,
  /// 发布失败（异常或存储层错误，已记日志）
  Failed,
}

impl Display for PublishMigratedIndexResult {
  fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
    match self {
      Self::Success => write!(f, "Success"),
      Self::SkippedAlreadyExists => write!(f, "SkippedAlreadyExists"),
      Self::SkippedReplaceNotSupported => write!(f, "SkippedReplaceNotSupported"),
      Self::Failed => write!(f, "Failed"),
    }
  }
}

/// libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:DefaultMigrationChunkSize
///
/// 迁移流式传输的默认分块大小（256KB）
pub const DEFAULT_MIGRATION_CHUNK_SIZE: usize = 256 * 1024;

/// 迁移读取器默认文件读缓冲（1MiB）
///
/// libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs:DefaultFileReadBufferSize
pub const DEFAULT_FILE_READ_BUFFER_SIZE: usize = 1 << 20;

/// 迁移编排面（C# partial RangeIndexManager 的 Migration 分片）
pub struct RangeIndexManager_Migration;

impl RangeIndexManager_Migration {
  /// libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:GetRangeIndexKeysForMigration
  ///
  /// 逐键经存根读取发现范围索引键（CLUSTER MIGRATE ... KEYS 路径）：
  /// 存根命中 → RI 键；NOTFOUND / WRONGTYPE → 跳过；其余错误上抛
  /// （对标 C# 非 OK 状态抛 GarnetException）。存根字节不在此捕获——
  /// 权威存根由快照阶段在独占锁下重读，规避 TOCTOU
  pub async fn get_range_index_keys_for_migration<D: Device>(
    session: &StoreSession<D>,
    keys: &[Vec<u8>],
  ) -> Result<HashSet<Vec<u8>>, MigrationError> {
    let mut range_index_keys = HashSet::new();
    for key in keys {
      match session.load_range_index_stub(key).await {
        Ok(Some(_)) => {
          range_index_keys.insert(key.clone());
        }
        Ok(None) | Err(RangeIndexError::NotFound | RangeIndexError::WrongType) => {
          // 不存在（NOTFOUND）或非 RI 类型键（WRONGTYPE）：不是 RI 键，跳过
        }
        Err(e) => return Err(e.into()),
      }
    }
    Ok(range_index_keys)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:SnapshotRangeIndexAndCreateReader
  ///
  /// 源侧：持键条带独占锁重读权威存根，把 BfTree 快照至迁移临时文件，
  /// 产出分块迁移读取器（读取器拥有快照文件，dispose 时删除，防止源侧
  /// 迁移快照残留）。C# 经 LocalServerSession 转发；Rust 直接以存储会话
  /// 读存根后走引擎锁与快照原语
  pub async fn snapshot_range_index_and_create_reader<D: Device>(
    session: &StoreSession<D>,
    key: &[u8],
  ) -> Result<RangeIndexMigrationReader<fs::File>, MigrationError> {
    let (_, stub) = session
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    let engine = &session.store.range_index;
    if stub.storage_backend == StorageBackendType::Memory.to_u8() {
      return Err(MigrationError::Invalid(
        "SnapshotForMigration: memory-only trees cannot be migrated".to_string(),
      ));
    }
    // 独占锁下重读的权威存根即迁移载荷（C# stubSpan.ToArray()）
    let stub_bytes = stub.encode();
    let migration_path = engine.derive_temp_migration_path();

    let key_hash = RangeIndexManager::key_hash_of(key);
    let key_id = RangeIndexManager::key_id_of(key);
    // 以下全为同步段（快照 / 拷贝 / 开文件均不 await），条带锁不跨 .await
    let _xlock = engine.locks().write(key_hash);
    match engine.get_tree(key) {
      Some(tree) => {
        // 树在线：per-tree 快照防重入 claim 下 CPR 快照直出迁移目标
        //（与并发检查点 / 刷盘快照在同一树上串行化）
        let entry = engine
          .live_indexes()
          .pin()
          .get(&key_id)
          .cloned()
          .ok_or_else(|| MigrationError::Invalid("live entry vanished".to_string()))?;
        entry
          .snapshot_under_claim(&tree, &migration_path)
          .map_err(|e| MigrationError::Invalid(e.to_string()))?;
      }
      None => {
        // 树已驱逐：拷贝工作文件 data.bftree（与检查点 pending 条目同源）
        let data_path = engine.data_file_path_for_key(key);
        if !data_path.exists() {
          return Err(MigrationError::Invalid(format!(
            "SnapshotForMigration: data.bftree not found: {}",
            data_path.display()
          )));
        }
        fs::copy(&data_path, &migration_path)?;
      }
    }
    drop(_xlock);

    let total_bytes = fs::metadata(&migration_path)?.len();
    log::info!(
      "SnapshotForMigration: snapshot file {}, size {total_bytes} bytes",
      migration_path.display()
    );
    let serializer = RangeIndexChunkedSerializer::new(key, &stub_bytes, total_bytes);
    let file = fs::File::open(&migration_path)?;
    let reader = RangeIndexMigrationReader::new(
      serializer,
      file,
      Some(migration_path),
      DEFAULT_FILE_READ_BUFFER_SIZE,
    )
    .map_err(|e| MigrationError::Invalid(e.to_string()))?;
    Ok(reader)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:KeyExists
  ///
  /// 判定本节点上该键名是否已存在任意类型键（字符串 / RI / 对象等）——
  /// 发布前置门：绝不用迁移索引覆盖既有键。字符串键经 `read` 命中
  /// （C# status.Found）；类型键经元记录命中（C# GET 拒收类型记录的
  /// IsWrongType 口径）
  pub async fn key_exists<D: Device>(
    session: &StoreSession<D>,
    key: &[u8],
  ) -> Result<bool, wkv::Error> {
    if session.read(key).await?.is_some() {
      return Ok(true);
    }
    Ok(
      session
        .load_meta(key)
        .await?
        .is_some_and(|meta| meta.size > 0),
    )
  }

  /// 推导进行中入站迁移的临时文件路径 {ri_log_root}/migration-tmp/{id}.bftree
  ///
  /// libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:DeriveTempMigrationPath
  pub fn derive_temp_migration_path(engine: &RangeIndexManager) -> PathBuf {
    engine.derive_temp_migration_path()
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn publish_result_display_matches_csharp_names() {
    // 活动日志按 C# 枚举名输出
    assert_eq!(PublishMigratedIndexResult::Success.to_string(), "Success");
    assert_eq!(
      PublishMigratedIndexResult::SkippedAlreadyExists.to_string(),
      "SkippedAlreadyExists"
    );
    assert_eq!(
      PublishMigratedIndexResult::SkippedReplaceNotSupported.to_string(),
      "SkippedReplaceNotSupported"
    );
    assert_eq!(PublishMigratedIndexResult::Failed.to_string(), "Failed");
  }

  #[test]
  fn constants_match_csharp_values() {
    // C# DefaultMigrationChunkSize = 256KB；DefaultFileReadBufferSize = 1MiB
    assert_eq!(DEFAULT_MIGRATION_CHUNK_SIZE, 256 * 1024);
    assert_eq!(DEFAULT_FILE_READ_BUFFER_SIZE, 1 << 20);
  }
}
