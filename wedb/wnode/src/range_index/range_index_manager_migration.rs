//! 范围索引与升阶分层集合的迁移支持：源侧发现 / 快照读取器 / 发布前置门
//! （对标 libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs）
//!
//! C# 的发布本体 PublishMigratedIndex（文件换入 + 引擎恢复 + 存根落盘）在
//! Rust 侧由 wkv `StoreSession::publish_migrated_range_index` 承接（见
//! wedb/wkv/src/range_index.rs）；本域负责会话侧编排：迁移键发现、持锁
//! 快照出迁移读取器、以及任意类型键存在性判定。
//!
//! rust 分层扩展面：升阶集合（Hash/Set/List/SortedSet 入驻 Meta 域 + wbftree
//! 树）与 RangeIndex 共用同一引擎与同一物理域，树快照与判别类型无关，
//! 故发现与快照出口一处通用（判据 `load_collection_stub`），发布判别类型与
//! 成员 TTL 水位、键级 TTL 以流元 [`TreeStreamMeta`] 随带外流帧透传。

use std::{
  fmt::{self, Display, Formatter},
  fs, io,
  path::Path,
};

use wbase::{convert::unix_time_in_milliseconds_from_ticks, map::HashSet, time::now_ticks};
use wbftree::{
  DEFAULT_FILE_READ_BUFFER_SIZE, ERR_MEMORY_TREE_MIGRATION, RangeIndexChunkedSerializer,
  RangeIndexManager, RangeIndexMigrationReader, StorageBackendType,
};
use wdev::Device;
use wkv::{Error, RangeIndexError, StoreSession};
use wval::GarnetObjectType;

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

/// wbftree 带外流的流元（发布形态三标量，源端快照单点派生、接收端首块捕获）：
/// RangeIndex 与升阶分层集合共用同一带外帧通道，只差本三元组
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TreeStreamMeta {
  /// 发布后元记录的集合判别类型
  pub obj_type: GarnetObjectType,
  /// 成员 TTL 水位（.NET Ticks；i64::MAX = 无成员挂 TTL，纯 RangeIndex 流恒此值）
  pub next_expiry: i64,
  /// 键级绝对过期 Unix 毫秒（0 = 无 TTL）
  pub expire_unix_ms: i64,
}

impl TreeStreamMeta {
  /// 判别类型是否为带外树流合法类型（RangeIndex 与升阶四族；接收端首块
  /// 校验单点，越界即协议违约，绝不静默归一为 RangeIndex）
  #[inline]
  pub fn obj_type_valid(obj_type: GarnetObjectType) -> bool {
    matches!(
      obj_type,
      GarnetObjectType::RangeIndex
        | GarnetObjectType::Hash
        | GarnetObjectType::Set
        | GarnetObjectType::List
        | GarnetObjectType::SortedSet
    )
  }
}

/// 迁移编排面（C# partial RangeIndexManager 的 Migration 分片）
pub struct RangeIndexManagerMigration;

impl RangeIndexManagerMigration {
  /// libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:GetRangeIndexKeysForMigration
  ///
  /// 逐键经存根读取发现 wbftree 带外流键（CLUSTER MIGRATE ... KEYS 路径）：
  /// rust 分层扩展下覆盖面为全部驻留 Meta 域 + 树的键（RangeIndex 与升阶的
  /// Hash/Set/List/SortedSet），存根命中 → 带外流键；键不存在 / 死元记录 /
  /// 非树类型 → 跳过；同键换入封窗（MigrationBusy，并发升阶/重灌/RENAME 在途）
  /// 本轮跳过、留待驱动下一轮重扫。存根字节不在此捕获——权威存根由快照阶段
  /// 在独占锁下重读，规避 TOCTOU
  pub async fn get_range_index_keys_for_migration<D: Device>(
    session: &StoreSession<D>,
    keys: &[Vec<u8>],
  ) -> Result<HashSet<Vec<u8>>, MigrationError> {
    let mut tree_stream_keys = HashSet::default();
    for key in keys {
      match session.load_collection_stub(key).await {
        Ok(Some(_)) => {
          tree_stream_keys.insert(key.clone());
        }
        // 不存在 / 死元记录 / 非分层非索引键：不是带外流键，跳过；
        // MigrationBusy = 同键换入封窗在途，本轮跳过（读写一致拒绝，重扫收敛）
        Ok(None) | Err(Error::MigrationBusy) => {}
        Err(e) => return Err(MigrationError::Store(e)),
      }
    }
    Ok(tree_stream_keys)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:SnapshotRangeIndexAndCreateReader
  ///
  /// libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:SnapshotForMigration
  /// 合并承接：C# 快照编排器内部转调 SnapshotForMigration（独占锁下重读存根、
  /// 在线树 CPR 快照直出迁移临时文件 / 已驱逐树拷贝 data.bftree），rust 两层
  /// 合一为本函数体。
  ///
  /// 源侧：持键条带独占锁重读权威存根，把 BfTree 快照至迁移临时文件，
  /// 产出分块迁移读取器（读取器拥有快照文件，dispose 时删除，防止源侧
  /// 迁移快照残留）。C# 经 LocalServerSession 转发；Rust 直接以存储会话
  /// 读存根后走引擎锁与快照原语。rust 分层扩展：装载判据为
  /// `load_collection_stub`（RangeIndex 与升阶四族通用，树快照与判别类型
  /// 无关），同键换入封窗（MigrationBusy）显式失败上抛，交传输门面收敛
  /// 为判败；随产物返回流元 [`TreeStreamMeta`]（判别类型与成员 TTL 水位取
  /// 权威 MetaValue，键级 TTL 取 TTL 域记录换算 Unix 毫秒，无 TTL 传 0），
  /// 供带外帧逐块携载、接收端按真实形态发布
  pub async fn snapshot_range_index_and_create_reader<D: Device>(
    session: &StoreSession<D>,
    key: &[u8],
  ) -> Result<(RangeIndexMigrationReader<fs::File>, TreeStreamMeta), MigrationError> {
    let (meta, stub) = session
      .load_collection_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    let engine = session.store.range_index();
    if stub.storage_backend == StorageBackendType::Memory.to_u8() {
      return Err(MigrationError::Invalid(
        ERR_MEMORY_TREE_MIGRATION.to_string(),
      ));
    }
    // 独占锁下重读的权威存根即迁移载荷（C# stubSpan.ToArray()）
    let stub_bytes = stub.encode();
    let migration_path = engine.derive_temp_migration_path();

    // 树身份键 = 物理 Meta 键（会话域内派生单点，rust 自定义面的域隔离身份；
    // C# RangeIndexManager 单实例单域无需域编码）——条带锁、注册表与数据
    // 文件定位全部按身份键进行
    let id_key = session.session_meta_key(key);
    let key_hash = whasher::fast_hash(&id_key);
    let key_id = RangeIndexManager::key_id_of(&id_key);
    // 以下全为同步段（快照 / 拷贝 / 开文件均不 await），条带锁不跨 .await
    let _xlock = engine.acquire_exclusive_for_delete(key_hash);
    match engine.get_tree(&id_key) {
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
        let data_path = engine.data_file_path_for_key(&id_key);
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
    let stream_meta = TreeStreamMeta {
      obj_type: meta.collection_type,
      next_expiry: meta.next_expiry,
      expire_unix_ms: match session.ttl_of(key).await? {
        Some(ticks) if ticks > now_ticks() => unix_time_in_milliseconds_from_ticks(ticks),
        _ => 0,
      },
    };
    Ok((reader, stream_meta))
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs:PublishMigratedIndex
  ///
  /// 发布迁移或重组后的 BfTree 索引至存储（1:1 对标 C# PublishMigratedIndex）。
  /// `next_expiry` 为发布后元记录的成员 TTL 水位（AOF 流块 arg2 从源侧透传；
  /// 纯用户 RI 迁移流无成员 TTL，传 i64::MAX）
  pub async fn publish_migrated_index<D: Device>(
    session: &StoreSession<D>,
    key: &[u8],
    stub_bytes: &[u8],
    temp_path: &Path,
    replace: bool,
    obj_type: GarnetObjectType,
    next_expiry: i64,
  ) -> PublishMigratedIndexResult {
    match session
      .publish_migrated_range_index(key, stub_bytes, temp_path, replace, obj_type, next_expiry)
      .await
    {
      Ok(()) => PublishMigratedIndexResult::Success,
      Err(RangeIndexError::AlreadyExists) => {
        if replace {
          PublishMigratedIndexResult::SkippedReplaceNotSupported
        } else {
          PublishMigratedIndexResult::SkippedAlreadyExists
        }
      }
      Err(e) => {
        log::error!("PublishMigratedIndex: failed to recover BfTree: {e}");
        PublishMigratedIndexResult::Failed
      }
    }
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
}
