//! 节点服务编排：存储引擎（wkv）与预写日志物理层（waof）的统一收口
//!
//! 对标 Garnet `StorageSession` 的 RangeIndexOps + AOF 协同：apply → log
//! 顺序在此固化，协议载荷由 [`crate::resp`] 产生，条目帧由 [`crate::aof`]
//! 框定。回放侧提供 [`NodeService::replay`]，把已提交 WAL 流分发给
//! [`Replay`] 实现（恢复端/副本端各按所需语义解释）。

use core::result;
use std::sync::Arc;

use thiserror::Error as ThisError;
use waof::WalLog;
use wdev::Device;
use wkv::{RangeIndexError, StorageBackend, StoreSession, TreeTuning, WedbStore};

use crate::{
  aof::{self, AofEntryRef, AofOp, Replay, encode_entry},
  resp,
};

#[derive(Debug, ThisError)]
pub enum Error {
  /// 存储引擎错误（含会话创建失败）
  #[error(transparent)]
  Store(#[from] wkv::Error),
  /// 范围索引操作错误
  #[error(transparent)]
  RangeIndex(#[from] RangeIndexError),
  /// WAL 物理层错误
  #[error(transparent)]
  Wal(#[from] waof::Error),
  /// AOF 条目解码错误
  #[error(transparent)]
  Aof(#[from] aof::Error),
  /// 回放期间未提交数据被环形覆写，扫描提前终止（缺数据优于错数据）
  #[error("wal overwritten during replay: {skipped} record(s) lost")]
  WalOverwritten { skipped: u64 },
}

pub type Result<T> = result::Result<T, Error>;

/// 节点共享引擎句柄类型别名（收敛 `Arc<WedbStore<D>>` 复合泛型签名）
pub type SharedStore<D> = Arc<WedbStore<D>>;

/// 单机网络服务：存储引擎会话 + 预写日志的编排门面
///
/// 内部持有一个 [`StoreSession`]（epoch 参与者随会话注册）。多连接场景下
/// 每连接经 [`Self::store`] 自行 `new_session`，本门面仅承担服务级编排
pub struct NodeService<D: Device> {
  session: StoreSession<D>,
  wal: Arc<WalLog<D>>,
}

impl<D: Device> NodeService<D> {
  /// 组装节点服务（引擎与日志设备由调用方装配，本层不绑设备构造）
  ///
  /// 按运行态 GC 配置拉起内置后台循环（幂等，对标 Garnet 服务启动时注册
  /// ExpiredKeyDeletionTask）：调用方已经 `open_shared`/`start_gc` 启动过则此处
  /// 为 no-op；未启动且 `gc.enabled` 时在此补启——服务端形态 TTL 主动过期
  /// 与紧缩调度由此保证，不依赖调用方记得手动启动
  pub fn new(store: SharedStore<D>, wal: Arc<WalLog<D>>) -> Result<Self>
  where
    D: Device + 'static,
  {
    store.start_gc();
    let session = store.new_session()?;
    Ok(Self { session, wal })
  }

  /// 存储引擎会话
  #[inline]
  pub fn session(&self) -> &StoreSession<D> {
    &self.session
  }

  /// 存储引擎句柄
  #[inline]
  pub fn store(&self) -> &SharedStore<D> {
    &self.session.store
  }

  /// 预写日志句柄
  #[inline]
  pub fn wal(&self) -> &Arc<WalLog<D>> {
    &self.wal
  }

  /// 创建范围索引并预写 WAL（apply → log，对标 StorageSession.RangeIndexOps + AOF）
  pub async fn ri_create(
    &self,
    key: &[u8],
    storage_backend: StorageBackend,
    tuning: TreeTuning,
  ) -> Result<()> {
    let frame = resp::encode_ri_create(key, &storage_backend, tuning);
    self
      .session
      .range_index_create(key, storage_backend, tuning)
      .await?;
    self.append(AofOp::RiCreate, key, &frame)?;
    Ok(())
  }

  /// 设置范围索引字段并预写 WAL
  pub async fn ri_set(&self, key: &[u8], field: &[u8], value: &[u8]) -> Result<()> {
    let frame = resp::encode_ri_set(key, field, value);
    self.session.range_index_set(key, field, value).await?;
    self.append(AofOp::RiSet, key, &frame)?;
    Ok(())
  }

  /// 删除范围索引字段并预写 WAL
  ///
  /// 与 Garnet `RangeIndexDel`"字段不存在则不写 AOF"的刻意差异：
  /// bf-tree 墓碑删除不区分字段是否存在（`BfTreeDeleteResult` 无
  /// NotFound 语义），故删除恒落日志，回放端按幂等删除处理
  pub async fn ri_del(&self, key: &[u8], field: &[u8]) -> Result<bool> {
    let deleted = self.session.range_index_del(key, field).await?;
    let frame = resp::encode_ri_del(key, field);
    self.append(AofOp::RiDel, key, &frame)?;
    Ok(deleted)
  }

  /// 回放已提交 WAL 流（对标 Garnet AofRecover：从提交位点扫描至尾部）
  ///
  /// 返回回放条目数；条目解码失败或回放器报错即中止
  pub async fn replay(&self, replay: &mut impl Replay) -> Result<u64> {
    let mut iter = self.wal.scan_committed();
    let mut count = 0u64;
    while let Some(record) = iter.next().await? {
      let entry = AofEntryRef::decode(&record.payload)?;
      replay.on_entry(entry)?;
      count += 1;
    }
    let overwritten = iter.overwritten_skips();
    if overwritten != 0 {
      return Err(Error::WalOverwritten {
        skipped: overwritten,
      });
    }
    Ok(count)
  }

  /// 追加一条操作镜像到 WAL（内存入队；提交时机由调用方经 wal() 驱动）
  #[inline]
  fn append(&self, op: AofOp, key: &[u8], blob: &[u8]) -> Result<()> {
    self.wal.enqueue(&encode_entry(op, 0, key, blob))?;
    Ok(())
  }
}
