use std::{
  fs,
  io::{self, ErrorKind},
  ops::Deref,
  path::Path,
  result::Result as StdResult,
  sync::Arc,
};

use compio::runtime::spawn_blocking;
use parking_lot::{RwLockReadGuard, RwLockWriteGuard};
use thiserror::Error as ThisError;
use wbftree::{
  BfTreeService, ERR_INDEX_ALREADY_EXISTS, Error as WbftreeError, RANGE_INDEX_STUB_SIZE,
  RangeIndexManager, RangeIndexStub,
};
use wval::{KeyTag, META_VALUE_SIZE, MetaValue, NamespaceDbCodec, TaggedKeyBuf};

use crate::{
  error,
  error::{CollectionError, Error},
};

mod drain;
mod heal;
mod migration;
mod ops;
mod promote;
mod stub;

/// RangeIndex 复合元记录存根治愈：wkv 内唯一内核与位变更器
/// （宿主端口 store/flush、store/cpr_host、compact 一律转调，禁止本地副本）
pub(crate) use heal::{
  clear_flushed_patch, mark_recovered_patch, patch_stub_record, range_index_stub_of,
};
/// wbftree 记录长度契约校验单点（RI 面与 wnode 分层集合写臂预校验共用）
pub use ops::validate_bftree_record;
pub use promote::SwapInWindowGuard;
pub use stub::rebind_stub;

/// 树身份键派生单点（显式域形态，供非会话上下文使用）：物理 Meta 键形态
/// `[vns varint][vdb varint][KeyTag::Meta][user_key]`
/// （[`NamespaceDbCodec::encode_tagged_key`]，与会话侧 [`StoreSession::session_meta_key`]
/// 同一编码内核——前缀同出 `SessionPrefixBuf::new`，两形态字节恒等）。
///
/// 树身份 = f(物理域, 用户键) 的 rust 自定义面必然要求（C# RangeIndexManager
/// 单实例单域，KeyId=XxHash128(keyBytes) 即可）：跨库同名键在树注册、升阶换入、
/// claim 封堵、FLUSHDB 回收四面按域隔离全靠此键含域。wbftree 全部身份 API
/// （注册表 live_indexes、claim 表 migrating、数据文件名、条带锁）一律吃本键，
/// 禁传裸用户键。会话上下文直接用 [`StoreSession::session_meta_key`]（元记录
/// 读写本就持有该键，零额外分配）；物理记录在手的场景（刷盘 OnFlush、紧缩
/// PreStage、恢复期登记）直接传该物理键字节，零解码零拷贝
pub(crate) fn tree_identity_key(vns: u64, vdb: u64, key: &[u8]) -> TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(vns, vdb, KeyTag::Meta, key)
}

/// 检查点屏障异步等待：基于 event_listener 无阻塞异步事件通知（零轮询、零 CPU）
///
/// 检查点外层屏障自一致性截断点捕获之前跨多个 await 持有（wcpr
/// create_checkpoint_inner 的 VersionShift 屏障，对标 C# OnCheckpoint(VersionShift)
/// → SetCheckpointBarrier），写者在此被动挂起等待清屏通知，不霸占 reactor。
/// `id_key` 为树身份键（见 [`tree_identity_key`]）
async fn wait_tree_checkpoint(
  mgr: &RangeIndexManager,
  id_key: &[u8],
) -> StdResult<(), RangeIndexError> {
  mgr
    .wait_for_tree_checkpoint_async(id_key)
    .await
    .map(|_| ())
    .map_err(RangeIndexError::from)
}

/// 弃置迁移快照残件（NotFound 视作已被换入内核 rename 消费，容忍静默）；
/// 残件另有 migration-tmp 启动清扫兜底。RENAME 迁移与升阶失败臂共用单点
/// （`ctx` 携调用语境前缀，统一日志文案）
pub(crate) fn discard_snapshot_file(path: &Path, ctx: &str) {
  if let Err(re) = fs::remove_file(path)
    && re.kind() != ErrorKind::NotFound
  {
    log::warn!("{ctx}快照残件删除失败，migration-tmp 启动清扫兜底: {re}");
  }
}

/// 范围索引操作错误类型 (1:1 对标 Garnet RangeIndexResult 与错误信息)
///
/// 存储层不带 RESP 网络文案：C# 存储层只回 GarnetStatus.WRONGTYPE，
/// RESP 错误帧由网络层出（libs/server/Resp/RangeIndex/
/// RespServerSessionRangeIndex.cs 引 CmdStrings.RESP_ERR_WRONG_TYPE），
/// rust 同位在 wnode/src/resp/rangeindex 转调 cs::RESP_ERR_WRONG_TYPE
#[derive(ThisError, Debug)]
pub enum RangeIndexError {
  /// 索引已存在
  #[error("{ERR_INDEX_ALREADY_EXISTS}")]
  AlreadyExists,
  /// 索引未找到
  #[error("ERR range index not found")]
  NotFound,
  /// 键类型不匹配（协议文案归网络层）
  #[error("key is not a range index")]
  WrongType,
  /// 键值长度超限
  #[error(
    "ERR key+value size must be between {min_record_size} and {max_record_size} bytes (got {total_len}), max key length {max_key_len} (got {key_len})"
  )]
  InvalidKV {
    min_record_size: u32,
    max_record_size: u32,
    max_key_len: u32,
    total_len: usize,
    key_len: usize,
  },
  /// 纯内存模式不支持扫描
  #[error("ERR RI.SCAN is not supported for MEMORY-mode indexes")]
  MemoryModeNotSupported,
  /// 底层存储错误
  #[error(transparent)]
  Store(#[from] Box<Error>),
  /// I/O 错误
  #[error(transparent)]
  Io(#[from] io::Error),
  /// 底层 BfTree 操作失败
  #[error(transparent)]
  Wbftree(WbftreeError),
  /// 内部或其它错误
  #[error("ERR {0}")]
  Internal(String),
}

impl PartialEq for RangeIndexError {
  fn eq(&self, other: &Self) -> bool {
    match (self, other) {
      (Self::AlreadyExists, Self::AlreadyExists) => true,
      (Self::NotFound, Self::NotFound) => true,
      (Self::WrongType, Self::WrongType) => true,
      (
        Self::InvalidKV {
          min_record_size: a_min,
          max_record_size: a_max,
          max_key_len: a_mkl,
          total_len: a_tot,
          key_len: a_kl,
        },
        Self::InvalidKV {
          min_record_size: b_min,
          max_record_size: b_max,
          max_key_len: b_mkl,
          total_len: b_tot,
          key_len: b_kl,
        },
      ) => a_min == b_min && a_max == b_max && a_mkl == b_mkl && a_tot == b_tot && a_kl == b_kl,
      (Self::MemoryModeNotSupported, Self::MemoryModeNotSupported) => true,
      (Self::Internal(a), Self::Internal(b)) => a == b,
      _ => false,
    }
  }
}

impl Eq for RangeIndexError {}

impl From<Error> for RangeIndexError {
  fn from(err: Error) -> Self {
    Self::Store(Box::new(err))
  }
}

impl From<WbftreeError> for RangeIndexError {
  fn from(err: WbftreeError) -> Self {
    match err {
      WbftreeError::IndexExists => Self::AlreadyExists,
      // 惰性恢复数据文件缺失 (并发删空的延迟 unlink 先于慢路径落地)：
      // 1:1 对标 C# RestoreTree 文件缺失臂 return false → 上层 NOTFOUND
      WbftreeError::NotFound => Self::NotFound,
      other => Self::Wbftree(other),
    }
  }
}

impl From<CollectionError> for RangeIndexError {
  fn from(err: CollectionError) -> Self {
    match err {
      CollectionError::Tree(w) => Self::from(w),
      other => Self::Internal(other.to_string()),
    }
  }
}

pub struct TreeReadGuard<'a> {
  tree: Arc<BfTreeService>,
  _guard: RwLockReadGuard<'a, ()>,
}

impl<'a> TreeReadGuard<'a> {
  #[inline]
  pub(crate) fn new(tree: Arc<BfTreeService>, guard: RwLockReadGuard<'a, ()>) -> Self {
    Self {
      tree,
      _guard: guard,
    }
  }

  #[inline]
  pub fn tree(&self) -> &Arc<BfTreeService> {
    &self.tree
  }
}

impl Deref for TreeReadGuard<'_> {
  type Target = BfTreeService;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.tree
  }
}

/// 树态键独占守卫（条带写锁 RAII）
///
/// 在 garnet 中的相对路径: libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:ExclusiveRangeIndexLock
/// （C# RAII 独占锁持有体的 rust 对位；C# 独占锁仅用于生命周期操作——DEL/淘汰/
/// 检查点快照/惰性恢复，C# 数据写每命令单树操作走共享锁即可（Garnet 元数据
/// 不维护计数，无锁内读改写窗口）；rust 分层集合写臂与 RI 单点写臂
/// （range_index_set / range_index_set_batch / range_index_del）是
/// 「装载 → 树写 → 计数 → meta 回写」多步序列，共享读锁下并发覆写丢 meta.size
/// 更新，独占串行对位 C# 对象域同键写经
/// Tsavorite 记录锁串行（TsavoriteKV.cs RMW InPlaceUpdater 前置记录 X 锁），
/// 承载面见 `acquire_tree_write`）
///
/// libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:Dispose 的承接：
/// C# ReadRangeIndexLock/ExclusiveRangeIndexLock 两 ref struct 的 Dispose
/// （ReleaseLock(token)）即 rust 守卫 Drop（内嵌 parking_lot 读写守卫析构
/// 释放），无显式释放函数面。
pub struct TreeWriteGuard<'a> {
  tree: Arc<BfTreeService>,
  _guard: RwLockWriteGuard<'a, ()>,
}

impl<'a> TreeWriteGuard<'a> {
  #[inline]
  pub(crate) fn new(tree: Arc<BfTreeService>, guard: RwLockWriteGuard<'a, ()>) -> Self {
    Self {
      tree,
      _guard: guard,
    }
  }

  #[inline]
  pub fn tree(&self) -> &Arc<BfTreeService> {
    &self.tree
  }
}

impl Deref for TreeWriteGuard<'_> {
  type Target = BfTreeService;

  #[inline]
  fn deref(&self) -> &Self::Target {
    &self.tree
  }
}

/// 分层臂树访问守卫：读写两态统一面（读臂共享 / 写臂独占，取锁判据见
/// wnode 侧 `needs_tree_write`；drop 语义随内嵌守卫）
pub enum TreeGuard<'a> {
  /// 共享读锁（纯读臂与穿透臂，对位 C# ReadRangeIndexLock 数据操作面）
  Read(TreeReadGuard<'a>),
  /// 独占写锁（多步写臂，对位 C# ExclusiveRangeIndexLock 形态 + 对象域 RMW 语义）
  Write(TreeWriteGuard<'a>),
}

impl TreeGuard<'_> {
  #[inline]
  pub fn tree(&self) -> &Arc<BfTreeService> {
    match self {
      Self::Read(g) => g.tree(),
      Self::Write(g) => g.tree(),
    }
  }
}

/// 范围索引运行状态与统计指标 (1:1 对标 Garnet RangeIndexMetrics 与 RI.METRICS RESP 响应字段)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RangeIndexMetrics {
  /// 对齐 C# RI.METRICS 协议字段；仅本进程内有句柄语义，跨进程仅为不透明标识
  pub tree_handle: u64,
  /// 索引是否处于活跃状态 (以注册表在线状态为准)
  pub is_live: bool,
  /// 存根是否已被刷盘标记
  pub is_flushed: bool,
  /// 存根是否由检查点快照恢复
  pub is_recovered: bool,
}

/// 把 wbftree 同步重操作卸载到 compio 阻塞线程 (基于 compio 生态的核保护优化)
///
/// thread-per-core 下同步阻塞会停摆整核任务：快照恢复 (整文件解析 + 环形缓冲
/// 分配)、整树释放 (Drop 遍历基页刷盘)、文件创建/换入均可能达到百毫秒级，一律
/// 经 `spawn_blocking` 在独立线程执行，宿主核继续调度其他任务。热路径 (注册表
/// 命中后的内存点操作) 不经此通道，与 C# 会话线程直调开销对齐。
///
/// 要求调用方处于 compio 运行时上下文 (RI 会话操作本就依赖运行时异步 I/O)。
/// manager 的同步方法在闭包内自取自放条带锁，锁不跨线程边界；持锁跨 await 的
/// 原子性窗口 (publish/rename) 由调用方任务承担，其安全性系**双前提**（票
/// zcode-r135c-lockorder 案二补齐第二半）：① compio 任务不迁移，锁随本核任务
/// 收放；② 同核其它任务不得无界阻塞取该条带锁——异步上下文的条带取锁一律走
/// 有界 try+让核档（wkv `acquire_tree_read`/`acquire_tree_write` 经
/// `try_read_range_index_lock`/`try_acquire_exclusive_for_delete` 承接，预算
/// 耗尽回存储忙），新点禁在异步任务内直取本 manager 的无界 `read`/`write`
/// 停车档（持锁跨 await 者依赖本核 reactor 驱动其 IO，一旦被停摆即互候永挂）
pub(crate) async fn range_index_blocking<T: Send + 'static>(
  op: impl FnOnce() -> T + Send + 'static,
) -> StdResult<T, RangeIndexError> {
  spawn_blocking(op)
    .await
    .map_err(|e| RangeIndexError::Internal(format!("RangeIndex 阻塞任务异常退出: {e}")))
}

/// 栈上编码 MetaValue 与 RangeIndexStub，消除堆内存分配 (零拷贝/零堆分配)
///
/// 存根元记录落盘工序列唯一编码实现，仅供门面 [`stub::save_bftree_meta_stub`]
/// 与 heal.rs 内联测试造数消费；检查点恢复路径 (store/cpr_host.rs
/// `run_recovery_pass`) 的存根自愈回写走 [`heal::patch_stub_record`] 原位治愈，
/// 不经本函数。模块私有：落盘一律经门面，禁绕行手拼
#[inline]
fn encode_meta_stub_record(
  meta: &MetaValue,
  stub: &RangeIndexStub,
) -> [u8; META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE] {
  let mut val = [0u8; META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE];
  val[..META_VALUE_SIZE].copy_from_slice(&meta.to_bytes());
  val[META_VALUE_SIZE..].copy_from_slice(&stub.encode());
  val
}

/// RangeIndex 复合元记录 `[MetaValue 32B][RangeIndexStub 35B]` 读侧唯一切分点，
/// 与 [`encode_meta_stub_record`] 成对：定长守卫、前窗 Meta 与后窗存根切分全部
/// 收口于此，装载点一律转调，禁绕行手拼（C# 对位 ReadIndex 的单点
/// reinterpret，见 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs；
/// 该锚由 range_index_stub_of 单点持有）
///
/// 长度不足复合记录以 [`RangeIndexError::WrongType`]（记录缺席/非复合）返回，
/// 该臂是结构性事实而非类型冲突，调用方按各自契约折叠（视同不存在 / 跳过 /
/// 终态拒绝）；前窗或后窗解码损坏则如实上抛，禁吞（缺数据优于错数据）
pub(crate) fn meta_and_stub_of(bytes: &[u8]) -> error::Result<(MetaValue, RangeIndexStub)> {
  if bytes.len() < META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE {
    return Err(Error::RangeIndex(RangeIndexError::WrongType));
  }
  let meta = MetaValue::from_slice(&bytes[..META_VALUE_SIZE])?;
  let stub =
    RangeIndexStub::decode(&bytes[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE])?;
  Ok((meta, stub))
}
