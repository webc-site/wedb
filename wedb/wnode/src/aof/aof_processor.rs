//! AOF 重放处理器（对标 libs/server/AOF/AofProcessor.cs:AofProcessor）
//!
//! 恢复回放与复制回放共用的条目处理内核：按条目头分发（检查点标记 /
//! FLUSH / 存储过程 / 事务 / 数据操作），数据操作经 [`ReplayTarget`] 落入
//! wkv 存储会话。rust 侧重放应用为异步（wkv 面天然异步），拓扑预处理
//! （key 哈希 / 一致性时间戳推进）保持同步快路径。
//!
//! C# 的拓扑特化预处理结构（SingleLogPreprocessKey 等）折叠为
//! `prepare_key`：按拓扑更新一致性时间戳并产出 key/payload 视图。
//!
//! 分派臂与重放族的兄弟文件：检查点 / FLUSH 臂见
//! [`super::aof_processor_record_arms`]，模糊区 / 事务组 / 数据操作重放见
//! [`super::aof_processor_op_replay`]，分块记录见
//! [`super::aof_processor_chunk_replay`]。

use std::sync::{
  Arc,
  atomic::{AtomicI64, Ordering},
};

use futures_util::future::LocalBoxFuture;
use parking_lot::RwLock;
use waof::{AofEntryType, AofHeader};
use wdev::Device;
use wkv::{CollectionError, Error, WedbStore};
use wval::NamespaceDbCodec;

use super::{
  garnet_append_only_file::GarnetAppendOnlyFile,
  readconsistency::read_consistency_manager::ReadConsistencyManager, record_gate,
  replaycoordinator::aof_replay_coordinator::AofReplayCoordinator,
};
use crate::{
  range_index::range_index_manager_replication::{RangeIndexManagerReplication, ReplicationError},
  storage::session::storage_session::StorageSession,
};

/// AOF 重放域错误（C# GarnetException 回放路径的 rust 形态）。
#[derive(Debug, thiserror::Error)]
pub enum AofReplayError {
  /// 重放语义错误（损坏条目 / 未接线子域 / 存储失败）。
  #[error("AOF replay: {0}")]
  Replay(String),
  /// 存储面错误（wkv 透传）。
  #[error(transparent)]
  Store(#[from] wkv::Error),
  /// 日志判读错误（waof 透传：未知回放头型 / 头损坏 / 未知操作类型）。
  #[error(transparent)]
  Log(#[from] waof::Error),
  /// 分块记录解析损坏（对标 C# ReadChunk 四道 throw GarnetException）
  #[error(transparent)]
  ChunkRead(#[from] super::aof_chunked_record_reader::AofChunkReadError),
  /// 并行恢复 worker 回放体 panic（监督隔离捕获的展开载荷文本；对标 C#
  /// RecoverReplayTask.cs RecoverReplayTaskAsync try/catch(Exception) 全捕
  /// 形态——C# 无 panic 穿透错误臂的形态，rust 以本变体归入既有错误链）。
  #[error("AOF replay worker panicked: {0}")]
  Panicked(String),
}

impl From<String> for AofReplayError {
  fn from(message: String) -> Self {
    Self::Replay(message)
  }
}

impl From<&str> for AofReplayError {
  fn from(message: &str) -> Self {
    Self::Replay(message.to_string())
  }
}

/// RI 复制族类型化错误归位（票 zcode-r135c 案二）：重放族错误带变体原样
/// 上浮至分派面，经既有 Store/Log 透明通道收敛，杜绝分派臂再套一段
/// `format!` 把类型化错误砸进 Replay(String) 击穿透明链（对标 C#
/// AofProcessor.cs 重放 catch 记 ex.ToString() 的 cause 链保全形态）；
/// Msg（携带上下文快照的损坏条目/状态机违例）系天然无类型源，留 Replay
impl From<ReplicationError> for AofReplayError {
  fn from(e: ReplicationError) -> Self {
    use ReplicationError as R;
    match e {
      R::Msg(message) => Self::Replay(message),
      R::Io(io) => Self::Store(Error::Io(io)),
      R::BfTree(tree) => Self::Store(Error::Collection(CollectionError::Tree(tree))),
      R::RangeIndex(index) => Self::Store(Error::RangeIndex(index)),
      R::Aof(log) => Self::Log(log),
    }
  }
}

/// 重放落点：存储会话 + 存储句柄（FLUSH 面）。
pub struct ReplayTarget<'a, 'b, D: Device> {
  /// 存储会话（读写应用面）。
  pub session: &'b StorageSession<'a, D>,
  /// 存储句柄（FLUSH ALL/DB 应用面）。
  pub store: Arc<WedbStore<D>>,
  /// 恢复面 AOF 重放位点下界向量（恢复检查点元数据的覆盖边界，按物理子日志
  /// 逐位保存，缺位/空 = 无下界；C# RecoveredSafeAofAddress 全向量形态）。
  /// [`record_gate::should_skip_record`] 的位点闸按条目所属物理子日志经
  /// [`Self::aof_floor_of`] 逐位取值，跳过快照已物化条目（截断前异常宕机纵深
  /// 防御）；副本面由闸内 `!as_replica` 位判天然禁用。绝不以子日志 0 标量
  /// 广播全维——子日志间地址空间独立，标量广播会把小写入量子日志的合法
  /// 区间整段误跳
  pub aof_floor: Vec<i64>,
}

impl<'a, 'b, D: Device> ReplayTarget<'a, 'b, D> {
  /// 装配重放落点（位点下界向量取自存储恢复面，装配点统一经此构造）。
  pub fn new(session: &'b StorageSession<'a, D>, store: &Arc<WedbStore<D>>) -> Self {
    Self {
      session,
      store: Arc::clone(store),
      aof_floor: store
        .recovered_aof_floor()
        .iter()
        .map(|&a| a as i64)
        .collect(),
    }
  }

  /// 按物理子日志取位点下界（缺位 = 无下界 0）
  #[inline]
  pub(crate) fn aof_floor_of(&self, sublog_idx: usize) -> i64 {
    self.aof_floor.get(sublog_idx).copied().unwrap_or(0)
  }

  /// 当前存储版本（SkipRecord 版本判定，逐记录动态读取；C#
  /// storeWrapper.store.CurrentVersion——检查点推进后即刻生效，绝不缓存快照）。
  #[inline]
  pub fn store_version(&self) -> i64 {
    self.store.current_version()
  }
}

/// 副本本地检查点钩子：类型擦除的无参异步动作，产出 [`wkv::Result`]。
///
/// 对标 C# 检查点结束臂 `storeWrapper.TakeCheckpointAsync`（AofProcessor.cs:302-317）
/// 下达面的 rust 注入形态。future 用 [`LocalBoxFuture`]（非 `Send`）：本仓 compio
/// 运行时线程本地驱动，重放钩子仅由背景重放线程同线程 await，与复制装配只加
/// `'static` 不加 `Send` 的既有约定同源；闭包对象只需 `Send + Sync`（宿主仅捕获
/// `Arc` 句柄），方可存入跨线程移动的 [`AofProcessor`]。
pub type ReplicaCheckpointHook = dyn Fn() -> LocalBoxFuture<'static, wkv::Result<()>> + Send + Sync;

/// libs/server/AOF/AofProcessor.cs:AofProcessor
///
/// AOF 重放处理器。
pub struct AofProcessor {
  /// 追加日志文件（一致性管理器 / 序列号 / 拓扑入口）。
  append_only_file: Arc<GarnetAppendOnlyFile>,
  /// 回放协调器（事务 / 模糊区 / 栅栏）。
  coordinator: AofReplayCoordinator,
  /// 活跃库 id。
  active_db_id: AtomicI64,
  /// 范围索引重放面（C# activeRangeIndexManager；RangeIndexPreview 关闭 /
  /// 未注入为 None，RI 族条目重放按 C# 同文案失败）
  range_index: Option<Arc<RangeIndexManagerReplication>>,
  /// 副本本地检查点钩子（C# 检查点结束臂 storeWrapper.TakeCheckpointAsync
  /// 下达面；AofProcessor 不持库管理器句柄、且非 [`Device`] 泛型，故宿主
  /// 装配期以类型擦除闭包注入。未注入为 None：恢复/单机重放臂据此维持
  /// 无本地打点的原语义，仅副本稳态重放链注入）。
  ///
  /// libs/server/AOF/AofProcessor.cs:ProcessAofRecordInternal（case
  /// CheckpointEndCommit 臂的 storeWrapper.TakeCheckpointAsync 触发）
  checkpoint_hook: RwLock<Option<Arc<ReplicaCheckpointHook>>>,
}

impl AofProcessor {
  /// libs/server/AOF/AofProcessor.cs:AofProcessor（构造）
  ///
  /// 依拓扑装配预处理路径并初始化回放上下文。多回放拓扑判定单源取
  /// GarnetAppendOnlyFile::multi_log_enabled（C# usingShardedLog 与
  /// MultiLogEnabled 全等，rust 不再复刻第二对字段）。
  pub fn new(append_only_file: Arc<GarnetAppendOnlyFile>) -> Self {
    let coordinator = AofReplayCoordinator::new(
      append_only_file.virtual_sublog_count(),
      append_only_file.multi_log_enabled(),
      // 栅栏启用第二道门（本地并发回放任务数，见协调器 replay_task_count 字段注）
      append_only_file.virtual_sublog_per_sublog(),
      // 栅栏会合时限与副本一致读同源（C# serverOptions.ReplicaSyncTimeout
      // 单一配置位投影，不建第二来源）
      append_only_file.read_timeout(),
    );
    if let Some(manager) = append_only_file.read_consistency_manager() {
      coordinator.set_consistency_manager(manager);
    }
    Self {
      append_only_file,
      coordinator,
      active_db_id: AtomicI64::new(0),
      range_index: None,
      checkpoint_hook: RwLock::new(None),
    }
  }
  /// 注入范围索引重放面（C# 由 storeWrapper.activeRangeIndexManager 装配）。
  pub fn set_range_index_manager(&mut self, manager: Arc<RangeIndexManagerReplication>) {
    self.range_index = Some(manager);
  }

  /// 范围索引重放面句柄。
  #[inline]
  pub const fn range_index_manager(&self) -> Option<&Arc<RangeIndexManagerReplication>> {
    self.range_index.as_ref()
  }

  /// 注入副本本地检查点钩子（宿主装配期调用；C# 由 storeWrapper 反查
  /// TakeCheckpointAsync 下达，rust 以类型擦除闭包承接）。仅副本稳态重放链
  /// 注入，恢复/单机驱动面不注入 → 检查点结束臂以钩子在场为门维持原语义。
  pub fn set_checkpoint_hook(&self, hook: Arc<ReplicaCheckpointHook>) {
    *self.checkpoint_hook.write() = Some(hook);
  }

  /// 副本本地检查点钩子快照。
  pub fn checkpoint_hook(&self) -> Option<Arc<ReplicaCheckpointHook>> {
    self.checkpoint_hook.read().clone()
  }

  /// 回放协调器句柄。
  #[inline]
  pub const fn coordinator(&self) -> &AofReplayCoordinator {
    &self.coordinator
  }

  /// 追加日志文件句柄。
  #[inline]
  pub const fn append_only_file(&self) -> &Arc<GarnetAppendOnlyFile> {
    &self.append_only_file
  }

  /// 读取一致性管理器快捷入口。
  pub fn read_consistency_manager(&self) -> Option<Arc<ReadConsistencyManager>> {
    self.append_only_file.read_consistency_manager()
  }

  /// libs/server/AOF/AofProcessor.cs:SwitchActiveDatabaseContext
  pub fn switch_active_database_context(&self, db_id: i64) {
    self.active_db_id.store(db_id, Ordering::Release);
  }

  /// 活跃库 id。
  pub fn active_db_id(&self) -> i64 {
    self.active_db_id.load(Ordering::Acquire)
  }

  /// 从记录负载解析 value（长度前缀形态）与剩余 input。
  #[inline]
  pub(crate) fn split_value_input(payload: &[u8]) -> Option<(&[u8], &[u8])> {
    record_gate::split_len_prefixed(payload)
  }

  pub(crate) fn replay_task_count(&self) -> usize {
    self.append_only_file.virtual_sublog_per_sublog()
  }

  /// libs/server/AOF/AofProcessor.cs:ProcessAofRecordInternal
  ///
  /// 共享回放入口（恢复回放与复制回放同一路径）。返回是否遇到检查点
  /// 起始标记（复制侧据此记录 ReplicationCheckpointStartOffset）。
  ///
  /// 分派骨架：逐记录纪元让步与帧头判读在此收口，检查点 / FLUSH 记录
  /// 类型族交 [`super::aof_processor_record_arms`] 各臂承接，数据操作族
  /// 交 [`Self::replay_op_dispatch`]，分块记录交
  /// [`super::aof_processor_chunk_replay::process_chunked_record`]。
  pub async fn process_aof_record_internal<D: Device>(
    &self,
    virtual_sublog_idx: usize,
    entry: &[u8],
    as_replica: bool,
    log_address_sequence_number: i64,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<bool, AofReplayError> {
    // 逐记录纪元让步（对标 C# 重放会话逐记录常规 context 的 enter/exit——
    // Tsavorite 重放不经 UnsafeContext 持整轮保护）：重放会话批守卫整轮在场，
    // 重入臂只递增计数不刷新公布纪元，不让步即把会话槽位公布纪元钉死入场值，
    // 全量重放期间目标引擎 safe_head/closed_until/检查点 fence 的排空屏障全部
    // 停摆（前台写环形回绕 evict_pages_for 乃至秒级停摆）。在上一条已完整落库
    // 的记录边界瞬时释放并重入（重入即公布最新纪元），单点收口于本共享入口，
    // 单任务快路径/页级并行/副本回放全拓扑覆盖
    target.session.batch.epoch_yield();
    // commit 元数据帧（waof 提交边界标记）非 AOF 条目，重放面跳过；
    // 复制推流面不过滤——帧随流保真传输，从侧恢复得以同样收敛
    if waof::is_commit_frame(entry) {
      return Ok(false);
    }

    // 顺序布局与并行恢复同构：分块帧每帧携带完整帧头，统一经
    // AofHeader::is_chunked 分发进重组读取器（裸续块旁路已随写端线协议
    // 对齐 C# WriteOneRecord 而移除，见 enqueue_span_chunked）

    let header = AofHeader::parse(entry).ok_or("AOF 条目头损坏")?;
    // 版本域自持的等值门（C# MaxSupportedAofHeaderVersion 上界臂的收窄）：
    // 本仓 AOF 载荷与 C# 异构且不做向下兼容，凡非本构建代际一律显式拒绝，
    // 使跨仓搬运文件在此判止而非被键域/输入区静默误读
    if header.aof_header_version != AofHeader::AOF_FORMAT_VERSION {
      return Err(
        format!(
          "Unsupported AOF format version {}; this build only accepts {}; \
           cross-repo (C# garnet) or foreign-generation AOF files are rejected",
          header.aof_header_version,
          AofHeader::AOF_FORMAT_VERSION
        )
        .into(),
      );
    }

    // 分块记录：累积至完成即直接分派（不物化连续镜像）
    if header.is_chunked() {
      let completed = self
        .coordinator
        .context(virtual_sublog_idx)
        .chunked_reader
        .read_chunk(entry)?;
      if let Some(acc) = completed {
        return super::aof_processor_chunk_replay::process_chunked_record(
          self,
          virtual_sublog_idx,
          acc,
          as_replica,
          log_address_sequence_number,
          target,
        )
        .await;
      }
      return Ok(false);
    }

    // C# 直接以枚举转型分发；未知判别值为损坏条目，显式拒绝
    //（replay_op_dispatch 的 default 分支同文案报错）
    let op_type = AofEntryType::try_from(header.op_type)
      .map_err(|_| format!("Unknown AOF header operation type {}", header.op_type))?;

    // 事务处理：TxnStart/TxnAbort/TxnCommit 及组内操作由协调器消化
    let action = self.coordinator.add_or_replay_transaction_operation(
      virtual_sublog_idx,
      entry,
      log_address_sequence_number,
    );
    match action {
      super::replaycoordinator::aof_replay_coordinator::TxnAction::Handled => return Ok(false),
      super::replaycoordinator::aof_replay_coordinator::TxnAction::Commit { group } => {
        let (sequence_number, participant_count) = record_gate::get_synchronized_operation_params(
          self.replay_task_count(),
          entry,
          log_address_sequence_number,
        )
        .unwrap_or((log_address_sequence_number, group.participant_count as i16));
        self
          .process_transaction_group(
            virtual_sublog_idx,
            sequence_number,
            participant_count,
            &group,
            as_replica,
            target,
          )
          .await?;
        return Ok(false);
      }
      // 非法事务流（活动组在位撞嵌套 TxnStart / 组内 StoredProcedure）：
      // 携协调器拒绝文案上抛，恢复即中止（C# 同位两臂 throw GarnetException）
      super::replaycoordinator::aof_replay_coordinator::TxnAction::Reject(reason) => {
        return Err(reason.into());
      }
      super::replaycoordinator::aof_replay_coordinator::TxnAction::None => {}
    }

    let mut is_checkpoint_start = false;
    match op_type {
      AofEntryType::CheckpointStartCommit => {
        is_checkpoint_start = true;
        self
          .process_checkpoint_start_commit(virtual_sublog_idx, entry, log_address_sequence_number)
          .await;
      }
      AofEntryType::CheckpointEndCommit => {
        self
          .process_checkpoint_end_commit(
            virtual_sublog_idx,
            entry,
            as_replica,
            log_address_sequence_number,
            &header,
            target,
          )
          .await?;
      }
      AofEntryType::FlushAll => {
        self
          .process_flush_all(
            virtual_sublog_idx,
            entry,
            log_address_sequence_number,
            header.unsafe_truncate_log(),
            target,
          )
          .await?;
      }
      AofEntryType::FlushDb => {
        self
          .process_flush_db(
            virtual_sublog_idx,
            entry,
            log_address_sequence_number,
            header.unsafe_truncate_log(),
            target,
          )
          .await?;
      }
      AofEntryType::FlushNs => {
        self
          .process_flush_ns(
            virtual_sublog_idx,
            entry,
            log_address_sequence_number,
            header.unsafe_truncate_log(),
            target,
          )
          .await?;
      }
      // 事务提交无本地分支：TxnCommit 已在函数头部
      // add_or_replay_transaction_operation 闭环消化——非模糊区经
      // TxnAction::Commit 立即整组重放，模糊区经 add_to_fuzzy_region_buffer
      // 登记提交标记 + txn_group_buffer 入队，清算期由
      // process_fuzzy_region_operations 识别标记出组重放（C# AofProcessor.cs:397
      // 的 TxnCommit 臂即 C# 侧不可达死支，rust 不再复刻）
      _ => {
        self
          .replay_op_dispatch(
            virtual_sublog_idx,
            header,
            entry,
            as_replica,
            log_address_sequence_number,
            target,
          )
          .await?;
      }
    }
    Ok(is_checkpoint_start)
  }

  /// libs/server/AOF/AofProcessor.cs:Dispose
  ///
  /// 释放回放执行器并清理未完成的范围索引流重组状态
  pub fn dispose(&self) {
    if let Some(ri) = &self.range_index {
      ri.dispose_incomplete_stream_reassembly();
    }
  }
}

impl Drop for AofProcessor {
  fn drop(&mut self) {
    self.dispose();
  }
}

/// FLUSH 族条目域载荷长度（[ns: u64 LE][db: u64 LE]）
const FLUSH_DOMAIN_LEN: usize = 16;

/// FLUSH 族条目域载荷解析（enqueue_safe_flush_aof 的对称读端）
///
/// 完整头之后 16B [ns: u64 LE][db: u64 LE]；载荷缺失或截断为损坏条目显式失败
///（恢复路径显式暴露原则，绝不静默按零域清库）。
///
/// 头尺寸经 waof 头面唯一口径 [`AofHeader::skip_header`] 按头型定长，本函数不
/// 自带第二套帧格式：FLUSH 广播条目在写侧 `GarnetLog::enqueue_broadcast_entry`
/// 按拓扑改写头型（对标 C# GarnetLog.cs:1196-1225——单物理日志 + 多重放任务落
/// SingleLogTransactionHeader、分片拓扑落 ShardedLogTransactionHeader，仅单日志
/// 拓扑保持 BasicHeader），载荷起点随头型移动；写侧 C# 把库号放在头字段
/// `databaseId`（GarnetLog.cs:1263 EnqueueSafeFlushAOF）故与头型无关，rust 以
/// 载荷承载 ns/db 全宽，读端必须按头型定位载荷，否则多重放/分片拓扑下清错域。
pub fn parse_flush_domain(entry: &[u8]) -> Result<(u64, u64), AofReplayError> {
  let at = AofHeader::skip_header(entry).ok_or("FLUSH 条目头损坏")?;
  let tail = entry
    .get(at..at + FLUSH_DOMAIN_LEN)
    .ok_or("FLUSH 条目域载荷缺失")?;
  let ns: [u8; 8] = tail[0..8].try_into().map_err(|_| "FLUSH 条目域载荷损坏")?;
  let db: [u8; 8] = tail[8..16].try_into().map_err(|_| "FLUSH 条目域载荷损坏")?;
  Ok((u64::from_le_bytes(ns), u64::from_le_bytes(db)))
}

/// 物理键 context 切换守卫（构造时解出 `(vns, vdb, 标签, 用户键)` 并**直设**
/// 会话虚拟域**与逻辑域**，drop 时复原进入前的虚拟域与逻辑域）。
///
/// 条目 key 统一为 wkv 物理键（`[NsVarint][DbVarint][KeyTag][用户键]`，
/// 等价 C# 单库 AOF 的用户键 + databaseId 组合，且跨 ns/db 域无损）；前缀两
/// 段是入账会话当时的**虚拟号**（`StoreSession::virtual_domain` 单点，与记录
/// 落域同值），故重放应用面只经 [`StoreSession::set_virtual_context`] 直设回
/// 条目原域——零虚号分配、零 DbMeta 落盘（doc/zh/db.md「从库完全继承主库的
/// 映射体系，不进行本地二次映射」）。C# 对位是
/// `AofProcessor.cs:SwitchActiveDatabaseContext` 按子日志切到**既有**库实例，
/// 从不重解析域号：本守卫取相同口径（切域即指物理解析完成后的域）。
///
/// 逻辑槽 `namespace`/`active_db` 随本守卫一并直设：版本轨种子=逻辑域
/// （wkv `bump_watch_version` 取 `session_logical_prefix` 单点），回放条目
/// 的写须落条目**逻辑域**槽位方能与副本在途 WATCH 的登记核验同槽——逻辑域
/// 于 enter 期经 `VirtualDbManager::version_domain_of` 对条目物理域做**一次**
/// 映射换算并固着进会话逻辑槽（set 期换算、bump 期零反查，禁逐次热路径
/// 反查），映射缺席（旧代死域条目）回条目物理号作孤域替身（该代内容已随
/// 换号销毁，至多数值重合碰撞面、只多 abort 不少 abort，属安全侧）。
pub(crate) struct KeyContextGuard<'a, D: Device> {
  batch: &'a wkv::BatchStoreSession<'a, D>,
  /// 进入前的会话虚拟域（drop 侧对称复原）
  prev: (u64, u64),
  /// 进入前的会话逻辑域（drop 侧对称复原，版本轨槽随逻辑槽回位）
  prev_logic: (u64, u64),
  /// 条目物理键标签（回放应用面据此选择值域：String / ObjectEnvelope / Acl）
  pub(crate) tag: wval::KeyTag,
  /// 用户键（物理键剥前缀）
  pub(crate) user_key: &'a [u8],
}

impl<'a, D: Device> KeyContextGuard<'a, D> {
  pub(crate) fn enter(
    session: &'a StorageSession<'_, D>,
    key: &'a [u8],
  ) -> Result<Self, AofReplayError> {
    let (vns, vdb, tag, user_key) =
      NamespaceDbCodec::decode_tagged_key(key).map_err(|e| format!("AOF 条目物理键损坏: {e}"))?;
    let batch = &session.batch;
    let prev = batch.virtual_domain();
    let prev_logic = (batch.namespace(), batch.active_db());
    let (lns, ldb) = batch.store().vdb.version_domain_of(vns, vdb);
    batch.set_virtual_context(vns, vdb, lns, ldb);
    Ok(Self {
      batch,
      prev,
      prev_logic,
      tag,
      user_key,
    })
  }
}

impl<D: Device> Drop for KeyContextGuard<'_, D> {
  fn drop(&mut self) {
    self.batch.set_virtual_context(
      self.prev.0,
      self.prev.1,
      self.prev_logic.0,
      self.prev_logic.1,
    );
  }
}
