//! AOF 记录类型族分派臂（对标 libs/server/AOF/AofProcessor.cs:ProcessAofRecordInternal）
//!
//! 检查点标记族（CheckpointStartCommit / CheckpointEndCommit）与 FLUSH 族
//! （FlushAll / FlushDb / FlushNs）的条目臂承接，连同其共用的同步栅栏编排
//! （synchronized_under_barrier）与 FLUSH 族栅栏骨架（flush_under_barrier）。
//! 主分派骨架见 [`super::aof_processor::AofProcessor::process_aof_record_internal`]。

use std::{future::Future, sync::Arc};

use waof::AofHeader;
use wdev::Device;
use wkv::WedbStore;

use super::{
  aof_processor::{AofProcessor, AofReplayError, ReplayTarget, parse_flush_domain},
  record_gate,
  replaycoordinator::aof_replay_coordinator::LeaderBarrierType,
};
use crate::resp::vector::vector_manager::RegistryReclaim;

/// FLUSH 族栅栏对齐位置组（回放位点三元组，三臂共用传递）
struct SyncPos<'a> {
  virtual_sublog_idx: usize,
  entry: &'a [u8],
  sequence_number: i64,
}

impl AofProcessor {
  /// 同步操作回放栅栏编排（C# ProcessAofRecordInternal 各
  /// GetSynchronizedOperationParams + ProcessSynchronizedOperation 支的合流入口：
  /// FLUSH 族清空、副本检查点结束臂本地打点共用同一栅栏机制，不留第二套）：
  /// 按条目头取 (序列号, 参与者数) 交协调器异步栅栏入口，Leader 独占段内 await
  /// 传入动作，全员对齐 → 独占执行 → 清栏放行 → 虚拟子日志最大序列号推进一体化
  /// 承接，非多回放形态由入口内部直执行 + 推进（对标 C# `!usingShardedLog`
  /// 的 BlockingWait 直调）。动作闭包自带其上下文（FLUSH 闭包克隆 store、
  /// 检查点闭包克隆钩子），本编排不持任何域句柄。
  async fn synchronized_under_barrier<F, Fut, R>(
    &self,
    virtual_sublog_idx: usize,
    entry: &[u8],
    log_address_sequence_number: i64,
    barrier_type: LeaderBarrierType,
    op: F,
  ) -> Result<(), AofReplayError>
  where
    F: FnOnce() -> Fut,
    Fut: Future<Output = wkv::Result<R>>,
  {
    let (sequence_number, participant_count) = record_gate::get_synchronized_operation_params(
      self.replay_task_count(),
      entry,
      log_address_sequence_number,
    )
    .ok_or("同步操作条目缺少栅栏参数")?;
    self
      .coordinator()
      .process_synchronized_operation_async(
        virtual_sublog_idx,
        sequence_number,
        participant_count,
        barrier_type as i32,
        Some(move || async move { op().await.map_err(AofReplayError::Store) }),
      )
      .await
      .map(|_| ())
  }

  /// FLUSH 族回放臂公共骨架（FlushAll/FlushDb/FlushNs 三臂单点同构）：
  /// 登记表域回收 → 纪元让渡 → 栅栏内域清空 + 尾截断。三臂差异仅域回收
  /// 类别、栅栏类别与域清空首步（`flush` 闭包），截断统一受
  /// unsafeTruncateLog 钳制，单点收口 `store.truncate()`。
  async fn flush_under_barrier<D: Device, F, Fut>(
    &self,
    target: &ReplayTarget<'_, '_, D>,
    pos: SyncPos<'_>,
    reclaim: RegistryReclaim,
    barrier_type: LeaderBarrierType,
    unsafe_truncate: bool,
    flush: F,
  ) -> Result<(), AofReplayError>
  where
    F: FnOnce(Arc<WedbStore<D>>) -> Fut,
    Fut: Future<Output = wkv::Result<()>>,
  {
    if let Some(vm) = self.append_only_file().vector_manager() {
      vm.reclaim_registry_domain(reclaim).await;
    }
    // 纪元让渡（对标检查点臂同款 suspend_epoch 单机制）：FLUSH 独占段
    // （flush_all_databases 的 shift 链含排空屏障）在栅栏内执行时，其余
    // 参与者正钉在其批会话保护区内的栅栏等待上——不让渡即排空互锁
    // （我等他放栏、他等我退区），恢复冻结。让渡须覆盖整段栅栏跨度
    // （先让渡再签到），检查点臂同款时序
    let _epoch_suspend = target.session.batch.suspend_epoch();
    let store = Arc::clone(&target.store);
    self
      .synchronized_under_barrier(
        pos.virtual_sublog_idx,
        pos.entry,
        pos.sequence_number,
        barrier_type,
        move || async move {
          flush(Arc::clone(&store)).await?;
          if unsafe_truncate {
            store.truncate().await?;
          }
          Ok(())
        },
      )
      .await
  }

  /// libs/server/AOF/AofProcessor.cs:ProcessAofRecordInternal（case
  /// CheckpointStartCommit 臂）：开启模糊区跟踪，撞上一未闭合模糊区即丢弃
  /// 其缓冲并留痕；多回放拓扑下推进虚拟子日志最大序列号。
  ///
  /// C# 的 aofHeaderVersion > 1 是 v1 旧文件兼容臂；本仓单版本域等值门
  /// 之下过门条目恒为本代际，模糊区跟踪无条件启用
  pub(crate) async fn process_checkpoint_start_commit(
    &self,
    virtual_sublog_idx: usize,
    entry: &[u8],
    log_address_sequence_number: i64,
  ) {
    if self
      .coordinator()
      .context(virtual_sublog_idx)
      .in_fuzzy_region()
    {
      // C# AofProcessor.cs:276 同款 Information 留痕：上一模糊区未遇
      // CheckpointEndCommit 即遭新 CheckpointStartCommit，此处将静默
      // 丢弃其缓冲重放条目，丢弃条数为排障唯一信号
      let discarded = self
        .coordinator()
        .fuzzy_region_buffer_count(virtual_sublog_idx);
      log::info!(
        "上一模糊区未遇 CheckpointEndCommit 即遭新 CheckpointStartCommit，丢弃先前模糊区缓冲 {discarded} 条"
      );
      self
        .coordinator()
        .clear_fuzzy_region_buffer(virtual_sublog_idx);
    }
    self
      .coordinator()
      .context(virtual_sublog_idx)
      .set_in_fuzzy_region(true);
    if self.append_only_file().multi_log_enabled() {
      let sequence_number = record_gate::get_synchronized_operation_params(
        self.replay_task_count(),
        entry,
        log_address_sequence_number,
      )
      .map_or(0, |(seq, _)| seq);
      self
        .append_only_file()
        .with_read_consistency_manager(|manager| {
          manager.update_virtual_sublog_max_sequence_number(virtual_sublog_idx, sequence_number);
        });
    }
  }

  /// libs/server/AOF/AofProcessor.cs:ProcessAofRecordInternal（case
  /// CheckpointEndCommit 臂）：闭合模糊区，副本遇主端更新版本标记先拍本地
  /// 检查点，再统一重放模糊区缓冲条目。
  ///
  /// C# 的 aofHeaderVersion > 1 是 v1 旧文件兼容臂；过门条目在本仓
  /// 单版本域等值门之下恒为本代际，直接判读模糊区状态
  pub(crate) async fn process_checkpoint_end_commit<D: Device>(
    &self,
    virtual_sublog_idx: usize,
    entry: &[u8],
    as_replica: bool,
    log_address_sequence_number: i64,
    header: &AofHeader,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    if !self
      .coordinator()
      .context(virtual_sublog_idx)
      .in_fuzzy_region()
    {
      // 无起始标记的结束标记：忽略（C# LogInformation 分支）
    } else {
      self
        .coordinator()
        .context(virtual_sublog_idx)
        .set_in_fuzzy_region(false);
      // 副本遇主端更新版本检查点结束标记：拍本地一次检查点，序次在
      // 重放模糊区缓冲条目之前（C# AofProcessor.cs:301-319 检查点在
      // ProcessFuzzyRegionOperations 之前）——非多回放形态
      // !usingShardedLog 直接 BlockingWait(TakeCheckpointAsync)，
      // 多回放形态 ProcessSynchronizedOperation(CHECKPOINT) 让 Leader
      // 独占拍；两形态由 synchronized_under_barrier 内 process_synchronized
      // _operation_async 单一入口按 multi_log_enabled 分派。
      // 判定复用 record_gate::is_new_version_record 单点（header.store_version
      // > 当前版本，与 C# :302 逐字对齐；钩子未注入 = 恢复/单机重放臂，
      // 维持原语义，不新增打点面），副本截断点由内核经
      // on_checkpoint_initiated / add_new_checkpoint_entry 既有单机制承接
      // 驱动面须在纪元保护区外（wcpr 检查点入口 ensure_epoch_unprotected 以
      // CheckpointWhileEpochProtected fail-fast：重放循环自带批会话纪元守卫，
      // 自钉使内核排空屏障永假），故在本条记录处（两记录之间、上一条会话
      // 操作已完整落库）挂起自持保护、拍完由守卫 Drop 按原深度重入，对标
      // C# 长 I/O 的 UnsafeSuspendThread/ResumeThread 协议
      if as_replica
        && record_gate::is_new_version_record(header, target.store.current_version())
        && let Some(hook) = self.checkpoint_hook()
      {
        let _epoch_suspend = target.session.batch.suspend_epoch();
        self
          .synchronized_under_barrier(
            virtual_sublog_idx,
            entry,
            log_address_sequence_number,
            LeaderBarrierType::Checkpoint,
            move || async move { hook().await },
          )
          .await?;
      }
      // 模糊区结束后统一重放缓冲的 (v+1) 条目
      self
        .process_fuzzy_region_operations(virtual_sublog_idx, as_replica, target)
        .await?;
      self
        .coordinator()
        .clear_fuzzy_region_buffer(virtual_sublog_idx);
    }
    Ok(())
  }

  /// libs/server/AOF/AofProcessor.cs:ProcessAofRecordInternal（case FlushAll →
  /// GetSynchronizedOperationParams + ProcessSynchronizedOperation
  /// (LeaderBarrierType.FLUSH_DB_ALL) 栅栏内 StoreWrapper.FlushAllDatabases
  /// (unsafeTruncateLog)）：全部库用户域清空。多回放拓扑下全员在条目序列号
  /// 对齐后由 Leader 独占清空，杜绝其它任务在途的 FLUSH 前记录清空后落库
  /// 复活被清数据；虚拟最大序列号推进随栅栏尾步生效（FLUSH 条目无 key，
  /// 走不到 keyed 记录的时间戳推进面）。非多回放形态由栅栏入口直执行 +
  /// 推进，保持原单任务语义。unsafeTruncateLog flag 经回放臂消费，
  /// 截断单点 store.truncate()，受 delete_floor 钳制。
  /// 登记表全域回收联动（换号后旧域条目在新域不可达，回收即清库；
  /// 主端 flush_all_databases 同臂，域值与广播条目同源）
  pub(crate) async fn process_flush_all<D: Device>(
    &self,
    virtual_sublog_idx: usize,
    entry: &[u8],
    log_address_sequence_number: i64,
    unsafe_truncate: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    self
      .flush_under_barrier(
        target,
        SyncPos {
          virtual_sublog_idx,
          entry,
          sequence_number: log_address_sequence_number,
        },
        RegistryReclaim::All,
        LeaderBarrierType::FlushDbAll,
        unsafe_truncate,
        |store| async move { store.flush_all_databases().await },
      )
      .await
  }

  /// libs/server/AOF/AofProcessor.cs:ProcessAofRecordInternal（case FlushDb →
  /// ProcessSynchronizedOperation(LeaderBarrierType.FLUSH_DB) 栅栏）：
  /// 仅清条目域指定库，其它库数据不受影响；栅栏与序列号推进同 FlushAll 支。
  /// 域载荷 (vns, 换号前旧 vdb) 取条目本身（与数据条目物理键前缀同域），
  /// 绝不取重放会话当前上下文——多租户共享单 AOF 下会话语域随上一条数据
  /// 条目漂移，误读必错域清库（C# 对应 databaseId 1 字节，rust u64 全宽
  /// 无截断）。换号映射与旧域判死已由先行的 DbMeta 镜像条目应用（主库
  /// commit_swap 落盘先于本条目入队），副本绝不在回放面本地取号换格——
  /// 本地二次映射即主从分叉；条目顺序面只由 Swapped 形生产（FirstMap
  /// 无旧域即无退役对象，主库侧按 None 旧域跳过本条目，哨兵形已灭绝；
  /// 顺序面 retire 纯空转，存活命中臂 fail-closed：先按盘上 0x02 权威
  /// 回建装载，回建后仍指旧域即显式错误上抛中止恢复，禁取号换格静默冒充），
  /// 条目臂只余栅栏对齐与登记表回收。
  /// unsafeTruncateLog flag 经回放臂消费，截断单点 store.truncate()，
  /// 受 delete_floor 钳制
  pub(crate) async fn process_flush_db<D: Device>(
    &self,
    virtual_sublog_idx: usize,
    entry: &[u8],
    log_address_sequence_number: i64,
    unsafe_truncate: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let (vns, old_vdb) = parse_flush_domain(entry)?;
    // 登记表域回收联动（载荷 (vns, 换号前旧 vdb) 即死亡域）
    self
      .flush_under_barrier(
        target,
        SyncPos {
          virtual_sublog_idx,
          entry,
          sequence_number: log_address_sequence_number,
        },
        RegistryReclaim::Database {
          vns,
          vdb: old_vdb,
          slot: None,
        },
        LeaderBarrierType::FlushDb,
        unsafe_truncate,
        |store| async move {
          // 旧域树回收经阻塞卸载通道离核（retire 链异步化，回放任务
          // 不触条带无界停车档）；回建失败错误以 ? 上抛（fail-closed，
          // Leader 让栏经 BarrierJoin Drop 放行其余参与者，禁折成静默）
          store.retire_dead_domain(vns, old_vdb).await
        },
      )
      .await
  }

  /// rust 多租户扩展（C# 无此形态）：整命名空间虚拟换号清库，域值取
  /// 条目载荷 (旧 vns, 0)，防多租户错域清库。与 FlushDb 同族局部清空，
  /// 复用其栅栏类别接同一跨回放任务同步（不另造 C# 没有的新类别）；
  /// 换号由先行 DbMeta 镜像条目承接（零本地换号取号），条目臂对齐栅栏、
  /// 回收登记表并兜底判死旧空间（存活命中臂 fail-closed：先按盘上 0x01
  /// 权威回建装载，回建后仍指旧空间即显式错误上抛中止恢复，禁取号换格
  /// 静默冒充；FirstMap 由主库侧按 None 旧域
  /// 跳过本条目，哨兵形已灭绝，同 FlushDb 臂）。
  /// unsafeTruncateLog flag 经回放臂消费，截断单点 store.truncate()，
  /// 受 delete_floor 钳制
  pub(crate) async fn process_flush_ns<D: Device>(
    &self,
    virtual_sublog_idx: usize,
    entry: &[u8],
    log_address_sequence_number: i64,
    unsafe_truncate: bool,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<(), AofReplayError> {
    let (old_vns, _) = parse_flush_domain(entry)?;
    // 登记表域回收联动（载荷 vns 即换号前旧命名空间域）
    self
      .flush_under_barrier(
        target,
        SyncPos {
          virtual_sublog_idx,
          entry,
          sequence_number: log_address_sequence_number,
        },
        RegistryReclaim::Namespace { vns: old_vns },
        LeaderBarrierType::FlushDb,
        unsafe_truncate,
        |store| async move {
          // 卸载形态同 FlushDb 臂 retire_dead_domain；回建失败错误以 ?
          // 上抛（fail-closed，禁折成静默）
          store.retire_dead_namespace(old_vns).await
        },
      )
      .await
  }
}
