//! 恢复回放驱动（对标 libs/server/AOF/Recover/RecoverLogDriver.cs:RecoverLogDriver）
//!
//! 单物理子日志的扫描-消费驱动器：支持单任务顺序消费快路径与多 Worker 页级并行
//! 双闸栏同步消费路径（对标 C# CreateAndRunIntraPageParallelReplayTasks 与 DoubleTurnstileBarrier）。

use std::{
  mem::{replace, take},
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  thread::{self, JoinHandle},
};

use compio::runtime::Runtime;
use parking_lot::{Mutex, RwLock};
use waof::WalRecord;
use wbase::{convert::TICKS_PER_MILLISECOND, supervise::supervise_item, time::now_stopwatch_ticks};
use wdev::Device;

use super::{
  double_turnstile_barrier::DoubleTurnstileBarrier,
  recover_replay_task::{ReplayPageArgs, replay_page},
};
use crate::{
  aof::{
    aof_processor::{AofProcessor, AofReplayError, ReplayTarget},
    garnet_append_only_file::GarnetAppendOnlyFile,
    record_gate,
  },
  resp::vector::vector_store_callbacks::ActiveVectorSessionGuard,
  storage::session::storage_session::StorageSession,
};

/// 回放进度日志步长：每 10 万条输出一次（libs/server/AOF/Recover/
/// RecoverLogDriver.cs:131-134 LogTrace 的 rust debug! 对位）。
const REPLAY_PROGRESS_INTERVAL: u64 = 100_000;

/// 并行恢复 worker 监督名（wbase 监督注册表同名归组：整段体与逐页包装
/// 共用一行 panic 计数，INFO 快照可观测）
const RECOVER_WORKER_TASK: &str = "aof_recover_worker";

/// 回放进度日志单点（C# 每 10 万条 LogTrace 进度：条数 + AOF 地址；
/// 计数已有 AtomicU64，热路径仅一次取模判零）。两路径（replay_one /
/// replay_page）共用。
#[inline]
pub(crate) fn log_replay_progress(count: u64, address: i64) {
  if count.is_multiple_of(REPLAY_PROGRESS_INTERVAL) {
    log::debug!("AOF 恢复进度:已重放 {count} 条 @ 地址 {address}");
  }
}

/// 装配失败收场面（会话 / 运行时 / 守卫外展开）：本 worker 未参与任何闸门
/// 会合，双闸栏到达计数不得增减——落错误槽 + 置取消位 + 关闸广播（closed
/// 使全员闸等待即刻逃逸），leader 依取消位截停后续批次发布，错误经
/// error_slot 于 join 后上抛；已有错误在场时不覆盖（保留首个真实错误）。
fn worker_assembly_failure(
  task_idx: usize,
  err: AofReplayError,
  error_slot: &Mutex<Option<AofReplayError>>,
  is_cancelled: &AtomicBool,
  barrier: &DoubleTurnstileBarrier,
) {
  log::error!("Worker [{task_idx}] 异常收场: {err}");
  let mut slot = error_slot.lock();
  if slot.is_none() {
    *slot = Some(err);
  }
  drop(slot);
  is_cancelled.store(true, Ordering::Release);
  barrier.notify_all();
}

/// 页面批处理上下文（供 Leader 与各 Worker 共享当前页数据）。
pub struct ReplayBatchContext {
  /// 当前批次记录切片。
  pub records: RwLock<Arc<Vec<WalRecord>>>,
  /// 是否为最后一批。
  pub is_last: AtomicBool,
  /// 取消/异常状态引用（Worker 异常时置位，Leader 检测后立即终止消费避免死锁）。
  pub is_cancelled: Arc<AtomicBool>,
}

impl ReplayBatchContext {
  pub fn new(is_cancelled: Arc<AtomicBool>) -> Self {
    Self {
      records: RwLock::new(Arc::new(Vec::new())),
      is_last: AtomicBool::new(false),
      is_cancelled,
    }
  }
}

/// 并行回放装配参数容器。
pub struct ParallelReplayInitArgs<'a, D: Device> {
  pub replay_task_count: usize,
  pub processor: &'a AofProcessor,
  pub aof: &'a GarnetAppendOnlyFile,
  pub target: &'a ReplayTarget<'a, 'a, D>,
  pub barrier: &'a Arc<DoubleTurnstileBarrier>,
  pub batch_context: &'a Arc<ReplayBatchContext>,
  pub is_cancelled: &'a Arc<AtomicBool>,
  pub error_slot: &'a Arc<Mutex<Option<AofReplayError>>>,
}

/// libs/server/AOF/Recover/RecoverLogDriver.cs:RecoverLogDriver
///
/// 恢复回放驱动。
pub struct RecoverLogDriver {
  /// 物理子日志下标。
  physical_sublog_idx: usize,
  /// 起始地址。
  start_address: i64,
  /// 目标地址（含）。
  until_address: i64,
  /// 前缀一致序列号上界（-1 = 不限）。
  until_sequence_number: i64,
  /// 副本回放身份位：本驱动专属单机崩溃恢复，装配点恒传 false
  /// （对标 C# AofRecover.cs:103 SingleLogRecover 的 asReplica: false 契约；
  /// 副本回放由 wedb/src/server/replication/replica_replay_driver 独立承接，
  /// 不经本驱动）。true 会使未决快照增量入模糊区缓冲并在恢复结束被物理丢弃。
  as_replica: bool,
  /// 已重放记录总数。
  replayed_record_count: AtomicU64,
  /// 前缀一致性上界到达标记（单调溢出终止）。
  prefix_consistency_boundary_reached: AtomicBool,
}

impl RecoverLogDriver {
  /// libs/server/AOF/Recover/RecoverLogDriver.cs:RecoverLogDriver
  ///
  /// 构造恢复驱动器。
  pub fn new(
    physical_sublog_idx: usize,
    start_address: i64,
    until_address: i64,
    until_sequence_number: i64,
    as_replica: bool,
  ) -> Self {
    Self {
      physical_sublog_idx,
      start_address,
      until_address,
      until_sequence_number,
      as_replica,
      replayed_record_count: AtomicU64::new(0),
      prefix_consistency_boundary_reached: AtomicBool::new(false),
    }
  }

  /// libs/server/AOF/Recover/RecoverLogDriver.cs:ReplayedRecordCount
  ///
  /// 获取已重放记录总数。
  #[inline]
  pub fn replayed_record_count(&self) -> u64 {
    self.replayed_record_count.load(Ordering::Relaxed)
  }

  /// libs/server/AOF/Recover/RecoverLogDriver.cs:Throttle
  ///
  /// 回放节流控制（空操作）。
  #[inline]
  pub fn throttle(&self) {}

  /// libs/server/AOF/Recover/RecoverLogDriver.cs:CreateAndRunIntraPageParallelReplayTasks
  ///
  /// 初始化并启动页内并行回放 Worker 任务集与双闸栏协调器。
  pub fn create_and_run_intra_page_parallel_replay_tasks<D: Device + 'static>(
    &self,
    args: ParallelReplayInitArgs<'_, D>,
  ) -> Vec<JoinHandle<()>> {
    let replay_task_count = args.replay_task_count;
    let as_replica = self.as_replica;
    let mut workers = Vec::with_capacity(replay_task_count);
    let virtual_sublog_base = self.physical_sublog_idx * args.aof.virtual_sublog_per_sublog();

    for task_idx in 0..replay_task_count {
      let virtual_sublog_idx = virtual_sublog_base + task_idx;
      let until_sequence_number = self.until_sequence_number;

      // 借用安全包装：Worker 线程在 run 结束前必全部 join
      let processor_ptr = args.processor as *const AofProcessor as usize;
      let aof_ptr = args.aof as *const GarnetAppendOnlyFile as usize;
      let target_ptr = args.target as *const ReplayTarget<'_, '_, D> as usize;
      let replayed_ptr = &self.replayed_record_count as *const AtomicU64 as usize;
      let boundary_ptr = &self.prefix_consistency_boundary_reached as *const AtomicBool as usize;

      let barrier_clone = Arc::clone(args.barrier);
      let batch_clone = Arc::clone(args.batch_context);
      let is_cancelled_clone = Arc::clone(args.is_cancelled);
      let error_slot_clone = Arc::clone(args.error_slot);

      let handle = thread::spawn(move || {
        let processor_ref = unsafe { &*(processor_ptr as *const AofProcessor) };
        let aof_ref = unsafe { &*(aof_ptr as *const GarnetAppendOnlyFile) };
        let target_ref = unsafe { &*(target_ptr as *const ReplayTarget<'_, '_, D>) };
        let replayed_ref = unsafe { &*(replayed_ptr as *const AtomicU64) };
        let boundary_ref = unsafe { &*(boundary_ptr as *const AtomicBool) };

        let rt = match Runtime::new() {
          Ok(rt) => rt,
          Err(err) => {
            worker_assembly_failure(
              task_idx,
              AofReplayError::Replay(format!("compio 运行时装配失败: {err}")),
              &error_slot_clone,
              &is_cancelled_clone,
              &barrier_clone,
            );
            return;
          }
        };

        // 守卫外展开兜底收场句柄（async 体 move 捕获原句柄，此组独立克隆
        // 供 block_on 返回后的兜底臂使用）
        let rescue_slot = Arc::clone(&error_slot_clone);
        let rescue_cancel = Arc::clone(&is_cancelled_clone);
        let rescue_barrier = Arc::clone(&barrier_clone);

        let outcome = rt.block_on(supervise_item(RECOVER_WORKER_TASK, async move {
          // worker 私有存储会话（对标 C# InitializeReplayContext 每虚拟子日志
          // 专用会话：并行回放上下文自持会话，无缺位兜底面）
          let worker_session = match target_ref.store.new_session() {
            Ok(session) => session,
            Err(err) => {
              worker_assembly_failure(
                task_idx,
                err.into(),
                &error_slot_clone,
                &is_cancelled_clone,
                &barrier_clone,
              );
              return;
            }
          };
          // 向量执行域会话绑定（对标 C# 重放上下文自持 RespServerSession 的
          // 线程槽形态）：worker 私有会话即本线程重放执行域会话，向量族条目
          // 应用（store_rmw 向量支经 VectorManager 读索引/写透登记表）须见
          // 当前执行域会话。守卫跨整段存活为刻意形态——重放臂属主线程即绑定
          // 线程，绑定与解绑同线程（LIFO 栈纪律），Drop 先还原槽位再随会话
          // 析构；单任务臂 leader 绑定维持不动，同一线程槽单一机制
          let _vector_domain = ActiveVectorSessionGuard::bind(&worker_session);
          let worker_batch = worker_session.enter_batch();
          let worker_storage = StorageSession::new(worker_batch);
          let worker_target = ReplayTarget {
            session: &worker_storage,
            store: Arc::clone(&target_ref.store),
            aof_floor: target_ref.aof_floor.clone(),
          };

          while !is_cancelled_clone.load(Ordering::Acquire) {
            // Rendezvous 1 (ready): 等待 Leader 发布当前页批次
            barrier_clone.signal_work_ready_wait_async().await;

            if is_cancelled_clone.load(Ordering::Acquire) {
              break;
            }

            let page_args = ReplayPageArgs {
              replay_task_idx: task_idx,
              virtual_sublog_idx,
              until_sequence_number,
              as_replica,
              processor: processor_ref,
              aof: aof_ref,
              target: &worker_target,
              batch_context: &batch_clone,
              prefix_consistency_boundary_reached: boundary_ref,
              replayed_record_count: replayed_ref,
            };

            // panic 隔离单点（wbase 监督薄包装）：replay_page 展开即捕获并
            // 归入既有错误链（落 error_slot + 置取消位 + 抵完成闸同序收场，
            // 对标 C# RecoverReplayTask try/catch(Exception) 全捕），线程不再
            // 静默死亡、闸栏不缺员
            let result = match supervise_item(RECOVER_WORKER_TASK, replay_page(page_args)).await {
              Ok(result) => result,
              Err(payload) => Err(AofReplayError::Panicked(payload.text())),
            };

            if let Err(err) = result {
              log::error!("Worker [{task_idx}] 回放异常: {err}");
              *error_slot_clone.lock() = Some(err);
              is_cancelled_clone.store(true, Ordering::Release);
              // 纪元让渡后停车（对标 FLUSH/检查点臂的 suspend_epoch 单机制）：
              // 本 worker 已退出记录处理循环、不再有记录边界让步窗，带着批会话
              // 保护区停靠完成闸会钉死排空屏障——栅栏超时续跑的 Leader 独占段
              // （FLUSH 独占清空）等不到排空即与完成闸互锁冻结
              let _epoch_suspend = worker_target.session.batch.suspend_epoch();
              barrier_clone.signal_work_completed_wait_async().await;
              break;
            }

            // Rendezvous 2 (completed): 页面应用完毕到达完成闸门
            barrier_clone.signal_work_completed_wait_async().await;
          }
        }));

        if let Err(payload) = outcome {
          // 守卫遗漏的守卫外展开（监督已落日志与计数）：按装配失败同面兜底
          // 收场，错误链不断
          worker_assembly_failure(
            task_idx,
            AofReplayError::Panicked(payload.text()),
            &rescue_slot,
            &rescue_cancel,
            &rescue_barrier,
          );
        }
      });

      workers.push(handle);
    }

    workers
  }

  /// 单任务回放门序小核（单点）：commit 帧跳过 → SkipReplay 上界判定 →
  /// ProcessAofRecordInternal → 计数。返回 Ok(false) 表示到达前缀一致上界，
  /// 调用方须立即停止消费；对标 C# 单任务臂（Consume 内 AofReplayTaskCount==1
  /// 分支）以 cts.Cancel 停止，不置 prefixConsistencyBoundaryReached——该标记
  /// 仅由并行 replay_page 置位。
  #[inline]
  async fn replay_one<D: Device>(
    processor: &AofProcessor,
    target: &ReplayTarget<'_, '_, D>,
    virtual_sublog_idx: usize,
    until_sequence_number: i64,
    as_replica: bool,
    record: &WalRecord,
    replayed_record_count: &AtomicU64,
  ) -> Result<bool, AofReplayError> {
    let entry = record.payload.as_slice();
    if waof::is_commit_frame(entry) {
      return Ok(true);
    }
    if let Some((true, _)) =
      record_gate::skip_replay(entry, until_sequence_number, record.address as i64)
    {
      return Ok(false);
    }
    processor
      .process_aof_record_internal(
        virtual_sublog_idx,
        entry,
        as_replica,
        record.address as i64,
        target,
      )
      .await?;
    let count = replayed_record_count.fetch_add(1, Ordering::Relaxed) + 1;
    log_replay_progress(count, record.address as i64);
    Ok(true)
  }

  /// libs/server/AOF/Recover/RecoverLogDriver.cs:Consume
  ///
  /// 单批次/单页消费入口（对齐 C# IBulkLogEntryConsumer.Consume）。
  pub async fn consume<D: Device>(
    &self,
    processor: &AofProcessor,
    aof: &GarnetAppendOnlyFile,
    target: &ReplayTarget<'_, '_, D>,
    batch: Vec<WalRecord>,
    barrier: Option<&DoubleTurnstileBarrier>,
    batch_context: Option<&ReplayBatchContext>,
  ) -> Result<bool, AofReplayError> {
    if batch.is_empty() {
      return Ok(true);
    }

    let replay_task_count = aof.log().replay_task_count();
    if let (Some(barrier), Some(batch_context)) = (barrier, batch_context)
      && replay_task_count > 1
    {
      // Worker 错误已置取消位时拒绝发布新批次（C# leader 两处闸栏等待携带
      // cts.Token：cts 置位即 OCE 逃逸截停扫描）——此刻 Worker 均已退出，
      // 双闸栏再无全员会合可能，续发布即 leader 永挂于 ready 闸
      if batch_context.is_cancelled.load(Ordering::Acquire) {
        return Ok(false);
      }

      *batch_context.records.write() = Arc::new(batch);

      // Rendezvous 1 (ready): 发布批次并会合 Worker 开始重放
      barrier.signal_work_ready_wait_async().await;

      // Rendezvous 2 (completed): 等待全部 Worker 完成本页重放
      barrier.signal_work_completed_wait_async().await;

      if batch_context.is_cancelled.load(Ordering::Acquire) {
        return Ok(false);
      }

      Ok(
        !self
          .prefix_consistency_boundary_reached
          .load(Ordering::Acquire),
      )
    } else {
      // 单任务顺序消费（门序单点复用 replay_one）
      let virtual_sublog_idx = self.physical_sublog_idx * aof.virtual_sublog_per_sublog();
      for record in batch {
        if !Self::replay_one(
          processor,
          target,
          virtual_sublog_idx,
          self.until_sequence_number,
          self.as_replica,
          &record,
          &self.replayed_record_count,
        )
        .await?
        {
          return Ok(false);
        }
      }
      Ok(true)
    }
  }

  /// libs/server/AOF/Recover/RecoverLogDriver.cs:RunAsync
  ///
  /// 扫描 [start, until] 并逐页/逐条目流式消费；返回重放条目数。
  pub async fn run<D: Device + 'static>(
    &self,
    processor: &AofProcessor,
    aof: &GarnetAppendOnlyFile,
    target: &ReplayTarget<'_, '_, D>,
  ) -> Result<u64, AofReplayError> {
    if self.start_address == self.until_address {
      return Ok(0);
    }

    let replay_task_count = aof.log().replay_task_count();
    // 子日志恢复起止日志（libs/server/AOF/Recover/RecoverLogDriver.cs:200
    // LogInformation「Recover sublog [idx] for address range (start,until)」
    // 对位：慢启动可观测面，正常路径起止区间单点输出）
    let recover_start_ticks = now_stopwatch_ticks();
    log::info!(
      "AOF 恢复子日志 [{}] 地址区间 [{}, {}]",
      self.physical_sublog_idx,
      self.start_address,
      self.until_address
    );

    let replayed = if replay_task_count <= 1 {
      // 单任务顺序快路径（对标 C# RunAsync → BulkConsumeAllAsync 以 Consume
      // 为回调的单回调拓扑；门序单点复用 replay_one，逐条内联零分配）
      let virtual_sublog_idx = self.physical_sublog_idx * aof.virtual_sublog_per_sublog();
      let until_sequence_number = self.until_sequence_number;
      let as_replica = self.as_replica;
      let replayed_record_count = &self.replayed_record_count;

      aof
        .log()
        .scan_single_async_with(
          self.physical_sublog_idx,
          self.start_address,
          self.until_address,
          |record| async move {
            Self::replay_one(
              processor,
              target,
              virtual_sublog_idx,
              until_sequence_number,
              as_replica,
              &record,
              replayed_record_count,
            )
            .await
          },
        )
        .await?;

      self.replayed_record_count()
    } else {
      // 页级多 Worker 并行双闸栏重放路径
      // Leader 会话纪元让渡（整并行段持守）：并行段 leader 不做任何记录应用，
      // 其批会话保护区若持守，FLUSH 独占段（flush 链的纪元排空屏障）将被钉死
      // ——排空等待体仅自刷新等待线程的纪元位，leader 停靠闸栏期间其线程位
      // 永不刷新，我等他退区、他等我放栏即互锁。让渡后 leader 会话在并行段
      // 恢复零保护区在途（单任务快路径不受此臂覆盖，语义分界即 replay 分叉点）
      let _leader_epoch_suspend = target.session.batch.suspend_epoch();
      let barrier = Arc::new(DoubleTurnstileBarrier::new(replay_task_count + 1));
      let is_cancelled = Arc::new(AtomicBool::new(false));
      let batch_context = Arc::new(ReplayBatchContext::new(Arc::clone(&is_cancelled)));
      let error_slot = Arc::new(Mutex::new(None));

      let init_args = ParallelReplayInitArgs {
        replay_task_count,
        processor,
        aof,
        target,
        barrier: &barrier,
        batch_context: &batch_context,
        is_cancelled: &is_cancelled,
        error_slot: &error_slot,
      };

      let workers = self.create_and_run_intra_page_parallel_replay_tasks(init_args);

      const BATCH_SIZE: usize = 256;
      let batch_buffer = Arc::new(Mutex::new(Vec::with_capacity(BATCH_SIZE)));

      let barrier_clone = Arc::clone(&barrier);
      let batch_ctx_clone = Arc::clone(&batch_context);
      let batch_buf = Arc::clone(&batch_buffer);

      let scan_res = aof
        .log()
        .scan_single_async_with(
          self.physical_sublog_idx,
          self.start_address,
          self.until_address,
          |record| {
            let barrier_ref = Arc::clone(&barrier_clone);
            let batch_ctx_ref = Arc::clone(&batch_ctx_clone);
            let batch_buf_ref = Arc::clone(&batch_buf);

            async move {
              let mut batch_to_flush = None;
              {
                let mut buf = batch_buf_ref.lock();
                buf.push(record);
                if buf.len() >= BATCH_SIZE {
                  batch_to_flush = Some(replace(&mut *buf, Vec::with_capacity(BATCH_SIZE)));
                }
              }

              if let Some(batch) = batch_to_flush {
                let cont = self
                  .consume(
                    processor,
                    aof,
                    target,
                    batch,
                    Some(&barrier_ref),
                    Some(&batch_ctx_ref),
                  )
                  .await?;
                if !cont {
                  return Ok::<bool, AofReplayError>(false);
                }
              }
              Ok(true)
            }
          },
        )
        .await;

      // 冲刷残留批次（Worker 错误已置取消位时拒绝：此刻 Worker 均已退出，
      // 双闸栏再无全员会合可能，续发布即 leader 永挂于 ready 闸——错误臂
      // 收口的关键闸门，错误须沿 error_slot 经 join 后上抛）
      let remaining = take(&mut *batch_buffer.lock());
      if scan_res.is_ok()
        && !remaining.is_empty()
        && !is_cancelled.load(Ordering::Acquire)
        && !self
          .prefix_consistency_boundary_reached
          .load(Ordering::Acquire)
      {
        let _ = self
          .consume(
            processor,
            aof,
            target,
            remaining,
            Some(&barrier),
            Some(&batch_context),
          )
          .await;
      }

      // 停机 Worker 协程并回收；join Err（守卫遗漏的守卫外展开，如线程
      // 闭包内监督面之外的字段装配 panic）双保险落账上抛，不得静默吞弃
      is_cancelled.store(true, Ordering::Release);
      barrier.notify_all();

      for worker in workers {
        if worker.join().is_err() {
          error_slot
            .lock()
            .get_or_insert_with(|| AofReplayError::Panicked("worker 线程守卫外展开".into()));
        }
      }

      if let Some(err) = error_slot.lock().take() {
        return Err(err);
      }

      scan_res?;

      self.replayed_record_count()
    };

    // 子日志恢复完成日志（C# RunAsync 完成侧对位：重放条数与耗时；
    // 错误臂经 ? / return Err 上抛不抵此行，与 C# 异常收敛同形）
    log::info!(
      "AOF 恢复子日志 [{}] 完成:重放 {replayed} 条,耗时 {} ms",
      self.physical_sublog_idx,
      now_stopwatch_ticks().saturating_sub(recover_start_ticks) / TICKS_PER_MILLISECOND as u64
    );
    Ok(replayed)
  }
}
