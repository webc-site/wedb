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
use wdev::Device;

use super::{
  double_turnstile_barrier::DoubleTurnstileBarrier,
  recover_replay_task::{ReplayPageArgs, replay_page},
};
use crate::aof::{
  aof_processor::{AofProcessor, AofReplayError, ReplayTarget},
  garnet_append_only_file::GarnetAppendOnlyFile,
  record_gate,
};

/// 页面批处理上下文（供 Leader 与各 Worker 共享当前页数据）。
pub struct ReplayBatchContext {
  /// 当前批次记录切片。
  pub records: RwLock<Arc<Vec<WalRecord>>>,
  /// 是否为最后一批。
  pub is_last: AtomicBool,
}

impl Default for ReplayBatchContext {
  fn default() -> Self {
    Self::new()
  }
}

impl ReplayBatchContext {
  pub fn new() -> Self {
    Self {
      records: RwLock::new(Arc::new(Vec::new())),
      is_last: AtomicBool::new(false),
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
  ) -> Self {
    Self {
      physical_sublog_idx,
      start_address,
      until_address,
      until_sequence_number,
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
    let mut workers = Vec::with_capacity(replay_task_count);
    let virtual_sublog_base = self.physical_sublog_idx * virtual_sublog_per_sublog(args.aof);

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

        let rt = Runtime::new().unwrap();
        rt.block_on(async {
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
              processor: processor_ref,
              aof: aof_ref,
              target: target_ref,
              batch_context: &batch_clone,
              prefix_consistency_boundary_reached: boundary_ref,
              replayed_record_count: replayed_ref,
            };

            let result = replay_page(page_args).await;

            if let Err(err) = result {
              log::error!("Worker [{task_idx}] 回放异常: {err}");
              *error_slot_clone.lock() = Some(err);
              is_cancelled_clone.store(true, Ordering::Release);
              barrier_clone.signal_work_completed_wait_async().await;
              break;
            }

            // Rendezvous 2 (completed): 页面应用完毕到达完成闸门
            barrier_clone.signal_work_completed_wait_async().await;
          }
        });
      });

      workers.push(handle);
    }

    workers
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
      *batch_context.records.write() = Arc::new(batch);

      // Rendezvous 1 (ready): 发布批次并会合 Worker 开始重放
      barrier.signal_work_ready_wait_async().await;

      // Rendezvous 2 (completed): 等待全部 Worker 完成本页重放
      barrier.signal_work_completed_wait_async().await;

      Ok(
        !self
          .prefix_consistency_boundary_reached
          .load(Ordering::Acquire),
      )
    } else {
      // 单任务顺序消费
      let virtual_sublog_idx = self.physical_sublog_idx * virtual_sublog_per_sublog(aof);
      for record in batch {
        let entry = record.payload.as_slice();
        if waof::is_commit_frame(entry) {
          continue;
        }
        if let Some((true, _)) =
          record_gate::skip_replay(entry, self.until_sequence_number, record.address as i64)
        {
          self
            .prefix_consistency_boundary_reached
            .store(true, Ordering::Release);
          return Ok(false);
        }
        processor
          .process_aof_record_internal(
            virtual_sublog_idx,
            entry,
            true,
            record.address as i64,
            target,
          )
          .await?;
        self.replayed_record_count.fetch_add(1, Ordering::Relaxed);
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

    if replay_task_count <= 1 {
      // 单任务顺序快路径
      let virtual_sublog_idx = self.physical_sublog_idx * virtual_sublog_per_sublog(aof);
      let until_sequence_number = self.until_sequence_number;

      aof
        .log()
        .scan_single_async_with(
          self.physical_sublog_idx,
          self.start_address,
          self.until_address,
          |record| async move {
            let entry = record.payload.as_slice();
            if waof::is_commit_frame(entry) {
              return Ok(true);
            }
            if let Some((true, _)) =
              record_gate::skip_replay(entry, until_sequence_number, record.address as i64)
            {
              return Ok::<bool, AofReplayError>(false);
            }
            processor
              .process_aof_record_internal(
                virtual_sublog_idx,
                entry,
                true,
                record.address as i64,
                target,
              )
              .await?;
            self.replayed_record_count.fetch_add(1, Ordering::Relaxed);
            Ok(true)
          },
        )
        .await?;

      Ok(self.replayed_record_count())
    } else {
      // 页级多 Worker 并行双闸栏重放路径
      let barrier = Arc::new(DoubleTurnstileBarrier::new(replay_task_count + 1));
      let batch_context = Arc::new(ReplayBatchContext::new());
      let is_cancelled = Arc::new(AtomicBool::new(false));
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

      // 冲刷残留批次
      let remaining = take(&mut *batch_buffer.lock());
      if scan_res.is_ok()
        && !remaining.is_empty()
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

      // 停机 Worker 协程并回收
      is_cancelled.store(true, Ordering::Release);
      barrier.notify_all();

      for worker in workers {
        let _ = worker.join();
      }

      if let Some(err) = error_slot.lock().take() {
        return Err(err);
      }

      scan_res?;

      Ok(self.replayed_record_count())
    }
  }
}

/// 单子日志的虚拟子日志数（路由换算）。
fn virtual_sublog_per_sublog(aof: &GarnetAppendOnlyFile) -> usize {
  aof.virtual_sublog_count() / aof.log().size().max(1)
}
