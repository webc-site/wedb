//! 量化工作通道与任务（对标 libs/server/Resp/Vector/VectorManager.Quantization.cs）
//!
//! VADD 在 Q8 等量化器建表前插入向量时，会产生"建表 → 分片回填"两阶段
//! 量化请求；C# 侧经无界 Channel 交由线程池 worker 处理，Rust 侧以
//! [`super::cleanup::vector_set_cleanup_work_channel::VectorSetCleanupWorkChannel`]
//! 承接通道语义，worker 为 std 线程；锁竞争时的协作让步以通道重投递实现。

use std::{
  sync::{Arc, atomic::Ordering},
  thread,
  time::Duration,
};

use super::{
  cleanup::vector_set_cleanup_work_channel::VectorSetCleanupWorkChannel,
  vector_manager::VectorManager, vector_manager_locking::ReadIndexOutcome,
};

/// 量化流程阶段。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantizationStep {
  Invalid = 0,
  /// 构建量化表 —— 每个 Vector Set 索引同一时刻仅一个任务执行。
  BuildQuantizationTable = 1,
  /// 回填量化向量 —— 同一 Vector Set 索引可由多任务并发分片执行。
  BackfillQuantizedVectors = 2,
}

/// 量化工作项。
#[derive(Debug, Clone)]
pub struct QuantizationState {
  /// Vector Set 键字节。
  pub key: Vec<u8>,
  /// 当前阶段。
  pub step: QuantizationStep,
  /// 分片下标（Backfill 阶段使用）。
  pub step_index: usize,
}

impl QuantizationState {
  /// 构造工作项。
  pub fn new(key: Vec<u8>, step: QuantizationStep, step_index: usize) -> Self {
    Self {
      key,
      step,
      step_index,
    }
  }
}

/// 量化请求工作通道（manager 持有）。
pub type QuantizationChannel = VectorSetCleanupWorkChannel<QuantizationState>;

impl VectorManager {
  /// libs/server/Resp/Vector/VectorManager.Quantization.cs:StartQuantizationTasks
  ///
  /// 启动 worker 线程排空量化通道。`task_count == 0` 时取默认并发度 4
  /// （C# 取 Environment.ProcessorCount）。
  pub fn start_quantization_tasks(self: &Arc<Self>, task_count: usize) {
    let count = task_count.clamp(1, 64);
    for _ in 0..count {
      let manager = Arc::clone(self);
      thread::Builder::new()
        .name("vector-quantization".into())
        .spawn(move || Self::quantization_task_async(&manager))
        .expect("量化 worker 线程创建失败");
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Quantization.cs:QuantizationTaskAsync
  ///
  /// worker 主循环：阻塞等待通道，逐项以非阻塞锁获取处理；
  /// 锁被占用时协作让步后重投递（对齐 C# Task.Yield / Task.Delay(1) 退避）。
  fn quantization_task_async(manager: &Arc<Self>) {
    let channel = &manager.quantization_channel;
    while channel.wait_to_read(250) {
      let Some(state) = channel.try_read() else {
        continue;
      };
      let mut attempt = 0u32;
      loop {
        if manager.try_process_quantization_request(&state) {
          break;
        }
        // 锁竞争：让步退避后重试（前 16 次微退避，其后毫秒级退避）
        thread::sleep(Duration::from_micros(50 * (attempt.min(16) as u64 + 1)));
        if channel.is_completed() {
          return;
        }
        attempt = attempt.saturating_add(1);
      }
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Quantization.cs:TryProcessQuantizationRequest
  ///
  /// 处理单个量化请求：经 `ReadVectorIndexCore(nonBlocking)` 读取索引
  /// （自带"需重建则重建"）；键已删除时静默忽略；BuildQuantizationTable
  /// 成功后为每个分片调度 BackfillQuantizedVectors。
  /// 返回 true 表示请求已处理（或终态）；false 表示锁竞争需让步重试。
  pub fn try_process_quantization_request(&self, state: &QuantizationState) -> bool {
    // 非阻塞锁读（竞争时报告 false 交由调用方让步重试）
    let (index, _lock) = match self.read_vector_index_core(&state.key, true) {
      ReadIndexOutcome::Hit(index, lock) => (index, lock),
      // 索引在请求处理前已被删除，忽略请求
      ReadIndexOutcome::NotFound => return true,
      ReadIndexOutcome::WouldBlock => return false,
    };
    let context = index.context;

    match state.step {
      QuantizationStep::BuildQuantizationTable => {
        if self.service.build_quantization_table(context) {
          self
            .quantization_requests_processed
            .fetch_add(1, Ordering::Relaxed);
          // 表就绪后为每个分片调度回填
          for i in 0..self.quantization_task_count.max(1) {
            if !self
              .quantization_channel
              .try_publish(QuantizationState::new(
                state.key.clone(),
                QuantizationStep::BackfillQuantizedVectors,
                i,
              ))
            {
              log::warn!("回填量化向量任务发布失败");
            }
          }
        }
      }
      QuantizationStep::BackfillQuantizedVectors => {
        self.service.backfill_quantized_vectors(
          context,
          state.step_index,
          self.quantization_task_count.max(1),
        );
        self
          .quantization_backfills_processed
          .fetch_add(1, Ordering::Relaxed);
      }
      QuantizationStep::Invalid => {
        log::error!("量化请求包含未知阶段: {:?}", state.step);
      }
    }
    true
  }
}

#[cfg(test)]
mod tests {
  use super::{super::vector_manager::VectorManagerOptions, *};

  #[test]
  fn quantization_pipeline_processes_requests() {
    let manager = VectorManager::new(VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    });

    // 恢复态的 Q8 索引桩记录（ptr=0）：worker 读取时自动重建原生索引
    let key = b"myset".to_vec();
    let record = super::super::vector_manager_index::Index {
      context: 8,
      index_ptr: 0,
      dimensions: 2,
      reduce_dims: 0,
      num_links: 4,
      build_exploration_factor: 32,
      quant_type: super::super::vector_types::VectorQuantType::Q8,
      distance_metric: super::super::vector_types::VectorDistanceMetricType::L2,
      flags: super::super::vector_types::VectorSetFlags::NONE,
    };
    manager
      .key_index_registry
      .lock()
      .insert(key.clone(), record.to_bytes());

    // 发布建表请求
    assert!(
      manager
        .quantization_channel
        .try_publish(QuantizationState::new(
          key.clone(),
          QuantizationStep::BuildQuantizationTable,
          0
        ))
    );

    // 同步排空（worker 线程语义的确定性等价）
    while let Some(state) = manager.quantization_channel.try_read() {
      assert!(manager.try_process_quantization_request(&state));
    }

    // 建表至少 1 次（重建路径可能按 C# requestQuantization 语义追加建表请求）
    assert!(
      manager
        .quantization_requests_processed
        .load(Ordering::Relaxed)
        >= 1
    );
    assert!(
      manager
        .quantization_backfills_processed
        .load(Ordering::Relaxed)
        > 0
    );

    // 键已删除的请求静默忽略
    let stale = QuantizationState::new(
      b"gone".to_vec(),
      QuantizationStep::BuildQuantizationTable,
      0,
    );
    assert!(manager.try_process_quantization_request(&stale));

    // 锁竞争 → 报告 false（调用方让步重试）
    let _held = manager.vector_set_locks.acquire_exclusive(&key);
    let contended = QuantizationState::new(key, QuantizationStep::BuildQuantizationTable, 0);
    assert!(!manager.try_process_quantization_request(&contended));
  }

  #[test]
  fn quantization_step_codes() {
    assert_eq!(QuantizationStep::Invalid as i32, 0);
    assert_eq!(QuantizationStep::BuildQuantizationTable as i32, 1);
    assert_eq!(QuantizationStep::BackfillQuantizedVectors as i32, 2);
  }
}
