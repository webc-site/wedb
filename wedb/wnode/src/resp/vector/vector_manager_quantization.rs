//! 量化工作通道与任务（对标 libs/server/Resp/Vector/VectorManager.Quantization.cs）
//!
//! VADD 在 Q8 等量化器建表前插入向量时，会产生"建表 → 分片回填"两阶段
//! 量化请求；C# 侧经无界 Channel 交由线程池 worker 处理，Rust 侧以
//! [`wbase::pool::EventWorkQueue`]
//! 承接通道语义，worker 协程基于 compio 驱动；锁竞争时的协作让步以 Task.Yield / Task.Delay 退避对标实现。

use std::{
  sync::{Arc, atomic::Ordering},
  time::Duration,
};

use compio::{
  runtime::{JoinHandle, spawn},
  time::sleep,
};
use wbase::{future::yield_now, pool::EventWorkQueue};

use super::{vector_manager::VectorManager, vector_manager_locking::ReadIndexOutcome};

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
pub type QuantizationChannel = EventWorkQueue<QuantizationState>;

use wvector::store::StoreCallbacks;

impl<S: StoreCallbacks> VectorManager<S> {
  /// libs/server/Resp/Vector/VectorManager.Quantization.cs:StartQuantizationTasks
  ///
  /// 启动 worker 协程排空量化通道。`task_count == 0` 时取默认并发度 4
  /// （C# 取 Environment.ProcessorCount）。
  pub fn start_quantization_tasks(self: &Arc<Self>, task_count: usize) -> Vec<JoinHandle<()>> {
    let count = if task_count == 0 {
      4
    } else {
      task_count.clamp(1, 64)
    };
    (0..count)
      .map(|_| {
        let manager = Arc::clone(self);
        spawn(async move {
          manager.run_quantization_task_loop().await;
        })
      })
      .collect()
  }

  /// libs/server/Resp/Vector/VectorManager.Quantization.cs:QuantizationTaskAsync
  ///
  /// worker 协程主循环：由 channel.wait_to_read().await 驱动，逐项以非阻塞锁获取处理；
  /// 锁被占用时协作让步后重试（1:1 对齐 C# 前 16 次 Task.Yield，其后 Task.Delay(1) 异步退避）。
  pub async fn run_quantization_task_loop(self: Arc<Self>) {
    let channel = &self.quantization_channel;
    while channel.wait_to_read().await {
      while let Some(state) = channel.try_pop() {
        for attempt in 0u32.. {
          if self.try_process_quantization_request(&state) {
            break;
          }
          // 锁竞争：协作让步（对齐 C# attempt < 16 => Task.Yield()，其后 Task.Delay(1)）
          if attempt < 16 {
            yield_now().await;
          } else {
            sleep(Duration::from_millis(1)).await;
          }
          if channel.is_closed() {
            return;
          }
        }
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
            if !self.quantization_channel.push(QuantizationState::new(
              state.key.clone(),
              QuantizationStep::BackfillQuantizedVectors,
              i,
            )) {
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
