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

use compio::{runtime::spawn, time::sleep};
use wbase::{
  future::yield_now,
  pool::EventWorkQueue,
  supervise::{supervise_item, supervise_task},
};

use super::{
  vector_manager::VectorManager,
  vector_manager_locking::{ReadIndexOutcome, domain_prefix, split_registry_key},
};

/// 监督快照里的任务名（wbase::supervise 归组键，INFO bg_task_health 可见）
const QUANT_TASK: &str = "quantization_worker";
/// 逐项监督归组名（量化请求处理体）
const QUANT_ITEM: &str = "quantization_item";

/// 量化流程阶段。
#[derive(Debug, PartialEq, Clone)]
pub enum QuantizationStep {
  Invalid = 0,
  /// 构建量化表 —— 每个 Vector Set 索引同一时刻仅一个任务执行。
  BuildQuantizationTable = 1,
  /// 回填量化向量 —— 同一 Vector Set 索引可由多任务并发分片执行。
  BackfillQuantizedVectors = 2,
}

/// 量化工作项。
pub struct QuantizationState {
  /// 登记表复合键字节（量化通道全程工作在复合登记键域）。
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
  /// （C# 取 Environment.ProcessorCount）。任务体经 wbase [`supervise_task`]
  /// 监督：panic 落日志与监督计数，INFO bg_task_health 可见——worker 静默
  /// 减员不再与空闲不可区分（C# 线程池不减员，同位异常必经 catch）。
  ///
  /// 生命周期形态：句柄就地 `.detach()` 交执行器持有，无返回值。C# 把任务
  /// 存 `quantizationTasks` 数组托管（.NET 任务弃引用不取消；数组另供
  /// Dispose 的 Task.WhenAll 收口，VectorManager.cs:231）——compio
  /// `JoinHandle` Drop 即 `task.cancel`（fire-and-cancel，见 wkv gc
  /// reclaim.rs 纪律注），「返回句柄交调用方托管」语义在 rust 侧必致
  /// 即生即灭；量化 worker 无停机 join 对位要求，detach 即终态形态
  ///（同仓 reclaim / primary_tasks 唯一后台形态），签名无返回在编译期
  /// 挡住「拉起后丢弃句柄」的复发。
  pub fn start_quantization_tasks(self: &Arc<Self>, task_count: usize) {
    let count = if task_count == 0 {
      4
    } else {
      task_count.clamp(1, 64)
    };
    (0..count).for_each(|_| {
      let manager = Arc::clone(self);
      spawn(async move {
        let _ = supervise_task(QUANT_TASK, manager.run_quantization_task_loop()).await;
      })
      .detach();
    });
  }

  /// libs/server/Resp/Vector/VectorManager.Quantization.cs:QuantizationTaskAsync
  ///
  /// worker 协程主循环：由 channel.wait_to_read().await 驱动，逐项以非阻塞锁获取处理；
  /// 锁被占用时协作让步后重试（1:1 对齐 C# 前 16 次 Task.Yield，其后 Task.Delay(1) 异步退避）。
  /// 逐项处理体经 wbase [`supervise_item`] 守卫：panic 按终态弹项进入下项
  ///（对标 C# TryProcessQuantizationRequest catch LogError 后按终态处理），
  /// 杜绝毒项死循环重试与 worker 减员。
  pub async fn run_quantization_task_loop(self: Arc<Self>) {
    let channel = &self.quantization_channel;
    while channel.wait_to_read().await {
      while let Some(state) = channel.try_pop() {
        for attempt in 0u32.. {
          // 会话绑定归处理体内置（[`Self::try_process_quantization_request`]
          // 顶：每次尝试自持专用会话），守卫绝不跨下方的
          // `yield_now()` / `sleep()` 持有
          // panic 臂（Err）：日志已由监督单点落，按终态弹项防毒项死循环
          if matches!(
            supervise_item(QUANT_ITEM, self.try_process_quantization_request(&state)).await,
            Ok(true) | Err(_)
          ) {
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
  /// （自带"需重建则重建"），共享守卫跨建表与回填全程持有；键已删除时静默
  /// 忽略；BuildQuantizationTable 成功后为每个分片调度 BackfillQuantizedVectors。
  /// 返回 true 表示请求已处理（或终态）；false 表示锁竞争需让步重试。
  pub async fn try_process_quantization_request(&self, state: &QuantizationState) -> bool {
    // 每次尝试自持专用会话（对标 C# 量化 worker 的自备 ActiveThreadSession：
    // 非阻塞锁读索引与登记表旁路写透须有当前执行域会话可用），返回即解绑
    let dedicated = self.bind_dedicated_session();
    // 拆解单点取 (域, 用户键)：入通道时即由 registry_key 构造，非法键不可达
    let (domain, user_key) = split_registry_key(&state.key);
    let prefix = domain_prefix(domain);
    // 落条目物理域（自持会话初值恒根域，vns/vdb 均 0；对位 C# worker 显式
    // TrySwitchActiveDatabaseSession(self.dbId)，VectorManager.Quantization.cs:84-88）：
    // 量化全链——登记读内的重建臂、建表训练取样、回填逐 id 读写——经 TLS 会话
    // 前缀寻址记录（vector_store_callbacks 模块头物理落位口径），不落域即全域
    // 错位：非根域集训练取样逐 id 读 miss 静默失训、量化记录写他域读取恒 miss。
    // 直设臂对齐 tiered_demote 单键落域窗（逻辑槽经 version_domain_of 一次换算
    // 固着，版本轨=逻辑域种子，禁反查）；本单点先于读索引覆盖重建/建表/回填
    // 全链，recreate_index_locked 内勿二次落域免双点
    if let Some(session) = dedicated.as_ref() {
      session.set_entry_domain(domain.vns, domain.vdb);
    }
    // 非阻塞锁读（竞争时报告 false 交由调用方让步重试）。守卫 `_lock` 随本
    // async 栈帧跨建表/回填的 `.await` 存活至函数返回——1:1 对齐 C#
    // `using (ReadVectorIndexCore(nonBlocking: true))` 罩住
    // BuildQuantizationTable 与 BackfillQuantizedVectors 全程的锁域：量化期间
    // 并发 DEL 的条带独占锁（ReadForDeleteVectorIndex）被挡在门外，索引不亡、
    // context 不归还，回填目标恒为活集合，杜绝孤儿量化记录与归还复用写穿。
    // 守卫在 async_lock 条带锁形态下跨 await 合法（见 vector_manager_locking
    // 模块头），建表/回填链路经 service 持 Arc 索引直下 provider，不再取同键
    // 条带锁，无重入。
    let (index, _lock) = match self
      .read_vector_index_core(prefix.as_slice(), user_key, true)
      .await
    {
      ReadIndexOutcome::Hit(index, lock) => (index, lock),
      // 索引在请求处理前已被删除，忽略请求
      ReadIndexOutcome::NotFound => return true,
      ReadIndexOutcome::WouldBlock => return false,
      // 重建失败（失败点已记 error 日志）：对齐 C# 量化 worker catch 后记日志、
      // 请求按终态处理；false 语义仅限锁竞争让步，永久性失败返回 false 会死循环重试
      ReadIndexOutcome::Failed => return true,
    };
    let context = index.context;

    match state.step {
      QuantizationStep::BuildQuantizationTable => {
        // 表就绪后为每个分片调度回填（守卫继续持有至本项返回）
        if self.service.build_quantization_table(context).await {
          self
            .quantization_requests_processed
            .fetch_add(1, Ordering::Relaxed);
          // 表就绪后为每个分片调度回填
          let shard_count = self.quantization_task_count.load(Ordering::Relaxed).max(1);
          for i in 0..shard_count {
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
        self
          .service
          .backfill_quantized_vectors(
            context,
            state.step_index,
            self.quantization_task_count.load(Ordering::Relaxed).max(1),
          )
          .await;
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
