//! 在线哈希索引动态扩容状态机 (1:1 对标 C# Garnet IndexResizeSM.cs / IndexResizeSMTask.cs / SplitIndex.cs)

use std::{
  ptr::eq,
  sync::{
    Arc,
    atomic::{AtomicI64, AtomicU8, AtomicUsize, Ordering},
  },
  thread::yield_now,
};

use arc_swap::{ArcSwap, ArcSwapOption};
use compio::runtime::spawn_blocking;
use wbase::addr::is_read_cache;
use wdev::Device;
use windex::{
  Error as WindexError, HashIndex, SPLIT_COMPLETED, SPLIT_IN_PROGRESS, SPLIT_UNSTARTED,
  chunk_count, chunk_offset_for_hash, split_chunk,
};

use super::WedbStore;
use crate::error::{Error, Result};

/// 索引扩容三阶段状态机 libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexResizeSM.cs:IndexResizeSM
/// （严格对标其 NextState 的 Phase: REST -> PREPARE_GROW -> IN_PROGRESS_GROW -> REST）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ResizePhase {
  /// 静止态 / 已完成态：无扩容进行中
  Rest = 0,
  /// 准备扩容态：事务屏障排空阶段，禁止新事务进入旧纪元
  PrepareGrow = 1,
  /// 扩容迁移态：新表就绪，全量后台分裂与按需按哈希分裂并行执行
  InProgressGrow = 2,
}

impl ResizePhase {
  #[inline(always)]
  pub fn from_u8(val: u8) -> Self {
    match val {
      1 => Self::PrepareGrow,
      2 => Self::InProgressGrow,
      _ => Self::Rest,
    }
  }
}

/// 索引动态扩容状态机运行时容器（基于 ArcSwap 零锁读取）
pub struct IndexResizeState {
  /// 当前扩容阶段 (原子状态)
  pub phase: AtomicU8,
  /// 分块分裂锁与状态数组 (0 = 未开始, 1 = 分裂中, 2 = 完成)
  pub split_status: ArcSwap<Vec<AtomicI64>>,
  /// 待完成分裂的分块计数
  pub num_pending_chunks: AtomicUsize,
  /// 扩容迁移期间持有的旧索引表共享句柄
  pub old_index: ArcSwapOption<HashIndex>,
}

impl IndexResizeState {
  pub fn new() -> Self {
    Self {
      phase: AtomicU8::new(ResizePhase::Rest as u8),
      split_status: ArcSwap::from_pointee(Vec::new()),
      num_pending_chunks: AtomicUsize::new(0),
      old_index: ArcSwapOption::empty(),
    }
  }

  #[inline(always)]
  pub fn phase(&self) -> ResizePhase {
    ResizePhase::from_u8(self.phase.load(Ordering::Acquire))
  }

  #[inline(always)]
  pub fn is_growing(&self) -> bool {
    self.phase.load(Ordering::Acquire) == (ResizePhase::InProgressGrow as u8)
  }
}

impl Default for IndexResizeState {
  fn default() -> Self {
    Self::new()
  }
}

impl<D: Device> WedbStore<D> {
  /// 纪元入口统一过 PREPARE_GROW 全事务屏障（对标 C# Garnet
  /// StateMachineDriver.cs:AcquireTransactionVersion 与 TsavoriteThread.cs 操作入口
  /// 的挂起协议——"we DO NOT allow new transactions to start in PREPARE_GROW
  /// (full barrier)"）
  ///
  /// phase 处于 PrepareGrow（活跃事务排空 + 新表构建 + 切表完成的窗口）时以
  /// enter→检查→exit→让步自旋挂起：挂起期间绝不持有纪元保护，否则 grow_index
  /// 的纪元排空永不收敛；phase 离开 PrepareGrow 后持保护返回，保证持桶锁事务
  /// 绝不跨越切表边界悬空。
  pub fn barrier_enter<'a>(
    &'a self,
    participant: &'a wepoch::Participant,
  ) -> wepoch::EpochGuard<'a> {
    loop {
      let guard = participant.enter();
      if self.resize.phase.load(Ordering::Acquire) != ResizePhase::PrepareGrow as u8 {
        return guard;
      }
      drop(guard);
      yield_now();
    }
  }

  /// 当前哈希索引是否正处于在线扩容迁移期
  #[inline(always)]
  pub fn is_growing(&self) -> bool {
    self.resize.is_growing()
  }

  /// 获取当前活跃的哈希索引表引用句柄
  #[inline(always)]
  pub fn active_index(&self) -> Arc<HashIndex> {
    self.index.load_full()
  }

  /// 按需按哈希进行分块分裂（严格对标 Garnet SplitIndex.cs:SplitBuckets）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitIndex.cs:SplitBuckets
  ///
  /// 在 IN_PROGRESS_GROW 阶段，会话操作在访问目标桶前先行按哈希检查所在分块；
  /// 若尚未迁移，当前会话无锁 CAS 抢占该分块的迁移权并执行迁移，消除读写穿透到未迁移新表的空洞。
  ///
  /// 迁移内核错误显式上抛（C# 纯内存直写无异常路径，rust 引入 Result 后不得降级为
  /// 丢弃）：调用方必须中止本次操作——半迁移状态下继续按新表裸写将产生双 tag 与写丢失。
  pub fn split_buckets(&self, hash: u64) -> Result<()> {
    let Some(old_index) = self.resize.old_index.load_full() else {
      return Ok(());
    };

    let old_mask = old_index.mask;
    let num_chunks = chunk_count(old_index.size);
    let chunk_offset = chunk_offset_for_hash(hash, old_mask);

    // 环形遍历所有分块，尝试抢占并分裂未完成分块
    for i in chunk_offset..chunk_offset + num_chunks {
      if self.split_single_chunk(i & (num_chunks - 1), num_chunks, &old_index)? {
        break;
      }
    }

    // 若目标分块正由其他线程分裂中，自旋让步等待其完成 (状态变为 2)
    let target_idx = chunk_offset & (num_chunks - 1);
    let split_status = self.resize.split_status.load();
    if let Some(status) = split_status.get(target_idx) {
      while status.load(Ordering::Acquire) == SPLIT_IN_PROGRESS {
        yield_now();
      }
    }
    Ok(())
  }

  /// 尝试对单个分块执行分裂迁移（CAS 抢占排他执行权，零锁竞争）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitIndex.cs:SplitSingleBucket
  ///
  /// 返回是否由本次调用完成了该分块迁移；迁移内核错误时状态回滚 SPLIT_UNSTARTED
  /// （交还会话协同重试，绝不标记 SPLIT_COMPLETED）并上抛。
  pub fn split_single_chunk(
    &self,
    chunk_idx: usize,
    num_chunks: usize,
    old_index: &HashIndex,
  ) -> Result<bool> {
    let split_status = self.resize.split_status.load();
    let Some(status) = split_status.get(chunk_idx) else {
      return Ok(false);
    };

    if status
      .compare_exchange(
        SPLIT_UNSTARTED,
        SPLIT_IN_PROGRESS,
        Ordering::AcqRel,
        Ordering::Acquire,
      )
      .is_ok()
    {
      let new_index = self.active_index();
      if eq(Arc::as_ptr(&new_index), old_index as *const HashIndex) {
        // 相位已发布而新表尚未切上的过渡窗：活跃表仍是迁移源自身，不迁移不抢分块
        // （状态回滚 UNSTARTED 交还全量迁移），调用方按旧表快照操作——旧表是完整
        // 真源，切表后的全量迁移将旧表整体搬移至新表
        status.store(SPLIT_UNSTARTED, Ordering::Release);
        return Ok(false);
      }
      let head_addr = self.hlog.head_address();

      let get_record_hash_and_prev = |addr: u64| -> Option<(u64, u64)> {
        // None（滑窗/不可判读）折断开链：迁移分块保守跳过，全量迁移兜底
        let main_addr = if is_read_cache(addr) {
          self.read_cache.skip_read_cache(addr).unwrap_or(0)
        } else {
          addr
        };
        if main_addr >= head_addr {
          self
            .hlog
            .with_memory_record(main_addr, |rec| {
              Ok((whasher::fast_hash(rec.key()), rec.prev_address()))
            })
            .ok()
            .flatten()
        } else {
          None
        }
      };

      if let Err(e) = split_chunk(
        old_index,
        &new_index,
        chunk_idx,
        num_chunks,
        get_record_hash_and_prev,
        |prev_addr, target_bit| {
          trace_back_for_other_chain_start(
            prev_addr,
            target_bit,
            head_addr,
            old_index.size,
            new_index.mask,
            get_record_hash_and_prev,
          )
        },
      ) {
        // 迁移失败：状态回滚 UNSTARTED、不递减 num_pending_chunks，绝不无条件标记
        // SPLIT_COMPLETED（部分条目未迁即完成将对该分块的会话协同与读路径永久失联）；
        // 错误上抛由 grow_index 中止扩容并留痕
        status.store(SPLIT_UNSTARTED, Ordering::Release);
        return Err(e.into());
      }

      status.store(SPLIT_COMPLETED, Ordering::Release);
      self
        .resize
        .num_pending_chunks
        .fetch_sub(1, Ordering::Release);
      return Ok(true);
    }

    Ok(false)
  }

  /// 全量后台分块分裂驱动（遇错即返，由 grow_index 中止扩容）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitIndex.cs:SplitAllBuckets
  fn split_all_buckets(&self, old_index: &HashIndex, num_chunks: usize) -> Result<()> {
    for i in 0..num_chunks {
      self.split_single_chunk(i, num_chunks, old_index)?;
    }

    while self.resize.num_pending_chunks.load(Ordering::Acquire) > 0 {
      yield_now();
    }
    Ok(())
  }

  /// 执行在线哈希索引扩容（容量翻倍，1:1 对标 Garnet Tsavorite.cs:GrowIndexAsync）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:GrowIndexAsync
  /// libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexResizeSMTask.cs:GlobalBeforeEnteringState
  /// libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexResizeSMTask.cs:GlobalAfterEnteringState
  ///
  /// 状态机转换流：
  /// 1. `REST -> PREPARE_GROW`：CAS 抢占扩容独占权，并通过纪元屏障等待旧版本活跃事务排空
  ///    （会话入口 [`crate::session::StoreSession::enter_gated`] 在本相位挂起新事务，
  ///    构成全事务屏障，杜绝持桶锁事务跨切表悬空）；
  /// 2. `PREPARE_GROW -> IN_PROGRESS_GROW`：构建 2 倍容量新索引表，先发布相位、后切换
  ///    活跃表句柄——会话拿到新表时 `is_growing()` 恒为真，split_buckets 协同必走，
  ///    切表窄窗闭合；相位发布至切表之间的过渡窗由 [`Self::split_single_chunk`]
  ///    的同表防护覆盖（对标 IndexResizeSMTask.cs:GlobalAfterEnteringState 全屏障语义）；
  /// 3. 分裂迁移所有分块（支持后台全量扫描与前台按需哈希分裂协同推进；迁移内核错误
  ///    显式上抛并中止扩容，绝不静默标记完成）；
  /// 4. `IN_PROGRESS_GROW -> REST`：所有分块完成分裂后，纪元推进并安全释放旧表。
  pub fn grow_index(&self) -> Result<bool> {
    // 1. 进入 PREPARE_GROW
    if self
      .resize
      .phase
      .compare_exchange(
        ResizePhase::Rest as u8,
        ResizePhase::PrepareGrow as u8,
        Ordering::AcqRel,
        Ordering::Acquire,
      )
      .is_err()
    {
      return Ok(false);
    }

    let target_epoch = self.epoch.bump_current_epoch();
    self.epoch.bump_and_wait(target_epoch);

    // 2. 进入 IN_PROGRESS_GROW
    let old_index = self.active_index();
    let new_size = old_index.size.checked_mul(2).ok_or_else(|| {
      self
        .resize
        .phase
        .store(ResizePhase::Rest as u8, Ordering::Release);
      Error::Index(WindexError::InvalidBucketCount(usize::MAX))
    })?;

    let new_index = match HashIndex::new(new_size) {
      Ok(idx) => Arc::new(idx),
      Err(e) => {
        self
          .resize
          .phase
          .store(ResizePhase::Rest as u8, Ordering::Release);
        return Err(e.into());
      }
    };

    let num_chunks = chunk_count(old_index.size);

    let mut status_vec = Vec::with_capacity(num_chunks);
    for _ in 0..num_chunks {
      status_vec.push(AtomicI64::new(SPLIT_UNSTARTED));
    }
    self.resize.split_status.store(Arc::new(status_vec));
    self
      .resize
      .num_pending_chunks
      .store(num_chunks, Ordering::Release);
    self.resize.old_index.store(Some(Arc::clone(&old_index)));

    // 3. 先发布相位、后切换活跃表：屏障释放的会话见新表必伴 is_growing()==true，
    //    分裂协同成为拿到新表的必要前置（顺序颠倒即复现窄窗写丢失）
    self
      .resize
      .phase
      .store(ResizePhase::InProgressGrow as u8, Ordering::Release);
    self.index.store(Arc::clone(&new_index));

    // 4. 执行全量分块迁移；错误即中止扩容——不回滚切表（相位发布后会话已写新表，
    //    回滚即写丢失），清理扩容态回 REST 并显式上抛，未迁条目经 hlog 重放索引重建自愈
    if let Err(e) = self.split_all_buckets(&old_index, num_chunks) {
      log::error!("在线扩容中止：分块迁移失败 ({e})，未迁移分块条目经检查点索引重建恢复");
      self.resize.old_index.store(None);
      self.resize.split_status.store(Arc::new(Vec::new()));
      self
        .resize
        .phase
        .store(ResizePhase::Rest as u8, Ordering::Release);
      return Err(e);
    }

    // 5. 完成扩容，进入 REST 态
    let drain_epoch = self.epoch.bump_current_epoch();
    self.epoch.bump_and_wait(drain_epoch);

    self.resize.old_index.store(None);
    self.resize.split_status.store(Arc::new(Vec::new()));
    self
      .resize
      .phase
      .store(ResizePhase::Rest as u8, Ordering::Release);

    Ok(true)
  }
}

/// 把 grow_index 卸载到 compio 阻塞线程执行（基于 compio 生态的核保护优化，
/// 形态对标 [`crate::range_index::range_index_blocking`]）
///
/// grow_index 的 compio 阻塞线程卸载包装，无独立 c# 对偶（GrowIndexAsync 的
/// 函数级映射唯一见 [`WedbStore::grow_index`]）。
///
/// grow_index 内含纪元排空忙等（Backoff 后期线程睡眠）与全量分块迁移自旋，
/// 大索引可达秒级——thread-per-core 下在 reactor 上直接执行会停摆同核全部任务。
/// 要求调用方处于 compio 运行时上下文。
pub async fn grow_index_blocking<D: Device>(store: Arc<WedbStore<D>>) -> Result<bool> {
  match spawn_blocking(move || store.grow_index()).await {
    Ok(result) => result,
    Err(e) => {
      log::error!("在线扩容阻塞任务异常退出: {e}");
      Err(Error::BlockingJoin(format!("{e}")))
    }
  }
}

/// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitIndex.cs:TraceBackForOtherChainStart
#[inline]
fn trace_back_for_other_chain_start<F>(
  mut curr: u64,
  target_bit: usize,
  head_addr: u64,
  old_size: usize,
  new_mask: usize,
  mut get_record_hash_and_prev: F,
) -> Option<u64>
where
  F: FnMut(u64) -> Option<(u64, u64)>,
{
  while curr >= head_addr {
    if let Some((hash, next_prev)) = get_record_hash_and_prev(curr) {
      let bit = usize::from(((hash as usize) & new_mask) >= old_size);
      if bit == target_bit {
        return Some(curr);
      }
      curr = next_prev;
    } else {
      break;
    }
  }
  (curr > 0 && curr < head_addr).then_some(curr)
}
