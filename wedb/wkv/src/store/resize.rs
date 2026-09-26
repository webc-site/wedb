//! 在线哈希索引动态扩容状态机 (1:1 对标 C# Garnet IndexResizeSM.cs / IndexResizeSMTask.cs / SplitIndex.cs)

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicI64, AtomicU8, AtomicUsize, Ordering},
  },
  thread::{sleep, yield_now},
  time::{Duration, Instant},
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

/// 索引扩容与检查点共槽状态机 libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexResizeSM.cs:IndexResizeSM
/// （严格对标其 NextState 的 Phase: REST -> PREPARE_GROW -> IN_PROGRESS_GROW -> REST；
/// Checkpoint 为 wedb 扩展相位，对标 C# StateMachineDriver.cs:164-166 单槽注册——
/// C# 扩容与检查点两类状态机共用同一驱动器槽位 `Interlocked.CompareExchange(ref
/// stateMachine, sm, null)` 互斥，rust 无独立驱动器，以同槽 Checkpoint 相位复刻）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ResizePhase {
  /// 静止态 / 已完成态：无扩容进行中
  Rest = 0,
  /// 准备扩容态：事务屏障排空阶段，禁止新事务进入旧纪元
  PrepareGrow = 1,
  /// 扩容迁移态：新表就绪，全量后台分裂与按需按哈希分裂并行执行
  InProgressGrow = 2,
  /// 检查点临界态：检查点自索引快照至 flush 全程独占，扩容入口 CAS 失败即拒
  Checkpoint = 3,
}

impl ResizePhase {
  #[inline(always)]
  pub fn from_u8(val: u8) -> Self {
    match val {
      1 => Self::PrepareGrow,
      2 => Self::InProgressGrow,
      3 => Self::Checkpoint,
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
  /// 活跃事务计数（对标 C# StateMachineDriver.cs:NumActiveTransactions）：
  /// 事务在屏障注册时递增、释放全部桶闩后递减；grow_index 于 PREPARE_GROW
  /// 排空该计数方可切表，确证旧表桶锁无事务跨版本悬空
  pub num_active_txns: AtomicUsize,
  /// 索引撕裂待重建显式标记（票 zcode-r135c-rehash 案二）：grow_index 分块
  /// 迁移内核错误中止后置位——活跃表停在带未迁残片的新表，未迁分块键在线
  /// 不可见。标记存续期内索引检查点入口 `ensure_not_growing` 拒发快照
  /// （宁试后重试不静默丢数，与索引预算耗尽显式上抛教义一致），杜绝撕裂空
  /// 索引被落盘固化成永久缺失；[`WedbStore::rebuild_index_from_hlog`] 成功
  /// 即消标记（失败保留，待下次重建成功或重启全量重放）
  pub index_torn: AtomicBool,
}

impl IndexResizeState {
  pub fn new() -> Self {
    Self {
      phase: AtomicU8::new(ResizePhase::Rest as u8),
      split_status: ArcSwap::from_pointee(Vec::new()),
      num_pending_chunks: AtomicUsize::new(0),
      old_index: ArcSwapOption::empty(),
      num_active_txns: AtomicUsize::new(0),
      index_torn: AtomicBool::new(false),
    }
  }

  #[inline(always)]
  pub fn phase(&self) -> ResizePhase {
    ResizePhase::from_u8(self.phase.load(Ordering::Acquire))
  }

  /// 扩容（含准备期）对外互斥可见性：PrepareGrow 与 InProgressGrow 均为扩容态
  ///
  /// 准备期即对外可见是检查点入口 `ensure_not_growing` 拦截的前提：事务屏障
  /// 排空 + 新表构建窗口与检查点快照窗口重叠同样构成撕裂源
  #[inline(always)]
  pub fn is_growing(&self) -> bool {
    matches!(
      self.phase(),
      ResizePhase::PrepareGrow | ResizePhase::InProgressGrow
    )
  }

  /// 事务屏障单次注册尝试（对标 C#
  /// libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/StateMachineDriver.cs:AcquireTransactionVersion
  /// 的 PREPARE_GROW 拦截 + IncrementActiveTransactions，无让步等待版）：
  ///
  /// 相位非 PrepareGrow 时原子递增活跃事务计数，并按 SeqCst store-load
  /// 「先递增后复查」与 grow_index 的「先置相位后查计数」构成 Dekker 配对，
  /// 杜绝「事务刚过屏障尚未计数、扩容恰见计数归零即切表」的竞态窗口；
  /// 复查命中 PrepareGrow 则回退注册返回 false（交调用方让步重试）。
  /// 注册成功后本事务钉定的索引版本在注销前绝不会被切换。
  #[inline]
  pub fn try_acquire_txn(&self) -> bool {
    if self.phase.load(Ordering::SeqCst) == ResizePhase::PrepareGrow as u8 {
      return false;
    }
    self.num_active_txns.fetch_add(1, Ordering::SeqCst);
    if self.phase.load(Ordering::SeqCst) != ResizePhase::PrepareGrow as u8 {
      return true;
    }
    self.num_active_txns.fetch_sub(1, Ordering::SeqCst);
    false
  }

  /// 事务屏障注销（对标 C# StateMachineDriver.cs:EndTransaction）：
  /// 事务在 unlock_all_keys 释放全部桶闩后递减，扩容见计数归零即确证
  /// 旧表锁面已净
  ///
  /// 阻塞过 PrepareGrow 的线程臂自旋在事务侧 `wtxn::TxnLockTable::acquire_txn`
  /// （本域只出入口，不持线程让步策略）；compio 态用 [`Self::try_acquire_txn`]
  /// 单次尝试 + 慢臂重驱。
  #[inline]
  pub fn release_txn(&self) {
    self.num_active_txns.fetch_sub(1, Ordering::SeqCst);
  }
}

impl Default for IndexResizeState {
  fn default() -> Self {
    Self::new()
  }
}

impl<D: Device> WedbStore<D> {
  /// 纪元入口统一过 PREPARE_GROW 全事务屏障（对标 C# Garnet
  /// StateMachineDriver.cs 的 AcquireTransactionVersion 与 TsavoriteThread.cs 操作入口
  /// 的挂起协议——"we DO NOT allow new transactions to start in PREPARE_GROW
  /// (full barrier)"）
  ///
  /// phase 处于 PrepareGrow（活跃事务排空 + 新表构建 + 切表完成的窗口）时以
  /// enter→检查→exit→让步自旋挂起：挂起期间绝不持有纪元保护，否则 grow_index
  /// 的纪元排空永不收敛；phase 离开 PrepareGrow 后持保护返回，保证持桶锁事务
  /// 绝不跨越切表边界悬空。
  ///
  /// 唯一放行例外：PrepareGrow 期间活跃事务计数仍大于 0。此时屏障后必是在收尾
  /// 的旧事务（含其会话操作）——它们钉锁旧表并等待释放桶闩，而 grow_index 正
  /// 等待其计数归零；若这里再拦阻其会话操作即「扩容等事务排空、事务等屏障放行」
  /// 的循环死锁。放行触达的仍是旧表（切表在计数排空 + 纪元排空之后），其漏网
  /// 在途操作由切表前的纪元 bump_and_wait 兜底等净。
  pub fn barrier_enter<'a>(
    &'a self,
    participant: &'a wepoch::Participant,
  ) -> wepoch::EpochGuard<'a> {
    loop {
      let guard = participant.enter();
      if self.resize.phase.load(Ordering::SeqCst) != ResizePhase::PrepareGrow as u8
        || self.resize.num_active_txns.load(Ordering::SeqCst) > 0
      {
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
  ///
  /// 等待判据 `!= SPLIT_COMPLETED`（C# 为 `== SPLIT_IN_PROGRESS`，其等待退出即完成
  /// 的不变量由 SplitSingleBucket 状态单向 0→1→2 保证；rust 迁移内核有错误回滚
  /// 1→0，同款判据会对回滚态放行）——SPLIT_UNSTARTED 回环重入环形遍历重新抢占，
  /// 兑现「等待退出即该分块迁移完成」的同款不变量。
  pub fn split_buckets(&self, hash: u64) -> Result<()> {
    let Some(old_index) = self.resize.old_index.load_full() else {
      return Ok(());
    };

    let old_mask = old_index.mask;
    let num_chunks = chunk_count(old_index.size);
    let chunk_offset = chunk_offset_for_hash(hash, old_mask);
    let target_idx = chunk_offset & (num_chunks - 1);

    loop {
      // 环形遍历所有分块，尝试抢占并分裂未完成分块（UNSTARTED 含他人迁移错误
      // 回滚态，一并抢占重试；重试再败错误原样上抛，绝不放行）
      for i in chunk_offset..chunk_offset + num_chunks {
        if self.split_single_chunk(i & (num_chunks - 1), num_chunks, &old_index)? {
          break;
        }
      }

      // 等待判据 != SPLIT_COMPLETED：回滚出的 SPLIT_UNSTARTED 严禁放行（放行即
      // 等待会话对未迁移桶加闩/建槽），自旋让步后回环重入环形遍历；SPLIT_COMPLETED
      // 或状态数组已撤除（扩容收口，观测面与入口 old_index 判空一致）方为合法退出
      let split_status = self.resize.split_status.load();
      match split_status
        .get(target_idx)
        .map(|s| s.load(Ordering::Acquire))
      {
        Some(SPLIT_COMPLETED) | None => return Ok(()),
        Some(_) => yield_now(),
      }
    }
  }

  /// 尝试对单个分块执行分裂迁移（CAS 抢占排他执行权，零锁竞争）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitIndex.cs:SplitSingleBucket
  ///
  /// 返回是否由本次调用完成了该分块迁移；迁移内核错误时状态回滚 SPLIT_UNSTARTED
  /// （交还会话协同重试，绝不标记 SPLIT_COMPLETED）并上抛。
  ///
  /// 不变量（对标 C# 切表严格先于相位发布的全屏障协议）：相位进入 IN_PROGRESS_GROW
  /// 即活跃表恒为新表，不存在「相位已发布而活跃表仍是迁移源」的观测态，本函数
  /// 无同表回滚旁路——未完成迁移的分块必须被当前会话抢占执行或自旋等待至完成。
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
      let head_addr = self.hlog.head_address();
      // 分裂期死条目 begin 门（防膨胀有意偏差，登记于 doc/zh/deviations.md：
      // C# SplitChunk 只门 HeadAddress、分裂期不滤死条目，靠查找惰性清退兜底；
      // rust 额外按 begin 过滤，杜绝物理截断死记录双写挤占新表槽位与伪分配溢出桶）
      let begin_addr = self.hlog.begin_address();

      let get_record_hash_and_prev = |addr: u64| -> Option<(u64, u64)> {
        // RC 条目折 0 为防御残臂（wkv-growsplit-rc-evicted-dead-slot-doublewrite
        // 订正，旧注释「保守跳过、全量迁移兜底」失实——全量迁移就是这些分块
        // 本身，None 并无后续修复通道）：折 0 后 0 < head_addr 恒使本函数返
        // None，落 split.rs:split_single_bucket else 臂把含 RC 位的死地址原样
        // 双写新表左右子桶，该键后续读 Retry 耗尽预算上抛 LockTimeout。滑出
        // RC 条目现已不可达本臂：驱逐清洗对注册时捕获的活跃表与 resize.old_index
        // 迁移源表双表并洗（read_cache/append.rs:pump_close_barrier），且切表前
        // 武装的清洗被 grow_index 步骤 1b bump_and_wait 强制收割——迁移读旧表
        // 槽位时其 RC 前缀恒已恢复为主日志地址，skip_read_cache 解析成功。保留
        // unwrap_or(0) 仅兜链尽/不可判读的真实 None 形态（对标 C# SkipReadCache
        // 的 RestartChain 已由清洗侧收口，见 cleanse.rs 走查不变式注释）
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
        begin_addr,
        get_record_hash_and_prev,
        |prev_addr, target_bit| {
          trace_back_for_other_chain_start(
            prev_addr,
            target_bit,
            head_addr,
            begin_addr,
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
  ///
  /// 尾段 pending 等待附带有限重扫（C# 无此需要：SplitSingleBucket 无错误路径，
  /// 状态单向保证等待收敛；rust 回滚态分块若无重扫，pending 永不递减——静默哈希
  /// 区间下驱动线程永久自旋、grow_index 永不返回）
  fn split_all_buckets(&self, old_index: &HashIndex, num_chunks: usize) -> Result<()> {
    for i in 0..num_chunks {
      self.split_single_chunk(i, num_chunks, old_index)?;
    }

    // 等待所有分块迁移完成（1:1 对标 C# SplitIndex.cs:26-27 的
    // `while (numPendingChunksToBeSplit > 0) Thread.Yield();`）：
    // 兼顾前台协同迁移耗时，超时设为 10s 并每轮 sleep 1ms，杜绝微秒级让步耗尽误判
    let start = Instant::now();
    let mut round = 0usize;
    loop {
      if self.resize.num_pending_chunks.load(Ordering::Acquire) == 0 {
        return Ok(());
      }

      // 重扫一圈：对并发会话迁移失败回滚出的 SPLIT_UNSTARTED 重试抢占迁移
      let split_status = self.resize.split_status.load();
      for (i, status) in split_status.iter().enumerate().take(num_chunks) {
        if status.load(Ordering::Acquire) == SPLIT_UNSTARTED {
          self.split_single_chunk(i, num_chunks, old_index)?;
        }
      }

      if self.resize.num_pending_chunks.load(Ordering::Acquire) == 0 {
        return Ok(());
      }

      if start.elapsed() > Duration::from_secs(10) {
        for (i, status) in split_status.iter().enumerate().take(num_chunks) {
          let st = status.load(Ordering::Acquire);
          if st != SPLIT_COMPLETED {
            log::error!(
              "分块 {i}/{num_chunks} 迁移超时: 状态={st} (0=UNSTARTED, 1=IN_PROGRESS, 2=COMPLETED)"
            );
          }
        }
        return Err(Error::GrowRescanExhausted(
          self.resize.num_pending_chunks.load(Ordering::Acquire),
        ));
      }

      round += 1;
      if round <= 5 {
        yield_now();
      } else {
        sleep(Duration::from_millis(1));
      }
    }
  }

  /// 执行在线哈希索引扩容（容量翻倍，1:1 对标 Garnet Tsavorite.cs:GrowIndexAsync）
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:GrowIndexAsync
  /// libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexResizeSMTask.cs:GlobalBeforeEnteringState
  /// libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexResizeSMTask.cs:GlobalAfterEnteringState
  ///
  /// 状态机步进函数同挂此处（REST → PREPARE_GROW → IN_PROGRESS_GROW → REST 四次
  /// 相位 CAS 即 NextState 转移的展开）：
  /// libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/IndexResizeSM.cs:NextState
  ///
  /// 状态机转换流：
  /// 1. `REST -> PREPARE_GROW`：CAS 抢占扩容独占权（phase 处于 Checkpoint 态时
  ///    CAS 天然失败返回 `Ok(false)`——检查点临界期扩容发起即时拒绝，对标 C#
  ///    Tsavorite.cs:857 `GrowIndexAsync` 经 StateMachineDriver 单槽注册被占用即
  ///    返回 false 的互斥语义）；先让步等待活跃事务计数归零（对标 C#
  ///    IndexResizeSMTask.cs:GlobalAfterEnteringState(PREPARE_GROW) 的
  ///    TrackLastVersion 排空与 GlobalBeforeEnteringState(IN_PROGRESS_GROW) 的
  ///    GetNumActiveTransactions == 0 硬断言），再经纪元 bump_and_wait 等净屏障
  ///    放行窗口的在途会话操作——旧表桶锁全部释放后方可切表；新事务经
  ///    [`IndexResizeState::try_acquire_txn`] 在本相位挂起（全事务屏障，对标
  ///    StateMachineDriver.cs 的 AcquireTransactionVersion），杜绝持桶锁事务跨切表悬空；
  /// 2. `PREPARE_GROW -> IN_PROGRESS_GROW`：构建 2 倍容量新索引表，屏障挂起下
  ///    先切换活跃表句柄、后发布相位（严格对标 C# StateMachineDriver.cs:220-234——
  ///    GlobalBeforeEnteringState 内完成新表初始化与 `resizeInfo.version` 翻转，
  ///    随后 GlobalStateMachineStep 才 "Write new phase"）——排空完成的 PrepareGrow
  ///    拦阻期内无任何操作可触达索引，屏障/事务释放者所见新表必伴 `is_growing()`
  ///    为真，split_buckets 协同必走；相位发布即新表就绪，未迁分块由
  ///    [`Self::split_buckets`] 抢占迁移或自旋等待至完成，绝无「按旧表快照写、
  ///    切表后穿透未迁移新桶」的旁路；且新注册事务钉定的锁面版本与数据版本
  ///    自此再无错缝窗口；
  /// 3. 分裂迁移所有分块（支持后台全量扫描与前台按需哈希分裂协同推进；迁移内核错误
  ///    显式上抛并中止扩容，绝不静默标记完成；中止臂先挂索引撕裂待重建标记再收口
  ///    状态机，标记存续期索引检查点被拒、未迁条目待在线收口重建，见 [`Self::
  ///    rebuild_index_from_hlog`]）；
  /// 4. `IN_PROGRESS_GROW -> REST`：先原子发布 Rest 闭环状态机，纪元推进完成
  ///    bump_and_wait 排空屏障后，再清空 old_index 与 split_status 资源——状态机
  ///    生命周期先收口、后回收底层依赖，并发会话绝不观测空旧表配活跃相位的撕裂态。
  pub fn grow_index(&self) -> Result<bool> {
    // 1. 进入 PREPARE_GROW（SeqCst：与事务注册端 try_acquire_txn 的
    //    fetch_add→load 构成 register-recheck Dekker 配对）
    if self
      .resize
      .phase
      .compare_exchange(
        ResizePhase::Rest as u8,
        ResizePhase::PrepareGrow as u8,
        Ordering::SeqCst,
        Ordering::SeqCst,
      )
      .is_err()
    {
      return Ok(false);
    }

    // 1a. 活跃事务排空：旧表桶锁必须全部释放（计数递减在 unlock 之后），
    //     方可进入切表流程。PrepareGrow 拦阻期内新注册恒被 try_acquire_txn 回绝，
    //     计数在本相位只降不增，故排空必收敛（对标 C# full barrier 无死锁论证）
    while self.resize.num_active_txns.load(Ordering::SeqCst) > 0 {
      yield_now();
    }
    // 1b. 纪元排空收尾：等净排空窗口内经屏障放行例外（计数 > 0）触达旧表的
    //     在途会话操作，此后到相位发布前索引触达面完全静默
    let target_epoch = self.epoch.bump_current_epoch();
    self.epoch.bump_and_wait(target_epoch);

    // 2. 构建新表（失败复位 Rest 上抛）——本函数内相位字读写一律 SeqCst：
    //    相位是全状态机槽（Rest / PrepareGrow / InProgressGrow / Checkpoint），
    //    复位写同入 SeqCst 全序，才不留「SeqCst 读到被覆盖前的陈旧 Rest」的
    //    推理缝，事务注册端复查据此恒见已生效相位
    let old_index = self.active_index();
    let new_size = old_index.size.checked_mul(2).ok_or_else(|| {
      self
        .resize
        .phase
        .store(ResizePhase::Rest as u8, Ordering::SeqCst);
      Error::Index(WindexError::InvalidBucketCount(usize::MAX))
    })?;

    let new_index = match HashIndex::new(new_size) {
      Ok(idx) => Arc::new(idx),
      Err(e) => {
        self
          .resize
          .phase
          .store(ResizePhase::Rest as u8, Ordering::SeqCst);
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

    // 3. 屏障挂起下先切表、后发相位：PrepareGrow 全事务屏障（活跃事务已排空、
    //    新事务注册被回绝并被 barrier_enter 拦阻）保证两写之间无任何会话可观测
    //    中间态，相位发布即新表就绪——进入会话所见活跃表恒为新表，split_buckets
    //    协同必走且绝无同表旁路；新注册事务 pin 到的即已切上的新表，钉锁版本与
    //    数据版本零错缝（顺序颠倒即复现「事务钉旧表、读写落新表」与过渡窗写丢失）
    self.index.store(Arc::clone(&new_index));
    self
      .resize
      .phase
      .store(ResizePhase::InProgressGrow as u8, Ordering::SeqCst);

    // 4. 执行全量分块迁移；错误即中止扩容——不回滚切表（相位发布后会话已写新表，
    //    回滚即写丢失），先挂撕裂待重建标记、再发布 Rest 闭环状态机、清理扩容态并
    //    显式上抛（票 zcode-r135c-rehash 案二）。标记先于 Rest 发布：检查点入口
    //    ensure_not_growing 自标记置位起即拒发索引快照，撕裂空表绝无被落盘固化
    //    的窗口；未迁条目由 [`Self::rebuild_index_from_hlog`] 在线收口重建
    //    （grow_index_blocking 于错误返回后即地驱动，见其文档）
    if let Err(e) = self.split_all_buckets(&old_index, num_chunks) {
      log::error!(
        "在线扩容中止：分块迁移失败 ({e})，置索引撕裂待重建标记，未迁分块交由 rebuild_index_from_hlog 在线收口"
      );
      self.resize.index_torn.store(true, Ordering::SeqCst);
      self
        .resize
        .phase
        .store(ResizePhase::Rest as u8, Ordering::SeqCst);
      self.resize.old_index.store(None);
      self.resize.split_status.store(Arc::new(Vec::new()));
      return Err(e);
    }

    // 5. 完成扩容，进入 REST 态：先发布 Rest 闭环状态机，纪元排空后再回收
    //    旧表与状态数组——并发会话在相位收口前后均不观测空资源撕裂态
    self
      .resize
      .phase
      .store(ResizePhase::Rest as u8, Ordering::SeqCst);
    let drain_epoch = self.epoch.bump_current_epoch();
    self.epoch.bump_and_wait(drain_epoch);

    self.resize.old_index.store(None);
    self.resize.split_status.store(Arc::new(Vec::new()));

    Ok(true)
  }

  /// 索引撕裂待重建标记查询（票 zcode-r135c-rehash 案二）：grow_index 错误
  /// 中止后为真，[`Self::rebuild_index_from_hlog`] 成功后复位；检查点入口
  /// `ensure_not_growing` 经 `CprStore::index_rebuild_pending` 端口同检
  #[inline]
  pub fn index_rebuild_pending(&self) -> bool {
    self.resize.index_torn.load(Ordering::SeqCst)
  }

  /// 扩容中止遗留撕裂索引的在线收口重建（票 zcode-r135c-rehash 案二）：对
  /// 活跃表按 [`whlog::HybridLog`] `[begin, tail)` 全量单趟重放补齐未迁分块
  /// 条目，复用恢复扫描内核 [`wcpr::run_recovery_kernel`] 同一建表原语，成功
  /// 即消撕裂标记
  ///
  /// 形态论证（与恢复期调用的差异全部收敛在此）：
  /// - 原地补齐不切表：不构造替换表原子切换——切换会覆丢重建窗口内在线写者
  ///   写入的更高地址版本（try_cas 快照 CAS 非单调，见 [`windex::HashEntryInfo`]），
  ///   原地单调补齐则与在线写天然合流（并发新版本恒高于重放地址，守卫下互为
  ///   no-op 或 CAS 竞败重试，绝无回退）；
  /// - `index_start_address` 传 `begin`：全区间皆为重放区；`undo_next_version`
  ///   传 `false`：中止窗口的在线提交全部有效，禁回滚臂（区别于崩溃恢复的
  ///   模糊区回滚语义，回调侧 NoopRecoveryVisitor 亦不收集素材）；
  /// - 单调补齐守卫：内核重插臂仅当槽位当前地址低于本记录地址才 CAS 推进
  ///   （恢复期写者冻结、扫描升序，该守卫恒不触发、恢复行为逐字节不变）；
  /// - 纪元临界区契约照 run_recovery_pass 同形：register + barrier_enter 守卫
  ///   下取活跃表句柄，重放期间索引版本不被切换（并发 PrepareGrow 的纪元排空
  ///   自然等待本趟收口，扩容让位于收口）。
  ///
  /// # Errors
  ///
  /// 扫描或索引原语首错透传上抛（[`wcpr::run_recovery_kernel`] 错误域），撕裂
  /// 标记保留待下一次重建成功或重启全量重放收口——绝不静默吞错。
  pub async fn rebuild_index_from_hlog(&self) -> Result<()> {
    let begin_addr = self.begin_address();
    let tail_addr = self.hlog.tail_address();
    let stats = {
      let participant = self.epoch.register()?;
      let _guard = self.barrier_enter(&participant);
      let index = self.active_index();
      wcpr::run_recovery_kernel(
        &self.hlog,
        &index,
        begin_addr,
        begin_addr,
        tail_addr,
        false,
        &mut wcpr::NoopRecoveryVisitor,
      )
      .await?
    };
    self.resize.index_torn.store(false, Ordering::SeqCst);
    log::info!(
      "扩容中止遗留撕裂索引在线收口完成：扫描 {} 条、补齐 CAS {} 条（区间 [{begin_addr}, {tail_addr})）",
      stats.visited,
      stats.replayed
    );
    Ok(())
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
///
/// 中止遗留收口（票 zcode-r135c-rehash 案二）：本包装是撕裂索引在线收口的
/// 驱动点。上一轮中止遗留撕裂标记时先就地重建收口再发起本轮扩容（重建失败
/// 即上抛拒在门口，不带撕裂表继续长大）；本轮 [`WedbStore::grow_index`] 错误
/// 中止且撕裂标记置位时，回 reactor 线程后立即驱动
/// [`WedbStore::rebuild_index_from_hlog`] 补齐未迁条目（grow_index 跑在阻塞
/// 线程、hlog 区间重放需异步设备读，无法在其中途就地重建，此为与票面「发布
/// Rest 前兜底重建」的执行位置偏差，防丢数语义不变——标记先于 Rest 挂起、
/// 标记存续期索引检查点恒被拒）。重建失败仅记日志、保留标记，错误原样上抛
/// 交调用方既有重试轨道。
pub async fn grow_index_blocking<D: Device>(store: Arc<WedbStore<D>>) -> Result<bool> {
  if store.index_rebuild_pending() {
    store.rebuild_index_from_hlog().await?;
  }
  let joined = {
    let s = Arc::clone(&store);
    spawn_blocking(move || s.grow_index()).await
  };
  match joined {
    Ok(Ok(result)) => Ok(result),
    Ok(Err(e)) => {
      if store.index_rebuild_pending()
        && let Err(rebuild_e) = store.rebuild_index_from_hlog().await
      {
        log::error!(
          "扩容中止后在线收口重建失败 ({rebuild_e})，撕裂标记保留：索引检查点将持续被拒，待下次重建或重启全量重放收口"
        );
      }
      Err(e)
    }
    Err(e) => {
      log::error!("在线扩容阻塞任务异常退出: {e}");
      Err(Error::BlockingJoin(format!("{e}")))
    }
  }
}

/// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitIndex.cs:TraceBackForOtherChainStart
///
/// C# 原型只门 HeadAddress（低于头即 break 并返回该低于头的锚地址）；本实现
/// 对返回的低于头锚地址额外要求 `>= begin_addr`——低于 begin 的锚点指向已物理
/// 回收的死记录，插回新表子桶即成死条目双写。属分裂期防膨胀有意偏差的组成部分
/// （登记于 doc/zh/deviations.md，与 `windex::split_single_bucket` 的 begin 门同源）。
#[inline]
fn trace_back_for_other_chain_start<F>(
  mut curr: u64,
  target_bit: usize,
  head_addr: u64,
  begin_addr: u64,
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
  (curr > 0 && curr >= begin_addr && curr < head_addr).then_some(curr)
}
