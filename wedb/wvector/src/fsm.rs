//! 空闲空间映射（对标 diskann-garnet 的 fsm.rs）
//!
//! 以 u32 内部 id 空间的位图追踪每 id 的 Free/Occupied 状态，状态以
//! `_fsm` 键前缀的 u64 宽 id 分块持久化到存储（Metadata 项，每块 8192 字节）。
//!
//! 单次 read/write/rmw 原子但序列不原子：以写入顺序消解并发冲突（先占 id
//! 后写数据、删除先摘映射后释放 id）。插入快速路径以"固定容量空闲队列 +
//! 原子 next_id 计数器"避免反复扫块。
//!
//! # 块的懒创建（rmw 自初始化）
//!
//! 块位图不预写整块零页：置位/清位一律经 rmw（缺失键零初始化 `write_len`
//! 字节后回调改写）一次闭环，块创建与位更新合一——比旧「先写整块零页再
//! rmw」少一次全块写，且天然杜绝「零写在途被同块并发 rmw 丢更新」的乱序
//! 窗口。账面 `max_block` 在铸造临界段（纯内存锁内）推进，仅末块存在
//! 「账面已记、落盘在途」的瞬态（铸造按 id 顺序推进，前一块的 rmw 必在
//! 后一块铸造前完成），[`Self::is_free`] 对账面哨兵（空表）直接报空闲。
//!
//! # 量化切换屏障（原子计数承接读屏障的跨 await 形态）
//!
//! 铸造须在「量化启用写屏障」保护下进行：插入以计数屏障登记在途（观测到
//! 未启用即计数 + 复读消解置位竞态），`enable_quantization` 先置位、排空
//! 置位前登记的在途计数后方取上界快照并再排空复核，定点后发布回填上界
//! （对账原生写屏障内快照的同等不变量：发布的上界恒覆盖全部未就地量化
//! 的已铸 id，见 [`FreeSpaceMap::enable_quantization`]）——等价旧
//! parking_lot 读/写屏障的跨 await 形态（parking_lot 守卫 `!Send` 且跨
//! await 持锁会饿死同线程任务队列，compio thread-per-core 下必死锁）；
//! [`ReuseGuard`] 随插入全程持有计数，Drop 注销。
//! 本计数屏障收敛发布形对原生阻塞快照形的机制分叉已登 deviations §127。

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

use async_lock::Mutex as AsyncMutex;
use parking_lot::{Mutex, RwLock};
use wbase::future::yield_now;

use crate::{
  error::{FsmError, StoreError},
  store::{Callbacks, Context, StoreCallbacks, Term},
};

/// 每块承载的 id 数（2^16）。
const BLOCK_SIZE_IDS: usize = 1 << 16;
/// 每块状态字节数（8192）。
const BLOCK_SIZE_BYTES: usize = BLOCK_SIZE_IDS / 8;
/// 快速空闲队列容量。
const FAST_SIZE: usize = 1024;
/// FSM 块键前缀（`_fsm`，块号左移 32 位组合成宽 id 键）。
const FSM_KEY_PREFIX: u32 = u32::from_be_bytes(*b"_fsm");

/// 基于标准互斥锁的快速空闲队列（锁竞争极低场景下避免无锁环形预分配的内存虚高与缓存颠簸）
#[derive(Debug, Default)]
struct FastFreeList(Mutex<Vec<u32>>);

impl FastFreeList {
  fn new(capacity: usize) -> Self {
    Self(Mutex::new(Vec::with_capacity(capacity)))
  }

  #[inline]
  fn push(&self, id: u32) -> Result<(), ()> {
    let mut v = self.0.lock();
    if v.len() < FAST_SIZE {
      v.push(id);
      Ok(())
    } else {
      Err(())
    }
  }

  #[inline]
  fn pop(&self) -> Option<u32> {
    self.0.lock().pop()
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.0.lock().is_empty()
  }

  #[cfg(test)]
  #[inline]
  fn len(&self) -> usize {
    self.0.lock().len()
  }
}

/// `next_id()` 返回的守卫：量化阶段切换期间以保证 id 占用与向量写入的原子
/// 窗口（在途计数随本守卫存活，`enable_quantization` 等其注销后方返回）；
/// `should_quantize()` 决定新 id 是否就地量化。
///
/// 旧形态为 parking_lot 读守卫（`!Send`，不可跨 await 持有）；现以原子计数
/// 承接：守卫仅持共享引用（`Sync` 面），随 set_element 的 awaits 全程存活
/// 合法，`Drop` 注销在途计数。
pub(crate) struct ReuseGuard<'a> {
  id: u32,
  /// 该 id 是否应就地量化（铸造时观测到的量化启用态）。
  should_quantize: bool,
  /// 铸造时是否登记了在途计数（观测到未启用才登记；Drop 据此注销）。
  counted: bool,
  /// 在途计数原子（所属 [`FreeSpaceMap`] 的字段引用）。
  inflight: &'a AtomicUsize,
}

impl Drop for ReuseGuard<'_> {
  #[inline]
  fn drop(&mut self) {
    if self.counted {
      self.inflight.fetch_sub(1, Ordering::AcqRel);
    }
  }
}

impl<'a> ReuseGuard<'a> {
  fn new(id: u32, should_quantize: bool, counted: bool, inflight: &'a AtomicUsize) -> Self {
    Self {
      id,
      should_quantize,
      counted,
      inflight,
    }
  }

  /// 占用的内部 id。
  pub fn id(&self) -> u32 {
    self.id
  }

  /// 该 id 是否应就地量化（量化已启用的新 id）。
  pub fn should_quantize(&self) -> bool {
    self.should_quantize
  }
}

/// 新 id 铸造器：next_id 计数 + FSM 块账面水位。
struct IdMinter {
  next_id: u32,
  /// 已账面化的最高块号；`u32::MAX` 哨兵 = 空表（无任何块，首铸懒建）。
  max_block: u32,
}

/// 空闲空间映射：u32 内部 id 池（对标 diskann-garnet fsm.rs FreeSpaceMap）。
///
/// `next_id()` 返回新铸或复用自已删元素的 id（已置占用位）；
/// [`Self::mark_free`] 释放 id。量化回填期以 `reuse_enabled = false` 禁止
/// 复用直至 [`Self::enable_reuse`]，[`Self::max_id_for_backfill`] 圈定回填上界。
pub(crate) struct FreeSpaceMap<S: StoreCallbacks> {
  callbacks: Callbacks<S>,
  /// 扫块后置位的"存在空闲 id"信号，避免多余块读。
  has_free_ids: AtomicBool,
  /// 已删 id 快速空闲队列。
  fast_free_list: FastFreeList,
  id_minter: RwLock<IdMinter>,
  total_used: AtomicUsize,
  reuse_enabled: AtomicBool,
  /// 量化启用开关（置位后铸造的新 id 就地量化）。
  quant_enabled: AtomicBool,
  /// 置位前在途插入计数（观测到未启用的铸造 + 复读确认，[`ReuseGuard`]
  /// 存续期持有；`enable_quantization` 排空本计数后方返回）。
  pre_switch_inflight: AtomicUsize,
  /// 量化回填上界（`enable_quantization` 置位→排空→快照→再排空定点收敛
  /// 后发布，恒覆盖全部未就地量化的已铸 id）。
  max_id_for_backfill: AtomicU32,
  /// 快速空闲列表重填锁（防并发重填）。
  refill_lock: AsyncMutex<()>,
}

/// 空表哨兵（无任何账面块）。
const NO_BLOCK: u32 = u32::MAX;

impl<S: StoreCallbacks> FreeSpaceMap<S> {
  /// 构造并从存储恢复状态（无 `_fsm` 块时保持空表哨兵，首铸经 rmw 自初始化
  /// 懒建首块）。
  pub async fn new(
    ctx: &Context,
    callbacks: Callbacks<S>,
    quantization_enabled: bool,
    reuse_enabled: bool,
  ) -> Result<Self, FsmError> {
    let mut this = Self {
      callbacks,
      has_free_ids: AtomicBool::new(false),
      fast_free_list: FastFreeList::new(FAST_SIZE),
      id_minter: RwLock::new(IdMinter {
        next_id: 0,
        max_block: NO_BLOCK,
      }),
      total_used: AtomicUsize::new(0),
      reuse_enabled: AtomicBool::new(reuse_enabled),
      quant_enabled: AtomicBool::new(quantization_enabled),
      pre_switch_inflight: AtomicUsize::new(0),
      max_id_for_backfill: AtomicU32::new(u32::MAX),
      refill_lock: AsyncMutex::new(()),
    };

    if this
      .callbacks
      .exists_wid(
        &ctx.term(Term::Metadata),
        Self::block_key(0),
        BLOCK_SIZE_BYTES,
      )
      .await
    {
      this.load_state(ctx).await?;
    }

    Ok(this)
  }

  /// 逐块扫描恢复状态（从高块到低块，空闲 id 以降序入快速队列）。
  async fn load_state(&mut self, ctx: &Context) -> Result<(), FsmError> {
    let mut max_block_id = 0usize;
    while self
      .callbacks
      .exists_wid(
        &ctx.term(Term::Metadata),
        Self::block_key(max_block_id as u32),
        BLOCK_SIZE_BYTES,
      )
      .await
    {
      max_block_id += 1;
    }

    let mut block = vec![0u8; BLOCK_SIZE_BYTES];
    let mut last_used_id = -1i64;
    let mut total_used = 0usize;

    for block_id in (0..max_block_id).rev() {
      let block_key = Self::block_key(block_id as u32);

      if !self
        .callbacks
        .read_single_wid(&ctx.term(Term::Metadata), block_key, &mut block[..])
        .await
      {
        break;
      }

      let mut id = (block_id * BLOCK_SIZE_IDS + BLOCK_SIZE_IDS - 1) as u32;

      for &byte in block.iter().rev() {
        for bidx in (0..8).rev() {
          if bit_used(byte, bidx) {
            last_used_id = last_used_id.max(id as i64);
            total_used += 1;
          } else if (id as i64) < last_used_id {
            let _ = self.fast_free_list.push(id);
          }

          id = id.saturating_sub(1);
        }
      }
    }

    let mut id_minter = self.id_minter.write();
    id_minter.max_block = max_block_id as u32 - 1;
    id_minter.next_id = (last_used_id + 1) as u32;
    drop(id_minter);

    self.total_used.store(total_used, Ordering::Release);

    if !self.fast_free_list.is_empty() {
      self.has_free_ids.store(true, Ordering::Release);
    }

    Ok(())
  }

  /// 释放 id（置空闲位并入快速队列）。
  pub async fn mark_free(&self, ctx: &Context, id: u32) -> Result<(), FsmError> {
    self.mark_id(ctx, id, false).await.map(|_| ())
  }

  /// 占用 id（返回状态是否变化）。
  async fn mark_used(&self, ctx: &Context, id: u32) -> Result<bool, FsmError> {
    self.mark_id(ctx, id, true).await
  }

  /// 免锁版置位（铸造临界段释放 id_minter 写锁后调用）。
  ///
  /// rmw 自初始化承接块创建：缺失键零初始化整块后按位更新，块创建与位更新
  /// 一次闭环（见模块头「块的懒创建」）。
  async fn mark_id_unchecked(&self, ctx: &Context, id: u32, used: bool) -> Result<bool, FsmError> {
    let (block_id, byte_idx, bit_idx) = Self::indexes_for_id(id);
    let block_key = Self::block_key(block_id);
    let mut changed = false;

    if !self
      .callbacks
      .rmw_wid(
        &ctx.term(Term::Metadata),
        block_key,
        BLOCK_SIZE_BYTES,
        |data: &mut [u8]| changed = update_status(used, &mut data[byte_idx], bit_idx),
      )
      .await
    {
      return Err(FsmError::Store(StoreError::Write));
    }

    if changed {
      if used {
        self.total_used.fetch_add(1, Ordering::AcqRel);
      } else {
        self.total_used.fetch_sub(1, Ordering::AcqRel);
      }
    }

    // 已空闲的 id 不重复入队
    if !used && changed {
      let _ = self.fast_free_list.push(id);
      self.has_free_ids.store(true, Ordering::Release);
    }

    Ok(changed)
  }

  /// 范围校验后置位。
  async fn mark_id(&self, ctx: &Context, id: u32, used: bool) -> Result<bool, FsmError> {
    if id >= self.id_minter.read().next_id {
      return Err(FsmError::IdOutOfRange(id));
    }

    self.mark_id_unchecked(ctx, id, used).await
  }

  /// id 是否空闲（超范围返回 IdOutOfRange）。
  pub async fn is_free(&self, ctx: &Context, id: u32) -> Result<bool, FsmError> {
    let max_block = {
      let id_minter = self.id_minter.read();
      if id >= id_minter.next_id {
        return Err(FsmError::IdOutOfRange(id));
      }

      id_minter.max_block
    };

    // 空表（无账面块）：id < next_id 恒假已排除，任意 id 必空闲
    if max_block == NO_BLOCK {
      return Ok(true);
    }

    let (block_id, byte_idx, bit_idx) = Self::indexes_for_id(id);
    if block_id > max_block {
      return Err(FsmError::Store(StoreError::Read));
    }

    let block_key = Self::block_key(block_id);
    let mut block = [0u8; BLOCK_SIZE_BYTES];

    if !self
      .callbacks
      .read_single_wid(&ctx.term(Term::Metadata), block_key, &mut block[..])
      .await
    {
      return Err(FsmError::Store(StoreError::Read));
    }

    Ok(!bit_used(block[byte_idx], bit_idx))
  }

  /// 取一个 id（新铸或复用，已置占用位）。
  ///
  /// 量化切换期间以计数屏障保证阶段切换原子：观测到未启用即登记在途并复读
  /// 消解置位竞态；`ReuseGuard::should_quantize()` 为 true 的新 id 应就地
  /// 量化（屏障保证插入数据在量化启用前全部落盘）。
  pub async fn next_id(&self, ctx: &Context) -> Result<ReuseGuard<'_>, FsmError> {
    // 快照量化开关并登记在途（enable 置位后必观测到启用，不得再登记；
    // 握手 store-load 对两侧 SeqCst，见 enable_quantization 注）
    let mut counted = false;
    let should_quantize = loop {
      if self.quant_enabled.load(Ordering::SeqCst) {
        break true;
      }
      if !counted {
        self.pre_switch_inflight.fetch_add(1, Ordering::SeqCst);
        counted = true;
        // 登记后复读：置位发生于两读之间则注销走启用臂
        if self.quant_enabled.load(Ordering::SeqCst) {
          self.pre_switch_inflight.fetch_sub(1, Ordering::AcqRel);
          counted = false;
          continue;
        }
      }
      break false;
    };

    let minted = self.reuse_or_mint(ctx).await;
    match minted {
      Ok(id) => Ok(ReuseGuard::new(
        id,
        should_quantize,
        counted,
        &self.pre_switch_inflight,
      )),
      Err(e) => {
        if counted {
          self.pre_switch_inflight.fetch_sub(1, Ordering::AcqRel);
        }
        Err(e)
      }
    }
  }

  /// 复用或铸造一个 id（已置占用位）。
  async fn reuse_or_mint(&self, ctx: &Context) -> Result<u32, FsmError> {
    if self.reuse_enabled.load(Ordering::Acquire) && self.has_free_ids.load(Ordering::Acquire) {
      // 反复尝试复用，直至空闲队列为空或占用置位成功
      loop {
        let id = if let Some(id) = self.fast_free_list.pop() {
          if !self.mark_used(ctx, id).await? {
            continue;
          }
          Some(id)
        } else {
          // 快速队列耗尽，扫块重填
          if self.refill_fast_free_list(ctx).await?
            && let Some(id) = self.fast_free_list.pop()
            && !self.mark_used(ctx, id).await?
          {
            continue;
          }
          None
        };

        if let Some(id) = id {
          return Ok(id);
        }

        break;
      }
    }

    // 铸造新 id：临界段（纯内存）推进计数与块账面，锁外 rmw 落盘置位
    //（rmw 自初始化承接块创建，见模块头）
    let id = {
      let mut id_minter = self.id_minter.write();
      let id = id_minter.next_id;
      id_minter.next_id = id.wrapping_add(1);
      let (block_id, ..) = Self::indexes_for_id(id);
      if id_minter.max_block == NO_BLOCK || block_id == id_minter.max_block + 1 {
        id_minter.max_block = block_id;
      } else if block_id > id_minter.max_block + 1 {
        // 不可达：id 顺序铸造，块号不会跳越
        return Err(FsmError::IdOutOfRange(id));
      }
      id
    };
    self.mark_id_unchecked(ctx, id, true).await?;

    Ok(id)
  }

  /// 已铸造的最大 id（可能已被删除释放）。
  pub fn max_id(&self) -> u32 {
    self.id_minter.read().next_id.saturating_sub(1)
  }

  /// 当前占用 id 总数。
  pub fn total_used(&self) -> usize {
    self.total_used.load(Ordering::Acquire)
  }

  /// id → (块号, 字节下标, 位下标)。
  fn indexes_for_id(id: u32) -> (u32, usize, usize) {
    let id = id as usize;
    let block_id = (id / BLOCK_SIZE_IDS) as u32;
    let block_idx = id % BLOCK_SIZE_IDS;
    (block_id, block_idx / 8, block_idx % 8)
  }

  /// 块号 → 宽 id 键（块号左移 32 位 | `_fsm` 前缀）。
  fn block_key(block_id: u32) -> u64 {
    (block_id as u64) << 32 | (FSM_KEY_PREFIX as u64)
  }

  /// 扫块重填快速空闲队列（重填锁防并发）。
  async fn refill_fast_free_list(&self, ctx: &Context) -> Result<bool, FsmError> {
    let _guard = self.refill_lock.lock().await;

    // 等锁期间可能已被他线程重填
    if !self.fast_free_list.is_empty() {
      return Ok(true);
    }

    let (max_block, next_id) = {
      let id_minter = self.id_minter.read();
      (id_minter.max_block, id_minter.next_id)
    };

    let mut has_free_ids = false;
    let mut id = 0u32;
    let mut block = [0u8; BLOCK_SIZE_BYTES];
    'scan: for block_id in 0..=max_block {
      if id >= next_id {
        break;
      }

      let block_key = Self::block_key(block_id);
      if !self
        .callbacks
        .read_single_wid(&ctx.term(Term::Metadata), block_key, &mut block[..])
        .await
      {
        if block_id == max_block {
          // 账面末块可能尚在首置位途中（并发铸造的 rmw 在飞，见模块头）：
          // 新块首个 id 即在铸 id，next_id 之下无空闲位可收，跳过即可
          continue;
        }
        // 非末块账面块必已落盘，缺失即真实读失败
        return Err(FsmError::Store(StoreError::Read));
      }

      for &byte in &block {
        if id >= next_id {
          break 'scan;
        }

        if byte == 0xff {
          id += 8;
          continue;
        }

        for bidx in 0..8 {
          if id >= next_id {
            break 'scan;
          }

          if !bit_used(byte, bidx) {
            has_free_ids = true;
            self.has_free_ids.store(true, Ordering::Release);
            if self.fast_free_list.push(id).is_err() {
              break 'scan;
            }
          }
          id += 1;
        }
      }
    }

    if !has_free_ids {
      self.has_free_ids.store(false, Ordering::Release);
    }

    Ok(has_free_ids)
  }

  /// 遍历全部占用 id（`f` 返回 false 提前终止）。
  pub async fn visit_used<F>(&self, ctx: &Context, mut f: F) -> Result<(), FsmError>
  where
    F: FnMut(u32) -> bool,
  {
    let max_block = self.id_minter.read().max_block;
    let mut block = [0u8; BLOCK_SIZE_BYTES];
    let mut id = 0u32;

    for block_id in 0..max_block + 1 {
      let block_key = Self::block_key(block_id);
      if !self
        .callbacks
        .read_single_wid(&ctx.term(Term::Metadata), block_key, &mut block[..])
        .await
      {
        return Err(FsmError::Store(StoreError::Read));
      }

      for &byte in &block {
        if byte == 0x00 {
          id += 8;
          continue;
        }

        for bidx in 0..8 {
          if bit_used(byte, bidx) {
            let keep_going = f(id);
            if !keep_going {
              return Ok(());
            }
          }
          id += 1;
        }
      }
    }

    Ok(())
  }

  /// 启用量化（新 id 置位量化标记；计数屏障定点收敛发布回填上界）。
  ///
  /// 对账原生 fsm.rs:enable_quantization（写屏障内取 max_id 后置于标志，
  /// 屏障期铸造被全阻，快照即精确上界）；rust 侧计数屏障不能阻塞铸造，
  /// 以「置位→排空→快照→再排空」的定点收敛序达成同等不变量——发布的
  /// 上界恒覆盖全部 `should_quantize = false` 的已铸 id：
  ///
  /// 1. 先置位：此后一切登记者必在 [`Self::next_id`] 的登记复读臂观测到
  ///    启用而注销计数走启用臂就地量化，铸造晚于快照亦无洞；
  /// 2. 排空置位前登记的在途计数——其铸造与数据写全部先于计数注销完成，
  ///    故排空后的 `max_id` 快照恒覆盖它们；
  /// 3. 快照后再排空并复核 `max_id`，若仍推进则重采（防御协议外交错），
  ///    定点后发布。
  ///
  /// 旧序「快照→置位→排空」使「登记早于置位、铸造晚于快照」的插入铸出
  /// 上界之外的未量化 id：回填区间不覆盖、置位后标志=1 重启免重回填，
  /// 该 id 在量化轨永久不可见——本序将其闭合。置位与登记握手为跨线程
  /// Dekker 窗（store-load 对），两侧统一 SeqCst，弱内存序下 Acq/Rel 不
  /// 足以闭合。等待循环以协作让位重驱，同线程排队的插入任务得以推进注销
  /// 计数，无饿死。
  pub async fn enable_quantization(&self) {
    self.quant_enabled.store(true, Ordering::SeqCst);
    while self.pre_switch_inflight.load(Ordering::SeqCst) > 0 {
      yield_now().await;
    }
    let snapshot = loop {
      let s = self.max_id();
      while self.pre_switch_inflight.load(Ordering::SeqCst) > 0 {
        yield_now().await;
      }
      if self.max_id() == s {
        break s;
      }
    };
    self.max_id_for_backfill.store(snapshot, Ordering::Release);
  }

  /// 启用已删 id 复用（量化回填完成后调用）。
  pub fn enable_reuse(&self) {
    self.reuse_enabled.store(true, Ordering::Release);
  }

  /// 量化回填上界（启用量化时的最大 id）。
  pub fn max_id_for_backfill(&self) -> u32 {
    self.max_id_for_backfill.load(Ordering::Acquire)
  }
}

/// 从左到右的位判定（MSB 为位 0）。
#[inline]
fn bit_used(byte: u8, bidx: usize) -> bool {
  (byte & (0x80 >> bidx)) != 0
}

/// 更新 `bidx` 位为 `used`，返回值是否变化。
#[inline]
fn update_status(used: bool, byte: &mut u8, bidx: usize) -> bool {
  let mask = 0x80 >> bidx;
  let old = *byte & mask != 0;
  if used {
    *byte |= mask;
  } else {
    *byte &= !mask;
  }
  used != old
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use futures_executor::block_on;
  use parking_lot::Mutex;
  use wbase::map::HashMap;

  use super::*;
  use crate::store::{LengthPrefixedIter, StoreCallbacks, TERM_BITMASK};

  type MemStoreMap = HashMap<(u64, Vec<u8>), Vec<u8>>;

  /// 内存桥接存储（块键 → 块字节）。
  struct MemStore {
    data: Mutex<MemStoreMap>,
  }

  impl MemStore {
    fn new() -> Self {
      Self {
        data: Mutex::new(HashMap::default()),
      }
    }
  }

  impl StoreCallbacks for MemStore {
    async fn read_multi<F>(&self, context: u64, keys: &[u8], _length_hint: usize, mut f: F) -> bool
    where
      F: FnMut(u32, &[u8]) + Send,
    {
      for (index, key) in LengthPrefixedIter::new(keys).enumerate() {
        if let Some(value) = self.data.lock().get(&(context, key.to_vec())) {
          f(index as u32, value);
        }
      }
      true
    }

    async fn read<F>(&self, context: u64, key: &[u8], mut f: F) -> bool
    where
      F: FnMut(&[u8]) + Send,
    {
      match self.data.lock().get(&(context, key.to_vec())) {
        Some(value) => {
          f(value);
          true
        }
        None => false,
      }
    }

    async fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
      self
        .data
        .lock()
        .insert((context, key.to_vec()), value.to_vec());
      true
    }

    async fn delete(&self, context: u64, key: &[u8]) -> bool {
      self.data.lock().remove(&(context, key.to_vec())).is_some()
    }

    async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, mut f: F) -> bool
    where
      F: FnMut(&mut [u8]) + Send,
    {
      // 纯读短路（对齐生产回调与 C# 谓词 WriteDesiredSize == 0 判假）：不建不写
      if write_len == 0 {
        return true;
      }
      // 对齐生产内核口径（wnode WedbVectorStoreCallbacks::rmw）：write_len 即
      // 目标记录尺寸，旧值截短/补零后闭包改写、整值写回——桥若保留旧记录
      // 全长，「rmw 缩记录」类缺陷（write_len 小于现记录）对本桥不可见
      let mut buf = self
        .data
        .lock()
        .get(&(context, key.to_vec()))
        .cloned()
        .unwrap_or_default();
      buf.resize(write_len, 0);
      f(&mut buf);
      self.data.lock().insert((context, key.to_vec()), buf);
      true
    }

    async fn filter(&self, _context: u64, _internal_id: u32) -> bool {
      false
    }

    /// 按基址清除全部项类型子域条目（内存桥接版的真实清扫，与生产端
    /// 「扫描 + 物理墓碑」同语义：清除后点查/遍历均不可见）
    async fn purge_context(&self, context: u64) -> bool {
      self
        .data
        .lock()
        .retain(|&(ctx, _), _| ctx & !TERM_BITMASK != context);
      true
    }

    fn log(&self, _context: u64, _msg: &str) {}
  }

  fn callbacks() -> Callbacks<MemStore> {
    Callbacks::new(Arc::new(MemStore::new()))
  }

  #[test]
  fn fresh_and_next_id() {
    let ctx = Context::new(8);
    let fsm = block_on(FreeSpaceMap::new(&ctx, callbacks(), false, true)).unwrap();
    assert!(!fsm.has_free_ids.load(Ordering::Acquire));
    assert_eq!(block_on(fsm.next_id(&ctx)).unwrap().id(), 0);
    assert_eq!(block_on(fsm.next_id(&ctx)).unwrap().id(), 1);
    assert_eq!(fsm.total_used(), 2);
  }

  #[test]
  fn mark_free_out_of_range() {
    let ctx = Context::new(8);
    let fsm = block_on(FreeSpaceMap::new(&ctx, callbacks(), false, true)).unwrap();
    assert_eq!(
      block_on(fsm.mark_free(&ctx, 0)),
      Err(FsmError::IdOutOfRange(0))
    );
  }

  #[test]
  fn delete_and_reuse() {
    let ctx = Context::new(8);
    let fsm = block_on(FreeSpaceMap::new(&ctx, callbacks(), false, true)).unwrap();

    for _ in 0u32..64 {
      let _ = block_on(fsm.next_id(&ctx)).unwrap();
    }
    assert_eq!(block_on(fsm.next_id(&ctx)).unwrap().id(), 64);

    block_on(fsm.mark_free(&ctx, 37)).unwrap();
    assert_eq!(block_on(fsm.next_id(&ctx)).unwrap().id(), 37);
    assert_eq!(block_on(fsm.next_id(&ctx)).unwrap().id(), 65);
    assert!(!fsm.has_free_ids.load(Ordering::Acquire));
  }

  #[test]
  fn recovery_from_store() {
    let ctx = Context::new(8);
    let cbs = callbacks();
    let fsm = block_on(FreeSpaceMap::new(&ctx, cbs.clone(), false, true)).unwrap();

    for _ in 0u32..64 {
      let _ = block_on(fsm.next_id(&ctx)).unwrap();
    }
    block_on(fsm.mark_free(&ctx, 37)).unwrap();

    // 同一存储重建，状态全量恢复
    let fsm = block_on(FreeSpaceMap::new(&ctx, cbs, false, true)).unwrap();
    assert_eq!(fsm.max_id() + 1, 64);
    assert!(fsm.has_free_ids.load(Ordering::Acquire));
    assert_eq!(fsm.fast_free_list.len(), 1);
    assert_eq!(block_on(fsm.next_id(&ctx)).unwrap().id(), 37);
  }

  #[test]
  fn block_expansion() {
    let ctx = Context::new(8);
    let fsm = block_on(FreeSpaceMap::new(&ctx, callbacks(), false, true)).unwrap();
    for _ in 0u32..BLOCK_SIZE_IDS as u32 + 1 {
      let _ = block_on(fsm.next_id(&ctx)).unwrap();
    }
    assert_eq!(fsm.max_id(), BLOCK_SIZE_IDS as u32);
    assert_eq!(fsm.total_used(), BLOCK_SIZE_IDS + 1);
  }

  #[test]
  fn visit_used_and_bit_helpers() {
    let ctx = Context::new(8);
    let fsm = block_on(FreeSpaceMap::new(&ctx, callbacks(), false, true)).unwrap();
    for _ in 0u32..8 {
      let _ = block_on(fsm.next_id(&ctx)).unwrap();
    }
    block_on(fsm.mark_free(&ctx, 5)).unwrap();

    let mut seen = Vec::new();
    block_on(fsm.visit_used(&ctx, |id| {
      seen.push(id);
      true
    }))
    .unwrap();
    assert_eq!(seen, vec![0, 1, 2, 3, 4, 6, 7]);

    assert!(bit_used(0b1000_0000, 0));
    assert!(!bit_used(0b1000_0000, 1));
  }
}
