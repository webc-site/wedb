//! 空闲空间映射（对标 diskann-garnet 的 fsm.rs）
//!
//! 以 u32 内部 id 空间的位图追踪每 id 的 Free/Occupied 状态，状态以
//! `_fsm` 键前缀的 u64 宽 id 分块持久化到存储（Metadata 项，每块 8192 字节）。
//!
//! 单次 read/write/rmw 原子但序列不原子：以写入顺序消解并发冲突（先占 id
//! 后写数据、删除先摘映射后释放 id）。插入快速路径以"固定容量空闲队列 +
//! 原子 next_id 计数器"避免反复扫块。

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use crossfire::flavor::{Array, Queue};
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::store::{Callbacks, Context, StoreCallbacks, StoreError, Term};

/// 每块承载的 id 数（2^16）。
const BLOCK_SIZE_IDS: usize = 1 << 16;
/// 每块状态字节数（8192）。
const BLOCK_SIZE_BYTES: usize = BLOCK_SIZE_IDS / 8;
/// 空白初始化块。
const ZERO_BLOCK: [u8; BLOCK_SIZE_BYTES] = [0u8; BLOCK_SIZE_BYTES];
/// 快速空闲队列容量。
const FAST_SIZE: usize = 1024;
/// FSM 块键前缀（`_fsm`，块号左移 32 位组合成宽 id 键）。
const FSM_KEY_PREFIX: u32 = u32::from_be_bytes(*b"_fsm");

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum FsmError {
  #[error(transparent)]
  Store(#[from] StoreError),
  #[error("requested ID is out of range {0}")]
  IdOutOfRange(u32),
}

/// `next_id()` 返回的守卫：量化阶段切换期间以读屏障保证 id 占用与
/// 向量写入的原子窗口；`should_quantize()` 决定新 id 是否就地量化。
pub struct ReuseGuard<'a> {
  id: u32,
  barrier: RwLockReadGuard<'a, Barrier>,
}

impl<'a> ReuseGuard<'a> {
  fn new(id: u32, barrier: RwLockReadGuard<'a, Barrier>) -> Self {
    Self { id, barrier }
  }

  /// 占用的内部 id。
  pub fn id(&self) -> u32 {
    self.id
  }

  /// 该 id 是否应就地量化（量化已启用的新 id）。
  pub fn should_quantize(&self) -> bool {
    self.barrier.quantization_enabled
  }
}

struct Barrier {
  max_id_for_backfill: u32,
  quantization_enabled: bool,
}

/// 新 id 铸造器：next_id 计数 + FSM 块扩展水位。
struct IdMinter {
  next_id: u32,
  max_block: u32,
}

/// 空闲空间映射：u32 内部 id 池（对标 diskann-garnet fsm.rs FreeSpaceMap）。
///
/// `next_id()` 返回新铸或复用自已删元素的 id（已置占用位）；
/// [`Self::mark_free`] 释放 id。量化回填期以 `reuse_enabled = false` 禁止
/// 复用直至 [`Self::enable_reuse`]，`max_id_for_backfill` 圈定回填上界。
pub struct FreeSpaceMap<S: StoreCallbacks> {
  callbacks: Callbacks<S>,
  /// 扫块后置位的"存在空闲 id"信号，避免多余块读。
  has_free_ids: AtomicBool,
  /// 已删 id 快速空闲队列。
  fast_free_list: Array<u32>,
  id_minter: RwLock<IdMinter>,
  total_used: AtomicUsize,
  reuse_enabled: AtomicBool,
  barrier: RwLock<Barrier>,
  /// 快速空闲列表重填锁（防并发重填）。
  refill_lock: Mutex<()>,
}

impl<S: StoreCallbacks> FreeSpaceMap<S> {
  /// 构造并从存储恢复状态（无 `_fsm` 块时分配首块）。
  pub fn new(
    ctx: &Context,
    callbacks: Callbacks<S>,
    quantization_enabled: bool,
    reuse_enabled: bool,
  ) -> Result<Self, FsmError> {
    let mut this = Self {
      callbacks,
      has_free_ids: AtomicBool::new(false),
      fast_free_list: Array::new(FAST_SIZE),
      id_minter: RwLock::new(IdMinter {
        next_id: 0,
        max_block: u32::MAX,
      }),
      total_used: AtomicUsize::new(0),
      reuse_enabled: AtomicBool::new(reuse_enabled),
      barrier: RwLock::new(Barrier {
        max_id_for_backfill: u32::MAX,
        quantization_enabled,
      }),
      refill_lock: Mutex::new(()),
    };

    let block_key = Self::block_key(0);
    if this
      .callbacks
      .exists_wid(&ctx.term(Term::Metadata), block_key, BLOCK_SIZE_BYTES)
    {
      this.load_state(ctx)?;
    } else {
      let (block_id, ..) = Self::indexes_for_id(0);
      let mut id_minter = this.id_minter.write();
      this.expand_to(&mut id_minter, ctx, block_id)?;
    }

    Ok(this)
  }

  /// 逐块扫描恢复状态（从高块到低块，空闲 id 以降序入快速队列）。
  fn load_state(&mut self, ctx: &Context) -> Result<(), FsmError> {
    let mut max_block_id = 0usize;
    while self.callbacks.exists_wid(
      &ctx.term(Term::Metadata),
      Self::block_key(max_block_id as u32),
      BLOCK_SIZE_BYTES,
    ) {
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
  pub fn mark_free(&self, ctx: &Context, id: u32) -> Result<(), FsmError> {
    self.mark_id(ctx, id, false).map(|_| ())
  }

  /// 占用 id（返回状态是否变化）。
  fn mark_used(&self, ctx: &Context, id: u32) -> Result<bool, FsmError> {
    self.mark_id(ctx, id, true)
  }

  /// 免锁版置位（持有 id_minter 写锁时安全调用）。
  fn mark_id_unchecked(&self, ctx: &Context, id: u32, used: bool) -> Result<bool, FsmError> {
    let (block_id, byte_idx, bit_idx) = Self::indexes_for_id(id);
    let block_key = Self::block_key(block_id);
    let mut changed = false;

    if !self.callbacks.rmw_wid(
      &ctx.term(Term::Metadata),
      block_key,
      BLOCK_SIZE_BYTES,
      |data: &mut [u8]| changed = update_status(used, &mut data[byte_idx], bit_idx),
    ) {
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
  fn mark_id(&self, ctx: &Context, id: u32, used: bool) -> Result<bool, FsmError> {
    if id >= self.id_minter.read().next_id {
      return Err(FsmError::IdOutOfRange(id));
    }

    self.mark_id_unchecked(ctx, id, used)
  }

  /// id 是否空闲（超范围返回 IdOutOfRange）。
  pub fn is_free(&self, ctx: &Context, id: u32) -> Result<bool, FsmError> {
    let max_block = {
      let id_minter = self.id_minter.read();
      if id >= id_minter.next_id {
        return Err(FsmError::IdOutOfRange(id));
      }

      id_minter.max_block
    };

    let (block_id, byte_idx, bit_idx) = Self::indexes_for_id(id);
    if block_id > max_block || max_block == u32::MAX {
      return Err(FsmError::Store(StoreError::Read));
    }

    let block_key = Self::block_key(block_id);
    let mut block = [0u8; BLOCK_SIZE_BYTES];

    if !self
      .callbacks
      .read_single_wid(&ctx.term(Term::Metadata), block_key, &mut block[..])
    {
      return Err(FsmError::Store(StoreError::Read));
    }

    Ok(!bit_used(block[byte_idx], bit_idx))
  }

  /// 取一个 id（新铸或复用，已置占用位）。
  ///
  /// 量化切换期间以读屏障阻止铸造：`ReuseGuard::should_quantize()` 为 true 的
  /// 新 id 应就地量化（屏障保证插入数据在量化启用前全部落盘）。
  pub fn next_id(&self, ctx: &Context) -> Result<ReuseGuard<'_>, FsmError> {
    let barrier = self.barrier.read();

    if self.reuse_enabled.load(Ordering::Acquire) && self.has_free_ids.load(Ordering::Acquire) {
      // 反复尝试复用，直至空闲队列为空或占用置位成功
      loop {
        let id = if let Some(id) = self.fast_free_list.pop() {
          if !self.mark_used(ctx, id)? {
            continue;
          }
          Some(id)
        } else {
          // 快速队列耗尽，扫块重填
          if self.refill_fast_free_list(ctx)?
            && let Some(id) = self.fast_free_list.pop()
            && !self.mark_used(ctx, id)?
          {
            continue;
          }
          None
        };

        if let Some(id) = id {
          return Ok(ReuseGuard::new(id, barrier));
        }

        break;
      }
    }

    // 铸造新 id 并置占用位
    let mut id_minter = self.id_minter.write();
    let id = id_minter.next_id;
    id_minter.next_id += 1;
    self.expand_to(&mut id_minter, ctx, id)?;
    self.mark_id_unchecked(ctx, id, true)?;

    Ok(ReuseGuard::new(id, barrier))
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
  fn refill_fast_free_list(&self, ctx: &Context) -> Result<bool, FsmError> {
    let _guard = self.refill_lock.lock();

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
      {
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

  /// 确保块位图覆盖 `id`（按需扩展新块）。
  fn expand_to(
    &self,
    id_minter: &mut RwLockWriteGuard<'_, IdMinter>,
    ctx: &Context,
    id: u32,
  ) -> Result<(), FsmError> {
    let (block_id, ..) = Self::indexes_for_id(id);
    if id_minter.max_block == u32::MAX || block_id == id_minter.max_block + 1 {
      let block_key = Self::block_key(block_id);

      if !self
        .callbacks
        .write_wid(&ctx.term(Term::Metadata), block_key, &ZERO_BLOCK)
      {
        return Err(FsmError::Store(StoreError::Write));
      }

      if id_minter.max_block == u32::MAX {
        id_minter.max_block = 0;
      } else {
        id_minter.max_block += 1;
      }
    } else if block_id > id_minter.max_block + 1 {
      return Err(FsmError::IdOutOfRange(id));
    }

    Ok(())
  }

  /// 遍历全部占用 id（`f` 返回 false 提前终止）。
  pub fn visit_used<F>(&self, ctx: &Context, mut f: F) -> Result<(), FsmError>
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

  /// 启用量化（新 id 置位量化标记；写屏障保证阶段切换原子）。
  pub fn enable_quantization(&self) {
    let mut guard = self.barrier.write();
    guard.max_id_for_backfill = self.max_id();
    guard.quantization_enabled = true;
  }

  /// 启用已删 id 复用（量化回填完成后调用）。
  pub fn enable_reuse(&self) {
    self.reuse_enabled.store(true, Ordering::Release);
  }

  /// 量化回填上界（启用量化时的最大 id）。
  pub fn max_id_for_backfill(&self) -> u32 {
    self.barrier.read().max_id_for_backfill
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

  use gxhash::HashMap;
  use parking_lot::Mutex;

  use super::*;
  use crate::store::StoreCallbacks;

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
    fn read_multi<F>(&self, context: u64, keys: &[u8], _length_hint: usize, mut f: F)
    where
      F: FnMut(u32, &[u8]),
    {
      let mut index = 0u32;
      let mut rest = keys;
      while rest.len() >= 4 {
        let len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
        let total = 4 + len;
        if rest.len() < total {
          break;
        }
        let key = &rest[4..total];
        if let Some(value) = self.data.lock().get(&(context, key.to_vec())) {
          f(index, value);
        }
        index += 1;
        rest = &rest[total..];
      }
    }

    fn read<F>(&self, context: u64, key: &[u8], mut f: F) -> bool
    where
      F: FnMut(&[u8]),
    {
      match self.data.lock().get(&(context, key.to_vec())) {
        Some(value) => {
          f(value);
          true
        }
        None => false,
      }
    }

    fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
      self
        .data
        .lock()
        .insert((context, key.to_vec()), value.to_vec());
      true
    }

    fn delete(&self, context: u64, key: &[u8]) -> bool {
      self.data.lock().remove(&(context, key.to_vec())).is_some()
    }

    fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, mut f: F) -> bool
    where
      F: FnMut(&mut [u8]),
    {
      let mut map = self.data.lock();
      let entry = map
        .entry((context, key.to_vec()))
        .or_insert_with(|| vec![0u8; write_len]);
      f(entry);
      true
    }

    fn filter(&self, _context: u64, _internal_id: u32) -> bool {
      false
    }

    fn log(&self, _context: u64, _msg: &str) {}
  }

  fn callbacks() -> Callbacks<MemStore> {
    Callbacks::new(Arc::new(MemStore::new()))
  }

  #[test]
  fn fresh_and_next_id() {
    let ctx = Context::new(8);
    let fsm = FreeSpaceMap::new(&ctx, callbacks(), false, true).unwrap();
    assert!(!fsm.has_free_ids.load(Ordering::Acquire));
    assert_eq!(fsm.next_id(&ctx).unwrap().id(), 0);
    assert_eq!(fsm.next_id(&ctx).unwrap().id(), 1);
    assert_eq!(fsm.total_used(), 2);
  }

  #[test]
  fn mark_free_out_of_range() {
    let ctx = Context::new(8);
    let fsm = FreeSpaceMap::new(&ctx, callbacks(), false, true).unwrap();
    assert_eq!(fsm.mark_free(&ctx, 0), Err(FsmError::IdOutOfRange(0)));
  }

  #[test]
  fn delete_and_reuse() {
    let ctx = Context::new(8);
    let fsm = FreeSpaceMap::new(&ctx, callbacks(), false, true).unwrap();

    for _ in 0u32..64 {
      let _ = fsm.next_id(&ctx).unwrap();
    }
    assert_eq!(fsm.next_id(&ctx).unwrap().id(), 64);

    fsm.mark_free(&ctx, 37).unwrap();
    assert_eq!(fsm.next_id(&ctx).unwrap().id(), 37);
    assert_eq!(fsm.next_id(&ctx).unwrap().id(), 65);
    assert!(!fsm.has_free_ids.load(Ordering::Acquire));
  }

  #[test]
  fn recovery_from_store() {
    let ctx = Context::new(8);
    let cbs = callbacks();
    let fsm = FreeSpaceMap::new(&ctx, cbs.clone(), false, true).unwrap();

    for _ in 0u32..64 {
      let _ = fsm.next_id(&ctx).unwrap();
    }
    fsm.mark_free(&ctx, 37).unwrap();

    // 同一存储重建，状态全量恢复
    let fsm = FreeSpaceMap::new(&ctx, cbs, false, true).unwrap();
    assert_eq!(fsm.max_id() + 1, 64);
    assert!(fsm.has_free_ids.load(Ordering::Acquire));
    assert_eq!(fsm.fast_free_list.len(), 1);
    assert_eq!(fsm.next_id(&ctx).unwrap().id(), 37);
  }

  #[test]
  fn block_expansion() {
    let ctx = Context::new(8);
    let fsm = FreeSpaceMap::new(&ctx, callbacks(), false, true).unwrap();
    for _ in 0u32..BLOCK_SIZE_IDS as u32 + 1 {
      let _ = fsm.next_id(&ctx).unwrap();
    }
    assert_eq!(fsm.max_id(), BLOCK_SIZE_IDS as u32);
    assert_eq!(fsm.total_used(), BLOCK_SIZE_IDS + 1);
  }

  #[test]
  fn visit_used_and_bit_helpers() {
    let ctx = Context::new(8);
    let fsm = FreeSpaceMap::new(&ctx, callbacks(), false, true).unwrap();
    for _ in 0u32..8 {
      let _ = fsm.next_id(&ctx).unwrap();
    }
    fsm.mark_free(&ctx, 5).unwrap();

    let mut seen = Vec::new();
    fsm
      .visit_used(&ctx, |id| {
        seen.push(id);
        true
      })
      .unwrap();
    assert_eq!(seen, vec![0, 1, 2, 3, 4, 6, 7]);

    assert!(bit_used(0b1000_0000, 0));
    assert!(!bit_used(0b1000_0000, 1));
  }
}
