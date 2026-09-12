use std::{
  cell::RefCell,
  mem::MaybeUninit,
  panic::{AssertUnwindSafe, catch_unwind},
  result::Result as StdResult,
  slice::from_raw_parts_mut,
  sync::atomic::Ordering,
};

use bf_tree::{BfTree, LeafInsertResult, LeafReadResult, ScanIter, ScanIterError};

use super::{BfTreeService, MIN_MAX_RECORD_SIZE, STACK_READ_BUF_SIZE, STACK_SCAN_BUF_SIZE};
use crate::{
  error::{Error, Result},
  types::{BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, ScanRecord, ScanReturnField},
};

// 线程本地点读暂存缓冲 (compio 线程每核：同线程串行复用，无竞争、无锁)。
// 容量按需增长到 cb_max_record_size 后终身复用，替代大值 GET 路径每次的
// 「分配 + 清零 + 收缩」三次堆操作；`read`/`read_into` 不重入、不跨 await，
// RefCell 独占借用恒安全。
thread_local! {
  static READ_SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
  static SCAN_SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// 在线程本地读暂存缓冲上执行 `f` (容量按需增长到 `max_record_size` 后终身复用；重入降级为临时堆分配)
#[inline]
fn with_read_scratch<R>(max_record_size: usize, f: impl FnOnce(&mut [u8]) -> R) -> R {
  READ_SCRATCH.with(|scratch| {
    if let Ok(mut s) = scratch.try_borrow_mut() {
      if s.len() < max_record_size {
        s.resize(max_record_size, 0);
      }
      f(&mut s[..max_record_size])
    } else {
      let mut fallback = vec![0u8; max_record_size];
      f(&mut fallback)
    }
  })
}

/// 在扫描缓冲区上执行 `f` (线程本地暂存复用，零堆分配与零栈清零；重入降级为临时分配)
#[inline]
fn with_scan_scratch<R>(max_record_size: usize, f: impl FnOnce(&mut [u8]) -> R) -> R {
  SCAN_SCRATCH.with(|scratch| {
    if let Ok(mut s) = scratch.try_borrow_mut() {
      if s.len() < max_record_size {
        s.resize(max_record_size, 0);
      }
      f(&mut s[..max_record_size])
    } else {
      let mut fallback = vec![0u8; max_record_size];
      f(&mut fallback)
    }
  })
}

/// 将 bf_tree::ScanIterError 映射为可读字符串 (替代 Debug 格式化)
#[inline]
fn scan_iter_error_to_string(e: ScanIterError) -> &'static str {
  match e {
    ScanIterError::CacheOnlyMode => "CacheOnlyMode",
    ScanIterError::InvalidStartKey => "InvalidStartKey",
    ScanIterError::InvalidEndKey => "InvalidEndKey",
    ScanIterError::InvalidCount => "InvalidCount",
    ScanIterError::InvalidKeyRange => "InvalidKeyRange",
  }
}

/// 全表扫描起始键常量 (1:1 对标 C# ScanAll 使用的单一 0 字节起始键)
pub const SCAN_ALL_START_KEY: &[u8] = &[0];

impl BfTreeService {
  /// 插入键值对 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:Insert)
  ///
  /// 零锁直调：通过原子指针直接获取 &BfTree，消除 RwLock 读锁争用。
  /// 顶部登记写者微守卫：常态无屏障时消除 SeqCst 争用。
  /// CPR 快照不经屏障 (引擎阶段协议与点写并发安全，对标 C# 非阻塞语义)。
  ///
  /// 空值守卫的真实依据：底层 bf-tree 叶子插入含 `debug_assert!(!value.is_empty())`
  /// (nodes/leaf_node.rs)，空值在 dev/test 构建 panic、release 构建行为未定义
  /// (C# 侧原生库同样从未容许空值入树，调用方契约排除)；此处提前以
  /// [`BfTreeInsertResult::InvalidKV`] 拒绝，把引擎断言变成结构化结果码。
  #[inline]
  pub fn insert(&self, key: &[u8], value: &[u8]) -> BfTreeInsertResult {
    if value.is_empty() {
      return BfTreeInsertResult::InvalidKV;
    }
    if self.barriers.load(Ordering::Relaxed) != 0 {
      self.wait_for_barrier();
    }
    let Ok(tree) = self.tree_ref() else {
      return BfTreeInsertResult::InvalidArguments;
    };
    match tree.insert(key, value) {
      LeafInsertResult::Success => BfTreeInsertResult::Success,
      LeafInsertResult::InvalidKV(_) => BfTreeInsertResult::InvalidKV,
    }
  }

  /// 零拷贝读取键对应的值切片并传给闭包 (零堆分配，零锁直调)
  pub fn read_callback<R>(&self, key: &[u8], f: impl FnOnce(BfTreeReadResult, &[u8]) -> R) -> R {
    let Ok(tree) = self.tree_ref() else {
      return f(BfTreeReadResult::InvalidArguments, &[]);
    };
    let max_record_size = self.max_record_size();
    if max_record_size <= STACK_READ_BUF_SIZE {
      let mut stack_buf = MaybeUninit::<[u8; STACK_READ_BUF_SIZE]>::uninit();
      // SAFETY: read_tree 成功时将值完全覆盖写入 buf[..len]，仅暴露已初始化的 [..len] 切片
      let buf =
        unsafe { from_raw_parts_mut(stack_buf.as_mut_ptr() as *mut u8, STACK_READ_BUF_SIZE) };
      let (res, len) = Self::read_tree(tree, key, buf);
      f(res, &buf[..len])
    } else {
      with_read_scratch(max_record_size, |scratch| {
        let (res, len) = Self::read_tree(tree, key, scratch);
        f(res, &scratch[..len])
      })
    }
  }

  /// 检查指定键是否存在 (带安全缓冲的零堆分配直测)
  #[inline]
  pub fn contains_key(&self, key: &[u8]) -> bool {
    self.read_callback(key, |res, _val| res == BfTreeReadResult::Found)
  }

  /// 读取键对应的值 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:Read)
  ///
  /// ≤4096 字节值走栈缓冲区零堆分配；更大值复用线程本地暂存缓冲。
  /// 底层 bf-tree 要求读取缓冲区不小于值长度（否则越界 panic），此处缓冲区恒 ≥ cb_max_record_size，绝无越界。
  ///
  /// 缓冲上限的载入位于 `with_tree` 读锁**之内**：与 [`recover_in_place`](Self::recover_in_place)
  /// 写锁临界段内「换树 + 发布新上限」构成 happens-before，读侧借得的树实例与
  /// sizing 依据严格同源，杜绝「小缓冲撞大树值」的引擎越界 panic。
  ///
  /// 大值路径的时间/空间复杂度优化 (compio 线程每核：同线程串行复用，无竞争)：
  /// 旧实现每次 GET 按 cb_max_record_size 堆分配 + 整段清零 + shrink_to_fit 二次
  /// 收缩 (2 次分配 + O(max_record_size) memset)；现改为线程本地暂存一次分配终身
  /// 复用，命中后仅按值长精确拷出 (1 次分配、零清零)，稳态空间 O(1)/线程。
  pub fn read(&self, key: &[u8]) -> (BfTreeReadResult, Option<Vec<u8>>) {
    self.read_callback(key, |res, bytes| {
      (
        res,
        (res == BfTreeReadResult::Found).then(|| bytes.to_vec()),
      )
    })
  }

  /// 在已借得的引擎实例上执行点读并映射结果码
  ///
  /// 调用方必须已持 `tree` 读锁（或独占 Arc）：与换树路径的 sizing 载入保持同步
  #[inline]
  fn read_tree(tree: &BfTree, key: &[u8], out_buf: &mut [u8]) -> (BfTreeReadResult, usize) {
    match tree.read(key, out_buf) {
      LeafReadResult::Found(n) => (BfTreeReadResult::Found, n as usize),
      LeafReadResult::NotFound => (BfTreeReadResult::NotFound, 0),
      LeafReadResult::Deleted => (BfTreeReadResult::Deleted, 0),
      LeafReadResult::InvalidKey => (BfTreeReadResult::InvalidKey, 0),
    }
  }

  #[inline]
  fn read_into_fallback(
    tree: &BfTree,
    key: &[u8],
    out_buf: &mut [u8],
    max_record_size: usize,
  ) -> (BfTreeReadResult, usize) {
    if max_record_size <= STACK_READ_BUF_SIZE {
      let mut stack_buf = MaybeUninit::<[u8; STACK_READ_BUF_SIZE]>::uninit();
      let buf =
        unsafe { from_raw_parts_mut(stack_buf.as_mut_ptr() as *mut u8, STACK_READ_BUF_SIZE) };
      let (res, len) = Self::read_tree(tree, key, buf);
      if res == BfTreeReadResult::Found {
        if len <= out_buf.len() {
          out_buf[..len].copy_from_slice(&buf[..len]);
          (res, len)
        } else {
          (BfTreeReadResult::InvalidArguments, 0)
        }
      } else {
        (res, 0)
      }
    } else {
      with_read_scratch(max_record_size, |scratch| {
        let (res, len) = Self::read_tree(tree, key, scratch);
        if res == BfTreeReadResult::Found {
          if len <= out_buf.len() {
            out_buf[..len].copy_from_slice(&scratch[..len]);
            (res, len)
          } else {
            (BfTreeReadResult::InvalidArguments, 0)
          }
        } else {
          (res, 0)
        }
      })
    }
  }

  /// 读取键对应的值到用户提供的输出切片中 (带输出缓冲区的零堆分配直读，零锁直调)
  /// 对标 BfTreeService.cs 的 Read(`ReadOnlySpan<byte>` key, `Span<byte>` outputBuffer, `out int` bytesWritten)
  /// 重载（读入调用方缓冲区形态；返回 Vec 的主形态由 read 映射）
  ///
  /// 当 out_buf 容量 ≥ cb_max_record_size 时走直读快路径；否则改用内部安全缓冲读取后按需拷贝，
  /// 值超出 out_buf 容量时返回 InvalidArguments（彻底消除底层越界与 catch_unwind 依赖）。
  #[inline]
  pub fn read_into(&self, key: &[u8], out_buf: &mut [u8]) -> (BfTreeReadResult, usize) {
    let Ok(tree) = self.tree_ref() else {
      return (BfTreeReadResult::InvalidArguments, 0);
    };
    let max_record_size = self.max_record_size();
    if out_buf.len() >= max_record_size {
      Self::read_tree(tree, key, out_buf)
    } else {
      Self::read_into_fallback(tree, key, out_buf, max_record_size)
    }
  }

  /// 删除指定键 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:Delete)
  ///
  /// 零锁直调：通过原子指针直接获取 &BfTree，消除 RwLock 读锁争用。
  /// 顶部登记写者微守卫：常态无屏障时消除 SeqCst 争用。
  #[inline]
  pub fn delete(&self, key: &[u8]) -> BfTreeDeleteResult {
    if self.barriers.load(Ordering::Relaxed) != 0 {
      self.wait_for_barrier();
    }
    let Ok(tree) = self.tree_ref() else {
      return BfTreeDeleteResult::InvalidArguments;
    };
    tree.delete(key);
    BfTreeDeleteResult::Success
  }

  /// 空操作测量纯调用开销 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:Noop)
  /// 保留 key 入参以准确模拟实际操作的参数传递开销
  #[inline]
  pub fn noop(&self, _key: &[u8]) -> i32 {
    0
  }

  // ---------------------------------------------------------------
  // 静态原生指针操作 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:InsertByPtr / ReadByPtr / ReadByPtrInto / DeleteByPtr)
  // ---------------------------------------------------------------
  // 1:1 对标 Garnet BfTreeService 的指针直调原生 API (消除所有包装开销)
  // ---------------------------------------------------------------

  /// 原生指针空操作测量纯调用开销 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:NoopByPtr)
  /// 保留 tree_ptr 和 key 入参以准确模拟原生 FFI 调用的参数传递开销
  ///
  /// # Safety
  /// 调用方传入的原生树指针标识用于对标原生调用开销，无需保证指针有效。
  #[inline]
  pub unsafe fn noop_by_ptr(_tree_ptr: u64, _key: &[u8]) -> i32 {
    0
  }

  /// 通过原生指针直接插入 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:InsertByPtr)
  ///
  /// # Safety
  /// 调用方必须确保 `tree_ptr` 指向有效且未被释放的 `BfTree` 实例。
  #[inline]
  pub unsafe fn insert_by_ptr(tree_ptr: u64, key: &[u8], value: &[u8]) -> BfTreeInsertResult {
    // 空指针拒绝映射为 InvalidArguments (1:1 对标原生互操作层 bftree_insert 的
    // INSERT_INVALID_ARGS 分支)；空值拒绝映射为 InvalidKV (引擎叶子插入断言非空值)
    if tree_ptr == 0 {
      return BfTreeInsertResult::InvalidArguments;
    }
    if value.is_empty() {
      return BfTreeInsertResult::InvalidKV;
    }
    let tree = unsafe { &*(tree_ptr as usize as *const BfTree) };
    match tree.insert(key, value) {
      LeafInsertResult::Success => BfTreeInsertResult::Success,
      LeafInsertResult::InvalidKV(_) => BfTreeInsertResult::InvalidKV,
    }
  }

  /// 通过原生指针直接读取到缓冲区 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:ReadByPtrInto)
  ///
  /// # Safety
  /// 调用方必须确保 `tree_ptr` 指向有效且未被释放的 `BfTree` 实例。
  #[inline]
  pub unsafe fn read_by_ptr_into(
    tree_ptr: u64,
    key: &[u8],
    out_buf: &mut [u8],
  ) -> (BfTreeReadResult, usize) {
    if tree_ptr == 0 {
      return (BfTreeReadResult::InvalidArguments, 0);
    }
    let tree = unsafe { &*(tree_ptr as usize as *const BfTree) };
    let max_record_size = tree
      .config()
      .get_cb_max_record_size()
      .max(MIN_MAX_RECORD_SIZE);
    if out_buf.len() >= max_record_size {
      Self::read_tree(tree, key, out_buf)
    } else {
      Self::read_into_fallback(tree, key, out_buf, max_record_size)
    }
  }

  /// 通过原生指针直接读取 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:ReadByPtr)
  ///
  /// # Safety
  /// 调用方必须确保 `tree_ptr` 指向有效且未被释放的 `BfTree` 实例。
  #[inline]
  pub unsafe fn read_by_ptr(tree_ptr: u64, key: &[u8]) -> (BfTreeReadResult, Option<Vec<u8>>) {
    if tree_ptr == 0 {
      return (BfTreeReadResult::InvalidArguments, None);
    }
    let tree = unsafe { &*(tree_ptr as usize as *const BfTree) };
    let max_record_size = tree
      .config()
      .get_cb_max_record_size()
      .max(MIN_MAX_RECORD_SIZE);
    if max_record_size <= STACK_READ_BUF_SIZE {
      let mut stack_buf = MaybeUninit::<[u8; STACK_READ_BUF_SIZE]>::uninit();
      let buf =
        unsafe { from_raw_parts_mut(stack_buf.as_mut_ptr() as *mut u8, STACK_READ_BUF_SIZE) };
      let (res, len) = Self::read_tree(tree, key, buf);
      (
        res,
        (res == BfTreeReadResult::Found).then(|| buf[..len].to_vec()),
      )
    } else {
      with_read_scratch(max_record_size, |scratch| {
        let (res, len) = Self::read_tree(tree, key, scratch);
        (
          res,
          (res == BfTreeReadResult::Found).then(|| scratch[..len].to_vec()),
        )
      })
    }
  }

  /// 通过原生指针直接删除 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:DeleteByPtr)
  ///
  /// # Safety
  /// 调用方必须确保 `tree_ptr` 指向有效且未被释放的 `BfTree` 实例。
  #[inline]
  pub unsafe fn delete_by_ptr(tree_ptr: u64, key: &[u8]) -> BfTreeDeleteResult {
    if tree_ptr == 0 {
      return BfTreeDeleteResult::InvalidArguments;
    }
    let tree = unsafe { &*(tree_ptr as usize as *const BfTree) };
    tree.delete(key);
    BfTreeDeleteResult::Success
  }

  /// 通过原生指针直接执行带数量范围扫描回调 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:ScanWithCountByPtrCallback)
  ///
  /// # Safety
  /// 调用方必须确保 `tree_ptr` 指向有效且未被释放的 `BfTree` 实例。
  pub unsafe fn scan_with_count_by_ptr_callback<F>(
    tree_ptr: u64,
    start_key: &[u8],
    count: usize,
    return_field: ScanReturnField,
    on_record: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    if tree_ptr == 0 {
      return Err(Error::InvalidArgument("原生树指针为空".into()));
    }
    if count == 0 {
      return Ok(0);
    }
    let tree = unsafe { &*(tree_ptr as usize as *const BfTree) };
    let mut iter = match catch_unwind(AssertUnwindSafe(|| {
      tree.scan_with_count(start_key, count, return_field)
    })) {
      Ok(Ok(iter)) => iter,
      Ok(Err(e)) => {
        return Err(Error::InvalidArgument(
          scan_iter_error_to_string(e).to_string(),
        ));
      }
      Err(_) => return Err(Error::Scan("底层引擎扫描初始化异常".into())),
    };
    let max_record_size = tree
      .config()
      .get_cb_max_record_size()
      .max(MIN_MAX_RECORD_SIZE);
    if max_record_size <= STACK_SCAN_BUF_SIZE {
      let mut stack_buf = MaybeUninit::<[u8; STACK_SCAN_BUF_SIZE]>::uninit();
      let buf =
        unsafe { from_raw_parts_mut(stack_buf.as_mut_ptr() as *mut u8, STACK_SCAN_BUF_SIZE) };
      catch_unwind(AssertUnwindSafe(|| {
        Self::drain_scan_iter(&mut iter, buf, return_field, on_record)
      }))
      .unwrap_or_else(|_| Err(Error::Scan("底层引擎扫描排空异常".into())))
    } else {
      with_scan_scratch(max_record_size, |buf| {
        catch_unwind(AssertUnwindSafe(|| {
          Self::drain_scan_iter(&mut iter, buf, return_field, on_record)
        }))
        .unwrap_or_else(|_| Err(Error::Scan("底层引擎扫描排空异常".into())))
      })
    }
  }

  /// 通过原生指针直接执行闭区间范围扫描回调 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:ScanWithEndKeyByPtrCallback)
  ///
  /// # Safety
  /// 调用方必须确保 `tree_ptr` 指向有效且未被释放的 `BfTree` 实例。
  pub unsafe fn scan_with_end_key_by_ptr_callback<F>(
    tree_ptr: u64,
    start_key: &[u8],
    end_key: &[u8],
    return_field: ScanReturnField,
    on_record: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    if tree_ptr == 0 {
      return Err(Error::InvalidArgument("原生树指针为空".into()));
    }
    if start_key > end_key {
      return Ok(0);
    }
    let tree = unsafe { &*(tree_ptr as usize as *const BfTree) };
    let mut iter = match catch_unwind(AssertUnwindSafe(|| {
      tree.scan_with_end_key(start_key, end_key, return_field)
    })) {
      Ok(Ok(iter)) => iter,
      Ok(Err(e)) => {
        return Err(Error::InvalidArgument(
          scan_iter_error_to_string(e).to_string(),
        ));
      }
      Err(_) => return Err(Error::Scan("底层引擎扫描初始化异常".into())),
    };
    let max_record_size = tree
      .config()
      .get_cb_max_record_size()
      .max(MIN_MAX_RECORD_SIZE);
    if max_record_size <= STACK_SCAN_BUF_SIZE {
      let mut stack_buf = MaybeUninit::<[u8; STACK_SCAN_BUF_SIZE]>::uninit();
      let buf =
        unsafe { from_raw_parts_mut(stack_buf.as_mut_ptr() as *mut u8, STACK_SCAN_BUF_SIZE) };
      catch_unwind(AssertUnwindSafe(|| {
        Self::drain_scan_iter(&mut iter, buf, return_field, on_record)
      }))
      .unwrap_or_else(|_| Err(Error::Scan("底层引擎扫描排空异常".into())))
    } else {
      with_scan_scratch(max_record_size, |buf| {
        catch_unwind(AssertUnwindSafe(|| {
          Self::drain_scan_iter(&mut iter, buf, return_field, on_record)
        }))
        .unwrap_or_else(|_| Err(Error::Scan("底层引擎扫描排空异常".into())))
      })
    }
  }

  /// 基于数量的流式范围扫描回调
  ///
  /// count == 0 直接返回 0 (1:1 对标 Garnet 原生层允许 count=0 的行为)
  pub fn scan_with_count_callback<F>(
    &self,
    start_key: &[u8],
    count: usize,
    return_field: ScanReturnField,
    on_record: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    if count == 0 {
      return Ok(0);
    }
    self.scan_callback(
      |tree| tree.scan_with_count(start_key, count, return_field),
      return_field,
      on_record,
    )
  }

  /// 基于数量的范围扫描并返回记录列表 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:ScanWithCount)
  pub fn scan_with_count(
    &self,
    start_key: &[u8],
    count: usize,
    return_field: ScanReturnField,
  ) -> Result<Vec<ScanRecord>> {
    let mut records = Vec::with_capacity(count.min(64));
    self.scan_with_count_callback(
      start_key,
      count,
      return_field,
      ScanRecord::sink(&mut records),
    )?;
    Ok(records)
  }

  /// 闭区间流式范围扫描回调
  ///
  /// start_key > end_key 时视为空区间直接返回 0 (1:1 对标 Garnet 原生层行为)
  pub fn scan_with_end_key_callback<F>(
    &self,
    start_key: &[u8],
    end_key: &[u8],
    return_field: ScanReturnField,
    on_record: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    if start_key > end_key {
      return Ok(0);
    }
    self.scan_callback(
      |tree| tree.scan_with_end_key(start_key, end_key, return_field),
      return_field,
      on_record,
    )
  }

  /// 闭区间范围扫描并返回记录列表 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:ScanWithEndKey)
  pub fn scan_with_end_key(
    &self,
    start_key: &[u8],
    end_key: &[u8],
    return_field: ScanReturnField,
  ) -> Result<Vec<ScanRecord>> {
    let mut records = Vec::with_capacity(32);
    self.scan_with_end_key_callback(
      start_key,
      end_key,
      return_field,
      ScanRecord::sink(&mut records),
    )?;
    Ok(records)
  }

  /// 全表顺序流式扫描回调
  pub fn scan_all_callback<F>(&self, return_field: ScanReturnField, on_record: F) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    self.scan_with_count_callback(SCAN_ALL_START_KEY, usize::MAX, return_field, on_record)
  }

  /// 全表顺序扫描 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:ScanAll)
  pub fn scan_all(&self, return_field: ScanReturnField) -> Result<Vec<ScanRecord>> {
    let mut records = Vec::with_capacity(32);
    self.scan_all_callback(return_field, ScanRecord::sink(&mut records))?;
    Ok(records)
  }

  /// 扫描统一驱动：经底层校验构造迭代器 (非法键/区间返回 Err，杜绝底层未定义行为)，逐条填充回调。
  ///
  /// 迭代前仅克隆一次底层 Arc 并随即释放包装读锁：整个用户回调期间不持有任何服务级锁，
  /// 回调因此可安全重入本服务的点读 (read/read_into) 乃至 dispose 而无包装层自死锁风险
  /// (1:1 对标 C# 扫描期间无托管锁的语义；parking_lot 写优先，持读锁跨回调时一旦有
  /// dispose 排队，回调内任何重入读取都将永久阻塞)。Arc 同时保证迭代期间底层引擎实例
  /// 存活，dispose 与扫描并发时扫描仍可在存活引擎上安全完成 (对标 C# LightEpoch 延迟释放)。
  ///
  /// 注意：回调不得对同一棵树重入写入 (insert/delete/scan)——底层引擎扫描持有叶子共享
  /// 闩锁，同线程重入写同叶子会在引擎闩锁层自死锁 (与 C# 原生层约束一致，非包装层问题)。
  /// 扫描迭代器排空回调 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:DrainScanIteratorWithCallback)
  #[inline]
  fn drain_scan_iter<F>(
    iter: &mut ScanIter<'_, '_>,
    buf: &mut [u8],
    return_field: ScanReturnField,
    mut on_record: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    let mut scanned = 0;
    match return_field {
      ScanReturnField::KeyAndValue => {
        while let Some((k_len, v_len)) = iter.next(buf) {
          scanned += 1;
          if !on_record(&buf[..k_len], &buf[k_len..k_len + v_len]) {
            break;
          }
        }
      }
      ScanReturnField::Key => {
        while let Some((k_len, _)) = iter.next(buf) {
          scanned += 1;
          if !on_record(&buf[..k_len], &[]) {
            break;
          }
        }
      }
      ScanReturnField::Value => {
        while let Some((k_len, v_len)) = iter.next(buf) {
          scanned += 1;
          if !on_record(&[], &buf[k_len..k_len + v_len]) {
            break;
          }
        }
      }
    }
    Ok(scanned)
  }

  /// 扫描统一驱动：经底层校验构造迭代器，对齐 C# 扫描缓冲逻辑 (栈缓冲 8192 字节优先直读，消除无谓的 thread_local 借用开销)
  /// 排空回调经 drain_scan_iter 复用；驱动面对标 BfTreeService.cs 的
  /// ScanWithCountByPtrCallback / ScanWithEndKeyByPtrCallback 公共流程
  fn scan_callback<F>(
    &self,
    make_iter: impl FnOnce(&BfTree) -> StdResult<ScanIter<'_, '_>, ScanIterError>,
    return_field: ScanReturnField,
    on_record: F,
  ) -> Result<usize>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    // 单次原子引用计数开销换取回调重入安全，绝不持锁跨用户回调
    let tree = self.tree_arc()?;
    let mut iter = match catch_unwind(AssertUnwindSafe(|| make_iter(&tree))) {
      Ok(Ok(iter)) => iter,
      Ok(Err(e)) => {
        return Err(Error::InvalidArgument(
          scan_iter_error_to_string(e).to_string(),
        ));
      }
      Err(_) => return Err(Error::Scan("底层引擎扫描初始化异常".into())),
    };

    let max_record_size = self.max_record_size();
    if max_record_size <= STACK_SCAN_BUF_SIZE {
      let mut stack_buf = MaybeUninit::<[u8; STACK_SCAN_BUF_SIZE]>::uninit();
      let buf =
        unsafe { from_raw_parts_mut(stack_buf.as_mut_ptr() as *mut u8, STACK_SCAN_BUF_SIZE) };
      catch_unwind(AssertUnwindSafe(|| {
        Self::drain_scan_iter(&mut iter, buf, return_field, on_record)
      }))
      .unwrap_or_else(|_| Err(Error::Scan("底层引擎扫描排空异常".into())))
    } else {
      with_scan_scratch(max_record_size, |buf| {
        catch_unwind(AssertUnwindSafe(|| {
          Self::drain_scan_iter(&mut iter, buf, return_field, on_record)
        }))
        .unwrap_or_else(|_| Err(Error::Scan("底层引擎扫描排空异常".into())))
      })
    }
  }
}
