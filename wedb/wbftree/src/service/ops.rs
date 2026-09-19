use std::{
  cell::RefCell,
  mem::MaybeUninit,
  panic::{AssertUnwindSafe, catch_unwind},
  result::Result as StdResult,
  slice::from_raw_parts_mut,
};

use bf_tree::{BfTree, LeafReadResult, ScanIter, ScanIterError};

use super::{BfTreeService, STACK_BUF_SIZE};
use crate::{
  error::{Error, Result},
  types::{BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, ScanRecord, ScanReturnField},
};

// 线程本地点读/扫描暂存缓冲 (compio 线程每核：同线程串行复用，无竞争、无锁)。
// 容量按需增长到 max_record_size 后终身复用，替代大值路径每次的
// 「分配 + 清零 + 收缩」三次堆操作；`read`/`read_into`/扫描不跨 await，
// RefCell 独占借用恒安全；回调内重入读/扫描借用冲突时降级为临时堆分配。
thread_local! {
  static BUF_SCRATCH: RefCell<Vec<u8>> = const { RefCell::new(Vec::new()) };
}

/// 读写缓冲统一选路单点：`max_record_size` ≤ 栈容量走栈缓冲零堆零初始化，
/// 否则复用线程本地暂存 (容量按需增长到 `max_record_size` 后终身复用；
/// 重入降级为临时堆分配)。读/扫描与 upsert 内核的存在性前查共用此选路。
///
/// 调用方契约：缓冲区长度恒为 `max_record_size ≥ cb_max_record_size`，
/// 底层引擎读/扫描要求缓冲区不小于值长度，绝无越界；栈路径仅暴露
/// 引擎完全覆盖写入的前缀切片。
#[inline]
pub(super) fn with_read_buffer<R>(max_record_size: usize, f: impl FnOnce(&mut [u8]) -> R) -> R {
  BUF_SCRATCH.with(|scratch| {
    if max_record_size <= STACK_BUF_SIZE {
      let mut stack_buf = MaybeUninit::<[u8; STACK_BUF_SIZE]>::uninit();
      // SAFETY: f 仅在 buf[..max_record_size] 上读写，读侧仅暴露引擎完全覆盖写入的 [..len] 切片
      let buf = unsafe { from_raw_parts_mut(stack_buf.as_mut_ptr() as *mut u8, max_record_size) };
      f(buf)
    } else if let Ok(mut s) = scratch.try_borrow_mut() {
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

impl BfTreeService {
  /// 插入键值对 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:Insert)
  ///
  /// 树内写入唯一真值路径是 [`bulk_load`](Self::bulk_load) 排序批量装载内核，
  /// 本方法即该内核的一元素栈上特例 (单条与批量同源，绝无第二条写路径)。
  ///
  /// 零锁直调：内核经 [`with_tree`](Self::with_tree) 无锁借用 &BfTree，
  /// (1:1 对标 Garnet 纯指针直读)。CPR 快照不经屏障 (引擎阶段协议与点写并发
  /// 安全，对标 C# 非阻塞语义)。
  ///
  /// 空值守卫的真实依据：底层 bf-tree 叶子插入含 `debug_assert!(!value.is_empty())`
  /// (nodes/leaf_node.rs)，空值在 dev/test 构建 panic、release 构建行为未定义
  /// (C# 侧原生库同样从未容许空值入树，调用方契约排除)；内核前置校验提前以
  /// [`BfTreeInsertResult::InvalidKV`] 拒绝，把引擎断言变成结构化结果码。
  #[inline]
  pub fn insert(&self, key: &[u8], value: &[u8]) -> BfTreeInsertResult {
    match self.bulk_load(&[(key, value)]) {
      Ok(_) => BfTreeInsertResult::Success,
      Err(res) => res,
    }
  }

  /// 零拷贝读取键对应的值切片并传给闭包 (零堆分配，零锁直调)
  ///
  /// 大值路径的时间/空间复杂度优化 (compio 线程每核：同线程串行复用，无竞争)：
  /// 旧实现每次 GET 按 cb_max_record_size 堆分配 + 整段清零 + shrink_to_fit 二次
  /// 收缩 (2 次分配 + O(max_record_size) memset)；现改为线程本地暂存一次分配终身
  /// 复用，命中后仅按值长精确拷出 (1 次分配、零清零)，稳态空间 O(1)/线程。
  pub fn read_callback<R>(&self, key: &[u8], f: impl FnOnce(BfTreeReadResult, &[u8]) -> R) -> R {
    // f 恰调用一次：with_tree 返回 Some 时引擎分支执行，None 时兜底分支执行，二者互斥
    let mut f = Some(f);
    self
      .with_tree(|tree| {
        with_read_buffer(self.max_record_size(), |buf| {
          let (res, len) = Self::read_tree(tree, key, buf);
          // SAFETY: 互斥分支保证此 unwrap 必为 Some
          (unsafe { f.take().unwrap_unchecked() })(res, &buf[..len])
        })
      })
      .unwrap_or_else(|| {
        // SAFETY: 引擎分支未执行，f 仍在
        (unsafe { f.take().unwrap_unchecked() })(BfTreeReadResult::InvalidArguments, &[])
      })
  }

  /// 检查指定键是否存在 (带安全缓冲的零堆分配直测)
  #[inline]
  pub fn contains_key(&self, key: &[u8]) -> bool {
    self.read_callback(key, |res, _val| res == BfTreeReadResult::Found)
  }

  /// 读取键对应的值 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:Read)
  ///
  /// ≤栈容量值走栈缓冲区零堆分配；更大值复用线程本地暂存缓冲。
  /// 缓冲上限随实例构造一次定型、终身不变 (本层无原地换树路径)，
  /// 读侧 sizing 与树实例天然同源。
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
  /// 调用方必须保证 `tree` 存活 (经 [`BfTreeService::with_tree`] 无锁借得，
  /// Guard 存续期内引用保活，实例树终身不变更)
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
    with_read_buffer(max_record_size, |buf| {
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
    })
  }

  /// 读取键对应的值到用户提供的输出切片中 (带输出缓冲区的零堆分配直读，零锁直调)
  /// 对标 BfTreeService.cs 的 Read(`ReadOnlySpan<byte>` key, `Span<byte>` outputBuffer, `out int` bytesWritten)
  /// 重载（读入调用方缓冲区形态；返回 Vec 的主形态由 read 映射）
  ///
  /// 当 out_buf 容量 ≥ cb_max_record_size 时走直读快路径；否则改用内部安全缓冲读取后按需拷贝，
  /// 值超出 out_buf 容量时返回 InvalidArguments（彻底消除底层越界与 catch_unwind 依赖）。
  #[inline]
  pub fn read_into(&self, key: &[u8], out_buf: &mut [u8]) -> (BfTreeReadResult, usize) {
    let max_record_size = self.max_record_size();
    self
      .with_tree(|tree| {
        if out_buf.len() >= max_record_size {
          Self::read_tree(tree, key, out_buf)
        } else {
          Self::read_into_fallback(tree, key, out_buf, max_record_size)
        }
      })
      .unwrap_or((BfTreeReadResult::InvalidArguments, 0))
  }

  /// 删除指定键 (1:1 对标 libs/native/bftree-garnet/BfTreeService.cs:Delete)
  ///
  /// 零锁直调：经 [`BfTreeService::with_tree`] 无锁借用 &BfTree
  /// (1:1 对标 Garnet 纯指针直读)。
  #[inline]
  pub fn delete(&self, key: &[u8]) -> BfTreeDeleteResult {
    self
      .with_tree(|tree| {
        tree.delete(key);
        BfTreeDeleteResult::Success
      })
      .unwrap_or(BfTreeDeleteResult::InvalidArguments)
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
      |tree| tree.scan_with_count(start_key, count, return_field.into()),
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
      |tree| tree.scan_with_end_key(start_key, end_key, return_field.into()),
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

  /// 扫描统一驱动：经底层校验构造迭代器 (非法键/区间返回 Err，杜绝底层未定义行为)，逐条填充回调。
  ///
  /// 迭代前仅克隆一次底层 Arc (load_full，单次引用计数)：整个用户回调期间不持有任何
  /// 服务级锁，回调因此可安全重入本服务的点读 (read/read_into) 乃至 dispose
  /// (1:1 对标 C# 扫描期间无托管锁的语义)。Arc 同时保证迭代期间底层引擎实例
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

  /// 扫描统一驱动：经底层校验构造迭代器，缓冲选路统一经
  /// [`with_read_buffer`] 单点 (栈缓冲 8192 字节优先直读，消除无谓的
  /// thread_local 借用开销)；排空回调经 drain_scan_iter 复用；驱动面对标
  /// BfTreeService.cs 的 ScanWithCountByPtrCallback / ScanWithEndKeyByPtrCallback 公共流程
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

    with_read_buffer(self.max_record_size(), |buf| {
      catch_unwind(AssertUnwindSafe(|| {
        Self::drain_scan_iter(&mut iter, buf, return_field, on_record)
      }))
      .unwrap_or_else(|_| Err(Error::Scan("底层引擎扫描排空异常".into())))
    })
  }
}
