//! List 列表操作 (ListTreeOps)
//!
//! - 序号大端符号翻转：`order_idx = ((index as u64) ^ (1 << 63)).to_be_bytes()`
//! - Key: `[TreePrefix::ListIndex as u8: 1B][order_idx: 8B be]` (定长 9 字节)
//! - Val: `element: &[u8]`
//! - `ListStub`: 51 字节（`RangeIndexStub: 35B` + `head: i64` + `tail: i64`）
//! - 接口：`lpush`, `rpush`, `lpop`, `rpop`, `lindex`, `lrange`

use wbftree::{
  BfTreeInsertResult, BfTreeReadResult, BfTreeService, RangeIndexStub, ScanReturnField,
};

use crate::{CollectionError, Result, prefix::TreePrefix};

/// List 存根总字节大小 (35 字节 RangeIndexStub + 8 字节 head + 8 字节 tail)
pub const LIST_STUB_SIZE: usize = 51;

/// 有符号 64 位序号转换为大端保序键 (8 字节)
#[inline]
pub const fn order_idx_from_i64(index: i64) -> [u8; 8] {
  let order = (index as u64) ^ (1 << 63);
  order.to_be_bytes()
}

/// 大端保序键解码为有符号 64 位序号
#[inline]
pub const fn i64_from_order_idx(bytes: [u8; 8]) -> i64 {
  let order = u64::from_be_bytes(bytes);
  (order ^ (1 << 63)) as i64
}

/// 有符号 64 位序号转换为带统一前缀的 9 字节 BfTree 物理键
#[inline(always)]
pub const fn list_key_from_i64(index: i64) -> [u8; 9] {
  let bytes = order_idx_from_i64(index);
  [
    TreePrefix::ListIndex as u8,
    bytes[0],
    bytes[1],
    bytes[2],
    bytes[3],
    bytes[4],
    bytes[5],
    bytes[6],
    bytes[7],
  ]
}

/// 从 9 字节物理键解码为有符号 64 位序号
#[inline(always)]
pub const fn i64_from_list_key(key: [u8; 9]) -> i64 {
  i64_from_order_idx([
    key[1], key[2], key[3], key[4], key[5], key[6], key[7], key[8],
  ])
}

/// 定长 51 字节 List 存根元数据 (组合 35 字节 RangeIndexStub 与双端索引)
#[derive(Copy, Clone, Debug, PartialEq, Eq, Default)]
pub struct ListStub {
  /// 底层 RangeIndexStub
  pub range_stub: RangeIndexStub,
  /// 头部元素当前序号（左闭右开区间 `[head, tail)`）
  pub head: i64,
  /// 尾部元素下一可用序号
  pub tail: i64,
}

/// 将 Redis 风格的 [start, stop] 相对索引标准化为左闭右闭区间 [s, e]（基于绝对序号偏移）
#[inline]
pub const fn normalize_range(len: usize, start: i64, stop: i64) -> Option<(usize, usize)> {
  if len == 0 {
    return None;
  }
  let len_i64 = len as i64;
  let s = if start >= 0 {
    start
  } else {
    match len_i64.checked_add(start) {
      Some(v) => v,
      None => 0,
    }
  };
  let s = if s < 0 { 0 } else { s as usize };

  let e = if stop >= 0 {
    stop
  } else {
    match len_i64.checked_add(stop) {
      Some(v) => v,
      None => -1,
    }
  };
  if e < 0 {
    return None;
  }
  let e = if e >= len_i64 { len - 1 } else { e as usize };

  if s > e { None } else { Some((s, e)) }
}

impl ListStub {
  /// 创建新的 List 存根
  #[inline]
  pub fn new(range_stub: RangeIndexStub, head: i64, tail: i64) -> Self {
    Self {
      range_stub,
      head,
      tail,
    }
  }

  /// 当列表为空时重置 head 和 tail 序号为 0，防止双端索引无限漂移
  #[inline]
  pub fn reset_if_empty(&mut self) {
    if self.is_empty() {
      self.head = 0;
      self.tail = 0;
    }
  }

  /// 编码为定长 51 字节数组 (零堆分配)
  #[inline]
  pub const fn encode(&self) -> [u8; LIST_STUB_SIZE] {
    let stub_bytes = self.range_stub.encode();
    let head_bytes = self.head.to_le_bytes();
    let tail_bytes = self.tail.to_le_bytes();
    let mut out = [0u8; LIST_STUB_SIZE];
    let mut i = 0;
    while i < 35 {
      out[i] = stub_bytes[i];
      i += 1;
    }
    let mut j = 0;
    while j < 8 {
      out[35 + j] = head_bytes[j];
      out[43 + j] = tail_bytes[j];
      j += 1;
    }
    out
  }

  /// 编码至输出切片
  #[inline]
  pub fn encode_into(&self, out: &mut [u8]) -> Result<()> {
    if out.len() < LIST_STUB_SIZE {
      return Err(CollectionError::InvalidArgument("输出切片长度不足 51 字节"));
    }
    out[..LIST_STUB_SIZE].copy_from_slice(&self.encode());
    Ok(())
  }

  /// 从切片解码 ListStub (零拷贝)
  #[inline]
  pub const fn decode_opt(bytes: &[u8]) -> Option<Self> {
    if bytes.len() < LIST_STUB_SIZE {
      return None;
    }
    let Some(range_stub) = RangeIndexStub::decode_opt(bytes) else {
      return None;
    };
    let head = i64::from_le_bytes([
      bytes[35], bytes[36], bytes[37], bytes[38], bytes[39], bytes[40], bytes[41], bytes[42],
    ]);
    let tail = i64::from_le_bytes([
      bytes[43], bytes[44], bytes[45], bytes[46], bytes[47], bytes[48], bytes[49], bytes[50],
    ]);
    Some(Self {
      range_stub,
      head,
      tail,
    })
  }

  /// 从切片解码 ListStub (零拷贝，返回 Result)
  #[inline]
  pub fn decode(bytes: &[u8]) -> Result<Self> {
    Self::decode_opt(bytes).ok_or(CollectionError::Corrupted(
      "ListStub 存根数据损坏或长度不足",
    ))
  }

  /// 获取当前列表长度
  #[inline]
  pub const fn len(&self) -> usize {
    if self.tail > self.head {
      (self.tail as u64).wrapping_sub(self.head as u64) as usize
    } else {
      0
    }
  }

  /// 检查列表是否为空
  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.tail <= self.head
  }
}

/// List 树操作 Trait
///
/// 在 garnet 中的相对路径:libs/server/Storage/Session/ObjectStore/ListOps.cs
pub trait ListTreeOps {
  /// 左推入元素 (返回推入后的列表总长度)
  fn lpush(&self, stub: &mut ListStub, element: &[u8]) -> Result<usize>;

  /// 右推入元素 (返回推入后的列表总长度)
  fn rpush(&self, stub: &mut ListStub, element: &[u8]) -> Result<usize>;

  /// 左弹出元素 (空列表返回 Ok(None))
  fn lpop(&self, stub: &mut ListStub) -> Result<Option<Vec<u8>>>;

  /// 右弹出元素 (空列表返回 Ok(None))
  fn rpop(&self, stub: &mut ListStub) -> Result<Option<Vec<u8>>>;

  /// 获取列表当前长度 (O(1) 存根算术直读)
  #[inline]
  fn llen(&self, stub: &ListStub) -> usize {
    stub.len()
  }

  /// 按索引读取元素 (支持负数倒数索引)
  fn lindex(&self, stub: &ListStub, index: i64) -> Result<Option<Vec<u8>>> {
    self.lindex_callback(stub, index, |opt| opt.map(|v| v.to_vec()))
  }

  /// 零拷贝按索引借用读取元素回调 (零堆分配)
  fn lindex_callback<R>(
    &self,
    stub: &ListStub,
    index: i64,
    f: impl FnOnce(Option<&[u8]>) -> R,
  ) -> Result<R>;

  /// 按范围读取元素列表 (闭区间，支持负数索引与边界截断)
  fn lrange(&self, stub: &ListStub, start: i64, stop: i64) -> Result<Vec<Vec<u8>>>;

  /// 按范围流式遍历元素 (闭区间，零分配回调)
  fn lrange_callback<F>(&self, stub: &ListStub, start: i64, stop: i64, on_elem: F) -> Result<usize>
  where
    F: FnMut(&[u8]) -> bool;

  /// 按索引修改元素 (LSET)
  fn lset(&self, stub: &ListStub, index: i64, element: &[u8]) -> Result<()>;

  /// 裁剪列表仅保留指定区间元素 (LTRIM，闭区间，返回裁剪后剩余长度)
  fn ltrim(&self, stub: &mut ListStub, start: i64, stop: i64) -> Result<usize>;
}

impl ListTreeOps for BfTreeService {
  fn lpush(&self, stub: &mut ListStub, element: &[u8]) -> Result<usize> {
    if element.is_empty() {
      return Err(CollectionError::EmptyValue);
    }
    let new_head = stub
      .head
      .checked_sub(1)
      .ok_or(CollectionError::InvalidArgument("list head 下溢"))?;
    let key = list_key_from_i64(new_head);
    match self.insert(&key, element) {
      BfTreeInsertResult::Success => {
        stub.head = new_head;
        Ok(stub.len())
      }
      BfTreeInsertResult::InvalidKV => Err(CollectionError::KeyTooLong),
      _ => Err(CollectionError::InvalidArgument("lpush 插入失败")),
    }
  }

  fn rpush(&self, stub: &mut ListStub, element: &[u8]) -> Result<usize> {
    if element.is_empty() {
      return Err(CollectionError::EmptyValue);
    }
    let new_tail = stub
      .tail
      .checked_add(1)
      .ok_or(CollectionError::InvalidArgument("list tail 上溢"))?;
    let key = list_key_from_i64(stub.tail);
    match self.insert(&key, element) {
      BfTreeInsertResult::Success => {
        stub.tail = new_tail;
        Ok(stub.len())
      }
      BfTreeInsertResult::InvalidKV => Err(CollectionError::KeyTooLong),
      _ => Err(CollectionError::InvalidArgument("rpush 插入失败")),
    }
  }

  fn lpop(&self, stub: &mut ListStub) -> Result<Option<Vec<u8>>> {
    if stub.is_empty() {
      return Ok(None);
    }
    let key = list_key_from_i64(stub.head);
    let next_head = stub
      .head
      .checked_add(1)
      .ok_or(CollectionError::InvalidArgument("list head 上溢"))?;
    let (res, val) = self.read(&key);
    match res {
      BfTreeReadResult::Found => {
        self.delete(&key);
        stub.head = next_head;
        stub.reset_if_empty();
        Ok(val)
      }
      BfTreeReadResult::NotFound | BfTreeReadResult::Deleted => {
        stub.head = next_head;
        stub.reset_if_empty();
        Ok(None)
      }
      _ => Err(CollectionError::InvalidArgument("lpop 读取失败")),
    }
  }

  fn rpop(&self, stub: &mut ListStub) -> Result<Option<Vec<u8>>> {
    if stub.is_empty() {
      return Ok(None);
    }
    let target_idx = stub
      .tail
      .checked_sub(1)
      .ok_or(CollectionError::InvalidArgument("list tail 下溢"))?;
    let key = list_key_from_i64(target_idx);
    let (res, val) = self.read(&key);
    match res {
      BfTreeReadResult::Found => {
        self.delete(&key);
        stub.tail = target_idx;
        stub.reset_if_empty();
        Ok(val)
      }
      BfTreeReadResult::NotFound | BfTreeReadResult::Deleted => {
        stub.tail = target_idx;
        stub.reset_if_empty();
        Ok(None)
      }
      _ => Err(CollectionError::InvalidArgument("rpop 读取失败")),
    }
  }

  fn lindex_callback<R>(
    &self,
    stub: &ListStub,
    index: i64,
    f: impl FnOnce(Option<&[u8]>) -> R,
  ) -> Result<R> {
    let len = stub.len() as i64;
    if len == 0 || index >= len || index < -len {
      return Ok(f(None));
    }
    let actual_offset = if index >= 0 { index } else { len + index };
    let Some(actual_idx) = stub.head.checked_add(actual_offset) else {
      return Ok(f(None));
    };
    let key = list_key_from_i64(actual_idx);
    self.read_callback(&key, |res, bytes| match res {
      BfTreeReadResult::Found => Ok(f(Some(bytes))),
      BfTreeReadResult::NotFound | BfTreeReadResult::Deleted => Ok(f(None)),
      _ => Err(CollectionError::InvalidArgument("lindex 读取失败")),
    })
  }

  fn lrange(&self, stub: &ListStub, start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
    let Some((s, e)) = normalize_range(stub.len(), start, stop) else {
      return Ok(Vec::new());
    };
    let count = e - s + 1;
    let mut records = Vec::with_capacity(count);
    lrange_normalized_callback(self, stub, s, e, |elem| {
      records.push(elem.to_vec());
      true
    })?;
    Ok(records)
  }

  fn lrange_callback<F>(&self, stub: &ListStub, start: i64, stop: i64, on_elem: F) -> Result<usize>
  where
    F: FnMut(&[u8]) -> bool,
  {
    let Some((s, e)) = normalize_range(stub.len(), start, stop) else {
      return Ok(0);
    };
    lrange_normalized_callback(self, stub, s, e, on_elem)
  }

  fn lset(&self, stub: &ListStub, index: i64, element: &[u8]) -> Result<()> {
    if element.is_empty() {
      return Err(CollectionError::EmptyValue);
    }
    let len = stub.len() as i64;
    if len == 0 || index >= len || index < -len {
      return Err(CollectionError::InvalidArgument("index out of range"));
    }
    let actual_offset = if index >= 0 { index } else { len + index };
    let actual_idx = stub
      .head
      .checked_add(actual_offset)
      .ok_or(CollectionError::InvalidArgument("index overflow"))?;
    let key = list_key_from_i64(actual_idx);
    match self.insert(&key, element) {
      BfTreeInsertResult::Success => Ok(()),
      BfTreeInsertResult::InvalidKV => Err(CollectionError::KeyTooLong),
      _ => Err(CollectionError::InvalidArgument("lset 修改失败")),
    }
  }

  fn ltrim(&self, stub: &mut ListStub, start: i64, stop: i64) -> Result<usize> {
    let len = stub.len();
    if len == 0 {
      return Ok(0);
    }
    let Some((s, e)) = normalize_range(len, start, stop) else {
      // 范围不合法或为空，清空整个列表
      for offset in 0..len {
        if let Some(idx) = stub.head.checked_add(offset as i64) {
          self.delete(&list_key_from_i64(idx));
        }
      }
      stub.head = 0;
      stub.tail = 0;
      return Ok(0);
    };

    // 删除保留区间左侧的元素 [0, s)
    for offset in 0..s {
      if let Some(idx) = stub.head.checked_add(offset as i64) {
        self.delete(&list_key_from_i64(idx));
      }
    }

    // 删除保留区间右侧的元素 (e, len)
    for offset in (e + 1)..len {
      if let Some(idx) = stub.head.checked_add(offset as i64) {
        self.delete(&list_key_from_i64(idx));
      }
    }

    // 更新存根 head 与 tail
    let new_head = stub
      .head
      .checked_add(s as i64)
      .ok_or(CollectionError::InvalidArgument("list head 溢出"))?;
    let new_tail = stub
      .head
      .checked_add(e as i64 + 1)
      .ok_or(CollectionError::InvalidArgument("list tail 溢出"))?;
    stub.head = new_head;
    stub.tail = new_tail;
    stub.reset_if_empty();
    Ok(stub.len())
  }
}

/// 经标准化的区间流式读取元素
fn lrange_normalized_callback<F>(
  service: &BfTreeService,
  stub: &ListStub,
  s: usize,
  e: usize,
  mut on_elem: F,
) -> Result<usize>
where
  F: FnMut(&[u8]) -> bool,
{
  let Some(start_idx) = stub.head.checked_add(s as i64) else {
    return Ok(0);
  };
  let Some(end_idx) = stub.head.checked_add(e as i64) else {
    return Ok(0);
  };
  let start_key = list_key_from_i64(start_idx);
  let end_key = list_key_from_i64(end_idx);

  let prefix_u8 = TreePrefix::ListIndex as u8;
  let mut count = 0;
  service.scan_with_end_key_callback(
    &start_key,
    &end_key,
    ScanReturnField::KeyAndValue,
    |k, v| {
      if k.len() < 9 || k[0] != prefix_u8 {
        return false;
      }
      count += 1;
      on_elem(v)
    },
  )?;
  Ok(count)
}

/// ListTree 包装结构体
pub struct ListTree<'a> {
  /// 底层 BfTree 树实例
  pub tree: &'a BfTreeService,
  /// 关联的 List 元数据存根
  pub stub: &'a mut ListStub,
}

impl<'a> ListTree<'a> {
  /// 创建 ListTree 包装
  #[inline]
  pub fn new(tree: &'a BfTreeService, stub: &'a mut ListStub) -> Self {
    Self { tree, stub }
  }

  /// 左推入
  #[inline]
  pub fn lpush(&mut self, element: &[u8]) -> Result<usize> {
    self.tree.lpush(self.stub, element)
  }

  /// 右推入
  #[inline]
  pub fn rpush(&mut self, element: &[u8]) -> Result<usize> {
    self.tree.rpush(self.stub, element)
  }

  /// 左弹出
  #[inline]
  pub fn lpop(&mut self) -> Result<Option<Vec<u8>>> {
    self.tree.lpop(self.stub)
  }

  /// 右弹出
  #[inline]
  pub fn rpop(&mut self) -> Result<Option<Vec<u8>>> {
    self.tree.rpop(self.stub)
  }

  /// 索引读取
  #[inline]
  pub fn lindex(&self, index: i64) -> Result<Option<Vec<u8>>> {
    self.tree.lindex(self.stub, index)
  }

  /// 零拷贝索引读取回调
  #[inline]
  pub fn lindex_callback<R>(&self, index: i64, f: impl FnOnce(Option<&[u8]>) -> R) -> Result<R> {
    self.tree.lindex_callback(self.stub, index, f)
  }

  /// 范围读取
  #[inline]
  pub fn lrange(&self, start: i64, stop: i64) -> Result<Vec<Vec<u8>>> {
    self.tree.lrange(self.stub, start, stop)
  }

  /// 范围流式回调
  #[inline]
  pub fn lrange_callback<F>(&self, start: i64, stop: i64, on_elem: F) -> Result<usize>
  where
    F: FnMut(&[u8]) -> bool,
  {
    self.tree.lrange_callback(self.stub, start, stop, on_elem)
  }

  /// 按索引修改元素 (LSET)
  #[inline]
  pub fn lset(&mut self, index: i64, element: &[u8]) -> Result<()> {
    self.tree.lset(self.stub, index, element)
  }

  /// 裁剪列表仅保留指定区间元素 (LTRIM)
  #[inline]
  pub fn ltrim(&mut self, start: i64, stop: i64) -> Result<usize> {
    self.tree.ltrim(self.stub, start, stop)
  }

  /// 获取列表长度 (O(1) 存根算术直读)
  #[inline]
  pub const fn llen(&self) -> usize {
    self.stub.len()
  }

  /// 长度
  #[inline]
  pub const fn len(&self) -> usize {
    self.stub.len()
  }

  /// 是否为空
  #[inline]
  pub const fn is_empty(&self) -> bool {
    self.stub.is_empty()
  }
}
