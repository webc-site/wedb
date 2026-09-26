//! wvector 集成测试共用桥 (`wvector_test`)
//!
//! 内存桥接存储 [`MemStore`]：[`wvector::StoreCallbacks`] 的纯内存实现，
//! 各臂对齐生产回调口径（`wnode::WedbVectorStoreCallbacks`）——rmw 按
//! write_len 截短/补零整值写回、read_multi 按 `[4B LE 长度][键]` 对串解包、
//! purge_context 按 Term 位域清命名空间。收口自 wvector/wnode 向量测试的
//! 逐字同形副本，测试侧经辅助口（`len_of`/`peek`/`poke`）或直曝字段
//! `data` 检视、注入落盘字节。

use parking_lot::Mutex;
use wbase::map::HashMap;
use wvector::{
  StoreCallbacks,
  store::{TERM_BITMASK, Term},
};

/// 存储映射：`(context, key) → 值`。
pub type MemStoreMap = HashMap<(u64, Vec<u8>), Vec<u8>>;

/// 内存桥接存储（(context, key) → 值），测试侧直接检视各 Term 落盘字节。
pub struct MemStore {
  pub data: Mutex<MemStoreMap>,
}

/// 项类型命名空间合成：`base` 低位并入 Term 位域（base 恒项类型位对齐）。
#[inline]
fn slot(base: u64, kind: Term) -> u64 {
  base | (kind as u64 & TERM_BITMASK)
}

impl MemStore {
  pub fn new() -> Self {
    Self {
      data: Mutex::new(HashMap::default()),
    }
  }

  /// 指定项类型命名空间下内部 id 记录的字节数（缺失 → None）。
  pub fn len_of(&self, base: u64, term: Term, iid: u32) -> Option<usize> {
    self
      .data
      .lock()
      .get(&(slot(base, term), iid.to_le_bytes().to_vec()))
      .map(Vec::len)
  }

  /// 直读落盘条目（存储侧观测口）。
  pub fn peek(&self, base: u64, kind: Term, key: &[u8]) -> Option<Vec<u8>> {
    self
      .data
      .lock()
      .get(&(slot(base, kind), key.to_vec()))
      .cloned()
  }

  /// 直写落盘条目（异长记录注入口）。
  pub fn poke(&self, base: u64, kind: Term, key: &[u8], value: Vec<u8>) {
    self
      .data
      .lock()
      .insert((slot(base, kind), key.to_vec()), value);
  }
}

impl Default for MemStore {
  fn default() -> Self {
    Self::new()
  }
}

impl StoreCallbacks for MemStore {
  async fn read_multi<F>(&self, context: u64, keys: &[u8], _length_hint: usize, mut f: F) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    let mut index = 0u32;
    let mut rest = keys;
    let guard = self.data.lock();
    while rest.len() >= 4 {
      let len = u32::from_le_bytes(rest[..4].try_into().unwrap()) as usize;
      let total = 4 + len;
      if rest.len() < total {
        break;
      }
      let key = &rest[4..total];
      if let Some(value) = guard.get(&(context, key.to_vec())) {
        f(index, value);
      }
      index += 1;
      rest = &rest[total..];
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
    // 纯读短路（对齐生产回调）：不建不写
    if write_len == 0 {
      return true;
    }
    // 对齐生产内核口径（wnode WedbVectorStoreCallbacks::rmw）：write_len 即
    // 目标记录尺寸，旧值截短/补零后闭包改写、整值写回——桥若保留旧记录
    // 全长，rmw 缩记录类缺陷对本桥不可见
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

  async fn purge_context(&self, context: u64) -> bool {
    self
      .data
      .lock()
      .retain(|&(ctx, _), _| ctx & !TERM_BITMASK != context);
    true
  }

  fn log(&self, _context: u64, _msg: &str) {}
}
