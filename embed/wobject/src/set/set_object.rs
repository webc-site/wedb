use std::io::{self, Read, Write};

use whasher::{GxPapayaSet, new_papaya_set};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SetOperation {
  Sadd = 0,
  Srem = 1,
  Spop = 2,
  Smove = 3,
  Srandmember = 4,
  Smembers = 5,
  Sinter = 6,
  Sunion = 7,
  Sdiff = 8,
  Sinterstore = 9,
  Sunionstore = 10,
  Sdiffstore = 11,
  Sismember = 12,
  Scard = 13,
}

/// 内存集合对象（嵌入式存储层实现，服务层权威实现见 wedb_standalone::objects::set::SetObject）
pub struct SetObject {
  pub set: GxPapayaSet<Vec<u8>>,
}

impl SetObject {
  pub fn new() -> Self {
    Self {
      set: new_papaya_set(),
    }
  }

  /// 从二进制流反序列化内存集合对象
  pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;
    let items: Vec<Vec<u8>> =
      bitcode::decode(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let set = new_papaya_set();
    let pin = set.pin();
    for item in items {
      pin.insert(item);
    }

    drop(pin);
    Ok(Self { set })
  }

  /// 序列化内存集合对象为二进制流
  pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
    let pin = self.set.pin();
    let mut items = Vec::with_capacity(pin.len());
    for item in pin.iter() {
      items.push(item.clone());
    }
    let bytes = bitcode::encode(&items);
    writer.write_all(&bytes)
  }

  /// 内存集合对象操作派发
  pub fn operate(&self, op: SetOperation, key: &[u8]) -> bool {
    let pin = self.set.pin();
    match op {
      SetOperation::Sadd => pin.insert(key.to_vec()),
      SetOperation::Srem => pin.remove(key),
      SetOperation::Sismember => pin.contains(key),
      _ => false,
    }
  }

  /// 元素数量
  pub fn count(&self) -> usize {
    self.set.pin().len()
  }
}

impl Default for SetObject {
  fn default() -> Self {
    Self::new()
  }
}

impl SetObject {
  /// 获取全部成员
  pub fn get_keys(&self) -> Vec<Vec<u8>> {
    let pin = self.set.pin();
    pin.iter().cloned().collect()
  }

  /// 随机弹出成员
  ///
  /// 刻意差异修正：原实现恒取首元素（确定性弹出，语义偏差）；对齐 C# 的
  /// `RandomNumberGenerator.GetInt32(0, Set.Count)` 随机下标弹出
  /// （`nth(index)` 与 C# `Set.ElementAt(index)` 同为 O(index) 链上遍历，复杂度持平）
  pub fn pop(&self) -> Option<Vec<u8>> {
    let pin = self.set.pin();
    let len = pin.len();
    if len == 0 {
      return None;
    }
    let item = pin.iter().nth(fastrand::usize(..len)).cloned()?;
    pin.remove(&item);
    Some(item)
  }

  /// 随机采样成员
  ///
  /// 刻意差异修正：原实现恒取首元素；对齐 C# `RandomUtils.PickRandomIndex` 随机采样
  pub fn random_member(&self) -> Option<Vec<u8>> {
    let pin = self.set.pin();
    let len = pin.len();
    if len == 0 {
      return None;
    }
    pin.iter().nth(fastrand::usize(..len)).cloned()
  }
}
