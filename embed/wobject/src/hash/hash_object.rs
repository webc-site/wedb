use std::io::{self, Read, Write};

use gxhash::GxBuildHasher;
use papaya::HashMap;

/// garnet相对路径:garnet/libs/server/Objects/Hash/HashOperation.cs:HashOperation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HashOperation {
  HSET = 0,
  HMSET = 1,
  HGET = 2,
  HMGET = 3,
  HGETALL = 4,
  HDEL = 5,
  HLEN = 6,
  HEXISTS = 7,
  HKEYS = 8,
  HVALS = 9,
  HINCRBY = 10,
  HINCRBYFLOAT = 11,
  HSETNX = 12,
  HRANDFIELD = 13,
  HSCAN = 14,
  HSTRLEN = 15,
}

/// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:HashObject
///
/// 刻意差异（对照 C#）：C# 携带 `expirationTimes`/`expirationQueue` 字段支撑
/// HEXPIRE/HTTL 字段级过期；Rust 侧过期统一由 wkv TTL 记录层承担，本结构
/// 不再冗余持有永不读写的过期容器（cycle2 遗留死字段，已清除）
pub struct HashObject {
  pub hash: HashMap<Vec<u8>, Vec<u8>, GxBuildHasher>,
}

impl HashObject {
  pub fn new() -> Self {
    Self {
      hash: HashMap::with_hasher(GxBuildHasher::default()),
    }
  }

  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:HashObject(BinaryReader)
  pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;
    let items: Vec<(Vec<u8>, Vec<u8>)> =
      bitcode::decode(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let hash = HashMap::with_hasher(GxBuildHasher::default());
    let pin = hash.pin();
    for (k, v) in items {
      pin.insert(k, v);
    }

    drop(pin);
    Ok(Self { hash })
  }

  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:Serialize
  pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
    let pin = self.hash.pin();
    let mut items = Vec::with_capacity(pin.len());
    for (k, v) in pin.iter() {
      items.push((k.clone(), v.clone()));
    }
    let bytes = bitcode::encode(&items);
    writer.write_all(&bytes)
  }

  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:Operate
  pub fn operate(&self, op_code: u8, key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
    let pin = self.hash.pin();
    match op_code {
      0 /* HSET */ => {
        pin.insert(key.to_vec(), value.to_vec());
        None
      }
      2 /* HGET */ => pin.get(key).cloned(),
      5 /* HDEL */ => pin.remove(key).cloned(),
      6 /* HLEN */ => {
          let mut buffer = itoa::Buffer::new();
          let len_str = buffer.format(pin.len());
          Some(len_str.as_bytes().to_vec())
      }
      7 /* HEXISTS */ => {
          let exists = if pin.contains_key(key) { b"1" } else { b"0" };
          Some(exists.to_vec())
      }
      _ => None,
    }
  }

  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:GetKeys
  pub fn get_keys(&self) -> Vec<Vec<u8>> {
    let pin = self.hash.pin();
    pin.iter().map(|(k, _)| k.clone()).collect()
  }

  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:GetValues
  pub fn get_values(&self) -> Vec<Vec<u8>> {
    let pin = self.hash.pin();
    pin.iter().map(|(_, v)| v.clone()).collect()
  }

  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:HashGetAll
  pub fn hash_get_all(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
    let pin = self.hash.pin();
    pin.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
  }
}

impl Default for HashObject {
  fn default() -> Self {
    Self::new()
  }
}
