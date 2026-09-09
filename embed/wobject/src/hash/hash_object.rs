use std::{cmp::Reverse, collections::BinaryHeap, sync::Mutex};

use gxhash::GxBuildHasher;
use papaya::HashMap;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Read, Write};

#[derive(Debug, PartialEq, Eq)]
pub struct ExpirationEntry {
  pub expiration: i64,
  pub key: Vec<u8>,
}

impl PartialOrd for ExpirationEntry {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for ExpirationEntry {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self
      .expiration
      .cmp(&other.expiration)
      .then_with(|| self.key.cmp(&other.key))
  }
}

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
pub struct HashObject {
  pub hash: HashMap<Vec<u8>, Vec<u8>, GxBuildHasher>,
  pub expiration_times: HashMap<Vec<u8>, i64, GxBuildHasher>,
  pub expiration_queue: Mutex<BinaryHeap<Reverse<ExpirationEntry>>>,
}

impl HashObject {
  pub fn new() -> Self {
    Self {
      hash: HashMap::with_hasher(GxBuildHasher::default()),
      expiration_times: HashMap::with_hasher(GxBuildHasher::default()),
      expiration_queue: Mutex::new(BinaryHeap::new()),
    }
  }

  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:HashObject(BinaryReader)
  pub fn deserialize<R: Read>(reader: &mut R) -> std::io::Result<Self> {
      let count = reader.read_i32::<LittleEndian>()?;
      let hash = HashMap::with_hasher(GxBuildHasher::default());
      let pin = hash.pin();
      for _ in 0..count {
          let k_len = reader.read_i32::<LittleEndian>()?;
          let mut k = vec![0u8; k_len as usize];
          reader.read_exact(&mut k)?;
          
          let v_len = reader.read_i32::<LittleEndian>()?;
          let mut v = vec![0u8; v_len as usize];
          reader.read_exact(&mut v)?;
          
          pin.insert(k, v);
      }
      drop(pin);
      Ok(Self {
          hash,
          expiration_times: HashMap::with_hasher(GxBuildHasher::default()),
          expiration_queue: Mutex::new(BinaryHeap::new()),
      })
  }

  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:Serialize
  pub fn serialize<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
      let pin = self.hash.pin();
      writer.write_i32::<LittleEndian>(pin.len() as i32)?;
      for (k, v) in pin.iter() {
          writer.write_i32::<LittleEndian>(k.len() as i32)?;
          writer.write_all(k)?;
          writer.write_i32::<LittleEndian>(v.len() as i32)?;
          writer.write_all(v)?;
      }
      Ok(())
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
          let len = pin.len().to_string().into_bytes();
          Some(len)
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

  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:HashIncrementByFloat
  pub fn hash_increment_by_float(&self, key: &[u8], increment: f64) -> Option<f64> {
      let pin = self.hash.pin();
      let mut current_val = 0.0;
      if let Some(v) = pin.get(key) {
          if let Ok(s) = std::str::from_utf8(v) {
              if let Ok(parsed) = s.parse::<f64>() {
                  current_val = parsed;
              }
          }
      }
      current_val += increment;
      pin.insert(key.to_vec(), current_val.to_string().into_bytes());
      Some(current_val)
  }
}

impl Default for HashObject {
  fn default() -> Self {
    Self::new()
  }
}
