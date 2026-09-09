use std::{cmp::Reverse, collections::BinaryHeap, sync::Mutex};

use gxhash::GxBuildHasher;
use papaya::HashMap;

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

  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:Operate
  pub fn operate(&self, op_code: u8, key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
    match op_code {
      // HSET
      0 => {
        self.hash.pin().insert(key.to_vec(), value.to_vec());
        None
      }
      // HGET
      1 => self.hash.pin().get(key).cloned(),
      // HDEL
      2 => self.hash.pin().remove(key).cloned(),
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
}

impl Default for HashObject {
  fn default() -> Self {
    Self::new()
  }
}
