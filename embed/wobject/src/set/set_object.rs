use gxhash::GxBuildHasher;
use papaya::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SetOperation {
  Sadd = 0,
  Srem = 1,
  Spop = 2,
  Smembers = 3,
  Scard = 4,
  Sscan = 5,
  Smove = 6,
  Srandmember = 7,
  Sismember = 8,
  Smismember = 9,
  Sunion = 10,
  Sunionstore = 11,
  Sdiff = 12,
  Sdiffstore = 13,
  Sinter = 14,
  Sinterstore = 15,
}

/// garnet相对路径:garnet/libs/server/Objects/Set/SetObject.cs:SetObject
pub struct SetObject {
  pub set: HashSet<Vec<u8>, GxBuildHasher>,
}

impl SetObject {
  pub fn new() -> Self {
    Self {
      set: HashSet::with_hasher(GxBuildHasher::default()),
    }
  }

  /// garnet相对路径:garnet/libs/server/Objects/Set/SetObject.cs:Operate
  pub fn operate(&self, op: SetOperation, key: &[u8]) -> bool {
    let pin = self.set.pin();
    match op {
      SetOperation::Sadd => pin.insert(key.to_vec()),
      SetOperation::Srem => pin.remove(key),
      SetOperation::Sismember => pin.contains(key),
      _ => false,
    }
  }

  /// garnet相对路径:garnet/libs/server/Objects/Set/SetObject.cs:Count
  pub fn count(&self) -> usize {
    self.set.pin().len()
  }

  /// garnet相对路径:garnet/libs/server/Objects/Set/SetObject.cs:GetKeys
  pub fn get_keys(&self) -> Vec<Vec<u8>> {
    let pin = self.set.pin();
    pin.iter().cloned().collect()
  }
}

impl Default for SetObject {
  fn default() -> Self {
    Self::new()
  }
}
