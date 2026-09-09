import os

list_content = """use parking_lot::Mutex;
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ListOperation {
  Lpop = 0, Lpush = 1, Lpushx = 2, Rpop = 3, Rpush = 4, Rpushx = 5,
  Llen = 6, Ltrim = 7, Lrange = 8, Lindex = 9, Linsert = 10, Lrem = 11,
  Rpoplpush = 12, Lmove = 13, Lset = 14, Brpop = 15, Blpop = 16, Lpos = 17,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OperationDirection {
  Left = 0, Right = 1, Unknown = 2,
}

/// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:ListObject
pub struct ListObject {
  pub list: Mutex<VecDeque<Vec<u8>>>,
}

impl ListObject {
  pub fn new() -> Self {
    Self { list: Mutex::new(VecDeque::new()) }
  }

  /// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:ListObject(BinaryReader)
  pub fn deserialize(bytes: &[u8]) -> std::io::Result<Self> {
    let vec: Vec<Vec<u8>> = bitcode::decode(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Self { list: Mutex::new(VecDeque::from(vec)) })
  }

  /// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:Serialize
  pub fn serialize(&self) -> std::io::Result<Vec<u8>> {
    let list = self.list.lock();
    let vec: Vec<Vec<u8>> = list.iter().cloned().collect();
    Ok(bitcode::encode(&vec))
  }

  /// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:Operate
  pub fn operate(&self, op: ListOperation, item: &[u8]) -> Option<Vec<u8>> {
    let mut list = self.list.lock();
    match op {
      ListOperation::Lpush => { list.push_front(item.to_vec()); None },
      ListOperation::Rpush => { list.push_back(item.to_vec()); None },
      ListOperation::Lpop => list.pop_front(),
      ListOperation::Rpop => list.pop_back(),
      _ => None,
    }
  }

  /// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:ListIndex
  pub fn index(&self, index: isize) -> Option<Vec<u8>> {
    let list = self.list.lock();
    let len = list.len() as isize;
    let actual_idx = if index < 0 { len + index } else { index };
    if actual_idx < 0 || actual_idx >= len { None } else { list.get(actual_idx as usize).cloned() }
  }

  /// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:ListRange
  pub fn range(&self, start: isize, stop: isize) -> Vec<Vec<u8>> {
    let list = self.list.lock();
    let len = list.len() as isize;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };
    if s < 0 { s = 0; }
    if e >= len { e = len - 1; }
    if s > e || s >= len { return vec![]; }
    list.range((s as usize)..=(e as usize)).cloned().collect()
  }

  /// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:ListTrim
  pub fn trim(&self, start: isize, stop: isize) {
    let mut list = self.list.lock();
    let len = list.len() as isize;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };
    if s < 0 { s = 0; }
    if e >= len { e = len - 1; }
    if s > e || s >= len { list.clear(); return; }
    list.truncate((e + 1) as usize);
    for _ in 0..s { list.pop_front(); }
  }

  /// garnet相对路径:garnet/libs/server/Objects/List/ListObject.cs:Count
  pub fn count(&self) -> usize { self.list.lock().len() }
}

impl Default for ListObject { fn default() -> Self { Self::new() } }
"""

set_content = """use fastrand;
use gxhash::GxBuildHasher;
use papaya::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SetOperation {
  Sadd = 0, Srem = 1, Spop = 2, Smembers = 3, Scard = 4, Sscan = 5,
  Smove = 6, Srandmember = 7, Sismember = 8, Smismember = 9, Sunion = 10,
  Sunionstore = 11, Sdiff = 12, Sdiffstore = 13, Sinter = 14, Sinterstore = 15,
}

/// garnet相对路径:garnet/libs/server/Objects/Set/SetObject.cs:SetObject
pub struct SetObject { pub set: HashSet<Vec<u8>, GxBuildHasher> }

impl SetObject {
  pub fn new() -> Self { Self { set: HashSet::with_hasher(GxBuildHasher::default()) } }

  /// garnet相对路径:garnet/libs/server/Objects/Set/SetObject.cs:SetObject(BinaryReader)
  pub fn deserialize(bytes: &[u8]) -> std::io::Result<Self> {
    let vec: Vec<Vec<u8>> = bitcode::decode(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let set = HashSet::with_hasher(GxBuildHasher::default());
    let pin = set.pin();
    for item in vec { pin.insert(item); }
    drop(pin);
    Ok(Self { set })
  }

  /// garnet相对路径:garnet/libs/server/Objects/Set/SetObject.cs:Serialize
  pub fn serialize(&self) -> std::io::Result<Vec<u8>> {
    let pin = self.set.pin();
    let vec: Vec<Vec<u8>> = pin.iter().cloned().collect();
    Ok(bitcode::encode(&vec))
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

  /// garnet相对路径:garnet/libs/server/Objects/Set/SetObject.cs:SetPop
  pub fn pop(&self) -> Option<Vec<u8>> {
    let pin = self.set.pin();
    let item = pin.iter().next().cloned();
    if let Some(ref i) = item { pin.remove(i); }
    item
  }

  /// garnet相对路径:garnet/libs/server/Objects/Set/SetObject.cs:SetRandomMember
  pub fn random_member(&self) -> Option<Vec<u8>> {
    let pin = self.set.pin();
    let count = pin.len();
    if count == 0 { return None; }
    let idx = fastrand::usize(..count);
    pin.iter().nth(idx).cloned()
  }

  /// garnet相对路径:garnet/libs/server/Objects/Set/SetObject.cs:Count
  pub fn count(&self) -> usize { self.set.pin().len() }

  /// garnet相对路径:garnet/libs/server/Objects/Set/SetObject.cs:GetKeys
  pub fn get_keys(&self) -> Vec<Vec<u8>> {
    let pin = self.set.pin();
    pin.iter().cloned().collect()
  }
}

impl Default for SetObject { fn default() -> Self { Self::new() } }
"""

hash_content = """use std::{cmp::Reverse, collections::BinaryHeap};
use parking_lot::Mutex;
use gxhash::GxBuildHasher;
use papaya::HashMap;

#[derive(Debug, PartialEq, Eq)]
pub struct ExpirationEntry { pub expiration: i64, pub key: Vec<u8> }

impl PartialOrd for ExpirationEntry { fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) } }
impl Ord for ExpirationEntry { fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.expiration.cmp(&other.expiration).then_with(|| self.key.cmp(&other.key)) } }

/// garnet相对路径:garnet/libs/server/Objects/Hash/HashOperation.cs:HashOperation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum HashOperation {
  HSET = 0, HMSET = 1, HGET = 2, HMGET = 3, HGETALL = 4, HDEL = 5, HLEN = 6, HEXISTS = 7,
  HKEYS = 8, HVALS = 9, HINCRBY = 10, HINCRBYFLOAT = 11, HSETNX = 12, HRANDFIELD = 13, HSCAN = 14, HSTRLEN = 15,
}

/// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:HashObject
pub struct HashObject {
  pub hash: HashMap<Vec<u8>, Vec<u8>, GxBuildHasher>,
  pub expiration_times: HashMap<Vec<u8>, i64, GxBuildHasher>,
  pub expiration_queue: Mutex<BinaryHeap<Reverse<ExpirationEntry>>>,
}

impl HashObject {
  pub fn new() -> Self {
    Self { hash: HashMap::with_hasher(GxBuildHasher::default()), expiration_times: HashMap::with_hasher(GxBuildHasher::default()), expiration_queue: Mutex::new(BinaryHeap::new()) }
  }

  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:HashObject(BinaryReader)
  pub fn deserialize(bytes: &[u8]) -> std::io::Result<Self> {
    let vec: Vec<(Vec<u8>, Vec<u8>)> = bitcode::decode(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let hash = HashMap::with_hasher(GxBuildHasher::default());
    let pin = hash.pin();
    for (k, v) in vec { pin.insert(k, v); }
    drop(pin);
    Ok(Self { hash, expiration_times: HashMap::with_hasher(GxBuildHasher::default()), expiration_queue: Mutex::new(BinaryHeap::new()) })
  }

  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:Serialize
  pub fn serialize(&self) -> std::io::Result<Vec<u8>> {
    let pin = self.hash.pin();
    let vec: Vec<(Vec<u8>, Vec<u8>)> = pin.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
    Ok(bitcode::encode(&vec))
  }

  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:Operate
  pub fn operate(&self, op_code: u8, key: &[u8], value: &[u8]) -> Option<Vec<u8>> {
    let pin = self.hash.pin();
    match op_code {
      0 /* HSET */ => { pin.insert(key.to_vec(), value.to_vec()); None },
      2 /* HGET */ => pin.get(key).cloned(),
      5 /* HDEL */ => pin.remove(key).cloned(),
      6 /* HLEN */ => Some(itoa::Buffer::new().format(pin.len()).as_bytes().to_vec()),
      7 /* HEXISTS */ => Some((if pin.contains_key(key) { b"1" } else { b"0" }).to_vec()),
      _ => None,
    }
  }

  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:GetKeys
  pub fn get_keys(&self) -> Vec<Vec<u8>> { self.hash.pin().iter().map(|(k, _)| k.clone()).collect() }
  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:GetValues
  pub fn get_values(&self) -> Vec<Vec<u8>> { self.hash.pin().iter().map(|(_, v)| v.clone()).collect() }
  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:HashGetAll
  pub fn hash_get_all(&self) -> Vec<(Vec<u8>, Vec<u8>)> { self.hash.pin().iter().map(|(k, v)| (k.clone(), v.clone())).collect() }

  /// garnet相对路径:garnet/libs/server/Objects/Hash/HashObject.cs:HashIncrementByFloat
  pub fn hash_increment_by_float(&self, key: &[u8], increment: f64) -> Option<f64> {
    let pin = self.hash.pin();
    let mut current_val = 0.0;
    if let Some(v) = pin.get(key) { if let Ok(s) = std::str::from_utf8(v) { if let Ok(parsed) = s.parse::<f64>() { current_val = parsed; } } }
    current_val += increment;
    pin.insert(key.to_vec(), zmij::Buffer::new().format(current_val).as_bytes().to_vec());
    Some(current_val)
  }
}

impl Default for HashObject { fn default() -> Self { Self::new() } }
"""

sorted_set_content = """use parking_lot::RwLock;
use std::collections::BTreeSet;
use gxhash::GxBuildHasher;
use ordered_float::OrderedFloat;
use papaya::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SortedSetOperation {
  Zadd = 0, Zcard = 1, Zpopmax = 2, Zscore = 3, Zrem = 4, Zcount = 5, Zincrby = 6,
  Zrank = 7, Zrange = 8, Geoadd = 9, Geohash = 10, Geodist = 11, Geopos = 12, Geosearch = 13,
  Zrevrank = 14, Zremrangebylex = 15, Zremrangebyrank = 16, Zremrangebyscore = 17, Zlexcount = 18,
  Zpopmin = 19, Zrandmember = 20, Zdiff = 21, Zscan = 22, Zmscore = 23, Zexpire = 24, Zttl = 25, Zpersist = 26, Zcollect = 27,
}

bitflags::bitflags! {
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct SortedSetRangeOpts: u8 { const NONE = 0; const BY_SCORE = 1; const BY_LEX = 1 << 1; const REVERSE = 1 << 2; const STORE = 1 << 3; const WITH_SCORES = 1 << 4; }
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct SortedSetAddOption: u8 { const NONE = 0; const XX = 1; const NX = 1 << 1; const LT = 1 << 2; const GT = 1 << 3; const CH = 1 << 4; }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortedSetEntry { pub score: OrderedFloat<f64>, pub member: Vec<u8> }

impl PartialOrd for SortedSetEntry { fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) } }
impl Ord for SortedSetEntry { fn cmp(&self, other: &Self) -> std::cmp::Ordering { self.score.cmp(&other.score).then_with(|| self.member.cmp(&other.member)) } }

/// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetObject
pub struct SortedSetObject {
  pub dict: HashMap<Vec<u8>, OrderedFloat<f64>, GxBuildHasher>,
  pub tree: RwLock<BTreeSet<SortedSetEntry>>,
}

impl SortedSetObject {
  pub fn new() -> Self { Self { dict: HashMap::with_hasher(GxBuildHasher::default()), tree: RwLock::new(BTreeSet::new()) } }

  /// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetObject(BinaryReader)
  pub fn deserialize(bytes: &[u8]) -> std::io::Result<Self> {
    let vec: Vec<(Vec<u8>, f64)> = bitcode::decode(bytes).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let dict = HashMap::with_hasher(GxBuildHasher::default());
    let pin = dict.pin();
    let mut tree = BTreeSet::new();
    for (k, v) in vec {
      let fscore = OrderedFloat(v);
      pin.insert(k.clone(), fscore);
      tree.insert(SortedSetEntry { score: fscore, member: k });
    }
    drop(pin);
    Ok(Self { dict, tree: RwLock::new(tree) })
  }

  /// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:Serialize
  pub fn serialize(&self) -> std::io::Result<Vec<u8>> {
    let pin = self.dict.pin();
    let vec: Vec<(Vec<u8>, f64)> = pin.iter().map(|(k, v)| (k.clone(), v.into_inner())).collect();
    Ok(bitcode::encode(&vec))
  }

  /// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:Operate
  pub fn operate(&self, op: SortedSetOperation, member: &[u8], score: f64) -> Option<f64> {
    match op {
      SortedSetOperation::Zadd => {
        let fscore = OrderedFloat(score);
        let mut tree = self.tree.write();
        if let Some(old_score) = self.dict.pin().get(member) { tree.remove(&SortedSetEntry { score: *old_score, member: member.to_vec() }); }
        self.dict.pin().insert(member.to_vec(), fscore);
        tree.insert(SortedSetEntry { score: fscore, member: member.to_vec() });
        Some(score)
      }
      SortedSetOperation::Zscore => self.dict.pin().get(member).map(|s| s.into_inner()),
      SortedSetOperation::Zrem => {
        if let Some(s) = self.dict.pin().remove(member) {
          let mut tree = self.tree.write();
          tree.remove(&SortedSetEntry { score: *s, member: member.to_vec() });
          Some(s.into_inner())
        } else { None }
      }
      _ => None,
    }
  }

  /// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetPop
  pub fn pop_min(&self) -> Option<(Vec<u8>, f64)> {
    let mut tree = self.tree.write();
    if let Some(first) = tree.iter().next().cloned() {
      tree.remove(&first);
      self.dict.pin().remove(&first.member);
      Some((first.member, first.score.into_inner()))
    } else { None }
  }

  pub fn pop_max(&self) -> Option<(Vec<u8>, f64)> {
    let mut tree = self.tree.write();
    if let Some(last) = tree.iter().next_back().cloned() {
      tree.remove(&last);
      self.dict.pin().remove(&last.member);
      Some((last.member, last.score.into_inner()))
    } else { None }
  }

  /// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:Count
  pub fn count(&self) -> usize { self.dict.pin().len() }
}

impl Default for SortedSetObject { fn default() -> Self { Self::new() } }
"""

import os
with open("wobject/src/list/list_object.rs", "w") as f: f.write(list_content)
with open("wobject/src/set/set_object.rs", "w") as f: f.write(set_content)
with open("wobject/src/hash/hash_object.rs", "w") as f: f.write(hash_content)
with open("wobject/src/sorted_set/sorted_set_object.rs", "w") as f: f.write(sorted_set_content)

