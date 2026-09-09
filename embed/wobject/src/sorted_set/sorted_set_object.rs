use std::{
  cmp::Ordering,
  collections::BTreeSet,
  io::{self, Read, Write},
};

use gxhash::GxBuildHasher;
use papaya::HashMap;
use parking_lot::Mutex;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SortedSetOperation {
  Zadd = 0,
  Zrem = 1,
  Zscore = 2,
  Zrank = 3,
  Zrevrank = 4,
  Zcount = 5,
  Zpopmin = 6,
  Zpopmax = 7,
  Zincrby = 8,
  Zcard = 9,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SortedSetEntry {
  pub score: f64,
  pub member: Vec<u8>,
}

impl Eq for SortedSetEntry {}

impl PartialOrd for SortedSetEntry {
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for SortedSetEntry {
  fn cmp(&self, other: &Self) -> Ordering {
    // Note: BTreeSet requires total ordering. f64 has NaN which is not completely ordered.
    // We use total_cmp for f64 here.
    self
      .score
      .total_cmp(&other.score)
      .then_with(|| self.member.cmp(&other.member))
  }
}

/// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetObject
pub struct SortedSetObject {
  pub dict: HashMap<Vec<u8>, f64, GxBuildHasher>,
  pub tree: Mutex<BTreeSet<SortedSetEntry>>,
}

impl SortedSetObject {
  pub fn new() -> Self {
    Self {
      dict: HashMap::with_hasher(GxBuildHasher::default()),
      tree: Mutex::new(BTreeSet::new()),
    }
  }

  /// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetObject(BinaryReader)
  pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;
    let items: Vec<(Vec<u8>, f64)> =
      bitcode::decode(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let dict = HashMap::with_hasher(GxBuildHasher::default());
    let pin = dict.pin();
    let mut tree = BTreeSet::new();

    for (member, score) in items {
      pin.insert(member.clone(), score);
      tree.insert(SortedSetEntry { score, member });
    }

    drop(pin);
    Ok(Self {
      dict,
      tree: Mutex::new(tree),
    })
  }

  /// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:Serialize
  pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
    let pin = self.dict.pin();
    let mut items = Vec::with_capacity(pin.len());
    for (member, score) in pin.iter() {
      items.push((member.clone(), *score));
    }
    let bytes = bitcode::encode(&items);
    writer.write_all(&bytes)
  }

  /// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:Operate
  ///
  /// Zadd：写入（含同成员改分），返回 None；Zrem：移除成员，返回旧分值
  /// （未命中返回 None）；Zincrby：按 delta 增减分值，返回新分值；
  /// Zscore：查询分值。其余读操作（rank/count/pop 等）由会话层直接调用
  /// 对应方法，不经本入口。
  pub fn operate(&self, op: SortedSetOperation, member: &[u8], score: f64) -> Option<f64> {
    let pin = self.dict.pin();
    match op {
      SortedSetOperation::Zadd => {
        let mut tree = self.tree.lock();
        if let Some(old_score) = pin.get(member) {
          if *old_score != score {
            tree.remove(&SortedSetEntry {
              score: *old_score,
              member: member.to_vec(),
            });
            tree.insert(SortedSetEntry {
              score,
              member: member.to_vec(),
            });
            pin.insert(member.to_vec(), score);
          }
        } else {
          tree.insert(SortedSetEntry {
            score,
            member: member.to_vec(),
          });
          pin.insert(member.to_vec(), score);
        }
        None
      }
      SortedSetOperation::Zrem => {
        let mut tree = self.tree.lock();
        match pin.remove(member) {
          Some(old_score) => {
            tree.remove(&SortedSetEntry {
              score: *old_score,
              member: member.to_vec(),
            });
            Some(*old_score)
          }
          None => None,
        }
      }
      SortedSetOperation::Zincrby => {
        let mut tree = self.tree.lock();
        let new_score = match pin.get(member) {
          Some(&old) => {
            tree.remove(&SortedSetEntry {
              score: old,
              member: member.to_vec(),
            });
            old + score
          }
          None => score,
        };
        tree.insert(SortedSetEntry {
          score: new_score,
          member: member.to_vec(),
        });
        pin.insert(member.to_vec(), new_score);
        Some(new_score)
      }
      SortedSetOperation::Zscore => pin.get(member).copied(),
      _ => None,
    }
  }

  /// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:Count
  pub fn count(&self) -> usize {
    self.dict.pin().len()
  }
}

impl Default for SortedSetObject {
  fn default() -> Self {
    Self::new()
  }
}

impl SortedSetObject {
  pub fn pop_min(&self) -> Option<(Vec<u8>, f64)> {
    let mut tree = self.tree.lock();
    if let Some(entry) = tree.iter().next().cloned() {
      tree.remove(&entry);
      self.dict.pin().remove(&entry.member);
      Some((entry.member, entry.score))
    } else {
      None
    }
  }

  pub fn pop_max(&self) -> Option<(Vec<u8>, f64)> {
    let mut tree = self.tree.lock();
    if let Some(entry) = tree.iter().next_back().cloned() {
      tree.remove(&entry);
      self.dict.pin().remove(&entry.member);
      Some((entry.member, entry.score))
    } else {
      None
    }
  }
}
