use std::{collections::BTreeSet, sync::RwLock};

use gxhash::GxBuildHasher;
use ordered_float::OrderedFloat;
use papaya::HashMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SortedSetOperation {
  Zadd = 0,
  Zcard = 1,
  Zpopmax = 2,
  Zscore = 3,
  Zrem = 4,
  Zcount = 5,
  Zincrby = 6,
  Zrank = 7,
  Zrange = 8,
  Geoadd = 9,
  Geohash = 10,
  Geodist = 11,
  Geopos = 12,
  Geosearch = 13,
  Zrevrank = 14,
  Zremrangebylex = 15,
  Zremrangebyrank = 16,
  Zremrangebyscore = 17,
  Zlexcount = 18,
  Zpopmin = 19,
  Zrandmember = 20,
  Zdiff = 21,
  Zscan = 22,
  Zmscore = 23,
  Zexpire = 24,
  Zttl = 25,
  Zpersist = 26,
  Zcollect = 27,
}

bitflags::bitflags! {
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct SortedSetRangeOpts: u8 {
    const NONE = 0;
    const BY_SCORE = 1;
    const BY_LEX = 1 << 1;
    const REVERSE = 1 << 2;
    const STORE = 1 << 3;
    const WITH_SCORES = 1 << 4;
  }

  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct SortedSetAddOption: u8 {
    const NONE = 0;
    const XX = 1;
    const NX = 1 << 1;
    const LT = 1 << 2;
    const GT = 1 << 3;
    const CH = 1 << 4;
  }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SortedSetEntry {
  pub score: OrderedFloat<f64>,
  pub member: Vec<u8>,
}

impl PartialOrd for SortedSetEntry {
  fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for SortedSetEntry {
  fn cmp(&self, other: &Self) -> std::cmp::Ordering {
    self
      .score
      .cmp(&other.score)
      .then_with(|| self.member.cmp(&other.member))
  }
}

/// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetObject
pub struct SortedSetObject {
  pub dict: HashMap<Vec<u8>, OrderedFloat<f64>, GxBuildHasher>,
  pub tree: RwLock<BTreeSet<SortedSetEntry>>,
}

impl SortedSetObject {
  pub fn new() -> Self {
    Self {
      dict: HashMap::with_hasher(GxBuildHasher::default()),
      tree: RwLock::new(BTreeSet::new()),
    }
  }

  /// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:Operate
  pub fn operate(&self, op: SortedSetOperation, member: &[u8], score: f64) -> Option<f64> {
    match op {
      SortedSetOperation::Zadd => {
        let fscore = OrderedFloat(score);
        let mut tree = self.tree.write().unwrap();
        if let Some(old_score) = self.dict.pin().get(member) {
          tree.remove(&SortedSetEntry {
            score: *old_score,
            member: member.to_vec(),
          });
        }
        self.dict.pin().insert(member.to_vec(), fscore);
        tree.insert(SortedSetEntry {
          score: fscore,
          member: member.to_vec(),
        });
        Some(score)
      }
      SortedSetOperation::Zscore => self.dict.pin().get(member).map(|s| s.into_inner()),
      SortedSetOperation::Zrem => {
        if let Some(s) = self.dict.pin().remove(member) {
          let mut tree = self.tree.write().unwrap();
          tree.remove(&SortedSetEntry {
            score: *s,
            member: member.to_vec(),
          });
          Some(s.into_inner())
        } else {
          None
        }
      }
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
