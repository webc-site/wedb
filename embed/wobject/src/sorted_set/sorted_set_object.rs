use std::{
  cmp::Ordering,
  collections::BTreeSet,
  io::{self, Read, Write},
  sync::RwLock,
};

use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
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
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for SortedSetEntry {
  fn cmp(&self, other: &Self) -> Ordering {
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

  /// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetObject(BinaryReader)
  pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
    let count = reader.read_i32::<LittleEndian>()?;
    let dict = HashMap::with_hasher(GxBuildHasher::default());
    let pin = dict.pin();
    let mut tree = BTreeSet::new();

    for _ in 0..count {
      let score = reader.read_f64::<LittleEndian>()?;
      let member_len = reader.read_i32::<LittleEndian>()?;
      let mut member = vec![0u8; member_len as usize];
      reader.read_exact(&mut member)?;

      let fscore = OrderedFloat(score);
      pin.insert(member.clone(), fscore);
      tree.insert(SortedSetEntry {
        score: fscore,
        member,
      });
    }
    drop(pin);
    Ok(Self {
      dict,
      tree: RwLock::new(tree),
    })
  }

  /// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:Serialize
  pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
    let pin = self.dict.pin();
    writer.write_i32::<LittleEndian>(pin.len() as i32)?;
    for (member, score) in pin.iter() {
      writer.write_f64::<LittleEndian>(score.into_inner())?;
      writer.write_i32::<LittleEndian>(member.len() as i32)?;
      writer.write_all(member)?;
    }
    Ok(())
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

  /// garnet相对路径:garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetPop
  pub fn pop_min(&self) -> Option<(Vec<u8>, f64)> {
    let mut tree = self.tree.write().unwrap();
    if let Some(first) = tree.iter().next().cloned() {
      tree.remove(&first);
      self.dict.pin().remove(&first.member);
      Some((first.member, first.score.into_inner()))
    } else {
      None
    }
  }

  pub fn pop_max(&self) -> Option<(Vec<u8>, f64)> {
    let mut tree = self.tree.write().unwrap();
    if let Some(last) = tree.iter().next_back().cloned() {
      tree.remove(&last);
      self.dict.pin().remove(&last.member);
      Some((last.member, last.score.into_inner()))
    } else {
      None
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
