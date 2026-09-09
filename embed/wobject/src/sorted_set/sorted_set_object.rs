use std::{collections::BTreeSet, sync::RwLock};

use gxhash::GxBuildHasher;
use ordered_float::OrderedFloat;
use papaya::HashMap;

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
}

impl Default for SortedSetObject {
  fn default() -> Self {
    Self::new()
  }
}
