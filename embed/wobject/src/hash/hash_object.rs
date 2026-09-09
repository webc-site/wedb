use papaya::HashMap;
use gxhash::GxBuildHasher;
use std::collections::BinaryHeap;
use std::sync::Mutex;
use std::cmp::Reverse;

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
        self.expiration.cmp(&other.expiration).then_with(|| self.key.cmp(&other.key))
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
}

impl Default for HashObject {
    fn default() -> Self {
        Self::new()
    }
}
