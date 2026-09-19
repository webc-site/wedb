use aok::{OK, Void};
use windex::{HashIndex, chunk_count, chunk_offset_for_hash, split_chunk, split_single_bucket};

use super::support::HashIndexTestOps;

#[test]
fn test_split_bucket_and_chunk() -> Void {
  let old_size = 1024;
  let new_size = 2048;
  let old_index = HashIndex::new(old_size)?;
  let new_index = HashIndex::new(new_size)?;

  let key1 = b"key_left_branch";
  let hash1 = HashIndex::hash_key(key1);
  let addr1 = 100u64;
  old_index.insert(key1, addr1)?;

  let key2 = b"key_right_branch_2";
  let hash2 = HashIndex::hash_key(key2);
  let addr2 = 200u64;
  old_index.insert(key2, addr2)?;

  let num_chunks = chunk_count(old_size);
  assert_eq!(num_chunks, 1);

  let offset1 = chunk_offset_for_hash(hash1, old_index.mask);
  assert_eq!(offset1, 0);

  split_chunk(
    &old_index,
    &new_index,
    0,
    num_chunks,
    |addr| {
      if addr == addr1 {
        Some((hash1, 0))
      } else if addr == addr2 {
        Some((hash2, 0))
      } else {
        None
      }
    },
    |_prev, _bit| None,
  )?;

  assert_eq!(new_index.find_tag(key1), Some(addr1));
  assert_eq!(new_index.find_tag(key2), Some(addr2));

  OK
}

#[test]
fn test_split_trace_back() -> Void {
  let old_size = 512;
  let new_size = 1024;
  let old_index = HashIndex::new(old_size)?;
  let new_index = HashIndex::new(new_size)?;

  let key_v2 = b"key_v2";
  let hash_v2 = HashIndex::hash_key(key_v2);
  let addr_v2 = 500u64;
  let addr_v1 = 400u64;

  old_index.insert(key_v2, addr_v2)?;

  let target_b = (hash_v2 as usize) & old_index.mask;

  split_single_bucket(
    &old_index,
    &new_index,
    target_b,
    |addr| {
      if addr == addr_v2 {
        Some((hash_v2, addr_v1))
      } else {
        None
      }
    },
    |prev_addr, _target_bit| {
      if prev_addr == addr_v1 {
        Some(addr_v1)
      } else {
        None
      }
    },
  )?;

  assert_eq!(new_index.find_tag(key_v2), Some(addr_v2));

  OK
}
