use aok::{OK, Void};
use log::info;
use windex::{CandidateAddresses, HashIndex, prefetch_read_l1};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

/// 验证 CandidateAddresses 栈/堆生命周期、retain 回填与原地降序重排
#[test]
fn test_candidate_addresses_stack_heap_lifecycle() -> Void {
  info!("验证 CandidateAddresses 栈/堆生命周期、retain 回填与排序");

  let mut list = CandidateAddresses::new();
  assert_eq!(list.len(), 0);
  assert!(list.is_empty());
  assert!(!list.is_heap_allocated());
  assert_eq!(list.as_slice(), Some([].as_slice()));
  assert_eq!(list.first(), None);

  // 1. 填入 8 个元素（栈上满载）
  for i in 1..=8 {
    list.push(i as u64 * 100);
  }
  assert_eq!(list.len(), 8);
  assert!(!list.is_heap_allocated());
  assert_eq!(list.first(), Some(100));
  assert_eq!(list[0], 100);
  assert_eq!(list[7], 800);
  assert!(list.contains(500));
  assert!(!list.contains(999));
  assert_eq!(
    list.as_slice(),
    Some(&[100, 200, 300, 400, 500, 600, 700, 800][..])
  );

  // 2. 填入第 9~12 个元素（突破 8 槽位进入堆扩展）
  for i in 9..=12 {
    list.push(i as u64 * 100);
  }
  assert_eq!(list.len(), 12);
  assert!(list.is_heap_allocated());
  assert_eq!(list.as_slice(), None);
  assert_eq!(list[11], 1200);
  assert!(list.contains(1200));

  // 3. 执行 retain 谓词过滤：过滤后剩余 6 个元素 (<= 8)，搬移回收至栈数组
  list.retain(|x| (x / 100) % 2 != 0); // 100, 300, 500, 700, 900, 1100
  assert_eq!(list.len(), 6);
  assert!(
    !list.is_heap_allocated(),
    "过滤后元素数 <= 8 必须回退回收至栈数组"
  );
  assert_eq!(list.as_slice(), Some(&[100, 300, 500, 700, 900, 1100][..]));

  // 4. 原地降序重排
  list.sort_descending();
  assert_eq!(list.as_slice(), Some(&[1100, 900, 700, 500, 300, 100][..]));

  // 5. 堆溢出模式下的 sort_descending 测试
  let mut heap_list = CandidateAddresses::new();
  let input = [3u64, 14, 1, 5, 9, 2, 6, 53, 58, 97, 93, 23, 84, 62, 64];
  for &x in &input {
    heap_list.push(x);
  }
  assert_eq!(heap_list.len(), 15);
  assert!(heap_list.is_heap_allocated());

  heap_list.sort_descending();
  let mut expected = input.to_vec();
  expected.sort_unstable_by(|a, b| b.cmp(a));
  assert_eq!(heap_list.to_vec(), expected);

  // 6. IntoIterator 拥有权遍历验证
  let collected: Vec<u64> = list.into_iter().collect();
  assert_eq!(collected, [1100, 900, 700, 500, 300, 100]);

  OK
}

/// 验证硬件预取与批量流水线查询一致性
#[test]
fn test_hardware_prefetch_and_batch_lookup() -> Void {
  info!("验证硬件预取与批量流水线查询一致性");

  let val = 42u64;
  prefetch_read_l1(&val);

  let arr = [1u8; 64];
  prefetch_read_l1(arr.as_ptr());

  let index = HashIndex::new(256)?;
  let count = 100usize;
  let mut keys = Vec::with_capacity(count);
  let mut hashes = Vec::with_capacity(count);

  for i in 0..count {
    let key = format!("batch_key_{}", i).into_bytes();
    let addr = (i as u64) + 1000;
    index.insert(&key, addr)?;
    hashes.push(HashIndex::hash_key(&key));
    keys.push(key);
  }

  let mut query_keys = Vec::with_capacity(count + 50);
  for i in 0..count + 50 {
    query_keys.push(format!("batch_key_{}", i).into_bytes());
  }
  let keys_slices: Vec<&[u8]> = query_keys.iter().map(|k| k.as_slice()).collect();

  // 1. 逐项单查 vs 批量预取查询
  let mut single_results = Vec::with_capacity(keys_slices.len());
  for &k in &keys_slices {
    single_results.push(index.lookup_candidates(k));
  }

  let mut batch_results = vec![CandidateAddresses::new(); keys_slices.len()];
  index.lookup_candidates_batch(&keys_slices, &mut batch_results);

  assert_eq!(single_results.len(), batch_results.len());
  for (single, batch) in single_results.iter().zip(&batch_results) {
    assert_eq!(single, batch);
  }

  // 边界测试：空列表与超短列表（< 12）
  let mut empty_res = Vec::new();
  index.lookup_candidates_batch(&[], &mut empty_res);

  let short_keys = [keys_slices[0], keys_slices[1], keys_slices[2]];
  let mut short_res = vec![CandidateAddresses::new(); 3];
  index.lookup_candidates_batch(&short_keys, &mut short_res);
  assert_eq!(&short_res[0], &single_results[0]);
  assert_eq!(&short_res[1], &single_results[1]);
  assert_eq!(&short_res[2], &single_results[2]);

  // 2. find_tag_batch 与 find_tag_batch_by_hash 流水线预取验证
  let sample_count = 20;
  let sample_keys: Vec<&[u8]> = keys[..sample_count].iter().map(|k| k.as_slice()).collect();
  let sample_hashes = &hashes[..sample_count];

  let mut results_by_key = vec![None; sample_count];
  index.find_tag_batch(&sample_keys, &mut results_by_key);

  let mut results_by_hash = vec![None; sample_count];
  index.find_tag_batch_by_hash(sample_hashes, &mut results_by_hash);

  assert_eq!(results_by_key, results_by_hash);
  for (i, &res) in results_by_hash.iter().enumerate() {
    let expected_addr = (i as u64) + 1000;
    assert_eq!(res, Some(expected_addr));
  }

  OK
}
