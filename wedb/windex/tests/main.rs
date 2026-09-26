use aok::{OK, Void};
use log::info;
use windex::{
  CandidateAddresses, Error, HashBucketEntry, HashIndex, PREFETCH_WINDOW, prefetch_read_l1,
};

/// 测试侧索引灌数便捷口：生产导出面不再提供 key 版 `insert`，
/// 统一按唯一免查重追加写入口 [`HashIndex::insert_to_bucket`] 等价构造
fn index_insert(index: &HashIndex, key: &[u8], address: u64) -> windex::Result<()> {
  let hash = HashIndex::hash_key(key);
  let tag = HashBucketEntry::tag_from_hash(hash);
  index.insert_to_bucket(index.bucket_index_for_hash(hash), tag, address)
}

/// 验证 CandidateAddresses 栈/堆生命周期、retain 回填与原地降序重排
#[test]
fn test_candidate_addresses_stack_heap_lifecycle() -> Void {
  info!("验证 CandidateAddresses 栈/堆生命周期、retain 回填与排序");

  let mut list = CandidateAddresses::new();
  assert_eq!(list.len(), 0);
  assert!(list.is_empty());
  assert_eq!(list.as_slice(), Some([].as_slice()));
  assert_eq!(list.first(), None);

  // 1. 填入 8 个元素（栈上满载）
  for i in 1..=8 {
    list.push(i as u64 * 100);
  }
  assert_eq!(list.len(), 8);
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
  assert_eq!(list.as_slice(), None, "突破 8 槽位必须进入堆扩展");
  assert_eq!(list[11], 1200);
  assert!(list.contains(1200));

  // 3. 执行 retain 谓词过滤：过滤后剩余 6 个元素 (<= 8)，搬移回收至栈数组
  list.retain(|x| (x / 100) % 2 != 0); // 100, 300, 500, 700, 900, 1100
  assert_eq!(list.len(), 6);
  assert_eq!(
    list.as_slice(),
    Some(&[100, 300, 500, 700, 900, 1100][..]),
    "过滤后元素数 <= 8 必须回退回收至栈数组"
  );

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
  assert_eq!(heap_list.as_slice(), None);

  heap_list.sort_descending();
  let mut expected = input.to_vec();
  expected.sort_unstable_by(|a, b| b.cmp(a));
  assert_eq!(heap_list.to_vec(), expected);

  // 6. IntoIterator 拥有权遍历验证
  let collected: Vec<u64> = list.into_iter().collect();
  assert_eq!(collected, [1100, 900, 700, 500, 300, 100]);

  OK
}

// 验证单批两级预取内核与逐项单查一致性
// （对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:ContextReadWithPrefetch）
#[test]
fn test_prefetch_batch_probes() -> Void {
  info!("验证单批两级预取内核与逐项单查一致性");

  let val = 42u64;
  prefetch_read_l1(&val);

  let arr = [1u8; 64];
  prefetch_read_l1(arr.as_ptr());

  let index = HashIndex::new(256)?;
  let window = PREFETCH_WINDOW;
  let mut keys = Vec::with_capacity(window);

  for i in 0..window {
    let key = format!("probe_key_{}", i).into_bytes();
    index_insert(&index, &key, (i as u64) + 1000)?;
    keys.push(key);
  }

  // 1. 满窗口批：探针哈希与首地址恒等于逐项单查口径，命中地址逐项交记录预取回调
  let mut record_addrs = Vec::with_capacity(window);
  let probes = index.prefetch_batch_probes(
    &keys,
    |_| -> windex::Result<()> { Ok(()) },
    |addr| record_addrs.push(addr),
  )?;
  for (i, probe) in probes[..window].iter().enumerate() {
    assert_eq!(probe.hash, HashIndex::hash_key(&keys[i]), "哈希单次算定");
    assert_eq!(
      probe.first_addr,
      index.find_tag_by_hash(probe.hash),
      "首地址与 FindTag 同源"
    );
    assert_eq!(probe.first_addr, Some(1000 + i as u64), "链首即本键地址");
  }
  assert_eq!(
    record_addrs.len(),
    window,
    "每个命中首地址各触发一次记录物理地址预取"
  );

  // 2. 短批（< 窗口）：窗口外槽位保持未填充探针，调用方按批长截断消费
  let probes =
    index.prefetch_batch_probes(&keys[..3], |_| -> windex::Result<()> { Ok(()) }, |_| {})?;
  for (i, probe) in probes.iter().enumerate().take(3) {
    assert_eq!(probe.first_addr, Some(1000 + i as u64));
  }
  assert_eq!(probes[3].hash, 0);
  assert_eq!(probes[3].first_addr, None);

  // 3. 扩容分块推进报错必须显式上抛，杜绝半迁移状态下产出探针
  let err = index.prefetch_batch_probes(
    &keys[..2],
    |_| Err(Error::InvalidBucketCount(0)),
    |_| unreachable!("第一级即失败，不得进入第二级"),
  );
  assert!(err.is_err(), "迁移推进错误直接上抛");

  OK
}
