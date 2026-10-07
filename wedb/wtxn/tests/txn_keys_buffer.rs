#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wtxn::{LockType, TxnKeysBuffer};

#[test]
fn push_o1_append_preserves_all_entries() {
  let mut buf = TxnKeysBuffer::new();
  assert!(buf.is_empty());
  assert_eq!(buf.len(), 0);
  assert!(buf.is_read_only());

  buf.push(b"key1", LockType::Shared);
  buf.push(b"key2", LockType::Shared);
  assert!(buf.is_read_only());

  // 重复键追加：纯 O(1) 尾部追加不去重，锁型原样保留
  buf.push(b"key1", LockType::Exclusive);
  assert!(!buf.is_read_only());

  buf.push(b"key1", LockType::Shared);
  buf.push(b"key3", LockType::Exclusive);

  assert_eq!(buf.len(), 5);
  assert!(!buf.is_empty());
  assert_eq!(buf.iter().next(), Some(&b"key1"[..]));
  assert_eq!(buf.iter().nth(1), Some(&b"key2"[..]));
  assert_eq!(buf.iter().nth(2), Some(&b"key1"[..]));
  assert_eq!(buf.iter().nth(3), Some(&b"key1"[..]));
  assert_eq!(buf.iter().nth(4), Some(&b"key3"[..]));
  assert_eq!(buf.iter().nth(5), None);

  let collected: Vec<(&[u8], LockType)> = buf.iter_with_lock().collect();
  assert_eq!(
    collected,
    vec![
      (&b"key1"[..], LockType::Shared),
      (&b"key2"[..], LockType::Shared),
      (&b"key1"[..], LockType::Exclusive),
      (&b"key1"[..], LockType::Shared),
      (&b"key3"[..], LockType::Exclusive)
    ]
  );
}

#[test]
fn push_large_batch_o1_scale() {
  let mut buf = TxnKeysBuffer::new();
  let key = b"common_prefix_same_key";
  let count = 100_000;
  for _ in 0..count {
    buf.push(key, LockType::Shared);
  }
  assert_eq!(buf.len(), count);
  assert!(buf.is_read_only());
}

#[test]
fn clear_reuse_capacity() {
  let mut buf = TxnKeysBuffer::new();
  buf.push(b"first_key_long_enough_to_allocate", LockType::Exclusive);
  let (byte_cap, offset_cap) = buf.capacity();

  buf.clear();
  assert!(buf.is_empty());
  assert_eq!(buf.len(), 0);
  assert!(buf.is_read_only());
  assert_eq!(buf.capacity(), (byte_cap, offset_cap));

  buf.push(b"second_key", LockType::Shared);
  assert_eq!(buf.len(), 1);
  assert_eq!(buf.iter().next(), Some(&b"second_key"[..]));
  assert!(buf.is_read_only());
}

/// 16 键（`TXN_KEYS_INLINE_CAPACITY` 内联槽位）以内容量恒为内联值 16、
/// 无堆分配；第 17 键起偏移表与锁类型表溢出至堆（容量 > 16）。
#[test]
fn inline_capacity_16_and_spill_on_17() {
  let mut buf = TxnKeysBuffer::new();
  for i in 0..16 {
    let key = format!("k{i}");
    buf.push(key.as_bytes(), LockType::Shared);
  }
  assert_eq!(buf.len(), 16);
  assert_eq!(buf.capacity().1, 16, "16 键以内应保持内联，0 堆分配");

  buf.push(b"overflow_17", LockType::Exclusive);
  assert_eq!(buf.len(), 17);
  assert!(buf.capacity().1 > 16, "超过 16 键优雅溢出至堆");
  assert!(!buf.is_read_only());
}
