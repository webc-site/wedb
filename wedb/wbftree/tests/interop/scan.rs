//! 自研依据: bf-tree 扫描语义（C# 参照面 RespRangeIndexTests.cs 扫描臂）
use aok::{OK, Result};
use wbftree::{BfTreeInsertResult, ScanReturnField, StorageBackendType, TreeTuning};

use super::common::{insert_test_data, managed_tree};

/// 测试限制数量扫描返回指定条数的结果（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ScanWithCount_ReturnsCorrectCount）
#[test]
fn test_scan_with_count_returns_correct_count() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_scan_count",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;

  insert_test_data(&tree, 10);
  let records = tree.scan_with_count(b"key:", 5, ScanReturnField::KeyAndValue)?;
  assert_eq!(records.len(), 5);

  OK
}

/// 测试扫描同时返回键和值数据（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ScanWithCount_ReturnsKeyAndValue）
#[test]
fn test_scan_with_count_returns_key_and_value() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_scan_kv",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;

  insert_test_data(&tree, 5);
  let records = tree.scan_with_count(b"key:", 10, ScanReturnField::KeyAndValue)?;
  assert_eq!(records.len(), 5);

  for r in records {
    assert!(!r.key.is_empty());
    assert!(!r.value.is_empty());
  }

  OK
}

/// 测试扫描仅返回键而不返回多余的值数据（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ScanWithCount_KeyOnly）
#[test]
fn test_scan_with_count_key_only() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_scan_key_only",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;

  insert_test_data(&tree, 5);
  let records = tree.scan_with_count(b"key:", 10, ScanReturnField::Key)?;
  assert_eq!(records.len(), 5);

  for r in records {
    assert!(!r.key.is_empty());
    assert!(r.value.is_empty());
  }

  OK
}

/// 测试扫描仅返回值而不返回多余的键数据（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ScanWithCount_ValueOnly）
#[test]
fn test_scan_with_count_value_only() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_scan_val_only",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;

  insert_test_data(&tree, 5);
  let records = tree.scan_with_count(b"key:", 10, ScanReturnField::Value)?;
  assert_eq!(records.len(), 5);

  for r in records {
    assert!(r.key.is_empty());
    assert!(!r.value.is_empty());
  }

  OK
}

/// 测试扫描返回结果保持键升序排列（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ScanWithCount_Ordering）
#[test]
fn test_scan_with_count_ordering() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_scan_order",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;

  insert_test_data(&tree, 10);
  let records = tree.scan_with_count(b"key:", 10, ScanReturnField::Key)?;
  assert_eq!(records.len(), 10);

  for i in 0..records.len() - 1 {
    assert!(records[i].key < records[i + 1].key);
  }

  OK
}

/// 测试从中间键起始执行限制数量扫描（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ScanWithCount_StartKeyInMiddle）
#[test]
fn test_scan_with_count_start_key_in_middle() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_scan_mid",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;

  insert_test_data(&tree, 10);
  let records = tree.scan_with_count(b"key:0005", 10, ScanReturnField::Key)?;
  assert_eq!(records.len(), 5);
  assert_eq!(records[0].key, b"key:0005");

  OK
}

/// 测试空树执行限制数量扫描返回空集合（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ScanWithCount_EmptyTree）
#[test]
fn test_scan_with_count_empty_tree() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_scan_empty",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;

  let records = tree.scan_with_count(b"key:", 10, ScanReturnField::KeyAndValue)?;
  assert!(records.is_empty());

  OK
}

/// 测试闭区间范围扫描返回指定区间内条目（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ScanWithEndKey_InclusiveRange）
#[test]
fn test_scan_with_end_key_inclusive_range() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_scan_end_key",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;

  insert_test_data(&tree, 10);
  let records = tree.scan_with_end_key(b"key:0002", b"key:0005", ScanReturnField::KeyAndValue)?;
  assert_eq!(records.len(), 4);
  assert_eq!(records[0].key, b"key:0002");
  assert_eq!(records[3].key, b"key:0005");

  OK
}

/// 测试范围扫描覆盖树中所有条目（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ScanWithEndKey_AllEntries）
#[test]
fn test_scan_with_end_key_all_entries() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_scan_end_all",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;

  insert_test_data(&tree, 5);
  let records = tree.scan_with_end_key(b"key:0000", b"key:0004", ScanReturnField::KeyAndValue)?;
  assert_eq!(records.len(), 5);

  OK
}

/// 测试起始键大于结束键时的逆向空区间扫描返回空集合（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ScanWithEndKey_EmptyRange）
#[test]
fn test_scan_with_end_key_empty_range() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_scan_end_empty",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;

  insert_test_data(&tree, 5);
  let records = tree.scan_with_end_key(b"key:0005", b"key:0002", ScanReturnField::KeyAndValue)?;
  assert!(records.is_empty());

  OK
}

/// 测试回调闭包遍历扫描实现零堆分配（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ScanWithCallback_ZeroAlloc）
#[test]
fn test_scan_with_callback_zero_alloc() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_scan_cb_zero",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;

  insert_test_data(&tree, 10);
  let mut count = 0;
  let scanned =
    tree.scan_with_count_callback(b"key:", 10, ScanReturnField::KeyAndValue, |_k, _v| {
      count += 1;
      true
    })?;
  assert_eq!(scanned, 10);
  assert_eq!(count, 10);

  OK
}

/// 测试回调闭包提前返回 false 能够及时终止扫描（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ScanWithCallback_EarlyStop）
#[test]
fn test_scan_with_callback_early_stop() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_scan_cb_stop",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;

  insert_test_data(&tree, 10);
  let mut count = 0;
  let scanned =
    tree.scan_with_count_callback(b"key:", 10, ScanReturnField::KeyAndValue, |_k, _v| {
      count += 1;
      count < 3
    })?;
  assert_eq!(scanned, 3);
  assert_eq!(count, 3);

  OK
}

/// 测试大规模插入后扫描所有条目的正确性（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:LargeInsertAndScan）
#[test]
fn test_large_insert_and_scan() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_large_scan",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;

  let count = 1000;
  for i in 0..count {
    let k = format!("large:{:06}", i).into_bytes();
    let v = format!("payload_{}_{}", i, "x".repeat(100)).into_bytes();
    assert_eq!(tree.insert(&k, &v), BfTreeInsertResult::Success);
  }

  let records = tree.scan_with_count(b"large:", count + 1, ScanReturnField::Key)?;
  assert_eq!(records.len(), count);

  OK
}

/// 测试扫描数量为 0 时直接返回空结果
#[test]
fn test_scan_with_zero_count_returns_empty() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_scan_zero",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;

  insert_test_data(&tree, 5);
  let records = tree.scan_with_count(b"key:", 0, ScanReturnField::KeyAndValue)?;
  assert!(records.is_empty());

  let mut callback_count = 0;
  let scanned = tree.scan_with_count_callback(b"key:", 0, ScanReturnField::Key, |_k, _v| {
    callback_count += 1;
    true
  })?;
  assert_eq!(scanned, 0);
  assert_eq!(callback_count, 0);

  OK
}

/// 测试非法扫描参数（空起始键、超长键）返回错误，空结束键安全视为空区间
#[test]
fn test_scan_invalid_arguments_rejected() -> Result<()> {
  let (_dir, _manager, tree) = managed_tree(
    "bftree_test_scan_invalid",
    StorageBackendType::Disk,
    TreeTuning::default(),
  )?;

  insert_test_data(&tree, 3);

  // 空起始键
  assert!(tree.scan_with_count(b"", 10, ScanReturnField::Key).is_err());
  // 空结束键：任何键都大于空键 → 空区间安全返回空
  let empty = tree.scan_with_end_key(b"key:0000", b"", ScanReturnField::Key)?;
  assert!(empty.is_empty());
  // 超长键 (引擎默认 cb_max_key_len = 16，600 远超上限)
  let long_key = vec![b'k'; 600];
  assert!(
    tree
      .scan_with_count(&long_key, 10, ScanReturnField::Key)
      .is_err()
  );

  OK
}
