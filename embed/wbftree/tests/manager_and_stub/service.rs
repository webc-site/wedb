use aok::{OK, Result};
use wbftree::{
  BfTreeConfig, BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, BfTreeService,
  RangeIndexManager, ScanReturnField, StorageBackend, TreeTuning,
};

use super::common::{ManagerEnvGuard, TUNE, TestFileGuard};

/// 测试 BfTreeService::read 栈缓冲区读取与大记录堆读取无缝切换
#[test]
fn test_bftree_service_read_stack_and_large_heap_value() -> Result<()> {
  let env = ManagerEnvGuard::new("srv_read");
  let manager = RangeIndexManager::new(&env.ri_root, &env.cpr_root);
  let key = b"srv_read_key";

  let tree = manager.create_bftree(
    key,
    StorageBackend::Std,
    TreeTuning {
      max_record_size: 6000,
      leaf_page_size: 16384,
      ..TUNE
    },
  )?;

  // 1. 测试常规 <= 4096 字节值（命中栈缓冲区路径）
  let small_val = b"hello_world_4096";
  assert_eq!(
    tree.insert(b"small_k", small_val),
    BfTreeInsertResult::Success
  );

  let (res1, val1) = tree.read(b"small_k");
  assert_eq!(res1, BfTreeReadResult::Found);
  assert_eq!(val1, Some(small_val.to_vec()));

  // 2. 测试超出 4096 字节大记录（无缝切换至堆内存分配）
  let large_val = vec![0x5A; 4500];
  assert_eq!(
    tree.insert(b"large_k", &large_val),
    BfTreeInsertResult::Success
  );

  let (res2, val2) = tree.read(b"large_k");
  assert_eq!(res2, BfTreeReadResult::Found);
  assert_eq!(val2, Some(large_val));

  // 3. 测试不存在键（零堆分配且安全返回 NotFound）
  let (res3, val3) = tree.read(b"non_existent_key");
  assert_eq!(res3, BfTreeReadResult::NotFound);
  assert_eq!(val3, None);

  OK
}

/// 测试 BfTreeService 的 is_disposed 状态与防并发访问安全性
#[test]
fn test_bftree_service_is_disposed_lifecycle() -> Result<()> {
  let tree = BfTreeService::open_memory(4)?;
  assert!(!tree.is_disposed());

  assert_eq!(
    tree.insert(b"live_k", b"live_v"),
    BfTreeInsertResult::Success
  );
  let (res, val) = tree.read(b"live_k");
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(val, Some(b"live_v".to_vec()));

  tree.dispose();
  assert!(tree.is_disposed());

  // 已释放状态下所有操作必须安全拒绝且无 panic
  assert_eq!(
    tree.insert(b"live_k", b"live_v"),
    BfTreeInsertResult::InvalidArguments
  );
  let (res2, val2) = tree.read(b"live_k");
  assert_eq!(res2, BfTreeReadResult::InvalidArguments);
  assert!(val2.is_none());
  let mut buf = [0u8; 32];
  let (res3, len) = tree.read_into(b"live_k", &mut buf);
  assert_eq!(res3, BfTreeReadResult::InvalidArguments);
  assert_eq!(len, 0);
  assert_eq!(tree.delete(b"live_k"), BfTreeDeleteResult::InvalidArguments);
  assert!(tree.scan_all(ScanReturnField::KeyAndValue).is_err());
  assert!(
    tree
      .scan_with_count(b"live_k", 10, ScanReturnField::KeyAndValue)
      .is_err()
  );

  // 重复释放幂等安全
  tree.dispose();
  assert!(tree.is_disposed());
  OK
}

/// 测试扫描回调重入安全：回调内可重入点读乃至 dispose 同一服务实例而不死锁
#[test]
fn test_scan_callback_reentrant_access() -> Result<()> {
  let tree = BfTreeService::open_memory(4)?;
  for i in 0..10 {
    let k = format!("rk_{i:02}").into_bytes();
    assert_eq!(tree.insert(&k, b"v"), BfTreeInsertResult::Success);
  }

  // 1. 回调内重入同一服务的点读：read 与 read_into 均安全返回
  let mut visited = 0;
  let scanned =
    tree.scan_with_count_callback(b"rk_", 10, ScanReturnField::KeyAndValue, |k, _v| {
      visited += 1;
      let (res, val) = tree.read(k);
      assert_eq!(res, BfTreeReadResult::Found);
      assert_eq!(val, Some(b"v".to_vec()));
      let mut buf = [0u8; 16];
      let (res2, len2) = tree.read_into(b"rk_00", &mut buf);
      assert_eq!(res2, BfTreeReadResult::Found);
      assert_eq!(&buf[..len2], b"v");
      true
    })?;
  assert_eq!(scanned, 10);
  assert_eq!(visited, 10);

  // 2. 回调内 dispose 自身服务：Arc 保障迭代期间引擎存活，剩余记录安全扫完
  let scanned2 = tree.scan_with_count_callback(b"rk_", 10, ScanReturnField::Key, |k, _| {
    if k == b"rk_05" {
      tree.dispose();
    }
    true
  })?;
  assert_eq!(scanned2, 10);
  assert!(tree.is_disposed());

  // 3. dispose 后新操作安全拒绝，无 panic
  assert_eq!(tree.read(b"rk_00").0, BfTreeReadResult::InvalidArguments);

  OK
}

/// 测试大记录读取 (>4096 字节) 绝不发生切片越界 Panic
#[test]
fn test_large_record_read_no_panic() -> Result<()> {
  let path = TestFileGuard::new("large_rec", "bftree");
  {
    let mut config = BfTreeConfig::default();
    config
      .file_path(&path)
      .cb_size_byte(16 * 1024 * 1024)
      .cb_min_record_size(8)
      .cb_max_record_size(8192)
      .cb_max_key_len(128)
      .leaf_page_size(32768);
    let tree = BfTreeService::new(config)?;

    let large_val = vec![b'x'; 6000];
    assert_eq!(
      tree.insert(b"big_k", &large_val),
      BfTreeInsertResult::Success
    );

    let (res, val) = tree.read(b"big_k");
    assert_eq!(res, BfTreeReadResult::Found);
    assert_eq!(val, Some(large_val));
  }

  OK
}

/// 测试空值 (0 字节) 安全拦截为 InvalidKV，绝不触发底层断言 Panic
#[test]
fn test_range_index_empty_value_safe_rejection() -> Result<()> {
  let tree = BfTreeService::open_memory(4)?;
  assert_eq!(
    tree.insert(b"empty_key", b""),
    BfTreeInsertResult::InvalidKV
  );

  let (res, val) = tree.read(b"empty_key");
  assert_eq!(res, BfTreeReadResult::NotFound);
  assert_eq!(val, None);

  let (res2, val2) = tree.read(b"non_existent");
  assert_eq!(res2, BfTreeReadResult::NotFound);
  assert_eq!(val2, None);

  OK
}
