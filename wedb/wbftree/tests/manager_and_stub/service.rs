use std::{env, fs, process};

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
  let manager = RangeIndexManager::new(&env.ri_root, &env.cpr_root).unwrap();
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

/// 测试 1:1 对标 Garnet 的静态原生指针操作 (InsertByPtr, ReadByPtr, ReadByPtrInto, DeleteByPtr, ScanByPtr, Noop)
#[test]
fn test_bftree_service_pointer_based_operations() -> Result<()> {
  let tree = BfTreeService::open_memory(4)?;
  let ptr = tree.native_ptr();
  assert_ne!(ptr, 0);

  // 1. noop 与 noop_by_ptr
  assert_eq!(tree.noop(b"noop_k"), 0);
  assert_eq!(unsafe { BfTreeService::noop_by_ptr(ptr, b"noop_k") }, 0);

  // 2. insert_by_ptr
  assert_eq!(
    unsafe { BfTreeService::insert_by_ptr(ptr, b"ptr_k1", b"ptr_v1") },
    BfTreeInsertResult::Success
  );
  assert_eq!(
    unsafe { BfTreeService::insert_by_ptr(ptr, b"ptr_k2", b"ptr_v2") },
    BfTreeInsertResult::Success
  );
  assert_eq!(
    unsafe { BfTreeService::insert_by_ptr(ptr, b"ptr_k3", b"") },
    BfTreeInsertResult::InvalidKV
  );

  // 3. read_by_ptr
  let (res, val) = unsafe { BfTreeService::read_by_ptr(ptr, b"ptr_k1") };
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(val, Some(b"ptr_v1".to_vec()));

  // 4. read_by_ptr_into
  let mut out_buf = [0u8; 32];
  let (res, len) = unsafe { BfTreeService::read_by_ptr_into(ptr, b"ptr_k2", &mut out_buf) };
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(&out_buf[..len], b"ptr_v2");

  // 5. scan_with_count_by_ptr_callback
  let mut count = 0;
  let scanned = unsafe {
    BfTreeService::scan_with_count_by_ptr_callback(
      ptr,
      b"ptr_k",
      10,
      ScanReturnField::KeyAndValue,
      |k, v| {
        count += 1;
        assert!(k.starts_with(b"ptr_k"));
        assert!(v.starts_with(b"ptr_v"));
        true
      },
    )?
  };
  assert_eq!(scanned, 2);
  assert_eq!(count, 2);

  // 6. scan_with_end_key_by_ptr_callback
  let mut count2 = 0;
  let scanned2 = unsafe {
    BfTreeService::scan_with_end_key_by_ptr_callback(
      ptr,
      b"ptr_k1",
      b"ptr_k2",
      ScanReturnField::Key,
      |_k, _v| {
        count2 += 1;
        true
      },
    )?
  };
  assert_eq!(scanned2, 2);
  assert_eq!(count2, 2);

  // 7. delete_by_ptr
  assert_eq!(
    unsafe { BfTreeService::delete_by_ptr(ptr, b"ptr_k1") },
    BfTreeDeleteResult::Success
  );
  let (res_del, val_del) = unsafe { BfTreeService::read_by_ptr(ptr, b"ptr_k1") };
  assert_eq!(res_del, BfTreeReadResult::Deleted);
  assert_eq!(val_del, None);

  // 8. 零指针防御 (零指针 → InvalidArguments，对标 C# INSERT_INVALID_ARGS；
  //    非零指针 + 空值 → InvalidKV，引擎叶子插入断言非空值)
  assert_eq!(
    unsafe { BfTreeService::insert_by_ptr(0, b"k", b"v") },
    BfTreeInsertResult::InvalidArguments
  );
  assert_eq!(
    unsafe { BfTreeService::insert_by_ptr(ptr, b"k", b"") },
    BfTreeInsertResult::InvalidKV
  );
  assert_eq!(
    unsafe { BfTreeService::read_by_ptr(0, b"k") }.0,
    BfTreeReadResult::InvalidArguments
  );
  assert_eq!(
    unsafe { BfTreeService::delete_by_ptr(0, b"k") },
    BfTreeDeleteResult::InvalidArguments
  );

  OK
}

/// 验证在 disposed 状态下调用 insert/delete 均安全拒绝，重复释放幂等
#[test]
fn test_write_guard_no_underflow_on_disposed() -> Result<()> {
  let service = BfTreeService::open_memory(0)?;
  assert_eq!(service.insert(b"k1", b"v1"), BfTreeInsertResult::Success);

  // 释放实例
  service.dispose();
  assert!(service.is_disposed());

  // 在已释放状态下连续尝试写入/删除
  assert_eq!(
    service.insert(b"k2", b"v2"),
    BfTreeInsertResult::InvalidArguments
  );
  assert_eq!(
    service.delete(b"k1"),
    wbftree::BfTreeDeleteResult::InvalidArguments
  );

  // 重复释放幂等
  assert!(service.dispose_quiesced().is_ok());

  OK
}

/// 验证通过原生指针操作大于默认栈缓冲区 (4096 / 8192) 的大记录树时绝不越界 panic
#[test]
fn test_pointer_operations_with_large_record() -> Result<()> {
  let dir = env::temp_dir().join(format!(
    "wbftree_ptr_large_{}_{}",
    process::id(),
    fastrand::u64(..)
  ));
  fs::create_dir_all(&dir)?;
  let work = dir.join("work.bftree");

  let mut config = wbftree::BfTreeConfig::default();
  config
    .use_snapshot(true)
    .leaf_page_size(32768)
    .cb_max_record_size(8192)
    .cb_max_key_len(512)
    .cb_min_record_size(8);
  config.file_path(&work);
  let tree = BfTreeService::new(config)?;
  let ptr = tree.native_ptr();

  let big_val = vec![b'v'; 6000];

  // 1. insert_by_ptr 大值
  assert_eq!(
    unsafe { BfTreeService::insert_by_ptr(ptr, b"big_k1", &big_val) },
    BfTreeInsertResult::Success
  );

  // 2. read_by_ptr 大值 (超过 STACK_READ_BUF_SIZE 4096，应自动走暂存缓冲且不 panic)
  let (res, val) = unsafe { BfTreeService::read_by_ptr(ptr, b"big_k1") };
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(val.as_deref(), Some(&big_val[..]));

  // 3. read_by_ptr_into 大值 (传入缓冲区足量)
  let mut big_out = vec![0u8; 8192];
  let (res, len) = unsafe { BfTreeService::read_by_ptr_into(ptr, b"big_k1", &mut big_out) };
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(&big_out[..len], &big_val[..]);

  // 4. read_by_ptr_into 容量不足安全返回 InvalidArguments，不越界 panic
  let mut small_out = [0u8; 64];
  let (res_small, _) = unsafe { BfTreeService::read_by_ptr_into(ptr, b"big_k1", &mut small_out) };
  assert_eq!(res_small, BfTreeReadResult::InvalidArguments);

  // 5. scan_with_count_by_ptr_callback 扫描大值 (不越界 panic)
  let mut count = 0;
  let scanned = unsafe {
    BfTreeService::scan_with_count_by_ptr_callback(
      ptr,
      b"big_k",
      10,
      ScanReturnField::KeyAndValue,
      |k, v| {
        assert_eq!(k, b"big_k1");
        assert_eq!(v, &big_val[..]);
        count += 1;
        true
      },
    )?
  };
  assert_eq!(scanned, 1);
  assert_eq!(count, 1);

  let _ = fs::remove_dir_all(&dir);
  OK
}
