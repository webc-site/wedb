use aok::{OK, Result};
use wbftree::{BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, BfTreeService};

use super::common::TempTreeGuard;

/// 测试基础键值插入与读取往返一致性（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:InsertAndRead_BasicRoundTrip）
#[test]
fn test_insert_and_read_basic_round_trip() -> Result<()> {
  let path = TempTreeGuard::new("basic_roundtrip");
  let tree = BfTreeService::open_disk(&path, 4)?;

  let key = b"mykey";
  let val = b"myvalue";
  assert_eq!(tree.insert(key, val), BfTreeInsertResult::Success);

  let (res, read_val) = tree.read(key);
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(read_val, Some(val.to_vec()));

  OK
}

/// 测试相同键重复插入覆盖后返回最新值（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:InsertOverwrite_ReturnsUpdatedValue）
#[test]
fn test_insert_overwrite_returns_updated_value() -> Result<()> {
  let path = TempTreeGuard::new("overwrite");
  let tree = BfTreeService::open_disk(&path, 4)?;

  let key = b"mykey";
  assert_eq!(tree.insert(key, b"value1"), BfTreeInsertResult::Success);
  assert_eq!(tree.insert(key, b"value2"), BfTreeInsertResult::Success);

  let (res, val) = tree.read(key);
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(val, Some(b"value2".to_vec()));

  OK
}

/// 测试批量插入多个键值对后均可正确读取（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:InsertMultiple_AllReadable）
#[test]
fn test_insert_multiple_all_readable() -> Result<()> {
  let path = TempTreeGuard::new("multi_read");
  let tree = BfTreeService::open_disk(&path, 4)?;

  for i in 0..100 {
    let k = format!("key:{:04}", i).into_bytes();
    let v = format!("value:{}", i).into_bytes();
    assert_eq!(tree.insert(&k, &v), BfTreeInsertResult::Success);
  }

  for i in 0..100 {
    let k = format!("key:{:04}", i).into_bytes();
    let expected = format!("value:{}", i).into_bytes();
    let (res, val) = tree.read(&k);
    assert_eq!(res, BfTreeReadResult::Found);
    assert_eq!(val, Some(expected));
  }

  OK
}

/// 测试读取不存在的键安全返回 NotFound（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ReadNotFound）
#[test]
fn test_read_not_found() -> Result<()> {
  let path = TempTreeGuard::new("not_found");
  let tree = BfTreeService::open_disk(&path, 4)?;

  let (res, val) = tree.read(b"nonexistent");
  assert_eq!(res, BfTreeReadResult::NotFound);
  assert_eq!(val, None);

  OK
}

/// 测试删除已存在的键后读取返回 Deleted（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ReadAfterDelete_ReturnsDeleted）
#[test]
fn test_read_after_delete_returns_deleted() -> Result<()> {
  let path = TempTreeGuard::new("read_after_del");
  let tree = BfTreeService::open_disk(&path, 4)?;

  let key = b"deleteme";
  tree.insert(key, b"value");
  tree.delete(key);

  let (res, val) = tree.read(key);
  assert_eq!(res, BfTreeReadResult::Deleted);
  assert_eq!(val, None);

  OK
}

/// 测试将值零分配直接读入目标切片缓冲区（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ReadIntoSpan_ZeroAlloc）
#[test]
fn test_read_into_span_zero_alloc() -> Result<()> {
  let path = TempTreeGuard::new("read_into");
  let tree = BfTreeService::open_disk(&path, 4)?;

  let key = b"span_key";
  let val = b"span_value";
  tree.insert(key, val);

  let mut buf = [0u8; 64];
  let (res, len) = tree.read_into(key, &mut buf);
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(&buf[..len], val);

  OK
}

/// 测试零分配读取不存在的键返回 NotFound 且读取长度为 0（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:ReadIntoSpan_NotFound）
#[test]
fn test_read_into_span_not_found() -> Result<()> {
  let path = TempTreeGuard::new("read_into_nf");
  let tree = BfTreeService::open_disk(&path, 4)?;

  let mut buf = [0u8; 64];
  let (res, len) = tree.read_into(b"nope", &mut buf);
  assert_eq!(res, BfTreeReadResult::NotFound);
  assert_eq!(len, 0);

  OK
}

/// 测试删除已存在的键成功返回 Success（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:DeleteExistingKey）
#[test]
fn test_delete_existing_key() -> Result<()> {
  let path = TempTreeGuard::new("del_exist");
  let tree = BfTreeService::open_disk(&path, 4)?;

  let key = b"toremove";
  tree.insert(key, b"data");
  assert_eq!(tree.delete(key), BfTreeDeleteResult::Success);

  let (res, _) = tree.read(key);
  assert_eq!(res, BfTreeReadResult::Deleted);

  OK
}

/// 测试删除不存在的键安全返回 Success（对照 test/standalone/BfTreeInterop.test/BfTreeInteropTests.cs:DeleteNonExistentKey_ReturnsSuccess）
#[test]
fn test_delete_non_existent_key_returns_success() -> Result<()> {
  let path = TempTreeGuard::new("del_ghost");
  let tree = BfTreeService::open_disk(&path, 4)?;

  assert_eq!(tree.delete(b"ghost"), BfTreeDeleteResult::Success);

  OK
}

/// 测试 read_into 缓冲区边界契约：小缓冲区读大值安全返回 InvalidArguments
#[test]
fn test_read_into_buffer_contract() -> Result<()> {
  let path = TempTreeGuard::new("read_into_contract");
  let tree = BfTreeService::open_disk(&path, 4)?;

  // 1. 小值 + 小缓冲区：内部安全缓冲兜底后拷贝成功
  tree.insert(b"small", b"tiny_value");
  let mut small_buf = [0u8; 64];
  let (res, len) = tree.read_into(b"small", &mut small_buf);
  assert_eq!(res, BfTreeReadResult::Found);
  assert_eq!(&small_buf[..len], b"tiny_value");

  // 2. 大值 (4000 字节) + 小缓冲区 (64 字节)：返回 InvalidArguments，绝不 panic
  let big_val = vec![b'x'; 4000];
  tree.insert(b"big", &big_val);
  let (res2, len2) = tree.read_into(b"big", &mut small_buf);
  assert_eq!(res2, BfTreeReadResult::InvalidArguments);
  assert_eq!(len2, 0);

  // 3. 足够大的缓冲区走直读快路径
  let mut big_buf = vec![0u8; 4096];
  let (res3, len3) = tree.read_into(b"big", &mut big_buf);
  assert_eq!(res3, BfTreeReadResult::Found);
  assert_eq!(len3, 4000);
  assert_eq!(&big_buf[..len3], &big_val[..]);

  OK
}
