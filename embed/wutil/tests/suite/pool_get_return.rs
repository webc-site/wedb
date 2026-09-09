//! BufferPool Get/Return 基础语义测试
//!
//! 对标 C#：libs/storage/Tsavorite/cs/test/SectorAlignedBufferPoolTests.cs 的
//! `GetCapacityCoversRequestAcrossSizes`、`GetReturnsZeroedBufferAndReuses`、
//! `OptOutClearThenDefaultGetIsZeroed` 与 EnsureSize / clearOnReturn 语义。

use aok::{OK, Void};
use log::info;
use wutil::{BufferPool, DEFAULT_SECTOR_SIZE, MIN_SECTOR_SIZE, NUM_CLASSES};

/// 跨尺寸请求：指针对齐、容量覆盖、首尾字节可写
#[test]
fn get_capacity_covers_request_across_sizes() -> Void {
  info!("对标 GetCapacityCoversRequestAcrossSizes：512 扇区池跨尺寸租借并触达首尾字节");

  let pool = BufferPool::new(MIN_SECTOR_SIZE)?;
  for bytes in [
    1, 100, 511, 512, 513, 4096, 4097, 60_000, 200_000, 2_000_000,
  ] {
    let mut page = pool.get_with_policy(bytes, false)?;
    assert_eq!(
      page.as_buf_ptr() as usize % MIN_SECTOR_SIZE,
      0,
      "{bytes} 字节请求的指针必须扇区对齐"
    );
    assert!(page.capacity() >= bytes, "容量必须覆盖 {bytes} 字节请求");
    // 触达首个与最后一个可用字节
    let slice = page.as_allocated_slice_mut();
    slice[0] = 1;
    slice[bytes - 1] = 1;
  }

  OK
}

/// 默认 Get 返回全零缓冲区，同线程归还后复用同一块内存并重新清零
#[test]
fn get_returns_zeroed_buffer_and_reuses() -> Void {
  info!("对标 GetReturnsZeroedBufferAndReuses：默认 Get 全零、归还后同指针复用且重新清零");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
  let ptr_val: usize;
  {
    let mut p1 = pool.get(4096)?;
    assert!(
      p1.as_allocated_slice().iter().all(|&b| b == 0),
      "默认 Get 必须返回全零缓冲区"
    );
    p1.as_allocated_slice_mut().fill(0xAB);
    ptr_val = p1.as_buf_ptr() as usize;
  }

  // 同线程归还 => 本地栈复用同一底层内存，且默认策略重新清零
  let p2 = pool.get(4096)?;
  assert_eq!(
    p2.as_buf_ptr() as usize,
    ptr_val,
    "同线程归还后必须复用本地缓存缓冲区"
  );
  assert!(
    p2.as_allocated_slice().iter().all(|&b| b == 0),
    "复用缓冲区必须重新清零"
  );

  OK
}

/// 免清零归还后，默认 Get 必须惰性清零脏缓冲区
#[test]
fn opt_out_clear_then_default_get_is_zeroed() -> Void {
  info!(
    "对标 OptOutClearThenDefaultGetIsZeroed：clearOnReturn=false 归还脏位，下次默认 Get 惰性清零"
  );

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
  let ptr_val: usize;
  {
    let mut p1 = pool.get_with_policy(4096, false)?;
    p1.as_allocated_slice_mut().fill(0xCD);
    ptr_val = p1.as_buf_ptr() as usize;
  } // 免清零归还 => 脏位不入清零路径

  let p2 = pool.get(4096)?;
  assert_eq!(p2.as_buf_ptr() as usize, ptr_val, "必须复用同一脏槽位");
  assert!(
    p2.as_allocated_slice().iter().all(|&b| b == 0),
    "默认 Get 必须惰性清零脏槽位"
  );

  OK
}

/// 运行时动态切换 clear_on_return 策略 (对标 libs/storage/Tsavorite/cs/src/core/Utilities/BufferPool.cs:clearOnReturn 属性)
#[test]
fn clear_on_return_dynamic_switch() -> Void {
  info!("验证租借期间动态切换归还清零策略后，复用方仍获得全零缓冲区");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;
  let ptr_val: usize;
  {
    let mut buf = pool.get(4096)?;
    assert!(buf.clear_on_return(), "池签发缓冲默认归还清零");
    buf.as_allocated_slice_mut().fill(0xEE);
    // 动态切换为免清零归还
    buf.set_clear_on_return(false);
    assert!(!buf.clear_on_return());
    ptr_val = buf.as_buf_ptr() as usize;
  }

  let reused = pool.get(4096)?;
  assert_eq!(reused.as_buf_ptr() as usize, ptr_val);
  assert!(
    reused.as_allocated_slice().iter().all(|&b| b == 0),
    "免清零策略留下的脏缓冲必须由下一次默认 Get 惰性清零"
  );

  OK
}

/// EnsureSize 就地复用容量并同步有效需求长度，不足时按需重借
#[test]
fn ensure_size_reuses_capacity_in_place() -> Void {
  info!("对标 EnsureSize：容量充足时指针不变且 required_bytes 同步为原始请求字节数");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;

  // 借一个 2 扇区 (8192B) 缓冲
  let mut buf = pool.get(8192)?;
  assert_eq!(buf.capacity(), 8192);
  assert_eq!(buf.required_len(), 8192);
  buf.as_allocated_slice_mut().fill(0x33);
  let old_ptr = buf.as_buf_ptr() as usize;

  // 请求 1 扇区 (4096B)：就地复用，指针不变，有效需求长度同步缩小
  pool.ensure_size(&mut buf, 4096)?;
  assert_eq!(buf.as_buf_ptr() as usize, old_ptr, "容量充足必须就地复用");
  assert_eq!(
    buf.required_len(),
    4096,
    "required_bytes 必须同步为原始请求"
  );
  assert_eq!(buf.capacity(), 8192);

  // 请求不足 1 扇区 (2048B)：同样就地复用
  pool.ensure_size(&mut buf, 2048)?;
  assert_eq!(buf.as_buf_ptr() as usize, old_ptr);
  assert_eq!(buf.required_len(), 2048);

  // 请求更大容量 (16384 > 8192)：旧缓冲自动归还入池，重新租借
  pool.ensure_size(&mut buf, 16384)?;
  assert!(buf.capacity() >= 16384);
  assert_eq!(buf.required_len(), 16384);

  OK
}

/// get_from_slice 拷贝数据入池化缓冲区，归还后复用同一内存
#[test]
fn get_from_slice_copies_data_and_reuses_memory() -> Void {
  info!("验证 get_from_slice 数据拷贝与归还后的内存复用、清零语义");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;

  // 空切片返回空缓冲区
  let empty = pool.get_from_slice(&[])?;
  assert_eq!(empty.len(), 0);

  // 常规数据切片：内容拷贝、容量至少一扇区
  let data = b"embedded storage wutil zero-copy test";
  let ptr_val: usize;
  {
    let buf = pool.get_from_slice(data)?;
    assert_eq!(buf.len(), data.len());
    assert_eq!(&buf[..], data);
    assert!(buf.capacity() >= DEFAULT_SECTOR_SIZE);
    ptr_val = buf.as_buf_ptr() as usize;
  }

  // 归还后再次获取，验证内存复用与全零清除
  let next = pool.get(DEFAULT_SECTOR_SIZE)?;
  assert_eq!(next.as_buf_ptr() as usize, ptr_val, "归还缓冲必须入池复用");
  assert!(
    next.as_allocated_slice().iter().all(|&b| b == 0),
    "复用缓冲必须全零"
  );

  OK
}

/// 0 字节请求返回空缓冲区，越界 class 查询安全返回 0
#[test]
fn zero_byte_request_and_unknown_class_queries_are_safe() -> Void {
  info!("验证 get(0) 空缓冲语义与 cached_len 越界 class 的防御性返回");

  let pool = BufferPool::new(DEFAULT_SECTOR_SIZE)?;

  // 越界 class 查询必须安全返回 0，绝不触发索引越界 panic
  assert_eq!(pool.cached_len(NUM_CLASSES), 0);
  assert_eq!(pool.cached_len(NUM_CLASSES + 42), 0);
  assert_eq!(pool.cached_len(usize::MAX), 0);

  // 0 字节请求返回空缓冲区 (与 C# 的刻意差异：不签发 1 扇区池化缓冲)
  let empty = pool.get(0)?;
  assert_eq!(empty.capacity(), 0);
  assert_eq!(empty.len(), 0);
  assert!(empty.is_empty());
  assert!(empty.is_ptr_aligned());
  assert_eq!(pool.reserved_bytes(), 0, "空缓冲不占预算许可");

  // 空缓冲克隆保持空且对齐元数据完好
  let cloned = empty.clone();
  assert_eq!(cloned.capacity(), 0);
  assert!(cloned.is_ptr_aligned());
  drop((empty, cloned));
  assert_eq!(pool.reserved_bytes(), 0);

  OK
}
