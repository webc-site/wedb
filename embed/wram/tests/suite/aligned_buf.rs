//! AlignedBuf 扇区对齐缓冲区本体测试
//!
//! 对标 C#：libs/storage/Tsavorite/cs/src/core/Allocator/SectorAlignedBufferPool.cs 中
//! `SectorAlignedMemory` 的缓冲区语义（对齐指针、容量、长度、克隆独立性）；
//! 对齐断言对标 libs/storage/Tsavorite/cs/test/SectorAlignedBufferPoolTests.cs
//! 的 `GetCapacityCoversRequestAcrossSizes`。

use std::{sync::Arc, thread};

use aok::{OK, Void};
use compio_buf::{IoBuf, IoBufMut, SetLen};
use log::info;
use wram::{AlignedBuf, DEFAULT_SECTOR_SIZE, Error, MIN_SECTOR_SIZE, SectorRange, is_aligned};

/// 对齐指针必须满足指定扇区大小的对齐要求
#[test]
fn aligned_pointer_matches_expected_alignment() -> Void {
  info!("验证多种扇区大小 × 多种容量下分配指针恒满足扇区对齐");

  for sector_size in [MIN_SECTOR_SIZE, DEFAULT_SECTOR_SIZE, 8192] {
    for size in [1, 511, 512, 1000, 4096, 7777] {
      let buf = AlignedBuf::new(size, sector_size)?;
      let ptr = buf.as_buf_ptr() as usize;
      assert_eq!(
        ptr % sector_size,
        0,
        "指针 {ptr} 必须按扇区大小 {sector_size} 对齐"
      );
      assert!(buf.is_ptr_aligned());
      assert!(buf.is_aligned_to(sector_size));

      // from_slice 拷贝创建路径同样必须对齐
      let copied = AlignedBuf::from_slice(&[0u8; 64], sector_size)?;
      assert_eq!(
        copied.as_buf_ptr() as usize % sector_size,
        0,
        "from_slice 指针必须按 {sector_size} 对齐"
      );
    }
  }

  OK
}

/// 非法对齐必须报错，零容量缓冲区可用且元数据完好
#[test]
fn new_rejects_invalid_alignment_and_supports_zero_capacity() -> Void {
  info!("验证 AlignedBuf::new/from_slice/zeroed 拒绝非 2 的幂或 <512 的对齐");

  assert!(AlignedBuf::new(1024, 256).is_err());
  assert!(AlignedBuf::new(1024, 300).is_err());
  assert!(AlignedBuf::from_slice(b"data", 100).is_err());
  assert!(AlignedBuf::zeroed(64, 100).is_err());

  // 零容量缓冲区：不分配内存，仅携带对齐元数据
  let mut empty = AlignedBuf::new(0, 4096)?;
  assert_eq!(empty.len(), 0);
  assert_eq!(empty.capacity(), 0);
  assert!(empty.is_empty());
  assert!(empty.is_ptr_aligned());
  assert_eq!(empty.align(), 4096);
  assert_eq!(empty.as_allocated_slice().len(), 0);
  assert_eq!(empty.as_allocated_slice_mut().len(), 0);

  OK
}

/// set_len/clear 长度语义与容量上界校验
#[test]
fn set_len_clear_and_len_bound_checks() -> Void {
  info!("验证 set_len 推进长度、clear 归零长度、超容量报 SetLenExceeded");

  let mut buf = AlignedBuf::with_sector_size(DEFAULT_SECTOR_SIZE)?;
  assert_eq!(buf.capacity(), DEFAULT_SECTOR_SIZE);
  assert_eq!(buf.len(), 0);
  assert!(buf.is_ptr_aligned());
  assert_eq!((buf.as_buf_ptr() as usize) % DEFAULT_SECTOR_SIZE, 0);

  // set_len 推进逻辑长度
  buf.set_len(1024)?;
  assert_eq!(buf.len(), 1024);
  assert!(!buf.is_empty());

  // clear 仅重置长度，不释放内存
  buf.clear();
  assert_eq!(buf.len(), 0);
  assert!(buf.is_empty());

  // 超出容量必须精确报错
  match buf.set_len(5000) {
    Err(Error::SetLenExceeded { len, capacity }) => {
      assert_eq!(len, 5000);
      assert_eq!(capacity, DEFAULT_SECTOR_SIZE);
    }
    _ => panic!("set_len 超容量必须返回 SetLenExceeded"),
  }

  // as_allocated_slice_mut 写入未初始化区域后按长度读出
  let alloc_slice = buf.as_allocated_slice_mut();
  alloc_slice[0..4].copy_from_slice(b"TEST");
  buf.set_len(4)?;
  assert_eq!(&buf[..], b"TEST");

  OK
}

/// zeroed 缓冲区长度恒等于容量且内容全零
#[test]
fn zeroed_buffer_len_equals_capacity_and_all_zero() -> Void {
  info!("验证 zeroed 系列构造 len == capacity 且逐字节为 0");

  let zbuf = AlignedBuf::zeroed_with_sector_size(DEFAULT_SECTOR_SIZE)?;
  assert_eq!(zbuf.len(), DEFAULT_SECTOR_SIZE);
  assert_eq!(zbuf.capacity(), DEFAULT_SECTOR_SIZE);
  assert!(zbuf.iter().all(|&b| b == 0));

  let z512 = AlignedBuf::zeroed(8192, MIN_SECTOR_SIZE)?;
  assert_eq!(z512.len(), 8192);
  assert_eq!(z512.align(), MIN_SECTOR_SIZE);
  assert!(z512.as_allocated_slice().iter().all(|&b| b == 0));

  OK
}

/// from_slice 拷贝创建、Deref/DerefMut 读写与克隆独立性
#[test]
fn from_slice_deref_mut_and_clone_independence() -> Void {
  info!("验证 from_slice 数据拷贝、DerefMut 原地修改与深拷贝克隆互不影响");

  let sample = b"hello wram storage engine";
  let mut buf = AlignedBuf::from_slice(sample, DEFAULT_SECTOR_SIZE)?;
  assert_eq!(buf.len(), sample.len());
  assert_eq!(buf.capacity(), sample.len());
  assert!(buf.is_ptr_aligned());
  assert_eq!(&buf[..], sample);
  assert_eq!(buf, sample.as_slice());

  // DerefMut 原地修改
  buf[0] = b'H';
  assert_eq!(buf[0], b'H');
  assert_eq!(&buf[..5], b"Hello");

  // 克隆为独立深拷贝：修改克隆体不影响原缓冲区
  let mut sec_buf = AlignedBuf::with_sector_size(DEFAULT_SECTOR_SIZE)?;
  sec_buf.as_allocated_slice_mut()[0..4].copy_from_slice(b"TEST");
  sec_buf.set_len(4)?;
  let mut cloned = sec_buf.clone();
  assert_eq!(cloned, sec_buf);
  cloned[0] = b'B';
  assert_eq!(&cloned[..], b"BEST");
  assert_eq!(&sec_buf[..], b"TEST");

  // 克隆保持容量与内容
  let mut zbuf = AlignedBuf::zeroed(DEFAULT_SECTOR_SIZE, DEFAULT_SECTOR_SIZE)?;
  zbuf[0] = 0xAA;
  zbuf[DEFAULT_SECTOR_SIZE - 1] = 0xBB;
  let cloned = zbuf.clone();
  assert_eq!(cloned.len(), DEFAULT_SECTOR_SIZE);
  assert_eq!(cloned.capacity(), DEFAULT_SECTOR_SIZE);
  assert_eq!(cloned[0], 0xAA);
  assert_eq!(cloned[DEFAULT_SECTOR_SIZE - 1], 0xBB);
  assert_eq!(cloned, zbuf);

  OK
}

/// compio_buf 的 IoBuf/IoBufMut/SetLen trait 契约
#[test]
fn io_buf_and_io_buf_mut_trait_contract() -> Void {
  info!("验证 IoBuf/IoBufMut 视图与 I/O 完成后 advance 推进长度");

  let mut buf = AlignedBuf::with_sector_size(DEFAULT_SECTOR_SIZE)?;
  assert_eq!(IoBuf::buf_len(&buf), 0);
  assert_eq!(buf.buf_capacity(), DEFAULT_SECTOR_SIZE);
  assert!(IoBuf::is_empty(&buf));

  // 模拟异步 I/O 写入底层未初始化缓冲
  let data = b"async disk data";
  let uninit = buf.as_uninit();
  assert_eq!(uninit.len(), DEFAULT_SECTOR_SIZE);
  for (i, &b) in data.iter().enumerate() {
    uninit[i].write(b);
  }

  // 模拟 compio 在 I/O 完成后推进已初始化长度
  unsafe { buf.advance(data.len()) };

  assert_eq!(buf.len(), data.len());
  assert_eq!(buf.as_init(), data);
  assert_eq!(buf.buf_ptr(), buf.as_buf_ptr());

  // Reader 适配器按已初始化数据消费
  let reader = buf.into_reader();
  assert_eq!(reader.as_remaining(), data);

  OK
}

/// 多线程共享只读缓冲区验证 Send/Sync 一致性
#[test]
fn concurrent_readers_share_immutable_buffer() -> Void {
  info!("验证 Arc<AlignedBuf> 跨线程并发只读观察一致");

  let sample_data = vec![42u8; 8192];
  let buf = AlignedBuf::from_slice(&sample_data, DEFAULT_SECTOR_SIZE)?;
  let shared = Arc::new(buf);

  let mut handles = Vec::new();
  for _ in 0..8 {
    let s = shared.clone();
    handles.push(thread::spawn(move || {
      assert_eq!(s.len(), 8192);
      assert_eq!(s.align(), DEFAULT_SECTOR_SIZE);
      assert!(s.iter().all(|&b| b == 42));
      assert!(s.is_ptr_aligned());
    }));
  }
  for h in handles {
    h.join().expect("读线程必须成功");
  }

  OK
}

/// 端到端扇区对齐读路径：SectorRange 换算 + 对齐缓冲 + 逻辑切片提取
#[test]
fn sector_aligned_read_path_end_to_end() -> Void {
  info!("模拟从偏移 4196 读 100 字节的完整扇区对齐读路径");

  let offset = 4196u64;
  let len = 100usize;
  let sector_size = DEFAULT_SECTOR_SIZE;

  // 逻辑请求换算为物理扇区范围
  let range = SectorRange::calculate(offset, len, sector_size)?;
  assert_eq!(range.aligned_offset, 4096);
  assert_eq!(range.aligned_len, 4096);
  assert_eq!(range.internal_offset, 100);
  assert!(is_aligned(range.aligned_offset, sector_size as u64));

  // 分配对齐缓冲并模拟从存储介质读入整扇区数据
  let mut io_buf = AlignedBuf::new(range.aligned_len, sector_size)?;
  assert_eq!(io_buf.capacity(), 4096);
  for (i, slot) in io_buf.as_uninit().iter_mut().enumerate() {
    slot.write((i % 256) as u8);
  }
  unsafe { io_buf.advance(range.aligned_len) };

  // 按逻辑切片提取用户请求的数据
  let user_data = &io_buf[range.sub_range(len)];
  assert_eq!(user_data.len(), 100);
  assert_eq!(user_data[0], 100);
  assert_eq!(user_data[99], 199);

  OK
}
