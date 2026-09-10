//! 对齐与参数校验防御：未对齐偏移/长度/缓冲区拒绝，非法段尺寸与扇区尺寸校验。
//!
//! 对标 C# 测试文件：
//! `/Users/z/git/db/garnet/libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs`
//! （方法 `NativeStorageDevice_UnalignedOffset_ReadAsync_Throws`、
//! `NativeStorageDevice_UnalignedLength_WriteAsync_Throws`、
//! `NativeStorageDevice_UnalignedBuffer_WriteAsync_Throws`、
//! `NativeStorageDevice_NonPowerOfTwoSegmentSize_Throws`、
//! `NativeStorageDevice_SegmentSizeSmallerThanSector_Throws`、
//! `NativeStorageDevice_ZeroSegmentSize_Throws`、
//! `NativeStorageDevice_SectorSize_IsPowerOfTwoAtLeast512`）。
//! 写路径未对齐偏移拒绝为 Rust 补齐的 C# `ThrowIfMisaligned` 防御（C# 未单列测试）。

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wbase::{AlignedBuf, BufferPool};
use wdev::{Device, Error, SegmentedDevice};

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:NativeStorageDevice_UnalignedOffset_ReadAsync_Throws：
/// 偏移量不是扇区大小整数倍的读取必须同步拒绝，并指明未对齐输入。
#[test]
fn unaligned_offset_read_async_throws() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = SegmentedDevice::new(dir.path().join("unaligned_read.log"), None, 4096)?;

    let rbuf = AlignedBuf::new(4 * 4096, 4096)?;
    let (res, rbuf) = device.read_aligned(4095, rbuf).await;
    drop(rbuf);
    match res {
      Err(Error::UnalignedOffset { offset, align }) => {
        assert_eq!(offset, 4095);
        assert_eq!(align, 4096);
      }
      other => panic!("预期 UnalignedOffset，实际为: {other:?}"),
    }

    info!("未对齐偏移读取拒绝校验通过 (UnalignedOffset_ReadAsync_Throws)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// Rust 补齐 C# `ThrowIfMisaligned` 写路径防御：偏移量未按扇区对齐的写入必须拒绝。
#[test]
fn unaligned_offset_write_async_throws() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = SegmentedDevice::new(dir.path().join("unaligned_write.log"), None, 4096)?;

    let valid = vec![0xAAu8; 4096];
    for bad_offset in [1u64, 511, 4095, 4097, 7000] {
      let wbuf = AlignedBuf::from_slice(&valid, 4096)?;
      let (res, _) = device.write_aligned(bad_offset, wbuf).await;
      match res {
        Err(Error::UnalignedOffset { offset, align }) => {
          assert_eq!(offset, bad_offset);
          assert_eq!(align, 4096);
        }
        other => panic!("偏移 {bad_offset} 预期 UnalignedOffset，实际为: {other:?}"),
      }
    }

    info!("未对齐偏移写入拒绝校验通过 (ThrowIfMisaligned 写路径)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:NativeStorageDevice_UnalignedLength_WriteAsync_Throws：
/// 长度不是扇区大小整数倍的写入必须拒绝（4097 字节，非 4096 整数倍）。
#[test]
fn unaligned_length_write_async_throws() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = SegmentedDevice::new(dir.path().join("unaligned_len.log"), None, 4096)?;

    let bad_len = vec![0xBBu8; 4096 + 1];
    let wbuf = AlignedBuf::from_slice(&bad_len, 4096)?;
    let (res, _) = device.write_aligned(0, wbuf).await;
    match res {
      Err(Error::UnalignedLen { len, align }) => {
        assert_eq!(len, 4097);
        assert_eq!(align, 4096);
      }
      other => panic!("预期 UnalignedLen，实际为: {other:?}"),
    }

    info!("未对齐长度写入拒绝校验通过 (UnalignedLength_WriteAsync_Throws)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:NativeStorageDevice_UnalignedBuffer_WriteAsync_Throws：
/// 缓冲区内存地址未对齐到扇区边界的读写必须拒绝。
/// 分配器不保证产出未对齐地址，故循环尝试 100 次，未遇到则视为环境满足对齐。
#[test]
fn unaligned_buffer_write_async_throws() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = SegmentedDevice::new(dir.path().join("unaligned_buf.log"), None, 4096)?;

    for _ in 0..100 {
      // 以 512 对齐分配，寻找物理地址未对齐到 4096 的缓冲区
      let candidate = AlignedBuf::new(4096, 512)?;
      if !candidate.is_aligned_to(4096) {
        let ptr_val = candidate.as_ptr() as usize;

        let (res, _) = device.write_aligned(0, candidate.clone()).await;
        match res {
          Err(Error::UnalignedBuffer { ptr, align }) => {
            assert_eq!(ptr, ptr_val);
            assert_eq!(align, 4096);
          }
          other => panic!("预期 UnalignedBuffer 写拒绝，实际为: {other:?}"),
        }

        let (res, _) = device.read_aligned(0, candidate).await;
        match res {
          Err(Error::UnalignedBuffer { ptr, align }) => {
            assert_eq!(ptr, ptr_val);
            assert_eq!(align, 4096);
          }
          other => panic!("预期 UnalignedBuffer 读拒绝，实际为: {other:?}"),
        }

        info!("未对齐缓冲区地址读写拒绝校验通过 (UnalignedBuffer_WriteAsync_Throws)");
        return aok::Result::<()>::Ok(());
      }
    }

    info!("100 次分配均为 4096 对齐，环境天然满足对齐，跳过未对齐缓冲区断言");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:NativeStorageDevice_NonPowerOfTwoSegmentSize_Throws：
/// 非 2 的幂段尺寸（3MiB）必须在初始化时拒绝。
#[test]
fn non_power_of_two_segment_size_throws() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    assert!(matches!(
      SegmentedDevice::new(dir.path().join("seg_3mib.log"), Some(3 * 1024 * 1024), 4096),
      Err(Error::InvalidSegmentSize(_))
    ));

    info!("非 2 的幂段尺寸拒绝校验通过 (NonPowerOfTwoSegmentSize_Throws)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:NativeStorageDevice_SegmentSizeSmallerThanSector_Throws：
/// 小于扇区尺寸的段尺寸会造成上层偏移算术错乱，必须拒绝。
#[test]
fn segment_size_smaller_than_sector_throws() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    // 512 段尺寸 < 4096 扇区尺寸
    assert!(matches!(
      SegmentedDevice::new(dir.path().join("seg_small1.log"), Some(512), 4096),
      Err(Error::InvalidSegmentSize(512))
    ));
    // 256 段尺寸 < 512 扇区尺寸（C# 用例取值）
    assert!(matches!(
      SegmentedDevice::new(dir.path().join("seg_small2.log"), Some(256), 512),
      Err(Error::InvalidSegmentSize(256))
    ));

    info!("小于扇区尺寸的段尺寸拒绝校验通过 (SegmentSizeSmallerThanSector_Throws)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:NativeStorageDevice_ZeroSegmentSize_Throws：0 段尺寸必须拒绝
/// （Rust 中 None 表示单文件无界模式，与 0 语义不同）。
#[test]
fn zero_segment_size_throws() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    assert!(matches!(
      SegmentedDevice::new(dir.path().join("seg_zero.log"), Some(0), 4096),
      Err(Error::InvalidSegmentSize(0))
    ));

    info!("0 段尺寸拒绝校验通过 (ZeroSegmentSize_Throws)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// Rust 补齐防御（C# RandomAccessLocalStorageDevice 无此校验）：注入共享缓冲池的
/// 扇区与设备扇区不一致时必须构造期拒绝——错配会使池化缓冲区的对齐口径与设备
/// Direct I/O 要求错位，运行期才以 EINVAL 暴露。
#[test]
fn mismatched_pool_sector_size_is_rejected() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let pool_512 = BufferPool::new(512)?;

    // 池 512 vs 设备 4096：拒绝
    assert!(
      matches!(
        SegmentedDevice::with_pool(
          dir.path().join("pool_mismatch.log"),
          None,
          4096,
          pool_512.clone()
        ),
        Err(Error::PoolSectorMismatch {
          pool: 512,
          device: 4096
        })
      ),
      "池/设备扇区错配必须构造期拒绝为 PoolSectorMismatch"
    );

    // 一致时正常创建
    let device =
      SegmentedDevice::with_pool(dir.path().join("pool_match.log"), None, 512, pool_512)?;
    assert_eq!(device.sector_size(), 512);

    info!("池/设备扇区一致性校验通过 (PoolSectorMismatch)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:NativeStorageDevice_SectorSize_IsPowerOfTwoAtLeast512：
/// 扇区尺寸必须为 2 的幂且不小于 512；非法值（256/500/3000）必须拒绝。
#[test]
fn sector_size_is_power_of_two_at_least_512() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;

    // 合法值：512 与 4096 均可用
    for sector in [512usize, 4096] {
      let device =
        SegmentedDevice::new(dir.path().join(format!("sec_{sector}.log")), None, sector)?;
      let s = device.sector_size();
      assert!(s >= 512, "扇区尺寸必须不小于 512");
      assert!(s.is_power_of_two(), "扇区尺寸必须为 2 的幂，实际为 {s}");
    }

    // 非法值：小于下界或非 2 的幂
    for bad in [256usize, 500, 3000] {
      assert!(
        matches!(
          SegmentedDevice::new(dir.path().join(format!("sec_bad_{bad}.log")), None, bad),
          Err(Error::InvalidSectorSize { .. })
        ),
        "扇区尺寸 {bad} 必须被拒绝"
      );
    }

    info!("扇区尺寸 2 的幂与 512 下界校验通过 (SectorSize_IsPowerOfTwoAtLeast512)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
