//! 并发读写语义：并行写、并行读、混合读写、突发流量与压力写，冷段并发竞争对抗，
//! 以及多 OS 线程多 Runtime 共享设备。
//!
//! 对标 C# 测试文件：
//! `/Users/z/git/db/garnet/libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs`
//! （方法 `IDevice_Parallel_32ConcurrentWrites`、`IDevice_Parallel_64ConcurrentReads`、
//! `IDevice_Parallel_MixedReadsAndWrites`、`IDevice_Parallel_BurstyTraffic`、
//! `IDevice_Parallel_StressBurst_100Writes`、`Native_HighConcurrency_ManyThreads_NoHang`）。
//! 冷段竞争用例为 Rust 扩展对抗语义（C# 无对应测试）。

use std::{sync::Arc, thread};

use aok::{OK, Void};
use compio::runtime::{Runtime, spawn};
use log::info;
use tempfile::tempdir;
use wdev::{Device, Error, SegmentedDevice};
use wutil::AlignedBuf;

use crate::support::make_pattern_data;

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:IDevice_Parallel_32ConcurrentWrites：32 个并发任务同时写入互不重叠的
/// 8KB 块（模式 `(j ^ (id * 17)) & 0xFF`），全部完成后逐块回读校验。
/// C# 段尺寸 64MiB，此处等比缩小为 1MiB，全部写入仍落在段 0。
#[test]
fn idevice_parallel_32_concurrent_writes() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::segmented(
      dir.path().join("p32w.log"),
      1 << 20,
    )?);

    const N: usize = 32;
    const BLOCK: usize = 8 * 1024;

    // 32 个并发写入：第 id 块位于偏移 id * BLOCK，模式 (j ^ (id * 17)) & 0xFF
    let mut handles = Vec::with_capacity(N);
    for id in 0..N {
      let dev = Arc::clone(&device);
      let offset = (id * BLOCK) as u64;
      let handle = spawn(async move {
        let pattern: Vec<u8> = (0..BLOCK).map(|j| ((j ^ (id * 17)) & 0xFF) as u8).collect();
        let wbuf = AlignedBuf::from_slice(&pattern, 4096)?;
        let (res, _) = dev.write_aligned(offset, wbuf).await;
        assert_eq!(res?, BLOCK);
        aok::Result::<()>::Ok(())
      });
      handles.push(handle);
    }
    for handle in handles {
      handle.await.unwrap()?;
    }

    // 逐块回读并严格校验全部字节
    for id in 0..N {
      let expected: Vec<u8> = (0..BLOCK).map(|j| ((j ^ (id * 17)) & 0xFF) as u8).collect();
      let check = AlignedBuf::new(BLOCK, 4096)?;
      let (res, check) = device.read_aligned((id * BLOCK) as u64, check).await;
      assert_eq!(res?, BLOCK);
      assert_eq!(
        check.as_slice(),
        &expected[..],
        "并发写回读块 {id} 内容不匹配"
      );
    }

    info!("32 并发互不重叠写入与逐块回读校验通过 (IDevice_Parallel_32ConcurrentWrites)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:IDevice_Parallel_64ConcurrentReads：预写 64 个 4KB 块（每块独立模式
/// `(blk * 31 + off) & 0xFF`），随后 64 个并发读取并逐块校验内容。
#[test]
fn idevice_parallel_64_concurrent_reads() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::segmented(
      dir.path().join("p64r.log"),
      1 << 20,
    )?);

    const N: usize = 64;
    const BLOCK: usize = 4 * 1024;

    // 预写覆盖全部 N 块的基线数据，每块独立模式
    let pre: Vec<u8> = (0..N * BLOCK)
      .map(|j| (((j / BLOCK) * 31 + j % BLOCK) & 0xFF) as u8)
      .collect();
    let pre_buf = AlignedBuf::from_slice(&pre, 4096)?;
    let (res, _) = device.write_aligned(0, pre_buf).await;
    assert_eq!(res?, N * BLOCK);

    // 64 个并发读取
    let mut handles = Vec::with_capacity(N);
    for i in 0..N {
      let dev = Arc::clone(&device);
      let offset = (i * BLOCK) as u64;
      let handle = spawn(async move {
        let rbuf = AlignedBuf::new(BLOCK, 4096)?;
        let (res, rbuf) = dev.read_aligned(offset, rbuf).await;
        assert_eq!(res?, BLOCK);
        aok::Result::<(usize, AlignedBuf)>::Ok((i, rbuf))
      });
      handles.push(handle);
    }
    for handle in handles {
      let (blk, buf) = handle.await.unwrap()?;
      let expected = &pre[blk * BLOCK..(blk + 1) * BLOCK];
      assert_eq!(buf.as_slice(), expected, "并发读块 {blk} 内容不匹配");
    }

    info!("64 并发读取逐块校验通过 (IDevice_Parallel_64ConcurrentReads)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:IDevice_Parallel_MixedReadsAndWrites：同时向互不重叠区域发起
/// 16 个读取与 16 个写入，校验读写互不干扰；并验证截断后
/// start_segment 单调推进（对标 libs/storage/Tsavorite/cs/src/core/Device/IDevice.cs:StartSegment 属性语义）。
#[test]
fn idevice_parallel_mixed_reads_and_writes() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = Arc::new(SegmentedDevice::segmented(
      dir.path().join("mixed_rw.log"),
      seg_size,
    )?);

    assert_eq!(device.start_segment(), 0);
    assert_eq!(Device::start_segment(&*device), 0);

    const N: usize = 16;
    const BLOCK: usize = 4096;
    let read_base: u64 = 0;
    let write_base: u64 = 256 * 1024; // 与读取区域不重叠的写入基址

    // 预写读取区域的基线数据，模式 (j * 3) & 0xFF
    let pre_size = N * BLOCK;
    let pre = make_pattern_data(pre_size, 3, 0);
    let pre_buf = AlignedBuf::from_slice(&pre, 4096)?;
    let (res, _) = device.write_aligned(read_base, pre_buf).await;
    assert_eq!(res?, pre_size);

    // 同时提交 16 个读取与 16 个写入（两组句柄分列，join 前互不等待，保证真实并发）
    let mut read_handles = Vec::with_capacity(N);
    let mut write_handles = Vec::with_capacity(N);
    for i in 0..N {
      let dev = Arc::clone(&device);
      let offset = read_base + (i * BLOCK) as u64;
      read_handles.push(spawn(async move {
        let rbuf = AlignedBuf::new(BLOCK, 4096)?;
        let (res, rbuf) = dev.read_aligned(offset, rbuf).await;
        assert_eq!(res?, BLOCK);
        aok::Result::<(usize, AlignedBuf)>::Ok((i, rbuf))
      }));

      let dev = Arc::clone(&device);
      let offset = write_base + (i * BLOCK) as u64;
      write_handles.push(spawn(async move {
        let wdata: Vec<u8> = (0..BLOCK).map(|j| ((j + i) & 0xFF) as u8).collect();
        let wbuf = AlignedBuf::from_slice(&wdata, 4096)?;
        let (res, _) = dev.write_aligned(offset, wbuf).await;
        assert_eq!(res?, BLOCK);
        aok::Result::<()>::Ok(())
      }));
    }
    for handle in read_handles {
      let (idx, buf) = handle.await.unwrap()?;
      for (j, &byte) in buf.as_slice().iter().enumerate() {
        let expected = (((idx * BLOCK + j) * 3) & 0xFF) as u8;
        assert_eq!(byte, expected, "并发读取块 {idx} 偏移 {j} 内容不匹配");
      }
    }
    for handle in write_handles {
      handle.await.unwrap()?;
    }

    // 逐块校验并发写入落盘正确
    for i in 0..N {
      let expected: Vec<u8> = (0..BLOCK).map(|j| ((j + i) & 0xFF) as u8).collect();
      let check = AlignedBuf::new(BLOCK, 4096)?;
      let (res, check) = device
        .read_aligned(write_base + (i * BLOCK) as u64, check)
        .await;
      assert_eq!(res?, BLOCK);
      assert_eq!(check.as_slice(), &expected[..], "写入块 {i} 内容不匹配");
    }

    // 截断后 start_segment 单调推进至 3
    device.truncate_until_segment(3).await?;
    assert_eq!(device.start_segment(), 3);
    assert_eq!(Device::start_segment(&*device), 3);

    info!("并发混合读写与 start_segment 跟踪通过 (IDevice_Parallel_MixedReadsAndWrites)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:IDevice_Parallel_BurstyTraffic：10 轮突发，每轮并发提交 10 个互不重叠的
/// 4KB 写入（模式 `(j + globalIdx) & 0xFF`），轮末全量等待后再进下一轮。
#[test]
fn idevice_parallel_bursty_traffic() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::segmented(
      dir.path().join("bursty.log"),
      1 << 20,
    )?);

    const BURSTS: usize = 10;
    const PER_BURST: usize = 10;
    const BLOCK: usize = 4 * 1024;

    for burst in 0..BURSTS {
      let mut handles = Vec::with_capacity(PER_BURST);
      for i in 0..PER_BURST {
        let dev = Arc::clone(&device);
        let global = burst * PER_BURST + i;
        let offset = (global * BLOCK) as u64;
        let handle = spawn(async move {
          let pattern: Vec<u8> = (0..BLOCK).map(|j| ((j + global) & 0xFF) as u8).collect();
          let wbuf = AlignedBuf::from_slice(&pattern, 4096)?;
          let (res, _) = dev.write_aligned(offset, wbuf).await;
          assert_eq!(res?, BLOCK);
          aok::Result::<()>::Ok(())
        });
        handles.push(handle);
      }
      // 轮末同步：等待本轮全部写入完成
      for handle in handles {
        handle.await.unwrap()?;
      }
    }

    info!("10 轮突发式并发写入全部成功 (IDevice_Parallel_BurstyTraffic)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:IDevice_Parallel_StressBurst_100Writes：100 个并发 4KB 写入
/// （模式 `(j * 5 + id) & 0xFF`），每 5 块抽读校验全部字节，并断言
/// end_segment 精确覆盖末字节所在段。
#[test]
fn idevice_parallel_stress_burst_100_writes() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = Arc::new(SegmentedDevice::segmented(
      dir.path().join("stress100.log"),
      seg_size,
    )?);

    const N: usize = 100;
    const BLOCK: usize = 4 * 1024;

    // 100 个并发写入
    let mut handles = Vec::with_capacity(N);
    for id in 0..N {
      let dev = Arc::clone(&device);
      let offset = (id * BLOCK) as u64;
      let handle = spawn(async move {
        let pattern: Vec<u8> = (0..BLOCK).map(|j| ((j * 5 + id) & 0xFF) as u8).collect();
        let wbuf = AlignedBuf::from_slice(&pattern, 4096)?;
        let (res, _) = dev.write_aligned(offset, wbuf).await;
        assert_eq!(res?, BLOCK);
        aok::Result::<()>::Ok(())
      });
      handles.push(handle);
    }
    for handle in handles {
      handle.await.unwrap()?;
    }

    // 每 5 块抽读并严格校验全部字节内容
    for id in (0..N).step_by(5) {
      let expected: Vec<u8> = (0..BLOCK).map(|j| ((j * 5 + id) & 0xFF) as u8).collect();
      let check = AlignedBuf::new(BLOCK, 4096)?;
      let (res, check) = device.read_aligned((id * BLOCK) as u64, check).await;
      assert_eq!(res?, BLOCK);
      assert_eq!(
        check.as_slice(),
        &expected[..],
        "压力写抽检块 {id} 内容不匹配"
      );
    }

    // 末字节所在段即 end_segment（100 * 4KB = 400KB，落在段 6）
    assert_eq!(
      device.end_segment(),
      Some(((N * BLOCK - 1) as u64 / seg_size) as u32)
    );

    info!("100 并发压力写与抽检校验通过 (IDevice_Parallel_StressBurst_100Writes)");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// Rust 扩展对抗语义（C# 无对应测试）：32 个并发任务在冷启动瞬间同时冲击同一
/// 未打开段（段 5），验证句柄缓存无死锁且收敛为单一句柄、数据无撕裂；
/// 并发截断后已删段绝不被重新插入缓存或重现磁盘（防幽灵段复活）。
#[test]
fn concurrent_cold_open_race_without_zombie_revival() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let seg_size: u64 = 64 * 1024;
    let device = Arc::new(SegmentedDevice::segmented(
      dir.path().join("cold_race.log"),
      seg_size,
    )?);

    const TASKS: usize = 32;
    const SECTOR: usize = 4096;
    let target_seg: u32 = 5;
    let seg_base = (target_seg as u64) * seg_size;

    // 1. 32 个并发任务同时写入段 5 内不同扇区（循环复用段内 16 个扇区），
    //    此时 files 缓存为空，考验 get_or_open_file 的竞争安全性
    let mut handles = Vec::with_capacity(TASKS);
    for i in 0..TASKS {
      let dev = Arc::clone(&device);
      let offset = seg_base + ((i % 16) * SECTOR) as u64;
      let handle = spawn(async move {
        let pattern = ((i * 7 + 11) & 0xFF) as u8;
        let buf_data = vec![pattern; SECTOR];
        let wbuf = AlignedBuf::from_slice(&buf_data, 4096)?;
        let (res, _) = dev.write_aligned(offset, wbuf).await;
        assert_eq!(res?, SECTOR);
        aok::Result::<()>::Ok(())
      });
      handles.push(handle);
    }
    for handle in handles {
      handle.await.unwrap()?;
    }

    // 高并发竞争后缓存必须收敛为单一句柄
    assert_eq!(
      device.cached_handle_count(),
      1,
      "同一冷段竞争后应只保留一个段句柄"
    );
    assert!(device.is_segment_cached(target_seg));

    // 2. 32 个并发任务回读段 5：槽位 s 同时被任务 s 与 s+16 竞写，
    //    整扇区字节必须同属单一候选模式（无撕裂混写）
    let mut handles = Vec::with_capacity(TASKS);
    for i in 0..TASKS {
      let dev = Arc::clone(&device);
      let offset = seg_base + ((i % 16) * SECTOR) as u64;
      let handle = spawn(async move {
        let rbuf = AlignedBuf::new(SECTOR, 4096)?;
        let (res, rbuf) = dev.read_aligned(offset, rbuf).await;
        assert_eq!(res?, SECTOR);
        let slot = i % 16;
        let cand_a = ((slot * 7 + 11) & 0xFF) as u8;
        let cand_b = (((slot + 16) * 7 + 11) & 0xFF) as u8;
        if cand_a != cand_b {
          let bytes = rbuf.as_slice();
          assert!(
            bytes.iter().all(|&b| b == cand_a) || bytes.iter().all(|&b| b == cand_b),
            "槽位 {slot} 读到撕裂数据（非整扇区单一模式）"
          );
        }
        aok::Result::<()>::Ok(())
      });
      handles.push(handle);
    }
    for handle in handles {
      handle.await.unwrap()?;
    }

    // 3. 并发截断防幽灵复活：写入段 0..4 基线后截断至段 4，
    //    16 个并发任务对已删段 0..3 的读写必须全部被拦截为 SegmentNotFound
    for seg_id in 0..4u32 {
      let buf = AlignedBuf::from_slice(&[0x77u8; 4096], 4096)?;
      let (res, _) = device.write_aligned((seg_id as u64) * seg_size, buf).await;
      assert_eq!(res?, 4096);
    }
    device.truncate_until_segment(4).await?;
    assert_eq!(device.start_segment(), 4);

    let mut handles = Vec::new();
    for i in 0..16 {
      let dev = Arc::clone(&device);
      let bad_seg = (i % 4) as u32;
      let offset = (bad_seg as u64) * seg_size;
      let handle = spawn(async move {
        let wbuf = AlignedBuf::from_slice(&[0x99u8; 4096], 4096)?;
        let (res, _) = dev.write_aligned(offset, wbuf).await;
        assert!(
          matches!(res, Err(Error::SegmentNotFound(id)) if id == bad_seg),
          "已截断段写入必须返回 SegmentNotFound，实际为 {res:?}"
        );

        let rbuf = AlignedBuf::new(4096, 4096)?;
        let (res, _) = dev.read_aligned(offset, rbuf).await;
        assert!(
          matches!(res, Err(Error::SegmentNotFound(id)) if id == bad_seg),
          "已截断段读取必须返回 SegmentNotFound，实际为 {res:?}"
        );
        aok::Result::<()>::Ok(())
      });
      handles.push(handle);
    }
    for handle in handles {
      handle.await.unwrap()?;
    }

    // 已截断段不得复活于缓存，磁盘无幽灵文件残留
    for seg_id in 0..4u32 {
      assert!(
        !device.is_segment_cached(seg_id),
        "已截断段严禁复活在缓存中"
      );
      assert!(
        !device.segment_path(seg_id).exists(),
        "已截断段严禁重现在磁盘上"
      );
      assert_eq!(device.get_file_size(seg_id)?, 0);
    }

    info!("冷段并发竞争收敛与防幽灵复活对抗通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 libs/storage/Tsavorite/cs/test/test.hlog/DeviceTests.cs:Native_HighConcurrency_ManyThreads_NoHang：多个 OS 线程各自运行独立
/// compio Runtime 共享同一设备实例。写入句柄按 (线程ID, 段号) 键控（thread-per-core），
/// sync 为全局屏障：任一线程调用即覆盖全部线程的在表句柄。本用例验证跨线程无句柄
/// 串扰、无死锁，各线程独占段的数据写读一致，且任一线程 sync 后全部段持久化可见。
#[test]
fn high_concurrency_many_threads_no_hang() -> Void {
  const THREADS: usize = 4;
  const SEGS_PER_THREAD: u32 = 4;
  const SECTOR: usize = 4096;

  let dir = tempdir()?;
  let seg_size: u64 = 64 * 1024;
  let device = Arc::new(SegmentedDevice::segmented(
    dir.path().join("mt.log"),
    seg_size,
  )?);

  // 各 OS 线程独立 Runtime：向本线程独占的段写入并即时回读校验
  let mut joins = Vec::with_capacity(THREADS);
  for t in 0..THREADS {
    let dev = Arc::clone(&device);
    joins.push(thread::spawn(move || -> Result<(), Error> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        for i in 0..SEGS_PER_THREAD {
          let seg = (t as u32) * SEGS_PER_THREAD + i;
          let offset = seg as u64 * seg_size;
          let pattern = ((seg * 31 + 7) & 0xFF) as u8;
          let wbuf = AlignedBuf::from_slice(&vec![pattern; SECTOR], 4096)?;
          let (res, _) = dev.write_aligned(offset, wbuf).await;
          assert_eq!(res?, SECTOR);

          let check = AlignedBuf::new(SECTOR, 4096)?;
          let (res, check) = dev.read_aligned(offset, check).await;
          assert_eq!(res?, SECTOR);
          assert!(
            check.as_slice().iter().all(|&b| b == pattern),
            "线程 {t} 段 {seg} 数据不匹配"
          );
        }
        // 本线程发起全局 sync（覆盖全部线程的在表句柄）
        dev.sync().await
      })
    }));
  }
  for join in joins {
    join.join().unwrap()?;
  }

  // 主线程复验：全线程 sync 后全部段持久化数据跨线程可见
  let rt = Runtime::new()?;
  rt.block_on(async {
    for t in 0..THREADS {
      for i in 0..SEGS_PER_THREAD {
        let seg = (t as u32) * SEGS_PER_THREAD + i;
        let pattern = ((seg * 31 + 7) & 0xFF) as u8;
        let check = AlignedBuf::new(SECTOR, 4096)?;
        let (res, check) = device.read_aligned(seg as u64 * seg_size, check).await;
        assert_eq!(res?, SECTOR);
        assert!(
          check.as_slice().iter().all(|&b| b == pattern),
          "主线程复验段 {seg} 持久化数据不匹配"
        );
      }
    }
    aok::Result::<()>::Ok(())
  })?;

  info!("多 OS 线程多 Runtime 共享设备并发校验通过 (Native_HighConcurrency_ManyThreads_NoHang)");
  OK
}
