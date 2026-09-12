use core::{hint::spin_loop, sync::atomic::Ordering};
use std::{
  slice,
  sync::{Arc, mpsc},
  thread,
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{
  DEFAULT_INITIAL_ADDRESS, Error, HybridLog, HybridLogConfig, RecordOutput, SECTOR_ALIGNMENT,
};
use wrecord::{HEADER_SIZE, encode_to_slice, record_size};

/// 测试 1: 单页追加与内存直读
#[test]
fn test_append_and_memory_read() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_test1.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let config = HybridLogConfig::new(64 * 1024, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    let k1 = b"user:1001";
    let v1 = b"alice_data";
    let addr1 = hlog.append(k1, v1, 0, false)?;
    assert_eq!(addr1, DEFAULT_INITIAL_ADDRESS);

    let k2 = b"user:1002";
    let v2 = b"bob_payload_string";
    let addr2 = hlog.append(k2, v2, addr1, false)?;
    assert!(addr2 > addr1);

    // 内存中直读
    assert!(hlog.is_in_memory(addr1));
    assert!(hlog.is_in_memory(addr2));
    assert!(hlog.is_mutable(addr1));
    assert!(hlog.is_mutable(addr2));

    let out1 = hlog.read_record(addr1).await?;
    assert!(matches!(out1, RecordOutput::Memory(_)));
    assert_eq!(out1.key()?, k1);
    assert_eq!(out1.value()?, v1);
    assert_eq!(out1.prev_address()?, 0);
    assert!(!out1.is_tombstone()?);

    let out2 = hlog.read_record(addr2).await?;
    assert!(matches!(out2, RecordOutput::Memory(_)));
    assert_eq!(out2.key()?, k2);
    assert_eq!(out2.value()?, v2);
    assert_eq!(out2.prev_address()?, addr1);
    assert_eq!(hlog.safe_tail_address(), hlog.tail_address());
    assert!(hlog.safe_tail_address() > addr2);

    info!("单页追加与内存直读测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 3: 跨页换页（Page Turn）与 Padding 验证
#[test]
fn test_page_turn_and_padding() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_test3.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    // 使用最小合法单页 4096 字节
    let page_size = SECTOR_ALIGNMENT; // 4096
    let config = HybridLogConfig::new(page_size, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    // 初始 tail 从 64 开始，先写入若干记录填满大部分空间
    // 每条记录 16(header) + 8(key) + 400(val) = 424 字节
    let mut addrs = Vec::new();
    let val_400 = vec![b'A'; 400];
    for i in 0..9 {
      let key = format!("k:{i:06}");
      let addr = hlog.append(key.as_bytes(), &val_400, 0, false)?;
      addrs.push(addr);
    }

    // 此时第 0 页已消耗 64 + 9 * 424 = 3880 字节，剩余 4096 - 3880 = 216 字节
    // 写入一条大小为 16 + 8 + 300 = 324 字节的记录，必然无法容纳，触发换页！
    let overflow_key = b"k:overflow";
    let overflow_val = vec![b'B'; 300];
    let overflow_addr = hlog.append(overflow_key, &overflow_val, 0, false)?;

    // 验证新记录写入了第 1 页的起始位置（page_id=1, addr=4096）
    assert_eq!(
      overflow_addr, page_size as u64,
      "换页后新记录必须位于下一页开头"
    );

    // 验证原页末尾 3880 偏移处写入了 Pad 记录
    let pad_addr = 3880u64;
    let pad_res = hlog.read_record(pad_addr).await;
    assert!(
      matches!(pad_res, Err(Error::PadRecord(a)) if a == pad_addr),
      "读取填充位置应返回 PadRecord 错误"
    );

    // 验证新记录在第 1 页可正常读取
    let out = hlog.read_record(overflow_addr).await?;
    assert_eq!(out.key()?, overflow_key);
    assert_eq!(out.value()?, &overflow_val[..]);

    info!("跨页换页与 Padding 验证通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 8: 多页连续 Scan 扫描与拉模式迭代器（自动跳过 PadRecord）
#[test]
fn test_scan_multipage_and_pull_iterator() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_scan_test.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let page_size = SECTOR_ALIGNMENT; // 4096
    let config = HybridLogConfig::new(page_size, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    let mut expected_records = Vec::new();
    // 写入跨越至少 3 页的数据
    for i in 0..30 {
      let key = format!("scan_k:{i:04}");
      let val = vec![(i & 0xFF) as u8; 200];
      let addr = hlog.append(key.as_bytes(), &val, 0, false)?;
      expected_records.push((addr, key.into_bytes(), val));
    }

    // 1. 测试 push-based scan 全量扫描
    let mut scanned_records = Vec::new();
    hlog
      .scan(0, hlog.tail_address(), |addr, rec| {
        scanned_records.push((addr, rec.key().to_vec(), rec.value().to_vec()));
        Ok(true)
      })
      .await?;

    assert_eq!(scanned_records.len(), expected_records.len());
    for (actual, expected) in scanned_records.iter().zip(expected_records.iter()) {
      assert_eq!(actual.0, expected.0, "地址不一致");
      assert_eq!(actual.1, expected.1, "Key 不一致");
      assert_eq!(actual.2, expected.2, "Value 不一致");
    }

    // 2. 测试 pull-based ScanIterator
    let mut iter = hlog.scan_iter(0, hlog.tail_address());
    let mut pulled_records = Vec::new();
    while let Some((addr, out)) = iter.next().await? {
      pulled_records.push((addr, out.key()?.to_vec(), out.value()?.to_vec()));
    }

    assert_eq!(pulled_records.len(), expected_records.len());
    for (actual, expected) in pulled_records.iter().zip(expected_records.iter()) {
      assert_eq!(actual.0, expected.0);
      assert_eq!(actual.1, expected.1);
      assert_eq!(actual.2, expected.2);
    }

    info!("多页连续 Scan 扫描与拉模式迭代器测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 9: 混合冷热数据穿透连续 Scan 扫描（磁盘区 + 内存只读区 + 内存可变区）
#[test]
fn test_scan_hybrid_disk_and_memory() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_scan_hybrid.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let page_size = SECTOR_ALIGNMENT; // 4096
    let config = HybridLogConfig::new(page_size, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    // 写入第 0 页数据
    let mut all_records = Vec::new();
    for i in 0..5 {
      let key = format!("cold_k:{}", i);
      let val = vec![0x11; 100];
      let addr = hlog.append(key.as_bytes(), &val, 0, false)?;
      all_records.push((addr, key.into_bytes(), val));
    }

    // 填平第 0 页促使换页
    let fill_key = b"fill";
    let rem = page_size - hlog.config.page_offset(hlog.tail_address());
    let fill_val = vec![0x00; rem - HEADER_SIZE - fill_key.len()];
    let _ = hlog.append(fill_key, &fill_val, 0, false)?;

    // 写入第 1 页数据
    for i in 0..5 {
      let key = format!("hot_k:{}", i);
      let val = vec![0x22; 100];
      let addr = hlog.append(key.as_bytes(), &val, 0, false)?;
      all_records.push((addr, key.into_bytes(), val));
    }

    // 刷盘第 0 页并推进 HeadAddress 将其驱逐为磁盘冷数据
    hlog.flush_page(0).await?;
    hlog.shift_read_only_address(page_size as u64);
    hlog.shift_head_address(page_size as u64);

    assert!(hlog.is_on_disk(all_records[0].0));
    assert!(hlog.is_in_memory(all_records[5].0));

    // 执行跨三区扫描，应顺序读取冷数据与热数据
    let mut scanned = Vec::new();
    hlog
      .scan(0, hlog.tail_address(), |addr, rec| {
        if rec.key() != fill_key {
          scanned.push((addr, rec.key().to_vec(), rec.value().to_vec()));
        }
        Ok(true)
      })
      .await?;

    assert_eq!(scanned.len(), all_records.len());
    for (actual, expected) in scanned.iter().zip(all_records.iter()) {
      assert_eq!(actual.0, expected.0);
      assert_eq!(actual.1, expected.1);
      assert_eq!(actual.2, expected.2);
    }

    info!("混合冷热数据穿透连续 Scan 扫描测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 10: Push-based Scan 提前终止（Early Termination）
#[test]
fn test_scan_early_termination() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_scan_early_stop.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    for i in 0..20 {
      let key = format!("k:{i:02}");
      let val = b"data";
      let _ = hlog.append(key.as_bytes(), val, 0, false)?;
    }

    // 扫描并在第 5 条记录时提前终止
    let mut count = 0;
    hlog
      .scan(0, hlog.tail_address(), |_addr, _rec| {
        count += 1;
        if count == 5 {
          Ok(false) // 提前终止
        } else {
          Ok(true)
        }
      })
      .await?;

    assert_eq!(count, 5, "Scan 应在第 5 条记录处成功提前终止");

    info!("Push-based Scan 提前终止测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 22: 页尾子头残片（0xFF 填充）的写入、读取拦截与扫描跳过
#[test]
fn test_subheader_fragment_pad() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_fragment.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let page_size = SECTOR_ALIGNMENT;
    let config = HybridLogConfig::new(page_size, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    // R1: 16 + 10 + 4000 = 4026 → 页尾仅剩 6 字节（不足以容纳记录头）
    let v = vec![7u8; 4000];
    let addr1 = hlog.append(b"0123456789", &v, 0, false)?;
    assert_eq!(addr1, DEFAULT_INITIAL_ADDRESS);

    let addr2 = hlog.append(b"frag", b"tail", 0, false)?;
    assert_eq!(addr2, page_size as u64, "残片后新记录必须落于下一页开头");

    // 残片地址读取 → PadRecord；扫描跳过残片完整读出两条记录
    let fragment_addr = addr1 + 4026;
    assert_eq!(fragment_addr, page_size as u64 - 6);
    assert!(matches!(
      hlog.read_record(fragment_addr).await,
      Err(Error::PadRecord(_))
    ));

    let mut scanned = Vec::new();
    hlog
      .scan(0, hlog.tail_address(), |addr, rec| {
        scanned.push((addr, rec.key().to_vec()));
        Ok(true)
      })
      .await?;
    assert_eq!(
      scanned,
      vec![(addr1, b"0123456789".to_vec()), (addr2, b"frag".to_vec())]
    );

    info!("页尾子头残片处理测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 23b: 在途预留零头不漏扫同页后续记录（对标 C# TsavoriteLogScanIterator.cs:779-790
/// scanUncommitted 模式的 SafeTailAddress + Thread.SpinWait(100) 复查语义）
///
/// append 协议「先 CAS 预占 tail 后编码」：本测试白盒清零中间记录字节，构造与
/// 在途预留槽位物理形态相同的零洞；扫描闭包读到前驱记录后经通道唤醒生产者线程，
/// 以 append encode_at 同款无锁裸指针路径补写该记录。扫描器须在零头上自旋等待
/// 编码完成后原址重试读出中间记录，且同页后续记录不漏。
#[test]
fn test_scan_inflight_zero_header_respin() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_inflight.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 16, 1.0)?;
    let hlog = Arc::new(HybridLog::new(config, device, epoch)?);

    let addr0 = hlog.append(b"k0", b"v0", 0, false)?;
    let addr1 = hlog.append(b"k1", b"v1", 0, false)?;
    let addr2 = hlog.append(b"k2", b"v2", 0, false)?;

    // 白盒模拟在途预留窗口：中间记录（16B 头 + 2B 键 + 2B 值，对齐逻辑尺寸 24B）整条清零，
    // 等价于 append 已发布 tail 但 encode_at 尚未落笔的物理形态
    let page1 = hlog.config.page_id(addr1);
    let off1 = hlog.config.page_offset(addr1);
    let rec1_len = record_size(2, 2);
    {
      let mut guard = hlog.buffer.write_page(page1);
      guard[off1..off1 + rec1_len].fill(0);
    }

    // 握手通道：扫描闭包读到 k0（零洞前驱）后唤醒生产者，生产者以与 encode_at
    // 完全一致的无锁裸指针路径补写 k1（零洞恰被扫描器触达时走自旋重试路径）
    let (encode_tx, encode_rx) = mpsc::channel::<()>();
    let producer = {
      let hlog = Arc::clone(&hlog);
      thread::spawn(move || {
        if encode_rx.recv().is_err() {
          return;
        }
        // 扫描器触达 k0 即将进入零洞：短自旋让扫描器触达零头并进入自旋重试
        for _ in 0..128 {
          spin_loop();
        }
        let slot = hlog.buffer.page_idx(page1);
        // SAFETY: 测试单扫描线程与生产者线程互斥于补写窗口，写入区间
        // [off1, off1 + rec1_len) 为已预留槽位，无其他并发访问者
        let ptr = unsafe { hlog.buffer.raw_page_ptr_mut(slot) };
        let dst = unsafe { slice::from_raw_parts_mut(ptr.add(off1), rec1_len) };
        let _ = encode_to_slice(dst, 0, b"k1", b"v1", false);
      })
    };

    let mut scanned = Vec::new();
    hlog
      .scan(0, hlog.tail_address(), |addr, rec| {
        scanned.push((addr, rec.key().to_vec()));
        if addr == addr0 {
          let _ = encode_tx.send(());
        }
        Ok(true)
      })
      .await?;
    producer.join().unwrap();

    assert_eq!(
      scanned,
      vec![
        (addr0, b"k0".to_vec()),
        (addr1, b"k1".to_vec()),
        (addr2, b"k2".to_vec())
      ],
      "在途零头自旋重试后必须原址读出 k1，且同页后续记录不得漏扫"
    );

    info!("在途预留零头自旋重试测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试: 48 位逻辑地址溢出防御
#[test]
fn test_append_address_overflow_defense() -> Void {
  let dir = tempdir()?;
  let db_path = dir.path().join("hlog_overflow.db");
  let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
  let epoch = Arc::new(LightEpoch::new(16));

  let config = HybridLogConfig::new(64 * 1024, 16, 0.5)?;
  let hlog = HybridLog::new(config, device, epoch)?;
  let tail = wrecord::ADDRESS_MASK - 10;
  let curr_page = tail >> hlog.config.page_bits();
  hlog.buffer.set_page_id(curr_page);

  // 推进已落盘与已驱逐边界，使换页检查 ensure_page_ready 顺利通过
  let next_page = curr_page + 1;
  let old_page = next_page - hlog.config.num_pages as u64;
  let min_evicted = hlog.config.page_start_address(old_page + 1);
  hlog.addresses.shift_flushed_until_address(min_evicted);
  hlog.addresses.shift_head_address(min_evicted);
  hlog.addresses.shift_safe_head_address(min_evicted);

  // 人为将 tail_address 设置在 48 位上限边界
  hlog.addresses.tail_address.store(tail, Ordering::Release);

  // 追加一条尺寸超过 10 字节的记录触发跨页，跨页目标起始地址超过 48 位，必须被拦截
  let res = hlog.append(b"k", b"val_that_overflows_48bits", 0, false);
  assert!(matches!(res, Err(Error::InvalidAddress(_))));

  OK
}
