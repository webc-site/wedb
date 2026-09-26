//! B3 在途零头跳页漏扫回归（whlog 尾部预占发布协议）
//!
//! 缺陷（持久化正确性，已提交记录读不到）：`append` 协议为「tail CAS 独占槽位 → 编码
//! 落笔」，旧协议下已预占未编码的槽位在**整段编码期**内头两字恒为全零，扫描器解不出
//! 物理尺寸，`ZERO_HEADER_SPIN_BUDGET` 自旋耗尽即 `skip_to_next_page` 整页跳过，而扫描
//! 游标单调前进绝不回头——同页其后已完整编码、已向上层返回地址的记录被一并漏扫。
//!
//! 修复：`encode_at` 首拍先单字发布只含槽位尺寸的 Pad 形态 extent 头（对标 C# 新记录先
//! 写扫描可见的关闭态头 `RecordInfo.WriteInfo` + 扫描器 `SkipOnScan` 跳记录不跳页，
//! SpanByteScanIterator.GetNext），扫描器据 `HEADER_SIZE + val_len` 精确越过在途槽位，
//! 同页后续记录恒被扫出；本轮唯一跳过的是仍在编码的那条记录本身，下一轮自愈。

use std::{
  sync::Arc,
  thread,
  time::{Duration, Instant},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{EncodeStall, HybridLog, HybridLogConfig, SECTOR_ALIGNMENT};
use wrecord::{HEADER_SIZE, RecordHeader, record_size};

/// 页 0 内一条已提交记录（地址 / 键 / 值）
type Rec = (u64, Vec<u8>, Vec<u8>);

/// 阻塞至生产者确实挂起在途窗口（超时判负，绝不死等拖垮门禁）
fn wait_parked(stall: &EncodeStall) {
  let started = Instant::now();
  while !stall.is_parked() {
    assert!(
      started.elapsed() < Duration::from_secs(10),
      "生产者未挂起在途编码窗口：注入门失效"
    );
    thread::sleep(Duration::from_millis(1));
  }
}

/// 测试 B3-1: 生产者在途挂起（编码窗口长于扫描自旋预算）时，同页后续已提交记录零漏扫
///
/// 本用例即本票的证伪判据：`encode_stall` 把生产者停在 extent 头发布之后、键值落笔之前，
/// 在途窗口贯穿整次扫描，扫描器自旋预算必然耗尽。旧协议此刻槽位全零、尺寸不可解 →
/// 整页跳过 → 断言只剩 `warm` 一条，直接失败；新协议槽位尺寸可解 → 精确越过 → 三条
/// 后继记录全部扫出。放行后再全量扫描，在途记录自愈读出且键值完整。
#[test]
fn test_parked_producer_loses_no_same_page_record() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("hlog_parked.db"),
    )?);
    let epoch = Arc::new(LightEpoch::new(16));
    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 16, 1.0)?;
    let hlog = Arc::new(HybridLog::new(config, device, epoch)?);

    // 页 0 布局：warm → in-flight（生产者挂起于此槽）→ after0..after2 三条已提交后继
    let key_warm = b"warm".to_vec();
    let val_warm = vec![b'w'; 64];
    let (addr_warm, _) = hlog.append(&key_warm, &val_warm, 0, false)?;

    let key_fly = b"in-flight".to_vec();
    let val_fly = vec![b'f'; 128];
    let size_fly = record_size(key_fly.len(), val_fly.len());
    let addr_fly = addr_warm + record_size(key_warm.len(), val_warm.len()) as u64;

    hlog.encode_stall.arm();
    let producer = {
      let hlog = Arc::clone(&hlog);
      let (key, val) = (key_fly.clone(), val_fly.clone());
      thread::spawn(move || hlog.append(&key, &val, 0, false).map(|(addr, _)| addr))
    };
    wait_parked(&hlog.encode_stall);

    // tail 已发布、键值未落笔——旧协议整页跳过的触发态
    assert_eq!(
      hlog.tail_address(),
      addr_fly + size_fly as u64,
      "在途槽位须已由 tail 独占预占"
    );

    let mut after = Vec::new();
    for i in 0..3u8 {
      let key = vec![b'a' + i; 8];
      let val = vec![0xE0 + i; 40];
      let (addr, _) = hlog.append(&key, &val, 0, false)?;
      assert_eq!(
        hlog.config.page_id(addr),
        hlog.config.page_id(addr_fly),
        "后继记录须同页"
      );
      after.push((addr, key, val));
    }

    let mut scanned: Vec<(u64, Vec<u8>)> = Vec::new();
    hlog
      .scan(0, hlog.tail_address(), |addr, rec| {
        scanned.push((addr, rec.key().to_vec()));
        Ok(true)
      })
      .await?;

    let mut want: Vec<(u64, Vec<u8>)> = vec![(addr_warm, key_warm.clone())];
    want.extend(after.iter().map(|(addr, key, _)| (*addr, key.clone())));
    assert_eq!(
      scanned, want,
      "漏扫：在途 extent 头之后的同页已提交记录必须全部扫出（旧协议在此整页跳过，只剩 warm 一条）"
    );

    // 在途槽位的物理形态：尺寸可解的 extent 头，精确覆盖本槽
    let page = hlog.config.page_id(addr_fly);
    let offset = hlog.config.page_offset(addr_fly);
    {
      let guard = hlog.buffer.read_page(page);
      let header = RecordHeader::decode_opt(&guard[offset..]).expect("在途槽位头部可读");
      assert!(
        header.is_pad(),
        "在途槽位必须以尺寸可解的 extent 头发布，当前形态 {header:?}"
      );
      assert_eq!(
        HEADER_SIZE + header.val_len() as usize,
        size_fly,
        "extent 头须精确覆盖预占槽位物理尺寸"
      );
    }

    // 放行生产者：在途记录本轮回跳过，放行后下一轮自愈读出，键值完整无残留
    hlog.encode_stall.release();
    assert_eq!(producer.join().unwrap()?, addr_fly);

    let mut healed: Vec<Rec> = Vec::new();
    hlog
      .scan(0, hlog.tail_address(), |addr, rec| {
        healed.push((addr, rec.key().to_vec(), rec.value().to_vec()));
        Ok(true)
      })
      .await?;

    let mut want_all: Vec<Rec> = vec![
      (addr_warm, key_warm.clone(), val_warm.clone()),
      (addr_fly, key_fly.clone(), val_fly.clone()),
    ];
    want_all.extend(after.iter().cloned());
    assert_eq!(
      healed, want_all,
      "放行后在途记录须自愈读出，且 extent 头被完整头覆写无残留"
    );

    info!("在途挂起生产者不漏扫同页后续记录测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 B3-2: 崩溃残留的在途 extent 头（持久形态）在冷数据区按槽位尺寸精确越过
///
/// 在途协议下被抢占的生产者若就此进程终止，页内残留恒为 extent 头而非全零，恢复后该槽
/// 仍是尺寸可解的空洞：扫描器（磁盘冷读路径，无在途自旋可言）按物理尺寸越过空洞，页内
/// 其余已落盘记录永久可读；旧协议残留的全零洞则令该页后续记录在每次扫描中永久丢失。
#[test]
fn test_extent_header_hole_on_disk_loses_no_record() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("hlog_residue.db"),
    )?);
    let epoch = Arc::new(LightEpoch::new(16));
    let page_size = SECTOR_ALIGNMENT;
    let config = HybridLogConfig::new(page_size, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    let mut recs = Vec::new();
    for i in 0..4u8 {
      let key = vec![b'r', i + b'0'];
      let val = vec![0x5A + i; 64];
      let (addr, _) = hlog.append(&key, &val, 0, false)?;
      recs.push((addr, key, val));
    }

    // 白盒把第 2 条槽位复原为「已预占、未编码」的在途 extent 头形态（含键值区清零）
    let (addr_hole, ..) = recs[1];
    let size_hole = record_size(2, 64);
    let offset = hlog.config.page_offset(addr_hole);
    {
      let mut guard = hlog.buffer.write_page(hlog.config.page_id(addr_hole));
      let pad = RecordHeader::pad(size_hole).to_bytes();
      guard[offset..offset + HEADER_SIZE].copy_from_slice(&pad);
      guard[offset + HEADER_SIZE..offset + size_hole].fill(0);
    }

    // 整页落盘并驱逐为磁盘冷数据：扫描走设备读路径，形态自此定稿
    hlog.flush_page(hlog.config.page_id(addr_hole)).await?;
    hlog.shift_read_only_address(page_size as u64);
    hlog.shift_head_address(page_size as u64);
    assert!(hlog.is_on_disk(addr_hole), "残留页须已滑为磁盘冷数据");

    let mut scanned: Vec<Rec> = Vec::new();
    hlog
      .scan(0, hlog.tail_address(), |addr, rec| {
        scanned.push((addr, rec.key().to_vec(), rec.value().to_vec()));
        Ok(true)
      })
      .await?;

    assert_eq!(
      scanned,
      vec![recs[0].clone(), recs[2].clone(), recs[3].clone()],
      "extent 头空洞须按物理尺寸越过，同页后续已落盘记录零漏扫、键值完整"
    );

    info!("崩溃残留 extent 头空洞冷数据扫描测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
