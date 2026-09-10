use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{
  DEFAULT_INITIAL_ADDRESS, Error, HybridLog, HybridLogConfig, RecordOutput, SECTOR_ALIGNMENT,
};

/// 测试 11: 快照恢复与状态机不变式校验（防幽灵段与脏状态）
#[test]
fn test_recover_and_snapshot_invariants() -> Void {
  use whlog::AddressSnapshot;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_recover.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));
    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 16, 0.5)?;

    // 1. 合法快照恢复成功
    let valid_snapshot = AddressSnapshot::new(
      64,    // begin
      4096,  // safe_head
      4096,  // head
      4096,  // safe_read_only
      8192,  // read_only
      10000, // tail
      4096,  // flushed_until
    );
    assert!(valid_snapshot.validate());

    let recovered_hlog = HybridLog::recover(
      config.clone(),
      Arc::clone(&device),
      Arc::clone(&epoch),
      valid_snapshot,
    )
    .await;
    assert!(recovered_hlog.is_ok(), "合法快照恢复应成功");
    let hlog = recovered_hlog.unwrap();
    assert_eq!(hlog.tail_address(), 10000);
    assert_eq!(hlog.head_address(), 4096);

    // 2. 非法快照：head > flushed_until（存在未落盘即被驱逐出内存的数据）
    let invalid_snapshot1 = AddressSnapshot::new(
      64, 4096, 8192, 8192, 8192, 10000, 4096, // flushed_until (4096) < head (8192)
    );
    assert!(!invalid_snapshot1.validate());
    let fail1 = HybridLog::recover(
      config.clone(),
      Arc::clone(&device),
      Arc::clone(&epoch),
      invalid_snapshot1,
    )
    .await;
    assert!(
      matches!(fail1, Err(Error::InvalidState(_))),
      "非法快照必须被拦截并返回 InvalidState"
    );

    // 3. 非法快照：safe_head > head（违反纪元单调顺序）
    let invalid_snapshot2 = AddressSnapshot::new(64, 8192, 4096, 4096, 8192, 10000, 4096);
    assert!(!invalid_snapshot2.validate());
    let fail2 = HybridLog::recover(config, device, epoch, invalid_snapshot2).await;
    assert!(matches!(fail2, Err(Error::InvalidState(_))));

    info!("快照恢复与状态机不变式校验测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 12: 崩溃恢复后磁盘页预热加载与无缝继续追加
#[test]
fn test_recovery_preload_and_resume_append() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_recover_resume.db");
    let page_size = SECTOR_ALIGNMENT; // 4096
    let config = HybridLogConfig::new(page_size, 16, 0.5)?;

    let addr0;
    let addr1;
    let tail_before;

    // 阶段 1: 写入数据并落盘
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let epoch = Arc::new(LightEpoch::new(16));
      let hlog = HybridLog::new(config.clone(), device, epoch)?;

      addr0 = hlog.append(b"k0", b"v0_before_crash", 0, false)?;
      // 填满第 0 页跨入第 1 页
      let pad_val = vec![b'P'; 4000];
      let _ = hlog.append(b"pad", &pad_val, 0, false)?;

      addr1 = hlog.append(b"k1", b"v1_page1", addr0, false)?;
      assert_eq!(hlog.config.page_id(addr1), 1);

      // 落盘第 0 页与第 1 页
      hlog.flush_all().await?;
      hlog.sync().await?;
      tail_before = hlog.tail_address();
    }

    // 阶段 2: 恢复 HybridLog 并校验预热数据与继续追加
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let epoch = Arc::new(LightEpoch::new(16));

      let snapshot = whlog::AddressSnapshot::from_bounds(
        DEFAULT_INITIAL_ADDRESS,
        DEFAULT_INITIAL_ADDRESS, // head 初始边界为 DEFAULT_INITIAL_ADDRESS (第 0 页)
        tail_before,             // flushed_until
        tail_before,             // read_only
        tail_before,             // tail
      );
      assert!(snapshot.validate());

      let hlog = HybridLog::recover(config, device, epoch, snapshot).await?;
      assert_eq!(hlog.tail_address(), tail_before);

      // 回读恢复前写入的第 0 页与第 1 页记录
      let out0 = hlog.read_record(addr0).await?;
      assert_eq!(out0.key()?, b"k0");
      assert_eq!(out0.value()?, b"v0_before_crash");

      let out1 = hlog.read_record(addr1).await?;
      assert_eq!(out1.key()?, b"k1");
      assert_eq!(out1.value()?, b"v1_page1");

      // 恢复后继续追加新记录，地址必须紧接 tail_before
      let addr2 = hlog.append(b"k2", b"v2_after_recovery", addr1, false)?;
      assert_eq!(addr2, tail_before);

      let out2 = hlog.read_record(addr2).await?;
      assert_eq!(out2.key()?, b"k2");
      assert_eq!(out2.value()?, b"v2_after_recovery");
      assert_eq!(out2.prev_address()?, addr1);

      info!("崩溃恢复后磁盘页预热加载与无缝继续追加测试通过");
    }

    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 21: 恢复时非持久化前缀 [flushed_until, tail) 必须被清洗（崩溃一致性边界）
#[test]
fn test_recover_scrubs_non_durable_prefix() -> Void {
  use whlog::AddressSnapshot;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_scrub.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let page_size = SECTOR_ALIGNMENT;
    let config = HybridLogConfig::new(page_size, 16, 0.5)?;
    let hlog = HybridLog::new(config.clone(), Arc::clone(&device), Arc::clone(&epoch))?;

    let big = vec![b'D'; 3900];
    let addr1 = hlog.append(b"durable", &big, 0, false)?;
    let addr2 = hlog.append(b"volatile", &big, 0, false)?; // 第 1 页
    hlog.flush_all().await?;
    hlog.sync().await?;
    let tail = hlog.tail_address();

    // 构造仅承诺第 0 页已落盘的快照（模拟崩溃时第 1 页数据未获持久化）
    let snapshot = AddressSnapshot::from_bounds(
      DEFAULT_INITIAL_ADDRESS,
      DEFAULT_INITIAL_ADDRESS,
      page_size as u64, // flushed_until：仅第 0 页
      tail,
      tail,
    );
    let recovered = HybridLog::recover(config, device, epoch, snapshot).await?;

    // 第 1 页已整页清零：读取返回 PadRecord，扫描按换页填充跳过
    assert!(matches!(
      recovered.read_record(addr2).await,
      Err(Error::PadRecord(_))
    ));
    let mut scanned = Vec::new();
    recovered
      .scan(0, tail, |addr, rec| {
        scanned.push((addr, rec.key().to_vec()));
        Ok(true)
      })
      .await?;
    assert_eq!(scanned, vec![(addr1, b"durable".to_vec())]);

    // 从 tail 无缝续写新记录并回读
    let addr3 = recovered.append(b"resumed", b"v3", addr2, false)?;
    assert_eq!(addr3, tail);
    let out3 = recovered.read_record(addr3).await?;
    assert_eq!(out3.key()?, b"resumed");

    info!("恢复非持久化前缀清洗测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 23: 配置校验（页大小、页数、可变比例含 NaN、初始地址）
#[test]
fn test_config_validation() -> Void {
  assert!(matches!(
    HybridLogConfig::new(4095, 16, 0.5),
    Err(Error::InvalidConfig(_))
  ));
  assert!(matches!(
    HybridLogConfig::new(4096, 0, 0.5),
    Err(Error::InvalidConfig(_))
  ));
  assert!(matches!(
    HybridLogConfig::new(4096, 3, 0.5),
    Err(Error::InvalidConfig(_))
  ));
  assert!(matches!(
    HybridLogConfig::new(4096, 16, 0.0),
    Err(Error::InvalidConfig(_))
  ));
  assert!(matches!(
    HybridLogConfig::new(4096, 16, 1.5),
    Err(Error::InvalidConfig(_))
  ));
  assert!(
    matches!(
      HybridLogConfig::new(4096, 16, f64::NAN),
      Err(Error::InvalidConfig(_))
    ),
    "NaN 可变比例必须被拦截"
  );
  assert!(matches!(
    HybridLogConfig::with_initial_address(4096, 16, 0.5, 63),
    Err(Error::InvalidConfig(_))
  ));
  assert!(HybridLogConfig::new(8192, 4, 1.0).is_ok());

  info!("配置校验测试通过");
  OK
}

/// 测试 24: 追加参数边界校验（48 位前驱地址溢出、记录超页）
#[test]
fn test_append_argument_guards() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_guards.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let page_size = SECTOR_ALIGNMENT;
    let config = HybridLogConfig::new(page_size, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    // 前驱地址超出 48 位
    let overflow_prev = 1u64 << 48;
    assert!(matches!(
      hlog.append(b"k", b"v", overflow_prev, false),
      Err(Error::InvalidAddress(_))
    ));

    // 记录尺寸超过单页容量
    let huge = vec![0u8; page_size];
    assert!(matches!(
      hlog.append(b"k", &huge, 0, false),
      Err(Error::RecordTooLarge { .. })
    ));

    // 校验失败不得推进 tail
    assert_eq!(hlog.tail_address(), DEFAULT_INITIAL_ADDRESS);

    info!("追加参数边界校验测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 25: 磁盘冷读大记录——整页冷读后按记录物理尺寸精确裁剪输出缓冲
#[test]
fn test_cold_read_large_record_exact_trim() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_probe.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    // 64KB 页：可容纳远超单条小记录的大记录
    let page_size = 64 * 1024;
    let config = HybridLogConfig::new(page_size, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    // 大记录：16(头) + 5(键) + 5000(值) = 5021 字节
    let big_key = b"big:1";
    let big_val = vec![b'A'; 5000];
    let addr_big = hlog.append(big_key, &big_val, 0, false)?;

    // 同页小记录：与大记录同居一页
    let small_key = b"s:1";
    let small_val = b"tiny";
    let addr_small = hlog.append(small_key, small_val, addr_big, false)?;

    // 落盘并驱逐第 0 页到磁盘区
    hlog.flush_page(0).await?;
    hlog.shift_read_only_address(page_size as u64);
    hlog.shift_head_address(page_size as u64);
    assert!(hlog.is_on_disk(addr_big));

    // 大记录冷读：整页读入后从页内偏移解析，输出缓冲按物理尺寸精确裁剪，内容逐字节一致
    let out_big = hlog.read_record(addr_big).await?;
    assert!(matches!(out_big, RecordOutput::Disk(_)));
    assert_eq!(out_big.key()?, big_key);
    assert_eq!(out_big.value()?, &big_val[..]);
    assert_eq!(out_big.prev_address()?, 0);
    assert_eq!(
      out_big.as_slice().len(),
      out_big.header()?.physical_size(),
      "磁盘冷读缓冲必须按物理尺寸精确裁剪"
    );

    // 同页小记录冷读：同样完整一致
    let out_small = hlog.read_record(addr_small).await?;
    assert!(matches!(out_small, RecordOutput::Disk(_)));
    assert_eq!(out_small.key()?, small_key);
    assert_eq!(out_small.value()?, small_val);
    assert_eq!(out_small.prev_address()?, addr_big);

    info!("磁盘冷读大记录精确裁剪测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 29: 跨多段设备恢复——整段批量预热 read_range 跨段读取 + num_pages 环形窗口守卫
#[test]
fn test_recover_multisegment_span_and_window_guard() -> Void {
  use whlog::AddressSnapshot;

  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_multiseg_recover.db");
    // 段大小 8192（两页一段）：5 页驻留窗口横跨段 0..=2，预热单次读必须跨段
    let device = Arc::new(SegmentedDevice::new(
      &db_path,
      Some(2 * SECTOR_ALIGNMENT as u64),
      SECTOR_ALIGNMENT,
    )?);
    let epoch = Arc::new(LightEpoch::new(16));

    let page_size = SECTOR_ALIGNMENT;
    let config = HybridLogConfig::new(page_size, 16, 0.5)?;

    let mut addrs = Vec::new();
    let tail_before;
    {
      let hlog = HybridLog::new(config.clone(), device, Arc::clone(&epoch))?;
      let big = vec![b'M'; 3900];
      for i in 0..5u8 {
        let key = format!("m{i}").into_bytes();
        let addr = hlog.append(&key, &big, 0, false)?;
        assert_eq!(hlog.config.page_id(addr), i as u64, "每页恰好一条记录");
        addrs.push((addr, key));
      }
      hlog.flush_all().await?;
      hlog.sync().await?;
      tail_before = hlog.tail_address();
    }

    // 环形窗口守卫：head 与 tail 页间隔 >= num_pages 的快照必须被拦截
    {
      let bad_snapshot = AddressSnapshot::from_bounds(
        DEFAULT_INITIAL_ADDRESS,
        DEFAULT_INITIAL_ADDRESS,
        64 * page_size as u64 + DEFAULT_INITIAL_ADDRESS, // flushed_until
        64 * page_size as u64 + DEFAULT_INITIAL_ADDRESS, // read_only
        64 * page_size as u64 + DEFAULT_INITIAL_ADDRESS, // tail（页号 64，远超 16 页窗口）
      );
      assert!(bad_snapshot.validate(), "快照本身须合法以触达窗口守卫");
      let result = HybridLog::recover(
        config.clone(),
        Arc::new(SegmentedDevice::segmented(
          &db_path,
          2 * SECTOR_ALIGNMENT as u64,
        )?),
        Arc::clone(&epoch),
        bad_snapshot,
      )
      .await;
      assert!(
        matches!(result, Err(Error::InvalidState(msg)) if msg.contains("驻留窗口")),
        "跨页窗口超限必须被环形页数守卫拦截"
      );
    }

    // 跨段恢复：head 页 0 → tail 页 4，预热区间 [0, 20480) 横跨段 0/1/2
    let snapshot = AddressSnapshot::from_bounds(
      DEFAULT_INITIAL_ADDRESS,
      DEFAULT_INITIAL_ADDRESS,
      tail_before,
      tail_before,
      tail_before,
    );
    let hlog = HybridLog::recover(
      config,
      Arc::new(SegmentedDevice::segmented(
        &db_path,
        2 * SECTOR_ALIGNMENT as u64,
      )?),
      epoch,
      snapshot,
    )
    .await?;

    // 跨段预热的全部记录逐页回读（键值逐字节一致）
    for (addr, key) in &addrs {
      let out = hlog.read_record(*addr).await?;
      assert_eq!(out.key()?, key.as_slice());
      assert_eq!(out.value()?, &[b'M'; 3900]);
    }
    assert_eq!(hlog.tail_address(), tail_before);

    // 扫描恰好命中全部 5 条记录（页尾 Pad 精确越过）
    let mut scanned = Vec::new();
    hlog
      .scan(0, tail_before, |addr, rec| {
        scanned.push((addr, rec.key().to_vec()));
        Ok(true)
      })
      .await?;
    assert_eq!(
      scanned,
      addrs
        .iter()
        .map(|(a, k)| (*a, k.clone()))
        .collect::<Vec<_>>()
    );

    // 从 tail 无缝续写
    let next = hlog.append(b"m5", &[b'N'; 100], 0, false)?;
    assert_eq!(next, tail_before);

    info!("跨多段恢复与环形窗口守卫测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// AddressSnapshot 定长 56 字节二进制编解码测试
#[test]
fn test_address_snapshot_binary_codec() -> Void {
  use whlog::AddressSnapshot;

  info!("开始测试: AddressSnapshot 定长 56 字节编解码");

  let snap = AddressSnapshot::new(64, 4096, 4096, 8192, 8192, 16384, 8192);
  let bytes = snap.to_bytes();
  assert_eq!(bytes.len(), AddressSnapshot::SNAPSHOT_SIZE);
  assert_eq!(AddressSnapshot::SNAPSHOT_SIZE, 56);

  let decoded = AddressSnapshot::from_bytes(bytes);
  assert_eq!(decoded, snap);

  let decoded_opt = AddressSnapshot::decode_opt(&bytes);
  assert_eq!(decoded_opt, Some(snap));

  assert_eq!(AddressSnapshot::decode_opt(&bytes[..55]), None);

  info!("AddressSnapshot 定长 56 字节编解码测试通过");
  OK
}
