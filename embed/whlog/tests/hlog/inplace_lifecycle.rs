use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{DEFAULT_INITIAL_ADDRESS, Error, HybridLog, HybridLogConfig, SECTOR_ALIGNMENT};

/// 测试 2: 可变区原位更新（In-place update）与只读区保护
#[test]
fn test_in_place_update_and_protection() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_test2.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let config = HybridLogConfig::new(64 * 1024, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    let key = b"counter:1";
    let val = b"value_01";
    let addr = hlog.append(key, val, 0, false)?;

    // 1. 原位更新：长度一致
    let new_val = b"value_99";
    let updated = hlog.try_update_in_place(addr, key, new_val)?;
    assert!(updated, "可变区原位更新应成功");

    // 回读验证
    let out = hlog.read_record(addr).await?;
    assert_eq!(out.value()?, new_val);

    // 2. 原位更新失败：长度不一致
    let bad_val = b"value_longer_than_original";
    let updated_fail = hlog.try_update_in_place(addr, key, bad_val)?;
    assert!(!updated_fail, "值长度不匹配时不应允许原位覆写");

    // 3. 推进只读边界将 addr 划入只读区
    let next_page_start = 64 * 1024;
    hlog.shift_read_only_address(next_page_start);
    assert!(hlog.is_read_only(addr));
    assert!(!hlog.is_mutable(addr));

    // 只读区原位更新必须被拒绝
    let ro_update = hlog.try_update_in_place(addr, key, b"value_02")?;
    assert!(!ro_update, "只读区不可进行原位修改");

    info!("可变区原位更新与只读区保护测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 13: 历史版本反向链表回溯（对标 Garnet IterateKeyVersions）
#[test]
fn test_iterate_version_chain() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_version_chain.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let config = HybridLogConfig::new(64 * 1024, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    let key = b"chain_key";
    let addr1 = hlog.append(key, b"v1", 0, false)?;
    let addr2 = hlog.append(key, b"v2", addr1, false)?;
    let addr3 = hlog.append(key, b"v3", addr2, false)?;
    let addr4 = hlog.append(key, b"v4", addr3, false)?;

    // 1. 全量反向回溯 (v4 -> v3 -> v2 -> v1)
    let mut collected = Vec::new();
    hlog
      .iterate_version_chain(addr4, |_addr, rec| {
        collected.push(rec.value()?.to_vec());
        Ok(true)
      })
      .await?;

    assert_eq!(
      collected,
      vec![
        b"v4".to_vec(),
        b"v3".to_vec(),
        b"v2".to_vec(),
        b"v1".to_vec()
      ]
    );

    // 2. 提前终止回溯（在看到 v3 时停止）
    let mut truncated = Vec::new();
    hlog
      .iterate_version_chain(addr4, |_addr, rec| {
        let val = rec.value()?.to_vec();
        let stop = val == b"v3";
        truncated.push(val);
        Ok(!stop)
      })
      .await?;

    assert_eq!(truncated, vec![b"v4".to_vec(), b"v3".to_vec()]);

    info!("历史版本反向链表回溯测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 18: 原位更新 / RMW / 墓碑 / 原位复活 全生命周期
#[test]
fn test_inplace_lifecycle() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_inplace.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let config = HybridLogConfig::new(64 * 1024, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    let key = b"life";
    let addr = hlog.append(key, b"1234567890", 0, false)?;
    assert_eq!(addr, DEFAULT_INITIAL_ADDRESS);

    // 槽位扩至 35 字节：富余 5 字节不足以容纳 Pad 头 → 吸纳为松弛填充
    hlog.revivify_record_at(addr, 35, key, b"1234567890", 0, false)?;
    let out = hlog.read_record(addr).await?;
    assert_eq!(out.value()?, b"1234567890");
    assert_eq!(out.header()?.filler_bytes(), 5, "富余空间必须转为松弛填充");

    // 原位松弛更新：新值 15 字节恰好填满 val_len + filler 容量
    assert!(hlog.try_update_in_place(addr, key, b"012345678901234")?);
    assert_eq!(hlog.read_record(addr).await?.value()?, b"012345678901234");

    // RMW 闭包原位修改
    let r = hlog.try_modify_record_in_place(addr, key, |v| {
      v[0] = b'X';
      Some(())
    })?;
    assert!(r.is_some());
    assert_eq!(hlog.read_record(addr).await?.value()?, b"X12345678901234");

    // 键不匹配 / 值超容量 → 原位失败
    assert!(!hlog.try_update_in_place(addr, b"wrong", b"y")?);
    assert!(!hlog.try_update_in_place(addr, key, &[b'z'; 16])?);

    // 经 revivify_record_at 将同槽位改写为空值墓碑
    hlog.revivify_record_at(addr, 35, key, b"", 0, true)?;
    assert!(hlog.read_record(addr).await?.is_tombstone()?);

    // 原位复活：清除墓碑并覆写新值
    assert!(hlog.try_revivify_in_chain(addr, key, b"revived!")?);
    let out = hlog.read_record(addr).await?;
    assert!(!out.is_tombstone()?);
    assert_eq!(out.value()?, b"revived!");

    // 非墓碑记录不可复活
    assert!(!hlog.try_revivify_in_chain(addr, key, b"again")?);

    info!("原位更新全生命周期测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 19: 复活槽位超配填充 Pad（剩余 >= 头）与逻辑视图边界
#[test]
fn test_revivify_record_at_with_pad() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_reviv.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let config = HybridLogConfig::new(64 * 1024, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    // R1: 16+3+21=40 字节槽位；R2: 16+1+3=20 字节，紧跟其后
    let val21 = vec![b'x'; 21];
    let addr1 = hlog.append(b"old", &val21, 0, false)?;
    let addr2 = hlog.append(b"b", b"vvv", 0, false)?;

    // 复活 R1 槽位：新记录 16+3+5=24，剩余 16 恰容纳 Pad 头（val_len=0）
    hlog.revivify_record_at(addr1, 40, b"new", b"value", 0, false)?;
    let out = hlog.read_record(addr1).await?;
    assert_eq!(out.key()?, b"new");
    assert_eq!(out.value()?, b"value");

    // 追加 R3 至尾部；扫描必须精确越过槽内 Pad（不跳页），完整读出 R1'/R2/R3
    let addr3 = hlog.append(b"c", b"vvv", 0, false)?;
    let mut scanned = Vec::new();
    hlog
      .scan(0, hlog.tail_address(), |addr, rec| {
        scanned.push((addr, rec.key().to_vec()));
        Ok(true)
      })
      .await?;
    assert_eq!(
      scanned,
      vec![
        (addr1, b"new".to_vec()),
        (addr2, b"b".to_vec()),
        (addr3, b"c".to_vec())
      ],
      "槽内 Pad 必须按物理尺寸精确越过，不得吞并同页后续记录"
    );

    // 槽位不足 → RecordTooLarge；非可变区地址 → AddressOutOfRange
    assert!(matches!(
      hlog.revivify_record_at(addr1, 10, b"new", b"value", 0, false),
      Err(Error::RecordTooLarge { .. })
    ));
    assert!(matches!(
      hlog.revivify_record_at(0, 100, b"x", b"y", 0, false),
      Err(Error::AddressOutOfRange { .. })
    ));

    // 尾部追加位置不受复活复用影响（复用旧槽位不推进 tail）
    assert_eq!(hlog.tail_address(), addr3 + 20);

    info!("复活槽位 Pad 填充测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 20: shift_begin_address 落盘前置校验、begin 推进与设备段物理截断
#[test]
fn test_shift_begin_address_and_truncate() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_shift_begin.db");
    // 段大小 8192：两页一段，shift_begin(8192) 应物理删除段 0
    let device = Arc::new(SegmentedDevice::new(
      &db_path,
      Some(2 * SECTOR_ALIGNMENT as u64),
      SECTOR_ALIGNMENT,
    )?);
    let epoch = Arc::new(LightEpoch::new(16));

    let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 16, 0.5)?;
    let hlog = HybridLog::new(config, Arc::clone(&device), epoch)?;

    let big = vec![b'B'; 3900];
    let addr1 = hlog.append(b"k1", &big, 0, false)?;
    let addr2 = hlog.append(b"k2", &big, 0, false)?;
    let addr3 = hlog.append(b"k3", &big, 0, false)?;
    assert_eq!(hlog.config.page_id(addr3), 2);

    // 全量落盘并封印只读后，推进 begin 越过段 0
    hlog.flush_all().await?;
    hlog.sync().await?;
    hlog.shift_read_only_to_tail();

    let new_begin = 2 * SECTOR_ALIGNMENT as u64;
    hlog.shift_begin_address(new_begin).await?;

    assert_eq!(hlog.begin_address(), new_begin);
    assert_eq!(hlog.head_address(), new_begin);

    // 段 0 已被物理截断
    assert_eq!(device.get_file_size(0)?, 0, "段 0 必须被物理删除");
    assert!(device.get_file_size(1)? > 0, "段 1 必须保留");

    // begin 以下地址越界，段 1 数据仍可读
    assert!(matches!(
      hlog.read_record(addr1).await,
      Err(Error::AddressOutOfRange { .. })
    ));
    assert!(matches!(
      hlog.read_record(addr2).await,
      Err(Error::AddressOutOfRange { .. })
    ));
    let out3 = hlog.read_record(addr3).await?;
    assert_eq!(out3.key()?, b"k3");

    info!("shift_begin_address 与设备段截断测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}
