use std::sync::Arc;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{DEFAULT_INITIAL_ADDRESS, Error, HybridLog, HybridLogConfig, SECTOR_ALIGNMENT};
use wrecord::{HEADER_SIZE, RDH_WORD_OFFSET};

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

    // 槽位扩至 40 字节：记录对齐逻辑尺寸 32（16+4+10），富余 8 字节（一个整词）
    // 不足以容纳 Pad 头 → 吸纳为松弛填充
    hlog.revivify_record_at(addr, 40, key, b"1234567890", 0, false)?;
    let out = hlog.read_record(addr).await?;
    assert_eq!(out.value()?, b"1234567890");
    assert_eq!(out.header()?.filler_bytes(), 8, "富余空间必须转为松弛填充");

    // 原位松弛更新：新值 15 字节（对齐(16+4+15)=40）恰好填满槽位物理容量
    assert!(hlog.try_update_in_place(addr, key, b"012345678901234")?);
    assert_eq!(hlog.read_record(addr).await?.value()?, b"012345678901234");

    // RMW 闭包原位修改
    let r = hlog.try_modify_record_in_place(addr, key, |v| {
      v[0] = b'X';
      Some(())
    })?;
    assert!(r.is_some());
    assert_eq!(hlog.read_record(addr).await?.value()?, b"X12345678901234");

    // 键不匹配 / 值超容量（对齐(16+4+21)=48 > 40）→ 原位失败
    assert!(!hlog.try_update_in_place(addr, b"wrong", b"y")?);
    assert!(!hlog.try_update_in_place(addr, key, &[b'z'; 21])?);

    // 经 revivify_record_at 将同槽位改写为空值墓碑（槽位 32：记录对齐 24，富余 8 字节
    // 不足一个 Pad 头 → 吸纳为松弛填充，保留原位复活容量）
    hlog.revivify_record_at(addr, 32, key, b"", 0, true)?;
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

    // 尾部追加位置不受复活复用影响（复用旧槽位不推进 tail；R2/R3 对齐逻辑尺寸 24）
    assert_eq!(hlog.tail_address(), addr3 + 24);

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
/// 测试 21: 原位增长（零复制改长）臂与槽位容量门
///
/// 对标 libs/server/Storage/Functions/MainStore/RMWMethods.cs:InPlaceUpdater 的
/// APPEND / SETRANGE 两分支：旧数据一律不动，只把新字节落在旧值尾部或间隙之上
#[test]
fn test_grow_record_in_place() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_grow.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let config = HybridLogConfig::new(64 * 1024, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    let key = b"grow";
    // R1 占槽 40（16+4+20 对齐后恰 40）；R2 紧随其后，作为「越界写」探针
    let addr1 = hlog.append(key, &[b'x'; 20], 0, false)?;
    let addr2 = hlog.append(b"nb", b"neighbor", 0, false)?;
    assert_eq!(addr2, addr1 + 40);

    // 槽位内改写为短值：对齐逻辑 32、槽位 40 → 富余 8 字节吸纳为松弛填充
    hlog.revivify_record_at(addr1, 40, key, b"1234567890", 0, false)?;
    let out = hlog.read_record(addr1).await?;
    assert_eq!(out.header()?.filler_bytes(), 8, "富余须转为松弛填充");
    let tail_before = hlog.tail_address();

    // APPEND 臂：旧值 10 → 15，闭包只落 5 新字节
    let grown = hlog.try_grow_record_in_place(addr1, key, |cap, old_len| {
      assert_eq!(cap.len(), 20, "值区容量 = 槽位 40 - 头 16 - 键 4");
      assert_eq!(&cap[..old_len], b"1234567890", "旧数据必须原封不动");
      let total = old_len + 5;
      cap[old_len..total].copy_from_slice(b"ABCDE");
      Some(total)
    })?;
    assert_eq!(grown, Some(15), "槽位容得下即原位改长");

    let out = hlog.read_record(addr1).await?;
    assert_eq!(out.value()?, b"1234567890ABCDE");
    assert_eq!(
      out.header()?.physical_size(),
      40,
      "原位增长绝不挪动槽位物理尺寸"
    );
    assert_eq!(out.prev_address()?, 0, "原位增长不得改写链接前驱");
    assert_eq!(hlog.tail_address(), tail_before, "原位增长不得留痕尾部");
    assert_eq!(
      hlog.read_record(addr2).await?.value()?,
      b"neighbor",
      "写面须止于槽位值区，邻槽不受侵蚀"
    );

    // 再增长至容量上限 20（filler 归零，槽位仍 40）
    let grown = hlog.try_grow_record_in_place(addr1, key, |cap, old_len| {
      let total = old_len + 5;
      if total > cap.len() {
        return None;
      }
      cap[old_len..total].copy_from_slice(b"FGHIJ");
      Some(total)
    })?;
    assert_eq!(grown, Some(20));
    assert_eq!(
      hlog.read_record(addr1).await?.value()?,
      b"1234567890ABCDEFGHIJ"
    );
    assert_eq!(hlog.read_record(addr2).await?.value()?, b"neighbor");

    // 谎报超容量长度：容量门拒绝且不留半成品（无第二处长度发布口）
    assert_eq!(
      hlog.try_grow_record_in_place(addr1, key, |_, old_len| Some(old_len + 1))?,
      None
    );
    // 键不匹配（Tag 碰撞）：双检拒绝
    assert_eq!(
      hlog.try_grow_record_in_place(addr1, b"other", |_, _| Some(1))?,
      None
    );
    assert_eq!(
      hlog.read_record(addr1).await?.value()?,
      b"1234567890ABCDEFGHIJ",
      "被拒的原位增长不得改写逻辑值"
    );
    assert_eq!(hlog.tail_address(), tail_before);

    // 墓碑记录不可原位增长（复活归 try_revivify_in_chain 单点）
    hlog.revivify_record_at(addr1, 32, key, b"", 0, true)?;
    assert_eq!(
      hlog.try_grow_record_in_place(addr1, key, |_, old_len| Some(old_len + 1))?,
      None
    );

    // 只读区地址：与等长臂同口径整体降级
    hlog.revivify_record_at(addr1, 40, key, b"1234567890", 0, false)?;
    hlog.shift_read_only_address(64 * 1024);
    assert_eq!(
      hlog.try_grow_record_in_place(addr1, key, |_, old_len| Some(old_len + 1))?,
      None,
      "只读区不可原位修改"
    );

    info!("原位增长（零复制改长）测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 22: 原位增长臂的零长起点、容量上限与贴页边界
///
/// 对标 libs/server/Storage/Functions/MainStore/RMWMethods.cs:InPlaceUpdater 增长臂
/// 的物理准入（`TrySetContentLengths` 的 `oldFillerLen < inlineValueGrowth` 判定）：
/// 空值记录（0 长度起点）可原位长出、容量贴满即整体回 `None` 降级尾部追加，
/// 贴边占满整页的记录更不得越出页界写一个字节
#[test]
fn test_grow_record_in_place_zero_len_and_capacity_boundaries() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_grow_boundary.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    const PAGE: usize = 64 * 1024;
    let config = HybridLogConfig::new(PAGE, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    // 1. 零长度起点：32 字节槽位内的空值记录，容量全部为松弛富余
    let key = b"zero";
    let addr = hlog.append(key, &[b't'; 8], 0, false)?;
    let addr_next = hlog.append(b"nb0", b"after-zero", 0, false)?;
    let slot = (addr_next - addr) as usize;
    assert_eq!(slot, 32, "16 + 4 + 8 = 28 → 对齐 32");
    hlog.revivify_record_at(addr, slot, key, b"", 0, false)?;
    let empty = hlog.read_record(addr).await?;
    assert_eq!(empty.value()?, b"", "空值记录逻辑值长为 0");
    assert_eq!(
      empty.header()?.filler_bytes(),
      8,
      "缩写腾出的富余转为松弛填充"
    );

    // 空值记录原位长出 8 字节（0 长度 APPEND 的对位臂），旧值区无从复制
    let grown = hlog.try_grow_record_in_place(addr, key, |cap, old_len| {
      assert_eq!(old_len, 0);
      assert_eq!(cap.len(), 12, "容量 = 槽位 32 - 头 16 - 键 4");
      cap[old_len..old_len + 8].copy_from_slice(b"APPENDED");
      Some(old_len + 8)
    })?;
    assert_eq!(grown, Some(8));
    assert_eq!(hlog.read_record(addr).await?.value()?, b"APPENDED");
    assert_eq!(hlog.read_record(addr_next).await?.value()?, b"after-zero");

    // 容量上限：再长 5 字节即超 12 字节容量，闭包自守回 None，逻辑值不动
    assert_eq!(
      hlog.try_grow_record_in_place(addr, key, |cap, old_len| {
        let total = old_len + 5;
        (total <= cap.len()).then(|| {
          cap[old_len..total].copy_from_slice(b"TOO");
          total
        })
      })?,
      None
    );
    assert_eq!(hlog.read_record(addr).await?.value()?, b"APPENDED");
    // 谎报超容量长度（越界写字节也不得发布长度）
    assert_eq!(
      hlog.try_grow_record_in_place(addr, key, |_, old_len| Some(old_len + 99))?,
      None
    );
    assert_eq!(hlog.read_record(addr).await?.value()?, b"APPENDED");

    // 2. 贴页边界：单条记录物理尺寸恰等于页容量。先以一条整页记录逼出换页，
    //    使贴边记录恰落页首、槽位尾紧顶页界
    let pad_addr = hlog.append(b"p", &vec![b'P'; PAGE - HEADER_SIZE - 1], 0, false)?;
    assert_eq!(pad_addr % PAGE as u64, 0, "整页记录必被推换新页落页首");
    let edge_key = b"edge";
    let edge_val_len = PAGE - HEADER_SIZE - edge_key.len();
    let edge_addr = hlog.append(edge_key, &vec![b'Z'; edge_val_len], 0, false)?;
    assert_eq!(edge_addr, pad_addr + PAGE as u64);
    // 下一条记录必然再换页落在新页首，成为「越界写」探针
    let after_page_addr = hlog.append(b"nb1", b"after-page", 0, false)?;
    assert_eq!(
      after_page_addr,
      edge_addr + PAGE as u64,
      "贴边记录须恰好吃到页界，下一条落新页首"
    );
    let tail_before = hlog.tail_address();

    let grown = hlog.try_grow_record_in_place(edge_addr, edge_key, |cap, old_len| {
      assert_eq!(cap.len(), old_len, "贴页记录的容量即逻辑值长，富余为 0");
      let total = old_len + 1;
      (total <= cap.len()).then(|| {
        cap[old_len..total].copy_from_slice(b"!");
        total
      })
    })?;
    assert_eq!(grown, None, "贴页记录不可原位增长，整体降级尾部追加");
    assert_eq!(hlog.tail_address(), tail_before, "降级路径不得留痕尾部");
    assert_eq!(
      hlog.read_record(edge_addr).await?.value()?,
      vec![b'Z'; edge_val_len],
      "被拒的原位增长不得改写贴页记录"
    );
    assert_eq!(
      hlog.read_record(after_page_addr).await?.value()?,
      b"after-page",
      "原位增长写面绝不可越出页界侵蚀邻页"
    );

    info!("原位增长零长起点与容量/贴页边界防御测试通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 测试 23: 原位增长的页内帧字节差分与「整帧重发到尾部」对照臂（最小写证据）
///
/// 对标 libs/server/Storage/Functions/MainStore/RMWMethods.cs:InPlaceUpdater 的 APPEND
/// 臂 :799-834（`TrySetContentLengths(totalLength)` 原位改长 +
/// `appendValue.CopyTo(logRecord.ValueSpan.Slice(originalLength))` 只拷新字节、旧数据
/// 一律不动）与 CopyUpdater 臂 :1040（本票之前的旧实现形态：整值另落一条新记录）。
///
/// 帧快照 = 页内该槽位解码可见的全部帧字节（16 字节头两字 + 键 + 逻辑值），增长前后对照：
/// - 头字节改动只可能落在 RDH 原子字 [RDH_WORD_OFFSET, HEADER_SIZE) 与 RecordInfo 字的
///   最高字节（原位发布的脏标记位），且该字至多翻上一枚位（前驱地址、墓碑/密封位不动）；
/// - 键区逐字节相同，旧值区段是新值的严格前缀（零复制、零搬迁的铁证）；
/// - 邻槽帧逐字节不动、`tail_address` 零推进（原位臂不留痕尾部）；
/// - 对照臂按旧实现装配（同键整值 `append` 落尾）：原槽位一字节未改、同址读回仍是改长
///   前的旧值，代价是页尾新增一整个 40 字节槽位。两臂可观测量互斥 ⇒ 原位臂一旦悄悄退化
///   成整帧重写，本用例「同址读回新全值」与「尾部零推进」双双必红。
#[test]
fn test_grow_record_in_place_frame_byte_diff() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("hlog_grow_frame_diff.db");
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let epoch = Arc::new(LightEpoch::new(16));

    let config = HybridLogConfig::new(64 * 1024, 16, 0.5)?;
    let hlog = HybridLog::new(config, device, epoch)?;

    let key = b"dfeq";
    let base_val = b"1234567890";
    let extra = b"ABCDE";
    let grown_val = [base_val.as_slice(), extra.as_slice()].concat();

    // 页内帧快照：头 16 字节 + RecordInfo 字 + 键 + 逻辑值
    let frame = |addr: u64| -> ([u8; HEADER_SIZE], u64, Vec<u8>, Vec<u8>) {
      hlog
        .with_memory_record(addr, |rec| {
          Ok((
            rec.header.to_bytes(),
            rec.header.prev_address,
            rec.key.to_vec(),
            rec.value.to_vec(),
          ))
        })
        .expect("帧快照不得失败")
        .expect("槽位常驻内存可变区")
    };
    let is_modified = |addr: u64| {
      hlog
        .with_memory_record(addr, |rec| Ok(rec.is_modified()))
        .expect("脏标记探测不得失败")
        .expect("槽位常驻内存可变区")
    };

    // 1. 甲臂起点：槽位 40（16 + 4 + 20 对齐）内落 10 字节旧值，腾空 8 字节为松弛填充
    let addr = hlog.append(key, &[b'x'; 20], 0, false)?;
    let nb_addr = hlog.append(b"nb4", b"neighbor4", 0, false)?;
    hlog.revivify_record_at(addr, 40, key, base_val, 0, false)?;
    let before = frame(addr);
    let nb_before = frame(nb_addr);
    let tail_before = hlog.tail_address();
    assert_eq!(before.3, base_val.to_vec());
    assert!(!is_modified(addr), "起点帧尚未置脏标记位");

    // 2. 原位增长 5 字节（旧值零复制，只落新字节）
    let grown = hlog.try_grow_record_in_place(addr, key, |cap, old_len| {
      let total = old_len + extra.len();
      (total <= cap.len()).then(|| {
        cap[old_len..total].copy_from_slice(extra);
        total
      })
    })?;
    assert_eq!(grown, Some(grown_val.len()));

    // 3. 帧字节差分：改动面恰为「RDH 原子字 + 脏标记位 + 新增段」
    let after = frame(addr);
    assert_eq!(
      after.3, grown_val,
      "同一槽位即读回新全值——旧实现（整帧重发）在此必红"
    );
    let hdr_changed: Vec<usize> = (0..HEADER_SIZE)
      .filter(|&i| before.0[i] != after.0[i])
      .collect();
    for &i in &hdr_changed {
      assert!(
        (RDH_WORD_OFFSET..HEADER_SIZE).contains(&i) || i == RDH_WORD_OFFSET - 1,
        "原位增长改动了不该动的头字节 {i}，全部头字节改动集 {hdr_changed:?}"
      );
    }
    assert!(is_modified(addr), "原位发布须置脏标记，否则页不会落盘");
    assert_eq!(before.1 | after.1, after.1, "RecordInfo 字只增不减");
    assert_eq!(
      (before.1 ^ after.1).count_ones(),
      1,
      "RecordInfo 字至多翻上一枚位（脏标记），前驱与墓碑/密封位不动"
    );
    assert_eq!(before.2, after.2, "键区不得被改写");
    assert_eq!(
      &after.3[..base_val.len()],
      before.3.as_slice(),
      "旧值前缀必须逐字节原封不动——零复制的字节证据"
    );
    assert_eq!(&after.3[base_val.len()..], extra, "新增段一次落齐");

    // 4. 槽位外零侵蚀、尾部零推进
    assert_eq!(frame(nb_addr), nb_before, "邻槽帧逐字节不动");
    assert_eq!(hlog.tail_address(), tail_before, "原位臂不得留痕尾部");
    let touched = hdr_changed.len() + (after.3.len() - before.3.len());

    // 5. 对照臂：旧实现形态（整值物化后另落一条尾部新记录）
    let old_key = b"olde";
    let old_addr = hlog.append(old_key, &[b'y'; 20], 0, false)?;
    hlog.revivify_record_at(old_addr, 40, old_key, base_val, 0, false)?;
    let old_before = frame(old_addr);
    let old_tail_before = hlog.tail_address();
    let whole = [old_before.3.as_slice(), extra.as_slice()].concat();
    hlog.append(old_key, &whole, 0, false)?;
    let old_after = frame(old_addr);
    assert_eq!(
      old_after, old_before,
      "整帧重发一字节也不碰原槽位：与甲臂写面互斥 ⇒ 甲臂断言绝非空转"
    );
    assert_eq!(old_after.3, base_val.to_vec(), "旧实现下原地址读不到新值");
    let reemit = hlog.tail_address() - old_tail_before;
    assert_eq!(reemit, 40, "旧实现的代价：页尾新增一整个 40 字节槽位");
    assert!(
      touched * 4 <= reemit as usize,
      "原位写面 {touched} 字节须至多是整帧重发 {reemit} 字节的四分之一"
    );

    info!(
      "原位增长页内帧字节差分与整帧重发对照测试通过: 原位 {touched} 字节 vs 重发 {reemit} 字节"
    );
    aok::Result::<()>::Ok(())
  })?;

  OK
}
