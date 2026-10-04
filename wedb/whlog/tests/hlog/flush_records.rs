//! 页内记录链走查单点内核测试（对标 C# ObjectAllocatorImpl.cs:FlushRecordsInRange
//! 的 OnFlush 走查臂与 ReadCache.cs:ReadCacheEvict 的链恢复走查臂共用判据）

use std::sync::Arc;

use aok::{OK, Void};
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{
  DEFAULT_INITIAL_ADDRESS, HybridLog, HybridLogConfig, SECTOR_ALIGNMENT, for_each_record_in_page,
};
use wrecord::{HEADER_SIZE, RecordHeader, record_size};

/// 构建小页混合日志（4096 字节页，容量小，便于制造页尾 pad 与跨页序列）
fn build_hlog() -> aok::Result<(tempfile::TempDir, HybridLog<SegmentedDevice>)> {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("walk.db"))?);
  let epoch = Arc::new(LightEpoch::new(16));
  let config = HybridLogConfig::new(SECTOR_ALIGNMENT, 16, 0.5)?;
  Ok((dir, HybridLog::new(config, device, epoch)?))
}

/// 走查记录序列恰等于追加序列：初始页自 initial_address 起步（前 64 字节保留区
/// 不产出幽灵记录）、页尾 pad 与零区正确终止、逻辑地址与键值精确交付
#[compio::test]
async fn test_flush_records_in_range_visits_all_appended() -> Void {
  let (_dir, hlog) = build_hlog()?;
  let val_400 = vec![b'V'; 400];
  let mut appended = Vec::new();
  for i in 0..12u32 {
    let key = format!("k{i:03}");
    let (addr, _) = hlog.append(key.as_bytes(), &val_400, 0, false)?;
    appended.push((addr, key.into_bytes()));
  }

  let mut visited = Vec::new();
  hlog.flush_records_in_range(0, 1, |header, addr, key, val| {
    assert!(!header.is_pad() && !header.is_null() && !header.is_tombstone());
    assert_eq!(val, vec![b'V'; 400].as_slice());
    visited.push((addr, key.to_vec()));
    true
  });
  assert_eq!(visited, appended, "走查序列必须与追加序列逐条相等");
  assert_eq!(
    visited.first().unwrap().0,
    DEFAULT_INITIAL_ADDRESS,
    "初始页首条记录应从 initial_address 起步"
  );
  OK
}

/// OnFlush 面原位改值：闭包内改写值字节直接落回页缓冲，读路径立即可见
/// （对标 C# OnFlush 在驻留记录上就地置 IsFlushed 旗标）
#[compio::test]
async fn test_flush_records_in_range_mutates_in_place() -> Void {
  let (_dir, hlog) = build_hlog()?;
  let (addr, _) = hlog.append(b"stub", b"dirty!", 0, false)?;
  hlog.flush_records_in_range(0, 0, |header, record_addr, key, val| {
    assert_eq!(record_addr, addr);
    assert_eq!(key, b"stub");
    assert!(!header.is_closed());
    val.copy_from_slice(b"flush!");
    true
  });
  let out = hlog.read_record(addr).await?;
  assert_eq!(out.value()?, b"flush!");
  OK
}

/// 闭包返回 false 提前终止整段走查；未驻留页静默跳过（不 panic、不产出记录）
#[compio::test]
async fn test_flush_records_in_range_early_stop_and_skip_unloaded() -> Void {
  let (_dir, hlog) = build_hlog()?;
  let val = vec![b'x'; 400];
  for i in 0..12u32 {
    hlog.append(format!("k{i:03}").as_bytes(), &val, 0, false)?;
  }

  let mut count = 0usize;
  hlog.flush_records_in_range(0, 1, |_header, _addr, _key, _val| {
    count += 1;
    false
  });
  assert_eq!(count, 1, "返回 false 必须即刻终止全部走查");

  let mut phantom = 0usize;
  hlog.flush_records_in_range(1u64 << 40, 1u64 << 40, |_h, _a, _k, _v| {
    phantom += 1;
    true
  });
  assert_eq!(phantom, 0);
  OK
}

/// 合成页写入一条完整记录头 + 键值，返回其物理尺寸
///
/// prev 非零：创世空键记录（prev=0 且键值皆零）与零头不可区分（`is_null`），
/// 须避开 null 判据才能测到「空键可步进」形态
fn put_record(
  page: &mut [u8],
  offset: usize,
  key_len: usize,
  val_len: usize,
  sealed: bool,
) -> usize {
  let size = record_size(key_len, val_len);
  let mut header = RecordHeader::from_raw(8, key_len as u32, val_len as u32);
  if sealed {
    header.set_sealed(true);
  }
  page[offset..offset + HEADER_SIZE].copy_from_slice(&header.to_bytes());
  size
}

/// 只读走查内核判据（对标 ReadCacheEvict 面）：零头终止页链；页尾 pad 按 pad_extent
/// 越过填充槽抵页尾后自然收束；sealed 记录照常步进不截断（跳过与否归调用方闭包裁决）；
/// 空键记录可步进
#[test]
fn test_for_each_record_in_page_predicate() -> Void {
  let mut page = vec![0u8; SECTOR_ALIGNMENT];
  let mut off = 0usize;
  off += put_record(&mut page, off, 4, 4, false);
  off += put_record(&mut page, off, 4, 4, true);
  off += put_record(&mut page, off, 0, 0, false);
  let pad = RecordHeader::pad(SECTOR_ALIGNMENT - off);
  page[off..off + HEADER_SIZE].copy_from_slice(&pad.to_bytes());

  let mut seen = Vec::new();
  for_each_record_in_page(&page, 0, |header, offset, key, val| {
    seen.push((offset, key.len(), val.len(), header.is_closed()));
    true
  });
  assert_eq!(
    seen.len(),
    3,
    "pad 与零区前的三条记录（含 sealed、空键）必须全部扫出"
  );
  assert_eq!(
    seen[1],
    (record_size(4, 4), 4, 4, true),
    "sealed 记录须以 closed 形态交付并续步"
  );
  assert_eq!(seen[2].1, 0, "空键记录键长应为 0 且可步进");

  // 闭包返回 false 提前终止
  let mut count = 0usize;
  for_each_record_in_page(&page, 0, |_h, _o, _k, _v| {
    count += 1;
    false
  });
  assert_eq!(count, 1);

  // 走查起点即页尾 pad：pad 覆盖至页尾，越过填充槽后抵页界自然收束，零产出
  let mut from_pad = 0usize;
  for_each_record_in_page(&page, off, |_h, _o, _k, _v| {
    from_pad += 1;
    true
  });
  assert_eq!(from_pad, 0, "起点为覆盖至页尾的 pad 时无后继记录可产出");
  OK
}

/// 页内中途 pad 不终止走查：pad 之后仍排有存活记录时，走查按 [`RecordHeader::pad_extent`]
/// 越过填充槽继续扫出后续记录（对标跨页写入与原位槽位复活收缩打 pad 后其后仍可能有存活
/// 记录的口径——旧实现在此处判 pad 即终止，漏扫 pad 之后的存活记录）
#[test]
fn test_for_each_record_in_page_continues_past_mid_page_pad() -> Void {
  let mut page = vec![0u8; SECTOR_ALIGNMENT];
  let mut off = 0usize;
  off += put_record(&mut page, off, 8, 8, false);
  // 中途收缩产生的 pad：仅覆盖一段填充区，其后紧接存活记录
  let pad_len = record_size(16, 0);
  let pad = RecordHeader::pad(pad_len);
  page[off..off + HEADER_SIZE].copy_from_slice(&pad.to_bytes());
  off += pad_len;
  let tail_off = off;
  put_record(&mut page, off, 4, 4, false);

  let mut offs = Vec::new();
  let natural = for_each_record_in_page(&page, 0, |_h, offset, _k, _v| {
    offs.push(offset);
    true
  });
  assert!(natural, "走查须自然收束至页尾");
  assert_eq!(
    offs,
    vec![0, tail_off],
    "pad 之前的记录与 pad 之后的存活记录均须被走到"
  );
  OK
}
