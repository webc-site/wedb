//! 复活臂 / 追加臂键长位段硬门对称性回归（两臂共用门 validate_append_args 收口）
//!
//! 缺陷形：24 位内联键长位段顶值（PAD_KEY_LEN）保留为 Pad 哨兵，合法上限
//! MAX_KEY_LEN；append 臂经 codec::build_header 硬拒越界，复活臂 revivify_record_at
//! 曾旁路该门——越界键长经 pack_rdh_word 静默掩码低 24 位，头推导布局与按真实键长
//! 落笔的键字节自歧（顶值恰落 Pad 哨兵时记录被扫描/点查整条误判跳过，静默失踪），
//! 与 append 臂拒写不互逆。本组用例在 32MiB 页（键长首个越界值 2^24-1 可容于页内、
//! 位段门是唯一拦截点）下锁定两臂对称拒写与 MAX_KEY_LEN 边界可写可读。
//!
//! 对标 C#：内联位段从不单独充当长度真源（RecordDataHeader.cs:86-98 窄位段 +
//! LogRecord.cs:GetObjectLogRecordStartPositionAndLength 高位另存组合还原）；
//! rust 简化形移除 overflow 双段机制后，写侧位段硬门即唯一合法性收口。

use std::sync::Arc;

use aok::{OK, Void};
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wepoch::LightEpoch;
use whlog::{
  DEFAULT_INITIAL_ADDRESS, Error, HybridLog, HybridLogConfig, RecordOutput, RevivifyArgs,
};
use wrecord::{Error as WrecordError, MAX_KEY_LEN, PAD_KEY_LEN, record_size};

/// 32MiB 页 + 2 页环形缓冲（单实例常驻 64MB，与 large_page 组同量级）：
/// 页容量 > PAD_KEY_LEN，越界键长不再被 RecordTooLarge 页容量门先拦，
/// 位段硬门成为唯一拒写点（whlog 自有 config 页仅受 2 的幂与 4GiB 钳制）
fn key_gate_config() -> HybridLogConfig {
  HybridLogConfig::new(32 * 1024 * 1024, 2, 0.5).expect("32MiB 页配置合法")
}

/// 测试 1：键长恰为 MAX_KEY_LEN+1（即 Pad 哨兵顶值）时两臂对称拒写
/// KeyLengthOverflow，复活臂槽位不落笔不发布（原记录完好），append 臂 tail 不动
#[compio::test]
async fn both_arms_reject_key_len_at_pad_sentinel() -> Void {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("gate_reject.db"),
  )?);
  let epoch = Arc::new(LightEpoch::new(16));
  let hlog = HybridLog::new(key_gate_config(), device, epoch)?;

  // 可变区先落一条小记录，作为复活臂的被覆写槽位与拒写后完好性锚
  let (addr, _) = hlog.append(b"orig", b"payload", 0, false)?;
  let tail_after_orig = hlog.tail_address();

  // 首个越界键长 = MAX_KEY_LEN + 1 = PAD_KEY_LEN：未收口时掩码后恰落 Pad 哨兵，
  // 复活出的记录被扫描/点查整条误判跳过（静默失踪），且 RDH 已发布无法撤销
  let over_key = vec![b'K'; PAD_KEY_LEN as usize];

  // append 臂：build_header 既有硬门（经共用门前置，tail 不得推进）
  let err = hlog.append(&over_key, b"v", 0, false).unwrap_err();
  assert!(
    matches!(&err, Error::Record(WrecordError::KeyLengthOverflow(n))
      if *n == PAD_KEY_LEN as usize),
    "append 臂越界键长必须拒为 KeyLengthOverflow: {err:?}"
  );
  assert_eq!(
    hlog.tail_address(),
    tail_after_orig,
    "被拒的追加不得推进 tail"
  );

  // 复活臂：与 append 臂同一错误对称拒写（缺陷形下此调用静默成功后失踪）
  let err = hlog
    .revivify_record_at(&RevivifyArgs {
      addr,
      slot_size: 32 * 1024 * 1024,
      key: &over_key,
      val: b"v",
      prev_addr: 0,
      is_tombstone: false,
      in_new_version: false,
    })
    .unwrap_err();
  assert!(
    matches!(&err, Error::Record(WrecordError::KeyLengthOverflow(n))
      if *n == PAD_KEY_LEN as usize),
    "复活臂越界键长必须与 append 臂对称拒为 KeyLengthOverflow: {err:?}"
  );

  // 拒写不得触碰槽位：原记录键值与物理布局完好
  let out = hlog.read_record(addr).await?;
  assert_eq!(out.key()?, b"orig");
  assert_eq!(out.value()?, b"payload");
  assert!(matches!(out, RecordOutput::Memory(_)));
  assert_eq!(hlog.tail_address(), tail_after_orig);

  // 扫描同样不见损坏记录、原记录在案
  let mut key_lens = Vec::new();
  hlog
    .scan(0, hlog.tail_address(), |_, rec| {
      key_lens.push(rec.key().len());
      Ok(true)
    })
    .await?;
  assert_eq!(key_lens, vec![b"orig".len()]);

  OK
}

/// 测试 2：键长恰为 MAX_KEY_LEN（合法顶格）两臂写入成功，点查与扫描可读、
/// 不误判 Pad，头位段与实写键字节严格一致
#[compio::test]
async fn both_arms_admit_max_key_len_boundary() -> Void {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("gate_admit.db"),
  )?);
  let epoch = Arc::new(LightEpoch::new(16));
  let hlog = HybridLog::new(key_gate_config(), device, epoch)?;

  // append 臂顶格：16MiB-1 键 + 4 字节值（记录约 16MiB，整页可容）
  let max_key = vec![b'K'; MAX_KEY_LEN];
  let (addr, _) = hlog.append(&max_key, b"abcd", 0, false)?;
  let out = hlog.read_record(addr).await?;
  assert_eq!(out.key()?, max_key.as_slice(), "顶格键长点查必须完整读回");
  assert_eq!(out.value()?, b"abcd");

  // 复活臂顶格：同长异指纹键覆写同槽位（slot_size = 记录对齐逻辑尺寸，零富余）
  let max_key2 = vec![b'J'; MAX_KEY_LEN];
  let rec_size = record_size(MAX_KEY_LEN, b"wxyz".len());
  let pad = hlog
    .revivify_record_at(&RevivifyArgs {
      addr,
      slot_size: rec_size,
      key: &max_key2,
      val: b"wxyz",
      prev_addr: 0,
      is_tombstone: false,
      in_new_version: false,
    })
    .expect("顶格键长复活写入必须成功");
  assert!(pad.is_none(), "slot_size 恒等于记录尺寸，不得报出切出块");
  let out = hlog.read_record(addr).await?;
  assert_eq!(out.key()?, max_key2.as_slice());
  assert_eq!(out.value()?, b"wxyz");
  assert_eq!(
    out.header()?.key_len() as usize,
    MAX_KEY_LEN,
    "头位段键长必须与实写键字节一致（不与 Pad 哨兵混判）"
  );

  // 扫描不误判 Pad：顶格键长记录完整交付
  let mut delivered = Vec::new();
  hlog
    .scan(0, hlog.tail_address(), |a, rec| {
      delivered.push((a, rec.key().first().copied().unwrap_or(0), rec.key().len()));
      Ok(true)
    })
    .await?;
  assert_eq!(delivered, vec![(addr, b'J', MAX_KEY_LEN)]);

  OK
}

/// 测试 3：键长跨位段回绕值（2^24-1+2^24，掩码后仍落 Pad 哨兵）在两臂位段门内
/// 先于页容量门被拒为 KeyLengthOverflow（位段界校验独立于页容量，两臂同门同序）
#[compio::test]
async fn both_arms_reject_wrapped_key_len() -> Void {
  let dir = tempdir()?;
  let device = Arc::new(SegmentedDevice::single_file(
    dir.path().join("gate_wrap.db"),
  )?);
  let epoch = Arc::new(LightEpoch::new(16));
  let hlog = HybridLog::new(key_gate_config(), device, epoch)?;

  let (addr, _) = hlog.append(b"orig", b"payload", 0, false)?;

  // 回绕值：低 24 位与 PAD_KEY_LEN 全同，未收口时头键长被掩码回落到哨兵
  let wrapped_len = PAD_KEY_LEN as usize + (1usize << 24);
  let wrapped_key = vec![b'W'; wrapped_len];

  let err = hlog.append(&wrapped_key, b"v", 0, false).unwrap_err();
  assert!(
    matches!(&err, Error::Record(WrecordError::KeyLengthOverflow(n)) if *n == wrapped_len),
    "append 臂回绕键长必须拒为 KeyLengthOverflow: {err:?}"
  );
  let err = hlog
    .revivify_record_at(&RevivifyArgs {
      addr,
      slot_size: 32 * 1024 * 1024,
      key: &wrapped_key,
      val: b"v",
      prev_addr: 0,
      is_tombstone: false,
      in_new_version: false,
    })
    .unwrap_err();
  assert!(
    matches!(&err, Error::Record(WrecordError::KeyLengthOverflow(n)) if *n == wrapped_len),
    "复活臂回绕键长必须拒为 KeyLengthOverflow: {err:?}"
  );
  assert_eq!(
    hlog.tail_address(),
    addr + record_size(4, 7) as u64,
    "tail 恒不动"
  );

  // 槽位完好 + 起始地址不变式：被拒写入零落笔
  let out = hlog.read_record(addr).await?;
  assert_eq!(out.key()?, b"orig");
  assert_eq!(addr, DEFAULT_INITIAL_ADDRESS);

  OK
}
