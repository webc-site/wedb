//! 区间索引元记录存根治愈单元集成测试（自 src/range_index/heal.rs 外迁）
//!
//! 覆盖：
//! 1. clear_flushed_patch 就地改位，Meta 与其余字段原样保留；
//! 2. 存根之后长扩展逐字保留，不截断；
//! 3. clear_flushed_patch 幂等性；
//! 4. recreate_patch 重绑句柄并清恢复位；
//! 5. recreate_patch 幂等性；
//! 6. mark_recovered_patch 句柄清零置恢复位与幂等性；
//! 7. transfer_out_patch 句柄清零置转出位与幂等性；
//! 8. patch 拒绝非分层域元记录与定长不足记录，合法接收升阶集合复合记录与空索引。

use wbftree::{RangeIndexStub, StorageBackendType};
use wkv::{
  STUB_WINDOW_END, STUB_WINDOW_START, clear_flushed_patch, encode_meta_stub_record,
  mark_recovered_patch, patch_stub_record, range_index_stub_of, recreate_patch, transfer_out_patch,
};
use wval::{GarnetObjectType, META_VALUE_SIZE, MetaValue};

/// 构造测试基准存根 (句柄/缓存/契约长度均为可辨识值)
fn base_stub(tree_handle: u64) -> RangeIndexStub {
  RangeIndexStub::new(
    tree_handle,
    4096,
    4,
    1024,
    128,
    4096,
    StorageBackendType::Disk,
  )
}

/// 编码 RI 元记录 (Meta + 存根定长栈编码)
fn record(stub: &RangeIndexStub, size: u64) -> Vec<u8> {
  let meta = MetaValue::new(1, GarnetObjectType::RangeIndex, size);
  encode_meta_stub_record(&meta, stub).to_vec()
}

/// 取治愈后帧内的存根 (存根窗口界内性由内核保证)
fn stub_in(val: &[u8]) -> RangeIndexStub {
  RangeIndexStub::decode(&val[STUB_WINDOW_START..STUB_WINDOW_END]).expect("治愈帧存根必须可解码")
}

/// RIPROMOTE / 紧缩搬迁共用的清 Flushed 治愈：就地改位，Meta 段与句柄等其余字段原样保留
#[test]
fn clear_flushed_patch_heals_in_place() {
  let mut stub = base_stub(0xdead);
  stub.set_flushed(true);
  let src = record(&stub, 3);
  let mut healed = src.clone();
  assert!(patch_stub_record(&mut healed, clear_flushed_patch));
  assert_eq!(&healed[..META_VALUE_SIZE], &src[..META_VALUE_SIZE]);
  let out = stub_in(&healed);
  assert!(!out.is_flushed());
  assert_eq!(out.tree_handle, 0xdead);
  assert_eq!(out.cache_size, 4096);
}

/// 值体长度无上限：存根之后的扩展字节（含超旧 [u8;128] 栈缓冲的长扩展）逐字保留，
/// 杜绝旧实现 `val.len().min(128)` 静默截断丢尾
#[test]
fn patch_preserves_extension_beyond_legacy_stack_cap() {
  let ext: Vec<u8> = (0..200u32).map(|i| i as u8).collect();
  let mut stub = base_stub(0xabcd);
  stub.set_flushed(true);
  stub.set_transferred(true);
  let mut healed = record(&stub, 5);
  healed.extend_from_slice(&ext);
  let total = healed.len();
  assert!(patch_stub_record(&mut healed, clear_flushed_patch));
  assert_eq!(healed.len(), total, "治愈不得改变值体长度");
  assert_eq!(&healed[STUB_WINDOW_END..], &ext[..], "扩展字段必须逐字保留");
  let out = stub_in(&healed);
  assert!(!out.is_flushed());
  assert!(out.is_transferred(), "非目标位不得被顺带改写");
}

/// 治愈内核幂等：未置 Flushed 的记录零写跳过
#[test]
fn clear_flushed_patch_idempotent_when_not_flushed() {
  let mut src = record(&base_stub(7), 1);
  assert!(!patch_stub_record(&mut src, clear_flushed_patch));
}

/// RIRESTORE 治愈：跨重启陈旧句柄重绑当前树并清恢复位，其余字段原样保留
#[test]
fn recreate_patch_rebinds_handle_and_clears_recovered() {
  let mut stub = base_stub(0x11);
  stub.set_recovered(true);
  let src = record(&stub, 2);
  let mut healed = src.clone();
  assert!(patch_stub_record(&mut healed, |s| recreate_patch(s, 0x22)));
  assert_eq!(&healed[..META_VALUE_SIZE], &src[..META_VALUE_SIZE]);
  let out = stub_in(&healed);
  assert_eq!(out.tree_handle, 0x22);
  assert!(!out.is_recovered());
  assert_eq!(out.cache_size, 4096);
}

/// RIRESTORE 治愈幂等：句柄已绑定当前树且恢复位已清时零写跳过
#[test]
fn recreate_patch_idempotent_when_bound_and_clear() {
  let mut src = record(&base_stub(9), 2);
  assert!(!patch_stub_record(&mut src, |s| recreate_patch(s, 9)));
}

/// 恢复期治愈 (对标 C# MarkRecoveredFromCheckpoint)：句柄清零 + 置恢复位；已是该态零写
#[test]
fn mark_recovered_patch_zeroes_handle_and_is_idempotent() {
  let mut src = record(&base_stub(0x1234), 1);
  assert!(patch_stub_record(&mut src, mark_recovered_patch));
  let out = stub_in(&src);
  assert_eq!(out.tree_handle, 0);
  assert!(out.is_recovered());
  assert!(
    !patch_stub_record(&mut src, mark_recovered_patch),
    "二次恢复零写"
  );
}

/// 所有权转出治愈 (对标 C# ClearTreeHandle + SetTransferredFlag)；已是该态零写
#[test]
fn transfer_out_patch_clears_handle_and_is_idempotent() {
  let mut src = record(&base_stub(0x99), 1);
  assert!(patch_stub_record(&mut src, transfer_out_patch));
  let out = stub_in(&src);
  assert_eq!(out.tree_handle, 0);
  assert!(out.is_transferred());
  assert!(!patch_stub_record(&mut src, transfer_out_patch));
}

/// 公共校验面：非分层域元记录与定长不足的记录一律零写跳过；
/// 升阶集合复合记录（原集合类型 + 存根窗口）是合法治愈目标
#[test]
fn patch_rejects_non_range_index_and_short_record() {
  let mut flushed = base_stub(1);
  flushed.set_flushed(true);
  // 非分层域记录（Null/All 形态的 Meta + 任意尾字节）一律拒绝：
  // 存根窗口只对分层域复合记录落笔
  let null_meta = MetaValue::new(1, GarnetObjectType::Null, 7);
  let mut not_ri = encode_meta_stub_record(&null_meta, &flushed).to_vec();
  assert!(!patch_stub_record(&mut not_ri, clear_flushed_patch));
  assert!(!patch_stub_record(&mut not_ri, mark_recovered_patch));
  assert!(range_index_stub_of(&not_ri).is_none());
  assert_eq!(
    not_ri,
    encode_meta_stub_record(&null_meta, &flushed).to_vec()
  );
  let all_meta = MetaValue::new(1, GarnetObjectType::All, 7);
  let mut all_rec = encode_meta_stub_record(&all_meta, &flushed).to_vec();
  assert!(!patch_stub_record(&mut all_rec, clear_flushed_patch));
  // 升阶集合复合记录（原集合类型保留，如 Hash）：合法治愈目标，
  // 与 RI.CREATE 形态同链同权
  let hash_meta = MetaValue::new(1, GarnetObjectType::Hash, 7);
  let mut tiered = encode_meta_stub_record(&hash_meta, &flushed).to_vec();
  assert!(patch_stub_record(&mut tiered, clear_flushed_patch));
  assert!(range_index_stub_of(&tiered).is_some());
  // 空索引（size=0 的存活 RI 元记录，RI.CREATE 即此态）同样纳入治愈
  let mut empty = record(&flushed, 0);
  assert!(patch_stub_record(&mut empty, clear_flushed_patch));
  // 定长不足
  let mut short = [0u8; 8];
  assert!(!patch_stub_record(&mut short, clear_flushed_patch));
  assert!(range_index_stub_of(&short).is_none());
}
