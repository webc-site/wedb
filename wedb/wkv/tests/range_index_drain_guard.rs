//! 区间索引迁移与排空守卫判据测试（自 src/range_index/migration.rs 外迁）
//!
//! 覆盖：
//! 1. 守卫判据：同 key_id 在册记录放行；缺失 / 定长不足 / key_id 漂移（live 并发重建）中止；
//! 2. 删空臂守卫判据（expect_size 形）：key_id 相符且计数归零放行，并发 SET 复活（size >= 1）中止。

use wbftree::RANGE_INDEX_STUB_SIZE;
use wkv::drain_guard_ok;
use wval::{GarnetObjectType, META_VALUE_SIZE, MetaValue};

/// 守卫判据：同 key_id 在册记录放行；缺失 / 定长不足 / key_id 漂移
/// （live 并发重建）一律保守中止；expect_size 在场时并发复活（size 漂移）
/// 亦判否
#[test]
fn drain_guard_judges_by_key_id() {
  let mut record = MetaValue::new(7, GarnetObjectType::RangeIndex, 3)
    .to_bytes()
    .to_vec();
  record.extend_from_slice(&[0u8; RANGE_INDEX_STUB_SIZE]);
  assert!(
    drain_guard_ok(Some(&record), 7, None),
    "在册 key_id 相符须放行"
  );
  assert!(!drain_guard_ok(None, 7, None), "记录缺失须中止");
  assert!(
    !drain_guard_ok(Some(&record[..4]), 7, None),
    "定长不足须中止"
  );
  let mut drifted = MetaValue::new(9, GarnetObjectType::RangeIndex, 3)
    .to_bytes()
    .to_vec();
  drifted.extend_from_slice(&record[META_VALUE_SIZE..]);
  assert!(
    !drain_guard_ok(Some(&drifted), 7, None),
    "并发重建 key_id 漂移须中止"
  );
}

/// 删空臂守卫判据（expect_size 形）：key_id 相符且计数归零才放行墓碑；
/// 窗内并发 SET 复活（size >= 1）即判否，收敛为「字段删后又有写」的
/// 正常串行终态
#[test]
fn drain_guard_judges_expected_size() {
  let mut empty_record = MetaValue::new(7, GarnetObjectType::RangeIndex, 0)
    .to_bytes()
    .to_vec();
  empty_record.extend_from_slice(&[0u8; RANGE_INDEX_STUB_SIZE]);
  assert!(
    drain_guard_ok(Some(&empty_record), 7, Some(0)),
    "key_id 相符且 size 归零须放行"
  );
  let mut resurrected = MetaValue::new(7, GarnetObjectType::RangeIndex, 1)
    .to_bytes()
    .to_vec();
  resurrected.extend_from_slice(&empty_record[META_VALUE_SIZE..]);
  assert!(
    !drain_guard_ok(Some(&resurrected), 7, Some(0)),
    "并发 SET 复活（size >= 1）须中止排空"
  );
  // expect_size 缺席时不复核计数（迁移三消费点纯 key_id 形维持零回归）
  assert!(
    drain_guard_ok(Some(&resurrected), 7, None),
    "expect_size 缺席须维持纯 key_id 判据"
  );
}
