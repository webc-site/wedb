//! 集合对象成员字节共享驻留与记账口径测试（纯集合层，不牵引擎）
//!
//! 票 task/ing/r6-mem-zset-member-byte-dedup-accounting 验收：
//! 1. 记账「计一份」：成员字节由主容器 update_size 单点计入，过期账本只加槽位
//!    （对位 C# UpdateSize + UpdateExpirationSize 的 byte[] 引用共享口径）；
//! 2. 物理驻留单份：成员字节跨 散列/有序视图/过期账本 共享同一 Arc 句柄，
//!    以 `Arc::strong_count` 直证（若仍是逐容器复制，计数恒为 1）；
//! 3. 升阶体积门限吃真实记账值：成员数不变、记账体积超阈值即触发。
//!
//! 自研依据: doc/zh/collection.md 信封内存计量（C# 对应对象 heapMemory 计量 libs/server/Storage/Session/ObjectStore）

use std::sync::Arc;

use wbase::{
  heap::{CONTAINER_BASE, EXPIRY_STRUCT_BASE, SLOT, round_up_ptr},
  time::now_ticks,
};
use wcol::{
  HashObject, SortedSetObject, TIERED_PROMOTE_BYTES, TIERED_PROMOTE_THRESHOLD,
  hash::hash_object::HashExpireResult, should_promote,
  zset::sorted_set_object::SortedSetExpireResult,
};
use wresp::options::ExpireOption;

/// 大成员长度（>64B，round_up_ptr 后仍主导槽位项，凸显字节驻留份额）
const MEMBER_LEN: usize = 128;
/// 较小的 value 长度（hash 臂用）
const VALUE_LEN: usize = 64;

#[test]
fn zset_member_bytes_shared_and_accounted_once() {
  let mut z = SortedSetObject::new();
  let base = z.heap_memory_size;
  assert_eq!(base, CONTAINER_BASE * 2);

  let member = vec![1_u8; MEMBER_LEN];
  let per_entry = round_up_ptr(MEMBER_LEN) as i64 + SLOT * 4;
  z.add(&member, 1.0);
  // 记账恰为一份成员字节 + 四槽（散列条目/树节点/双份分值），无逐容器字节重复
  assert_eq!(z.heap_memory_size, base + per_entry);

  // 运行时挂成员级 TTL：账本收主容器共享句柄，只加「字典两槽 + 堆两槽」
  // 与结构基线，不再计一份字节
  assert_eq!(
    z.set_expiration(&member, i64::MAX, ExpireOption::NONE),
    SortedSetExpireResult::ExpireUpdated
  );
  assert_eq!(
    z.heap_memory_size,
    base + per_entry + SLOT * 4 + EXPIRY_STRUCT_BASE
  );

  // 物理驻留单份：克隆快照与本体共享同源句柄；弹出后快照 2 份 + 弹出句柄
  // 1 份 = 计数 3。若实现仍是逐容器复制字节，此处计数恒为 1
  let snapshot = z.clone();
  let (_, popped) = z.pop_min_or_max(false).unwrap();
  assert_eq!(
    Arc::strong_count(&popped),
    5,
    "成员字节应跨容器共享同一份驻留"
  );
  // 弹出全回收：主容器条目与账本槽位、结构基线全部退账
  assert_eq!(z.heap_memory_size, base);
  drop((snapshot, popped));
}

#[test]
fn hash_member_bytes_accounted_once_and_reclaimed() {
  let mut h = HashObject::new();
  let base = h.heap_memory_size;
  assert_eq!(base, CONTAINER_BASE);

  let field = vec![7_u8; MEMBER_LEN];
  let value = vec![9_u8; VALUE_LEN];
  let per_entry = (round_up_ptr(MEMBER_LEN) + round_up_ptr(VALUE_LEN)) as i64 + SLOT * 3;
  h.update_size(&field, &value, true);
  h.hash.insert(Arc::from(field.clone()), value);
  // 记账恰为 key+value 各一份字节 + 三槽
  assert_eq!(h.heap_memory_size, base + per_entry);

  // 运行时挂字段级 TTL：账本收散列共享句柄，只加槽位与结构基线，不再计字段字节
  assert_eq!(
    h.set_expiration(&field, i64::MAX, ExpireOption::NONE),
    HashExpireResult::ExpireUpdated
  );
  assert_eq!(
    h.heap_memory_size,
    base + per_entry + SLOT * 4 + EXPIRY_STRUCT_BASE
  );

  h.set_expiration(&field, now_ticks() - 1, ExpireOption::NONE);
  h.delete_expired_items();
  assert_eq!(h.heap_memory_size, base);
}

#[test]
fn promote_triggers_on_real_heap_bytes() {
  // 条目数远低于条目维阈值，体积维独触发：小成员灌到记账体积越线
  let mut z = SortedSetObject::new();
  let mut i = 0_u64;
  while (z.heap_memory_size as usize) < TIERED_PROMOTE_BYTES {
    let member = format!("m{i:07}_{:0<240}", "");
    z.add(member.as_bytes(), i as f64);
    i += 1;
  }
  assert!(
    (i as usize) < TIERED_PROMOTE_THRESHOLD,
    "条目数不应先于体积维触发"
  );
  assert!(should_promote(z.len(), z.heap_memory_size as usize));
}
