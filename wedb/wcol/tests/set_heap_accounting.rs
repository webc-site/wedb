//! SetObject 聚合构造与编解码堆记账一致性回归测试
//!（票 wcol-set-store-aggregator-heap-accounting-bypass）
//!
//! 缺陷本体：SINTERSTORE/SUNIONSTORE/SDIFFSTORE 三聚合 builder 曾在裸 `HashSet` 上
//! 直接 clone/extend/retain 装配成员，全程零 `update_size`，结果对象 `heap_memory_size`
//! 恒停在 [`SetObject::default`] 的 CONTAINER_BASE 基线，使 `obj_save_or_gc` 升阶体积维
//! （bytes >= 4MB）在写回点成死值——条目 < 65536 但成员串长致总体积超 4MB 的结果按契约
//! 必升阶而实际滞留信封。
//!
//! 修复：全仓唯一的记账恢复出口 [`SetObject::from_members`]（交并差裸集合装配后逐成员
//! 计入，对位 C# `libs/server/Storage/Session/ObjectStore/SetOps.cs` 三 STORE 臂的
//! `foreach (item in members) { newSetObject.Set.Add(item); newSetObject.UpdateSize(item); }`
//! 与 zset 侧 `from_entries` 同口径），信封解码路径 [`SetObject::deserialize_from_slice`]
//! 亦收敛于此单机制。本文件把「构造态 == 逐条 add 态 == 解码态」记账全等钉死在判定点上游。
//!
//! 自研依据: doc/zh/collection.md §2 容量承诺 + §3.2 双维度升阶契约

use wbase::{
  heap::{CONTAINER_BASE, SLOT, round_up_ptr},
  map::HashSet,
};
use wcol::{
  TIERED_PROMOTE_BYTES, TIERED_PROMOTE_THRESHOLD, object_payload::GarnetObjectPayload,
  set::set_object::SetObject, should_promote,
};

/// 单成员记账构成（对位 `SetObject::update_size`：数据按指针宽取整实计 + 两槽）
#[inline]
fn per_member(member_len: usize) -> i64 {
  round_up_ptr(member_len) as i64 + SLOT * 2
}

/// 装配一组成员为裸 `HashSet`（聚合 builder 的输入视图，零记账）后经 from_members 计账
fn assemble(members: &[&[u8]]) -> SetObject {
  let mut set = HashSet::<Vec<u8>>::default();
  for m in members {
    set.insert(m.to_vec());
  }
  SetObject::from_members(set)
}

#[test]
fn from_members_accounting_equals_per_member_update_size_sum() {
  // 构造态记账 = 容器基线 + Σ 逐成员构成，绝不因裸集合装配而漏记
  let members: Vec<Vec<u8>> = [1_usize, 7, 8, 9, 33, 1000]
    .iter()
    .map(|&n| vec![0xAB_u8; n])
    .collect();
  let refs: Vec<&[u8]> = members.iter().map(Vec::as_slice).collect();

  let obj = assemble(&refs);
  let expected = CONTAINER_BASE + members.iter().map(|m| per_member(m.len())).sum::<i64>();
  assert_eq!(
    obj.heap_memory_size, expected,
    "from_members 未按逐成员 update_size 足额计账"
  );
  assert_eq!(obj.len(), members.len());
  // 关键：非空结果绝不停在 CONTAINER_BASE 基线（漏账旁路的标志态）
  assert!(
    obj.heap_memory_size > CONTAINER_BASE,
    "聚合构造结果记账漏报，升阶体积门将成死值"
  );
}

#[test]
fn aggregate_state_matches_decode_state_heap_memory() {
  // 编解码记账不变式：聚合构造态与 to_blob→from_blob 解码态 heap_memory_size 全等，
  // 证明 builder 与 deserialize_from_slice 收敛于同一 from_members 单机制
  let members: Vec<Vec<u8>> = (0..64_u32)
    .map(|i| format!("member-{i:0>4}-xxxxxxxxxxxxxxxxxxxxxxx").into_bytes())
    .collect();
  let refs: Vec<&[u8]> = members.iter().map(Vec::as_slice).collect();

  let built = assemble(&refs);
  let blob = built.to_blob();
  let decoded = SetObject::from_blob(&blob).expect("解码失败");

  assert_eq!(
    built.heap_memory_size, decoded.heap_memory_size,
    "构造态与解码态 heap_memory_size 不等：记账机制双标或旁路"
  );
  assert_eq!(built.heap_memory_size, decoded.heap_memory_size);
  assert_eq!(built.set, decoded.set);
}

#[test]
fn aggregate_state_equals_incremental_add_state() {
  // 与逐条 add 的增量写臂口径全等（对位 C# foreach Add+UpdateSize）：
  // 聚合结果与从零逐成员 add 的对象记账、内容一致，杜绝 builder 特殊旁路
  let members: Vec<Vec<u8>> = (0..32_u32)
    .map(|i| format!("s{i:0>3}").into_bytes())
    .collect();
  let refs: Vec<&[u8]> = members.iter().map(Vec::as_slice).collect();

  let built = assemble(&refs);

  let mut added = SetObject::new();
  for m in &members {
    assert!(added.add(m));
  }
  assert_eq!(built.heap_memory_size, added.heap_memory_size);
  assert_eq!(built.set, added.set);
}

#[test]
fn long_member_store_result_triggers_volume_dimension_promote() {
  // 危害确证修复点：条目数远低于条目维阈值，但成员长串致总体积超 4MB 的聚合结果，
  // 修复后 heap_memory_size 足额，should_promote 的体积维真实命中
  let mut members: Vec<Vec<u8>> = Vec::new();
  let mut obj = SetObject::new();
  let mut i = 0_u64;
  while (obj.heap_memory_size as usize) < TIERED_PROMOTE_BYTES {
    let member = format!("m{i:0>7}_{}", "x".repeat(1024)).into_bytes();
    obj.add(&member);
    members.push(member);
    i += 1;
  }
  // 增量 add 参考态体积维命中
  assert!(
    obj.len() < TIERED_PROMOTE_THRESHOLD,
    "条目数应先于体积维保持低位，凸显体积维独触发"
  );
  assert!(should_promote(obj.len(), obj.heap_memory_size as usize));

  // 聚合 builder 经 from_members 装配同一成员集，heap 记账与 add 参考态全等，
  // 故同一判定点体积维同样命中（漏账旁路下此处 heap 恒为 CONTAINER_BASE → 永假）
  let refs: Vec<&[u8]> = members.iter().map(Vec::as_slice).collect();
  let built = assemble(&refs);
  assert_eq!(built.heap_memory_size, obj.heap_memory_size);
  assert!(should_promote(built.len(), built.heap_memory_size as usize));
}
