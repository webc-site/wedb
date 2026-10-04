//! wcol SortedSet 堆内存记账与目标指针宽度回归测试
//!（票 task/todo/wcol-sorted-set-memory-accounting-undercount）
//!
//! 对标 C# `libs/server/Objects/SortedSet/SortedSetObject.cs:UpdateSize` 构成项：
//! `RoundUp(len, IntPtr.Size) + ByteArrayOverhead + 2*sizeof(double)
//!  + SortedSetEntryOverhead + DictionaryEntryOverhead`，rust 口径按构成项逐项
//! 塌缩为 `round_up_ptr(len) + SLOT * 4`（散列条目槽 / BTreeSet 树节点槽 /
//! 双份分值槽 SLOT*2，见 `account_entry` 注释）。
//!
//! 修复前必红口径：条目固定槽位被低估为 `SLOT * 2`（与无分值的 SetObject 同价，
//! 漏记 BTreeSet 树节点与双份 f64 分值），本文件逐条断言精确构成即红。
//!
//! 自研依据: zset 堆内存计量（doc/zh/collection.md 信封内存模型）

use std::sync::Arc;

use wbase::heap::{CONTAINER_BASE, PTR_SIZE, SLOT, round_up_ptr};
use wcol::SortedSetObject;

/// 指针宽度常量必须来自编译期真实目标指针宽度（杜绝裸字面量 8/4 的平台假设）：
/// const 求值期断言，若 PTR_SIZE 与 `size_of::<*const u8>()` 脱钩则编译失败
const _: () = assert!(PTR_SIZE == size_of::<*const u8>());
const _: () = assert!(PTR_SIZE == size_of::<usize>());

/// round_up_ptr 必须按 PTR_SIZE 步进取整（对位 C# `Utility.RoundUp(len, IntPtr.Size)`）：
/// const 求值期断言，len=1 取整即一个指针宽，len 恰为整宽不再进位
const _: () = assert!(round_up_ptr(1) == PTR_SIZE);
const _: () = assert!(round_up_ptr(PTR_SIZE) == PTR_SIZE);
const _: () = assert!(round_up_ptr(PTR_SIZE + 1) == PTR_SIZE * 2);

/// 单条目记账构成：成员字节按 PTR_SIZE 向上取整实计 + 四槽
#[inline]
fn per_entry(member_len: usize) -> i64 {
  round_up_ptr(member_len) as i64 + SLOT * 4
}

#[test]
fn ptr_size_matches_target_pointer_width() {
  // 运行时复核与 const 断言同口径（const 断言已保证编译期一致，此处防误导）
  assert_eq!(PTR_SIZE, size_of::<*const u8>());
  assert_eq!(PTR_SIZE, size_of::<*const SortedSetObject>());
  assert!(PTR_SIZE.is_power_of_two() && PTR_SIZE >= 4);
}

#[test]
fn zset_entry_accounting_covers_both_containers_and_dual_scores() {
  // 短成员（长度非指针宽整数倍）：构成项必须精确到 round_up + SLOT*4；
  // 低估口径（SLOT*2）下本断言必红
  let mut z = SortedSetObject::new();
  let base = z.heap_memory_size;
  assert_eq!(base, CONTAINER_BASE * 2);

  let mut expected = base;
  for (i, len) in [1_usize, 3, 7, PTR_SIZE, PTR_SIZE + 1, 33]
    .iter()
    .enumerate()
  {
    let member = vec![i as u8 + 1; *len];
    z.add(&member, i as f64);
    expected += per_entry(*len);
    assert_eq!(
      z.heap_memory_size,
      expected,
      "{len} 字节成员第 {} 次插入后记账未按「数据取整实计 + 四槽」逐项累加",
      i + 1
    );
  }

  // 全量回收对称回基线（对位 C# 移除臂 Debug.Assert 下界不变式）
  for i in 0..6_u64 {
    let len = [1_usize, 3, 7, PTR_SIZE, PTR_SIZE + 1, 33][i as usize];
    let member = vec![i as u8 + 1; len];
    assert_eq!(z.rem(&member), Some(i as f64));
  }
  assert_eq!(z.heap_memory_size, base);
}

#[test]
fn zset_heap_accounting_grows_linearly_with_member_count() {
  // 小集合等长成员：逐成员增量恒定 = 该长度条目构成，总记账随成员数严格线性；
  // 低估口径下斜率恒偏小 SLOT*2，本断言必红
  let mut z = SortedSetObject::new();
  let base = z.heap_memory_size;
  const MEMBER_LEN: usize = 11;
  let step = per_entry(MEMBER_LEN);

  let mut prev = base;
  for i in 0..16_u64 {
    let member = format!("{i:0MEMBER_LEN$}").into_bytes();
    assert!(z.add(&member, i as f64));
    let delta = z.heap_memory_size - prev;
    assert_eq!(
      delta,
      step,
      "第 {} 个成员增量偏离「取整实计 + 四槽」构成",
      i + 1
    );
    assert_eq!(z.heap_memory_size, base + step * (i as i64 + 1));
    prev = z.heap_memory_size;
  }
}

#[test]
fn zset_accounting_covers_long_string_member_heap_bytes() {
  // 长字符串成员：记账增量必须覆盖成员字节的堆占用（round_up 后 ≥ 原始长度），
  // 且在字节主导下仍足额带四槽开销；低估口径（漏双份分值/树节点槽）必红
  let mut z = SortedSetObject::new();
  let base = z.heap_memory_size;

  const LONG_LEN: usize = 1000; // 非 PTR_SIZE 整数倍，验证取整步进
  let member = vec![0xAB_u8; LONG_LEN];
  z.add(&member, 1.0);
  let delta = z.heap_memory_size - base;
  assert_eq!(delta, per_entry(LONG_LEN));
  assert!(
    delta > LONG_LEN as i64,
    "长成员记账应覆盖字符串堆占用并另带槽位开销，实测 {delta} 字节 < {LONG_LEN} + 槽位"
  );
  // 双份分值（2*sizeof(double)=16B）与两容器条目槽构成的下界直证：
  // 纯 HashSet<Vec<u8>> 口径（SetObject 的 SLOT*2）不足以覆盖
  assert!(delta - round_up_ptr(LONG_LEN) as i64 >= SLOT * 4);
}

#[test]
fn zset_score_update_arm_keeps_heap_and_shares_dict_handle() {
  // 更新臂真共享回归（票 wcol-zset-update-arm-member-arc-double-residency）：
  // 同成员反复更新分值（ZADD 存储臂 add / ZINCRBY 存储臂 incr_by；RESP 臂
  // sorted_set_add/sorted_set_increment/geo_add 与之同构，命令族应答零变化由
  // 既有 zset 测试与 wnode/tests/tiered_cmds_align.rs 回归锁）——
  // ① heap_memory_size 恒定：更新臂不动账、无新建成员 Arc 滞留；
  // ② 字典键句柄与有序视图成员句柄指针同一（Arc::ptr_eq）。
  // 修复前必红口径：更新臂新建 Arc 进树、字典保旧键，双份驻留下 ② 的
  // ptr_eq 断言必假，且 ① 的恒值下每个被更新成员实漏记 round_up_ptr(len) 一份。
  let mut z = SortedSetObject::new();
  const LEN: usize = 23; // 非指针宽整数倍：双份驻留下误差即 round_up_ptr(LEN)
  let hot = vec![0x42_u8; LEN];
  let cold = b"cold-member".to_vec();

  assert!(z.add(&hot, 1.0));
  assert!(z.add(&cold, 5.0));
  let expected = CONTAINER_BASE * 2 + per_entry(LEN) + per_entry(cold.len());
  assert_eq!(z.heap_memory_size, expected);

  for i in 1..=8_u64 {
    assert!(!z.add(&hot, i as f64), "既有成员更新应返回 false（非新增）");
    z.incr_by(&hot, 0.5);
    assert_eq!(
      z.heap_memory_size, expected,
      "第 {i} 轮分值更新后堆记账变动：更新臂存在新建成员 Arc 或重复入账"
    );
  }

  // 双索引句柄逐一指针配对（含被反复更新的热点成员与从未更新的冷成员）
  for entry in z.sorted_set.iter() {
    let dict_key = z
      .sorted_set_dict
      .get_key_value(entry.member.as_ref())
      .expect("有序视图成员必在散列字典")
      .0
      .clone();
    assert!(
      Arc::ptr_eq(&dict_key, &entry.member),
      "成员 {:?} 分值更新后字典键与有序视图句柄指针不同一（成员字节双份驻留）",
      String::from_utf8_lossy(&entry.member)
    );
  }
}
