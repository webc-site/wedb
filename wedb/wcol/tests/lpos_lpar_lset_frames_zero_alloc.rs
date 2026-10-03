//! LPOS/LSET 帧形、零分配与记账集成测试册（自 wcol/src/list/list_object_impl.rs
//! 内联 mod tests 整体外迁，判据与断言原样，零扩面）
//!
//! 对标关系（C# 命令位）：garnet `libs/server/Resp/Objects/ListCommands.cs`
//! LPOS 位 `:131 ListPosition`（解析帧头 ListOp=LPOS 后经 storageApi 直落对象）与
//! LSET 位 `:808-829 ListSet`（ListOp=LSET）；rust 侧以 `ListObject::operate`
//! 直驱对应该命令层落点（`ListOperation::Lpos = 17` / `Lset = 14` 臂），解析用例
//! 走 pub 壳 `read_list_position_params`（对位 C# ReadListPositionInput + 三门），
//! 全程不触私面（`read_list_position_input` 私、`list_position`/`list_set`
//! pub(crate) 均不外抬）。
//!
//! 分配探针进程语义：nextest 逐用例独立进程执行，`#[global_allocator]` 注册即
//! 本测试二进制自身，计数差 = 本用例该段代码的分配次数，与内联时代一致。
//!
//! 在 garnet 中的相对路径: libs/server/Resp/Objects/ListCommands.cs（LPOS/LSET 命令位）
//! + libs/server/Objects/List/ListObjectImpl.cs（ListPosition/ListSet/ReadListPositionInput 被测体）

use wcol::{
  ListObject, ListOperation, ObjectOutput, list::list_object_impl::read_list_position_params,
};
use wresp::cmd_strings::{
  RESP_ERR_GENERIC_INDEX_OUT_RANGE, RESP_ERR_GENERIC_NOSUCHKEY, RESP_ERR_GENERIC_SYNTAX_ERROR,
  RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_OK,
};

/// 分配计数探针（仅测试构建）：LPOS 默认形态零堆分配判据的观测口
///
/// 转调 [`std::alloc::System`] 只追加计数，不改变分配语义；nextest 逐用例
/// 独立进程执行，故计数差即本用例该段代码的分配次数。
mod probe {
  use std::{
    alloc::{GlobalAlloc, Layout, System},
    cell::Cell,
  };

  thread_local! {
    static ALLOCS: Cell<usize> = const { Cell::new(0) };
  }

  struct Counting;

  // SAFETY: alloc/dealloc 原样转调 System，计数仅为无副作用的自增；
  // realloc/alloc_zeroed 走 trait 默认实现（即本 alloc/dealloc 组合）
  unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
      let _ = ALLOCS.try_with(|c| c.set(c.get() + 1));
      unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
      unsafe { System.dealloc(ptr, layout) }
    }
  }

  #[global_allocator]
  static COUNTING: Counting = Counting;

  /// 本线程至今累计的分配次数
  pub(crate) fn allocs() -> usize {
    ALLOCS.with(|c| c.get())
  }
}

/// 样本表 `[a, c, b, c, d]`（C# RespListTests.cs:LPOSWithOptions 的列表形态）
fn sample_list() -> ListObject {
  let mut obj = ListObject::default();
  for value in [b"a", b"c", b"b", b"c", b"d"] {
    obj.list.push_back(value.to_vec());
  }
  obj
}

/// 跑一枪 LPOS，回（应答字节, result1）
///
/// 触达路径经 pub 命令位 `ListObject::operate`（Lpos=17 臂直落 list_position）
fn lpos(obj: &mut ListObject, args: &[&[u8]], resp_protocol_version: u8) -> (Vec<u8>, i64) {
  let mut payload = Vec::new();
  let result1;
  {
    let mut out = ObjectOutput::mount(&mut payload);
    // Lpos 臂不消费 arg1/arg2（词元全在 args 内），补零位仅满足 operate 形参
    obj.operate(
      ListOperation::Lpos as u8,
      args,
      0,
      0,
      &mut out,
      resp_protocol_version,
    );
    result1 = out.result1;
  }
  (payload, result1)
}

/// 跑一枪 LSET，回（应答字节, result1）
///
/// 触达路径经 pub 命令位 `ListObject::operate`（Lset=14 臂直落 list_set；
/// 该臂不消费协议版本，固定传 2）
fn lset(obj: &mut ListObject, args: &[&[u8]]) -> (Vec<u8>, i64) {
  let mut payload = Vec::new();
  let result1;
  {
    let mut out = ObjectOutput::mount(&mut payload);
    // Lset 臂不消费 arg1/arg2（下标/元素在 args 内），补零位仅满足 operate 形参
    obj.operate(ListOperation::Lset as u8, args, 0, 0, &mut out, 2);
    result1 = out.result1;
  }
  (payload, result1)
}

#[test]
fn test_read_list_position_input() {
  // 词元大小写不敏感（全大写、全小写、混合形态）——经 pub 壳，产物即出参束
  let params = read_list_position_params(&[
    b"elem".as_slice(),
    b"RANK",
    b"2",
    b"count",
    b"5",
    b"MAXLEN",
    b"100",
  ]);
  assert!(params.is_ok());
  let params = params.unwrap();
  assert_eq!(params.rank, 2);
  assert_eq!(params.count, 5);
  assert!(!params.is_default_count);
  assert_eq!(params.maxlen, 100);

  // 缺少参数值边界守卫（防止越界 panic）
  for opt in [
    b"RANK".as_slice(),
    b"COUNT",
    b"maxlen",
    b"Rank",
    b"Count",
    b"MaxLen",
  ] {
    assert_eq!(
      read_list_position_params(&[b"elem".as_slice(), opt]).err(),
      Some(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes())
    );
  }

  // 非整数值
  assert_eq!(
    read_list_position_params(&[b"elem".as_slice(), b"rank", b"abc"]).err(),
    Some(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes())
  );

  // 未知选项
  assert_eq!(
    read_list_position_params(&[b"elem".as_slice(), b"UNKNOWN"]).err(),
    Some(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes())
  );

  // 混合大小写词元匹配（eq_ignore_ascii_case）
  for (opt, val_str, expect_rank, expect_count, expect_maxlen) in [
    (b"Rank".as_slice(), b"2".as_slice(), 2, 1, 0),
    (b"rAnK", b"-2", -2, 1, 0),
    (b"CounT", b"4", 1, 4, 0),
    (b"cOuNt", b"5", 1, 5, 0),
    (b"MaxLen", b"50", 1, 1, 50),
    (b"mAxLeN", b"60", 1, 1, 60),
  ] {
    let p = read_list_position_params(&[b"elem".as_slice(), opt, val_str]);
    assert!(p.is_ok());
    let p = p.unwrap();
    assert_eq!(p.rank, expect_rank);
    assert_eq!(p.count, expect_count);
    assert_eq!(p.maxlen, expect_maxlen);
  }

  // 未知/畸变选项仍报语法错误
  for opt in [b"Rankk".as_slice(), b"CountX", b"Max_Len", b"UNKNOWN"] {
    assert_eq!(
      read_list_position_params(&[b"elem".as_slice(), opt, b"1"]).err(),
      Some(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes())
    );
  }
}

/// 默认形态（无 COUNT）：命中直写整数、未命中 null，且全程零堆分配
///
/// 证伪判据：两支共用无条件 `Vec<i64>` 收集时，本用例至少付一次 8 字节分配。
#[test]
fn lpos_default_form_scalar_is_zero_alloc() {
  let mut obj = sample_list();
  // 出帧缓冲预留足量容量，隔离应答字节本身的扩容，使计数只反映命中容器
  let mut payload = Vec::with_capacity(64);

  // 命中：首个出现下标
  let before = probe::allocs();
  {
    let mut out = ObjectOutput::mount(&mut payload);
    obj.operate(ListOperation::Lpos as u8, &[b"c"], 0, 0, &mut out, 2);
    assert_eq!(out.payload_view(), b":1\r\n");
    assert_eq!(out.result1, 1);
  }
  assert_eq!(probe::allocs() - before, 0, "默认形态命中路径不得堆分配");

  // 未命中出口：RESP2 bulk null / RESP3 null，同样零分配
  for (version, expect) in [(2u8, &b"$-1\r\n"[..]), (3u8, &b"_\r\n"[..])] {
    payload.clear();
    let before = probe::allocs();
    {
      let mut out = ObjectOutput::mount(&mut payload);
      obj.operate(
        ListOperation::Lpos as u8,
        &[b"nx", b"RANK", b"-1"],
        0,
        0,
        &mut out,
        version,
      );
      assert_eq!(out.payload_view(), expect);
      assert_eq!(out.result1, 0);
    }
    assert_eq!(probe::allocs() - before, 0, "默认形态未命中路径不得堆分配");
  }

  // 混合大小写选项仍保持零堆分配
  for opt in [b"Rank".as_slice(), b"rAnK"] {
    payload.clear();
    let before = probe::allocs();
    {
      let mut out = ObjectOutput::mount(&mut payload);
      obj.operate(
        ListOperation::Lpos as u8,
        &[b"c", opt, b"-2"],
        0,
        0,
        &mut out,
        2,
      );
      assert_eq!(out.payload_view(), b":1\r\n");
      assert_eq!(out.result1, 1);
    }
    assert_eq!(
      probe::allocs() - before,
      0,
      "混合大小写选项命中路径不得堆分配"
    );
  }

  // rank 越界：缺省形态无第 3 次出现 → null
  let (payload, result1) = lpos(&mut obj, &[b"c", b"RANK", b"3"], 2);
  assert_eq!(payload, b"$-1\r\n");
  assert_eq!(result1, 0);

  // rank 反向命中 + maxlen 截断（缺省 COUNT 仍标量直写）
  let (payload, _) = lpos(&mut obj, &[b"c", b"RANK", b"-2"], 2);
  assert_eq!(payload, b":1\r\n");
  let (payload, _) = lpos(&mut obj, &[b"c", b"MAXLEN", b"1"], 2);
  assert_eq!(payload, b"$-1\r\n");
}

/// 显式 COUNT 形态：保留收集，数组应答与 C# 预写数组头/缩头逐字节全等
#[test]
fn lpos_count_form_collects_array() {
  let mut obj = sample_list();

  // 多命中数组（正向）
  let (payload, result1) = lpos(&mut obj, &[b"c", b"COUNT", b"2"], 2);
  assert_eq!(payload, b"*2\r\n:1\r\n:3\r\n");
  assert_eq!(result1, 2);

  // 词元全形态（大写/小写/混合大小写）+ count=0 全量扫描（C# count = list.Count，尾段缩头）
  for token in [b"COUNT".as_slice(), b"count", b"Count", b"cOuNt"] {
    let (payload, result1) = lpos(&mut obj, &[b"c", token, b"0"], 2);
    assert_eq!(payload, b"*2\r\n:1\r\n:3\r\n");
    assert_eq!(result1, 2);
  }

  // 反向：自尾向头的命中序
  let (payload, result1) = lpos(&mut obj, &[b"c", b"RANK", b"-1", b"COUNT", b"2"], 2);
  assert_eq!(payload, b"*2\r\n:3\r\n:1\r\n");
  assert_eq!(result1, 2);

  // 命中数不足 count：数组头按实际命中收缩
  let (payload, result1) = lpos(&mut obj, &[b"c", b"RANK", b"2", b"COUNT", b"5"], 2);
  assert_eq!(payload, b"*1\r\n:3\r\n");
  assert_eq!(result1, 1);

  // maxlen 截断（正向限前 3 项 / 反向限后 2 项）
  let (payload, result1) = lpos(&mut obj, &[b"c", b"COUNT", b"0", b"MAXLEN", b"3"], 2);
  assert_eq!(payload, b"*1\r\n:1\r\n");
  assert_eq!(result1, 1);
  let (payload, result1) = lpos(
    &mut obj,
    &[b"c", b"RANK", b"-1", b"COUNT", b"0", b"MAXLEN", b"2"],
    2,
  );
  assert_eq!(payload, b"*1\r\n:3\r\n");
  assert_eq!(result1, 1);

  // 混合大小写词元组合（Rank + Count + MaxLen）
  let (payload, result1) = lpos(
    &mut obj,
    &[b"c", b"Rank", b"-1", b"cOuNt", b"0", b"MaxLen", b"2"],
    2,
  );
  assert_eq!(payload, b"*1\r\n:3\r\n");
  assert_eq!(result1, 1);

  // 未命中出口：空数组（RESP2/RESP3 同形，C# WriteEmptyArray）
  for version in [2u8, 3u8] {
    let (payload, result1) = lpos(&mut obj, &[b"nx", b"COUNT", b"3"], version);
    assert_eq!(payload, b"*0\r\n");
    assert_eq!(result1, 0);
  }

  // 显式 COUNT 1：与缺省形态计数同、应答仍为单元素数组
  let (payload, result1) = lpos(&mut obj, &[b"c", b"COUNT", b"1"], 2);
  assert_eq!(payload, b"*1\r\n:1\r\n");
  assert_eq!(result1, 1);
}

/// LSET 四类应答帧回归（对位 r19-listset L8 矩阵）：空键/非整数下标/越界/成功
/// 四臂应答与 result1 各如旧，错误臂不留痕（零拷贝收口仅改换值路径）
#[test]
fn lset_reply_frames() {
  let err = |msg: &str| format!("-{msg}\r\n").into_bytes();

  let mut empty = ListObject::default();
  assert_eq!(
    lset(&mut empty, &[b"0", b"x"]),
    (err(RESP_ERR_GENERIC_NOSUCHKEY), 0)
  );

  let mut obj = sample_list();
  assert_eq!(
    lset(&mut obj, &[b"abc", b"x"]),
    (err(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER), 0)
  );
  for idx in [b"5".as_slice(), b"-6"] {
    assert_eq!(
      lset(&mut obj, &[idx, b"x"]),
      (err(RESP_ERR_GENERIC_INDEX_OUT_RANGE), 0)
    );
  }
  assert_eq!(obj.list.len(), 5, "错误臂不得留痕");

  // 正下标原位覆写
  assert_eq!(lset(&mut obj, &[b"1", b"c2"]), (RESP_OK.to_vec(), 1));
  assert_eq!(obj.list[1], b"c2".to_vec());
  // 负下标自尾换算（C#: index < 0 → Count + index）
  assert_eq!(lset(&mut obj, &[b"-1", b"d2"]), (RESP_OK.to_vec(), 1));
  assert_eq!(obj.list.back().map(Vec::as_slice), Some(b"d2".as_slice()));
  assert_eq!(obj.list.len(), 5);
}

/// LSET 覆写大元素的 heap_memory_size 差值恰为两条目记账之差
/// (round_up_ptr(new)+SLOT*2) - (round_up_ptr(old)+SLOT*2)，记账单点语义不变
#[test]
fn lset_heap_accounting_delta() {
  use wbase::heap::{SLOT, round_up_ptr};
  let per_entry = |len: usize| round_up_ptr(len) as i64 + SLOT * 2;

  let mut obj = ListObject::default();
  let old = vec![b'o'; 5000];
  let new = vec![b'n'; 9000];
  obj.list.push_back(old.clone());
  obj.update_size(&old, true);
  let base = obj.heap_memory_size;

  let (payload, result1) = lset(&mut obj, &[b"0", new.as_slice()]);
  assert_eq!((payload, result1), (RESP_OK.to_vec(), 1));
  assert_eq!(
    obj.heap_memory_size,
    base + per_entry(new.len()) - per_entry(old.len()),
    "LSET 覆写记账差值漂移"
  );
}

/// LSET 覆写热路径零额外分配锁（同文件 LPOS 零分配 probe 先例）：除 element
/// 落地必需的 to_vec 一次外零分配——收口前旧值整包 clone 多付一次 O(len)
#[test]
fn lset_overwrite_is_extra_alloc_free() {
  let big = vec![b'x'; 64 * 1024];
  let mut obj = ListObject::default();
  obj.list.push_back(big.clone());
  obj.update_size(&big, true);

  // 出帧缓冲预留 RESP_OK 5B 容量，隔离 payload 扩容使计数只反映换值路径
  let mut payload = Vec::with_capacity(8);
  let before = probe::allocs();
  {
    let mut out = ObjectOutput::mount(&mut payload);
    obj.operate(
      ListOperation::Lset as u8,
      &[b"0", big.as_slice()],
      0,
      0,
      &mut out,
      2,
    );
  }
  assert_eq!(payload, RESP_OK);
  assert_eq!(
    probe::allocs() - before,
    1,
    "LSET 覆写除 element 落地 to_vec 外不得有额外分配（clone 回苏即漂移）"
  );
}
