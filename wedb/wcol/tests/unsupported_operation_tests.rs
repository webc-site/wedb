//! 集合对象未知子操作码防御帧锁用例（纯集合层，不牵引擎）
//!
//! doc/zh/deviations.md 条目 34：C# 四对象 Operate 的 switch default 臂各抛
//! GarnetException("Unsupported operation {op} in {Type}Object.Operate") 走
//! 异常通道掐断连接——libs/server/Objects/Hash/HashObject.cs:298、
//! List/ListObject.cs:185、Set/SetObject.cs:167、SortedSet/SortedSetObject.cs:449。
//! rust 以单行错误帧 `-ERR unsupported operation`（连接存活）替代，四对象共用
//! 常量 RESP_ERR_GENERIC_UNSUPPORTED_OPERATION（wresp/src/cmd_strings.rs）。
//!
//! 自研依据: 不支持操作报错矩阵（C# 对应 collections 对象 MethodLayer 报错路径）

use wcol::{HashObject, ListObject, ObjectOutput, SetObject, SortedSetObject};

/// 未知子操作码（四族判别值黄金表 0 起连续占位，0xFF 恒不命中）
const UNKNOWN_SUB_OP: u8 = 0xFF;

/// 驱动 operate 并断言整帧 `-ERR unsupported operation\r\n`
fn assert_unsupported_frame(operate: impl FnOnce(&mut ObjectOutput<'_>) -> bool) {
  let mut buf = Vec::new();
  let mut out = ObjectOutput::mount(&mut buf);
  assert!(operate(&mut out));
  assert_eq!(
    out.payload_view(),
    b"-ERR unsupported operation\r\n".as_slice()
  );
}

#[test]
fn hash_unknown_sub_op_replies_error_frame() {
  let mut hash = HashObject::new();
  assert_unsupported_frame(|out| hash.operate(UNKNOWN_SUB_OP, &[], 0, 0, out, 2));
}

#[test]
fn list_unknown_sub_op_replies_error_frame() {
  let mut list = ListObject::new();
  assert_unsupported_frame(|out| list.operate(UNKNOWN_SUB_OP, &[], 0, 0, out, 2));
}

#[test]
fn set_unknown_sub_op_replies_error_frame() {
  let mut set = SetObject::new();
  assert_unsupported_frame(|out| set.operate(UNKNOWN_SUB_OP, &[], 0, 0, out, 2));
}

#[test]
fn zset_unknown_sub_op_replies_error_frame() {
  let mut zset = SortedSetObject::new();
  assert_unsupported_frame(|out| zset.operate(UNKNOWN_SUB_OP, &[], 0, 0, out, 2));
}
