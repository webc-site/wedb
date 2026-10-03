//! 共享对象命令（ZSCAN/HSCAN/SSCAN）参数校验与遍历集成测试
//! （对应 libs/server/Resp/Objects/SharedObjectCommands.cs:ObjectScan）

use wnode::resp::RespServerSession;
use wval::GarnetObjectType;

#[test]
fn object_scan_validates_args() {
  let mut sess = RespServerSession::default();
  let mut out = Vec::new();

  // 参数不足：中止返回 true（命令已完整消费，对齐 C# Abort 语义）
  assert!(sess.object_scan(
    &[b"key"],
    GarnetObjectType::SortedSet,
    10,
    &mut out,
    |_, _, _, _| {},
  ));
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'ZSCAN' command\r\n"
  );

  // HSCAN 参数不足
  out.clear();
  assert!(sess.object_scan(
    &[b"key"],
    GarnetObjectType::Hash,
    10,
    &mut out,
    |_, _, _, _| {},
  ));
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'HSCAN' command\r\n"
  );

  // SSCAN 参数不足
  out.clear();
  assert!(sess.object_scan(
    &[b"key"],
    GarnetObjectType::Set,
    10,
    &mut out,
    |_, _, _, _| {},
  ));
  assert_eq!(
    out,
    b"-ERR wrong number of arguments for 'SSCAN' command\r\n"
  );

  // 非法光标
  out.clear();
  assert!(sess.object_scan(
    &[b"key", b"-1"],
    GarnetObjectType::Hash,
    10,
    &mut out,
    |_, _, _, _| {},
  ));
  assert_eq!(out, b"-ERR invalid cursor\r\n");

  out.clear();
  assert!(sess.object_scan(
    &[b"key", b"not_a_number"],
    GarnetObjectType::Hash,
    10,
    &mut out,
    |_, _, _, _| {},
  ));
  assert_eq!(out, b"-ERR invalid cursor\r\n");

  // 合法输入透传至操作回调
  out.clear();
  assert!(sess.object_scan(
    &[b"key", b"0", b"COUNT", b"5"],
    GarnetObjectType::SortedSet,
    10,
    &mut out,
    |_sub_id, args, arg2, out| {
      assert_eq!(arg2, 10);
      assert_eq!(args.len(), 3); // 键已在底层剥离
      out.payload.extend_from_slice(b"*2\r\n$1\r\n0\r\n*0\r\n");
    },
  ));
  assert_eq!(out, b"*2\r\n$1\r\n0\r\n*0\r\n");
}
