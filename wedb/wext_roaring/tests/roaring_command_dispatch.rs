//! R.* 四条命令静态分发面（自 src/roaring_bitmap_commands.rs 内联测试迁出；
//! 私有解析面 try_parse_uint32 的单测仍留守 src）
//!
//! 全部断言走编译期清单 pub 面：[`RoaringCommand::fns`] 函数指针集、
//! [`COMMAND_INFOS`] 命令目录、按名解析 [`RoaringCommand::match_command`]。
//!
//! 自研依据: Roaring 命令分发（C# 对应 test/standalone/Garnet.test/RespRoaringBitmapTests.cs + RoaringBitmapDataTests.cs）

use wext_roaring::{COMMAND_INFOS, RoaringBitmapCommands, RoaringCommand};

/// RESP 版本（各用例统一 RESP2）
const VER: u8 = 2;

/// R.SETBIT Updater 臂按编译期清单派发：断言成功，应答帧留在 out
fn set(payload: &mut Vec<u8>, args: &[&[u8]], out: &mut Vec<u8>) {
  out.clear();
  let up = RoaringCommand::SetBit.fns().updater;
  assert!(up(payload, args, out, VER));
}

/// Reader 臂按编译期清单派发：断言成功，应答帧留在 out
fn read(cmd: RoaringCommand, payload: &[u8], args: &[&[u8]], out: &mut Vec<u8>) {
  out.clear();
  let rd = cmd.fns().reader;
  assert!(rd(payload, args, out, VER));
}

/// NotFound 臂按编译期清单派发：应答帧写入 out
fn nf(cmd: RoaringCommand, args: &[&[u8]], out: &mut Vec<u8>) {
  out.clear();
  (cmd.fns().not_found)(args, out, VER);
}

/// 静态清单按名匹配（大小写不敏感）与注册名校验同径
///
/// （src 侧 `is_command_registered` 即 `match_command(..).is_some()` 的
/// cfg(test) 包装，随迁移删除，此处直走同一 pub 面）
#[test]
fn static_match_and_registration() {
  let mc = RoaringCommand::match_command;
  let registered = |name: &str| mc(name.as_bytes()).is_some();
  assert_eq!(mc(b"r.setbit"), Some(RoaringCommand::SetBit));
  assert_eq!(mc(b"R.BITPOS"), Some(RoaringCommand::BitPos));
  assert_eq!(mc(b"R.SETBIT").map(RoaringCommand::arity), Some(4));
  assert_eq!(mc(b"R.NOPE"), None);
  assert!(registered("r.getbit"));
  assert!(!registered(""));
  assert!(!registered("SET"));
}

/// 命令目录 [`COMMAND_INFOS`] 与按名解析 [`RoaringCommand::ALL`] 同一名单
/// 源（枚举 [`RoaringCommand::name`]）：新增命令漏登记任一侧由本用例挡住，
/// 杜绝目录与解析判定漂移
#[test]
fn directory_and_match_share_one_name_source() {
  let registered = |name: &str| RoaringCommand::match_command(name.as_bytes()).is_some();
  assert_eq!(
    COMMAND_INFOS.len(),
    RoaringCommand::ALL.len(),
    "命令目录与枚举全集条目数漂移"
  );
  for cmd in RoaringCommand::ALL.iter().copied() {
    let name = cmd.name();
    assert!(
      COMMAND_INFOS.iter().any(|info| info.name == name),
      "命令目录缺项 {name}"
    );
    assert_eq!(RoaringCommand::match_command(name.as_bytes()), Some(cmd));
    assert!(registered(name));
  }
}

/// 非空载荷上空对象判定不成立；置位/清空后随动
///
/// （`SetBit.fns().is_empty` 即 src 侧 `payload_is_empty` 经编译期清单
/// 发布的同一函数指针）
#[test]
fn empty_object_detection() {
  let mut payload = Vec::new();
  let mut out = Vec::new();
  let empty = RoaringCommand::SetBit.fns().is_empty;
  assert!(empty(&payload));

  set(&mut payload, &[b"42", b"1"], &mut out);
  assert_eq!(out, b":0\r\n");
  assert!(!empty(&payload));

  // 幂等重复置 1：previous 为 1，载荷未改动且非空（跳过重编码）
  let old_payload = payload.clone();
  set(&mut payload, &[b"42", b"1"], &mut out);
  assert_eq!(out, b":1\r\n");
  assert_eq!(payload, old_payload);
  assert!(!empty(&payload));

  set(&mut payload, &[b"42", b"0"], &mut out);
  assert_eq!(out, b":1\r\n");
  assert!(empty(&payload));

  // 空载荷置 0：previous 为 0，维持空载荷
  set(&mut payload, &[b"42", b"0"], &mut out);
  assert_eq!(out, b":0\r\n");
  assert!(empty(&payload));
}

/// 防空墓碑：畸形参数在 NeedInitialUpdate 即拒绝，不触碰载荷
///
/// （`SetBit.fns().need_initial_update` 即 src 侧
/// `set_bit_need_initial_update` 经编译期清单发布的同一函数指针）
#[test]
fn need_initial_update_rejects_bad_args() {
  let niu = RoaringCommand::SetBit.fns().need_initial_update;
  let mut out = Vec::new();
  assert!(!niu(&[b"notanumber", b"1"], &mut out, VER));
  assert_eq!(
    out,
    b"-ERR bit offset is not an unsigned 32-bit integer\r\n"
  );
  out.clear();
  assert!(!niu(&[b"-5", b"1"], &mut out, VER));
  assert_eq!(
    out,
    b"-ERR bit offset is not an unsigned 32-bit integer\r\n"
  );
  out.clear();
  assert!(!niu(&[b"5", b"2"], &mut out, VER));
  assert_eq!(out, b"-ERR bit value must be 0 or 1\r\n");
  out.clear();
  assert!(niu(&[b"5", b"0"], &mut out, VER));
  assert!(out.is_empty());
}

/// 读命令 NotFound 语义（读不建键的应答面）
#[test]
fn not_found_semantics() {
  let mut out = Vec::new();
  nf(RoaringCommand::GetBit, &[b"12345"], &mut out);
  assert_eq!(out, b":0\r\n");
  nf(RoaringCommand::BitCount, &[], &mut out);
  assert_eq!(out, b":0\r\n");
  nf(RoaringCommand::BitPos, &[b"1"], &mut out);
  assert_eq!(out, b":-1\r\n");
  nf(RoaringCommand::BitPos, &[b"0"], &mut out);
  assert_eq!(out, b":0\r\n");
  nf(RoaringCommand::BitPos, &[b"0", b"100"], &mut out);
  assert_eq!(out, b":100\r\n");
  nf(RoaringCommand::BitPos, &[b"2"], &mut out);
  assert_eq!(out, b"-ERR bit must be 0 or 1\r\n");
}

/// Reader 命中路径：setbit 置位后 getbit/bitcount/bitpos 读值
#[test]
fn reader_paths_after_updates() {
  let mut payload = Vec::new();
  let mut out = Vec::new();
  set(&mut payload, &[b"100", b"1"], &mut out);
  assert_eq!(out, b":0\r\n");
  read(RoaringCommand::GetBit, &payload, &[b"100"], &mut out);
  assert_eq!(out, b":1\r\n");
  read(RoaringCommand::GetBit, &payload, &[b"99"], &mut out);
  assert_eq!(out, b":0\r\n");
  read(RoaringCommand::BitCount, &payload, &[], &mut out);
  assert_eq!(out, b":1\r\n");
  read(RoaringCommand::BitPos, &payload, &[b"1"], &mut out);
  assert_eq!(out, b":100\r\n");
}

/// try_parse_uint32 对标 C# Utf8Parser 默认整数路径：
/// 符号（仅 -0 类负值合法）、任意长前导零、u32 边界与溢出
#[test]
fn try_parse_uint32_aligns_utf8_parser() {
  let parse = RoaringBitmapCommands::try_parse_uint32;
  // 常规十进制
  assert_eq!(parse(b"0"), Some(0));
  assert_eq!(parse(b"42"), Some(42));
  assert_eq!(parse(b"4294967295"), Some(u32::MAX));
  // 值域上界溢出（C# long 解析成功后 signed <= uint.MaxValue 过滤拒绝）
  assert_eq!(parse(b"4294967296"), None);
  assert_eq!(parse(b"99999999999999999999"), None);
  // 可选符号：+N 合法；仅 -0 类经值域过滤后合法；残缺符号非法
  assert_eq!(parse(b"+42"), Some(42));
  assert_eq!(parse(b"+0"), Some(0));
  assert_eq!(parse(b"-0"), Some(0));
  assert_eq!(parse(b"-00"), Some(0));
  assert_eq!(parse(b"-5"), None);
  assert_eq!(parse(b"+"), None);
  assert_eq!(parse(b"-"), None);
  assert_eq!(parse(b"+-1"), None);
  // 前导零不计溢出（TryParseInt64D 零串吞并，长度无上限）
  assert_eq!(parse(b"0000000000042"), Some(42));
  assert_eq!(parse(b"00000000000000000000005"), Some(5));
  assert_eq!(parse(b"004294967295"), Some(u32::MAX));
  assert_eq!(parse(b"004294967296"), None);
  assert_eq!(parse(b"-0000000000000005"), None);
  // 非法字符与空串（C# 整体消费校验等价拒绝）
  assert_eq!(parse(b""), None);
  assert_eq!(parse(b"12x"), None);
  assert_eq!(parse(b" 1"), None);
  assert_eq!(parse(b"+1 "), None);

  // 接受面贯穿命令层：+5 / 长前导零偏移可正常置位读回
  let mut payload = Vec::new();
  let mut out = Vec::new();
  let updater = RoaringCommand::SetBit.fns().updater;
  assert!(updater(&mut payload, &[b"+5", b"1"], &mut out, VER));
  assert_eq!(out, b":0\r\n");
  let reader = RoaringCommand::GetBit.fns().reader;
  out.clear();
  assert!(reader(&payload, &[b"0000000000005"], &mut out, VER));
  assert_eq!(out, b":1\r\n");
}
