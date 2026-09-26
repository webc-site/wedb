//! RESP 写出扩展面（Vec<u8> / &[u8] 会话缓冲入口）。
//!
//! null 一族在本 crate 只有 [`RespVecExt::write_resp_null_ver`] 与
//! [`RespVecExt::write_resp_null_array_ver`] 两个入口（二者各持 RESP2/RESP3
//! 一支成帧臂，对位 C# WriteNull / WriteNullArray 两套出口，非可合并样板），
//! 版本裁决统一走 [`is_resp3`]（阈 [`RESP3_MIN`]），其余 crate 与
//! wnode/wcol/wpubsub/wmetric/wext_* 一律转调，不得再就地展开版本
//! if/else，也不得自存第二份协议版本状态。
//!
//! 整型标量作为批量字符串写出同样只有一处定义：writer 面
//! [`RespWriter::write_integer_as_bulk_string`]（itoa 栈上格式化 → bulk 成帧单点），
//! `Vec<u8>` 会话缓冲面 [`RespVecExt::write_resp_int_as_bulk_string`] 薄壳转调。
//! 命令侧不得再 `to_string().as_bytes()` / `to_string().into_bytes()` 造临时堆中间物
//! （规约依据 .agents/skills/rust_review/SKILL.md「数字转字符串用 itoa」）。
//!
//! 流式应答的「帧头预留 + 计数回填」在本 crate 也只有 [`reserve_resp_frame_head`]
//! 与 [`backfill_resp_frame_head`] 一处实现（RI.SCAN / RI.RANGE 与分层集合输出臂
//! 共用），其余 crate 一律转调，不得另写第二套预留-回填，也不得退回
//! `scratch: Vec<u8>` 整块中转双缓冲——不变式见 [`RESP_FRAME_HEAD_RESERVED`] 文注。
//!
//! 对位 C# 的两层版本裁决：会话层 RespServerSession.cs:WriteNull /
//! WriteNullArray（经 RespServerSessionOutput.cs 的 respProtocolVersion 裁决）
//! 与 writer 层 RespMemoryWriter.cs 的 resp3 字段分支
//! （WriteNull/WriteNullArray/WriteDoubleNumeric）。rust 因 RespWriter 以
//! Resp2/Resp3 类型参数静态分派，两层在此合并为一处运行时二选一。
//! 协议恒定的 `$-1\r\n` 不再作为命令面应答存在，仅集群配置线格式
//! （wedb/src/server/cluster_config/serializer.rs）按 C# ClusterConfig.cs
//! 的序列化口径持有。
//!
//! 自研依据: 扩展命令枚举（本仓自定义 RI.COUNT/RI.SCAN 面，enum u8 前缀契约）

use core::str;

use itoa::Integer;
use wbase::num::strict_i64;

use crate::resp_memory_writer::{Resp2, Resp3, RespProtocol, RespWriter};

/// Redis 规范错误前缀最小长度（如 "ERR" 长度为 3）
pub const MIN_ERROR_PREFIX_LEN: usize = 3;

/// 最大单行错误文案长度（防恶意巨幅文案攻击）
pub const MAX_ERROR_MSG_LEN: usize = 512;

/// RESP3 版本阈（协议版本判定单源）：会话版本 ≥ 本值即走 RESP3 帧型，否则 RESP2。
///
/// 对位 C# `RespServerSessionOutput.cs` 的 `respProtocolVersion >= 3` 裁决；
/// 本 crate 是协议底层，全仓版本判定以此为唯一出处，散写 `>= 3` 字面量即旁路。
pub const RESP3_MIN: u8 = 3;

/// 会话协议版本是否 RESP3：编译期判定单点，消费方不得自存第二份版本比较。
#[inline]
pub const fn is_resp3(resp_version: u8) -> bool {
  resp_version >= RESP3_MIN
}

/// 错误帧净化单点（字节域）：以 `\r` 或 `\n` 截断防止 RESP 协议帧注入，并施加长度帽
/// （按字节边界切，不保证切在 UTF-8 字符边界）
///
/// 回显客户可控字节（命令名/段名/事件名/脚本错误文案）的错误帧必须经本函数或
/// 其消费方 [`RespWriter::write_error`]（含 `cmd_strings::abort_with_*` 门面）
/// 出口，不得在业务面手拼 `-ERR ...` 前缀裸写字节、也不得在调用侧另立清洗 ——
/// 后者即为本机制的旁路。
///
/// C# 侧无对位实现：`RespMemoryWriter` 的 `WriteError` 直落 `RespWriteUtils` 的
/// `TryWriteError` 原样拷贝字节，只在 XML 注释里声明
/// “The string mustn't contain a CR (\r) or LF (\n) bytes” 的前置约定，全仓不设错误
/// 文本清洗层。本层是 wedb 输出门面自立的纵深防御（我方机制收口，非对标差异），
/// 故按字节截断而不走 `from_utf8_lossy`：错误应答是行分隔帧，不要求客户端可解码为
/// 合法 UTF-8，lossy 会静默改写脚本原始错误字节（如 `error("\u{fffd}")` 类文案）。
#[inline]
pub fn sanitize_error_bytes(s: &[u8], max_len: usize) -> &[u8] {
  let cut = s
    .iter()
    .position(|&b| b == b'\r' || b == b'\n')
    .unwrap_or(s.len());
  &s[..cut.min(max_len)]
}

/// [`sanitize_error_bytes`] 的 `&str` 门面：同一套清洗逻辑，额外把长度帽回退到
/// UTF-8 字符边界，使返回值仍可直接当 `&str` 消费（既有调用点语义不变）。
/// `\r`/`\n` 为单字节，不可能是多字节字符的后缀，故 CRLF 切断点恒在字符边界上。
#[inline]
pub fn sanitize_error_str(s: &str, max_len: usize) -> &str {
  let cleaned = sanitize_error_bytes(s.as_bytes(), max_len);
  &s[..s.floor_char_boundary(cleaned.len())]
}

pub trait RespSliceExt {
  /// 非 UTF-8 字节串整体回空串、合法多字节按 UTF-8 原样吐出——与 C#
  /// Encoding.ASCII 逐字节 '?' 折叠语义刻意分叉，族级裁决见
  /// doc/zh/deviations.md §110（严禁调用点散改回改，收口只许本单点升格）
  fn as_str_safe(&self) -> &str;
  /// 严格解析：参数整体须为合法整数（对标 C# parseState.TryGetLong，
  /// allowLeadingZeros: false；C# TryGetInt 因死参放行 007，rust 统一严格收口拒前导零，见 doc/zh/deviations.md §32），失败返回 None
  fn try_parse_i64(&self) -> Option<i64>;
}

impl RespSliceExt for [u8] {
  #[inline]
  fn as_str_safe(&self) -> &str {
    str::from_utf8(self).unwrap_or("")
  }
  #[inline]
  fn try_parse_i64(&self) -> Option<i64> {
    strict_i64(self)
  }
}

pub trait RespVecExt {
  fn write_resp_int(&mut self, val: i64);
  fn write_resp_bulk_string(&mut self, val: &[u8]);
  /// 整数作为批量字符串写出 `$<len>\r\n<digits>\r\n`
  ///
  /// 网络应答面上的整型标量唯一出口：格式化走 itoa 栈上缓冲，帧型转调
  /// [`RespWriter::write_integer_as_bulk_string`] 单点，调用侧不得再
  /// `to_string().as_bytes()` / `into_bytes()` 造临时堆中间物。
  /// C# 对位 libs/common/RespWriteUtils.cs:542,565 `TryWriteInt32/Int64AsBulkString`。
  fn write_resp_int_as_bulk_string<I: Integer>(&mut self, val: I);
  fn write_resp_array_len(&mut self, len: usize);
  fn write_resp_error(&mut self, msg: &str);
  fn write_resp_simple_string(&mut self, msg: &str);
  /// 按会话 RESP 版本写 null 应答（RESP3 `_\r\n`、RESP2 `$-1\r\n`）
  ///
  /// libs/server/Resp/RespServerSessionOutput.cs:WriteNull 的会话版本分派
  /// 在分离写缓冲域的形态（会话自持缓冲经 RespServerSession::write_null 共用本单源）
  fn write_resp_null_ver(&mut self, resp_version: u8);
  /// 按会话 RESP 版本写 null 数组应答（RESP3 `_\r\n`、RESP2 `*-1\r\n`）
  ///
  /// libs/server/Resp/RespServerSessionOutput.cs:WriteNullArray 的会话版本
  /// 分派在分离写缓冲域的形态
  fn write_resp_null_array_ver(&mut self, resp_version: u8);
  /// 双精度浮点数作为批量字符串写出 `$<len>\r\n<score>\r\n`
  ///
  /// 分值文本化唯一出口：统一经 [`crate::resp_memory_writer::format_double`] 零堆格式化（非有限值输出
  /// "inf"/"-inf"/"nan"；整数值去末尾 ".0"），帧型转调
  /// [`RespWriter::write_double_bulk_string`] 单点。
  /// ZSCAN（内存与分层双态）与 ZRANGE WITHSCORES 共享此入口。
  /// C# 对位 libs/common/RespWriteUtils.cs:TryWriteDoubleBulkString。
  fn write_resp_double_bulk_string(&mut self, val: f64);
  fn resp_writer<P: RespProtocol>(&mut self) -> RespWriter<&mut Vec<u8>, P>;
  fn resp_writer2(&mut self) -> RespWriter<&mut Vec<u8>, Resp2>;
  fn resp_writer3(&mut self) -> RespWriter<&mut Vec<u8>, Resp3>;
}

impl RespVecExt for Vec<u8> {
  #[inline]
  fn write_resp_int(&mut self, val: i64) {
    RespWriter::new_ref(self).write_int64(val);
  }
  #[inline]
  fn write_resp_bulk_string(&mut self, val: &[u8]) {
    RespWriter::new_ref(self).write_bulk_string(val);
  }
  #[inline]
  fn write_resp_int_as_bulk_string<I: Integer>(&mut self, val: I) {
    RespWriter::new_ref(self).write_integer_as_bulk_string(val);
  }
  #[inline]
  fn write_resp_array_len(&mut self, len: usize) {
    RespWriter::new_ref(self).write_array_length(len);
  }
  #[inline]
  fn write_resp_error(&mut self, msg: &str) {
    let mut writer = RespWriter::new_ref(self);
    if let Some((prefix, _)) = msg.split_once(' ')
      && prefix.len() >= MIN_ERROR_PREFIX_LEN
      && prefix.bytes().all(|b| b.is_ascii_uppercase())
    {
      writer.write_error(msg);
    } else if msg == "ERR" {
      writer.write_error("ERR");
    } else {
      writer.write_error_with_prefix("ERR", msg);
    }
  }
  #[inline]
  fn write_resp_simple_string(&mut self, msg: &str) {
    RespWriter::new_ref(self).write_simple_string(msg);
  }
  #[inline]
  fn write_resp_null_ver(&mut self, resp_version: u8) {
    if is_resp3(resp_version) {
      Resp3::write_null(self);
    } else {
      Resp2::write_null(self);
    }
  }
  #[inline]
  fn write_resp_null_array_ver(&mut self, resp_version: u8) {
    if is_resp3(resp_version) {
      Resp3::write_null_array(self);
    } else {
      Resp2::write_null_array(self);
    }
  }
  #[inline]
  fn write_resp_double_bulk_string(&mut self, val: f64) {
    RespWriter::new_ref(self).write_double_bulk_string(val);
  }
  #[inline]
  fn resp_writer<P: RespProtocol>(&mut self) -> RespWriter<&mut Vec<u8>, P> {
    RespWriter::new_ref_p(self)
  }
  #[inline]
  fn resp_writer2(&mut self) -> RespWriter<&mut Vec<u8>, Resp2> {
    RespWriter::new_ref(self)
  }
  #[inline]
  fn resp_writer3(&mut self) -> RespWriter<&mut Vec<u8>, Resp3> {
    RespWriter::new_ref_p(self)
  }
}

/// 流式应答帧头的默认预留位宽 `*NN\r\n`（2 位计数，≤99 条回填零移动）
///
/// 与 C# `libs/server/Storage/Session/MainStore/RangeIndexOps.cs` 的
/// `ReservedHeaderSize` 同值（对位调用 :821 / :827，回填实现 :897
/// `BackfillArrayHeader`），适用「条数由用户 COUNT 界定、无先验上界可用」的
/// 流式扫描面（RI.SCAN / RI.RANGE）。
///
/// 本模块的「预留-回填」是全仓**唯一**一处（[`reserve_resp_frame_head`] +
/// [`backfill_resp_frame_head`]），适用面：**出帧条数只有扫完才确定**的流式应答
/// （RI.SCAN / RI.RANGE 区间迭代、分层集合整集合输出臂）。C# 对象层应答
/// （SetMembers / HashGetAll / HashGetKeysOrValues）条数由 `Count()` 先验可得，
/// 故 `WriteSetLength` / `WriteMapLength` / `WriteArrayLength` 直接落头后流式
/// 直写，无第二份应答缓冲；分层态条数不可先验（成员级到期语义下 `meta.size`
/// 与实际存活条数可不符），改由本机制达成同一形态：先占位预留帧头位 → 实体逐条
/// 直写最终 `output` → 扫完以**实际出帧计数**回填，位宽不等时 `copy_within`
/// 移动实体消除间隙。任何输出臂不得另写第二套预留-回填，尤其不得退回
/// `scratch: Vec<u8>` 整块中转双缓冲（那是应答堆峰值翻倍 + 一次全量 memcpy）。
///
/// 两条不变量（调用方必须共同保持）：
/// 1. **帧头与实体同源**：回填计数恒取实际出帧计数，严禁以先验计数（如
///    `meta.size`）直接落头；预留位宽只是「少移动一次」的性能提示，不参与帧内容；
/// 2. **错误路径应答未落帧**：预留位是 `output` 的一部分，实体写出后仍可能
///    上抛 `Err`（扫描失败 / 出账重灌失败 / 封窗被拒），故调用方在 `Err` 支必须
///    `output.truncate(base)` 撤帧（连同预留头一并回退），错误帧由上游漏斗闭环
///    ——与 `wnode` rangeindex 错误臂、`basic_commands/get.rs` 既有 `truncate`
///    撤帧惯例同形。
pub const RESP_FRAME_HEAD_RESERVED: usize = 5;

/// 帧头字节上限宽：前缀 1 + 十进制计数上界（usize 20 位，RESP2 map 头取
/// `2×len` 故至多 21 位）+ CRLF 2，仅作一次性小缓冲的容量提示
const FRAME_HEAD_MAX: usize = 24;

/// 现地生成协议感知帧头字节：`write_head` 必须是既有单源写头函数
/// （[`RespVecExt::write_resp_array_len`] / `cmd_strings::write_set_len` /
/// `cmd_strings::write_map_len`），帧头形态与直写路径逐字节同源，本函数不自拼
/// 任何协议字节
#[inline]
fn frame_head_bytes(write_head: impl Fn(&mut Vec<u8>, usize), count: usize) -> Vec<u8> {
  let mut head: Vec<u8> = Vec::with_capacity(FRAME_HEAD_MAX);
  write_head(&mut head, count);
  head
}

/// 计数 `count` 对应帧头的实际位宽（预留量估算口，与回填同源估宽）
///
/// 输出臂以「存活条数上界」（`meta.size`）调用：上界与实际同位宽是常态，回填
/// 即零移动；上界偏大（成员到期出账）走左移收窄支、偏小（副本 size 滞后）走
/// 右移扩宽支，两支都只移动一次实体段且不改帧内容
#[inline]
pub fn resp_frame_head_len(count: usize, write_head: impl Fn(&mut Vec<u8>, usize)) -> usize {
  frame_head_bytes(write_head, count).len()
}

/// 在 `output` 尾部落 `reserved` 字节帧头占位，返回回填基址（不变量 2 的回退点）
#[inline]
pub fn reserve_resp_frame_head(output: &mut Vec<u8>, reserved: usize) -> usize {
  let base = output.len();
  output.resize(base + reserved, 0);
  base
}

/// 把真实帧头就地替换 `output[base..base + reserved]` 的预留位
///
/// libs/server/Storage/Session/MainStore/RangeIndexOps.cs:BackfillArrayHeader
///
/// 流式回调在预留位之后直写实体，实际计数位宽与预留不等长时以 memmove 同型
/// （`copy_within`）移动实体段消除间隙：窄于预留走左移收窄支、宽于预留走右移
/// 扩宽支。`count` 恒为**实际出帧计数**（不变量 1），预留位宽只是移动量估算
#[inline]
pub fn backfill_resp_frame_head(
  output: &mut Vec<u8>,
  base: usize,
  reserved: usize,
  count: usize,
  write_head: impl Fn(&mut Vec<u8>, usize),
) {
  let head = frame_head_bytes(write_head, count);
  let actual = head.len();
  debug_assert!(output.len() >= base + reserved, "预留帧头位被越界改写");
  if actual < reserved {
    output.copy_within(base + reserved.., base + actual);
    output.truncate(output.len() - (reserved - actual));
  } else if actual > reserved {
    let old_len = output.len();
    output.resize(old_len + (actual - reserved), 0);
    output.copy_within(base + reserved..old_len, base + actual);
  }
  output[base..base + actual].copy_from_slice(&head);
}

#[cfg(test)]
mod tests {
  use std::fmt;

  use super::*;
  use crate::cmd_strings::{RESP_ERR_WRONG_TYPE, write_error_raw, write_map_len, write_set_len};

  #[test]
  fn try_parse_i64_strict() {
    assert_eq!(b"42".try_parse_i64(), Some(42));
    assert_eq!(b"-7".try_parse_i64(), Some(-7));
    assert_eq!(b"+3".try_parse_i64(), Some(3));
    assert_eq!(b"0".try_parse_i64(), Some(0));
    assert_eq!(b"-0".try_parse_i64(), Some(0));
    assert_eq!(b"9223372036854775807".try_parse_i64(), Some(i64::MAX));
    assert_eq!(b"-9223372036854775808".try_parse_i64(), Some(i64::MIN));
    assert_eq!(b"007".try_parse_i64(), None);
    assert_eq!(b"-007".try_parse_i64(), None);
    assert_eq!(b"abc".try_parse_i64(), None);
    assert_eq!(b"".try_parse_i64(), None);
    assert_eq!(b"1 2".try_parse_i64(), None);
    assert_eq!(b" 1".try_parse_i64(), None);
    assert_eq!(b"5 ".try_parse_i64(), None);
    assert_eq!(b"1x".try_parse_i64(), None);
    assert_eq!(b"9223372036854775808".try_parse_i64(), None);
    assert_eq!(b"-9223372036854775809".try_parse_i64(), None);
  }

  #[test]
  fn vec_ext_formatting() {
    let mut buf = Vec::new();
    buf.write_resp_int(100);
    buf.write_resp_simple_string("OK");
    buf.write_resp_bulk_string(b"foo");
    buf.write_resp_array_len(2);
    buf.write_resp_null_ver(2);
    buf.write_resp_error("something failed");
    assert_eq!(
      buf,
      b":100\r\n+OK\r\n$3\r\nfoo\r\n*2\r\n$-1\r\n-ERR something failed\r\n"
    );

    let mut buf2 = Vec::new();
    buf2.write_resp_error("ERR already has prefix");
    assert_eq!(buf2, b"-ERR already has prefix\r\n");

    let mut buf3 = Vec::new();
    buf3.write_resp_error(RESP_ERR_WRONG_TYPE);
    let mut want = Vec::new();
    write_error_raw(&mut want, RESP_ERR_WRONG_TYPE);
    assert_eq!(buf3, want);
  }

  #[test]
  fn vec_ext_versioned_null_writers() {
    let mut buf = Vec::new();
    buf.write_resp_null_ver(2);
    buf.write_resp_null_ver(3);
    buf.write_resp_null_array_ver(2);
    buf.write_resp_null_array_ver(3);
    assert_eq!(buf, b"$-1\r\n_\r\n*-1\r\n_\r\n");
  }

  /// 整型批量串出口（itoa 栈上格式化）与旧 `to_string().as_bytes()` 写法逐位等帧：
  /// 零/负数/位宽边界全覆，且与 [`RespWriter::write_int64_as_bulk_string`] 同字节，
  /// 证明收口后仍是一套成帧机制。
  #[test]
  fn int_as_bulk_string_is_to_string_byte_equivalent() {
    // 参照帧：老写法（临时 String）产出的字节，测试域内分配无妨
    fn old_frame(v: impl fmt::Display) -> Vec<u8> {
      let s = v.to_string();
      let mut out = Vec::new();
      out.write_resp_bulk_string(s.as_bytes());
      out
    }
    fn new_frame_i64(v: i64) -> Vec<u8> {
      let mut out = Vec::new();
      out.write_resp_int_as_bulk_string(v);
      out
    }

    for v in [0i64, 1, 7, 65_536, i64::MAX, i64::MIN, -7] {
      assert_eq!(new_frame_i64(v), old_frame(v), "i64 {v} 帧型漂移");
    }
    for v in [0i32, -128, i32::MAX, i32::MIN] {
      assert_eq!(
        new_frame_i64(i64::from(v)),
        old_frame(v),
        "i32 {v} 帧型漂移"
      );
    }
    for v in [0u32, 65_536, u32::MAX] {
      assert_eq!(
        new_frame_i64(i64::from(v)),
        old_frame(v),
        "u32 {v} 帧型漂移"
      );
    }
    for v in [0u64, 1_099_511_627_776, u64::MAX] {
      let mut out = Vec::new();
      out.write_resp_int_as_bulk_string(v);
      assert_eq!(out, old_frame(v), "u64 {v} 帧型漂移");
    }

    // 与既有单点同帧：ext 出口只是薄壳，不持有第二份帧字节
    let mut via_ext = Vec::new();
    via_ext.write_resp_int_as_bulk_string(1_024i32);
    let mut via_writer = Vec::new();
    RespWriter::new_ref(&mut via_writer).write_int32_as_bulk_string(1_024);
    assert_eq!(via_ext, via_writer);
    assert_eq!(via_ext, b"$4\r\n1024\r\n");
  }

  /// 错误帧净化单点：字节域核心与 `&str` 门面同源（CRLF 切断 + 长度帽），
  /// 门面只是把长度帽回退到 UTF-8 字符边界，不是第二套清洗。
  #[test]
  fn sanitize_error_bytes_and_str_share_one_mechanism() {
    assert_eq!(
      sanitize_error_bytes(b"boom\r\n:4242", 512),
      b"boom".as_slice()
    );
    assert_eq!(
      sanitize_error_bytes(b"boom\n:4242", 512),
      b"boom".as_slice()
    );
    assert_eq!(sanitize_error_bytes(b"plain", 512), b"plain".as_slice());
    assert_eq!(sanitize_error_bytes(b"abcdef", 3), b"abc".as_slice());
    // 字节域长度帽按字节切，允许落在多字节字符中间（成帧点不要求可解码）
    assert_eq!(
      sanitize_error_bytes("é中".as_bytes(), 3),
      [0xC3u8, 0xA9, 0xE4].as_slice()
    );

    assert_eq!(sanitize_error_str("boom\r\n:4242", 512), "boom");
    assert_eq!(sanitize_error_str("abcdef", 3), "abc");
    // 门面的长度帽回退到字符边界：5 字节落在第二个 é 之后
    assert_eq!(sanitize_error_str("ééé", 5), "éé");
  }

  /// 帧头体（bulk string 序列）用于观察回填移动是否破坏实体
  fn frame_head_body(items: &[&[u8]]) -> Vec<u8> {
    let mut buf = Vec::new();
    for item in items {
      buf.write_resp_bulk_string(item);
    }
    buf
  }

  /// 「预留-回填」结果与「先直写头再写体」逐字节全等，预留位宽取三档：
  /// 恰等（零移动）、偏大（左移收窄）、偏小（右移扩宽）
  fn assert_backfill_matches_direct(
    label: &str,
    count: usize,
    items: &[&[u8]],
    write_head: impl Fn(&mut Vec<u8>, usize) + Copy,
  ) {
    let mut want = Vec::new();
    write_head(&mut want, count);
    want.extend_from_slice(&frame_head_body(items));

    for hint in [count, count + 1_000_000, count / 2] {
      let mut out = Vec::new();
      let reserved = resp_frame_head_len(hint, write_head);
      let base = reserve_resp_frame_head(&mut out, reserved);
      out.extend_from_slice(&frame_head_body(items));
      backfill_resp_frame_head(&mut out, base, reserved, count, write_head);
      assert_eq!(out, want, "{label} 计数 {count} 预留按 {hint} 估算");
    }
  }

  /// 计数位宽边界（含 9→10、99→100、999→1000）× 三种协议感知头 × RESP2/RESP3
  /// 逐字节全等：回填机制不产出第二套帧字节，帧头恒由单源写头函数现地生成
  #[test]
  fn frame_head_backfill_is_byte_identical_across_width_boundaries() {
    for count in [0usize, 1, 8, 9, 10, 98, 99, 100, 101, 999, 1000] {
      let items: Vec<Vec<u8>> = (0..count.max(1))
        .map(|i| i.to_string().into_bytes())
        .collect();
      let refs: Vec<&[u8]> = items.iter().map(Vec::as_slice).collect();
      // 空集无实体，其余计数按 items 前 count 条取材
      let body_refs = if count == 0 { &[][..] } else { &refs[..count] };
      for ver in [2u8, 3] {
        assert_backfill_matches_direct("array", count, body_refs, |buf, n| {
          buf.write_resp_array_len(n)
        });
        assert_backfill_matches_direct("set", count, body_refs, |buf, n| {
          write_set_len(buf, n, ver)
        });
        assert_backfill_matches_direct("map", count, body_refs, |buf, n| {
          write_map_len(buf, n, ver)
        });
      }
    }
  }

  /// 基址非零（应答前段已有帧字节，如外层数组头）时前段与实体都不得受损，
  /// 移动量恰为位宽差（只动实体段）
  #[test]
  fn frame_head_backfill_preserves_prefix_and_moves_only_body() {
    let mut out = b"*2\r\n".to_vec();
    let reserved = resp_frame_head_len(30, |buf, n| buf.write_resp_array_len(n));
    let base = reserve_resp_frame_head(&mut out, reserved);
    out.extend_from_slice(&frame_head_body(&[b"a", b"b", b"c"]));
    backfill_resp_frame_head(&mut out, base, reserved, 3, |buf, n| {
      buf.write_resp_array_len(n)
    });
    assert_eq!(out, b"*2\r\n*3\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n");
  }

  /// 预留位宽随头形态同源（RESP2 map 头按 `2×count` 计），不得自估；
  /// 默认预留 [`RESP_FRAME_HEAD_RESERVED`] 的零移动带覆盖 ≤99 条
  #[test]
  fn frame_head_reserved_width_follows_head_form() {
    let resp3 = resp_frame_head_len(50, |buf, n| write_map_len(buf, n, 3));
    let resp2 = resp_frame_head_len(50, |buf, n| write_map_len(buf, n, 2));
    assert_eq!(resp3, 5); // %50\r\n
    assert_eq!(resp2, 6); // *100\r\n
    assert_eq!(
      resp_frame_head_len(99, |buf, n| buf.write_resp_array_len(n)),
      RESP_FRAME_HEAD_RESERVED
    );
    assert_eq!(
      resp_frame_head_len(100, |buf, n| buf.write_resp_array_len(n)),
      RESP_FRAME_HEAD_RESERVED + 1
    );
  }

  /// 撤帧惯例（不变量 2）：错误路径 `truncate(base)` 后 output 回到臂进入点，
  /// 预留头与部分实体一并消失，应答未落帧
  #[test]
  fn frame_head_truncate_rolls_back_reserved_head() {
    let mut out = Vec::new();
    let reserved = resp_frame_head_len(9, |buf, n| buf.write_resp_array_len(n));
    let base = reserve_resp_frame_head(&mut out, reserved);
    out.extend_from_slice(&frame_head_body(&[b"partial"]));
    out.truncate(base);
    assert!(out.is_empty(), "撤帧后不得残留预留头或部分实体");
  }
}
