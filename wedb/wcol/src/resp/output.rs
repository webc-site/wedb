//! 集合 RESP 结构化输出（对标 libs/server/Objects/Types/ObjectOutput.cs）
//!
//! C# ObjectOutput 的输出缓冲由会话侧经 SpanByteAndMemory / FromPinnedPointer
//! 直接挂载进对象应答结构，无中转向量；rust 对位为 [`ObjectOutput::mount`]
//! 挂载会话输出尾段，对象命令的 RESP 字节单步直写（消除中转缓冲二次拷贝）。
//! RESP 写出由调用点就地构造 [`wresp::resp_memory_writer::RespWriter`] 承接
//! （对齐 GarnetObjectBase.cs:Scan 的就地构造写法），本类型不镜像写出原语。

use bitflags::bitflags;
use wresp::{
  cmd_strings,
  ext::{RespVecExt, is_resp3},
  resp_memory_writer::RespWriter,
};

bitflags! {
  /// 存储输出标志（对标 libs/server/Objects/Types/ObjectOutput.cs:ObjectOutputFlags）
  #[derive(Debug, Clone, Copy, PartialEq, Eq)]
  pub struct ObjectOutputFlags: u8 {
    /// 无标志
    const NONE = 0;
    /// 移除键（对象为空时回收）
    const REMOVE_KEY = 1;
  }
}

/// 对象操作的结构化输出：计数字段 + 挂载输出尾段
///
/// libs/server/Objects/Types/ObjectOutput.cs:ObjectOutput（输出缓冲挂载
/// 形态对位 `ObjectOutput.cs:FromPinnedPointer`；C# SpanByteAndMemory 的
/// 指针/长度二元组在 rust 侧收敛为 `&'a mut Vec<u8>` + 挂载起点偏移）
#[derive(Debug)]
pub struct ObjectOutput<'a> {
  /// 挂载的输出缓冲尾段（对应 C# SpanByteAndMemory 挂载的会话缓冲）
  pub payload: &'a mut Vec<u8>,
  /// 挂载起点偏移（payload 有效段为 base..，前段属调用方既有应答）
  base: usize,
  /// 操作结果计数（如成功添加的元素个数）
  pub result1: i64,
  /// 输出标志
  pub output_flags: ObjectOutputFlags,
}

impl<'a> ObjectOutput<'a> {
  /// 挂载输出缓冲尾段（对标 ObjectOutput.cs:FromPinnedPointer）
  #[inline]
  pub fn mount(payload: &'a mut Vec<u8>) -> Self {
    Self {
      base: payload.len(),
      payload,
      result1: 0,
      output_flags: ObjectOutputFlags::NONE,
    }
  }

  /// 本次操作产出的有效负载视图（挂载起点之后）
  #[inline]
  pub fn payload_view(&self) -> &[u8] {
    &self.payload[self.base..]
  }

  /// 负载是否已写出（payload_written 判定单点）
  #[inline]
  pub fn written(&self) -> bool {
    self.payload.len() > self.base
  }

  /// 回退到挂载起点（对标 C# writer.ResetPosition）：丢弃本次已写负载，
  /// 调用方即可整体改写应答（错误覆盖 / 降级重放前清场）
  #[inline]
  pub fn reset(&mut self) {
    self.payload.truncate(self.base);
  }
}

// ---- 运行时协议版本分派适配（crate 内） ----
//
// RespWriter 以 Resp2/Resp3 类型标记静态分派；用户会话协议是运行时事实，
// 调用点禁止各自展开 if/else 两臂，一律经下列适配口。wresp 已有同语义版本
// 分派单点的（null 一族、map/set 头、双精度数值），本文件转调而不复制第二份
// 分派体：null 一族的二选一只存在于 wresp::ext::RespVecExt::write_resp_null_ver /
// write_resp_null_array_ver 两处入口（随机成员缺失态复合帧体收口于
// write_resp_missing_member_ver，见 write_random_member_missing），map/set 头与
// 数值转调 wresp::cmd_strings 的 write_map_len / write_set_len / write_double_numeric。

/// null 应答：RESP3 `_`，RESP2 `$-1`（C# libs/common/RespMemoryWriter.cs:WriteNull）
#[inline]
pub(crate) fn write_null(output: &mut ObjectOutput, resp_protocol_version: u8) {
  output.payload.write_resp_null_ver(resp_protocol_version);
}

/// null 数组应答：RESP3 `_`，RESP2 `*-1`（C# libs/common/RespMemoryWriter.cs:WriteNullArray）
#[inline]
pub(crate) fn write_null_array(output: &mut ObjectOutput, resp_protocol_version: u8) {
  output
    .payload
    .write_resp_null_array_ver(resp_protocol_version);
}

/// map 头：RESP3 `%<n>`，RESP2 双倍长度数组
///
/// 版本二选一分派体转调 [`wresp::cmd_strings::write_map_len`] 单点
/// （对位 C# libs/common/RespMemoryWriter.cs:WriteMapLength）
#[inline]
pub(crate) fn write_map_length(output: &mut ObjectOutput, len: usize, resp_protocol_version: u8) {
  cmd_strings::write_map_len(output.payload, len, resp_protocol_version);
}

/// set 头：RESP3 `~<n>`，RESP2 数组
///
/// 版本二选一分派体转调 [`wresp::cmd_strings::write_set_len`] 单点
/// （对位 C# libs/common/RespMemoryWriter.cs:WriteSetLength）
#[inline]
pub(crate) fn write_set_length(output: &mut ObjectOutput, len: usize, resp_protocol_version: u8) {
  cmd_strings::write_set_len(output.payload, len, resp_protocol_version);
}

/// 数值双精度：RESP3 `,<v>`，RESP2 退化 bulk string
///
/// 版本二选一分派体不再在本 crate 重复展开：一律转调
/// [`wresp::cmd_strings::write_double_numeric`] 单点（对位 C# RespMemoryWriter.cs 的
/// resp3 字段分支，符号锚点登记在该单点，此处不复挂），本函数只做
/// [`ObjectOutput::payload`] 到命令面同一 sink 形态的适配
///
/// 【有意偏差登记】同 `wresp::format_double`，未对齐 C# 的 15 位截断与大写 E 等格式，
/// 保持最短往返。因 INCRBYFLOAT 落盘文本与应答同串，该偏差会导致双侧累加序列发散。
#[inline]
pub(crate) fn write_double_numeric(
  output: &mut ObjectOutput,
  value: f64,
  resp_protocol_version: u8,
) {
  cmd_strings::write_double_numeric(output.payload, value, resp_protocol_version);
}

/// 随机成员族存活数零早退二选一出帧单点（HRANDFIELD/ZRANDMEMBER 共用，与
/// set_random_member 同构守卫）：带 count 形态回空数组、无 count 形态回 null，
/// `result1` 归零
///
/// 成员级 TTL 全到期键上采样空域零产出的防御臂（C# 原型 Random.Next(0) 抛断流
/// 属登记危险面，见 doc/zh/deviations.md §12；「声明数恒等实写数」§53 不受豁免）。
/// 帧体单源 [`wresp::ext::RespVecExt::write_resp_missing_member_ver`]（wnode 同帧
/// 同源），本壳只承担 [`ObjectOutput`] 结构化输出层的 `result1` 归零
#[inline]
pub(crate) fn write_random_member_missing(
  output: &mut ObjectOutput,
  included_count: bool,
  resp_protocol_version: u8,
) {
  output
    .payload
    .write_resp_missing_member_ver(included_count, resp_protocol_version);
  output.result1 = 0;
}

/// 随机成员带值形态成对出帧单点（HRANDFIELD WITHVALUES / ZRANDMEMBER
/// WITHSCORES 共用）：RESP3 先嵌套 2 元数组头（RESP2 扁平不分叉），成员恒写，
/// 二次项（值/分值）交 `write_payload` 项写出
///
/// hash_object_impl / sorted_set_object_impl 两随机臂 sink 的同构出帧体收敛于此
#[inline]
pub(crate) fn write_random_member_with_payload(
  output: &mut ObjectOutput,
  with_payload: bool,
  resp_protocol_version: u8,
  member: &[u8],
  write_payload: impl FnOnce(&mut ObjectOutput),
) {
  if with_payload && is_resp3(resp_protocol_version) {
    RespWriter::new_ref(output.payload).write_array_length(2);
  }
  RespWriter::new_ref(output.payload).write_bulk_string(member);
  if with_payload {
    write_payload(output);
  }
}

/// 成员级 TTL 族出帧骨架单点（HEXPIRE/HTTL/HPERSIST 与 ZEXPIRE/ZTTL/ZPERSIST
/// 六方法共用）：声明成员数数组头 → 逐成员调宿主成员级 API 出 int64 →
/// `result1` 回填成员数
///
/// hash_object_impl / sorted_set_object_impl 三对同构出帧循环收敛于此；
/// 成员存在性/过期判定语义（hash 侧过滤式 contains_key、zset 侧裸判定 + 二次
/// 采样矫正）与环前是否 purge 由各宿主闭包自理，本骨架只承担出帧，不抹平
/// 双态差异
#[inline]
pub(crate) fn write_member_int64_results<H>(
  output: &mut ObjectOutput<'_>,
  args: &[&[u8]],
  host: &mut H,
  member_result: impl FnMut(&mut H, &[u8]) -> i64,
) {
  RespWriter::new_ref(output.payload).write_array_length(args.len());
  let mut member_result = member_result;
  for &member in args {
    let result = member_result(host, member);
    RespWriter::new_ref(output.payload).write_int64(result);
  }
  output.result1 = args.len() as i64;
}
