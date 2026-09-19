//! 集合 RESP 结构化输出（对标 libs/server/Objects/Types/ObjectOutput.cs）
//!
//! C# ObjectOutput 是纯数据 struct（SpanByteAndMemory/result1/OutputFlags），
//! RESP 写出由调用点就地构造 [`wresp::resp_memory_writer::RespWriter`] 承接
//! （对齐 GarnetObjectBase.cs:Scan 的就地构造写法），本类型不镜像写出原语。

use bitflags::bitflags;
use wresp::{
  cmd_strings,
  ext::RespVecExt,
  resp_memory_writer::{Resp3, RespWriter},
};

bitflags! {
  /// 存储输出标志（对标 libs/server/Objects/Types/ObjectOutput.cs:ObjectOutputFlags）
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
  pub struct ObjectOutputFlags: u8 {
    /// 无标志
    const NONE = 0;
    /// 移除键（对象为空时回收）
    const REMOVE_KEY = 1;
  }
}

/// 对象操作的结构化输出：计数字段 + RESP 负载
///
/// libs/server/Objects/Types/ObjectOutput.cs:ObjectOutput
#[derive(Debug, Clone, Default)]
pub struct ObjectOutput {
  /// 操作产出的 RESP 字节（对应 C# SpanByteAndMemory）
  pub payload: Vec<u8>,
  /// 操作结果计数（如成功添加的元素个数）
  pub result1: i64,
  /// 输出标志
  pub output_flags: ObjectOutputFlags,
}

impl ObjectOutput {
  /// 空输出
  #[inline]
  pub fn new() -> Self {
    Self::default()
  }
}

// ---- 运行时协议版本分派适配（crate 内） ----
//
// RespWriter 以 Resp2/Resp3 类型标记静态分派；用户会话协议是运行时事实，
// 调用点禁止各自展开 if/else 两臂，一律经下列适配口。wresp 已有同语义版本
// 分派单点的（null 一族、双精度数值），本文件转调而不复制第二份分派体：
// null 一族的二选一只存在于 wresp::ext::RespVecExt::write_resp_null_ver /
// write_resp_null_array_ver 两处入口。

/// null 应答：RESP3 `_`，RESP2 `$-1`（C# RespMemoryWriter.cs:WriteNull）
#[inline]
pub(crate) fn write_null(output: &mut ObjectOutput, resp_protocol_version: u8) {
  output.payload.write_resp_null_ver(resp_protocol_version);
}

/// null 数组应答：RESP3 `_`，RESP2 `*-1`（C# RespMemoryWriter.cs:WriteNullArray）
#[inline]
pub(crate) fn write_null_array(output: &mut ObjectOutput, resp_protocol_version: u8) {
  output
    .payload
    .write_resp_null_array_ver(resp_protocol_version);
}

/// map 头：RESP3 `%<n>`，RESP2 双倍长度数组（C# RespMemoryWriter.cs:WriteMapLength）
#[inline]
pub(crate) fn write_map_length(output: &mut ObjectOutput, len: usize, resp_protocol_version: u8) {
  if resp_protocol_version >= 3 {
    RespWriter::<&mut Vec<u8>, Resp3>::new_ref_p(&mut output.payload).write_map_length(len);
  } else {
    RespWriter::new_ref(&mut output.payload).write_map_length(len);
  }
}

/// set 头：RESP3 `~<n>`，RESP2 数组（C# RespMemoryWriter.cs:WriteSetLength）
#[inline]
pub(crate) fn write_set_length(output: &mut ObjectOutput, len: usize, resp_protocol_version: u8) {
  if resp_protocol_version >= 3 {
    RespWriter::<&mut Vec<u8>, Resp3>::new_ref_p(&mut output.payload).write_set_length(len);
  } else {
    RespWriter::new_ref(&mut output.payload).write_set_length(len);
  }
}

/// 数值双精度：RESP3 `,<v>`，RESP2 退化 bulk string
///
/// 版本二选一分派体不再在本 crate 重复展开：一律转调
/// [`wresp::cmd_strings::write_double_numeric`] 单点（对位 C# RespMemoryWriter.cs 的
/// resp3 字段分支，符号锚点登记在该单点，此处不复挂），本函数只做
/// [`ObjectOutput::payload`] 到命令面同一 sink 形态的适配
#[inline]
pub(crate) fn write_double_numeric(
  output: &mut ObjectOutput,
  value: f64,
  resp_protocol_version: u8,
) {
  cmd_strings::write_double_numeric(&mut output.payload, value, resp_protocol_version);
}
