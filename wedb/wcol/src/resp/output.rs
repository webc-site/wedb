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
// 分派单点的（null 一族、双精度数值），本文件转调而不复制第二份分派体：
// null 一族的二选一只存在于 wresp::ext::RespVecExt::write_resp_null_ver /
// write_resp_null_array_ver 两处入口。

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

/// map 头：RESP3 `%<n>`，RESP2 双倍长度数组（C# libs/common/RespMemoryWriter.cs:WriteMapLength）
#[inline]
pub(crate) fn write_map_length(output: &mut ObjectOutput, len: usize, resp_protocol_version: u8) {
  if resp_protocol_version >= 3 {
    RespWriter::<&mut Vec<u8>, Resp3>::new_ref_p(output.payload).write_map_length(len);
  } else {
    RespWriter::new_ref(output.payload).write_map_length(len);
  }
}

/// set 头：RESP3 `~<n>`，RESP2 数组（C# libs/common/RespMemoryWriter.cs:WriteSetLength）
#[inline]
pub(crate) fn write_set_length(output: &mut ObjectOutput, len: usize, resp_protocol_version: u8) {
  if resp_protocol_version >= 3 {
    RespWriter::<&mut Vec<u8>, Resp3>::new_ref_p(output.payload).write_set_length(len);
  } else {
    RespWriter::new_ref(output.payload).write_set_length(len);
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
  cmd_strings::write_double_numeric(output.payload, value, resp_protocol_version);
}
