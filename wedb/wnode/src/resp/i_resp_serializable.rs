//! RESP 可序列化抽象（对标 libs/server/Resp/IRespSerializable.cs）

use super::resp_memory_writer::RespMemoryWriter;

/// RESP 可序列化抽象
///
/// libs/server/Resp/IRespSerializable.cs:IRespSerializable
pub trait IRespSerializable {
  /// libs/server/Resp/IRespSerializable.cs:ToRespFormat
  fn to_resp_format(&self, writer: &mut RespMemoryWriter);
}
