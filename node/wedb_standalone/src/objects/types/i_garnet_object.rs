//! Garnet 对象接口（对标 libs/server/Objects/Types/IGarnetObject.cs）
//!
//! C# 接口含 Type 属性、Operate、Scan 三成员；Rust 以 trait 等价表达，
//! 由各对象类型（有序集合见 objects::sortedset、其余经 wobject 适配）实现。

use crate::{inputs::ObjectInput, objects::types::object_output::ObjectOutput};

/// Garnet 对象统一接口
///
/// libs/server/Objects/Types/IGarnetObject.cs:IGarnetObject
pub trait IGarnetObject {
  /// 对象类型字节（GarnetObjectType）
  fn type_byte(&self) -> u8;

  /// 对象操作（RESP 语义经 ObjectInput/ObjectOutput 通道）
  ///
  /// libs/server/Objects/Types/IGarnetObject.cs:Operate
  fn operate(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) -> bool;

  /// 集合遍历（ZSCAN/HSCAN/SSCAN），返回 (条目序列, 新光标)
  ///
  /// libs/server/Objects/Types/IGarnetObject.cs:Scan
  fn scan(
    &self,
    start: i64,
    count: usize,
    pattern: &[u8],
    is_no_value: bool,
  ) -> (Vec<Option<Vec<u8>>>, i64);
}
