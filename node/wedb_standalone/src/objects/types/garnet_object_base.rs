//! Garnet 对象基类（对标 libs/server/Objects/Types/GarnetObjectBase.cs）
//!
//! C# 抽象基类承载 HeapMemorySize、WriteType 与 ZSCAN 族输入解析；
//! Rust 以 trait + 缺省方法表达，对象自持内存记账字段。

use std::io::{self, Write};

use crate::{
  inputs::ObjectInput,
  objects::{
    parse_utils::{equals_ignore_case, try_get_int, try_get_long},
    types::i_garnet_object::IGarnetObject,
  },
  types::GarnetObjectType,
};

/// ZSCAN 输入参数（ReadScanInput 的解析产物）
#[derive(Debug, Clone, Default)]
pub struct ScanInput {
  pub cursor: i64,
  pub pattern: Vec<u8>,
  pub count: usize,
  pub is_no_value: bool,
}

/// Garnet 对象基类契约
///
/// libs/server/Objects/Types/GarnetObjectBase.cs:GarnetObjectBase
pub trait GarnetObjectBase: IGarnetObject {
  /// 堆内存记账（相对值；C# MemoryUtils 开销口径见各对象说明）
  fn heap_memory_size(&self) -> i64;

  fn add_heap_memory_size(&mut self, delta: i64);

  /// 类型字节写入（isNull 时写 GarnetObjectType::Null）
  ///
  /// libs/server/Objects/Types/GarnetObjectBase.cs:WriteType
  fn write_type(&self, writer: &mut impl Write, is_null: bool) -> io::Result<()> {
    let type_byte = if is_null {
      GarnetObjectType::Null as u8
    } else {
      self.type_byte()
    };
    writer.write_all(&[type_byte])
  }

  /// 解析 ZSCAN 族输入：光标 / MATCH pattern / COUNT n / NOVALUES
  ///
  /// libs/server/Objects/Types/GarnetObjectBase.cs:ReadScanInput
  /// （limit_count_in_output <= 0 时不钳制；解析失败返回错误文本）
  fn read_scan_input(
    &self,
    input: &ObjectInput,
    limit_count_in_output: i32,
  ) -> Result<ScanInput, &'static [u8]> {
    let mut result = ScanInput {
      cursor: 0,
      pattern: Vec::new(),
      count: 10,
      is_no_value: false,
    };

    let Some(cursor) = (if input.parse_state.count > 0 {
      try_get_long(arg(input, 0))
    } else {
      None
    })
    .filter(|c| *c >= 0) else {
      return Err(b"ERR invalid cursor");
    };
    result.cursor = cursor;

    let mut curr_token_idx = 1;
    while curr_token_idx < input.parse_state.count {
      let param = arg(input, curr_token_idx);
      curr_token_idx += 1;

      if equals_ignore_case(param, b"MATCH") {
        if curr_token_idx >= input.parse_state.count {
          return Err(b"ERR syntax error");
        }
        result.pattern = arg(input, curr_token_idx).to_vec();
        curr_token_idx += 1;
      } else if equals_ignore_case(param, b"COUNT") {
        if curr_token_idx >= input.parse_state.count {
          return Err(b"ERR syntax error");
        }
        match try_get_int(arg(input, curr_token_idx)) {
          Some(c) => {
            curr_token_idx += 1;
            result.count = c as usize;
            // 调用方给出正限额时钳制单轮数量
            if limit_count_in_output > 0 && result.count > limit_count_in_output as usize {
              result.count = limit_count_in_output as usize;
            }
          }
          None => return Err(b"ERR value is not an integer or out of range"),
        }
      } else if equals_ignore_case(param, b"NOVALUES") {
        result.is_no_value = true;
      }
    }

    Ok(result)
  }
}

/// 取第 i 个参数字节
#[inline]
fn arg<'a>(input: &ObjectInput, i: usize) -> &'a [u8] {
  input.parse_state.get_arg_slice_by_ref(i).as_slice()
}
