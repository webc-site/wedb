//! 集合对象（对标 libs/server/Objects/Set/SetObject.cs）
//!
//! 结构与 C# 一致：`set`（成员散列集合）+ 堆内存记账 `heap_memory_size`。
//! 集合无成员级过期（C# 亦无），基数即 `set.len()`。

use std::io::{self, Read, Write};

use gxhash::HashSet;

use crate::{
  inputs::ObjectInput,
  objects::{
    hash::hash_object::{glob_match, scan_operate_shared},
    types::object_output::{ObjectOutput, ObjectOutputFlags},
  },
  types::GarnetObjectType,
};

/// 集合操作（AOF 持久化值，C# 侧为显式追加语义，不得改序/复用既有值）
///
/// libs/server/Objects/Set/SetObject.cs:SetOperation
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, num_enum::TryFromPrimitive, num_enum::IntoPrimitive,
)]
#[repr(u8)]
pub enum SetOperation {
  Sadd = 0,
  Srem = 1,
  Spop = 2,
  Smembers = 3,
  Scard = 4,
  Sscan = 5,
  Smove = 6,
  Srandmember = 7,
  Sismember = 8,
  Smismember = 9,
  Sunion = 10,
  Sunionstore = 11,
  Sdiff = 12,
  Sdiffstore = 13,
  Sinter = 14,
  Sinterstore = 15,
}

/// 集合对象
///
/// libs/server/Objects/Set/SetObject.cs:SetObject
#[derive(Debug, Clone, Default)]
pub struct SetObject {
  /// 成员散列集合
  pub set: HashSet<Vec<u8>>,
  /// 堆内存记账（相对值；C# 语义见 hash 域文件头刻意差异）
  pub heap_memory_size: i64,
}

impl SetObject {
  /// 构造空集合
  ///
  /// libs/server/Objects/Set/SetObject.cs:SetObject()
  pub fn new() -> Self {
    Self::default()
  }

  /// 从 C# BinaryWriter 序列化格式反序列化
  ///
  /// BinaryReader 反序列化构造 (SetObject.cs BinaryReader)
  pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
    let mut obj = Self::new();

    let mut len_buf = [0_u8; 4];
    reader.read_exact(&mut len_buf)?;
    let count = i32::from_le_bytes(len_buf);

    for _ in 0..count {
      reader.read_exact(&mut len_buf)?;
      let mut item = vec![0_u8; i32::from_le_bytes(len_buf) as usize];
      reader.read_exact(&mut item)?;
      obj.set.insert(item.clone());
      obj.update_size(&item, true);
    }

    Ok(obj)
  }

  /// 序列化为 C# BinaryWriter 兼容格式
  ///
  /// libs/server/Objects/Set/SetObject.cs:DoSerialize
  pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
    writer.write_all(&(self.set.len() as i32).to_le_bytes())?;
    for item in &self.set {
      writer.write_all(&(item.len() as i32).to_le_bytes())?;
      writer.write_all(item)?;
    }
    Ok(())
  }

  /// 与 wkv 对象存储层的既有载荷格式（wobject bitcode `Vec<u8>` 数组）互转
  ///
  /// 无 C# 对应（wkv blob 装载入口）
  pub fn from_members(members: Vec<Vec<u8>>) -> Self {
    let mut obj = Self::new();
    for member in members {
      if obj.set.insert(member.clone()) {
        obj.update_size(&member, true);
      }
    }
    obj
  }

  /// 导出为成员数组（`from_members` 的逆操作）
  ///
  /// 无 C# 对应（wkv blob 回写出口）
  pub fn to_members(&self) -> Vec<Vec<u8>> {
    self.set.iter().cloned().collect()
  }

  /// 对象操作统一入口（RESP 分派，具体实现在 partial 分片中）
  ///
  /// 刻意差异：C# 对 switch default 抛 GarnetException（SMOVE 与
  /// SUNION/SINTER/SDIFF 族为 storage 侧多键聚合，不经对象层分派），
  /// Rust 以错误回复表达（会话层异常最终也落为错误回复）
  ///
  /// libs/server/Objects/Set/SetObject.cs:Operate
  pub fn operate(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) -> bool {
    // 类型不符直接回 WrongType（对标 C# 先查 header.type）
    if input.header.data[0] != GarnetObjectType::Set as u8 {
      output.output_flags |= ObjectOutputFlags::WRONG_TYPE;
      output.payload.clear();
      return true;
    }

    let Some(op) = set_op_from_header(input) else {
      // C#: switch default 抛 GarnetException("Unsupported operation ...")
      output.write_error(b"ERR unsupported operation");
      return true;
    };

    match op {
      SetOperation::Sadd => self.set_add(input, output),
      SetOperation::Smembers => self.set_members(output, resp_protocol_version),
      SetOperation::Sismember => self.set_is_member(input, output, resp_protocol_version),
      SetOperation::Smismember => self.set_multi_is_member(input, output, resp_protocol_version),
      SetOperation::Srem => self.set_remove(input, output),
      SetOperation::Scard => self.set_length(output),
      SetOperation::Spop => self.set_pop(input, output, resp_protocol_version),
      SetOperation::Srandmember => self.set_random_member(input, output, resp_protocol_version),
      SetOperation::Sscan => {
        self.scan_operate(input, output);
      }
      // SMOVE / SUNION / SINTER / SDIFF 族：C# 侧同样不经 SetObject.Operate 分派
      SetOperation::Smove
      | SetOperation::Sunion
      | SetOperation::Sunionstore
      | SetOperation::Sdiff
      | SetOperation::Sdiffstore
      | SetOperation::Sinter
      | SetOperation::Sinterstore => {
        output.write_error(b"ERR unsupported operation");
      }
    }

    if self.set.is_empty() {
      output.output_flags |= ObjectOutputFlags::REMOVE_KEY;
    }

    true
  }

  /// 条目内存记账（add=false 回收）
  ///
  /// libs/server/Objects/Set/SetObject.cs:UpdateSize
  pub fn update_size(&mut self, item: &[u8], add: bool) {
    // RoundUp(len, 8) + ByteArrayOverhead(16) + HashSetEntryOverhead
    let memory_size = (item.len().div_ceil(8) * 8 + 16 + 16) as i64;

    if add {
      self.heap_memory_size += memory_size;
    } else {
      self.heap_memory_size -= memory_size;
    }
  }

  /// SSCAN：遍历成员（光标 + MATCH/COUNT）
  ///
  /// libs/server/Objects/Set/SetObject.cs:Scan
  ///
  /// 与 hash 不同：单条目形态，count 不翻倍；`cursor == Set.Count` 即归零
  pub fn scan(&self, start: i64, count: i64, pattern: &[u8]) -> (Vec<Vec<u8>>, i64) {
    let mut items: Vec<Vec<u8>> = Vec::new();
    let mut cursor = start;

    if (self.set.len() as i64) < start {
      cursor = 0;
      return (items, cursor);
    }

    let mut index = 0_i64;
    for item in &self.set {
      if index < start {
        index += 1;
        continue;
      }

      if pattern.is_empty() || glob_match(pattern, item) {
        items.push(item.clone());
      }

      cursor += 1;

      // C# 以相等判断截断（负 COUNT 恒不命中 → 全量遍历，1:1 保留）
      if items.len() as i64 == count {
        break;
      }
    }

    // Indicates end of collection has been reached.
    if cursor == self.set.len() as i64 {
      cursor = 0;
    }

    (items, cursor)
  }
}

/// 从 ObjectInput 头部提取集合操作码（C# header.SetOp = subId）
#[inline]
pub fn set_op_from_header(input: &ObjectInput) -> Option<SetOperation> {
  SetOperation::try_from(input.header.sub_id()).ok()
}

impl SetObject {
  /// SSCAN 的对象层入口（解析光标/MATCH/COUNT 后走 [`Self::scan`]）
  ///
  /// libs/server/Objects/Types/GarnetObjectBase.cs:Scan(ref ObjectInput, ...)
  pub(crate) fn scan_operate(&mut self, input: &ObjectInput, output: &mut ObjectOutput) {
    scan_operate_shared(input, output, |cursor, count, pattern, _is_no_value| {
      self.scan(cursor, count, pattern)
    });
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  fn obj_with(members: &[&str]) -> SetObject {
    let mut obj = SetObject::new();
    for m in members {
      obj.set.insert(m.as_bytes().to_vec());
      obj.update_size(m.as_bytes(), true);
    }
    obj
  }

  #[test]
  fn serde_round_trip() {
    let obj = obj_with(&["a", "b", "c"]);
    let mut bytes = Vec::new();
    obj.serialize(&mut bytes).unwrap();
    let restored = SetObject::deserialize(&mut io::Cursor::new(&bytes)).unwrap();
    assert_eq!(restored.set.len(), 3);
    assert!(restored.set.contains(b"a".as_slice()));
  }

  #[test]
  fn members_round_trip_bitcode_path() {
    let obj = obj_with(&["x", "y"]);
    let restored = SetObject::from_members(obj.to_members());
    assert_eq!(restored.set.len(), 2);
  }

  #[test]
  fn scan_single_item_form() {
    let obj = obj_with(&["a", "b", "c", "d"]);

    // 单条目形态：count=2 取两枚即停（count 不翻倍）
    let (items, cursor) = obj.scan(0, 2, b"");
    assert_eq!(items.len(), 2);
    assert_eq!(cursor, 2);

    let (items, cursor) = obj.scan(2, 2, b"");
    assert_eq!(items.len(), 2);
    assert_eq!(cursor, 0); // 末页耗尽归零

    // MATCH 过滤
    let (items, _) = obj.scan(0, 10, b"a*");
    assert_eq!(items, [b"a".to_vec()]);

    // start 超集 → 光标归零
    let (items, cursor) = obj.scan(99, 10, b"");
    assert!(items.is_empty());
    assert_eq!(cursor, 0);

    // count=0 怪癖：全量遍历
    let (items, cursor) = obj.scan(0, 0, b"");
    assert_eq!(items.len(), 4);
    assert_eq!(cursor, 0);
  }
}
