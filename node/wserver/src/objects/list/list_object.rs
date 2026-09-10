//! 列表对象（对标 libs/server/Objects/List/ListObject.cs + LinkedListHelper.cs）
//!
//! 刻意差异（对照 C#）：C# 以 `LinkedList<byte[]>` 承载（节点式双向链表），
//! Rust 以 `VecDeque<Vec<u8>>` 承载——头尾进出 O(1)，中段插删经
//! `insert`/`remove` 线性扫描，语义与 C# 节点操作一一对应。

use std::{
  collections::VecDeque,
  io::{self, Read, Write},
};

use crate::{
  inputs::ObjectInput,
  objects::types::object_output::{ObjectOutput, ObjectOutputFlags},
  types::GarnetObjectType,
};

/// 列表操作（AOF 持久化值，C# 侧为显式追加语义，不得改序/复用既有值）
///
/// libs/server/Objects/List/ListObject.cs:ListOperation
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, num_enum::TryFromPrimitive, num_enum::IntoPrimitive,
)]
#[repr(u8)]
pub enum ListOperation {
  Lpop = 0,
  Lpush = 1,
  Lpushx = 2,
  Rpop = 3,
  Rpush = 4,
  Rpushx = 5,
  Llen = 6,
  Ltrim = 7,
  Lrange = 8,
  Lindex = 9,
  Linsert = 10,
  Lrem = 11,
  Rpoplpush = 12,
  Lmove = 13,
  Lset = 14,
  Brpop = 15,
  Blpop = 16,
  Lpos = 17,
}

/// 列表操作方向（头/尾）
///
/// libs/server/Objects/List/ListObject.cs:OperationDirection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum OperationDirection {
  /// 左端（头）
  Left = 0,
  /// 右端（尾）
  Right = 1,
  /// 未知（参数解析失败）
  Unknown = 2,
}

/// 列表对象
///
/// libs/server/Objects/List/ListObject.cs:ListObject
#[derive(Debug, Clone, Default)]
pub struct ListObject {
  /// 双端队列（头为 list 首元素，尾为末元素）
  pub list: VecDeque<Vec<u8>>,
  /// 堆内存记账（相对值；C# 语义见文件头刻意差异）
  pub heap_memory_size: i64,
}

impl ListObject {
  /// 构造空列表
  ///
  /// libs/server/Objects/List/ListObject.cs:ListObject()
  pub fn new() -> Self {
    Self::default()
  }

  /// 从 C# BinaryWriter 序列化格式反序列化
  ///
  /// libs/server/Objects/List/ListObject.cs:ListObject(BinaryReader)
  pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
    let mut obj = Self::new();

    let mut len_buf = [0_u8; 4];
    reader.read_exact(&mut len_buf)?;
    let count = i32::from_le_bytes(len_buf);

    for _ in 0..count {
      reader.read_exact(&mut len_buf)?;
      let mut item = vec![0_u8; i32::from_le_bytes(len_buf) as usize];
      reader.read_exact(&mut item)?;
      obj.list.push_back(item.clone());
      obj.update_size(&item, true);
    }

    Ok(obj)
  }

  /// 序列化为 C# BinaryWriter 兼容格式
  ///
  /// libs/server/Objects/List/ListObject.cs:DoSerialize
  pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
    writer.write_all(&(self.list.len() as i32).to_le_bytes())?;
    for item in &self.list {
      writer.write_all(&(item.len() as i32).to_le_bytes())?;
      writer.write_all(item)?;
    }
    Ok(())
  }

  /// 与 wkv 对象存储层的既有载荷格式（wobject bitcode `Vec<u8>` 数组）互转
  ///
  /// 无 C# 对应（wkv blob 装载入口）
  pub fn from_items(items: Vec<Vec<u8>>) -> Self {
    let mut obj = Self::new();
    for item in items {
      obj.list.push_back(item.clone());
      obj.update_size(&item, true);
    }
    obj
  }

  /// 导出为元素数组（`from_items` 的逆操作）
  ///
  /// 无 C# 对应（wkv blob 回写出口）
  pub fn to_items(&self) -> Vec<Vec<u8>> {
    self.list.iter().cloned().collect()
  }

  /// 对象操作统一入口（RESP 分派，具体实现在 partial 分片中）
  ///
  /// 刻意差异：C# 对 switch default 抛 GarnetException（LPOP/RPOP/LMOVE 等经
  /// 默认分支；阻塞族与 RPOPLPUSH/LMOVE 由命令层/信使层承载），Rust 以错误
  /// 回复表达（会话层异常最终也落为错误回复）
  ///
  /// libs/server/Objects/List/ListObject.cs:Operate
  pub fn operate(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) -> bool {
    // 类型不符直接回 WrongType（对标 C# 先查 header.type）
    if input.header.data[0] != GarnetObjectType::List as u8 {
      output.output_flags |= ObjectOutputFlags::WRONG_TYPE;
      output.payload.clear();
      return true;
    }

    let Some(op) = list_op_from_header(input) else {
      // C#: switch default 抛 GarnetException("Unsupported operation ...")
      output.write_error(b"ERR unsupported operation");
      return true;
    };

    match op {
      ListOperation::Lpush | ListOperation::Lpushx => self.list_push(input, output, true),
      ListOperation::Rpush | ListOperation::Rpushx => self.list_push(input, output, false),
      ListOperation::Lpop => self.list_pop(input, output, resp_protocol_version, true),
      ListOperation::Rpop => self.list_pop(input, output, resp_protocol_version, false),
      ListOperation::Llen => self.list_length(output),
      ListOperation::Ltrim => self.list_trim(input, output),
      ListOperation::Lrange => self.list_range(input, output, resp_protocol_version),
      ListOperation::Lindex => self.list_index(input, output, resp_protocol_version),
      ListOperation::Linsert => self.list_insert(input, output),
      ListOperation::Lrem => self.list_remove(input, output),
      ListOperation::Lset => self.list_set(input, output, resp_protocol_version),
      ListOperation::Lpos => self.list_position(input, output, resp_protocol_version),
      // RPOPLPUSH/LMOVE/阻塞族：C# 侧同样不经 ListObject.Operate 分派
      // （LMOVE 走 storageApi.ListMove 双键操作，阻塞族走 ItemBroker）
      ListOperation::Rpoplpush
      | ListOperation::Lmove
      | ListOperation::Brpop
      | ListOperation::Blpop => {
        output.write_error(b"ERR unsupported operation");
      }
    }

    if self.list.is_empty() {
      output.output_flags |= ObjectOutputFlags::REMOVE_KEY;
    }

    true
  }

  /// 条目内存记账（add=false 回收）
  ///
  /// libs/server/Objects/List/ListObject.cs:UpdateSize
  pub fn update_size(&mut self, item: &[u8], add: bool) {
    // RoundUp(len, 8) + ByteArrayOverhead(16) + ListEntryOverhead
    let memory_size = (item.len().div_ceil(8) * 8 + 16 + 16) as i64;

    if add {
      self.heap_memory_size += memory_size;
    } else {
      self.heap_memory_size -= memory_size;
    }
  }

  /// 链表节点序列（下标 0..len；C# Nodes() 供按位定位节点，此处以相同
  /// 遍历形态产出节点下标）
  ///
  /// libs/server/Objects/List/ListObject.cs:Nodes
  #[inline]
  pub fn nodes(&self) -> impl Iterator<Item = usize> {
    0..self.list.len()
  }
}

/// 从 ObjectInput 头部提取列表操作码（C# header.ListOp = subId）
#[inline]
pub fn list_op_from_header(input: &ObjectInput) -> Option<ListOperation> {
  ListOperation::try_from(input.header.sub_id()).ok()
}

#[cfg(test)]
mod tests {
  use super::*;

  fn obj_with_items(items: &[&str]) -> ListObject {
    let mut obj = ListObject::new();
    for item in items {
      obj.list.push_back(item.as_bytes().to_vec());
      obj.update_size(item.as_bytes(), true);
    }
    obj
  }

  #[test]
  fn serde_round_trip() {
    let obj = obj_with_items(&["a", "b", "c"]);
    let mut bytes = Vec::new();
    obj.serialize(&mut bytes).unwrap();
    let restored = ListObject::deserialize(&mut io::Cursor::new(&bytes)).unwrap();
    assert_eq!(restored.to_items(), obj.to_items());
    assert_eq!(restored.list.len(), 3);
  }

  #[test]
  fn items_round_trip_bitcode_path() {
    let obj = obj_with_items(&["x", "y"]);
    let restored = ListObject::from_items(obj.to_items());
    assert_eq!(restored.to_items(), vec![b"x".to_vec(), b"y".to_vec()]);
  }

  #[test]
  fn nodes_walk_all_positions() {
    let obj = obj_with_items(&["a", "b", "c"]);
    assert_eq!(obj.nodes().collect::<Vec<_>>(), vec![0, 1, 2]);
  }
}
