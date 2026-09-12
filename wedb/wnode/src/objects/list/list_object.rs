//! 列表对象（对标 libs/server/Objects/List/ListObject.cs + LinkedListHelper.cs）
//!
//! 刻意差异（对照 C#）：C# 以 `LinkedList<byte[]>` 承载（节点式双向链表），
//! Rust 以 `VecDeque<Vec<u8>>` 承载——头尾进出 O(1)，中段插删经
//! `insert`/`remove` 线性扫描，语义与 C# 节点操作一一对应。

use std::{
  collections::VecDeque,
  io::{self, Read, Write},
};

use wresp::cmd_strings::RESP_ERR_GENERIC_UNSUPPORTED_OPERATION as RESP_ERR_UNSUPPORTED_OPERATION;

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

#[derive(
  Debug, Clone, Copy, PartialEq, Eq, num_enum::TryFromPrimitive, num_enum::IntoPrimitive,
)]
#[repr(u8)]
pub enum OperationDirection {
  Left = 0,
  Right = 1,
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

  /// 从二进制流反序列化列表对象
  pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;
    let list: VecDeque<Vec<u8>> =
      bitcode::decode(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut obj = Self::new();
    for item in &list {
      obj.update_size(item, true);
    }
    obj.list = list;
    Ok(obj)
  }

  /// 序列化列表对象为二进制流
  pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
    let bytes = bitcode::encode(&self.list);
    writer.write_all(&bytes)
  }

  /// 列表长度
  pub fn len(&self) -> usize {
    self.list.len()
  }

  /// 元素数量
  pub fn count(&self) -> usize {
    self.list.len()
  }

  /// 是否为空
  pub fn is_empty(&self) -> bool {
    self.list.is_empty()
  }

  /// 列表基础操作派发
  pub fn operate_basic(&mut self, op: ListOperation, item: &[u8]) -> Option<Vec<u8>> {
    match op {
      ListOperation::Lpush => {
        self.update_size(item, true);
        self.list.push_front(item.to_vec());
        None
      }
      ListOperation::Rpush => {
        self.update_size(item, true);
        self.list.push_back(item.to_vec());
        None
      }
      ListOperation::Lpop => {
        let val = self.list.pop_front()?;
        self.update_size(&val, false);
        Some(val)
      }
      ListOperation::Rpop => {
        let val = self.list.pop_back()?;
        self.update_size(&val, false);
        Some(val)
      }
      _ => None,
    }
  }

  /// 按索引取元素
  pub fn index(&self, index: isize) -> Option<Vec<u8>> {
    let len = self.list.len() as isize;
    let actual_idx = if index < 0 { len + index } else { index };
    if actual_idx < 0 || actual_idx >= len {
      None
    } else {
      self.list.get(actual_idx as usize).cloned()
    }
  }

  /// 区间切片
  pub fn range(&self, start: isize, stop: isize) -> Vec<Vec<u8>> {
    let len = self.list.len() as isize;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };

    if s < 0 {
      s = 0;
    }
    if e >= len {
      e = len - 1;
    }
    if s > e || s >= len {
      return vec![];
    }

    self
      .list
      .range((s as usize)..=(e as usize))
      .cloned()
      .collect()
  }

  /// 区间裁剪
  pub fn trim(&mut self, start: isize, stop: isize) {
    let len = self.list.len() as isize;
    let mut s = if start < 0 { len + start } else { start };
    let mut e = if stop < 0 { len + stop } else { stop };

    if s < 0 {
      s = 0;
    }
    if e >= len {
      e = len - 1;
    }
    if s > e || s >= len {
      self.list.clear();
      self.heap_memory_size = 0;
      return;
    }

    self.list.truncate((e + 1) as usize);
    for _ in 0..s {
      if let Some(item) = self.list.pop_front() {
        self.update_size(&item, false);
      }
    }
  }

  /// 与 wkv 对象存储层的载荷格式互转
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
      output.write_error(RESP_ERR_UNSUPPORTED_OPERATION.as_bytes());
      return true;
    };

    match op {
      ListOperation::Lpush | ListOperation::Lpushx => self.list_push(input, output, true),
      ListOperation::Rpush | ListOperation::Rpushx => self.list_push(input, output, false),
      ListOperation::Lpop => self.list_pop(input, output, resp_protocol_version, true),
      ListOperation::Rpop => self.list_pop(input, output, resp_protocol_version, false),
      ListOperation::Llen => self.list_length(output),
      ListOperation::Ltrim => self.list_trim(input, output),
      ListOperation::Lrange => self.list_range(input, output),
      ListOperation::Lindex => self.list_index(input, output),
      ListOperation::Linsert => self.list_insert(input, output),
      ListOperation::Lrem => self.list_remove(input, output),
      ListOperation::Lset => self.list_set(input, output),
      ListOperation::Lpos => self.list_position(input, output),
      // RPOPLPUSH/LMOVE/阻塞族：C# 侧同样不经 ListObject.Operate 分派
      // （LMOVE 走 storageApi.ListMove 双键操作，阻塞族走 ItemBroker）
      ListOperation::Rpoplpush
      | ListOperation::Lmove
      | ListOperation::Brpop
      | ListOperation::Blpop => {
        output.write_error(RESP_ERR_UNSUPPORTED_OPERATION.as_bytes());
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
