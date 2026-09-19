//! 列表对象（对标 libs/server/Objects/List/ListObject.cs + LinkedListHelper.cs）
//!
//! 刻意差异（对照 C#）：C# 以 `LinkedList<byte[]>` 承载（节点式双向链表），
//! Rust 以 `VecDeque<Vec<u8>>` 承载——头尾进出 O(1)，中段插删经
//! `insert`/`remove` 线性扫描，语义与 C# 节点操作一一对应。
//!
//! 记账口径（刻意差异，对照 C# `HeapMemorySize` 的 .NET GC 开销记账）：Rust 不照搬
//! `MemoryUtils.*Overhead`，改按「容器常驻基线 + 每条目相对字节数」自定口径具名记账，
//! 单点定义见 [`wbase::heap`]（`SLOT` / `CONTAINER_BASE`）。

use std::{
  collections::VecDeque,
  io::{self, Read, Write},
};

use wbase::heap::{CONTAINER_BASE, SLOT, round_up_ptr};
use wresp::{
  cmd_strings::RESP_ERR_GENERIC_UNSUPPORTED_OPERATION as RESP_ERR_UNSUPPORTED_OPERATION,
  resp_memory_writer::RespWriter,
};
use wval::GarnetObjectType;

use crate::{
  object_payload::GarnetObjectPayload,
  types::{ObjectOutput, ObjectOutputFlags},
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
#[derive(Debug, Clone)]
pub struct ListObject {
  /// 双端队列（头为 list 首元素，尾为末元素）
  pub list: VecDeque<Vec<u8>>,
  /// 堆内存记账（相对值；C# 语义见文件头记账口径 [`wbase::heap`]）
  pub heap_memory_size: i64,
}

/// 构造即带容器常驻基线（对位 C# `ListObject` 构造 `base(ListOverhead)`）：空列表
/// `heap_memory_size` 非零，[`ListObject::update_size`] 回收不变式以此为准。
impl Default for ListObject {
  fn default() -> Self {
    Self {
      list: VecDeque::new(),
      heap_memory_size: CONTAINER_BASE,
    }
  }
}

impl ListObject {
  /// 构造空列表
  ///
  /// libs/server/Objects/List/ListObject.cs:ListObject()
  pub fn new() -> Self {
    Self::default()
  }

  /// 直接从二进制切片反序列化列表对象（零中间 buffer 拷贝）
  pub fn deserialize_from_slice(slice: &[u8]) -> io::Result<Self> {
    let list: VecDeque<Vec<u8>> =
      bitcode::decode(slice).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut obj = Self::new();
    for item in &list {
      obj.update_size(item, true);
    }
    obj.list = list;
    Ok(obj)
  }

  /// 从二进制流反序列化列表对象
  pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;
    Self::deserialize_from_slice(&buf)
  }

  /// 直接序列化为字节向量（零封装中转开销）
  ///
  /// 在 garnet 中的相对路径:libs/server/Objects/List/ListObject.cs:DoSerialize
  #[inline]
  pub fn serialize_to_vec(&self) -> Vec<u8> {
    bitcode::encode(&self.list)
  }

  /// 序列化列表对象为二进制流
  pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
    writer.write_all(&self.serialize_to_vec())
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

  /// 对象操作统一入口（RESP 分派，具体实现在 partial 分片中）
  ///
  /// 刻意差异：C# 对 switch default 抛 GarnetException（LPOP/RPOP/LMOVE 等经
  /// 默认分支；阻塞族与 RPOPLPUSH/LMOVE 由命令层/信使层承载），Rust 以错误
  /// 回复表达（会话层异常最终也落为错误回复）
  ///
  /// libs/server/Objects/List/ListObject.cs:Operate
  pub fn operate(
    &mut self,
    sub_id: u8,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) -> bool {
    let Some(op) = ListOperation::try_from(sub_id).ok() else {
      // C#: switch default 抛 GarnetException("Unsupported operation ...")
      RespWriter::new_ref(output.payload)
        .write_error_bytes(RESP_ERR_UNSUPPORTED_OPERATION.as_bytes());
      return true;
    };

    match op {
      ListOperation::Lpush | ListOperation::Lpushx => self.list_push(args, output, true),
      ListOperation::Rpush | ListOperation::Rpushx => self.list_push(args, output, false),
      ListOperation::Lpop => self.list_pop(args, arg1, output, resp_protocol_version, true),
      ListOperation::Rpop => self.list_pop(args, arg1, output, resp_protocol_version, false),
      ListOperation::Llen => self.list_length(output),
      ListOperation::Ltrim => self.list_trim(args, arg1, arg2, output),
      ListOperation::Lrange => self.list_range(args, arg1, arg2, output),
      ListOperation::Lindex => self.list_index(args, arg1, output),
      ListOperation::Linsert => self.list_insert(args, output),
      ListOperation::Lrem => self.list_remove(args, arg1, output),
      ListOperation::Lset => self.list_set(args, output),
      ListOperation::Lpos => self.list_position(args, output, resp_protocol_version),
      // RPOPLPUSH/LMOVE/阻塞族：C# 侧同样不经 ListObject.Operate 分派
      // （LMOVE 走 storageApi.ListMove 双键操作，阻塞族走 ItemBroker）
      ListOperation::Rpoplpush
      | ListOperation::Lmove
      | ListOperation::Brpop
      | ListOperation::Blpop => {
        RespWriter::new_ref(output.payload)
          .write_error_bytes(RESP_ERR_UNSUPPORTED_OPERATION.as_bytes());
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
    // 数据实计 + 两槽（item 句柄 + 链表节点槽位）
    // C#: RoundUp(len,IntPtr.Size) + ByteArrayOverhead + ListEntryOverhead，rust 口径塌缩为 SLOT*2
    let memory_size = round_up_ptr(item.len()) as i64 + SLOT * 2;

    if add {
      self.heap_memory_size += memory_size;
    } else {
      self.heap_memory_size -= memory_size;
      // C#: Debug.Assert(HeapMemorySize >= ListOverhead)
      debug_assert!(self.heap_memory_size >= CONTAINER_BASE);
    }
  }
}

impl GarnetObjectPayload for ListObject {
  const OBJECT_TAG: GarnetObjectType = GarnetObjectType::List;

  #[inline]
  fn from_blob(raw: &[u8]) -> Option<Self> {
    // 单格式：载荷恒带 4B 计数头（to_blob 恒落头），无头二次回退臂已删；
    // 畸形载荷显式失败（对标 C# GarnetObjectSerializer.DeserializeInternal fail-fast）
    Self::deserialize_from_slice(raw.get(4..)?).ok()
  }

  #[inline]
  fn to_blob(&self) -> Vec<u8> {
    let count = self.list.len() as u32;
    let payload = self.serialize_to_vec();
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&payload);
    out
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.list.is_empty()
  }
}
