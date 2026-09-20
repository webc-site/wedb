//! 集合对象（对标 libs/server/Objects/Set/SetObject.cs）
//!
//! 结构与 C# 一致：`set`（成员散列集合）+ 堆内存记账 `heap_memory_size`。
//! 集合无成员级过期（C# 亦无），基数即 `set.len()`。
//!
//! 记账口径（刻意差异，对照 C# `HeapMemorySize` 的 .NET GC 开销记账）：Rust 不照搬
//! `MemoryUtils.*Overhead`，改按「容器常驻基线 + 每条目相对字节数」自定口径具名记账，
//! 单点定义见 [`wbase::heap`]（`SLOT` / `CONTAINER_BASE`）。

use std::io::{self, Read, Write};

use wbase::{
  glob::glob_match,
  heap::{CONTAINER_BASE, SLOT, round_up_ptr},
  map::HashSet,
};
use wresp::{
  cmd_strings::RESP_ERR_GENERIC_UNSUPPORTED_OPERATION as RESP_ERR_UNSUPPORTED_OPERATION,
  resp_memory_writer::RespWriter,
};
use wval::GarnetObjectType;

use crate::{
  hash::hash_object::scan_operate_shared,
  object_payload::{COUNT_BLOB_HEADER, GarnetObjectPayload},
  types::{ObjectOutput, ObjectOutputFlags},
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
#[derive(Debug, Clone)]
pub struct SetObject {
  /// 成员散列集合
  pub set: HashSet<Vec<u8>>,
  /// 堆内存记账（相对值；C# 语义见文件头记账口径 [`wbase::heap`]）
  pub heap_memory_size: i64,
}

/// 构造即带容器常驻基线（对位 C# `SetObject` 构造 `base(HashSetOverhead)`）：
/// 空集合 `heap_memory_size` 非零，[`SetObject::update_size`] 回收不变式以此为准。
impl Default for SetObject {
  fn default() -> Self {
    Self {
      set: HashSet::default(),
      heap_memory_size: CONTAINER_BASE,
    }
  }
}

impl SetObject {
  /// 构造空集合
  ///
  /// libs/server/Objects/Set/SetObject.cs:SetObject()
  pub fn new() -> Self {
    Self::default()
  }

  /// 成员数量
  #[inline]
  pub fn len(&self) -> usize {
    self.set.len()
  }

  /// 是否为空集合
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.set.is_empty()
  }

  /// 直接从二进制切片反序列化集合对象（零中间 buffer 拷贝）
  pub fn deserialize_from_slice(slice: &[u8]) -> io::Result<Self> {
    let set: HashSet<Vec<u8>> =
      bitcode::decode(slice).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut obj = Self::new();
    for item in &set {
      obj.update_size(item, true);
    }
    obj.set = set;

    Ok(obj)
  }

  /// 从二进制流反序列化集合对象
  pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;
    Self::deserialize_from_slice(&buf)
  }

  /// 直接序列化为字节向量（零封装中转开销）
  ///
  /// 在 garnet 中的相对路径:libs/server/Objects/Set/SetObject.cs:DoSerialize
  #[inline]
  pub fn serialize_to_vec(&self) -> Vec<u8> {
    bitcode::encode(&self.set)
  }

  pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
    writer.write_all(&self.serialize_to_vec())
  }

  /// 获取成员总数
  pub fn count(&self) -> usize {
    self.set.len()
  }

  /// 添加成员
  pub fn add(&mut self, member: &[u8]) -> bool {
    if self.set.insert(member.to_vec()) {
      self.update_size(member, true);
      true
    } else {
      false
    }
  }

  /// 移除成员
  pub fn remove(&mut self, member: &[u8]) -> bool {
    if self.set.remove(member) {
      self.update_size(member, false);
      true
    } else {
      false
    }
  }

  /// 包含成员
  pub fn contains(&self, member: &[u8]) -> bool {
    self.set.contains(member)
  }

  /// 弹出一个成员
  pub fn pop(&mut self) -> Option<Vec<u8>> {
    let item = self.set.iter().next().cloned()?;
    self.set.remove(&item);
    self.update_size(&item, false);
    Some(item)
  }

  /// 导出为成员数组（SINTER/SUNION/SDIFF 结果的 RESP 输出视图）
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
    sub_id: u8,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) -> bool {
    let Some(op) = SetOperation::try_from(sub_id).ok() else {
      // C#: switch default 抛 GarnetException("Unsupported operation ...")
      RespWriter::new_ref(output.payload)
        .write_error_bytes(RESP_ERR_UNSUPPORTED_OPERATION.as_bytes());
      return true;
    };

    match op {
      SetOperation::Sadd => self.set_add(args, output),
      SetOperation::Smembers => self.set_members(output, resp_protocol_version),
      SetOperation::Sismember => self.set_is_member(args, output),
      SetOperation::Smismember => self.set_multi_is_member(args, output),
      SetOperation::Srem => self.set_remove(args, output),
      SetOperation::Scard => self.set_length(output),
      SetOperation::Spop => self.set_pop(args, arg1, output, resp_protocol_version),
      SetOperation::Srandmember => {
        self.set_random_member(args, arg1, arg2, output, resp_protocol_version);
      }
      SetOperation::Sscan => {
        self.scan_operate(args, arg2, output);
      }
      // SMOVE / SUNION / SINTER / SDIFF 族：C# 侧同样不经 SetObject.Operate 分派
      SetOperation::Smove
      | SetOperation::Sunion
      | SetOperation::Sunionstore
      | SetOperation::Sdiff
      | SetOperation::Sdiffstore
      | SetOperation::Sinter
      | SetOperation::Sinterstore => {
        RespWriter::new_ref(output.payload)
          .write_error_bytes(RESP_ERR_UNSUPPORTED_OPERATION.as_bytes());
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
    // 数据实计 + 两槽（member 句柄 + 集合槽位）
    // C#: RoundUp(len,IntPtr.Size) + ByteArrayOverhead + HashSetEntryOverhead，rust 口径塌缩为 SLOT*2
    let memory_size = round_up_ptr(item.len()) as i64 + SLOT * 2;

    if add {
      self.heap_memory_size += memory_size;
    } else {
      self.heap_memory_size -= memory_size;
      // C#: Debug.Assert(HeapMemorySize >= HashSetOverhead)
      debug_assert!(self.heap_memory_size >= CONTAINER_BASE);
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

impl SetObject {
  /// SSCAN 的对象层入口，转发至 [`scan_operate_shared`]。
  pub(crate) fn scan_operate(&mut self, args: &[&[u8]], limit: i32, output: &mut ObjectOutput<'_>) {
    scan_operate_shared(args, limit, output, |cursor, count, pattern, _| {
      self.scan(cursor, count, pattern)
    });
  }
}

impl GarnetObjectPayload for SetObject {
  const OBJECT_TAG: GarnetObjectType = GarnetObjectType::Set;

  #[inline]
  fn from_blob(raw: &[u8]) -> Option<Self> {
    // 单格式：载荷恒带 4B 计数头（to_blob 恒落头；Set 无成员级 TTL，无水位
    // 域），无头二次回退臂已删；畸形载荷显式失败（对标 C#
    // GarnetObjectSerializer.DeserializeInternal fail-fast）
    Self::deserialize_from_slice(raw.get(COUNT_BLOB_HEADER..)?).ok()
  }

  #[inline]
  fn to_blob(&self) -> Vec<u8> {
    let count = self.set.len() as u32;
    let payload = self.serialize_to_vec();
    let mut out = Vec::with_capacity(COUNT_BLOB_HEADER + payload.len());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&payload);
    out
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.set.is_empty()
  }
}
