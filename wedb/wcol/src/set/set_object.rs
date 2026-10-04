//! 集合对象（对标 libs/server/Objects/Set/SetObject.cs）
//!
//! 结构与 C# 一致：`set`（成员散列集合）+ 堆内存记账 `heap_memory_size`。
//! 集合无成员级过期（C# 亦无），基数即 `set.len()`。
//!
//! 记账口径（刻意差异，对照 C# `HeapMemorySize` 的 .NET GC 开销记账）：Rust 不照搬
//! `MemoryUtils.*Overhead`，改按「容器常驻基线 + 每条目相对字节数」自定口径具名记账，
//! 单点定义见 [`wbase::heap`]（`SLOT` / `CONTAINER_BASE`）。

use std::io;

use wbase::{
  heap::{CONTAINER_BASE, SLOT, round_up_ptr},
  map::HashSet,
};
use wresp::{
  cmd_strings::RESP_ERR_GENERIC_UNSUPPORTED_OPERATION as RESP_ERR_UNSUPPORTED_OPERATION,
  resp_memory_writer::RespWriter,
};
use wval::GarnetObjectType;

use crate::{
  object_payload::{COUNT_BLOB_HEADER, GarnetObjectPayload},
  types::{ObjectOutput, ObjectOutputFlags, scan_kernel, scan_operate_shared},
};

/// 集合操作（AOF 持久化值，C# 侧为显式追加语义，不得改序/复用既有值）
///
/// libs/server/Objects/Set/SetObject.cs:SetOperation
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::FromRepr)]
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
#[derive(Debug)]
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

  /// 由已装配的成员散列集合构造并逐成员恢复堆记账
  ///
  /// 全仓唯一的「零记账集合 → 足额记账」恢复机制，对位 C#
  /// libs/server/Storage/Session/ObjectStore/SetOps.cs 三 STORE 臂的
  /// `foreach (item in members) { newSetObject.Set.Add(item); newSetObject.UpdateSize(item); }`
  /// 与 zset 侧 `from_entries` 的逐成员计账口径：集合代数的交/并/差已在裸
  /// `HashSet` 上完成去重与收缩，本构造仅按最终存留成员逐项 `update_size` 计入，
  /// 保证构造态 `heap_memory_size` 与内容恒一致（`obj_save_or_gc` 升阶体积门判据的前提）。
  ///
  /// 无独立 C# 对应（本仓信封 blob 与聚合 builder 的共用记账恢复出口）
  pub fn from_members(set: HashSet<Vec<u8>>) -> Self {
    let mut obj = Self::new();
    for item in &set {
      obj.update_size(item, true);
    }
    obj.set = set;
    obj
  }

  /// 直接从二进制切片反序列化集合对象（零中间 buffer 拷贝）
  pub fn deserialize_from_slice(slice: &[u8]) -> io::Result<Self> {
    let set: HashSet<Vec<u8>> =
      bitcode::decode(slice).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(Self::from_members(set))
  }

  /// 直接序列化为字节向量（零封装中转开销）
  ///
  /// 在 garnet 中的相对路径:libs/server/Objects/Set/SetObject.cs:DoSerialize
  #[inline]
  pub fn serialize_to_vec(&self) -> Vec<u8> {
    bitcode::encode(&self.set)
  }

  /// 添加成员（插入/堆记账配对单点：成功插入方记账，重复成员 contains 短路
  /// 零分配——生产臂 SADD 与测试参考态共用本入口，严禁旁路内联第二实现）
  pub fn add(&mut self, member: &[u8]) -> bool {
    if self.set.contains(member) {
      return false;
    }
    self.set.insert(member.to_vec());
    self.update_size(member, true);
    true
  }

  /// 包含成员
  pub fn contains(&self, member: &[u8]) -> bool {
    self.set.contains(member)
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
    let Some(op) = SetOperation::from_repr(sub_id) else {
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
  /// 与 hash 不同：单条目形态，count 不翻倍。emit 回调借成员切片直写出帧
  /// （返回发出条目计数，替代 C# out List 的引用收集——集合内字节零拷贝，
  /// 无逐条目堆物化），游标由本内核返回
  ///
  /// 迭代/跳过/匹配/截断/收敛骨架经单点 [`scan_kernel`]（与 hash/zset 侧
  /// 同链收敛）：set 无成员级过期语义（C# 亦无），以恒存活判定 `|_| false`
  /// 接入——到期垫数恒 0，收敛臂 `>=` 退化为与 C# `==` 逐字节等价
  /// （游标本轮增量至多补齐到总量，恒不会越过）
  pub fn scan(
    &self,
    start: i64,
    count: i64,
    pattern: &[u8],
    mut emit: impl FnMut(&[u8]) -> usize,
  ) -> i64 {
    scan_kernel(
      self.set.len() as i64,
      start,
      count,
      pattern,
      self.set.iter().map(|item| (item.as_slice(), ())),
      |_| false,
      |item, ()| emit(item),
    )
  }
}

impl SetObject {
  /// SSCAN 的对象层入口，转发至 [`scan_operate_shared`]（总量即 set.len()，
  /// 供游标/条目帧位预留估宽；NOVALUES 对 set 无语义，仅保守估宽减半）。
  pub(crate) fn scan_operate(&mut self, args: &[&[u8]], limit: i32, output: &mut ObjectOutput<'_>) {
    scan_operate_shared(
      args,
      limit,
      output,
      self.set.len(),
      |cursor, count, pattern, _no_values, sink| {
        self.scan(cursor, count, pattern, |member| {
          sink.emit(member);
          1
        })
      },
    );
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

impl From<SetOperation> for u8 {
  #[inline]
  fn from(op: SetOperation) -> Self {
    op as u8
  }
}
