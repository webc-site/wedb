//! 哈希对象（对标 libs/server/Objects/Hash/HashObject.cs）
//!
//! 结构与 C# 一致：`hash`（field → value 散列）+ 成员级过期结构
//! `expiration_times`（字典，惰性初始化）+ `expiration_queue`（最小堆，
//! 对应 C# PriorityQueue<byte[], long>），另有堆内存记账 `heap_memory_size`。
//!
//! 记账口径（刻意差异，对照 C# `HeapMemorySize` 的 .NET GC 开销记账）：Rust 不照搬
//! `MemoryUtils.*Overhead`，改按「结构常驻基线 + 每条目相对字节数」自定口径具名记账，
//! 单点定义见 [`wbase::heap`]（`ENTRY_SLOT` / `EXPIRY_STRUCT_BASE` / `HASH_STRUCT_BASE`）。

use std::{
  cmp::Reverse,
  collections::BinaryHeap,
  io::{self, Read, Write},
};

use fastrand::Rng;
use gxhash::{GxBuildHasher, HashMap, HashSet};
use wbase::{
  glob::glob_match,
  heap::{CONTAINER_BASE, EXPIRY_STRUCT_BASE, SLOT, round_up_ptr},
  time::now_ticks,
};
use wresp::{
  cmd_strings::RESP_ERR_GENERIC_UNSUPPORTED_OPERATION as RESP_ERR_UNSUPPORTED_OPERATION,
  options::ExpireOption, resp_memory_writer::RespWriter,
};
use wval::GarnetObjectType;

use crate::{
  object_payload::GarnetObjectPayload,
  types::{
    ObjectOutput, ObjectOutputFlags,
    expiration_queue::{ExpirationQueue, ExpirationQueueEntry},
    scan_input::read_scan_input,
  },
};

/// 哈希操作（AOF 持久化值，C# 侧为显式追加语义，不得改序/复用既有值）
///
/// libs/server/Objects/Hash/HashObject.cs:HashOperation
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, num_enum::TryFromPrimitive, num_enum::IntoPrimitive,
)]
#[repr(u8)]
pub enum HashOperation {
  Hcollect = 0,
  Hexpire = 1,
  Httl = 2,
  Hpersist = 3,
  Hget = 4,
  Hmget = 5,
  Hset = 6,
  Hmset = 7,
  Hsetnx = 8,
  Hlen = 9,
  Hdel = 10,
  Hexists = 11,
  Hgetall = 12,
  Hkeys = 13,
  Hvals = 14,
  Hincrby = 15,
  Hincrbyfloat = 16,
  Hrandfield = 17,
  Hscan = 18,
  Hstrlen = 19,
}

/// 成员级过期操作结果码
///
/// libs/server/Objects/Hash/HashObject.cs:ExpireResult
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum HashExpireResult {
  /// 键不存在
  KeyNotFound = -2,
  /// 过期条件不满足（NX/XX/GT/LT 语义）
  ExpireConditionNotMet = 0,
  /// 过期已更新
  ExpireUpdated = 1,
  /// 键已过期（给定时间戳在过去，条目被移除）
  KeyAlreadyExpired = 2,
}

/// 哈希对象
///
/// libs/server/Objects/Hash/HashObject.cs:HashObject
#[derive(Debug, Clone)]
pub struct HashObject {
  /// field → value 散列
  pub hash: HashMap<Vec<u8>, Vec<u8>>,
  /// field → 过期 ticks（惰性初始化）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:expirationTimes
  pub expiration_times: Option<HashMap<Vec<u8>, i64>>,
  /// 过期最小堆（与 expiration_times 同生命周期，惰性初始化）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:expirationQueue
  pub expiration_queue: Option<ExpirationQueue>,
  /// 堆内存记账（相对值；口径见文件头 [`wbase::heap`]）
  pub heap_memory_size: i64,
  /// 惰性过期剔除已实际移除条目（运行时状态，不进 [`HashWire`] 序列化）
  ///
  /// 刻意差异：C# 对象常驻 Tsavorite 对象缓存，HTTL 读路径的
  /// DeleteExpiredItems 就地剔除经 checkpoint 序列化落盘；Rust 信封无常驻
  /// 对象层，以该标志升格写回等价闭环，避免已剔除字段重装载复活
  ///
  /// 无 C# 对应（信封写回判定标志，见上方刻意差异）
  mutated_by_ttl: bool,
}

/// 构造即带容器常驻基线（对位 C# `HashObject` 构造 `base(DictionaryOverhead)`）：
/// 空集合 `heap_memory_size` 非零，[`HashObject::update_size`] 回收不变式以此为准。
impl Default for HashObject {
  fn default() -> Self {
    Self {
      hash: HashMap::default(),
      expiration_times: None,
      expiration_queue: None,
      heap_memory_size: CONTAINER_BASE,
      mutated_by_ttl: false,
    }
  }
}

/// 哈希线格式（bitcode 载荷，AOF/检查点回写）
///
/// 刻意差异：bitcode 0.6 的零拷贝借用编码仅支持 `&str`，`&[u8]` 无 `Encode`
/// 实现，编码侧无法借引条目免 clone（owned 收集为格式约束下的最优解）
#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
struct HashWire {
  entries: Vec<(Vec<u8>, Vec<u8>)>,
  expirations: Option<Vec<(Vec<u8>, i64)>>,
}

impl HashObject {
  /// 构造空哈希
  ///
  /// libs/server/Objects/Hash/HashObject.cs:HashObject()
  pub fn new() -> Self {
    Self::default()
  }

  /// 字段数量
  #[inline]
  pub fn len(&self) -> usize {
    self.hash.len()
  }

  /// 是否为空哈希
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.hash.is_empty()
  }

  /// 直接从二进制切片反序列化哈希对象（零中间 buffer 拷贝）
  pub fn deserialize_from_slice(slice: &[u8]) -> io::Result<Self> {
    let wire: HashWire =
      bitcode::decode(slice).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut obj = Self::new();
    obj.hash.reserve(wire.entries.len());
    let now = now_ticks();

    for (item, value) in wire.entries {
      obj.update_size(&item, &value, true);
      obj.hash.insert(item, value);
    }

    if let Some(expirations) = wire.expirations {
      for (item, expiration) in expirations {
        if expiration < now {
          if let Some(val) = obj.hash.remove(&item) {
            obj.update_size(&item, &val, false);
          }
        } else if obj.hash.contains_key(&item) {
          obj.insert_expiration(item, expiration);
        }
      }
      obj.cleanup_expiration_structures_if_empty();
    }

    Ok(obj)
  }

  /// 从二进制流反序列化哈希对象
  pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;
    Self::deserialize_from_slice(&buf)
  }

  /// 序列化为字节向量（过滤已过期条目；带过期成员附加 ticks）
  /// 序列化为线格式载荷，同时单次遍历产出存活条目数与 bitcode 载荷（严禁二次遍历）
  pub(crate) fn serialize_wire(&self) -> (u32, Vec<u8>) {
    let now = now_ticks();
    let has_expirations = self.expiration_times.is_some();
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(self.hash.len());
    for (k, v) in &self.hash {
      if !has_expirations || !self.is_expired_at(k, now) {
        entries.push((k.clone(), v.clone()));
      }
    }
    let count = entries.len() as u32;

    let expirations = self.expiration_times.as_ref().and_then(|times| {
      let mut active: Vec<(Vec<u8>, i64)> = Vec::with_capacity(times.len());
      for (k, exp) in times {
        if *exp >= now && self.hash.contains_key(k) {
          active.push((k.clone(), *exp));
        }
      }
      (!active.is_empty()).then_some(active)
    });

    let wire = bitcode::encode(&HashWire {
      entries,
      expirations,
    });
    (count, wire)
  }

  /// 序列化为字节向量（过滤已过期条目；带过期成员附加 ticks）
  ///
  /// 在 garnet 中的相对路径:libs/server/Objects/Hash/HashObject.cs:DoSerialize
  ///
  /// 单遍 filter 收集（容量预分配免 collect 增长重分配），替代原
  /// has_expirations 双分支遍历
  #[inline]
  pub fn serialize_to_vec(&self) -> Vec<u8> {
    self.serialize_wire().1
  }

  /// 序列化为二进制流（过滤已过期条目；带过期成员附加 ticks）
  pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
    writer.write_all(&self.serialize_to_vec())
  }

  /// 对象操作统一入口（RESP 分派，具体实现在 partial 分片中）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:Operate
  ///
  /// 刻意差异：C# 对 switch default 抛 GarnetException，
  /// Rust 以错误回复表达（会话层异常最终也落为错误回复）。
  /// 对象类型等同性由装载层（obj_decode / obj_load_typed_sync）保证，
  /// 对象层不再复读 header 类型字节
  pub fn operate(
    &mut self,
    sub_id: u8,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) -> bool {
    let Some(op) = HashOperation::try_from(sub_id).ok() else {
      // C#: switch default 抛 GarnetException("Unsupported operation ...")
      RespWriter::new_ref(&mut output.payload)
        .write_error_bytes(RESP_ERR_UNSUPPORTED_OPERATION.as_bytes());
      return true;
    };

    match op {
      HashOperation::Hset | HashOperation::Hmset | HashOperation::Hsetnx => {
        self.hash_set(sub_id, args, output);
      }
      HashOperation::Hget => self.hash_get(args, output, resp_protocol_version),
      HashOperation::Hmget => self.hash_multiple_get(args, output, resp_protocol_version),
      HashOperation::Hgetall => self.hash_get_all(output, resp_protocol_version),
      HashOperation::Hdel => self.hash_delete(args, output),
      HashOperation::Hlen => self.hash_length(output),
      HashOperation::Hstrlen => self.hash_str_length(args, output),
      HashOperation::Hexists => self.hash_exists(args, output),
      HashOperation::Hexpire => self.hash_expire(args, arg1, arg2, output),
      HashOperation::Httl => self.hash_time_to_live(args, arg1, arg2, output),
      HashOperation::Hpersist => self.hash_persist(args, output),
      HashOperation::Hkeys | HashOperation::Hvals => {
        self.hash_get_keys_or_values(sub_id, args, output);
      }
      HashOperation::Hincrby => self.hash_increment(args, output),
      HashOperation::Hincrbyfloat => self.hash_increment_float(args, output),
      HashOperation::Hrandfield => {
        self.hash_random_field(args, arg1, arg2, output, resp_protocol_version);
      }
      HashOperation::Hcollect => self.hash_collect(output),
      HashOperation::Hscan => self.scan_operate(args, arg2, output),
    }

    if self.hash.is_empty() {
      output.output_flags |= ObjectOutputFlags::REMOVE_KEY;
    }

    true
  }

  /// 条目内存记账（add=false 回收）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:UpdateSize
  #[inline]
  pub fn update_size(&mut self, key: &[u8], value: &[u8], add: bool) {
    // 数据实计 + 三槽（key 句柄 + value 句柄 + 字典槽位）
    // C#: RoundUp(key,IntPtr.Size) + RoundUp(value,IntPtr.Size)
    //     + 2*ByteArrayOverhead + DictionaryEntryOverhead（rust 口径塌缩为 SLOT*3，值不等）
    let memory_size = (round_up_ptr(key.len()) + round_up_ptr(value.len())) as i64 + SLOT * 3;

    if add {
      self.heap_memory_size += memory_size;
    } else {
      self.heap_memory_size -= memory_size;
      // C#: Debug.Assert(HeapMemorySize >= DictionaryOverhead)
      debug_assert!(self.heap_memory_size >= CONTAINER_BASE);
    }
  }

  /// 惰性创建过期字典 + 最小堆
  ///
  /// libs/server/Objects/Hash/HashObject.cs:InitializeExpirationStructures
  pub fn initialize_expiration_structures(&mut self) {
    if self.expiration_times.is_none() {
      self.expiration_times = Some(HashMap::with_hasher(GxBuildHasher::default()));
      self.expiration_queue = Some(BinaryHeap::new());
      self.heap_memory_size += EXPIRY_STRUCT_BASE;
    }
  }

  /// 挂成员过期条目（字典 + 最小堆 + 记账，结构未初始化时先建）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:expirationTimes.Add + expirationQueue.Enqueue
  /// （线格式装载与分层物化还原共用此单点）
  pub fn insert_expiration(&mut self, key: Vec<u8>, expiration: i64) {
    self.initialize_expiration_structures();
    if let Some(times) = self.expiration_times.as_mut() {
      times.insert(key.clone(), expiration);
    }
    if let Some(queue) = self.expiration_queue.as_mut() {
      queue.push(Reverse(ExpirationQueueEntry { expiration, key }));
    }
    self.update_expiration_size(true, true);
  }

  /// 过期条目内存记账（add=false 回收）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:UpdateExpirationSize
  #[inline]
  pub fn update_expiration_size(&mut self, add: bool, include_pq: bool) {
    // 字典项（key 句柄 + long ticks）两槽；含堆项再叠两槽
    // C#: UpdateExpirationSize = IntPtr.Size + sizeof(long) + DictionaryEntryOverhead
    //     (+ IntPtr.Size + sizeof(long) + PriorityQueueEntryOverhead if includePQ)
    //     口径塌缩为 SLOT 的整数倍，值不等
    let mut memory_size = SLOT * 2;
    if include_pq {
      memory_size += SLOT * 2;
    }

    if add {
      self.heap_memory_size += memory_size;
    } else {
      self.heap_memory_size -= memory_size;
      // C#: Debug.Assert(HeapMemorySize >= DictionaryOverhead)
      debug_assert!(self.heap_memory_size >= CONTAINER_BASE);
    }
  }

  /// 过期结构全空则整体回收
  ///
  /// libs/server/Objects/Hash/HashObject.cs:CleanupExpirationStructuresIfEmpty
  pub fn cleanup_expiration_structures_if_empty(&mut self) {
    let Some(times) = self.expiration_times.as_ref() else {
      return;
    };
    if !times.is_empty() {
      return;
    }

    if let Some(queue) = self.expiration_queue.as_ref() {
      // 逐堆项回收（C#: (IntPtr.Size + sizeof(long) + PriorityQueueEntryOverhead) * Count）
      self.heap_memory_size -= SLOT * 2 * queue.len() as i64;
    }
    self.heap_memory_size -= EXPIRY_STRUCT_BASE;
    // C#: CleanupExpirationStructuresIfEmpty 后 HeapMemorySize 回到主容器基线
    debug_assert!(self.heap_memory_size >= CONTAINER_BASE);
    self.expiration_times = None;
    self.expiration_queue = None;
  }

  /// HSCAN：遍历字段（光标 + MATCH/COUNT/NOVALUES）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:Scan
  ///
  /// 与 sortedset 不同：NOVALUES 生效（只回字段），且 count 在成对形态下先翻倍
  /// （`count = isNoValue ? count : count * 2`），恰集满 `count` 项即停
  /// （count=0 时永不触发的上游怪癖与 `==` 判断一并 1:1 保留）
  pub fn scan(
    &self,
    start: i64,
    count: i64,
    pattern: &[u8],
    is_no_value: bool,
  ) -> (Vec<Vec<u8>>, i64) {
    let mut items: Vec<Vec<u8>> = Vec::new();
    let mut cursor = start;

    if (self.hash.len() as i64) < start {
      cursor = 0;
      return (items, cursor);
    }

    // Hashset has key and value, so count is multiplied by 2
    let count = if is_no_value { count } else { count * 2 };
    let mut index = 0_i64;
    let mut expired_keys_count = 0_i64;

    for (key, value) in self.hash.iter() {
      if self.is_expired(key) {
        expired_keys_count += 1;
        continue;
      }

      if index < start {
        index += 1;
        continue;
      }

      if pattern.is_empty() || glob_match(pattern, key) {
        items.push(key.clone());
        if !is_no_value {
          items.push(value.clone());
        }
      }

      cursor += 1;

      // C# 以相等判断截断（负 COUNT 恒不命中 → 全量遍历；count=0 首个
      // 未命中条目即停的上游怪癖一并 1:1 保留）
      if items.len() as i64 == count {
        break;
      }
    }

    // 到达集合末尾则光标归零
    if cursor + expired_keys_count == self.hash.len() as i64 {
      cursor = 0;
    }

    (items, cursor)
  }

  /// 成员在给定时间戳是否已过期
  #[inline]
  pub fn is_expired_at(&self, key: &[u8], now: i64) -> bool {
    self
      .expiration_times
      .as_ref()
      .and_then(|t| t.get(key))
      .is_some_and(|&expiration| expiration < now)
  }

  /// 成员是否已过期
  ///
  /// libs/server/Objects/Hash/HashObject.cs:IsExpired
  #[inline]
  pub fn is_expired(&self, key: &[u8]) -> bool {
    self.is_expired_at(key, now_ticks())
  }

  /// 是否存在带过期结构的成员
  ///
  /// libs/server/Objects/Hash/HashObject.cs:HasExpirableItems
  #[inline]
  pub fn has_expirable_items(&self) -> bool {
    self.expiration_times.is_some()
  }

  /// 惰性过期剔除是否已实际移除条目（写回升格判定依据）
  #[inline]
  pub const fn mutated_by_ttl(&self) -> bool {
    self.mutated_by_ttl
  }

  /// 清除全部已过期成员（堆序快速路径）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:DeleteExpiredItems
  pub fn delete_expired_items(&mut self) {
    if self.expiration_times.is_none() {
      return;
    }
    self.delete_expired_items_worker();
  }

  /// libs/server/Objects/Hash/HashObject.cs:DeleteExpiredItemsWorker
  fn delete_expired_items_worker(&mut self) {
    // The PQ is ordered such that oldest items are dequeued first
    let now = now_ticks();
    while let Some(queue) = self.expiration_queue.as_mut() {
      let Some(Reverse(head)) = queue.peek() else {
        break;
      };
      if head.expiration >= now {
        break;
      }

      let key = head.key.clone();
      let expiration = head.expiration;

      // expirationTimes 与 expirationQueue 失步（续期重复入堆）时以
      // expirationTimes 为准，仅弹掉陈旧堆项
      let in_times = self
        .expiration_times
        .as_ref()
        .and_then(|t| t.get(&key))
        .is_some_and(|&actual| actual == expiration);

      if in_times {
        self.expiration_times.as_mut().unwrap().remove(&key);
        queue.pop();
        self.update_expiration_size(false, true);
        if let Some(value) = self.hash.get(&key).cloned() {
          self.hash.remove(&key);
          self.update_size(&key, &value, false);
          self.mutated_by_ttl = true;
        }
      } else {
        // The key was not in expirationTimes. It may have been Remove()d.
        queue.pop();

        // Adjust memory size for the priority queue entry removal.
        self.heap_memory_size -= SLOT * 2;
      }
    }

    self.cleanup_expiration_structures_if_empty();
  }

  /// 取字段值（过期字段视为不存在）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:TryGetValue
  #[inline]
  pub fn try_get_value(&self, key: &[u8]) -> Option<&Vec<u8>> {
    if self.is_expired(key) {
      return None;
    }
    self.hash.get(key)
  }

  /// 移除字段（含过期结构清理与记账）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:Remove
  pub fn remove(&mut self, key: &[u8]) -> Option<Vec<u8>> {
    self.delete_expired_items();
    let value = self.hash.remove(key)?;
    if self.has_expirable_items() {
      // PQ 无法定位移除，仅清字典项，残余堆项交下一次 DeleteExpiredItems 清理
      self.expiration_times.as_mut().unwrap().remove(key);
      self.update_expiration_size(false, false);
    }
    self.update_size(key, &value, false);
    Some(value)
  }

  /// 字段数（剔除已过期，O(1) 摊还）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:Count
  ///
  /// 与 C# 的刻意差异声明：C# Count()（HashObject.cs:510）逐键过滤
  /// expirationTimes，为 O(带 TTL 字段数) 只读计数；rust 依
  /// doc/zh/collection.md §6「计数命令严格 O(1)」规约改为先走堆序
  /// DeleteExpiredItems（摊还 O(弹过量)，稳态堆顶 peek 短路 O(1)），
  /// 再直读 hash.len()。应答值与 C# 逐值一致（两侧已过期项均不计入）；
  /// 物理剔除经 mutated_by_ttl 写回升格闭环，杜绝已剔除字段重装载复活
  /// （C# HTTL 读路径 DeleteExpiredItems 经 checkpoint 落盘同理）
  pub fn count(&mut self) -> usize {
    self.delete_expired_items();
    self.hash.len()
  }

  /// 字段是否存在（过期字段视为不存在）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:ContainsKey
  pub fn contains_key(&self, key: &[u8]) -> bool {
    self.hash.contains_key(key) && !self.is_expired(key)
  }

  /// 新增字段（调用方须已验证字段不存在）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:Add
  pub(crate) fn add(&mut self, key: &[u8], value: Vec<u8>) {
    // Called only when we have verified the key exists
    self.delete_expired_items();
    // update_size 仅读 len，先记账再 move，省一次 value.clone()
    self.update_size(key, &value, true);
    self.hash.insert(key.to_vec(), value);
  }

  /// 设置成员过期（NX/XX/GT/LT 语义），返回 [`HashExpireResult`] 码
  ///
  /// libs/server/Objects/Hash/HashObject.cs:SetExpiration
  ///
  /// 与 C# 的刻意差异声明（XX/GT 拒绝臂的字段存活）：C# 先以
  /// `CollectionsMarshal.GetValueRefOrAddDefault` 向过期字典插入值为 0 的
  /// 缺省项（HashObject.cs:581-586），其后才是 XX/GT 拒绝臂
  /// （:607-610）；0 值被 IsExpired 判恒过期（:429），被拒字段自此在
  /// HGET/HLEN/HEXPIRE 眼中一律视作不存在——而幻影项既未入
  /// expirationQueue、也未走 UpdateExpirationSize，队列驱动的
  /// DeleteExpiredItemsWorker（:443）永远清不掉它、结构回收条件亦永不
  /// 满足，即 HEXPIRE 应答 0 却静默丢字段且记账缺失，属 C# 侧缺陷。
  /// rust 以只读探测取现值（下方 current_expiration），拒绝臂零副作用、
  /// 字段存活；应答值与 C# 一致而后续可见状态更正确，为有意不复刻，
  /// 非未移植遗漏，禁止为「对齐 C#」复刻幻影项
  pub fn set_expiration(
    &mut self,
    key: &[u8],
    expiration: i64,
    expire_option: ExpireOption,
  ) -> HashExpireResult {
    if !self.contains_key(key) {
      return HashExpireResult::KeyNotFound;
    }

    if expiration <= now_ticks() {
      self.remove(key);
      return HashExpireResult::KeyAlreadyExpired;
    }

    self.initialize_expiration_structures();

    let current_expiration = self
      .expiration_times
      .as_ref()
      .and_then(|t| t.get(key))
      .copied();

    // 条件闸门：既有过期按 NX/GT/LT 判定；无过期按 XX/GT（C# 分支语义）
    let denied = match current_expiration {
      Some(current) => {
        expire_option.contains(ExpireOption::NX)
          || (expire_option.contains(ExpireOption::GT) && expiration <= current)
          || (expire_option.contains(ExpireOption::LT) && expiration >= current)
      }
      None => expire_option.contains(ExpireOption::XX) || expire_option.contains(ExpireOption::GT),
    };
    if denied {
      return HashExpireResult::ExpireConditionNotMet;
    }

    // 构造一次 key 缓冲：既有槽位原地更新（零分配），堆项消费唯一所有权
    let key_vec = key.to_vec();
    if current_expiration.is_some() {
      if let Some(slot) = self.expiration_times.as_mut().unwrap().get_mut(key) {
        *slot = expiration;
      }
      // 字典项槽位已计，仅补堆项
      self.heap_memory_size += SLOT * 2;
    } else {
      self
        .expiration_times
        .as_mut()
        .unwrap()
        .insert(key_vec.clone(), expiration);
      self.update_expiration_size(true, true);
    }
    self
      .expiration_queue
      .as_mut()
      .unwrap()
      .push(Reverse(ExpirationQueueEntry {
        expiration,
        key: key_vec,
      }));

    HashExpireResult::ExpireUpdated
  }

  /// 移除成员过期（HPERSIST 语义：-2 键不存在，1 成功，-1 无过期）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:Persist
  pub fn persist(&mut self, key: &[u8]) -> i32 {
    if !self.contains_key(key) {
      return HashExpireResult::KeyNotFound as i32;
    }

    if self.has_expirable_items()
      && let Some(_) = self.expiration_times.as_mut().unwrap().remove(key)
    {
      self.heap_memory_size -= SLOT * 2;
      self.cleanup_expiration_structures_if_empty();
      return HashExpireResult::ExpireUpdated as i32;
    }

    -1
  }

  /// 查询成员过期 ticks（-2 键不存在，-1 无过期）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:GetExpiration
  pub fn get_expiration(&self, key: &[u8]) -> i64 {
    if !self.contains_key(key) {
      return i64::from(HashExpireResult::KeyNotFound as i32);
    }
    if let Some(&expiration) = self.expiration_times.as_ref().and_then(|t| t.get(key)) {
      return expiration;
    }
    -1
  }

  /// 按散列迭代序取第 index 个字段值对（跳过已过期；越界返回 None）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:ElementAt
  /// （C# 越界抛 ArgumentOutOfRangeException，Rust 以 None 表达）
  pub fn element_at(&self, index: usize) -> Option<(Vec<u8>, Vec<u8>)> {
    if self.has_expirable_items() {
      return self
        .hash
        .iter()
        .filter(|(k, _)| !self.is_expired(k))
        .nth(index)
        .map(|(k, v)| (k.clone(), v.clone()));
    }

    self
      .hash
      .iter()
      .nth(index)
      .map(|(k, v)| (k.clone(), v.clone()))
  }
}

/// 从 n 个元素中随机取 k 个下标（HRANDFIELD/SRANDMEMBER/ZRANDMEMBER 共用）
///
/// libs/common/RandomUtils.cs:PickKRandomIndexes
///
/// 刻意差异（对照 C#）：.NET `Random(seed)` 的洗牌/迭代抽取序列与 fastrand 不同，
/// 仅保语义等价。分支结构 1:1 对齐：
/// - `distinct=false` 或 `k/n < K_OVER_N_THRESHOLD` 走迭代抽取（distinct 用
///   拒绝采样，O(k) 空间，C# PickKRandomIndexesIteratively）；
/// - 否则全量洗牌取前 k（C# PickKRandomDistinctIndexesWithShuffle）。
///
/// 空集直接返回空（C# `Random.Next(0)` 抛 ArgumentOutOfRangeException，
/// 按无结果处理）
pub(crate) fn pick_k_random_indexes(n: usize, k: usize, seed: i32, distinct: bool) -> Vec<usize> {
  /// k/n 低于该阈值走迭代抽取（C# RandomUtils.KOverNThreshold）
  const K_OVER_N_THRESHOLD: f64 = 0.1;

  let mut rng = Rng::with_seed(u64::from(seed as u32));
  if n == 0 || k == 0 {
    return Vec::new();
  }

  if !distinct || (k as f64) / (n as f64) < K_OVER_N_THRESHOLD {
    let mut indexes = Vec::with_capacity(k);
    if !distinct {
      indexes.extend((0..k).map(|_| rng.usize(..n)));
    } else {
      // 拒绝采样：k <= n 保证可终止
      let mut picked = HashSet::with_capacity_and_hasher(k, GxBuildHasher::default());
      while indexes.len() < k {
        let idx = rng.usize(..n);
        if picked.insert(idx) {
          indexes.push(idx);
        }
      }
    }
    indexes
  } else {
    // 部分洗牌取前 k（k == n 时即全量洗牌）
    let mut perm: Vec<usize> = (0..n).collect();
    for i in 0..k.min(n) {
      let j = rng.usize(i..perm.len());
      perm.swap(i, j);
    }
    perm.truncate(k);
    perm
  }
}

/// 单下标随机取（HRANDFIELD/SRANDMEMBER 无 count 形态）
///
/// libs/common/RandomUtils.cs:PickRandomIndex（.NET rand 为非负随机数，% 取模）
#[inline]
pub(crate) fn pick_random_index(n: usize, rand: i32) -> usize {
  (rand as u32 as usize) % n
}

/// Scan 输入解析 + 输出回写：HSCAN/SSCAN 共用（对应 C# GarnetObjectBase 的
/// 基类角色，抽象 Scan 以闭包注入；sortedset 因分值可空项走独立实现）
///
/// libs/server/Objects/Types/GarnetObjectBase.cs:Scan
pub(crate) fn scan_operate_shared(
  args: &[&[u8]],
  limit_count_in_output: i32,
  output: &mut ObjectOutput<'_>,
  do_scan: impl FnOnce(i64, i64, &[u8], bool) -> (Vec<Vec<u8>>, i64),
) {
  // 参数解析走 GarnetObjectBase::ReadScanInput 单点（错误直接写 RESP 错误）
  let params = match read_scan_input(args, limit_count_in_output) {
    Ok(params) => params,
    Err(msg) => {
      RespWriter::new_ref(&mut output.payload).write_error_bytes(msg);
      return;
    }
  };

  let (items, cursor_output) = do_scan(
    params.cursor,
    params.count,
    params.pattern,
    params.is_no_value,
  );
  let items_len = items.len();

  RespWriter::new_ref(&mut output.payload).write_array_length(2);
  RespWriter::new_ref(&mut output.payload).write_int64_as_bulk_string(cursor_output);

  if items.is_empty() {
    RespWriter::new_ref(&mut output.payload).write_empty_array();
  } else {
    RespWriter::new_ref(&mut output.payload).write_array_length(items.len());
    for item in items {
      RespWriter::new_ref(&mut output.payload).write_bulk_string(&item);
    }
  }

  output.result1 = items_len as i64;
}

impl GarnetObjectPayload for HashObject {
  const OBJECT_TAG: GarnetObjectType = GarnetObjectType::Hash;

  #[inline]
  fn from_blob(raw: &[u8]) -> Option<Self> {
    // 单格式：载荷恒带 4B 计数头（to_blob 恒落头），无头二次回退臂已删；
    // 畸形载荷显式失败（对标 C# GarnetObjectSerializer.DeserializeInternal fail-fast）
    Self::deserialize_from_slice(raw.get(4..)?).ok()
  }

  #[inline]
  fn to_blob(&self) -> Vec<u8> {
    let (count, wire) = self.serialize_wire();
    let mut out = Vec::with_capacity(4 + wire.len());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&wire);
    out
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.hash.is_empty()
  }
}
