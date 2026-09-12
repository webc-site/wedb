//! 哈希对象（对标 libs/server/Objects/Hash/HashObject.cs）
//!
//! 结构与 C# 一致：`hash`（field → value 散列）+ 成员级过期结构
//! `expiration_times`（字典，惰性初始化）+ `expiration_queue`（最小堆，
//! 对应 C# PriorityQueue<byte[], long>），另有堆内存记账 `heap_memory_size`。
//!
//! 刻意差异（对照 C#）：`HeapMemorySize` 的 C# GC 对象开销记账
//! （MemoryUtils.*Overhead）不适用于 Rust，这里仅按条目字节数做相对记账。

use std::{
  cmp::{Ordering, Reverse},
  collections::BinaryHeap,
  io::{self, Read, Write},
};

use gxhash::{GxBuildHasher, HashMap};

use crate::{
  inputs::ObjectInput,
  objects::types::object_output::{ObjectOutput, ObjectOutputFlags},
  types::GarnetObjectType,
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

/// 成员级过期选项（域内复用 sortedset 定义，与 C# 同为单一 ExpireOption）
///
/// libs/server/ExpireOption.cs:ExpireOption
pub use crate::objects::sortedset::sorted_set_object::ExpireOption;

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

/// 过期队列条目：按 (expiration, key) 升序的最小堆元素
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExpirationQueueEntry {
  expiration: i64,
  key: Vec<u8>,
}

impl PartialOrd for ExpirationQueueEntry {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for ExpirationQueueEntry {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    self
      .expiration
      .cmp(&other.expiration)
      .then_with(|| self.key.cmp(&other.key))
  }
}

/// 反转堆序 → BinaryHeap 即最小堆
pub type ExpirationQueue = BinaryHeap<Reverse<ExpirationQueueEntry>>;

#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
pub struct HashWire {
  pub entries: Vec<(Vec<u8>, Vec<u8>)>,
  pub expirations: Option<Vec<(Vec<u8>, i64)>>,
}

/// 哈希对象
///
/// libs/server/Objects/Hash/HashObject.cs:HashObject
#[derive(Debug, Clone, Default)]
pub struct HashObject {
  /// field → value 散列
  pub hash: HashMap<Vec<u8>, Vec<u8>>,
  /// field → 过期 ticks（惰性初始化）
  pub expiration_times: Option<HashMap<Vec<u8>, i64>>,
  /// 过期最小堆（与 expiration_times 同生命周期，惰性初始化）
  pub expiration_queue: Option<ExpirationQueue>,
  /// 堆内存记账（相对值；C# 语义见文件头刻意差异）
  pub heap_memory_size: i64,
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

  /// 从二进制流反序列化哈希对象
  pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;
    let wire: HashWire =
      bitcode::decode(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

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
          obj.initialize_expiration_structures();
          if let Some(times) = obj.expiration_times.as_mut() {
            times.insert(item.clone(), expiration);
          }
          if let Some(queue) = obj.expiration_queue.as_mut() {
            queue.push(Reverse(ExpirationQueueEntry {
              expiration,
              key: item,
            }));
          }
          obj.update_expiration_size(true, true);
        }
      }
      obj.cleanup_expiration_structures_if_empty();
    }

    Ok(obj)
  }

  /// 序列化为二进制流（过滤已过期条目；带过期成员附加 ticks）
  pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
    let now = now_ticks();
    let has_expirations = self.expiration_times.is_some();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = if has_expirations {
      self
        .hash
        .iter()
        .filter(|(k, _)| !self.is_expired_at(k, now))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
    } else {
      self
        .hash
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
    };

    let expirations = self.expiration_times.as_ref().and_then(|times| {
      let active: Vec<(Vec<u8>, i64)> = times
        .iter()
        .filter(|(k, exp)| **exp >= now && self.hash.contains_key(*k))
        .map(|(k, exp)| (k.clone(), *exp))
        .collect();
      if active.is_empty() {
        None
      } else {
        Some(active)
      }
    });

    let wire = HashWire {
      entries,
      expirations,
    };
    let bytes = bitcode::encode(&wire);
    writer.write_all(&bytes)
  }

  /// 获取全部字段名
  pub fn get_keys(&self) -> Vec<Vec<u8>> {
    self.hash.keys().cloned().collect()
  }

  /// 获取全部字段值
  pub fn get_values(&self) -> Vec<Vec<u8>> {
    self.hash.values().cloned().collect()
  }

  /// 与 wkv 对象存储层的载荷格式互转：
  /// RESP 命令层经此装载/回写，保持与 storage 会话域的 blob 统一
  ///
  /// 刻意差异：该路径不携带成员级过期（bitcode 载荷无过期槽位）
  ///
  /// 无 C# 对应（wkv blob 装载入口，见文件头刻意差异说明）
  pub fn from_pairs(pairs: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
    let mut obj = Self::new();
    obj.hash.reserve(pairs.len());
    for (key, value) in pairs {
      if !obj.hash.contains_key(&key) {
        obj.update_size(&key, &value, true);
        obj.hash.insert(key, value);
      }
    }
    obj
  }

  /// 导出为 (field, value) 数组（`from_pairs` 的逆操作）
  ///
  /// 无 C# 对应（wkv blob 回写出口）
  pub fn to_pairs(&self) -> Vec<(Vec<u8>, Vec<u8>)> {
    self
      .hash
      .iter()
      .map(|(k, v)| (k.clone(), v.clone()))
      .collect()
  }

  /// 对象操作统一入口（RESP 分派，具体实现在 partial 分片中）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:Operate
  ///
  /// 刻意差异：C# 对 switch default 抛 GarnetException，
  /// Rust 以错误回复表达（会话层异常最终也落为错误回复）
  pub fn operate(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) -> bool {
    // 类型不符直接回 WrongType（对标 C# 先查 header.type）
    if input.header.data[0] != GarnetObjectType::Hash as u8 {
      output.output_flags |= ObjectOutputFlags::WRONG_TYPE;
      output.payload.clear();
      return true;
    }

    let Some(op) = hash_op_from_header(input) else {
      // C#: switch default 抛 GarnetException("Unsupported operation ...")
      output.write_error(RESP_ERR_UNSUPPORTED_OPERATION.as_bytes());
      return true;
    };

    match op {
      HashOperation::Hset | HashOperation::Hmset | HashOperation::Hsetnx => {
        self.hash_set(input, output);
      }
      HashOperation::Hget => self.hash_get(input, output, resp_protocol_version),
      HashOperation::Hmget => self.hash_multiple_get(input, output, resp_protocol_version),
      HashOperation::Hgetall => self.hash_get_all(output, resp_protocol_version),
      HashOperation::Hdel => self.hash_delete(input, output),
      HashOperation::Hlen => self.hash_length(output),
      HashOperation::Hstrlen => self.hash_str_length(input, output),
      HashOperation::Hexists => self.hash_exists(input, output),
      HashOperation::Hexpire => self.hash_expire(input, output),
      HashOperation::Httl => self.hash_time_to_live(input, output),
      HashOperation::Hpersist => self.hash_persist(input, output),
      HashOperation::Hkeys | HashOperation::Hvals => self.hash_get_keys_or_values(input, output),
      HashOperation::Hincrby => self.hash_increment(input, output),
      HashOperation::Hincrbyfloat => self.hash_increment_float(input, output),
      HashOperation::Hrandfield => self.hash_random_field(input, output, resp_protocol_version),
      HashOperation::Hcollect => self.hash_collect(output),
      HashOperation::Hscan => self.scan_operate(input, output),
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
    // RoundUp(key, 8) + RoundUp(value, 8) + 2 * ByteArrayOverhead(16) + 字典项开销
    let memory_size =
      (key.len().div_ceil(8) * 8 + value.len().div_ceil(8) * 8 + 16 + 16 + 16) as i64;

    if add {
      self.heap_memory_size += memory_size;
    } else {
      self.heap_memory_size -= memory_size;
    }
  }

  /// 惰性创建过期字典 + 最小堆
  ///
  /// libs/server/Objects/Hash/HashObject.cs:InitializeExpirationStructures
  pub fn initialize_expiration_structures(&mut self) {
    if self.expiration_times.is_none() {
      self.expiration_times = Some(HashMap::with_hasher(GxBuildHasher::default()));
      self.expiration_queue = Some(BinaryHeap::new());
      self.heap_memory_size += 16; // C#: DictionaryOverhead + PriorityQueueOverhead
    }
  }

  /// 过期条目内存记账（add=false 回收）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:UpdateExpirationSize
  #[inline]
  pub fn update_expiration_size(&mut self, add: bool, include_pq: bool) {
    // 字典项 + 堆项各计 16 字节（指针+long）
    let mut memory_size = 16 + 16;
    if include_pq {
      memory_size += 16 + 16;
    }

    if add {
      self.heap_memory_size += memory_size;
    } else {
      self.heap_memory_size -= memory_size;
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
      self.heap_memory_size -= (16 + 16) * queue.len() as i64;
    }
    self.heap_memory_size -= 16; // C#: DictionaryOverhead + PriorityQueueOverhead
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
        }
      } else {
        // The key was not in expirationTimes. It may have been Remove()d.
        queue.pop();

        // Adjust memory size for the priority queue entry removal.
        self.heap_memory_size -= 16 + 16;
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

  /// 字段数（剔除已过期）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:Count
  pub fn count(&self) -> usize {
    let Some(times) = self.expiration_times.as_ref() else {
      return self.hash.len();
    };

    let expired_keys_count = times.keys().filter(|k| self.is_expired(k)).count();
    self.hash.len() - expired_keys_count
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
    self.hash.insert(key.to_vec(), value.clone());
    self.update_size(key, &value, true);
  }

  /// 设置成员过期（NX/XX/GT/LT 语义），返回 [`HashExpireResult`] 码
  ///
  /// libs/server/Objects/Hash/HashObject.cs:SetExpiration
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

    if let Some(current_expiration) = current_expiration {
      if expire_option.contains(ExpireOption::NX)
        || (expire_option.contains(ExpireOption::GT) && expiration <= current_expiration)
        || (expire_option.contains(ExpireOption::LT) && expiration >= current_expiration)
      {
        return HashExpireResult::ExpireConditionNotMet;
      }

      self
        .expiration_times
        .as_mut()
        .unwrap()
        .insert(key.to_vec(), expiration);
      self
        .expiration_queue
        .as_mut()
        .unwrap()
        .push(Reverse(ExpirationQueueEntry {
          expiration,
          key: key.to_vec(),
        }));
      // 字典项槽位已计，仅补堆项
      self.heap_memory_size += 16 + 16;
    } else {
      // C# 分支：无既有过期时，XX 或 GT 均视为条件不满足
      if expire_option.contains(ExpireOption::XX) || expire_option.contains(ExpireOption::GT) {
        return HashExpireResult::ExpireConditionNotMet;
      }

      self
        .expiration_times
        .as_mut()
        .unwrap()
        .insert(key.to_vec(), expiration);
      self
        .expiration_queue
        .as_mut()
        .unwrap()
        .push(Reverse(ExpirationQueueEntry {
          expiration,
          key: key.to_vec(),
        }));
      self.update_expiration_size(true, true);
    }

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
      self.heap_memory_size -= 16 + 16;
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

/// 从 ObjectInput 头部提取哈希操作码（C# header.HashOp = subId）
#[inline]
pub fn hash_op_from_header(input: &ObjectInput) -> Option<HashOperation> {
  HashOperation::try_from(input.header.sub_id()).ok()
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

  let mut rng = fastrand::Rng::with_seed(u64::from(seed as u32));
  if n == 0 || k == 0 {
    return Vec::new();
  }

  if !distinct || (k as f64) / (n as f64) < K_OVER_N_THRESHOLD {
    let mut indexes = Vec::with_capacity(k);
    if !distinct {
      indexes.extend((0..k).map(|_| rng.usize(..n)));
    } else {
      // 拒绝采样：k <= n 保证可终止
      let mut picked = gxhash::HashSet::with_capacity_and_hasher(k, GxBuildHasher::default());
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

pub(crate) use wbase::{glob::glob_match, time::now_ticks};

/// Scan 输入解析 + 输出回写：HSCAN/SSCAN 共用（对应 C# GarnetObjectBase 的
/// 基类角色，抽象 Scan 以闭包注入；sortedset 因分值可空项走独立实现）
///
/// libs/server/Objects/Types/GarnetObjectBase.cs:Scan(ref ObjectInput, ...)
pub(crate) fn scan_operate_shared(
  input: &ObjectInput,
  output: &mut ObjectOutput,
  do_scan: impl FnOnce(i64, i64, &[u8], bool) -> (Vec<Vec<u8>>, i64),
) {
  // 单轮最多返回的条目数由调用方经 arg2 下发
  let limit_count_in_output = input.arg2 as i64;

  // 默认 COUNT
  let mut pattern: &[u8] = &[];
  let mut count = 10_i64;
  let mut is_no_value = false;

  let cursor = if input.parse_state.count > 0 {
    match try_get_long(arg(input, 0)) {
      Some(c) if c >= 0 => c,
      _ => {
        output.write_error(RESP_ERR_GENERIC_INVALIDCURSOR.as_bytes());
        return;
      }
    }
  } else {
    output.write_error(RESP_ERR_GENERIC_INVALIDCURSOR.as_bytes());
    return;
  };

  let mut curr_token_idx = 1;
  while curr_token_idx < input.parse_state.count {
    let param = arg(input, curr_token_idx);
    curr_token_idx += 1;

    if equals_ignore_case(param, b"MATCH") {
      if curr_token_idx >= input.parse_state.count {
        output.write_error(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
        return;
      }
      pattern = arg(input, curr_token_idx);
      curr_token_idx += 1;
    } else if equals_ignore_case(param, b"COUNT") {
      if curr_token_idx >= input.parse_state.count {
        output.write_error(RESP_ERR_GENERIC_SYNTAX_ERROR.as_bytes());
        return;
      }
      match try_get_int(arg(input, curr_token_idx)) {
        Some(c) => {
          curr_token_idx += 1;
          count = c as i64;
          // 无条件钳制到输出上限（对标 C# countInInput > limitCountInOutput）
          if count > limit_count_in_output {
            count = limit_count_in_output;
          }
        }
        None => {
          output.write_error(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER.as_bytes());
          return;
        }
      }
    } else if equals_ignore_case(param, b"NOVALUES") {
      is_no_value = true;
    }
  }

  let (items, cursor_output) = do_scan(cursor, count, pattern, is_no_value);
  let items_len = items.len();

  output.write_array_length(2);
  output.write_int64_as_bulk_string(cursor_output);

  if items.is_empty() {
    output.write_empty_array();
  } else {
    output.write_array_length(items.len());
    for item in items {
      output.write_bulk_string(&item);
    }
  }

  output.result1 = items_len as i64;
}

/// 取第 i 个参数字节
///
#[inline]
fn arg<'a>(input: &ObjectInput, i: usize) -> &'a [u8] {
  input.parse_state.get_arg_slice_by_ref(i).as_slice()
}

use wresp::cmd_strings::{
  RESP_ERR_GENERIC_INVALIDCURSOR, RESP_ERR_GENERIC_SYNTAX_ERROR,
  RESP_ERR_GENERIC_UNSUPPORTED_OPERATION as RESP_ERR_UNSUPPORTED_OPERATION,
  RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
};

use crate::objects::parse_utils::{equals_ignore_case, try_get_int, try_get_long};
