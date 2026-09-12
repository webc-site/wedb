//! 有序集合对象（对标 libs/server/Objects/SortedSet/SortedSetObject.cs）
//!
//! 双索引结构与 C# 一致：`sorted_set`（(score, member) 有序视图，BTreeSet 对应
//! C# SortedSet + SortedSetComparer）+ `sorted_set_dict`（member → score 散列），
//! 另有成员级过期结构 `expiration_times`（字典）+ `expiration_queue`
//! （最小堆，对应 C# PriorityQueue<byte[], long>）。
//!
//! 刻意差异（对照 C#）：`HeapMemorySize` 的 C# GC 对象开销记账
//! （MemoryUtils.*Overhead）不适用于 Rust，这里仅按条目字节数做相对记账。

use std::{
  cmp::{Ordering, Reverse},
  collections::{BTreeSet, BinaryHeap},
  io::{self, Read, Write},
};

use bitflags::bitflags;
use gxhash::{GxBuildHasher, HashMap};
use wbase::{glob::glob_match, time::now_ticks};
use wresp::cmd_strings::RESP_ERR_GENERIC_UNSUPPORTED_OPERATION as RESP_ERR_UNSUPPORTED_OPERATION;

use crate::{
  inputs::ObjectInput,
  objects::types::object_output::{ObjectOutput, ObjectOutputFlags},
  types::GarnetObjectType,
};

/// 有序集合操作（AOF 持久化值，C# 侧为显式追加语义，不得改序/复用既有值）
///
/// libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetOperation
#[derive(
  Debug, Clone, Copy, PartialEq, Eq, num_enum::TryFromPrimitive, num_enum::IntoPrimitive,
)]
#[repr(u8)]
pub enum SortedSetOperation {
  Zadd = 0,
  Zcard = 1,
  Zpopmax = 2,
  Zscore = 3,
  Zrem = 4,
  Zcount = 5,
  Zincrby = 6,
  Zrank = 7,
  Zrange = 8,
  Geoadd = 9,
  Geohash = 10,
  Geodist = 11,
  Geopos = 12,
  Geosearch = 13,
  Zrevrank = 14,
  Zremrangebylex = 15,
  Zremrangebyrank = 16,
  Zremrangebyscore = 17,
  Zlexcount = 18,
  Zpopmin = 19,
  Zrandmember = 20,
  Zdiff = 21,
  Zscan = 22,
  Zmscore = 23,
  Zexpire = 24,
  Zttl = 25,
  Zpersist = 26,
  Zcollect = 27,
}

bitflags! {
  /// ZRANGE 族范围选项
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetRangeOpts
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
  pub struct SortedSetRangeOpts: u8 {
    /// 无选项
    const NONE = 0;
    /// 按分值取范围
    const BY_SCORE = 1;
    /// 按字典序取范围
    const BY_LEX = 1 << 1;
    /// 逆序
    const REVERSE = 1 << 2;
    /// 存储结果（ZRANGESTORE）
    const STORE = 1 << 3;
    /// 结果带分值
    const WITH_SCORES = 1 << 4;
  }
}

bitflags! {
  /// ZADD 选项
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetAddOption
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
  pub struct SortedSetAddOption: u8 {
    /// 无选项
    const NONE = 0;
    /// 仅更新既有元素
    const XX = 1;
    /// 仅新增元素
    const NX = 1 << 1;
    /// 新分值小于当前分值才更新
    const LT = 1 << 2;
    /// 新分值大于当前分值才更新
    const GT = 1 << 3;
    /// 返回值改为"新增+变更"总数
    const CH = 1 << 4;
    /// ZADD 退化为 ZINCRBY，仅允许单对 score-element
    const INCR = 1 << 5;
  }
}

/// 排序维度
///
/// libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetOrderOperation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortedSetOrderOperation {
  /// 按索引排名
  ByRank,
  /// 按分值
  ByScore,
  /// 按字典序（要求同分）
  ByLex,
}

bitflags! {
  /// 成员级过期选项
  ///
  /// libs/server/ExpireOption.cs:ExpireOption
  #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
  pub struct ExpireOption: u8 {
    /// 无条件设置
    const NONE = 0;
    /// 仅当无既有过期时设置
    const NX = 1 << 0;
    /// 仅当有既有过期时设置
    const XX = 1 << 1;
    /// 仅当新过期晚于当前时设置
    const GT = 1 << 2;
    /// 仅当新过期早于当前时设置
    const LT = 1 << 3;
  }
}

/// 过期时间戳（.NET Ticks）+ 过期选项的压缩编码：低 4 位为选项，高位为 ticks
///
/// libs/server/ExpirationWithOption.cs:ExpirationWithOption
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExpirationWithOption {
  word: i64,
}

impl ExpirationWithOption {
  /// libs/server/ExpirationWithOption.cs:ExpirationWithOption(long, ExpireOption)
  #[inline]
  pub fn new(expiration_time_in_ticks: i64, expire_option: ExpireOption) -> Self {
    Self {
      word: ((expiration_time_in_ticks >> 4) << 4) | (expire_option.bits() as i64 & 0xF),
    }
  }

  /// 由既有 64 位整型字构筑（对照 C# ExpirationWithOption(long word) 单参构造）
  #[inline]
  pub fn from_word(word: i64) -> Self {
    Self { word }
  }

  /// 由 (word_head, word_tail) 两个 i32 拼装（C# RespServerSession 传参形态）
  #[inline]
  pub fn from_word_head_tail(word_head: i32, word_tail: i32) -> Self {
    Self {
      word: ((((word_head as u32) as u64) << 32) | (word_tail as u32 as u64)) as i64,
    }
  }

  /// libs/server/ExpirationWithOption.cs:ExpirationTimeInTicks
  #[inline]
  pub fn expiration_time_in_ticks(&self) -> i64 {
    (self.word >> 4) << 4
  }

  /// libs/server/ExpirationWithOption.cs:ExpireOption
  #[inline]
  pub fn expire_option(&self) -> ExpireOption {
    ExpireOption::from_bits_truncate((self.word & 0xF) as u8)
  }

  /// libs/server/ExpirationWithOption.cs:Word
  #[inline]
  pub fn word(&self) -> i64 {
    self.word
  }
}

/// 成员级过期操作结果码
///
/// libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetExpireResult
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i32)]
pub enum SortedSetExpireResult {
  /// 键不存在
  KeyNotFound = -2,
  /// 过期条件不满足（NX/XX/GT/LT 语义）
  ExpireConditionNotMet = 0,
  /// 过期已更新
  ExpireUpdated = 1,
  /// 键已过期（给定时间戳在过去，条目被移除）
  KeyAlreadyExpired = 2,
}

/// 有序集合条目：分值 + 成员，序由 [`crate::objects::sorted_set_comparer::SortedSetComparer`] 决定
#[derive(Debug, Clone, PartialEq)]
pub struct SortedSetEntry {
  pub score: f64,
  pub member: Vec<u8>,
}

impl Eq for SortedSetEntry {}

impl PartialOrd for SortedSetEntry {
  #[inline]
  fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
    Some(self.cmp(other))
  }
}

impl Ord for SortedSetEntry {
  #[inline]
  fn cmp(&self, other: &Self) -> Ordering {
    // 对标 SortedSetComparer.Compare：(score, member) 字典序（total_cmp 对齐
    // double.CompareTo 的全序要求，NaN 处理差异见比较器文件说明）
    self
      .score
      .total_cmp(&other.score)
      .then_with(|| self.member.cmp(&other.member))
  }
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
pub struct SortedSetWire {
  pub entries: Vec<(Vec<u8>, f64)>,
  pub expirations: Option<Vec<(Vec<u8>, i64)>>,
}

/// 有序集合对象
///
/// libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetObject
#[derive(Debug, Clone, Default)]
pub struct SortedSetObject {
  /// (score, member) 有序视图
  pub sorted_set: BTreeSet<SortedSetEntry>,
  /// member → score 散列
  pub sorted_set_dict: HashMap<Vec<u8>, f64>,
  /// member → 过期 ticks（惰性初始化）
  pub expiration_times: Option<HashMap<Vec<u8>, i64>>,
  /// 过期最小堆（与 expiration_times 同生命周期，惰性初始化）
  pub expiration_queue: Option<ExpirationQueue>,
  /// 堆内存记账（相对值；C# 语义见文件头刻意差异）
  pub heap_memory_size: i64,
}

impl SortedSetObject {
  /// 构造空集合
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetObject()
  pub fn new() -> Self {
    Self::default()
  }

  /// 成员数量
  #[inline]
  pub fn len(&self) -> usize {
    self.sorted_set_dict.len()
  }

  /// 是否为空有序集合
  #[inline]
  pub fn is_empty(&self) -> bool {
    self.sorted_set_dict.is_empty()
  }

  /// 从二进制流反序列化有序集合对象
  pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;
    let wire: SortedSetWire =
      bitcode::decode(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let mut obj = Self::new();
    obj.sorted_set_dict.reserve(wire.entries.len());
    let now = now_ticks();

    for (member, score) in wire.entries {
      obj.update_size(&member, true);
      obj.sorted_set.insert(SortedSetEntry {
        score,
        member: member.clone(),
      });
      obj.sorted_set_dict.insert(member, score);
    }

    if let Some(expirations) = wire.expirations {
      for (member, expiration) in expirations {
        if expiration < now {
          if let Some(score) = obj.sorted_set_dict.remove(&member) {
            obj.sorted_set.remove(&SortedSetEntry {
              score,
              member: member.clone(),
            });
            obj.update_size(&member, false);
          }
        } else if obj.sorted_set_dict.contains_key(&member) {
          obj.initialize_expiration_structures();
          if let Some(times) = obj.expiration_times.as_mut() {
            times.insert(member.clone(), expiration);
          }
          if let Some(queue) = obj.expiration_queue.as_mut() {
            queue.push(Reverse(ExpirationQueueEntry {
              expiration,
              key: member,
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
    let entries: Vec<(Vec<u8>, f64)> = if has_expirations {
      self
        .sorted_set_dict
        .iter()
        .filter(|(m, _)| !self.is_expired_at(m, now))
        .map(|(m, s)| (m.clone(), *s))
        .collect()
    } else {
      self
        .sorted_set_dict
        .iter()
        .map(|(m, s)| (m.clone(), *s))
        .collect()
    };

    let expirations = self.expiration_times.as_ref().and_then(|times| {
      let active: Vec<(Vec<u8>, i64)> = times
        .iter()
        .filter(|(m, exp)| **exp >= now && self.sorted_set_dict.contains_key(*m))
        .map(|(m, exp)| (m.clone(), *exp))
        .collect();
      if active.is_empty() {
        None
      } else {
        Some(active)
      }
    });

    let wire = SortedSetWire {
      entries,
      expirations,
    };
    let bytes = bitcode::encode(&wire);
    writer.write_all(&bytes)
  }

  /// 与 wkv 对象存储层的载荷格式互转：
  /// RESP 命令层经此装载/回写，保持与 storage 会话域的 blob 统一兼容
  ///
  /// 刻意差异：该路径不携带成员级过期（bitcode 载荷无过期槽位）
  ///
  /// 无 C# 对应（wkv blob 装载入口，见文件头刻意差异说明）
  pub fn from_entries(entries: Vec<(Vec<u8>, f64)>) -> Self {
    let mut obj = Self::new();
    obj.sorted_set_dict.reserve(entries.len());
    for (member, score) in entries {
      if !obj.sorted_set_dict.contains_key(&member) {
        obj.update_size(&member, true);
        obj.sorted_set.insert(SortedSetEntry {
          score,
          member: member.clone(),
        });
        obj.sorted_set_dict.insert(member, score);
      }
    }
    obj
  }

  /// 添加或更新成员分值
  pub fn add(&mut self, member: &[u8], score: f64) -> bool {
    if let Some(old_score) = self.sorted_set_dict.get_mut(member) {
      if *old_score != score {
        let old = *old_score;
        *old_score = score;
        let m = member.to_vec();
        self.sorted_set.remove(&SortedSetEntry {
          score: old,
          member: m.clone(),
        });
        self.sorted_set.insert(SortedSetEntry { score, member: m });
      }
      false
    } else {
      let m = member.to_vec();
      self.sorted_set_dict.insert(m.clone(), score);
      self.sorted_set.insert(SortedSetEntry { score, member: m });
      self.update_size(member, true);
      true
    }
  }

  /// 移除成员
  pub fn rem(&mut self, member: &[u8]) -> Option<f64> {
    if let Some((m, old_score)) = self.sorted_set_dict.remove_entry(member) {
      self.sorted_set.remove(&SortedSetEntry {
        score: old_score,
        member: m,
      });
      self.update_size(member, false);
      self.try_remove_expiration(member);
      Some(old_score)
    } else {
      None
    }
  }

  /// 增量调整成员分值（成员缺失以 0 为基底），返回新分值
  pub fn incr_by(&mut self, member: &[u8], delta: f64) -> f64 {
    let current = self.try_get_score(member).unwrap_or(0.0);
    let new_score = current + delta;
    self.add(member, new_score);
    new_score
  }

  /// 弹出最低分成员 (member, score)
  pub fn pop_min(&mut self) -> Option<(Vec<u8>, f64)> {
    self.pop_min_or_max(false).map(|(s, m)| (m, s))
  }

  /// 弹出最高分成员 (member, score)
  pub fn pop_max(&mut self) -> Option<(Vec<u8>, f64)> {
    self.pop_min_or_max(true).map(|(s, m)| (m, s))
  }

  /// 基础操作派发
  pub fn operate_basic(
    &mut self,
    op: SortedSetOperation,
    member: &[u8],
    score: f64,
  ) -> Option<Vec<u8>> {
    match op {
      SortedSetOperation::Zadd => {
        self.add(member, score);
        None
      }
      SortedSetOperation::Zrem => {
        let old = self.rem(member)?;
        Some(old.to_string().into_bytes())
      }
      SortedSetOperation::Zscore => {
        let s = self.try_get_score(member)?;
        Some(s.to_string().into_bytes())
      }
      _ => None,
    }
  }

  /// 导出为 (member, score) 数组（`from_entries` 的逆操作）
  ///
  /// 无 C# 对应（wkv blob 回写出口）
  pub fn to_entries(&self) -> Vec<(Vec<u8>, f64)> {
    self
      .sorted_set_dict
      .iter()
      .map(|(m, s)| (m.clone(), *s))
      .collect()
  }

  /// 判断两集合是否相等（两侧同键同分；成员级过期参与比较）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:Equals
  pub fn equals(&self, other: &SortedSetObject) -> bool {
    if self.sorted_set_dict.len() != other.sorted_set_dict.len() {
      return false;
    }

    for (key, value) in self.sorted_set_dict.iter() {
      // 1:1 保留 C# 原文的重复谓词形态（两处 IsExpired 调用等价于一次）
      if self.is_expired(key) && self.is_expired(key) {
        continue;
      }

      if self.is_expired(key) || self.is_expired(key) {
        return false;
      }

      match other.sorted_set_dict.get(key) {
        Some(other_value) if other_value == value => {}
        _ => return false,
      }
    }

    true
  }

  /// 对象操作统一入口（RESP 分派，具体实现在 partial 分片中）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:Operate
  ///
  /// 刻意差异：C# 对 GEOSEARCH/ZDIFF 走 switch default 抛 GarnetException，
  /// Rust 以错误回复表达（会话层异常最终也落为错误回复）
  pub fn operate(
    &mut self,
    input: &ObjectInput,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) -> bool {
    // 类型不符直接回 WrongType（对标 C# 先查 header.type）
    if input.header.data[0] != GarnetObjectType::SortedSet as u8 {
      output.output_flags |= ObjectOutputFlags::WRONG_TYPE;
      output.payload.clear();
      return true;
    }

    let Some(op) = sorted_set_op_from_header(input) else {
      // C#: switch default 抛 GarnetException("Unsupported operation ...")
      output.write_error(RESP_ERR_UNSUPPORTED_OPERATION.as_bytes());
      return true;
    };

    match op {
      SortedSetOperation::Zadd => self.sorted_set_add(input, output, resp_protocol_version),
      SortedSetOperation::Zrem => self.sorted_set_remove(input, output),
      SortedSetOperation::Zcard => self.sorted_set_length(output),
      SortedSetOperation::Zpopmax => {
        self.sorted_set_pop_min_or_max_count(input, output, resp_protocol_version, op)
      }
      SortedSetOperation::Zscore => self.sorted_set_score(input, output, resp_protocol_version),
      SortedSetOperation::Zmscore => self.sorted_set_scores(input, output, resp_protocol_version),
      SortedSetOperation::Zcount => self.sorted_set_count(input, output),
      SortedSetOperation::Zincrby => {
        self.sorted_set_increment(input, output, resp_protocol_version)
      }
      SortedSetOperation::Zrank => self.sorted_set_rank(input, output, resp_protocol_version, true),
      SortedSetOperation::Zexpire => self.sorted_set_expire(input, output),
      SortedSetOperation::Zttl => self.sorted_set_time_to_live(input, output),
      SortedSetOperation::Zpersist => self.sorted_set_persist(input, output),
      SortedSetOperation::Zcollect => self.sorted_set_collect(output),
      SortedSetOperation::Geoadd => self.geo_add(input, output),
      SortedSetOperation::Geohash => self.geo_hash(input, output, resp_protocol_version),
      SortedSetOperation::Geodist => self.geo_distance(input, output, resp_protocol_version),
      SortedSetOperation::Geopos => self.geo_position(input, output, resp_protocol_version),
      SortedSetOperation::Zrange => self.sorted_set_range(input, output, resp_protocol_version),
      SortedSetOperation::Zrevrank => {
        self.sorted_set_rank(input, output, resp_protocol_version, false)
      }
      SortedSetOperation::Zremrangebylex => {
        self.sorted_set_remove_or_count_range_by_lex(input, output, op)
      }
      SortedSetOperation::Zremrangebyrank => self.sorted_set_remove_range_by_rank(input, output),
      SortedSetOperation::Zremrangebyscore => self.sorted_set_remove_range_by_score(input, output),
      SortedSetOperation::Zlexcount => {
        self.sorted_set_remove_or_count_range_by_lex(input, output, op)
      }
      SortedSetOperation::Zpopmin => {
        self.sorted_set_pop_min_or_max_count(input, output, resp_protocol_version, op)
      }
      SortedSetOperation::Zrandmember => {
        self.sorted_set_random_member(input, output, resp_protocol_version)
      }
      SortedSetOperation::Zscan => self.scan_operate(input, output, resp_protocol_version),
      // GEOSEARCH 由命令层经 geo_search(opts) 直入（携带 GeoSearchOptions 束）；
      // ZDIFF/ZUNION/ZINTER 属存储 API 域聚合（C# 同样不经 ObjectInput 分派）
      SortedSetOperation::Geosearch | SortedSetOperation::Zdiff => {
        output.write_error(RESP_ERR_UNSUPPORTED_OPERATION.as_bytes());
      }
    }

    if self.sorted_set_dict.is_empty() {
      output.output_flags |= ObjectOutputFlags::REMOVE_KEY;
    }

    true
  }

  /// ZSCAN：遍历成员（光标 + MATCH/COUNT/NOVALUES）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:Scan
  ///
  /// 刻意差异（对照 C#）：上游 SortedSetObject.Scan 完全忽略 `isNoValue`
  /// 形参——成员后恒附带分值文本，且恰集满 `count * 2` 项即停（含 count=0
  /// 时永不触发的上游怪癖，`==` 不放宽为 `>=`），此处 1:1 保留；
  /// 分值 ±inf/NaN 时 .NET Utf8Formatter.TryFormat 失败回写 null，
  /// 以 `None` 项表达。
  pub fn scan(
    &self,
    start: i64,
    count: i64,
    pattern: &[u8],
    _is_no_value: bool,
  ) -> (Vec<Option<Vec<u8>>>, i64) {
    let mut items: Vec<Option<Vec<u8>>> = Vec::new();
    let mut cursor = start;

    if (self.sorted_set_dict.len() as i64) < start {
      return (items, 0);
    }

    let mut index = 0_i64;
    let mut expired_keys_count = 0_i64;

    for (member, score) in self.sorted_set_dict.iter() {
      if self.is_expired(member) {
        expired_keys_count += 1;
        continue;
      }

      if index < start {
        index += 1;
        continue;
      }

      if pattern.is_empty() || glob_match(pattern, member) {
        items.push(Some(member.clone()));
        // 分值文本化失败（±inf/NaN）以 None 表达（C# Utf8Formatter 失败写 null）
        items.push(if score.is_finite() {
          Some(ObjectOutput::format_double(*score).into_bytes())
        } else {
          None
        });
      }

      cursor += 1;

      // 每个成员在结果中占 2 项（成员 + 分值）；C# 用相等判断
      // （负 COUNT 恒不命中 → 全量遍历；count=0 首个未命中条目即停的
      // 上游怪癖一并 1:1 保留）
      if items.len() as i64 == count * 2 {
        break;
      }
    }

    // 到达集合末尾则光标归零
    if cursor + expired_keys_count == self.sorted_set_dict.len() as i64 {
      cursor = 0;
    }

    (items, cursor)
  }

  // ---- Common Methods ----

  /// 两集合字典求差（新结果容器）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:CopyDiff
  pub fn copy_diff(
    sorted_set_object1: Option<&SortedSetObject>,
    sorted_set_object2: Option<&SortedSetObject>,
  ) -> HashMap<Vec<u8>, f64> {
    let mut result = HashMap::with_hasher(GxBuildHasher::default());
    let Some(obj1) = sorted_set_object1 else {
      return result;
    };

    for (key, value) in obj1.sorted_set_dict.iter() {
      let expired1 = obj1.is_expired(key);
      match sorted_set_object2 {
        None => {
          if !expired1 {
            result.insert(key.clone(), *value);
          }
        }
        Some(obj2) => {
          if !expired1 && !obj2.is_expired(key) && !obj2.sorted_set_dict.contains_key(key) {
            result.insert(key.clone(), *value);
          }
        }
      }
    }
    result
  }

  /// 就地从 dict1 移除存在于 obj2 的键
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:InPlaceDiff
  pub fn in_place_diff(
    dict1: &mut HashMap<Vec<u8>, f64>,
    sorted_set_object2: Option<&SortedSetObject>,
  ) {
    let Some(obj2) = sorted_set_object2 else {
      return;
    };
    let doomed: Vec<Vec<u8>> = dict1
      .iter()
      .filter(|(k, _)| !obj2.is_expired(k) && obj2.sorted_set_dict.contains_key(*k))
      .map(|(k, _)| k.clone())
      .collect();
    for k in doomed {
      dict1.remove(&k);
    }
  }

  /// 取成员分值（过期成员视为不存在）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:TryGetScore
  #[inline]
  pub fn try_get_score(&self, key: &[u8]) -> Option<f64> {
    if self.is_expired(key) {
      return None;
    }
    self.sorted_set_dict.get(key).copied()
  }

  /// 成员数（剔除已过期）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:Count
  pub fn count(&self) -> usize {
    let Some(times) = self.expiration_times.as_ref() else {
      return self.sorted_set_dict.len();
    };

    let expired_keys_count = times.keys().filter(|k| self.is_expired(k)).count();
    self.sorted_set_dict.len() - expired_keys_count
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
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:IsExpired
  #[inline]
  pub fn is_expired(&self, key: &[u8]) -> bool {
    self.is_expired_at(key, now_ticks())
  }

  /// 是否存在带过期结构的成员
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:HasExpirableItems
  #[inline]
  pub fn has_expirable_items(&self) -> bool {
    self.expiration_times.is_some()
  }

  // ---- 过期结构内部方法 ----

  /// 惰性创建过期字典 + 最小堆
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:InitializeExpirationStructures
  pub fn initialize_expiration_structures(&mut self) {
    if self.expiration_times.is_none() {
      self.expiration_times = Some(HashMap::with_hasher(GxBuildHasher::default()));
      self.expiration_queue = Some(BinaryHeap::new());
      self.heap_memory_size += 16; // C#: DictionaryOverhead + PriorityQueueOverhead
    }
  }

  /// 过期条目内存记账（add=false 回收）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:UpdateExpirationSize
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
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:CleanupExpirationStructuresIfEmpty
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

  /// 清除全部已过期成员（堆序快速路径）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:DeleteExpiredItems
  pub fn delete_expired_items(&mut self) {
    if self.expiration_times.is_none() {
      return;
    }
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

      let in_times = self
        .expiration_times
        .as_ref()
        .and_then(|t| t.get(&key))
        .is_some_and(|&actual| actual == expiration);

      if in_times {
        self.expiration_times.as_mut().unwrap().remove(&key);
        queue.pop();
        self.update_expiration_size(false, true);

        if let Some(value) = self.sorted_set_dict.get(&key).copied() {
          self.sorted_set_dict.remove(&key);
          self.sorted_set.remove(&SortedSetEntry {
            score: value,
            member: key.clone(),
          });
          self.update_size(&key, false);
        }
      } else {
        // 键已被 ZREM 等移除：仅弹掉堆项
        queue.pop();
        self.heap_memory_size -= 16 + 16;
      }
    }

    self.cleanup_expiration_structures_if_empty();
  }

  /// 设置成员过期（NX/XX/GT/LT 语义），返回 [`SortedSetExpireResult`] 码
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:SetExpiration
  pub fn set_expiration(
    &mut self,
    key: &[u8],
    expiration: i64,
    expire_option: ExpireOption,
  ) -> SortedSetExpireResult {
    if !self.sorted_set_dict.contains_key(key) {
      return SortedSetExpireResult::KeyNotFound;
    }

    if expiration <= now_ticks() {
      if let Some(value) = self.sorted_set_dict.remove(key) {
        self.sorted_set.remove(&SortedSetEntry {
          score: value,
          member: key.to_vec(),
        });
        self.update_size(key, false);
      }
      return SortedSetExpireResult::KeyAlreadyExpired;
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
        return SortedSetExpireResult::ExpireConditionNotMet;
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
        return SortedSetExpireResult::ExpireConditionNotMet;
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

    SortedSetExpireResult::ExpireUpdated
  }

  /// 移除成员过期（ZPERSIST 语义：-2 键不存在，1 成功，-1 无过期）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:Persist
  pub fn persist(&mut self, key: &[u8]) -> i32 {
    if !self.sorted_set_dict.contains_key(key) {
      return -2;
    }
    if self.try_remove_expiration(key) {
      1
    } else {
      -1
    }
  }

  /// 移除成员过期结构
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:TryRemoveExpiration
  #[inline]
  pub fn try_remove_expiration(&mut self, key: &[u8]) -> bool {
    if self.expiration_times.is_none() {
      return false;
    }
    self.try_remove_expiration_worker(key)
  }

  /// libs/server/Objects/SortedSet/SortedSetObject.cs:TryRemoveExpirationWorker
  fn try_remove_expiration_worker(&mut self, key: &[u8]) -> bool {
    let Some(times) = self.expiration_times.as_mut() else {
      return false;
    };
    if times.remove(key).is_none() {
      return false;
    }

    self.update_expiration_size(false, false);
    self.cleanup_expiration_structures_if_empty();
    true
  }

  /// 查询成员过期 ticks（-2 键不存在，-1 无过期）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:GetExpiration
  pub fn get_expiration(&self, key: &[u8]) -> i64 {
    if !self.sorted_set_dict.contains_key(key) {
      return -2;
    }
    if let Some(&expiration) = self.expiration_times.as_ref().and_then(|t| t.get(key)) {
      return expiration;
    }
    -1
  }

  /// 按字典迭代序取第 index 个成员（跳过已过期；越界返回 None）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:ElementAt
  /// （C# 越界抛 ArgumentOutOfRangeException，Rust 以 None 表达）
  pub fn element_at(&self, index: usize) -> Option<(Vec<u8>, f64)> {
    if self.has_expirable_items() {
      return self
        .sorted_set_dict
        .iter()
        .filter(|(k, _)| !self.is_expired(k))
        .nth(index)
        .map(|(k, v)| (k.clone(), *v));
    }

    self
      .sorted_set_dict
      .iter()
      .nth(index)
      .map(|(k, v)| (k.clone(), *v))
  }

  /// 条目内存记账（add=false 回收）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:UpdateSize
  #[inline]
  pub fn update_size(&mut self, item: &[u8], add: bool) {
    // RoundUp(len, 8) + ByteArrayOverhead(16) + 2 * sizeof(double) + 有序集合/字典项开销
    let memory_size = (item.len().div_ceil(8) * 8 + 16 + 16) as i64;

    if add {
      self.heap_memory_size += memory_size;
    } else {
      self.heap_memory_size -= memory_size;
    }
  }
}

/// 从 ObjectInput 头部提取有序集合操作码（C# header.SortedSetOp = subId）
#[inline]
pub fn sorted_set_op_from_header(input: &ObjectInput) -> Option<SortedSetOperation> {
  SortedSetOperation::try_from(input.header.sub_id()).ok()
}
