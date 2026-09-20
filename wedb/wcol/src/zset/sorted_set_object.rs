//! 有序集合对象（对标 libs/server/Objects/SortedSet/SortedSetObject.cs）
//!
//! 双索引结构与 C# 一致：`sorted_set`（(score, member) 有序视图，BTreeSet 对应
//! C# SortedSet + SortedSetComparer）+ `sorted_set_dict`（member → score 散列），
//! 另有成员级过期账本 [`ExpiryLedger`]（过期字典 + 最小堆单点，hash/zset 共用）。
//!
//! 记账口径（刻意差异，对照 C# `HeapMemorySize` 的 .NET GC 开销记账）：Rust 不照搬
//! `MemoryUtils.*Overhead`，改按「结构常驻基线 + 每条目相对字节数」自定口径具名记账，
//! 单点定义见 [`wbase::heap`]（`SLOT` / `CONTAINER_BASE` / `EXPIRY_STRUCT_BASE`）。

use std::{
  cmp::Ordering,
  collections::BTreeSet,
  io::{self, Read, Write},
};

use bitflags::bitflags;
use wbase::{
  glob::glob_match,
  heap::{CONTAINER_BASE, SLOT, round_up_ptr},
  map::HashMap,
  time::now_ticks,
};
use wresp::{
  cmd_strings::RESP_ERR_GENERIC_UNSUPPORTED_OPERATION as RESP_ERR_UNSUPPORTED_OPERATION,
  options::ExpireOption,
  resp_memory_writer::{RespWriter, format_double},
};
use wval::GarnetObjectType;
use zmij::Buffer;

use crate::{
  object_payload::{GarnetObjectPayload, NO_EXPIRY_WATERMARK, WATERMARKED_BLOB_HEADER},
  types::{ObjectOutput, ObjectOutputFlags, expiry_ledger::ExpiryLedger},
  zset::comparer::SortedSetComparer,
};

/// 主容器常驻基线：过期账本记账的透支断言底线（zset 双容器：有序视图 + 散列）
pub(crate) const EXPIRY_FLOOR: i64 = CONTAINER_BASE * 2;

/// 有序集合操作（AOF 持久化值，C# 侧为显式追加语义，不得改序/复用既有值）
///
/// libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetOperation
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::FromRepr)]
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

/// 有序集合条目：分值 + 成员，序由 [`crate::zset::comparer::SortedSetComparer`] 决定
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
    // 判序单点委托比较器（C# SortedSet 以 SortedSetComparer 为唯一比较入口，
    // 全序口径见其文档：NaN/±0.0 逐字对位 .NET Double.CompareTo）
    SortedSetComparer::compare((&self.score, &self.member), (&other.score, &other.member))
  }
}

/// 有序集合线格式（bitcode 载荷，AOF/检查点回写）
#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
pub struct SortedSetWire {
  pub entries: Vec<(Vec<u8>, f64)>,
  pub expirations: Option<Vec<(Vec<u8>, i64)>>,
}

/// 有序集合对象
///
/// libs/server/Objects/SortedSet/SortedSetObject.cs:SortedSetObject
#[derive(Debug, Clone)]
pub struct SortedSetObject {
  /// (score, member) 有序视图
  pub sorted_set: BTreeSet<SortedSetEntry>,
  /// member → score 散列
  pub sorted_set_dict: HashMap<Vec<u8>, f64>,
  /// 成员过期账本（过期字典 + 最小堆，惰性初始化；语义与记账单点见
  /// [`ExpiryLedger`]，挂/摘/回收经其方法、成员剔除经宿主闭包）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:expirationTimes/expirationQueue
  pub(crate) ledger: ExpiryLedger,
  /// 堆内存记账（相对值；C# 语义见文件头记账口径 [`wbase::heap`]）
  pub heap_memory_size: i64,
  /// 惰性过期剔除已实际移除成员（运行时状态，不进 [`SortedSetWire`] 序列化）
  ///
  /// 刻意差异：C# 对象常驻 Tsavorite 对象缓存，ZCARD 等读路径的
  /// DeleteExpiredItems 就地剔除经 checkpoint 序列化落盘；Rust 信封无常驻
  /// 对象层，以该标志升格写回等价闭环，避免已剔除成员重装载复活，
  /// 并使剔除持久化后堆序 peek 短路得以兑现计数 O(1) 摊还
  ///
  /// 与 hash 域同源范式（见 HashObject::mutated_by_ttl）
  ///
  /// 无 C# 对应（信封写回判定标志，见上方刻意差异）
  mutated_by_ttl: bool,
}

/// 构造即带双容器常驻基线（有序视图 + 散列，对位 C# `SortedSetObject` 构造
/// `base(SortedSetOverhead + DictionaryOverhead)`）：空集合 `heap_memory_size` 非零，
/// 回收不变式以此为准。
impl Default for SortedSetObject {
  fn default() -> Self {
    Self {
      sorted_set: BTreeSet::new(),
      sorted_set_dict: HashMap::default(),
      ledger: ExpiryLedger::default(),
      heap_memory_size: CONTAINER_BASE * 2,
      mutated_by_ttl: false,
    }
  }
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

  /// 直接从二进制切片反序列化有序集合对象（零中间 buffer 拷贝）
  pub fn deserialize_from_slice(slice: &[u8]) -> io::Result<Self> {
    let wire: SortedSetWire =
      bitcode::decode(slice).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

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
            // 装载即实际剔除（信封陈旧过期），与堆序惰性剔除同责：置升格
            // 写回标志，首个 RMW 通道读命令把矫正固化回信封并触发删空自愈
            obj.mutated_by_ttl = true;
          }
        } else if obj.sorted_set_dict.contains_key(&member) {
          obj.insert_expiration(member, expiration);
        }
      }
      obj
        .ledger
        .cleanup_if_empty(&mut obj.heap_memory_size, EXPIRY_FLOOR);
    }

    Ok(obj)
  }

  /// 从二进制流反序列化有序集合对象
  pub fn deserialize<R: Read>(reader: &mut R) -> io::Result<Self> {
    let mut buf = Vec::new();
    reader.read_to_end(&mut buf)?;
    Self::deserialize_from_slice(&buf)
  }

  /// 序列化为线格式载荷，单次遍历产出存活成员数、最早到期水位与 bitcode 载荷
  ///（严禁二次遍历）
  ///
  /// 水位即存活挂期成员的最早到期刻度（信封头 8B 域，读侧计数水位门单源，
  /// 见 [`crate::object_payload::expiry_watermark_of_blob`]，与 hash 域同源
  /// 范式）；无存活挂期恒 [`crate::object_payload::NO_EXPIRY_WATERMARK`]
  /// 永快道。写时已剔除到期成员，水位内不可能再有成员到期，读侧
  /// `now <= 水位` 期间头部计数恒精确
  pub(crate) fn serialize_wire(&self) -> (u32, i64, Vec<u8>) {
    let now = now_ticks();
    let has_expirations = self.ledger.has_items();
    let mut entries: Vec<(Vec<u8>, f64)> = Vec::with_capacity(self.sorted_set_dict.len());
    for (m, s) in &self.sorted_set_dict {
      if !has_expirations || !self.ledger.is_expired_at(m, now) {
        entries.push((m.clone(), *s));
      }
    }
    let count = entries.len() as u32;

    let mut watermark = NO_EXPIRY_WATERMARK;
    let expirations = self.ledger.times.as_ref().and_then(|times| {
      let mut active: Vec<(Vec<u8>, i64)> = Vec::with_capacity(times.len());
      for (m, exp) in times {
        if *exp >= now && self.sorted_set_dict.contains_key(m) {
          active.push((m.clone(), *exp));
          watermark = watermark.min(*exp);
        }
      }
      (!active.is_empty()).then_some(active)
    });

    let wire = SortedSetWire {
      entries,
      expirations,
    };
    (count, watermark, bitcode::encode(&wire))
  }

  /// 序列化为字节向量（过滤已过期条目；带过期成员附加 ticks）
  ///
  /// 在 garnet 中的相对路径:libs/server/Objects/SortedSet/SortedSetObject.cs:DoSerialize
  #[inline]
  pub fn serialize_to_vec(&self) -> Vec<u8> {
    self.serialize_wire().2
  }

  /// 序列化为二进制流（过滤已过期条目；带过期成员附加 ticks）
  pub fn serialize<W: Write>(&self, writer: &mut W) -> io::Result<()> {
    writer.write_all(&self.serialize_to_vec())
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
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:Add
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
      self
        .ledger
        .remove_expiration(&mut self.heap_memory_size, EXPIRY_FLOOR, member);
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
    sub_id: u8,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput<'_>,
    resp_protocol_version: u8,
  ) -> bool {
    let Some(op) = SortedSetOperation::from_repr(sub_id) else {
      // C#: switch default 抛 GarnetException("Unsupported operation ...")
      RespWriter::new_ref(output.payload)
        .write_error_bytes(RESP_ERR_UNSUPPORTED_OPERATION.as_bytes());
      return true;
    };

    match op {
      SortedSetOperation::Zadd => self.sorted_set_add(args, output, resp_protocol_version),
      SortedSetOperation::Zrem => self.sorted_set_remove(args, output),
      SortedSetOperation::Zcard => self.sorted_set_length(output),
      SortedSetOperation::Zpopmax => {
        self.sorted_set_pop_min_or_max_count(args, arg1, arg2, output, resp_protocol_version, op)
      }
      SortedSetOperation::Zscore => self.sorted_set_score(args, output, resp_protocol_version),
      SortedSetOperation::Zmscore => self.sorted_set_scores(args, output, resp_protocol_version),
      SortedSetOperation::Zcount => self.sorted_set_count(args, output),
      SortedSetOperation::Zincrby => {
        self.sorted_set_increment(args, arg2, output, resp_protocol_version)
      }
      SortedSetOperation::Zrank => {
        self.sorted_set_rank(args, arg1, output, resp_protocol_version, true)
      }
      SortedSetOperation::Zexpire => self.sorted_set_expire(args, arg1, arg2, output),
      SortedSetOperation::Zttl => self.sorted_set_time_to_live(args, arg1, arg2, output),
      SortedSetOperation::Zpersist => self.sorted_set_persist(args, output),
      SortedSetOperation::Zcollect => self.sorted_set_collect(output),
      SortedSetOperation::Geoadd => self.geo_add(args, arg1, output),
      SortedSetOperation::Geohash => self.geo_hash(args, output, resp_protocol_version),
      SortedSetOperation::Geodist => self.geo_distance(args, output, resp_protocol_version),
      SortedSetOperation::Geopos => self.geo_position(args, output, resp_protocol_version),
      SortedSetOperation::Zrange => {
        self.sorted_set_range(args, arg2, output, resp_protocol_version)
      }
      SortedSetOperation::Zrevrank => {
        self.sorted_set_rank(args, arg1, output, resp_protocol_version, false)
      }
      SortedSetOperation::Zremrangebylex => {
        self.sorted_set_remove_or_count_range_by_lex(args, output, op)
      }
      SortedSetOperation::Zremrangebyrank => self.sorted_set_remove_range_by_rank(args, output),
      SortedSetOperation::Zremrangebyscore => self.sorted_set_remove_range_by_score(args, output),
      SortedSetOperation::Zlexcount => {
        self.sorted_set_remove_or_count_range_by_lex(args, output, op)
      }
      SortedSetOperation::Zpopmin => {
        self.sorted_set_pop_min_or_max_count(args, arg1, arg2, output, resp_protocol_version, op)
      }
      SortedSetOperation::Zrandmember => {
        self.sorted_set_random_member(args, arg1, arg2, output, resp_protocol_version)
      }
      SortedSetOperation::Zscan => self.scan_operate(args, arg2, output, resp_protocol_version),
      // GEOSEARCH 由命令层经 geo_search(opts) 直入（携带 GeoSearchOptions 束）；
      // ZDIFF/ZUNION/ZINTER 属存储 API 域聚合（C# 同样不经 operate 分派）
      SortedSetOperation::Geosearch | SortedSetOperation::Zdiff => {
        RespWriter::new_ref(output.payload)
          .write_error_bytes(RESP_ERR_UNSUPPORTED_OPERATION.as_bytes());
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
          let mut fbuf = Buffer::new();
          Some(format_double(*score, &mut fbuf).as_bytes().to_vec())
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

  /// 成员数（剔除已过期，O(1) 摊还）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:Count
  ///
  /// 与 C# 的刻意差异声明：C# Count()（SortedSetObject.cs:606）逐键过滤
  /// expirationTimes，为 O(带 TTL 成员数) 只读计数；rust 依
  /// doc/zh/collection.md §6「计数命令严格 O(1)」规约改为先走堆序
  /// DeleteExpiredItems（摊还 O(弹过量)，稳态堆顶 peek 短路 O(1)），
  /// 再直读 sorted_set_dict.len()。应答值与 C# 逐值一致（两侧已过期项均不计入）；
  /// 物理剔除经 mutated_by_ttl 写回升格闭环，杜绝已剔除成员重装载复活
  /// （与 hash 域 mutated_by_ttl 同源范式）。显式命名 purge 语义，与
  /// trait [`IGarnetObject::count`]（raw len 只读，仅供升阶判定）同名
  /// 双口径消歧：&mut 语境方法解析优先命中本方法，同名会让调用方
  /// 误以为只读
  pub fn purge_expired_len(&mut self) -> usize {
    self.delete_expired_items();
    self.sorted_set_dict.len()
  }

  /// 成员是否已过期
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:IsExpired
  #[inline]
  pub fn is_expired(&self, key: &[u8]) -> bool {
    self.ledger.is_expired_at(key, now_ticks())
  }

  /// 惰性过期剔除是否已实际移除成员（读命令写回升格判定标志）
  ///
  /// 无 C# 对应（信封写回判定标志，见 [`SortedSetObject::mutated_by_ttl`] 字段说明）
  #[inline]
  pub const fn mutated_by_ttl(&self) -> bool {
    self.mutated_by_ttl
  }

  /// 挂成员过期条目（字典 + 最小堆 + 记账，结构未初始化时先建）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:expirationTimes.Add +
  /// expirationQueue.Enqueue（线格式装载与分层物化还原共用此单点）
  pub fn insert_expiration(&mut self, member: Vec<u8>, expiration: i64) {
    self
      .ledger
      .insert(&mut self.heap_memory_size, member, expiration);
  }

  /// 清除全部已过期成员（堆序快速路径）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:DeleteExpiredItems
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:DeleteExpiredItemsWorker
  ///
  /// C# 的 DeleteExpiredItems（:677）只转起后台线程、剔除主体在
  /// DeleteExpiredItemsWorker（:684）；rust 信封无常驻对象层与后台线程，
  /// 两枚方法体一次内联完成。堆序摘除核心（失步裁决 + 记账 + 尾部全空回收）
  /// 收敛于 [`ExpiryLedger::pop_expired`]，宿主闭包只承担成员剔除：
  /// 双索引移除 + 条目记账 + 置写回升格标志
  pub fn delete_expired_items(&mut self) {
    if !self.ledger.has_items() {
      return;
    }
    let Self {
      sorted_set,
      sorted_set_dict,
      ledger,
      heap_memory_size,
      mutated_by_ttl,
      ..
    } = self;
    ledger.pop_expired(heap_memory_size, now_ticks(), EXPIRY_FLOOR, |key, heap| {
      if let Some(score) = sorted_set_dict.remove(key) {
        sorted_set.remove(&SortedSetEntry {
          score,
          member: key.to_vec(),
        });
        account_entry(heap, key, false);
        *mutated_by_ttl = true;
      }
    });
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
    // 成员存在性判定为 zset 宿主语义（C# 裸 sortedSetDict.ContainsKey，
    // 不过滤已过期成员，与 Hash 侧的过滤版 ContainsKey 差异 1:1 保留）
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

    if self
      .ledger
      .set_expiration(&mut self.heap_memory_size, key, expiration, expire_option)
    {
      SortedSetExpireResult::ExpireUpdated
    } else {
      SortedSetExpireResult::ExpireConditionNotMet
    }
  }

  /// 移除成员过期（ZPERSIST 语义：-2 键不存在，1 成功，-1 无过期）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:Persist
  pub fn persist(&mut self, key: &[u8]) -> i32 {
    if !self.sorted_set_dict.contains_key(key) {
      return -2;
    }
    if self
      .ledger
      .remove_expiration(&mut self.heap_memory_size, EXPIRY_FLOOR, key)
    {
      1
    } else {
      -1
    }
  }

  /// 查询成员过期 ticks（-2 键不存在，-1 无过期）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:GetExpiration
  pub fn get_expiration(&self, key: &[u8]) -> i64 {
    if !self.sorted_set_dict.contains_key(key) {
      return -2;
    }
    self.ledger.get_time(key).unwrap_or(-1)
  }

  /// 按字典迭代序取第 index 个成员（跳过已过期；越界返回 None）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:ElementAt
  /// （C# 越界抛 ArgumentOutOfRangeException，Rust 以 None 表达；
  /// C# 返回 KeyValuePair<byte[], double> 即字典内的引用，故成员名回借用切片，
  /// 采样路径零拷贝直写 RESP）
  pub fn element_at(&self, index: usize) -> Option<(&[u8], f64)> {
    if self.ledger.has_items() {
      return self
        .sorted_set_dict
        .iter()
        .filter(|(k, _)| !self.is_expired(k))
        .nth(index)
        .map(|(k, v)| (k.as_slice(), *v));
    }

    self
      .sorted_set_dict
      .iter()
      .nth(index)
      .map(|(k, v)| (k.as_slice(), *v))
  }

  /// 条目内存记账（add=false 回收）
  ///
  /// libs/server/Objects/SortedSet/SortedSetObject.cs:UpdateSize
  #[inline]
  pub fn update_size(&mut self, item: &[u8], add: bool) {
    account_entry(&mut self.heap_memory_size, item, add);
  }
}

/// 条目内存记账单点（`SortedSetObject::update_size` 与到期剔除闭包共用）：
/// 数据实计 + 两槽（member 句柄 + 有序集合/字典槽位）
///
/// libs/server/Objects/SortedSet/SortedSetObject.cs:UpdateSize
#[inline]
fn account_entry(heap: &mut i64, item: &[u8], add: bool) {
  // C#: RoundUp(len,IntPtr.Size) + ByteArrayOverhead + 2*sizeof(double)
  //     + SortedSetEntryOverhead + DictionaryEntryOverhead(=168)，rust 口径塌缩为 SLOT*2(=32)，值不等
  let memory_size = round_up_ptr(item.len()) as i64 + SLOT * 2;

  if add {
    *heap += memory_size;
  } else {
    *heap -= memory_size;
    // C#: Debug.Assert(HeapMemorySize >= DictionaryOverhead)
    debug_assert!(*heap >= EXPIRY_FLOOR);
  }
}

impl GarnetObjectPayload for SortedSetObject {
  const OBJECT_TAG: GarnetObjectType = GarnetObjectType::SortedSet;

  #[inline]
  fn from_blob(raw: &[u8]) -> Option<Self> {
    // 单格式：载荷恒带 `[4B 计数][8B 到期水位]` 头（to_blob 恒落头），无头
    // 二次回退臂已删；畸形载荷显式失败（对标 C# GarnetObjectSerializer.
    // DeserializeInternal fail-fast）
    Self::deserialize_from_slice(raw.get(WATERMARKED_BLOB_HEADER..)?).ok()
  }

  #[inline]
  fn to_blob(&self) -> Vec<u8> {
    let (count, watermark, wire) = self.serialize_wire();
    let mut out = Vec::with_capacity(WATERMARKED_BLOB_HEADER + wire.len());
    out.extend_from_slice(&count.to_le_bytes());
    out.extend_from_slice(&watermark.to_le_bytes());
    out.extend_from_slice(&wire);
    out
  }

  #[inline]
  fn is_empty(&self) -> bool {
    self.sorted_set_dict.is_empty()
  }
}

impl From<SortedSetOperation> for u8 {
  #[inline]
  fn from(op: SortedSetOperation) -> Self {
    op as u8
  }
}
