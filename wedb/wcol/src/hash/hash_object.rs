//! 哈希对象（对标 libs/server/Objects/Hash/HashObject.cs）
//!
//! 结构与 C# 一致：`hash`（field → value 散列）+ 成员级过期账本
//! [`ExpiryLedger`]（过期字典 + 最小堆单点，hash/zset 共用），另有堆内存记账
//! `heap_memory_size`。
//!
//! 记账口径（刻意差异，对照 C# `HeapMemorySize` 的 .NET GC 开销记账）：Rust 不照搬
//! `MemoryUtils.*Overhead`，改按「结构常驻基线 + 每条目相对字节数」自定口径具名记账，
//! 单点定义见 [`wbase::heap`]（`SLOT` / `CONTAINER_BASE` / `EXPIRY_STRUCT_BASE`）。

use std::{
  io::{self, Read, Write},
  sync::Arc,
};

use wbase::{
  glob::glob_match,
  heap::{CONTAINER_BASE, SLOT, round_up_ptr},
  map::HashMap,
  time::now_ticks,
};
use wresp::{
  cmd_strings::RESP_ERR_GENERIC_UNSUPPORTED_OPERATION as RESP_ERR_UNSUPPORTED_OPERATION,
  options::ExpireOption, resp_memory_writer::RespWriter,
};
use wval::GarnetObjectType;

use crate::{
  object_payload::{GarnetObjectPayload, NO_EXPIRY_WATERMARK, WATERMARKED_BLOB_HEADER},
  types::{ObjectOutput, ObjectOutputFlags, expiry_ledger::ExpiryLedger, scan_converge_cursor},
};

/// 主容器常驻基线：过期账本记账的透支断言底线（hash 单容器）
pub(crate) const EXPIRY_FLOOR: i64 = CONTAINER_BASE;

/// 哈希操作（AOF 持久化值，C# 侧为显式追加语义，不得改序/复用既有值）
///
/// libs/server/Objects/Hash/HashObject.cs:HashOperation
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::FromRepr)]
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
  /// field → value 散列（field 键为共享句柄 [`Arc`]：与过期账本共享同一句柄，
  /// 对位 C# hash 字典与 expirationTimes/expirationQueue 的 byte[] 引用共享）
  pub hash: HashMap<Arc<[u8]>, Vec<u8>>,
  /// 成员过期账本（过期字典 + 最小堆，惰性初始化；语义与记账单点见
  /// [`ExpiryLedger`]，挂/摘/回收经其方法、成员剔除经宿主闭包）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:expirationTimes/expirationQueue
  pub(crate) ledger: ExpiryLedger,
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
      ledger: ExpiryLedger::default(),
      heap_memory_size: CONTAINER_BASE,
      mutated_by_ttl: false,
    }
  }
}

/// 哈希线格式（bitcode 载荷，AOF/检查点回写）
///
/// 刻意差异：bitcode 0.6 的零拷贝借用编码仅支持 `&str`，`&[u8]` 无 `Encode`
/// 实现，编码侧无法借引条目免 clone（owned 收集为格式约束下的最优解）
///
/// 演进不变量约束：结构体禁含枚举变体，字段只许尾部追加；若破坏结构须增加版本域并拒旧。
#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
pub struct HashWire {
  pub entries: Vec<(Vec<u8>, Vec<u8>)>,
  pub expirations: Option<Vec<(Vec<u8>, i64)>>,
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
      // 一处分配多处持有：散列与过期账本共享同一 Arc
      let item = Arc::<[u8]>::from(item);
      // 重复 field 判重守卫：C# 侧 hash.Add 遇重复键抛 ArgumentException 由上层
      // 拒载畸形载荷（HashObject.cs:131/:140）；此处与 zset 侧 entries 同守卫——
      // 跳过重复保持首条有效，否则 update_size 逐条累加致记账永久虚标且值静默
      // 末条覆盖（与 zset 侧单套守卫形态，双票一次收口）
      if obj.hash.contains_key(item.as_ref()) {
        continue;
      }
      obj.update_size(&item, &value, true);
      obj.hash.insert(item, value);
    }

    if let Some(expirations) = wire.expirations {
      for (item, expiration) in expirations {
        if expiration < now {
          if let Some(val) = obj.hash.remove(item.as_slice()) {
            obj.update_size(&item, &val, false);
            // 装载即实际剔除（信封陈旧过期），与堆序惰性剔除同责：置升格
            // 写回标志，首个 RMW 通道读命令把矫正固化回信封并触发删空自愈
            obj.mutated_by_ttl = true;
          }
        } else if obj.ledger.get_time(item.as_slice()).is_none() {
          // expirations 判重守卫：重复条目二次满额入账而字典仅覆盖，净虚标
          // 两槽且堆内滞留陈旧项（与 entries 守卫同形，跳过保持首条刻度）
          if let Some(handle) = obj
            .hash
            .get_key_value(item.as_slice())
            .map(|(k, _)| k.clone())
          {
            // 取回散列内同一共享句柄（对位 C# 同一 byte[] 引用传入账本）
            obj.insert_expiration(handle, expiration);
          }
        }
      }
      obj
        .ledger
        .cleanup_if_empty(&mut obj.heap_memory_size, EXPIRY_FLOOR);
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
  /// 序列化为线格式载荷，单次遍历产出存活条目数、最早到期水位与 bitcode 载荷
  ///（严禁二次遍历）
  ///
  /// 水位即存活挂期成员的最早到期刻度（信封头 8B 域，读侧计数水位门单源，
  /// 见 [`crate::object_payload::expiry_watermark_of_blob`]）；无存活挂期恒
  /// [`crate::object_payload::NO_EXPIRY_WATERMARK`] 永快道。写时已剔除到期
  /// 成员，水位内不可能再有成员到期，读侧 `now <= 水位` 期间头部计数恒精确
  pub(crate) fn serialize_wire(&self) -> (u32, i64, Vec<u8>) {
    let now = now_ticks();
    let has_expirations = self.ledger.has_items();
    let mut entries: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(self.hash.len());
    for (k, v) in &self.hash {
      if !has_expirations || !self.ledger.is_expired_at(k, now) {
        entries.push((k.to_vec(), v.clone()));
      }
    }
    let count = entries.len() as u32;

    let mut watermark = NO_EXPIRY_WATERMARK;
    let expirations = self.ledger.times.as_ref().and_then(|times| {
      let mut active: Vec<(Vec<u8>, i64)> = Vec::with_capacity(times.len());
      for (k, exp) in times {
        if *exp >= now && self.hash.contains_key(k) {
          active.push((k.to_vec(), *exp));
          watermark = watermark.min(*exp);
        }
      }
      (!active.is_empty()).then_some(active)
    });

    let wire = bitcode::encode(&HashWire {
      entries,
      expirations,
    });
    (count, watermark, wire)
  }

  /// 序列化为字节向量（过滤已过期条目；带过期成员附加 ticks）
  ///
  /// 在 garnet 中的相对路径:libs/server/Objects/Hash/HashObject.cs:DoSerialize
  #[inline]
  pub fn serialize_to_vec(&self) -> Vec<u8> {
    self.serialize_wire().2
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
    let Some(op) = HashOperation::from_repr(sub_id) else {
      // C#: switch default 抛 GarnetException("Unsupported operation ...")
      RespWriter::new_ref(output.payload)
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
    account_entry(&mut self.heap_memory_size, key, value, add);
  }

  /// 挂成员过期条目（字典 + 最小堆 + 记账，结构未初始化时先建）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:expirationTimes.Add + expirationQueue.Enqueue
  /// （线格式装载与分层物化还原共用此单点）
  ///
  /// key 取共享句柄：账本内字典与堆只增引用计数，不复制字段字节
  pub fn insert_expiration(&mut self, key: Arc<[u8]>, expiration: i64) {
    self
      .ledger
      .insert(&mut self.heap_memory_size, key, expiration);
  }

  /// 清除全部已过期成员（堆序快速路径）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:DeleteExpiredItems
  /// libs/server/Objects/Hash/HashObject.cs:DeleteExpiredItemsWorker
  ///
  /// 堆序摘除核心（失步裁决 + 记账 + 尾部全空回收）收敛于
  /// [`ExpiryLedger::pop_expired`]，宿主闭包只承担成员剔除：
  /// hash.remove + 条目记账 + 置写回升格标志
  pub fn delete_expired_items(&mut self) {
    if !self.ledger.has_items() {
      return;
    }
    let Self {
      hash,
      ledger,
      heap_memory_size,
      mutated_by_ttl,
      ..
    } = self;
    ledger.pop_expired(heap_memory_size, now_ticks(), EXPIRY_FLOOR, |key, heap| {
      if let Some(value) = hash.remove(key) {
        account_entry(heap, key, &value, false);
        *mutated_by_ttl = true;
      }
    });
  }

  /// HSCAN：遍历字段（光标 + MATCH/COUNT/NOVALUES）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:Scan
  ///
  /// 与 sortedset 不同：NOVALUES 生效（只回字段），且 count 在成对形态下先翻倍
  /// （`count = isNoValue ? count : count * 2`），恰集满 `count` 项即停——该
  /// 「截断比较」以 `==` 判定保留（count=0 时首个未命中条目即停的上游
  /// 怪癖一并保留）。1:1 宣称在极值点失真：C# int32 unchecked 侧
  /// COUNT=-2147483648 翻倍回绕为 0，退化为「首条未命中即停」空页爬行
  /// （不属负值全量遍历族），rust 按 i64 加宽永不回绕、不复刻该形态
  /// （登记见 doc/zh/deviations.md 第 20 条 d)，严禁按 C# 回改为 i32 截断
  /// 回绕——回改才是真回归且破坏双态全等）；尾段游标收敛则经单点
  /// [`scan_converge_cursor`] 以 `>=` 归零，系对上游相等判定死锁死角的刻意
  /// 修复（偏离登记见 doc/zh/deviations.md）。
  ///
  /// emit 回调借字段/值切片直写出帧（返回发出条目计数，替代 C# out List 的
  /// 引用收集——集合内字节零拷贝，无逐条目堆物化），游标由本内核返回
  pub fn scan(
    &self,
    start: i64,
    count: i64,
    pattern: &[u8],
    is_no_value: bool,
    mut emit: impl FnMut(&[u8], &[u8]) -> usize,
  ) -> i64 {
    let mut cursor = start;

    if (self.hash.len() as i64) < start {
      return 0;
    }

    // Hashset has key and value, so count is multiplied by 2
    let count = if is_no_value { count } else { count * 2 };
    let mut index = 0_i64;
    let mut expired_keys_count = 0_i64;
    // 发出条目计数（对位 C# items.Count 的截断比较口径）
    let mut emitted = 0_i64;

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
        emitted += emit(key, value) as i64;
      }

      cursor += 1;

      // C# 以相等判断截断（负 COUNT 恒不命中 → 全量遍历；count=0 首个未命中
      // 条目即停的上游怪癖一并保留）。极值点 1:1 失真已登记：C# int32
      // unchecked 侧 COUNT=-2147483648 翻倍回绕为 0 致首条未命中即停（非
      // 全量遍历族），rust i64 加宽不复刻该回绕（登记见
      // doc/zh/deviations.md 第 20 条 d)，勿按 C# 回改）
      if emitted == count {
        break;
      }
    }

    // 到达集合末尾则光标归零：经单点判定收敛（内存态与分层态同口径），
    // 修复上游 `==` 判定在存活数 < 起始游标 <= 含到期总数死角的原游标死锁
    scan_converge_cursor(cursor, expired_keys_count, self.hash.len() as i64)
  }

  /// 成员是否已过期
  ///
  /// libs/server/Objects/Hash/HashObject.cs:IsExpired
  #[inline]
  pub fn is_expired(&self, key: &[u8]) -> bool {
    self.ledger.is_expired_at(key, now_ticks())
  }

  /// 惰性过期剔除是否已实际移除条目（写回升格判定依据）
  #[inline]
  pub const fn mutated_by_ttl(&self) -> bool {
    self.mutated_by_ttl
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
    // 摘账本字典项 + 全空整体回收（remove_expiration 完整语义，zset 侧 rem
    // 同形态）：C# Remove 残余堆项交下一次 DeleteExpiredItems，记账短暂滞留
    // 结构基线与堆槽；rust 收口为即时归位，末字段移除后 heap_memory_size
    // 恒回容器基线，RESP 可见行为不变
    self
      .ledger
      .remove_expiration(&mut self.heap_memory_size, EXPIRY_FLOOR, key);
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
  /// （C# HTTL 读路径 DeleteExpiredItems 经 checkpoint 落盘同理）。
  /// 显式命名 purge 语义，与 trait [`IGarnetObject::count`]（raw len 只读，
  /// 仅供升阶判定）同名双口径消歧：&mut 语境方法解析优先命中本方法，
  /// 同名会让调用方误以为只读
  pub fn purge_expired_len(&mut self) -> usize {
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
    self.hash.insert(Arc::from(key.to_vec()), value);
  }

  /// 设置成员过期（NX/XX/GT/LT 语义），返回 [`HashExpireResult`] 码
  ///
  /// libs/server/Objects/Hash/HashObject.cs:SetExpiration
  ///
  /// 与 C# 的刻意差异声明（XX/GT 拒绝臂的字段存活与幻影项缺陷，详见
  /// [`ExpiryLedger::set_expiration`]）：C# 先以 GetValueRefOrAddDefault 插入
  /// 0 值幻影项，被拒字段自此在 HGET/HLEN/HEXPIRE 眼中失活且记账缺失，
  /// 属 C# 侧缺陷；rust 只读探测现值、拒绝臂零副作用，应答值与 C# 一致
  /// 而后续可见状态更正确，为有意不复刻，禁止为「对齐 C#」复刻幻影项
  pub fn set_expiration(
    &mut self,
    key: &[u8],
    expiration: i64,
    expire_option: ExpireOption,
  ) -> HashExpireResult {
    // 成员存在性判定为 hash 宿主语义（C# 过滤已过期字段的 ContainsKey）；
    // 过期账本收主容器内的共享句柄，不另行分配（字段字节全容器单份）
    let Some(handle) = self
      .hash
      .get_key_value(key)
      .filter(|_| !self.is_expired(key))
      .map(|(k, _)| k.clone())
    else {
      return HashExpireResult::KeyNotFound;
    };

    if expiration <= now_ticks() {
      self.remove(key);
      return HashExpireResult::KeyAlreadyExpired;
    }

    if self.ledger.set_expiration(
      &mut self.heap_memory_size,
      handle,
      expiration,
      expire_option,
    ) {
      HashExpireResult::ExpireUpdated
    } else {
      HashExpireResult::ExpireConditionNotMet
    }
  }

  /// 移除成员过期（HPERSIST 语义：-2 键不存在，1 成功，-1 无过期）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:Persist
  pub fn persist(&mut self, key: &[u8]) -> i32 {
    if !self.contains_key(key) {
      return HashExpireResult::KeyNotFound as i32;
    }

    if self
      .ledger
      .remove_expiration(&mut self.heap_memory_size, EXPIRY_FLOOR, key)
    {
      HashExpireResult::ExpireUpdated as i32
    } else {
      -1
    }
  }

  /// 查询成员过期 ticks（-2 键不存在，-1 无过期）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:GetExpiration
  pub fn get_expiration(&self, key: &[u8]) -> i64 {
    if !self.contains_key(key) {
      return i64::from(HashExpireResult::KeyNotFound as i32);
    }
    self.ledger.get_time(key).unwrap_or(-1)
  }

  /// 按散列迭代序取第 index 个字段值对（越界返回 None；调用方须先 purge）
  ///
  /// libs/server/Objects/Hash/HashObject.cs:ElementAt
  /// （C# 越界抛 ArgumentOutOfRangeException，Rust 以 None 表达；
  /// C# 返回 KeyValuePair<byte[], byte[]> 即字典内的引用，故此处回借用切片，
  /// 采样路径零拷贝直写 RESP）
  ///
  /// 刻意差异（对位 C# 的过滤臂删除）：C# ElementAt 迭代臂逐项 IsExpired
  /// 重采样（HashObject.cs:673 不足项越界抛），与 Count() 的单次采样窗错位时
  /// 可取项数少于声明数即断流；rust 消费点全部前置 purge_expired_len 物理摘除
  /// 到期项，此处不再二次重采样，声明数恒等可取项数（修复型偏离，
  /// doc/zh/deviations.md「成员级 TTL 容器读臂两次时钟采样」条）
  pub fn element_at(&self, index: usize) -> Option<(&[u8], &[u8])> {
    self
      .hash
      .iter()
      .nth(index)
      .map(|(k, v)| (&**k, v.as_slice()))
  }
}

/// 条目内存记账单点（`HashObject::update_size` 的内部实现，与到期剔除闭包共用；
/// C# UpdateSize 锚由宿主方法单点持有，本函数不重复挂锚）：
/// 数据实计 + 三槽（key 句柄 + value 句柄 + 字典槽位）
///
/// key 字节全容器仅一份（散列与过期账本共享同一 [`Arc`] 句柄），此处计一份
/// 即足额，对位 C# byte[] 引用共享下的 UpdateSize 单份口径；过期账本侧按
/// C# UpdateExpirationSize 只计槽位不计字节，无重复
#[inline]
fn account_entry(heap: &mut i64, key: &[u8], value: &[u8], add: bool) {
  // C#: RoundUp(key,IntPtr.Size) + RoundUp(value,IntPtr.Size)
  //     + 2*ByteArrayOverhead + DictionaryEntryOverhead（rust 口径塌缩为 SLOT*3，值不等）
  let memory_size = (round_up_ptr(key.len()) + round_up_ptr(value.len())) as i64 + SLOT * 3;

  if add {
    *heap += memory_size;
  } else {
    *heap -= memory_size;
    // C#: Debug.Assert(HeapMemorySize >= DictionaryOverhead)
    debug_assert!(*heap >= EXPIRY_FLOOR);
  }
}

impl GarnetObjectPayload for HashObject {
  const OBJECT_TAG: GarnetObjectType = GarnetObjectType::Hash;

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
    self.hash.is_empty()
  }
}

impl From<HashOperation> for u8 {
  #[inline]
  fn from(op: HashOperation) -> Self {
    op as u8
  }
}
