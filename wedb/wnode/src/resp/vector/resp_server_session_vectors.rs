//! Vector Set 命令的网络层（对标 libs/server/Resp/Vector/RespServerSessionVectors.cs）
//!
//! C# 为 RespServerSession 的 partial，直接消费 parseState / storageApi /
//! networkSender；Rust 侧 RespServerSession 属并行域，此处以
//! `&[&[u8]]` 参数面 + [`VectorReply`] 应答面承接同一命令语义
//! （选项解析、重复项报错、默认值、RESP2/RESP3 应答布局）。
//!
//! 数值解析复用 [`wbase::num`] 的 `strict_i32`/`strict_f32`（逐项对齐 C#
//! `parseState.TryGetInt`/`TryGetFloat` 的严格语义），错误文案与命令应答对齐
//! C# 字面量。
//!
//! 命令体按族拆分：共享取参骨架在 [`super::vectors_parse`]，写命令族
//! （VADD/VREM/VSETATTR）在 [`super::vectors_write`]，查询命令族（VSIM/
//! VEMB/VCARD 等）在 [`super::vectors_query`]；本文件保留应答模型、守卫
//! 裁决与快/慢路径分派骨架。

use core::fmt;
use std::{borrow::Cow, ops::RangeBounds, sync::Arc};

use wdev::Device;
use wresp::{
  cmd_strings::{RESP_ERR_SLOW_PATH_STORAGE, write_error_raw},
  command::RespCommand,
  ext::is_resp3,
  resp_memory_writer::{Resp2, Resp3, RespProtocol as _, RespWriter},
};
use wval::KeyTag;
use wvector::store::StoreCallbacks;

use super::{
  ERR_VECTOR_SET_DISABLED,
  vector_manager::{ERR_VECTOR_SERVICE_RESPONSE, VectorManager},
};

// ── 错误文案常量（逐字节对齐 C# 字面量；自带 RESP 前缀原样写出） ──

/// AbortVectorSetWrongType：对齐 Redis 行为、不指名具体类型。
/// 注意：本条为无句点版，C# 在
/// libs/server/Resp/Vector/RespServerSessionVectors.cs:AbortVectorSetWrongType
/// 就地书写同款无句点字面量，与 CmdStrings.RESP_ERR_WRONG_TYPE 的带句点版
/// 是两条不同文案，不合并（与已合并的 wresp::cmd_strings 单点无副本关系）。
/// 向量十二命令全族统一本无句点版系刻意偏差——C# 五命令（VCARD/VISMEMBER/
/// VLINKS/VRANDMEMBER/VSETATTR）臂间改发带句点版属上游混乱，严禁在本域引
/// wresp 带句点常量按命令分流回写，裁决与对拍口径见 doc/zh/deviations.md §164。
const ERR_VECTOR_SET_WRONG_TYPE: &[u8] =
  b"WRONGTYPE Operation against a key holding the wrong kind of value";
pub(super) const ERR_INVALID_VECTOR_SPEC: &[u8] = b"ERR invalid vector specification";
pub(super) const ERR_REDUCE_MUST_BE_POSITIVE: &[u8] = b"REDUCE dimension must be > 0";
pub(super) const ERR_REDUCE_EXCEEDS_DIMS: &[u8] =
  b"ERR REDUCE dimension must be <= vector dimensions";
pub(super) const ERR_INVALID_OPTION_AFTER_ELEMENT: &[u8] = b"ERR invalid option after element";
pub(super) const ERR_QUANT_SPECIFIED_TWICE: &[u8] = b"Quantization specified multiple times";
pub(super) const ERR_EF_RANGE: &[u8] = b"ERR EF must be an integer between 1 and 1000000";
pub(super) const ERR_M_RANGE: &[u8] = b"ERR M must be an integer between 4 and 4096";
pub(super) const ERR_INVALID_DISTANCE_METRIC: &[u8] = b"ERR invalid XDISTANCE_METRIC";
pub(super) const ERR_EMPTY_VECTOR_SET_KEY: &[u8] = b"ERR Vector Set key cannot be empty";
pub(super) const ERR_QUANT_MISMATCH: &[u8] = super::vector_manager::ERR_QUANTIZATION_MISMATCH;
pub(super) const ERR_FP32_MULTIPLE_OF_4: &[u8] = b"FP32 values must be multiple of 4-bytes in size";
pub(super) const ERR_VALUES_COUNT_MUST_BE_POSITIVE: &[u8] = b"VALUES count must > 0";
pub(super) const ERR_VALUES_MUST_BE_FLOAT: &[u8] = b"VALUES value must be valid float";
pub(super) const ERR_VSIM_EXPECTED_KIND: &[u8] = b"VSIM expected ELE, FP32, or VALUES";
pub(super) const ERR_COUNT_RANGE: &[u8] = b"ERR COUNT must be an integer between 0 and 100000000";
pub(super) const ERR_EPSILON_MUST_BE_POSITIVE: &[u8] = b"EPSILON must be float > 0";
pub(super) const ERR_FILTER_EF_RANGE: &[u8] = b"ERR FILTER-EF must be an integer between 4 and 256";
pub(super) const ERR_UNKNOWN_OPTION: &[u8] = b"Unknown option";
/// 元素不在集合中（刻意偏差见 `doc/zh/deviations.md` §79：激活 C# 会话层死分支文案，严禁回改）。
pub(crate) const ERR_ELEMENT_NOT_IN_SET: &[u8] = b"Element not in Vector Set";
pub(super) const ERR_VEMB_UNEXPECTED_OPTION: &[u8] = b"Unexpected option to VEMB";
pub(super) const ERR_KEY_NOT_FOUND: &[u8] = b"ERR Key not found";
pub(super) const ERR_VLINKS_UNEXPECTED_OPTION: &[u8] = b"ERR Unexpected option";
pub(super) const ERR_EXPECTED_INTEGER_COUNT: &[u8] = b"ERR expected integer count";

/// 选项重复文案（对齐 C# `"<OPT> specified multiple times"` 字面量）。
macro_rules! err_dup {
  ($opt:literal) => {
    VectorReply::err(concat!($opt, " specified multiple times").as_bytes())
  };
}

/// 开关型选项消费骨架（各 NetworkV* 选项循环共用）：重复置位即回
/// [`err_dup!`] 编译期文案，否则置位并跳过关键字参。
macro_rules! dup_flag {
  ($cur:expr, $seen:expr, $opt:literal) => {
    if *$seen {
      return Err(err_dup!($opt));
    }
    *$seen = true;
    $cur.skip();
  };
}

/// 命令入口 arity 守卫骨架（各 NetworkV* 入口共用）：预览未启用 / 参数
/// 个数不在 `range` 区间即就地应答返回（文案 [`wrong_num_args!`] 单点）。
macro_rules! wna_entry {
  ($sess:expr, $args:expr, $range:expr, $cmd:literal) => {
    if let Some(reply) = $sess.entry($args, $range, wrong_num_args!($cmd).as_bytes()) {
      return reply;
    }
  };
}

pub(super) use dup_flag;
pub(super) use err_dup;
pub(super) use wna_entry;

/// 命令应答（RESP 数据模型）。
///
/// 静态文案（错误/简单字符串）以 `&'static [u8]` 借用承载，动态载荷
/// （存储读出值、非常量错误）经 `Cow<'static, [u8]>` 落堆：
/// 检索命中 id/属性的源缓冲（`SimilarityOutput`）为函数局部量，应答归还
/// 后即释放，故借用上限为 'static，非静态载荷一律 Owned。
/// 整型标量不入 `Bulk`：一律走 [`VectorReply::BulkInt`]，帧字节由 wresp 单点在
/// 编码期以 itoa 栈上缓冲产出，构造期零堆分配。

#[derive(Debug, PartialEq)]
pub enum VectorReply {
  /// 简单字符串（+...，载荷恒为编译期常量文案）。
  Simple(&'static [u8]),
  /// 错误（-...，含完整前缀）。
  Error(Cow<'static, [u8]>),
  /// 整数。
  Integer(i64),
  /// 批量字符串（None = NULL）。
  Bulk(Option<Cow<'static, [u8]>>),
  /// 整数值批量字符串（`$<len>\r\n<digits>\r\n`）。
  ///
  /// 对标 C# RespServerSessionVectors.cs:1608-1616 `WriteInt32AsBulkString` /
  /// `WriteInt64AsBulkString`：应答面把整型标量按 bulk 串交付的命令（VINFO 的
  /// 维度/参数/基数）一律用本变体，不得再 `to_string().into_bytes()` 造临时堆物。
  BulkInt(i64),
  /// 数组。
  Array(Vec<VectorReply>),
  /// 空（NULL）数组：RESP2 `*-1\r\n`、RESP3 `_\r\n`（帧型由 wresp Resp2/Resp3 单点承载）。
  NullArray,
  /// RESP3 映射（RESP2 退化为键值交错的扁平数组）。
  Map(Vec<(VectorReply, VectorReply)>),
  /// RESP3 双精度浮点。
  Double(f64),
  /// 布尔（RESP3）。
  Boolean(bool),
}

impl VectorReply {
  /// 静态错误文案应答（借用零分配）。
  #[inline]
  pub(super) fn err(msg: &'static [u8]) -> Self {
    Self::Error(Cow::Borrowed(msg))
  }

  /// 编码为 RESP2 字节（Double 退化为 bulk 字符串、Map 退化为双倍长度数组）。
  ///
  /// 各臂帧型一律转调 wresp 单点（RespWriter / Resp2 协议面），本枚举仅作
  /// 「数据模型 → 单点」薄壳，不持有第二套帧字节。
  pub fn encode_resp2(&self, out: &mut Vec<u8>) {
    let mut w = RespWriter::new_ref(out);
    match self {
      VectorReply::Simple(s) => w.write_simple_string_bytes(s),
      VectorReply::Error(e) => w.write_error_bytes(e),
      VectorReply::Integer(i) => w.write_int64(*i),
      VectorReply::Bulk(v) => match v {
        Some(v) => w.write_bulk_string(v),
        None => Resp2::write_null(w.buf_mut()),
      },
      // 整型 bulk 臂：帧字节由 wresp 单点（RespWriteUtils.cs:542,565 对位）产出，
      // RESP3 同型（该臂在 encode_resp3 落 `other => encode_resp2` 兜底，无第二份帧）
      VectorReply::BulkInt(i) => w.write_integer_as_bulk_string(*i),
      VectorReply::NullArray => Resp2::write_null_array(w.buf_mut()),
      // RESP2 口径 map 头即双倍长度数组（C# TryWriteMapLength resp2 分支）
      VectorReply::Map(pairs) => {
        w.write_map_length(pairs.len());
        let out = w.buf_mut();
        for (k, v) in pairs {
          k.encode_resp2(out);
          v.encode_resp2(out);
        }
      }
      VectorReply::Double(d) => w.write_double_bulk_string(*d),
      VectorReply::Boolean(b) => {
        // RESP2 面批量串布尔（生产调用点在构造期即分派 Integer 臂，本臂只余
        // 嵌套数组形态）；帧字节由 bulk 单点产出，与 RESP3 的 `#t/#f` 同处枚举分派
        w.write_bulk_string(if *b { b"1" } else { b"0" });
      }
      VectorReply::Array(items) => {
        w.write_array_length(items.len());
        let out = w.buf_mut();
        for item in items {
          item.encode_resp2(out);
        }
      }
    }
  }

  /// 编码为 RESP3 字节（Double 为 `,`、Boolean 为 `#`、Map 为 `%`、
  /// null 族为 `_\r\n`；其余帧与 RESP2 同型，转调 [`Self::encode_resp2`]）。
  pub fn encode_resp3(&self, out: &mut Vec<u8>) {
    let mut w = RespWriter::<_, Resp3>::new_ref_p(out);
    match self {
      VectorReply::Double(d) => w.write_double_numeric(*d),
      VectorReply::Boolean(b) => Resp3::write_bool(w.buf_mut(), *b),
      VectorReply::Map(pairs) => {
        w.write_map_length(pairs.len());
        let out = w.buf_mut();
        for (k, v) in pairs {
          k.encode_resp3(out);
          v.encode_resp3(out);
        }
      }
      VectorReply::Array(items) => {
        w.write_array_length(items.len());
        let out = w.buf_mut();
        for item in items {
          item.encode_resp3(out);
        }
      }
      VectorReply::Bulk(None) => Resp3::write_null(w.buf_mut()),
      VectorReply::NullArray => Resp3::write_null_array(w.buf_mut()),
      other => other.encode_resp2(w.buf_mut()),
    }
  }

  /// 统一按协议版本编码为 RESP 字节
  #[inline]
  pub fn encode_resp(&self, out: &mut Vec<u8>, resp3: bool) {
    if resp3 {
      self.encode_resp3(out);
    } else {
      self.encode_resp2(out);
    }
  }
}

// ======================== 命令入口解析骨架 ========================

/// 真值应答：RESP3 布尔 / RESP2 整数 1·0（对齐 C# 各命令的 resp3 分派）。
#[inline]
pub(super) fn bool_reply(value: bool, resp3: bool) -> VectorReply {
  if resp3 {
    VectorReply::Boolean(value)
  } else {
    VectorReply::Integer(i64::from(value))
  }
}

use crate::{
  resp::vector::vector_store_callbacks::WedbVectorStoreCallbacks,
  storage::session::{
    common::{
      TagRead, read_tag_sync,
      ttl_sync::{probe_alive_domain, purge_expired_residue_sync},
    },
    storage_session::StorageSession,
  },
};

/// 向量集命令前置守卫裁决（快路径同步探针三态，[`RespServerSessionVectors::vector_key_guard`] 产出）
pub enum VectorGuardVerdict {
  /// 键驻留非向量值域：回 WRONGTYPE（对齐 C# res==WRONGTYPE 分支）
  Reject(VectorReply),
  /// 值域双缺：放行走登记表命令面
  Allow,
  /// 磁盘候选待裁决 / 存储错误：同步段读不准。只读命令由 exec 转慢路径
  /// [`RespServerSessionVectors::network_vector_read_slow`] 真读裁决；写命令保守拒（取舍登记
  /// doc/zh/deviations.md §22）
  Degrade,
}

/// Vector Set 命令处理（会话承接层）。
pub struct RespServerSessionVectors<
  S: StoreCallbacks = WedbVectorStoreCallbacks<wdev::SegmentedDevice>,
> {
  /// 向量集合管理器。
  pub manager: Arc<VectorManager<S>>,
}

impl<S: StoreCallbacks> RespServerSessionVectors<S> {
  /// 创建命令处理层。
  pub fn new(manager: Arc<VectorManager<S>>) -> Self {
    Self { manager }
  }

  /// 向量集命令前置守卫判据（exec 向量分支单点；C# 无对位函数——C# 各
  /// NetworkV* 经 VectorManager.Locking.cs:ReadVectorIndexCore 的
  /// Read_MainStore 真读落盘裁决后 res 三态就地分派，rust 快路径以同步探针
  /// 承接同一判据、读不准态转 [`RespServerSessionVectors::network_vector_read_slow`] 闭环）
  ///
  /// 键驻留 wkv 值域（string / 对象信封，或 RangeIndex 专属的 Meta 元记录
  /// 域）即 Reject，杜绝与既有非向量键并行建向量集产生双域键（C# 判定为主
  /// 存记录 RecordType 非向量集；rust 索引记录驻留域内登记表，wkv 域命中即
  /// 非向量键）。磁盘候选待裁决与存储错误同步段读不准交 Degrade——写命令由
  /// exec 按保守拒处置（误拒可 DEL 后重试，双域键一经写即成幽灵，取舍登记
  /// doc/zh/deviations.md §22），只读命令降级异步真读
  pub fn vector_key_guard<'a, D: Device>(
    &self,
    key: &[u8],
    store: &wkv::BatchStoreSession<'a, D>,
  ) -> VectorGuardVerdict {
    match probe_alive_domain(store, key) {
      // 存活命中非向量值域：错型事实确证
      Ok(Some(Some(_))) => VectorGuardVerdict::Reject(self.wrong_type_reply()),
      // 值域双缺：再探 Meta 元记录域（RangeIndex 单树专属物理域）
      Ok(Some(None)) => match read_tag_sync(store, key, KeyTag::Meta, |_| ()) {
        Ok(TagRead::Missing) => VectorGuardVerdict::Allow,
        // Meta 内存命中 = RI / 升阶记录在驻，非向量事实确证
        Ok(TagRead::Hit(())) => VectorGuardVerdict::Reject(self.wrong_type_reply()),
        // Meta 磁盘候选 / 存储错误：与首层同款读不准降级
        Ok(TagRead::Deferred) | Err(_) => VectorGuardVerdict::Degrade,
      },
      // 磁盘候选待裁决 / 存储错误：读不准降级
      Ok(None) | Err(_) => VectorGuardVerdict::Degrade,
    }
  }

  /// 守卫 WRONGTYPE 应答帧（文案单点：本仓向量族统一无句点版，同 C#
  /// RespServerSessionVectors.cs:AbortVectorSetWrongType 的无句点字面量；
  /// C# 五命令臂间另发带句点 CmdStrings.RESP_ERR_WRONG_TYPE 系上游混乱，
  /// 本仓不跟随分流，裁决见 doc/zh/deviations.md §164）
  pub(crate) fn wrong_type_reply(&self) -> VectorReply {
    VectorReply::err(ERR_VECTOR_SET_WRONG_TYPE)
  }

  /// 只读向量命令冷态降级裁决与应答（慢路径承接；对标 C# 各 NetworkV* 的
  /// res 三态分派——Read_MainStore 真读落盘裁决后 WRONGTYPE / NOTFOUND 族
  /// 就地应答，RespServerSessionVectors.cs:905-911/1529-1533/1830 等）
  ///
  /// 快路径守卫（[`Self::vector_key_guard`]）报 Degrade（磁盘候选待裁决 /
  /// 存储错误）时由 exec 转投本面：异步三域真读后——存活非向量键回
  /// WRONGTYPE（res==WRONGTYPE 分支），缺失键（冷态墓碑 / 不存在）放行登记
  /// 表 NOTFOUND 族应答（res==NOTFOUND 分支：VSIM/VEMB 空数组、VREM 0、
  /// VGETATTR null、VDIM "ERR Key not found" 等），存储错误回慢路径统一错误帧
  pub async fn network_vector_read_slow<D: Device>(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    args: &[&[u8]],
    resp_version: u8,
    output: &mut Vec<u8>,
  ) {
    let key = args.first().copied().unwrap_or(&[]);
    let resp3 = is_resp3(resp_version);
    match storage.probe_alive_domain(key).await {
      // 真读存活：非向量记录，对齐 C# res==WRONGTYPE
      Ok(Some(_)) => self.wrong_type_reply().encode_resp(output, resp3),
      // 真读缺失：登记表 NOTFOUND 族应答（对齐 C# res==NOTFOUND）
      Ok(None) => {
        // 循环前缀外提（快路径同款）：本执行域 (ns, db) 会话域内寻址登记表
        let prefix = storage.batch.session_prefix();
        let prefix = prefix.as_slice();
        let reply = match cmd {
          RespCommand::Vsim => self.network_vsim(prefix, args, resp3).await,
          RespCommand::Vemb => self.network_vemb(prefix, args).await,
          RespCommand::Vcard => self.network_vcard(prefix, args).await,
          RespCommand::Vdim => self.network_vdim(prefix, args).await,
          RespCommand::Vgetattr => self.network_vgetattr(prefix, args).await,
          RespCommand::Vinfo => self.network_vinfo(prefix, args).await,
          RespCommand::Vismember => self.network_vismember(prefix, args, resp3).await,
          RespCommand::Vlinks => self.network_vlinks(prefix, args).await,
          RespCommand::Vrandmember => self.network_vrandmember(prefix, args).await,
          _ => unreachable!("is_vector_read_command 钉住慢路径分派集"),
        };
        reply.encode_resp(output, resp3);
      }
      // 存储错误：慢路径统一错误帧
      Err(_) => write_error_raw(output, RESP_ERR_SLOW_PATH_STORAGE),
    }
  }

  /// 向量写族（VADD / VSETATTR）冷态降级裁决与应答（慢路径承接；与
  /// [`RespServerSessionVectors::network_vector_read_slow`] 同型，对标 C# 各 NetworkV* 的
  /// res 三态分派——Read_MainStore 真读落盘裁决后 WRONGTYPE 就地应答）
  ///
  /// 快路径守卫（[`Self::vector_key_guard`]）报 Allow 后命令体挂起
  /// SlowWait（同步段 inline_wait 收割移除，插入/属性写链为 compio 存储
  /// 异步操作），由 exec_slow 转投本面：执行前以异步真读复判键域——挂起
  /// 窗口内键可被并发 SET 写入 wkv 值域，真读存活非向量记录即回 WRONGTYPE，
  /// 杜绝挂起拉长竞态窗口后落双域键（取舍方向同 doc/zh/deviations.md §22）。
  /// 注意真读裁决先于参数解析（原同步形态解析错误优先）——仅挂起期键被
  /// 并发覆写的竞态窗口内二者同现时序可见，字节面各分支同源不变。
  ///
  /// `args` 为参数快照（VADD 尾参携 2 字节 LE 库级定槽，对标 INFO 库数上限 /
  /// MSETNX 续跑标记的快照尾参先例；exec_slow 无会话可达面，槽位随调度点
  /// 快照带入），末尾定槽尾参剥除后交 [`Self::network_vadd`]。
  pub async fn network_vector_write_slow<D: Device>(
    &self,
    storage: &StorageSession<'_, D>,
    cmd: RespCommand,
    args: &[&[u8]],
    resp_version: u8,
    output: &mut Vec<u8>,
  ) {
    let key = args.first().copied().unwrap_or(&[]);
    let resp3 = is_resp3(resp_version);
    match storage.probe_alive_domain(key).await {
      // 真读存活：非向量记录（挂起期并发覆写），对齐 C# res==WRONGTYPE
      Ok(Some(_)) => self.wrong_type_reply().encode_resp(output, resp3),
      // 真读缺失：值域双缺维持 Allow 裁决，写族命令体闭环
      Ok(None) => {
        // 循环前缀外提（快/只读慢路径同款）：本执行域 (ns, db) 会话域内
        // 寻址登记表
        let prefix = storage.batch.session_prefix();
        let prefix = prefix.as_slice();
        let reply = match cmd {
          RespCommand::Vadd => {
            // VADD 登记创建位过期残留清退·异步承接（案二 zcode-r151c-exwatch，
            // 快臂见 garnet_api exec Allow 分支）：同源 helper 重跑（exec 段
            // 已闭环形回 Pass 零写，幂等）；失闩/磁盘候选/页翻转等不可闭环
            // 形在**建籍态**（登记表 miss）经统一 delete 级联窗内闭环——残留
            // TTL 与判死值记录物理清退、bump 与 VADD 自身合并同命令内，恒先
            // 于任何后续 WATCH 登记；已建籍键按 §75（登记表无过期刻度）无
            // 残留复合态，降级仅系桶闩交叠，零副作用放行，绝不触缺席清退
            // 钩子（delete_vector_set 整集删除，误触即毁集）
            if !purge_expired_residue_sync(&storage.batch, key).unwrap_or(false)
              && self.manager.read_stored_index(prefix, key).is_none()
              && storage.batch.delete(key).await.is_err()
            {
              write_error_raw(output, RESP_ERR_SLOW_PATH_STORAGE);
              return;
            }
            // 尾参剥除定槽（兜底 0 = 根域单库语义，分派集钉住 VADD 快照
            // 至少 4 参 + 尾槽，正常路径恒走 and_then 命中）
            let slot = args
              .last()
              .and_then(|a| <[u8; 2]>::try_from(*a).ok())
              .map(u16::from_le_bytes)
              .unwrap_or(0);
            self
              .network_vadd(prefix, &args[..args.len() - 1], slot, resp3)
              .await
          }
          RespCommand::Vsetattr => self.network_vsetattr(prefix, args, resp3).await,
          // VREM 挂起化（元素删除为存储异步回调，登记写透 async 化后快
          // 路径不再承接）归本臂真读复判后闭环
          RespCommand::Vrem => self.network_vrem(prefix, args).await,
          _ => unreachable!("is_vector_set_command 钉住写族慢路径分派集"),
        };
        reply.encode_resp(output, resp3);
      }
      // 存储错误：慢路径统一错误帧
      Err(_) => write_error_raw(output, RESP_ERR_SLOW_PATH_STORAGE),
    }
  }

  /// Vector Set 预览未启用的统一拒绝。
  fn abort_disabled(&self) -> VectorReply {
    VectorReply::err(ERR_VECTOR_SET_DISABLED)
  }

  /// 命令入口骨架守卫：预览未启用 → 统一拒绝；参数个数不在 `len` 区间 →
  /// `bad` 文案。返回 Some(应答) 即入口拒止（各命令合法参数个数区间逐一对
  /// 齐 C# 各 NetworkV*）。
  #[inline]
  pub(super) fn entry(
    &self,
    args: &[&[u8]],
    len: impl RangeBounds<usize>,
    bad: &'static [u8],
  ) -> Option<VectorReply> {
    if !self.manager.is_enabled() {
      return Some(self.abort_disabled());
    }
    (!len.contains(&args.len())).then(|| VectorReply::err(bad))
  }

  /// OK 后合成写注入 AOF 的统一失败口径（对标 C# 各 VectorStoreOps 的
  /// ReplicateVectorSet* 失败臂：记日志并回服务错误帧）。
  #[inline]
  pub(super) fn aof_failed<E: fmt::Display>(
    &self,
    op: &str,
    res: Result<(), E>,
  ) -> Option<VectorReply> {
    res
      .map_err(|e| {
        log::error!("network_{op}: 向量 AOF 合成写入队失败: {e}");
        VectorReply::err(ERR_VECTOR_SERVICE_RESPONSE)
      })
      .err()
  }
}

// ======================== 检索结果序列化 ========================

/// 检索输出上限：有位图时 popcount，否则全部命中，再与 count 取小（C# 同款）。
#[inline]
fn output_limit(total_found: usize, filter_bitmap: &[u8], count: usize) -> usize {
  if filter_bitmap.is_empty() {
    return total_found.min(count);
  }
  filter_bitmap
    .iter()
    .map(|b| b.count_ones() as usize)
    .sum::<usize>()
    .min(count)
}

/// 过滤通过项下标序列（RESP2/RESP3 输出共用骨架）：零压实契约下按原结果
/// 下标剔除未过项、至多产出 `limit` 项（与逐位 `break`/`continue` 手写循环同序同集）。
fn passed_indices(
  total_found: usize,
  filter_bitmap: &[u8],
  limit: usize,
) -> impl Iterator<Item = usize> {
  let has_filter = !filter_bitmap.is_empty();
  (0..total_found)
    .filter(move |&i| !has_filter || (filter_bitmap[i >> 3] >> (i & 7)) & 1 != 0)
    .take(limit)
}

impl RespServerSessionVectors {
  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:WriteRESP3Result
  ///
  /// RESP3：无 score/attr 时为普通数组；否则为 map（id → score/attr/[score, attr]），
  /// 过滤未过项剔除，空属性写 NULL。
  pub fn write_resp3_result(
    count: usize,
    ids: &[&[u8]],
    distances: &[f32],
    filter_bitmap: &[u8],
    attributes: Option<&[&[u8]]>,
    with_scores: bool,
  ) -> VectorReply {
    let with_attribs = attributes.is_some();
    // C#：有位图时输出上限 = popcount(bitmap)，否则全部命中；再与 count 取小
    let total_found = ids.len();
    let output_count = output_limit(total_found, filter_bitmap, count);
    // 扁平数组与 map 两形态互斥（`!flat` 即原 `with_scores || with_attribs`），
    // 任一时刻仅一路产出：预容量按该路布尔折算（另一路恒 0 容量、不分配）
    let flat = !with_scores && !with_attribs;
    let mut plain = Vec::with_capacity(usize::from(flat) * output_count);
    let mut map = Vec::with_capacity(usize::from(!flat) * output_count);
    for result_index in passed_indices(total_found, filter_bitmap, output_count) {
      let id = ids[result_index];
      let score = VectorReply::Double(f64::from(
        distances.get(result_index).copied().unwrap_or(0.0),
      ));
      let attr_reply = match attributes
        .and_then(|attrs| attrs.get(result_index))
        .copied()
      {
        // 命中属性源为函数局部检索缓冲，应答需拥有数据（仅此处落堆）
        Some(a) if !a.is_empty() => VectorReply::Bulk(Some(a.to_vec().into())),
        // RESP3：空属性写 NULL
        _ => VectorReply::Bulk(None),
      };
      if flat {
        plain.push(VectorReply::Bulk(Some(id.to_vec().into())));
      } else {
        // 分数与属性齐备时以二元素数组为 map 值（顺序：score → attr）
        let value = if with_scores && with_attribs {
          VectorReply::Array(vec![score, attr_reply])
        } else if with_scores {
          score
        } else {
          attr_reply
        };
        map.push((VectorReply::Bulk(Some(id.to_vec().into())), value));
      }
    }
    if flat {
      VectorReply::Array(plain)
    } else {
      VectorReply::Map(map)
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:WriteRESP2Result
  ///
  /// RESP2：扁平数组（WITHSCORES 时 id/score 成对；WITHATTRIBS 附加属性；
  /// 二者齐备时长度为三倍），空属性写空 bulk 字符串。
  pub fn write_resp2_result(
    count: usize,
    ids: &[&[u8]],
    distances: &[f32],
    filter_bitmap: &[u8],
    attributes: Option<&[&[u8]]>,
    with_scores: bool,
  ) -> VectorReply {
    let with_attribs = attributes.is_some();
    let total_found = ids.len();
    let output_count = output_limit(total_found, filter_bitmap, count);

    let multiplier = 1 + usize::from(with_scores) + usize::from(with_attribs);
    let mut items = Vec::with_capacity(output_count * multiplier);
    for result_index in passed_indices(total_found, filter_bitmap, output_count) {
      let id = ids[result_index];
      // 命中 id/属性源为函数局部检索缓冲，应答需拥有数据（仅此处落堆）
      items.push(VectorReply::Bulk(Some(id.to_vec().into())));
      if with_scores {
        items.push(VectorReply::Double(f64::from(
          distances.get(result_index).copied().unwrap_or(0.0),
        )));
      }
      if with_attribs && let Some(attr) = attributes.and_then(|attrs| attrs.get(result_index)) {
        items.push(VectorReply::Bulk(Some(attr.to_vec().into())));
      }
    }
    VectorReply::Array(items)
  }
}
