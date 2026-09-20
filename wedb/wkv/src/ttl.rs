//! 两套过期载体总览与判定单点收敛
//!
//! # 架构背景与 C# 对标差异
//!
//! C# Garnet 采用变长 `LogRecord` 布局，其 `RecordDataHeader` 内嵌 `HasExpiration` 与
//! `HasETag` 标志位，过期绝对时间（.NET Ticks）直接内联于数据记录物理尾部。读判定单点在
//! `Storage/Functions/MainStore/ReadMethods.cs` 与 `PrivateMethods.cs:EvaluateExpire`
//! （经 `LogRecordUtils.cs:CheckExpiry` 判定 `expiration < nowTicks`）。
//!
//! Rust wedb 为保证 CPU 缓存行对齐与零堆分配开销，底层采用 16B 定长紧凑头（`RecordHeader`），
//! 无变长内联可选标志位。系统在架构层面收敛为两套定位互补、各具 C# 对标的过期载体：
//!
//! 1. **键级旁路记录（Key-Level Out-of-Band Record）**：
//!    - 物理载体：独立标签物理键 `KeyTag::Ttl`（`[NsVarint] + [DbVarint] + 0x09 + [用户键]`），
//!      记录值为定长 8B 大端 .NET Ticks（见 [`wval::I64Codec`]）。
//!    - 读门控单点：[`StoreSession::probe_alive`] 与 [`StoreSession::ttl_gate_mem_at`]，
//!      单次哈希探针初筛（无 TTL 记录零 I/O 放行），到期则闭环 NOTFOUND 语义；
//!    - 写与更新：EXPIRE/PEXPIRE 原位改写优先，失败降级 RCU 盲插；
//!    - 主动清理与紧缩：后台 GC（[`crate::gc::GcManager`]）热区滑动窗口 + 冷区欠账游标增量扫描
//!      双检清除，日志在线紧缩（[`crate::compact`]）将过期 TTL 及孤儿记录判死丢弃。
//!
//! 2. **对象成员级过期堆（In-Object Expiration Heap）**：
//!    - 物理载体：集合对象（Hash / SortedSet）内部内联的优先队列
//!      （`wcol::types::expiration_queue::ExpirationQueue`，基于 `BinaryHeap`）；
//!    - C# 对标：`garnet/libs/server/Objects/Hash/HashObject.cs` 与
//!      `SortedSet/SortedSetObject.cs` 内部的 `PriorityQueue<byte[], long>`；
//!    - 判定与淘汰：对象成员读写或迭代时惰性淘汰，并在集合生命周期内闭环管理。
//!
//! C# 侧对象字段级过期另有后台周期收集（`libs/server/StoreWrapper.cs:ObjectCollectTaskAsync`
//! 周期驱动 `storageSession.HashCollect` 等）；rust 的收集宿主在 wnode
//! （`object_collect_all` / 分层到期重灌执行体共用唯一内核），
//! 调度面同样不在本引擎（wkv 只提供 Meta 元记录随帧回写通道）：信封域以上述第 2 套
//! 载体承载（读路径惰性淘汰 + HCOLLECT/ZCOLLECT 闭环清理），分层态树记录由
//! `wcol::types::member_ttl` 编码 8B 过期刻度，计数臂校正 / 显式与周期收集物理出账
//! （见 [`crate::gc`] 模块文档）。
//!
//! # 统一过期判定单点
//!
//! 尽管两套载体物理形态各异，但所有过期时间的到期判定算法统一收敛到本模块的
//! 两个纯函数，通过 [`TtlCarrier`] trait，无论是原始切片 `&[u8]`（自动经
//! `I64Codec::decode` 解码）、解码后的 `i64`、`Option<i64>` 还是其引用，均复用
//! 同一套判定逻辑，彻底消除 `gc.rs`、`compact.rs`、读门控模块与 EXPIRE 写路径中
//! 各自手写的字节切片校验与时间比对代码：
//! - [`is_expired`]（严格小于：`exp < now_ticks`，对标 C#
//!   `LogRecordUtils.cs:CheckExpiry`）：读路径与惰性过期统一口径；
//! - [`is_expired_or_now`]（含相等：`exp <= now_ticks`）：EXPIRE 族过去时间戳
//!   立即删除的写路径口径——C# 落库过去值后由惰性过期（严格小于）随后清理，
//!   rust 直接物理删除，等号时刻键已不可见，含相等保证同刻落库同刻清除。
//!
//! 时间戳编解码与换算同样维持严格单点：
//! - 8B 大端 Ticks 编解码：单点收口于 `wval::I64Codec`；
//! - Unix 毫秒/秒与 .NET Ticks 互转：单点收口于 `wbase::convert`；
//! - 键级过期 4-bit coarse 粗化（值域裁决）：单点收口于
//!   `wbase::convert::coarse_expire_ticks`，且只在两个入口各施加一次——
//!   同步快路径 wnode `network_expire` 命令边界、异步会话入口
//!   [`StoreSession::expire_at`] 头部（C# 粗化仅是 EXPIRE 族把 ExpireOption
//!   打包进低 4 位的产物，字段级随打包在 `wresp::ExpirationWithOption`；
//!   SET/GETEX/RENAME 族 C# 走裸 ticks，禁止粗化）；TTL 写内核
//!   [`StoreSession::put_ttl`] 与 `ttl_sync::put_ttl_sync` 一律裸写不判。

use std::sync::{
  Arc,
  atomic::{AtomicUsize, Ordering::Relaxed},
};

use wbase::{
  convert::{
    coarse_expire_ticks, milliseconds_from_diff_ticks, unix_time_in_milliseconds_from_ticks,
  },
  time::now_ticks,
};
use wdev::Device;
use windex::{Error as IndexError, HashIndex};
use wval::{I64_VAL_LEN, I64Codec, KeyTag, TaggedKeyBuf};

use crate::{
  error::Result,
  session::{StoreResult, StoreSession},
  store::StoreEvent,
};

/// 过期时间载体抽象：统一提取绝对过期 .NET Ticks
pub trait TtlCarrier {
  /// 提取绝对过期 .NET Ticks（None 表示无 TTL 记录、墓碑或数据长度非法）
  fn expire_at_ticks(&self) -> Option<i64>;
}

impl TtlCarrier for i64 {
  #[inline(always)]
  fn expire_at_ticks(&self) -> Option<i64> {
    Some(*self)
  }
}

impl<T: TtlCarrier> TtlCarrier for Option<T> {
  #[inline(always)]
  fn expire_at_ticks(&self) -> Option<i64> {
    self.as_ref().and_then(TtlCarrier::expire_at_ticks)
  }
}

impl TtlCarrier for [u8] {
  #[inline(always)]
  fn expire_at_ticks(&self) -> Option<i64> {
    I64Codec::decode(self)
  }
}

impl<const N: usize> TtlCarrier for [u8; N] {
  #[inline(always)]
  fn expire_at_ticks(&self) -> Option<i64> {
    self.as_slice().expire_at_ticks()
  }
}

impl<T: TtlCarrier + ?Sized> TtlCarrier for &T {
  #[inline(always)]
  fn expire_at_ticks(&self) -> Option<i64> {
    (**self).expire_at_ticks()
  }
}

/// 统一判定载体是否已过期（严格小于判定基准，对标 C# LogRecordUtils.cs:20 读路径口径）
///
/// - `Some(exp) if exp < now_ticks` -> true（已过期）
/// - `Some(exp) if exp >= now_ticks` -> false（未过期）
/// - `None` -> false（无 TTL / 墓碑 / 非法长度）
#[inline(always)]
pub fn is_expired(carrier: impl TtlCarrier, now_ticks: i64) -> bool {
  carrier.expire_at_ticks().is_some_and(|exp| exp < now_ticks)
}

/// 统一判定载体过期时刻是否已到（含相等：`exp <= now_ticks`，EXPIRE 族过去
/// 时间戳立即删除的写路径口径）
///
/// 读路径判定统一走 [`is_expired`]（严格小于）；本谓词仅供 EXPIRE 族写路径
/// 判定"给定时刻不是未来"用——过期时刻 <= 当前即立即物理删除，与 C# 落库
/// 过去值后经惰性过期（严格小于）清理的终态等价，含相等保证同刻时刻同刻清除
///
/// - `Some(exp) if exp <= now_ticks` -> true（立即删除）
/// - `Some(exp) if exp > now_ticks` -> false（正常落 TTL 记录）
/// - `None` -> false（无 TTL / 墓碑 / 非法长度）
#[inline(always)]
pub fn is_expired_or_now(carrier: impl TtlCarrier, now_ticks: i64) -> bool {
  carrier
    .expire_at_ticks()
    .is_some_and(|exp| exp <= now_ticks)
}

/// 过期写选项（Redis EXPIRE NX/XX/GT/LT，风格对齐 wedb_hash::ExpireOpt）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TtlOpt {
  /// 仅当未设置过期时设置 (NX)
  pub nx: bool,
  /// 仅当已设置过期时设置 (XX)
  pub xx: bool,
  /// 仅当新过期时间大于当前过期时间时设置 (GT)
  pub gt: bool,
  /// 仅当新过期时间小于当前过期时间时设置 (LT)
  pub lt: bool,
}

impl TtlOpt {
  /// 无附加条件（普通 EXPIRE）
  pub const NONE: Self = Self {
    nx: false,
    xx: false,
    gt: false,
    lt: false,
  };
}

/// TTL 内存门裁决三态（全系统单点定义，对标 C# LogRecordUtils.cs:CheckExpiry
/// 在读路径内的内联判定形态：无 TTL / 未到期放行，已到期读失败 NOTFOUND，
/// 磁盘候选降级异步）。异步读/写门控、同步批处理裁决（node 侧 ttl_sync）与
/// 批量回调探针（[`StoreSession::probe_ttl`]）共用同一组臂
pub enum TtlGate {
  /// 无 TTL 记录或未到期：放行内存直读
  Pass,
  /// 已到期：快路径直接按 NOTFOUND 闭环（物理清理留写路径惰性清退与后台 GC）
  Due,
  /// TTL 记录存在磁盘候选：降级异步裁决（批量回调探针场景含内存探针异常出口）
  Degrade,
}

/// purge 链物理写镜像抑制守卫（RAII save/restore，panic/早退路径 Drop 兜底）
///
/// 进入时以 `swap` 把会话身份写入 [`WedbStore::purge_suppress`] 抑制槽，Drop 时
/// 无条件恢复进入前的旧值：嵌套 purge（已被"先删 TTL 记录"递归防护杜绝，此处
/// 纵深防御）与 unwind 路径均不会让标志残留——普通"置位 + Drop 清零"在嵌套场景
/// 会提前解抑制，save/restore 语义天然免疫
struct PurgeNotifyGuard<'a> {
  slot: &'a AtomicUsize,
  prev: usize,
}

impl<'a> PurgeNotifyGuard<'a> {
  /// 进入抑制窗口：`token` 为会话身份（&StoreSession 裸地址）
  #[inline]
  fn enter(slot: &'a AtomicUsize, token: usize) -> Self {
    let prev = slot.swap(token, Relaxed);
    Self { slot, prev }
  }
}

impl Drop for PurgeNotifyGuard<'_> {
  #[inline]
  fn drop(&mut self) {
    self.slot.store(self.prev, Relaxed);
  }
}

impl<D: Device> StoreSession<D> {
  /// 生成当前会话专属 key 级 TTL 记录物理键 (KeyTag::Ttl)
  ///
  /// 复用标签物理键单点 [`StoreSession::session_tag_key`]：`[NsVarint] + [DbVarint] + 0x09 + [用户键]`
  #[inline(always)]
  pub fn ttl_key(&self, user_key: &[u8]) -> TaggedKeyBuf {
    self.session_tag_key(KeyTag::Ttl, user_key)
  }

  /// 显式前缀编码 key 级 TTL 记录物理键（循环前缀外提对位，复用
  /// [`StoreSession::session_tag_key_with_prefix`] 单点；rust 工程优化无 c# 对应）
  #[inline(always)]
  pub fn ttl_key_with_prefix(prefix: &[u8], user_key: &[u8]) -> TaggedKeyBuf {
    Self::session_tag_key_with_prefix(prefix, KeyTag::Ttl, user_key)
  }

  /// 快速探测 TTL 记录标签是否存在于哈希索引（纯内存单次哈希探针，无记录 I/O）
  #[inline(always)]
  pub(crate) fn has_ttl_key(&self, ttl_k: &TaggedKeyBuf) -> Result<bool> {
    let _guard = self.enter_gated();
    self.has_ttl_key_unprotected(ttl_k)
  }

  /// 在已有纪元保护下快速探测 TTL 记录标签是否存在（彻底绕过 enter() 原子开销）
  #[inline(always)]
  pub(crate) fn has_ttl_key_unprotected(&self, ttl_k: &TaggedKeyBuf) -> Result<bool> {
    let index = self.store.index.load();
    let old_index = self.store.resize.old_index.load();
    let hash = HashIndex::hash_key(ttl_k);
    if old_index
      .as_ref()
      .is_some_and(|old| !Arc::ptr_eq(old, &index))
    {
      self.store.split_buckets(hash)?;
    }
    Ok(index.find_tag_by_hash(hash).is_some())
  }

  /// 快速探测用户键的 TTL 记录标签是否存在于哈希索引（纯内存单次哈希探针，无记录 I/O）
  #[inline]
  pub fn has_ttl_tag(&self, user_key: &[u8]) -> Result<bool> {
    let ttl_k = self.ttl_key(user_key);
    self.has_ttl_key(&ttl_k)
  }

  /// 读取 TTL 记录的绝对过期 .NET Ticks（None = 无 TTL；记录值非法长度亦按无 TTL 容错）
  pub async fn ttl_of(&self, user_key: &[u8]) -> Result<Option<i64>> {
    let ttl_k = self.ttl_key(user_key);
    self.read_i64_sidecar(&ttl_k).await
  }

  /// I64 值旁路记录读取单点（键已编码，与标签无关；TTL 与 ETag 两域共用）
  ///
  /// 记录不存在 / 墓碑 / 值非法长度一律 None 容错；ttl_of、etag_of 与紧缩会话
  /// 的 read_ttl_expiry 皆经此处，杜绝各自重抄 `read_raw_with(.., I64Codec::decode)`
  pub(crate) async fn read_i64_sidecar(&self, key: &[u8]) -> Result<Option<i64>> {
    Ok(self.read_raw_with(key, I64Codec::decode).await?.flatten())
  }

  /// 写入 TTL 记录（定长 8B .NET Ticks，经 I64 旁路写内核 [`Self::put_i64_sidecar`]）
  ///
  /// 裸写内核（对标 C# MainStore/RMWMethods.cs:TrySetExpiration 收
  /// `input.arg1` 裸 ticks）：不粗化、不判值域——4-bit coarse 粗化是值域裁决，
  /// 只发生在两个入口：同步快路径 wnode `network_expire` 命令边界、异步会话
  /// 入口 [`Self::expire_at`] 头部（单点函数 `wbase::convert::coarse_expire_ticks`）；
  /// 与同步臂 `ttl_sync::put_ttl_sync` 同构，杜绝内核再长出第二道掩码
  pub async fn put_ttl(&self, user_key: &[u8], expire_at_ticks: i64) -> Result<()> {
    let ttl_k = self.ttl_key(user_key);
    self.put_i64_sidecar(&ttl_k, expire_at_ticks).await
  }

  /// I64 值旁路记录写入单点（键已编码，与标签无关；TTL 与 ETag 两域共用）
  ///
  /// 可变区原位改写优先、失败降级 RCU 盲插：原位改写零追加、零哈希表 CAS，是
  /// EXPIRE 反复续期与 SETWITHETAG 反复推进 etag 的主路径；记录已落盘或复活槽位
  /// 不适配时，upsert 写路径自身含链内复活/盲插兜底，无需在此重复处理。原位改写
  /// 走 raw unprotected 内核，本异步入口须自带纪元保护（对标 C# Tsavorite：一切
  /// 索引/日志访问都必须在 epoch 保护下执行，否则并发驱逐可在改写窗口内回收页内存）。
  /// 长度守卫：现存值非定长 8B（外部经 upsert_raw 误写或损坏日志）时放弃原位改写，
  /// 降级 RCU 重写自愈——杜绝 copy_from_slice 长度失配 panic
  pub(crate) async fn put_i64_sidecar(&self, key: &[u8], val: i64) -> Result<()> {
    let bytes = I64Codec::encode(val);
    let in_place = {
      let _guard = self.enter_gated();
      self
        .try_modify_raw_in_place_unprotected(key, |slot| {
          if slot.len() != I64_VAL_LEN {
            return None;
          }
          slot.copy_from_slice(&bytes);
          Some(())
        })?
        .is_some()
    };
    if !in_place {
      self.upsert_raw(key, &bytes).await?;
    }
    Ok(())
  }

  /// 删除 TTL 记录（先单次哈希探针初筛：绝大多数无 TTL 键零额外写入与零索引污染）
  ///
  /// 供 wnode 键管理慢路径（RENAME Meta 域臂的目标键旧 TTL 清退）与内核
  /// 删除/迁移路径共用；无记录时零写零入账（幂等）
  pub async fn del_ttl(&self, user_key: &[u8]) -> Result<()> {
    let ttl_k = self.ttl_key(user_key);
    if self.has_ttl_key(&ttl_k)? {
      self.delete_raw(&ttl_k).await?;
    }
    Ok(())
  }

  /// 物理清除已过期键（数据 + TTL 记录，走统一 DEL 路径保证索引/墓碑/WAL 钩子一致）
  ///
  /// delete 内部首部已级联清理 del_ttl 与 del_etag，天然杜绝递归（delete 内部
  /// load_meta 的 TTL 守卫探测不到记录而不触发二次清除）。
  ///
  /// AOF 单条化（对标 Garnet `RespInputFlags.Deterministic` 携带绝对过期时间的
  /// 单条确定性逻辑条目语义）：purge 端口在场时，链内物理写（TTL 记录墓碑 +
  /// 数据墓碑两条）的写监听镜像经会话级抑制槽精确跳过（仅本会话，其他会话并发
  /// 写不受影响），链成功闭环后触发一次端口，携带
  /// `(ns, db, 用户键, expire_at_ticks)`；端口未注册时保持现状两条物理条目
  /// （嵌入式无 AOF 场景行为不变）。链中途失败（`?` 早退）守卫即时解抑制且
  /// 不触发端口——清除未闭环不得宣告过期。
  pub(crate) async fn purge_expired(&self, user_key: &[u8], expire_at_ticks: i64) -> Result<()> {
    // RENAME 迁移 claim 判点先于一切清退（与 SET 两臂/DEL 两臂同纪律）：本链
    // 自身亦属破坏性动作，判点必须置于其前——claim 在册键的清退
    // 由迁移臂全程接管（RENAME 随迁 TTL、降阶臂换域保留 TTL），此处零副作用
    // 放行，TTL 记录保持原态留待窗后重试（后台扫描逐键双检幂等、读路径按键
    // 惰性重入），杜绝「先删 TTL 记录、数据删除臂被拒」的半程清除形（TTL 记
    // 录已亡而数据存活 = 永生键）。放行后调用方的「已过期」语义仍成立：过期
    // 数据读面视同不存在（expired ≈ NOTFOUND），物理回收仅顺延至窗外
    let meta_k = self.session_meta_key(user_key);
    if self.store.index.load().find_tag(&meta_k).is_some()
      && self.store.range_index.migration_claimed(&meta_k)
    {
      return Ok(());
    }
    let _suppress =
      PurgeNotifyGuard::enter(&self.store.purge_suppress, self as *const Self as usize);
    self.delete(user_key).await?;
    // 先解抑制再触发事件：事件处理器内部的写操作不受抑制窗口影响
    drop(_suppress);
    // 事件域取会话物理域（与被清记录的键前缀同源），副本按条目域直设落回原域
    let (ns, db) = self.virtual_domain();
    self.store.emit_event(StoreEvent::TtlPurge {
      ns,
      db,
      key: user_key,
      expire_at: expire_at_ticks,
    })
  }

  /// 同步内存 TTL 门裁决单点（调用方须已处于纪元保护下；`now` 为 .NET Ticks 判定基准）
  ///
  /// has_ttl 单次哈希探针初筛（无 TTL 记录零额外 I/O 直接放行）+ 单次内存读
  /// TTL 记录比对到期：到期判定严格小于（读路径口径，对标 C# LogRecordUtils.cs:20，
  /// exp == now 未过期）。不触达磁盘：TTL 记录有磁盘候选时返回 [`TtlGate::Degrade`]
  /// 交由调用方降级异步裁决，绝不跨纪元 await
  pub fn ttl_gate_mem_at(&self, user_key: &[u8], now: i64) -> Result<TtlGate> {
    let prefix = self.session_prefix();
    self.ttl_gate_mem_at_with_prefix(prefix.as_slice(), user_key, now)
  }

  /// 显式前缀同步内存 TTL 门裁决单点（循环前缀外提对位，[`Self::ttl_gate_mem_at`]
  /// 语义一致；rust 工程优化无 c# 对应）
  pub fn ttl_gate_mem_at_with_prefix(
    &self,
    prefix: &[u8],
    user_key: &[u8],
    now: i64,
  ) -> Result<TtlGate> {
    let ttl_k = Self::ttl_key_with_prefix(prefix, user_key);
    if !self.has_ttl_key_unprotected(&ttl_k)? {
      return Ok(TtlGate::Pass);
    }
    Ok(
      match self.try_read_raw_in_memory(&ttl_k, I64Codec::decode)? {
        // TTL 记录有磁盘候选：降级异步裁决
        StoreResult::RecordOnDisk => TtlGate::Degrade,
        StoreResult::NotFound => TtlGate::Pass,
        // 内存闭环：墓碑与非法值长度视同无 TTL
        StoreResult::Success(v) => match v {
          Some(exp) if is_expired(exp, now) => TtlGate::Due,
          _ => TtlGate::Pass,
        },
      },
    )
  }

  /// 同步内存 TTL 三态探针（GC 热区扫描与 SCAN 族键遍历批量回调用，不触达磁盘，不传播错误）
  ///
  /// [`Self::ttl_gate_mem_at`] 的无错误出口对位：内存探针异常与磁盘候选统一归并
  /// [`TtlGate::Degrade`]（调用方降级异步裁决，check_expired 内含完整磁盘路径与
  /// 错误传播）。`now` 为 .NET Ticks 判定基准（与 TTL 记录值同域）
  pub fn probe_ttl(&self, user_key: &[u8], now: i64) -> TtlGate {
    match self.ttl_gate_mem_at(user_key, now) {
      Err(_) => TtlGate::Degrade,
      Ok(gate) => gate,
    }
  }

  /// 惰性过期裁决 + TTL 快门控单点（对标 C# SessionFunctionsUtils 过期判定单点形态）
  ///
  /// `has_ttl_tag` 单次哈希探针快门控（无 TTL 记录零额外 I/O 直接放行）+ 零成本短路
  /// 后才进入 `check_expired` 完整裁决（含磁盘路径与 purge_expired 物理清除）。
  /// 返回 true = 键存活（无 TTL 或未到期）；false = 已过期且已物理清除（读入口视同不存在）。
  ///
  /// 读入口统一接线（read_tag_with/read_tag_with_size/contains_key/load_meta/
  /// load_range_index_stub/rmw）：本键在整条同步调用链内的 TTL 裁决只在此入口
  /// 做一次，内部裸读（read_raw_with）绝不嵌套二次裁决
  #[inline]
  pub(crate) async fn probe_alive(&self, user_key: &[u8]) -> Result<bool> {
    Ok(!(self.has_ttl_tag(user_key)? && self.check_expired(user_key).await?))
  }

  /// 惰性过期检查：key 已过期则物理删除（数据 + TTL 记录）并返回 true
  ///
  /// 读入口统一接线（read/contains_key/load_meta 等），内部仅经 raw 路径探测 TTL 记录，
  /// 绝不回调这些入口，无递归。约定调用方"先确认 key 存在再探测"（TTL 记录不存在即无
  /// TTL，快路径仅一次哈希探针零额外 I/O）；对不存在键调用亦安全（返回 false）
  pub async fn check_expired(&self, user_key: &[u8]) -> Result<bool> {
    let Some(exp) = self.ttl_of(user_key).await? else {
      return Ok(false);
    };
    // 到期判定取严格小于（统一收敛至 is_expired）
    if !is_expired(exp, now_ticks()) {
      return Ok(false);
    }
    // 装箱打破 async 递归布局环（purge_expired → delete → load_meta → check_expired），
    // 仅在真正过期的冷路径付出一次堆分配
    Box::pin(self.purge_expired(user_key, exp)).await?;
    Ok(true)
  }

  /// 设置 key 绝对过期 .NET Ticks (EXPIREAT / PEXPIREAT 语义；Unix 秒/毫秒 → ticks
  /// 的换算见 `wbase::convert`，对标 garnet/libs/server/Resp/KeyAdminCommands.cs 在
  /// RESP 边界换算成 ticks 再进存储；相对时长由调用方换算)
  ///
  /// 返回码（对齐 Redis 7.4）：-2 key 不存在；0 NX/XX/GT/LT 条件不满足；
  /// 1 成功；2 已过期并立即物理删除。
  ///
  /// 融合遍历（原 3 次压至 2 次）：1 次`Self::contains_key_ignore_ttl`裸数据存活
  /// 判定（剥离 TTL 探测）+ 1 次 TTL 记录读取，后者同时服务「TTL 记录存在且已到期
  /// = 键已过期 = 视同不存在」的存活修正与 NX/XX/GT/LT 条件判定。语义不变式：
  /// - 判序对齐 Redis 与 wedb_hash::hexpire：先键存活、再选项校验、最后过去时间戳
  ///   删除，条件不满足时绝不误删 key；
  /// - TTL 记录不存在不代表键不存在（键可能从未设 TTL），存活判定始终以数据记录为
  ///   准，孤儿 TTL 记录（数据已亡）绝不误判存活；
  /// - TTL 记录已到期的键先 `purge_expired`（先删 TTL 记录再删数据）再返回 -2，
  ///   与 contains_key 的惰性过期口径一致；
  /// - GT/LT 与既有 TTL 同为 ticks 域比较；比较与落盘同用入口粗化后的值，
  ///   判定与写入同源（对标 C# NetworkEXPIRE 打包粗化后存储侧同域评估；
  ///   本函数头是异步/外部入口的唯一粗化门，内核 `put_ttl` 裸写不再二次粗化）；
  /// - 持本键独占桶锁串行化读改写窗口（对齐 hexpire 先例），锁内记录遍历由 3 压至 2
  pub async fn expire_at(&self, user_key: &[u8], expire_at_ticks: i64, opt: TtlOpt) -> Result<i32> {
    // 键级过期异步/外部入口的粗化单点（对标 C# 存储侧 EXPIRE RMW 处理器
    // 无条件重打包 UnifiedStore/RMWMethods.cs:216、:228
    // `new ExpirationWithOption(input.arg1)`，即命令层打包之外的第二处有意
    // 粗化）：AOF 重放、迁移导入、复制应用三类外部 ticks 不经命令层直达
    // 本入口，故在头部裁决值域（外部入口的绝对秒/毫秒换算与存量迁移值
    // 皆 16 对齐，此处对之为幂等恒等），条件判定与落盘同源；同步快路径
    // 的对应粗化在 wnode `network_expire` 命令边界（`coarse_expire_ticks` 同一单点）
    let expire_at_ticks = coarse_expire_ticks(expire_at_ticks);
    let index = self.store.index.load();
    // 本键独占桶闩守卫整个读改写窗口（对标 C# 单键 ephemeral X-lock：一次尝试取闩，
    // 取不到即返回，索引层不自旋不回滚）；取闩失败的 C# RETRY_LATER 语义由本调用方
    // 承接——上浮为锁忙错误交客户端重试，不在索引层造超时
    let Some(_key_lock) = index.try_lock_key_exclusive(user_key) else {
      return Err(IndexError::LockTimeout.into());
    };
    // 遍历 1/2：裸数据存活判定（无 TTL 探测，本键 TTL 裁决统一收敛到遍历 2）
    if !self.contains_key_ignore_ttl(user_key).await? {
      return Ok(-2);
    }
    // 遍历 2/2：单次 TTL 记录读取，同时完成「已过期」存活修正与 NX/XX/GT/LT 判定
    match self.ttl_of(user_key).await? {
      // TTL 记录已到期：惰性过期即视同不存在，物理清除（先删 TTL 再删数据）后 -2
      Some(c) if is_expired(c, now_ticks()) => {
        self.purge_expired(user_key, c).await?;
        Ok(-2)
      }
      // 已设未到期 TTL：NX 禁设、GT/LT 按当前值比较（多项同设须全部满足才放行）
      Some(c) if opt.nx || (opt.gt && expire_at_ticks <= c) || (opt.lt && expire_at_ticks >= c) => {
        Ok(0)
      }
      Some(_) => self.expire_at_apply(user_key, expire_at_ticks).await,
      // 从未设 TTL：XX/GT 无当前值可比，一律不满足
      None if opt.xx || opt.gt => Ok(0),
      None => self.expire_at_apply(user_key, expire_at_ticks).await,
    }
  }

  /// expire_at 尾段公共体：条件全部通过后写 TTL，或过去时间戳立即物理删除
  async fn expire_at_apply(&self, user_key: &[u8], expire_at_ticks: i64) -> Result<i32> {
    // 过去时间戳：立即物理删除（数据 + TTL 记录，先删 TTL 再删数据）。
    // 含相等口径（is_expired_or_now）刻意区别于读路径严格小于：C# 落库过去值
    // 随后由惰性过期清理，rust 直接删除，终态等价
    if is_expired_or_now(expire_at_ticks, now_ticks()) {
      self.purge_expired(user_key, expire_at_ticks).await?;
      return Ok(2);
    }
    self.put_ttl(user_key, expire_at_ticks).await?;
    Ok(1)
  }

  /// 移除 key 的过期时间 (PERSIST)。返回 1=移除成功；0=key 不存在或未设 TTL
  ///
  /// 融合遍历（原 contains_key 内嵌惰性过期裁决 + 独立 ttl_of 共 3 次压至 2 次）：
  /// 1 次`Self::contains_key_ignore_ttl`裸数据存活判定 + 1 次 TTL 记录读取，
  /// 后者同时完成「已到期视同不存在」存活修正与有无 TTL 判定；语义不变式与
  /// [`Self::expire_at`]一致（已到期键先 purge_expired 再返回 0，先删 TTL 再删数据）
  ///
  /// 持本键独占桶锁串行化读改写窗口（与 [`Self::expire_at`] 同款）：无锁时
  /// ttl_of → del_ttl 间隙内并发的 expire_at 可写入新 TTL 而被本命令误删
  /// （EXPIRE 已返回 1 但键无 TTL 的用户可见异常）；锁内记录遍历由 3 压至 2
  pub async fn persist(&self, user_key: &[u8]) -> Result<i32> {
    let index = self.store.index.load();
    // 与 [`Self::expire_at`] 同款单键独占桶闩：一次尝试取闩，失败的 C# RETRY_LATER
    // 语义由本调用方承接为锁忙错误（索引层无自旋、无回滚、无超时）
    let Some(_key_lock) = index.try_lock_key_exclusive(user_key) else {
      return Err(IndexError::LockTimeout.into());
    };
    if !self.contains_key_ignore_ttl(user_key).await? {
      return Ok(0);
    }
    match self.ttl_of(user_key).await? {
      // 从未设 TTL：无可移除
      None => Ok(0),
      // TTL 已到期：键视同不存在，惰性物理清除后返回 0（对齐原 contains_key 口径）
      Some(c) if is_expired(c, now_ticks()) => {
        self.purge_expired(user_key, c).await?;
        Ok(0)
      }
      Some(_) => {
        self.del_ttl(user_key).await?;
        Ok(1)
      }
    }
  }

  /// 查询 key 剩余过期毫秒数 (TTL / PTTL 语义)。-2 无 key；-1 无 TTL；否则 >0
  ///
  /// 内部存储为 .NET Ticks，出参按 garnet/libs/common/ConvertUtils.cs:
  /// MillisecondsFromDiffUtcNowTicks 口径换算为毫秒（已到期差值折叠为 -1）
  pub async fn pttl_ms(&self, user_key: &[u8]) -> Result<i64> {
    if !self.contains_key(user_key).await? {
      return Ok(-2);
    }
    match self.ttl_of(user_key).await? {
      None => Ok(-1),
      Some(exp) => Ok(milliseconds_from_diff_ticks(exp, now_ticks())),
    }
  }

  /// 查询 key 绝对过期 Unix 毫秒时间戳 (EXPIRETIME / PEXPIRETIME 语义。
  /// -2 无 key；-1 无 TTL；返回值仍是 Unix 毫秒语义——内部 .NET Ticks 经
  /// `unix_time_in_milliseconds_from_ticks` 换算)
  pub async fn expiretime_ms(&self, user_key: &[u8]) -> Result<i64> {
    if !self.contains_key(user_key).await? {
      return Ok(-2);
    }
    match self.ttl_of(user_key).await? {
      None => Ok(-1),
      Some(exp) => Ok(unix_time_in_milliseconds_from_ticks(exp)),
    }
  }
}

#[cfg(test)]
mod tests {
  use std::panic::{AssertUnwindSafe, catch_unwind};

  use super::*;

  /// 抑制守卫 RAII 语义：正常退出与 panic unwind 路径均恢复进入前旧值（标志零残留）
  #[test]
  fn purge_notify_guard_restores_on_drop_and_unwind() {
    let slot = AtomicUsize::new(0);

    // 正常作用域退出：恢复 save/restore 链上的旧值
    {
      let _g = PurgeNotifyGuard::enter(&slot, 0x1234);
      assert_eq!(slot.load(Relaxed), 0x1234);
    }
    assert_eq!(slot.load(Relaxed), 0);

    // 嵌套：内层退出恢复外层窗口，而非直接清零
    let outer = PurgeNotifyGuard::enter(&slot, 0xAAAA);
    {
      let _inner = PurgeNotifyGuard::enter(&slot, 0xBBBB);
      assert_eq!(slot.load(Relaxed), 0xBBBB);
    }
    assert_eq!(slot.load(Relaxed), 0xAAAA);
    drop(outer);
    assert_eq!(slot.load(Relaxed), 0);

    // panic unwind：Drop 兜底，抑制标志不残留
    let _ = catch_unwind(AssertUnwindSafe(|| {
      let _g = PurgeNotifyGuard::enter(&slot, 0x5678);
      assert_eq!(slot.load(Relaxed), 0x5678);
      panic!("unwind through guard");
    }));
    assert_eq!(slot.load(Relaxed), 0, "panic 路径不得残留抑制标志");
  }

  /// 统一过期判定单点：严格小于判定基准（exp < now_ticks），多形态载体零分配
  #[test]
  fn test_is_expired_carriers() {
    let now = 10_000_000i64;

    // i64 载体
    assert!(is_expired(now - 1, now));
    assert!(!is_expired(now, now));
    assert!(!is_expired(now + 1, now));

    // &i64 载体
    let past = now - 1;
    let curr = now;
    assert!(is_expired(past, now));
    assert!(!is_expired(curr, now));

    // Option<i64> 载体
    assert!(is_expired(Some(now - 1), now));
    assert!(!is_expired(Some(now), now));
    assert!(!is_expired(Some(now + 1), now));
    assert!(!is_expired(None::<i64>, now));

    // &Option<i64> 载体
    let some_past = Some(now - 1);
    let none_val: Option<i64> = None;
    assert!(is_expired(some_past, now));
    assert!(!is_expired(none_val, now));

    // &[u8] 编码载体
    let exp_bytes = I64Codec::encode(now - 1);
    let not_exp_bytes = I64Codec::encode(now);
    let future_bytes = I64Codec::encode(now + 1);
    assert!(is_expired(&exp_bytes[..], now));
    assert!(!is_expired(&not_exp_bytes[..], now));
    assert!(!is_expired(&future_bytes[..], now));

    // 非法长度切片按无 TTL 容错放行
    assert!(!is_expired(&b""[..], now));
    assert!(!is_expired(&b"123"[..], now));
    assert!(!is_expired(&b"123456789"[..], now));
  }

  /// EXPIRE 写路径口径：含相等即立即删除（exp <= now），与读路径严格小于互补
  #[test]
  fn test_is_expired_or_now_boundary() {
    let now = 10_000_000i64;
    assert!(is_expired_or_now(now - 1, now));
    // 含相等：同刻时刻同刻清除（读路径 is_expired(now, now) 为 false，互补不矛盾）
    assert!(is_expired_or_now(now, now));
    assert!(!is_expired_or_now(now + 1, now));
    assert!(!is_expired_or_now(None::<i64>, now));
  }
}
