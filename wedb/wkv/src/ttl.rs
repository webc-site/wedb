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
//! - Unix 毫秒/秒与 .NET Ticks 互转：单点收口于 `wbase::convert`（GETEX 族
//!   EX/PX/EXAT/PXAT 的 epoch/now 乘加亦归该模块 `try_` checked 臂，引擎与
//!   命令层禁再手写第二份）；
//! - 键级过期 4-bit coarse 粗化（值域裁决）：函数单点收口于
//!   `wbase::convert::coarse_expire_ticks`，且唯 wnode `network_expire` 命令
//!   边界一处施加（对标 C# 粗化仅发生在二参 `ExpirationWithOption` 打包构造器
//!   ExpirationWithOption.cs:20-24，唯 EXPIRE 族调用；字段级随打包在
//!   `wresp::ExpirationWithOption`）。本引擎 [`StoreSession::expire_at`] 会话
//!   入口与 TTL 写内核 [`StoreSession::put_ttl`]、`ttl_sync::put_ttl_sync`
//!   一律恒等裸写不判不移位（对标 C# 存储侧 word 形恒等装载
//!   UnifiedStore/RMWMethods.cs:216、:228，`ExpirationWithOption.cs:30-33`），
//!   SET/GETEX/RENAME 族裸 ticks 经 AOF 重放/迁移导入/复制应用与主端存值逐位
//!   一致（历史头部粗化门已收口，形态登记见 doc/zh/deviations.md §143）。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/ExpirationTests.cs（TTL 门裁决与融合读改写）

use std::sync::atomic::{AtomicBool, Ordering::Relaxed};

use wbase::{
  convert::{milliseconds_from_diff_ticks, unix_time_in_milliseconds_from_ticks},
  time::now_ticks,
};
use wdev::Device;
use whasher::scoped_hash;
use windex::Error as IndexError;
use wval::{I64_VAL_LEN, I64Codec, KeyTag, TaggedKeyBuf};

use crate::{
  error::{Error, Result},
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
/// 进入时对会话私有 [`StoreSession::purge_window`] 窗位 `swap` 置真并记住旧值，
/// Drop 时恢复旧值：窗位归属当前会话自身，跨会话的清退窗交错从构造上互不可达
/// （旧全局槽的恢复值串线与已毁会话地址被新会话复用错杀两臂皆随之消亡）；
/// 同调用栈嵌套 purge 由 save/restore 逐层回卷——内层退出恢复的是外层窗位而非
/// 清零，外层窗不被击穿
struct PurgeNotifyGuard<'a> {
  flag: &'a AtomicBool,
  prev: bool,
}

impl<'a> PurgeNotifyGuard<'a> {
  /// 进入抑制窗口：`flag` 为本会话私有窗位
  #[inline]
  fn enter(flag: &'a AtomicBool) -> Self {
    let prev = flag.swap(true, Relaxed);
    Self { flag, prev }
  }
}

impl Drop for PurgeNotifyGuard<'_> {
  #[inline]
  fn drop(&mut self) {
    self.flag.store(self.prev, Relaxed);
  }
}

/// EXPIRE 族写面共序单源（宏展开为语句序列就地生效：`_key_lock` 归属调用函数体
/// 块，持闩覆盖整个读改写窗直至函数返回，绝不因守卫逃逸提前放闩）：claim 封堵
/// → 本键独占桶闩（对标 C# 单键 ephemeral X-lock，一次尝试取闩，失败的 C#
/// `RETRY_LATER` 由调用方承接为 [`windex::Error::LockTimeout`] 交客户端重试，
/// 索引层不自旋不回滚；scoped 寻桶单点，与 rmw 窗、事务键锁三面同键同桶互斥，
/// 票 wtxn-wkv-keybucket-hash-scope-desync）→ 遍历 1/2 裸数据存活判定（无 TTL
/// 探测，本键 TTL 裁决收敛到调用方遍历 2，缺席回 `$absent`）；判序纪律与各命令
/// 头注同源（见 [`StoreSession::expire_at`]）；首参显式收 `self`——宏体不词法捕获调用方标识符
macro_rules! ttl_write_gate {
  ($ses:ident, $user_key:expr, $absent:expr) => {
    if $ses.migration_claim_busy($user_key)? {
      return Err(Error::MigrationBusy);
    }
    let index = $ses.store.index.load();
    let Some(_key_lock) =
      index.try_lock_key_hash_exclusive(scoped_hash(&$ses.session_prefix(), $user_key))
    else {
      return Err(IndexError::LockTimeout.into());
    };
    if !$ses.contains_key_ignore_ttl($user_key).await? {
      return Ok($absent);
    }
  };
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
    self.has_tag_key(ttl_k)
  }

  /// 在已有纪元保护下快速探测 TTL 记录标签是否存在（彻底绕过 enter() 原子开销）
  #[inline(always)]
  pub(crate) fn has_ttl_key_unprotected(&self, ttl_k: &TaggedKeyBuf) -> Result<bool> {
    self.has_tag_key_unprotected(ttl_k)
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

  /// 显式前缀异步读取 TTL 记录绝对到期 ticks（裸读不写；None = 无 TTL / 墓碑 / 非法长度）
  ///
  /// keyspace_stats 与 is_expired_at 共用本底层读取
  pub async fn raw_ttl_with_prefix(&self, prefix: &[u8], user_key: &[u8]) -> Result<Option<i64>> {
    let ttl_k = Self::ttl_key_with_prefix(prefix, user_key);
    self.read_i64_sidecar(&ttl_k).await
  }

  /// 显式前缀异步读取用户键的 TTL 并判定在指定 ticks 是否已过期（无 TTL 视同未过期）
  pub async fn is_expired_at_with_prefix(
    &self,
    prefix: &[u8],
    user_key: &[u8],
    now: i64,
  ) -> Result<bool> {
    let ttl = self.raw_ttl_with_prefix(prefix, user_key).await?;
    Ok(is_expired(ttl, now))
  }

  /// 当前会话异步读取用户键的 TTL 并判定在指定 ticks 是否已过期（无 TTL 视同未过期）
  pub async fn is_expired_at(&self, user_key: &[u8], now: i64) -> Result<bool> {
    let prefix = self.session_prefix();
    self
      .is_expired_at_with_prefix(prefix.as_slice(), user_key, now)
      .await
  }

  /// 写入 TTL 记录（定长 8B .NET Ticks，经 I64 旁路写内核 [`Self::put_i64_sidecar`]）
  ///
  /// 裸写内核（对标 C# MainStore/RMWMethods.cs:TrySetExpiration 收
  /// `input.arg1` 裸 ticks）：不粗化、不判值域——4-bit coarse 粗化是值域裁决，
  /// 唯 wnode `network_expire` 命令边界一处施加（单点函数
  /// `wbase::convert::coarse_expire_ticks`）；本内核与异步会话入口
  /// [`Self::expire_at`]、同步臂 `ttl_sync::put_ttl_sync` 同为恒等裸写，
  /// 杜绝内核再长出第二道掩码
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
  ///
  /// C# 统一存 RMW 的拷贝臂单点（EXPIRE/PERSIST 原位不适配时 CopyUpdater
  /// 另分配新记录整值重写）：libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:CopyUpdater
  /// ——rust 原位/拷贝分歧收敛为本函数一处的「原位优先、RCU 降级」，不设第二形态
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
  /// 数据墓碑两条）的写监听镜像经会话私有窗位 [`StoreSession::purge_window`]
  /// 跳过（级联写恒经本会话走漏斗；其他会话的清退与写镜像与本案互不相干），
  /// 链成功闭环后触发一次端口，携带
  /// `(ns, db, 用户键, expire_at_ticks)`；端口未注册时保持现状两条物理条目
  /// （嵌入式无 AOF 场景行为不变）。链中途失败（`?` 早退）守卫即时解抑制且
  /// 不触发端口——清除未闭环不得宣告过期。
  pub(crate) async fn purge_expired(&self, user_key: &[u8], expire_at_ticks: i64) -> Result<()> {
    // RENAME 迁移 claim 判点先于一切清退（复合判定经单点 [`Self::migration_claim_busy`]，
    // 与 SET 两臂/DEL 两臂同纪律）：本链自身亦属破坏性动作，判点必须置于其前——
    // claim 在册键的清退由迁移臂全程接管（RENAME 随迁 TTL、降阶臂换域保留 TTL），
    // 此处零副作用放行，TTL 记录保持原态留待窗后重试（后台扫描逐键双检幂等、读
    // 路径按键惰性重入），杜绝「先删 TTL 记录、数据删除臂被拒」的半程清除形
    // （TTL 记录已亡而数据存活 = 永生键）。放行后调用方的「已过期」语义仍成立：
    // 过期数据读面视同不存在（expired ≈ NOTFOUND），物理回收仅顺延至窗外
    if self.migration_claim_busy(user_key)? {
      return Ok(());
    }
    let _suppress = PurgeNotifyGuard::enter(&self.purge_window);
    self.delete(user_key).await?;
    // 先解抑制再触发事件：事件处理器内部的写操作不受抑制窗口影响
    drop(_suppress);
    // 事件域取会话物理域（与被清记录的键前缀同源），副本按条目域直设落回原域
    let (ns, db) = self.virtual_domain();
    self.store.emit_event(
      self.aof_session_id,
      StoreEvent::TtlPurge {
        ns,
        db,
        key: user_key,
        expire_at: expire_at_ticks,
      },
    )
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
  /// 返回 true = 键存活（无 TTL 或未到期）；false = 已过期（读入口视同不存在）。
  ///
  /// 读入口统一接线（read_tag_with/read_tag_with_size/contains_key/load_meta/
  /// load_range_index_stub/rmw）：本键在整条同步调用链内的 TTL 裁决只在此入口
  /// 做一次，内部裸读（read_raw_with）绝不嵌套二次裁决。已到期键尽力加锁执行
  /// 物理清除（若锁争用则跳过物理清除，但逻辑上仍按已过期判死）
  #[inline]
  pub(crate) async fn probe_alive(&self, user_key: &[u8]) -> Result<bool> {
    if !self.has_ttl_tag(user_key)? {
      return Ok(true);
    }
    let Some(exp) = self.ttl_of(user_key).await? else {
      return Ok(true);
    };
    if !is_expired(exp, now_ticks()) {
      return Ok(true);
    }
    let _ = self.check_expired(user_key).await;
    Ok(false)
  }

  /// 惰性过期检查：key 已过期则物理删除（数据 + TTL 记录）并返回 true
  ///
  /// 读入口统一接线（read/contains_key/load_meta 等），内部仅经 raw 路径探测 TTL 记录，
  /// 绝不回调这些入口，无递归。约定调用方"先确认 key 存在再探测"（TTL 记录不存在即无
  /// TTL，快路径仅一次哈希探针零额外 I/O）；对不存在键调用亦安全（返回 false）
  ///
  /// DELIFEXPIM 会话级语义单点（RMW ExpireAndStop：在库且已到期才物理删除，
  /// 未到期零写入）：libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:DELIFEXPIM
  /// ——C# 由 UnifiedRMW 三回调（NeedInitialUpdate/NeedCopyUpdate/InPlaceUpdaterWorker）
  /// 的 ExpireAndStop 链承接，rust 收敛为本入口一条判定；后台过期扫描与
  /// 读路径惰性清除共用（扫描侧收集见 gc::ttl_sweep）
  pub async fn check_expired(&self, user_key: &[u8]) -> Result<bool> {
    let Some(exp) = self.ttl_of(user_key).await? else {
      return Ok(false);
    };
    // 到期判定取严格小于（统一收敛至 is_expired）
    if !is_expired(exp, now_ticks()) {
      return Ok(false);
    }
    let index = self.store.index.load();
    // scoped 口径寻桶（会话物理前缀种子，`whasher::scoped_hash` 全仓单点）：
    // 与 rmw 读改写窗口、wtxn 事务键锁三面同键同桶互斥
    //（票 wtxn-wkv-keybucket-hash-scope-desync）
    let Some(_key_lock) =
      index.try_lock_key_hash_exclusive(scoped_hash(&self.session_prefix(), user_key))
    else {
      return Ok(false);
    };
    let Some(exp_recheck) = self.ttl_of(user_key).await? else {
      return Ok(false);
    };
    if !is_expired(exp_recheck, now_ticks()) {
      return Ok(false);
    }
    // 装箱打破 async 递归布局环（purge_expired → delete → load_meta → check_expired），
    // 仅在真正过期的冷路径付出一次堆分配
    Box::pin(self.purge_expired(user_key, exp_recheck)).await?;
    Ok(true)
  }

  /// RENAME 迁移 claim 复合判点（TTL 写面封堵单点：Meta 在场探测 +
  /// migration_claimed，与 upsert_tag / purge_expired / DEL 两臂同位序）
  ///
  /// claim 在册键必有存活元记录（普通键仅多一次 Meta 哈希探针），命中即键正处
  /// RENAME 树迁移窗：EXPIRE/PERSIST 的 TTL 变更若放行，随迁值与段五旧键清退
  /// 即构成「续期静默蒸发 / 已撤销 TTL 借尸还魂」——C# RENAME 以双键排他锁
  /// 把 EXPIRE/PERSIST 串行化到迁移完成后（UnifiedStoreOps.cs:RENAME），rust
  /// 以本判点把窗内 TTL 写零副作用拒绝（MigrationBusy 交客户端重试）承接同一
  /// 封堵语义。`true` = 在册（调用方拒绝/降级）；`Ok(false)` = 无迁移窗放行
  pub fn migration_claim_busy(&self, user_key: &[u8]) -> Result<bool> {
    let meta_k = self.session_meta_key(user_key);
    // Meta 在场探针前先走分裂协同（与 SET/DEL/purge 臂同纪律），杜绝扩容期
    // 漏检在册 claim 键的元记录致封堵失效
    self.ensure_split(&meta_k)?;
    Ok(
      self.store.index.load().find_tag(&meta_k).is_some()
        && self.store.range_index.migration_claimed(&meta_k),
    )
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
  /// - GT/LT 与既有 TTL 同为 ticks 域比较；入参值域裁决已在上游完成
  ///   （EXPIRE 族在 wnode `network_expire` 命令边界粗化，其余族为裸 ticks
  ///   原值），本入口恒等直通，判定与写入同源（对标 C# 打包粗化后存储侧同域
  ///   评估与 word 形恒等重打包 UnifiedStore/RMWMethods.cs:216、:228；内核
  ///   `put_ttl` 裸写不再二次粗化）；
  /// - 持本键独占桶锁串行化读改写窗口（对齐 hexpire 先例），锁内记录遍历由 3 压至 2
  ///
  /// RENAME 迁移 claim 判点先于取键闩（判点在一切读写之前，命中即
  /// [`Error::MigrationBusy`] 零副作用上抛）：claim 登记与判定间隙的派发微窗
  /// 为 migration.rs 段一既述残余，段五以窗内现势读取折叠（见该处注释）
  ///
  /// C# 统一存 EXPIRE/PERSIST/DELIFEXPIM 三命令共用的 RMW 执行链在 rust 侧
  /// 折叠为本入口 + [`Self::expire_at_apply`] + [`Self::put_i64_sidecar`]
  /// 三段（TTL 旁路记录单轨化后原位/拷贝双臂合一），四处映射挂准：
  /// libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:NeedInitialUpdate
  /// （键缺席三命令均不建记录：EXPIRE/PERSIST/DELIFEXPIM → false，即本函数
  /// 遍历 1 判假直接 -2 的缺席早退门）；
  /// libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:InitialUpdater
  /// （C# 对 TTL 命令臂 throw 恒不可达，rust 由同一缺席早退门承接）；
  /// libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:InPlaceUpdater
  /// （裁决 + 条件评估 + 落笔全链：到期 purge ↔ ExpireAndStop/Resume，
  /// NX/XX/GT/LT ↔ ExpirationWithOption 四选项，尾部交 apply）
  pub async fn expire_at(&self, user_key: &[u8], expire_at_ticks: i64, opt: TtlOpt) -> Result<i32> {
    // 会话入口恒等直通（对标 C# 存储侧重打包点 UnifiedStore/RMWMethods.cs:216、
    // :228 的单参 word 构造器 `new ExpirationWithOption(input.arg1)` 恒等装载
    // 不移位，ExpirationWithOption.cs:30-33——该形不含粗化，粗化唯二参构造器
    // :20-24 在 EXPIRE 族命令打包时施加，rust 对位于 wnode `network_expire`
    // 命令边界单点）：AOF 重放、迁移导入、复制应用三类外部 ticks 与主端存值
    // 逐位一致落盘，SET/GETEX/RENAME 族非 16 对齐裸值不再被头部粗化清低 4 位
    // （历史粗化门主副 ≤15 ticks 恒早偏移形态收口登记见 deviations.md §143）
    // 写面共序单源 [`ttl_write_gate!]`（claim 封堵 → 单键独占桶闩 → 遍历 1/2
    // 裸数据存活判定），键缺席回 -2
    ttl_write_gate!(self, user_key, -2);
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
      // 从未设 TTL：XX/GT 无当前值可比，一律不满足
      None if opt.xx || opt.gt => Ok(0),
      // 条件全通过（未设 TTL 放行，或已设未到期且无阻断项）
      _ => self.expire_at_apply(user_key, expire_at_ticks).await,
    }
  }

  /// expire_at 尾段公共体：条件全部通过后写 TTL，或过去时间戳立即物理删除
  ///
  /// libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:InPlaceUpdaterWorker
  /// （worker 判定尾：EXPIRE 过去时间戳 ↔ ExpireAndResume 物理清除、
  /// 未过期落笔 ↔ HandleExpireInPlaceUpdate/HandlePersistInPlaceUpdate 的
  /// 改写臂；DELIFEXPIM「未到期 no-op」臂由 expire_at 的条件不满足 -2/0 承接）
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
  ///
  /// RENAME 迁移 claim 判点先于取键闩（与 [`Self::expire_at`] 同纪律）：在册
  /// 即 [`Error::MigrationBusy`] 零副作用上抛——PERSIST 撤销 TTL 若落迁移窗，
  /// 段五按窗前快照随迁即把已撤销的 TTL 借尸还魂到新键（键按用户已撤销的
  /// TTL 提前蒸发）
  pub async fn persist(&self, user_key: &[u8]) -> Result<i32> {
    // 写面共序单源 [`ttl_write_gate!]`（与 expire_at 同款 claim 封堵 → 单键独占
    // 桶闩 → 裸数据存活判定），键缺席回 0
    ttl_write_gate!(self, user_key, 0);
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
  ///
  /// 单读正序（票 zcode-r127c-genexpire1，与 wnode 快径 `ttl_read_sync` 同判序）：
  /// 先单次 [`Self::ttl_of`] 取 TTL，再经 [`Self::contains_key_ignore_ttl`] 裸
  /// 数据走查裁决存在性——旧形 `contains_key` 内嵌 probe_alive 的 TTL 裁决与
  /// 尾段 `ttl_of` 二读同构撕裂（并发删除瞬态误报 -1），TTL 探测收敛为同一
  /// 调用链单次读取（读路径 TTL 探测收敛不变式，同 [`Self::persist`]）。
  /// 已到期臂的惰性物理清除副作用与原 `contains_key`→`probe_alive`→
  /// `check_expired` 链等价保留（闩内双检，防窗内续期误删）；哨兵 -1/-2
  /// 全臂与快径逐值对照
  /// 异步 TTL / EXPIRETIME 探测与到期惰性清除单源
  async fn query_expiry_ms(&self, user_key: &[u8], at: impl FnOnce(i64) -> i64) -> Result<i64> {
    let expiry = self.ttl_of(user_key).await?;
    // 折叠终判权归数据走查：三域皆缺即 -2，TTL 记录读到何值不改判
    if !self.contains_key_ignore_ttl(user_key).await? {
      return Ok(-2);
    }
    match expiry {
      // 数据在场且无 TTL：真实无 TTL 键，-1 唯一合法出口
      None => Ok(-1),
      Some(exp) if is_expired(exp, now_ticks()) => {
        // 已到期视同不存在（对标 C# Reader 快照内 CheckExpiry 失败即 NOTFOUND
        // →上层 status != OK 折 -2，KeyAdminCommands.cs:495-559）；惰性清除
        let _ = self.check_expired(user_key).await;
        Ok(-2)
      }
      Some(exp) => Ok(at(exp)),
    }
  }

  pub async fn pttl_ms(&self, user_key: &[u8]) -> Result<i64> {
    self
      .query_expiry_ms(user_key, |exp| {
        milliseconds_from_diff_ticks(exp, now_ticks())
      })
      .await
  }

  /// 查询 key 绝对过期 Unix 毫秒时间戳 (EXPIRETIME / PEXPIRETIME 语义。
  /// -2 无 key；-1 无 TTL；返回值仍是 Unix 毫秒语义——内部 .NET Ticks 经
  /// `unix_time_in_milliseconds_from_ticks` 换算)
  pub async fn expiretime_ms(&self, user_key: &[u8]) -> Result<i64> {
    self
      .query_expiry_ms(user_key, unix_time_in_milliseconds_from_ticks)
      .await
  }
}

#[cfg(test)]
mod tests {
  use std::panic::{AssertUnwindSafe, catch_unwind};

  use super::*;

  /// 抑制守卫 RAII 语义（会话私有窗位形态）：正常退出与 panic unwind 路径均恢复
  /// 进入前旧值（窗位零残留）
  #[test]
  fn purge_notify_guard_restores_on_drop_and_unwind() {
    let flag = AtomicBool::new(false);

    // 正常作用域退出：恢复 save/restore 链上的旧值
    {
      let _g = PurgeNotifyGuard::enter(&flag);
      assert!(flag.load(Relaxed), "窗内必须置位");
    }
    assert!(!flag.load(Relaxed), "退出后窗位必须清零");

    // 嵌套：内层退出恢复外层窗位，而非直接清零
    let outer = PurgeNotifyGuard::enter(&flag);
    {
      let _inner = PurgeNotifyGuard::enter(&flag);
      assert!(flag.load(Relaxed));
    }
    assert!(flag.load(Relaxed), "内层退出必须保持外层窗口，不得击穿");
    drop(outer);
    assert!(!flag.load(Relaxed));

    // panic unwind：Drop 兜底，抑制窗位不残留
    let _ = catch_unwind(AssertUnwindSafe(|| {
      let _g = PurgeNotifyGuard::enter(&flag);
      assert!(flag.load(Relaxed));
      panic!("unwind through guard");
    }));
    assert!(!flag.load(Relaxed), "panic 路径不得残留抑制窗位");
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
