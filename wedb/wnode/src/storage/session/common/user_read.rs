//! 用户数据与物理标签域同步读状态抽象
//!
//! 将存储层底层嵌套三态 / 五态折叠为类型化枚举，
//! 消除命令层（GET/GETEX/GETDEL/INCR/BITMAP 等）反复手写的嵌套 match 样板。

use wdev::Device;
use wkv::{BatchStoreSession, StoreResult};
use wmetric::SessionMetricsHandle;
use wresp::{
  cmd_strings::{RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE},
  ext::RespVecExt,
};
use wval::KeyTag;

use super::ttl_sync::{
  read_adjudicated_tag_sync_with_prefix, read_adjudicated_user_sync,
  read_adjudicated_user_sync_with_prefix,
};

/// 用户数据同步读结果快捷判空与解包宏（单点单源）
///
/// 覆盖 Hit / Missing / WrongType / Deferred / Err 全部五态。
#[macro_export]
macro_rules! read_user_or_bail {
  ($res:expr, $output:expr, $missing:expr) => {
    match $res {
      Ok($crate::storage::session::common::UserRead::Hit(v)) => v,
      Ok($crate::storage::session::common::UserRead::Missing) => $missing,
      Ok($crate::storage::session::common::UserRead::WrongType) => {
        $output.write_resp_error(wresp::cmd_strings::RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok($crate::storage::session::common::UserRead::Deferred) => return Ok(false),
      Err(_) => {
        $output.write_resp_error(wresp::cmd_strings::RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
  };
}

/// 用户数据三域读结果（String / ObjectEnvelope / Meta 域判型折叠，Meta 臂
/// 供升阶键对象键 WRONGTYPE 判定）
#[derive(Debug, Copy, PartialEq, Eq, Clone)]
pub enum UserRead<R> {
  /// String 域命中（值内容任意）
  Hit(R),
  /// 键缺失（三域皆缺或已过期）
  Missing,
  /// 集合对象键（信封域或升阶 Meta 域命中，C# ValueIsObject → WRONGTYPE）
  WrongType,
  /// 磁盘候选 / TTL 待异步裁决，须降级异步
  Deferred,
}

/// 读入账折叠唯一规则（found/notfound/静默三态，标量入参保单机制）：
/// `Some(true)`=命中计 found、`Some(false)`=缺失计 notfound、`None`=WrongType
/// 静默。快路径 [`UserRead::record_outcome`] 与慢路径异步漏斗出口
/// [`UserReadAsync::record_outcome`] 共用本函数，杜绝两套计数规则；对位 C#
/// MainStoreOps GET `status.Found` → incr_session_found / else →
/// incr_session_notfound / WrongType 双臂均不计数
#[inline]
pub(crate) fn fold_outcome(kind: Option<bool>, metrics: Option<&SessionMetricsHandle>) {
  let Some(metrics) = metrics else { return };
  match kind {
    Some(true) => metrics.incr_total_found(1),
    Some(false) => metrics.incr_total_notfound(1),
    None => {}
  }
}

impl<R> UserRead<R> {
  /// 从存储层三域读权威状态枚举转换
  ///
  /// Ok(StoreResult::Success(Ok(v))) -> Ok(UserRead::Hit(v))
  /// Ok(StoreResult::Success(Err(()))) -> Ok(UserRead::WrongType)
  /// Ok(StoreResult::NotFound) -> Ok(UserRead::Missing)
  /// Ok(StoreResult::RecordOnDisk) -> Ok(UserRead::Deferred)
  /// Err(e) -> Err(e)
  #[inline]
  pub fn from_result<E>(res: Result<StoreResult<Result<R, ()>>, E>) -> Result<Self, E> {
    match res {
      Ok(StoreResult::Success(Ok(v))) => Ok(Self::Hit(v)),
      Ok(StoreResult::Success(Err(()))) => Ok(Self::WrongType),
      Ok(StoreResult::NotFound) => Ok(Self::Missing),
      Ok(StoreResult::RecordOnDisk) => Ok(Self::Deferred),
      Err(e) => Err(e),
    }
  }

  /// 快路径读命中/未命中入账（对位 C# MainStoreOps 的 GET `status.Found` →
  /// incr_session_found / else → incr_session_notfound 判定：Hit→found、
  /// Missing→notfound；WrongType 两侧同不计数，Deferred 交慢路径既有单点
  /// `StorageSession::record_read_outcome`。C# GET 锚由
  /// `StorageSession::read_string` 单点持有，本函数为其入账臂，不重复挂锚）。
  /// 句柄 None = 采样关闭或 RMW 前置读（C# RMW 前置读漏斗不入账），空条件跳过；
  /// 折叠规则走 [`fold_outcome`] 单点，与异步漏斗出口同一套。慢臂对位同律：
  /// RMW 前置读慢臂一律走 `StorageSession::read_user_quiet` 零入账口。
  ///
  /// 【入账纪律与适用边界】
  /// 1. 读族：
  ///    - GET/GETEX/GETRANGE/STRLEN：快臂直传 `session_metrics`，慢臂走 `read_user` 簿记入口折叠；
  ///    - ETag 读族（GETWITHETAG / GETIFNOTMATCH）：快臂 read_user_sync 传入 `session_metrics`
  ///      （键命中计 found，缺失计 notfound，对位 GET 先例；慢臂 read_value_and_etag_async
  ///      既有 read_user 折叠不动，快慢两臂同构）。
  /// 2. RMW 前置读全链零入账谱系：
  ///    GETDEL 族（C# NetworkGETDEL 与 MainStoreOps 的 GETDEL 件全链零 incr_session_*）
  ///    与 string-RMW 族 INCR/INCRBY/DECR/DECRBY/INCRBYFLOAT/SETRANGE/APPEND（C#
  ///    MainStoreOps.cs:Increment 零计数、SETRANGE/APPEND 走 RMW_MainStore 口，
  ///    `AdvancedOps.cs:RMW_MainStore` 与 `CompletePending.cs` 均不触 total_found/total_notfound）
  ///    同谱；后续席勿按 GET 慢臂先例回补入账。
  /// 3. 条件写族命令层终态单源收口谱系：
  ///    - SET 条件写（SETEXNX / SET NX/XX 等）：快臂前置读保持 None 静默防降级重放双计，
  ///      终态按存在性于函数尾 fold_outcome 补账；慢臂走 probe_alive_domain_quiet_with_prefix
  ///      静默探针 + record_read_outcome 单点补账；
  ///    - ETag 条件写族（SETIFMATCH / SETIFGREATER / SETWITHETAG / DELIFGREATER）：
  ///      快臂前置读保持传入 None 保持静默纪律（防止降级重放双计）；慢臂走
  ///      `read_value_and_etag_quiet_async`（内核转调 `read_user_quiet`）静默读；
  ///      条件写族会话入账在命令出帧收口处按【C# 真实计数表】收口：
  ///      * SET 族：键存活（命中/失配零写）计 found；缺席/过期初写计 notfound；对象键 promote
  ///        后二次 SET_Conditional 初写计 notfound；
  ///      * DELIFGREATER：删生效计 found；缺席计 notfound；对象键计 notfound；失配零删零计（两臂均不计）。
  #[inline]
  pub fn record_outcome(&self, metrics: Option<&SessionMetricsHandle>) {
    fold_outcome(
      match self {
        Self::Hit(_) => Some(true),
        Self::Missing => Some(false),
        Self::WrongType | Self::Deferred => None,
      },
      metrics,
    );
  }
}

/// 快路径值读裁决收尾单源（GET / STRLEN / GETRANGE / GETWITHETAG / GETIFNOTMATCH 尾部）：
/// Hit 帧已由 [`read_user_sync`] 的消费回调写出；Missing 交 `on_missing` 按本命令口径出
/// 缺席帧（GET 族 null、GETRANGE 空 bulk、STRLEN `:0`）；WrongType 与存储错误分别出
/// WRONGTYPE / 通用错误帧；Deferred 回 `false` 交调用方降级异步，`true` = 应答已闭环。
/// `start_len` 承接各点差异：`Some` = 非命中臂出帧前回退到起笔（etag 族先写数组头再裁决），
/// `None` = 该点非命中臂未起笔不回退——回退之有无系行为差异，逐点原样保留不得合并
#[inline]
pub(crate) fn finish_value_read(
  read: wkv::Result<UserRead<()>>,
  output: &mut Vec<u8>,
  start_len: Option<usize>,
  on_missing: impl FnOnce(&mut Vec<u8>),
) -> bool {
  let rewind = matches!(&read, Ok(UserRead::WrongType | UserRead::Deferred) | Err(_));
  if let Some(start_len) = start_len.filter(|_| rewind) {
    output.truncate(start_len);
  }
  match read {
    Ok(UserRead::Hit(())) => {}
    Ok(UserRead::Missing) => on_missing(output),
    Ok(UserRead::WrongType) => output.write_resp_error(RESP_ERR_WRONG_TYPE),
    Ok(UserRead::Deferred) => return false,
    Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
  }
  true
}

/// 用户数据三域异步读结果（[`UserRead`] 的异步闭环子集：磁盘候选已在
/// 异步读内核内闭环，无 `Deferred` 降级态）
#[derive(Debug, Copy, PartialEq, Eq, Clone)]
pub enum UserReadAsync<R> {
  /// String 域命中（值内容任意）
  Hit(R),
  /// 键缺失（三域皆缺或已过期）
  Missing,
  /// 集合对象键（信封 / 升阶 Meta 域命中，C# ValueIsObject → WRONGTYPE）
  WrongType,
}

impl<R> UserReadAsync<R> {
  /// 异步三域漏斗出口的单条入账（与快路径 [`UserRead::record_outcome`] 同一
  /// [`fold_outcome`] 折叠规则：Hit→found、Missing→notfound、WrongType 静默）。
  /// 域内逐探针不入账，收编到本出口恰一条，对位 C# MainStoreOps GET 快慢
  /// （pending）通道同口径各一条；句柄 None = 采样关闭，空条件跳过
  #[inline]
  pub fn record_outcome(&self, metrics: Option<&SessionMetricsHandle>) {
    fold_outcome(
      match self {
        Self::Hit(_) => Some(true),
        Self::Missing => Some(false),
        Self::WrongType => None,
      },
      metrics,
    );
  }
}

/// 物理标签域单域读结果
#[derive(Debug, Copy, PartialEq, Eq, Clone)]
pub enum TagRead<R> {
  /// 标签域命中
  Hit(R),
  /// 键缺失或已过期
  Missing,
  /// 磁盘候选 / TTL 待异步裁决，须降级异步
  Deferred,
}

impl<R> TagRead<R> {
  /// 从存储层标签域读权威状态枚举转换
  ///
  /// Ok(StoreResult::Success(v)) -> Ok(TagRead::Hit(v))
  /// Ok(StoreResult::NotFound) -> Ok(TagRead::Missing)
  /// Ok(StoreResult::RecordOnDisk) -> Ok(TagRead::Deferred)
  /// Err(e) -> Err(e)
  #[inline]
  pub fn from_result<E>(res: Result<StoreResult<R>, E>) -> Result<Self, E> {
    match res {
      Ok(StoreResult::Success(v)) => Ok(Self::Hit(v)),
      Ok(StoreResult::NotFound) => Ok(Self::Missing),
      Ok(StoreResult::RecordOnDisk) => Ok(Self::Deferred),
      Err(e) => Err(e),
    }
  }
}

/// 带 TTL 裁决的用户数据三域同步读（折叠为 UserRead 四态）
///
/// 附着一致读会话时单键 pre/post 协议在三域回合外触发（超时上抛中止；
/// 三域单回合对位 C# UnifiedStore 一致读）。触发哈希域取 **String 记录物理键**
/// （[`wkv::StoreSession::consistent_read_hash`] 单点，与回放侧草图入账同键同
/// 哈希）：用户数据域是本漏斗的返回值来源域，信封 / Meta 探域仅在 String 域
/// 确认缺失后发生，其新鲜度由各自带标签读口（[`read_tag_sync`] /
/// [`read_envelope_sync`]）单独承担
///
/// `metrics` 为会话指标共享句柄（GET/GET_SG/MGET/GETRANGE/STRLEN 读命令臂
/// 传接）：漏斗尾一次收口命中/未命中入账（对位 C# MainStoreOps.cs:30/:39
/// GET 族 `status.Found` 判定，批量臂逐键经本漏斗即逐键入账与 C# 同口径）；
/// RMW 前置读与「读后还有命令内降级出口」的 GETEX 臂传 None 静默，由命令体
/// 收尾经 [`fold_outcome`] 单点补账（防降级重放双计，见 network_getex /
/// network_get_sg 守卫注），Deferred 交慢路径既有单点；元数据读族
/// （OBJECT/TYPE/TTL/EXISTS/MEMORY USAGE）按 C# Read_UnifiedStore
/// （libs/server/Storage/Session/UnifiedStore/AdvancedOps.cs:12-21 零入账）
/// 恒传 None 零计，严禁按 GET/DUMP 形制补句柄（票 zcode-r157c-objenc 案二
/// 防回改单点真源，快臂 network_object / 慢臂 object_slow 两处注记回指本段）
///
/// C# 主存 ISessionFunctions 读回调（快路径值拷贝 / CheckExpiry / ValueIsObject
/// WrongType 三段职责）的 rust 漏斗：libs/server/Storage/Functions/MainStore/ReadMethods.cs:Reader
/// ——快路径 `CopyRespTo` 由消费闭包 `f` 承接（零拷贝借用视图）、过期判定由
/// `read_adjudicated_user_sync` 承接、对象键 WrongType 由三域折叠承接；
/// per-command 解释（GET 拷出 / ETag 族 / 自定义命令）留在命令层各自单点，
/// 不再经单回调分派
///
/// 过期×判型序刻意采 C# UnifiedStore Reader :22 同侧（过期先于判型，其主存
/// Reader :31 判型先行系原型内部不一致），deviations §133 严禁回改
#[inline]
pub(crate) fn read_user_sync<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  metrics: Option<&SessionMetricsHandle>,
  f: impl FnOnce(&[u8]) -> R,
) -> wkv::Result<UserRead<R>> {
  let res = session.with_session_consistent_read(key, KeyTag::String, || {
    read_adjudicated_user_sync(session, key, f)
  })?;
  let res = UserRead::from_result(res);
  if let Ok(read) = &res {
    read.record_outcome(metrics);
  }
  res
}

/// 带 TTL 裁决的用户数据三域同步读的显式前缀变体（循环前缀外提对位，语义与
/// [`read_user_sync`] 一致；rust 工程优化无 c# 对应：批量遍历单次外提
/// `session_prefix()` 消除逐键重读 ns/db 原子变量与重算 Varint）
#[inline]
pub(crate) fn read_user_sync_with_prefix<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  prefix: &[u8],
  key: &[u8],
  metrics: Option<&SessionMetricsHandle>,
  f: impl FnOnce(&[u8]) -> R,
) -> wkv::Result<UserRead<R>> {
  let res =
    session.with_session_consistent_read_with_prefix(prefix, key, KeyTag::String, || {
      read_adjudicated_user_sync_with_prefix(session, prefix, key, f)
    })?;
  let res = UserRead::from_result(res);
  if let Ok(read) = &res {
    read.record_outcome(metrics);
  }
  res
}

/// 带 TTL 裁决的指定物理标签域同步读（折叠为 TagRead 三态）
///
/// 附着一致读会话时单键 pre/post 协议触发（超时上抛中止），触发哈希按本域
/// 记录物理键取（[`wkv::StoreSession::consistent_read_hash`]）
#[inline]
pub(crate) fn read_tag_sync<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: KeyTag,
  f: impl FnOnce(&[u8]) -> R,
) -> wkv::Result<TagRead<R>> {
  let prefix = session.session_prefix();
  read_tag_sync_with_prefix(session, prefix.as_slice(), key, tag, f)
}

/// 指定物理标签域同步读的显式前缀变体（循环前缀外提对位，语义与
/// [`read_tag_sync`] 逐臂一致；rust 工程优化无 c# 对应：批量/多探点位在循环外
/// 单次外提 `session_prefix()` 交本口，一致读回合与 TTL 裁决复用同一前缀，
/// 消除逐域重读 ns/db 原子变量与重算 Varint，镜像 [`read_user_sync_with_prefix`]）
#[inline]
pub(crate) fn read_tag_sync_with_prefix<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  prefix: &[u8],
  key: &[u8],
  tag: KeyTag,
  f: impl FnOnce(&[u8]) -> R,
) -> wkv::Result<TagRead<R>> {
  let res = session.with_session_consistent_read_with_prefix(prefix, key, tag, || {
    read_adjudicated_tag_sync_with_prefix(session, prefix, key, tag, f)
  })?;
  TagRead::from_result(res)
}

/// 带 TTL 裁决的对象信封域同步读（折叠为 TagRead 三态）
///
/// C# 简单对象会话函数读回调（ValueIsObject → 取 IGarnetObject，缺对象域即
/// 判败）的 rust 信封域读漏斗：
/// libs/server/Storage/Functions/SimpleGarnetObjectSessionFunctions.cs:Reader
/// ——IGarnetObject 具体化由消费闭包 `f` 内反序列化承接，对象域缺席 =
/// Missing（C# false 返回同判）
#[inline]
pub(crate) fn read_envelope_sync<R, D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  f: impl FnOnce(&[u8]) -> R,
) -> wkv::Result<TagRead<R>> {
  read_tag_sync(session, key, KeyTag::ObjectEnvelope, f)
}
