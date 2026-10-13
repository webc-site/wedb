//! EXPIRE / PERSIST / EXISTS / TTL / EXPIRETIME 键生存期族（对标
//! libs/server/Resp/KeyAdminCommands.cs 的 NetworkEXPIRE / NetworkPERSIST /
//! NetworkTTL / NetworkEXPIRETIME 与 NetworkEXISTS）

use wbase::{
  convert::{
    coarse_expire_ticks, compute_expiration_ticks, milliseconds_from_diff_ticks,
    seconds_from_diff_ticks, unix_time_in_milliseconds_from_ticks, unix_time_in_seconds_from_ticks,
  },
  time::now_ticks,
};
use wdev::Device;
use wkv::{StoreResult, TtlOpt, is_expired, is_expired_or_now};
use wresp::{
  check_args::{check_arg_count, parse_i64_arg, unpack_args},
  cmd_strings as cs,
  cmd_strings::{
    RESP_ERR_GENERIC, abort_with_error_message, abort_with_unsupported_option, write_raw,
  },
  ext::{RespSliceExt, RespVecExt},
  options::{ExpireOption, try_get_expire_option},
};
use wval::KeyTag;

use super::super::super::resp_server_session::RespServerSession;
use crate::{
  resp::vector::vector_manager::VectorManager,
  storage::session::common::ttl_sync::{
    data_alive_domain_with_prefix, del_ttl_sync, probe_alive_domain, probe_alive_with_registry,
    put_ttl_sync, ttl_of_sync, ttl_of_sync_with_prefix,
  },
};

/// 0/1 旗标应答帧表（下标即「未生效 / 已生效」，C# `RESP_RETURN_VAL_0`、`_1`）
pub(crate) const FLAG_FRAMES: [&[u8]; 2] = [cs::RESP_RETURN_VAL_0, cs::RESP_RETURN_VAL_1];
pub(crate) const REPLY_TTL_MISSING: i64 = -2;
pub(crate) const REPLY_TTL_NO_EXPIRY: i64 = -1;

/// rust 自有枚举分派（C# 无独立枚举对位，判据落仓内自陈契约；对标 libs/server/Resp/KeyAdminCommands.cs::NetworkEXPIRE）
///
/// EXPIRE 族命令形态（对标 libs/server/Resp/RespServerSession.cs 对
/// KeyAdminCommands.NetworkEXPIRE 的四路派发）
#[derive(Copy, strum::IntoStaticStr, Clone)]
#[strum(serialize_all = "UPPERCASE")]
pub enum ExpireCmd {
  Expire,
  Pexpire,
  Expireat,
  Pexpireat,
}

impl ExpireCmd {
  /// 命令名（错误文案用）
  #[inline]
  pub fn as_str(self) -> &'static str {
    self.into()
  }

  /// 是否为毫秒域
  #[inline]
  pub const fn is_milliseconds(self) -> bool {
    matches!(self, Self::Pexpire | Self::Pexpireat)
  }

  /// 是否为绝对时间戳
  #[inline]
  pub const fn is_timestamp(self) -> bool {
    matches!(self, Self::Expireat | Self::Pexpireat)
  }

  /// 换算绝对过期 .NET Ticks（对标 KeyAdminCommands.cs:421-427 的换算 switch）
  #[inline]
  pub fn expire_at_ticks(self, expiration: i64) -> i64 {
    compute_expiration_ticks(
      now_ticks(),
      expiration,
      self.is_milliseconds(),
      self.is_timestamp(),
    )
  }
}

/// rust 自有枚举分派（C# 无独立枚举对位，判据落仓内自陈契约；对标 libs/server/Resp/KeyAdminCommands.cs::NetworkTTL）
///
/// TTL 族命令形态（TTL / PTTL）
#[derive(Copy, strum::IntoStaticStr, Clone)]
#[strum(serialize_all = "UPPERCASE")]
pub enum TtlCmd {
  Ttl,
  Pttl,
}

/// rust 自有枚举分派（C# 无独立枚举对位，判据落仓内自陈契约；对标 libs/server/Resp/KeyAdminCommands.cs::NetworkEXPIRETIME）
///
/// EXPIRETIME 族命令形态（EXPIRETIME / PEXPIRETIME）
#[derive(strum::IntoStaticStr, Clone)]
#[strum(serialize_all = "UPPERCASE")]
pub enum ExpireTimeCmd {
  Expiretime,
  Pexpiretime,
}

/// 键生命周期命令的「存储三态 → 应答」单源骨架（EXPIRE / PERSIST / TTL /
/// EXPIRETIME 四命令共用）：`Ok(Some(v))` 交 `frame` 出帧（各命令保留自己的
/// 出帧原语，帧字节不变）、`Ok(None)` 降级异步、`Err` 写通用错误帧
#[inline]
fn reply_tristate<T>(
  res: Result<Option<T>, wkv::Error>,
  output: &mut Vec<u8>,
  frame: impl FnOnce(T, &mut Vec<u8>),
) -> wresp::Result<bool> {
  match res {
    Ok(Some(v)) => frame(v, output),
    Ok(None) => return Ok(false),
    Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
  }
  Ok(true)
}

impl RespServerSession {
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkEXPIRE
  ///
  /// EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT 共同体：完整 C# 参数校验次序
  /// （个数 → 整数 → 非负 → NX/XX/GT/LT 选项组合），过期经
  /// [`crate::storage::session::common::ttl_sync`] 同步落 TTL 记录；磁盘候选/过期清除须异步时整体降级
  ///
  /// 【有意偏差登记】EXPIRE 大值秒数，本系统转换到 `i64::MAX` ticks 正常回 :1，远端大值不再掐连接；
  /// C# 侧 `DateTimeOffset.UtcNow.AddSeconds` 越界则抛异常掐连接。
  /// 与 EXPIREAT `i64::MAX` 钳制同源。
  pub fn network_expire<'a, D: Device>(
    &mut self,
    command: ExpireCmd,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(args) = parse_expire_args(command, parse_state, output) else {
      return Ok(true);
    };
    let ExpireArgs {
      key,
      expire_at_ticks,
      opt,
    } = args;

    reply_tristate(
      expire_apply_sync(store, key, expire_at_ticks, opt),
      output,
      |applied, out| {
        // C# status != OK（键缺失等）回 :0；成功由存储回 :1（含过去时间戳
        // 立即删除的 Redis 7.4 语义 :1）
        write_raw(out, FLAG_FRAMES[usize::from(applied != 0)]);
      },
    )
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkPERSIST
  pub fn network_persist<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "PERSIST") else {
      return Ok(true);
    };

    reply_tristate(persist_apply_sync(store, key), output, |removed, out| {
      out.write_resp_int(i64::from(removed));
    })
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkTTL
  pub fn network_ttl<'a, D: Device>(
    &mut self,
    command: TtlCmd,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, command.into()) else {
      return Ok(true);
    };
    let now = now_ticks();
    query_expiry_sync(
      store,
      key,
      |exp| match command {
        TtlCmd::Pttl => milliseconds_from_diff_ticks(exp, now),
        TtlCmd::Ttl => seconds_from_diff_ticks(exp, now),
      },
      output,
    )
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkEXPIRETIME
  ///
  /// 参数个数错误恒报 EXPIRETIME（C# `nameof(RespCommand.EXPIRETIME)` quirk，
  /// PEXPIRETIME 同文案）
  pub fn network_expiretime<'a, D: Device>(
    &mut self,
    command: ExpireTimeCmd,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "EXPIRETIME") else {
      return Ok(true);
    };
    query_expiry_sync(
      store,
      key,
      |exp| match command {
        ExpireTimeCmd::Pexpiretime => unix_time_in_milliseconds_from_ticks(exp),
        ExpireTimeCmd::Expiretime => unix_time_in_seconds_from_ticks(exp),
      },
      output,
    )
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkEXISTS
  ///
  /// 多键计数；任一键须异步裁决（磁盘候选/TTL 待裁决）时整体降级，
  /// 存储错误直接回错，避免计数口径失真
  pub fn network_exists<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1.., output, "EXISTS");

    let mut exists_count = 0i64;
    let prefix = store.session_prefix();
    let prefix_slice = prefix.as_slice();
    for key in parse_state {
      // 三域探针 + 向量登记表第四态（存活观测单点，对标 C# Reader 无类型门）
      if probe_alive_or_bail!(store, prefix_slice, key, vector, output) {
        exists_count += 1;
      }
    }

    output.write_resp_int(exists_count);
    Ok(true)
  }
}

/// NetworkEXPIRE 的参数推导单源（快慢路径共用；解析失败时已写出错误应答并
/// 返回 None）
pub(crate) struct ExpireArgs<'p> {
  pub(crate) key: &'p [u8],
  /// 已粗化的绝对过期 .NET Ticks（快路径打包侧粗化口径）
  pub(crate) expire_at_ticks: i64,
  pub(crate) opt: TtlOpt,
}

/// NetworkEXPIRE 族推导（个数 → 整数 → 非负 → NX/XX/GT/LT 选项组合 →
/// 换算绝对 ticks → 命令边界粗化），快慢两侧同一入口，杜绝第二套推导
pub(crate) fn parse_expire_args<'p>(
  command: ExpireCmd,
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
) -> Option<ExpireArgs<'p>> {
  check_arg_count!(parse_state, 2..=4, output, command.as_str(), return None);
  let count = parse_state.len();

  let key = parse_state[0];
  let expiration = parse_i64_arg(parse_state[1], output)?;
  if expiration < 0 {
    // C# 文案（与 Redis 的 "must be positive" 不同，逐字节保留）
    abort_with_error_message(output, cs::RESP_ERR_INVALID_EXPIRE_TIME);
    return None;
  }

  // NX/XX/GT/LT 选项与两参组合（XXGT/XXLT），位运算解析并校验兼容规则
  let mut opt = TtlOpt::NONE;
  if count > 2 {
    let Some(first_opt) = try_get_expire_option(parse_state[2]) else {
      abort_with_unsupported_option(output, parse_state[2].as_str_safe());
      return None;
    };
    let mut combined = first_opt;
    if count > 3 {
      let Some(second_opt) = try_get_expire_option(parse_state[3]) else {
        abort_with_unsupported_option(output, parse_state[3].as_str_safe());
        return None;
      };
      let merged = first_opt | second_opt;
      let compatible = merged == ExpireOption::XXGT || merged == ExpireOption::XXLT;
      if first_opt == second_opt || !compatible {
        abort_with_error_message(
          output,
          "ERR NX and XX, GT or LT options at the same time are not compatible",
        );
        return None;
      }
      combined = merged;
    }
    opt = TtlOpt {
      nx: combined.contains(ExpireOption::NX),
      xx: combined.contains(ExpireOption::XX),
      gt: combined.contains(ExpireOption::GT),
      lt: combined.contains(ExpireOption::LT),
    };
  }

  // 换算绝对过期 .NET Ticks（对标 KeyAdminCommands.cs:421-427 换算 switch，
  // rust TTL 记录即 ticks 口径，wkv/GC 同域解释）
  let expire_at_ticks = command.expire_at_ticks(expiration);

  // 键级过期粗化唯此一处（对标 C# NetworkEXPIRE:430 打包侧
  // `new ExpirationWithOption(ticks, option)` 的粗化半段，
  // ExpirationWithOption.cs:22-23）：C# 粗化是 ExpireOption 借用低 4 位的
  // 产物、只覆盖 EXPIRE 族，条件判定（GT/LT）与落盘同域；下游会话入口
  // wkv `expire_at`、同步臂 `put_ttl_sync` 与存储内核 `put_ttl` 一律恒等
  // 裸写不判不移位（对标 C# 存储侧 word 形恒等装载
  // UnifiedStore/RMWMethods.cs:216、:228）。判据边界：SET/GETEX/RENAME 族
  // 对应 C# MainStore/RMWMethods.cs TrySetExpiration/EvaluateExpire* 的裸
  // ticks 路径，全链路禁止粗化（deviations.md §143）
  let expire_at_ticks = coarse_expire_ticks(expire_at_ticks);
  Some(ExpireArgs {
    key,
    expire_at_ticks,
    opt,
  })
}

/// EXPIRE / PERSIST 两内核共用的存活判定（三域域探针单源 + Meta 域迁移 claim
/// 复合判点，含「过期键视同缺失」裁决）：
/// `Ok(None)` 须整体降级异步（探针磁盘候选 / claim 在册——由 wkv `expire_at`、
/// `persist` 判点统一 MigrationBusy 拒绝，杜绝快路径旁路迁移封堵窗；claim 在册
/// 键必有存活元记录，窗内 TTL 写会被段五随迁/清退吞没）；
/// `Ok(Some(false))` 键缺失；`Ok(Some(true))` 键存活可改 TTL
fn ttl_write_alive<D: Device>(
  store: &wkv::BatchStoreSession<'_, D>,
  key: &[u8],
) -> Result<Option<bool>, wkv::Error> {
  let Some(found) = probe_alive_domain(store, key)? else {
    return Ok(None);
  };
  Ok(Some(match found {
    None => false,
    Some(KeyTag::Meta) if store.migration_claim_busy(key)? => return Ok(None),
    Some(_) => true,
  }))
}

/// EXPIRE 应用内核（同步镜像 wkv::StoreSession::expire_at 的判定表，全链
/// .NET Ticks 同域比较）
///
/// 返回 `Ok(None)` 须降级异步；`Ok(Some(0))` 条件不满足/键缺失；
/// `Ok(Some(1))` 已设置（或过去时间戳已物理删除）
///
/// 过去时间戳路径与 C# 的刻意差异声明：C#
/// （libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs EXPIRE 分支 +
/// HandleExpireInPlaceUpdate → EvaluateExpire（SessionFunctionsUtils.cs:32，
/// 写路径 NX/XX/GT/LT 设置判定））仅把记录过期设为过去值（惰性过期，
/// 后续读路径 CheckExpiry（LogRecordUtils.cs:18）清理），rust 在写命令内即
/// 物理删键（记录墓碑先行、随键 TTL 后剥，判定统一收敛 wkv `is_expired_or_now`
/// 含相等口径）。两者应答
/// （:1）与最终可见状态（键消失）一致；AOF 语义 rust 为 DEL 形态墓碑、
/// C# 为 RMW-EXPIRE 条目（重放经 AofProcessor UnifiedStoreRMW 原样重发
/// RMW、复设同一过去 ticks 与时刻无关故幂等，非 DELIFEXPIM 转换），重放
/// 均幂等——主端
/// 已物理删除时重放 DEL 零写入无副作用。已知差异：WATCH 版本推进次数
/// （rust TTL 墓碑 + 数据墓碑两写 vs C# 单记录 RMW 一次）不影响事务失效
/// 判定的正确性（推进只多不少，方向单调）
///
/// 读改写原子窗口：入口取本键桶排他闩（[`wkv::BatchStoreSession::try_rmw_window`]，
/// 与 wkv `expire_at` 同一把键闩），闩内完成「存活判定 → TTL 读 → 条件判定 →
/// 原位改写/删除」全程，杜绝 ttl_of → put/del_ttl 间隙内并发写入的新 TTL 被误删
/// （对标 C# libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:HandleExpireInPlaceUpdate 经 InternalRMW
/// ephemeral 桶闩内读改写天然串行）；同步域取闩失败沿 `Ok(None)` 降级信号交
/// 异步持闩版 `expire_at` 收口，绝不自旋等闩
///
/// libs/server/API/GarnetApiUnifiedCommands.cs:EXPIRE
/// libs/server/API/GarnetApiUnifiedCommands.cs:EXPIREAT
/// libs/server/API/GarnetApiUnifiedCommands.cs:PEXPIREAT
///（C# GarnetApi 的 EXPIRE 三重载（RMW input 形 / expiryMs 形 / TimeSpan 形）
/// 与 EXPIREAT / PEXPIREAT 转发 storageSession.EXPIRE / EXPIREAT 单内核；
/// rust 无该 API 包装层，命令面 `network_expire` 直调本函数完成同一
/// 语义，PEXPIREAT 的毫秒乘换算在 `parse_expire_args` 边界完成）
/// TTL 写族共享前置探针三态（expire/persist 双内核同判据收口）：取本键读改写
/// 窗口（事务态让闩见 try_rmw_window 契约）+ 存活判定与 Meta 域迁移 claim 复合
/// 判点（单源 [`ttl_write_alive`]）+ 当前 TTL 读取（[`ttl_of_sync`]）。窗口守卫
/// 随 [`TtlWriteProbe::Proceed`] 交调用方持有跨全程（RAII 放闩），探针内提前
/// 返回即守卫同步释放。
enum TtlWriteProbe<'a, 'k, D: Device> {
  /// 须降级异步：取闩失败 / 存活不可判 / TTL 记录在磁盘候选
  Degrade,
  /// 键缺失或不存活：调用方应答 :0
  Zero,
  /// 键健在可安全写族操作：携窗口守卫与当前 TTL（`None` = 无 TTL 记录）
  Proceed(wkv::RmwWindow<'a, 'k, D>, Option<i64>),
}

/// 探针入口（三态语义见 [`TtlWriteProbe`]）
fn ttl_window_probe<'a, 'k, D: Device>(
  store: &'a wkv::BatchStoreSession<'a, D>,
  key: &'k [u8],
) -> Result<TtlWriteProbe<'a, 'k, D>, wkv::Error> {
  // 本键读改写窗口跨全程（RAII 放闩；事务态让闩，见 try_rmw_window 契约）
  let Some(window) = store.try_rmw_window(key) else {
    return Ok(TtlWriteProbe::Degrade);
  };
  // 存活判定 + Meta 域迁移 claim 复合判点（单源见 [`ttl_write_alive`]）
  match ttl_write_alive(store, key)? {
    None => return Ok(TtlWriteProbe::Degrade),
    // C# status != OK → 调用方回 :0
    Some(false) => return Ok(TtlWriteProbe::Zero),
    Some(true) => {}
  }
  let current = match ttl_of_sync(store, key)? {
    StoreResult::RecordOnDisk => return Ok(TtlWriteProbe::Degrade),
    StoreResult::NotFound => None,
    StoreResult::Success(cur) => cur,
  };
  Ok(TtlWriteProbe::Proceed(window, current))
}

fn expire_apply_sync<'a, D: Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
  expire_at_ticks: i64,
  opt: TtlOpt,
) -> Result<Option<i32>, wkv::Error> {
  // 窗口 + 存活判定 + TTL 读前置（单点 [`ttl_window_probe`]，窗口守卫跨全程）
  let (_window, current) = match ttl_window_probe(store, key)? {
    TtlWriteProbe::Degrade => return Ok(None),
    TtlWriteProbe::Zero => return Ok(Some(0)),
    TtlWriteProbe::Proceed(window, current) => (window, current),
  };
  // NX/XX/GT/LT 判定（镜像 wkv：多项同设须全部满足才放行，ticks 同域比较）
  let denied = match current {
    None => opt.xx || opt.gt,
    Some(c) => opt.nx || (opt.gt && expire_at_ticks <= c) || (opt.lt && expire_at_ticks >= c),
  };
  if denied {
    return Ok(Some(0));
  }
  if is_expired_or_now(expire_at_ticks, now_ticks()) {
    // 过去时间戳：物理删除（记录墓碑先行、随键 TTL 后剥——wkv 删内核级联
    // 承接；前置 del_ttl 已移除：先剥 TTL 而删记录途中失败/崩溃即留「值
    // 永生 + TTL 已亡」窗，票
    // wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal）
    return store
      .try_delete_sync(key)
      .map(|done| if done.is_ok() { Some(1) } else { None });
  }
  put_ttl_sync(store, key, expire_at_ticks).map(|done| if done { Some(1) } else { None })
}

/// PERSIST 应用内核（同步镜像 wkv::StoreSession::persist）
///
/// 返回 `Ok(None)` 须降级；`Ok(Some(1))` 已移除；`Ok(Some(0))` 无 TTL 或键缺失
///
/// 读改写原子窗口：与 [`expire_apply_sync`] 同一把本键桶排他闩，闩内完成
/// 「存活判定 → TTL 读 → del_ttl」全程，杜绝 ttl_of → del_ttl 间隙内并发
/// EXPIRE 写入的新 TTL 被误删（对标 C# UnifiedStore/RMWMethods.cs:
/// HandlePersistInPlaceUpdate 经 InternalRMW ephemeral 桶闩内读改写）；
/// 取闩失败沿 `Ok(None)` 降级交异步持闩版 `persist` 收口
///
/// libs/server/API/GarnetApiUnifiedCommands.cs:PERSIST
///（C# GarnetApi.PERSIST 转发 storageSession.RMW_UnifiedStore；
/// rust 无该 API 包装层，命令面 `network_persist` 直调本函数完成
/// 同一语义）
fn persist_apply_sync<'a, D: Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
) -> Result<Option<i32>, wkv::Error> {
  // 窗口 + 存活判定 + TTL 读前置（与 [`expire_apply_sync`] 同一判据源探针
  // [`ttl_window_probe`]：在册即降级异步由 wkv persist 判点统一拒绝；窗口
  // 守卫跨全程，杜绝 ttl_of → del_ttl 间隙并发 EXPIRE 交叠）
  let (_window, current) = match ttl_window_probe(store, key)? {
    TtlWriteProbe::Degrade => return Ok(None),
    TtlWriteProbe::Zero => return Ok(Some(0)),
    TtlWriteProbe::Proceed(window, current) => (window, current),
  };
  match current {
    // 无 TTL 记录：无可移除
    None => Ok(Some(0)),
    Some(_) => del_ttl_sync(store, key).map(|done| if done { Some(1) } else { None }),
  }
}

/// TTL 族读取结果三态
enum ExpiryRead {
  /// 键不存在（TTL → -2）
  Missing,
  /// 键存在但无 TTL（TTL → -1）
  NoExpiry,
  /// 有 TTL：绝对过期 .NET Ticks
  At(i64),
}

/// TTL/PTTL 读内核（同步镜像 wkv::StoreSession::pttl_ms）
///
/// `Ok(None)` 须降级（磁盘候选 / 过期清除待异步）
///
/// 单读正序收口（票 zcode-r127c-genexpire1）：先 [`ttl_of_sync_with_prefix`]
/// 一次取 TTL 旁路记录，再经 [`data_alive_domain_with_prefix`] 数据走查收尾，
/// 旁路记录热径读取由二压一（旧形 probe_alive 内 TTL 读 + [`ttl_of_sync`] 二次
/// 读，窗内并发删除——DEL 级联与 EXPIRE 过去戳臂皆记录墓碑先行、TTL 随后——
/// 第二次
/// 读折叠 NoExpiry，键正消亡/已消亡瞬态误报 -1「存在且永不过期」第三态）。
/// 判序「TTL 先、数据走查后」使折叠终判权归数据走查：数据三域皆缺一律
/// Missing(-2)，TTL 记录读到何值不改判——键全亡后出 -1 就此根除，残留两读
/// 互斥窗只余正数旧值翼与窗内真实无 TTL 观测，与 C# 单记录快照读同构不可达
/// 该第三态（对标 UnifiedStore/ReadMethods.cs:19-46 Reader 单次取回记录、
/// HandleTtl/HandleExpireTime 于 :162-188 同一快照内折叠 HasExpiration 与
/// Expiration，键亡经上层 status != OK 折 -2，KeyAdminCommands.cs:495-559；
/// 按甄别注记口径：窗收窄而非理论归零，勿作全称断言复核）。
/// [`expiretime_read_sync`] 直委托本函数，TTL/PTTL/EXPIRETIME/PEXPIRETIME
/// 四命令一臂单点收口。
///
/// 向量键收敛三域探针单源（不接登记表第四态）：TTL 族读写两侧同源同答——
/// 同键 TTL -2 与 EXPIRE :0 / PERSIST :0 一致，杜绝「读 -1 写 :0」合成缺口；
/// 详见 `doc/zh/deviations.md` §75 刻意收敛声明与对齐前提（杜绝单端开闸造成主从撕裂）。
///
/// libs/server/API/GarnetApiUnifiedCommands.cs:TTL
///（C# GarnetApi.TTL 转发 storageSession.Read_UnifiedStore（HandleTTL 读内核）；
/// rust 无该 API 包装层，命令面 `network_ttl` 直调本函数完成同一语义）
fn ttl_read_sync<'a, D: Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
) -> Result<Option<ExpiryRead>, wkv::Error> {
  let prefix = store.session_prefix();
  let prefix = prefix.as_slice();
  // TTL 旁路记录单读（唯一一次）：磁盘候选照旧整体降级异步裁决
  let expiry = match ttl_of_sync_with_prefix(store, prefix, key)? {
    StoreResult::RecordOnDisk => return Ok(None),
    // 墓碑/无记录/非法长度经 ttl_of_sync_with_prefix 已折 Success(None)，
    // NotFound 臂防御性同折
    StoreResult::NotFound | StoreResult::Success(None) => None,
    StoreResult::Success(Some(exp)) => Some(exp),
  };
  // 数据走查收尾（折叠终判权单点）：三域皆缺即 Missing，任域磁盘候选即降级
  match data_alive_domain_with_prefix(store, prefix, key)? {
    None => Ok(None),
    Some(None) => Ok(Some(ExpiryRead::Missing)),
    Some(Some(_)) => Ok(Some(match expiry {
      // 数据在场且无 TTL 记录：真实无 TTL 键，-1 唯一合法出口
      None => ExpiryRead::NoExpiry,
      // 到期判定严格小于，与探针路数据在场 TTL 门同口径（LogRecordUtils.cs:20）
      Some(exp) if is_expired(exp, now_ticks()) => ExpiryRead::Missing,
      Some(exp) => ExpiryRead::At(exp),
    })),
  }
}

/// TTL/PTTL/EXPIRETIME/PEXPIRETIME 查询内核（同步探测与应答帧生成单源）
#[inline]
fn query_expiry_sync<'a, D: Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
  calc_expiry: impl FnOnce(i64) -> i64,
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  match ttl_read_sync(store, key) {
    Ok(Some(read)) => {
      let value = match read {
        ExpiryRead::Missing => REPLY_TTL_MISSING,
        ExpiryRead::NoExpiry => REPLY_TTL_NO_EXPIRY,
        ExpiryRead::At(exp) => calc_expiry(exp),
      };
      output.write_resp_int(value);
      Ok(true)
    }
    Ok(None) => Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      Ok(true)
    }
  }
}
