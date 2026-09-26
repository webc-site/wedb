//! KEYS / RENAME / RENAMENX / EXPIRE / TTL 键名与生命周期管理命令（对标 libs/server/Resp/KeyAdminCommands.cs）

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
use wval::{KeyTag, NO_ETAG};

use super::super::resp_server_session::RespServerSession;
use crate::{
  read_user_or_bail,
  resp::vector::vector_manager::VectorManager,
  storage::session::common::{
    TagRead,
    etag_sync::{del_etag_sync, etag_of_sync_with_prefix, put_etag_sync},
    read_tag_sync_with_prefix, read_user_sync,
    ttl_sync::{
      data_alive_domain_with_prefix, del_ttl_sync, meta_collection_type_of, probe_alive_domain,
      probe_alive_with_registry, put_ttl_sync, registry_alive, ttl_of_sync,
      ttl_of_sync_with_prefix,
    },
  },
};

/// 0/1 旗标应答帧表（下标即「未生效 / 已生效」，C# `RESP_RETURN_VAL_0`、`_1`）
pub(crate) const FLAG_FRAMES: [&[u8]; 2] = [cs::RESP_RETURN_VAL_0, cs::RESP_RETURN_VAL_1];
pub(crate) const REPLY_TTL_MISSING: i64 = -2;
pub(crate) const REPLY_TTL_NO_EXPIRY: i64 = -1;

/// rust 自有枚举分派（C# NetworkEXPIRE 族；精确锚点见本文件 157 行）
///
/// EXPIRE 族命令形态（对标 libs/server/Resp/RespServerSession.cs 对
/// KeyAdminCommands.NetworkEXPIRE 的四路派发）
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
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
  fn expire_at_ticks(self, expiration: i64) -> i64 {
    compute_expiration_ticks(
      now_ticks(),
      expiration,
      self.is_milliseconds(),
      self.is_timestamp(),
    )
  }
}

/// rust 自有枚举分派（C# NetworkTTL；精确锚点见本文件 262 行）
///
/// TTL 族命令形态（TTL / PTTL）
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum TtlCmd {
  Ttl,
  Pttl,
}

/// rust 自有枚举分派（C# NetworkEXPIRETIME；精确锚点见本文件 301 行）
///
/// EXPIRETIME 族命令形态（EXPIRETIME / PEXPIRETIME）
#[derive(Debug, Clone, Copy, PartialEq, Eq, strum::IntoStaticStr)]
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

/// RENAME/RENAMENX 成应答帧（同键早退与尾部收尾两处共用）
#[inline]
pub(crate) fn reply_renamed(nx: bool, output: &mut Vec<u8>) {
  if nx {
    output.write_resp_int(1);
  } else {
    write_raw(output, cs::RESP_OK);
  }
}

impl RespServerSession {
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRENAME
  pub fn network_rename<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([old_key, new_key]) = unpack_args(parse_state, output, "RENAME") else {
      return Ok(true);
    };
    rename_sync(store, old_key, new_key, false, vector, output)
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRENAMENX
  pub fn network_renamenx<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([old_key, new_key]) = unpack_args(parse_state, output, "RENAMENX") else {
      return Ok(true);
    };
    rename_sync(store, old_key, new_key, true, vector, output)
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkGETDEL
  pub fn network_getdel<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "GETDEL") else {
      return Ok(true);
    };

    // 读改写窗口 + 域判探针（零拷贝：只裁 WRONGTYPE/缺席/降级，应答值由取删
    // 一体内核捕获）同窗收口（对标 C# NetworkGETDEL 单次 RMW，RMWMethods.cs:763-768）
    let Some(_window) = store.try_rmw_window(key) else {
      return Ok(false);
    };

    let start_len = output.len();
    read_user_or_bail!(read_user_sync(store, key, None, |_| ()), output, {
      output.write_resp_null_ver(self.resp_protocol_version);
      return Ok(true);
    });
    // 取删一体收口（C# 锁内 CopyRespTo + ExpireAndStop 的同原子性：
    // RMWMethods.cs:764-768 + InternalRMW.cs:70——应答值即本次实际摘除记录
    // 的值，杜绝探针读 old、并发盲写 SET 落 new、删除摘走 new 的答旧删新
    // 撕裂）；闭包直写 output 消除中间 Vec 暂存；摘除空手（并发盲 DEL 已先行）沿串行序 DEL→GETDEL 答 nil
    match store.try_take_sync_with(key, |val| {
      output.write_resp_bulk_string(val);
    }) {
      Ok(Ok(Some(()))) => {}
      Ok(Ok(None)) => {
        output.truncate(start_len);
        output.write_resp_null_ver(self.resp_protocol_version);
      }
      // 先删后答的降级口径不变：摘除遇异步闭环（环形页翻转/冷数据）时
      // 整体降级，避免已答出值而键未删成
      Ok(Err(_)) => {
        output.truncate(start_len);
        return Ok(false);
      }
      Err(_) => {
        output.truncate(start_len);
        output.write_resp_error(RESP_ERR_GENERIC);
      }
    }
    Ok(true)
  }

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

/// RENAME/RENAMENX 共同内核（对标 libs/server/Storage/Session/UnifiedStore/
/// UnifiedStoreOps.cs 的 RENAME，C# 以 isNX 单实现双命令）
///
/// RENAMENX 命令级锚点（isNX=true 分支：新键存活 → result=0 不动旧键）：
/// libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:RENAMENX
///
/// 序次对齐 C#：同键早退（首个检查，先于一切读取与 NX 判定）→ 双探旧键定
/// 物理域（String 命中 → 字符串；未命中探信封域，命中 → 对象键连同标签整体
/// 迁移；皆缺 → 向量登记表承接，未登记 →
/// NOSUCHKEY）→ 旧键 TTL 记录 → [NX] 新键存活判定（唯一 NX 判据）→ 对象域
/// 新键旁域预清 → 写新键（isNX/非 isNX 共用 SET（upsert）单写入口，对标 C#
/// :363 单条 `SET(newKey, logRecord)`）→ TTL 随键迁移
///（C# TryCopyFrom 连同 Expiration 拷入新记录）→ 清旧键 TTL → 删旧键。
///
/// 新键清退（C# needDeleteNewKey 与 isNX 无涉，UnifiedStoreOps.cs:338-347）：
/// 向量集项登记表命中先降级慢路径清退（快路径不做真异步登记摘除）；对象键
/// 迁移无条件经 [`wkv::BatchStoreSession::try_delete_sync`] 级联清退新键
/// String 残留与随键 TTL/ETag 旁域——与慢路径 `delete_string(new_key)`
/// （slow.rs 无条件臂）同一删除单点，杜绝探针判死的过期残留经信封写入后
/// 双域并存或新键随残 TTL 隐死。
///
/// 任一步遇异步闭环（磁盘候选/环形页翻转）即整体降级 `Ok(false)`：调用方
/// 重试整条命令，旧键未删时幂等重放。`Ok(true)` 已闭环（应答已写入 output）
fn rename_sync<'a, D: Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  old_key: &[u8],
  new_key: &[u8],
  nx: bool,
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  // C# 同键早退：RENAME → OK；RENAMENX → 1（result=1，先于 NX 存在性判定）
  if old_key == new_key {
    reply_renamed(nx, output);
    return Ok(true);
  }

  // 会话前缀单次外提（循环前缀外提对位：本函数三域探针、TTL/ETag 记录读、
  // registry 判定、NX 存活探测与向量集迁移统一复用该缓冲，消除逐探点重读
  // ns/db 原子变量与重算 Varint；对偶慢路径 rename_slow 同一外提口径）
  let prefix = store.session_prefix();

  // 双键读改写窗口（票 zcode-r15-generic 发现一，对标 C# UnifiedStoreOps.RENAME
  // 的 SaveKeyEntryToLock(oldKey, Exclusive) + SaveKeyEntryToLock(newKey,
  // Exclusive) 双键排他事务锁：UnifiedStoreOps.cs:241 起「探 new → GET old →
  // [NX] 判定 → DELETE(new) → SET(new) → DELETE(old)」全程锁内一体，并发
  // SET/DEL 与 RENAME 严格互斥）：桶序取闩（哈希升序定序防 RENAME a b /
  // RENAME b a 交叉死锁），闩内完成本函数全序列，杜绝三探旧键 → 写新删旧
  // 间隙的并发 SET new 覆写丢失与并发 DEL old 后键复活。失闩沿既有 Ok(false)
  // 降级慢路径同段持窗重放，绝不自旋等闩
  let Some(_windows) = store.try_rmw_window_sorted([old_key, new_key]) else {
    return Ok(false);
  };

  // 三探旧键定物理域：String 域命中 → 字符串迁移；信封域命中 → 对象键迁移
  //（值首字节起即信封载荷，原样搬移不嗅探内容）；Meta 域命中（RangeIndex / 升阶键）
  // → 降级异步完整路由（树排空重建）；皆缺 → 向量登记表承接（C# 统一记录
  // RecordType=VectorManager.RecordType 的 rust 对偶，与 wkv 用户键删除单点的「双域未命中且登记表命中」缺席观测钩子同口径）；未登记 → NOSUCHKEY
  #[derive(Clone, Copy, PartialEq, Eq)]
  enum RenameDomain {
    Str,
    Obj,
  }
  impl RenameDomain {
    /// 本域对应的物理键标签（旧键双探与新键补偿读共用同一映射单源）
    const fn tag(self) -> KeyTag {
      match self {
        Self::Str => KeyTag::String,
        Self::Obj => KeyTag::ObjectEnvelope,
      }
    }
  }
  // String → ObjectEnvelope 顺序双探：两域共用同一读内核与同一三态收尾臂
  //（命中即定域早退，杜绝逐域抄一套 Deferred/Err 臂）
  let mut hit = None;
  for domain in [RenameDomain::Str, RenameDomain::Obj] {
    match read_tag_sync_with_prefix(store, prefix.as_slice(), old_key, domain.tag(), |v| {
      v.to_vec()
    }) {
      Ok(TagRead::Hit(val)) => {
        hit = Some((val, domain));
        break;
      }
      // 本域内存确认缺席 → 续探下一域
      Ok(TagRead::Missing) => {}
      // 旧键本域有磁盘候选 / TTL 待裁决：降级
      Ok(TagRead::Deferred) => return Ok(false),
      Err(_) => bail_err_frame!(output),
    }
  }
  let (old_val, domain) = match hit {
    Some(found) => found,
    // 双域皆缺：Meta 域探针 + 向量登记表第四态 + NOSUCHKEY（本分支各臂皆收尾）
    None => {
      match read_tag_sync_with_prefix(
        store,
        prefix.as_slice(),
        old_key,
        KeyTag::Meta,
        meta_collection_type_of,
      ) {
        // Meta 域命中（RangeIndex / 升阶键）：须走慢路径进行树排空与重建
        Ok(TagRead::Hit(Some(_))) | Ok(TagRead::Deferred) => return Ok(false),
        Ok(TagRead::Hit(None)) | Ok(TagRead::Missing) => {}
        Err(_) => bail_err_frame!(output),
      }
      // 登记表域键单次外提（会话域内寻址）；「三域皆缺 → 是否向量集」的
      // 第四态判定与存活折叠同一判据源（ttl_sync registry_alive 单点，
      // 已含「有登记表」前置），臂内不再手抄 read_stored_index 命中式。
      // 登记写透 async 化后快路径不再承接向量集迁移：登记命中即整体
      // 降级慢路径（rename_slow 臂真异步清退新键 + 迁移 + 合成写闭环）
      if registry_alive(vector, prefix.as_slice(), old_key) {
        return Ok(false);
      }
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_NOSUCHKEY);
      return Ok(true);
    }
  };

  // 旧键 TTL 记录（RecordOnDisk：TTL 值在磁盘候选，降级）
  let old_ttl = match ttl_of_sync_with_prefix(store, prefix.as_slice(), old_key) {
    Ok(StoreResult::Success(ttl)) => ttl,
    Ok(StoreResult::NotFound) => None,
    Ok(StoreResult::RecordOnDisk) => return Ok(false),
    Err(_) => bail_err_frame!(output),
  };

  // 旧键 ETag 记录（对标 C# RENAME 搬迁记录连同可选 ETag 字段；
  // Ok(None)：磁盘候选降级）
  let old_etag = match etag_of_sync_with_prefix(store, prefix.as_slice(), old_key) {
    Ok(Some(etag)) => etag,
    Ok(None) => return Ok(false),
    Err(_) => bail_err_frame!(output),
  };

  // RENAMENX：新键存活（含过期裁决，双域 + 向量登记表第四态）→ 0，不动旧键。
  // 存活判据接探针三态收尾单源（与 EXISTS 计数 / RESTORE NX 同一判序，
  // 删去内联 read_stored_index 手兜底与本处手抄四臂，去重）
  if nx && probe_alive_or_bail!(store, prefix.as_slice(), new_key, vector, output) {
    output.write_resp_int(0);
    return Ok(true);
  }

  // 覆写语义下先清退新键既有记录（新键向量集清退 + 对象键迁移清退既有记录与随键 TTL）。
  // 登记写透 async 化后快路径不再承接向量集清退：新键登记命中即整体降级
  // 慢路径（rename_slow 臂真异步清退 + 迁移闭环）；未命中零操作放行。
  // 向量半爿仍门 !nx：存活探针（上方 nx 臂）已折叠 registry_alive 第四态，
  // NX 通过即登记表不命中，无须二次判定（慢径臂为无条件幂等清退，同口径不
  // 分叉）。对象域旁域清退不门 !nx（对标 C# needDeleteNewKey 与 isNX 无涉，
  // UnifiedStoreOps.cs:338-347；位置对偶慢径 delete_string(new_key)，
  // 该句在 NX 判定之后、域分派之前无条件执行）：探针判死的过期 String 残留
  // 物理仍在，信封写入既不自带跨域清退、亦按 RMW 语义保留残留 TTL，缺此
  // 预清即出双域并存（读面 String 优先命中 → WRONGTYPE）或新键随残 TTL
  // 整键隐死。键不存在时本原语回 Ok(Ok(false)) 零副作用（墓碑与 WATCH 推进
  // 由 wkv 用户键删除单点统一收口，与 DEL 缺席形同向）
  if !nx && registry_alive(vector, prefix.as_slice(), new_key) {
    return Ok(false);
  }
  if domain == RenameDomain::Obj {
    let pre_clear = store.try_delete_sync(new_key);
    bail_store_step!(output, pre_clear, Ok(Ok(_)), Ok(Err(_)));
  }

  // 写新键：String / 信封域各取其域的单套 SET（upsert）写入口，isNX 不另起
  // 第二写原语（对标 C# 「GET(newKey) 存活判定即唯一 NX 裁决（:301-306，
  // CheckExpiry 令过期键判 NOTFOUND）→ SET(newKey) 整记录覆写（:363）两臂
  // 共用」，与慢径 Str 臂 upsert_string 同一原语对位）。纯物理 NX 插口在
  // 探针已放行后必不拒（同闩窗内无并发写者），残留过期记录即「探针判死、
  // 物理在场」，误用插口即出假 :0 应答（实际可 rename）
  let write_res = match domain {
    RenameDomain::Str => store.try_upsert_sync(new_key, old_val.as_slice()),
    RenameDomain::Obj => {
      store.try_upsert_tag_sync(new_key, KeyTag::ObjectEnvelope, old_val.as_slice())
    }
  };
  bail_store_step!(output, write_res, Ok(Ok(_)), Ok(Err(_)));
  // 对象域新键入账（对标 C# UnifiedStoreOps.RENAME 的 SET(newKey) →
  // WriteLogUpsert 全量条目：libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:RENAME）
  // ：resp 层直写信封域不经 StorageSession::upsert_tag
  // 的通知漏斗，此处显式补发 ObjectStoreUpsert——漏记则重放端只删旧键、
  // 新键无从建立，集合键丢失
  if domain == RenameDomain::Obj {
    let raw_key = store.session_tag_key(KeyTag::ObjectEnvelope, new_key);
    // AOF 入队失败按 error.rs AofEnqueue 契约以错误拒绝本命令（票
    // wnode-objrmw-aof-enqueue-swallow-matrix，与 r167c-aoffail 同族收口）：
    // 新键写已生效不回滚，漏记即副本只删旧键、新键无从建立（本域注释自证
    // 危害），吞错冒答 +OK 是假成功，禁沿用；错误帧与本域其余存储失败臂同形
    if let Err(e) = store.notify_envelope_upsert(raw_key.as_slice(), old_val.as_slice()) {
      log::error!("RENAME 对象域 AOF 入队失败，命令拒绝: {e}");
      bail_err_frame!(output);
    }
  }
  if let Some(exp) = old_ttl {
    // RENAME 迁移裸写、不套粗化（对标 C# UnifiedStoreOps.cs:363 RENAME 用
    // `SET(newKey, in logRecord)` 把旧记录的 expiration optional 原样随记录
    // 迁移；MainStore/RMWMethods.cs:499-501 TrySetExpiration 亦收裸 ticks）：
    // old_ttl 取自 ttl_of_sync 的存量值，EXPIRE 族来源已在上游定域、SET 族
    // 本就裸值，此处二次粗化只会引入偏移，破坏逐位相等迁移语义
    let ttl_migrated = put_ttl_sync(store, new_key, exp);
    bail_store_step!(output, ttl_migrated, Ok(true), Ok(false));
  }
  // 新键同步旧 etag：旧键有 etag 则回填，无 etag 则清退新键残留 etag
  //（旧键标签由尾部 try_delete_sync 级联清理）
  let etag_sync_res = if old_etag > NO_ETAG {
    put_etag_sync(store, new_key, old_etag)
  } else {
    del_etag_sync(store, new_key)
  };
  bail_store_step!(output, etag_sync_res, Ok(true), Ok(false));
  // C# DELETE 将记录连同 Expiration 一并移除：先清旧键 TTL 记录再删数据，
  // 避免孤儿 TTL 记录令后续读取长期走异步裁决慢路径
  if old_ttl.is_some() {
    bail_store_step!(output, del_ttl_sync(store, old_key), Ok(true), Ok(false));
  }
  match store.try_delete_sync(old_key) {
    Ok(Ok(true)) => {}
    Ok(Ok(false)) => {
      // 补偿臂：确认待删内容确系本次所写再删，杜绝裸删吞并发写
      //（两域同一读内核、同一四态折叠，域标签经 RenameDomain::tag 单源）
      let is_our_val =
        match read_tag_sync_with_prefix(store, prefix.as_slice(), new_key, domain.tag(), |v| {
          v == old_val.as_slice()
        }) {
          Ok(TagRead::Hit(matches)) => matches,
          Ok(TagRead::Missing) | Err(_) => false,
          // 磁盘候选无从比对：沿用原判序保守视为本次所写
          Ok(TagRead::Deferred) => true,
        };
      if is_our_val {
        if old_ttl.is_some() {
          let _ = del_ttl_sync(store, new_key);
        }
        if old_etag > NO_ETAG {
          let _ = del_etag_sync(store, new_key);
        }
        let del_res = match domain {
          RenameDomain::Str => store.try_delete_sync(new_key),
          RenameDomain::Obj => store.try_delete_tag_sync(new_key, KeyTag::ObjectEnvelope),
        };
        bail_store_step!(output, del_res, Ok(Ok(_)), Ok(Err(_)));
      }
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_NOSUCHKEY);
      return Ok(true);
    }
    Ok(Err(_)) => return Ok(false),
    Err(_) => bail_err_frame!(output),
  }

  reply_renamed(nx, output);
  Ok(true)
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
/// 物理删键（先删 TTL 记录
/// 再删数据，判定统一收敛 wkv `is_expired_or_now` 含相等口径）。两者应答
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
/// （对标 C# UnifiedStore/RMWMethods.cs:HandleExpireInPlaceUpdate 经 InternalRMW
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
fn expire_apply_sync<'a, D: Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
  expire_at_ticks: i64,
  opt: TtlOpt,
) -> Result<Option<i32>, wkv::Error> {
  // 本键读改写窗口跨全程（RAII 放闩；事务态让闩，见 try_rmw_window 契约）
  let Some(_window) = store.try_rmw_window(key) else {
    return Ok(None);
  };
  // 存活判定 + Meta 域迁移 claim 复合判点（单源见 [`ttl_write_alive`]）
  match ttl_write_alive(store, key)? {
    None => return Ok(None),
    // C# status != OK → 调用方回 :0
    Some(false) => return Ok(Some(0)),
    Some(true) => {}
  }
  let current = match ttl_of_sync(store, key)? {
    StoreResult::RecordOnDisk => return Ok(None),
    StoreResult::NotFound => None,
    StoreResult::Success(cur) => cur,
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
    // 过去时间戳：物理删除（先删 TTL 再删数据，镜像 purge_expired 顺序）
    if !del_ttl_sync(store, key)? {
      return Ok(None);
    }
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
  let Some(_window) = store.try_rmw_window(key) else {
    return Ok(None);
  };
  // 三域域探针 + Meta 域迁移 claim 复合判点（与 [`expire_apply_sync`] 同
  // 一判据源 [`ttl_write_alive`]：在册即降级异步由 wkv persist 判点统一拒绝）
  match ttl_write_alive(store, key)? {
    None => return Ok(None),
    Some(false) => return Ok(Some(0)),
    Some(true) => {}
  }
  match ttl_of_sync(store, key)? {
    StoreResult::RecordOnDisk => Ok(None),
    // 无 TTL 记录：无可移除
    StoreResult::NotFound | StoreResult::Success(None) => Ok(Some(0)),
    StoreResult::Success(Some(_)) => {
      del_ttl_sync(store, key).map(|done| if done { Some(1) } else { None })
    }
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
/// 读，窗内并发删除——EXPIRE 过去戳臂与 DEL 级联皆先删 TTL 再删数据——第二次
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

#[cfg(test)]
mod tests {
  use wbase::{
    convert::{
      TICKS_PER_SECOND, expire_after_ms_to_ticks, expire_after_to_ticks,
      expire_at_milliseconds_to_ticks, expire_at_seconds_to_ticks,
    },
    time::now_ticks,
  };

  use super::{ExpireCmd, ExpireTimeCmd, TtlCmd};

  /// 命令端与 AOF 重放端的换算逐位一致（同输入同函数）：
  /// 绝对域（EXPIREAT/PEXPIREAT）命令端换算面与 wbase 单点恒等
  #[test]
  fn expire_at_matches_replay_conversion() {
    for seconds in [0, 1, 1_700_000_000, 4_102_444_799, i64::MAX, -1] {
      assert_eq!(
        ExpireCmd::Expireat.expire_at_ticks(seconds),
        expire_at_seconds_to_ticks(seconds)
      );
    }
    for millis in [0, 1, 1_700_000_000_000, i64::MAX, -1] {
      assert_eq!(
        ExpireCmd::Pexpireat.expire_at_ticks(millis),
        expire_at_milliseconds_to_ticks(millis)
      );
    }
  }

  /// 相对域（EXPIRE/PEXPIRE）命令端与重放端共享同一饱和乘加单点；
  /// 非饱和路径时钟在两次调用间推进，以 1 秒容差断言同源
  #[test]
  fn expire_after_matches_replay_conversion() {
    const DRIFT: i64 = TICKS_PER_SECOND;
    let now = now_ticks();
    for seconds in [0, 1, 60, 86_400] {
      let cmd_ticks = ExpireCmd::Expire.expire_at_ticks(seconds);
      assert!(
        (cmd_ticks - expire_after_to_ticks(now, seconds)).abs() <= DRIFT,
        "EXPIRE {seconds}s: {cmd_ticks} vs {}",
        expire_after_to_ticks(now, seconds)
      );
    }
    for millis in [0, 1, 500, 86_400_000] {
      let cmd_ticks = ExpireCmd::Pexpire.expire_at_ticks(millis);
      assert!(
        (cmd_ticks - expire_after_ms_to_ticks(now, millis)).abs() <= DRIFT,
        "PEXPIRE {millis}ms: {cmd_ticks} vs {}",
        expire_after_ms_to_ticks(now, millis)
      );
    }
    // 饱和边界：与 now 无关，重放端与命令端逐位一致（同钉 i64::MAX）
    assert_eq!(
      ExpireCmd::Expire.expire_at_ticks(i64::MAX),
      expire_after_to_ticks(now_ticks(), i64::MAX)
    );
    assert_eq!(
      ExpireCmd::Pexpire.expire_at_ticks(i64::MAX),
      expire_after_ms_to_ticks(now_ticks(), i64::MAX)
    );
  }

  /// 命令形态枚举文本与 C# command.ToString() 对齐（错误文案面）
  #[test]
  fn cmd_as_str() {
    assert_eq!(ExpireCmd::Expire.as_str(), "EXPIRE");
    assert_eq!(ExpireCmd::Pexpire.as_str(), "PEXPIRE");
    assert_eq!(ExpireCmd::Expireat.as_str(), "EXPIREAT");
    assert_eq!(ExpireCmd::Pexpireat.as_str(), "PEXPIREAT");
    assert_eq!(<&'static str>::from(TtlCmd::Ttl), "TTL");
    assert_eq!(<&'static str>::from(TtlCmd::Pttl), "PTTL");
    assert_eq!(
      <&'static str>::from(ExpireTimeCmd::Expiretime),
      "EXPIRETIME"
    );
    assert_eq!(
      <&'static str>::from(ExpireTimeCmd::Pexpiretime),
      "PEXPIRETIME"
    );
  }
}
