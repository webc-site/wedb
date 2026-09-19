//! KEYS / RENAME / RENAMENX / COPY / TOUCH / EXPIRE / TTL 键名与生命周期管理命令（对标 libs/server/Resp/KeyAdminCommands.cs）

use wbase::{
  convert::{
    coarse_expire_ticks, expire_after_ms_to_ticks, expire_after_to_ticks,
    expire_at_milliseconds_to_ticks, expire_at_seconds_to_ticks,
    milliseconds_from_diff_utc_now_ticks, seconds_from_diff_utc_now_ticks,
    unix_time_in_milliseconds_from_ticks, unix_time_in_seconds_from_ticks,
  },
  num::strict_i64,
  time::now_ticks,
};
use wkv::{StoreResult, TtlOpt, is_expired_or_now};
use wresp::{
  check_args::{check_arg_count, unpack_args},
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
  resp::vector::vector_manager::VectorManager,
  storage::session::common::{
    TagRead, UserRead,
    etag_sync::{del_etag_sync, etag_of_sync, put_etag_sync},
    read_tag_sync, read_user_sync,
    ttl_sync::{
      del_ttl_sync, meta_collection_type_of, probe_alive, probe_alive_with_registry, put_ttl_sync,
      ttl_of_sync,
    },
  },
};

/// rust 自有枚举分派（C# NetworkEXPIRE 族；精确锚点见本文件 157 行）
///
/// EXPIRE 族命令形态（对标 libs/server/Resp/RespServerSession.cs 对
/// KeyAdminCommands.NetworkEXPIRE 的四路派发）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireCmd {
  Expire,
  Pexpire,
  Expireat,
  Pexpireat,
}

impl ExpireCmd {
  /// C# `command.ToString()` 的命令名（错误文案用）
  pub const fn as_str(self) -> &'static str {
    match self {
      Self::Expire => "EXPIRE",
      Self::Pexpire => "PEXPIRE",
      Self::Expireat => "EXPIREAT",
      Self::Pexpireat => "PEXPIREAT",
    }
  }

  /// 换算绝对过期 .NET Ticks（对标 KeyAdminCommands.cs:421-427 的换算 switch）
  ///
  /// EXPIRE → UtcNow.AddSeconds(...).UtcTicks、PEXPIRE → AddMilliseconds、
  /// EXPIREAT → UnixTimestampInSecondsToTicks、PEXPIREAT →
  /// UnixTimestampInMillisecondsToTicks；乘加/钳制公式统一委托
  /// [`wbase::convert`] 单点（超界钳到最大可表示 ticks，杜绝 debug 构建
  /// 溢出 panic，C# unchecked 环绕对应的确定性降级），与 AOF 重放端同函数。
  /// 刻意保持纯线性换算、不含粗化：重放端换算同样纯线性，二者的逐位一致
  /// 不变式由本文件 tests 焊死；C# 打包侧的 4-bit coarse 粗化（借用低 4 位
  /// 存 ExpireOption 的产物）对应本命令处理器 `network_expire` 的出口单点
  fn expire_at_ticks(self, expiration: i64) -> i64 {
    match self {
      Self::Expire => expire_after_to_ticks(now_ticks(), expiration),
      Self::Pexpire => expire_after_ms_to_ticks(now_ticks(), expiration),
      Self::Expireat => expire_at_seconds_to_ticks(expiration),
      Self::Pexpireat => expire_at_milliseconds_to_ticks(expiration),
    }
  }
}

/// rust 自有枚举分派（C# NetworkTTL；精确锚点见本文件 262 行）
///
/// TTL 族命令形态（TTL / PTTL）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TtlCmd {
  Ttl,
  Pttl,
}

/// rust 自有枚举分派（C# NetworkEXPIRETIME；精确锚点见本文件 301 行）
///
/// EXPIRETIME 族命令形态（EXPIRETIME / PEXPIRETIME）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ExpireTimeCmd {
  Expiretime,
  Pexpiretime,
}

impl RespServerSession {
  /// libs/server/Resp/KeyAdminCommands.cs:NetworkRENAME
  pub fn network_rename<'a, D: wdev::Device>(
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
  pub fn network_renamenx<'a, D: wdev::Device>(
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
  pub fn network_getdel<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "GETDEL") else {
      return Ok(true);
    };

    // 双域读：String 域命中取值删除；信封域命中（对象键）→ WRONGTYPE 且不删
    match read_user_sync(store, key, |v| v.to_vec()) {
      Ok(UserRead::Hit(val)) => {
        // 先删后答：删除遇异步闭环（环形页翻转/复合对象）时整体降级，
        // 避免已答出旧值而键未删成
        match store.try_delete_sync(key) {
          Ok(Ok(_)) => output.write_resp_bulk_string(&val),
          Ok(Err(_)) => return Ok(false),
          Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
        }
      }
      Ok(UserRead::WrongType) => {
        output.write_resp_error(cs::RESP_ERR_WRONG_TYPE);
      }
      Ok(UserRead::Missing) => {
        output.write_resp_null_ver(self.resp_protocol_version);
      }
      Ok(UserRead::Deferred) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkEXPIRE
  ///
  /// EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT 共同体：完整 C# 参数校验次序
  /// （个数 → 整数 → 非负 → NX/XX/GT/LT 选项组合），过期经
  /// [`crate::storage::session::common::ttl_sync`] 同步落 TTL 记录；磁盘候选/过期清除须异步时整体降级
  pub fn network_expire<'a, D: wdev::Device>(
    &mut self,
    command: ExpireCmd,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2..=4, output, command.as_str());
    let count = parse_state.len();

    let key = parse_state[0];
    let Some(expiration) = strict_i64(parse_state[1]) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };
    if expiration < 0 {
      // C# 文案（与 Redis 的 "must be positive" 不同，逐字节保留）
      abort_with_error_message(output, cs::RESP_ERR_INVALID_EXPIRE_TIME);
      return Ok(true);
    }

    // NX/XX/GT/LT 选项与两参组合（XXGT/XXLT），位运算解析并校验兼容规则
    let mut opt = TtlOpt::NONE;
    if count > 2 {
      let Some(first_opt) = try_get_expire_option(parse_state[2]) else {
        abort_with_unsupported_option(output, parse_state[2].as_str_safe());
        return Ok(true);
      };
      let mut combined = first_opt;
      if count > 3 {
        let Some(second_opt) = try_get_expire_option(parse_state[3]) else {
          abort_with_unsupported_option(output, parse_state[3].as_str_safe());
          return Ok(true);
        };
        let merged = first_opt | second_opt;
        let compatible = merged == ExpireOption::XXGT || merged == ExpireOption::XXLT;
        if first_opt == second_opt || !compatible {
          abort_with_error_message(
            output,
            "ERR NX and XX, GT or LT options at the same time are not compatible",
          );
          return Ok(true);
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

    // 键级过期同步入口的粗化单点（对标 C# NetworkEXPIRE:430 打包侧
    // `new ExpirationWithOption(ticks, option)` 的粗化半段，
    // ExpirationWithOption.cs:22-23）：C# 粗化是 ExpireOption 借用低 4 位的
    // 产物、只覆盖 EXPIRE 族，条件判定（GT/LT）与落盘同域；存储内核
    // put_ttl_sync 裸写不判。判据边界：SET/GETEX/RENAME 族对应 C#
    // MainStore/RMWMethods.cs TrySetExpiration/EvaluateExpire* 的裸 ticks
    // 路径，明确禁止一并粗化（异步/外部入口的对应粗化在 wkv `expire_at` 头部）
    let expire_at_ticks = coarse_expire_ticks(expire_at_ticks);

    match expire_apply_sync(store, key, expire_at_ticks, opt) {
      Ok(Some(applied)) => {
        // C# status != OK（键缺失等）回 :0；成功由存储回 :1（含过去时间戳
        // 立即删除的 Redis 7.4 语义 :1）
        if applied != 0 {
          write_raw(output, cs::RESP_RETURN_VAL_1);
        } else {
          write_raw(output, cs::RESP_RETURN_VAL_0);
        }
        Ok(true)
      }
      Ok(None) => Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        Ok(true)
      }
    }
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkPERSIST
  pub fn network_persist<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "PERSIST") else {
      return Ok(true);
    };

    match persist_apply_sync(store, key) {
      Ok(Some(removed)) => {
        output.write_resp_int(i64::from(removed));
        Ok(true)
      }
      Ok(None) => Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        Ok(true)
      }
    }
  }

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkTTL
  pub fn network_ttl<'a, D: wdev::Device>(
    &mut self,
    command: TtlCmd,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, command_name_of_ttl(command)) else {
      return Ok(true);
    };

    match ttl_read_sync(store, key, vector) {
      Ok(Some(read)) => {
        let value = match read {
          // 键缺失 → -2（C# status != OK → RESP_RETURN_VAL_N2）
          ExpiryRead::Missing => -2,
          ExpiryRead::NoExpiry => -1,
          // 对标 ConvertUtils：PTTL → MillisecondsFromDiffUtcNowTicks、
          // TTL → SecondsFromDiffUtcNowTicks（ReadMethods.cs:169-170 同源换算）
          ExpiryRead::At(exp) => {
            if command == TtlCmd::Pttl {
              milliseconds_from_diff_utc_now_ticks(exp)
            } else {
              seconds_from_diff_utc_now_ticks(exp)
            }
          }
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

  /// libs/server/Resp/KeyAdminCommands.cs:NetworkEXPIRETIME
  ///
  /// 参数个数错误恒报 EXPIRETIME（C# `nameof(RespCommand.EXPIRETIME)` quirk，
  /// PEXPIRETIME 同文案）
  pub fn network_expiretime<'a, D: wdev::Device>(
    &mut self,
    command: ExpireTimeCmd,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "EXPIRETIME") else {
      return Ok(true);
    };

    match expiretime_read_sync(store, key, vector) {
      Ok(Some(read)) => {
        let value = match read {
          ExpiryRead::Missing => -2,
          ExpiryRead::NoExpiry => -1,
          // 对标 ConvertUtils：PEXPIRETIME → UnixTimeInMillisecondsFromTicks、
          // EXPIRETIME → UnixTimeInSecondsFromTicks（ReadMethods.cs:183-184）
          ExpiryRead::At(exp) => {
            if command == ExpireTimeCmd::Pexpiretime {
              unix_time_in_milliseconds_from_ticks(exp)
            } else {
              unix_time_in_seconds_from_ticks(exp)
            }
          }
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
}

/// TTL/PTTL 命令名（错误文案用）
const fn command_name_of_ttl(command: TtlCmd) -> &'static str {
  match command {
    TtlCmd::Ttl => "TTL",
    TtlCmd::Pttl => "PTTL",
  }
}

/// RENAME/RENAMENX 共同内核（对标 libs/server/Storage/Session/UnifiedStore/
/// UnifiedStoreOps.cs 的 RENAME，C# 以 isNX 单实现双命令）
///
/// 序次对齐 C#：同键早退（首个检查，先于一切读取与 NX 判定）→ 双探旧键定
/// 物理域（String 命中 → 字符串；未命中探信封域，命中 → 对象键连同标签整体
/// 迁移；皆缺 → 向量登记表承接 [`rename_vector_set_sync`]，未登记 →
/// NOSUCHKEY）→ 旧键 TTL 记录 → [NX] 新键存活判定 → 写新键（同域写入，
/// SET 语义自动清新键残留 TTL，对标 C# 全新记录拷贝）→ TTL 随键迁移
///（C# TryCopyFrom 连同 Expiration 拷入新记录）→ 清旧键 TTL → 删旧键。
///
/// 新键为向量集时先显式清退（C# needDeleteNewKey：RecordType 变更/新键
/// 向量集须 DELETE(new)，登记表项与上下文随 [`VectorManager::
/// delete_vector_set`] 清理，杜绝幽灵残留）。
///
/// 任一步遇异步闭环（磁盘候选/环形页翻转）即整体降级 `Ok(false)`：调用方
/// 重试整条命令，旧键未删时幂等重放。`Ok(true)` 已闭环（应答已写入 output）
fn rename_sync<'a, D: wdev::Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  old_key: &[u8],
  new_key: &[u8],
  nx: bool,
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  // C# 同键早退：RENAME → OK；RENAMENX → 1（result=1，先于 NX 存在性判定）
  if old_key == new_key {
    if nx {
      output.write_resp_int(1);
    } else {
      write_raw(output, cs::RESP_OK);
    }
    return Ok(true);
  }

  // 三探旧键定物理域：String 域命中 → 字符串迁移；信封域命中 → 对象键迁移
  //（值首字节起即信封载荷，原样搬移不嗅探内容）；Meta 域命中（RangeIndex / 升阶键）
  // → 降级异步完整路由（树排空重建）；皆缺 → 向量登记表承接（C# 统一记录
  // RecordType=VectorManager.RecordType 的 rust 对偶，与 wkv 用户键删除单点的「双域未命中且登记表命中」缺席观测钩子同口径）；未登记 → NOSUCHKEY
  #[derive(Clone, Copy, PartialEq, Eq)]
  enum RenameDomain {
    Str,
    Obj,
  }
  let read_domain = |domain: RenameDomain| {
    let tag = match domain {
      RenameDomain::Str => KeyTag::String,
      RenameDomain::Obj => KeyTag::ObjectEnvelope,
    };
    read_tag_sync(store, old_key, tag, |v| v.to_vec())
  };
  let (old_val, domain) = match read_domain(RenameDomain::Str) {
    Ok(TagRead::Hit(val)) => (val, RenameDomain::Str),
    Ok(TagRead::Missing) => match read_domain(RenameDomain::Obj) {
      Ok(TagRead::Hit(val)) => (val, RenameDomain::Obj),
      Ok(TagRead::Missing) => {
        match read_tag_sync(store, old_key, KeyTag::Meta, meta_collection_type_of) {
          // Meta 域命中（RangeIndex / 升阶键）：须走慢路径进行树排空与重建
          Ok(TagRead::Hit(Some(_))) => return Ok(false),
          Ok(TagRead::Deferred) => return Ok(false),
          Ok(TagRead::Hit(None)) | Ok(TagRead::Missing) => {}
          Err(_) => {
            output.write_resp_error(RESP_ERR_GENERIC);
            return Ok(true);
          }
        }
        // 登记表域键单次外提（会话域内寻址）
        let prefix = store.session_prefix();
        if let Some(vm) = vector
          && vm.read_stored_index(prefix.as_slice(), old_key).is_some()
        {
          return rename_vector_set_sync(store, &prefix, vm, old_key, new_key, nx, output);
        }
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_NOSUCHKEY);
        return Ok(true);
      }
      // 旧键信封域有磁盘候选 / TTL 待裁决：降级
      Ok(TagRead::Deferred) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    },
    Ok(TagRead::Deferred) => return Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }
  };

  // 旧键 TTL 记录（RecordOnDisk：TTL 值在磁盘候选，降级）
  let old_ttl = match ttl_of_sync(store, old_key) {
    Ok(StoreResult::Success(ttl)) => ttl,
    Ok(StoreResult::NotFound) => None,
    Ok(StoreResult::RecordOnDisk) => return Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }
  };

  // 旧键 ETag 记录（对标 C# RENAME 搬迁记录连同可选 ETag 字段；
  // Ok(None)：磁盘候选降级）
  let old_etag = match etag_of_sync(store, old_key) {
    Ok(Some(etag)) => etag,
    Ok(None) => return Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }
  };

  // RENAMENX：新键存活（含过期裁决，双域 + 向量登记表第四态）→ 0，不动旧键。
  // 存活判据统一走探针单点，删去内联 read_stored_index 手兜底（去重）
  if nx {
    let nx_prefix = store.session_prefix();
    match probe_alive_with_registry(store, nx_prefix.as_slice(), new_key, vector) {
      Ok(Some(true)) => {
        output.write_resp_int(0);
        return Ok(true);
      }
      Ok(Some(false)) => {}
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
  }

  // 新键向量集清退（C# needDeleteNewKey 的显式 DELETE(new)：新键为向量集
  // 或 RecordType 变更时覆写前必删，登记项与上下文一并清理）。登记表未
  // 命中即无操作
  if let Some(vm) = vector {
    vm.delete_vector_set(store.session_prefix().as_slice(), new_key);
  }

  // 对象键迁移：覆写语义下先清退新键既有记录（含 String 域残留与随键 TTL，
  // 信封写入不自带跨域清退），再整体搬移信封载荷
  if domain == RenameDomain::Obj {
    match store.try_delete_sync(new_key) {
      Ok(Ok(_)) => {}
      Ok(Err(_)) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
  }
  let upsert_done = match domain {
    RenameDomain::Str => store.try_upsert_sync(new_key, old_val.as_slice()),
    RenameDomain::Obj => {
      store.try_upsert_tag_sync(new_key, KeyTag::ObjectEnvelope, old_val.as_slice())
    }
  };
  match upsert_done {
    Ok(Ok(_)) => {}
    Ok(Err(_)) => return Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }
  }
  // 对象域新键入账（对标 C# UnifiedStoreOps.RENAME 的 SET(newKey) →
  // WriteLogUpsert 全量条目：libs/server/Storage/Session/UnifiedStore/
  // UnifiedStoreOps.cs:RENAME）：resp 层直写信封域不经 StorageSession::upsert_tag
  // 的通知漏斗，此处显式补发 ObjectStoreUpsert——漏记则重放端只删旧键、
  // 新键无从建立，集合键丢失
  if domain == RenameDomain::Obj {
    let raw_key = store.session_tag_key(KeyTag::ObjectEnvelope, new_key);
    // AOF 入队失败不回滚已生效的 RENAME（主存先行语义），告警可见
    if let Err(e) = store
      .store
      .notify_envelope_upsert(raw_key.as_slice(), old_val.as_slice())
    {
      log::error!("RENAME 对象域 AOF 入队失败: {e}");
    }
  }
  if let Some(exp) = old_ttl {
    // RENAME 迁移裸写、不套粗化（对标 C# UnifiedStoreOps.cs:363 RENAME 用
    // `SET(newKey, in logRecord)` 把旧记录的 expiration optional 原样随记录
    // 迁移；MainStore/RMWMethods.cs:499-501 TrySetExpiration 亦收裸 ticks）：
    // old_ttl 取自 ttl_of_sync 的存量值，EXPIRE 族来源已在上游定域、SET 族
    // 本就裸值，此处二次粗化只会引入偏移，破坏逐位相等迁移语义
    match put_ttl_sync(store, new_key, exp) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
  }
  // 新键同步旧 etag：旧键有 etag 则回填，无 etag 则清退新键残留 etag
  //（旧键标签由尾部 try_delete_sync 级联清理）
  let etag_sync_res = if old_etag > NO_ETAG {
    put_etag_sync(store, new_key, old_etag)
  } else {
    del_etag_sync(store, new_key)
  };
  match etag_sync_res {
    Ok(true) => {}
    Ok(false) => return Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }
  }
  // C# DELETE 将记录连同 Expiration 一并移除：先清旧键 TTL 记录再删数据，
  // 避免孤儿 TTL 记录令后续读取长期走异步裁决慢路径
  if old_ttl.is_some() {
    match del_ttl_sync(store, old_key) {
      Ok(true) => {}
      Ok(false) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
  }
  match store.try_delete_sync(old_key) {
    Ok(Ok(_)) => {}
    Ok(Err(_)) => return Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }
  }

  if nx {
    output.write_resp_int(1);
  } else {
    write_raw(output, cs::RESP_OK);
  }
  Ok(true)
}

/// RENAME 向量集分支（对标 libs/server/Storage/Session/UnifiedStore/
/// UnifiedStoreOps.cs 的 RENAME 向量特判的登记表对偶；C# 四象限在 rust 双域
/// 模型下的投影：旧名必为向量集，新名可为 空缺 / 字符串 / 对象信封 / 向量集）
///
/// 次序对齐 C#：[NX] 新键存活（wkv 双域 + 登记表）→ 0 → 新键清退（wkv
/// 域残留删除，随键 TTL/ETag 级联；登记表项与上下文经 delete_vector_set
/// 显式清理）→ 登记表迁移（标记前快照拷入新名 → 开窗标记旧名 → 槽位
/// 同步 → 摘除旧名，窗口内清理被抑制）→ AOF 合成 RENAME 条目（arg1=
/// RecordType 哨兵，副本/恢复端迁移登记项）。
fn rename_vector_set_sync<'a, D: wdev::Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  prefix: &wval::SessionPrefixBuf,
  vm: &VectorManager,
  old_key: &[u8],
  new_key: &[u8],
  nx: bool,
  output: &mut Vec<u8>,
) -> wresp::Result<bool> {
  if nx {
    match probe_alive(store, new_key) {
      Ok(Some(true)) => {
        output.write_resp_int(0);
        return Ok(true);
      }
      Ok(Some(false)) => {}
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
    if vm.read_stored_index(prefix.as_slice(), new_key).is_some() {
      output.write_resp_int(0);
      return Ok(true);
    }
  }

  // 新键 wkv 域残留清退（C# DELETE(newKey)：新键为字符串/对象信封时覆写
  // 前显式删除；未命中即无操作，遇异步闭环整体降级）
  match store.try_delete_sync(new_key) {
    Ok(Ok(_)) => {}
    Ok(Err(_)) => return Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }
  }
  // 新键向量集清退（C# case #3/#4：新键为向量集须显式 DELETE）
  vm.delete_vector_set(prefix.as_slice(), new_key);

  // 登记表迁移（C# MarkSuppressCleanup(old) → SET(new) → UpdateHashSlot →
  // DELETE(old) 的窗口序；见 VectorManager::rename_vector_set）
  vm.rename_vector_set(prefix.as_slice(), old_key, new_key);

  // AOF 合成条目（主存先行语义，入队失败不回滚已生效的 RENAME）
  vm.replicate_vector_set_rename(prefix.as_slice(), old_key, new_key);

  if nx {
    output.write_resp_int(1);
  } else {
    write_raw(output, cs::RESP_OK);
  }
  Ok(true)
}

/// EXPIRE 应用内核（同步镜像 wkv::StoreSession::expire_at 的判定表，全链
/// .NET Ticks 同域比较）
///
/// 返回 `Ok(None)` 须降级异步；`Ok(Some(0))` 条件不满足/键缺失；
/// `Ok(Some(1))` 已设置（或过去时间戳已物理删除）
///
/// 过去时间戳路径与 C# 的刻意差异声明：C#
/// （libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs EXPIRE 分支 +
/// SessionFunctionsUtils.cs:EvaluateExpire）仅把记录过期设为过去值（惰性过期，
/// 后续读路径 CheckExpiry 清理），rust 在写命令内即物理删键（先删 TTL 记录
/// 再删数据，判定统一收敛 wkv `is_expired_or_now` 含相等口径）。两者应答
/// （:1）与最终可见状态（键消失）一致；AOF 语义 rust 为 DEL 形态墓碑、
/// C# 为 RMW-EXPIRE 条目（重放端 DELIFEXPIM 确定性），重放均幂等——主端
/// 已物理删除时重放 DEL 零写入无副作用。已知差异：WATCH 版本推进次数
/// （rust TTL 墓碑 + 数据墓碑两写 vs C# 单记录 RMW 一次）不影响事务失效
/// 判定的正确性（推进只多不少，方向单调）
fn expire_apply_sync<'a, D: wdev::Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
  expire_at_ticks: i64,
  opt: TtlOpt,
) -> Result<Option<i32>, wkv::Error> {
  // 存活判定（含过期键视同缺失的裁决；过期清除须异步时降级）
  match probe_alive(store, key)? {
    None => Ok(None),
    // C# status != OK → 调用方回 :0
    Some(false) => Ok(Some(0)),
    Some(true) => {
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
  }
}

/// PERSIST 应用内核（同步镜像 wkv::StoreSession::persist）
///
/// 返回 `Ok(None)` 须降级；`Ok(Some(1))` 已移除；`Ok(Some(0))` 无 TTL 或键缺失
fn persist_apply_sync<'a, D: wdev::Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
) -> Result<Option<i32>, wkv::Error> {
  match probe_alive(store, key)? {
    None => Ok(None),
    Some(false) => Ok(Some(0)),
    Some(true) => match ttl_of_sync(store, key)? {
      StoreResult::RecordOnDisk => Ok(None),
      // 无 TTL 记录：无可移除
      StoreResult::NotFound | StoreResult::Success(None) => Ok(Some(0)),
      StoreResult::Success(Some(_)) => {
        del_ttl_sync(store, key).map(|done| if done { Some(1) } else { None })
      }
    },
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
fn ttl_read_sync<'a, D: wdev::Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
  vector: Option<&VectorManager>,
) -> Result<Option<ExpiryRead>, wkv::Error> {
  let prefix = store.session_prefix();
  // 存活判定接向量登记表第四态：存活向量键无过期记录时 TTL 回 -1（对标
  // C# HandleTtl `HasExpiration ? Expiration : -1`），不再误判缺失回 -2；
  // TTL 值仍由 ttl_of_sync 单点裁决，不新增第二次 TTL 读
  match probe_alive_with_registry(store, prefix.as_slice(), key, vector)? {
    None => Ok(None),
    // C# status != OK（键缺失）→ :n2
    Some(false) => Ok(Some(ExpiryRead::Missing)),
    Some(true) => match ttl_of_sync(store, key)? {
      StoreResult::RecordOnDisk => Ok(None),
      StoreResult::NotFound | StoreResult::Success(None) => Ok(Some(ExpiryRead::NoExpiry)),
      StoreResult::Success(Some(exp)) => Ok(Some(ExpiryRead::At(exp))),
    },
  }
}

/// EXPIRETIME/PEXPIRETIME 读内核（同步镜像 wkv::StoreSession::expiretime_ms）
///
/// `Ok(None)` 须降级
fn expiretime_read_sync<'a, D: wdev::Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
  vector: Option<&VectorManager>,
) -> Result<Option<ExpiryRead>, wkv::Error> {
  ttl_read_sync(store, key, vector)
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

  use super::ExpireCmd;

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
    assert_eq!(ExpireCmd::Pexpireat.as_str(), "PEXPIREAT");
  }
}
