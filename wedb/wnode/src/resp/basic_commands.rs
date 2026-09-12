use std::{
  fmt::Write as _,
  str,
  time::{SystemTime, UNIX_EPOCH},
};

use wbase::{
  convert::{TICKS_PER_MILLISECOND, TICKS_PER_SECOND, UNIX_EPOCH_TICKS},
  time::now_ticks,
};
use wresp::{
  RespSliceExt, RespVecExt, cmd_strings as cs,
  cmd_strings::{
    abort_with_error_message, abort_with_unknown_subcommand, abort_with_unsupported_option,
    abort_with_wrong_number_of_arguments, write_error_raw, write_map_len_resp2, write_raw,
  },
};

use super::{
  parser::session_parse_state::{strict_f64, strict_i32, strict_i64},
  resp_server_session::RespServerSession,
  ttl_sync::{del_ttl_sync, probe_alive, put_ttl_sync, read_adjudicated_sync, ttl_of_sync},
};
use crate::session_parse_state_extensions::{
  SimpleRespKeySpec, extract_keys_and_flags_from_slice, extract_keys_from_slice,
};

/// 字符串类命令负载上限（libs/server/Resp/Bitmap/BitmapManager.cs:MaxBitmapPayloadBytes，
/// Bitmap 域共用）
pub(crate) const MAX_STRING_PAYLOAD_BYTES: usize = 512 * 1024 * 1024;
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER
const ERR_NOT_INTEGER: &str = cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_OFFSETOUTOFRANGE
const ERR_OFFSET_OUT_OF_RANGE: &str = cs::RESP_ERR_GENERIC_OFFSETOUTOFRANGE;
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_STRING_EXCEEDS_MAX_SIZE
const ERR_STRING_EXCEEDS_MAX: &str = "ERR string exceeds maximum allowed size (proto-max-bulk-len)";
/// libs/server/Auth/GarnetNoAuthAuthenticator.cs:CanAuthenticate
///
/// rust 会话尚未接线认证器；C# 默认（无 AuthSettings）即 NoAuth 认证器，
/// CanAuthenticate = false，AUTH/HELLO 认证按该路径报错（文案与 C# 逐字节一致）
const CAN_AUTHENTICATE: bool = false;

/// TimeSpan.MaxValue.TotalSeconds 对标上限（libs/server/Resp/BasicCommands.cs:NetworkGETEX）
pub const MAX_TIMESPAN_SECONDS: i64 = i64::MAX / TICKS_PER_SECOND;
/// TimeSpan.MaxValue.TotalMilliseconds 对标上限（libs/server/Resp/BasicCommands.cs:NetworkGETEX）
pub const MAX_TIMESPAN_MILLISECONDS: i64 = i64::MAX / TICKS_PER_MILLISECOND;
/// DateTimeOffset.MaxValue.ToUnixTimeSeconds() 对标上限（libs/server/Resp/BasicCommands.cs:NetworkGETEX）
pub const MAX_UNIX_TIME_SECONDS: i64 = 253_402_300_799;
/// DateTimeOffset.MaxValue.ToUnixTimeMilliseconds() 对标上限（libs/server/Resp/BasicCommands.cs:NetworkGETEX）
pub const MAX_UNIX_TIME_MILLISECONDS: i64 = 253_402_300_799_999;

/// SET 选项解析后的条件写命令形态（对标 libs/server/Resp/BasicCommands.cs:NetworkSETEXNX
/// 派发到的 RespCommand：SET/SETEXNX/SETEXXX/SETKEEPTTL/SETKEEPTTLXX）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetCmd {
  /// 无 NX/XX 的普通 SET（含 GET 选项与 GETSET 路径）
  Set,
  /// NX：仅键不存在时设置
  SetExNx,
  /// XX：仅键存在时设置
  SetExXx,
  /// KEEPTTL：保留既有 TTL 的无条件设置
  SetKeepTtl,
  /// KEEPTTL + XX
  SetKeepTtlXx,
}

impl SetCmd {
  /// 是否 KEEPTTL 族（须保留既有 TTL）
  #[inline]
  const fn is_keep_ttl(self) -> bool {
    matches!(self, Self::SetKeepTtl | Self::SetKeepTtlXx)
  }

  /// 是否 XX 族（仅键存在时设置）
  #[inline]
  const fn is_xx(self) -> bool {
    matches!(self, Self::SetExXx | Self::SetKeepTtlXx)
  }

  /// 是否 NX 族（仅键不存在时设置）
  #[inline]
  const fn is_nx(self) -> bool {
    matches!(self, Self::SetExNx)
  }
}

/// INCR 族命令形态（对标 libs/server/Resp/RespServerSession.cs:ProcessBasicCommands
/// 对 NetworkIncrement 的四路派发）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IncrCmd {
  Incr,
  Decr,
  IncrBy,
  DecrBy,
}

impl IncrCmd {
  /// C# `cmd.ToString()` 的命令名（错误文案用）
  const fn as_str(self) -> &'static str {
    match self {
      Self::Incr => "INCR",
      Self::Decr => "DECR",
      Self::IncrBy => "INCRBY",
      Self::DecrBy => "DECRBY",
    }
  }

  /// DECR/DECRBY 的增量取负
  const fn sign(self) -> i64 {
    match self {
      Self::Decr | Self::DecrBy => -1,
      _ => 1,
    }
  }

  /// 是否携带显式增量参数（INCRBY/DECRBY）
  const fn has_by(self) -> bool {
    matches!(self, Self::IncrBy | Self::DecrBy)
  }
}

/// OBJECT 子命令形态（对标 libs/server/Resp/Parser/RespCommand.cs:OBJECT_*）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectSubCmd {
  Encoding,
  Freq,
  Idletime,
  Refcount,
}

impl ObjectSubCmd {
  /// C# 错误文案中的子命令名（NetworkOBJECT 的 subCommandName 映射）
  const fn as_str(self) -> &'static str {
    match self {
      Self::Encoding => "object|encoding",
      Self::Freq => "object|freq",
      Self::Idletime => "object|idletime",
      // C# 默认分支即 refcount
      Self::Refcount => "object|refcount",
    }
  }
}

/// GETEX 过期形态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GetexExpiry {
  /// 无选项：仅取值
  None,
  /// 清除过期（PERSIST，或 EXAT/PXAT 落在过去被 C# 折算为 0 = 移除过期，
  /// 见 BasicCommands.cs:150 的 `tsExpiry.Ticks > 0` 三态）
  Persist,
  /// 绝对过期 .NET Ticks（EX/PX/EXAT/PXAT 归一）
  At(i64),
}

/// 计算相对过期时间（EX/PX 换算 .NET Ticks）
#[inline]
const fn compute_relative_expiry(
  now: i64,
  expire_time: i64,
  max_val: i64,
  scale: i64,
) -> Result<i64, &'static str> {
  if expire_time > max_val {
    return Err(cs::RESP_ERR_GENERIC_INVALIDEXP_IN_GETEX);
  }
  let Some(ts_ticks) = expire_time.checked_mul(scale) else {
    return Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX);
  };
  let Some(target) = now.checked_add(ts_ticks) else {
    return Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX);
  };
  if target < 0 {
    return Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX);
  }
  Ok(target)
}

/// 计算绝对过期时间（EXAT/PXAT 换算 .NET Ticks）
#[inline]
const fn compute_absolute_expiry(
  expire_time: i64,
  max_val: i64,
  scale: i64,
) -> Result<i64, &'static str> {
  if expire_time > max_val {
    return Err(cs::RESP_ERR_GENERIC_INVALIDEXP_IN_GETEX);
  }
  let Some(scaled) = expire_time.checked_mul(scale) else {
    return Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX);
  };
  let Some(exp) = scaled.checked_add(UNIX_EPOCH_TICKS) else {
    return Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX);
  };
  if exp < 0 {
    return Err(cs::RESP_ERR_OVERFLOWEXP_IN_GETEX);
  }
  Ok(exp)
}

/// SET 选项解析产物（对标 NetworkSETEXNX 的局部变量组）
pub struct SetOptions<'a> {
  pub key: &'a [u8],
  pub val: &'a [u8],
  /// EX/PX 的秒或毫秒数（KEEPTTL/无过期时为 0）
  pub expiry: i64,
  /// PX 口径（expiry 单位为毫秒而非秒）
  pub exp_high_precision: bool,
  cmd: SetCmd,
  pub get_value: bool,
}

/// 相对时长换算为绝对过期 .NET Ticks（EX/PX 共用；high_precision 即 PX）。
/// 对标 C# `UtcNow + TimeSpan.FromSeconds/FromMilliseconds` 的 ticks 加算
fn expiry_ticks_from_now(expiry: i64, high_precision: bool) -> i64 {
  let step = if high_precision {
    TICKS_PER_MILLISECOND
  } else {
    TICKS_PER_SECOND
  };
  now_ticks().saturating_add(expiry.saturating_mul(step))
}

/// SET 值 + 过期应用的写共同体尾部：upsert 自带同步清 TTL，随后按需写新 TTL
///
/// 返回 `Err(())` 表示存储错误且已写出应答；`Ok(false)` 表示须降级异步；
/// `Ok(true)` 表示写闭环（应答由调用方续写）
fn apply_set_with_expiry<'a, D: wdev::Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
  val: &[u8],
  expiry: i64,
  high_precision: bool,
  keep_ttl: Option<Option<i64>>,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  match store.try_upsert_sync(key, val) {
    Ok(Ok(_)) => {}
    // 环形页翻转 / 既有 TTL 清除须异步：先于任何输出整体降级
    Ok(Err(_)) => return Ok(false),
    Err(_) => {
      output.write_resp_error("generic error");
      return Err(());
    }
  }
  let apply_ttl = |ticks: i64| put_ttl_sync(store, key, ticks).map_err(|_| ());
  if let Some(old_ttl) = keep_ttl {
    // KEEPTTL：upsert 已同步清 TTL，按旧值回填；旧值本不存在则保持无 TTL
    return match old_ttl {
      Some(ticks) => apply_ttl(ticks),
      None => Ok(true),
    };
  }
  if expiry != 0 {
    let expire_at_ticks = expiry_ticks_from_now(expiry, high_precision);
    return apply_ttl(expire_at_ticks);
  }
  Ok(true)
}

pub use crate::session_parse_state_extensions::try_get_client_name_bytes as try_get_client_name;

/// libs/server/Resp/CmdStrings.cs:GenericSyntaxErrorOption
///
/// `ERR Syntax error in {0} option '{1}'`（零堆分配直接写入，净化防换行注入）
#[inline]
fn write_syntax_error_option(output: &mut Vec<u8>, cmd: &str, option: &str) {
  let clean_cmd = cs::sanitize_error_str(cmd, cs::MAX_PARAM_NAME_LEN);
  let clean_option = cs::sanitize_error_str(option, cs::MAX_PARAM_NAME_LEN);
  output.extend_from_slice(b"-ERR Syntax error in ");
  output.extend_from_slice(clean_cmd.as_bytes());
  output.extend_from_slice(b" option '");
  output.extend_from_slice(clean_option.as_bytes());
  output.extend_from_slice(b"'\r\n");
}

/// 严格解析 f64（对标 C# parseState.TryGetDouble 默认 canBeInfinite: true；
/// INF 白名单 + NaN 拒绝，单一实现位于 parser::session_parse_state）
fn try_parse_double(raw: &[u8]) -> Option<f64> {
  strict_f64(raw, true)
}

/// COMMAND GETKEYS[ANDFLAGS] 提取上下文
struct CommandKeysContext<'a> {
  cmd_args: &'a [&'a [u8]],
  key_specs: &'static [SimpleRespKeySpec],
  is_sub_command: bool,
}

impl RespServerSession {
  /// libs/server/Resp/BasicCommands.cs:NetworkGET
  pub fn network_get<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      return Ok(false);
    }
    let key = parse_state[0];
    match read_adjudicated_sync(store, key, |v| {
      output.write_resp_bulk_string(v);
    }) {
      Ok(Some(Some(()))) => {}
      Ok(Some(None)) => {
        output.extend_from_slice(b"$-1\r\n");
      }
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkGETEX
  pub fn network_getex<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 对标 C# 选项次序：PERSIST 直通；其余选项先校验第 3 参为正整数
    // （缺失/非整数/非正值均报 value is out of range），再按选项名换算；
    // 未识别选项报 ERR Unsupported option。换算对标 BasicCommands.cs:119-150：
    // EX/PX 相对时长，EXAT/PXAT 绝对 Unix 时间戳，非正 TimeSpan（过去绝对
    // 时刻）连同 PERSIST 一并折算为"移除过期"
    let (key, expiry) = match parse_state {
      [key] => (*key, GetexExpiry::None),
      [key, option] if option.eq_ignore_ascii_case(b"PERSIST") => (*key, GetexExpiry::Persist),
      [key, option, _] if option.eq_ignore_ascii_case(b"PERSIST") => (*key, GetexExpiry::Persist),
      [key, option, expire_arg] => {
        let Some(expire_time) = expire_arg.try_parse_i64() else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE);
          return Ok(true);
        };
        if expire_time <= 0 {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE);
          return Ok(true);
        }
        let now = now_ticks();
        let res = if option.eq_ignore_ascii_case(b"EX") {
          compute_relative_expiry(now, expire_time, MAX_TIMESPAN_SECONDS, TICKS_PER_SECOND)
        } else if option.eq_ignore_ascii_case(b"PX") {
          compute_relative_expiry(
            now,
            expire_time,
            MAX_TIMESPAN_MILLISECONDS,
            TICKS_PER_MILLISECOND,
          )
        } else if option.eq_ignore_ascii_case(b"EXAT") {
          compute_absolute_expiry(expire_time, MAX_UNIX_TIME_SECONDS, TICKS_PER_SECOND)
        } else if option.eq_ignore_ascii_case(b"PXAT") {
          compute_absolute_expiry(
            expire_time,
            MAX_UNIX_TIME_MILLISECONDS,
            TICKS_PER_MILLISECOND,
          )
        } else {
          abort_with_unsupported_option(output, option.as_str_safe());
          return Ok(true);
        };
        let target_ticks = match res {
          Ok(ticks) => ticks,
          Err(err) => {
            abort_with_error_message(output, err);
            return Ok(true);
          }
        };
        // C#：仅 tsExpiry.Ticks > 0 才设置过期，否则（过去绝对时刻）移除过期
        let exp = if target_ticks > now {
          GetexExpiry::At(target_ticks)
        } else {
          GetexExpiry::Persist
        };
        (*key, exp)
      }
      [_, _] => {
        // 两参且非 PERSIST：缺少过期时长参数
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE);
        return Ok(true);
      }
      _ => {
        abort_with_wrong_number_of_arguments(output, "GETEX");
        return Ok(true);
      }
    };

    let start_len = output.len();
    match read_adjudicated_sync(store, key, |v| {
      output.write_resp_bulk_string(v);
    }) {
      Ok(Some(Some(()))) => {
        // 过期应用须先于应答闭环；同步 TTL 写遭环形页翻转时整体降级，
        // 避免已答出旧值而 TTL 未生效
        let applied = match expiry {
          GetexExpiry::None => Ok(true),
          GetexExpiry::Persist => del_ttl_sync(store, key),
          GetexExpiry::At(ticks) => put_ttl_sync(store, key, ticks),
        };
        match applied {
          Ok(true) => {}
          Ok(false) => {
            output.truncate(start_len);
            return Ok(false);
          }
          Err(_) => {
            output.truncate(start_len);
            output.write_resp_error("generic error");
          }
        }
      }
      Ok(Some(None)) => {
        output.write_resp_null();
      }
      Ok(None) => {
        output.truncate(start_len);
        return Ok(false);
      }
      Err(_) => {
        output.truncate(start_len);
        output.write_resp_error("generic error");
      }
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkGETAsync
  ///
  /// C# 异步 GET 路径（GET_WithPending + 完成端口）；rust 侧统一由
  /// `try_read_sync` 的 `Ok(None)` → `Ok(false)` 降级信号承接磁盘冷读与
  /// TTL 过期裁决，故语义与 [`Self::network_get`] 同体
  pub fn network_get_async<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.network_get(parse_state, store, output)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkGET_SG
  ///
  /// C# scatter-gather GET 沿接收缓冲前视吞并后续 GET 批量下发（依赖
  /// NextCommandMaybeGet/ParseGETAndKey 的 recvBuffer 直读）。rust 命令层仅
  /// 持有本命令的 parse_state，无接收缓冲访问入口，故退化为单键 GET 语义——
  /// 单命令场景（SG 本就会走简单路径）应答逐字节一致，仅损失多 GET 流水线
  /// 的批量合并吞吐
  pub fn network_get_sg<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.network_get(parse_state, store, output)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkSET
  pub fn network_set<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      abort_with_wrong_number_of_arguments(output, "SET");
      return Ok(true);
    }
    // SET 声明 arity -3，快速解析器仅把 3..7 参数的 SET 路由到选项解析器；
    // 更长的选项 SET 也落在本函数——C# 此处交还 NetworkSETEXNX 解析选项
    if parse_state.len() > 2 {
      return self.network_setexnx(parse_state, store, output);
    }
    let key = parse_state[0];
    let value = parse_state[1];
    match store.try_upsert_sync(key, value) {
      Ok(Ok(_)) => output.write_resp_simple_string("OK"),
      Ok(Err(_)) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkGETSET
  pub fn network_getset<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "GETSET");
      return Ok(true);
    }
    // C# 走 NetworkSET_Conditional(SET, getValue: true)：无条件写入并回旧值
    let opts = SetOptions {
      key: parse_state[0],
      val: parse_state[1],
      expiry: 0,
      exp_high_precision: false,
      cmd: SetCmd::Set,
      get_value: true,
    };
    self.network_set_conditional(&opts, store, output)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkSetRange
  pub fn network_set_range<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      abort_with_wrong_number_of_arguments(output, "SETRANGE");
      return Ok(true);
    }
    let key = parse_state[0];
    // 对标 C#：偏移须为可解析整数（TryGetInt 口径），负值越界报错，
    // offset + value 不得越过 512MB 负载上限（u64 口径，杜绝 usize 溢出 panic）
    let Some(offset) = strict_i32(parse_state[1]) else {
      abort_with_error_message(output, ERR_NOT_INTEGER);
      return Ok(true);
    };
    let offset: i64 = i64::from(offset);
    let val = parse_state[2];
    if offset < 0 {
      abort_with_error_message(output, ERR_OFFSET_OUT_OF_RANGE);
      return Ok(true);
    }
    if offset as u64 + val.len() as u64 > MAX_STRING_PAYLOAD_BYTES as u64 {
      abort_with_error_message(output, ERR_STRING_EXCEEDS_MAX);
      return Ok(true);
    }
    let offset = offset as usize;

    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(mut existing))) => {
        if offset + val.len() > existing.len() {
          existing.resize(offset + val.len(), 0);
        }
        existing[offset..offset + val.len()].copy_from_slice(val);
        match store.try_upsert_sync(key, &existing) {
          Ok(Ok(_)) => output.write_resp_int(existing.len() as i64),
          Ok(Err(_)) => return Ok(false),
          Err(_) => output.write_resp_error("generic error"),
        }
      }
      Ok(Some(None)) => {
        let mut new_val = vec![0; offset + val.len()];
        new_val[offset..offset + val.len()].copy_from_slice(val);
        match store.try_upsert_sync(key, &new_val) {
          Ok(Ok(_)) => output.write_resp_int(new_val.len() as i64),
          Ok(Err(_)) => return Ok(false),
          Err(_) => output.write_resp_error("generic error"),
        }
      }
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkGetRange
  pub fn network_get_range<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      abort_with_wrong_number_of_arguments(output, "GETRANGE");
      return Ok(true);
    }
    // 对标 C#：start/end 须可解析为整数（TryGetInt 口径），否则报 not-integer
    let Some(mut start) = strict_i32(parse_state[1]).map(i64::from) else {
      abort_with_error_message(output, ERR_NOT_INTEGER);
      return Ok(true);
    };
    let Some(mut end) = strict_i32(parse_state[2]).map(i64::from) else {
      abort_with_error_message(output, ERR_NOT_INTEGER);
      return Ok(true);
    };
    let key = parse_state[0];

    match store.try_read_sync(key, |val| {
      let len = val.len() as i64;
      // 负下标归一化用 saturating_add：调用方可传任意 i64，杜绝溢出 panic
      if start < 0 {
        start = start.saturating_add(len);
      }
      if end < 0 {
        end = end.saturating_add(len);
      }
      if start < 0 {
        start = 0;
      }
      if end >= len {
        end = len - 1;
      }

      if start > end || start >= len {
        output.write_resp_bulk_string(b"");
      } else {
        // SAFETY: 前置逻辑保证 0 <= start <= end < len
        let slice = unsafe { val.get_unchecked((start as usize)..=(end as usize)) };
        output.write_resp_bulk_string(slice);
      }
    }) {
      Ok(Some(Some(()))) => {}
      Ok(Some(None)) => output.write_resp_bulk_string(b""),
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkSETEX
  pub fn network_setex<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.network_setex_impl(false, "SETEX", parse_state, store, output)
  }
  /// PSETEX 入口（调用 network_setex_impl(highPrecision = true)）
  pub fn network_psetex<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.network_setex_impl(true, "PSETEX", parse_state, store, output)
  }
  /// SETEX/PSETEX 共同实现体（对应 C# NetworkSETEX highPrecision 参数化实现）；写值后经
  /// [`super::ttl_sync::put_ttl_sync`] 同步落 TTL 记录
  fn network_setex_impl<'a, D: wdev::Device>(
    &mut self,
    high_precision: bool,
    cmd_name: &str,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 3 {
      abort_with_wrong_number_of_arguments(output, cmd_name);
      return Ok(true);
    }
    let key = parse_state[0];

    // 对标 C#：过期须为整数（TryGetInt 口径）且 > 0
    let Some(expiry) = strict_i32(parse_state[1]) else {
      abort_with_error_message(output, ERR_NOT_INTEGER);
      return Ok(true);
    };
    if expiry <= 0 {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET);
      return Ok(true);
    }
    let val = parse_state[2];

    match store.try_upsert_sync(key, val) {
      Ok(Ok(_)) => {}
      // 异步闭环信号须整体降级，吞掉即静默丢写
      Ok(Err(_)) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    }
    let expire_at_ticks = expiry_ticks_from_now(i64::from(expiry), high_precision);
    match put_ttl_sync(store, key, expire_at_ticks) {
      Ok(true) => output.write_resp_simple_string("OK"),
      Ok(false) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkSETNX
  pub fn network_setnx<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "SETNX");
      return Ok(true);
    }
    let key = parse_state[0];
    let val = parse_state[1];

    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(_))) => output.write_resp_int(0),
      Ok(Some(None)) => match store.try_upsert_sync(key, val) {
        Ok(Ok(_)) => output.write_resp_int(1),
        Ok(Err(_)) => return Ok(false),
        Err(_) => output.write_resp_error("generic error"),
      },
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkSETEXNX
  ///
  /// SET 全选项状态机（EX/PX/KEEPTTL + NX/XX/GET）。C# 对未知选项先原地大写
  /// 重试再判，等价于选项大小写不敏感；SET 只接受 EX/PX/KEEPTTL（EXAT/PXAT
  /// 报语法错误）
  pub fn network_setexnx<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(opts) = parse_set_options(parse_state, output) else {
      return Ok(true);
    };

    // 对标 C# 派发表：无 NX/XX 的 EX/PX/无过期走盲写，其余走条件写
    if opts.cmd == SetCmd::Set && !opts.get_value {
      return self.network_set_ex(&opts, store, output);
    }
    self.network_set_conditional(&opts, store, output)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkSET_EX
  ///
  /// 无条件盲写 + 过期应用（SET k v [EX s|PX ms] 的无 NX/XX/GET 路径）
  pub fn network_set_ex<'a, D: wdev::Device>(
    &mut self,
    opts: &SetOptions,
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    match apply_set_with_expiry(
      store,
      opts.key,
      opts.val,
      opts.expiry,
      opts.exp_high_precision,
      None,
      output,
    ) {
      Ok(true) => {
        output.write_resp_simple_string("OK");
        Ok(true)
      }
      Ok(false) => Ok(false),
      Err(()) => Ok(true),
    }
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkSET_Conditional
  ///
  /// 条件写共同体（SET/SETEXNX/SETEXXX/SETKEEPTTL/SETKEEPTTLXX + GET 选项）。
  /// `expiry` 为相对数值（0 = 不过期；KEEPTTL 族恒 0），`high_precision` 表示
  /// 该数值为毫秒口径（PX）。C# 的 WRONGTYPE 分支（对象存储域）在 rust 字符串
  /// 域不适用，未复刻
  pub fn network_set_conditional<'a, D: wdev::Device>(
    &mut self,
    opts: &SetOptions,
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let SetOptions {
      key,
      val,
      expiry,
      exp_high_precision: high_precision,
      cmd,
      get_value,
      ..
    } = *opts;
    if !get_value {
      // KEEPTTL 无标志形态不关心键是否存在，一律写并回 OK
      let (must_exist, must_absent) = (cmd.is_xx(), cmd.is_nx());

      let old_ttl = if cmd.is_keep_ttl() {
        match ttl_of_sync(store, key) {
          Ok(Some(ttl)) => Some(ttl),
          // TTL 记录需磁盘裁决（或已过期待清除）：降级
          Ok(None) => return Ok(false),
          Err(_) => {
            output.write_resp_error("generic error");
            return Ok(true);
          }
        }
      } else {
        None
      };

      // 存活判定仅条件形态需要（SetExNx/SetExXx/SetKeepTtlXx）；
      // C# SETKEEPTTLXX 对缺键不创建（RMWMethods.NeedInitialUpdate = false），
      // 条件不满足统一回 nil（BasicCommands.cs:770-784）
      let exists = if must_exist || must_absent {
        match probe_alive(store, key) {
          Ok(Some(alive)) => alive,
          Ok(None) => return Ok(false),
          Err(_) => {
            output.write_resp_error("generic error");
            return Ok(true);
          }
        }
      } else {
        true
      };
      if (must_exist && !exists) || (must_absent && exists) {
        // 条件不满足：C# 以 nil 表失败（SETEXNX 翻转 ok 标志后同一出口）
        output.extend_from_slice(b"$-1\r\n");
        return Ok(true);
      }

      match apply_set_with_expiry(store, key, val, expiry, high_precision, old_ttl, output) {
        Ok(true) => output.write_resp_simple_string("OK"),
        Ok(false) => return Ok(false),
        Err(()) => {}
      }
      return Ok(true);
    }

    // GET 形态：回旧值（不存在则 nil），条件语义同上；
    // 带 TTL 键经 try_read_sync 门控降级（过期裁决归异步路径）
    let old = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(found)) => found,
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };

    let should_set = if cmd.is_keep_ttl() {
      // SETKEEPTTL 无条件写；SETKEEPTTLXX 仍要求键存在
      !cmd.is_xx() || old.is_some()
    } else if cmd.is_nx() {
      old.is_none()
    } else if cmd.is_xx() {
      old.is_some()
    } else {
      true
    };

    let old_ttl = if should_set && cmd.is_keep_ttl() {
      match ttl_of_sync(store, key) {
        Ok(Some(ttl)) => Some(ttl),
        Ok(None) => return Ok(false),
        Err(_) => {
          output.write_resp_error("generic error");
          return Ok(true);
        }
      }
    } else {
      None
    };

    if should_set {
      match apply_set_with_expiry(store, key, val, expiry, high_precision, old_ttl, output) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(()) => return Ok(true),
      }
    }

    match old {
      Some(old) => output.write_resp_bulk_string(&old),
      None => output.extend_from_slice(b"$-1\r\n"),
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkIncrement
  pub fn network_increment<'a, D: wdev::Device>(
    &mut self,
    cmd: IncrCmd,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let min_args = if cmd.has_by() { 2 } else { 1 };
    if parse_state.len() < min_args {
      abort_with_wrong_number_of_arguments(output, cmd.as_str());
      return Ok(true);
    }
    let key = parse_state[0];

    let mut delta = cmd.sign();
    if cmd.has_by() {
      let Some(by) = strict_i64(parse_state[1]) else {
        abort_with_error_message(output, ERR_NOT_INTEGER);
        return Ok(true);
      };
      delta = delta.saturating_mul(by);
    }

    let cur = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(found)) => found,
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };
    let val = match cur {
      Some(bytes) => match bytes.as_str_safe().parse::<i64>() {
        Ok(v) => v,
        // C# IsValidNumber 失败即 StringOutput 错误 → not-integer
        Err(_) => {
          abort_with_error_message(output, ERR_NOT_INTEGER);
          return Ok(true);
        }
      },
      // InitialUpdater：键不存在时从 0 起算
      None => 0,
    };
    // C# checked 加法溢出与"非整数旧值"共用 not-integer 错误且不落写
    let Some(next) = val.checked_add(delta) else {
      abort_with_error_message(output, ERR_NOT_INTEGER);
      return Ok(true);
    };

    let mut buf = itoa::Buffer::new();
    match store.try_upsert_sync(key, buf.format(next).as_bytes()) {
      Ok(Ok(_)) => output.write_resp_int(next),
      Ok(Err(_)) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkIncrementByFloat
  pub fn network_increment_by_float<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() < 2 {
      // C# 无显式 arity 检查，由派发层 arity 校验拦截（IsCommandArityValid）
      abort_with_wrong_number_of_arguments(output, "INCRBYFLOAT");
      return Ok(true);
    }
    let key = parse_state[0];
    let Some(incr_by) = try_parse_double(parse_state[1]) else {
      abort_with_error_message(output, cs::RESP_ERR_NOT_VALID_FLOAT);
      return Ok(true);
    };
    if incr_by.is_infinite() {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_NAN_INFINITY_INCR);
      return Ok(true);
    }

    let cur = match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(found)) => found,
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error("generic error");
        return Ok(true);
      }
    };
    let val = match cur {
      Some(bytes) => match try_parse_double(&bytes) {
        Some(v) => v,
        // C# IsValidDouble 失败 → InvalidTypeError → not-valid-float
        None => {
          abort_with_error_message(output, cs::RESP_ERR_NOT_VALID_FLOAT);
          return Ok(true);
        }
      },
      None => 0.0,
    };
    let next = val + incr_by;
    if next.is_nan() || next.is_infinite() {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_NAN_INFINITY_INCR);
      return Ok(true);
    }

    // 对标 NumUtils.WriteDouble：无指数记法的十进制表示（Rust Display 为最短
    // 往返表示，整数结果无小数点，与 C# 输出在常规值域逐字节一致）
    let mut formatted = String::new();
    let _ = write!(&mut formatted, "{next}");
    match store.try_upsert_sync(key, formatted.as_bytes()) {
      Ok(Ok(_)) => output.write_resp_bulk_string(formatted.as_bytes()),
      Ok(Err(_)) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkAppend
  pub fn network_append<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "APPEND");
      return Ok(true);
    }
    let key = parse_state[0];
    let val = parse_state[1];

    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(mut existing))) => {
        existing.extend_from_slice(val);
        match store.try_upsert_sync(key, &existing) {
          Ok(Ok(_)) => output.write_resp_int(existing.len() as i64),
          Ok(Err(_)) => return Ok(false),
          Err(_) => output.write_resp_error("generic error"),
        }
      }
      Ok(Some(None)) => match store.try_upsert_sync(key, val) {
        Ok(Ok(_)) => output.write_resp_int(val.len() as i64),
        Ok(Err(_)) => return Ok(false),
        Err(_) => output.write_resp_error("generic error"),
      },
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkPING / ArrayCommands.cs:NetworkArrayPING
  ///
  /// 零参回 +PONG（订阅会话 RESP2 同为 PONG）；单参回显消息 bulk；
  /// 多参报参数错误（C# ProcessBasicCommands 依 Count 分流两实现）
  pub fn network_ping(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() > 1 {
      abort_with_wrong_number_of_arguments(output, "PING");
      return Ok(true);
    }
    if let Some(msg) = parse_state.first() {
      output.write_resp_bulk_string(msg);
    } else {
      // C# 订阅会话 + RESP2 走 SUSCRIBE_PONG；响应同为 +PONG，统一普通路径
      output.extend_from_slice(cs::RESP_PONG);
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkASKING
  pub fn network_asking(&mut self, output: &mut Vec<u8>) -> wresp::Result<bool> {
    if self.parse_state.count != 0 {
      abort_with_wrong_number_of_arguments(output, "ASKING");
      return Ok(true);
    }
    self.session_asking = 2;
    write_raw(output, cs::RESP_OK);
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkQUIT
  pub fn network_quit(&mut self, output: &mut Vec<u8>) -> wresp::Result<bool> {
    self.to_dispose = true;
    output.write_resp_simple_string("OK");
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkFLUSHDB
  pub fn network_flushdb(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() > 2 {
      abort_with_wrong_number_of_arguments(output, "FLUSHDB");
      return Ok(true);
    }
    // C# 副本只读拦截依赖 clusterProvider；standalone 直达 FlushDb
    self.flush_db("FLUSHDB", parse_state, output)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkFLUSHALL
  pub fn network_flushall(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() > 3 {
      abort_with_wrong_number_of_arguments(output, "FLUSHALL");
      return Ok(true);
    }
    // Garnet 单库，FLUSHALL 与 FLUSHDB 共用 FlushDb
    self.flush_db("FLUSHALL", parse_state, output)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkREADONLY
  pub fn network_readonly(&mut self, output: &mut Vec<u8>) -> wresp::Result<bool> {
    self.read_only_session = true;
    write_raw(output, cs::RESP_OK);
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkREADWRITE
  pub fn network_readwrite(&mut self, output: &mut Vec<u8>) -> wresp::Result<bool> {
    self.read_only_session = false;
    write_raw(output, cs::RESP_OK);
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkSTRLEN
  pub fn network_strlen<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "STRLEN");
      return Ok(true);
    }
    let key = parse_state[0];

    match store.try_read_sync(key, |v| v.len()) {
      Ok(Some(Some(len))) => output.write_resp_int(len as i64),
      Ok(Some(None)) | Ok(None) => output.write_resp_int(0),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:WriteCOMMANDResponse
  pub fn write_command_response(&mut self, output: &mut Vec<u8>) -> wresp::Result<bool> {
    if let Some(infos) = super::resp_commands_info::try_get_resp_commands_info(true) {
      let mut writer =
        super::resp_memory_writer::RespMemoryWriter::new(self.resp_protocol_version >= 3);
      writer.write_array_length(infos.len());
      for info in infos.values() {
        info.to_resp_format(&mut writer);
      }
      output.extend_from_slice(&writer.out);
    } else {
      write_raw(output, cs::RESP_EMPTYLIST);
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND
  pub fn network_command<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if !parse_state.is_empty() {
      abort_with_unknown_subcommand(output, parse_state[0].as_str_safe(), "COMMAND");
    } else {
      self.write_command_response(output)?;
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_COUNT
  pub fn network_command_count<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "COMMAND|COUNT");
      return Ok(true);
    }
    let count = super::resp_commands_info::try_get_resp_commands_info_count(true).unwrap_or(0);
    output.write_resp_int(count as i64);
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_DOCS
  pub fn network_command_docs<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let count = parse_state.len();
    let mut writer =
      super::resp_memory_writer::RespMemoryWriter::new(self.resp_protocol_version >= 3);
    if count == 0 {
      if let Some(cmds_docs) = super::resp_command_docs::try_get_resp_commands_docs(true) {
        writer.write_map_length(cmds_docs.len());
        for cmd_docs in cmds_docs.values() {
          cmd_docs.to_resp_format(&mut writer);
        }
      } else {
        writer.write_map_length(0);
      }
    } else {
      let mut docs = Vec::new();
      for raw in parse_state {
        let name = raw.as_str_safe();
        if let Some(cmd_docs) =
          super::resp_command_docs::try_get_resp_command_docs(name, true, true)
        {
          docs.push(cmd_docs);
        }
      }
      writer.write_map_length(docs.len());
      for cmd_docs in docs {
        cmd_docs.to_resp_format(&mut writer);
      }
    }
    output.extend_from_slice(&writer.out);
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_INFO
  pub fn network_command_info<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let count = parse_state.len();
    if count == 0 {
      // 零参等价无参 COMMAND
      return self.write_command_response(output);
    }
    let mut writer =
      super::resp_memory_writer::RespMemoryWriter::new(self.resp_protocol_version >= 3);
    writer.write_array_length(count);
    for raw in parse_state {
      let name = raw.as_str_safe();
      if let Some(info) =
        super::resp_commands_info::try_get_resp_command_info_by_name(name, true, true)
      {
        info.to_resp_format(&mut writer);
      } else {
        writer.write_null();
      }
    }
    output.extend_from_slice(&writer.out);
    Ok(true)
  }

  /// 准备提取命令 Key 的参数切片与规格上下文（消除 GETKEYS / GETKEYSANDFLAGS 重复代码，零多余堆分配）
  fn prepare_command_keys_context<'b>(
    parse_state: &'b [&'b [u8]],
    output: &mut Vec<u8>,
    cmd_name_for_err: &str,
  ) -> Option<CommandKeysContext<'b>> {
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, cmd_name_for_err);
      return None;
    }
    let cmd_name = parse_state[0].as_str_safe();
    let cmd = super::resp_commands_info_data::resp_command_from_cs_name(cmd_name);
    let mut simple_info = cmd.and_then(super::resp_commands_info::try_get_simple_resp_command_info);

    if let Some(info) = simple_info
      && info.is_parent
      && parse_state.len() >= 2
    {
      let sub_name = format!("{}_{}", cmd_name, parse_state[1].as_str_safe());
      if let Some(sub_cmd) = super::resp_commands_info_data::resp_command_from_cs_name(&sub_name)
        && let Some(sub_info) = super::resp_commands_info::try_get_simple_resp_command_info(sub_cmd)
      {
        simple_info = Some(sub_info);
      }
    }

    let Some(simple_info) = simple_info else {
      abort_with_error_message(output, cs::RESP_INVALID_COMMAND_SPECIFIED);
      return None;
    };
    if simple_info.key_specs.is_empty() {
      abort_with_error_message(output, cs::RESP_COMMAND_HAS_NO_KEY_ARGS);
      return None;
    }
    let slice_offset = if simple_info.is_sub_command { 2 } else { 1 };
    let cmd_args = if parse_state.len() >= slice_offset {
      &parse_state[slice_offset..]
    } else {
      &[]
    };
    Some(CommandKeysContext {
      cmd_args,
      key_specs: simple_info.key_specs.as_slice(),
      is_sub_command: simple_info.is_sub_command,
    })
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_GETKEYS
  pub fn network_command_getkeys<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(ctx) = Self::prepare_command_keys_context(parse_state, output, "COMMAND|GETKEYS")
    else {
      return Ok(true);
    };
    let keys = extract_keys_from_slice(ctx.cmd_args, ctx.key_specs, ctx.is_sub_command);
    output.write_resp_array_len(keys.len());
    for key in keys {
      output.write_resp_bulk_string(key);
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_GETKEYSANDFLAGS
  pub fn network_command_getkeysandflags<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(ctx) =
      Self::prepare_command_keys_context(parse_state, output, "COMMAND|GETKEYSANDFLAGS")
    else {
      return Ok(true);
    };
    let pairs = extract_keys_and_flags_from_slice(ctx.cmd_args, ctx.key_specs, ctx.is_sub_command);
    output.write_resp_array_len(pairs.len());
    for (key, flags_byte) in pairs {
      output.write_resp_array_len(2);
      output.write_resp_bulk_string(key);
      let flags = super::resp_command_key_specification::KeySpecificationFlags(flags_byte as u16);
      let count = flags.count();
      if self.resp_protocol_version >= 3 {
        output.push(b'~');
        let mut buf = itoa::Buffer::new();
        output.extend_from_slice(buf.format(count).as_bytes());
        output.extend_from_slice(b"\r\n");
      } else {
        output.write_resp_array_len(count);
      }
      for flag in flags.iter_descriptions() {
        output.write_resp_bulk_string(flag.as_bytes());
      }
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkECHO
  pub fn network_echo(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "ECHO");
      return Ok(true);
    }
    let msg = parse_state[0];
    output.write_resp_bulk_string(msg);
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkHELLO
  pub fn network_hello<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let count = parse_state.len();
    if count > 6 {
      abort_with_wrong_number_of_arguments(output, "HELLO");
      return Ok(true);
    }

    let mut tmp_resp_protocol_version: Option<u8> = None;
    let mut auth_username: &[u8] = &[];
    let mut tmp_client_name: Option<&str> = None;

    if count > 0 {
      let mut token_idx = 0usize;
      // 校验协议版本（C# TryGetInt 严格口径）
      let Some(local_resp_protocol_version) = strict_i32(parse_state[token_idx]) else {
        abort_with_error_message(output, cs::RESP_ERR_PROTOCOL_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      };
      token_idx += 1;

      if !(2..=3).contains(&local_resp_protocol_version) {
        abort_with_error_message(output, cs::RESP_ERR_UNSUPPORTED_PROTOCOL_VERSION);
        return Ok(true);
      }
      tmp_resp_protocol_version = Some(local_resp_protocol_version as u8);

      while token_idx < count {
        let param = parse_state[token_idx];
        token_idx += 1;

        if param.eq_ignore_ascii_case(b"AUTH") {
          if count - token_idx < 2 {
            write_syntax_error_option(output, "HELLO", "AUTH");
            return Ok(true);
          }
          auth_username = parse_state[token_idx];
          token_idx += 2;
        } else if param.eq_ignore_ascii_case(b"SETNAME") {
          if count - token_idx < 1 {
            write_syntax_error_option(output, "HELLO", "SETNAME");
            return Ok(true);
          }
          let Some(name) = try_get_client_name(parse_state[token_idx]) else {
            abort_with_error_message(output, cs::RESP_ERR_INVALID_CLIENT_NAME);
            return Ok(true);
          };
          token_idx += 1;
          tmp_client_name = Some(name);
        } else {
          write_syntax_error_option(output, "HELLO", param.as_str_safe());
          return Ok(true);
        }
      }
    }

    self.process_hello_command(
      tmp_resp_protocol_version,
      auth_username,
      tmp_client_name,
      store,
      output,
    )
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkTIME
  pub fn network_time(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "TIME");
      return Ok(true);
    }

    let Ok(elapsed) = SystemTime::now().duration_since(UNIX_EPOCH) else {
      output.write_resp_error("generic error");
      return Ok(true);
    };
    let seconds = elapsed.as_secs();
    let micros = elapsed.subsec_micros();
    // 对标 C# 应答帧：*2 + 秒 + 6 位微秒
    output.write_resp_array_len(2);
    let mut buf = itoa::Buffer::new();
    output.write_resp_bulk_string(buf.format(seconds).as_bytes());
    let mut micro_buf = [b'0'; 6];
    let s = buf.format(micros).as_bytes();
    if s.len() <= 6 {
      micro_buf[6 - s.len()..].copy_from_slice(s);
    }
    output.write_resp_bulk_string(&micro_buf);
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkAUTH
  pub fn network_auth<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // AUTH [<username>] <password>
    let count = parse_state.len();
    if !(1..=2).contains(&count) {
      abort_with_wrong_number_of_arguments(output, "AUTH");
      return Ok(true);
    }

    if CAN_AUTHENTICATE {
      // 认证器接线后：AuthenticateUser 成功回 OK；失败按用户名有无回
      // WRONGPASS（Invalid password / Invalid username/password combination）
      write_raw(output, cs::RESP_OK);
    } else {
      // C# 默认 GarnetNoAuthAuthenticator：CanAuthenticate = false
      write_error_raw(
        output,
        "ERR Client sent AUTH, but configured authenticator does not accept passwords",
      );
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkMemoryUsage
  pub fn network_memory_usage<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let count = parse_state.len();
    if count != 1 && count != 3 {
      abort_with_wrong_number_of_arguments(output, "MEMORY|USAGE");
      return Ok(true);
    }

    let key = parse_state[0];
    if count == 3 {
      // 嵌套类型采样对 Garnet 无效，仅为 API 兼容校验语法
      if !parse_state[1].eq_ignore_ascii_case(b"SAMPLES") {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
      let Some(samples) = strict_i32(parse_state[2]) else {
        abort_with_error_message(output, ERR_NOT_INTEGER);
        return Ok(true);
      };
      if samples < 0 {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
    }

    // C# MEMORYUSAGE 经 UnifiedStore ReadMethods:HandleMemoryUsage 统计记录分配
    // 尺寸；rust 存储层无该统计入口（记录分配粒度未暴露），按 C# status != OK
    // 的降级路径回 nil——缺口：wkv 需暴露 record allocated-size 查询
    match store.try_read_sync(key, |_| ()) {
      Ok(Some(_)) => output.extend_from_slice(b"$-1\r\n"),
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkOBJECT
  pub fn network_object<'a, D: wdev::Device>(
    &mut self,
    sub_cmd: ObjectSubCmd,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, sub_cmd.as_str());
      return Ok(true);
    }
    let key = parse_state[0];

    // 语义对照 UnifiedStore ReadMethods:HandleObjectEncoding/RefCount/IdleTime/
    // Freq：字符串域键存在时 ENCODING 恒 "raw"（C# 字符串无 int/embedded 表示）、
    // REFCOUNT 恒 1（不共享值对象）、IDLETIME 恒 0（无 LRU 追踪）、FREQ 恒报
    // 不支持（无 LFU 策略）；对象存储键归 objects 域，此处按字符串域应答
    match store.try_read_sync(key, |_| ()) {
      Ok(Some(Some(()))) => match sub_cmd {
        ObjectSubCmd::Encoding => output.write_resp_bulk_string(b"raw"),
        ObjectSubCmd::Refcount => output.write_resp_int(1),
        ObjectSubCmd::Idletime => output.write_resp_int(0),
        ObjectSubCmd::Freq => {
          abort_with_error_message(output, cs::RESP_ERR_OBJECT_FREQ_UNSUPPORTED)
        }
      },
      // C# 键缺失（status != OK）一律回 nil
      Ok(Some(None)) => output.extend_from_slice(b"$-1\r\n"),
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkOBJECTHELP
  pub fn network_objecthelp<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if !parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "object|help");
      return Ok(true);
    }

    const OBJECT_HELP: [&str; 11] = [
      "OBJECT <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
      "ENCODING <key>",
      "\tReturn the kind of internal representation used in order to store the value associated with a <key>.",
      "FREQ <key>",
      "\tNot supported in Garnet: always returns an error, as access frequency (LFU) is not tracked.",
      "IDLETIME <key>",
      "\tReturn the idle time of the <key>. Garnet does not track per-key idle time, so this is always 0.",
      "REFCOUNT <key>",
      "\tReturn the number of references of the value associated with the <key>. Garnet does not share value objects, so this is always 1.",
      "HELP",
      "\tPrints this help.",
    ];
    output.write_resp_array_len(OBJECT_HELP.len());
    for line in OBJECT_HELP {
      output.write_resp_simple_string(line);
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkASYNC
  pub fn network_async<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // rust 会话未携带 respProtocolVersion（默认 RESP2），C# RESP2 下同样直接报
    // "not supported in RESP2"；ON/OFF/BARRIER 分支为 RESP3 接线后保留（结构对标 C#）
    let resp_protocol_version: u8 = 2;
    if resp_protocol_version <= 2 {
      abort_with_error_message(output, cs::RESP_ERR_NOT_SUPPORTED_RESP2);
      return Ok(true);
    }

    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "ASYNC");
      return Ok(true);
    }

    let param = parse_state[0];
    if param.eq_ignore_ascii_case(b"ON") || param.eq_ignore_ascii_case(b"OFF") {
      // C# 写会话 useAsync 标志；rust 无该会话字段（恒同步路径）
    } else if param.eq_ignore_ascii_case(b"BARRIER") {
      // C# 等待在途异步操作清零；rust 恒同步无在途操作，空等待
    } else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      return Ok(true);
    }

    write_raw(output, cs::RESP_OK);
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:ProcessHelloCommand
  ///
  /// 校验 → 认证 → 升级协议版本 / 落客户端名 → 组 HELLO 应答 map；协议
  /// 版本、客户端名与会话 Id 直读会话真实状态（委托
  /// [`RespServerSession::process_hello_command_state`] 承接）。
  pub fn process_hello_command<'a, D: wdev::Device>(
    &mut self,
    resp_protocol_version: Option<u8>,
    username: &[u8],
    client_name: Option<&str>,
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# 默认 NoAuth 认证器 Authenticate 恒 false → 带 AUTH 的 HELLO 报 WRONGPASS
    if !username.is_empty() && !CAN_AUTHENTICATE {
      write_error_raw(output, cs::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD);
      return Ok(true);
    }

    if let Some(version) = resp_protocol_version {
      self.update_resp_protocol_version(version);
    }
    if let Some(name) = client_name {
      self.set_client_name(Some(name));
    }

    // 应答 map（RESP2 退化为双倍数组）；字段序对齐 C#：server/version/
    // garnet_version/proto/id/mode/role + modules 空数组
    write_map_len_resp2(output, 8);
    output.write_resp_bulk_string(b"server");
    output.write_resp_bulk_string(b"redis");
    output.write_resp_bulk_string(b"version");
    output.write_resp_bulk_string(super::resp_server_session::REDIS_PROTOCOL_VERSION.as_bytes());
    output.write_resp_bulk_string(b"garnet_version");
    output.write_resp_bulk_string(env!("CARGO_PKG_VERSION").as_bytes());
    output.write_resp_bulk_string(b"proto");
    output.write_resp_int(i64::from(self.resp_protocol_version));
    output.write_resp_bulk_string(b"id");
    output.write_resp_int(self.id);
    output.write_resp_bulk_string(b"mode");
    output.write_resp_bulk_string(b"standalone");
    output.write_resp_bulk_string(b"role");
    output.write_resp_bulk_string(b"master");
    output.write_resp_bulk_string(b"modules");
    output.extend_from_slice(b"*0\r\n");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:FlushDb
  ///
  /// FLUSHDB/FLUSHALL 共同体：解析 \[ASYNC|SYNC\] \[UNSAFETRUNCATELOG\] 选项
  pub fn flush_db(
    &mut self,
    cmd: &str,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let mut unsafe_truncate_log = false;
    let mut async_flush = false;
    let mut sync_flush = false;

    for token in parse_state {
      if token.eq_ignore_ascii_case(b"UNSAFETRUNCATELOG") {
        if unsafe_truncate_log {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        unsafe_truncate_log = true;
      } else if token.eq_ignore_ascii_case(b"ASYNC") {
        if sync_flush || async_flush {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        async_flush = true;
      } else if token.eq_ignore_ascii_case(b"SYNC") {
        if sync_flush || async_flush {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        sync_flush = true;
      } else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
    }

    self.execute_flush_db(cmd, unsafe_truncate_log, async_flush, output)
  }
  /// libs/server/Resp/BasicCommands.cs:ExecuteFlushDb
  ///
  /// C# 调 storeWrapper.FlushDatabase/FlushAllDatabases；rust wkv 仅有异步
  /// `WedbStore::flush_all`（全库口径、须跨 await），命令层同步约束下无入口——
  /// 缺口：wkv 需暴露会话可达的 flush-db 通道。按本域存储失败惯例  /// libs/server/Resp/BasicCommands.cs:ExecuteFlushDB
  ///
  /// Garnet 原实现转发至 ClusterSession / StoreWrapper；当前单机基础命令面未承载全量落盘库清空，回显
  /// "generic error"，绝不假报 OK（客户端须知道数据未清）
  pub fn execute_flush_db(
    &mut self,
    _db_name: &str,
    _async_flush: bool,
    _flush_all: bool,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("generic error");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:ParseGETAndKey
  ///
  /// SG GET 前视解析依赖接收缓冲直读（rust 命令层无此入口），SG 路径已退化为
  /// 单键 GET，本助手无调用方；恒 false 即"放弃批量"
  pub fn parse_get_and_key<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    _output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    Ok(false)
  }
  /// libs/server/Resp/BasicCommands.cs:NextCommandMaybeGet
  ///
  /// 接收缓冲上对 `*2\r\n$3\r\nGET\r\n` 前缀的单次比较；rust 命令层无接收
  /// 缓冲访问，恒 false（放弃批量而非出错）
  pub fn next_command_maybe_get(&self) -> bool {
    false
  }
  /// libs/server/Resp/BasicCommands.cs:TryGetSimpleCommandInfo
  pub fn try_get_simple_command_info<'a, D: wdev::Device>(
    &mut self,
    cmd_name: &[u8],
    _store: &wkv::BatchStoreSession<'a, D>,
    _output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let name_str = cmd_name.as_str_safe();
    if let Some(cmd) = super::resp_commands_info_data::resp_command_from_cs_name(name_str)
      && super::resp_commands_info::try_get_simple_resp_command_info(cmd).is_some()
    {
      return Ok(true);
    }
    Ok(false)
  }
  /// libs/server/Resp/BasicCommands.cs:SetResult
  ///
  /// C# SG pending 输出槽数组管理（惰性分配/幂次扩容）；rust 无 scratch 槽
  /// 机制，SG 已退化单键路径，无调用方
  pub fn set_result<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    _output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    Ok(true)
  }
}

/// NetworkSETEXNX 的选项解析前半段
///
/// 解析失败时已写出错误应答并返回 None。错误次序对标 C#：重复/非法过期选项
/// → syntax error；EX/PX 缺值 → syntax error；值非整数 → not-integer；值非正
/// → invalid expire in set；重复 NX/XX → syntax error；未知选项 → unknown command
fn parse_set_options<'p>(parse_state: &[&'p [u8]], output: &mut Vec<u8>) -> Option<SetOptions<'p>> {
  let key = parse_state[0];
  let val = parse_state[1];

  let mut expiry: i64 = 0;
  let mut exp_high_precision = false;
  let mut exp_keep_ttl = false;
  let mut exist_nx = false;
  let mut exist_xx = false;
  let mut get_value = false;

  let mut token_idx = 2usize;
  while token_idx < parse_state.len() {
    let next_opt = parse_state[token_idx];
    token_idx += 1;

    // 过期选项（大小写不敏感）；SET 仅接受 EX/PX/KEEPTTL，
    // EXAT/PXAT 命中过期选项解析器但不属可接受集 → syntax error
    let is_expiry_option = next_opt.eq_ignore_ascii_case(b"EX")
      || next_opt.eq_ignore_ascii_case(b"PX")
      || next_opt.eq_ignore_ascii_case(b"KEEPTTL")
      || next_opt.eq_ignore_ascii_case(b"EXAT")
      || next_opt.eq_ignore_ascii_case(b"PXAT");
    if is_expiry_option {
      if exp_keep_ttl
        || expiry != 0
        || next_opt.eq_ignore_ascii_case(b"EXAT")
        || next_opt.eq_ignore_ascii_case(b"PXAT")
      {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return None;
      }
      if next_opt.eq_ignore_ascii_case(b"KEEPTTL") {
        exp_keep_ttl = true;
        continue;
      }
      // EX/PX 后必须跟过期数值；C# 修复过末参越界读，缺值即 syntax error
      let Some(raw) = parse_state.get(token_idx) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return None;
      };
      token_idx += 1;
      let Some(v) = strict_i32(raw) else {
        abort_with_error_message(output, ERR_NOT_INTEGER);
        return None;
      };
      if v <= 0 {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET);
        return None;
      }
      expiry = i64::from(v);
      exp_high_precision = next_opt.eq_ignore_ascii_case(b"PX");
      continue;
    }

    // NX/XX/GET（C# 经原地大写重试，等价大小写不敏感）
    if next_opt.eq_ignore_ascii_case(b"NX") {
      if exist_nx || exist_xx {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return None;
      }
      exist_nx = true;
    } else if next_opt.eq_ignore_ascii_case(b"XX") {
      if exist_nx || exist_xx {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return None;
      }
      exist_xx = true;
    } else if next_opt.eq_ignore_ascii_case(b"GET") {
      get_value = true;
    } else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_UNK_CMD);
      return None;
    }
  }

  // 组合派发（对标 C# switch）：XX 与 KEEPTTL 组合为 SetKeepTtlXx；
  // NX + KEEPTTL 仍走 SetExNx（C# KEEPTTL/ExistOptions.NX → SETEXNX）
  // KEEPTTL 族相对过期恒为 0（C# Debug.Assert(expiry == 0)）
  if exp_keep_ttl {
    expiry = 0;
  }
  let cmd = if exist_nx {
    SetCmd::SetExNx
  } else if exist_xx {
    if exp_keep_ttl {
      SetCmd::SetKeepTtlXx
    } else {
      SetCmd::SetExXx
    }
  } else if exp_keep_ttl {
    SetCmd::SetKeepTtl
  } else {
    SetCmd::Set
  };

  Some(SetOptions {
    key,
    val,
    expiry,
    exp_high_precision,
    cmd,
    get_value,
  })
}
