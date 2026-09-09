use std::{
  fmt::Write as _,
  str,
  time::{SystemTime, UNIX_EPOCH},
};

use super::{
  cmd_strings as cs,
  cmd_strings::{
    abort_with_error_message, abort_with_wrong_number_of_arguments, write_error_raw,
    write_map_len_resp2, write_raw,
  },
  parser::resp_ext::{RespSliceExt, RespVecExt},
  resp_server_session::RespServerSession,
  ttl_sync::{
    del_ttl_sync, now_unix_ms, probe_alive, put_ttl_sync, read_adjudicated_sync, ttl_of_sync,
  },
};

/// 字符串类命令负载上限（libs/server/Resp/Bitmap/BitmapManager.cs:MaxBitmapPayloadBytes）
const MAX_STRING_PAYLOAD_BYTES: usize = 512 * 1024 * 1024;
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER
const ERR_NOT_INTEGER: &str = cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER;
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_GENERIC_OFFSETOUTOFRANGE
const ERR_OFFSET_OUT_OF_RANGE: &str = cs::RESP_ERR_GENERIC_OFFSETOUTOFRANGE;
/// libs/server/Resp/CmdStrings.cs:RESP_ERR_STRING_EXCEEDS_MAX_SIZE
const ERR_STRING_EXCEEDS_MAX: &str = "ERR string exceeds maximum allowed size (proto-max-bulk-len)";
/// libs/host/GarnetServer.cs:RedisProtocolVersion（HELLO 应答中的 version 字段）
const REDIS_PROTOCOL_VERSION: &str = "7.4.3";
/// libs/server/Auth/GarnetNoAuthAuthenticator.cs:CanAuthenticate
///
/// rust 会话尚未接线认证器；C# 默认（无 AuthSettings）即 NoAuth 认证器，
/// CanAuthenticate = false，AUTH/HELLO 认证按该路径报错（文案与 C# 逐字节一致）
const CAN_AUTHENTICATE: bool = false;

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
enum GetexExpiry {
  /// 无选项：仅取值
  None,
  /// PERSIST：清除过期
  Persist,
  /// 绝对过期毫秒时间戳（EX/PX/EXAT/PXAT 归一）
  At(u64),
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

/// 相对时长换算为绝对过期毫秒时间戳（EX/PX 共用；high_precision 即 PX）
fn expiry_ms_from_now(expiry: i64, high_precision: bool) -> u64 {
  let now = now_unix_ms() as i64;
  let span = if high_precision {
    expiry
  } else {
    expiry.saturating_mul(1000)
  };
  now.saturating_add(span).max(0) as u64
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
  keep_ttl: Option<Option<u64>>,
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
  let apply_ttl = |ms: u64| put_ttl_sync(store, key, ms).map_err(|_| ());
  if let Some(old_ttl) = keep_ttl {
    // KEEPTTL：upsert 已同步清 TTL，按旧值回填；旧值本不存在则保持无 TTL
    return match old_ttl {
      Some(ms) => apply_ttl(ms),
      None => Ok(true),
    };
  }
  if expiry != 0 {
    let expire_at_ms = expiry_ms_from_now(expiry, high_precision);
    return apply_ttl(expire_at_ms);
  }
  Ok(true)
}

/// libs/server/SessionParseStateExtensions.cs:TryGetClientName
///
/// 客户端名仅允许 33..=126 可打印字符；空串允许（引用语义允许清名）；
/// 非 UTF-8 视为非法（C# GetString 返回 null）
pub fn try_get_client_name(raw: &[u8]) -> Option<&str> {
  let name = str::from_utf8(raw).ok()?;
  if name.is_empty() {
    return Some(name);
  }
  name
    .bytes()
    .all(|c| (33..=126).contains(&c))
    .then_some(name)
}

/// libs/server/Resp/CmdStrings.cs:GenericSyntaxErrorOption
///
/// `ERR Syntax error in {0} option '{1}'`
fn format_error_option(cmd: &str, option: &str) -> String {
  format!("ERR Syntax error in {cmd} option '{option}'")
}

/// 严格解析 f64（对标 C# parseState.TryGetDouble：整体须为合法浮点）
fn try_parse_double(raw: &[u8]) -> Option<f64> {
  str::from_utf8(raw).ok()?.parse().ok()
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
    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
        output.write_resp_bulk_string(&val);
      }
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
    if parse_state.is_empty() || parse_state.len() > 3 {
      abort_with_wrong_number_of_arguments(output, "GETEX");
      return Ok(true);
    }
    let key = parse_state[0];

    // 对标 C# 选项次序：PERSIST 直通；其余选项先校验第 3 参为正整数
    // （缺失/非整数/非正值均报 value is out of range），再按选项名换算；
    // 未识别选项报 ERR Unsupported option
    let expiry = if parse_state.len() > 1 {
      let option = parse_state[1];
      if option.eq_ignore_ascii_case(b"PERSIST") {
        GetexExpiry::Persist
      } else {
        let Some(expire_time) = parse_state.get(2).and_then(|t| t.try_parse_i64()) else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE);
          return Ok(true);
        };
        if expire_time <= 0 {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE);
          return Ok(true);
        }
        let now = now_unix_ms() as i64;
        if option.eq_ignore_ascii_case(b"EX") {
          GetexExpiry::At((now + expire_time.saturating_mul(1000)).max(0) as u64)
        } else if option.eq_ignore_ascii_case(b"PX") {
          GetexExpiry::At((now + expire_time).max(0) as u64)
        } else if option.eq_ignore_ascii_case(b"EXAT") {
          GetexExpiry::At(expire_time.saturating_mul(1000).max(0) as u64)
        } else if option.eq_ignore_ascii_case(b"PXAT") {
          GetexExpiry::At(expire_time.max(0) as u64)
        } else {
          abort_with_error_message(
            output,
            &cs::GENERIC_ERR_UNSUPPORTED_OPTION.replace("{0}", option.as_str_safe()),
          );
          return Ok(true);
        }
      }
    } else {
      GetexExpiry::None
    };

    match read_adjudicated_sync(store, key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
        // 过期应用须先于应答闭环；同步 TTL 写遭环形页翻转时整体降级，
        // 避免已答出旧值而 TTL 未生效
        let applied = match &expiry {
          GetexExpiry::None => Ok(true),
          GetexExpiry::Persist => del_ttl_sync(store, key),
          GetexExpiry::At(ms) => put_ttl_sync(store, key, *ms),
        };
        match applied {
          Ok(true) => output.write_resp_bulk_string(&val),
          Ok(false) => return Ok(false),
          Err(_) => output.write_resp_error("generic error"),
        }
      }
      Ok(Some(None)) => {
        output.extend_from_slice(b"$-1\r\n");
      }
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error("generic error"),
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
    self.network_set__conditional(&opts, store, output)
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
    // 对标 C#：偏移须为可解析整数，负值越界报错，offset + value 不得越过
    // 512MB 负载上限（全程 i64 口径，杜绝 usize 溢出 panic）
    let Some(offset) = parse_state[1].try_parse_i64() else {
      abort_with_error_message(output, ERR_NOT_INTEGER);
      return Ok(true);
    };
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
    // 对标 C#：start/end 须可解析为整数，否则报 value is not an integer
    let Some(mut start) = parse_state[1].try_parse_i64() else {
      abort_with_error_message(output, ERR_NOT_INTEGER);
      return Ok(true);
    };
    let Some(mut end) = parse_state[2].try_parse_i64() else {
      abort_with_error_message(output, ERR_NOT_INTEGER);
      return Ok(true);
    };
    let key = parse_state[0];

    match store.try_read_sync(key, |v| v.to_vec()) {
      Ok(Some(Some(val))) => {
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
          output.write_resp_bulk_string(&val[(start as usize)..=(end as usize)]);
        }
      }
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
  /// libs/server/Resp/BasicCommands.cs:NetworkSETEX（highPrecision = PSETEX）
  pub fn network_psetex<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.network_setex_impl(true, "PSETEX", parse_state, store, output)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkSETEX
  ///
  /// SETEX/PSETEX 共同体（C# 以 bool highPrecision 参数化）；写值后经
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

    // 对标 C#：过期须为整数且 > 0
    let Some(expiry) = parse_state[1].try_parse_i64() else {
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
    let expire_at_ms = expiry_ms_from_now(expiry, high_precision);
    match put_ttl_sync(store, key, expire_at_ms) {
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
    self.network_set__conditional(&opts, store, output)
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
  pub fn network_set__conditional<'a, D: wdev::Device>(
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

      let exists = if cmd.is_keep_ttl() {
        // KEEPTTL 族不因条件短路，但仍需存活判定语义一致的探针
        true
      } else {
        match probe_alive(store, key) {
          Ok(Some(alive)) => alive,
          Ok(None) => return Ok(false),
          Err(_) => {
            output.write_resp_error("generic error");
            return Ok(true);
          }
        }
      };
      if !cmd.is_keep_ttl() && ((must_exist && !exists) || (must_absent && exists)) {
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
      let Some(by) = parse_state[1].try_parse_i64() else {
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
  /// libs/server/Resp/BasicCommands.cs:NetworkPING
  pub fn network_ping(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      // C# 订阅会话 + RESP2 走 SUSCRIBE_PONG；响应同为 +PONG，统一普通路径
      output.extend_from_slice(cs::RESP_PONG);
    } else {
      let msg = parse_state[0];
      output.write_resp_bulk_string(msg);
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkASKING
  pub fn network_asking<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# 仅在 EnableCluster 时置 SessionAsking = 2；rust 集群会话域未接线，
    // standalone 下 C# 同样只回 OK
    write_raw(output, cs::RESP_OK);
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkQUIT
  pub fn network_quit(
    &mut self,
    _parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# 置 toDispose 关闭连接；连接生命周期归会话派发域，此处仅应答
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
  pub fn network_readonly<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# 经 clusterSession.SetReadOnlySession 标记会话；rust 集群会话域未接线，
    // standalone 语义即仅回 OK
    write_raw(output, cs::RESP_OK);
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkREADWRITE
  pub fn network_readwrite<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
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
  ///
  /// C# 从 RespCommandsInfo 表 + 自定义命令表汇总输出；rust 命令元数据表
  /// （resp_commands_info 域）尚未建成、自定义命令数为 0，与 C# 表加载失败
  /// 的降级路径一致：仅输出空 custom 命令数组 `*0`
  pub fn write_command_response<'a, D: wdev::Device>(
    &mut self,
    _parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    write_raw(output, cs::RESP_EMPTYLIST);
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND
  pub fn network_command<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if !parse_state.is_empty() {
      let error_msg = cs::GENERIC_ERR_UNKNOWN_SUB_COMMAND
        .replace("{0}", parse_state[0].as_str_safe())
        .replace("{1}", "COMMAND");
      write_error_raw(output, &error_msg);
    } else {
      self.write_command_response(parse_state, store, output)?;
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
    // C# 元数据表加载失败时 respCommandCount 置 0，再加自定义命令数（0）
    output.write_resp_int(0);
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_DOCS
  pub fn network_command_docs<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // rust 无 RespCommandDocs 表：无参走 C# 的 WriteEmptyMap 降级，带参走
    // docs.Count == 0 的空 map——RESP2 下两者均为双倍数组 `*0`
    let _ = parse_state.len();
    write_map_len_resp2(output, 0);
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_INFO
  pub fn network_command_info<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let count = parse_state.len();
    if count == 0 {
      // 零参等价无参 COMMAND
      return self.write_command_response(parse_state, store, output);
    }
    // 命令元数据表缺席：逐名查找均按 C# writer.WriteNull 降级
    output.write_resp_array_len(count);
    for _ in 0..count {
      output.extend_from_slice(b"$-1\r\n");
    }
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_GETKEYS
  pub fn network_command_getkeys<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "COMMAND|GETKEYS");
      return Ok(true);
    }
    // 命令元数据表缺席：TryGetSimpleCommandInfo 恒失败 → C# 的
    // AbortWithErrorMessage(RESP_INVALID_COMMAND_SPECIFIED) 降级
    abort_with_error_message(output, cs::RESP_INVALID_COMMAND_SPECIFIED);
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:NetworkCOMMAND_GETKEYSANDFLAGS
  pub fn network_command_getkeysandflags<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "COMMAND|GETKEYSANDFLAGS");
      return Ok(true);
    }
    abort_with_error_message(output, cs::RESP_INVALID_COMMAND_SPECIFIED);
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
    let mut error_msg: Option<String> = None;

    if count > 0 {
      let mut token_idx = 0usize;
      // 校验协议版本
      let Some(local_resp_protocol_version) = parse_state[token_idx].try_parse_i64() else {
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
            error_msg = Some(format_error_option("HELLO", "AUTH"));
            break;
          }
          auth_username = parse_state[token_idx];
          token_idx += 2;
        } else if param.eq_ignore_ascii_case(b"SETNAME") {
          if count - token_idx < 1 {
            error_msg = Some(format_error_option("HELLO", "SETNAME"));
            break;
          }
          let Some(name) = try_get_client_name(parse_state[token_idx]) else {
            abort_with_error_message(output, cs::RESP_ERR_INVALID_CLIENT_NAME);
            return Ok(true);
          };
          token_idx += 1;
          tmp_client_name = Some(name);
        } else {
          error_msg = Some(format_error_option("HELLO", param.as_str_safe()));
          break;
        }
      }
    }

    if let Some(error_msg) = error_msg {
      write_error_raw(output, &error_msg);
      return Ok(true);
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
    let frame = format!(
      "*2\r\n${}\r\n{seconds}\r\n$6\r\n{micros:06}\r\n",
      digits_len(seconds)
    );
    write_raw(output, frame.as_bytes());
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
      let Some(samples) = parse_state[2].try_parse_i64() else {
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
  pub fn process_hello_command<'a, D: wdev::Device>(
    &mut self,
    resp_protocol_version: Option<u8>,
    username: &[u8],
    client_name: Option<&str>,
    _store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let _ = client_name;
    // RESP 规范：校验 → 认证 → 才允许切换协议/名字。rust 会话无
    // respProtocolVersion/clientName 字段可持久化（待会话状态域补齐），亦无
    // 在途异步操作（协议切换的 pending 拦截不触发）
    if let Some(ver) = resp_protocol_version {
      let _ = ver;
    }

    // C# 默认 NoAuth 认证器 Authenticate 恒 false → 带 AUTH 的 HELLO 报 WRONGPASS
    if !username.is_empty() && !CAN_AUTHENTICATE {
      write_error_raw(output, cs::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD);
      return Ok(true);
    }

    // 应答 map（RESP2 退化为双倍数组）：server/version/garnet_version/proto/
    // id/mode/role + modules 空数组
    write_map_len_resp2(output, 8);
    for (name, value) in [
      ("server", "redis"),
      ("version", REDIS_PROTOCOL_VERSION),
      ("garnet_version", env!("CARGO_PKG_VERSION")),
      ("mode", "standalone"),
      ("role", "master"),
    ] {
      output.write_resp_bulk_string(name.as_bytes());
      output.write_resp_bulk_string(value.as_bytes());
    }
    output.write_resp_bulk_string(b"proto");
    // C# 回写会话当前协议版本；rust 恒默认 RESP2
    output.write_resp_int(i64::from(resp_protocol_version.unwrap_or(2).min(2)));
    output.write_resp_bulk_string(b"id");
    // C# 为会话 Id；rust 会话结构无 Id 字段
    output.write_resp_int(0);
    output.write_resp_bulk_string(b"modules");
    output.extend_from_slice(b"*0\r\n");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:FlushDb
  ///
  /// FLUSHDB/FLUSHALL 共同体：解析 [ASYNC|SYNC] [UNSAFETRUNCATELOG] 选项
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
  /// 缺口：wkv 需暴露会话可达的 flush-db 通道。按本域存储失败惯例回
  /// "generic error"，绝不假报 OK（客户端须知道数据未清）
  pub fn execute_flush_db(
    &mut self,
    _cmd: &str,
    _unsafe_truncate_log: bool,
    _async_flush: bool,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    output.write_resp_error("generic error");
    Ok(true)
  }
  /// libs/server/Resp/BasicCommands.cs:WriteClientInfo
  ///
  /// 将会话描述写入 info 行（不追加换行）。C# 字段来源 → rust 现状：
  /// Id/networkSender 端点/CreationTicks/clientName/userHandle/lib-* 均为
  /// 会话字段，rust 会话结构未携带 → 以空/零值占位；flags=N（非订阅会话）
  pub fn write_client_info(into: &mut String) {
    let _ = write!(
      into,
      "id=0 addr= laddr= age=0 flags=N db=0 resp=2 lib-name= lib-ver="
    );
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
  ///
  /// C# 从 RespCommandsInfo 表解析命令名取简化信息；rust 元数据表
  /// （resp_commands_info 域）未建成，恒按"未知命令"失败，调用方据此回
  /// RESP_INVALID_COMMAND_SPECIFIED
  pub fn try_get_simple_command_info<'a, D: wdev::Device>(
    &mut self,
    _cmd_name: &[u8],
    _store: &wkv::BatchStoreSession<'a, D>,
    _output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
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

/// 十进制无符号位数（TIME 应答帧长度前缀用）
const fn digits_len(mut v: u64) -> usize {
  let mut n = 1;
  while v >= 10 {
    v /= 10;
    n += 1;
  }
  n
}

/// libs/server/Resp/BasicCommands.cs:NetworkSETEXNX 的选项解析前半段
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
      let Some(v) = raw.try_parse_i64() else {
        abort_with_error_message(output, ERR_NOT_INTEGER);
        return None;
      };
      if v <= 0 {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET);
        return None;
      }
      expiry = v;
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

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use compio::runtime::Runtime;
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};

  use super::{super::ttl_sync::ttl_of_sync, *};

  type Batch<'a> = wkv::BatchStoreSession<'a, SegmentedDevice>;

  /// 独立临时库 + 批处理纪元上下文（纯内存同步路径闭环，无磁盘 I/O）
  fn with_batch(f: impl FnOnce(&mut RespServerSession, &Batch)) {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let dir = tempfile::tempdir().unwrap();
      let device = Arc::new(SegmentedDevice::single_file(dir.path().join("basic.db")).unwrap());
      let mut config = StoreConfig::new(1024, 4096, 16, 0.5).unwrap();
      config.gc.enabled = false;
      let store = Arc::new(WedbStore::open(config, device).unwrap());
      let session = store.new_session().unwrap();
      let batch = session.enter_batch();
      let mut s = RespServerSession;
      f(&mut s, &batch);
    });
  }

  #[test]
  fn set_get_roundtrip_and_null() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s.network_set(&[b"k", b"v1"], batch, &mut out).unwrap();
      assert_eq!(out, b"+OK\r\n");

      let mut out = Vec::new();
      let _ = s.network_get(&[b"k"], batch, &mut out).unwrap();
      assert_eq!(out, b"$2\r\nv1\r\n");

      let mut out = Vec::new();
      let _ = s.network_get(&[b"missing"], batch, &mut out).unwrap();
      assert_eq!(out, b"$-1\r\n");

      // 选项 SET 落 3 参即走 GET 形态条件写
      let mut out = Vec::new();
      let _ = s
        .network_set(&[b"k", b"v2", b"GET"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$2\r\nv1\r\n");
    });
  }

  #[test]
  fn set_wrong_args_error() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s.network_set(&[b"k"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR wrong number of arguments for 'SET' command\r\n");
    });
  }

  #[test]
  fn append_and_strlen() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s.network_append(&[b"k", b"ab"], batch, &mut out).unwrap();
      assert_eq!(out, b":2\r\n");

      let mut out = Vec::new();
      let _ = s.network_append(&[b"k", b"cd"], batch, &mut out).unwrap();
      assert_eq!(out, b":4\r\n");

      let mut out = Vec::new();
      let _ = s.network_strlen(&[b"k"], batch, &mut out).unwrap();
      assert_eq!(out, b":4\r\n");

      let mut out = Vec::new();
      let _ = s.network_strlen(&[b"x"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");
    });
  }

  #[test]
  fn setnx_semantics() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s.network_setnx(&[b"k", b"v"], batch, &mut out).unwrap();
      assert_eq!(out, b":1\r\n");

      let mut out = Vec::new();
      let _ = s.network_setnx(&[b"k", b"w"], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");

      let mut out = Vec::new();
      let _ = s.network_get(&[b"k"], batch, &mut out).unwrap();
      assert_eq!(out, b"$1\r\nv\r\n");
    });
  }

  #[test]
  fn incr_family_and_overflow() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s
        .network_increment(IncrCmd::Incr, &[b"n"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":1\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_increment(IncrCmd::IncrBy, &[b"n", b"41"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":42\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_increment(IncrCmd::Decr, &[b"n"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":41\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_increment(IncrCmd::DecrBy, &[b"n", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":40\r\n");

      // 非整数增量
      let mut out = Vec::new();
      let _ = s
        .network_increment(IncrCmd::IncrBy, &[b"n", b"x"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

      // 旧值非整数
      let _ = s
        .network_set(&[b"s", b"abc"], batch, &mut Vec::new())
        .unwrap();
      let mut out = Vec::new();
      let _ = s
        .network_increment(IncrCmd::Incr, &[b"s"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

      // i64 溢出不落写
      let _ = s
        .network_set(&[b"m", b"9223372036854775807"], batch, &mut Vec::new())
        .unwrap();
      let mut out = Vec::new();
      let _ = s
        .network_increment(IncrCmd::Incr, &[b"m"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
      let mut out = Vec::new();
      let _ = s.network_get(&[b"m"], batch, &mut out).unwrap();
      assert_eq!(out, b"$19\r\n9223372036854775807\r\n");
    });
  }

  #[test]
  fn incrbyfloat_formatting() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s
        .network_increment_by_float(&[b"f", b"10.5"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$4\r\n10.5\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_increment_by_float(&[b"f", b"0.1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$4\r\n10.6\r\n");

      // 0.1 + 0.2：最短往返 17 位有效数字
      let mut out = Vec::new();
      let _ = s
        .network_increment_by_float(&[b"z", b"0.2"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$3\r\n0.2\r\n");
      let mut out = Vec::new();
      let _ = s
        .network_increment_by_float(&[b"z", b"0.1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$19\r\n0.30000000000000004\r\n");

      // NaN 可被 .NET TryGetDouble 解析，C# 语义经 NaN/Infinity 检查报错
      let mut out = Vec::new();
      let _ = s
        .network_increment_by_float(&[b"f", b"nan"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR increment would produce NaN or Infinity\r\n");

      // 非法浮点增量
      let mut out = Vec::new();
      let _ = s
        .network_increment_by_float(&[b"f", b"abc"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR value is not a valid float\r\n");

      // 旧值非浮点
      let _ = s
        .network_set(&[b"g", b"abc"], batch, &mut Vec::new())
        .unwrap();
      let mut out = Vec::new();
      let _ = s
        .network_increment_by_float(&[b"g", b"1.0"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR value is not a valid float\r\n");
    });
  }

  #[test]
  fn setex_writes_ttl_record() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s
        .network_setex(&[b"k", b"100", b"v"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"+OK\r\n");
      // TTL 记录已同步落库（绝对毫秒 >= 当前 + 99s）
      let ttl = ttl_of_sync(batch, b"k").unwrap().unwrap().unwrap();
      assert!(ttl > now_unix_ms() + 99_000);

      // 非法过期
      let mut out = Vec::new();
      let _ = s
        .network_setex(&[b"k", b"0", b"v"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR invalid expire time in 'set' command\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_setex(&[b"k", b"abc", b"v"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

      // PSETEX 毫秒口径
      let mut out = Vec::new();
      let _ = s
        .network_psetex(&[b"p", b"500", b"v"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"+OK\r\n");
      let ttl = ttl_of_sync(batch, b"p").unwrap().unwrap().unwrap();
      assert!(ttl > now_unix_ms() + 400);
    });
  }

  #[test]
  fn getex_persist_and_ex() {
    with_batch(|s, batch| {
      let _ = s
        .network_set(&[b"k", b"v"], batch, &mut Vec::new())
        .unwrap();
      let _ = put_ttl_sync(batch, b"k", now_unix_ms() + 60_000).unwrap();

      // GETEX k PERSIST：清 TTL 并回值
      let mut out = Vec::new();
      let _ = s
        .network_getex(&[b"k", b"PERSIST"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$1\r\nv\r\n");
      assert_eq!(ttl_of_sync(batch, b"k").unwrap(), Some(None));

      // GETEX k EX 100：回值并落 TTL
      let mut out = Vec::new();
      let _ = s
        .network_getex(&[b"k", b"EX", b"100"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$1\r\nv\r\n");
      assert!(ttl_of_sync(batch, b"k").unwrap().unwrap().unwrap() > now_unix_ms() + 99_000);

      // GETEX k PX 0 → value out of range（先数值校验后选项识别）
      let mut out = Vec::new();
      let _ = s
        .network_getex(&[b"k", b"PX", b"0"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR value is out of range, must be positive.\r\n");

      // GETEX k EXAT 100：绝对过期秒
      let mut out = Vec::new();
      let _ = s
        .network_getex(&[b"k", b"EXAT", b"100"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$1\r\nv\r\n");
      let ttl = ttl_of_sync(batch, b"k").unwrap().unwrap().unwrap();
      assert!(ttl < now_unix_ms() + 200_000);

      // 未识别选项
      let mut out = Vec::new();
      let _ = s
        .network_getex(&[b"k", b"EXXX", b"100"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR Unsupported option EXXX\r\n");
    });
  }

  #[test]
  fn getset_returns_old_value() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s.network_getset(&[b"k", b"v2"], batch, &mut out).unwrap();
      // 键原不存在：nil，但已写入新值
      assert_eq!(out, b"$-1\r\n");
      let mut out = Vec::new();
      let _ = s.network_get(&[b"k"], batch, &mut out).unwrap();
      assert_eq!(out, b"$2\r\nv2\r\n");

      let mut out = Vec::new();
      let _ = s.network_getset(&[b"k", b"v3"], batch, &mut out).unwrap();
      assert_eq!(out, b"$2\r\nv2\r\n");
    });
  }

  #[test]
  fn set_nx_xx_get_variants() {
    with_batch(|s, batch| {
      // SET k v NX：不存在 → OK
      let mut out = Vec::new();
      let _ = s
        .network_setexnx(&[b"k", b"v1", b"NX"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"+OK\r\n");

      // SET k v NX：已存在 → nil
      let mut out = Vec::new();
      let _ = s
        .network_setexnx(&[b"k", b"v2", b"NX"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$-1\r\n");

      // SET k v XX GET：存在 → 写入并回旧值
      let mut out = Vec::new();
      let _ = s
        .network_setexnx(&[b"k", b"v3", b"XX", b"GET"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$2\r\nv1\r\n");

      // SET missing v XX GET：不存在 → nil 且不写入
      let mut out = Vec::new();
      let _ = s
        .network_setexnx(&[b"missing", b"v", b"XX", b"GET"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$-1\r\n");
      let mut out = Vec::new();
      let _ = s.network_get(&[b"missing"], batch, &mut out).unwrap();
      assert_eq!(out, b"$-1\r\n");

      // SET k v EX 100：写入并落 TTL
      let mut out = Vec::new();
      let _ = s
        .network_setexnx(&[b"t", b"v", b"EX", b"100"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"+OK\r\n");
      let ttl = ttl_of_sync(batch, b"t").unwrap().unwrap().unwrap();
      assert!(ttl > now_unix_ms() + 99_000);

      // KEEPTTL：写入保留原 TTL
      let _ = put_ttl_sync(batch, b"t", now_unix_ms() + 50_000).unwrap();
      let mut out = Vec::new();
      let _ = s
        .network_setexnx(&[b"t", b"v2", b"KEEPTTL"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"+OK\r\n");
      let ttl = ttl_of_sync(batch, b"t").unwrap().unwrap().unwrap();
      assert!(ttl > now_unix_ms() + 49_000 && ttl < now_unix_ms() + 51_000);
    });
  }

  #[test]
  fn setexnx_option_errors_match_csharp() {
    with_batch(|s, batch| {
      // EXAT 不可用于 SET → syntax error
      let mut out = Vec::new();
      let _ = s
        .network_setexnx(&[b"k", b"v", b"EXAT", b"100"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR syntax error\r\n");

      // EX 末参缺值 → syntax error
      let mut out = Vec::new();
      let _ = s
        .network_setexnx(&[b"k", b"v", b"EX"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR syntax error\r\n");

      // NX 重复 → syntax error
      let mut out = Vec::new();
      let _ = s
        .network_setexnx(&[b"k", b"v", b"NX", b"NX"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR syntax error\r\n");

      // 未知选项 → unknown command
      let mut out = Vec::new();
      let _ = s
        .network_setexnx(&[b"k", b"v", b"WHATEVER"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR unknown command\r\n");
    });
  }

  #[test]
  fn setrange_getrange_bounds() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s
        .network_set_range(&[b"k", b"1", b"ab"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":3\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_set_range(&[b"k", b"5", b"cd"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":7\r\n");
      let mut out = Vec::new();
      let _ = s.network_get(&[b"k"], batch, &mut out).unwrap();
      assert_eq!(out, b"$7\r\n\x00ab\x00\x00cd\r\n");

      // 负偏移
      let mut out = Vec::new();
      let _ = s
        .network_set_range(&[b"k", b"-1", b"x"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR offset is out of range\r\n");

      // 非整数偏移
      let mut out = Vec::new();
      let _ = s
        .network_set_range(&[b"k", b"x", b"x"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");

      // GETRANGE 负下标与截断
      let mut out = Vec::new();
      let _ = s
        .network_get_range(&[b"k", b"-2", b"-1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$2\r\ncd\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_get_range(&[b"k", b"0", b"999"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$7\r\n\x00ab\x00\x00cd\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_get_range(&[b"k", b"5", b"2"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$0\r\n\r\n");
    });
  }

  #[test]
  fn simple_frames_ping_echo_time_quit() {
    let mut s = RespServerSession;
    let mut out = Vec::new();
    let _ = s.network_ping(&[], &mut out).unwrap();
    assert_eq!(out, b"+PONG\r\n");

    let mut out = Vec::new();
    let _ = s.network_ping(&[b"hey"], &mut out).unwrap();
    assert_eq!(out, b"$3\r\nhey\r\n");

    let mut out = Vec::new();
    let _ = s.network_echo(&[b"msg"], &mut out).unwrap();
    assert_eq!(out, b"$3\r\nmsg\r\n");

    let mut out = Vec::new();
    let _ = s.network_echo(&[], &mut out).unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'ECHO' command\r\n"
    );

    let mut out = Vec::new();
    let _ = s.network_time(&[b"x"], &mut out).unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'TIME' command\r\n"
    );

    let mut out = Vec::new();
    let _ = s.network_time(&[], &mut out).unwrap();
    // *2\r\n$<len>\r\n<secs>\r\n$6\r\n<micros>\r\n
    assert!(out.starts_with(b"*2\r\n$"));
    let tail = &out[out.len() - 12..];
    assert!(tail.starts_with(b"$6\r\n") && tail.ends_with(b"\r\n"));

    let mut out = Vec::new();
    let _ = s.network_quit(&[], &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");
  }

  #[test]
  fn auth_default_noauthenticator_error() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s.network_auth(&[b"pass"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR Client sent AUTH, but configured authenticator does not accept passwords\r\n"
      );

      let mut out = Vec::new();
      let _ = s.network_auth(&[], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'AUTH' command\r\n"
      );

      let mut out = Vec::new();
      let _ = s
        .network_auth(&[b"u", b"p", b"x"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'AUTH' command\r\n"
      );
    });
  }

  #[test]
  fn hello_validation_and_frame() {
    with_batch(|s, batch| {
      // 协议版本非法
      let mut out = Vec::new();
      let _ = s.network_hello(&[b"4"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR Unsupported protocol version\r\n");

      let mut out = Vec::new();
      let _ = s.network_hello(&[b"abc"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR Protocol version is not an integer or out of range.\r\n"
      );

      // 参数过多
      let mut out = Vec::new();
      let _ = s
        .network_hello(
          &[b"2", b"AUTH", b"a", b"b", b"SETNAME", b"c", b"d"],
          batch,
          &mut out,
        )
        .unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'HELLO' command\r\n"
      );

      // AUTH 选项缺参
      let mut out = Vec::new();
      let _ = s
        .network_hello(&[b"2", b"AUTH", b"u"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR Syntax error in HELLO option 'AUTH'\r\n");

      // SETNAME 非法字符
      let mut out = Vec::new();
      let _ = s
        .network_hello(&[b"2", b"SETNAME", b"a b"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR Client names cannot contain spaces, newlines or special characters.\r\n"
      );

      // 未知选项
      let mut out = Vec::new();
      let _ = s.network_hello(&[b"2", b"WHAT"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR Syntax error in HELLO option 'WHAT'\r\n");

      // 正常应答：RESP2 map 退化为双倍数组 *16
      let mut out = Vec::new();
      let _ = s.network_hello(&[], batch, &mut out).unwrap();
      assert!(out.starts_with(b"*16\r\n$6\r\nserver\r\n$5\r\nredis\r\n"));

      // 带 AUTH 且无认证器 → WRONGPASS
      let mut out = Vec::new();
      let _ = s
        .process_hello_command(Some(3), b"user", None, batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-WRONGPASS Invalid username/password combination\r\n");
    });
  }

  #[test]
  fn command_family_degradation_frames() {
    with_batch(|s, batch| {
      // 无表 COMMAND → *0（C# 表加载失败同路径）
      let mut out = Vec::new();
      let _ = s.network_command(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"*0\r\n");

      // COMMAND COUNT → :0
      let mut out = Vec::new();
      let _ = s.network_command_count(&[], batch, &mut out).unwrap();
      assert_eq!(out, b":0\r\n");

      // COMMAND|COUNT 带参 → wrong args
      let mut out = Vec::new();
      let _ = s.network_command_count(&[b"X"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'COMMAND|COUNT' command\r\n"
      );

      // COMMAND 未知子命令
      let mut out = Vec::new();
      let _ = s.network_command(&[b"WHAT"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR unknown subcommand 'WHAT'. Try COMMAND HELP\r\n");

      // COMMAND DOCS → 空 map（RESP2 双倍数组）
      let mut out = Vec::new();
      let _ = s.network_command_docs(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"*0\r\n");

      // COMMAND INFO get → nil 项
      let mut out = Vec::new();
      let _ = s.network_command_info(&[b"get"], batch, &mut out).unwrap();
      assert_eq!(out, b"*1\r\n$-1\r\n");

      // COMMAND GETKEYS get → Invalid command specified
      let mut out = Vec::new();
      let _ = s
        .network_command_getkeys(&[b"get"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-Invalid command specified\r\n");

      // COMMAND GETKEYSANDFLAGS 带参同上；零参 wrong args
      let mut out = Vec::new();
      let _ = s
        .network_command_getkeysandflags(&[], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'COMMAND|GETKEYSANDFLAGS' command\r\n"
      );
    });
  }

  #[test]
  fn object_and_memory_validation() {
    with_batch(|s, batch| {
      let _ = s
        .network_set(&[b"k", b"v"], batch, &mut Vec::new())
        .unwrap();

      let mut out = Vec::new();
      let _ = s
        .network_object(ObjectSubCmd::Encoding, &[b"k"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$3\r\nraw\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_object(ObjectSubCmd::Refcount, &[b"k"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":1\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_object(ObjectSubCmd::Idletime, &[b"k"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":0\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_object(ObjectSubCmd::Freq, &[b"k"], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        &b"-ERR OBJECT FREQ is not supported: Garnet does not track access frequency (no LFU maxmemory policy).\x0d\x0a"[..]
      );

      // 键缺失 → nil
      let mut out = Vec::new();
      let _ = s
        .network_object(ObjectSubCmd::Idletime, &[b"nope"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$-1\r\n");

      // 参数错 → object|encoding wrong args
      let mut out = Vec::new();
      let _ = s
        .network_object(ObjectSubCmd::Encoding, &[], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'object|encoding' command\r\n"
      );

      // MEMORY USAGE 语法校验
      let mut out = Vec::new();
      let _ = s
        .network_memory_usage(&[b"k", b"SAMPLES", b"-1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR syntax error\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_memory_usage(&[b"k", b"BAD", b"1"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"-ERR syntax error\r\n");

      let mut out = Vec::new();
      let _ = s
        .network_memory_usage(&[b"k", b"SAMPLES", b"5"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$-1\r\n");
    });
  }

  #[test]
  fn object_help_frame() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s.network_objecthelp(&[], batch, &mut out).unwrap();
      assert!(out.starts_with(b"*11\r\n+OBJECT <subcommand>"));

      let mut out = Vec::new();
      let _ = s.network_objecthelp(&[b"x"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        b"-ERR wrong number of arguments for 'object|help' command\r\n"
      );
    });
  }

  #[test]
  fn async_rejected_on_resp2() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s.network_async(&[b"ON"], batch, &mut out).unwrap();
      assert_eq!(out, b"-ERR command not supported in RESP2\r\n");
    });
  }

  #[test]
  fn flush_options_and_unavailable_flush() {
    let mut s = RespServerSession;
    // 非法选项 → syntax error
    let mut out = Vec::new();
    let _ = s.network_flushdb(&[b"WHAT"], &mut out).unwrap();
    assert_eq!(out, b"-ERR syntax error\r\n");

    // ASYNC SYNC 互斥 → syntax error
    let mut out = Vec::new();
    let _ = s.network_flushall(&[b"ASYNC", b"SYNC"], &mut out).unwrap();
    assert_eq!(out, b"-ERR syntax error\r\n");

    // flush 通道缺席 → generic error（不假报 OK）
    let mut out = Vec::new();
    let _ = s.network_flushdb(&[], &mut out).unwrap();
    assert_eq!(out, b"-ERR generic error\r\n");

    let mut out = Vec::new();
    let _ = s.network_flushall(&[], &mut out).unwrap();
    assert_eq!(out, b"-ERR generic error\r\n");
  }

  #[test]
  fn readonly_readwrite_asking_frames() {
    with_batch(|s, batch| {
      let mut out = Vec::new();
      let _ = s.network_asking(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"+OK\r\n");

      let mut out = Vec::new();
      let _ = s.network_readonly(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"+OK\r\n");

      let mut out = Vec::new();
      let _ = s.network_readwrite(&[], batch, &mut out).unwrap();
      assert_eq!(out, b"+OK\r\n");
    });
  }

  #[test]
  fn client_name_validation() {
    assert_eq!(try_get_client_name(b"client-1"), Some("client-1"));
    assert_eq!(try_get_client_name(b""), Some(""));
    assert_eq!(try_get_client_name(b"a b"), None);
    assert_eq!(try_get_client_name(b"\xff"), None);
  }

  #[test]
  fn digits_len_counts_decimal_digits() {
    assert_eq!(digits_len(0), 1);
    assert_eq!(digits_len(9), 1);
    assert_eq!(digits_len(10), 2);
    assert_eq!(digits_len(1_700_000_000), 10);
  }
}
