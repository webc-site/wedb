//! 数值自增自减命令域（INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT，
//! 对标 libs/server/Resp/BasicCommands.cs 数值命令段）

use itoa::Buffer;
use wbase::num::{strict_f64, strict_i64};
use wresp::{
  check_args::unpack_args_rest,
  cmd_strings::{self as cs, RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, abort_with_error_message},
  ext::RespVecExt,
  resp_memory_writer::format_double,
};
use zmij::Buffer as ZmijBuffer;

use crate::{
  resp::resp_server_session::RespServerSession,
  storage::session::common::{UserRead, read_user_sync},
};

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

/// 严格解析 f64（对标 C# parseState.TryGetDouble 默认 canBeInfinite: true；
/// INF 白名单 + NaN 拒绝，单一实现位于 wresp::session_parse_state）
fn try_parse_double(raw: &[u8]) -> Option<f64> {
  strict_f64(raw, true)
}

impl RespServerSession {
  /// libs/server/Resp/BasicCommands.cs:NetworkIncrement
  ///
  /// INCR/DECR/INCRBY/DECRBY 共同体；RMW 语义写回（保留既有 key 级 TTL，
  /// 对标 C# GetRMWModifiedFieldInfo）
  pub fn network_increment<'a, D: wdev::Device>(
    &mut self,
    cmd: IncrCmd,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# NetworkIncrement 下界 arity 门（Count<1 / Count<2），多余实参被忽略：
    // RMWMethods InPlace/Initial 的 INCR/DECR 臂固定增量 ±1，从不读 arg1
    let (key, delta) = if cmd.has_by() {
      let Some(([key, by_raw], _)) = unpack_args_rest(parse_state, output, cmd.as_str()) else {
        return Ok(true);
      };
      let Some(by) = strict_i64(by_raw) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      };
      (key, cmd.sign().saturating_mul(by))
    } else {
      let Some(([key], rest)) = unpack_args_rest(parse_state, output, cmd.as_str()) else {
        return Ok(true);
      };
      // C# Count>1 时对第二参仅做整数校验（非整数 → not-integer），值弃用
      if rest.first().is_some_and(|raw| strict_i64(raw).is_none()) {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      }
      (key, cmd.sign())
    };

    // 读改写原子窗口：跨「读旧值—算新值—写回」全程持本键桶排他闩，杜绝同键
    // 并发丢更新（对标 C# BasicSessionLocker 的 ephemeral 闩跨 InternalRMW 全程）；
    // 同步域取闩失败即回降级通道，绝不自旋等闩
    let Some(window) = store.try_rmw_window(key) else {
      return Ok(false);
    };
    // 解析在读取闭包内完成：免整值堆分配（旧值口径对位 C# IsValidNumber →
    // NumUtils.TryReadInt64，拒前导零；与参数路径 strict_i64 同源单一实现）
    let val = match read_user_sync(store, key, strict_i64) {
      Ok(UserRead::Hit(Some(v))) => v,
      // 值非整数 → not-integer
      Ok(UserRead::Hit(None)) => {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      }
      // 对象键（信封域命中）→ WRONGTYPE
      Ok(UserRead::WrongType) => {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok(UserRead::Missing) => 0,
      Ok(UserRead::Deferred) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    };
    // C# checked 加法溢出与"非整数旧值"共用 not-integer 错误且不落写
    let Some(next) = val.checked_add(delta) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };

    let mut buf = Buffer::new();
    match window.try_rmw_sync(buf.format(next).as_bytes()) {
      Ok(Ok(_)) => output.write_resp_int(next),
      Ok(Err(_)) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkIncrementByFloat
  ///
  /// RMW 语义写回（保留既有 key 级 TTL，对标 C# GetRMWModifiedFieldInfo）
  pub fn network_increment_by_float<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# NetworkIncrementByFloat 无 arity 门：第二参缺失或非浮点均回
    // not-valid-float，多余实参忽略；0 参在 C# 为 GetArgSliceByRef(0) 越界 UB，
    // rust 以 arity 下界门兜底属有意修复（在册偏差）
    let Some(([key], rest)) = unpack_args_rest(parse_state, output, "INCRBYFLOAT") else {
      return Ok(true);
    };
    let Some(incr_by) = rest.first().and_then(|raw| try_parse_double(raw)) else {
      abort_with_error_message(output, cs::RESP_ERR_NOT_VALID_FLOAT);
      return Ok(true);
    };
    if incr_by.is_infinite() {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_NAN_INFINITY_INCR);
      return Ok(true);
    }

    // 读改写原子窗口：同 [`Self::network_increment`]，跨读算写全程持本键桶排他闩
    let Some(window) = store.try_rmw_window(key) else {
      return Ok(false);
    };
    // 解析在读取闭包内完成：免整值堆分配（C# IsValidDouble 失败 → not-valid-float）
    let val = match read_user_sync(store, key, try_parse_double) {
      Ok(UserRead::Hit(Some(v))) => v,
      // 值非浮点 → not-valid-float
      Ok(UserRead::Hit(None)) => {
        abort_with_error_message(output, cs::RESP_ERR_NOT_VALID_FLOAT);
        return Ok(true);
      }
      // 对象键（信封域命中）→ WRONGTYPE
      Ok(UserRead::WrongType) => {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok(UserRead::Missing) => 0.0,
      Ok(UserRead::Deferred) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    };
    // 对标 C# PrivateMethods.cs:IsValidDouble：旧值自身非有限（±inf）→
    // NaNOrInfinityError 旗标 → NaN/Infinity 文案
    if !val.is_finite() {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_NAN_INFINITY_INCR);
      return Ok(true);
    }
    let next = val + incr_by;
    // 对标 C# TryInPlaceUpdateNumber/TryCopyUpdateNumber（double 版）
    // `!double.IsFinite(val)` → InvalidTypeError 旗标：有限+有限相加溢出
    // 无穷大与非法旧值同报 not-valid-float（BasicCommands.cs:942-944）
    if !next.is_finite() {
      abort_with_error_message(output, cs::RESP_ERR_NOT_VALID_FLOAT);
      return Ok(true);
    }

    // 对标 NumUtils.WriteDouble：无指数记法的十进制表示（经 zmij 栈缓冲零堆分配格式化，整数结果无小数点）
    let mut buf = ZmijBuffer::new();
    let formatted = format_double(next, &mut buf);
    match window.try_rmw_sync(formatted.as_bytes()) {
      Ok(Ok(_)) => output.write_resp_bulk_string(formatted.as_bytes()),
      Ok(Err(_)) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }
}
