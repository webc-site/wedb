//! 字符串读命令域（GET/GET_SG/GETRANGE/SUBSTR/STRLEN/GETEX，
//! 对标 libs/server/Resp/BasicCommands.cs 读命令段）

use smallvec::SmallVec;
use wbase::{
  convert::{TICKS_PER_MILLISECOND, TICKS_PER_SECOND},
  time::now_ticks,
};
use wconf::ServerConfigType;
use wdev::Device;
use wresp::{
  argslice::ArgSlice,
  check_args::{check_arg_count, parse_i32_arg, unpack_args},
  cmd_strings::{
    self as cs, RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, abort_with_error_message,
    abort_with_unsupported_option,
  },
  command::RespCommand,
  ext::{RespSliceExt, RespVecExt},
  options::{ExpirationOption, try_get_expiration_option},
};

use super::ttl::{
  GetexExpiry, MAX_TIMESPAN_MILLISECONDS, MAX_TIMESPAN_SECONDS, MAX_UNIX_TIME_MILLISECONDS,
  MAX_UNIX_TIME_SECONDS, compute_absolute_expiry, compute_relative_expiry,
};
use crate::{
  resp::resp_server_session::RespServerSession,
  storage::session::common::{
    UserRead, fold_outcome, read_user_sync, read_user_sync_with_prefix,
    ttl_sync::{del_ttl_sync, put_ttl_sync},
    user_read::finish_value_read,
  },
};

impl RespServerSession {
  /// libs/server/Resp/BasicCommands.cs:NetworkGET
  pub fn network_get<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if self.runtime_config.get_bool(ServerConfigType::SgGet) && self.next_command_maybe_get() {
      return self.network_get_sg(parse_state, store, output);
    }
    let Some([key]) = unpack_args(parse_state, output, "GET") else {
      return Ok(true);
    };
    // 双域读（一处定义）：String 域命中即用户数据（值内容任意），信封域命中
    // 即对象键 WRONGTYPE——对标 C# UnifiedStore ReadMethods 的 ValueIsObject 位
    let read = read_user_sync(store, key, self.session_metrics.as_deref(), |v| {
      output.write_resp_bulk_string(v);
    });
    Ok(finish_value_read(read, output, None, |out| {
      out.write_resp_null_ver(self.resp_protocol_version)
    }))
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkGET_SG
  ///
  /// 流水线冷读 Scatter-Gather (SG) 批量聚合与异步批量 I/O 读路径
  ///
  /// 【有意修正】批量计数口径与 C# 不同：C# SG 批量臂整批仅计 1 次
  /// 命令统计，rust 在批尾对 (c-1) 个流水线合并键逐键补账
  ///（incr_total_commands_processed / read_commands_processed /
  /// command_stats calls）——每键确实各执行了一次读操作，rust 口径更准，
  /// 刻意保留不删（与 C# 的差异经本注记登记）。
  pub fn network_get_sg<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([_key0]) = unpack_args(parse_state, output, "GET") else {
      return Ok(true);
    };

    let mut key_slices: SmallVec<[ArgSlice; 16]> = SmallVec::new();
    key_slices.push(self.parse_state.get_arg_slice_by_ref(0));

    while let Some(slice) = self.parse_get_and_key() {
      key_slices.push(slice);
    }

    let c = key_slices.len();
    let prefix = store.session_prefix();
    let prefix_slice = prefix.as_slice();

    let start_len = output.len();
    let mut deferred = false;
    // 逐键命中/未命中本地累加、非 deferred 收尾一次入账（终值与 C# 批量
    // GET 循环逐键累加同口径；deferred 整批转慢路径重执，即时入账会双计，
    // 故延迟到收尾判定）
    let (mut found, mut notfound) = (0u64, 0u64);

    for &arg_slice in &key_slices {
      let key = arg_slice.resolve(&self.recv_buffer);
      match read_user_sync_with_prefix(store, prefix_slice, key, None, |v| {
        output.write_resp_bulk_string(v);
      }) {
        Ok(UserRead::Hit(())) => found += 1,
        Ok(UserRead::WrongType) => {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
        }
        Ok(UserRead::Missing) => {
          notfound += 1;
          output.write_resp_null_ver(self.resp_protocol_version);
        }
        Ok(UserRead::Deferred) => {
          deferred = true;
          break;
        }
        Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
      }
    }

    if !deferred && let Some(metrics) = &self.session_metrics {
      metrics.incr_total_found(found);
      metrics.incr_total_notfound(notfound);
    }

    if c > 1 {
      let extra = (c - 1) as u64;
      if let Some(metrics) = &self.session_metrics {
        metrics.incr_total_commands_processed(extra);
        metrics.add_total_read_commands_processed(extra);
      }
      if let Some(stats) = &self.command_stats {
        let mut stats = stats.lock();
        stats.increment_calls_by(RespCommand::Get, extra);
      }
    }

    if deferred {
      output.truncate(start_len);
      let batched_keys: Vec<Vec<u8>> = key_slices
        .iter()
        .map(|s| s.resolve(&self.recv_buffer).to_vec())
        .collect();
      self.sg_batched_keys = Some(batched_keys);
      return Ok(false);
    }

    Ok(true)
  }

  /// libs/server/Storage/Functions/MainStore/PrivateMethods.cs:NormalizeRange
  ///
  /// 1:1 复刻 C# PrivateMethods.cs 368 行起的 NormalizeRange（static (int, int)），
  /// 返回半开区间 (start, end)，长度 end - start；(0, 0) 表示空串。
  /// 入参 start/end 来自 RESP strict_i32，len 为值长（i64），故 len + end、
  /// end + 1 等中间量在 i64 域无溢出；% 与 C# 同为向零截断，语义一致。
  /// 刻意复刻的怪癖（勿按 Redis 官方语义修复）：C# 387 行
  /// `end = end > len ? len : (end % len)`，当 end == len 时折为 0，
  /// 若折后 end < start 则整体判空。样例：len = 10、GETRANGE k -2 10 →
  /// 回空串；而 C# 375 行起 start >= 0 分支 end == len 不折叠（钳为 len），
  /// 两分支行为刻意不同。回归锚点见 wnode/tests/resp_tests.rs get_slice_test。
  pub(crate) const fn normalize_range(start: i64, end: i64, len: i64) -> (i64, i64) {
    // C# 372 行：len == 0 无有效区间，提前返回并避免 start < 0 分支对 0 取模
    if len == 0 {
      return (0, 0);
    }
    if start >= 0 && start <= len {
      // C# 375-380 行：正起点分支，end == len 不折叠
      if end < 0 && (len + end) > 0 {
        (start, len + end + 1)
      } else if end >= start {
        (start, if end < len { end + 1 } else { len })
      } else {
        (0, 0)
      }
    } else if start < 0 {
      // C# 382-391 行：负起点分支
      if start > end {
        return (0, 0);
      }
      let start = start % len;
      let start = if start >= 0 { start } else { len + start };
      // C# 387 行怪癖：end == len 经 end % len 折为 0
      let end = if end > len { len } else { end % len };
      if end < 0 && (len + end) > 0 {
        (start, len + end + 1)
      } else if end >= start {
        (
          start,
          if end < len {
            if start == end { end + 1 } else { end }
          } else {
            len
          },
        )
      } else {
        (0, 0)
      }
    } else {
      // C# 掉出分支链：start > len 等情形
      (0, 0)
    }
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkGetRange
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:GETRANGE
  ///
  /// `cmd_name` 承接 C# 的 cmd.ToString()（GETRANGE/SUBSTR 各报实名）
  /// 区间归一化委托上方关联函数 normalize_range，1:1 复刻 C#
  /// PrivateMethods.cs 368 行起的 NormalizeRange（含 387 行 end == len
  /// 折 0 怪癖），不按 Redis 官方语义修正，保持与 C# 行为一致。
  pub fn network_get_range<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
    cmd_name: &str,
  ) -> wresp::Result<bool> {
    let Some([key, start_raw, end_raw]) = unpack_args(parse_state, output, cmd_name) else {
      return Ok(true);
    };
    // 对标 C#：start/end 须可解析为整数（溢出走 not-integer 对标 C# TryGetInt；
    // 前导零拒收系 rust 严格收口，C# TryGetInt 因 TryReadInt32Safe 死参实际放行 007，见 doc/zh/deviations.md §32），否则报 not-integer
    let Some(start) = parse_i32_arg(start_raw, output).map(i64::from) else {
      return Ok(true);
    };
    let Some(end) = parse_i32_arg(end_raw, output).map(i64::from) else {
      return Ok(true);
    };

    let read = read_user_sync(store, key, self.session_metrics.as_deref(), |val| {
      let len = val.len() as i64;
      let (start, end) = Self::normalize_range(start, end, len);
      // 对标 C# CopyRespTo（PrivateMethods.cs 41 行）半边防御 `(start < end) ? .. : 0`：
      // normalize_range 可产出 start > end 的反转区间（如 len=5 时 GETRANGE k 5 -2
      // 归一化 (5,4)、GETRANGE k -6 -3 归一化 (4,3)），反转与相等一律回空串
      if start >= end {
        output.write_resp_bulk_string(b"");
      } else {
        // SAFETY: normalize_range 保证 0 <= start 且 end <= len，
        // 结合外层 start < end 保证 0 <= start < end <= len
        let slice = unsafe { val.get_unchecked((start as usize)..(end as usize)) };
        output.write_resp_bulk_string(slice);
      }
    });
    // 缺失回空 bulk（与 GET 的 null 帧形刻意不同）；非命中臂未起笔 → None 不回退
    Ok(finish_value_read(read, output, None, |out| {
      out.write_resp_bulk_string(b"")
    }))
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkSTRLEN
  pub fn network_strlen<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "STRLEN") else {
      return Ok(true);
    };

    // 长度帧交消费回调出帧（Hit 域唯一出帧点），非命中收尾转调单源
    let read = read_user_sync(store, key, self.session_metrics.as_deref(), |v| {
      output.write_resp_int(v.len() as i64);
    });
    Ok(finish_value_read(read, output, None, |out| {
      out.write_resp_int(0)
    }))
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkGETEX
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:GETEX
  ///（存储侧 GETEX 读值+续期语义由本函数闭环：读经 read_adjudicated_user_sync、
  /// 续期经 put_ttl_sync/del_ttl_sync，对标 MainStoreOps GETEX 的 UnifiedInput 分支）
  pub fn network_getex<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some((key, expiry)) = parse_getex_args(parse_state, output) else {
      return Ok(true);
    };

    // 读值 + TTL 应用整段同窗收口（对标 C# NetworkGETEX 单次 RMW：读值与
    // TrySetExpiration/RemoveExpiration 在 ephemeral 锁内一体完成，
    // RMWMethods.cs:771-799）；失闩沿 Ok(false) 降级慢路径同段持窗重放
    let Some(_window) = store.try_rmw_window(key) else {
      return Ok(false);
    };

    let start_len = output.len();
    // 入账收尾判定单点（同 network_get_sg 守卫纪律：读臂即时入账遭 TTL 写
    // 降级整命令重放慢臂会双计，故 read_user_sync 传 None 静默，本地三态
    // 记读态，仅确定以 Ok(true) 收尾的臂经 fold_outcome 单规则补账恰一条
    // ——Hit 闭环=found、Missing=notfound、WrongType/TTL 写 Err 错误帧静默
    // （对位 C# MainStoreOps GETEX 三臂 Found→incr_session_found /
    // IsWrongType 不计数 / else→notfound，Err 形无 Found 入账对位）；一切
    // truncate+Ok(false) 降级出口零入账，交慢臂 read_user 唯一出口收口，
    // C# 侧读算写锁内一体无重放，计数恒一）
    let mut outcome: Option<bool> = None;
    match read_user_sync(store, key, None, |v| {
      output.write_resp_bulk_string(v);
    }) {
      Ok(UserRead::Hit(())) => {
        // 过期应用须先于应答闭环；同步 TTL 写遭环形页翻转时整体降级，
        // 回退已向输出缓冲写入的 bulk string，杜绝慢路径重执致双应答
        match expiry {
          GetexExpiry::None => outcome = Some(true),
          GetexExpiry::Persist => match del_ttl_sync(store, key) {
            Ok(true) => outcome = Some(true),
            Ok(false) => {
              output.truncate(start_len);
              return Ok(false);
            }
            Err(_) => {
              output.truncate(start_len);
              output.write_resp_error(RESP_ERR_GENERIC);
            }
          },
          GetexExpiry::At(ticks) => match put_ttl_sync(store, key, ticks) {
            Ok(true) => outcome = Some(true),
            Ok(false) => {
              output.truncate(start_len);
              return Ok(false);
            }
            Err(_) => {
              output.truncate(start_len);
              output.write_resp_error(RESP_ERR_GENERIC);
            }
          },
        }
      }
      Ok(UserRead::WrongType) => {
        output.truncate(start_len);
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
      }
      Ok(UserRead::Missing) => {
        output.write_resp_null_ver(self.resp_protocol_version);
        outcome = Some(false);
      }
      Ok(UserRead::Deferred) => {
        output.truncate(start_len);
        return Ok(false);
      }
      Err(_) => {
        output.truncate(start_len);
        output.write_resp_error(RESP_ERR_GENERIC);
      }
    }
    fold_outcome(outcome, self.session_metrics.as_deref());
    Ok(true)
  }
}

/// NetworkGETEX 的参数推导单源（快慢路径共用；解析失败时已写出错误应答并
/// 返回 None，返回 `(key, 过期应用形态)`）
///
/// 对标 C# 选项次序：PERSIST 直通；其余选项先校验第 3 参为正整数
/// （缺失/非整数/非正值均报 value is out of range），再按选项名换算；
/// 未识别选项报 ERR Unsupported option。换算对标 BasicCommands.cs:119-150：
/// EX/PX 相对时长，EXAT/PXAT 绝对 Unix 时间戳；折算结果不在未来时 C#
/// tsExpiry.Ticks <= 0 → expiry=0（BasicCommands.cs:175），存储层 GETEX
/// 分支 arg1==0 且非 PERSIST 时 NotUpdated，既有 TTL 保留不动
pub(crate) fn parse_getex_args<'p>(
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'p [u8], GetexExpiry)> {
  check_arg_count!(parse_state, 1..=3, output, "GETEX", return None);
  let (key, expiry) = match parse_state {
    [key] => (*key, GetexExpiry::None),
    [key, option] | [key, option, _] if option.eq_ignore_ascii_case(cs::PERSIST) => {
      (*key, GetexExpiry::Persist)
    }
    [key, option, expire_arg] => {
      let Some(expire_time) = expire_arg.try_parse_i64().filter(|&t| t > 0) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE);
        return None;
      };
      let now = now_ticks();
      let res = match try_get_expiration_option(option) {
        Some(ExpirationOption::Ex) => {
          compute_relative_expiry(now, expire_time, MAX_TIMESPAN_SECONDS, TICKS_PER_SECOND)
        }
        Some(ExpirationOption::Px) => compute_relative_expiry(
          now,
          expire_time,
          MAX_TIMESPAN_MILLISECONDS,
          TICKS_PER_MILLISECOND,
        ),
        Some(ExpirationOption::Exat) => {
          compute_absolute_expiry(expire_time, MAX_UNIX_TIME_SECONDS, TICKS_PER_SECOND)
        }
        Some(ExpirationOption::Pxat) => compute_absolute_expiry(
          expire_time,
          MAX_UNIX_TIME_MILLISECONDS,
          TICKS_PER_MILLISECOND,
        ),
        _ => {
          abort_with_unsupported_option(output, option.as_str_safe());
          return None;
        }
      };
      let target_ticks = match res {
        Ok(ticks) => ticks,
        Err(err) => {
          abort_with_error_message(output, err);
          return None;
        }
      };
      // 对标 BasicCommands.cs:175 的 `tsExpiry.Ticks > 0` 三态：折算结果
      // 不在未来即 expiry=0，既有 TTL 保留（RMWMethods.cs GETEX NotUpdated）
      let exp = if target_ticks > now {
        GetexExpiry::At(target_ticks)
      } else {
        GetexExpiry::None
      };
      (*key, exp)
    }
    [_, _] => {
      // 两参且非 PERSIST：缺少过期时长参数
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE);
      return None;
    }
    _ => unreachable!(),
  };
  Some((key, expiry))
}
