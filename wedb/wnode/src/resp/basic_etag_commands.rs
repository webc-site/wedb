//! ETag 族命令（GETWITHETAG / GETIFNOTMATCH / DELIFGREATER / SETIFMATCH /
//! SETIFGREATER / SETWITHETAG）
//!
//! 条件语义 1:1 对标 C# libs/server/Resp/BasicEtagCommands.cs 与
//! libs/server/Storage/Functions/MainStore/RMWMethods.Etags.cs /
//! ReadMethods.Etags.cs：etag 真值源为 KeyTag::Etag 旁路记录（对标 C#
//! Tsavorite LogRecord 记录尾可选 ETag 字段，NoETag = 0），条件判定
//! （等于 / 严格大于）一律以 0 为缺省基线，绝不恒真。
//!
//! 应答数组格式对标 C# RespWriteUtils.WriteEtagValArray：`[etag, value]`
//! 二元数组，etag 为 RESP 整数在前、值 bulk string 在后；条件不命中回
//! `[existingEtag, nil]`（NOGET）或 `[existingEtag, 旧值]`（SetGetFlag）。
//! nil 元素随会话协议分派（C# FunctionsState.cs:nilResp：RESP3 `_\r\n`、
//! RESP2 `$-1\r\n`），键缺失 null 同口径（C# WriteNull）。

use wbase::num::{strict_i32, strict_i64};
use wdev::Device;
use wkv::StoreResult;
use wresp::{
  check_args::{check_arg_count, parse_i64_arg, unpack_args},
  cmd_strings::{self as cs, RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, abort_with_error_message},
  command::RespCommand,
  ext::RespVecExt,
  options::{ExpirationOption, try_get_expiration_option},
};
use wval::NO_ETAG;

use super::basic_commands::{apply_set_with_expiry, try_get_absolute_expiry_ticks};
use crate::{
  resp::{EtagResume, TtlResume, resp_server_session::RespServerSession},
  storage::session::{
    common::{
      UserRead, UserReadAsync,
      etag_sync::{etag_of_sync, put_etag_sync},
      read_user_sync,
      ttl_sync::ttl_of_sync,
      user_read::finish_value_read,
    },
    storage_session::StorageSession,
  },
};

/// libs/common/RespWriteUtils.cs:WriteEtagValArray
///
/// 写 `[etag, value]` / `[etag, nil]` 二元数组（etag 整数在前，值在后）
#[inline]
fn write_etag_val_array(output: &mut Vec<u8>, etag: i64, value: Option<&[u8]>, resp_version: u8) {
  output.write_resp_array_len(2);
  output.write_resp_int(etag);
  match value {
    Some(v) => output.write_resp_bulk_string(v),
    None => output.write_resp_null_ver(resp_version),
  }
}

/// ETag 快路径旁路探针单源（GETWITHETAG / GETIFNOTMATCH / SETIFMATCH 族 /
/// SETWITHETAG 共用）：命中取旧 etag；旁路缺失（磁盘候选）就地展开为
/// `return Ok(false)` 降级；存储错误写出通用错误帧后 `return Ok(true)`
macro_rules! etag_of_sync_or_bail {
  ($store:expr, $key:expr, $output:expr) => {
    match etag_of_sync($store, $key) {
      Ok(Some(etag)) => etag,
      Ok(None) => return Ok(false),
      Err(_) => {
        $output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
  };
}

/// GETIFNOTMATCH 参数推导单源（快慢路径共用；解析失败已写出错误应答并返回
/// None）：etag 仅校验数值、不限负值（相等判定与旁路真值直接比较，
/// 对标 NetworkGETIFNOTMATCH）
fn parse_getifnotmatch_args<'p>(
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'p [u8], i64)> {
  let [key, etag_raw] = unpack_args(parse_state, output, "GETIFNOTMATCH")?;
  let given_etag = parse_i64_arg(etag_raw, output)?;
  Some((key, given_etag))
}

/// ETag 参数推导（非负整数；非数值或负值写出 INVALID_ETAG 并返回 None）
#[inline]
fn parse_etag_arg(etag_raw: &[u8], output: &mut Vec<u8>) -> Option<i64> {
  if let Some(given_etag) = strict_i64(etag_raw).filter(|&e| e >= 0) {
    Some(given_etag)
  } else {
    abort_with_error_message(output, cs::RESP_ERR_INVALID_ETAG);
    None
  }
}

/// DELIFGREATER 参数推导单源（快慢路径共用；解析失败已写出错误应答并返回
/// None）：etag 非数值或负值拒绝
fn parse_delifgreater_args<'p>(
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'p [u8], i64)> {
  let [key, etag_raw] = unpack_args(parse_state, output, "DELIFGREATER")?;
  let given_etag = parse_etag_arg(etag_raw, output)?;
  Some((key, given_etag))
}

/// 条件写命令判别（SETIFMATCH / SETIFGREATER）
enum EtagCondCmd {
  /// givenEtag == existingEtag 命中，新 etag = givenEtag + 1
  IfMatch,
  /// givenEtag > existingEtag 命中，新 etag = givenEtag
  IfGreater,
}

/// SETIFMATCH / SETIFGREATER 参数推导单源（快慢路径共用）
struct EtagConditionalArgs<'p> {
  key: &'p [u8],
  val: &'p [u8],
  given_etag: i64,
  /// EX/PX 的秒或毫秒数（0 = 无过期）
  expiry: i64,
  /// PX 口径（expiry 单位为毫秒）
  high_precision: bool,
  /// NOGET：条件不命中应答折叠为 `[existing, nil]`
  no_get: bool,
}

/// SET/ETAG 过期数值解析：正整数有效，非正报 INVALIDEXP_IN_SET，缺失/非数字报 VALUE_IS_NOT_INTEGER
#[inline]
fn parse_exp_in_set(raw: Option<&[u8]>) -> Result<i64, &'static str> {
  match raw.and_then(strict_i32) {
    Some(e) if e > 0 => Ok(i64::from(e)),
    Some(_) => Err(cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET),
    None => Err(cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER),
  }
}

/// SETIFMATCH / SETIFGREATER 的参数推导单源（快慢路径共用；解析失败时
/// 已写出错误应答并返回 None）
///
/// 选项循环（C# :212-257）：NOGET 去重；EX|PX 后跟正整数 expiry；其余 token
/// 一律 syntax error；etag 校验（C# :259-264）非数值或负值拒绝，且文案覆盖
/// 先前的选项错误
fn parse_etag_conditional_args<'p>(
  parse_state: &[&'p [u8]],
  cmd_name: &str,
  output: &mut Vec<u8>,
) -> Option<EtagConditionalArgs<'p>> {
  check_arg_count!(parse_state, 3..=6, output, cmd_name, return None);
  let (key, val, etag_raw) = (parse_state[0], parse_state[1], parse_state[2]);

  // 选项循环（C# :212-257）：NOGET 去重；EX|PX 后跟正整数 expiry；
  // 其余 token 一律 syntax error
  let mut expiry = 0i64;
  let mut high_precision = false;
  let mut no_get = false;
  let mut error: Option<&str> = None;
  let mut token_idx = 3;
  while token_idx < parse_state.len() {
    let token = parse_state[token_idx];
    if token.eq_ignore_ascii_case(cs::NOGET.as_bytes()) {
      if no_get {
        error = Some(cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        break;
      }
      no_get = true;
      token_idx += 1;
      continue;
    }
    if let Some(opt) = try_get_expiration_option(token) {
      if !matches!(opt, ExpirationOption::Ex | ExpirationOption::Px) {
        error = Some(cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        break;
      }
      high_precision = opt == ExpirationOption::Px;
      token_idx += 1;
      // C# parseState.TryGetInt（int32 域，:239）：超 int 范围回 not integer；前导零拒收系 rust 严格收口，见 doc/zh/deviations.md §32
      match parse_exp_in_set(parse_state.get(token_idx).copied()) {
        Ok(e) => expiry = e,
        Err(err) => {
          error = Some(err);
          break;
        }
      }
      token_idx += 1;
      continue;
    }
    error = Some(cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
    break;
  }

  // etag 校验（C# :259-264）：非数值或负值拒绝，且文案覆盖选项错误
  let given_etag = parse_etag_arg(etag_raw, output)?;
  if let Some(err) = error {
    abort_with_error_message(output, err);
    return None;
  }

  Some(EtagConditionalArgs {
    key,
    val,
    given_etag,
    expiry,
    high_precision,
    no_get,
  })
}

impl EtagCondCmd {
  /// 命中判定（对标 RMWMethods.Etags.cs:HandleSetIfMatch* 的
  /// comparisonResult != expectedResult 条件）
  #[inline]
  const fn hit(&self, given: i64, existing: i64) -> bool {
    match *self {
      Self::IfMatch => given == existing,
      Self::IfGreater => given > existing,
    }
  }

  /// 命中后的新 etag（SETIFMATCH 以客户端 etag 为基 +1；SETIFGREATER 直取）
  #[inline]
  const fn next_etag(&self, given: i64) -> i64 {
    match *self {
      Self::IfMatch => given + 1,
      Self::IfGreater => given,
    }
  }
}

impl RespServerSession {
  /// ETag 读族快路径应答单源（GETWITHETAG / GETIFNOTMATCH 尾部共用）：旁路
  /// 探针 → 值读出帧 → [`finish_value_read`] 收尾；`if_not_match` 为 Some 且
  /// 等于旁路 etag 时值折叠为 nil（对标 HandleEtagReader），None = 恒带值
  #[inline]
  fn etag_read_fast<D: Device>(
    &self,
    store: &wkv::BatchStoreSession<'_, D>,
    key: &[u8],
    if_not_match: Option<i64>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let resp_version = self.resp_protocol_version;
    let current = etag_of_sync_or_bail!(store, key, output);
    let start_len = output.len();
    let read = read_user_sync(store, key, None, |v| {
      let value = (if_not_match != Some(current)).then_some(v);
      write_etag_val_array(output, current, value, resp_version);
    });
    Ok(finish_value_read(read, output, Some(start_len), |out| {
      out.write_resp_null_ver(resp_version)
    }))
  }

  /// libs/server/Resp/BasicEtagCommands.cs:NetworkGETWITHETAG
  ///
  /// 键不存在 → null；存在 → `[etag, value]`（无 etag 记录即 0）
  pub fn network_getwithetag<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "GETWITHETAG") else {
      return Ok(true);
    };
    self.etag_read_fast(store, key, None, output)
  }

  /// libs/server/Resp/BasicEtagCommands.cs:NetworkGETIFNOTMATCH
  ///
  /// 键不存在 → null；etag 匹配 → `[etag, nil]`；不匹配 → `[etag, value]`
  /// （对标 ReadMethods.Etags.cs:HandleEtagReader）
  pub fn network_getifnotmatch<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some((key, given_etag)) = parse_getifnotmatch_args(parse_state, output) else {
      return Ok(true);
    };
    self.etag_read_fast(store, key, Some(given_etag), output)
  }

  /// libs/server/Resp/BasicEtagCommands.cs:NetworkDELIFGREATER
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:DEL_Conditional
  ///
  /// C# 三层链路（Resp 命令 → GarnetApi → StorageSession RMW）在此合并实现。
  /// 仅当 givenEtag > existingEtag 才真实删除（RMWMethods.Etags.cs:
  /// HandleDelIfGreater* 的 ExpireAndStop 条件）；etag 非数值或负值拒绝。
  /// 删除经 `try_delete_sync` 级联清理 TTL 与 ETag 旁路记录。
  /// 对象键拦截：C# RMW（HandleEtagNeedCopyUpdate 对 RecordType != 0 置
  /// RMWAction.WrongType）判型先于 etag 比较，未过期即
  /// NOTFOUND → keysDeleted = 0 回 `:0`，键保留
  pub fn network_delifgreater<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some((key, given_etag)) = parse_delifgreater_args(parse_state, output) else {
      return Ok(true);
    };

    // 条件删除整段同窗（票 zcode-r32-rmwmatrix 立项二：对标 C#
    // DEL_ETagConditional 走 DEL_Conditional 的记录闩内条件判定——判定与
    // 删除两步分离时，并发 SETIFMATCH 抬 etag 落库即「基于陈旧 etag 的判定
    // 误删他客户端刚确认的条件写」）；失闩沿 Ok(false) 降级慢路径同段持窗
    // 重放
    let Some(_window) = store.try_rmw_window(key) else {
      return Ok(false);
    };

    match read_user_sync(store, key, None, |_| ()) {
      // 数据 / TTL 记录有磁盘候选，或键已过期待物理清除：降级异步
      Ok(UserRead::Deferred) => return Ok(false),
      // 对象键：判型先于 etag 比较，一律不删（键存活）
      // 键缺失：无删除目标
      Ok(UserRead::WrongType | UserRead::Missing) => output.write_resp_int(0),
      // String 键存活：按 etag 条件判定删除
      Ok(UserRead::Hit(())) => match etag_of_sync(store, key) {
        Ok(Some(existing)) if given_etag > existing => match store.try_delete_sync(key) {
          Ok(Ok(_)) => output.write_resp_int(1),
          Ok(Err(_)) => return Ok(false),
          Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
        },
        Ok(Some(_)) => output.write_resp_int(0),
        Ok(None) => return Ok(false),
        Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
      },
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicEtagCommands.cs:NetworkSETIFMATCH
  pub fn network_setifmatch<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.network_set_etag_conditional(
      EtagCondCmd::IfMatch,
      "SETIFMATCH",
      parse_state,
      store,
      output,
    )
  }

  /// libs/server/Resp/BasicEtagCommands.cs:NetworkSETIFGREATER
  pub fn network_setifgreater<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.network_set_etag_conditional(
      EtagCondCmd::IfGreater,
      "SETIFGREATER",
      parse_state,
      store,
      output,
    )
  }

  /// libs/server/Resp/BasicEtagCommands.cs:NetworkSETWITHETAG
  ///
  /// 无条件写，etag = existing + 1（初始 NoETag + 1 = 1），应答新 etag 整数；
  /// EX/PX 设置过期，无 expiry 清除既有过期（SET 语义）；缺失/过期键按初写
  /// 口径 etag 域从头计（C# 过期臂先行 RemoveETag，案三对齐，旁路残值不参与）
  pub fn network_setwithetag<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 进入命令即复位本族续跑标记（沿 SET 的 ttl_resume 复位纪律，杜绝跨命令残留）
    self.etag_resume = EtagResume::Full;
    check_arg_count!(parse_state, 2..=4, output, "SETWITHETAG");
    let (key, val) = (parse_state[0], parse_state[1]);
    let (expiry, high_precision) = match parse_setwithetag_expiry(&parse_state[2..]) {
      Ok(opts) => opts,
      Err(err) => {
        abort_with_error_message(output, err);
        return Ok(true);
      }
    };

    // etag 读旧 → 递增 → 写值写 etag 整段同窗（票 zcode-r32-rmwmatrix 立项
    // 二：对标 C# SETWITHETAG 单次 RMW 锁内 HandleSetWithEtag*——两步分离时
    // 并发 SETWITHETAG 双双读得 existing=E 写回 E+1，etag 严格递增承诺丢失）；
    // 失闩沿 Ok(false) 降级慢路径同段持窗重放
    let Some(_window) = store.try_rmw_window(key) else {
      return Ok(false);
    };
    // 存活裁决单点分流（票 zcode-r139c-etag2 案三）：复用 read_user_sync 已握
    // 的 Hit/Missing/Deferred/WrongType 裁决定既有无旁路残值——C# 过期记录上
    // ETag RMW 在 InPlaceUpdaterWorker 过期臂先行 RemoveETag + ExpireAndResume
    // （RMWMethods.cs:441-446），随后按 HandleSetWithEtagInitialUpdate 初写口径
    // newEtag = NoETag + 1（RMWMethods.Etags.cs:289-311）——过期边界后 etag 域
    // 从头计；rust ETag 为 KeyTag::Etag 独立旁路记录、自身无 TTL
    // （etag_sync.rs:24-48 无存活门），旁路直探 etag_of_sync 会读得过期未回收键
    // 残留旧 etag 回 stale+1，与慢臂 read_value_and_etag_async Missing 臂
    // （初写回 1）及 C# 三面分叉
    let new_etag = match read_user_sync(store, key, None, |_| ()) {
      // 对象键走 C# promote 删写（ExecuteETagSetCommand WRONGTYPE 分支对
      // SETWITHETAG 同径）：先 DELETE 对象键，再按初写口径固定新 etag =
      // NoETag + 1（HandleSetWithEtagInitialUpdate），应答整数
      Ok(UserRead::WrongType) => {
        match promote_delete_object_key(store, key, output) {
          Ok(true) => {}
          Ok(false) => return Ok(false),
          Err(()) => return Ok(true),
        }
        NO_ETAG + 1
      }
      // 键存活：读 etag 旁路旧值参与递增（旁路记录缺失 = NoETag 0）；存储错误
      // 维持既有口径交旁路探针与慢臂终裁
      Ok(UserRead::Hit(())) | Err(_) => {
        let existing = etag_of_sync_or_bail!(store, key, output);
        existing + 1
      }
      // 缺失（含过期已裁决态）→ 初写口径，不读旁路残值（本案要害：修复前
      // 本臂照探 etag_of_sync 回 stale+1）
      Ok(UserRead::Missing) => NO_ETAG + 1,
      // Deferred：任一域磁盘候选不可内存闭环，沿既有降级通道交慢臂（慢臂
      // Missing 臂已同口径闭环）；不新增第二存活探针
      Ok(UserRead::Deferred) => return Ok(false),
    };
    let args = EtagSetArgs::plain(key, val, expiry, high_precision);
    match apply_etag_write(store, &args, new_etag, output, &mut self.etag_resume) {
      Ok(true) => output.write_resp_int(new_etag),
      Ok(false) => return Ok(false),
      Err(()) => {}
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicEtagCommands.cs:NetworkSetETagConditional
  ///
  /// SETIFMATCH / SETIFGREATER 共同实现：选项循环（NOGET 去重、EX|PX 校验）
  /// 后 etag 校验（C# :259-264 顺序：etag 错误文案覆盖先前的选项错误），
  /// 再按条件真实判定写入
  fn network_set_etag_conditional<'a, D: Device>(
    &mut self,
    cmd: EtagCondCmd,
    cmd_name: &str,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 进入命令即复位本族续跑标记（沿 SET 的 ttl_resume 复位纪律，杜绝跨命令残留）
    self.etag_resume = EtagResume::Full;
    let Some(args) = parse_etag_conditional_args(parse_state, cmd_name, output) else {
      return Ok(true);
    };
    let resp_version = self.resp_protocol_version;
    let EtagConditionalArgs {
      key,
      val,
      given_etag,
      expiry,
      high_precision,
      no_get,
    } = args;

    // 条件写整段同窗（票 zcode-r32-rmwmatrix 立项二：对标 C#
    // SET_Conditional 记录闩内 updater 判定 HandleSetIfMatch*——条件判定
    // （读值读 etag）与写入（写值写 TTL 写 etag）共一个线性化点，并发
    // SETIFMATCH 同 etag 恰一者命中；两步分离即比较并交换语义双成功）；
    // 失闩沿 Ok(false) 降级慢路径同段持窗重放
    let Some(_window) = store.try_rmw_window(key) else {
      return Ok(false);
    };

    let existing = etag_of_sync_or_bail!(store, key, output);

    let start_len = output.len();
    let hit_mismatch = !cmd.hit(given_etag, existing);
    match read_user_sync(store, key, None, |v| {
      if hit_mismatch {
        let value = if no_get { None } else { Some(v) };
        write_etag_val_array(output, existing, value, resp_version);
      }
    }) {
      Ok(UserRead::Hit(())) => {
        if hit_mismatch {
          return Ok(true);
        }
        let new_etag = cmd.next_etag(given_etag);
        // 命中且 expiry == 0：保留旧 TTL（C# CopyUpdate
        // TrySetExpiration(arg1 != 0 ? arg1 : srcRecord.Expiration)）
        let keep_ttl = if expiry == 0 {
          Some(match ttl_of_sync(store, key) {
            Ok(StoreResult::Success(t)) => t,
            Ok(StoreResult::NotFound) => None,
            Ok(StoreResult::RecordOnDisk) => {
              output.truncate(start_len);
              return Ok(false);
            }
            Err(_) => {
              output.truncate(start_len);
              output.write_resp_error(RESP_ERR_GENERIC);
              return Ok(true);
            }
          })
        } else {
          None
        };
        let args = EtagSetArgs {
          key,
          val,
          expiry,
          high_precision,
          keep_ttl,
        };
        match apply_etag_write(store, &args, new_etag, output, &mut self.etag_resume) {
          Ok(true) => write_etag_val_array(output, new_etag, None, resp_version),
          Ok(false) => {
            output.truncate(start_len);
            return Ok(false);
          }
          Err(()) => {}
        }
      }
      Ok(UserRead::Missing) => {
        // 键不存在（对标 HandleSetIfMatchInitialUpdate）：无条件写入，
        // newEtag = givenEtag + (SETIFMATCH ? 1 : 0)，初始记录无旧 TTL 保留
        let args = EtagSetArgs::plain(key, val, expiry, high_precision);
        let new_etag = cmd.next_etag(given_etag);
        return self.etag_initial_write_fast(store, &args, new_etag, output);
      }
      Ok(UserRead::WrongType) => {
        // C# ExecuteETagSetCommand WRONGTYPE 分支：promote 删写语义——
        // 先 DELETE 对象键，再按 InitialUpdater 无条件初写（条件判定不在
        // 初写路径，NOGET 标志同样不参与），应答 `[newEtag, nil]`
        let args = EtagSetArgs::plain(key, val, expiry, high_precision);
        let new_etag = cmd.next_etag(given_etag);
        match promote_delete_object_key(store, key, output) {
          Ok(true) => {}
          Ok(false) => return Ok(false),
          Err(()) => return Ok(true),
        }
        return self.etag_initial_write_fast(store, &args, new_etag, output);
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
    Ok(true)
  }

  /// 快路径初写收尾（Missing / WrongType 两臂共用）：写闭环应答
  /// `[new_etag, nil]`；降级直接回 `Ok(false)`——两臂自 start_len 起未向
  /// output 写过帧，故不截断（与 Hit 不命中臂有别）
  fn etag_initial_write_fast<D: Device>(
    &mut self,
    store: &wkv::BatchStoreSession<'_, D>,
    args: &EtagSetArgs<'_>,
    new_etag: i64,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    match apply_etag_write(store, args, new_etag, output, &mut self.etag_resume) {
      Ok(true) => write_etag_val_array(output, new_etag, None, self.resp_protocol_version),
      Ok(false) => return Ok(false),
      Err(()) => {}
    }
    Ok(true)
  }
}

/// SETWITHETAG 的 `[EX|PX] [expiry]` 尾部解析（对应 C#
/// BasicEtagCommands.cs:154-181 NetworkSETWITHETAG 内嵌校验链）：
/// 空参 → 无过期；首 token 须为 EX/PX（其余过期形式与未知 token 均
/// syntax error）；expiry 须为正整数（C# TryGetInt int32 域，:167：
/// 缺失/非整数/超 int 范围 → not integer，非正 → invalid expire；前导零拒收系 rust 严格收口，见 doc/zh/deviations.md §32）
fn parse_setwithetag_expiry(args: &[&[u8]]) -> Result<(i64, bool), &'static str> {
  let Some(&token) = args.first() else {
    return Ok((0, false));
  };
  match try_get_expiration_option(token) {
    Some(opt @ (ExpirationOption::Ex | ExpirationOption::Px)) => {
      let expiry = parse_exp_in_set(args.get(1).copied())?;
      Ok((expiry, opt == ExpirationOption::Px))
    }
    _ => Err(cs::RESP_ERR_GENERIC_SYNTAX_ERROR),
  }
}

/// ETag 写共同体入参（对标 C# ExecuteETagSetCommand 的参数组）
struct EtagSetArgs<'k> {
  key: &'k [u8],
  val: &'k [u8],
  /// EX/PX 的秒或毫秒数（0 = 无过期）
  expiry: i64,
  /// PX 口径（expiry 单位为毫秒）
  high_precision: bool,
  /// 旧 TTL 保留：`None` 为 SET 语义（expiry == 0 保持无 TTL），`Some(old)`
  /// 为 SETIFMATCH 族保留语义（expiry == 0 回填旧 TTL）
  keep_ttl: Option<Option<i64>>,
}

impl<'k> EtagSetArgs<'k> {
  /// SET 语义便捷构造（keep_ttl = None：expiry == 0 清 TTL / 保持无 TTL；
  /// 初写与无条件写臂共用）
  #[inline]
  fn plain(key: &'k [u8], val: &'k [u8], expiry: i64, high_precision: bool) -> Self {
    Self {
      key,
      val,
      expiry,
      high_precision,
      keep_ttl: None,
    }
  }
}

/// ETag 写共同体：upsert 值（自带清 TTL）→ 按 keep_ttl 回填或写新过期 →
/// 推进 etag 标签
///
/// 前置契约：调用方须已持本键读改写窗口（`try_rmw_window`）——「清 TTL →
/// 值写 → 写 TTL → 写 etag」跨调用交叠的串行化由调用方窗口承接（对标 C#
/// SET_Conditional 记录闩内 updater 一体完成，票 zcode-r32-rmwmatrix 立项二）
///
/// 值写失败时 etag 未动，重入重发幂等
///
/// 降级信号必达调用方（票 zcode-r139c-etag2 案一）：值腿三处 `Ok(false)`
/// （RI 门磁盘候选 / `try_upsert_sync` 页翻转 / `put_ttl_sync` 页翻转）任一
/// 命中即整体回 `Ok(false)`，绝不在值未落库时前推 etag 侧写出成功帧（假 ACK
/// 丢写 + CAS 漂移）；值腿已提交而 TTL / etag 余腿遭环形页翻转降级时，把已裁决
/// 新 etag 与待投刻度置入 `resume`（[`EtagResume`]），交慢臂持窗补投余腿，
/// 杜绝整命令重放自碰已提交值
///
/// `Ok(true)` 写闭环（应答由调用方续写）；`Ok(false)` 须降级异步；
/// `Err(())` 存储错误且已写出应答，调用方绝不再续写
fn apply_etag_write<D: Device>(
  store: &wkv::BatchStoreSession<'_, D>,
  args: &EtagSetArgs<'_>,
  new_etag: i64,
  output: &mut Vec<u8>,
  resume: &mut EtagResume,
) -> Result<bool, ()> {
  let EtagSetArgs {
    key,
    val,
    expiry,
    high_precision,
    keep_ttl,
  } = *args;
  // 值腿的 TTL 降级刻度经本局标记折入 EtagResume 尾参（ETag 族不占 SET/RESTORE
  // 的 ttl_resume 会话槽，两族各携本族成功帧所需载荷）
  let mut ttl_resume = TtlResume::Full;
  if !apply_set_with_expiry(
    store,
    key,
    val,
    (expiry, high_precision),
    keep_ttl,
    &mut ttl_resume,
    output,
  )? {
    if let TtlResume::Pending(ticks) = ttl_resume {
      *resume = EtagResume::Pending { ticks, new_etag };
    }
    return Ok(false);
  }
  match put_etag_sync(store, key, new_etag) {
    Ok(true) => Ok(true),
    // 值与 TTL 腿已闭环、etag 腿遭环形页翻转降级：携新 etag 交慢臂补投
    Ok(false) => {
      *resume = EtagResume::Pending { ticks: 0, new_etag };
      Ok(false)
    }
    Err(_) => Err(()),
  }
}

/// libs/server/Resp/BasicEtagCommands.cs:ExecuteETagSetCommand（WRONGTYPE 分支）
///
/// promote 删写第一步：对象键整体删除。C# 对 ValueIsObject 记录先收
/// WRONGTYPE，再在 promote 事务（Main | Object 双域排他锁）内 DELETE 统一存
/// 键；Rust 侧单会话直连免 promote，`try_delete_sync` 双域级联一并清理
/// TTL 与 ETag 旁路记录。返回 `Ok(false)` 须降级异步，`Err(())` 已答通用错误
fn promote_delete_object_key<D: Device>(
  store: &wkv::BatchStoreSession<'_, D>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  match store.try_delete_sync(key) {
    Ok(Ok(_)) => Ok(true),
    // 环形页翻转 / 复合元数据：降级全异步路径重入
    Ok(Err(_)) => Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      Err(())
    }
  }
}

/// ETag 读域结果（异步口，[`ReadOutcome`] 的磁盘候选闭环对偶）
enum ReadOutcomeAsync {
  /// 键存在（值 + etag，无 etag 记录即 NoETag 0）
  Hit(Vec<u8>, i64),
  /// 键不存在
  Missing,
  /// 集合对象（WRONGTYPE）
  WrongType,
}

/// ETag 读共同体（异步口）：值（含 TTL 裁决 + WRONGTYPE 判定）与 etag 一次
/// 读齐，磁盘候选在 await 内闭环（对标快路径 [`read_value_and_etag`]，
/// 慢路径无降级态）
async fn read_value_and_etag_async(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
) -> Result<ReadOutcomeAsync, ()> {
  match storage
    .read_user(key, |v| v.to_vec())
    .await
    .map_err(|_| ())?
  {
    UserReadAsync::Hit(val) => {
      let etag = storage
        .batch
        .etag_of(key)
        .await
        .map_err(|_| ())?
        .unwrap_or(NO_ETAG);
      Ok(ReadOutcomeAsync::Hit(val, etag))
    }
    UserReadAsync::WrongType => Ok(ReadOutcomeAsync::WrongType),
    UserReadAsync::Missing => Ok(ReadOutcomeAsync::Missing),
  }
}

/// ETag 读族慢路径应答单源（GETWITHETAG / GETIFNOTMATCH 两臂共用，快路径
/// [`RespServerSession::etag_read_fast`] 的异步对偶）：值 + etag 一次读齐、
/// 磁盘候选 await 内闭环；`if_not_match` 语义同快径
async fn etag_read_slow(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  if_not_match: Option<i64>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let resp_version = storage.resp_version;
  match read_value_and_etag_async(storage, key).await? {
    ReadOutcomeAsync::Hit(val, current) => {
      let value = (if_not_match != Some(current)).then_some(val.as_slice());
      write_etag_val_array(output, current, value, resp_version);
    }
    ReadOutcomeAsync::Missing => output.write_resp_null_ver(resp_version),
    ReadOutcomeAsync::WrongType => output.write_resp_error(RESP_ERR_WRONG_TYPE),
  }
  Ok(())
}

/// ETag 写共同体（异步口）：RI 门 → upsert 值（自带清 TTL）→ 按 keep_ttl
/// 回填或写新过期 → 推进 etag 标签
///
/// 前置契约：调用方须已持本键读改写窗口（`rmw_window`），契约同快路径
/// [`apply_etag_write`]（票 zcode-r32-rmwmatrix 立项二）
///
/// 出帧责任全在调用方（票 zcode-r139c-etag2 案二，与
/// [`crate::storage::session::storage_session::StorageSession::ri_write_gate`]
/// 头注「WRONGTYPE 由调用方出帧」及 SET 慢臂 `slow_set_conditional` 同款纪律）：
/// `Ok(true)` 写闭环（成功帧由调用方续写）；`Ok(false)` RI 门挡写且本函数零出
/// 帧，调用方须按本命令口径出单 WRONGTYPE 帧后立即返回，绝不再续写成功帧
/// （C# 一命令恒一应答帧，ExecuteETagSetCommand 的 ProcessOutput 恰调用一次，
/// BasicEtagCommands.cs:293-310）；`Err(())` 存储错误零出帧，交 exec_slow 统一
/// 应答面出单帧
///
/// 挡写绝不经 `Err(())`：快臂同径（set.rs:149-150 门内已答 + 调用方
/// `Err(()) => {}` 抑制成功帧）成立只因快臂无叠帧应答面，慢臂 `Err(())` 出口
/// 会在 WRONGTYPE 之上再被 exec_slow 追一帧 RESP_ERR_SLOW_PATH_STORAGE，
/// 正是本案要灭的双帧形态
async fn apply_etag_write_async(
  storage: &StorageSession<'_, impl Device>,
  args: &EtagSetArgs<'_>,
  new_etag: i64,
) -> Result<bool, ()> {
  // RI 键门（写共同体单点前置，异步对偶快路径 apply_set_with_expiry 内的
  // ri_write_gate）：存活 RangeIndex 上拒写，挡写帧交调用方按本命令口径出
  if storage.ri_write_gate(args.key).await.map_err(|_| ())? {
    return Ok(false);
  }
  let expire_at_ticks = if args.expiry != 0 {
    try_get_absolute_expiry_ticks(args.expiry, args.high_precision).ok_or(())?
  } else {
    0
  };
  storage
    .upsert_string(args.key, args.val)
    .await
    .map_err(|_| ())?;
  match args.keep_ttl {
    // KEEPTTL 保留族：upsert 已同步清 TTL，按旧值回填（本不存在保持无 TTL）
    Some(Some(ticks)) => {
      storage
        .batch
        .put_ttl(args.key, ticks)
        .await
        .map_err(|_| ())?;
    }
    None if args.expiry != 0 => {
      storage
        .batch
        .put_ttl(args.key, expire_at_ticks)
        .await
        .map_err(|_| ())?;
    }
    _ => {}
  }
  storage
    .batch
    .put_etag(args.key, new_etag)
    .await
    .map_err(|_| ())?;
  Ok(true)
}

/// 快臂值已同步提交、TTL / etag 余腿遭环形页翻转降级时的持窗补投余腿单源
/// （案一情形 B，SETIFMATCH 族 / SETWITHETAG 慢臂共用）：ticks 非零即回填
/// 待投刻度，再推进 etag 侧写；成功帧由调用方按本命令口径续写
async fn replay_pending_legs(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  ticks: i64,
  new_etag: i64,
) -> Result<(), ()> {
  if ticks != 0 {
    storage.batch.put_ttl(key, ticks).await.map_err(|_| ())?;
  }
  storage
    .batch
    .put_etag(key, new_etag)
    .await
    .map_err(|_| ())?;
  Ok(())
}

/// 慢臂 WrongType 的 promote 删写前置挡写门（SETIFMATCH 族 / SETWITHETAG
/// 两臂共用单源，票 wnode-ri-cold-etag-gate-unreached 定裁产品侧）：
/// RI 存活元记录先于删写早停——单 WRONGTYPE 帧零副作用返回 `Ok(false)`；
/// 信封对象键返回 `Ok(true)` 交调用方续走 promote 删写初写臂
///
/// 要害：`delete_string` 是统一级联删除（含 Meta 域与树存根，
/// `StorageSession::delete_string` 降级臂走 wkv collection 层 delete），先删
/// 后挡会使 [`StorageSession::ri_write_gate`] 恒读 Missing——挡写形同虚设且
/// 把存活 RI 毁成无主幽灵（快臂同径靠 `try_delete_sync` 对复合元记录
/// `Ok(Err)` 零副作用降级才幸免，wkv write/mod.rs:592-593）
///
/// C# 映射：ExecuteETagSetCommand 的 promote DELETE
/// （BasicEtagCommands.cs:299-307）只覆盖对象存 ValueIsObject 键；
/// RangeIndex 系本仓自定义扩展类型，其写面挡写判据与
/// [`crate::resp::basic_commands::set::ri_write_gate`] 一族同判据单点
/// （`meta_is_range_index`），慢臂门内前置不得被删写先行毁形
async fn slow_wrongtype_promote_gate(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  if storage.ri_write_gate(key).await.map_err(|_| ())? {
    output.write_resp_error(RESP_ERR_WRONG_TYPE);
    return Ok(false);
  }
  storage.delete_string(key).await.map_err(|_| ())?;
  Ok(true)
}

/// 条件写成功收尾单源（Hit 命中 / Missing 初写 / WrongType promote 删写三臂
/// 共用）：RI 门挡写按本命令口径出单 WRONGTYPE 帧后立即返回（案二），写闭环
/// 出成功帧 `[new_etag, nil]`
async fn write_conditional_success(
  storage: &StorageSession<'_, impl Device>,
  set_args: &EtagSetArgs<'_>,
  new_etag: i64,
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  if !apply_etag_write_async(storage, set_args, new_etag).await? {
    output.write_resp_error(RESP_ERR_WRONG_TYPE);
    return Ok(());
  }
  write_etag_val_array(output, new_etag, None, resp_version);
  Ok(())
}

/// SETIFMATCH / SETIFGREATER 慢路径共同实现体（快路径
/// `network_set_etag_conditional` 的异步对偶：持窗重放，序次与应答形态逐
/// 字节一致）
///
/// `resume`：快路径快照尾参续跑标记（[`EtagResume`]，票 zcode-r139c-etag2
/// 案一情形 B）——[`EtagResume::Pending`] 表示值腿已同步提交、TTL / etag
/// 余腿遭环形页翻转降级，本臂复取同窗仅补投余腿出成功帧，绝不整命令重放
/// 自碰已提交值（快臂 Missing / WrongType 臂重放后读态翻成 Hit，条件重判
/// 即失配回 `[0, 新值]`、etag 侧写永缺）
async fn etag_conditional_slow(
  storage: &StorageSession<'_, impl Device>,
  cmd: EtagCondCmd,
  args: &EtagConditionalArgs<'_>,
  resume: EtagResume,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let EtagConditionalArgs {
    key,
    val,
    given_etag,
    expiry,
    high_precision,
    no_get,
  } = *args;
  let resp_version = storage.resp_version;
  // 条件写整段同窗（快路径同一窗口契约）
  let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
  if let EtagResume::Pending { ticks, new_etag } = resume {
    // 值已提交：持同一窗补投待投 TTL（ticks 非零即是）与 etag 侧写，应答与
    // 快臂成功帧逐字节一致（对标 C# 单记录 RMW 锁内值 / 过期 / etag 一体落库
    // 的终态）
    replay_pending_legs(storage, key, ticks, new_etag).await?;
    write_etag_val_array(output, new_etag, None, resp_version);
    return Ok(());
  }
  let outcome = read_value_and_etag_async(storage, key).await?;
  // WrongType：promote 删写第一步（快路径同终态：先 DELETE 对象键再初写）；
  // RI 存活元记录经挡写门早停（单 WRONGTYPE 帧零副作用，绝不得先删毁形）
  if matches!(outcome, ReadOutcomeAsync::WrongType)
    && !slow_wrongtype_promote_gate(storage, key, output).await?
  {
    return Ok(());
  }
  let new_etag = cmd.next_etag(given_etag);
  match outcome {
    ReadOutcomeAsync::Hit(old_val, existing) => {
      if !cmd.hit(given_etag, existing) {
        // 条件不命中：不写。NOGET → [existing, nil]；否则回旧值
        let value = if no_get { None } else { Some(&old_val) };
        write_etag_val_array(output, existing, value.map(|v| v.as_slice()), resp_version);
        return Ok(());
      }
      // 命中且 expiry == 0：保留旧 TTL（快路径同序）
      let keep_ttl = if expiry == 0 {
        Some(storage.batch.ttl_of(key).await.map_err(|_| ())?)
      } else {
        None
      };
      let set_args = EtagSetArgs {
        key,
        val,
        expiry,
        high_precision,
        keep_ttl,
      };
      write_conditional_success(storage, &set_args, new_etag, resp_version, output).await
    }
    // Missing 初写 / WrongType 删后初写（对标 HandleSetIfMatchInitialUpdate：
    // 无条件写入，条件判定与 NOGET 不参与初写臂）
    ReadOutcomeAsync::Missing | ReadOutcomeAsync::WrongType => {
      let set_args = EtagSetArgs::plain(key, val, expiry, high_precision);
      write_conditional_success(storage, &set_args, new_etag, resp_version, output).await
    }
  }
}

/// ETag 族慢路径执行段入口（exec_slow 分派；`Err(())` 为存储错误，调用方
/// 统一应答）
///
/// 六命令快路径失闩 / 磁盘候选降级承接（票 zcode-r32-rmwmatrix 立项二）：
/// 解析一律转调快侧同一推导单源，读共同体异步闭环（值与 etag 磁盘候选在
/// await 内收口），写臂持本键读改写窗口重放（对标 C# SET_Conditional /
/// DEL_Conditional 记录闩内全程），应答形态与快路径逐字节一致
pub(crate) async fn etag_slow(
  storage: &StorageSession<'_, impl Device>,
  cmd: RespCommand,
  parse_state: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  // 写族快照尾参剥除（[`EtagResume::from_tail`]，沿 MSETNX / DEL / SET 尾参
  // 先例，exec 降级快照对 SETIFMATCH / SETIFGREATER / SETWITHETAG 恒追加
  // 17 字节续跑标记，与 garnet_api 推入侧同一命令集，票 zcode-r139c-etag2
  // 案一情形 B）：读族快照无尾参，一律按 Full 全量重放既有形态
  let (parse_state, resume) = match cmd {
    RespCommand::Setifmatch | RespCommand::Setifgreater | RespCommand::Setwithetag => {
      match parse_state.split_last() {
        Some((tail, args)) => (args, EtagResume::from_tail(Some(tail))),
        // 快路径校验 arity 通过后才降级，快照恒为「≥ 2 参数 + 尾参」；形态
        // 不符即快照损坏，本臂不写帧交调用方统一应答单帧存储错误
        None => return Err(()),
      }
    }
    _ => (parse_state, EtagResume::Full),
  };
  match cmd {
    RespCommand::Getwithetag => {
      let Some([key]) = unpack_args(parse_state, output, "GETWITHETAG") else {
        return Ok(());
      };
      etag_read_slow(storage, key, None, output).await
    }
    RespCommand::Getifnotmatch => {
      let Some((key, given_etag)) = parse_getifnotmatch_args(parse_state, output) else {
        return Ok(());
      };
      etag_read_slow(storage, key, Some(given_etag), output).await
    }
    RespCommand::Delifgreater => {
      let Some((key, given_etag)) = parse_delifgreater_args(parse_state, output) else {
        return Ok(());
      };
      // 条件删除整段同窗（快路径同一窗口契约）
      let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      match read_value_and_etag_async(storage, key).await? {
        // 对象键：判型先于 etag 比较，一律不删（键存活）
        ReadOutcomeAsync::WrongType | ReadOutcomeAsync::Missing => output.write_resp_int(0),
        ReadOutcomeAsync::Hit(_, existing) => {
          if given_etag > existing {
            let deleted = storage.delete_string(key).await.map_err(|_| ())?;
            output.write_resp_int(i64::from(deleted));
          } else {
            output.write_resp_int(0);
          }
        }
      }
      Ok(())
    }
    RespCommand::Setifmatch => {
      let Some(args) = parse_etag_conditional_args(parse_state, "SETIFMATCH", output) else {
        return Ok(());
      };
      etag_conditional_slow(storage, EtagCondCmd::IfMatch, &args, resume, output).await
    }
    RespCommand::Setifgreater => {
      let Some(args) = parse_etag_conditional_args(parse_state, "SETIFGREATER", output) else {
        return Ok(());
      };
      etag_conditional_slow(storage, EtagCondCmd::IfGreater, &args, resume, output).await
    }
    RespCommand::Setwithetag => {
      check_arg_count!(parse_state, 2..=4, output, "SETWITHETAG", return Err(()));
      let (key, val) = (parse_state[0], parse_state[1]);
      let (expiry, high_precision) = match parse_setwithetag_expiry(&parse_state[2..]) {
        Ok(opts) => opts,
        Err(err) => {
          abort_with_error_message(output, err);
          return Ok(());
        }
      };
      // etag 读旧 → 递增 → 写值写 etag 整段同窗（快路径同一窗口契约）
      let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      // 快臂值已同步提交、TTL / etag 余腿遭环形页翻转降级：持同一窗补投
      // 余腿，应答与快臂成功帧逐字节一致（案一情形 B，杜绝重放自碰已提交值）
      if let EtagResume::Pending { ticks, new_etag } = resume {
        replay_pending_legs(storage, key, ticks, new_etag).await?;
        output.write_resp_int(new_etag);
        return Ok(());
      }
      // promote 删写（快路径同终态）：初写口径固定新 etag = NoETag + 1；
      // Missing → 初写口径不读旁路残值（案三，与快臂同分流）
      let new_etag = match read_value_and_etag_async(storage, key).await? {
        // RI 存活元记录经挡写门早停（单 WRONGTYPE 帧零副作用，同
        // etag_conditional_slow 臂同判据单源）
        ReadOutcomeAsync::WrongType => {
          if !slow_wrongtype_promote_gate(storage, key, output).await? {
            return Ok(());
          }
          NO_ETAG + 1
        }
        ReadOutcomeAsync::Hit(_, existing) => existing + 1,
        ReadOutcomeAsync::Missing => NO_ETAG + 1,
      };
      let set_args = EtagSetArgs::plain(key, val, expiry, high_precision);
      // 挡写单帧出口（案二，快臂 Err(()) => {} 抑制成功帧的慢臂对偶）
      if !apply_etag_write_async(storage, &set_args, new_etag).await? {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        return Ok(());
      }
      output.write_resp_int(new_etag);
      Ok(())
    }
    _ => {
      // 分派表漏接线信号：本臂只应承接 ETag 族，其余落此即缺陷
      cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
      Ok(())
    }
  }
}
