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
use wkv::StoreResult;
use wresp::{
  check_args::{check_arg_count, unpack_args},
  cmd_strings::{self as cs, RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, abort_with_error_message},
  ext::RespVecExt,
  options::{ExpirationOption, try_get_expiration_option},
};
use wval::NO_ETAG;

use super::basic_commands::apply_set_with_expiry;
use crate::{
  resp::resp_server_session::RespServerSession,
  storage::session::common::{
    UserRead,
    etag_sync::{etag_of_sync, put_etag_sync},
    read_user_sync,
    ttl_sync::ttl_of_sync,
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

/// ETag 读域结果（值与 etag 标签双读三态折叠）
enum ReadOutcome {
  /// 键存在且值与 etag 均内存闭环
  Hit(Vec<u8>, i64),
  /// 键不存在
  NotFound,
  /// 集合对象（WRONGTYPE）
  WrongType,
  /// 任一记录有磁盘候选，须降级异步
  Deferred,
  /// 存储错误
  Failed,
}

/// ETag 读共同体：值（含 TTL 裁决 + WRONGTYPE 判定）与 etag 标签一次读齐
///
/// 对标 C# Reader 直读记录（值与记录尾 etag 同记录一体可达）：Rust 旁路
/// 记录下两读串行，任一遇磁盘候选即整体降级。值读经双域裁决：String 域
/// 命中即用户数据（值内容任意），信封域命中即对象键（WRONGTYPE）
fn read_value_and_etag<'a, D: wdev::Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
) -> ReadOutcome {
  match read_user_sync(store, key, |v| v.to_vec()) {
    Ok(UserRead::Hit(val)) => match etag_of_sync(store, key) {
      Ok(Some(etag)) => ReadOutcome::Hit(val, etag),
      Ok(None) => ReadOutcome::Deferred,
      Err(_) => ReadOutcome::Failed,
    },
    Ok(UserRead::WrongType) => ReadOutcome::WrongType,
    Ok(UserRead::Missing) => ReadOutcome::NotFound,
    Ok(UserRead::Deferred) => ReadOutcome::Deferred,
    Err(_) => ReadOutcome::Failed,
  }
}

/// 条件写命令判别（SETIFMATCH / SETIFGREATER）
enum EtagCondCmd {
  /// givenEtag == existingEtag 命中，新 etag = givenEtag + 1
  IfMatch,
  /// givenEtag > existingEtag 命中，新 etag = givenEtag
  IfGreater,
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
  /// libs/server/Resp/BasicEtagCommands.cs:NetworkGETWITHETAG
  ///
  /// 键不存在 → null；存在 → `[etag, value]`（无 etag 记录即 0）
  pub fn network_getwithetag<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "GETWITHETAG") else {
      return Ok(true);
    };
    let resp_version = self.resp_protocol_version;
    match read_value_and_etag(store, key) {
      ReadOutcome::Hit(val, etag) => write_etag_val_array(output, etag, Some(&val), resp_version),
      ReadOutcome::NotFound => output.write_resp_null_ver(resp_version),
      ReadOutcome::WrongType => output.write_resp_error(RESP_ERR_WRONG_TYPE),
      ReadOutcome::Deferred => return Ok(false),
      ReadOutcome::Failed => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicEtagCommands.cs:NetworkGETIFNOTMATCH
  ///
  /// 键不存在 → null；etag 匹配 → `[etag, nil]`；不匹配 → `[etag, value]`
  /// （对标 ReadMethods.Etags.cs:HandleEtagReader）
  pub fn network_getifnotmatch<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key, etag_raw]) = unpack_args(parse_state, output, "GETIFNOTMATCH") else {
      return Ok(true);
    };
    let Some(given_etag) = strict_i64(etag_raw) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };
    let resp_version = self.resp_protocol_version;

    match read_value_and_etag(store, key) {
      ReadOutcome::Hit(val, current) => {
        if current == given_etag {
          write_etag_val_array(output, current, None, resp_version);
        } else {
          write_etag_val_array(output, current, Some(&val), resp_version);
        }
      }
      ReadOutcome::NotFound => output.write_resp_null_ver(resp_version),
      ReadOutcome::WrongType => output.write_resp_error(RESP_ERR_WRONG_TYPE),
      ReadOutcome::Deferred => return Ok(false),
      ReadOutcome::Failed => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
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
  pub fn network_delifgreater<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key, etag_raw]) = unpack_args(parse_state, output, "DELIFGREATER") else {
      return Ok(true);
    };
    let Some(given_etag) = strict_i64(etag_raw).filter(|&e| e >= 0) else {
      abort_with_error_message(output, cs::RESP_ERR_INVALID_ETAG);
      return Ok(true);
    };

    match read_user_sync(store, key, |_| ()) {
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
  pub fn network_setifmatch<'a, D: wdev::Device>(
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
  pub fn network_setifgreater<'a, D: wdev::Device>(
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
  /// EX/PX 设置过期，无 expiry 清除既有过期（SET 语义）
  pub fn network_setwithetag<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 2..=4, output, "SETWITHETAG");
    let (key, val) = (parse_state[0], parse_state[1]);
    let (expiry, high_precision) = match parse_setwithetag_expiry(&parse_state[2..]) {
      Ok(opts) => opts,
      Err(err) => {
        abort_with_error_message(output, err);
        return Ok(true);
      }
    };

    // 对象键走 C# promote 删写（ExecuteETagSetCommand WRONGTYPE 分支对
    // SETWITHETAG 同径）：先 DELETE 对象键，再按初写口径固定新 etag =
    // NoETag + 1（HandleSetWithEtagInitialUpdate），应答整数
    if matches!(read_user_sync(store, key, |_| ()), Ok(UserRead::WrongType)) {
      let new_etag = NO_ETAG + 1;
      match promote_delete_object_key(store, key, output) {
        Ok(true) => {}
        Ok(false) => return Ok(false),
        Err(()) => return Ok(true),
      }
      let args = EtagSetArgs {
        key,
        val,
        expiry,
        high_precision,
        keep_ttl: None,
      };
      match apply_etag_write(store, &args, new_etag, output) {
        Ok(true) => output.write_resp_int(new_etag),
        Ok(false) => return Ok(false),
        Err(()) => {}
      }
      return Ok(true);
    }

    let existing = match etag_of_sync(store, key) {
      Ok(Some(etag)) => etag,
      Ok(None) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    };
    let new_etag = existing + 1;
    let args = EtagSetArgs {
      key,
      val,
      expiry,
      high_precision,
      keep_ttl: None,
    };
    match apply_etag_write(store, &args, new_etag, output) {
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
  fn network_set_etag_conditional<'a, D: wdev::Device>(
    &mut self,
    cmd: EtagCondCmd,
    cmd_name: &str,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 3..=6, output, cmd_name);
    let (key, val, etag_raw) = (parse_state[0], parse_state[1], parse_state[2]);
    let resp_version = self.resp_protocol_version;

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
        // C# parseState.TryGetInt（int32 域，:239）：超 int 范围回 not integer
        match parse_state.get(token_idx).copied().and_then(strict_i32) {
          Some(e) if e > 0 => expiry = i64::from(e),
          Some(_) => {
            error = Some(cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET);
            break;
          }
          None => {
            error = Some(cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
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
    let Some(given_etag) = strict_i64(etag_raw).filter(|&e| e >= 0) else {
      abort_with_error_message(output, cs::RESP_ERR_INVALID_ETAG);
      return Ok(true);
    };
    if let Some(err) = error {
      abort_with_error_message(output, err);
      return Ok(true);
    }

    match read_value_and_etag(store, key) {
      ReadOutcome::Hit(old_val, existing) => {
        if !cmd.hit(given_etag, existing) {
          // 条件不命中：不写。NOGET → [existing, nil]；否则回旧值
          let value = if no_get {
            None
          } else {
            Some(old_val.as_slice())
          };
          write_etag_val_array(output, existing, value, resp_version);
          return Ok(true);
        }
        let new_etag = cmd.next_etag(given_etag);
        // 命中且 expiry == 0：保留旧 TTL（C# CopyUpdate
        // TrySetExpiration(arg1 != 0 ? arg1 : srcRecord.Expiration)）
        let keep_ttl = if expiry == 0 {
          Some(match ttl_of_sync(store, key) {
            Ok(StoreResult::Success(t)) => t,
            Ok(StoreResult::NotFound) => None,
            Ok(StoreResult::RecordOnDisk) => return Ok(false),
            Err(_) => {
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
        match apply_etag_write(store, &args, new_etag, output) {
          Ok(true) => write_etag_val_array(output, new_etag, None, resp_version),
          Ok(false) => return Ok(false),
          Err(()) => {}
        }
      }
      ReadOutcome::NotFound => {
        // 键不存在（对标 HandleSetIfMatchInitialUpdate）：无条件写入，
        // newEtag = givenEtag + (SETIFMATCH ? 1 : 0)，初始记录无旧 TTL 保留
        let new_etag = cmd.next_etag(given_etag);
        let args = EtagSetArgs {
          key,
          val,
          expiry,
          high_precision,
          keep_ttl: None,
        };
        match apply_etag_write(store, &args, new_etag, output) {
          Ok(true) => write_etag_val_array(output, new_etag, None, resp_version),
          Ok(false) => return Ok(false),
          Err(()) => {}
        }
      }
      ReadOutcome::WrongType => {
        // C# ExecuteETagSetCommand WRONGTYPE 分支：promote 删写语义——
        // 先 DELETE 对象键，再按 InitialUpdater 无条件初写（条件判定不在
        // 初写路径，NOGET 标志同样不参与），应答 `[newEtag, nil]`
        let new_etag = cmd.next_etag(given_etag);
        match promote_delete_object_key(store, key, output) {
          Ok(true) => {}
          Ok(false) => return Ok(false),
          Err(()) => return Ok(true),
        }
        let args = EtagSetArgs {
          key,
          val,
          expiry,
          high_precision,
          keep_ttl: None,
        };
        match apply_etag_write(store, &args, new_etag, output) {
          Ok(true) => write_etag_val_array(output, new_etag, None, resp_version),
          Ok(false) => return Ok(false),
          Err(()) => {}
        }
      }
      ReadOutcome::Deferred => return Ok(false),
      ReadOutcome::Failed => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }
}

/// SETWITHETAG 的 `[EX|PX] [expiry]` 尾部解析（对应 C#
/// BasicEtagCommands.cs:154-181 NetworkSETWITHETAG 内嵌校验链）：
/// 空参 → 无过期；首 token 须为 EX/PX（其余过期形式与未知 token 均
/// syntax error）；expiry 须为正整数（C# TryGetInt int32 域，:167：
/// 缺失/非整数/超 int 范围 → not integer，非正 → invalid expire）
fn parse_setwithetag_expiry(args: &[&[u8]]) -> Result<(i64, bool), &'static str> {
  let Some(&token) = args.first() else {
    return Ok((0, false));
  };
  match try_get_expiration_option(token) {
    Some(opt @ (ExpirationOption::Ex | ExpirationOption::Px)) => {
      let high_precision = opt == ExpirationOption::Px;
      match args.get(1).copied().and_then(strict_i32) {
        Some(e) if e > 0 => Ok((i64::from(e), high_precision)),
        Some(_) => Err(cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET),
        None => Err(cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER),
      }
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

/// ETag 写共同体：upsert 值（自带清 TTL）→ 按 keep_ttl 回填或写新过期 →
/// 推进 etag 标签
///
/// 值写失败时 etag 未动，重入重发幂等
fn apply_etag_write<D: wdev::Device>(
  store: &wkv::BatchStoreSession<'_, D>,
  args: &EtagSetArgs<'_>,
  new_etag: i64,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let EtagSetArgs {
    key,
    val,
    expiry,
    high_precision,
    keep_ttl,
  } = *args;
  apply_set_with_expiry(store, key, val, expiry, high_precision, keep_ttl, output)?;
  match put_etag_sync(store, key, new_etag) {
    Ok(true) => Ok(true),
    Ok(false) => Ok(false),
    Err(_) => Err(()),
  }
}

/// libs/server/Resp/BasicEtagCommands.cs:ExecuteETagSetCommand（WRONGTYPE 分支）
///
/// promote 删写第一步：对象键整体删除。C# 对 ValueIsObject 记录先收
/// WRONGTYPE，再在 promote 事务（Main | Object 双域排他锁）内 DELETE 统一存
/// 键；Rust 侧单会话直连免 promote，`try_delete_sync` 双域级联一并清理
/// TTL 与 ETag 旁路记录。返回 `Ok(false)` 须降级异步，`Err(())` 已答通用错误
fn promote_delete_object_key<D: wdev::Device>(
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
