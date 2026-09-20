//! 字符串写命令域（SET/SETEX/PSETEX/SETNX/SETEXNX/GETSET/SETRANGE/APPEND，
//! 对标 libs/server/Resp/BasicCommands.cs 写命令段）

use wbase::num::{strict_i32, strict_i64};
use wbitmap::MAX_BITMAP_PAYLOAD_BYTES;
use wkv::{RmwGrow, StoreResult};
use wresp::{
  check_args::unpack_args,
  cmd_strings::{self as cs, RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, abort_with_error_message},
  ext::RespVecExt,
  options::{ExistOptions, ExpirationOption, try_get_exist_options, try_get_expiration_option},
};
use wval::KeyTag;

use super::ttl::try_get_absolute_expiry_ticks;
use crate::{
  resp::resp_server_session::RespServerSession,
  storage::session::common::{
    UserRead, read_user_sync,
    ttl_sync::{meta_is_range_index, probe_alive, probe_alive_domain, put_ttl_sync, ttl_of_sync},
  },
};

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
  pub(crate) const fn is_keep_ttl(self) -> bool {
    matches!(self, Self::SetKeepTtl | Self::SetKeepTtlXx)
  }

  /// 是否 XX 族（仅键存在时设置）
  #[inline]
  pub(crate) const fn is_xx(self) -> bool {
    matches!(self, Self::SetExXx | Self::SetKeepTtlXx)
  }

  /// 是否 NX 族（仅键不存在时设置）
  #[inline]
  pub(crate) const fn is_nx(self) -> bool {
    matches!(self, Self::SetExNx)
  }
}

/// SET 选项解析产物（对标 NetworkSETEXNX 的局部变量组）
pub struct SetOptions<'a> {
  pub key: &'a [u8],
  pub val: &'a [u8],
  /// EX/PX 的秒或毫秒数（KEEPTTL/无过期时为 0）
  pub expiry: i64,
  /// PX 口径（expiry 单位为毫秒而非秒）
  pub exp_high_precision: bool,
  pub(crate) cmd: SetCmd,
  pub get_value: bool,
}

/// RangeIndex 写门裁决结果（[`ri_write_gate`] 出口，字符串写入口共用）
pub(crate) enum RiWriteGate {
  /// 放行：键上无存活 RangeIndex 元记录
  Pass,
  /// 拦截：应答（WRONGTYPE 或门控 I/O 错误）已写入 output，本轮不得续写
  Blocked,
  /// 元记录有磁盘候选：判据不可内存闭环，调用方整体降级异步
  Deferred,
}

/// 字符串写入口的 RangeIndex 键门（写面 WRONGTYPE 单点，字符串写共同体前置）
///
/// libs/server/Resp/Parser/RespCommand.cs:IsLegalOnRangeIndex
///
/// 判据是记录自己的物理域事实（`KeyTag::Meta` 存活元记录
/// `collection_type == RangeIndex`，单点见
/// [`crate::storage::session::common::ttl_sync::meta_is_range_index`]），不吃
/// 命令位图：C# 把该判别挂在存储函数层（MainStore UpsertMethods 的
/// InPlaceWriter 与 RMWMethods 的 InPlaceUpdater 双臂置 WrongType 动作），
/// rust 的写原语无 WrongType 通道且 RI 键有专属物理域（元记录 + 独立
/// wbftree 树文件），故门挂在 RESP 写入口、upsert 之前。C# 上层对
/// WRONGTYPE 的「promote 后 DELETE 重写」臂是对象存储专用（RecordType 0
/// 与 object 位共用一记录），对 RI 记录会连树一起清退，rust 不照抄该
/// 覆写通道——盲写会留下字符串值与孤儿树文件的双域残留
pub(crate) fn ri_write_gate<D: wdev::Device>(
  store: &wkv::BatchStoreSession<'_, D>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> RiWriteGate {
  match meta_is_range_index(store, key) {
    // 存活 RI 记录：字符串写一律拒
    Ok(Some(true)) => {
      output.write_resp_error(RESP_ERR_WRONG_TYPE);
      RiWriteGate::Blocked
    }
    Ok(Some(false)) => RiWriteGate::Pass,
    Ok(None) => RiWriteGate::Deferred,
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      RiWriteGate::Blocked
    }
  }
}

/// SET 值 + 过期应用的写共同体尾部：upsert 自带同步清 TTL，随后按需写新 TTL
///
/// 返回 `Err(())` 表示存储错误且已写出应答；`Ok(false)` 表示须降级异步；
/// `Ok(true)` 表示写闭环（应答由调用方续写）
pub(crate) fn apply_set_with_expiry<'a, D: wdev::Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
  val: &[u8],
  expiry: i64,
  high_precision: bool,
  keep_ttl: Option<Option<i64>>,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let expire_at_ticks = if expiry != 0 {
    let Some(ticks) = try_get_absolute_expiry_ticks(expiry, high_precision) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET);
      return Err(());
    };
    Some(ticks)
  } else {
    None
  };

  // RI 键门（写共同体单点前置）：Blocked 走 `Err(())` 已应答出口，调用方
  // 绝不再续写 +OK / etag
  match ri_write_gate(store, key, output) {
    RiWriteGate::Pass => {}
    RiWriteGate::Blocked => return Err(()),
    RiWriteGate::Deferred => return Ok(false),
  }
  match store.try_upsert_sync(key, val) {
    Ok(Ok(_)) => {}
    // 环形页翻转 / 既有 TTL 清除须异步：先于任何输出整体降级
    Ok(Err(_)) => return Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
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
  if let Some(ticks) = expire_at_ticks {
    return apply_ttl(ticks);
  }
  Ok(true)
}

impl RespServerSession {
  /// libs/server/Resp/BasicCommands.cs:NetworkSET
  pub fn network_set<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // SET 声明 arity -3，快速解析器仅把 3..7 参数的 SET 路由到选项解析器；
    // 更长的选项 SET 也落在本函数——C# 此处交还 NetworkSETEXNX 解析选项
    if parse_state.len() > 2 {
      return self.network_setexnx(parse_state, store, output);
    }
    let Some([key, value]) = unpack_args(parse_state, output, "SET") else {
      return Ok(true);
    };
    // RI 键门：裸 SET 盲写无前置读，三域折叠够不到，须在此拦
    match ri_write_gate(store, key, output) {
      RiWriteGate::Pass => {}
      RiWriteGate::Blocked => return Ok(true),
      RiWriteGate::Deferred => return Ok(false),
    }
    match store.try_upsert_sync(key, value) {
      Ok(Ok(_)) => output.write_resp_simple_string("OK"),
      Ok(Err(_)) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
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
    let Some([key, val]) = unpack_args(parse_state, output, "GETSET") else {
      return Ok(true);
    };
    // C# 走 NetworkSET_Conditional(SET, getValue: true)：无条件写入并回旧值
    let opts = SetOptions {
      key,
      val,
      expiry: 0,
      exp_high_precision: false,
      cmd: SetCmd::Set,
      get_value: true,
    };
    self.network_set_conditional(&opts, store, output)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkSetRange
  ///
  /// RMW 语义写回（保留既有 key 级 TTL，对标 C# MainStore/RMWMethods.cs
  /// SETRANGE 分支 "not changing the presence of ETag or Expiration"）；写回按
  /// C# 同一状态机两级派发——槽位容得下即 InPlaceUpdater 原位增长臂，否则回落
  /// CopyUpdater 整值尾部追加，两路应答逐字节一致
  pub fn network_set_range<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some((key, offset, val)) = parse_setrange_args(parse_state, output) else {
      return Ok(true);
    };

    // 读改写原子窗口：跨「读旧值—拼接改段—写回」全程持本键桶排他闩，杜绝同键
    // 并发丢更新（对标 C# BasicSessionLocker 的 ephemeral 闩跨 InternalRMW 全程）
    let Some(window) = store.try_rmw_window(key) else {
      return Ok(false);
    };

    // 原位增长臂（对标 C# MainStore/RMWMethods.cs:InPlaceUpdater SETRANGE 分支
    // :734-763）：offset+len 超出现值长且槽位富余容得下时原位改长、只覆写改段，
    // 旧数据零复制、零尾部追加、零中间 Vec；超富余即落回下方整值读改写臂
    match window.try_grow_in_place(|cap, old_len| {
      let total = offset + val.len();
      let new_len = old_len.max(total);
      if new_len > cap.len() {
        return None;
      }
      // 间隙补零：旧值末端至 offset 一段落在本次新增区内，槽位富余可能残留
      // 该槽位更早的字节，绝不依赖其为零（对位整值路径的 resize(required, 0)）
      if offset > old_len {
        cap[old_len..offset].fill(0);
      }
      cap[offset..total].copy_from_slice(val);
      Some(new_len)
    }) {
      Ok(RmwGrow::InPlace(new_len)) => {
        output.write_resp_int(new_len as i64);
        return Ok(true);
      }
      Ok(RmwGrow::Degrade) => return Ok(false),
      // 槽位不容 / 记录非活 / 引擎侧错损一律回落全量臂，应答与今日路径一致
      Ok(RmwGrow::Fallback) | Err(_) => {}
    }

    match read_user_sync(store, key, None, |v| {
      let mut existing = v.to_vec();
      let required_len = offset + val.len();
      if existing.len() < required_len {
        existing.resize(required_len, 0);
      }
      existing[offset..offset + val.len()].copy_from_slice(val);
      existing
    }) {
      Ok(UserRead::Hit(existing)) => match window.try_rmw_sync(&existing) {
        Ok(Ok(_)) => output.write_resp_int(existing.len() as i64),
        Ok(Err(_)) => return Ok(false),
        Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
      },
      Ok(UserRead::WrongType) => {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
      }
      Ok(UserRead::Missing) => {
        let mut new_val = vec![0u8; offset + val.len()];
        new_val[offset..].copy_from_slice(val);
        match window.try_rmw_sync(&new_val) {
          Ok(Ok(_)) => output.write_resp_int(new_val.len() as i64),
          Ok(Err(_)) => return Ok(false),
          Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
        }
      }
      Ok(UserRead::Deferred) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
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

  /// C# NetworkSETEX 毫秒精度形态（highPrecision=true；精确锚点见本文件 302 行）
  ///
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
  /// [`crate::storage::session::common::ttl_sync::put_ttl_sync`] 同步落 TTL 记录
  fn network_setex_impl<'a, D: wdev::Device>(
    &mut self,
    high_precision: bool,
    cmd_name: &str,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some((key, expiry, val)) = parse_setex_args(cmd_name, parse_state, output) else {
      return Ok(true);
    };

    let Some(expire_at_ticks) = try_get_absolute_expiry_ticks(expiry, high_precision) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET);
      return Ok(true);
    };

    // RI 键门：SETEX/PSETEX 同为盲写 + 独立写 TTL，无前置读
    match ri_write_gate(store, key, output) {
      RiWriteGate::Pass => {}
      RiWriteGate::Blocked => return Ok(true),
      RiWriteGate::Deferred => return Ok(false),
    }
    match store.try_upsert_sync(key, val) {
      Ok(Ok(_)) => {}
      // 异步闭环信号须整体降级，吞掉即静默丢写
      Ok(Err(_)) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }
    match put_ttl_sync(store, key, expire_at_ticks) {
      Ok(true) => output.write_resp_simple_string("OK"),
      Ok(false) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
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
    let Some([key, val]) = unpack_args(parse_state, output, "SETNX") else {
      return Ok(true);
    };

    // 存在性探测（双域：对象键同计存在，C# NX 语义）
    match probe_alive(store, key) {
      Ok(Some(true)) => output.write_resp_int(0),
      Ok(Some(false)) => match store.try_upsert_sync(key, val) {
        Ok(Ok(_)) => output.write_resp_int(1),
        Ok(Err(_)) => return Ok(false),
        Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
      },
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkSETEXNX
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:SET_Conditional
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

    if opts.expiry != 0
      && try_get_absolute_expiry_ticks(opts.expiry, opts.exp_high_precision).is_none()
    {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET);
      return Ok(true);
    }

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
  /// 该数值为毫秒口径（PX）。C# 的 WRONGTYPE 分支（对象存储域）由对象键域
  /// 分流承接，见下方 object 分支注释
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

      // 双域存活探测（附带存活键所在域）：无条件形态也须探测以区分
      // object 键（WRONGTYPE 事务重试域）与 string 键（KEEPTTL 回填前提）
      let found = match probe_alive_domain(store, key) {
        // Option<KeyTag>：Some = 存活键所在域，None = 键不存在
        Ok(Some(found)) => found,
        // 数据 / TTL 记录需磁盘裁决（或已过期待清除）：降级
        Ok(None) => return Ok(false),
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      };

      // RI 键门（Meta 域臂）：存活 RangeIndex 元记录上任何字符串写形态皆拒，
      // 尤其 XX 族的 DELETE 清退不得波及 RI 键（存根与树文件随键一并消失）
      if matches!(found, Some(KeyTag::Meta)) {
        match ri_write_gate(store, key, output) {
          RiWriteGate::Pass => {}
          RiWriteGate::Blocked => return Ok(true),
          // `probe_alive_domain` 命中 Meta 域即记录已在内存，本臂 Deferred
          // 不可达；保守仍按降级出口处理
          RiWriteGate::Deferred => return Ok(false),
        }
      }

      // 对象键：C# WRONGTYPE 分支（BasicCommands.cs:NetworkSET_Conditional:788-796）
      // promote 事务 DELETE 后重试 SET_Conditional，旧 TTL 随 DELETE 一并消失。
      // rust 无双 store，单会话直连免 promote，`try_delete_sync` 双域级联承接
      // （对标 ETag 族 promote_delete_object_key 先例）：
      // XX 族重试必 NOTFOUND → 回 nil；NX 族键已删条件成立 → 写入回 OK；
      // SETKEEPTTL 无条件写入且旧 TTL 不回填（杜绝 object 时代幽灵 TTL）
      if matches!(found, Some(KeyTag::ObjectEnvelope)) {
        if must_exist {
          return match store.try_delete_sync(key) {
            Ok(Ok(_)) => {
              output.write_resp_null_ver(self.resp_protocol_version);
              Ok(true)
            }
            // 环形页翻转 / 冷数据 / 复合元数据：降级全异步路径重入
            Ok(Err(_)) => Ok(false),
            Err(_) => {
              output.write_resp_error(RESP_ERR_GENERIC);
              Ok(true)
            }
          };
        }
        return match apply_set_with_expiry(store, key, val, expiry, high_precision, None, output) {
          Ok(true) => {
            output.write_resp_simple_string("OK");
            Ok(true)
          }
          Ok(false) => Ok(false),
          Err(()) => Ok(true),
        };
      }

      let exists = found.is_some();
      if (must_exist && !exists) || (must_absent && exists) {
        // 条件不满足：C# 以 nil 表失败（SETEXNX 翻转 ok 标志后同一出口）
        output.write_resp_null_ver(self.resp_protocol_version);
        return Ok(true);
      }

      // KEEPTTL：string 域按旧值回填（upsert 自带同步清 TTL）
      let old_ttl = if cmd.is_keep_ttl() {
        match ttl_of_sync(store, key) {
          Ok(StoreResult::Success(ttl)) => Some(ttl),
          Ok(StoreResult::NotFound) => None,
          Ok(StoreResult::RecordOnDisk) => return Ok(false),
          Err(_) => {
            output.write_resp_error(RESP_ERR_GENERIC);
            return Ok(true);
          }
        }
      } else {
        None
      };

      match apply_set_with_expiry(store, key, val, expiry, high_precision, old_ttl, output) {
        Ok(true) => output.write_resp_simple_string("OK"),
        Ok(false) => return Ok(false),
        Err(()) => {}
      }
      return Ok(true);
    }

    // GET 形态：回旧值（不存在则 nil），条件语义同上；
    // 双域读判定对象键 WRONGTYPE
    let old = match read_user_sync(store, key, None, |v| v.to_vec()) {
      Ok(UserRead::Hit(found)) => Some(found),
      Ok(UserRead::WrongType) => {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Ok(UserRead::Missing) => None,
      Ok(UserRead::Deferred) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
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
        Ok(StoreResult::Success(ttl)) => Some(ttl),
        Ok(StoreResult::NotFound) => None,
        Ok(StoreResult::RecordOnDisk) => return Ok(false),
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
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
      None => output.write_resp_null_ver(self.resp_protocol_version),
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkAppend
  ///
  /// RMW 语义写回（保留既有 key 级 TTL，对标 C# MainStore/RMWMethods.cs
  /// APPEND 分支 "not changing the presence of ETag or Expiration"）；写回按
  /// C# 同一状态机两级派发——槽位容得下即 InPlaceUpdater 原位增长臂，否则回落
  /// CopyUpdater 整值尾部追加，两路应答逐字节一致
  pub fn network_append<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key, val]) = unpack_args(parse_state, output, "APPEND") else {
      return Ok(true);
    };

    // 读改写原子窗口：跨「读旧值—尾部追加—写回」全程持本键桶排他闩（同
    // [`Self::network_set_range`]，对标 C# ephemeral 闩跨 InternalRMW 全程）
    let Some(window) = store.try_rmw_window(key) else {
      return Ok(false);
    };

    // 原位增长臂（对标 C# MainStore/RMWMethods.cs:InPlaceUpdater APPEND 分支
    // :799-834）：命中内存可变区存活记录且槽位富余容得下时，只把新字节落在旧值
    // 尾部并原位发布新值长——旧数据零复制、零尾部追加、零中间 Vec。空追加对位
    // C# :799「If nothing to append, can avoid copy update」不改长，落回下方整值
    // 读改写臂
    match window.try_grow_in_place(|cap, old_len| {
      let total = old_len + val.len();
      if val.is_empty() || total > cap.len() {
        return None;
      }
      cap[old_len..total].copy_from_slice(val);
      Some(total)
    }) {
      Ok(RmwGrow::InPlace(new_len)) => {
        output.write_resp_int(new_len as i64);
        return Ok(true);
      }
      Ok(RmwGrow::Degrade) => return Ok(false),
      // 槽位不容 / 记录非活 / 引擎侧错损一律回落全量臂，应答与今日路径一致
      Ok(RmwGrow::Fallback) | Err(_) => {}
    }

    match read_user_sync(store, key, None, |v| {
      let mut buf = Vec::with_capacity(v.len() + val.len());
      buf.extend_from_slice(v);
      buf
    }) {
      Ok(UserRead::Hit(mut existing)) => {
        existing.extend_from_slice(val);
        match window.try_rmw_sync(&existing) {
          Ok(Ok(_)) => output.write_resp_int(existing.len() as i64),
          Ok(Err(_)) => return Ok(false),
          Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
        }
      }
      Ok(UserRead::WrongType) => {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
      }
      Ok(UserRead::Missing) => match window.try_rmw_sync(val) {
        Ok(Ok(_)) => output.write_resp_int(val.len() as i64),
        Ok(Err(_)) => return Ok(false),
        Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
      },
      Ok(UserRead::Deferred) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }
}

/// NetworkSETEX / NetworkPSETEX 的参数推导单源（快慢路径共用；解析失败时
/// 已写出错误应答并返回 None，返回 `(key, expiry 秒或毫秒正数, val)`）
///
/// 对标 C#：过期须为整数（TryGetLong 口径）且 > 0
pub(crate) fn parse_setex_args<'p>(
  cmd_name: &str,
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'p [u8], i64, &'p [u8])> {
  let [key, expiry_raw, val] = unpack_args(parse_state, output, cmd_name)?;
  let Some(expiry) = strict_i64(expiry_raw) else {
    abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
    return None;
  };
  if expiry <= 0 {
    abort_with_error_message(output, cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET);
    return None;
  }
  Some((key, expiry, val))
}

/// NetworkSetRange 的参数推导单源（快慢路径共用；解析失败时已写出错误应答
/// 并返回 None，返回 `(key, offset, val)`，offset 已换算 usize 非负值域）
///
/// 对标 C#：偏移须为可解析整数（TryGetInt 口径），负值越界报错，
/// offset + value 不得越过 512MB 负载上限（u64 口径，杜绝 usize 溢出 panic）
pub(crate) fn parse_setrange_args<'p>(
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'p [u8], usize, &'p [u8])> {
  let [key, offset_raw, val] = unpack_args(parse_state, output, "SETRANGE")?;
  let Some(offset) = strict_i32(offset_raw) else {
    abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
    return None;
  };
  let offset: i64 = i64::from(offset);
  if offset < 0 {
    abort_with_error_message(output, cs::RESP_ERR_GENERIC_OFFSETOUTOFRANGE);
    return None;
  }
  if offset as u64 + val.len() as u64 > MAX_BITMAP_PAYLOAD_BYTES as u64 {
    abort_with_error_message(output, cs::RESP_ERR_STRING_EXCEEDS_MAX_SIZE);
    return None;
  }
  Some((key, offset as usize, val))
}

/// NetworkSETEXNX 的选项解析前半段
///
/// 解析失败时已写出错误应答并返回 None。错误次序对标 C#：重复/非法过期选项
/// → syntax error；EX/PX 缺值 → syntax error；值非整数 → not-integer；值非正
/// → invalid expire in set；重复 NX/XX → syntax error；未知选项 → unknown command
pub(crate) fn parse_set_options<'p>(
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
) -> Option<SetOptions<'p>> {
  let key = parse_state[0];
  let val = parse_state[1];

  let mut expiry: i64 = 0;
  let mut exp_high_precision = false;
  let mut exp_option = ExpirationOption::None;
  let mut exist_options = ExistOptions::None;
  let mut get_value = false;

  let mut token_idx = 2usize;
  while token_idx < parse_state.len() {
    let next_opt = parse_state[token_idx];
    token_idx += 1;

    // 过期选项经 wresp options 单点解析（对标 BasicCommands.cs:628
    // parseState.TryGetExpirationOptionWithToken，与 basic_etag_commands.rs 同源，
    // 大小写不敏感）；SET 仅接受 EX/PX/KEEPTTL，EXAT/PXAT 命中过期选项解析器
    // 但不属可接受集 → syntax error
    if let Some(parsed_option) = try_get_expiration_option(next_opt) {
      // 对标 C#:632 已出现过期选项，或本选项不在可接受集 (EX/PX/KEEPTTL) → syntax error
      if exp_option != ExpirationOption::None
        || !matches!(
          parsed_option,
          ExpirationOption::Ex | ExpirationOption::Px | ExpirationOption::Keepttl
        )
      {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return None;
      }
      exp_option = parsed_option;
      // 基于上面的可接受集，非 KEEPTTL 即为 EX/PX，后必须跟过期数值
      if exp_option != ExpirationOption::Keepttl {
        // C# 修复过末参越界读，缺值即 syntax error
        let Some(raw) = parse_state.get(token_idx) else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return None;
        };
        token_idx += 1;
        let Some(v) = strict_i64(raw) else {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return None;
        };
        if v <= 0 {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET);
          return None;
        }
        expiry = v;
        exp_high_precision = exp_option == ExpirationOption::Px;
      }
      continue;
    }

    // 存在性约束 NX/XX 经 wresp 单点解析（对标 BasicCommands.cs NetworkSETEXNX
    // ExistOptions 分支与 ObjectInputExtensions.cs:TryGetExistOption，大小写不敏感）；
    // GET 不属 ExistOptions（C# 单独跟踪 getValue）
    if let Some(opt) = try_get_exist_options(next_opt) {
      if exist_options != ExistOptions::None {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return None;
      }
      exist_options = opt;
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
  if exp_option == ExpirationOption::Keepttl {
    expiry = 0;
  }
  let cmd = match exist_options {
    ExistOptions::Nx => SetCmd::SetExNx,
    ExistOptions::Xx => {
      if exp_option == ExpirationOption::Keepttl {
        SetCmd::SetKeepTtlXx
      } else {
        SetCmd::SetExXx
      }
    }
    ExistOptions::None => {
      if exp_option == ExpirationOption::Keepttl {
        SetCmd::SetKeepTtl
      } else {
        SetCmd::Set
      }
    }
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
