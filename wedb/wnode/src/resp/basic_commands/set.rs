//! 字符串写命令域（SET/SETEX/PSETEX/SETNX/SETEXNX/GETSET/SETRANGE/APPEND，
//! 对标 libs/server/Resp/BasicCommands.cs 写命令段）

use std::borrow::Cow;

use wbitmap::MAX_BITMAP_PAYLOAD_BYTES;
use wdev::Device;
use wkv::{RmwGrow, RmwWindow, StoreResult};
use wresp::{
  check_args::{parse_i32_arg, unpack_args},
  cmd_strings::{self as cs, RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, abort_with_error_message},
  ext::RespVecExt,
  options::{ExistOptions, ExpirationOption, try_get_exist_options, try_get_expiration_option},
};
use wval::KeyTag;

use super::ttl::try_get_absolute_expiry_ticks;
use crate::{
  read_user_or_bail,
  resp::{
    TtlResume, resp_server_session::RespServerSession, vector::vector_manager::VectorManager,
  },
  storage::session::common::{
    UserRead, fold_outcome, read_user_sync,
    ttl_sync::{
      meta_is_range_index, probe_alive_domain, probe_alive_with_registry, put_ttl_sync,
      registry_alive, ttl_of_sync,
    },
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
pub(crate) fn ri_write_gate<D: Device>(
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
/// 前置契约：调用方须已持本键读改写窗口（`try_rmw_window`）——KEEPTTL 的
/// 「读旧 → 清 → 回填」与 EX/PX 的「清 → 写值 → 写新 TTL」跨调用交叠的
/// 串行化由调用方窗口承接（对标 C# 单记录 RMW 锁内一体完成）
///
/// `ttl_resume`：值已提交后 put_ttl_sync 遭环形页翻转降级时置
/// [`TtlResume::Pending`]（票 wnode-nx-conditional-ttl-degrade-replay-selfhit，
/// 随 exec 降级快照尾参续跑，慢臂仅补投 TTL，杜绝整命令重放自碰已提交值）
///
/// 返回 `Err(())` 表示存储错误且已写出应答；`Ok(false)` 表示须降级异步；
/// `Ok(true)` 表示写闭环（应答由调用方续写）
pub(crate) fn apply_set_with_expiry<'a, D: Device>(
  store: &wkv::BatchStoreSession<'a, D>,
  key: &[u8],
  val: &[u8],
  exp: (i64, bool),
  keep_ttl: Option<Option<i64>>,
  ttl_resume: &mut TtlResume,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let (expiry, high_precision) = exp;
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
  let mut apply_ttl = |ticks: i64| match put_ttl_sync(store, key, ticks) {
    Ok(true) => Ok(true),
    // 值已同步提交、TTL 遭环形页翻转降级：置「值已提交 + TTL 待投」标记，
    // 随 exec 降级快照尾参续跑（票 wnode-nx-conditional-ttl-degrade-replay-selfhit），
    // 杜绝整命令重放自碰已提交值（SET NX 误回 nil / KEEPTTL 回填读已清 TTL 静默丢）
    Ok(false) => {
      *ttl_resume = TtlResume::Pending(ticks);
      Ok(false)
    }
    Err(_) => Err(()),
  };
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

/// 写共同体应答尾部（[`apply_set_with_expiry`] 出口 → 快臂 `Result<bool>` 的
/// 统一收口）：写成功补 OK 帧回 true，页翻转降级透传 false，已写出错误帧
/// （Err）视作完成回 true；盲写臂 / 对象键臂 / KEEPTTL 臂三处同形 match 归一
#[inline]
fn set_write_reply(res: Result<bool, ()>, output: &mut Vec<u8>) -> wresp::Result<bool> {
  match res {
    Ok(true) => {
      output.write_resp_simple_string("OK");
      Ok(true)
    }
    Ok(false) => Ok(false),
    Err(()) => Ok(true),
  }
}

impl RespServerSession {
  /// SET 族盲写臂的窗内向量第四态复验（票 zcode-r163c-setguard 案一）：
  /// 派发层 `set_vector_guard` 窗外裁决与本格臂取窗之间存在 TOCTOU 天窗，
  /// 窗内以全链单源判据（[`registry_alive`]）复验登记表；命中即
  /// `true`，调用方诚实降级 `Ok(false)` 交慢臂 `blind_write_gate`
  /// 持窗临界区内清退后覆写（快臂无法同步摘除登记，`delete_vector_set`
  /// 系真异步；与 MSET 慢臂窗内清退同一终态契约）
  #[inline]
  fn vector_live_in_window<D: Device>(
    store: &wkv::BatchStoreSession<'_, D>,
    vector: Option<&VectorManager>,
    key: &[u8],
  ) -> bool {
    let prefix = store.session_prefix();
    registry_alive(vector, prefix.as_slice(), key)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkSET
  pub fn network_set<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // SET 声明 arity -3，快速解析器仅把 3..7 参数的 SET 路由到选项解析器；
    // 更长的选项 SET 也落在本函数——C# 此处交还 NetworkSETEXNX 解析选项
    // 续跑标记进入即复位（沿 MSETNX resume 复位纪律，杜绝跨命令残留）
    self.ttl_resume = TtlResume::Full;
    if parse_state.len() > 2 {
      return self.network_setexnx(parse_state, store, vector, output);
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
    // 盲写落窗（票 zcode-r32-rmwmatrix 立项一：对标 C# InternalUpsert.cs:67
    // FindOrCreateTagAndTryEphemeralXLock，纯写回同取记录闩，盲写绝不容插入
    // 他者读算写间隙——GETDEL 答旧删新、INCR 写回顶替皆非可串行化）；失闩沿
    // Ok(false) 降级慢路径同段持窗重放
    let Some(_window) = store.try_rmw_window(key) else {
      return Ok(false);
    };
    // 窗内第四态复验（票 zcode-r163c-setguard 案一，杜绝派发层放行后至
    // 取窗间并发 VADD 逃逸致双域并存）
    if Self::vector_live_in_window(store, vector, key) {
      return Ok(false);
    }
    match store.try_upsert_sync(key, value) {
      Ok(Ok(_)) => output.write_resp_simple_string("OK"),
      Ok(Err(_)) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkGETSET
  pub fn network_getset<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
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
    self.network_set_conditional(&opts, store, vector, output)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkSetRange
  ///
  /// RMW 语义写回（保留既有 key 级 TTL，对标 C# MainStore/RMWMethods.cs
  /// SETRANGE 分支 "not changing the presence of ETag or Expiration"）；写回按
  /// C# 同一状态机两级派发——槽位容得下即 InPlaceUpdater 原位增长臂，否则回落
  /// CopyUpdater 整值尾部追加，两路应答逐字节一致
  pub fn network_set_range<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some((key, offset, val)) = parse_setrange_args(parse_state, output) else {
      return Ok(true);
    };

    // 单页容量前置门（见 [`string_record_fits_page`]）：SETRANGE 新值长恒为
    // max(old_len, offset+len) 且旧值必已在页内，offset+len 超页即终值注定撞
    // 写侧 RecordTooLarge——按现终态同帧形在取窗前收口，免整值回读与零填充
    // 物化整轮
    if !string_record_fits_page(store, key, offset + val.len()) {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(true);
    }

    // 读改写原子窗口：跨「读旧值—拼接改段—写回」全程持本键桶排他闩，杜绝同键
    // 并发丢更新（对标 C# BasicSessionLocker 的 ephemeral 闩跨 InternalRMW 全程）
    let Some(window) = store.try_rmw_window(key) else {
      return Ok(false);
    };

    // 原位增长臂（对标 C# MainStore/RMWMethods.cs:InPlaceUpdater SETRANGE 分支
    // :734-763）：offset+len 超出现值长且槽位富余容得下时原位改长、只覆写改段，
    // 旧数据零复制、零尾部追加、零中间 Vec；超富余即落回下方整值读改写臂
    if let Some(res) = in_place_grow(&window, output, |cap, old_len| {
      let total = offset + val.len();
      let new_len = old_len.max(total);
      if new_len > cap.len() {
        return None;
      }
      // 间隙补零：旧值末端至 offset 一段落在本次新增区内，槽位富余可能残留
      // 该槽位更早的字节，绝不依赖其为零（对位整值路径的 resize(required, 0)；
      // C# InPlace 臂 RMWMethods.cs:744/:751 zeroInit 缺省不清间隙系上游缺陷，
      // rust 刻意填零，裁决见 doc/zh/deviations.md 第 82 条）
      if offset > old_len {
        cap[old_len..offset].fill(0);
      }
      cap[offset..total].copy_from_slice(val);
      Some(new_len)
    }) {
      return res;
    }

    // 整值读改写臂：旧值在位即在旧值上插改段，缺键以空旧值零填充构造终值
    rmw_full_grow(store, &window, key, output, false, |old| {
      let mut existing = old.unwrap_or_default().to_vec();
      let required_len = offset + val.len();
      if existing.len() < required_len {
        existing.resize(required_len, 0);
      }
      existing[offset..offset + val.len()].copy_from_slice(val);
      Cow::Owned(existing)
    })
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkSETEX
  pub fn network_setex<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.network_setex_impl(false, "SETEX", parse_state, store, vector, output)
  }

  /// C# NetworkSETEX 毫秒精度形态（highPrecision=true；精确锚点见本文件 302 行）
  ///
  /// PSETEX 入口（调用 network_setex_impl(highPrecision = true)）
  pub fn network_psetex<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.network_setex_impl(true, "PSETEX", parse_state, store, vector, output)
  }

  /// SETEX/PSETEX 共同实现体（对应 C# NetworkSETEX highPrecision 参数化实现）；写值后经
  /// [`crate::storage::session::common::ttl_sync::put_ttl_sync`] 同步落 TTL 记录
  fn network_setex_impl<'a, D: Device>(
    &mut self,
    high_precision: bool,
    cmd_name: &str,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
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
    // 「清 TTL + 值写 + 新 TTL 写」整段同窗收口（对标 C# NetworkSETEX 把
    // valMetadata 内嵌单条 SET 一次 CAS 落库：BasicCommands.cs:552-559）；
    // 失闩沿 Ok(false) 降级慢路径同段持窗重放，杜绝 upsert 清 TTL 与
    // put_ttl 间隙的并发 EXPIRE 交叠丢更新
    let Some(_window) = store.try_rmw_window(key) else {
      return Ok(false);
    };
    // 窗内第四态复验（票 zcode-r163c-setguard 案一，同裸 SET 臂）
    if Self::vector_live_in_window(store, vector, key) {
      return Ok(false);
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
  pub fn network_setnx<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key, val]) = unpack_args(parse_state, output, "SETNX") else {
      return Ok(true);
    };

    // 条件写整段同窗（票 zcode-r32-rmwmatrix 立项三：对标 C# NetworkSETNX
    // 走 SET_Conditional 单次 RMW，存在性条件在记录闩内判定——探测与写入
    // 两步分离时并发 SET 落间隙即「SET +OK 且 SETNX :1 而终值为 SETNX 值」
    // 的已确认写丢失）；失闩沿 Ok(false) 降级慢路径同段持窗重放
    let Some(_window) = store.try_rmw_window(key) else {
      return Ok(false);
    };

    // 存在性探测（闩窗内折叠存活探针单源：三域 + 向量登记表第四态，对象键
    // 同计存在，C# NX 语义；对标 C# Reader 主存单记录——VADD 落主存与
    // String 同槽，EXISTS/SETEXNX 对存活向量记录恒判在，
    // ReadMethods.cs:19-46。票 zcode-r161c-msetnx 案一：派发层不再于窗外
    // 另出 :0 终态应答，NX 存在性唯本位裁决）
    //
    // 存在性记账单点（票 wnode-string-bitmap-found-notfound-accounting-matrix：
    // C# NetworkSETNX:592 收敛 MainStoreOps SET_Conditional 无输出重载，
    // :279 NotFound→incr_session_notfound / :284 else→incr_session_found 恰一帧；
    // 缺席写入成功即 C# happy path NOTFOUND 帧，计数按存在性口径勿按命令成败）：
    // 探针零入账静默判定，本地三态折叠——存活（含对象键 / 向量第四态）→found、
    // 缺席→notfound，函数尾 fold_outcome 单点补账恰一条；一切 Ok(false) 降级
    // 出口提前返回零入账交慢臂唯一出口收口（防重放双计），探针未裁决（Err）
    // 与写回错误帧臂零入账对位 C# 异常臂无 incr
    let prefix = store.session_prefix();
    let mut outcome: Option<bool> = None;
    match probe_alive_with_registry(store, prefix.as_slice(), key, vector) {
      Ok(Some(true)) => {
        output.write_resp_int(0);
        outcome = Some(true);
      }
      Ok(Some(false)) => match store.try_upsert_sync(key, val) {
        Ok(Ok(_)) => {
          output.write_resp_int(1);
          outcome = Some(false);
        }
        Ok(Err(_)) => return Ok(false),
        Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
      },
      Ok(None) => return Ok(false),
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
    }
    fold_outcome(outcome, self.session_metrics.as_deref());
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkSETEXNX
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:SET_Conditional
  ///
  /// SET 全选项状态机（EX/PX/KEEPTTL + NX/XX/GET）。C# 对未知选项先原地大写
  /// 重试再判，等价于选项大小写不敏感；SET 只接受 EX/PX/KEEPTTL（EXAT/PXAT
  /// 报语法错误）
  pub fn network_setexnx<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
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
      return self.network_set_ex(&opts, store, vector, output);
    }
    self.network_set_conditional(&opts, store, vector, output)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkSET_EX
  ///
  /// 无条件盲写 + 过期应用（SET k v [EX s|PX ms] 的无 NX/XX/GET 路径）
  pub fn network_set_ex<'a, D: Device>(
    &mut self,
    opts: &SetOptions,
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 盲写 + 过期应用整段同窗（[`apply_set_with_expiry`] 契约：调用方持窗；
    // 失闩沿 Ok(false) 降级慢路径同段持窗重放）
    // 续跑标记进入即复位（沿 MSETNX resume 复位纪律，杜绝跨命令残留）
    self.ttl_resume = TtlResume::Full;
    let Some(_window) = store.try_rmw_window(opts.key) else {
      return Ok(false);
    };
    // 窗内第四态复验（票 zcode-r163c-setguard 案一，同裸 SET 臂；命中交
    // 慢臂 blind_write_gate 持窗清退后覆写）
    if Self::vector_live_in_window(store, vector, opts.key) {
      return Ok(false);
    }
    self.apply_set_opts(opts, store, None, output)
  }

  /// SET 族「写共同体 → 应答尾」三臂同形收口（盲写臂 / 对象键臂 / KEEPTTL 臂的
  /// [`apply_set_with_expiry`] 七参调用 + [`set_write_reply`] 映射归一；GET 臂另有
  /// 已成应答段回抽契约，不入本函数）。调用方须已持本键读改写窗口
  fn apply_set_opts<D: Device>(
    &mut self,
    opts: &SetOptions,
    store: &wkv::BatchStoreSession<'_, D>,
    keep_ttl: Option<Option<i64>>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let res = apply_set_with_expiry(
      store,
      opts.key,
      opts.val,
      (opts.expiry, opts.exp_high_precision),
      keep_ttl,
      &mut self.ttl_resume,
      output,
    );
    set_write_reply(res, output)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkSET_Conditional
  ///
  /// 条件写共同体（SET/SETEXNX/SETEXXX/SETKEEPTTL/SETKEEPTTLXX + GET 选项）。
  /// `expiry` 为相对数值（0 = 不过期；KEEPTTL 族恒 0），`high_precision` 表示
  /// 该数值为毫秒口径（PX）。C# 的 WRONGTYPE 分支（对象存储域）由对象键域
  /// 分流承接，见下方 object 分支注释。
  ///
  /// 向量登记表第四态窗内折叠（票 zcode-r163c-setguard 案一，判据单源
  /// [`registry_alive`]）：NX 命中第四态即「键在」，窗内直出 nil 零副作用
  /// 保留登记（与 SETNX 闩窗折叠探针同一裁决源，票 zcode-r161c-msetnx
  /// 案一同形）；XX / KEEPTTL / GET 等覆写或回显形态命中第四态时快臂无法
  /// 同步清退登记（`delete_vector_set` 系真异步），诚实降级 `Ok(false)`
  /// 交慢臂窗内折叠终裁；GET 形态命中第四态对位 C# getValue 臂
  /// （BasicCommands.cs:832-835）无 DELETE 重试，窗内直出 -WRONGTYPE 保留登记
  pub fn network_set_conditional<'a, D: Device>(
    &mut self,
    opts: &SetOptions,
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
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
    // 续跑标记进入即复位（沿 MSETNX resume 复位纪律；GETSET 亦入本函数，
    // 无过期形态恒不置位）
    self.ttl_resume = TtlResume::Full;
    // 存在性记账单点（票 wnode-string-bitmap-found-notfound-accounting-matrix：
    // C# NetworkSET_Conditional 无 GET 形 :786 与 GET 形 :830 均收敛 MainStoreOps
    // SET_Conditional 双重载，非 WRONGTYPE 结局恰计一帧——无输出重载 :279
    // notfound / :284 found、输出重载 :339 / :344 同式，WRONGTYPE 臂零计）：
    // 主体在 'fold 标块内推进，终态 break 携带存在性裁决（Some(true)=存活→found /
    // Some(false)=缺席→notfound / None=零入账），块尾函数尾 fold_outcome 单点
    // 补账恰一条。一切 Ok(false) 降级出口（含 Pending/ReplyEcho 续跑与截帧
    // 全量重放）提前返回零入账，交慢臂唯一出口收口，防降级重放双计；
    // WRONGTYPE 与存储错误帧出臂携 None 零入账，对位 C# WRONGTYPE / 异常臂
    // 无 incr。计数按存在性口径而非命令成败（票审核记录第 5 点）
    let outcome = 'fold: {
      if !get_value {
        // KEEPTTL 无标志形态不关心键是否存在，一律写并回 OK
        let (must_exist, must_absent) = (cmd.is_xx(), cmd.is_nx());

        // 条件写共同体整段同窗（存活探测 → RI 门 → 对象键分支 → 条件裁决 →
        // KEEPTTL 读旧 → 写共同体；对标 C# NetworkSET_Conditional 的 RMW
        // 单记录锁内全程：RMWMethods.cs MainStore/UnifiedStore 双臂）；失闩沿
        // Ok(false) 降级慢路径同段持窗重放
        let Some(_window) = store.try_rmw_window(key) else {
          return Ok(false);
        };

        // 窗内第四态复验（杜绝派发层 set_vector_guard 窗外放行后至取窗间
        // 并发 VADD 逃逸：NX 窗内终裁 nil，覆写形态降级交慢臂窗内清退）
        if Self::vector_live_in_window(store, vector, key) {
          if must_absent {
            // NX 命中第四态 = 键在（向量记录与主存 String 同槽恒判在）：
            // nil 出且绝不写值、登记保留，对位 C# SET_Conditional 键在分支计 found
            output.write_resp_null_ver(self.resp_protocol_version);
            break 'fold Some(true);
          }
          return Ok(false);
        }

        // 双域存活探测（附带存活键所在域）：无条件形态也须探测以区分
        // object 键（WRONGTYPE 事务重试域）与 string 键（KEEPTTL 回填前提）
        let found = match probe_alive_domain(store, key) {
          // Option<KeyTag>：Some = 存活键所在域，None = 键不存在
          Ok(Some(found)) => found,
          // 数据 / TTL 记录需磁盘裁决（或已过期待清除）：降级
          Ok(None) => return Ok(false),
          Err(_) => {
            output.write_resp_error(RESP_ERR_GENERIC);
            break 'fold None;
          }
        };

        // RI 键门（Meta 域臂）：存活 RangeIndex 元记录上任何字符串写形态皆拒，
        // 尤其 XX 族的 DELETE 清退不得波及 RI 键（存根与树文件随键一并消失）
        if matches!(found, Some(KeyTag::Meta)) {
          match ri_write_gate(store, key, output) {
            // WRONGTYPE / 通用错误帧出臂零入账（C# WRONGTYPE 臂无 incr，
            // 错误帧对位异常臂）
            RiWriteGate::Blocked => break 'fold None,
            RiWriteGate::Pass => {}
            // `probe_alive_domain` 命中 Meta 域即记录已在内存，本臂 Deferred
            // 不可达；保守仍按降级出口处理
            RiWriteGate::Deferred => return Ok(false),
          }
        }

        // 对象键（内存信封 / 经上方 RI 门放行的升阶 Meta 域，二者同为 C# 对象存储
        // 记录语义）：C# WRONGTYPE 分支（BasicCommands.cs:NetworkSET_Conditional:
        // 788-796）promote 事务 DELETE 后重试 SET_Conditional，旧 TTL 随 DELETE
        // 一并消失。rust 无双 store，单会话直连免 promote，`try_delete_sync` 双域
        // 级联承接（对标 ETag 族 promote_delete_object_key 先例）：
        // XX 族重试必 NOTFOUND → 回 nil；NX 族键已删条件成立 → 写入回 OK；
        // SETKEEPTTL 无条件写入且旧 TTL 不回填（杜绝 object 时代幽灵 TTL）。
        // Meta 域键两原语自动落降级臂（try_delete_sync 复合元数据 / try_upsert_sync
        // 在场即 Ok(Err)），交慢路径 delete_string / upsert_string 完整树清退闭环
        //
        // 记账形制：对象键首帧 WRONGTYPE 零计，终帧系 DELETE 后重试的
        // SET_Conditional——键必已缺席 → NOTFOUND → incr_session_notfound，
        // 故对象分支终态恒携 Some(false)（存在性即 C# 重试所见的窗内末态域况）
        if matches!(found, Some(KeyTag::ObjectEnvelope) | Some(KeyTag::Meta)) {
          if must_exist {
            break 'fold match store.try_delete_sync(key) {
              Ok(Ok(_)) => {
                output.write_resp_null_ver(self.resp_protocol_version);
                Some(false)
              }
              // 环形页翻转 / 冷数据 / 复合元数据：降级全异步路径重入
              Ok(Err(_)) => return Ok(false),
              Err(_) => {
                output.write_resp_error(RESP_ERR_GENERIC);
                break 'fold None;
              }
            };
          }
          break 'fold match self.apply_set_opts(opts, store, None, output) {
            Ok(true) => Some(false),
            Ok(false) => return Ok(false),
            Err(e) => return Err(e),
          };
        }

        let exists = found.is_some();
        if (must_exist && !exists) || (must_absent && exists) {
          // 条件不满足：C# 以 nil 表失败（SETEXNX 翻转 ok 标志后同一出口）；
          // 计帧按存在性——XX 判缺 → notfound、NX 判在 → found
          output.write_resp_null_ver(self.resp_protocol_version);
          break 'fold Some(exists);
        }

        // KEEPTTL：string 域按旧值回填（upsert 自带同步清 TTL）
        let old_ttl = if cmd.is_keep_ttl() {
          match ttl_of_sync(store, key) {
            Ok(StoreResult::Success(ttl)) => Some(ttl),
            Ok(StoreResult::NotFound) => None,
            Ok(StoreResult::RecordOnDisk) => return Ok(false),
            Err(_) => {
              output.write_resp_error(RESP_ERR_GENERIC);
              break 'fold None;
            }
          }
        } else {
          None
        };

        if let Some(Some(ticks)) = old_ttl {
          self.ttl_resume = TtlResume::KeepTtl(ticks);
        }

        break 'fold match self.apply_set_opts(opts, store, old_ttl, output) {
          Ok(true) => Some(exists),
          Ok(false) => return Ok(false),
          Err(e) => return Err(e),
        };
      }

      // GET 形态：回旧值（不存在则 nil），条件语义同上；
      // 双域读判定对象键 WRONGTYPE。读旧值 + 条件裁决 + 写共同体整段同窗
      //（GETSET 对标 C# 单次 RMW 读旧写新），失闩沿 Ok(false) 降级慢路径
      // 同段持窗重放
      let Some(_window) = store.try_rmw_window(key) else {
        return Ok(false);
      };
      // GET 形态窗内第四态复验：对位 C# getValue 臂（BasicCommands.cs:832-835）
      // 回 -WRONGTYPE 不重试、登记保留，杜绝派发层放行后并发 VADD 逃逸成
      // 旧值 nil + 覆写双成功（WRONGTYPE 臂零入账，C# 输出重载 IsWrongType
      // 侧无 incr）
      if Self::vector_live_in_window(store, vector, key) {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        break 'fold None;
      }
      let start_len = output.len();
      // 静默读（传 None）：hit/missing 折叠为存在性布尔，终态于本函数尾
      // 单点补账（对位 C# 输出重载 :339 notfound / :344 found 恰一帧；
      // 降级出口提前返回零入账，慢臂 read_cold 簿记唯一出口防双计）
      let hit = match read_user_sync(store, key, None, |v| {
        output.write_resp_bulk_string(v);
      }) {
        Ok(UserRead::Hit(())) => true,
        Ok(UserRead::WrongType) => {
          output.truncate(start_len);
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
          break 'fold None;
        }
        Ok(UserRead::Missing) => false,
        Ok(UserRead::Deferred) => {
          output.truncate(start_len);
          return Ok(false);
        }
        Err(_) => {
          output.truncate(start_len);
          output.write_resp_error(RESP_ERR_GENERIC);
          break 'fold None;
        }
      };

      // 条件裁决（SetCmd 五变体全覆盖：SETKEEPTTL 无条件写，SETKEEPTTLXX 仍要求键存在）
      let should_set = match cmd {
        SetCmd::Set | SetCmd::SetKeepTtl => true,
        SetCmd::SetExNx => !hit,
        SetCmd::SetExXx | SetCmd::SetKeepTtlXx => hit,
      };

      let old_ttl = if should_set && cmd.is_keep_ttl() {
        match ttl_of_sync(store, key) {
          Ok(StoreResult::Success(ttl)) => Some(ttl),
          Ok(StoreResult::NotFound) => None,
          Ok(StoreResult::RecordOnDisk) => {
            output.truncate(start_len);
            return Ok(false);
          }
          Err(_) => {
            output.truncate(start_len);
            output.write_resp_error(RESP_ERR_GENERIC);
            break 'fold None;
          }
        }
      } else {
        None
      };

      // 缺席键 nil 帧前置：与命中旧值 bulk 同段成帧（票 zcode-r153c-setrangeget
      // 处方二，对标 C# BasicCommands.cs:840-843 NOTFOUND→WriteNull 的完整应答
      // 形态），杜绝保留已成应答续跑时携出半帧
      if !hit {
        output.write_resp_null_ver(self.resp_protocol_version);
      }

      if should_set {
        // 整应答段末界（旧值 bulk 或 nil 帧毕）：Err(()) 臂回抽应答段只留
        // apply 已写错误帧
        let reply_end = output.len();
        if let Some(Some(ticks)) = old_ttl {
          // KEEPTTL 预置旧刻度续跑标记（与非 GET 臂同一单源纪律）：值写遇页
          // 翻转降级时慢臂按此刻度重放写值并回填，杜绝重读已被快臂 TTL 腿
          // 清退的墓碑致静默丢
          self.ttl_resume = TtlResume::KeepTtl(ticks);
        }
        match apply_set_with_expiry(
          store,
          key,
          val,
          (expiry, high_precision),
          old_ttl,
          &mut self.ttl_resume,
          output,
        ) {
          Ok(true) => {}
          Ok(false) => {
            // GET 臂续跑契约（票 zcode-r153c-setrangeget 案一）：值已提交后
            // TTL 腿降级置 Pending，就地表重映射为 ReplyEcho——保留已成应答
            //（旧值已覆写不可复原），随 exec 尾参续跑，慢臂仅补投 TTL 出
            // 零字节，禁整命令重放自碰已提交值（新值冒充旧值回显 / NX+GET
            // 反判 / KEEPTTL+GET 静默丢，对标 C# 单次 RMW 锁内旧值回显与值/
            // 过期一体落库）。提交前降级（Full / KeepTtl，值未落）照旧抽帧
            // 转全量重放
            if let TtlResume::Pending(ticks) = self.ttl_resume {
              self.ttl_resume = TtlResume::ReplyEcho(ticks);
            } else {
              output.truncate(start_len);
            }
            return Ok(false);
          }
          Err(()) => {
            // RI 门 WRONGTYPE / 写回错误帧出臂：回抽应答段后零入账（票口径，
            // 对位 C# WRONGTYPE 臂与异常臂无 incr）
            output.drain(start_len..reply_end);
            break 'fold None;
          }
        }
      }
      // 终态存在性折叠：命中→found / 缺席→notfound 恰一帧（C# 输出重载
      // :339/:344；慢臂 read_cold 簿记既有恰一帧同数，双臂同账）
      Some(hit)
    };
    fold_outcome(outcome, self.session_metrics.as_deref());
    Ok(true)
  }

  /// libs/server/Resp/BasicCommands.cs:NetworkAppend
  ///
  /// RMW 语义写回（保留既有 key 级 TTL，对标 C# MainStore/RMWMethods.cs
  /// APPEND 分支 "not changing the presence of ETag or Expiration"）；写回按
  /// C# 同一状态机两级派发——槽位容得下即 InPlaceUpdater 原位增长臂，否则回落
  /// CopyUpdater 整值尾部追加，两路应答逐字节一致
  pub fn network_append<'a, D: Device>(
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
    // 尾部并原位发布新值长——旧数据零复制、零尾部追加、零中间 Vec。空载荷走
    // 等长发布臂即 C# :800「If nothing to append, can avoid copy update」的
    // 免复制回长短路（等长零改写，WATCH 版本与写通知镜像经 try_grow_in_place
    // 同一发布收口照发，对位 C# Succeeded 臂 :425-430 照常推进版本）；缺失键
    // 与磁盘候选态交下方整值臂，与 C# InitialUpdater 建空键、CopyUpdater 整值
    // 重建两形态同构
    if let Some(res) = in_place_grow(&window, output, |cap, old_len| {
      let total = old_len + val.len();
      if total > cap.len() {
        return None;
      }
      cap[old_len..total].copy_from_slice(val);
      Some(total)
    }) {
      return res;
    }

    rmw_full_grow(store, &window, key, output, true, |old| match old {
      Some(v) => {
        let mut buf = Vec::with_capacity(v.len() + val.len());
        buf.extend_from_slice(v);
        buf.extend_from_slice(val);
        Cow::Owned(buf)
      }
      None => Cow::Borrowed(val),
    })
  }
}

/// SET 族过期数值整数校验单源（SETEX/PSETEX 的 [`parse_setex_args`] 与 SET 选项
/// EX/PX 的 [`parse_set_options`] 共用；对标 C# NetworkSET/NetworkSETEX 的
/// TryGetInt int32 口径）：非整数（含越 i32 幅值；前导零拒收系 rust 严格收口，
/// C# 死参放行 007，见 doc/zh/deviations.md §32）写 not-integer，非正写
/// invalid-expire-in-set；返回已校验正值（秒或毫秒），失败已落帧返回 None
#[inline]
fn parse_set_expiry(raw: &[u8], output: &mut Vec<u8>) -> Option<i64> {
  let v = parse_i32_arg(raw, output)?;
  if v <= 0 {
    abort_with_error_message(output, cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET);
    return None;
  }
  Some(v as i64)
}

/// NetworkSETEX / NetworkPSETEX 的参数推导单源（快慢路径共用；解析失败时
/// 已写出错误应答并返回 None，返回 `(key, expiry 秒或毫秒正数, val)`）
///
/// 对标 C# BasicCommands.cs:542 NetworkSETEX：过期须为整数（溢出走 not-integer 对标
/// C# TryGetInt int32 口径；前导零拒收系 rust 严格收口，C# TryGetInt 因死参放行 007，
/// 见 doc/zh/deviations.md §32）且 > 0
pub(crate) fn parse_setex_args<'p>(
  cmd_name: &str,
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'p [u8], i64, &'p [u8])> {
  let [key, expiry_raw, val] = unpack_args(parse_state, output, cmd_name)?;
  let expiry = parse_set_expiry(expiry_raw, output)?;
  Some((key, expiry, val))
}

/// NetworkSetRange 的参数推导单源（快慢路径共用；解析失败时已写出错误应答
/// 并返回 None，返回 `(key, offset, val)`，offset 已换算 usize 非负值域）
///
/// 对标 C#：偏移须为可解析整数（溢出走 not-integer 对标 C# TryGetInt 口径；
/// 前导零拒收系 rust 严格收口，C# TryGetInt 因死参放行 007，见 doc/zh/deviations.md §32），
/// 负值越界报错，offset + value 不得越过 512MB 负载上限（u64 口径，杜绝 usize 溢出 panic）
pub(crate) fn parse_setrange_args<'p>(
  parse_state: &[&'p [u8]],
  output: &mut Vec<u8>,
) -> Option<(&'p [u8], usize, &'p [u8])> {
  let [key, offset_raw, val] = unpack_args(parse_state, output, "SETRANGE")?;
  let offset = parse_i32_arg(offset_raw, output)?;
  if offset < 0 {
    abort_with_error_message(output, cs::RESP_ERR_GENERIC_OFFSETOUTOFRANGE);
    return None;
  }
  let offset = offset as usize;
  if offset as u64 + val.len() as u64 > MAX_BITMAP_PAYLOAD_BYTES as u64 {
    abort_with_error_message(output, cs::RESP_ERR_STRING_EXCEEDS_MAX_SIZE);
    return None;
  }
  Some((key, offset, val))
}

/// 字符串域增长臂的引擎单页容量前置门（SETRANGE/APPEND/SETBIT/BITFIELD 写臂，
/// 快慢双臂多落点共用判据源；票 zcode-r143c-setrangex 建门、zcode-r163c-bitopdst
/// 接线位图族写臂）
///
/// 与对象域 `envelope_overflow`（objects/object_store_utils.rs）同一单源判据：
/// `WedbStore::record_fits_page` 转发 whlog `record_fits`，与写侧 `try_rmw_sync`
/// 落笔的 `RecordTooLarge` 拒绝（whlog `validate_append_args`）共用 record_size
/// 单一公式，物理键构形亦与写侧同谱（`session_tag_key_with_prefix`
/// KeyTag::String），不新增第二上限常量。
///
/// 动机（review.md 板块4「边界输入响应前置拦截 / 单页容纳性前置校验」）：
/// C# 契约是「协议闸收 512MB、引擎页闸抛断连」两级形态——NetworkSetRange
/// （BasicCommands.cs:460-463）自认超页硬抛（AllocatorBase.cs:TryAllocate
/// "Entry does not fit on page"）会连累断连，故在 RESP 层预拒止损；页容量与
/// 512MB 协议闸之间的窗在 C# 属崩溃面。本仓已裁为整笔显式报错、旧值槽位无损
/// 的更优形态并锁测（tests/string_in_place_grow.rs
/// append_beyond_page_ceiling_leaves_record_intact），但止损点迟至物化之后：
/// 终值落败窗（页容量, 512MB] 的命令要先付整值回读（磁盘候选含整值冷 I/O）
/// 加至多近 512MB 零填充物化加注定失败的写回整轮才收口。本门将败局收口前移
/// 到取窗与物化之前，错误帧形与现终态逐字节不变
pub(crate) fn string_record_fits_page<D: Device>(
  store: &wkv::BatchStoreSession<'_, D>,
  key: &[u8],
  new_len: usize,
) -> bool {
  let phys = store.session_tag_key(KeyTag::String, key);
  store.store.record_fits_page(phys.len(), new_len)
}

/// SETRANGE/APPEND 原位增长臂三态收尾骨架（对标 C# MainStore/RMWMethods.cs
/// InPlaceUpdater 臂）：命中回新值长整帧，`Degrade` 整体降级异步，`Fallback`
/// （槽位不容 / 记录非活 / 引擎侧错损）回 `None` 不早返、交调用方续落整值臂
#[inline]
fn in_place_grow<D: Device>(
  window: &RmwWindow<'_, '_, D>,
  output: &mut Vec<u8>,
  grow: impl FnOnce(&mut [u8], usize) -> Option<usize>,
) -> Option<wresp::Result<bool>> {
  match window.try_grow_in_place(grow) {
    Ok(RmwGrow::InPlace(new_len)) => {
      output.write_resp_int(new_len as i64);
      Some(Ok(true))
    }
    Ok(RmwGrow::Degrade) => Some(Ok(false)),
    Ok(RmwGrow::Fallback) | Err(_) => None,
  }
}

/// 整值读改写体（[`in_place_grow`] 的回落通道，对标 C# CopyUpdater / InitialUpdater
/// 两形态）：[`UserRead`] 五臂 + `try_rmw_sync` 三态同形收口，`build` 依旧值在场
/// 与否构造同一终值（缺键走 `build(None)`：SETRANGE 零填充、APPEND 借用免抄），
/// 应答帧与逐臂旧写法一致；`page_gate` 为真时写回前按**终值长度**过
/// [`string_record_fits_page`] 单页门（判据数与旧两臂同），SETRANGE 取窗前已自收口
fn rmw_full_grow<'b, D: Device>(
  store: &wkv::BatchStoreSession<'_, D>,
  window: &RmwWindow<'_, '_, D>,
  key: &[u8],
  output: &mut Vec<u8>,
  page_gate: bool,
  build: impl Fn(Option<&[u8]>) -> Cow<'b, [u8]>,
) -> wresp::Result<bool> {
  let val = read_user_or_bail!(
    read_user_sync(store, key, None, |v| build(Some(v))),
    output,
    build(None)
  );
  if page_gate && !string_record_fits_page(store, key, val.len()) {
    output.write_resp_error(RESP_ERR_GENERIC);
    return Ok(true);
  }
  match window.try_rmw_sync(&val) {
    Ok(Ok(_)) => {
      output.write_resp_int(val.len() as i64);
      Ok(true)
    }
    Ok(Err(_)) => Ok(false),
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      Ok(true)
    }
  }
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
        // 过期数值整数校验（int32 域 not-integer / 非正 invalid-expire）归
        // [`parse_set_expiry`] 单源，与 parse_setex_args 同谱
        expiry = parse_set_expiry(raw, output)?;
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
  let is_keep_ttl = exp_option == ExpirationOption::Keepttl;
  if is_keep_ttl {
    expiry = 0;
  }
  let cmd = match (exist_options, is_keep_ttl) {
    (ExistOptions::Nx, _) => SetCmd::SetExNx,
    (ExistOptions::Xx, true) => SetCmd::SetKeepTtlXx,
    (ExistOptions::Xx, false) => SetCmd::SetExXx,
    (ExistOptions::None, true) => SetCmd::SetKeepTtl,
    (ExistOptions::None, false) => SetCmd::Set,
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
