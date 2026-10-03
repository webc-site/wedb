//! SET 选项形态与写共同体（SetCmd/SetOptions 产物类型、RangeIndex 写门、
//! 值 + TTL 写内核与应答尾部；对标 BasicCommands.cs SET 族写命令段）

use wdev::Device;
use wresp::{
  cmd_strings::{self as cs, RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, abort_with_error_message},
  ext::RespVecExt,
};

use crate::{
  resp::{TtlLeg, TtlResume, basic_commands::ttl::try_get_absolute_expiry_ticks},
  storage::session::common::ttl_sync::{meta_is_range_index, put_ttl_sync},
};

/// SET 选项解析后的条件写命令形态（对标 libs/server/Resp/BasicCommands.cs:NetworkSETEXNX
/// 派发到的 RespCommand：SET/SETEXNX/SETEXXX/SETKEEPTTL/SETKEEPTTLXX）
#[derive(Debug, Copy, PartialEq, Eq, Clone)]
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
    // 杜绝整命令重放自碰已提交值（SET NX 误回 nil / KEEPTTL 回填读已清 TTL 静默丢）；
    // 域锚随刻度同点捕获，慢臂跨换号域比对失配即跳余腿补投（幽灵 TTL 收口）
    Ok(false) => {
      *ttl_resume = TtlResume::Pending(TtlLeg {
        ticks,
        domain: store.virtual_domain(),
      });
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
pub(super) fn set_write_reply(res: Result<bool, ()>, output: &mut Vec<u8>) -> wresp::Result<bool> {
  match res {
    Ok(true) => {
      output.write_resp_simple_string("OK");
      Ok(true)
    }
    Ok(false) => Ok(false),
    Err(()) => Ok(true),
  }
}
