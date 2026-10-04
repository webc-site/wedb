//! 慢路径公共壳：定长 arity 校验、纯读出帧共同体、整值冷读折叠、RMW
//! 写回收口、SET 写共同体异步内核与向量登记清退（各命令族慢臂共用单源）

use wdev::Device;
use wkv::RmwWindow;
use wresp::{
  cmd_strings::{self as cs, RESP_ERR_WRONG_TYPE, abort_with_error_message},
  ext::RespVecExt,
};

use crate::{
  resp::{
    basic_commands::ttl::try_get_absolute_expiry_ticks, vector::vector_manager::VectorManager,
  },
  storage::session::{common::UserReadAsync, storage_session::StorageSession},
};

/// SET 族写共同体成功应答 `+OK` 帧文本单源（裸写 / 条件写 / Pending 补投臂雷同）
pub(super) const REPLY_OK: &str = "OK";

/// 定长 arity 校验单源：长度不为 `N` 即以 `cmd_name` 出 wrong-arity 错误帧并回 `None` 早退
pub(super) fn arity<'a, const N: usize>(
  parse_state: &[&'a [u8]],
  cmd_name: &str,
  output: &mut Vec<u8>,
) -> Option<[&'a [u8]; N]> {
  <[&'a [u8]; N]>::try_from(parse_state)
    .map_err(|_| cs::abort_with_wrong_number_of_arguments(output, cmd_name))
    .ok()
}

/// SET 盲写臂共同体（SET 裸形 / SETEX / PSETEX 配对骨架）：取本键读改写窗
/// 交调用方持握 → RI 门（存活 RangeIndex 元记录拒写出统一 WRONGTYPE 帧并回
/// `None` 早退，盲写无前置读须在此拦）→ 窗内向量登记清退（票
/// zcode-r163c-setguard 案三：严格对齐 array_commands/mset_slow.rs MSET 慢臂
/// 「持窗后临界区内清退」单源标准，清退绝不漏在锁窗外，杜绝清退与取窗
/// 之间存在并发 VADD 再插入的双域天窗；未命中零操作幂等）。
/// （快路径 network_set / network_setex_impl 同一窗口契约，票
/// zcode-r32-rmwmatrix 立项一：对标 C# InternalUpsert.cs:67 / NetworkSETEX
/// 单记录 CAS 落库，裸值与带 TTL 统一持窗）
pub(super) async fn blind_write_gate<'s, 'k, D: Device>(
  storage: &'s StorageSession<'_, D>,
  vector: Option<&VectorManager>,
  key: &'k [u8],
  output: &mut Vec<u8>,
) -> Result<Option<RmwWindow<'s, 'k, D>>, ()> {
  let window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
  if storage.ri_write_gate(key).await.map_err(|_| ())? {
    output.write_resp_error(RESP_ERR_WRONG_TYPE);
    return Ok(None);
  }
  clear_vector_registry(storage, vector, key).await;
  Ok(Some(window))
}

/// 纯读臂出帧共同体（read_and_frame / read_and_frame_quiet 共用出帧核）：
/// Hit / Missing 分别出帧，对象键出统一 WRONGTYPE 帧；三臂出帧后直接收口
/// 无后续 await，与快臂逐字节一致
fn frame_user_read<T>(
  read: UserReadAsync<T>,
  output: &mut Vec<u8>,
  on_hit: impl FnOnce(&mut Vec<u8>, T),
  on_miss: impl FnOnce(&mut Vec<u8>),
) {
  match read {
    UserReadAsync::Hit(v) => on_hit(output, v),
    UserReadAsync::WrongType => output.write_resp_error(RESP_ERR_WRONG_TYPE),
    UserReadAsync::Missing => on_miss(output),
  }
}

/// 纯读臂出帧（漏斗入账臂）：GETRANGE/SUBSTR、STRLEN、GET 等 GET 族计数
/// 口径命令专用（对位 C# ReadWithUnsafeContext GET 形每键恰一帧）；
/// 位图族慢臂一律改走 [`read_and_frame_quiet`]（票
/// wnode-string-bitmap-found-notfound-accounting-matrix：C# 位图五命令全走
/// RMW_MainStore / Read_MainStore 零 incr_session_*，见 bitmap_slow 头注）
pub(super) async fn read_and_frame<T>(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  parse: impl FnMut(&[u8]) -> T,
  output: &mut Vec<u8>,
  on_hit: impl FnOnce(&mut Vec<u8>, T),
  on_miss: impl FnOnce(&mut Vec<u8>),
) -> Result<(), ()> {
  let read = storage.read_user(key, parse).await.map_err(|_| ())?;
  let _: () = frame_user_read(read, output, on_hit, on_miss);
  Ok(())
}

/// 纯读臂出帧零入账对偶（[`read_and_frame`] 的静默口，镜像
/// read_cold / read_cold_quiet 双口先例）：位图族慢臂
/// （GETBIT / BITCOUNT / BITPOS）专用——C# BitmapOps 五命令全走
/// RMW_MainStore / Read_MainStore，AdvancedOps 该两口零 incr_session_*，
/// 出帧不折叠命中/未命中；出帧体与入账臂共用 [`frame_user_read`]，
/// 不另起第二套（票 wnode-string-bitmap-found-notfound-accounting-matrix）
pub(super) async fn read_and_frame_quiet<T>(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  parse: impl FnMut(&[u8]) -> T,
  output: &mut Vec<u8>,
  on_hit: impl FnOnce(&mut Vec<u8>, T),
  on_miss: impl FnOnce(&mut Vec<u8>),
) -> Result<(), ()> {
  let read = storage.read_user_quiet(key, parse).await.map_err(|_| ())?;
  let _: () = frame_user_read(read, output, on_hit, on_miss);
  Ok(())
}

/// 整值冷读三态折叠（read_cold / read_cold_quiet 共用内核）：Hit→`Some(Some)`、
/// Missing→`Some(None)`，对象键出统一 WRONGTYPE 帧并回 `None` 供调用方早退
#[inline]
fn fold_cold(read: UserReadAsync<Vec<u8>>, output: &mut Vec<u8>) -> Option<Option<Vec<u8>>> {
  match read {
    UserReadAsync::Hit(v) => Some(Some(v)),
    UserReadAsync::Missing => Some(None),
    UserReadAsync::WrongType => {
      output.write_resp_error(RESP_ERR_WRONG_TYPE);
      None
    }
  }
}

/// 数值自增族前置读四臂折叠（[`read_cold_quiet`] 同款 `None`=已出帧早退口径，
/// read_user_quiet 零入账口，RMW 前置读不入账纪律）：Hit(Some)=旧值、
/// Hit(None)=旧值非数文案、WrongType=类型错帧、Missing=零值。INCR 族 i64
/// 与 INCRBYFLOAT f64 同形单源（C# NumUtils TryReadInt64/TryReadDouble 前置读同位）
pub(super) async fn read_user_num<T>(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  parse: impl FnMut(&[u8]) -> Option<T>,
  zero: T,
  not_num_msg: &'static str,
  output: &mut Vec<u8>,
) -> Result<Option<T>, ()> {
  match storage.read_user_quiet(key, parse).await.map_err(|_| ())? {
    UserReadAsync::Hit(Some(v)) => Ok(Some(v)),
    UserReadAsync::Hit(None) => {
      abort_with_error_message(output, not_num_msg);
      Ok(None)
    }
    UserReadAsync::WrongType => {
      output.write_resp_error(RESP_ERR_WRONG_TYPE);
      Ok(None)
    }
    UserReadAsync::Missing => Ok(Some(zero)),
  }
}

/// 整值冷读（漏斗入账臂）：GETEX / GET 形态条件读共用，对位 C# GET 族计数
/// 口径（ReadWithUnsafeContext GET 形每键恰一帧）；三态折叠见 [`fold_cold`]。
/// 位图写臂（SETBIT / BITFIELD）不入账，改走 [`read_cold_quiet`]（票
/// wnode-string-bitmap-found-notfound-accounting-matrix：原头注「SETBIT 写臂 /
/// BITFIELD 共用」系失真表述——C# 位图五命令全走 RMW_MainStore /
/// Read_MainStore 零计数口径，本订正回指该票）
pub(super) async fn read_cold(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> Result<Option<Option<Vec<u8>>>, ()> {
  let read = storage.read_user(key, |v| v.to_vec()).await;
  Ok(fold_cold(read.map_err(|_| ())?, output))
}

/// 整值冷读零入账对偶（RMW 前置读不入账纪律，SETRANGE / APPEND / SETBIT /
/// BITFIELD 写臂共用；对位快臂 read_user_sync 传 None 与 C# MainStoreOps
/// RMW 口零 incr_session_*，票 wnode-string-bitmap-found-notfound-accounting-
/// matrix 将位图写臂自 [`read_cold`] 入账臂迁入本口）
pub(super) async fn read_cold_quiet(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> Result<Option<Option<Vec<u8>>>, ()> {
  let read = storage.read_user_quiet(key, |v| v.to_vec()).await;
  Ok(fold_cold(read.map_err(|_| ())?, output))
}

/// 窗内写回 + 长度整数帧出帧尾部（SETRANGE / APPEND 写臂同形收口）
pub(super) async fn rmw_write_len<'k, 'w, D: Device>(
  storage: &StorageSession<'_, D>,
  window: &RmwWindow<'w, 'k, D>,
  val: &[u8],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let len = val.len() as i64;
  storage.rmw_string(window, val).await.map_err(|_| ())?;
  output.write_resp_int(len);
  Ok(())
}

/// SET 写共同体尾部（异步口）：upsert 自带同步清 TTL（对标快路径
/// `apply_set_with_expiry` 的 try_upsert_sync 面），随后按需写新 TTL 或
/// KEEPTTL 回填旧值（裸 ticks，对标 C# TrySetExpiration 裸值路径）
///
/// 前置契约：调用方须已持本键读改写窗口（`rmw_window`）——「清 TTL → 值写 →
/// TTL 写」与裸值覆写跨 await 交叠的串行化由调用方窗口承接（对标 C#
/// InternalUpsert.cs:67 纯写回同取记录闩，票 zcode-r32-rmwmatrix 立项一）；
/// SET 慢臂（[`super::set_conditional::slow_set_conditional`] 等）与 ETag 异步写共同体
/// （[`crate::resp::basic_etag_commands`] 的 `apply_etag_write_async` 值+TTL
/// 中段）共用本内核，RI 门两族各按本族出帧口径前置（SET 慢臂于域探针后
/// `ri_write_gate_quiet`，ETag 于本内核调用前单点前置）
///
/// `keep_ttl` 三态：`None` = 非 KEEPTTL 形态（expiry != 0 时写新 TTL）；
/// `Some(None)` = KEEPTTL 无旧值（保持无 TTL）；`Some(Some(ticks))` = 回填
pub(crate) async fn apply_set_with_expiry_async(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  val: &[u8],
  exp: (i64, bool),
  keep_ttl: Option<Option<i64>>,
) -> Result<(), ()> {
  let (expiry, high_precision) = exp;
  let expire_at_ticks = if expiry != 0 {
    try_get_absolute_expiry_ticks(expiry, high_precision).ok_or(())?
  } else {
    0
  };
  storage.upsert_string(key, val).await.map_err(|_| ())?;
  if let Some(old) = keep_ttl {
    // KEEPTTL：upsert 已同步清 TTL，按旧值回填；旧值本不存在则保持无 TTL
    if let Some(ticks) = old {
      storage.batch.put_ttl(key, ticks).await.map_err(|_| ())?;
    }
    return Ok(());
  }
  if expiry != 0 {
    storage
      .batch
      .put_ttl(key, expire_at_ticks)
      .await
      .map_err(|_| ())?;
  }
  Ok(())
}

/// SET 族写命令的向量登记表守卫（对标 exec 层 `set_vector_guard` 的写面：
/// 登记命中即预清退后覆写，杜绝 wkv string 域与向量域并存的幽灵双域键；
/// 摘除臂为真异步（条带独占锁 + 登记写透 `.await` 闭环，无内联收割），
/// 未命中零操作，幂等）。清退面全仓单源：MSET 慢臂
/// [`crate::resp::array_commands::slow::mset`] 持窗后亦复用本壳（票
/// zcode-r163c-setguard 案一，禁立第二套清退形态）
pub(crate) async fn clear_vector_registry(
  storage: &StorageSession<'_, impl Device>,
  vector: Option<&VectorManager>,
  key: &[u8],
) {
  if let Some(vm) = vector {
    vm.delete_vector_set(storage.batch.session_prefix().as_slice(), key)
      .await;
  }
}
