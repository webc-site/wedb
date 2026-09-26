//! 字符串族 / OBJECT / 位图族慢路径执行段（快路径 `Ok(false)` 降级承接）
//!
//! 对标 C# BasicCommands.cs / BitmapCommands.cs 同步函数体内 CompletePending
//! 就地闭环的应答形态：快路径在环形页翻转 / RI 门 / 存在性探针降级后，本
//! 模块以同一套参数解析单源（快侧纯函数）+ 存储会话异步口重放整条命令，
//! 产出与快路径逐字节一致的应答。参数推导一律转调快侧同一纯函数，
//! 不在本模块重写第二套推导。

use itoa::Buffer as ItoaBuffer;
use wbase::num::{strict_f64, strict_i64};
use wbitmap::{
  BitFieldSecondaryCommand, BitOpAccumulator, BitmapOperation, bit_count_driver, bit_field_execute,
  bit_field_execute_ro, bit_pos_driver, get_bit, length_in_bytes, new_block_alloc_length_from_type,
  try_validate_bit_pos_offsets, update_bitmap,
};
use wdev::Device;
use wkv::RmwWindow;
use wresp::{
  check_args::parse_i32_arg,
  cmd_strings::{self as cs, RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, abort_with_error_message},
  command::RespCommand,
  ext::RespVecExt,
  resp_memory_writer::format_double,
};
use wval::{GarnetObjectType, KeyTag};
use zmij::Buffer as ZmijBuffer;

use super::{
  ObjectSubCmd,
  get::parse_getex_args,
  incr::{IncrCmd, parse_incr_args, parse_incr_by_float_args},
  set::{
    SetCmd, SetOptions, parse_set_options, parse_setex_args, parse_setrange_args,
    string_record_fits_page,
  },
  ttl::{GetexExpiry, try_get_absolute_expiry_ticks},
};
use crate::{
  resp::{
    TtlResume,
    bitmap::bitmap_commands::{parse_bit_args, parse_bit_count_args, parse_bit_pos_args},
    resp_server_session::RespServerSession,
    vector::vector_manager::VectorManager,
  },
  storage::session::{
    common::{
      UserReadAsync,
      ttl_sync::{meta_collection_type_of, probe_alive_with_registry_async_quiet, registry_alive},
    },
    storage_session::StorageSession,
  },
};

pub(super) const ENCODING_RAW: &[u8] = b"raw";
/// 未知信封标签兜底（对齐 C# UnifiedStore ReadMethods.cs:65 `_ => CmdStrings.hashtable`）
pub(super) const ENCODING_HASHTABLE: &[u8] = b"hashtable";

/// SET 族写共同体成功应答 `+OK` 帧文本单源（裸写 / 条件写 / Pending 补投臂雷同）
const REPLY_OK: &str = "OK";
/// BITOP 键数上限（含 destkey，对齐 C# BitmapOps MaxKeys = 64）
const BITOP_KEYS_MAX: usize = 64;

/// OBJECT 编码名映射（信封内层标签 / Meta collection_type 同表，快慢路径
/// 单一真源；集合默认臂对齐 C# UnifiedStore ReadMethods.cs:65
/// `_ => CmdStrings.hashtable`）
///
/// RangeIndex 显式回 raw：C# RI 主存记录不置 ValueIsObject 位（仅以
/// RecordType 字节判别，RangeIndexManager.cs:54 RangeIndexRecordType = 2），
/// HandleObjectEncoding else 臂恒 raw（ReadMethods.cs:69-72，非对象记录
/// 无 native integer/embstr 表示）——hashtable 默认臂仅覆盖集合对象
pub(super) const fn encoding_of_object_type(obj_type: GarnetObjectType) -> &'static [u8] {
  match obj_type {
    GarnetObjectType::SortedSet => b"skiplist",
    GarnetObjectType::List => b"quicklist",
    GarnetObjectType::RangeIndex => ENCODING_RAW,
    _ => ENCODING_HASHTABLE,
  }
}

/// 信封首字节→编码名共享解析（未知 / 扩展标签域 >= 0x40 恒兜底 hashtable，
/// 对齐 C# UnifiedStore ReadMethods.cs:48-77 HandleObjectEncoding）
#[inline]
pub(super) fn encoding_of_envelope_payload(raw: &[u8]) -> &'static [u8] {
  raw
    .first()
    .copied()
    .and_then(GarnetObjectType::from_u8)
    .map_or(ENCODING_HASHTABLE, encoding_of_object_type)
}

/// 定长 arity 校验单源：长度不为 `N` 即以 `cmd_name` 出 wrong-arity 错误帧并回 `None` 早退
fn arity<'a, const N: usize>(
  parse_state: &[&'a [u8]],
  cmd_name: &str,
  output: &mut Vec<u8>,
) -> Option<[&'a [u8]; N]> {
  <[&'a [u8]; N]>::try_from(parse_state)
    .map_err(|_| cs::abort_with_wrong_number_of_arguments(output, cmd_name))
    .ok()
}

/// 向量登记表命中探针（OBJECT 慢臂值域门用，判据与 exec 层值域门
/// `read_stored_index` 同一来源；SET 族第四态裁决已收拢窗内折叠单源
/// [`registry_alive`]，本壳不再服务 SET 臂，票 zcode-r163c-setguard 案二）
fn reg_hit(
  storage: &StorageSession<'_, impl Device>,
  vector: Option<&VectorManager>,
  key: &[u8],
) -> bool {
  vector.is_some_and(|vm| {
    vm.read_stored_index(storage.batch.session_prefix().as_slice(), key)
      .is_some()
  })
}

/// SET 盲写臂共同体（SET 裸形 / SETEX / PSETEX 配对骨架）：取本键读改写窗
/// 交调用方持握 → RI 门（存活 RangeIndex 元记录拒写出统一 WRONGTYPE 帧并回
/// `None` 早退，盲写无前置读须在此拦）→ 窗内向量登记清退（票
/// zcode-r163c-setguard 案三：严格对齐 array_commands.rs MSET 慢臂
/// 「持窗后临界区内清退」单源标准，清退绝不漏在锁窗外，杜绝清退与取窗
/// 之间存在并发 VADD 再插入的双域天窗；未命中零操作幂等）。
/// （快路径 network_set / network_setex_impl 同一窗口契约，票
/// zcode-r32-rmwmatrix 立项一：对标 C# InternalUpsert.cs:67 / NetworkSETEX
/// 单记录 CAS 落库，裸值与带 TTL 统一持窗）
async fn blind_write_gate<'s, 'k, D: Device>(
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
async fn read_and_frame<T>(
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
async fn read_and_frame_quiet<T>(
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

/// OBJECT 出帧单源（向量登记特判与编码判定共用同一映射）：命中编码时按
/// 子命令回编码名 / 1 / 0 / FREQ 不支持帧，键缺失（C# status != OK）回版本 nil
fn object_frame(
  sub_cmd: ObjectSubCmd,
  encoding: Option<&'static [u8]>,
  resp_version: u8,
  output: &mut Vec<u8>,
) {
  match (encoding, sub_cmd) {
    (Some(enc), ObjectSubCmd::Encoding) => output.write_resp_bulk_string(enc),
    (Some(_), ObjectSubCmd::Refcount) => output.write_resp_int(1),
    (Some(_), ObjectSubCmd::Idletime) => output.write_resp_int(0),
    (Some(_), ObjectSubCmd::Freq) => {
      abort_with_error_message(output, cs::RESP_ERR_OBJECT_FREQ_UNSUPPORTED)
    }
    (None, _) => output.write_resp_null_ver(resp_version),
  }
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

/// 整值冷读（漏斗入账臂）：GETEX / GET 形态条件读共用，对位 C# GET 族计数
/// 口径（ReadWithUnsafeContext GET 形每键恰一帧）；三态折叠见 [`fold_cold`]。
/// 位图写臂（SETBIT / BITFIELD）不入账，改走 [`read_cold_quiet`]（票
/// wnode-string-bitmap-found-notfound-accounting-matrix：原头注「SETBIT 写臂 /
/// BITFIELD 共用」系失真表述——C# 位图五命令全走 RMW_MainStore /
/// Read_MainStore 零计数口径，本订正回指该票）
async fn read_cold(
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
async fn read_cold_quiet(
  storage: &StorageSession<'_, impl Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> Result<Option<Option<Vec<u8>>>, ()> {
  let read = storage.read_user_quiet(key, |v| v.to_vec()).await;
  Ok(fold_cold(read.map_err(|_| ())?, output))
}

/// 窗内写回 + 长度整数帧出帧尾部（SETRANGE / APPEND 写臂同形收口）
async fn rmw_write_len<'k, 'w, D: Device>(
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
/// InternalUpsert.cs:67 纯写回同取记录闩，票 zcode-r32-rmwmatrix 立项一）
///
/// `keep_ttl` 三态：`None` = 非 KEEPTTL 形态（expiry != 0 时写新 TTL）；
/// `Some(None)` = KEEPTTL 无旧值（保持无 TTL）；`Some(Some(ticks))` = 回填
async fn apply_set_with_expiry_async(
  storage: &StorageSession<'_, impl Device>,
  opts: &SetOptions<'_>,
  keep_ttl: Option<Option<i64>>,
) -> Result<(), ()> {
  let expire_at_ticks = if opts.expiry != 0 {
    try_get_absolute_expiry_ticks(opts.expiry, opts.exp_high_precision).ok_or(())?
  } else {
    0
  };
  storage
    .upsert_string(opts.key, opts.val)
    .await
    .map_err(|_| ())?;
  if let Some(old) = keep_ttl {
    if let Some(ticks) = old {
      storage
        .batch
        .put_ttl(opts.key, ticks)
        .await
        .map_err(|_| ())?;
    }
    return Ok(());
  }
  if opts.expiry != 0 {
    storage
      .batch
      .put_ttl(opts.key, expire_at_ticks)
      .await
      .map_err(|_| ())?;
  }
  Ok(())
}

/// NetworkSET_Conditional 的异步对偶（SET 选项形态 / GETSET 条件写共同体），
/// 序次对齐快路径 `network_set_conditional`：取窗 → 第四态复验 → 域探针 →
/// RI 门 → 对象键分支 → 条件裁决 → KEEPTTL 旧值 → 写共同体 → 应答
///
/// 向量登记表第四态窗内统一裁决（票 zcode-r163c-setguard 案二，判据单源
/// [`registry_alive`]，与快臂同一折叠式）：NX 命中第四态即「键在」，直出
/// nil 终态零副作用、登记保留（与 SETNX 慢臂
/// [`crate::storage::session::common::ttl_sync::probe_alive_with_registry_async`]
/// 同一裁决源）；XX / 无条件 / KEEPTTL
/// 命中第四态且条件成立，持窗临界区内 [`clear_vector_registry`] 清退后
/// 覆写（对位 C# 锁内 DELETE+SET_Conditional 重投终态，BasicCommands.cs:
/// 788-796）；GET 形态命中第四态对位 C# getValue 臂（:832-835）无 DELETE
/// 重试，直出 -WRONGTYPE 登记保留。窗外破坏性预清退就此删除——旧序
/// 「窗外 clear → 窗内三域探针」使 NX 误判缺写 +OK 毁登记、XX 判缺回 nil
/// 却已静默摧毁存活向量索引（NX/XX 语义反转），本臂收拢为窗内一次折叠终裁
async fn slow_set_conditional(
  storage: &StorageSession<'_, impl Device>,
  opts: &SetOptions<'_>,
  vector: Option<&VectorManager>,
  resume: TtlResume,
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let SetOptions {
    key,
    cmd,
    get_value,
    ..
  } = *opts;
  // 条件写共同体整段同窗（第四态复验 → 存活探测 → RI 门 → 对象键分支 →
  // 条件裁决 → KEEPTTL 读旧 → 写共同体；对标快路径 network_set_conditional
  // 同一窗口契约，C# 单记录 RMW 锁内全程）
  let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
  // 第四态判据窗内单次折叠取值（registry_alive 全链单源），存活判定
  // exists = 三域命中 ∨ 第四态在场，与快臂 probe_alive_with_registry 同式
  let prefix = storage.batch.session_prefix();
  let vector_alive = registry_alive(vector, prefix.as_slice(), key);
  if !get_value {
    let (must_exist, must_absent) = (cmd.is_xx(), cmd.is_nx());

    // NX 命中第四态：键在，nil 出且绝不写值、绝不摘除登记；终态「键在」
    // → found 恰一帧（与快臂 NX 第四态命中同形，票
    // wnode-string-bitmap-found-notfound-accounting-matrix）
    if must_absent && vector_alive {
      output.write_resp_null_ver(resp_version);
      storage.record_read_outcome(true);
      return Ok(());
    }

    // 双域存活探测（三域异步裁决，对标快路径 probe_alive_domain）；静默
    // 对偶口 + 终态单点补账（票 wnode-string-bitmap-found-notfound-
    // accounting-matrix：簿记档逐域入账使缺失键计 3、对象键计 2，与 C#
    // SET_Conditional 单帧口径失联，MainStoreOps.cs:279/:284）
    let found = storage
      .probe_alive_domain_quiet_with_prefix(prefix.as_slice(), key)
      .await
      .map_err(|_| ())?;
    if matches!(found, Some(KeyTag::Meta))
      && storage.ri_write_gate_quiet(key).await.map_err(|_| ())?
    {
      // 存活 RangeIndex 元记录：字符串写一律拒（对标快路径 ri_write_gate）；
      // WRONGTYPE 臂零入账（C# WRONGTYPE 臂无 incr，MainStoreOps.cs:273）
      output.write_resp_error(RESP_ERR_WRONG_TYPE);
      return Ok(());
    }

    // 对象键（内存信封 / 经上方 RI 门放行的升阶 Meta 域）：与快路径
    // network_set_conditional 对象键臂对称双域匹配
    if matches!(found, Some(KeyTag::ObjectEnvelope) | Some(KeyTag::Meta)) {
      if must_exist {
        // C# WRONGTYPE 分支 XX 族：promote DELETE 重试必 NOTFOUND → 回 nil；
        // 先删对象键（用户键级联删除，含随键 TTL；Meta 域经 delete_string
        // 降级臂完整树清退）再答 nil，与快路径 try_delete_sync 同终态；
        // 重试形末态 NOTFOUND → notfound 恰一帧（首次 WRONGTYPE 零计）
        storage.delete_string(key).await.map_err(|_| ())?;
        output.write_resp_null_ver(resp_version);
        storage.record_read_outcome(false);
        return Ok(());
      }
      // NX / 无条件 / KEEPTTL：覆写且 keep_ttl 恒 None（upsert_string 自带
      // 信封 / Meta 树覆写清退，旧 TTL 随清退消失，杜绝 object 时代幽灵 TTL）；
      // C# DELETE+SET_Conditional 重试形末态缺席写入 → notfound 恰一帧
      apply_set_with_expiry_async(storage, opts, None).await?;
      output.write_resp_simple_string(REPLY_OK);
      storage.record_read_outcome(false);
      return Ok(());
    }

    let exists = found.is_some() || vector_alive;
    if (must_exist && !exists) || (must_absent && exists) {
      // 条件不满足：C# 以 nil 表失败；按存在性折叠 found / notfound 恰一帧
      output.write_resp_null_ver(resp_version);
      storage.record_read_outcome(exists);
      return Ok(());
    }
    if vector_alive {
      // XX / 无条件 / KEEPTTL 命中第四态且条件成立：持窗临界区内先清退
      // 登记再覆写（C# DELETE+SET_Conditional 重投同终态，杜绝双域并存）
      clear_vector_registry(storage, vector, key).await;
    }

    // KEEPTTL：按旧值回填（upsert 自带同步清 TTL）
    let keep_ttl = if cmd.is_keep_ttl() {
      if let TtlResume::KeepTtl(ticks) = resume {
        Some(Some(ticks))
      } else {
        Some(storage.batch.ttl_of(key).await.map_err(|_| ())?)
      }
    } else {
      None
    };
    apply_set_with_expiry_async(storage, opts, keep_ttl).await?;
    output.write_resp_simple_string(REPLY_OK);
    // 写成功终态：按存在性折叠 found（覆写存活键）/ notfound（缺席写入）
    // 恰一帧（票 wnode-string-bitmap-found-notfound-accounting-matrix）
    storage.record_read_outcome(exists);
    return Ok(());
  }

  // GET 形态命中第四态：-WRONGTYPE 登记保留（C# getValue 臂无 DELETE 重试，
  // 旧值不可读即无覆写义务）
  if vector_alive {
    output.write_resp_error(RESP_ERR_WRONG_TYPE);
    return Ok(());
  }

  // GET 形态：回旧值（不存在则 nil），条件语义同上；双域读判定对象键 WRONGTYPE
  // （[`read_cold`] 折叠入账整值冷读）
  let Some(old) = read_cold(storage, key, output).await? else {
    return Ok(());
  };

  let should_set = if cmd.is_keep_ttl() {
    !cmd.is_xx() || old.is_some()
  } else if cmd.is_nx() {
    old.is_none()
  } else if cmd.is_xx() {
    old.is_some()
  } else {
    true
  };

  let keep_ttl = if should_set && cmd.is_keep_ttl() {
    if let TtlResume::KeepTtl(ticks) = resume {
      Some(Some(ticks))
    } else {
      Some(storage.batch.ttl_of(key).await.map_err(|_| ())?)
    }
  } else {
    None
  };
  if should_set {
    apply_set_with_expiry_async(storage, opts, keep_ttl).await?;
  }
  match old {
    Some(old) => output.write_resp_bulk_string(&old),
    None => output.write_resp_null_ver(resp_version),
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

/// 字符串族 / OBJECT 慢路径执行段入口（exec_slow 分派；`Err(())` 为存储
/// 错误，调用方统一应答）
pub(crate) async fn string_slow(
  storage: &StorageSession<'_, impl Device>,
  cmd: RespCommand,
  parse_state: &[&[u8]],
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  use RespCommand as C;
  let resp_version = storage.resp_version;
  match cmd {
    C::Set | C::Setexnx => {
      // SET 全形态（裸 SET 即无选项默认）与 SETEXNX 同走选项单源；
      // 无 NX/XX 的 EX/PX/无过期走盲写共同体（对标快路径 network_set_ex）
      // 尾参为快路径「值已提交 + TTL 待投」续跑标记（[`TtlResume::from_tail`]
      // 逆解析，沿 MSETNX/DEL 尾参先例，exec 降级快照恒追加）：Pending = 值
      // 已同步提交、TTL 遭环形页翻转降级，剥尾参后跳过整命令重放自碰已提交
      // 值（SET NX 误回 nil / KEEPTTL 回填读已清 TTL 静默丢，票
      // wnode-nx-conditional-ttl-degrade-replay-selfhit），持窗仅补投 TTL
      // 回 +OK；ReplyEcho = GET 回旧值形值已提交且应答已由快臂成帧保留
      //（票 zcode-r153c-setrangeget 案一），持窗补投 TTL 后出零字节——会话
      // 累积应答由泵先冲出，禁全量重放；Full / KeepTtl = 提交前降级，
      // 照旧整命令全量重放
      let Some((tail, cmd_args)) = parse_state.split_last() else {
        cs::abort_with_wrong_number_of_arguments(output, "SET");
        return Ok(());
      };
      let resume = TtlResume::from_tail(Some(tail));
      let Some(opts) = parse_set_options(cmd_args, output) else {
        return Ok(());
      };
      if let TtlResume::Pending(ticks) | TtlResume::ReplyEcho(ticks) = resume {
        // 快臂已持同一窗口契约提交值，Pending 降级零应答 / ReplyEcho 应答
        // 已保留；本臂复取同窗补投裸 ticks（对标 C# 单记录 RMW 锁内值与
        // 过期一体落库的终态）：Pending 出 +OK，ReplyEcho 不出任何帧
        let _window = storage.batch.rmw_window(opts.key).await.map_err(|_| ())?;
        storage
          .batch
          .put_ttl(opts.key, ticks)
          .await
          .map_err(|_| ())?;
        if matches!(resume, TtlResume::Pending(_)) {
          output.write_resp_simple_string(REPLY_OK);
        }
        return Ok(());
      }
      if opts.expiry != 0
        && try_get_absolute_expiry_ticks(opts.expiry, opts.exp_high_precision).is_none()
      {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET);
        return Ok(());
      }
      if opts.cmd == SetCmd::Set && !opts.get_value {
        // 盲写共同体：取窗 → RI 门 → 登记窗内清退（窗口契约单源见
        // [`blind_write_gate`]，清退次序对齐 MSET 窗内标准）
        let Some(_window) = blind_write_gate(storage, vector, opts.key, output).await? else {
          return Ok(());
        };
        apply_set_with_expiry_async(storage, &opts, None).await?;
        output.write_resp_simple_string(REPLY_OK);
        return Ok(());
      }
      // 选项形态条件写：第四态裁决（NX 判在出 nil 保留登记 / GET 形态
      // -WRONGTYPE / XX 与 KEEPTTL 覆写形窗内清退）已收拢 slow_set_conditional
      // 持窗临界区内一次折叠终裁（票 zcode-r163c-setguard 案二），本臂不再
      // 于窗外 reg_hit 预清退
      slow_set_conditional(storage, &opts, vector, resume, resp_version, output).await
    }
    C::Setex | C::Psetex => {
      let cmd_name = if cmd == C::Setex { "SETEX" } else { "PSETEX" };
      let Some((key, expiry, val)) = parse_setex_args(cmd_name, parse_state, output) else {
        return Ok(());
      };
      let high_precision = cmd == C::Psetex;
      let Some(expire_at_ticks) = try_get_absolute_expiry_ticks(expiry, high_precision) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_INVALIDEXP_IN_SET);
        return Ok(());
      };
      // 盲写共同体（取窗 → RI 门 → 登记窗内清退 +「清 TTL + 值写 + 新 TTL
      // 写」整段同窗收口，契约单源见 [`blind_write_gate`]，对标 C#
      // NetworkSETEX 单记录一次 CAS 落库）
      let Some(_window) = blind_write_gate(storage, vector, key, output).await? else {
        return Ok(());
      };
      storage.upsert_string(key, val).await.map_err(|_| ())?;
      storage
        .batch
        .put_ttl(key, expire_at_ticks)
        .await
        .map_err(|_| ())?;
      output.write_resp_simple_string(REPLY_OK);
      Ok(())
    }
    C::Setnx => {
      let Some([key, val]) = arity(parse_state, "SETNX", output) else {
        return Ok(());
      };
      // 条件写整段同窗（快路径 network_setnx 同一窗口契约，票
      // zcode-r32-rmwmatrix 立项三：探测与写入一体，杜绝并发 SET 落两步间隙
      // 丢已确认写）
      let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      // 存在性探测（闩窗内折叠探针单源：三域异步裁决 + 向量登记表第四态，
      // 对象键同计存在，C# NX 语义；票 zcode-r161c-msetnx 案一：原窗外
      // 手抄 is_some_and(read_stored_index) 位删除，判据收拢至本窗内一次
      // 折叠，与快臂 probe_alive_with_registry 同一折叠式，ttl_sync.rs
      // 「同一判据源勿再手抄」纪律归一）
      //
      // 恰一帧终态补账（票 wnode-string-bitmap-found-notfound-accounting-
      // matrix）：探针改静默对偶口（簿记档逐域入账使缺失键计 3，与 C#
      // SETNX→SET_Conditional 单帧口径失联，MainStoreOps.cs:279/:284），
      // 存活 found / 缺席写成功 notfound 经 record_read_outcome 单点折叠；
      // 存储错误 Err(()) 臂零入账（对位 C# 异常臂无 incr，沿 getex 先例）
      let prefix = storage.batch.session_prefix();
      if probe_alive_with_registry_async_quiet(storage, prefix.as_slice(), key, vector)
        .await
        .map_err(|_| ())?
      {
        storage.record_read_outcome(true);
        output.write_resp_int(0);
        return Ok(());
      }
      storage.upsert_string(key, val).await.map_err(|_| ())?;
      storage.record_read_outcome(false);
      output.write_resp_int(1);
      Ok(())
    }
    C::Getset => {
      // C# 走 NetworkSET_Conditional(SET, getValue: true)：无条件写入并回旧值；
      // 登记命中即错型不可读旧值，回 -WRONGTYPE 保留登记（exec 层值域门同判，
      // 窗内折叠终裁收拢 slow_set_conditional，票 zcode-r163c-setguard 案二）
      let Some([key, val]) = arity(parse_state, "GETSET", output) else {
        return Ok(());
      };
      let opts = SetOptions {
        key,
        val,
        expiry: 0,
        exp_high_precision: false,
        cmd: SetCmd::Set,
        get_value: true,
      };
      slow_set_conditional(
        storage,
        &opts,
        vector,
        TtlResume::Full,
        resp_version,
        output,
      )
      .await
    }
    C::Setrange => {
      let Some((key, offset, val)) = parse_setrange_args(parse_state, output) else {
        return Ok(());
      };
      // 与快臂 network_set_range 同一前置判据源（[`string_record_fits_page`]）：
      // 超窗终值在取窗与整值冷读回之前即按快臂 generic 同帧形收口，杜绝慢臂
      // RecordTooLarge 经 ? 上抛的次级落点与快臂帧形分叉
      if !string_record_fits_page(&storage.batch, key, offset + val.len()) {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(());
      }
      // 冷读旧值前先取本键读改写原子窗口（持本键桶排他闩贯穿读—算—写回全程，
      // 对标 C# InternalRMW 的 ephemeral 独占闩）；前置读走 [`read_cold_quiet`]
      // 零入账口（RMW 前置读不入账纪律，同 C# MainStoreOps SETRANGE RMW 口）
      let window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      let Some(old) = read_cold_quiet(storage, key, output).await? else {
        return Ok(());
      };
      // 补零增长 + 拷贝覆写（缺失键空旧值同形折叠，对位 C# SetAndCopyTo 追加补零）
      let mut new_val = old.unwrap_or_default();
      let required_len = offset + val.len();
      if new_val.len() < required_len {
        new_val.resize(required_len, 0);
      }
      new_val[offset..offset + val.len()].copy_from_slice(val);
      rmw_write_len(storage, &window, &new_val, output).await?;
      Ok(())
    }
    C::Append => {
      let Some([key, val]) = arity(parse_state, "APPEND", output) else {
        return Ok(());
      };
      // 同 SETRANGE：追加读改写全程持本键桶排他闩，前置读同走零入账口
      let window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      let Some(old) = read_cold_quiet(storage, key, output).await? else {
        return Ok(());
      };
      let mut new_val = old.unwrap_or_default();
      new_val.extend_from_slice(val);
      // 与快臂 network_append 回落臂同一前置判据源（[`string_record_fits_page`]）：
      // 超窗终值在写回前按快臂 generic 同帧形收口，免走注定失败的引擎写回
      //（Hit 臂整值冷回读本身系 C# CopyUpdater 同构成本，不收进本门射程；
      // 磁盘候选记录经本臂整值读—重建即 C# CopyUpdater 形态，空载荷与之同构）
      if !string_record_fits_page(&storage.batch, key, new_val.len()) {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(());
      }
      rmw_write_len(storage, &window, &new_val, output).await?;
      Ok(())
    }
    C::Incr | C::Decr | C::Incrby | C::Decrby => {
      let incr_cmd = match cmd {
        C::Incr => IncrCmd::Incr,
        C::Decr => IncrCmd::Decr,
        C::Incrby => IncrCmd::IncrBy,
        _ => IncrCmd::DecrBy,
      };
      let Some((key, delta)) = parse_incr_args(incr_cmd, parse_state, output) else {
        return Ok(());
      };
      // 读—算—写回全程持本键桶排他闩；旧值口径对位 C# IsValidNumber →
      // NumUtils.TryReadInt64：拒前导零（含 '+' 前缀形态，C# IsValidNumber
      // 存在 '+' 绕过缺陷放行 "+007"，见 deviations 条目 32）；前置读走
      // read_user_quiet 零入账口（RMW 前置读不入账纪律，对位快臂 incr.rs
      // read_user_sync 传 None 与 C# MainStoreOps.cs:Increment 全链零计数）
      let window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      let val = match storage
        .read_user_quiet(key, strict_i64)
        .await
        .map_err(|_| ())?
      {
        UserReadAsync::Hit(Some(v)) => v,
        UserReadAsync::Hit(None) => {
          abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return Ok(());
        }
        UserReadAsync::WrongType => {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
          return Ok(());
        }
        UserReadAsync::Missing => 0,
      };
      // C# checked 加法溢出与"非整数旧值"共用 not-integer 错误且不落写
      let Some(next) = val.checked_add(delta) else {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(());
      };
      let mut buf = ItoaBuffer::new();
      storage
        .rmw_string(&window, buf.format(next).as_bytes())
        .await
        .map_err(|_| ())?;
      output.write_resp_int(next);
      Ok(())
    }
    C::Incrbyfloat => {
      let Some((key, incr_by)) = parse_incr_by_float_args(parse_state, output) else {
        return Ok(());
      };
      // C# parseState.TryGetDouble 默认 canBeInfinite: true（INF 白名单 + NaN 拒）
      // 前置读同走 read_user_quiet 零入账口（同 INCR 族臂纪律）
      let window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      let val = match storage
        .read_user_quiet(key, |raw| strict_f64(raw, true))
        .await
        .map_err(|_| ())?
      {
        UserReadAsync::Hit(Some(v)) => v,
        UserReadAsync::Hit(None) => {
          abort_with_error_message(output, cs::RESP_ERR_NOT_VALID_FLOAT);
          return Ok(());
        }
        UserReadAsync::WrongType => {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
          return Ok(());
        }
        UserReadAsync::Missing => 0.0,
      };
      // 对标 C# IsValidDouble：旧值自身非有限 → NaN/Infinity 文案
      if !val.is_finite() {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_NAN_INFINITY_INCR);
        return Ok(());
      }
      let next = val + incr_by;
      // 有限 + 有限相加溢出无穷大与非法旧值同报 not-valid-float
      if !next.is_finite() {
        abort_with_error_message(output, cs::RESP_ERR_NOT_VALID_FLOAT);
        return Ok(());
      }
      // 对标 NumUtils.WriteDouble：无指数记法十进制表示，整数结果无小数点
      let mut buf = ZmijBuffer::new();
      let formatted = format_double(next, &mut buf);
      storage
        .rmw_string(&window, formatted.as_bytes())
        .await
        .map_err(|_| ())?;
      output.write_resp_bulk_string(formatted.as_bytes());
      Ok(())
    }
    C::Getex => {
      let Some((key, expiry)) = parse_getex_args(parse_state, output) else {
        return Ok(());
      };
      // 读值 + TTL 应用整段同窗收口（对标快路径 network_getex 同一窗口契约；
      // C# 单次 RMW 锁内一体完成）。persist_key / expire_at_ticks 包装自取
      // 本键桶闩（wkv persist/expire_at 键闩与本窗口同址互斥），持窗内禁调
      // ——窗内改 batch 裸写 + 同款 WATCH 推进尾巴：At 恒写恒推进（对齐
      // expire_at_ticks_opt 的 applied > 0）；Persist 真实删除才推进（对齐
      // del_ttl_sync 纪律）。读面 Hit 已含 TTL 裁决，wkv persist/expire_at
      // 的到期 purge 面与过去时间戳面（parse 层 target > now）均不可达
      let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      let Some(old) = read_cold(storage, key, output).await? else {
        return Ok(());
      };
      match old {
        Some(val) => {
          // 过期应用先于应答闭环（对标快路径同序）
          match expiry {
            GetexExpiry::None => {}
            // PERSIST：移除键级 TTL（裸写单点）
            GetexExpiry::Persist => {
              if storage.batch.has_ttl_tag(key).map_err(|_| ())? {
                storage.batch.del_ttl(key).await.map_err(|_| ())?;
                storage.batch.bump_watch_version(key);
              }
            }
            // 绝对过期刻度（裸写单点）
            GetexExpiry::At(ticks) => {
              storage.batch.put_ttl(key, ticks).await.map_err(|_| ())?;
              storage.batch.bump_watch_version(key);
            }
          }
          output.write_resp_bulk_string(&val);
        }
        // 键缺失（C# NOTFOUND / 过期键）回版本 nil
        None => output.write_resp_null_ver(resp_version),
      }
      Ok(())
    }
    C::Getrange | C::Substr => {
      let cmd_name = if cmd == C::Getrange {
        "GETRANGE"
      } else {
        "SUBSTR"
      };
      let Some([key, start_raw, end_raw]) = arity(parse_state, cmd_name, output) else {
        return Ok(());
      };
      // start/end 须可解析为整数（溢出走 not-integer 对齐 C# TryGetInt；前导零拒收系 rust 严格收口，见 doc/zh/deviations.md §32），否则报 not-integer
      let Some(start) = parse_i32_arg(start_raw, output).map(i64::from) else {
        return Ok(());
      };
      let Some(end) = parse_i32_arg(end_raw, output).map(i64::from) else {
        return Ok(());
      };
      read_and_frame(
        storage,
        key,
        |val| {
          let len = val.len() as i64;
          let (start, end) = RespServerSession::normalize_range(start, end, len);
          // 对标 C# CopyRespTo 半边防御：normalize_range 的反转区间（start > end）
          // 与相等同样回空，杜绝安全切片 panic
          if start >= end {
            Vec::new()
          } else {
            val[(start as usize)..(end as usize)].to_vec()
          }
        },
        output,
        |out, slice| out.write_resp_bulk_string(&slice),
        // 键缺失回空串（C# NOTFOUND → 空批量串）
        |out| out.write_resp_bulk_string(b""),
      )
      .await?;
      Ok(())
    }
    C::Strlen => {
      let Some([key]) = arity(parse_state, "STRLEN", output) else {
        return Ok(());
      };
      read_and_frame(
        storage,
        key,
        |v| v.len(),
        output,
        |out, len| out.write_resp_int(len as i64),
        |out| out.write_resp_int(0),
      )
      .await?;
      Ok(())
    }
    C::ObjectEncoding | C::ObjectFreq | C::ObjectIdletime | C::ObjectRefcount => {
      let sub_cmd = match cmd {
        C::ObjectEncoding => ObjectSubCmd::Encoding,
        C::ObjectFreq => ObjectSubCmd::Freq,
        C::ObjectIdletime => ObjectSubCmd::Idletime,
        _ => ObjectSubCmd::Refcount,
      };
      object_slow(storage, sub_cmd, parse_state, vector, output).await
    }
    _ => {
      // 分派表漏接线信号：本臂只应承接字符串族与 OBJECT，其余落此即缺陷
      cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
      Ok(())
    }
  }
}

/// NetworkOBJECT 的异步对偶（四子命令；语义对照 UnifiedStore
/// ReadMethods:HandleObjectEncoding，与快路径 `network_object` 同口径：
/// 向量键登记特判 → String 域 raw / 信封标签映射 / 升阶键 Meta 映射 /
/// 缺失回 nil）
///
/// libs/server/API/GarnetApiUnifiedCommands.cs:OBJECT
///（C# GarnetApi.OBJECT(key) 转发 storageSession.Read_UnifiedStore →
/// HandleObjectEncoding 编码判定；rust 无该 API 包装层，四子命令读写折叠于
/// 本函数与快路径 `network_object`，本注释挂异步对偶即判定内核落点）
pub(crate) async fn object_slow(
  storage: &StorageSession<'_, impl Device>,
  sub_cmd: ObjectSubCmd,
  parse_state: &[&[u8]],
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let Some([key]) = arity(parse_state, sub_cmd.as_str(), output) else {
    return Ok(());
  };
  let resp_version = storage.resp_version;

  // 向量键登记特判（快路径同形：raw / 1 / 0 / FREQ 不支持）——等价 raw 编码
  if reg_hit(storage, vector, key) {
    object_frame(sub_cmd, Some(ENCODING_RAW), resp_version, output);
    return Ok(());
  }

  // 编码判定：String 域命中 → raw；信封域按内层标签映射；升阶键按 Meta
  // collection_type 同款映射；过期键与缺失键一致回 nil
  // 三域漏斗与信封/Meta 二级续探一律走零入账静默口（对位快臂
  // read_user_sync 传 None 与 C# Read_UnifiedStore 恒零计，单点真源注记见
  // user_read.rs read_user_sync 头注，票 zcode-r157c-objenc 案一；严禁按
  // GET/DUMP 簿记先例回补入账）
  let encoding = match storage.read_user_quiet(key, |_| ()).await.map_err(|_| ())? {
    UserReadAsync::Hit(()) => Some(ENCODING_RAW),
    UserReadAsync::WrongType => {
      let envelope = storage
        .read_tag_quiet(key, KeyTag::ObjectEnvelope, encoding_of_envelope_payload)
        .await
        .map_err(|_| ())?;
      match envelope {
        Some(enc) => Some(enc),
        None => storage
          .read_tag_quiet(key, KeyTag::Meta, meta_collection_type_of)
          .await
          .map_err(|_| ())?
          .flatten()
          .map(encoding_of_object_type),
      }
    }
    UserReadAsync::Missing => None,
  };
  object_frame(sub_cmd, encoding, resp_version, output);
  Ok(())
}

/// 位图族慢路径执行段入口（快路径降级承接；解析一律转调 bitmap_commands
/// 的推导单源，应答形态与快路径逐字节一致）
pub(crate) async fn bitmap_slow(
  storage: &StorageSession<'_, impl Device>,
  cmd: RespCommand,
  parse_state: &[&[u8]],
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  use RespCommand as C;

  use crate::resp::bitmap::bitmap_commands::{
    bitfield_write_need, parse_bitfield_args, parse_bitfield_ro_args,
  };
  match cmd {
    C::Setbit | C::Getbit => {
      let (cmd_name, with_bit) = if cmd == C::Setbit {
        ("SETBIT", true)
      } else {
        ("GETBIT", false)
      };
      let Some((key, offset, bit)) = parse_bit_args(cmd_name, parse_state, output, with_bit) else {
        return Ok(());
      };
      // SETBIT 写臂：冷读与写回全程持本键桶排他闩（GETBIT 纯读不取窗）。
      // 增长字节数单点 wbitmap::length_in_bytes（C# BitmapManager.Length；
      // parse_bit_args 已保证 offset 合法，None 不可达）
      if with_bit {
        let need = length_in_bytes(offset).unwrap() as usize;
        // 与快臂 network_string_set_bit 同一前置判据源（[`string_record_fits_page`]）：
        // 超窗 need 在取窗与整值冷读回之前即按快臂 generic 同帧形收口，杜绝持闩期
        // 至多 512MB 盲目零填充分配与 RecordTooLarge 经 ? 上抛的次级落点分叉
        if !string_record_fits_page(&storage.batch, key, need) {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(());
        }
        let window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
        // 位图写臂零入账（票 wnode-string-bitmap-found-notfound-accounting-
        // matrix：C# SETBIT 走 RMW_MainStore 零计数，与快臂 None 静默同账）
        let Some(old) = read_cold_quiet(storage, key, output).await? else {
          return Ok(());
        };
        // 增长补零到位（缺失键空旧值同形折叠，C# BitmapManager.Length 补零同构）
        let mut val = old.unwrap_or_default();
        if val.len() < need {
          val.resize(need, 0);
        }
        // 单点位写单源（libs/server/Resp/Bitmap/BitmapManager.cs:UpdateBitmap）
        let old_bit = update_bitmap(&mut val, offset, bit);
        storage.rmw_string(&window, &val).await.map_err(|_| ())?;
        output.write_resp_int(i64::from(old_bit));
      } else {
        // 纯读臂（GETBIT）：闭包内按偏移直接取位（单点 wbitmap::get_bit，
        // 对标 BitmapManager.GetBit）；C# NOTFOUND → :0；零入账（C# 走
        // Read_MainStore 零计数，票 wnode-string-bitmap-found-notfound-
        // accounting-matrix）
        read_and_frame_quiet(
          storage,
          key,
          |val| get_bit(offset, val),
          output,
          |out, bit| out.write_resp_int(i64::from(bit)),
          |out| out.write_resp_int(0),
        )
        .await?;
      }
      Ok(())
    }
    C::Bitcount => {
      let Some((key, start, end, offset_type)) = parse_bit_count_args(parse_state, output) else {
        return Ok(());
      };
      // C# NOTFOUND → :0；零入账（C# 走 Read_MainStore 零计数，票
      // wnode-string-bitmap-found-notfound-accounting-matrix）
      read_and_frame_quiet(
        storage,
        key,
        |val| bit_count_driver(start, end, offset_type, val, val.len() as i64),
        output,
        |out, total| out.write_resp_int(total),
        |out| out.write_resp_int(0),
      )
      .await?;
      Ok(())
    }
    C::Bitpos => {
      let Some(args) = parse_bit_pos_args(parse_state, output) else {
        return Ok(());
      };
      // 区间越界直接 -1（快路径同判，单点 try_validate_bit_pos_offsets）
      if try_validate_bit_pos_offsets(
        args.start_offset,
        args.end_offset,
        args.offset_type,
        args.has_start_offset,
        args.has_end_offset,
      ) {
        output.write_resp_int(-1);
        return Ok(());
      }
      let search_for = args.search_for;
      // 零入账（C# 走 Read_MainStore 零计数，票 wnode-string-bitmap-found-
      // notfound-accounting-matrix）
      read_and_frame_quiet(
        storage,
        args.key,
        |val| {
          bit_pos_driver(
            val,
            val.len() as i64,
            args.start_offset,
            args.end_offset,
            search_for,
            args.offset_type,
          )
        },
        output,
        |out, pos| out.write_resp_int(pos),
        // C# NOTFOUND：找 0 回 0，找 1 回 -1
        |out| {
          out.extend_from_slice(if search_for == 0 {
            cs::RESP_RETURN_VAL_0
          } else {
            cs::RESP_RETURN_VAL_N1
          });
        },
      )
      .await?;
      Ok(())
    }
    C::BitopAnd | C::BitopOr | C::BitopXor | C::BitopNot | C::BitopDiff => {
      let bit_op = match cmd {
        C::BitopAnd => BitmapOperation::And,
        C::BitopOr => BitmapOperation::Or,
        C::BitopXor => BitmapOperation::Xor,
        C::BitopNot => BitmapOperation::Not,
        _ => BitmapOperation::Diff,
      };
      slow_bit_operation(storage, bit_op, parse_state, vector, output).await
    }
    C::Bitfield | C::BitfieldRo => {
      // has_write 对标 C# StringBitFieldAction 的 hasWriteCommands：BITFIELD_RO 恒
      // false；BITFIELD 依解析出的写子命令标志（SET / INCRBY 才有副作用）
      let (key, secondary_command_args, has_write) = if cmd == C::Bitfield {
        let Some((key, args, has_write)) = parse_bitfield_args(parse_state, output) else {
          return Ok(());
        };
        (key, args, has_write)
      } else {
        let Some((key, args)) = parse_bitfield_ro_args(parse_state, output) else {
          return Ok(());
        };
        (key, args, false)
      };
      if secondary_command_args.is_empty() {
        output.write_resp_array_len(0);
        return Ok(());
      }
      let resp_version = storage.resp_version;
      // 与快臂 string_bit_field_action 同一前置判据源（[`string_record_fits_page`]）：
      // 写子命令终值长上界超页即在取窗与整值冷读回之前按快臂 generic 同帧形收口，
      // 杜绝持闩期至多 512MB 盲目零填充分配与 RecordTooLarge 经 ? 上抛的次级落点
      // 分叉；纯读形态（全 GET 与 BITFIELD_RO）不取窗无增长义务，不入本门射程
      if has_write
        && !string_record_fits_page(
          &storage.batch,
          key,
          bitfield_write_need(&secondary_command_args),
        )
      {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(());
      }
      // 仅含写子命令时才持本键桶排他闩贯穿「冷读快照—逐子命令算新值—写回」全程
      //（与同步臂 string_bit_field_action 同窗，对标 C# LockType.Exclusive）；纯读
      //（全 GET 的 BITFIELD 及 BITFIELD_RO）走一次性快照读，不取排他闩（对标
      // C# LockType.Shared），消除并发只读互抢排他闩的串行化与误排队
      let window = if has_write {
        Some(storage.batch.rmw_window(key).await.map_err(|_| ())?)
      } else {
        None
      };
      // 零入账（C# BITFIELD/BITFIELD_RO 走 RMW_MainStore / Read_MainStore
      // 零计数，与快臂 None 静默同账，票 wnode-string-bitmap-found-notfound-
      // accounting-matrix）
      let Some(mut value) = read_cold_quiet(storage, key, output).await? else {
        return Ok(());
      };
      output.write_resp_array_len(secondary_command_args.len());
      let mut dirty = false;
      for args in &secondary_command_args {
        let is_get = args.secondary_command == BitFieldSecondaryCommand::Get;
        if is_get {
          match value.as_mut() {
            // NOTFOUND + GET → :0
            None => output.write_resp_int(0),
            Some(buf) => {
              // 执行核随命令形态二选一（对标 C# BITFIELD → BitFieldExecute、
              // BITFIELD_RO → BitFieldExecute_RO）
              let ans = if has_write {
                bit_field_execute(args, buf)
              } else {
                bit_field_execute_ro(args, buf).map(|v| (v, false))
              };
              match ans {
                Some((v, false)) => output.write_resp_int(v),
                // 只读 GET 不产生溢出
                _ => output.write_resp_null_ver(resp_version),
              }
            }
          }
        } else {
          // 写子命令：增长到位域所需长度后执行
          let need = new_block_alloc_length_from_type(args, 0) as usize;
          let buf = value.get_or_insert_with(|| Vec::with_capacity(need));
          if buf.len() < need {
            buf.resize(need, 0);
          }
          match bit_field_execute(args, buf) {
            Some((v, false)) => output.write_resp_int(v),
            Some((_, true)) => output.write_resp_null_ver(resp_version),
            None => output.write_resp_error(RESP_ERR_GENERIC),
          }
          dirty = true;
        }
      }
      // RMW 语义写回（保留既有 key 级 TTL，有写子命令时统一一次）
      if let Some(buf) = dirty.then_some(value).flatten() {
        match window {
          Some(window) => storage.rmw_string(&window, &buf).await.map_err(|_| ())?,
          // dirty 只在写子命令下置位，写子命令只属 BITFIELD 臂，缺窗即接线缺陷，
          // 宁回错误帧也不走无窗盲写
          None => output.write_resp_error(RESP_ERR_GENERIC),
        }
      }
      Ok(())
    }
    _ => {
      // 分派表漏接线信号：本臂只应承接位图族，其余落此即缺陷
      cs::write_error_raw(output, cs::RESP_ERR_ASYNC_REQUIRED);
      Ok(())
    }
  }
}

/// NetworkStringBitOperation 的异步对偶（BITOP AND/OR/XOR/NOT/DIFF；
/// 逐源异步读折叠 + dest 写共同体，语义对齐快路径
/// `network_string_bit_operation`：源键命中向量登记或对象域即整体
/// WRONGTYPE 零写，dest 命中登记在有源命中时预清退再写）
async fn slow_bit_operation(
  storage: &StorageSession<'_, impl Device>,
  bit_op: BitmapOperation,
  parse_state: &[&[u8]],
  vector: Option<&VectorManager>,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  use wresp::cmd_strings::{
    RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED, RESP_ERR_BITOP_KEY_LIMIT,
    RESP_ERR_BITOP_NOT_SINGLE_SOURCE_KEY, RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS,
  };
  let count = parse_state.len();
  // 参数过少（parse_state = [destkey, srckey...]）
  if count < 2 {
    abort_with_error_message(output, RESP_ERR_WRONG_NUMBER_OF_ARGUMENTS);
    return Ok(());
  }
  // DIFF 至少两个源
  if bit_op == BitmapOperation::Diff && count < 3 {
    abort_with_error_message(output, RESP_ERR_BITOP_DIFF_TWO_SOURCE_KEYS_REQUIRED);
    return Ok(());
  }
  // NOT 为一元：恰一个源键
  if bit_op == BitmapOperation::Not && count > 2 {
    abort_with_error_message(output, RESP_ERR_BITOP_NOT_SINGLE_SOURCE_KEY);
    return Ok(());
  }
  // 源键上限（含 destkey 共 [`BITOP_KEYS_MAX`]）
  if count > BITOP_KEYS_MAX {
    abort_with_error_message(output, RESP_ERR_BITOP_KEY_LIMIT);
    return Ok(());
  }
  let dest_key = parse_state[0];

  // dest 读改写窗口先于逐源折叠建立、跨「折叠读 → 求值 → 落笔」全程持有（与
  // 快路径 network_string_bit_operation 同一窗口契约，对标 C# BitmapOps.cs
  // StringBitOperation keys[0] Exclusive 自任何读前即建立罩至 dest SET 全程）：
  // 慢路径折叠逐源 await 为结构化让出面，dest∈srcs 自指形的窗内自读须与并发
  // 持窗写者互斥，否则旧折叠视图尾段盲写顶掉窗内已提交写（非可串行化）
  let _window = storage.batch.rmw_window(dest_key).await.map_err(|_| ())?;

  // 逐源回调折叠（快路径同步段命中即弃切片，与快路径 network_string_bit_operation
  // 同构零克隆）：会话前缀循环外单次外提交带前缀读口，逐源零前缀重算；缺失键
  // 跳过（C# NOTFOUND continue）；源键命中向量登记或对象域（信封 / Meta 升阶）
  // 即整体 WRONGTYPE 短路（dest 尚未写入，无副作用）
  let prefix = storage.batch.session_prefix();
  let mut acc = BitOpAccumulator::new(bit_op);
  for src_key in &parse_state[1..] {
    if vector.is_some_and(|vm| vm.read_stored_index(prefix.as_slice(), src_key).is_some()) {
      output.write_resp_error(RESP_ERR_WRONG_TYPE);
      return Ok(());
    }
    match storage
      .read_user_with_prefix(prefix.as_slice(), src_key, |v| acc.fold(v))
      .await
      .map_err(|_| ())?
    {
      UserReadAsync::Hit(()) | UserReadAsync::Missing => {}
      UserReadAsync::WrongType => {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        return Ok(());
      }
    }
  }

  let result = match acc.finish() {
    Ok(dst) => {
      // C# maxBitmapLen > 0 才 SET；全缺失/全空源回 0 不写（dest 为存活
      // 向量键时同臂保留登记）
      let longest = dst.len();
      if longest > 0 {
        // dest 写共同体前置 RI 门（异步对偶：存活 RangeIndex 上拒写）+
        // 登记预清退（C# dest DELETE+SET 重试臂同终态）；静默口——dest
        // 非计数对象（C# ReadWithUnsafeContext 仅逐源计数，BitmapOps.cs:
        // 44），簿记档 gate 读虚增 1 帧会使本臂逐源 N 帧变 N+1、与快臂
        // 逐源补账失联（票 wnode-string-bitmap-found-notfound-accounting-
        // matrix）
        if storage
          .ri_write_gate_quiet(dest_key)
          .await
          .map_err(|_| ())?
        {
          output.write_resp_error(RESP_ERR_WRONG_TYPE);
          return Ok(());
        }
        clear_vector_registry(storage, vector, dest_key).await;
        // dest 落笔续用折叠前已建立的本键读改写窗口（`_window` 在手至本函数
        // 返回），盲写与折叠读同闩域互斥（票 zcode-r32-rmwmatrix 立项一 + 本票
        // dest 窗罩折叠读算全程，对位 C# BitmapOps.cs dest SET 记录闩内落笔）
        storage
          .upsert_string(dest_key, &dst)
          .await
          .map_err(|_| ())?;
        longest as i64
      } else {
        0
      }
    }
    // C# GarnetException（源被吞并后 DIFF 单源）→ 通用错误应答
    Err(_) => {
      output.write_resp_error(RESP_ERR_GENERIC);
      return Ok(());
    }
  };
  output.write_resp_int(result);
  Ok(())
}
