//! 批量操作慢路径执行臂（exec_slow 冷键分派；独立模块保持对同步段零侵入）
//!
//! 对标 libs/server/Resp/ArrayCommands.cs 的 NetworkMGET/NetworkMSET/
//! NetworkDEL 在 Tsavorite pending 读 / 环形页翻转后 CompletePending 重放
//! 的异步形态。`Err(())` 为存储 IO 失败，由 exec_slow 统一应答
//! RESP_ERR_SLOW_PATH_STORAGE
//!
//! MSET 慢臂单列 [`super::mset_slow`]，经本模块路径别名保持
//! `slow::mset` 外部调用点不变。

use wdev::Device;
use wresp::{
  check_args::unpack_args_rest,
  cmd_strings::{RESP_ERR_WRONG_TYPE, write_error_raw},
  ext::{RespVecExt, is_resp3},
  resp_memory_writer::{Resp2, Resp3, RespProtocol, RespWriter},
};
use wval::KeyTag;

pub(crate) use super::mset_slow::mset;
use super::{
  envelope_object_type_name,
  lcs::{parse_lcs_options, write_lcs_output},
};
use crate::{
  resp::vector::vector_manager::VectorManager,
  storage::session::{
    common::{UserReadAsync, ttl_sync::meta_collection_type_of},
    storage_session::StorageSession,
  },
};

/// DEL / UNLINK 慢路径执行臂
///
/// C# 无 arity 校验（0 参即空循环回 :0），1:1 保留。`delete_string` 内部
/// `try_delete_sync` 与快路径共用同一 wkv 用户键删除单点，向量集清退随
/// 缺席观测钩子收口，快慢两臂计数口径一致（对标 C# 主存 DELETE 单回调）。
/// `initial_count` 继承快路径降级前已物理删除的键计数（SlowWait 快照尾参带入），
/// 快路径已删键在重放中得 false（计数 +0），未删键/降级键在慢路径正常删除计数，
/// 确保整命令应答准确等于实际删除键总数
pub(crate) async fn del(
  storage: &StorageSession<'_, impl Device>,
  refs: &[&[u8]],
  initial_count: i64,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let mut deleted_count = initial_count;
  for key in refs {
    // 逐键短窗（快路径 network_del 同一窗口契约：对标 C# InternalDelete.cs:60
    // 逐键记录闩；本臂让核等闩，预算耗尽按本臂存储错误应答）
    let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
    let deleted = storage.delete_string(key).await.map_err(|_| ())?;
    deleted_count += i64::from(deleted);
  }
  output.write_resp_int(deleted_count);
  Ok(())
}

/// MGET 慢路径执行臂
///
/// String 域读：命中写值；缺失写 nil（Redis MGET 对非字符串键同答 nil，
/// 不报错；对位 C# 主存单域读）；TTL 过期键经异步读惰性清除后视同缺失
pub(crate) async fn mget(
  storage: &StorageSession<'_, impl Device>,
  refs: &[&[u8]],
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  if is_resp3(resp_version) {
    let mut writer = RespWriter::<_, Resp3>::new_ref_p(output);
    mget_inner(storage, refs, &mut writer).await?;
  } else {
    let mut writer = RespWriter::<_, Resp2>::new_ref(output);
    mget_inner(storage, refs, &mut writer).await?;
  }
  Ok(())
}

/// 数组头先行、批量流式补元素（对标 C# NetworkMGET 先 `TryWriteArrayLength`
/// 再 `ReadWithPrefetch` + `CompletePending` 顺序补写应答）
///
/// 批量读口以 `Err` 中止时，本臂写入的数组头与首个磁盘候选之前已先行交付的
/// 元素一并回滚（wkv `session/raw/batch.rs:read_batch_raw_with` 次序契约：部
/// 分结果不可回滚、调用方须整体丢弃），错误交由 `exec_slow_impl` 的
/// `C::Mget` 臂在干净的 output 上落单条完整错误帧。
/// 回滚形态与快路径 `do_network_mget` 的 `Deferred` 出口同一机制
///（`writer.len()` 记点 + `buf_mut().truncate`）。
async fn mget_inner<P: RespProtocol>(
  storage: &StorageSession<'_, impl Device>,
  refs: &[&[u8]],
  writer: &mut RespWriter<&mut Vec<u8>, P>,
) -> Result<(), ()> {
  let start_len = writer.len();
  writer.write_array_length(refs.len());
  if storage
    .read_string_batch_into(refs, writer.buf_mut())
    .await
    .is_err()
  {
    writer.buf_mut().truncate(start_len);
    return Err(());
  }
  Ok(())
}

/// TYPE 慢路径执行臂
///
/// C# NetworkTYPE → storageApi.TYPE → Read_UnifiedStore：单次读 pending 就地
/// CompletePendingForUnifiedStoreSession 闭环，终态恒类型名或 none。rust 慢
/// 路径对偶：磁盘候选在异步读内闭环（无降级态），三域判型与应答形态同快路径
/// [`crate::resp::RespServerSession::network_type`] 逐字节一致（vector 登记特
/// 判前置，String / 升阶 Meta / 对象信封三域，内层标签缺省兜底 none）
pub(crate) async fn type_cmd(
  storage: &StorageSession<'_, impl Device>,
  vector: Option<&VectorManager>,
  refs: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let Some([key]) = <[&[u8]; 1]>::try_from(refs).ok() else {
    // 可达性防御：分派前快路径 unpack_args 已校验 arity，降级快照不失真
    return Err(());
  };
  // vector 登记特判前置（登记表纯内存，无降级面，与快路径同单点）
  let prefix = storage.batch.session_prefix();
  if vector.is_some_and(|vm| vm.read_stored_index(prefix.as_slice(), key).is_some()) {
    output.write_resp_simple_string("vectorset");
    return Ok(());
  }
  // 三域异步判型（String 域命中 → string；升阶键 Meta 元记录带集合类型直读
  // 类型名；信封域按内层标签映射含扩展注册名；三域皆缺 / 已过期 → none）
  match storage.read_user(key, |_| ()).await {
    Ok(UserReadAsync::Hit(())) => output.write_resp_simple_string("string"),
    Ok(UserReadAsync::Missing) => output.write_resp_simple_string("none"),
    Ok(UserReadAsync::WrongType) => {
      match storage
        .read_tag_with(key, KeyTag::Meta, meta_collection_type_of)
        .await
      {
        Ok(Some(Some(obj_type))) => output.write_resp_simple_string(obj_type.as_str()),
        // Meta 域缺失/死记录：落信封域读（对象信封键口径，含扩展注册名）；
        // 信封命中已由双探确认，内层标签缺省兜底 none
        Ok(_) => match storage
          .read_tag_with(key, KeyTag::ObjectEnvelope, |raw| {
            raw.first().copied().and_then(envelope_object_type_name)
          })
          .await
        {
          Ok(Some(Some(name))) => output.write_resp_simple_string(name),
          Ok(_) => output.write_resp_simple_string("none"),
          Err(_) => return Err(()),
        },
        Err(_) => return Err(()),
      }
    }
    Err(_) => return Err(()),
  }
  Ok(())
}

/// LCS 慢路径执行臂
///
/// C# NetworkLCS → storageApi.LCS → MainStoreOps.LCS/LCSInternal：双键单次读
/// pending 就地闭环，终态恒结果帧或 WRONGTYPE。rust 慢路径对偶：双键异步读
/// 后复用既有纯函数 [`StorageSession::compute_lcs_length`] /
/// [`StorageSession::compute_lcs_with_indices`] / [`StorageSession::compute_lcs`]
/// （零新机制），选项解析与快路径同一单源 [`parse_lcs_options`]，应答同快路径
/// [`crate::resp::RespServerSession::network_lcs`] 逐字节一致（含 WRONGTYPE /
/// 缺键空帧 / LEN / IDX 形态）。IO 失败与缺键严禁合流——合流即伪应答空 LCS
pub(crate) async fn lcs<D: Device>(
  storage: &StorageSession<'_, D>,
  refs: &[&[u8]],
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let Some(([key1, key2], rest)) = unpack_args_rest(refs, output, "LCS") else {
    return Ok(());
  };
  let opts = match parse_lcs_options(rest) {
    Ok(opts) => opts,
    Err(err) => {
      write_error_raw(output, err);
      return Ok(());
    }
  };
  // 双键异步读：String 域命中即值 / 确证缺键（两域皆缺或已过期，TTL 过期键经
  // 异步读惰性清除后视同缺失）/ 对象键 → WRONGTYPE / 存储 IO 失败（与 exec_slow
  // RESP_ERR_SLOW_PATH_STORAGE 同一口径）
  let mut vals = [None, None];
  for (slot, key) in vals.iter_mut().zip([key1, key2]) {
    match storage.read_user(key, |v| v.to_vec()).await {
      Ok(UserReadAsync::Hit(v)) => *slot = Some(v),
      Ok(UserReadAsync::Missing) => {}
      Ok(UserReadAsync::WrongType) => {
        write_error_raw(output, RESP_ERR_WRONG_TYPE);
        return Ok(());
      }
      Err(_) => return Err(()),
    }
  }

  write_lcs_output::<D>(
    vals[0].as_deref(),
    vals[1].as_deref(),
    &opts,
    is_resp3(resp_version),
    output,
  );
  Ok(())
}
