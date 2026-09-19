//! HyperLogLog RESP 命令（对标 libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs
//! 与 libs/server/Storage/Session/MainStore/HyperLogLogOps.cs 的 RMW 语义）
//!
//! PFADD/PFCOUNT/PFMERGE 直读本模块 [`HyperLogLog`] 的稀疏/稠密编码；
//! 载荷按稀疏实际占用截断存储，稠密恒为 12304 字节。快路径读原语遇磁盘
//! 候选（冷数据）返回 [`HllLoad::Degrade`]，命令臂 `Ok(false)` 转慢路径
//! 异步装载裁决（[`slow_hll_add`] 等）——Tsavorite RMW 挂起 pending 磁盘
//! 读后重放、NOTFOUND 才允许新建的同栈语义，冷区基数绝不被盲插覆盖。
//! 写回全链 RMW 语义（快路径 try_rmw_sync / 慢路径 rmw_string，缺失键
//! 新建、既有键保留 key 级 TTL），对标 C# CopyUpdater 的 PFADD/PFMERGE
//! 分支 TryCopyOptionals 保留 Expiration，杜绝快慢两路漂移。

use whyperlog::HyperLogLog;
use wresp::{
  check_args::check_arg_count,
  cmd_strings::{
    RESP_ERR_WRONG_TYPE_HLL, RESP_OK, RESP_RETURN_VAL_0, RESP_RETURN_VAL_1, write_error_raw,
  },
  ext::RespVecExt,
};

use crate::{
  resp::resp_server_session::RespServerSession, storage::session::storage_session::StorageSession,
};

/// HLL 键同步装载三态（消解 wkv 读面 `Ok(None)` 的两义：磁盘候选须降级 ≠ 键缺失）
enum HllLoad {
  /// String 域命中且载荷长度合法
  Present(Vec<u8>),
  /// 双域皆缺：键不存在
  Missing,
  /// String 域磁盘候选 / TTL 待裁决：命令臂转 `Ok(false)` 慢路径异步装载
  Degrade,
}

/// 慢路径异步装载三态（磁盘候选经 `read_tag_with` 闭环后消除降级态）
enum HllCold {
  Present(Vec<u8>),
  Missing,
  /// 集合对象键或载荷非法（C# output 0xFF → HyperLogLogWrongType）
  WrongType,
}

/// 从存储装载 HLL 载荷并校验（同步快路径）
///
/// 双域判定：String 域未命中时反探对象信封域——命中即集合对象键，杜绝
/// HLL 写入覆盖对象键。`Err(())` 为对象键 / 载荷非法 / 存储错误（快路径
/// 原口径统一答 WRONGTYPE_HLL）；[`HllLoad::Degrade`] 绝不视同缺失
fn load_hll<'s>(
  hll: &HyperLogLog,
  store: &wkv::BatchStoreSession<'s, impl wdev::Device>,
  key: &[u8],
) -> Result<HllLoad, ()> {
  use crate::storage::session::common::{UserRead, read_user_sync};
  match read_user_sync(store, key, |v| v.to_vec()) {
    Ok(UserRead::Hit(raw)) if hll.is_valid_hyll_len(&raw, raw.len()) => Ok(HllLoad::Present(raw)),
    // 信封域命中：对象键；载荷长度非法：C# output 0xFF → WRONGTYPE
    Ok(UserRead::Hit(_)) | Ok(UserRead::WrongType) => Err(()),
    Ok(UserRead::Missing) => Ok(HllLoad::Missing),
    // String 域磁盘候选 / TTL 待裁决：转慢路径异步装载裁决（视同缺失会
    // 令 PFADD 盲插覆盖冷区历史基数——HyperLogLogOps 的 HyperLogLogAdd
    // 在磁盘候选时挂起 pending 读，NOTFOUND 才允许新建）
    Ok(UserRead::Deferred) => Ok(HllLoad::Degrade),
    Err(_) => Err(()),
  }
}

/// 载荷按稀疏实际占用截断（快/慢路径写面前共用口径）
fn hll_truncate<'a>(hll: &HyperLogLog, blob: &'a [u8]) -> &'a [u8] {
  let len = if hll.is_sparse(blob) {
    hll
      .sparse_current_size_in_bytes(blob)
      .max(hll.sparse_bytes())
  } else {
    hll.dense_bytes()
  };
  &blob[..len.min(blob.len())]
}

/// 将 HLL 载荷截断至实际编码长度并回写（同步快路径）
///
/// RMW 语义写回（保留既有 key 级 TTL，PFADD/PFMERGE 共用；对标 C#
/// GetRMWModifiedFieldInfo 的 HasExpiration 保留）
fn store_hll<'s>(
  hll: &HyperLogLog,
  store: &wkv::BatchStoreSession<'s, impl wdev::Device>,
  key: &[u8],
  blob: &[u8],
) {
  if let Err(err) = store.try_rmw_sync(key, hll_truncate(hll, blob)) {
    log::error!("HLL try_rmw_sync failed: {err:?}");
  }
}

/// 新键初始载荷：按元素数推算初始长度（超上限即稠密），init 内部按长度分派编码
fn hll_init_payload(hll: &HyperLogLog, elements: &[&[u8]]) -> Vec<u8> {
  let initial = hll.sparse_initial_length(elements.len());
  let mut blob = vec![0_u8; initial];
  hll.init(elements, &mut blob);
  blob
}

/// 目标键缺失的稀疏初始化载荷（PFMERGE dest 专用）
///
/// 稀疏初始化只写头部 + 零段（前 146B），直接按初始长度分配，
/// 免 SPARSE_SIZE_MAX_CAP（4KB）全量清零与二次拷贝
fn hll_sparse_seed(hll: &HyperLogLog) -> Vec<u8> {
  let mut blob = vec![0_u8; hll.sparse_bytes()];
  hll.init_sparse(&mut blob);
  blob
}

/// PFADD 元素并入载荷内核（快/慢路径共用）：有变更返回 Some(新载荷)
///
/// 稠密原位更新；稀疏可原位更新则原位，否则经 update_grow 扩容/稠密化
fn hll_add_payload(hll: &HyperLogLog, mut blob: Vec<u8>, elements: &[&[u8]]) -> Option<Vec<u8>> {
  let mut updated = false;
  if hll.is_dense(&blob) {
    hll.update(elements, &mut blob, &mut updated);
  } else if !hll.update(elements, &mut blob, &mut updated) {
    let new_len = hll.update_grow(elements.len(), &blob);
    let mut grown = vec![0_u8; new_len];
    hll.copy_update(elements, &blob, &mut grown);
    return Some(grown);
  }
  updated.then_some(blob)
}

/// 源并入目标载荷内核（快/慢路径共用）：多源择大，容量不足按 MergeGrow 迁移
fn hll_merge_payload(hll: &HyperLogLog, mut dst: Vec<u8>, src: &[u8]) -> Vec<u8> {
  let new_len = hll.merge_grow(src, &dst);
  if new_len != dst.len() {
    let mut grown = vec![0_u8; new_len];
    let old_len = dst.len();
    hll.copy_update_merge(src, &dst, &mut grown, old_len, new_len);
    grown
  } else {
    hll.merge(src, &mut dst);
    hll.set_card(&mut dst, i64::MIN);
    dst
  }
}

/// 载荷稠密化视图（稀疏临时稠密化；PFCOUNT 虚拟并集快/慢路径共用）
fn hll_dense_view(hll: &HyperLogLog, raw: &[u8]) -> Vec<u8> {
  let mut view = vec![0_u8; hll.dense_bytes()];
  if hll.is_sparse(raw) {
    hll.init_dense(&mut view);
    hll.sparse_to_dense(raw, &mut view);
  } else {
    view.copy_from_slice(&raw[..hll.dense_bytes()]);
  }
  view
}

/// 慢路径双域异步装载（[`load_hll`] 的磁盘候选闭环对位）
///
/// `read_tag_with` 内存未命中时异步装载磁盘冷区（含惰性过期物理清除）后
/// 裁决，对标 C# RMW 的 CompletePending 重放；String 域未命中探对象信封域
/// （双域次序同 [`load_hll`]）。`Err(())` 为存储错误
async fn load_hll_cold(
  hll: &HyperLogLog,
  storage: &StorageSession<'_, impl wdev::Device>,
  key: &[u8],
) -> Result<HllCold, ()> {
  use wval::KeyTag;
  Ok(
    match storage
      .read_tag_with(key, KeyTag::String, |v| v.to_vec())
      .await
    {
      Ok(Some(raw)) if hll.is_valid_hyll_len(&raw, raw.len()) => HllCold::Present(raw),
      // 载荷长度非法：C# output 0xFF → WRONGTYPE
      Ok(Some(_)) => HllCold::WrongType,
      // String 域确认缺失：探对象信封域
      Ok(None) => match storage
        .read_tag_with(key, KeyTag::ObjectEnvelope, |_| ())
        .await
      {
        Ok(Some(())) => HllCold::WrongType,
        Ok(None) => HllCold::Missing,
        Err(_) => return Err(()),
      },
      Err(_) => return Err(()),
    },
  )
}

/// WRONGTYPE 应答帧写出（慢路径）
fn reply_wrong_type_hll(output: &mut Vec<u8>) {
  write_error_raw(output, RESP_ERR_WRONG_TYPE_HLL);
}

/// PFADD 慢路径执行臂（exec_slow 分派；`Err(())` 为存储错误，调用方统一应答）
///
/// libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:HyperLogLogAdd
///（RMW 语义承接：异步装载磁盘冷区后裁决，缺失才新建）
pub(crate) async fn slow_hll_add(
  storage: &StorageSession<'_, impl wdev::Device>,
  parse_state: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let hll = HyperLogLog::new();
  let key = parse_state.first().copied().unwrap_or(&[]);
  let elements = parse_state.get(1..).unwrap_or(&[]);

  // 零元素：C# HyperLogLogAdd 元素循环零次 pfaddUpdated==0，不触达存储
  // 不建键（含 WRONGTYPE 探测），直答 :0
  if elements.is_empty() {
    output.extend_from_slice(RESP_RETURN_VAL_0);
    return Ok(());
  }

  let updated = match load_hll_cold(&hll, storage, key).await? {
    HllCold::Missing => {
      let blob = hll_init_payload(&hll, elements);
      // RMW 新建（InitialUpdater：新记录无 Expiration）；装载已探对象信封域
      // 拦截对象键，无需 SET 语义清退
      storage
        .rmw_string(key, hll_truncate(&hll, &blob))
        .await
        .map_err(|_| ())?;
      true
    }
    HllCold::Present(raw) => match hll_add_payload(&hll, raw, elements) {
      Some(blob) => {
        // RMW 写回保留既有 key 级 TTL（对标 RMWMethods.cs:CopyUpdater 的
        // PFADD 分支 TryCopyOptionals 保留 Expiration；SET 语义会误清）
        storage
          .rmw_string(key, hll_truncate(&hll, &blob))
          .await
          .map_err(|_| ())?;
        true
      }
      // 寄存器无变更不写（C# pfaddUpdated == 0 同口径）
      None => false,
    },
    HllCold::WrongType => {
      reply_wrong_type_hll(output);
      return Ok(());
    }
  };

  output.extend_from_slice(if updated {
    RESP_RETURN_VAL_1
  } else {
    RESP_RETURN_VAL_0
  });
  Ok(())
}

/// PFCOUNT 慢路径执行臂：逐键异步装载后虚拟并集计数（不改写存储）
///
/// libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:HyperLogLogLength
pub(crate) async fn slow_hll_count(
  storage: &StorageSession<'_, impl wdev::Device>,
  parse_state: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let hll = HyperLogLog::new();

  // 单键短路：直接对原载荷估算，免 12KB 临时稠密缓冲与全量展开拷贝
  if parse_state.len() == 1 {
    let key = parse_state[0];
    let card = match load_hll_cold(&hll, storage, key).await? {
      HllCold::Missing => 0,
      HllCold::WrongType => {
        reply_wrong_type_hll(output);
        return Ok(());
      }
      HllCold::Present(mut raw) => hll.count(&mut raw),
    };
    output.write_resp_int(card as i64);
    return Ok(());
  }

  let mut dense: Option<Vec<u8>> = None;
  for key in parse_state {
    match load_hll_cold(&hll, storage, key).await? {
      HllCold::Missing => {}
      HllCold::WrongType => {
        reply_wrong_type_hll(output);
        return Ok(());
      }
      HllCold::Present(raw) => match &mut dense {
        None => dense = Some(hll_dense_view(&hll, &raw)),
        Some(dst) => {
          let view = hll_dense_view(&hll, &raw);
          hll.dense_to_dense(&view, dst);
        }
      },
    }
  }

  let card = dense.as_mut().map(|d| hll.count(d)).unwrap_or(0);
  output.write_resp_int(card);
  Ok(())
}

/// PFMERGE 慢路径执行臂：逐源异步装载择大并入目标后回写 +OK
///
/// libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:HyperLogLogMerge
pub(crate) async fn slow_hll_merge(
  storage: &StorageSession<'_, impl wdev::Device>,
  parse_state: &[&[u8]],
  output: &mut Vec<u8>,
) -> Result<(), ()> {
  let hll = HyperLogLog::new();
  let dest = parse_state.first().copied().unwrap_or(&[]);
  let sources = parse_state.get(1..).unwrap_or(&[]);

  // 零源：C# HyperLogLogMerge 源循环零次，不 GET 不 SET 不建键（dest 亦不
  // 探测 WRONGTYPE），直答 +OK
  if sources.is_empty() {
    output.extend_from_slice(RESP_OK);
    return Ok(());
  }

  let mut dst = match load_hll_cold(&hll, storage, dest).await? {
    HllCold::Present(raw) => raw,
    HllCold::Missing => hll_sparse_seed(&hll),
    HllCold::WrongType => {
      reply_wrong_type_hll(output);
      return Ok(());
    }
  };

  for src_key in sources {
    match load_hll_cold(&hll, storage, src_key).await? {
      HllCold::Missing => {}
      HllCold::WrongType => {
        reply_wrong_type_hll(output);
        return Ok(());
      }
      HllCold::Present(raw) => dst = hll_merge_payload(&hll, dst, &raw),
    }
  }

  // RMW 写回保留 dest 既有 key 级 TTL（对标 RMWMethods.cs:CopyUpdater 的
  // PFMERGE 分支 TryCopyOptionals 保留 Expiration；SET 语义会误清）
  storage
    .rmw_string(dest, hll_truncate(&hll, &dst))
    .await
    .map_err(|_| ())?;
  output.extend_from_slice(RESP_OK);
  Ok(())
}

impl RespServerSession {
  /// PFADD key [element ...]：登记元素，寄存器有变更回 :1 否则 :0
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs:HyperLogLogAdd
  pub fn hyper_log_log_add<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1.., output, "PFADD");

    // 零元素：C# HyperLogLogAdd 元素循环零次 pfaddUpdated==0，不触达存储
    // 不建键（含 WRONGTYPE 探测），直答 :0
    if parse_state.len() == 1 {
      output.extend_from_slice(RESP_RETURN_VAL_0);
      return Ok(true);
    }

    let key = parse_state[0];
    let elements = &parse_state[1..];
    let hll = HyperLogLog::new();

    let existing = match load_hll(&hll, store, key) {
      Ok(HllLoad::Present(raw)) => Some(raw),
      Ok(HllLoad::Missing) => None,
      // 磁盘候选：降级慢路径异步装载裁决，降级前不残留输出
      Ok(HllLoad::Degrade) => return Ok(false),
      Err(()) => {
        write_error_raw(output, RESP_ERR_WRONG_TYPE_HLL);
        return Ok(true);
      }
    };

    let mut updated = false;
    match existing {
      None => {
        let blob = hll_init_payload(&hll, elements);
        updated = true;
        store_hll(&hll, store, key, &blob);
      }
      Some(raw) => {
        if let Some(blob) = hll_add_payload(&hll, raw, elements) {
          updated = true;
          store_hll(&hll, store, key, &blob);
        }
      }
    }

    output.extend_from_slice(if updated {
      RESP_RETURN_VAL_1
    } else {
      RESP_RETURN_VAL_0
    });
    Ok(true)
  }

  /// PFCOUNT key [key ...]：单键直读基数；多键做虚拟并集（不改写存储）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs:HyperLogLogLength
  pub fn hyper_log_log_length<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, 1.., output, "PFCOUNT");

    let hll = HyperLogLog::new();

    // 单键短路：直接对原载荷估算，免 12KB 临时稠密缓冲与全量展开拷贝
    if parse_state.len() == 1 {
      let key = parse_state[0];
      let card = match load_hll(&hll, store, key) {
        Err(()) => {
          write_error_raw(output, RESP_ERR_WRONG_TYPE_HLL);
          return Ok(true);
        }
        Ok(HllLoad::Missing) => 0,
        Ok(HllLoad::Degrade) => return Ok(false),
        Ok(HllLoad::Present(mut raw)) => hll.count(&mut raw),
      };
      output.write_resp_int(card as i64);
      return Ok(true);
    }

    // 装载全部键的稠密视图（稀疏临时稠密化）；应答在裁决后统一写出，
    // 任一键磁盘候选即整体降级，无半成品应答残留
    let mut dense: Option<Vec<u8>> = None;
    for key in parse_state {
      match load_hll(&hll, store, key) {
        Err(()) => {
          write_error_raw(output, RESP_ERR_WRONG_TYPE_HLL);
          return Ok(true);
        }
        Ok(HllLoad::Missing) => continue,
        Ok(HllLoad::Degrade) => return Ok(false),
        Ok(HllLoad::Present(raw)) => {
          let view = hll_dense_view(&hll, &raw);
          match &mut dense {
            None => dense = Some(view),
            Some(dst) => {
              hll.dense_to_dense(&view, dst);
            }
          }
        }
      }
    }

    let card = dense.as_mut().map(|d| hll.count(d)).unwrap_or(0);
    output.write_resp_int(card);
    Ok(true)
  }

  /// PFMERGE dest [src ...]：多源择大并入目标，回 +OK（零源合法，dest 不触达）
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs:HyperLogLogMerge
  pub fn hyper_log_log_merge<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // C# HyperLogLogMerge 仅要求 Count>=1（存储层对 Count==0 才短路）
    check_arg_count!(parse_state, 1.., output, "PFMERGE");

    // 零源：C# 源循环零次，不 GET 不 SET 不建键（dest 亦不探测 WRONGTYPE），
    // 直答 +OK
    if parse_state.len() == 1 {
      output.extend_from_slice(RESP_OK);
      return Ok(true);
    }

    let dest = parse_state[0];
    let sources = &parse_state[1..];
    let hll = HyperLogLog::new();

    // 目标载荷（缺失按稀疏初始化）；+OK 在裁决后统一写出，dest 或任一
    // src 磁盘候选即整体降级，无半成品写面
    let mut dst = match load_hll(&hll, store, dest) {
      Err(()) => {
        write_error_raw(output, RESP_ERR_WRONG_TYPE_HLL);
        return Ok(true);
      }
      Ok(HllLoad::Present(raw)) => raw,
      Ok(HllLoad::Missing) => hll_sparse_seed(&hll),
      Ok(HllLoad::Degrade) => return Ok(false),
    };

    for src_key in sources {
      let src = match load_hll(&hll, store, src_key) {
        Err(()) => {
          write_error_raw(output, RESP_ERR_WRONG_TYPE_HLL);
          return Ok(true);
        }
        Ok(HllLoad::Present(raw)) => raw,
        Ok(HllLoad::Missing) => continue,
        Ok(HllLoad::Degrade) => return Ok(false),
      };
      dst = hll_merge_payload(&hll, dst, &src);
    }

    store_hll(&hll, store, dest, &dst);
    output.extend_from_slice(RESP_OK);
    Ok(true)
  }
}
