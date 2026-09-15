//! HyperLogLog RESP 命令（对标 libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs
//! 与 libs/server/Storage/Session/MainStore/HyperLogLogOps.cs 的 RMW 语义）
//!
//! PFADD/PFCOUNT/PFMERGE 直读本模块 [`HyperLogLog`] 的稀疏/稠密编码；
//! 载荷按稀疏实际占用截断存储，稠密恒为 12304 字节。

use itoa::Buffer;
use whyperlog::HyperLogLog;
use wresp::check_arg_count;

use crate::resp::resp_server_session::RespServerSession;

/// WRONGTYPE 错误帧（对标 CmdStrings.RESP_ERR_WRONG_TYPE_HLL）
const RESP_ERR_WRONG_TYPE_HLL: &[u8] =
  b"-WRONGTYPE Key is not a valid HyperLogLog string value.\r\n";

/// 从存储装载 HLL 载荷并校验；缺失返回 Ok(None)，非法类型返回 Err(())
///
/// 双域判定：String 域未命中时反探对象信封域——命中即集合对象键
/// （Err(()) → WRONGTYPE_HLL），杜绝 HLL 写入覆盖对象键
fn load_hll<'s>(
  hll: &HyperLogLog,
  store: &wkv::BatchStoreSession<'s, impl wdev::Device>,
  key: &[u8],
) -> Result<Option<Vec<u8>>, ()> {
  use crate::storage::session::common::ttl_sync::read_adjudicated_user_sync;
  match read_adjudicated_user_sync(store, key, |v| v.to_vec()) {
    Ok(Some(Some(Ok(raw)))) => {
      if hll.is_valid_hyll_len(&raw, raw.len()) {
        Ok(Some(raw))
      } else {
        Err(())
      }
    }
    // 信封域命中：对象键
    Ok(Some(Some(Err(())))) => Err(()),
    Ok(Some(None)) => Ok(None),
    // String 域磁盘候选 / TTL 待裁决（原口径视同缺失）
    Ok(None) => Ok(None),
    Err(_) => Err(()),
  }
}

/// 将 HLL 载荷截断至实际编码长度并回写
fn store_hll<'s>(
  hll: &HyperLogLog,
  store: &wkv::BatchStoreSession<'s, impl wdev::Device>,
  key: &[u8],
  blob: &[u8],
) {
  let len = if hll.is_sparse(blob) {
    hll
      .sparse_current_size_in_bytes(blob)
      .max(hll.sparse_bytes())
  } else {
    hll.dense_bytes()
  };
  if let Err(err) = store.try_upsert_sync(key, &blob[..len.min(blob.len())]) {
    log::error!("HLL try_upsert_sync failed: {err:?}");
  }
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
    check_arg_count!(parse_state, !empty, output, "PFADD");

    let key = parse_state[0];
    let elements = &parse_state[1..];
    let hll = HyperLogLog::new();

    let existing = match load_hll(&hll, store, key) {
      Ok(existing) => existing,
      Err(()) => {
        output.extend_from_slice(RESP_ERR_WRONG_TYPE_HLL);
        return Ok(true);
      }
    };

    let mut updated = false;
    match existing {
      None => {
        // 新键：按元素数推算初始长度（超上限即稠密），init 内部按长度分派编码
        let initial = hll.sparse_initial_length(elements.len());
        let mut blob = vec![0_u8; initial];
        hll.init(elements, &mut blob);
        updated = true;
        store_hll(&hll, store, key, &blob);
      }
      Some(raw) => {
        if hll.is_dense(&raw) {
          let mut blob = raw;
          hll.update(elements, &mut blob, &mut updated);
          if updated {
            store_hll(&hll, store, key, &blob);
          }
        } else {
          // 稀疏：可原位更新则原位，否则经 update_grow 扩容/稠密化
          let mut blob = raw;
          if !hll.update(elements, &mut blob, &mut updated) {
            let new_len = hll.update_grow(elements.len(), &blob);
            let mut grown = vec![0_u8; new_len];
            hll.copy_update(elements, &blob, &mut grown);
            updated = true;
            store_hll(&hll, store, key, &grown);
          } else if updated {
            store_hll(&hll, store, key, &blob);
          }
        }
      }
    }

    output.extend_from_slice(if updated { b":1\r\n" } else { b":0\r\n" });
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
    check_arg_count!(parse_state, !empty, output, "PFCOUNT");

    let hll = HyperLogLog::new();

    // 装载全部键的稠密视图（稀疏临时稠密化）
    let mut dense: Option<Vec<u8>> = None;
    for key in parse_state {
      match load_hll(&hll, store, key) {
        Err(()) => {
          output.extend_from_slice(RESP_ERR_WRONG_TYPE_HLL);
          return Ok(true);
        }
        Ok(None) => continue,
        Ok(Some(raw)) => {
          let mut view = vec![0_u8; hll.dense_bytes()];
          if hll.is_sparse(&raw) {
            hll.init_dense(&mut view);
            hll.sparse_to_dense(&raw, &mut view);
          } else {
            view.copy_from_slice(&raw[..hll.dense_bytes()]);
          }

          match &mut dense {
            None => dense = Some(view),
            Some(dst) => {
              hll.dense_to_dense(&view, dst);
            }
          }
        }
      }
    }

    let card = dense.map(|mut d| hll.count(&mut d)).unwrap_or(0);
    let mut buf = Buffer::new();
    output.push(b':');
    output.extend_from_slice(buf.format(card).as_bytes());
    output.extend_from_slice(b"\r\n");
    Ok(true)
  }

  /// PFMERGE dest src [src ...]：多源择大并入目标，回 +OK
  ///
  /// libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs:HyperLogLogMerge
  pub fn hyper_log_log_merge<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, >= 2, output, "PFMERGE");

    let dest = parse_state[0];
    let sources = &parse_state[1..];
    let hll = HyperLogLog::new();

    // 目标载荷（缺失按稀疏初始化）
    let mut dst = match load_hll(&hll, store, dest) {
      Err(()) => {
        output.extend_from_slice(RESP_ERR_WRONG_TYPE_HLL);
        return Ok(true);
      }
      Ok(Some(raw)) => raw,
      Ok(None) => {
        // 稀疏初始化只写头部 + 零段（前 146B），直接按初始长度分配，
        // 免 SPARSE_SIZE_MAX_CAP（4KB）全量清零与二次拷贝
        let mut blob = vec![0_u8; hll.sparse_bytes()];
        hll.init_sparse(&mut blob);
        blob
      }
    };

    for src_key in sources {
      let src = match load_hll(&hll, store, src_key) {
        Err(()) => {
          output.extend_from_slice(RESP_ERR_WRONG_TYPE_HLL);
          return Ok(true);
        }
        Ok(Some(raw)) => raw,
        Ok(None) => continue,
      };

      // 目标容量不足时按 MergeGrow 迁移（稀疏扩容或稠密化）
      let new_len = hll.merge_grow(&src, &dst);
      if new_len != dst.len() {
        let mut grown = vec![0_u8; new_len];
        let old_len = dst.len();
        hll.copy_update_merge(&src, &dst, &mut grown, old_len, new_len);
        dst = grown;
      } else {
        hll.merge(&src, &mut dst);
        hll.set_card(&mut dst, i64::MIN);
      }
    }

    store_hll(&hll, store, dest, &dst);
    output.extend_from_slice(b"+OK\r\n");
    Ok(true)
  }
}
