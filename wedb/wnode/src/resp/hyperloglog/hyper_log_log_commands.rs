//! HyperLogLog RESP 命令（对标 libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs
//! 与 libs/server/Storage/Session/MainStore/HyperLogLogOps.cs 的 RMW 语义）
//!
//! PFADD/PFCOUNT/PFMERGE 直读本模块 [`HyperLogLog`] 的稀疏/稠密编码；
//! 载荷按稀疏实际占用截断存储，稠密恒为 12304 字节。

use wresp::cmd_strings as cs;

use crate::{
  hyperloglog::hyper_log_log::{HyperLogLog, SPARSE_SIZE_MAX_CAP},
  resp::resp_server_session::RespServerSession,
};

/// WRONGTYPE 错误帧（对标 CmdStrings.RESP_ERR_WRONG_TYPE_HLL）
const RESP_ERR_WRONG_TYPE_HLL: &[u8] =
  b"-WRONGTYPE Key is not a valid HyperLogLog string value.\r\n";

/// 从存储装载 HLL 载荷并校验；缺失返回 Ok(None)，非法类型返回 Err(())
fn load_hll<'s>(
  hll: &HyperLogLog,
  store: &wkv::BatchStoreSession<'s, impl wdev::Device>,
  key: &[u8],
) -> Result<Option<Vec<u8>>, ()> {
  match store.try_read_sync(key, |v| v.to_vec()) {
    Ok(Some(Some(raw))) => {
      if hll.is_valid_hyll_len(&raw, raw.len()) {
        Ok(Some(raw))
      } else {
        Err(())
      }
    }
    Ok(Some(None)) | Ok(None) => Ok(None),
    Err(_) => Err(()),
  }
}

/// 稀疏载荷回写（保底下限长度）
fn store_hll(
  hll: &HyperLogLog,
  store: &wkv::BatchStoreSession<impl wdev::Device>,
  key: &[u8],
  blob: &[u8],
) {
  // 长度不得低于初始分配（274B，C# IsValidHLLLength 的下界校验），不足处零填充
  let len = if hll.is_sparse(blob) {
    hll
      .sparse_current_size_in_bytes(blob)
      .max(hll.sparse_bytes())
  } else {
    hll.dense_bytes()
  };
  if let Err(err) = store.try_upsert_sync(key, &blob[..len]) {
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
    if parse_state.is_empty() {
      cs::abort_with_wrong_number_of_arguments(output, "PFADD");
      return Ok(true);
    }

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
    if parse_state.is_empty() {
      cs::abort_with_wrong_number_of_arguments(output, "PFCOUNT");
      return Ok(true);
    }

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
    let mut buf = itoa::Buffer::new();
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
    if parse_state.len() < 2 {
      cs::abort_with_wrong_number_of_arguments(output, "PFMERGE");
      return Ok(true);
    }

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
        let mut blob = vec![0_u8; SPARSE_SIZE_MAX_CAP];
        hll.init_sparse(&mut blob);
        blob[..hll.sparse_bytes()].to_vec()
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
