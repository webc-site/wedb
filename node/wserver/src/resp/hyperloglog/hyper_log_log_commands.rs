//! HyperLogLog RESP 命令（对标 libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs
//! 与 libs/server/Storage/Session/MainStore/HyperLogLogOps.cs 的 RMW 语义）
//!
//! PFADD/PFCOUNT/PFMERGE 直读本模块 [`HyperLogLog`] 的稀疏/稠密编码；
//! 载荷按稀疏实际占用截断存储，稠密恒为 12304 字节。

use crate::resp::{
  hyperloglog::hyper_log_log::{HyperLogLog, SPARSE_SIZE_MAX_CAP},
  resp_server_session::RespServerSession,
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
  let _ = store.try_upsert_sync(key, &blob[..len]);
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
    if parse_state.len() < 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'PFADD' command\r\n");
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
    if parse_state.len() < 1 {
      output.extend_from_slice(b"-ERR wrong number of arguments for 'PFCOUNT' command\r\n");
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
      output.extend_from_slice(b"-ERR wrong number of arguments for 'PFMERGE' command\r\n");
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

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use tempfile::{TempDir, tempdir};
  use wdev::SegmentedDevice;
  use wkv::{StoreConfig, WedbStore};

  use super::*;

  type TestSession = wkv::StoreSession<SegmentedDevice>;

  /// 临时文件库 + 批处理会话
  fn fixture(tag: &str) -> (TempDir, Arc<WedbStore<SegmentedDevice>>, TestSession) {
    let dir = tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
    let config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let session = store.new_session().unwrap();
    (dir, store, session)
  }

  #[test]
  fn pfadd_pfcount_pfmerge_flow() {
    let (_dir, _store, session) = fixture("hll.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession;
    let mut out = Vec::new();

    // PFADD 新键：寄存器变更 → :1
    sess
      .hyper_log_log_add(&[b"h1", b"a", b"b", b"c"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // 重复元素：无变更 → :0
    out.clear();
    sess
      .hyper_log_log_add(&[b"h1", b"a", b"b", b"c"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // 新增元素：变更 → :1
    out.clear();
    sess
      .hyper_log_log_add(&[b"h1", b"d"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // PFCOUNT 单键：精确基数 4
    out.clear();
    sess
      .hyper_log_log_length(&[b"h1"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":4\r\n");

    // 缺失键计数为 0
    out.clear();
    sess
      .hyper_log_log_length(&[b"missing"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":0\r\n");

    // PFMERGE h1 → h2
    out.clear();
    sess
      .hyper_log_log_merge(&[b"h2", b"h1"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    // PFCOUNT 多键虚拟并集 = 4
    out.clear();
    sess
      .hyper_log_log_length(&[b"h1", b"h2"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, b":4\r\n");

    // WRONGTYPE：非 HLL 载荷
    let _ = batch.try_upsert_sync(b"bad", b"plain-string-value");
    out.clear();
    sess
      .hyper_log_log_add(&[b"bad", b"x"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, RESP_ERR_WRONG_TYPE_HLL);
    out.clear();
    sess
      .hyper_log_log_length(&[b"bad"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, RESP_ERR_WRONG_TYPE_HLL);
    out.clear();
    sess
      .hyper_log_log_merge(&[b"dest", b"bad"], &batch, &mut out)
      .unwrap();
    assert_eq!(out, RESP_ERR_WRONG_TYPE_HLL);

    // 参数不足
    out.clear();
    sess.hyper_log_log_add(&[], &batch, &mut out).unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'PFADD' command\r\n"
    );
    out.clear();
    sess
      .hyper_log_log_merge(&[b"dest"], &batch, &mut out)
      .unwrap();
    assert_eq!(
      out,
      b"-ERR wrong number of arguments for 'PFMERGE' command\r\n"
    );
  }

  /// 大规模元素：稀疏稠密化后基数仍受控，PFMERGE 后计数不变
  #[test]
  fn dense_upgrade_and_merge_monotonic() {
    let (_dir, _store, session) = fixture("hll2.db");
    let batch = session.enter_batch();
    let mut sess = RespServerSession;

    let elements: Vec<Vec<u8>> = (0..1000)
      .map(|i| format!("elem-{i}").into_bytes())
      .collect();
    let args: Vec<&[u8]> = std::iter::once(b"big".as_slice())
      .chain(elements.iter().map(|e| e.as_slice()))
      .collect();

    let mut out = Vec::new();
    sess.hyper_log_log_add(&args, &batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    sess
      .hyper_log_log_length(&[b"big"], &batch, &mut out)
      .unwrap();
    let card: i64 = String::from_utf8_lossy(&out[1..out.len() - 2])
      .parse()
      .unwrap();
    // HLL 标准误差 ~1.1%
    assert!((880..=1120).contains(&card), "card = {card}");

    // merge 到空目标，计数不变
    out.clear();
    sess
      .hyper_log_log_merge(&[b"copy", b"big"], &batch, &mut out)
      .unwrap();
    out.clear();
    sess
      .hyper_log_log_length(&[b"copy"], &batch, &mut out)
      .unwrap();
    let card2: i64 = String::from_utf8_lossy(&out[1..out.len() - 2])
      .parse()
      .unwrap();
    assert_eq!(card, card2);
  }
}
