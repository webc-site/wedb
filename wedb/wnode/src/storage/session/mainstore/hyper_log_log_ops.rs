//! HyperLogLog 操作（对标 libs/server/Storage/Session/MainStore/HyperLogLogOps.cs，C# 为 StorageSession partial）
//!
//! 基于 Garnet 官方 [`HyperLogLog`] 稀疏/稠密双编码实现，与 RESP 命令域
//! 数据格式及 C# 语义完全对齐。

use wdev::Device;

use super::super::storage_session::StorageSession;
use crate::{
  hyperloglog::hyper_log_log::{HyperLogLog, SPARSE_SIZE_MAX_CAP},
  types::GarnetStatus,
};

fn store_hll_slice<'b>(hll: &HyperLogLog, blob: &'b [u8]) -> &'b [u8] {
  let len = if hll.is_sparse(blob) {
    hll
      .sparse_current_size_in_bytes(blob)
      .max(hll.sparse_bytes())
  } else {
    hll.dense_bytes()
  };
  &blob[..len.min(blob.len())]
}

impl<'a, D: Device, CR: wkv::ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// PFADD：登记元素，返回寄存器是否发生变更（-1 表示新建，1 变更，0 未变更）
  ///
  /// libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:HyperLogLogAdd
  pub async fn hyper_log_log_add(
    &self,
    key: &[u8],
    elements: &[&[u8]],
  ) -> wkv::Result<(GarnetStatus, i32)> {
    let hll = HyperLogLog::new();
    let existing = match self.read_string(key).await? {
      Some(raw) => {
        if hll.is_valid_hyll_len(&raw, raw.len()) {
          Some(raw)
        } else {
          return Ok((GarnetStatus::WrongType, 0));
        }
      }
      None => None,
    };

    let mut updated = false;
    let existed = existing.is_some();
    match existing {
      None => {
        let initial = hll.sparse_initial_length(elements.len());
        let mut blob = vec![0u8; initial];
        hll.init(elements, &mut blob);
        updated = true;
        let to_store = store_hll_slice(&hll, &blob);
        self.upsert_string(key, to_store).await?;
      }
      Some(raw) => {
        if hll.is_dense(&raw) {
          let mut blob = raw;
          hll.update(elements, &mut blob, &mut updated);
          if updated {
            let to_store = store_hll_slice(&hll, &blob);
            self.upsert_string(key, to_store).await?;
          }
        } else {
          let mut blob = raw;
          if !hll.update(elements, &mut blob, &mut updated) {
            let new_len = hll.update_grow(elements.len(), &blob);
            let mut grown = vec![0u8; new_len];
            hll.copy_update(elements, &blob, &mut grown);
            updated = true;
            let to_store = store_hll_slice(&hll, &grown);
            self.upsert_string(key, to_store).await?;
          } else if updated {
            let to_store = store_hll_slice(&hll, &blob);
            self.upsert_string(key, to_store).await?;
          }
        }
      }
    }

    let status = if updated {
      if existed { 1 } else { -1 }
    } else {
      0
    };
    Ok((GarnetStatus::Ok, status))
  }

  /// PFCOUNT：基数估计（零拷贝读取底层字节，避免分配堆缓冲）
  ///
  /// libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:HyperLogLogLength
  pub async fn hyper_log_log_length(&self, key: &[u8]) -> wkv::Result<(GarnetStatus, f64)> {
    let res = self
      .read_string_with(key, |raw| {
        let hll = HyperLogLog::new();
        if !hll.is_valid_hyll_len(raw, raw.len()) {
          return Err(GarnetStatus::WrongType);
        }
        let mut dense = vec![0u8; hll.dense_bytes()];
        if hll.is_sparse(raw) {
          hll.init_dense(&mut dense);
          hll.sparse_to_dense(raw, &mut dense);
        } else {
          dense.copy_from_slice(&raw[..hll.dense_bytes()]);
        }
        let count = hll.count(&mut dense);
        Ok(count as f64)
      })
      .await?;
    match res {
      Some(Ok(c)) => Ok((GarnetStatus::Ok, c)),
      Some(Err(status)) => Ok((status, 0.0)),
      None => Ok((GarnetStatus::NotFound, 0.0)),
    }
  }

  /// PFMERGE：多源寄存器逐槽取最大合并入目标键
  ///
  /// libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:HyperLogLogMerge
  pub async fn hyper_log_log_merge(
    &self,
    dest: &[u8],
    sources: &[&[u8]],
  ) -> wkv::Result<GarnetStatus> {
    let hll = HyperLogLog::new();
    let mut dst = match self.read_string(dest).await? {
      Some(raw) => {
        if !hll.is_valid_hyll_len(&raw, raw.len()) {
          return Ok(GarnetStatus::WrongType);
        }
        raw
      }
      None => {
        let mut blob = vec![0u8; SPARSE_SIZE_MAX_CAP];
        hll.init_sparse(&mut blob);
        blob[..hll.sparse_bytes()].to_vec()
      }
    };

    for src_key in sources {
      let Some(src) = self.read_string(src_key).await? else {
        continue;
      };
      if !hll.is_valid_hyll_len(&src, src.len()) {
        return Ok(GarnetStatus::WrongType);
      }
      let new_len = hll.merge_grow(&src, &dst);
      if new_len != dst.len() {
        let mut grown = vec![0u8; new_len];
        let old_len = dst.len();
        hll.copy_update_merge(&src, &dst, &mut grown, old_len, new_len);
        dst = grown;
      } else {
        hll.merge(&src, &mut dst);
        hll.set_card(&mut dst, i64::MIN);
      }
    }

    let to_store = store_hll_slice(&hll, &dst);
    self.upsert_string(dest, to_store).await?;
    Ok(GarnetStatus::Ok)
  }
}
