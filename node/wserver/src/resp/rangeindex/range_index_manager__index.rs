//! 范围索引存根的值span读写（对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs）
//!
//! 存根（RangeIndexStub，35 字节定长）是主存中每个 RI 键的行内值。C# 以
//! `Unsafe.As` 把值span零拷贝重解释为结构体；Rust 引擎以纯编解码承接
//! （[`wkv::RangeIndexStub`] 的 `encode_into` / `decode_opt` / slice 系列），
//! 本分片提供同名的span原语面：写入新存根、零拷贝读存根、清除刷盘标志。
//! 其余 span 原语（RecreateIndex / ClearTreeHandle / SetTransferredFlag /
//! InvalidateStub / MarkRecoveredFromCheckpoint）已由引擎 stub 层以同名
//! slice_* 静态面承接，不在此重复。

use wkv::{RANGE_INDEX_STUB_SIZE, RangeIndexStub, TreeTuning};

use super::range_index_manager__migration::MigrationError;

/// 存根span操作面（C# partial RangeIndexManager 的 Index 分片）
pub struct RangeIndexManager_Index;

impl RangeIndexManager_Index {
  /// libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:CreateIndex
  ///
  /// 向主存值span写入全新存根（RI.CREATE 的 InitialUpdater 路径调用）：
  /// BfTree 配置 + 在线树句柄 + 清零标志位 + 序列化阶段号。span 长度不足
  /// 时返回错误（C# 为 Debug.Assert + 未定义行为；Rust 显式拒绝）。
  /// C# 的 5 个配置参数以 [`TreeTuning`] 打包（Rust 引擎同构）
  pub fn create_index(
    tuning: TreeTuning,
    storage_backend: u8,
    tree_handle: u64,
    value_span: &mut [u8],
  ) -> Result<(), MigrationError> {
    if value_span.len() < RANGE_INDEX_STUB_SIZE {
      return Err(MigrationError::Invalid(format!(
        "CreateIndex: value span too small: {} < {RANGE_INDEX_STUB_SIZE}",
        value_span.len()
      )));
    }
    let stub = RangeIndexStub::new(
      tree_handle,
      tuning.cache_size as u64,
      tuning.min_record_size as u32,
      tuning.max_record_size as u32,
      tuning.max_key_len as u32,
      tuning.leaf_page_size as u32,
      wkv::StorageBackendType::from_u8(storage_backend),
    );
    // encode_into 恒写入 RANGE_INDEX_STUB_SIZE 字节（定长编码，无变长尾巴）
    stub
      .encode_into(&mut value_span[..RANGE_INDEX_STUB_SIZE])
      .map_err(|e| MigrationError::Invalid(e.to_string()))
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:ReadIndex
  ///
  /// 零拷贝读存根：值span不足 35 字节（非 RI 记录 / 损坏）返回 None
  /// （C# 由调用方保证长度后 Unsafe.As；Rust 以 Option 承载校验）
  #[inline]
  pub fn read_index(value: &[u8]) -> Option<RangeIndexStub> {
    RangeIndexStub::decode_opt(value)
  }

  /// libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:ClearFlushedFlag
  ///
  /// 清除值span内存根的 Flushed 标志（CopyUpdater 把存根晋升至可变尾区后
  /// 调用，后续操作不再触发晋升）。span 过短时返回错误
  pub fn clear_flushed_flag(value_span: &mut [u8]) -> Result<(), MigrationError> {
    RangeIndexStub::slice_set_flushed(value_span, false)
      .map_err(|e| MigrationError::Invalid(e.to_string()))
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  const DISK: u8 = 0;

  fn tuning(cache: usize, min: usize, max: usize, key_len: usize, leaf: usize) -> TreeTuning {
    TreeTuning {
      cache_size: cache,
      min_record_size: min,
      max_record_size: max,
      max_key_len: key_len,
      leaf_page_size: leaf,
    }
  }

  #[test]
  fn create_then_read_roundtrip() {
    let mut span = [0u8; 64];
    RangeIndexManager_Index::create_index(
      tuning(16 * 1024 * 1024, 64, 1024, 128, 4096),
      DISK,
      0xDEAD_BEEF,
      &mut span,
    )
    .unwrap();

    let stub = RangeIndexManager_Index::read_index(&span).expect("stub decodable");
    assert_eq!(stub.tree_handle, 0xDEAD_BEEF);
    assert_eq!(stub.cache_size, 16 * 1024 * 1024);
    assert_eq!(stub.min_record_size, 64);
    assert_eq!(stub.max_record_size, 1024);
    assert_eq!(stub.max_key_len, 128);
    assert_eq!(stub.leaf_page_size, 4096);
    assert_eq!(stub.storage_backend, DISK);
    // 新建存根：标志位与序列化阶段号全零
    assert_eq!(stub.flags, 0);
    assert_eq!(stub.serialization_phase, 0);
    assert!(!stub.is_flushed());
  }

  #[test]
  fn create_index_rejects_short_span() {
    let mut span = [0u8; RANGE_INDEX_STUB_SIZE - 1];
    let err =
      RangeIndexManager_Index::create_index(TreeTuning::default(), DISK, 0, &mut span).unwrap_err();
    assert!(err.to_string().contains("value span too small"));
  }

  #[test]
  fn read_index_returns_none_for_non_stub_span() {
    assert!(RangeIndexManager_Index::read_index(&[]).is_none());
    assert!(RangeIndexManager_Index::read_index(&[0u8; 8]).is_none());
  }

  #[test]
  fn clear_flushed_flag_flips_only_that_bit() {
    let mut span = [0u8; 64];
    RangeIndexManager_Index::create_index(tuning(1, 8, 64, 16, 512), DISK, 7, &mut span).unwrap();
    // 先置 Flushed（引擎 slice 原语），确认读回为真
    RangeIndexStub::slice_set_flushed(&mut span, true).unwrap();
    assert!(
      RangeIndexManager_Index::read_index(&span)
        .unwrap()
        .is_flushed()
    );

    RangeIndexManager_Index::clear_flushed_flag(&mut span).unwrap();
    let stub = RangeIndexManager_Index::read_index(&span).unwrap();
    assert!(!stub.is_flushed());
    // 其余字段不受影响
    assert_eq!(stub.tree_handle, 7);
    assert_eq!(stub.max_key_len, 16);

    // 短span拒绝
    let mut short = [0u8; 4];
    assert!(RangeIndexManager_Index::clear_flushed_flag(&mut short).is_err());
  }
}
