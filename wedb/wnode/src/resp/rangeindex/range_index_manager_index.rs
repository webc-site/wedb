//! 范围索引存根的值span读写（对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs）
//!
//! 存根（RangeIndexStub，35 字节定长）是主存中每个 RI 键的行内值。C# 以
//! `Unsafe.As` 把值span零拷贝重解释为结构体；Rust 引擎以纯编解码承接
//! （[`wbftree::RangeIndexStub`] 的 `encode_into` / `decode_opt` / slice 系列），
//! 本分片提供同名的span原语面：写入新存根、零拷贝读存根、清除刷盘标志。
//! 其余 span 原语（RecreateIndex / ClearTreeHandle / SetTransferredFlag /
//! InvalidateStub / MarkRecoveredFromCheckpoint）已由引擎 stub 层以同名
//! slice_* 静态面承接，不在此重复。

use wbftree::{RANGE_INDEX_STUB_SIZE, RangeIndexStub, StorageBackendType, TreeTuning};

use super::range_index_manager_migration::MigrationError;

/// 存根span操作面（C# partial RangeIndexManager 的 Index 分片）
pub struct RangeIndexManagerIndex;

impl RangeIndexManagerIndex {
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
    let stub = RangeIndexStub::from_tuning(
      tree_handle,
      &tuning,
      StorageBackendType::from_u8(storage_backend),
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
