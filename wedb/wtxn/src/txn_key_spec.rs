//! 事务键规格参数（对标 libs/server/Transaction/TxnKeyManager.cs）

/// 键规格检索参数（C# KeySpecification 检索产物的本域投影：
/// `firstIdx..=lastIdx step` 迭代窗口 + 读写属性）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TxnKeySpec {
  /// 起始参数下标
  pub first_idx: usize,
  /// 终止参数下标（含；可为 -1 表"倒数第一个"，对齐键规格负索引）
  pub last_idx: i64,
  /// 步长
  pub step: usize,
  /// 是否只读键（决定共享 / 排他锁型）
  pub read_only: bool,
}

impl TxnKeySpec {
  /// 构造逐键检索窗口
  pub fn new(first_idx: usize, last_idx: i64, step: usize, read_only: bool) -> Self {
    Self {
      first_idx,
      last_idx,
      step,
      read_only,
    }
  }
}
