//! 网络缓冲规格组与共享池工厂
//!
//! 在 garnet 中的相对路径:libs/common/NetworkBufferSettings.cs:NetworkBufferSettings
//!（PR #2157 为自适应预算补 `minAllocationSize` 与预算参与面）
//!
//! `min_allocation_size` 使池的规格级可以起步于 `initial_receive_buffer_size`
//! 之下：没有它无法表达「新连接仍按 64KB 起步、缓冲回收下探到 16KB」——被
//! 钳到派生最小规格之下的缓冲会落在所有规格级之外，被弃置而非回收。

use std::sync::Arc;

use super::{
  limited::{DEFAULT_MAX_ENTRIES_PER_LEVEL, LimitedFixedBufferPool},
  net_budget::NetworkBufferBudget,
};

/// 每层级闲置条目数自字节预算派生时的绝对上限（C# `MaxEntriesPerLevelCap`
/// = 1024，NetworkBufferSettings.cs:60）：字节预算是真界，让任一层级可支用
/// 全部预算（单规格级突发不被节流），绝对帽防最小规格级在预算相对过大时
/// 圈占数万闲置条目的簿记膨胀
const MAX_ENTRIES_PER_LEVEL_CAP: usize = 1024;

/// 网络缓冲规格组（对标 C# `NetworkBufferSettings` 三尺寸 + PR 新增
/// `minAllocationSize`）
#[derive(Clone, Copy, Debug)]
pub struct NetworkBufferSettings {
  /// send 缓冲规格（定长不自动扩容；调用方负责分配足量与分片更大负载）
  pub send_buffer_size: usize,
  /// receive 缓冲初始分配规格（可按负载自动扩容）
  pub initial_receive_buffer_size: usize,
  /// receive 缓冲最大分配规格
  pub max_receive_buffer_size: usize,
  /// 由本规格组建的池可回收的最小规格级；0 = 由上面三尺寸派生
  pub min_allocation_size: usize,
}

impl Default for NetworkBufferSettings {
  /// 默认规格（C# 无参构造 1<<17/1<<17/1<<20 在 rust 保持现状 64KB 基准，
  /// PR 明言默认值不动现行为）
  fn default() -> Self {
    Self {
      send_buffer_size: super::DEFAULT_BUFFER_SIZE,
      initial_receive_buffer_size: super::DEFAULT_BUFFER_SIZE,
      max_receive_buffer_size: 1 << 20,
      min_allocation_size: 0,
    }
  }
}

impl NetworkBufferSettings {
  /// 创建共享网络缓冲池（对标 `CreateBufferPool`）
  ///
  /// 在 garnet 中的相对路径:libs/common/NetworkBufferSettings.cs:NetworkBufferSettings.CreateBufferPool
  ///
  /// * `max_entries_per_level`：每层级闲置条目上限；`max_pooled_bytes > 0`
  ///   时按「字节预算 / 最小规格」重派生并施 [`MAX_ENTRIES_PER_LEVEL_CAP`]
  /// * `max_pooled_bytes`：全层级闲置字节总上限；0 = 按每层级条目界派生
  /// * `budget`：参与进程级活跃缓冲预算；None = 不参与
  pub fn create_buffer_pool(
    &self,
    max_entries_per_level: usize,
    max_pooled_bytes: i64,
    budget: Option<Arc<NetworkBufferBudget>>,
  ) -> Arc<LimitedFixedBufferPool> {
    let mut min_size = self
      .send_buffer_size
      .min(self.initial_receive_buffer_size)
      .min(self.max_receive_buffer_size);
    if self.min_allocation_size > 0 {
      min_size = min_size.min(self.min_allocation_size);
    }
    let max_size = self
      .send_buffer_size
      .max(self.initial_receive_buffer_size)
      .max(self.max_receive_buffer_size);

    let mut levels = LimitedFixedBufferPool::get_level(min_size, max_size) + 1;
    levels = levels.max(4);

    let mut entries = if max_entries_per_level == 0 {
      DEFAULT_MAX_ENTRIES_PER_LEVEL
    } else {
      max_entries_per_level
    };
    if max_pooled_bytes > 0 {
      // 字节预算是真界：任一层级可支用全部预算；绝对帽防最小规格级圈占
      // 过多闲置条目（默认预算开启时最小规格级为 16KB 接收地板，派生 4096
      // 条/层，帽把它按在 1024）
      entries = MAX_ENTRIES_PER_LEVEL_CAP.min((max_pooled_bytes / min_size as i64).max(1) as usize);
    }

    LimitedFixedBufferPool::with_geometry(
      min_size,
      self.send_buffer_size,
      entries,
      levels,
      max_pooled_bytes,
      budget,
    )
  }
}
