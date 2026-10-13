use std::sync::atomic::{AtomicU64, Ordering::Relaxed};

/// RespServerSession 发出的性能指标快照（对标
/// libs/server/Metrics/GarnetSessionMetrics.cs:GarnetSessionMetrics 的字段面）。
///
/// C# 为每会话实例的普通字段累加（单写者，无锁）；rust 会话体与存储执行域跨
/// compio 任务共享同一指标对象，写面由 [`SessionMetricsHandle`]（原子承接）
/// 承担，本结构为纯数据快照与监视器聚合体（add/reset 对位 C# Add/Reset）。
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GarnetSessionMetrics {
  /// 网络入字节累计。
  pub total_net_input_bytes: u64,
  /// 网络出字节累计。
  pub total_net_output_bytes: u64,
  /// 已处理命令累计。
  pub total_commands_processed: u64,
  /// pending 累计。
  pub total_pending: u64,
  /// 命中累计。
  pub total_found: u64,
  /// 未命中累计。
  pub total_notfound: u64,
  /// 集群命令处理累计。
  pub total_cluster_commands_processed: u64,
  /// 写命令执行计数。
  pub total_write_commands_processed: u64,
  /// 读命令执行计数。
  pub total_read_commands_processed: u64,
  /// 全部 resp 服务器会话 try consume 触发的异常总数。
  pub total_number_resp_server_session_exceptions: u64,
}

/// C# 对位 getter 样板的表驱动展开（方法名/文档锚点逐条与原手写一致）
macro_rules! cs_getters {
  ($($(#[$doc:meta])* $get:ident : $field:ident),+ $(,)?) => {
    $(
      $(#[$doc])*
      #[inline]
      pub fn $get(&self) -> u64 {
        self.$field
      }
    )+
  };
}

/// C# 对位 fetch_add 访问器样板的表驱动展开
macro_rules! cs_incr {
  ($($(#[$doc:meta])* $incr:ident : $field:ident),+ $(,)?) => {
    $(
      $(#[$doc])*
      #[inline]
      pub fn $incr(&self, count: u64) {
        self.$field.fetch_add(count, Relaxed);
      }
    )+
  };
}

/// 十字段清单单源（值面 [`GarnetSessionMetrics`] 与原子面
/// [`SessionMetricsHandle`] 同构同序）：`add`/`snapshot`/原子 `reset` 三处
/// 逐字段遍历全部按本清单回调展开，新增字段只改此清单，遍历面零漏改
macro_rules! for_each_metrics_field {
  ($mac:ident) => {
    $mac! {
      total_net_input_bytes,
      total_net_output_bytes,
      total_commands_processed,
      total_pending,
      total_found,
      total_notfound,
      total_cluster_commands_processed,
      total_write_commands_processed,
      total_read_commands_processed,
      total_number_resp_server_session_exceptions,
    }
  };
}

/// `GarnetSessionMetrics::add` 表驱动展开（逐字段 wrapping_add：溢出按二进制
/// 环绕不 panic，与 C# 无检查普通字段累加同语义）
macro_rules! metrics_value_add {
  ($($field:ident),+ $(,)?) => {
    /// libs/server/Metrics/GarnetSessionMetrics.cs:Add
    ///
    /// 将另一份会话指标并入本实例。
    pub fn add(&mut self, add: &GarnetSessionMetrics) {
      $(self.$field = self.$field.wrapping_add(add.$field);)+
    }

    /// libs/server/Metrics/GarnetSessionMetrics.cs:Reset
    ///
    /// 清零全部计数。
    pub fn reset(&mut self) {
      *self = Self::default();
    }
  };
}

/// `SessionMetricsHandle::snapshot` 表驱动展开（逐字段 Relaxed load 收口为纯数据快照）
macro_rules! metrics_handle_snapshot {
  ($($field:ident),+ $(,)?) => {
    /// 全部计数 Relaxed 装载，收口为纯数据快照（读面唯一出口）。
    pub fn snapshot(&self) -> GarnetSessionMetrics {
      GarnetSessionMetrics {
        $($field: self.$field.load(Relaxed),)+
      }
    }
  };
}

/// `SessionMetricsHandle::reset` 表驱动展开（逐字段 Relaxed 清零）
macro_rules! metrics_handle_reset {
  ($($field:ident),+ $(,)?) => {
    /// INFO RESET STATS 的活跃会话原地复位（consumer 镜像点单一调用方）：
    /// 全部计数清零，复位后自零位重新累计；原子承接面与 [`Self::snapshot`]
    /// 同一字段集，杜绝快照/复位两套字段漂移。
    ///
    /// C# `GarnetSessionMetrics.cs` 的 Reset 一枚 1:1 挂载在值语义的
    /// [`GarnetSessionMetrics::reset`]，本原子镜像臂是 rust 侧共享计数承接面
    /// （C# 由会话独占计数，无对位方法），不复挂
    pub fn reset(&self) {
      $(self.$field.store(0, Relaxed);)+
    }
  };
}

impl GarnetSessionMetrics {
  for_each_metrics_field!(metrics_value_add);

  // 宏体表驱动展开的逐条 C# 锚点登记（方法名与下列 cs:名一一对位）：
  // libs/server/Metrics/GarnetSessionMetrics.cs:get_total_net_input_bytes
  // libs/server/Metrics/GarnetSessionMetrics.cs:get_total_net_output_bytes
  // libs/server/Metrics/GarnetSessionMetrics.cs:get_total_commands_processed
  // libs/server/Metrics/GarnetSessionMetrics.cs:get_total_pending
  // libs/server/Metrics/GarnetSessionMetrics.cs:get_total_found
  // libs/server/Metrics/GarnetSessionMetrics.cs:get_total_notfound
  // libs/server/Metrics/GarnetSessionMetrics.cs:get_total_cluster_commands_processed
  // libs/server/Metrics/GarnetSessionMetrics.cs:get_total_write_commands_processed
  // libs/server/Metrics/GarnetSessionMetrics.cs:get_total_read_commands_processed
  // libs/server/Metrics/GarnetSessionMetrics.cs:get_total_number_resp_server_session_exceptions
  cs_getters! {
  get_total_net_input_bytes: total_net_input_bytes,
  get_total_net_output_bytes: total_net_output_bytes,
  get_total_commands_processed: total_commands_processed,
  get_total_pending: total_pending,
  get_total_found: total_found,
  get_total_notfound: total_notfound,
  get_total_cluster_commands_processed: total_cluster_commands_processed,
  get_total_write_commands_processed: total_write_commands_processed,
  get_total_read_commands_processed: total_read_commands_processed,
  get_total_number_resp_server_session_exceptions: total_number_resp_server_session_exceptions,
  }
}

/// 会话指标共享句柄（对标 C# GarnetSessionMetrics 的类引用语义（文件映射在
/// [`GarnetSessionMetrics`] 结构文档处一处声明）：C# 每会话一个对象，
/// RespServerSession 与 StorageSession 共持同一实例直写计数；rust 会话体与
/// 存储执行域跨 compio 任务共享，以同数同序的
/// [`AtomicU64`] 字段 + `&self` Relaxed 写口承接 C# 普通字段累加）。
///
/// 本句柄即会话计数真值源：读面统一经 [`Self::snapshot`] 收口为
/// [`GarnetSessionMetrics`] 纯数据（监视器聚合 / INFO 呈现），不设第二套累加器；
/// 采样关闭（C# trackStats false → sessionMetrics 为 null）时会话与存储会话两侧
/// 句柄均为 None，写口不可达，零开销与 C# `sessionMetrics?.` 空条件调用同形。
#[derive(Debug, Default)]
pub struct SessionMetricsHandle {
  /// 网络入字节累计。
  total_net_input_bytes: AtomicU64,
  /// 网络出字节累计。
  total_net_output_bytes: AtomicU64,
  /// 已处理命令累计。
  total_commands_processed: AtomicU64,
  /// pending 累计。
  total_pending: AtomicU64,
  /// 命中累计。
  total_found: AtomicU64,
  /// 未命中累计。
  total_notfound: AtomicU64,
  /// 集群命令处理累计。
  total_cluster_commands_processed: AtomicU64,
  /// 写命令执行计数。
  total_write_commands_processed: AtomicU64,
  /// 读命令执行计数。
  total_read_commands_processed: AtomicU64,
  /// 全部 resp 服务器会话 try consume 触发的异常总数。
  total_number_resp_server_session_exceptions: AtomicU64,
}

impl SessionMetricsHandle {
  for_each_metrics_field!(metrics_handle_snapshot);
  for_each_metrics_field!(metrics_handle_reset);

  // 原子句柄宏体表驱动展开的逐条 C# 锚点登记（incr/add 臂一一对位）：
  // libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_net_input_bytes
  // libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_net_output_bytes
  // libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_commands_processed
  // libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_pending
  // libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_found
  // libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_notfound
  // libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_cluster_commands_processed
  // libs/server/Metrics/GarnetSessionMetrics.cs:add_total_write_commands_processed
  // libs/server/Metrics/GarnetSessionMetrics.cs:add_total_read_commands_processed
  // libs/server/Metrics/GarnetSessionMetrics.cs:incr_total_number_resp_server_session_exceptions
  cs_incr! {
  incr_total_net_input_bytes: total_net_input_bytes,
  incr_total_net_output_bytes: total_net_output_bytes,
  incr_total_commands_processed: total_commands_processed,
  incr_total_pending: total_pending,
  incr_total_found: total_found,
  incr_total_notfound: total_notfound,
  incr_total_cluster_commands_processed: total_cluster_commands_processed,
  add_total_write_commands_processed: total_write_commands_processed,
  add_total_read_commands_processed: total_read_commands_processed,
  incr_total_number_resp_server_session_exceptions: total_number_resp_server_session_exceptions,
  }
}
