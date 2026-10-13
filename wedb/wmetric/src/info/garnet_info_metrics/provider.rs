//! INFO 数据源 trait 与 GarnetInfoMetrics 聚合实现（段填充函数族见
//! [`super::sections`]，行表见 [`super::tables`]）。

use itoa::Buffer;
use wresp::metrics::{InfoMetricsType, MetricsItem};

use super::{
  snapshots::{DbSnapshot, GlobalMetricsSnapshot, ServerFacts},
  tables::SECTION_HEADERS,
};

/// INFO 数据源（对标 C# 侧 StoreWrapper / monitor / clusterProvider 的
/// 直读面；StoreWrapper 域落地后由其实现本 trait）。
pub trait InfoProvider {
  /// 服务器级事实。
  fn server_facts(&self) -> ServerFacts;

  /// 全部库的快照（按 id 升序）。
  fn databases(&self) -> Vec<DbSnapshot>;

  /// 全局指标快照（监视器未启用为 None）。
  fn global_metrics(&self) -> Option<GlobalMetricsSnapshot>;

  /// 聚合命令统计：`(cmdstat 名(小写), calls, rejected_calls, failed_calls)`，
  /// 已过滤 calls/rejected 两栏判零（failed 恒透出不参与过滤，对齐 C#
  /// PopulateCommandStatsInfo 谓词两栏判零，GarnetInfoMetrics.cs:274）与
  /// "unknown"；两栏口径实码见 info_provider.rs 聚合臂，分叉登记 deviations §130。
  fn command_stats(&self) -> Vec<(String, u64, u64, u64)>;

  /// 库的 (键数, 过期键数)。
  fn keyspace_stats(&self, db_id: i32) -> (u64, u64);

  /// 集群复制信息段；None = 无集群提供方（C# clusterProvider == null）。
  fn replication_info(&self) -> Option<Vec<MetricsItem>>;

  /// gossip 统计段。
  fn gossip_stats(&self, metrics_disabled: bool) -> Vec<MetricsItem>;

  /// 缓冲池统计：`(server_socket_i / 集群端口名, 统计文本)`。
  fn buffer_pool_stats(&self) -> Vec<(String, String)>;

  /// 集群 checkpoint 信息段。
  fn checkpoint_info(&self) -> Option<Vec<MetricsItem>>;

  /// 每库混合日志内存分布转储：`(主存储转储, 对象存储转储)`。
  fn hlog_scan_dump(&self) -> Vec<(String, String)>;

  /// 主侧安全 AOF 地址：真复制位点投影（cluster 主侧 get_primary_info 首元素），
  /// 非主/无提供方/无集群回 0——C# 对位字段系仅声明 -1 的死字段（恒 "-1"），
  /// 哨兵方向反向分叉已登 deviations §130，严禁按死字段形回改。
  fn safe_aof_address(&self) -> i64;

  /// 后台任务健康快照：`(任务名/计数名, 展示值)`——存活任务 `name=alive`,
  /// 死亡任务 `name=dead(panic=N)`,登记计数 `name=N`（r30-bgthread 发现五：
  /// server 段 `bg_task_health` 一行暴露；宿主未接监督面为空，绝不虚报）。
  fn bg_task_health(&self) -> Vec<(String, String)> {
    Vec::new()
  }

  /// 原生分配器记账字节数（对标 libs/server/Metrics/Info/GarnetInfoMetrics.cs:143
  /// `native_allocator_bytes` ← Tsavorite NativeMemoryTracker.Bytes 全局总账）。
  /// 观测叠影字段，不参与 total_main_store_size 求和；无 windex 记账源的宿主
  /// 形态走本缺省 0 占位，绝不虚报。
  fn native_allocator_bytes(&self) -> i64 {
    0
  }

  /// server_socket 池统计行：`(server_socket_i, 统计文本)`（对标
  /// GarnetInfoMetrics.cs:413 逐 TCP server 出行的宿主侧供给臂）。无网络
  /// 监听面的宿主形态走本缺省空表，与 C# 无 server 态同。
  fn server_socket_buffer_pool_stats(&self) -> Vec<(String, String)> {
    Vec::new()
  }
}

/// INFO 各段指标的填充与序列化
///（对标 libs/server/Metrics/Info/GarnetInfoMetrics.cs:GarnetInfoMetrics）。
pub struct GarnetInfoMetrics {
  pub(super) server_info: Option<Vec<MetricsItem>>,
  pub(super) memory_info: Option<Vec<MetricsItem>>,
  pub(super) cluster_info: Option<Vec<MetricsItem>>,
  pub(super) replication_info: Option<Vec<MetricsItem>>,
  pub(super) stats_info: Option<Vec<MetricsItem>>,
  pub(super) store_info: Option<Vec<Vec<MetricsItem>>>,
  pub(super) store_hash_distr_info: Option<Vec<Vec<MetricsItem>>>,
  pub(super) store_reviv_info: Option<Vec<Vec<MetricsItem>>>,
  pub(super) persistence_info: Option<Vec<Vec<MetricsItem>>>,
  pub(super) clients_info: Option<Vec<MetricsItem>>,
  pub(super) keyspace_info: Option<Vec<MetricsItem>>,
  pub(super) buffer_pool_stats: Option<Vec<MetricsItem>>,
  pub(super) checkpoint_stats: Option<Vec<MetricsItem>>,
  pub(super) hlog_scan_stats: Option<Vec<Vec<MetricsItem>>>,
  pub(super) command_stats_info: Option<Vec<MetricsItem>>,
}

impl Default for GarnetInfoMetrics {
  fn default() -> Self {
    Self::new()
  }
}

impl GarnetInfoMetrics {
  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GarnetInfoMetrics（构造）。
  pub fn new() -> Self {
    Self {
      server_info: None,
      memory_info: None,
      cluster_info: None,
      replication_info: None,
      stats_info: None,
      store_info: None,
      store_hash_distr_info: None,
      store_reviv_info: None,
      persistence_info: None,
      clients_info: None,
      keyspace_info: None,
      buffer_pool_stats: None,
      checkpoint_stats: None,
      hlog_scan_stats: None,
      command_stats_info: None,
    }
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetSectionHeader
  ///
  /// 段头词干 + 是否携带 `_DB_{db_id}` 后缀：固定段名编译期 `&'static str`
  /// 零分配，DB 参数化段由 [`Self::get_section_resp_info`] 向 sb_response
  /// 直写拼接，免中间 String。段名内不得含词分隔符，否则部分客户端无法
  /// 解析 INFO 输出。
  #[inline]
  pub const fn get_section_header_parts(info_type: InfoMetricsType) -> (&'static str, bool) {
    SECTION_HEADERS[info_type as usize]
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetSectionRespInfo
  ///
  /// 追加 `# <header>[\_DB_<id>]\r\n` 与全部指标行；单次预估容量，零额外
  /// `format!`/堆分配（DB 后缀 itoa 栈上格式化直写）。无名首项（多行字符串
  /// 指标）直接裸出值，避免行首游离冒号。
  #[inline]
  fn get_section_resp_info(
    section_header: &str,
    header_db_id: Option<i32>,
    info: Option<&[MetricsItem]>,
    sb_response: &mut String,
  ) {
    // itoa 栈上缓冲：文本借用存活至段头写毕，id 长度先行得知，容量预估保持精确
    let mut db_id_buf = Buffer::new();
    let db_id_text = header_db_id.map(|id| db_id_buf.format(id));
    let header_len =
      2 + section_header.len() + db_id_text.as_ref().map_or(0, |t| "_DB_".len() + t.len()) + 2;
    let items_len = match info {
      Some(items) => items
        .iter()
        .map(|it| it.name.len() + it.value.len() + 3)
        .sum::<usize>(),
      None => 0,
    };
    sb_response.reserve(header_len + items_len);

    sb_response.push_str("# ");
    sb_response.push_str(section_header);
    if let Some(id_text) = db_id_text {
      sb_response.push_str("_DB_");
      sb_response.push_str(id_text);
    }
    sb_response.push_str("\r\n");
    let Some(info) = info else {
      return;
    };
    if info.first().is_some_and(|item| item.name.is_empty()) {
      sb_response.push_str(&info[0].value);
      sb_response.push_str("\r\n");
      return;
    }
    for item in info {
      sb_response.push_str(&item.name);
      sb_response.push(':');
      sb_response.push_str(&item.value);
      sb_response.push_str("\r\n");
    }
  }

  /// 对应 GetRespInfo 单段填充实现
  fn get_resp_info_single(
    &mut self,
    section: InfoMetricsType,
    db_id: i32,
    provider: &impl InfoProvider,
    sb_response: &mut String,
  ) {
    // PERSISTENCE 未启用 AOF 整段不出（连段头都不落笔，对齐 C# 短路位）
    if section == InfoMetricsType::Persistence && !provider.server_facts().enable_aof {
      return;
    }
    let (header, with_db) = Self::get_section_header_parts(section);
    let info = self.populate_section(section, db_id, provider);
    Self::get_section_resp_info(header, with_db.then_some(db_id), info, sb_response);
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetRespInfo（多段）
  ///
  /// 按序填充并拼接各段；段间以 `\r\n` 分隔。
  pub fn get_resp_info(
    &mut self,
    sections: &[InfoMetricsType],
    db_id: i32,
    provider: &impl InfoProvider,
  ) -> String {
    let mut sb_response = String::new();
    for (i, section) in sections.iter().enumerate() {
      self.get_resp_info_single(*section, db_id, provider, &mut sb_response);
      if i != sections.len() - 1 {
        sb_response.push_str("\r\n");
      }
    }
    sb_response
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetMetric
  ///
  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetMetricInternal
  ///
  /// BpStats/CInfo/HlogScan 三段仅 RESP 渲染面（C# 同走 `_ => null` 不覆盖），
  /// 此处前置过滤保持不填充。
  pub fn get_metric(
    &mut self,
    section: InfoMetricsType,
    db_id: i32,
    provider: &impl InfoProvider,
  ) -> Option<Vec<MetricsItem>> {
    if matches!(
      section,
      InfoMetricsType::BpStats | InfoMetricsType::CInfo | InfoMetricsType::HlogScan
    ) {
      return None;
    }
    self
      .populate_section(section, db_id, provider)
      .map(<[MetricsItem]>::to_vec)
  }

  /// libs/server/Metrics/Info/GarnetInfoMetrics.cs:GetInfoMetrics
  pub fn get_info_metrics(
    &mut self,
    sections: &[InfoMetricsType],
    db_id: i32,
    provider: &impl InfoProvider,
  ) -> Vec<(InfoMetricsType, Vec<MetricsItem>)> {
    let mut result = Vec::with_capacity(sections.len());
    for &section in sections {
      if let Some(items) = self.get_metric(section, db_id, provider) {
        result.push((section, items));
      }
    }
    result
  }
}
