//! client/server 共引的指标类型单点（对标 libs/common/Metrics）
//!
//! C# 的 InfoMetricsType 与 MetricsItem 都在 Garnet.common 程序集，
//! Garnet.client 与 Garnet.server 同引一份；rust 对位落到 wconn 与 wmetric
//! 共同依赖的协议层 wresp（wmetric 只承接 libs/server/Metrics 的服务端面）。
//! BCL 的 `ToString("N2")` 三处调用面亦收敛为 `n2_format` 单点。

pub mod info_metrics_type;
pub mod metrics_item;
pub mod n2_format;

pub use info_metrics_type::InfoMetricsType;
pub use metrics_item::MetricsItem;
pub use n2_format::{fmt_n2, fmt_n2_into};
