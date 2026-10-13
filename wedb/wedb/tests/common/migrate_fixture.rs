//! 迁移驱动夹具单源（发送侧 MigrateTaskSpec 预置 + 假端端口解析）
//!
//! 收口 cluster_migration / migrate_deleting_unheld_claim /
//! migrate_epoch_drain_failclose / migrate_fail_inject /
//! reviv_pause_migration_interleave 五册逐字同形的 `migrate_spec` 装配
//!（DE11 源 → DE12 目标，127.0.0.1，空凭据，非 copy / 非 replace）与
//! `port_of` 地址解析。超时差异面（用例参数 / 5000ms 基线）以参数保留。
//! 宿主册直挂（沿用 primary_assets 先例），并须同时直挂 de11_node_id /
//! de12_node_id 单源：
//!
//! ```text
//! #[path = "common/migrate_fixture.rs"]
//! mod migrate_fixture;
//! use migrate_fixture::{migrate_spec, port_of};
//! ```

use wedb::server::migration::migrate_session::MigrateTaskSpec;
use wedb_test::{de11_node_id::DE11_NODE_ID, de12_node_id::DE12_NODE_ID};

/// 迁移驱动发送侧 spec（DE11 源 → DE12@port 目标；`timeout_ms` 各册原值
/// 保留：用例参数或 5000 基线）
pub fn migrate_spec(port: i32, timeout_ms: i32) -> MigrateTaskSpec {
  MigrateTaskSpec {
    source_node_id: DE11_NODE_ID,
    target_address: "127.0.0.1".to_string(),
    target_port: port,
    target_node_id: DE12_NODE_ID,
    username: String::new(),
    passwd: String::new(),
    copy_option: false,
    replace_option: false,
    timeout: timeout_ms,
  }
}

/// 解析假端监听地址的端口号
pub fn port_of(addr: &str) -> i32 {
  addr.rsplit(':').next().unwrap().parse().unwrap()
}
