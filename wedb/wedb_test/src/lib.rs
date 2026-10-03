//! Wedb 集群装配层测试支撑 (`wedb_test`)
//!
//! 分层口径：底层测试装配（小预算存储配置、临时目录开库、RESP 帧工具、
//! 假端点、日志 ctor）只在 `wtest_base`，本 crate 承载需要顶层集群门面的
//! 测试基建：[`cluster_decorate`] / [`start_node`] 等集群形态起服工具，与
//! 原 `wedb/tests/common/` `#[path]` 直挂面收口而来的纯 helper 单源
//! （装配级 fixture——真 socket 宿主、回放装配束、检查点/迁移假端等——
//! 仍留 `wedb/tests/common/` 按册直挂，见各文件头）。仅集群集成测试
//! （wedb/tests）以 dev-dependencies 形式消费，不进入任何生产链接面；
//! 更下层 crate 的测试一律改引 `wtest_base`，不得反向依赖本 crate
//! （杜绝测试链经 wedb 门面拖起整个集群栈）。

// ===== 节点身份与存储底座 =====
pub mod de11_node_id;
pub mod de12_node_id;
pub mod node_storage;
pub mod store_node;
pub mod wal_dir;

// ===== RESP 泵族与帧解析 =====
pub mod fake_frame_pump;
pub mod replica_wire_test_wire;

pub mod resp_drive_scratch;
pub mod resp_frame_args;
pub mod resp_pump_scratch;
pub mod resp_value;

// ===== 集群拓扑预置 =====
pub mod cluster_manager_init;
pub mod cluster_seed;
pub mod cluster_seed_remote;
pub mod diskless_provider;
pub mod primary_assets;
pub mod two_primary_provider;
pub mod two_primary_provider_100ms;

// ===== 集群会话消费者装配 =====
pub mod cluster_consumer;
pub mod cluster_consumer_fresh;
pub mod cluster_consumer_fresh_store;
pub mod cluster_consumer_store;
pub mod cluster_consumer_with;

// ===== 副本域装配件 =====
pub mod replica_attach;
pub mod replica_session_face;
pub mod seed_tree;
pub mod sync_meta_seed;

// ===== 集群起服（原 node 单源） =====
pub mod node;

pub use node::{NodeAssembly, cluster_decorate, start_node};
