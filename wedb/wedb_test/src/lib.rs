//! Wedb 集群装配层测试支撑 (`wedb_test`)
//!
//! 分层口径：底层测试装配（小预算存储配置、临时目录开库、RESP 帧工具、
//! 假端点、日志 ctor）只在 `wtest_base`，本 crate 只承载需要顶层集群
//! 门面的节点装配：[`cluster_decorate`] / [`start_node`] 等集群形态起服
//! 工具。仅集群集成测试（wedb/tests）以 dev-dependencies 形式消费，
//! 不进入任何生产链接面；更下层 crate 的测试一律改引 `wtest_base`，
//! 不得反向依赖本 crate（杜绝测试链经 wedb 门面拖起整个集群栈）。

mod node;

pub use node::{NodeAssembly, cluster_decorate, start_node};
