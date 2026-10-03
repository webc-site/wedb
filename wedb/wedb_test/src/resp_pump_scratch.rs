//! 泵等价消费单源（scratch 直投形态）
//!
//! 收口 diskless_epoch_drain_failclose / diskless_sync_write_window /
//! expire_replica_replay / script_txn_replica_replay / snapshot_swap_domain_pin
//! 五册逐字同形的 `pump`（接收缓冲直填 → 唯一入口 → 应答取出，忽略消费返回值）。
//! 实现委托 `wnode_test::pump` 单源——两份函数体逐字同、仅返回形态差（回传
//! 剩余 vs 忽略剩余），本面即「忽略剩余」封装；wnode_test 依赖链不含
//! wedb_test，委托无环。带完整消费断言与慢路径闭环的 drive 形态见
//! `resp_drive_scratch`。消费面经 `wedb_test::resp_pump_scratch` 引用（原
//! common/ 直挂面已收口进本 crate）。

use wnode::RespSessionConsumer;

/// 泵等价消费（直填会话接收缓冲 → 唯一入口 → 应答取出；剩余未消费量不入判）
pub fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  wnode_test::pump(consumer, frame).1
}
