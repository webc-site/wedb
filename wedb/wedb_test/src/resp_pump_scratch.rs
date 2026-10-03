//! 泵等价消费单源（scratch 直投形态）
//!
//! 收口 diskless_epoch_drain_failclose / diskless_sync_write_window /
//! expire_replica_replay / script_txn_replica_replay / snapshot_swap_domain_pin
//! 五册逐字同形的 `pump`（接收缓冲直填 → 唯一入口 → 应答取出，忽略消费返回值；
//! 带完整消费断言与慢路径闭环的 drive 形态见 resp_drive_scratch / resp_drive_pump）。
//! 消费面经 `wedb_test::resp_pump_scratch` 引用（原 common/ 直挂面已收口进本 crate）。

use wnode::{MessageConsumerFace, RespSessionConsumer};

/// 泵等价消费（直填会话接收缓冲 → 唯一入口 → 应答取出）
pub fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let _ = consumer.try_consume_messages_into(&mut resp);
  resp
}
