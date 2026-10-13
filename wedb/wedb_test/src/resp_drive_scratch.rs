//! 慢命令往返驱动单源（scratch 系：接收缓冲直投 + 消费断言 + block_on 慢路径闭环）
//!
//! 收口 checkpoint_import / receive_checkpoint_dir_fsync /
//! replica_receive_checkpoint_retry / replica_diskbased_vector_rebuild 四册
//! 逐字同形的 `drive`。消费面经 `wedb_test::resp_drive_scratch` 引用（原 common/ 直挂面已收口进本 crate）。
//! 调用点零改动。

use compio::runtime::Runtime;
use wnode::{MessageConsumerFace, RespSessionConsumer};

/// 慢命令往返：同步段消费，挂起慢路径时 block_on 驱动应答
pub fn drive(rt: &Runtime, c: &mut RespSessionConsumer, frame_bytes: &[u8]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(frame_bytes);
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let remaining = c.try_consume_messages_into(&mut out);
  assert_eq!(remaining, Some(0), "帧应被完整消费");
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async {
      out.extend_from_slice(&slow.resolve().await);
    });
  }
  out
}
