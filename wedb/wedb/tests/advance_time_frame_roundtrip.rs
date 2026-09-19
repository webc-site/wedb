//! 主端时间脉冲编码帧 → 副本侧 CLUSTER 子命令解析 往返回归
//!
//! 把 wconn `encode_advance_time_frame` 产出的真实字节灌进副本侧解析器
//! （`RespServerSession::parse_command` 的 CLUSTER 父命令 + 子命令查表路径），
//! 断言解析命中 `RespCommand::ClusterAdvanceTime` 而非 `Invalid`/未知，杜绝帧名
//! 漂移（如子命令名漏下划线错拼、批量串长度误作 $11 与规范 $12 不符）这一类只被
//! 前缀字节流式断言掩盖、副本侧恒不可达的断链缺陷再次溜过。
//!
//! 对标 C# 写出端
//! libs/client/ClientSession/GarnetClientSessionReplicationExtensions.cs:ExecuteClusterAdvanceTime
//! （TryWriteBulkString(advance_time) 写出 `$12 ADVANCE_TIME`）与接收端
//! libs/server/Resp/Parser/RespCommandHashLookupData.cs:325
//! ("ADVANCE_TIME", CLUSTER_ADVANCE_TIME)。

use wconn::session::encode_advance_time_frame;
use wnode::resp::resp_server_session::RespServerSession;
use wresp::command::RespCommand;

/// 将一段完整 RESP 帧灌入会话接收缓冲并解析，返回命中的命令
fn parse_frame(session: &mut RespServerSession, buffer: &[u8]) -> Option<RespCommand> {
  session.recv_buffer.clear();
  session.recv_buffer.extend_from_slice(buffer);
  session.bytes_read = session.recv_buffer.len();
  session.read_head = 0;
  session.parse_command()
}

/// 往返核心：encode_advance_time_frame 产帧 → 副本侧解析 → ClusterAdvanceTime
#[test]
fn advance_time_pulse_frame_resolves_to_cluster_advance_time() {
  let frame = encode_advance_time_frame(3, 42);

  // 帧面整帧校验：*4 + CLUSTER + $12 ADVANCE_TIME + 尾元素子日志下标 3 与
  // 序列号 42 全量字节锁定（批量串长度随带下划线的规范名取 $12，帧名/元素
  // 数/元素值任一漂移即红）
  assert_eq!(
    frame.as_slice(),
    b"*4\r\n$7\r\nCLUSTER\r\n$12\r\nADVANCE_TIME\r\n$1\r\n3\r\n$2\r\n42\r\n",
    "编码帧应为 $12 ADVANCE_TIME 四元素整帧"
  );

  // 副本侧解析：CLUSTER 父命令命中子命令表后，首参 ADVANCE_TIME 精确查表
  let mut session = RespServerSession::default();
  let cmd = parse_frame(&mut session, &frame);

  // 断链修复点：命中 ClusterAdvanceTime 而非 Invalid/未知（帧名错拼时解析落
  // Invalid，副本侧永不可达）
  assert_eq!(cmd, Some(RespCommand::ClusterAdvanceTime));
  assert_ne!(cmd, Some(RespCommand::Invalid));
}
