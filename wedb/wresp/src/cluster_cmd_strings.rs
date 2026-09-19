//! 集群域 RESP 错误文案与重定向帧写出（对标 libs/cluster/CmdStrings.cs）
//!
//! 集群专属文案单源：wnode / wedb 两消费方共引本表；通用文案与
//! [`crate::cmd_strings`] 单源，此处不重复。

use itoa::Buffer;

/// libs/cluster/CmdStrings.cs:RESP_ERR_CLUSTERDOWN
pub const RESP_ERR_CLUSTERDOWN: &str = "CLUSTERDOWN Hash slot not served";
/// libs/cluster/CmdStrings.cs:RESP_ERR_TRYAGAIN
pub const RESP_ERR_TRYAGAIN: &str = "TRYAGAIN Multiple keys request during rehashing of slot";
/// libs/cluster/CmdStrings.cs:RESP_ERR_INVALID_SLOT
pub const RESP_ERR_INVALID_SLOT: &str = "ERR Invalid or out of range slot";
/// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE
pub const RESP_ERR_GENERIC_SLOT_OUT_OFF_RANGE: &str = "ERR Slot out of range";
/// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_CONFIG_EPOCH_ASSIGNMENT
pub const RESP_ERR_GENERIC_CONFIG_EPOCH_ASSIGNMENT: &str =
  "ERR The user can assign a config epoch only when the node does not know any other node";
/// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_CONFIG_UPDATE
pub const RESP_ERR_GENERIC_CONFIG_UPDATE: &str = "ERR Updating the config epoch";
/// libs/cluster/CmdStrings.cs:RESP_ERR_RESET_WITH_KEYS_ASSIGNED
pub const RESP_ERR_RESET_WITH_KEYS_ASSIGNED: &str =
  "ERR CLUSTER RESET can't be called with master nodes containing keys";
/// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_CANNOT_FORGET_MYSELF
pub const RESP_ERR_GENERIC_CANNOT_FORGET_MYSELF: &str =
  "ERR I tried hard but I can't forget myself";
/// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_CANNOT_FORGET_MY_PRIMARY
pub const RESP_ERR_GENERIC_CANNOT_FORGET_MY_PRIMARY: &str = "ERR Can't forget my primary";
/// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_CANNOT_FAILOVER_FROM_NON_MASTER
pub const RESP_ERR_GENERIC_CANNOT_FAILOVER_FROM_NON_MASTER: &str =
  "ERR Cannot failover a non-master node";
/// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_UNKNOWN_ENDPOINT
pub const RESP_ERR_GENERIC_UNKNOWN_ENDPOINT: &str = "ERR Unknown endpoint";
/// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_SLOT_STATE
pub const RESP_ERR_GENERIC_SLOT_STATE: &str = "ERR Invalid slot state";
/// libs/cluster/CmdStrings.cs:RESP_ERR_MULTI_LOG_DISABLED
pub const RESP_ERR_MULTI_LOG_DISABLED: &str = "ERR Multi-log disabled";
/// libs/cluster/CmdStrings.cs:RESP_ERR_IOERR
pub const RESP_ERR_IOERR: &str = "IOERR Migrate keys failed";
/// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_VALUE_IS_NOT_BOOLEAN
pub const RESP_ERR_GENERIC_VALUE_IS_NOT_BOOLEAN: &str = "ERR value is not a boolean.";
/// libs/cluster/CmdStrings.cs:RESP_ERR_GENERIC_REPLICATION_AOF_TURNEDOFF
/// （CLUSTER REPLICATE 在未开 AOF 时拒绝翻转角色；C# 原文拼写 unaivalable 同字面保留）
pub const RESP_ERR_GENERIC_REPLICATION_AOF_TURNEDOFF: &str = "ERR Replication unaivalable because AOF is switched off, please restart replica with --aof option";

/// 非预期集群命令完整错误帧 `-ERR unexpected cluster command\r\n`
/// （libs/cluster/CmdStrings.cs 口径；用于
/// [`crate::server::replication::cluster_replication_session`] 协议面违规应答）
pub const RESP_ERR_UNEXPECTED_CLUSTER_CMD: &[u8] = b"-ERR unexpected cluster command\r\n";
/// 畸形 APPENDLOG 帧完整错误帧 `-ERR malformed APPENDLOG frame\r\n`
/// （libs/cluster/CmdStrings.cs 口径；与 [`RESP_ERR_UNEXPECTED_CLUSTER_CMD`] 同域）
pub const RESP_ERR_MALFORMED_APPENDLOG_FRAME: &[u8] = b"-ERR malformed APPENDLOG frame\r\n";
/// 协议错误消息前缀 `-ERR Protocol Error: `（不含具体消息与 CRLF 尾；
/// 用于承载解析异常原始文本 `e.to_string()`，避免消费方手拼协议帧头）
/// （对标 C# RespParsingException → catch 块写出 `"-ERR Protocol Error: " + msg`）
pub const RESP_ERR_PROTOCOL_ERROR_PREFIX: &[u8] = b"-ERR Protocol Error: ";

/// 写出 duplicate 槽位错误帧 `-ERR Slot <n> specified multiple times\r\n`
/// （文案取自 C# ClusterCommands 的 TryParseSlots 同名报错，解析层映射定义
/// 在 wedb cluster_session 的 try_parse_slots，此处只承载其错误帧单点写出；
/// itoa Buffer 数值拼接收敛至此，消费方只调不买 itoa）
#[inline]
pub fn write_slot_duplicate_error(output: &mut Vec<u8>, slot: i64) {
  let mut buf = Buffer::new();
  output.extend_from_slice(b"-ERR Slot ");
  output.extend_from_slice(buf.format(slot).as_bytes());
  output.extend_from_slice(b" specified multiple times\r\n");
}

/// 写出区间倒挂错误帧 `-ERR Invalid range <start> > <end>!\r\n`
/// （文案取自 C# ClusterCommands 的 TryParseSlots 同名报错，动态实参拼帧；
/// 判定序与错误映射定义在 wedb cluster_session 的 try_parse_slots，此处只承载
/// 其错误帧单点写出；itoa Buffer 数值拼接收敛至此，消费方只调不买 itoa）
#[inline]
pub fn write_slot_range_error(output: &mut Vec<u8>, start: i64, end: i64) {
  let mut buf = Buffer::new();
  output.extend_from_slice(b"-ERR Invalid range ");
  output.extend_from_slice(buf.format(start).as_bytes());
  output.extend_from_slice(b" > ");
  output.extend_from_slice(buf.format(end).as_bytes());
  output.extend_from_slice(b"!\r\n");
}

/// 写出 MOVED/ASK 重定向错误行 `-<kind> <slot> <endpoint>:<port>\r\n`（零堆分配；
/// 对标 C# RespWriteUtils.TryWriteError 组帧 `$"MOVED {slot} {endpoint}:{port}"`）
#[inline]
pub fn write_redirect_error(
  output: &mut Vec<u8>,
  kind: &str,
  slot: u16,
  endpoint: &str,
  port: i32,
) {
  let mut buf = Buffer::new();
  output.push(b'-');
  output.extend_from_slice(kind.as_bytes());
  output.push(b' ');
  output.extend_from_slice(buf.format(slot).as_bytes());
  output.push(b' ');
  output.extend_from_slice(endpoint.as_bytes());
  output.push(b':');
  output.extend_from_slice(buf.format(port).as_bytes());
  output.extend_from_slice(b"\r\n");
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn unexpected_cluster_cmd_is_wire_ready() {
    assert_eq!(
      RESP_ERR_UNEXPECTED_CLUSTER_CMD,
      b"-ERR unexpected cluster command\r\n"
    );
    assert_eq!(
      RESP_ERR_MALFORMED_APPENDLOG_FRAME,
      b"-ERR malformed APPENDLOG frame\r\n"
    );
    assert_eq!(RESP_ERR_PROTOCOL_ERROR_PREFIX, b"-ERR Protocol Error: ");
  }

  #[test]
  fn duplicate_slot_error_frames_number() {
    let mut out = Vec::new();
    write_slot_duplicate_error(&mut out, 3999);
    assert_eq!(out, b"-ERR Slot 3999 specified multiple times\r\n");

    let mut out = Vec::new();
    write_slot_duplicate_error(&mut out, 0);
    assert_eq!(out, b"-ERR Slot 0 specified multiple times\r\n");
  }

  #[test]
  fn invalid_slot_range_error_frames_both_endpoints() {
    let mut out = Vec::new();
    write_slot_range_error(&mut out, 20000, 10000);
    assert_eq!(out, b"-ERR Invalid range 20000 > 10000!\r\n");

    let mut out = Vec::new();
    write_slot_range_error(&mut out, 10, 5);
    assert_eq!(out, b"-ERR Invalid range 10 > 5!\r\n");
  }

  #[test]
  fn redirect_error_frames_slot_and_endpoint() {
    let mut out = Vec::new();
    write_redirect_error(&mut out, "MOVED", 3999, "127.0.0.1", 7001);
    assert_eq!(out, b"-MOVED 3999 127.0.0.1:7001\r\n");

    let mut out = Vec::new();
    write_redirect_error(&mut out, "ASK", 12000, "node2.example.com", 6379);
    assert_eq!(out, b"-ASK 12000 node2.example.com:6379\r\n");
  }
}
