use crate::types::RespCommand;

/// 慢日志条目（对标 libs/server/Metrics/Slowlog/SlowlogEntry.cs:SlowLogEntry）。
#[derive(Debug, Clone)]
pub struct SlowLogEntry {
  /// 自增 id。
  pub id: i32,
  /// 入库时间戳（秒）。
  pub timestamp: i32,
  /// 耗时（微秒）。
  pub duration: i32,
  /// 触发命令。
  pub command: RespCommand,
  /// 序列化的参数（解析状态快照）；无参数为 None。
  pub arguments: Option<Vec<u8>>,
  /// 客户端 IP:端口。
  pub client_ip_port: String,
  /// 客户端名。
  pub client_name: String,
}
