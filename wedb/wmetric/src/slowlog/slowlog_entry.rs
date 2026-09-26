use std::sync::Arc;

use wresp::command::RespCommand;

/// 慢日志条目（对标 libs/server/Metrics/Slowlog/SlowlogEntry.cs:SlowLogEntry）。
#[derive(Debug, Clone)]
pub struct SlowLogEntry {
  /// 自增 id。C# 原型字段与计数器同为 int（2^31 即回绕负值）；rust 计数器
  /// 为 AtomicI64，字段随计数器取 i64 消除截断，兑现 SLOWLOG 协议的
  /// 全局递增非负唯一 id
  pub id: i64,
  /// 入库时间戳（秒）。
  pub timestamp: i32,
  /// 耗时（微秒）。
  pub duration: i32,
  /// 触发命令。
  pub command: RespCommand,
  /// 序列化的参数（解析状态快照）Arc 胶囊；无参数为 None。
  ///
  /// C# SlowLogEntry 为 struct，byte[] Arguments 按引用共享，读出面
  /// （GetEntries 对 ConcurrentQueue 无锁枚举）零载荷字节复制。rust 容器
  /// 以 `Mutex<VecDeque>` 承接，快照写路径 move 入库（`Arc::new` 包裹零
  /// 拷贝），`get_entries` 克隆降为引用计数递增，锁内时长回归 O(n) 元数据
  /// 级，不再正比载荷字节。
  pub arguments: Option<Arc<Vec<u8>>>,
  /// 客户端 IP:端口。
  pub client_ip_port: String,
  /// 客户端名。
  pub client_name: String,
}
