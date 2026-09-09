use itoa::Buffer as IntBuf;
use zmij::Buffer as FloatBuf;

use crate::{Error, GarnetClient, Result};

pub enum InfoMetricsType {
  Default,
  Server,
  Memory,
  Cluster,
  Replication,
  Stats,
  Keyspace,
}

impl InfoMetricsType {
  pub fn as_str(&self) -> &str {
    match self {
      Self::Default => "default",
      Self::Server => "server",
      Self::Memory => "memory",
      Self::Cluster => "cluster",
      Self::Replication => "replication",
      Self::Stats => "stats",
      Self::Keyspace => "keyspace",
    }
  }
}

pub struct SortedSetPairCollection {
  pub entries: Vec<(f64, String)>,
}

/// RESP 整数应答统一解析（收敛重复的 parse + 错误包装）
fn to_i64(s: String) -> Result<i64> {
  s.parse().map_err(|_| Error::Other("Invalid integer".into()))
}

impl GarnetClient {
  // Basic Resp Commands

  /// libs/client/GarnetClientAPI/GarnetClientBasicRespCommands.cs:QuitAsync
  pub async fn quit_async(&self) -> Result<String> {
    self.execute_for_string_result_async(&["QUIT"]).await
  }

  /// libs/client/GarnetClientAPI/GarnetClientBasicRespCommands.cs:PingAsync
  pub async fn ping_async(&self) -> Result<String> {
    self.execute_for_string_result_async(&["PING"]).await
  }

  /// libs/client/GarnetClientAPI/GarnetClientBasicRespCommands.cs:StringGetAsync
  pub async fn string_get_async(&self, key: &str) -> Result<String> {
    self.execute_for_string_result_async(&["GET", key]).await
  }

  /// libs/client/GarnetClientAPI/GarnetClientBasicRespCommands.cs:StringSetAsync
  pub async fn string_set_async(&self, key: &str, value: &str) -> Result<bool> {
    let res = self
      .execute_for_string_result_async(&["SET", key, value])
      .await?;
    Ok(res == "OK")
  }

  /// libs/client/GarnetClientAPI/GarnetClientBasicRespCommands.cs:KeyDeleteAsync
  pub async fn key_delete_async(&self, key: &str) -> Result<bool> {
    let res = self.execute_for_string_result_async(&["DEL", key]).await?;
    Ok(res == "1")
  }

  /// libs/client/GarnetClientAPI/GarnetClientBasicRespCommands.cs:StringIncrement
  pub async fn string_increment(&self, key: &str) -> Result<i64> {
    to_i64(self.execute_for_string_result_async(&["INCR", key]).await?)
  }

  /// libs/client/GarnetClientAPI/GarnetClientBasicRespCommands.cs:StringDecrement
  pub async fn string_decrement(&self, key: &str) -> Result<i64> {
    to_i64(self.execute_for_string_result_async(&["DECR", key]).await?)
  }

  // Admin Commands

  /// libs/client/GarnetClientAPI/GarnetClientAdminCommands.cs:Save
  pub async fn save(&self) -> Result<bool> {
    let res = self.execute_for_string_result_async(&["SAVE"]).await?;
    Ok(res == "OK")
  }

  /// libs/client/GarnetClientAPI/GarnetClientAdminCommands.cs:Info
  pub async fn info(&self, info_section: InfoMetricsType) -> Result<String> {
    self
      .execute_for_string_result_async(&["INFO", info_section.as_str()])
      .await
  }

  /// libs/client/GarnetClientAPI/GarnetClientAdminCommands.cs:ReplicaOf
  pub async fn replica_of(&self, address: &str, port: u16) -> Result<String> {
    let mut port_buf = IntBuf::new();
    self
      .execute_for_string_result_async(&["REPLICAOF", address, port_buf.format(port)])
      .await
  }

  // List Commands

  /// libs/client/GarnetClientAPI/GarnetClientListCommands.cs:ListLeftPushAsync
  pub async fn list_left_push_async(&self, key: &str, elements: &[&str]) -> Result<i64> {
    let mut cmd = vec!["LPUSH", key];
    cmd.extend(elements);
    to_i64(self.execute_for_string_result_async(&cmd).await?)
  }

  /// libs/client/GarnetClientAPI/GarnetClientListCommands.cs:ListRightPushAsync
  pub async fn list_right_push_async(&self, key: &str, elements: &[&str]) -> Result<i64> {
    let mut cmd = vec!["RPUSH", key];
    cmd.extend(elements);
    to_i64(self.execute_for_string_result_async(&cmd).await?)
  }

  /// libs/client/GarnetClientAPI/GarnetClientListCommands.cs:ListRangeAsync
  pub async fn list_range_async(&self, key: &str, start: i32, stop: i32) -> Result<Vec<String>> {
    let (mut start_buf, mut stop_buf) = (IntBuf::new(), IntBuf::new());
    self
      .execute_for_string_array_result_async(&[
        "LRANGE",
        key,
        start_buf.format(start),
        stop_buf.format(stop),
      ])
      .await
  }

  /// libs/client/GarnetClientAPI/GarnetClientListCommands.cs:ListLengthAsync
  pub async fn list_length_async(&self, key: &str) -> Result<i64> {
    to_i64(self.execute_for_string_result_async(&["LLEN", key]).await?)
  }

  // Sorted Set Commands

  /// libs/client/GarnetClientAPI/GarnetClientSortedSetCommands.cs:SortedSetAddAsync
  pub async fn sorted_set_add_async(&self, key: &str, member: &str, score: f64) -> Result<i64> {
    let mut score_buf = FloatBuf::new();
    let res = self
      .execute_for_string_result_async(&["ZADD", key, score_buf.format(score), member])
      .await?;
    to_i64(res)
  }

  /// libs/client/GarnetClientAPI/GarnetClientSortedSetCommands.cs:SortedSetAddAsync
  pub async fn sorted_set_add_collection_async(
    &self,
    key: &str,
    entries: &SortedSetPairCollection,
  ) -> Result<i64> {
    // 分数先栈上格式化收敛为字符串，成员与分数以引用表组装，避免逐成员克隆
    let scores: Vec<String> = entries
      .entries
      .iter()
      .map(|(score, _)| FloatBuf::new().format(*score).to_string())
      .collect();
    let mut cmd: Vec<&str> = Vec::with_capacity(2 + entries.entries.len() * 2);
    cmd.push("ZADD");
    cmd.push(key);
    for ((_, member), score) in entries.entries.iter().zip(&scores) {
      cmd.push(score);
      cmd.push(member);
    }
    to_i64(self.execute_for_string_result_async(&cmd).await?)
  }

  /// libs/client/GarnetClientAPI/GarnetClientSortedSetCommands.cs:SortedSetRemoveAsync
  pub async fn sorted_set_remove_async(&self, key: &str, member: &str) -> Result<i64> {
    let res = self
      .execute_for_string_result_async(&["ZREM", key, member])
      .await?;
    to_i64(res)
  }

  /// libs/client/GarnetClientAPI/GarnetClientSortedSetCommands.cs:SortedSetLengthAsync
  pub async fn sorted_set_length_async(&self, key: &str) -> Result<i64> {
    to_i64(self.execute_for_string_result_async(&["ZCARD", key]).await?)
  }
}
