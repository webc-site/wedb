use itoa::Buffer as IntBuf;
use wresp::{metrics::InfoMetricsType, resp_memory_writer::format_double};
use zmij::Buffer as FloatBuf;

use crate::{Error, Result, client::GarnetClient};

pub struct SortedSetPairCollection {
  pub entries: Vec<(f64, String)>,
}

/// RESP 整数应答统一解析（收敛重复的 parse + 错误包装）
fn to_i64(s: String) -> Result<i64> {
  s.parse().map_err(|_| Error::InvalidInteger)
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

  /// MGET 多键臂：C# 侧 `GarnetClientBasicRespCommands.cs` 内 StringGetAsync 是同名
  /// 重载族（单键 / 单键+token / 键数组 / 键数组+token 四枚），本仓锚点按符号名登记，
  /// 1:1 挂载唯一在 [`GarnetClient::string_get_async`]，此处不复挂
  pub async fn string_get_multi_async(&self, keys: &[&str]) -> Result<Vec<String>> {
    let mut cmd = Vec::with_capacity(keys.len() + 1);
    cmd.push("MGET");
    cmd.extend_from_slice(keys);
    self.execute_for_string_array_result_async(&cmd).await
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

  /// DEL 多键臂，返回实际删除键数：C# 侧 `GarnetClientBasicRespCommands.cs` 内
  /// KeyDeleteAsync 是八枚同名重载族（string/Memory<byte> × 单键/键数组 ×
  /// 带/不带 token），本仓锚点按符号名登记，1:1 挂载唯一在
  /// [`GarnetClient::key_delete_async`]，此处不复挂
  pub async fn key_delete_multi_async(&self, keys: &[&str]) -> Result<i64> {
    let mut cmd = Vec::with_capacity(keys.len() + 1);
    cmd.push("DEL");
    cmd.extend_from_slice(keys);
    to_i64(self.execute_for_string_result_async(&cmd).await?)
  }

  /// libs/client/GarnetClientAPI/GarnetClientBasicRespCommands.cs:StringIncrement
  pub async fn string_increment(&self, key: &str) -> Result<i64> {
    to_i64(self.execute_for_string_result_async(&["INCR", key]).await?)
  }

  /// INCRBY 带增量臂：C# 侧 `GarnetClientBasicRespCommands.cs` 内 StringIncrement 是
  /// 八枚同名重载族（单键/带增量 × string/Memory<byte> × 带/不带 token），本仓锚点
  /// 按符号名登记，1:1 挂载唯一在 [`GarnetClient::string_increment`]，此处不复挂
  pub async fn string_increment_by_async(&self, key: &str, delta: i64) -> Result<i64> {
    let mut delta_buf = IntBuf::new();
    to_i64(
      self
        .execute_for_string_result_async(&["INCRBY", key, delta_buf.format(delta)])
        .await?,
    )
  }

  /// libs/client/GarnetClientAPI/GarnetClientBasicRespCommands.cs:StringDecrement
  pub async fn string_decrement(&self, key: &str) -> Result<i64> {
    to_i64(self.execute_for_string_result_async(&["DECR", key]).await?)
  }

  /// DECRBY 带减量臂：C# 侧 `GarnetClientBasicRespCommands.cs` 内 StringDecrement 是
  /// 八枚同名重载族（单键/带减量 × string/Memory<byte> × 带/不带 token），本仓锚点
  /// 按符号名登记，1:1 挂载唯一在 [`GarnetClient::string_decrement`]，此处不复挂
  pub async fn string_decrement_by_async(&self, key: &str, delta: i64) -> Result<i64> {
    let mut delta_buf = IntBuf::new();
    to_i64(
      self
        .execute_for_string_result_async(&["DECRBY", key, delta_buf.format(delta)])
        .await?,
    )
  }

  // Admin Commands

  /// libs/client/GarnetClientAPI/GarnetClientAdminCommands.cs:Save
  pub async fn save(&self) -> Result<bool> {
    let res = self.execute_for_string_result_async(&["SAVE"]).await?;
    Ok(res == "OK")
  }

  /// libs/client/GarnetClientAPI/GarnetClientAdminCommands.cs:Info
  ///
  /// 段名口径与 C# 逐字节对齐：C# 以 `InfoCommandUtils.GetRespFormattedInfoSection`
  /// 出预格式化 bulk string，默认段 SERVER 返回 null（即只发 `INFO`，服务端回全部
  /// 默认段）。rust 客户端以裸 token 出帧，故此处取
  /// [`InfoMetricsType::as_cs_name`] 大写段名等价承接，SERVER 同样省略段参数
  pub async fn info(&self, info_section: InfoMetricsType) -> Result<String> {
    if info_section == InfoMetricsType::Server {
      return self.execute_for_string_result_async(&["INFO"]).await;
    }
    self
      .execute_for_string_result_async(&["INFO", info_section.as_cs_name()])
      .await
  }

  /// libs/client/GarnetClientAPI/GarnetClientAdminCommands.cs:ReplicaOf
  ///
  /// REPLICAOF 下发面的唯一命令组装点（C# 原生客户端同名实现 1:1 对标，
  /// 端口 int 口径）：集群控制面 facade 一律委托此处，不得二次手抄序列
  pub async fn replica_of(&self, address: &str, port: i32) -> Result<String> {
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
      .execute_for_string_result_async(&["ZADD", key, format_double(score, &mut score_buf), member])
      .await?;
    to_i64(res)
  }

  /// 集合批量添加（对应 C# SortedSetAddAsync(key, collection) 批量重载）
  pub async fn sorted_set_add_collection_async(
    &self,
    key: &str,
    entries: &SortedSetPairCollection,
  ) -> Result<i64> {
    // 分数先栈上格式化收敛为字符串，复用 FloatBuf 避免重复分配
    let mut buf = FloatBuf::new();
    let scores: Vec<String> = entries
      .entries
      .iter()
      .map(|(score, _)| format_double(*score, &mut buf).to_string())
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

  /// libs/client/GarnetClientAPI/GarnetClientSortedSetCommands.cs:SortedSetRemove(key, members)
  /// 批量删员，返回实际移除成员数
  pub async fn sorted_set_remove_multi_async(&self, key: &str, members: &[&str]) -> Result<i64> {
    let mut cmd = Vec::with_capacity(members.len() + 2);
    cmd.push("ZREM");
    cmd.push(key);
    cmd.extend_from_slice(members);
    to_i64(self.execute_for_string_result_async(&cmd).await?)
  }

  /// libs/client/GarnetClientAPI/GarnetClientSortedSetCommands.cs:SortedSetLengthAsync
  pub async fn sorted_set_length_async(&self, key: &str) -> Result<i64> {
    to_i64(
      self
        .execute_for_string_result_async(&["ZCARD", key])
        .await?,
    )
  }
}
