//! GarnetClient 管理命令面（生产存活面，r7c 裁决后的库门面）
//!
//! 原镜像 C# GarnetClientAPI 全家（基础 RESP / List / SortedSet 便捷封装 22
//! 方法 + SortedSetPairCollection）在本仓零生产消费——唯一调用方是本 crate
//! 自测；便捷封装语义可由 [`GarnetClient::execute_for_string_result_async`]
//! 族底层执行口直接表达，已删（测试侧改走底层口，见 wconn/tests/main.rs）。
//! 存留单方法：`replica_of` 为 REPLICAOF 下发面唯一命令组装点（wedb 集群
//! 控制面 facade 委托）；INFO 无便捷壳，段名口径经底层执行口直发（见
//! wconn/tests/main.rs）。

use itoa::Buffer as IntBuf;

use crate::{Result, client::GarnetClient};

impl GarnetClient {
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
}
