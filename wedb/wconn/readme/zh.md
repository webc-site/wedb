# wconn : RESP 客户端连接库

## 定位

wconn 是 workspace 的独立 RESP 客户端库，对标独立客户端程序集 `garnet/libs/client/Garnet.client.csproj`。与 C# 侧一致，它不是 server 的内部构件：server 自身也在运行时主链消费——`Garnet.server.csproj` 引用 client 工程，复制与迁移链路复用 `GarnetClientSession`。本仓库同样如此：`wedb/src/client.rs` 驱动 `GarnetClient`，`wedb/src/server/replication/replica_wire.rs` 驱动 `GarnetClientSession` 与复制帧编码器，`wedb/src/error.rs` 透传 `wconn::Error`。故 wconn 保持独立 workspace 成员与独立版本号，与 `libs/client` 作为独立程序集一一对应，不属于可裁剪的悬空死 crate。

## 模块组成

- `client`：`GarnetClient`，面向使用者的异步客户端门面（对标 `GarnetClient.cs`）
- `session`：`GarnetClientSession` 与复制线帧编码器 `encode_append_log_frame` / `encode_append_log_init_frame` / `encode_advance_time_frame`（对标 `ClientSession/`）
- `network`：TCP 管道、网络泵与回复分发（对标 `ClientTcpNetworkSender.cs` / `GarnetClientTcpNetworkHandler.cs` / `NetworkWriter.cs`）
- `parser`：`RespReadResponseUtils`，RESP 回复读取（对标 `RespReadResponseUtils.cs`）
- `api`：类型化辅助 `InfoMetricsType` / `SortedSetPairCollection`（对标 `GarnetClientAPI/`）
- `types`：网络泵共享的通道条目与往返槽位
- `error`：`Error` / `Result`

## 核心 API

- `GarnetClient`：连接 / 认证 / string、hash、set、sorted set、list、key、server 命令族；支持发后即忘与可等待往返
- `GarnetClientSession`：裸会话，命令管道化与批量 append-log 帧提交，服务复制路径
- `RespReadResponseUtils`：基于切片的 RESP 回复读取器

## 设计要点

- 基于 compio（线程每核）；客户端泵为 spawn 任务，经 crossfire 通道供料，对标 C# 基于 TCS 的完成管道，无阻塞等待
- 会话帧编码器置于本 crate 的原因：C# `GarnetClientSession` 被 server 复制栈共享；协议知识保留在客户端库内，不内联进 server crate

## 测试覆盖

`tests/main.rs` 基于本地回环服务承载集成夹具；复制端到端覆盖续于 `wedb/tests/replication_stream_e2e.rs` 与 `wedb/tests/replication_assembly_e2e.rs`。
