[English](#en) | [中文](#zh)

---

<a name="en"></a>

# wconn : RESP Client Connection Library

- [Positioning](#positioning)
- [Module Layout](#module-layout)
- [Core API](#core-api)
- [Design Notes](#design-notes)
- [Test Coverage](#test-coverage)

## Positioning

wconn is the standalone RESP client-side library of the workspace, mirroring the independent client assembly `garnet/libs/client/Garnet.client.csproj`. Like its C# counterpart it is not an internal chunk of the server: the server itself consumes it on the runtime path — `Garnet.server.csproj` references the client project, and replication / migration reuse `GarnetClientSession` on the primary and replica sides. In this workspace `wedb` consumes `wconn` the same way: `src/client.rs` drives `GarnetClient`, `src/server/replication/replica_wire.rs` drives `GarnetClientSession` plus the wire frame encoders, and `src/error.rs` forwards `wconn::Error`. The crate stays a separate workspace member with an independent version, exactly as `libs/client` is a separate assembly.

## Module Layout

- `client`: `GarnetClient`, the end-user async client facade (mirrors `GarnetClient.cs`)
- `session`: `GarnetClientSession` and the replication wire frame encoders `encode_append_log_frame` / `encode_append_log_init_frame` / `encode_advance_time_frame` (mirrors `ClientSession/`)
- `network`: TCP plumbing, the network pump and reply dispatch (mirrors `ClientTcpNetworkSender.cs` / `GarnetClientTcpNetworkHandler.cs` / `NetworkWriter.cs`)
- `parser`: `RespReadResponseUtils`, RESP reply reading (mirrors `RespReadResponseUtils.cs`)
- `api`: typed helpers `InfoMetricsType` / `SortedSetPairCollection` (mirrors `GarnetClientAPI/`)
- `types`: channel items and roundtrip slots shared by the pump
- `error`: `Error` / `Result`

## Core API

- `GarnetClient`: connect / auth / string, hash, set, sorted set, list, key, server commands; fire-and-forget and awaitable roundtrips
- `GarnetClientSession`: raw session with command piping and batched append-log frame submission for the replication path
- `RespReadResponseUtils`: slice-based RESP reply readers

## Design Notes

- Built on compio (thread-per-core); the client pump is a spawned task fed by crossfire channels, mirroring the C# TCS-based completion pipeline with no blocking waits
- The session frame encoders exist because C# `GarnetClientSession` is shared by the server replication stack; keep them here rather than inlining protocol knowledge into the server crate

## Test Coverage

`tests/main.rs` hosts integration fixtures over a live loopback server; replication end-to-end coverage continues in `wedb/tests/replication_stream_e2e.rs` and `wedb/tests/replication_assembly_e2e.rs`.


---

<a name="zh"></a>

# wconn : RESP 客户端连接库

- [定位](#定位)
- [模块组成](#模块组成)
- [核心 API](#核心-api)
- [设计要点](#设计要点)
- [测试覆盖](#测试覆盖)

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

