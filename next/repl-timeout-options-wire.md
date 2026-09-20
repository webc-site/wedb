# 复制超时配置项 ReplicaSyncTimeout 与 ReplicaAttachTimeout 支持

来源：next/zcode-r5-repl.md 问题 7

## 问题

C# ReplicaSyncTimeout 与 ReplicaAttachTimeout 可在慢盘或慢网络场景下调大超时窗口。
Rust 侧以硬编码常量 REPLICA_SYNC_TIMEOUT 与 REPL_ATTACH_TIMEOUT 对齐默认值（5s / 60s），
缺少配置文件和参数覆盖支持。

## 涉及路径

- wedb/wedb/src/server/replication/replica_wire.rs
- wedb/wconf/src/server_config_type.rs
- libs/server/Servers/GarnetServerOptions.cs

## 解决建议

1. 在 ServerConfig / ReplicationOptions 中接入 replica_sync_timeout_secs 与 replica_attach_timeout_secs 配置。
2. 将硬编码常量改为优先读取配置，未配置时回退默认常量。
