# 复制域 6 个启动旋钮接入 NodeArgs 与配置

来源：next/zcode-r6-cli.md 问题 1

## 问题

复制域六个启动参数在 RuntimeServerOptions 中恒取 Default：
replica_sync_timeout_secs: 5
replica_attach_timeout_secs: 60
replica_sync_delay_ms: 5
aof_sync_max_lag_bytes: -1
aof_tail_witness_freq_ms: 10
cluster_replication_reestablishment_timeout: 0
命令行与配置文件均不可覆盖设置。

## 涉及路径

- wedb/wconf/src/runtime_server_options.rs
- wedb/wconf/src/node_options.rs
- wedb/wnode/src/server.rs
- wedb/wedb/src/server/boot.rs

## 解决建议

1. 在 NodeArgs 中增加对应参数开关（None 时保留默认值）。
2. 在 runtime_server_options 构造处接入命令行与配置文件的值。
3. 超时参数 <=0 时作为无限超时哨兵处理。
