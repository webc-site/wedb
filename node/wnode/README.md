# wnode

单机网络服务底座，对标 [Garnet](https://github.com/microsoft/garnet) `libs/server` 单机半区：

- `resp` — RESP2 协议帧编码（RI.* 命令帧，用于 WAL 预写与主从复制流）
- `aof` — AOF 逻辑层：类型化条目头 + 零拷贝解码 + 回放分发 trait（对标 `AofHeader`/`AofProcessor` 职责）
- `service` — `NodeService`：编排存储引擎（`wkv::WedbStore`）与预写日志物理层（`waof::WalLog`），apply → log 顺序统一收口

依赖方向：`wnode → waof`（同 workspace）+ `wkv/wdev`（embed 引擎层），引擎层永不反向依赖本包。
