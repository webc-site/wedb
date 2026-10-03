# wedb-replication-egress-clients-missing-tls（P1，源自 task/issue/replication-egress-clients-missing-tls-and-network-pool.md）

## 甄别结论：通过（2026-09-28 主控现码复验）
票面池缺一项已被在先合入补齐（三处 set_network_pool 均在位：assembly.rs:227、
replica_diskless_sync.rs:120、replication_sync_manager.rs 扇出臂），现存缺口仅
TLS 面：三条复制出站链建连不带 TlsClientOptions，集群 TLS 开启时凭据走明文。

## 问题
C# 复制域全部出站客户端四锚均带 tlsOptions 与 GetNetworkPool 双形参：
- garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:111-112
- garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDisklessSync.cs:148-149
- garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:102-103
- garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:137-138
（注：issue 票面前两条路径写作 PrimaryOps/DiskbasedReplication/ 系笔误，上列为现树实位。）

rust 侧 apply_tls! 现仅 2 个复制域调用点（client.rs:599 定义，replica_sync_session.rs:159、
assets.rs:54；另有 failover_session.rs:44、gossip/node_connection.rs:73），以下三处
GarnetClient::with_auth 建连无 TLS 臂：
- wedb/wedb/src/server/replication/assembly.rs:222（INITIATE_REPLICA_SYNC 发起，
  对位 C# ReplicaDiskbasedSync.cs:112）
- wedb/wedb/src/server/replication/replica_diskless_sync.rs:115（ATTACH_SYNC attach，
  对位 C# ReplicaDisklessSync.cs:149）
- wedb/wedb/src/server/replication/diskless_replication/replication_sync_manager.rs:340
  （无盘扇出客户端，对位 C# AofSyncTask.cs:138 或同族扇出锚，席位现场核 C# 对位件后挂准锚）

## 方案
三处建连后、连接前补 apply_tls!(client, provider)（Arc 包装位若宏形态不合，按
replica_sync_session.rs:159 先例适配，不改宏语义）；文档注释补对应 C# 锚。

## 验证
新增/扩展集成测试（tests/ 下）：TLS 档位下三链建连走 TLS（server 证书握手成功、
明文客户端被拒），复用 replica_sync_session 既有 TLS 测试夹具先例；子代理只跑
cargo check --workspace --all-targets 与该专测，门禁归主控。

## 收口记录（2026-09-28 主控）
- 合入：源提交 e8be8476 → dev 合并 6389c39f（`--no-ff`）。
- 收口形态：三处建连位补 `apply_tls!(client, provider)` 单源透传，不改宏语义
  （assembly.rs:232、replica_diskless_sync.rs:122、replication_sync_manager.rs:351；
  扇出 Arc 包装位按 `with_auth` → `apply_tls!` → `Arc::new` 次序重排，先例
  replica_sync_session.rs:159）。锚注以 C# 双形参（networkPool + tlsOptions）叙述位挂载，
  `AofSyncTask.cs:AofSyncTask` 单点锚在扇出位。
- 测试：新增 `wedb/wedb/tests/replication_egress_tls_gate.rs`（392L，`#![cfg(feature = "tls")]`）
  三臂对拍——明文↔明文建连位通畅、明文↔TLS 服务面建连位拒绝、配出站 TLS 后越过建连位；
  覆盖 INITIATE / ATTACH_SYNC / 无盘扇出三链。夹具面新增
  `wnode_tls_test::test_client_tls()`（对位 C# `ServerCertificateRequired=false`）。
- 票面勘误落实：issue 原两条路径笔误（PrimaryOps/DiskbasedReplication/）已按现树实位改写；
  `set_network_pool` 在先合入已在位，本票仅补 TLS 面，与甄别结论一致。
