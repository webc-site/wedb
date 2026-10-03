# replication-egress-client-name-observability（P3，源自 task/issue/replication-egress-client-name-observability.md）

## 终态注记（合入哈希：dee2edb / 收口形态：出站三链 clientName 诊断名透传）
- 合入哈希：dee2edb
- 收口形态：
  1. GarnetClient::with_auth 新增 client_name: Option<String> 形参对标 C# 构造重载 clientName 形参位，并提供 client_name(&self) 访问器。
  2. 复制三链出站客户端精准对位传诊断名：
     - assembly.rs:226 传 TryReplicateDiskbasedSyncAsync（对标 ReplicaDiskbasedSync.cs:115）
     - replication_sync_manager.rs:346 传 AofSyncTask:(endpoint)（对标 AofSyncTask.cs:141）
     - replica_sync_session.rs:153 传 SendCheckpointAsync（对标 ReplicaSyncSession.cs:106）
  3. 其余链保持 None，对标 C# 无名形态。
  4. tests/replication_egress_client_name.rs 集成锁测：断言三链 clientName 属性与 CLIENT LIST 服务端回显。

## 甄别结论：通过（2026-09-28 独立审核现码复验，P3 维持，清单与映射按审核修正）

审核修正三处（承接后以此为准）：
1. 错列摘出：C# ReplicaDisklessSync.cs 的 gcs 构造（约 :148-155）不带 clientName（全仓实参恰 4 处：复制三链 + GarnetServerNode.cs:94 Gossip-{endpoint}，后者 rust 已对齐 node_connection.rs:58-65），故 rust replica_diskless_sync.rs:116 对位链与 C# 同为无名，不属契约落差，从执行清单摘出；若想补名属自选观测增强，须显式标注、不得冒充契约对账。
2. 漏列补入：C# ReplicaSyncSession.cs 的 rust 真身是 wedb/wedb/src/server/replication/replica_sync_session.rs:153 egress_client（文件头自证对标，消费点 :738 checkpoint send 即 SendCheckpointAsync 链），必须列入落点。
3. C# SETNAME 真发链核实：GarnetClient.cs:165 构造 ["SETNAME", name] 载荷，:244-249 ConnectAsync 尾段先 CLIENT SETINFO 后 CLIENT SETNAME 真发，与 rust wconn network/mod.rs:210-212 同序，机制在位。

三链三值对位映射（唯一落点清单）：
- wedb/wedb/src/server/replication/assembly.rs:226 → TryReplicateDiskbasedSyncAsync
- wedb/wedb/src/server/replication/diskless_replication/replication_sync_manager.rs:346 → AofSyncTask-{idx}:(endpoint)
- wedb/wedb/src/server/replication/replica_sync_session.rs:153 → SendCheckpointAsync

idx 取值锚提示：C# 的 idx 是 AofSyncTask 槽号 physicalSublogIdx，rust 扇出位 DisklessSyncSession（diskless_replication/replica_sync_session.rs:60）无现成同义字段；执行时不得为凑 idx 新增会话字段或引入魔数，以 (endpoint) 锚定观测可辨识性即满足，idx 有现成驱动槽号可取则取，无则省。

边界不动面：facade :142 缺省 "wedb" 恒发与 C# null 不发的既有整体差异本票不触碰（波及全部无名链行为，超票面）；deviations.md §43/§60 为 SETNAME 收端解析面裁决，与出站诊断名对账无涉。

原票面：

复制域出站客户端缺 clientName 诊断名形参位：CLIENT LIST 观测名失真

定级：P3（纯观测面，无权限/路由影响；先于 egress TLS 票存在，非该票引入）

问题分析：
1. Garnet 契约对齐
C# 复制域出站客户端构造带 clientName 诊断名并经握手 CLIENT SETNAME 真发（GarnetClient.cs:165/:244-249，先 SETINFO 后 SETNAME）：ReplicaDiskbasedSync.cs:115 传 nameof(TryReplicateDiskbasedSyncAsync)、AofSyncTask.cs:141 传 "AofSyncTask-{idx}:(endpoint)"、ReplicaSyncSession.cs:106 传 nameof(SendCheckpointAsync)，运维经 CLIENT LIST 可直读每条复制链身份。
2. 工程现状确证
rust facade wedb/wedb/src/client.rs:64-70 with_auth 构造不含 client_name 形参，缺省名恒 "wedb"（:142）；复制出站链 assembly.rs:226、diskless_replication/replication_sync_manager.rs:346、replica_sync_session.rs:153 全走 with_auth，诊断名缺位；wconn 侧握手参数链已支持 client_name 透传（wconn/src/client.rs:178 入 ConnectParams，network/mod.rs:210-212 握手真发 SETINFO+SETNAME），仅 facade 形参面缺位，纯上游透传缺失非机制缺失。
3. 逻辑危害确证
主端 CLIENT LIST 中复制链与 gossip 链均显示同一名 "wedb"，复制排障（区分扇出/检查点/INITIATE 链）失去身份判据；无安全面影响（SETNAME 值不参与任何门判定，收端解析面既有裁决 §43/§60 不涉出站），故定 P3 观测面对账项。

涉及代码：
rust 文件与函数：
wedb/wedb/src/client.rs:GarnetClient::with_auth（:64-70，缺 client_name 形参）/ 缺省名消费点（:142）
wedb/wedb/src/server/replication/assembly.rs:226（INITIATE_REPLICA_SYNC 链构造）
wedb/wedb/src/server/replication/diskless_replication/replication_sync_manager.rs:346（扇出链构造）
wedb/wedb/src/server/replication/replica_sync_session.rs:153（检查点发送链 egress_client）

对应 c# 文件与函数：
garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:115（clientName 实参）
garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:141
garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:106
garnet/libs/client/GarnetClient.cs:165/:244-249（SETNAME 载荷构造与真发臂）

精炼执行方案：
1. facade 补 client_name 形参（with_auth 增第 4 形参对位 C# 构造重载 clientName 形参位，或三链调用点改走既有 with_config 全参构造），三链按上方映射对位传值；replica_diskless_sync.rs:116 不动（C# 同位无名）
2. 测试验证点：wconn 握手参数断言三链 SETNAME 值（network/mod.rs:217 tests 模块先例）；CLIENT LIST 回显含诊断名（regress 端到端）
