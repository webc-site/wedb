CLIENT 类型与 flags 判定缺 RemoteNodeId 门：集群形态下普通客户端全体误标 MASTER/REPLICA

来源：glm.net 第 6 条（分拣判定成立且待做）。取证基线：主仓 HEAD 50d1cb5f，行号为当下实况。

现状
- 判定体：/Users/z/git/db/wedb/wedb/wedb/wnode/src/resp/client_commands.rs:431-433
  current_client_type 当前实现只看两件事——self.cluster_session.is_some() 命中即按本地节点角色
  （cluster_provider.is_replica() :234）返 ClientType::Master 或 Replica，与是否节点间连接无关。
- 消费面：同文件 :416 current_client_view 复用该判定产出 flags 字符，故 CLIENT LIST（含 TYPE 过滤）、
  CLIENT INFO、CLIENT KILL TYPE、INFO clients 的 connected_clients 分类全部继承该误判。
- 成因：rust 集群形态下每个会话都统一装配 cluster_session
  （/Users/z/git/db/wedb/wedb/wedb/src/server/boot.rs 的 with_cluster 装配链，会话侧
  inject_dependencies 透传），is_some 因此不再是「节点间连接」的判据。
- 数据已在位，只缺切面：
  /Users/z/git/db/wedb/wedb/wedb/src/server/cluster_session/mod.rs:45
  `pub(super) remote_node_id: RwLock<Option<u128>>`，:87 初始 None，仅 CLUSTER GOSSIP 建链时写入
  （同目录 basic.rs:194）与读取（basic.rs:225）。会话侧切面
  /Users/z/git/db/wedb/wedb/wedb/wnode/src/cluster_session.rs:70 起的 ClusterSessionFace
  未暴露该字段，故 client_commands 无从判空——这是唯一缺口。
- 远端角色查询口也已在位：ClusterProvider::is_replica_node(node_id)
  （/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_provider.rs:1354，切面
  /Users/z/git/db/wedb/wedb/wnode/src/cluster_provider.rs:63 trait 法 / :208 实现 / :377 门面转发）。
- 后果：CLIENT LIST TYPE NORMAL 恒空、TYPE MASTER 返回全部客户端、CLIENT KILL TYPE MASTER
  会杀掉普通客户端、INFO clients 的 blocked/master/replica 计数失真。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/server/Resp/ClientCommands.cs:443-461
  CLIENT KILL 的 TYPE 过滤谓词以 `clusterSession?.RemoteNodeId` 非空为前提再比 provider.IsReplica(nodeId)。
- /Users/z/git/db/wedb/garnet/libs/server/Resp/BasicCommands.cs:1968-1986
  WriteClientInfo 的 flags 臂同构：仅当 clusterSession.RemoteNodeId 有值才写 M/S，否则按 PUBSUB/NORMAL。
- CLIENT LIST TYPE 过滤链同口径：ClientCommands.cs:66-84。
- RemoteNodeId 写入点：/Users/z/git/db/wedb/garnet/libs/cluster/Session/RespClusterBasicCommands.cs:413-414
  （CLUSTER GOSSIP 时 SetClusterSessionRemoteNodeId），与 rust basic.rs:194 一一对位。

修法
1. ClusterSessionFace 增 `remote_node_id(&self) -> Option<u128>` 只读访问口
   （wkv 侧无环依赖：ClusterSession 已有 RwLock<Option<u128>> 字段，转发一次即可），
   Noop/测试切面补 None 实现，禁另立第二份镜像字段。
2. current_client_type 改为：取 remote_node_id，None → 继续走本地 pubsub/normal 判定；
   Some(id) → 按 `cluster_provider.is_replica_node(id)` 返 Replica/Master。
   本地 provider.is_replica() 在客户端类型判定面退役（它表达的是「本节点是从库」，
   不是「这条连接是从库发来的」，语义不同源）。
3. current_client_view（:416）随判定体单点收敛，禁在 view 侧再抄一份 M/S 推断。
4. 若个别调用方确需「本节点角色」，另名函数表达，不得复用 client 类型判定。

优先级
功能缺口（客户端分类是运维面契约：误判致 CLIENT LIST/KILL 语义错位且可误杀连接）。

协调
- 只动判定体与切面访问口，不碰 RESP 帧产出与 CLIENT 命令族解析面（resp null/RESP3 在途票不涉）。
- cluster_session 域在途票：task/ing/info-replication-slave-line-endpoint.md（INFO 渲染）、
  task/ing/spublish-cross-node-shard-delivery.md（发布路由）与本票同文件不同事实，
  同文件开工需错开；basic.rs:194 的写入侧本票零改动。

验收
- 集群形态下普通客户端：CLIENT LIST TYPE NORMAL 列出、TYPE MASTER 不含、flags 无 M/S。
- gossip 连接（remote_node_id 已写）：TYPE MASTER/REPLICA 各按其远端节点角色命中，
  与 provider.is_replica() 当下值无关（用主端发起的 gossip 连接断言不判 Replica）。
- 现有 CLIENT 相关测试期望按新口径复核，禁止仅改期望值放行；
  新增断言：同一节点上普通连接与 gossip 连接并存时 TYPE 过滤互斥且并集为全量。
- 验证纪律：仅 cargo check --workspace --all-targets（私有 target 目录）零 error 零 warning；
  test.sh/clippy 由中央整合轮执行。

## 细化方案（实现代理核备 2026-09-19）

甄别结论：票据事实全部成立（票据内 wnode 路径多写一层 wedb 前缀，正确路径为
wedb/wnode/...，不影响判定）。
- 判定体 wedb/wnode/src/resp/client_commands.rs:431 current_client_type 现状 =
  cluster_session.is_some() + 本地 provider.is_replica()，与远端连接无关，误判属实。
- C# 三消费面（ClientCommands.cs KILL 谓词 :443-461 / LIST TYPE :66-84 /
  BasicCommands.cs WriteClientInfo :1968-1986）一致以
  `clusterProvider is not null && clusterSession?.RemoteNodeId is not null` 双门出 M/S。
- HELLO role（BasicCommands.cs:1826）与 FLUSH 只读门（:1038/:1060）是「本节点角色」
  语义，rust resp_server_session.rs:2452 与 resp/basic_commands/mod.rs:132
  flush_replica_read_only_gate 与 C# 同构正确，本票不动（票修法 4 场景）。
- 消费面已单点：LIST/KILL/INFO 全走 ClientView.client_type
  （client_commands.rs:517/:547 + resp_server_session.rs:2382 write_client_info_state），
  flags 字符单点 consumer_registry.rs:86 flags_char，只改判定体即全网生效。
- ClusterProviderFace 全方法有默认体；ClusterSessionFace 已有默认方法先例
  （take_pending_slow 等），新访问口给默认 None 实现，StubClusterSession 桩零改动。

改动三处
1. wedb/wnode/src/cluster_session.rs ClusterSessionFace 增默认方法
   `fn remote_node_id(&self) -> Option<u128> { None }`，
   映射 libs/cluster/Session/ClusterSession.cs:RemoteNodeId。
2. wedb/wedb/src/server/cluster_session/mod.rs impl ClusterSessionFace 增转发
   `*self.remote_node_id.read()`（字段权威注释已在 :44，转发处引字段）。
3. wedb/wnode/src/resp/client_commands.rs current_client_type 重写：
   cluster_session 与 cluster_provider 双在位（Option::zip 直译 C# 双条件）且
   remote_node_id Some(id) → is_replica_node(id) ? Replica : Master；
   否则 is_subscription_session ? Pubsub : Normal。本地 is_replica() 在此判定面退役。
   同步更新该函数与 current_client_view 注释口径（远端节点角色臂）。

测试（wnode/tests/resp_server_session_tests.rs；cargo check --all-targets 编译验证，
运行归中央轮）
- StubClusterSession 增 remote_node_id 态（AtomicU128，0=None）并覆盖 trait 方法；
  新增 StubProvider 仅覆盖 is_replica_node（trait 全默认体，桩两方法即成形）。
- pump CLIENT INFO 断言 flags：普通会话 N；gossip 确立 + is_replica_node=false → M；
  gossip 确立 + is_replica_node=true → S；provider 缺席（remote Some）→ N（C# 双门）。
- 互斥断言：同注册视图下 ClientListFilter::Type(Master)/Type(Normal) 对普通/gossip
  两类视图判集互斥。
