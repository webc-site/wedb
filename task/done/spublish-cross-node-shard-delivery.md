SPUBLISH 跨节点接收端路由错向：共用 broker.publish 致本分片其余节点的分片订阅者收不到

来源：glm.net 第 7 条（分拣判定成立且待做）。取证基线：主仓 HEAD 50d1cb5f，行号为当下实况。

现状（接收侧错向，发送侧已正确）
- 入口合并：/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_session/mod.rs:246
  `RespCommand::ClusterPublish | RespCommand::ClusterSpublish =>` 单臂共调
  同目录 basic.rs:609 network_cluster_publish——两种命令在接收端不再区分。
- 投递口：basic.rs:609-626 一律 `broker.publish(...)`
  （/Users/z/git/db/wedb/wedb/wpubsub/src/subscribe_broker.rs:422），入 pending 队列后由
  consume_pending → broadcast（同文件 :145-172）分发；broadcast 只遍历
  subscriptions 与 pattern_subscriptions，不触达 :84 的 shard_subscriptions。
- 分片投递面在 rust 独立存在且已实现：subscribe_broker.rs:178 broadcast_shard、:411 publish_shard_now
  （本节点 SSUBSCRIBE 直发即走 :411，形态正常）。
- 发送侧已是分片定向：/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_manager.rs:246-257
  try_cluster_publish_async 经 get_node_ids_for_shard 只把 SPUBLISH 转给本分片其余节点，
  即「路由按分片、落地按全图」——远端收到后既不进分片订阅表，又错误地投给了本节点普通通道
  与模式订阅者（同名频道撞车时串台）。
- 兜底缺失：跨节点 SPUBLISH 无任何端到端用例（tests 面仅命令解析断言）。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/cluster/Session/RespClusterBasicCommands.cs:505-526
  NetworkClusterPublish 对 PUBLISH 与 SPUBLISH 确实不区分，一律 subscribeBroker.PublishNow。
- 但 C# 的分片订阅不是独立结构：/Users/z/git/db/wedb/garnet/libs/server/PubSub/SubscribeBroker.cs
  全文件只有 subscriptions / patternSubscriptions 两张表（SSUBSCRIBE 复用普通频道图），
  所以「接收端不区分」在 C# 是无害的，远端 SSUBSCRIBE 订阅者按普通频道命中同一张表。
- rust 按 doc/zh 与 SKILL 的命名空间/分片隔离模型自建第三张表
  （subscribe_broker.rs:84 shard_subscriptions），照抄 C# 的「不区分」即把消息投错了图。
  该条属「C# 前提在 rust 不成立」的转写错配，不是刻意差异。

修法
1. network_cluster_publish 按命令分派：ClusterSpublish → 分片投递面，ClusterPublish → 现路径；
   分派在接收端一处完成，禁在 broker 内部再按频道名或调用序推断。
2. 分片侧优先复用既有 pending 消费模型：给 pending 条目补一个投递域标记（普通/分片），
   consume_pending → 按标记分别走 broadcast 与 broadcast_shard（:178）；
   或最小改动形态——SPUBLISH 直调 publish_shard_now（:411）同步投递，
   代价是失去与其它发布的同批序，须在注释里点名该取舍。二者择一，禁两条并存。
3. 串台防护：分片投递面不得同时向 subscriptions/pattern_subscriptions 复制一份
   （现 broadcast 的误投即此副作用），验收含同名频道双订阅形态断言。
4. 补端到端用例：两节点同分片，A 节点 SSUBSCRIBE ch，B 节点 SPUBLISH ch，
   断言 A 收到且 A 上普通 SUBSCRIBE ch 的对照订阅者收不到；反向 CLUSTER PUBLISH ch
   断言只达普通订阅者。

优先级
功能缺口（跨节点分片订阅整体不可达，属命令族功能缺失而非打磨）。

协调
- 与 task/ing/pubsub-namespace-isolation.md（订阅表按 ns/db 隔离）同表域：两票都动
  subscribe_broker 投递面，命名空间维度与分片投递维度须一次收敛，勿先改签名再改路由两次触碰。
- 与 task/ing/client-type-remote-node-id-gate.md 同文件 cluster_session/basic.rs 不同事实，开工错开。
- 发送侧 cluster_manager.rs:246-257 的分片定向已正确，本票零改动。

验收
- 上述新增端到端用例通过（本机多节点测试形态）。
- broadcast 与 broadcast_shard 职责互斥：grep 确认 network_cluster_publish 无「两投递面同调」。
- 现有跨节点 PUBLISH 用例不回退；SPUBLISH 解析面用例期望不变。
- 验证纪律：仅 cargo check --workspace --all-targets（私有 target 目录）零 error 零 warning。

## 实现方案（f57-spublish，甄别基线 dev HEAD 2026-09-19）

甄别核实（行号以当前 dev 为准，票据原行号有漂移但事实全部成立）：
- mod.rs:254 单臂共调、basic.rs:660 network_cluster_publish 一律 broker.publish：成立。
- broker 三表分离（subscribe_broker.rs:109 shard_subscriptions）、broadcast(:177) 只投
  普通+模式、broadcast_shard(:210) 只投分片、publish(:456) 入 pending、publish_shard_now(:445)：
  全部成立。
- 发送侧 cluster_manager.rs:298 分片定向（get_node_ids_for_shard + is_spublish 透传
  node_connection.rs:189 → CLUSTER SPUBLISH 帧）：成立，本票零改动。
- C# SubscribeBroker.cs:23-24 仅 subscriptions/patternSubscriptions 两张表（grep shard 零命中），
  NetworkClusterPublish 不区分在 C# 无害；rust 三表分离后照抄即错向。票据判断正确。
- ns 隔离已落地：发送端 network_publish 折叠隔离键后转发（session_commands.rs:464-472），
  收端以隔离键原样入 broker，分片面同样以隔离键投递，键一致无需额外处理。

修法选型：pending 域标记（票据修法 1，非直调 publish_shard_now）：
- 收端与 C# Publish 入队形态同构（C# 收端唯一入口即入队），保持同批序；
- 直调 now 会让收端 PUBLISH/SPUBLISH 投递路径形态分裂且失去批次序。

改动面（三处，一处定义）：
1. wpubsub/src/subscribe_broker.rs
   - PendingEntry: tuple → enum { Standard(k,v), Shard(k,v) }（域标记）
   - 私有 enqueue 单点（disposed/is_idle 早退 + push + notify），publish() 收敛到 enqueue
   - 新增 publish_shard()（分片域入队；rust 自有面，C# 无对应）
   - consume_pending() 按 match 域分派 broadcast / broadcast_shard
2. wedb/src/server/cluster_session/basic.rs network_cluster_publish
   - cmd == ClusterSpublish → broker.publish_shard；否则 broker.publish（与
     cluster_manager.rs:304 的 cmd == Publish 判定形态一致）
   - 文档注明 C# 不区分的前提（单图）与 rust 三表分域差异
3. 测试
   - wpubsub 内嵌单测：publish_shard 入队 consume_pending 只投分片域、普通域不串台
   - wedb/tests/cluster_resp_session.rs 新增跨节点接收面用例（two_primary_provider
     + 真 broker 形态，即现有 cluster_publish_local_delivery_success 同款本机多节点形态）：
     同 broker 上 SSUBSCRIBE ch 与 SUBSCRIBE ch 并存 → CLUSTER SPUBLISH 0:ch 仅分片订阅者
     收 smessage；CLUSTER PUBLISH 0:ch 仅普通订阅者收 message；双向断言不串台。
