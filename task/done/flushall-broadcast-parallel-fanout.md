FLUSHALL_NS 广播改并行扇出：串行逐节点 await 致 N 倍 RTT 与 node-timeout=0 永久挂死

来源：glm.my 第 4 条（分拣判定成立）。取证基线：主仓 HEAD 1b944517，行号为当下实况。

现状
- /Users/z/git/db/wedb/wedb/wedb/src/server/cluster_manager.rs:297 flushall_broadcast_async：
  :319 起 `for (node_id, endpoint) in targets` 在循环体内逐节点 await
  `wait_async(wait, conn.try_flushall_ns_async(ns, &origin_hex, epoch))`（:330），
  任一节点 Err/超时即上抛（:331-338）。每节点一次建连 + 一次往返
  （/Users/z/git/db/wedb/wedb/wedb/src/server/gossip/node_connection.rs:158-176，内部
  initialize_async 惰性建连 :162）。
- 超时上界：:318 `let wait = self.cluster_provider.cluster_node_timeout()`，
  cluster_node_timeout_ms 为 0 时返 None（/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_provider.rs:430），
  而 /Users/z/git/db/wedb/wedb/wedb/src/server/mod.rs:28-36 wait_async 对 None 不挂计时器直接
  `fut.await` → 任一不可达节点令整个 FLUSHALL 永不应答（协调者本地已换号，客户端挂死）。
- 扇出延迟：默认超时下最坏 N × 超时时长（N = 全部 Primary 数）串行阻塞客户端应答。
- 同文件并行先例已在位：/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_manager.rs:246-286
  try_cluster_publish_async 对全部目标节点 `spawn(async move { ... }).detach()`（:282-285），
  即「扇出 + 建连复用 connection_store」的形态本文件已用过一次，无需新机制。

C# 参考
- 无对位函数：CLUSTER FLUSHALL_NS 广播为 wedb 多租户扩展（C# 无 ns 维度，
  /Users/z/git/db/wedb/garnet/libs/cluster/Server/Gossip/Gossip.cs 无该路径）。
- 规范源 /Users/z/git/db/wedb/doc/zh/db.md 4.5：「FLUSHALL 通过 Cluster Bus 广播全网 Master 原子换号，
  协调者收集广播确认后向客户端响应 +OK」——语义只要求收齐全部 ack，未要求串行逐节点。

修法
1. 扇出：targets 先经 connection_store 取/建连接（get_connection / get_or_add，:321-328 现口径不变），
   再对每个 (node_id, conn) 起一个任务发 CLUSTER FLUSHALL_NS 并 await ack，聚合等全部任务终结。
   聚合形态取全仓既有先例（crossfire / compio 任务句柄收集 / futures join_all 之一，
   以本文件与 gossip 域已在用的那条为准），禁另立第二套聚合器或自建 channel 轮询。
2. 应答语义逐字节保持：任一节点 Err 或超时 → 整体 Err（调用方据此拒绝向客户端回 +OK，
   严禁先应答再异步广播）；全部 OK → Ok(())。is_banned(:320) 过滤仍留在扇出前同一处。
3. 超时不改语义：wait_async 的 None = 无限仍是 cluster-node-timeout=0 的既有配置口径
   （无界自旋面归 cluster-slot-gate-sync-spin 票射程，本票不重开）。

优先级
功能缺口（应答延迟随主节点数线性放大；node-timeout=0 下单个死节点挂死整条 FLUSHALL 应答）。

协调
- 与 pubsub 广播无冲突（同文件 :246 已是并行形态，本票只把 FLUSHALL 臂拉平到同一形态）。
- 与 cluster-slot-gate-sync-spin（同步自旋/无限超时口径）、flushdb-number-swap-o1-cell（换号本体）
  均不同面，勿把超时策略改动夹带进本票。

验收
- 三节点场景（其一不回 ack）：FLUSHALL 应答时限 ≈ 单节点超时，而非 3 倍；
  全部可达时并行扇出，应答内容与串行版一致（+OK 或错误串）。
- 单元/集成用例覆盖「任一失败即整体失败且不先应答」。
- 无新增第二套广播或聚合实现：grep 本文件 spawn 扇出口径唯一。

细化方案（2026-09-19 实现代理追加，行号按主仓 1b3a0165 复核）
- 复核准况：串行循环在 cluster_manager.rs:369-391（票面 :319 行号漂移，代码同）；
  聚合先例确认为 futures_util::future::join_all——
  server/replication/diskless_replication/replication_snapshot_iterator.rs:86 fan_out_send
  （扇出 + 逐目标 timeout + 结果收集，与本票形态全同）、
  server/failover/replica_failover_session.rs:311-317（join_all 等全部终结）。
  本文件 pubsub 的 spawn().detach() 是无聚合的 fire-and-forget，不收 ack，不适用本票，
  不构成第二形态。futures-util 已在 wedb crate 依赖（Cargo.toml:18）。
- 实现：flushall_broadcast_async 保 targets 构造 / 空目标 Ok / gossip manager 缺失 Err /
  cluster_node_timeout 取值全部不动；串行 for 改为
  1) 单遍 filter(!is_banned) + get_connection/get_or_add 取连接（口径不变）
  2) join_all 并行 await 全部 wait_async(try_flushall_ns_async)，每节点结果映射
     Some(Ok)->Ok、Some(Err)->Err、None->超时 Err（错误串逐字节保持现值）
  3) find_map(Result::err) 首错上抛，无错 Ok(())；等全部任务终结（不用 try_join_all
     早退，避免中途丢弃在途换号帧任务）。
- 文档注释同步：函数头「逐节点发」改「并行扇出」，注明形态对齐 fan_out_send。
- 测试：既有集成用例 cluster_flushall_broadcast.rs
  flushall_broadcast_unreachable_primary_errors 已覆盖「任一失败即整体失败且不先应答」、
  flushall_broadcast_flushes_all_primaries 覆盖「收齐全部 ack 回 +OK」，
  语义不变无需新增；按流程约束本票仅跑 cargo check。

