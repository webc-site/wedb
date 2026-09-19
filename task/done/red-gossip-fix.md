优先级：高（dev 测试基线红，计数轮前置）

单题：集群会话两条红——节点查问命令应答为空、gossip WITHMEET 往返取响应时越界。

红测试与实测证据（2026-09-19 dev 6e008a33 复跑，确定性失败）
1 wedb::cluster_resp_session::cluster_node_inspection_commands —
  wedb/tests/cluster_resp_session.rs:1342
  left: [] / right: "-ERR I don't know about node aaaa..."（字节 [45,69,82,82,...]）
  即查问未知节点的命令（CLUSTER FORGET/RENAME 一类）根本没有产出应答字节。
2 wedb::cluster_resp_session::cluster_gossip_withmeet_roundtrip —
  wedb/tests/cluster_resp_session.rs:1495:17 panic: index out of bounds: the len is 0 but the
  index is 0
  读侧按固定下标取响应数组第 0 项，而该命令此时产出 0 项。

判读方向（须自行核实）
两条同指「集群会话写路径在某条分支上未落任何字节」，与错误帧收口（RespWriter::write_error_frame
单一 sink）和 version_map 死槽删除两批改动交界。先确认是产线在未知节点分支上漏写应答，还是会话
测试驱动与命令实现口径不一致（例如该命令走异步旁路未 flush）。第 2 条越界属用例侧硬下标，
若产线行为正确则改为按帧解析并说明依据；若产线漏写则补写口。分开定性，别互相顶包。

避让
wedb/src/server/replication/replica_wire.rs 与 wresp/src/resp_memory_writer.rs 有在途分支
fix-err-frame-sanitize（HEAD 3146533e）待合，动这两处前先 git merge dev 并把本票判读写进回报，
避免与它撞同一 sink；cluster_migration 的向量发现条属 qw13-red-vector-registry-key-domain。

改动域
wedb/src/server/ 下集群会话命令分发与节点查问面、gossip WITHMEET 处理，以及
wedb/tests/cluster_resp_session.rs。禁止触碰 wkv、waof、wmetric。

---

落地记录（2026-09-19，red-gossip worktree，分支 red-gossip → dev）

结论：两条都是**用例侧驱动夹具错**，产线无偏离、无漏写；两条均不 reject。零产线改动。

复现（具名跑法，fork 时 dev e177bd0c，票面行号已漂移，实测行为一致）
- cluster_node_inspection_commands → panic at tests/cluster_resp_session.rs:1391
  left: [] / right: "-ERR I don't know about node aaaa..."（票面记 1342）
- cluster_gossip_withmeet_roundtrip → panic at tests/cluster_resp_session.rs:1544:17
  index out of bounds: the len is 0 but the index is 0（票面记 1495）
- 同文件基线：46 passed / 2 failed，仅此二条红，确定性非 flaky。

定性依据（产线正确，逐条对 C#）
1 CLUSTER FORGET：wedb/src/server/cluster_session/basic.rs:507 network_cluster_forget 同步段只做
  参数校验与 hex 解析，摘除段取 active_merge_lock 写锁属异步域，登记 SlowWait（basic.rs:539），
  应答由异步执行体 cluster_forget_slow（basic.rs:218）产出；产线转挂在
  wedb/wnode/src/resp/admin_commands.rs:324（take_pending_slow → 会话 pending_slow → 网络泵 await）。
  文案逐字节对位 C#:
    -ERR I don't know about node <id>. <- garnet/libs/cluster/Server/ClusterManagerWorkerState.cs:68
    -ERR I tried hard but I can't forget myself <- 同文件 :62 取 garnet/libs/cluster/CmdStrings.cs:42
  临界区对位 garnet/libs/cluster/Session/RespClusterBasicCommands.cs:80-95（ReleaseCurrentEpoch 后 TryRemoveWorker）。
2 CLUSTER GOSSIP WITHMEET：basic.rs:252 同步段登记 SlowWait（basic.rs:287），应答在
  cluster_gossip_slow（basic.rs:161）内 write_resp_bulk_string(当前配置字节)，
  对位 garnet/libs/cluster/Session/RespClusterBasicCommands.cs:422-427
  （lastSentConfig != current || gossipWithMeet → TryWriteBulkString(current.ToByteArray())），
  合并临界区对位同文件 :401-410。
两条断言要求的行为在 C# 中确实存在，故不 reject；红因是夹具：本文件 roundtrip/pump 只跑同步段，
慢路径应答须走本文件既有 slow_roundtrip（:1549，另有 8 处用例已在用），改走即绿。

改动（仅 wedb/tests/cluster_resp_session.rs，+54/-24）
- 两条 FORGET 断言 roundtrip → slow_roundtrip（补 rt）。
- WITHMEET 读侧硬下标 out[0] → 新增 parse_bulk_frame 按 bulk string 帧解析（帧头长度自洽 +
  帧尾 CRLF + 载荷等于会话当前配置序列化字节），替代原 out.len() > 10 弱断言。
- 顺带修帧构造：原 payload.iter().map(|&b| b as char) 对 >0x7f 字节是失真转换（UTF-8 编码
  长度 ≠ 字节数，帧长发帧头即错），改逐字节组帧；这是本条用例的潜在假红源，非新增机制。

门禁（合并前于最终树复跑）
- cargo test -p wedb --test cluster_resp_session cluster_node_inspection_commands → ok，1 passed
- cargo test -p wedb --test cluster_resp_session cluster_gossip_withmeet_roundtrip → ok，1 passed
- 整文件 48 passed / 0 failed
- cargo check --workspace --all-targets → exit 0，0 warning
- bun js/check.js（仅本 worktree）→ exit 0，报告与改动前逐字节相同（A/B 双跑比对）；
  其自动回写的 js/check/ignore/server.yml 已 git checkout 复原，未随提交带出

事故与处置（须主代理知悉）
A 本票 merge 用 git merge dev 连做三次，第二次落在 dev=03540706（该 sha 随后被主代理改写为
  0a453215 的后继，03540706 成为游离兄弟），第三次 merge 把 12 个文件按陈旧兄弟树投到 dev，
  即误回滚 zset o1 mutated_by_ttl、object 序列化、shared_object_commands、sorted_set_commands、
  hosting/storage ignore 登记与三张票归档（坏合并 f8eb8dab 已入 dev 历史）。
  已逐路径核对恢复：现 dev 上上述 7 个 rust 文件与 hosting.yml 的 blob 与权威 0a453215 全等，
  三张票在 task/done，storage.yml 仅余他票后加的 1 处注释演进（非回滚）。他人在 f8eb8dab 之后
  的 wconf/aof/service/wnode 记账重构与本票改动并存，无相互覆盖。
  教训：dev 高速改写期 merge 会引入陈旧兄弟树，合入前应 re-fetch 并用「树 diff 只含本票文件」
  作硬校验；建议本仓改用「以 dev 现 tip read-tree + 仅覆写本票文件」的树构造法。
B refs/stash 在 .git 共享、跨 worktree 可见：本票用 git stash push 做 A/B 时 pop 到的是他代理
  in-flight 的 RespWriter sink 重构 stash（8 文件），并被 drop。已把该内容逐字节回存进 stash 列表
  （stash@{0}，消息含 RED-GOSSIP 误 pop 恢复字样，diff 与原件字节全等），未丢内容。
  本仓并发下**不要用 git stash 做 A/B**，改用 cp 暂存文件。

