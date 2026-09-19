批四甄别拒录（来源票据现归档 /Users/z/git/db/wedb/task/done/zero-consumer-dead-surfaces-batch-four.md，
只记不成立/需修正项；文中行号以认领基线 dev bc31ba1 为准，落地时 dev f50588a 若位移按函数名为准）

本批三符号「零消费」主张全部成立（全仓 grep 仅定义行与注释命中），清理动作照常落地；
以下条目原文与代码事实不符，拒录其判读与修法，防止后续按错误前提重开。

一、条目一「注册前无校验」的现状判读不实（其首选修法 a) 一并拒录）

原文（/Users/z/git/db/wedb/task/done/zero-consumer-dead-surfaces-batch-four.md:14-15）：
「对位注册链为 /Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_sync_session.rs
的 attach_replica_wire → try_add_replication_driver，注册前无「旧驱动已终止」校验。」

原文（同文档 :21-23）：
「a) 首选：在 try_add_replication_driver 调用之前补该断言（cfg debug_assertions 内，
与 C# [Conditional("DEBUG")] 等效），一次接线即恢复 C# 语义。」

拒绝原因（代码事实）：
- rust 对位注册点在 try_add 之前确有处置动作：
  /Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_sync_session.rs:91（attach_replica_wire 起于 :83）
  `self.rm.aof_sync_driver_store.try_remove(remote_node_id);`，随后 :102 才
  `try_add_replication_driver`；该函数文档注释 :79-81 已写明「重复 attach 先移除旧驱动
  （对标 C# AcquireCheckpointEntryAsync 内 AssertDoesNotExist + 断链重连的驱动置换语义）」。
  故「注册前无校验」不成立，实为「无断言、以置换取代断言」的有意设计。
- 由此 a) 案落地即恒真断言：断言体是 `registry.get(&id).is_none()`
  （/Users/z/git/db/wedb/wedb/wedb/src/server/replication/aof_sync_driver.rs:609 原体），
  而 :91 已先摘除该 id，插入点在其后必然为 none，接线不恢复任何语义，只添一具死壳
  （违 .agents/skills/transpile/SKILL.md:79「严禁在代码中写占位函数或虚设实现」）。
- C# 该探针为 `[Conditional("DEBUG")]` + `Debug.Fail`
  （/Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:659-673，
  唯一消费点 /Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:299），
  release 面零行为；实质防护（同节点位点越截断线）在 rust 由
  aof_sync_driver.rs:294-310 `try_add_replication_driver` 的 truncated 越线拒绝分支承担。
- 结论：采文档次选 b) 案——删方法体 + 在
  /Users/z/git/db/wedb/js/check/ignore/garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.yml
  追加 AssertDoesNotExist 条目，理由按上述代码事实写（不采用文档原拟的
  「registry 拒绝分支在 release 面同样兜住」一句话，该句只覆盖了拒绝分支、未说明
  try_remove 置换已消掉断言前置条件）。

二、条目三「预计零 use 变动」不实（修正后照做）

原文（/Users/z/git/db/wedb/task/done/zero-consumer-dead-surfaces-batch-four.md:55-56）：
「修法：删 writer_p 及仅为其存在的 use（wresp 协议参数在 writer2/writer3 具体返回类型里仍在用，
预计零 use 变动）」

修正原因：`RespProtocol` 在 /Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session_output.rs
仅出现在 writer_p 的泛型界 `P: RespProtocol`，writer2/writer3 返回类型用的是具体类型
`Resp2`/`Resp3`，故删口后该 import 必成未用告警，须同批从 use 列表摘除（不做「零 use 变动」处理）。
落地即按此执行。

三、去重基准与取证基线失效（不影响本批成立性，仅记账）

原文（:3-8）称取证基线为主仓 HEAD 50d1cb5f，并引 task/done/zero-consumer-pub-surface-census.md、
task/done/zero-consumer-dead-symbols-cleanup.md 为在册清单。实测主仓历史已压缩（HEAD 为
bc31ba1 一线），`/Users/z/git/db/wedb/task/done/` 为空目录，50d1cb5f 不可解析。
本批改按当前 dev 代码事实重验：三符号全仓 grep 零消费，且与在途
task/ing/zero-consumer-surfaces-batch-two.md、zero-consumer-dead-surfaces-batch-three.md、
zero-consumer-dead-surfaces-batch-five.md 三张清单无符号重叠（批五仅在 :179 记「同域禁并发」）。
