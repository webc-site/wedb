零消费 pub 面普查批四：3 处真孤儿符号（AOF 调试断言口 / 记录头合并谓词 / 会话输出泛型取口）

来源：glm.design 第 1、2、3 条合并立项（本轮普查余量，逐条 C# 对位已核）。取证基线：
主仓 HEAD 50d1cb5f。去重基准：task/done/zero-consumer-pub-surface-census.md（批一）、
next/zero-consumer-surfaces-batch-two.md（批二）、
task/ing/zero-consumer-dead-surfaces-batch-three.md（批三）、
task/done/zero-consumer-dead-symbols-cleanup.md 四张在册清单均不含本批三符号，
grep 双确认零消费（全仓仅定义行命中）。

条目一 AofSyncDriverStore::assert_does_not_exist 调试断言口零接线
- 现状：/Users/z/git/db/wedb/wedb/wedb/src/server/replication/aof_sync_driver.rs:609
  `pub fn assert_does_not_exist(&self, remote_node_id: u128)`，全仓唯一命中即该定义行；
  对位注册链为 /Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_sync_session.rs
  的 attach_replica_wire → try_add_replication_driver，注册前无「旧驱动已终止」校验。
- C#：/Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:659-660
  `[Conditional("DEBUG")] public void AssertDoesNotExist(string remoteNodeId)`；
  消费点 /Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:299
  （注册新副本驱动前断言同节点无存活驱动）。
- 判定：链条在移植中断裂——方法本体转写了，消费点没跟。属带 C# 锚点的孤儿面，非纯冗余。
- 修法（二选一，禁第三态「留着不接」）：
  a) 首选：在 try_add_replication_driver 调用之前补该断言（cfg debug_assertions 内，
     与 C# [Conditional("DEBUG")] 等效），一次接线即恢复 C# 语义；该点恰在
     next/aof-driver-register-pre-transfer.md 的射程内，两票可并档落地（见协调）。
  b) 次选：删方法，并在既有
     /Users/z/git/db/wedb/js/check/ignore/garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.yml
     追加 AssertDoesNotExist 条目，理由写明「注册冲突由 registry 拒绝分支在 release 面同样兜住，
     无独立断言价值」。

条目二 RecordHeader::is_closed_or_tombstoned 纯 bool 合并谓词零消费
- 现状：/Users/z/git/db/wedb/wedb/wrecord/src/header.rs:267-271
  `pub const fn is_closed_or_tombstoned(&self) -> bool { self.is_closed() || self.is_tombstone() }`，
  全仓零消费。rust 消费面按必要形态拆成两臂：
  /Users/z/git/db/wedb/wedb/wkv/src/session/raw/read.rs:79-83
  `is_closed() → ReadProbeResult::Retry` / `is_tombstone() → ReadProbeResult::Tombstone`
  （C# 用 ref OperationStatus 出参在一个谓词内同时回状态，rust 以枚举分支承接，拆分是正确形态）。
- C#：/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/RecordInfo.cs:115
  `internal readonly bool IsClosedOrTombstoned(ref OperationStatus internalStatus)`；
  消费点 /Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRead.cs:118、:130。
- 修法：删该函数，并在既有 /Users/z/git/db/wedb/js/check/ignore/storage.yml:1319-1333
  RecordInfo.cs 块的方法清单追加 IsClosedOrTombstoned，理由：「C# 该谓词以 ref OperationStatus
  合并回状态；rust 读探针以枚举分支区分 RETRY/TOMBSTONE 两态，合并 bool 版无承接位，
  消费面见 wkv/src/session/raw/read.rs:79-83」。
  不得保留「为形状对位而无人调用」的第四态（既有 ignore 登记机制即为此类残留的单点收口）。

条目三 RespServerSessionOutput::writer_p 泛型取口零消费
- 现状：/Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session_output.rs:27
  `pub fn writer_p<P: RespProtocol>(&mut self) -> RespWriter<&mut Vec<u8>, P>`，全仓唯一命中即定义行；
  同文件 :15 writer2、:21 writer3 两口已覆盖全部现役消费点。
- 与 done/resp-output-facade-triple.md 的关系：该票只统一了定义侧（保留会话门面为全仓唯一门面，
  并删 wcol/wlua 侧三取口），未判定门面内部的取口消费面，writer_p 属其漏网的第三形态，
  不与该票结论冲突。
- C#：无独立对位（/Users/z/git/db/wedb/garnet/libs/server/Resp/RespServerSessionOutput.cs 无泛型取口；
  协议位由 /Users/z/git/db/wedb/garnet/libs/common/RespMemoryWriter.cs 构造器 resp3 布尔承担，
  rust 已用 wresp RespWriter 类型参数静态分派承接）。
- 修法：删 writer_p 及仅为其存在的 use（wresp 协议参数在 writer2/writer3 具体返回类型里仍在用，
  预计零 use 变动），门面取口收敛为两口；无 C# 锚点故 check.js 侧不需登记。

优先级
死代码（三条同为「写了没人调用」的转写残留；本批不含行为缺口）。

协调
- 条目一 a) 案与 next/aof-driver-register-pre-transfer.md 同一注册点，该票若先行落地，
  本票条目一只补断言行、不重开注册逻辑；两票并档或本票留该条待其合入后收口均可，
  但不得出现「注册改了、断言口仍孤儿」的中间态交付。
- 条目二删函数触碰 wrecord 公开面，与批二/批三同性质（纯删），无文件冲突预期。
- 条目三所在 resp_server_session 巨峰文件在途拆分票
  （next/resp-server-session-file-split.md）只管该文件族移动，本票是删口，
  同文件开工需错开，禁在纯移动拆分里夹带删改。

验收
- 三符号 grep 归零（条目一采 b) 案时同样要求方法体删除）。
- cargo check --workspace --all-targets（私有 target 目录）零 error 零 warning，
  含 tests 面（`--all-targets` 覆盖，防止仅 src 编译通过）。
- ./js/check.js 无新增「缺失锚点/虚构锚点」报告：条目一 a) 案使
  AofSyncDriverStore.cs:AssertDoesNotExist 锚点转为已消费；b) 案与条目二须核对
  ignore 追加条目生效（不再计入未消费清单）。
- test.sh/clippy 由中央整合轮执行，本票不跑。

落地（dev f50588a，worktree /tmp/fork/fix-zero-consumer-batch-four）
- 条目一采 b) 案（a) 案与代码事实不符，拒录见
  /Users/z/git/db/wedb/task/reject/zero-consumer-dead-surfaces-batch-four.md）：
  删 wedb/wedb/src/server/replication/aof_sync_driver.rs 的
  AofSyncDriverStore::assert_does_not_exist，锚点入
  js/check/ignore/garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.yml。
  不触碰注册逻辑，与 next/aof-driver-register-pre-transfer.md 无交集（该票改的是
  try_add 调用时机与 start 取值，本票删的是 DEBUG 期探针，不存在「注册改了、断言口仍孤儿」中间态）。
- 条目二照做：删 wedb/wrecord/src/header.rs 的
  RecordHeader::is_closed_or_tombstoned，IsClosedOrTombstoned 入
  js/check/ignore/storage.yml:1319 RecordInfo.cs 块方法清单并补理由。
- 条目三照做，但文档「预计零 use 变动」不实：RespProtocol 只为 writer_p 的泛型界存在，
  已同批从 wedb/wnode/src/resp/resp_server_session_output.rs 的 use 摘除，门面取口收敛为
  writer2/writer3 两口。
- 验收：三符号全仓 grep 归零；cargo check -p wrecord -p wnode -p wedb --all-targets
  （私有 target /tmp/fork/fix-zero-consumer-batch-four/target）exit 0、零 warning 零 error；
  bun js/check.js 与改前基线逐字节同输出（重复定义/实现缺失/B 层 128 处计数均无变化），
  两份 ignore 经 yaml.parse 自检可解析、条目未被自动淘汰。

