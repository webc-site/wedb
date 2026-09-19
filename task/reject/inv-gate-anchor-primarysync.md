裁决：不成立（判拒）。PrimarySync 不是「族」而是 110 行单文件三方法，票面 C# 路径
`PrimarySync/PrimaryOps/` 在 garnet 全树不存在（虚构一级目录）；该三枚方法在现刻 dev HEAD
已由 rust 文档注释全覆盖、对「实现缺失」贡献为 0；门禁在本棒两棵干净 worktree 独立复测
均 exit 0 且 stdout 逐字节相同。票面两条前提（登记缺位、未证复绿）同时被实测推翻，
且其修法实测既不改变任何判定、又会持续销毁自身理由文本。本棒零代码改动、零语料改动，
票转 task/reject/ 归档。

取证基线：主仓 /Users/z/git/db/wedb 分支 dev。认领提交 074cf2b（移入 task/ing/）+
b9e8fc4（next/ 侧删除）。全部实测在两棵只读 worktree 内完成，均已清理：
/tmp/fork/inv-gate-primarysync @ dev 8a07a7a（分支 inv-gate-primarysync，
`git rev-list --left-right --count dev...inv-gate-primarysync` = 10/0，已 `branch -d`）、
/tmp/gate-igp @ dev 893ede9（--detach，已 remove）。主仓未跑 check.js 本体、未 add 任何
他人路径。探针为纯读脚本（/tmp/psprobe*.mjs、/tmp/missnames.mjs），只 import
js/check/{garnetScan,rustScan}.js 与 check.js 导出的 ignoreLoadAndPrune，且把 IGNORE_DIR
指到 /tmp 副本，主语料零写入。

一、C# 实态：路径虚构，「族」不成立
- `find garnet -type d -name 'PrimarySync*'` 零命中。票面「对应 C# 族在
  garnet/libs/cluster/Server/Replication/PrimarySync/PrimaryOps/（整族文件）」把目录层级
  多写一级——真身是 garnet/libs/cluster/Server/Replication/PrimaryOps/PrimarySync.cs，
  110 行、`internal sealed partial class ReplicationManager` 的分片文件，不是目录、没有族。
- 该文件在门禁名录里只有 3 枚（garnetScan 实测，非 AST 降级文件）：
  TryBeginDisklessSyncAsync（:25）、TryBeginDiskbasedSyncAsync（:58）、
  ReplicaSyncSessionBackgroundTaskAsync（:76，是 TryBeginDiskbasedSyncAsync 体内的局部函数）。
  票面「逐文件甄别（整族文件）」的工作量不存在。

二、登记并不缺位：3/3 覆盖、miss=0（按 check.js 自身 isDocumented/isIgnored 口径实测）
- TryBeginDisklessSyncAsync —— 精确锚在位：
  wedb/wedb/src/server/replication/replica_diskless_sync.rs:217
  `/// 在 garnet 中的相对路径: libs/cluster/Server/Replication/PrimaryOps/PrimarySync.cs:TryBeginDisklessSyncAsync`
- ReplicaSyncSessionBackgroundTaskAsync —— 精确锚在位：
  wedb/wedb/src/server/cluster_session/replication.rs:531（:532-537 并论证
  TryBeginDiskbasedSyncAsync 尾部局部函数体的承接关系）
- TryBeginDiskbasedSyncAsync —— 由文档 token 覆盖：
  replication_manager.rs:842 与 replication.rs:532 的 `///` 行以名引用（rustScan 的
  doc_set 全局口径命中，非精确文件级锚）
- 走注释锚点而非 ignore，正是 js/check/README.md §3 规定的优先次序（「先检查 Rust 是否
  已有等价实现，若有则优先在 Rust 函数上方补齐文档注释」）。上游 gate-anchor-drift-reclean
  二棒 §一.2 已把这条记入归档正文：「LogAddress.cs 与 PrimarySync.cs 两族不走 ignore，
  而由 rust 文档注释登记为『已文档化』」。
- 唯一可改进处（不构成本票主张）：TryBeginDiskbasedSyncAsync 若日后那两处散文改写，
  会复现一条缺失；正当处置是补 `PrimarySync.cs:TryBeginDiskbasedSyncAsync` 精确锚，
  不是建 ignore。现刻门禁绿、缺失 0，属可选小注释票，不由本票代开。

三、票面修法实测：无效且自我侵蚀（负控两枚，跑完即删，主语料零改动）
- 负控 A：照票面字面新建
  js/check/ignore/garnet/libs/cluster/Server/Replication/PrimarySync/PrimarySync.yml，
  条目路径按票面写 `libs/cluster/Server/Replication/PrimarySync/PrimaryOps/PrimarySync.cs`
  → 跑 check.js 后文件原样留存、stdout 与基线逐字节相同、exit 仍 0。
  它镜像一个不存在的 C# 路径，file_ignore_map 的 key 永不与任何语料文件相遇，
  成为永久无人校验的死角——check.js 的 A 层符号存在性断言（symbolCheck.js）只作用于
  .rs 注释锚点，对 ignore 语料内的假路径不设防。照票落盘＝把 gate-anchor-drift-reclean
  立票要清的「登记语料失效锚」品类再造一份。
- 负控 B：同一条目写到真实路径 .../PrimaryOps/PrimarySync.cs 并列出三枚方法
  → 跑一次即被裁掉两枚（js/check.js:141-191 的「已文档化即淘汰」），只剩
  TryBeginDiskbasedSyncAsync，且文件首行的 `# 取证书` 被 js/check.js:229 的
  yaml.stringify 抹除；stdout 仍与基线逐字节相同、exit 仍 0。
  即：该修法既不改变任何判定，又每跑一次销毁一次自身论据。

四、门禁「未证复绿」不成立（本棒不采信归档，独立复测两次）
- 归档侧：上游票正文（`git show 6a39bef^:task/done/gate-anchor-drift-reclean.md` 可取回）
  二棒 §二 已记录 worktree 内 exit 0、改动前后 stdout+stderr 逐字节相同、并以「摘掉
  DoubleTurnstileBarrier 条目即转红」做过负向验证。
- 本棒复测：dev 8a07a7a 与 dev 893ede9 两棵干净 worktree 各跑 `bun js/check.js`
  → 两次 EXIT=0；两次 stdout `diff` 逐字节相同；段头仅「# 重复定义」「# 实现缺失」，
  无「# 虚构锚点」、无红色语料失效段；全文件 `grep -c PrimarySync` = 0。
- 机制口径（本票原委所在）：check.js 的 exit 码只由 corpus_invalid 与 symbol_fail 决定
  （js/check.js:532-538），「实现缺失」段与「重复定义」段均为信息节、不参与判定。
  因此「补 PrimarySync 的 ignore 登记以使门禁复绿」在机制上不成立——它既不影响 exit 码，
  也不影响缺失段（该段本就不含 PrimarySync）。

五、Replication 全域已无登记缺口（防「族外还有族」）
- 按 check.js 同一口径遍历 Server/Replication/ 全树：可见 C# 文件 42 个、名录函数 237 枚、
  未登记 miss 0 枚。含本票射程点名的 ReplicaOps/ReplicaDiskbasedSync.cs（名录 6 / miss 0）
  与 ReplicaOps/ReplicaDisklessSync.cs（名录 3 / miss 0），及 PrimaryOps 全 22 个文件。
- 该树仅 GarnetClusterCheckpointManager.cs 一处 AST 降级（ERROR 4 处，词法兜底 9→9 未丢名），
  不存在「条目登记在失活文件上故假绿」的 READM §1 陷阱。

现刻「实现缺失」段的 7 族均不属本票射程（不越界代改，逐条点名供主代理派单）
- libs/server/AOF/AofProcessor.cs: BeginReplayOp
- libs/server/Resp/HyperLogLog/HyperLogLog.cs: DenseCountNonZero
- libs/server/Resp/Vector/VectorManager.Callbacks.cs: AdvanceTo, SlowPath, GetKey, GetInput,
  GetOutput, SetOutput, MakeVectorElementKey
- libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs: AllocateUserWord, ReleaseUserWord,
  ThisThreadUserWord, GetMinUserWord
- libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/SplitIndex.cs: TraceBackForOtherChainStart
- test/standalone/Garnet.test.vectorset/VectorSetRecallSmokeTests.cs: Jitter
- test/standalone/Garnet.test/RespAdminCommandsTests.cs（test 桶）: ConfigWrongNumberOfArguments,
  ConfigGetWrongNumberOfArguments
其中 core/Epochs/LightEpoch 与 RespAdminCommandsTests 在 gate-anchor-drift-reclean 归档里
判为「已登记族」，现复现缺失，疑因下述登记流失通道，宜另开一棒逐条取证、勿并入本票。

补记（本棒实测发现，不属本票射程，未落任何改动）
- dev@893ede9 的已提交语料 js/check/ignore/storage.yml 仍会被 check.js 回写：实测
  ConsistentReadContext 块裁掉已文档化的 ResetModified、另一块裁掉
  GetMinRevivifiableAddress，并把 `理由` 明文标量重折行（git diff 3+/3-）。
  这正是主仓 js/check/ignore/{server,storage,test}.yml 长期呈钩子脏的直接来源
  （.husky/pre-commit.js 每次提交跑 fixrs → 连带跑 check.js）。此类无语义 diff 一旦被
  `git add -u` 扫进无关提交，登记与取证书即无声流失——历史上已发生数轮
  （gate-anchor-drift-reclean §三事故条）。本棒取证期间该通道已再次落地：并发提交
  24f4049「wip: 合并前主仓快照(dead-batch-six)」把这份回写挂上 dev，实测改动
  39+/4-、`#` 取证书零流失，损失面仅两条已被文档锚点覆盖的陈旧条目
  （storage.yml 的 GetMinRevivifiableAddress 被裁、ConsistentReadContext 块内
  ResetModified 重排），不新增缺失。风险在该通道本身而非这一次。建议主代理把
  合并窗口前的 `git checkout -- js/check/ignore/` 定为固定动作。
- 射程文件本次核实为无需改动：replica_diskless_sync.rs:217 与 cluster_session/replication.rs:531
  两枚精确锚在位未漂移；replica_diskbased_sync.rs 挂的是 ReplicaDiskbasedSync.cs 一族
  （:26、:57），与 PrimarySync.cs 无涉。

---

以下为原票面（立项取证，判拒后原文留档）

优先级：低

问题
gate-anchor-drift-reclean 票的唯一残留收尾：C# PrimarySync 族的 js/check/ignore
登记仍缺位，门禁（check.js）是否复绿未证。其余该票判据均已落地（虚构锚点
HashObjectImpl.cs:Set 已订正为 HashSet，19 族甄别大部已登记），本票只收尾。

取证（dev e75716e，按当下代码）
- js/check/ignore/ 全域 grep PrimarySync 零命中；
  js/check/ignore/garnet/libs/cluster/Server/Replication/ 下仅 ReplicationNetworkBufferSettings.yml
  与 PrimaryOps 目录，无 PrimarySync 子目录；
- 对应 C# 族在 garnet/libs/cluster/Server/Replication/PrimarySync/PrimaryOps/
  （整族文件），rust 侧同步面实现在 wedb/wedb/src/server/replication/
  （replica_diskbased_sync.rs / replica_diskless_sync.rs 等，符号级对位需逐文件甄别）。

C# 对标（garnet 相对路径:符号）
garnet/libs/cluster/Server/Replication/PrimarySync/PrimaryOps/ 整族（逐文件判
已实现 / 无需实现并登记 ignore / 补文档注释锚点）。

修法建议
逐文件甄别 PrimarySync 族：已实现的在 rust 函数文档注释挂 C# 相对路径锚点；
无需实现的在 js/check/ignore/garnet/libs/cluster/Server/Replication/PrimarySync/
下建 yml 登记理由；完成后实跑 bun js/check.js 取退出码，复绿即闭环（此前
gate-anchor 票因门禁未证复绿而保留，本票是它的全部剩余工作量）。
