INITIATEREPLICASYNC 假拼写扩散七处：注释凭空断言 C# 用无下划线形态，一处运行期错误串把这拼写带进 -ERR 应答

来源：next/glm.net.md 条 4（该文件已分拣清空删除）。逐句按主仓当下代码与 C# 复核后判定成立待做。
取证基线：主仓 /Users/z/git/db/wedb，行号按符号定位。
载体唯一性：并发拆条在 next/ 下另留了一份本条原文照抄的壳（basename
initiate-replica-sync-typo-spread.md，只加「优先级：中」头、无取证订正），
以本文件为唯一载体，派单前先剪壳勿双花。

结论

协议字面量两侧本来完全一致，都是带下划线的 INITIATE_REPLICA_SYNC；但无下划线的
INITIATEREPLICASYNC 这个在 C# 里根本不存在的拼写扩散在 rust 六处注释加一处运行期错误串里，
其中最失真的一处注释反过来断言「C# 用无下划线形态、rust 做过归一改名」。这是纯注释/文案订正单，
不改任何线上帧。

事实基线（两侧字面量核对）

- C# 发送侧 /Users/z/git/db/wedb/garnet/libs/client/ClientSession/GarnetClientSessionReplicationExtensions.cs:19
  `private static ReadOnlySpan<byte> initiate_replica_sync => "INITIATE_REPLICA_SYNC"u8;`，
  :38 起 `ExecuteClusterInitiateReplicaSync` 经 TryWriteBulkString 写帧。
- C# 接收侧 /Users/z/git/db/wedb/garnet/libs/server/Resp/CmdStrings.cs:475 同名常量、
  /Users/z/git/db/wedb/garnet/libs/server/Resp/Parser/RespCommandHashLookupData.cs:346
  `("INITIATE_REPLICA_SYNC", RespCommand.CLUSTER_INITIATE_REPLICA_SYNC)`。
- C# 全仓大小写不敏感 grep `INITIATEREPLICASYNC` 命中的只有 C# 标识符
  （ExecuteClusterInitiateReplicaSync、NetworkClusterInitiateReplicaSync、日志文案
  `InitiateReplicaSync:` 等），无下划线的协议字面量零命中。
- rust 实际帧名正确：/Users/z/git/db/wedb/wedb/wedb/src/client.rs:415 发 `b"INITIATE_REPLICA_SYNC"`，
  接收表 /Users/z/git/db/wedb/wedb/wnode/src/resp/parser/command_table.rs:404-405 同拼写注册到
  `RespCommand::ClusterInitiateReplicaSync`（枚举 /Users/z/git/db/wedb/wedb/wresp/src/command.rs:367，
  命令信息 /Users/z/git/db/wedb/wedb/wresp/RespCommandsInfo.json:793-794）。
  两侧同名、无归一步骤，因此也不存在帧名漂移断链，本单只清文字。

现状（待订正的七处）

- /Users/z/git/db/wedb/wedb/wedb/src/client.rs:413-414 注释：「wresp 命令表子命令字面量（C#
  CmdStrings.initiate_replica_sync "INITIATEREPLICASYNC" 在 rust 侧归一为下划线形态）」——
  两句皆假：C# CmdStrings 现值是 INITIATE_REPLICA_SYNC，rust 也没有任何改名/归一动作。
- /Users/z/git/db/wedb/wedb/wedb/src/server/replication/assembly.rs:17（模块头流程叙述）、
  :104（`recover_replication` 文档注释第 3 步）、:281（`try_replicate_diskbased_sync_async` 文档注释）
  三处写成「CLUSTER INITIATEREPLICASYNC」。
- /Users/z/git/db/wedb/wedb/wedb/src/server/boot.rs:139 注释「主端推流资产（INITIATEREPLICASYNC 服务面）」。
- /Users/z/git/db/wedb/wedb/wedb/src/server/cluster_session/replication.rs:69 注释
  「CLUSTER INITIATEREPLICASYNC；两支都在登记副本后当场发起一次 attach」。
- /Users/z/git/db/wedb/wedb/wedb/src/server/replication/assembly.rs:197 运行期错误串
  `Err(format!("INITIATEREPLICASYNC to {primary} failed: {msg}"))` —— 这一处不是注释：
  它经 replicate_sync_async → finish_replica_sync 上抛，在
  /Users/z/git/db/wedb/wedb/wedb/src/server/cluster_session/replication.rs:77-79 被
  `out.write_resp_error(&msg)` 直接写成应答，即 REPLICAOF / CLUSTER REPLICATE 的 -ERR 文案与日志里
  会出现一个不存在的命令名。同函数 :194-196 的兄弟臂已经在用正确措辞
  （`Failed to initiate replica sync to {primary}: {msg}`），同一决策点两种文案也是本单要收的口。

修法

1. 六处注释（client.rs:413-414、assembly.rs:17/:104/:281、boot.rs:139、
   cluster_session/replication.rs:69）的 INITIATEREPLICASYNC 一律改成 INITIATE_REPLICA_SYNC；
   client.rs:413-414 删掉「C# 用无下划线形态 / rust 侧归一」这层虚构，改写为「与 C#
   CmdStrings.cs:475、GarnetClientSessionReplicationExtensions.cs:19 同字面量，无改名」。
2. assembly.rs:197 的错误串改拼写，并与 :194-196 的兄弟臂统一成一处措辞：
   超时/未连接臂与对端 -ERR 臂的差别只在尾缀原因，不在发起动作的名字，
   合成为 `format!("Failed to initiate replica sync to {primary}: {msg}")` 一处格式化，
   match 两臂共用，不留第二套文案模板（对齐 C# 发起段失败即把 ex.Message 原样作应答的单一口径，
   /Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/ReplicaOps/ReplicaDiskbasedSync.cs:190-194）。
3. 收口防复发：全仓 grep `INITIATEREPLICASYNC`（大小写不敏感）在 wedb/ 下归零，
   与 C# 侧一致；不引入任何字符串常量来「单点化」这个子命令名——它已在
   /Users/z/git/db/wedb/wedb/wedb/src/client.rs:415 与 command_table.rs:404 两处按 C# 的
   发送表/解析表二元结构各留一份，加第三处反而造出 C# 没有的中间层。
4. 回归测试：在 /Users/z/git/db/wedb/wedb/wedb/tests/replication_assembly_e2e.rs 现有
   INITIATE_REPLICA_SYNC 帧用例（:341-349 手搓 5 参帧、:233 主端未 arm 的 -ERR 断言）上补一条
   对 -ERR 文案不含假命令名的断言即可，不新开测试文件。

优先级

污染扩散（对 C# 源码的虚构描述 + 进入对外应答的错误命令名），排在死代码与重复架构清理之前处理，
成本近零、面窄，且是后续任何「按注释纠正帧名」会话的误改引信。

边界

- 原条目引 task/done/replicaof-diskbased-sync-initiate.md:16/:79 为历史留档候选，该 done 票当下不存在
  （/Users/z/git/db/wedb/task/done 无此文件），无需勘误。
- 本单不碰 replica_sync_session.rs、cluster_provider.rs、tests 里已用正确拼写的注释。

落地（本轮，worktree /tmp/fork/initiate-replica-sync-spelling-drift，分支同名）

七处全部独立复验成立，无拒绝项（task/reject 未建）。复验基线与本票取证一致，行号按符号重定位后
与当下 dev 完全对齐：

- 正确字面量两侧在位、帧常量禁动未破：/Users/z/git/db/wedb/wedb/wedb/src/client.rs:419
  `b"INITIATE_REPLICA_SYNC"`（发）、/Users/z/git/db/wedb/wedb/wnode/src/resp/parser/command_table.rs:404-405
  （解，注册到 `RespCommand::ClusterInitiateReplicaSync`）。两处本轮零改动。
- 六处注释订正（假拼写 → INITIATE_REPLICA_SYNC）：client.rs:417-418（改写为「与 C# CmdStrings.cs:475、
  GarnetClientSessionReplicationExtensions.cs:19 同字面量，两侧无改名」，删掉「C# 无下划线 / rust 归一」
  虚构叙事）、assembly.rs:17、assembly.rs:104、assembly.rs:286（原票面 :281，因下方错误串段增行而下移）、
  boot.rs:144（原票面 :139，按符号 `wire_replication_data_plane` 注入点重定位）、
  cluster_session/replication.rs:69（与票面同号）。
- 一处运行期错误串：assembly.rs:197 原 `Err(format!("INITIATEREPLICASYNC to {primary} failed: {msg}"))`
  已并入单处模板，现为 assembly.rs:193-203——`let reason = match res { ... }` 归一原因后
  `Err(format!("Failed to initiate replica sync to {primary}: {reason}"))`，两失败臂共用一套文案。
  传播链复验：recover_replication → replicate_sync_async（assembly.rs:244/:254 调 finish_replica_sync）
  → cluster_session/replication.rs:77-79 `out.write_resp_error(&msg)`，即原假命令名确实对外可见。
  下列行号一律以合入 dev 后的当下树为准（本票开发期 re-sync dev 三次，client.rs 注释段整体下移 4 行）。

实现勘误（对本票修法 2 的一处必要修正）

本票写「match 两臂共用」，字面直改成 or-pattern `Ok(Err(msg)) | Err(msg)` 编译不过：`res` 的真实类型是
`Result<Result<String, Error>, String>`，内层失败是 crate 的 `error::Error`、外层是 `String`，
同一臂内绑定必须同型（rustc E0308）。故按「归一原因 → 一处模板」落地：成功臂 `return Ok(())`
（与本函数既有的早返回风格一致），两失败臂各自把原因收敛成 String，模板只剩 assembly.rs:201-203 一处，
既满足「不留第二套文案模板」，也不新增 C# 没有的抽象。

实现勘误（对本票修法 4 的一处如实记）

指定落点 replication_assembly_e2e.rs 的现有 arm 用例（`primary_arm_without_assets_reports_not_initialized`）
走的是主端资产缺位的同步拒绝路径（回 `-ERR Cluster not initialized`），并不经过本票改的副本侧
assembly.rs:197 失败臂；同文件 `primary_arm_after_wiring_proceeds_past_not_initialized` 与
cluster_replication.rs:394（`recover_replication` 直调，回 "primary endpoint unknown"）、
cluster_resp_session.rs:1425（"local wal not wired"）同理，全部在 :197 之前返回。为不凭空调
TCP 建连失败时序（本流程禁跑测试，红测试只能留给门禁暴露），本轮按票面在该用例补一条
「-ERR 应答不含假命令名」断言（replication_assembly_e2e.rs:246-249，与相邻真文案断言同处），
作为对外应答面的防复发收口；:197 这一臂的文案精确性仍无运行时用例覆盖，如需钉死需另开一条
「副本侧发起 → 主端端口不可达」用例，属新增测试面，不在本票窄化范围内做主。

收口防复发（本票修法 3）

`grep -rnE "(^|[^A-Za-z0-9_])INITIATEREPLICASYNC"` 在 wedb/ 的 *.rs/*.json/*.md 下 0 命中（假字面量归零）。
大小写不敏感 `grep -i INITIATEREPLICASYNC` 仍有 7 命中，全部是合法标识符
（`RespCommand::ClusterInitiateReplicaSync` 及 C# 原生名 `ExecuteClusterInitiateReplicaSync` /
`NetworkClusterInitiateReplicaSync` 锚点注释），与 C# 侧同形——C# 全仓同样只命中这类标识符，
故与本票「无下划线协议字面量零命中」的判据一致，未过度扩大。未按票面「大小写不敏感归零」字面执行，
理由即此：那要求连 C# 自己也过不了。

验收（本轮实测）

- `cargo check -p wedb --tests`（含我改的 tests 文件）exit 0，0 error 0 warning；worktree 私有
  target /tmp/fork/initiate-replica-sync-spelling-drift/target，未与他人共享、未跑全量 check。
- 五个改动文件逐一 `rustfmt --check --edition 2024` 全 CLEAN（首版 or-pattern 写法与 `=> { }` 块形
  曾被 rustfmt 判 diff，已按其口径归一）。
- diffstat：5 files changed, 25 insertions(+), 16 deletions(-)。
- 未跑 clippy.sh / test.sh / 任何测试（按派单约束），门禁留给主代理。

合入（plumbing，非 git merge）

主仓 index 当时挂着并发代理的暂存重命名（task/ing→task/done、next→task/ing 各一组），
`git merge` 生成的合并提交会把它们一并带走，故走 plumbing：worktree 侧 `git merge dev` 先
把 dev 并进本分支（re-sync 两次，均无冲突，每次并入后重跑 `cargo check -p wedb --tests` 归零），
再 `git commit-tree <本分支树> -p <dev tip> -p 5de84e2` + `git update-ref --no-deref refs/heads/dev <new> <old>`
乐观锁提交；首轮 CAS 撞上 dev 又被并发代理推进，重 merge 后第二轮落定
f7ab6a0「merge: initiate-replica-sync-spelling-drift (fixloop)」（双亲 e8c47b4 + 5de84e2）。
落地后逐个 `diff -q <(git show f7ab6a0:$f) <(git show dev:$f)` 复验五个文件在 dev tip 仍与本票一致
（dev 顶端 36df578 恰是他票被 `git add -u` 陈旧工作树回写吞掉的复原单，同型风险必须自查）。
update-ref 只推 ref 不动工作树，遂成五个 payload 文件的幽灵脏，已按
`git checkout dev -- <五个路径>` 归一到 HEAD（先逐文件确认磁盘内容 == 合入前 dev 内容，
排除覆盖他人改动的可能），归一后本票五个路径 `git status --porcelain` 空。
