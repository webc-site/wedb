# glm.design 分拣拒绝台账

来源：next/glm.design.md（死代码/重复机制/模块拓扑审查第 6-13 轮增量）分拣。取证基线：主仓
/Users/z/git/db/wedb，分支 dev，全部行号按当下代码重取。

## 1. 第 6 轮：迁移帧导入收尾臂 unreachable!() 与注释「兜底拒绝面」声明错位，panic 直达网络泵线程无隔离

原文要点
/Users/z/git/db/wedb/wedb/wedb/src/server/migration/frame_import.rs:174-176 的帧类型分派收尾臂对
RangeIndexStream / VectorSetIndex / VectorSetElement 三变体写 unreachable!()，而紧邻的
:179-181 注释自称「此为帧内兜底拒绝面，绝不静默当 string 写入……C# SYNC 面意外 kind 本就
抛」。该票据此认定：注释承诺的「兜底拒绝」语义应是返回 Err，实现却是 panic；执行链挂在网络
泵 worker 上（SlowWait::new 经 take_slow_wait 后直接 await），全仓无 catch_unwind 包裹，上游
门控一旦在重构中漏一个变体，对端一帧即撕掉该 worker 上的全部连接；C# 对位
/Users/z/git/db/wedb/garnet/libs/cluster/Session/RespClusterMigrateCommands.cs:135、:303 是 try
块内可捕获的 throw。修法：三臂改 return Err。

拒绝原因
一、注释归属读错。:179-182 的「帧内暂不支持类型显式拒绝」注释是下一段（:183-190 的
`if let MigrationRecord::Env { .. }` 配 `migratable_object_type` 门，命中即
`return Err("ERR Unsupported migration record kind 2 …")`）的自述，不是对 :174-176 收尾臂的
承诺。真正的「帧内兜底拒绝面」实现确实返回 Err，与注释一致，不存在「声明要 Err、实现给
panic」的错位。

二、C# 对位的 throw 面在 rust 已有 Err 承接，且承接位置更靠前。C#
RespClusterMigrateCommands.cs:135、:303 的 `Unexpected MigrationRecordSpanType` 抛点对应 rust
的帧解码期：/Users/z/git/db/wedb/wedb/wconn/src/record.rs:316 对未知 kind 直接
`Err(Error::InvalidRecord("Unsupported migration record kind …"))`（同文件 :234 单记录路径同
口径）。即「意外 kind → 可捕获错误」的 C# 语义已由 wconn 解析层承担，命令层拿到的
MigrationFrame 已是封闭的六个合法变体，不再有「意外 kind」。

三、:174-176 三臂是结构性不可达，不是被削弱的拒绝面。同一函数体内，RangeIndexStream 在
:95-106 处理并 continue，VectorSetIndex / VectorSetElement 在 :128-156 的 matches! 分支处理并
continue，走到 :164 的 match 时三者已被穷尽排除；rust 的穷尽分派要求列出这些臂，
unreachable!() 是 C# 编译器判定不可达代码的等价物，改成 return Err 属为不可达分支造第二套
文案，违背 transpile SKILL「杜绝写死函数、杜绝占位实现」的口径（
/Users/z/git/db/wedb/.agents/skills/transpile/SKILL.md:79）。

四、票内风险论证是假设性的：其成立前提是「上游门控在将来重构中漏一个变体」，当下无触发路径；
且同一票据此也未提供任何可构造的输入帧能抵达该臂。C# 侧亦无 catch_unwind 对位需求（throw 落
在会话 try 内），rust 侧真正需要收口的 panic 面（生产 expect/panic 全量）已由既有台账口径处理，
本票不构成独立待做项。

## 独立复核（第二路分拣代理，主仓 /Users/z/git/db/wedb，分支 dev，按当下代码重取）

上述四条拒绝理由逐条取证成立，判决采信：

- 注释归属：`/Users/z/git/db/wedb/wedb/wedb/src/server/migration/frame_import.rs:179-182` 的
  「帧内暂不支持类型显式拒绝…此为帧内兜底拒绝面」确为紧随其后的 :183-190
  `if let MigrationRecord::Env` + `migratable_object_type` 门的自述，该门命中即
  `return Err("ERR Unsupported migration record kind 2 …")`，声明与实现一致。
- C# 抛面的 rust 承接：`/Users/z/git/db/wedb/wedb/wconn/src/record.rs:232-236` 与 :315-319 两处
  兜底臂均 `return Err(Error::InvalidRecord("Unsupported migration record kind …"))`，即
  「意外 kind → 可捕获错误」已在解码层承担，命令层的
  `/Users/z/git/db/wedb/wedb/wedb/src/server/migration/frame_import.rs:174-176` 三臂为穷尽分派
  的结构性不可达（同函数 :95-106、:128-156 已各自 continue）。
- 更正一处票面数字：全仓 catch_unwind 实际位点为
  `/Users/z/git/db/wedb/wedb/wlua/src/context.rs:57`、`/Users/z/git/db/wedb/wedb/wkv/src/ttl.rs:578`
  （测试内）、`/Users/z/git/db/wedb/wedb/wnode/src/resp/vector/vector_manager_cleanup.rs:287`、
  `/Users/z/git/db/wedb/wedb/wbftree/src/service/ops.rs:344,:355`、
  `/Users/z/git/db/wedb/wedb/wbftree/src/service/snapshot.rs:56,:82`，迁移/集群会话链上仍为零，
  不影响判决。

留给后续同类票的判据（不改变本票结论）：`/Users/z/git/db/wedb/wedb/Cargo.toml:232-236`
`[profile.release] panic = "abort"`，即网络驱动分派路径上任何真可达的 panic 都是整进程 abort
而非单线程停摆；因此该族的存废判定应只看「可达性是否已证」，不看爆炸半径论证——本票不可达性
已由解码层封闭枚举证伪，故拒。

## 双花对账（本文件被并发拆分后的处置归属）

本文件在 13:23:43 被另一路分拣代理逐条机械拆成 12 个 `next/` 单问题件（原文逐字照抄，仅加
「优先级」头），与本台账同批产出的 10 个 `task/ing/` 文档按主题重合。并发代理已自行按
「重复拒绝」收口，映射为：block-on-four-variants-converge →
task/ing/block-on-single-source.md；snapshot-chunk-aligned-buf-zero-copy →
task/ing/snapshot-chunk-pooled-borrow.md；object-scan-coscan-sync-slow-dedup →
task/ing/object-scan-coscan-kernel-single-source.md；readme-bilingual-wram-shard-fabrication 与
root-readme-wram-stale-api-reference → task/ing/readme-crate-map-drift.md（同件两条已合并）；
checkjs-csharp-parser-miss-gate → task/ing/garnet-scan-cs-corpus-parse-gate.md；
cmd-strings-input-tokens-single-source → task/ing/cmd-strings-input-token-single-source.md；
sunsubscribe-catalog-acl-gap → task/ing/sunsubscribe-catalog-acl-gap.md；
ticks-conversion-constants-single-source → task/ing/tick-scale-conversion-single-source.md；
resp-frame-assembly-single-source → task/ing/resp-frame-literal-single-source.md；
frame-import-unreachable-to-err → 即本台账第 1 条，维持拒绝；waof 死依赖条的拆分件
waof-event-listener-dead-dep → task/ing/waof-dead-event-listener-dep.md。

唯一残留：/Users/z/git/db/wedb/next/acl-store-scan-err-dedup.md（13:23 拆出，尚无 `-dup` 收口件，
/tmp/fork 与 git branch 均无同名工作树/分支，即未开工）。该件内容是
task/ing/block-on-single-source.md 的第二面（acl_store.rs:65 私有 scan_err 逐字复抄
wnode/src/storage/session/common/array_key_iteration_functions.rs:35 的 pub(crate) 单点，
修法见该文档修法第 4 步），按主题判为双花，主代理可直接删除，不必另立单。本路无权删改
next/ 下他人产物，故仅在此登记。本路产物中无任何 item 处于未裁决状态。
