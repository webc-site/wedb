# glm.db 分拣拒绝台账

来源：next/glm.db.md（底层引擎/存储设计审查）分拣。取证基线：主仓 /Users/z/git/db/wedb，分支
dev，全部行号按当下代码重取。

## 1. 迁移复活暂停「只落 pause 半协议、缺纪元排空等待」：排空在位，落在 begin_migration_phase 的 epoch_gate 段，票面取证区间截断致误判

立票：task/ing/reviv-pause-epoch-drain.md（next/glm.db.md 条 8；源文件本波已剪空删除，无源行可
删）。本条复核判不成立，ing 件删除，不开发。

原文要点
C# 的暂停复活是两半协议：/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/
Tsavorite/Implementation/Revivification/RevivificationManager.cs:26-28 递减挂起计数挡新 take，
随后 /Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/
Tsavorite.cs:119-146 PauseRevivification 在 lock 内做 epoch.Resume → pause →
pauseRevivEvent.Reset → epoch.BumpCurrentEpoch(() => pauseRevivEvent.Set()) → epoch.Suspend，
出锁 pauseRevivEvent.Wait(timeout, token)（注释 "Wait for everyone to observe the reviv suspend
signal"），调用点 /Users/z/git/db/wedb/garnet/libs/cluster/Server/Migration/MigrationDriver.cs:139
与 :225 finally ResumeRevivification。rust 侧
/Users/z/git/db/wedb/wedb/wedb/src/server/migration/migrate_driver/slots.rs:91-95
RevivPauseGuard::new 只调 store.reviv_pool.pause()，:98-103 Drop 里 resume；据此认定 :114 构造守
卫后「紧接着 connect_migrate_client、begin_migration_phase、store.new_session 并进逐槽取键循环，
全程无排空动作」，pause 生效前已 take 出槽位的在途复活写者可在键枚举窗口内改写中部空闲槽位并原
子挂链，漏迁键既不传输也不删除，槽位迁出后成不可达成孤儿键；修法要求在 RevivPauseGuard::new 内
pause() 之后补纪元 bump + 排空等待（或下沉为 store.pause_revivification_async()）。

拒绝原因

一、事实错误：排空在位，且位置正是票面要求的「pause 之后、首次取键之前」，只是不在守卫函数体内。
slots.rs:114 构造 RevivPauseGuard（:93 pause）→ slots.rs:120-129 调
begin_migration_phase(..., epoch_gate = true, TransferOption::Slots) →
/Users/z/git/db/wedb/wedb/wedb/src/server/migration/migrate_driver/keys.rs:325-341 该形参为真时
`session.cluster_provider.bump_and_wait_for_epoch_transition_async().await`，等待不成立即
recover + Err 返回，绝不带着未排空的状态进扫描；首次键枚举在同文件 slots.rs:147-149
get_keys_in_slot。票面取证区间写成 :109-131，正好截在 begin_migration_phase 调用之后数行、未下钻
keys.rs 内部，把「排空落在编排函数里」读成「全程无排空」。该落点与 C# 同位：C#
MigrationDriver.cs:139 PauseRevivification 之后，:152-160 仍是 TryPrepareLocalForMigration +
BumpAndWaitForEpochTransitionAsync 才进 MigrateSlotsDriverInlineAsync，rust 由
keys.rs:322-324 注释自述对标该段。

二、语义等价性核实：这个等待确实排空在途复活写者，不是只等配置位可见。
/Users/z/git/db/wedb/wedb/wedb/src/server/cluster_provider.rs:731-741
bump_and_wait_for_epoch_transition_async 以 AcqRel fetch_add 推进纪元后，自旋
all_sessions_caught_up（:767-790）直到每个活跃集群会话满足 entry_epoch == 0（批外空闲）或
entry_epoch >= current_epoch。纪元快照由会话在命令批首尾取放
（/Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:1014-1020
try_consume_messages 包 acquire/release_current_epoch，对标 C# RespServerSession.cs:490/:576
`clusterSession?.AcquireCurrentEpoch()` 与 finally ReleaseCurrentEpoch），而复活写者唯一的取槽现场
 wedb/wkv/src/session/raw/mod.rs:132 reviv_pool.take 只由会话写路径
wedb/wkv/src/session/raw/write/inplace.rs:71 抵达，全仓无第二处 take，必落在该快照窗口内。故等待
返回时：bump 前起批的会话已收批（其 take + hlog.revivify_record_at 裸写 + 索引挂链已完成），bump
后起批的会话经 bump 的 release → 快照的 acquire → take 入口 is_enabled()
（/Users/z/git/db/wedb/wedb/wreviv/src/pool.rs:267-272，Acquire load）读到暂停计数、直接取不到槽
位。C# pauseRevivEvent 那两件事（在途写者排空 + 暂停信号被全员观察到）在同一处同时达成。CAS 失败
的会话内暂存复用写（inplace.rs:16-24 RetryAlloc，:142/:346 构造）作用域限定在单次写操作内、不跨
命令批，因此也不可能绕过这次排空。

三、按票面补第二次排空即造第二套机制，与裁决口径相悖。同一原语会在同一次迁移任务里相隔数行连等
两次（守卫内一次、begin_migration_phase 一次），第二次不改变任何可观察行为，只为把「由调用方按需
组合」的注释搬进 RAII。票面给的另一个落点 store.pause_revivification_async() 是把 C#
TsavoriteKV 的门面搬回 rust，而本仓已判定该门面不投影：
/Users/z/git/db/wedb/js/check/ignore/storage.yml:1803-1825（Tsavorite.cs 条目含
PauseRevivification/ResumeRevivification，理由「C# KV 引擎门面……rust 拆解承接——操作面 wkv
session raw」），wreviv 侧只保 RevivificationManager 级的 pause/resume
（pool.rs:180-199，其文档明写强同步暂停「需配合上层 epoch 排空……由调用方按需组合」）。上层调用方
已经组合了（见一），再开第二处违背「一套机制、复杂度对标 C#」口径。

四、残余覆盖面差异不构成漏迁窗口。C# 的 store 级纪元覆盖全部线程域，rust 的集群级等待只覆盖已注
册集群会话；差的那部分是内部会话（紧缩/检查点/purge/分层重刷）不经快照。但能把「中部空闲槽位改写
并原子挂链成一个未被枚举的新键」的，只有客户端命令批（集群会话，已被等待覆盖）；内部会话即便在窗
口内动作，其经复活池落的是墓碑或既有键的改写帧，正是驱动 slots.rs:291-320「已不存在或被并发改写」
登记面承接的形态，不会产出漏收的新键。故无「漏迁新键滞留源端」的可构造路径。

五、验证面亦无需补：票面要求「enable_revivification=true 下并发写 + 槽位迁移用例」，其断言（迁移
后源端不残留未迁键）与上述排空是否落在守卫内无关——现序已在枚举前排空，用例无法区分两种实现，
为不可观测差异写测试属占位验证。

遗留（本票不开发，供主代理定夺）：slots.rs:90 守卫文档只写「构造守卫并暂停存储复活分配」，未点明
排空半由 begin_migration_phase 的 epoch_gate 承接，契约靠调用顺序而非类型/注释保证；若将来新增第
二个 reviv_pool.pause() 调用方（如检查点封印窗口自行 pause），须同批把排空半接上，届时最省的做法
是在该调用方的纪元等待处配对，而非给守卫加句柄。
