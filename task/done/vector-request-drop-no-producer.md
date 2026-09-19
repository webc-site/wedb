优先级：死面整链（读侧在跑、写侧零投递）

4 单问题：向量集内存索引丢弃请求（request_drop_in_memory_index）在 rust 里是「消费端全链在跑、
生产端无人投递」的空转面——处理侧 run_request_drop_task_loop / process_request_drop_once /
drop_requested / wait_for_disk_ann_index_drop 都在产线，但请求入口零生产调用者。

取证现状（2026-09-19 由向量登记键修红票代理实测上报，主代理未复核，须自行验证）
- 入口：wedb/wnode/src/resp/vector/vector_manager.rs:665 request_drop_in_memory_index，
  全仓生产侧零调用者（测试夹具除外）。
- C# 挂点：./garnet libs/server/Storage/Functions/GarnetRecordTriggers.cs:116（记录逐出触发器内
  调用丢弃请求），VectorManager.cs:707 / :728 为该请求的语义端（以本库键为身份）。
- rust 侧 wkv 无对应的记录逐出触发臂，故请求通道无人接。

裁决要求（二选一，不许含糊并存）
1 若 C# 的逐出触发在 rust 有等价落点（分层 demote / 对象逐出路径），则把请求接到那一处单点，
  让消费链由空转变真产线，并补一条端到端断言（写入向量集 → 触发逐出 → 内存索引丢弃被请求/完成）。
2 若 rust 形态确无该机制（无逐出触发面），则整链删净：入口、loop、once、drop_requested、wait 口
  及其专属测试与文档锚点、js/check/ignore 登记一并清账，不留「等以后接」的悬空消费者。
禁止第三种结局（保留消费链却无生产者）。判 2 前须先在 ./garnet 与 wkv/wcol 的逐出路径 grep 取证，
不得凭「找不到」直接删。

改动域：wedb/wnode/src/resp/vector/**、wkv 逐出/触发面（判 1 时）、相关测试与 ignore 登记。
避让：wkv/src/vdb.rs 与 store 层 dbmeta 有修红代理在跑；分层写臂版本推进另有一棒。

结案注记（判 2 整链删净；代码提交 e87da51，载体分支 vector-request-drop，合并 sha 见 dev 上
「merge: 并入最新 dev 7d336cd」93196a7 一笔；本票射程 12 文件 +61/−360）

一、判词：判 2（rust 形态确无记录逐出触发面，整链删净）

写侧取证（./garnet）
- C# 的 RequestDropInMemoryIndex 全仓唯一生产点确为 libs/server/Storage/Functions/
  GarnetRecordTriggers.cs:116 的 OnEvict 臂（`RecordType == VectorManager.RecordType` 且
  `!ValueIsObject` 时投递），语义端 VectorManager.cs:707（登记 requestedDrops + 发信号）。
- 消费侧三处（Cleanup.cs:135 常驻任务、Locking.cs:185/:199 与 :405/:410 的重建前等待、
  Migration.cs:213/:223 的迁移前等待）全挂在同一 requestedDrops 集合上；集合唯一生产者即上臂。
  → 「无生产者死面」主张成立。rust 侧对应面：入口生产调用者除测试夹具外零命中（dev 复测）。

判 1 落点排查（逐路取证后排除，非凭「找不到」）
- wkv 侧无逐出触发臂：全仓 `grep -rn "on_evict|OnEvict|EvictionSource|RecordTriggers"` 零命中
  函数实现；rust 逐出是页/段地址推进（wkv/src/session/raw/mod.rs:237 evict_pages_for、
  wkv/src/store/addr.rs:143 HeadAddress 推进、read_cache/window.rs:116 need_to_wait_for_eviction
  为地址失效等待），无「逐记录回调 + 记录类型分派」形态。既存文档同源佐证：
  wkv/src/store/flush.rs:19-21 明写 C#「VectorManager 仅挂 OnEvict/OnDiskRead，无 OnFlush 钩子」，
  故 rust 只落了 OnFlush/OnDispose/OnTruncate/PostCopyToTail 系，OnEvict 臂整体缺位。
- 向量索引记录不入 wkv 值域：全仓 put_stored_index/write_stored_index 只写
  `VectorManager::key_index_registry`（进程内 ConcurrentMap），wnode/src/storage/session/
  common/ttl_sync.rs:439 已文档化「rust 把该记录单列于 key_index_registry（不驻 wkv 值域）」，
  登记表既在 VectorManager 内，就没有能被逐出的宿主可投递请求。
- 分层 demote 面不相交：wcol::should_demote（wcol/src/lib.rs:56）在 rust 的唯一消费面是
  wnode/src/resp/objects/tiered_demote.rs:255-264，四类对象 Hash/Set/SortedSet/List；
  `git grep -i vector -- wcol/src` 零命中 → 无向量索引降阶臂可接。
→ 三条候选落点均不存在，判 1 无落点可接，禁第三种结局，取判 2 整删（票面要求的端到端断言
  属判 1 分支，本判不补；判 2 的断言即「删净后全链零符号残留」，见下四）。

二、前棒 patch 采信情况

/tmp/fork/vector-request-drop-no-producer-payload.patch（707 行 / 12 文件）在本票开窗基线
dev 893ede9 上 `git apply --check` 零漂移、`git apply` 一次干净落地，逐处对照票面与现刻
HEAD 复核后 12/12 文件全部采信，无一处需手工重放。本棒唯一自写改动：
- wedb/wnode/src/resp/vector/vector_manager_cleanup.rs::is_quiescent 的 rustfmt 收敛
  （删 RequestDrop 一条后剩两条 `&&` 应折单行；patch 基于旧基线未含此步）。
故净载荷为 12 文件 +61/−360（patch 报 −359，多删的 1 行即上述 fmt 收敛）。
删除范围与本票射程逐项一致，未越界一处：未碰 wkv/src/vdb.rs、store 层 dbmeta、
wbase/pool 其余面、wnode/tests/resp_admin.rs（避让清单内），他域在飞现场零交叠。

三、删除清单（每符号一句 C# 依据）

1 request_drop_in_memory_index（入口）↔ VectorManager.cs:707 RequestDropInMemoryIndex：C# 由
  GarnetRecordTriggers.cs:116 OnEvict 臂投递，rust 无该臂，入口成零生产者死口。
2 VectorManager::requested_drops 字段 ↔ VectorManager.Cleanup.cs:92 `requestedDrops`
  （`VectorSetCleanupWorkSet<(ulong, nint)>`）：全类唯一消费者即本链，随链删。
3 VectorManager::request_drop_task_channel 字段 ↔ VectorManager.cs:215 的 requestDropTaskChannel
  （C# 构造器 spawn 第三条任务）：删后 Dispose 收敛序退为 requestCleanup → cleanup。
4 run_request_drop_task_loop / run_request_drop_task_async ↔ VectorManager.Cleanup.cs:135
  RunRequestDropTaskAsync：无生产点即无消费循环，常驻协程一并删。
5 process_request_drop_once / perform_drop（私有）↔ 同任务体内 Cleanup.cs:153
  `foreach (var (k, (context, indexPtr)) in requestedDrops)` 与 :167 `Service.DropIndex`：
  唯一调用者即被删循环。
6 drop_requested ↔ Cleanup.cs:380 DropRequested（`requestedDrops.Contains`）；
  wait_for_disk_ann_index_drop ↔ Cleanup.cs:387 WaitForDiskANNIndexDrop
  （`requestedDrops.WaitForCompletion`）：两等待口随集合删净。
7 vector_manager_locking.rs 读索引阶段 1→2 之间的丢弃等待臂 ↔ Locking.cs:185/:199
  （C# 另一对 :405/:410 在 rust 无对应面）：臂在则永假，删。
8 CleanupTaskKind::RequestDrop 变体 + CleanupRuntime::running 三槽 ↔ C# 三条常驻任务退二，
  改以 `COUNT = 2` 定长并顺延 index()，不加 allow、不留占位。
9 wbase::pool::EventWorkSet（wbase/src/pool/work_set.rs 127 行）及其测试件
  wbase/tests/suite/pool_work_set.rs 与 mod.rs 挂载 ↔ Cleanup/VectorSetCleanupWorkSet.cs
  （73 行 internal 类，成员 IsEmpty/Contains/WaitForCompletion/TryAdd/TryComplete/
  GetEnumerator）：该载体全仓唯一产线消费者是 requested_drops，链删即成死码，连根删。
10 测试面：resp_vector_set.rs::drop_in_memory_index_flow 的请求丢弃段（登记/信号/重复拒/
  SuppressCleanup 忽略四断言，随入口删；直落 drop_in_memory_index 段保留）；
  vector_set_cleanup_vs_reset_race.rs 的第三条常驻协程拉起。
11 service.rs 清理协程拉起/停机两处注释「三条 → 两条」并记删因。
12 ignore 登记逐条改登：VectorManager.yml 的 理由 段补 RequestDropInMemoryIndex 未接线因由 +
  条目；server.yml 的 VectorManager.Cleanup.cs 组补 RunRequestDropTaskAsync / DropRequested /
  WaitForDiskANNIndexDrop；VectorSetCleanupWorkSet.cs 由「文件 + GetEnumerator」嵌套条目改整文件
  登记（其 rust 载体已删，无待映射面），并在原 EventWorkQueue 条目 理由 里记 WorkSet 侧改登因由。

四、门禁实测（私有 target：/tmp/ct-vrd、/tmp/ct-vrd-clippy；未跑主仓 ./test.sh ./sh/clippy.sh）
- cargo check --workspace --all-targets：删除前后各一轮，exit 0，`^error|^warning` 计数 0。
- cargo clippy -p wbase -p wnode --all-targets：exit 0，3 条告警全在他域文件
  （wbftree lib 一处 loop、wnode/src/aof/aof_processor.rs:905、wnode/tests/transaction_tests.rs:249），
  本票 12 文件零告警；无 unused 残留，故无需二次连根删。
- cargo nextest run -p wbase --test main -p wnode --test vector_set_cleanup_vs_reset_race
  及 12 支 vector 套件（--no-fail-fast）：两轮（基线树 / 并入 dev 后的合并树）均
  116 tests run: 116 passed, 1 skipped，exit 0；pool_work_set 两例随删除退册，无孤儿用例。
- bun js/check.js 树内前后对跑（基线取同刻 dev 90171be 的干净树）：# 实现缺失 段逐项一致
  （文件清单与基线同集合）、语料零回写（本树跑完 git status 干净），唯一差异是「重复定义」段
  两条既存锚的行号漂移 vector_manager.rs:537→:531、vector_manager_cleanup.rs:222→:223，
  系本票删行所致、非新增映射。反事实复测（只落代码、不落语料）新暴露
  VectorSetCleanupWorkSet / VectorManager.Cleanup / VectorManager 三文件缺失，与本次三条
  登记逐一对位 → 语料改动既不多遮蔽一分、也不少消一项。
- 语料健康自检：js/check/ignore 全 78 份 yaml.load 通过（rules 494、fail 0），无声失效零风险。
- cargo fmt：本票 12 文件 rustfmt --check 全 OK；树上余 diff 均在避让清单内的他域文件
  （wkv/src/session/raw/** 三处、wkv/tests/store/reviv.rs、wlua/src/commands.rs、
  wnode/src/resp/objects/sorted_set_commands/write.rs），未搭车。
- 删后残留：`grep -rn "request_drop|RequestDrop|EventWorkSet|drop_requested|
  wait_for_disk_ann_index_drop|pool_work_set" --include="*.rs"` 于 wedb 全域仅 service.rs:1580
  一条解释性注释命中（记删因，非锚点）；dev 上 `git grep` 同上符号亦零命中。

五、残余与移交
- VectorManager::drop_in_memory_index（↔ VectorManager.cs:740 DropInMemoryIndex，C# 侧本身
  private 且全仓零调用者）在本票前后都是「零生产调用者 + 仅测试夹具调用」，属既有独立死面、
  不在 request-drop 链上（本链走 perform_drop → service.drop_index），本票未吞它；
  归零消费面清账族（zero-consumer-dead-surfaces 系）处置为宜。
- 若日后 rust 落记录级逐出回调（OnEvict 形态），重建此链的凭据在本票第三节与
  js/check/ignore/server.yml 的两处 理由 段。
