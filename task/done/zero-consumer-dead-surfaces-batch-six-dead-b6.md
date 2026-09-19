优先级：高（死代码：三处零生产消费者的表面残留，测试自证不算装配）

批五收尸时逐项复核遗留的三条，均在 dev HEAD 上确认「生产侧零消费者、只有 tests 或同文件测试自证在读」。一条一张票会互踩同一编辑面，故打包一批，但每条独立判定、独立可剔除。

取证基线：主仓绝对根 /Users/z/git/db/wedb，rust 面在 wedb/ 子目录（cargo 工作区根是嵌套的 wedb/wedb），C# 源在 garnet/。行号按当下 dev 重取。

条目一 wedb/wlua/src/timeout.rs:164 `pub(crate) fn active_deadlines`
全仓 grep 只有两处读它，都在 wedb/wlua/src/cache.rs 的 mod tests（:320 起）内，:405 与 :411 用它做断言。C# 侧 LuaScriptManager 的截止登记没有对应的「枚举全部活跃截止」查询口。判读：写侧 arm/disarm/shared_deadline 全是现役，只有这一个只读枚举面是测试脚手架冒充 API。落地形态：删该方法，测试侧改读它本就登记的内部字段或经由 registration.shared_deadline() 断言，保持测试能证伪（不得把断言改成恒真）。

条目二 wedb/wnode/src/aof/replaycoordinator/aof_replay_context.rs:64 `pub fn iter_keys_to_lock`
唯一读点在同文件 mod tests（:155 起）的 :215。C# 事务组回放按 operation 逐条取键加锁，没有「先枚举整组待锁键」这一步。判读：该迭代器是为测试可视化造的中间层，产线回放走另一条路。落地前先证伪：确认产线回放路径确实不经它（grep 锁获取点），再删方法与随之孤立的 import。

条目三 同文件 :199 附近 MemoryLogger 相关簇（wedb/wnode/src/logging.rs、lib.rs:51 再导出）
这一条与前两条不同，不是「零引用」而是「有类型面无装配」：C# 的 GarnetServer 构造器有 initLogger 与 FlushMemoryLogger 生命周期，rust 侧 MemoryLogger / MemoryLoggerProvider / memory_loggers 表（logging.rs:200、:207、:216、:221）在场，却没有任何启动路径把它挂进日志管线。判读要对照 C# 后二选一并给出证据：若该内存在 garnet 里只服务测试与诊断（生产不启用），按本项目「不做兼容、不留死面」的口径整簇删除并清 lib.rs 再导出；若 C# 生产确实启用（宿主启动即 initLogger），则本条改判为功能缺口，按 C# 对位补装配口，并把形态写回本票。禁止造半截装配（挂上去但从不 flush）。

门禁与验收
只跑 cargo check --workspace --all-targets（在 <worktree>/wedb 下，后台运行、重定向到文件、读 exit 码），必须 exit 0 且 0 warning；bun js/check.js 只在 worktree 内跑，且报告须与合入前逐字节相同（若条目三判删除，需先在 js/check/ignore 相应 yml 里登记 C# 面不再转植，登记生效后 check.js 才会绿）。禁 test.sh、禁 ./sh/clippy.sh，由主代理集中跑。
自证：合入后 git grep active_deadlines HEAD、git grep iter_keys_to_lock HEAD 各自只剩测试侧或零命中（按落地形态说明）；条目三按二选一结论给出对应 grep。

坑与边界
删 pub 前看泛型界与 trait 实现侧是否需要它；测试引用不算生产消费者；读侧与写侧分开判（条目一只删读侧，写侧保留）。本机 nightly 的 slice 无 dedup/dedup_by，任何「用 std 替换自研」的顺手的想法先跑 rustc 探针再决定，别把它当成本票的一部分。

落地记录（worktree dead-b6，合入 dev d0bb114b，父 735466ae；改动仅 6 个文件 +90/-62）

条目一 删（读侧枚举面）
/Users/z/git/db/wedb/wedb/wlua/src/timeout.rs 的 `#[cfg(test)] pub(crate) fn active_deadlines` 整块删除；
写侧 arm/disarm/shared_deadline/deadline 与 tick/active_count 全保留（现役）。
测试侧（/Users/z/git/db/wedb/wedb/wlua/src/cache.rs）改从登记项断言，不是恒真：
`registration.deadline()` 与换挂给 runner 的 `shared_deadline().load(Acquire)` 双读，
arm 后应为 arm_at+300、disarm 后应为 0。

条目二 删（组级枚举中间层）
`TransactionGroup::iter_keys_to_lock` 整体删除。落地前复核仍成立：产线回放
（aof_processor.rs process_transaction_group*/process_transaction_group_operations）
全程不碰键锁集，唯一读点在原测试内。C# 锚点
`AofReplayCoordinator.cs:SaveTransactionGroupKeysToLock` 下移到现役的逐操作取键口
`ReplayOperation::key`（该函数体即 C# 逐 op 取 `acc.key` / 跳头后长度前缀键的对位件），
测试改写为 C# 的逐操作形态（`operations.iter().map(ReplayOperation::key)`）。
锚点未消失、未新增重复定义，check.js 报告逐字节同基线。

条目三 改判：功能缺口，按 C# 对位补装配（未删簇）
裁决依据：garnet/libs/host/GarnetServer.cs :47 字段 `initLogger`、:86-88
`new MemoryLoggerProvider()` + `CreateLogger("ArgParser")`、:97 与 :232 两处
`FlushMemoryLogger`——生产宿主启动即启用，不是测试/诊断专用面。
形态（全仓唯一一条日志装配链，未造第二套）：
- `MemoryForwardLogger::install()`：内部建 `MemoryLoggerProvider`、按
  `INIT_LOG_CATEGORY = "ArgParser"`（新增常量，C# 字面量对位）取先行缓冲、
  提供器随即弃置（对齐 C# 只借一次 CreateLogger），自身注册为全局 logger。
- `LoggingBuilder::install` 已删除（它是被本次装配取代的第二条装配口），
  改为私有 `build()` + 唯一收口 `flush_into(&forward)`（对位 C# FlushMemoryLogger）。
- 两个宿主 main 按 C# 生命周期接入：参数解析前 install、解析后 flush_into
  （/Users/z/git/db/wedb/wedb/wedb/src/main.rs:19,28、
  /Users/z/git/db/wedb/wedb/wedb_standalone/src/main.rs:47,58）。
- 顺带删掉孤立面 `MemoryLoggerProvider::categories()`（C# 无对位、仅测试在读；
  测试改判 `Arc::ptr_eq` 的 GetOrAdd 同一性 + dispose 后新取即新实例）。
- 未动 /Users/z/git/db/wedb/wedb/wnode/src/lib.rs（:51 再导出已在场，避免与
  boot-assembly 票同文件互踩）。
残留缺口（本票不扩面，另计）：wconf/args 解析路径今天一条 `log::` 都不发，
先行缓冲实际暂无写者——根因是 C# ServerSettingsManager 的 22 处
`logger?.Log*` 未转植；补那条装配前，本形态只是把管线接通。
测试-only 成员保留未删：`MemoryLogger::{len,is_empty}`、`LogTarget::Memory`、
`From<MemoryLogger>`、`ConsoleLogger::install`、`LoggingBuilder::disable_console`。

门禁读数
`cargo check --workspace --all-targets`（worktree 私有 CARGO_TARGET_DIR）exit 0、0 warning；
`bun js/check.js` exit 0，报告与 `git archive <dev-sha>` 出的合入前基线逐字节相同
（js/check/ignore/ 被 check.js 自动裁剪属 dev 工具既有漂移，已 checkout 还原、未提交）。
自证（dev HEAD）：`git grep active_deadlines` 仅 1 处（cache.rs:405 的解释性注释）、
`git grep iter_keys_to_lock` 零命中、`grep -n "flush_into(&pre_parse)" wedb/*/src/main.rs` 两宿主各 1 处。

票名冲突注记
本票认领时落名 task/ing/claim-dead-b6.md，与 dev 在册的
task/ing/zero-consumer-dead-surfaces-batch-six.md（qw.design 第 11 轮拆出的
24 口增量批，内容完全不同）重名不同件。为避免覆盖，归档改名
task/done/zero-consumer-dead-surfaces-batch-six-dead-b6.md，那份 24 口票留在 ing/ 未动。
