零消费 pub 面普查批五：25 处死口 / 调试门丢失 / C# 有生产调用而 rust 旁路

来源：qcode10.design 条 4、5、13、14、15、16 合并立项（同族：一条为「零消费者壳」，
一条为「测试件常驻生产」，一条为「调试门丢失」，一条为「门面零读者臂」，一条为「诊断口进生产面」，
一条为「纯访问器喂用例」——同一普查口径下的同型面，拆多单必互踩同一文件）。
取证基线：主仓 HEAD f974dd1f，全部行号按当下 HEAD 重取（原快照 fd17e895 的旧行号一律作废）。
去重基准：task/done/zero-consumer-pub-surface-census.md（批一）、
task/ing/zero-consumer-surfaces-batch-two.md（批二）、
task/ing/zero-consumer-dead-surfaces-batch-three.md（批三）、
task/ing/zero-consumer-dead-surfaces-batch-four.md（批四）、
task/done/zero-consumer-dead-symbols-cleanup.md 五张在册清单均不含本批符号；
GcConfig 字段与 DEFAULT_GC_* 常量归 task/ing/wkv-gc-compaction-interval-num-segments-surface.md，
whlog 三件归 task/ing/whlog-safe-tail-identity-alias.md，本批不重复立项。

类一 零生产消费者 pub 面：C# 无对位职，删或收 #[cfg(test)]

1. wlua 两份 script_digest 便捷包装（注释谎指接线）
   /Users/z/git/db/wedb/wedb/wlua/src/commands.rs:463-466（doc「供 resp 域接线使用」，resp 域零引用）、
   /Users/z/git/db/wedb/wedb/wlua/src/runner/mod.rs:391-394（doc「SCRIPT/EVAL 调度路径使用」，该路径零引用）。
   真实单点 /Users/z/git/db/wedb/wedb/wlua/src/cache.rs:290
   `SessionScriptCache::get_script_digest`，生产消费点 commands.rs:229、commands.rs:337、
   functions/redis.rs:111、functions/redis.rs:120。
   C#：/Users/z/git/db/wedb/garnet/libs/server/Lua/SessionScriptCache.cs:286 与 :289 是唯一摘要口，
   调用点 /Users/z/git/db/wedb/garnet/libs/server/Lua/LuaCommands.cs:114、:275、
   /Users/z/git/db/wedb/garnet/libs/server/Lua/LuaRunner.Functions.cs:412。
   删两口（不得两头各留一份带谎指的壳）。
2. wlua 导出面死常量 BLOCK_HEADER_SIZE
   /Users/z/git/db/wedb/wedb/wlua/src/limited_allocator.rs:554 + 再导出
   /Users/z/git/db/wedb/wedb/wlua/src/lib.rs:42，全仓零引用；分配路径按 BlockRef 偏移寻址
   （同文件 :241 一带 `self.block(block_ref)?.offset`），无 16 常量参与。
   C#：/Users/z/git/db/wedb/garnet/libs/server/Lua/LuaLimitedManagedAllocator.cs:49
   `private struct BlockHeader` 为私有嵌套类型，块头偏移由该文件 :31 注释与 :150 DataOffset 承担，
   C# 无「对外暴露块头尺寸常量」一职。删常量与导出项，块头语义如需留文档改为注释。
3. wbase 条带闩三谓词
   /Users/z/git/db/wedb/wedb/wbase/src/striped.rs:449 shared_count_at、:456 is_locked_at、
   :463 is_locked_exclusive_at；读者全在同文件 :559-:593 的 cfg(test) 区。
   批一已收该文件 try_* 六口，本三只 _at 白盒谓词未在册。
   C# 无对位符号（grep SharedCountAt / IsLockedAt 于 garnet 零命中）。收 #[cfg(test)] 或删。
4. wnode 回放上下文双 accessor 壳
   /Users/z/git/db/wedb/wedb/wnode/src/aof/replaycoordinator/aof_replay_context.rs:35 as_chunk、
   :43 as_record；读者全在同文件 :170 起 cfg(test) 区（:207、:238-:242）。
   C# 对位是 public readonly 字段直读
   （/Users/z/git/db/wedb/garnet/libs/server/AOF/ReplayCoordinator/ReplayOperation.cs:14 Record、
   :17 Chunk），rust 以 enum 承接后无需再留两个 Option 壳。删壳，形态判断按现役
   `ReplayOperation::key`（同文件 :51-56）的 matches/直匹配式收敛。
5. wnode 对象输出辅助 write_n2_array
   /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/object_store_utils.rs:156，
   读者仅同文件 :1363-:1365；C# 无 WriteN2Array。
   二选一：生产 n2 数组头输出改走该口（归单点），或收 #[cfg(test)]；禁「生产另有内联、口只喂用例」并存。
6. wedb 检查点条目白盒谓词 is_suspended
   /Users/z/git/db/wedb/wedb/wedb/src/server/replication/checkpoint_entry.rs:200，
   读者仅同文件 :260；C# CheckpointEntry.cs 侧只有 TrySuspendReaders（:80），无 IsSuspended 属性。收门或删。
7. wvector 写命名空间口 write_namespace
   /Users/z/git/db/wedb/wedb/wvector/src/store.rs:78，读者仅同文件 :548、:551；
   C# 无 WriteNamespace。无生产需求即删。
8. wvector 超时登记白盒口 active_deadlines
   /Users/z/git/db/wedb/wedb/wlua/src/timeout.rs:164（pub(crate)），读者在
   /Users/z/git/db/wedb/wedb/wlua/src/cache.rs:405、:411 用例；C# 全域无 ActiveDeadlines。收 #[cfg(test)] 或删。
9. wconn 客户端应答门面零读者臂（C# 门面类内不存在该臂）
   /Users/z/git/db/wedb/wedb/wconn/src/parser.rs:47 try_read_integer、
   :232 try_read_byte_array_array_with_length_header —— 读者全在同文件 cfg(test) 区
   （:312、:318 与 /Users/z/git/db/wedb/wedb/wconn/src/session.rs:386、:413，后者在 :350
   `#[cfg(test)] mod tests` 内）。
   /Users/z/git/db/wedb/wedb/wconn/src/parser.rs:82 try_read_int_with_length_header 有 C# 对位
   （/Users/z/git/db/wedb/garnet/libs/client/RespReadResponseUtils.cs:253），但 rust 读者仅
   :424、:430 用例。
   /Users/z/git/db/wedb/wedb/wconn/src/parser.rs:126 try_read_byte_array_with_length_header
   与 /Users/z/git/db/wedb/wedb/wresp/src/read.rs:239 同名重定义，门面内不留第二定义，
   臂改走 wresp::read 单点。
   C# 门面方法清单实读：RespReadResponseUtils.cs:18/:25/:44/:51/:77/:102/:155/:162/:212/:253/:260，
   无 TryReadInteger、无 TryReadByteArrayWithLengthHeader、无 TryReadByteArrayArrayWithLengthHeader。
   门面本体（逐臂薄转调 wresp::read）是正确形态，与 C# 逐个转调 RespReadUtils 同构，不动有读者各臂。
10. whyperlog 稠密寄存器数 getter reg_cnt
   /Users/z/git/db/wedb/wedb/whyperlog/src/lib.rs:143（返回 :144 self.mcnt），
   生产内部直读 mcnt，读者仅 lib.rs:247、:380、:649、:664（:218 起 cfg(test) 区）。
   C#：/Users/z/git/db/wedb/garnet/libs/server/Resp/HyperLogLog/HyperLogLog.cs:120
   `public int RegCnt => mcnt;` 在 C# 有生产读者（:111 DenseBytes 计算）；
   rust 该处已改预计算字段 dense_bytes（lib.rs:71 字段、:108 初始化、:119 同名访问器），故口失去读者。
   二选一：让生产侧改走该口（dense_bytes 由该口派生，消直读），或删口 + 用例改直读字段；勿并存。

类二 调试门转写丢失：C# 锁在 #if DEBUG / [Conditional("DEBUG")]，rust 进生产导出面

11. whyperlog 调试三件
   /Users/z/git/db/wedb/wedb/whyperlog/src/regs.rs:64 dump_raw_bytes、:74 compare_sparse_to_dense、
   :108 dump_regs；读者全在 /Users/z/git/db/wedb/wedb/whyperlog/src/lib.rs:218 起的 cfg(test) 区
   （:356、:618、:620、:626、:640、:645、:656）。
   C#：/Users/z/git/db/wedb/garnet/libs/server/Resp/HyperLogLog/HyperLogLog.cs:1100 `#if DEBUG`
   段内含 :1142 DumpRawBytes、:1161 CompareSparseToDense、:1208 DumpRegs。
12. wlua 分配器调试校验口
   /Users/z/git/db/wedb/wedb/wlua/src/limited_allocator.rs:288 debug_check（读者仅同文件 :578）；
   C#：/Users/z/git/db/wedb/garnet/libs/server/Lua/LuaLimitedManagedAllocator.cs:691-694
   `[Conditional("DEBUG")] static void DebugCheck(...)`，同文件 :1064 另有一处
   `[Conditional("DEBUG")] UpdateDebugAllocatedBytes` 同类门形态。
   修法（11、12 同）：补 `#[cfg(debug_assertions)]`（对位 C# 调试段）或收 `#[cfg(test)]`，
   禁 release 出货调试打印面，禁以新增 pub 面换取测试可见。

类三 C# 有生产调用点而 rust 生产旁路：一处定义被破，须接线或删壳（非纯删）

13. wlua 分配器白盒三件
   /Users/z/git/db/wedb/wedb/wlua/src/limited_allocator.rs:368 get_free_list（生产 :220、:229、
   :303-:304 直读 self.free_list 字段）、:210 is_infallible_allocation、
   /Users/z/git/db/wedb/wedb/wlua/src/tracked_allocator.rs:58 is_infallible_allocation（同名第二实现）。
   C#：/Users/z/git/db/wedb/garnet/libs/server/Lua/LuaLimitedManagedAllocator.cs:1171 私有
   IsInfallibleAllocation 有生产调用 :427、:452；私有 GetFreeList 有生产调用 :261、:311、:351、:517；
   同名件 /Users/z/git/db/wedb/garnet/libs/server/Lua/LuaTrackedAllocator.cs:83 亦在调用。
   修法：生产侧改走口（消字段直读第二形态），或判定口无价值删之并让测试改走公开面；
   两 is_infallible_allocation 实现须合一（对标 C# 两文件的继承关系，禁两份同义实现各自演化）。
14. wlua 常量串注册口
   /Users/z/git/db/wedb/wedb/wlua/src/strings.rs:175 constant_string_to_registry，
   读者仅同文件 :188（`#[cfg(test)] mod tests`，:180 起）。
   C#：/Users/z/git/db/wedb/garnet/libs/server/Lua/LuaRunner.Strings.cs:178-181 逐条生产调用
   （Ok/OkLower/Err/NoSessionAvailable 等常量注册）。
   修法：本仓常量注册链若走他径，把该口接回注册单点；若确无需求则删口并按既有 ignore 机制登记。
15. wlua 栈面与钩子口
   /Users/z/git/db/wedb/wedb/wlua/src/state.rs:635 update_stack_top、:402 push_cfunction、
   :330 push_constant_string、:138 try_ensure_minimum_stack_capacity、:594 try_set_hook。
   C#：/Users/z/git/db/wedb/garnet/libs/server/Lua/LuaStateWrapper.cs:207-:606 的 push/pop 族
   逐处内调 UpdateStackTop；:588 PushCFunction 被同文件 :601 内调；:132
   TryEnsureMinimumStackCapacity 被 /Users/z/git/db/wedb/garnet/libs/server/Lua/LuaRunner.cs:518、
   :547、:595、:629、:664 调用；:695 TrySetHook 被同文件链上 LuaRunner.cs:1373、:1379 调用；
   PushConstantString 对位见 LuaStateWrapper.cs:197 注释指引。
   现状：rust 生产侧以 lua_settop/lua_pushcclosurek 等内联形态绕开这些口，
   try_set_hook 与 /Users/z/git/db/wedb/wedb/wlua/src/state.rs:609 hook_shared_deadline
   成两套换挂入口（生产走后者：commands.rs:422）。
   修法：口与内联二选一地收口（生产改走口、内联副本删除），try_set_hook 与
   hook_shared_deadline 合并为单口；禁保留只喂用例的第二出口。
16. wkv 会话哈希薄壳
   /Users/z/git/db/wedb/wedb/wkv/src/session/consistent_read.rs:102 get_key_hash
   仅转发同文件 :64 自由函数 key_hash，读者仅 :376 用例；生产臂 :109、:126、:141、:158、:174
   一律直调 key_hash。修法：删薄壳或让生产统一走壳，不留同一事实两个出口。
17. 事务组锁键口（功能未接线，非死面）
   /Users/z/git/db/wedb/wedb/wnode/src/aof/replaycoordinator/aof_replay_context.rs:80
   iter_keys_to_lock 读者仅同文件 :231 用例；生产侧遍历 operations 的点是
   /Users/z/git/db/wedb/wedb/wnode/src/aof/aof_processor.rs:758 与
   /Users/z/git/db/wedb/wedb/wnode/src/aof/replaycoordinator/aof_replay_coordinator.rs:326-328。
   C#：/Users/z/git/db/wedb/garnet/libs/server/AOF/ReplayCoordinator/AofReplayCoordinator.cs:360
   生产调用 SaveTransactionGroupKeysToLock，实现 :436-:450 逐操作读 op.Chunk/op.Record 后
   SaveKeyEntryToLock。修法：核对本仓副本/恢复期事务组是否需要在重放前统一锁键——
   需要则生产改走该口，不需要则删口并登记，禁「有实现无消费」的第三份锁键链。
18. 内存日志收集器无装配点（功能未接线）
   /Users/z/git/db/wedb/wedb/wnode/src/logging.rs:205 MemoryLoggerProvider::create_logger 读者仅
   同文件 :536 用例；/Users/z/git/db/wedb/wedb/wnode/src/logging.rs:298
   MemoryForwardLogger::install 全仓生产零调用（唯一读者 :572-:574 用例），文件头 :8-:11 自称
   承接 C# GarnetServer 构造器的 initLogger + FlushMemoryLogger 生命周期；除
   /Users/z/git/db/wedb/wedb/wnode/src/lib.rs:52 再导出外整簇无实例化。
   C#：/Users/z/git/db/wedb/garnet/libs/host/GarnetServer.cs:86-88（new MemoryLoggerProvider →
   CreateLogger("ArgParser")）、:97 与 :232（FlushMemoryLogger 转真实 loggerFactory）、
   FlushMemoryLogger 实现 :614；/Users/z/git/db/wedb/garnet/libs/host/MemoryLogger.cs:14/:44。
   修法：把先行缓冲接进启动链单点（LoggingBuilder::from_node 现即日志装配单点，
   见 /Users/z/git/db/wedb/wedb/wedb/src/main.rs:22-24 与
   /Users/z/git/db/wedb/wedb/wedb_standalone/src/main.rs:50-56），或判定本仓不要该生命周期并
   删整簇 + 登记；禁「注释声称承接、实则无人装配」。

类四 复核后不立项（与 C# 同构，登记以防下轮重抄）
- /Users/z/git/db/wedb/wedb/wresp/src/read.rs:239 try_read_byte_array_with_length_header 本身：
  C# 同名件 /Users/z/git/db/wedb/garnet/libs/common/RespReadUtils.cs:725 全仓零消费者
  （原报「在生产读路径被真调」不实），rust 1:1 转写并挂锚点即合规；本批只处理 wconn 门面内的
  同名第二定义（类一第 9 项）。
- /Users/z/git/db/wedb/wedb/wvector/src/filter/attribute_extractor.rs:113 extract_field：
  C# 件 /Users/z/git/db/wedb/garnet/libs/server/Resp/Vector/AttributeExtractor.cs:99 在 libs、
  test、benchmark 全域零消费者，rust 同构，不删。
- /Users/z/git/db/wedb/wedb/wedb/src/server/replication/aof_sync_driver.rs:148 get_start_address：
  C# 件 /Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:93
  零消费者，rust 同构，不删。
- /Users/z/git/db/wedb/wedb/whyperlog/src/sparse.rs:196 sparse_to_dense_copy、:207
  sparse_to_sparse_copy：C# 件 HyperLogLog.cs:521/:537 为 private 且零消费者，
  且 C# CopyUpdate（HyperLogLog.cs:491-:512）与 rust copy_update（sparse.rs:168-:191）
  同为内联同款步骤——原报「按 C# 由生产 CopyUpdate 路调用」不成立，本批不动。
- /Users/z/git/db/wedb/wedb/wlua/src/runner/mod.rs:392 与 commands.rs:464 之外的
  LuaRunner/SessionScriptCache 面，以及 /Users/z/git/db/wedb/wedb/wnode/src/logging.rs:49
  LogFormatter::format_time（C# 对位件唯一消费者也在测试侧，1:1 同构）——沿用既有撤销结论，不重报。

优先级
死代码 > 重复/多套架构 > 功能缺口：类一为纯死面（写了没人调用）；类三第 13-16 项为
「同一事实两个出口」的重复实现；第 17、18 项为 C# 有生产调用点而 rust 未接线的功能缺口，
须补装配而非删壳。

协调
- 批四条目一（AofSyncDriver::assert_does_not_exist）与本批第 13-15 项同在
  wedb/wedb/src/server/replication 与 wlua 域，落地顺序无冲突，但禁同文件并发搬运。
- 第 5 项所在 object_store_utils.rs 的纯移动拆分票在册（object-store-utils-file-split），
  开工须错开，禁在纯移动里夹带删改。
- 第 9 项门面文件与 resp-server-session-file-split、
  task/ing/resp-null-protocol-single-source.md 同域，只删零读者臂，不碰协议裁决链。
- 第 18 项日志装配与启动装配投影单点化票
  task/ing/boot-assembly-projection-single-source.md 同文件族：那条管三字段样板，
  本项管 MemoryForwardLogger 装配，勿在两处各改一半。

验收
- 类一 10 组符号 grep 归零或转为仅 cfg(test)/debug_assertions 可见；
  `cargo check --workspace --all-targets`（私有 target 目录）零 error 零 warning，含 tests 面。
- 类二 4 口在 release 面不再导出（无新增 allow，无新增 pub 面）。
- 类三 6 组逐条给出「接线」或「删壳 + 登记」的终态，全仓不得留中间态；
  is_infallible_allocation 同名实现全仓唯一。
- ./js/check.js 无新增「缺失锚点/虚构锚点」报告（类一删口若涉 C# 件对位，须逐条登记 ignore）。
- test.sh/clippy 由中央整合轮执行，本单不跑。
