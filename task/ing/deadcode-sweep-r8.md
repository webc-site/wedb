# deadcode-sweep-r8 细化方案

本票合并处理 next/zcode.design.md 的问题 10（跨 crate 零引用辅助函数与未接入校验）与问题 8（wresp 错误模板常量与对象存储手写字面量脱节）。逐条对照 C# 复核后给出处置，处置原则为对标 C#：功能性能力在 C# 生产链路真实存在且 rust 应接而未接者接通；纯冗余或 rust 已有单点替代者删除；错误模板按 C# 一处定义收敛。

## 问题 10 逐条处置

### 1. wnode/src/primary_tasks.rs commit_task_running：删除
rg 复核：全仓仅定义处一处，无任何引用（连测试都不用）。C# 对位为 libs/server/TaskManager/TaskManager.cs:IsRunning，复核 rg 全仓，IsRunning 仅在 test/standalone/TaskManagerTests.cs 出现，生产 libs 零调用。且 C# 的提交任务运行态由 taskManager 注册表内部承载，TryStartCommitTask 用 RegisterAndRun 幂等注册，从不查询运行位。rust 侧周期提交判定已经由 PrimaryTasks 的 commit_started 位在 try_start_commit_task 的 swap 与 spawn_aof_commit_task 的自退出置位完整承接（见 config_owner.rs:46、service.rs:1558 的调停与拉起链路），该 getter 是纯冗余观测访问器，无 C# 生产对位调用点，接通等于凭空造调用点，属扩面。故删除方法及其文档注释。姊妹方法 object_collect_running 因被 object_collect_task.rs 测试引用且不在本票票面，保留不动。

### 2. wtxn/src/transaction_manager.rs add_transaction_store_types（复数）：删除
rg 复核：复数版全仓仅定义处，零引用。C# libs/server/Transaction/TransactionManager.cs:AddTransactionStoreTypes 与单数 AddTransactionStoreType 并存且复数被大量调用（BitmapOps、MainStoreOps、SetOps、SortedSetOps 等以 Main|Unified 组合登记）。但 rust 侧存储面登记已收敛到单点单数版 add_transaction_store_type（生产调用在 txn_key_manager.rs:72 与 txn_resp_commands.rs:308，逐键按 StoreType 登记），复数版在 rust 无对应消费形态，属与单数版重复的残留。故删除复数方法。注：store_types 字段在 rust 目前只写不读，读取接线不在本票票面，不处理。

### 3. wbitmap/src/bitfield/parse.rs is_large_enough_for_type：删除
票面称 C# 仅 Debug.Assert 用，复核不实：C# 在 Storage/Functions/MainStore/RMWMethods.cs:612 的 BITFIELD RMW 生产分支调用 BitmapManager.IsLargeEnoughForType 决定是否扩容值缓冲。但 rust 侧该扩容判定已由单点 new_block_alloc_length_from_type 与 length_from_type 承接（生产调用在 wnode slow.rs:906、bitmap_commands.rs:501 与 550），is_large_enough_for_type 仅是 length_from_type(args) <= vlen 的薄封装，rust 无消费点。属 rust 已有单点替代，删除该函数，并删除 wbitmap/src/lib.rs 对它的重导出。

### 4. wbase/src/pool/limited.rs as_slice 与 as_mut_slice：删除
PooledRefBuffer 已实现 Deref/DerefMut 到 Vec，切片访问经 deref 覆盖。as_mut_slice 全仓零引用。as_slice 除自身外仅被本文件 Debug 实现（用 .len()）与两处测试使用。C# LimitedFixedBufferPool.cs 的 PoolEntry 无对应独立切片方法。删除两方法，Debug 改用底层 Vec 取长度，两处测试的 .as_slice() 改切 (&b[..])。

### 5. wnode/src/resp/resp_server_session/pump.rs write_direct_large：删除
rg 复核：生产零调用，仅 4 处集成测试用作写 output 的桩。C# RespServerSession.cs:WriteDirectLarge 是真实生产大块直写（KeyAdminCommands.cs:213 DUMP 租用大缓冲回写）。但 rust 采用参数线程化输出：DUMP 等大输出命令处理函数签名收 output: &mut Vec<u8>（key_admin_commands/slow.rs:480 output.extend_from_slice），且托管 Vec 自动扩容使「大块」与「小块」写无差别，生产一律直接 extend_from_slice。session 绑定 self.output 的 write_direct_large 因借用冲突在生产处理函数内不可调用，无 C# 对位生产调用点。而 RespServerSession.output 为 pub 字段（core.rs:170），测试直接 session.output.extend_from_slice 即可，符合测试已有的 s.output.clear() 用法。故删除方法，4 处测试改为直接 session.output.extend_from_slice。

### 6. wconf/src/runtime_server_config.rs ensure_valid_kind 与 ensure_supported_enum：接通
C# libs/server/Config/RuntimeServerConfig.cs:EnsureValidKind 与 EnsureSupportedEnum 在 BuildMeta 的 Set（:100 与 :99）与 SetReadOnly（:108）局部函数里逐条调用，建表期声明非法即抛 InvalidOperationException。rust 的 META 为编译期常量数组，无 C# 的运行时建表循环，故这两个校验函数目前只被 tests/runtime_server_config.rs 引用，生产未接入。处置：新增 validate_meta 一次性遍历，逐条对 is_runtime 项调用两函数，首个 RuntimeServerConfig::new 构造时在 Once 门内执行，声明非法 expect panic，承接 C# 建表期硬校验语义（表为静态常量，正确性由单元测试保证，运行期绝不误触发）。两函数因此转为生产引用，删除原注释中「建表处以 debug_assert 调用」的不实描述。集成测试 meta_static_validity 独立锚定同一不变式，保留。

### 7. wedb/src/server/cluster_manager.rs get_range：接通
C# libs/cluster/Server/ClusterManager.cs:GetRange 将升序槽位数组合并为 start-end 区间串，仅在日志格式化中调用：ClusterManagerSlotState.cs 的 TryAddSlots(:40)、TryRemoveSlots(:70)、TryPrepareSlotsForMigration 批量(:202)、TryPrepareSlotsForImport 批量(:319) 的 LogTrace。rust 对应四个方法 try_add_slots、try_remove_slots、try_prepare_slots_for_migration、try_prepare_slots_for_import 均有同点位 trace! 但以 {:?} 直打 HashSet（无序），未接 get_range。C# 单槽版与非批量版不调 GetRange，rust 亦不动单槽臂。处置：把上述四个批量槽位方法的 trace! 接到 get_range——先把 slots 集合收集升序排序再格式化，产出与 C# 一致的区间串。这些是低频集群管理操作（前后本有 flush_config 落盘），排序开销可忽略，非热路径。get_range 的 rust 版已能处理空集，无需改函数体。

## 问题 8 处置

### 8. wresp/src/cmd_strings.rs GENERIC_ERR_MANDATORY_MISSING 与 GENERIC_ERR_MUST_MATCH_NO_OF_ARGS：接通（收敛复用）
两模板常量对位 C# libs/server/Resp/CmdStrings.cs:GenericErrMandatoryMissing(:339) 与 GenericErrMustMatchNoOfArgs(:340)，带 {0} 占位。C# 调用点以 string.Format(常量, "FIELDS"/"numFields") 逐命令传词元（HashCommands.cs:611/:621 等）。rust 侧 object_store_utils.rs 的 mandatory_missing_err 与 must_match_args_err 手写了两组完整字面量（FIELDS/MEMBERS、numFields/numMembers），而模板常量零引用。C# 有对应常量，按「一处定义」让业务侧复用标准常量。处置：删除 mandatory_missing_err 与 must_match_args_err 两个手写方法，调用点改 cs::GENERIC_ERR_MANDATORY_MISSING.replace("{0}", kind.token()) 与 cs::GENERIC_ERR_MUST_MATCH_NO_OF_ARGS.replace("{0}", kind.num_param())，与既有 sorted_set_commands、list_commands 对 GENERIC_PARAM_SHOULD_BE_GREATER_THAN_ZERO 的 replace 用法同形。kind.token 与 kind.num_param 保留（现由调用点使用）。替换后字节输出与既有断言逐字一致（test_parse_elements_header_fields_and_members 不变）。票面未列 greater_than_zero_err，其模板 GENERIC_PARAM_SHOULD_BE_GREATER_THAN_ZERO 非死常量，不在本票范围，不动。

## 验收
仅跑受影响 crate 的 cargo check 与定向测试：wbase（pool 测试）、wbitmap、wtxn、wconf（runtime_server_config 测试）、wresp、wnode（object_store_utils 测试、resp_server_session 与 net_pump 集成）、wedb（cluster_management 测试）。
