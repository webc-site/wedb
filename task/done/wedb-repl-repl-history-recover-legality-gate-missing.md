甄别结论：通过（甄别席 J3，2026-09-27，定级 P3——生产 count 恒 1 条件触发，机制完备性案）。版本门只写不读亲验：REPLICATION_HISTORY_VERSION 全仓 grep 仅 :15 定义、:78 写入两处，from_byte_array 只 parse 不校验，recover_or_init Ok 分支照单全收；C# FromByteArray :63-86 版本闸（:68-71 非空验、:73-74 抛 InvalidDataException）与 RecoverReplicationHistory catch→InitializeReplicationHistory(:119-131) 全命中。address.rs 函数行号逐点精确命中（new:45/set:63/equals:90/from_string:141/from_aof_binary:195/monotonic_update:257/min_exchange:275/any_lesser:290）；negotiate_resync unwrap_or(i64::MAX) 现码在（:932）；兄弟门 wcpr/meta.rs:340 format_version 闸在位。勘误：C# MaxSublogCount = 4 实为 AofAddress.cs:25（票面 :24，一行漂移）。派沙箱席 c01b。

审核结论：通过，两处描述修正（length-0 真危害在 negotiate:932 unwrap_or(i64::MAX) 钳位放行=错发增量而非错发全量；兄弟门实为三处非四处）

复制历史档案恢复臂无合法性判据：版本常量只写不读、位点向量长度不与装配子日志数对账，异代档与编辑档静默错读并连带序列化失败清空纪元档

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# 复制历史（主端纪元身份与档位点向量）读侧设单一代次闸：ReplicationHistory.FromByteArray（garnet/libs/cluster/Server/Replication/ReplicationHistoryManager.cs:63-86）先验载荷非空（:68-71），再判 version != ReplicationHistoryVersion 即抛 InvalidDataException（:73-74）；RecoverReplicationHistory（:119-131）catch InvalidDataException/EndOfStreamException/IOException → InitializeReplicationHistory(storeWrapper.serverOptions.AofPhysicalSublogCount)（:113-117）按装配子日志数重建 ReplicationHistory（:29-34 以 AofAddress.Create(aofPhysicalSublogCount, …) 铸造两位点向量）并 FlushConfig。即「代次不符＝非法档＝按装配 count 重建」是 C# 唯一出口，档位点向量长度恒等于装配 count 由该出口保证。count 自身在 C# 有硬界：garnet/libs/host/Configuration/Options.cs:223-225 [IntRangeValidation(1, AofAddress.MaxSublogCount)]（AofAddress.cs:24 MaxSublogCount = 4）。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
先记本席已核验为真、非案的三面：count 取值链只有单一来源（wconf/src/runtime_server_options.rs:207 取 DEFAULT_AOF_PHYSICAL_SUBLOG_COUNT=1；node_options.rs:1425 runtime_server_options 投影全函数不设置该字段；RuntimeServerOptions 无 Deserialize/serde 派生，故无 toml 通路；CONFIG 槽 aof-physical-sublog-count 系 ConfigMeta::read_only（runtime_server_config.rs:369-374）不可 SET；wedb/src/server/boot.rs:108 装配门强制 ==1 且在 initialize_replication_manager 之前）；分桶函数两侧同构（wnode/src/aof/garnet_log/addresses.rs:27 hash 单点转引 whasher::fast_hash_i64、:32/:39 与 C# GarnetLog.cs:92-100 同式，含负补码 (ulong)% 口径；分块链 C# GarnetLog.cs:737 与 rust single_log_branch.rs:165/206/209 皆取同一 chunk_header.key_hash 复用、不重算）；主端写出侧任务数长度源与 C# 同源（AofSyncDriver::new 取 rm.sublog_count，rust 侧 wedb/src/server/replication/aof_sync_driver.rs:73 与 C# AofSyncDriver.cs:111 一致；AofSyncDriverStore 折叠取 rm.sublog_count，aof_sync_driver.rs:433-435 与 C# :257 一致；replica_replay_driver_store.rs:14 定长界收口已由 todo/wedb-repl-appendlog-sublog-idx-range-guard-missing 在册，本票不重开）。
缺口在档案侧判据整段缺失：
其一，版本门只写不读。wedb/wedb/src/server/replication/replication_history.rs:15 REPLICATION_HISTORY_VERSION 全仓仅两处出现（:15 定义、:78 构造写入），from_byte_array（:103-110）只 parse 不校验 version，纯假桩。对照同仓四兄弟档案臂皆在码设门：wcpr/src/meta.rs:340（:13 注释自陈「非当前版本一律拒绝恢复」）、wcpr/src/index_ckpt/read.rs:70、wedb/src/server/cluster_config/serializer.rs:169、waof/src/aof/header/basic.rs:89——本臂独漏，系漏项非裁决，deviations 无同点面登记（§26 系 AofAddress 逗号串解析门禁，§116 系 repl_offset2 钳位判据本身）。
其二，位点向量长度不与装配 count 对账。recover_or_init（:132-143）入参 aof_physical_sublog_count 只在失败重建臂用到，Ok 分支对档案照单全收；ReplicationManager::with_options（replication_manager.rs:177-232）另以 AofAddress::create(sublog_count, …) 铸造活位点，current_replication_config 却留档案长度（recover_replication_history :312-317），两长度自此分叉。分叉后全平面的 min-length 口径开始静默补位：equals（waof/src/aof/address.rs:90）长度不等即假、monotonic_update（:257）/min_exchange（:275）/any_lesser（:290）只走 shared=min(len)、negotiate_resync 逐子日志 get(i).unwrap_or(0)/unwrap_or(i64::MAX)（replication_manager.rs:887-941，其中 :932 即 deviations §116 的 repl_offset2 钳位源）——缺槽按默认值参与裁决，陈旧槽按档案残值被当真值取用。
其三，同一越界判据三臂口径分叉。aof_address_toml::from_toml（replication_history.rs:36-56）对 6 元数组以 AofAddress::new（address.rs:45-51 clamp）静默截为 4 元、set 对 i>=MAX 静默丢（address.rs:63-66）、空数组回 length=0 位点；同文件另两解码臂硬拒：from_string 段数超界回 None（address.rs:141-152）、from_aof_binary 前缀越界或实长不符回 None（:195-205）。
其四，写侧同源毁档臂。to_byte_array（:87-100）Err 仅记日志并回 Vec::new()，flush_to_file（:127-129）把该空字节经 write_into 原子改名覆盖在册档案，纪元身份档被静默清成 0 字节；下次启动因 with_options 的 can_recover 判 len>0（replication_manager.rs:221-228）直接换新 repl_id。

3. 逻辑危害确证
代次闸缺位即「无旧兼容架构」下唯一在位的档案代次判据没了：新代档案（逐子日志扇出票落地后 replication_offset 语义/布局即属另一代）被老码当 v1 采纳，旧主纪元 ID 与旧位点原样生效，主端据不该存在的 repl_id/repl_id2 认领副本接续（same_main_store_checkpoint_history 与 same_history2 两判据皆以该 ID 为输入，replication_manager.rs:834-858），§116 修出的分叉历史钳位反被跨代残值喂入，副本照原样回放已确认分叉区间。
长度不对账即换 count 重启不报错也不拒绝（正是本域「无旧兼容」应给出的合法性处置缺位）：主端持久位点向量与 create(count) 活向量取 min 交集，未覆盖子日志的截断线/背压水位不再受副本进度钳制（fold_min_addresses 的 min_addr.get(i) 缺项直接跳过，aof_sync_driver.rs:433-441），AOF 删段可越过慢副本；空数组臂更令 any_lesser 恒 false，wait_for_replication_offset_async（replication_manager.rs:1109-1133）判「已追平」即时放行 failover/ensure_replication，把未追平副本顶上去＝静默丢写。toml 臂静默截断与另两臂硬拒分叉，使文件头自称可人读编辑的 replication.toml 一改即成不报错的失真档。
以上均非既定改良：本仓无 CLI/toml 投影、CONFIG 只读、boot 门 ==1 三面已核验在位，本票只补档案判据（生产 count 恒 1 面零行为变更），不建第二轨、不引入 count 用户通路。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/replication/replication_history.rs:REPLICATION_HISTORY_VERSION/from_byte_array/recover_or_init/to_byte_array/flush_to_file/aof_address_toml::from_toml
wedb/wedb/src/server/replication/replication_manager.rs:with_options/recover_replication_history/negotiate_resync/fold 消费面 wait_for_replication_offset_async
wedb/waof/src/aof/address.rs:new/set/get/equals/monotonic_update/min_exchange/any_lesser/from_string/from_aof_binary
wedb/wedb/src/server/replication/aof_sync_driver.rs:AofSyncDriver::new/fold_min_addresses
wedb/wedb/src/server/boot.rs:（aof_physical_sublog_count == 1 装配门，现状在位勿动）

对应 c# 文件与函数：
garnet/libs/cluster/Server/Replication/ReplicationHistoryManager.cs:ReplicationHistory.FromByteArray/InitializeReplicationHistory/RecoverReplicationHistory
garnet/libs/server/AOF/AofAddress.cs:AofAddress/Serialize/Deserialize/Equals/EqualsAll/AnyLesser/MonotonicUpdate
garnet/libs/host/Configuration/Options.cs:AofPhysicalSublogCount（IntRangeValidation 1..MaxSublogCount）
garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:AofSyncDriver（任务数组尺寸）/AofSyncDriverStore.cs:PublishShippedAddresses/SafeTruncateAof（主端写出侧长度源对账）

精炼执行方案：
1 判据落既有单点：recover_or_init（replication_history.rs:132-143）Ok 分支前加两校验——version != REPLICATION_HISTORY_VERSION、或 replication_offset/replication_offset2 的 length() != aof_physical_sublog_count——任一不符即视同损坏，落既有 Self::new(count)+flush_to_file 重建臂（对位 C# catch → InitializeReplicationHistory，不新建第二出口）。
2 版本校验落 from_byte_array（读侧唯一入口），不符回 io::ErrorKind::InvalidData，与 parse 失败共用同一 Err 通道；version 缺省仍走 #[toml(default = 1)]，既有 round-trip 锁测零改动。
3 长度对账走 AofAddress::length() 直读，不在调用侧另写数组长度推断；count 恒 1 时判据即「档案标量形或单元数组放行、其余重建」，天然覆盖手编辑越界档。
4 aof_address_toml::from_toml 越界口径向另两解码臂收口：数组元数 > MAX_SUBLOG_COUNT 报 Failed（弃静默截断），空数组报 Failed（弃 length=0 位点）。
5 to_byte_array 失败改为 Result 上抛（或 flush_to_file 见空字节即跳过落盘并告警），杜绝原子写把在册档清成 0 字节。
6 测试验证点：wedb/wedb/tests/replication_history_unit.rs 增四臂锁测——version=2 档、count=1 配 3 元数组、6 元数组、空数组，各断言走重建（新 repl_id + 向量长度等 count）或 Err；既有 test_replication_history_roundtrip 与 test_ignore_unknown_fields_forward_compatibility 维持绿；replication_manager.rs 面补一条「重建臂后 get_current_replication_offset().length() == sublog_count」判据锁。

收口记录（收票席 R4 批次，2026-09-28）：合入 a3b2e9eb（验货 6f9ead73）。收口形态=from_byte_array 读侧代次闸（version 不符回 InvalidData，对位 C# FromByteArray:73-74）+ recover_or_init Ok 分支对账两向量 length 与装配 count、不符落既有重建臂（catch→Initialize 唯一出口）+ from_toml 空数组/超 MAX_SUBLOG_COUNT 硬拒向 from_string/from_aof_binary 口径收口 + flush_to_file 空字节跳写保档；waof 仅导出常量，address.rs 零改动。锁测 replication_history_unit 四臂 + replication_manager 判据锁，撤修复双红实测。风险备案：flush 空字节系跳过告警不上抛（内存/磁盘短暂分叉换不毁档）。
