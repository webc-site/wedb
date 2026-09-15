# aof 重放域三处重复收敛

来源：next/glm.md 条 24、51、52（主代理预清理后下发）。分支
w3-aof-replay-dedup（已合并 dev 并合回主目录，worktree 与分支已清理）。

## 甄别结论（三条全成立）

一、object_store_rmw 四对象块泛型化（P1）：成立。C#
libs/server/AOF/AofProcessor.cs:659 ObjectStoreRMW<TObjectContext> 为单份
泛型实现，经 Tsavorite objectContext 多态单通道；rust 原状
wedb/wnode/src/aof/aof_processor.rs 的 replay_object! 宏 + match 四分支是
文本级复用（四份类型各异的宏展开），且 load 路径多一层 Vec::from 堆克隆
（obj_decode 借用切片后先 clone 再 from_blob）。

二、序列号提取单点（P2）：成立，且原实现与 C# 有偏差。C# 四处取数语义
一致（GetSynchronizedOperationParams / CanReplay / SkipReplay /
AofReplayCoordinator.cs:UpdateMaxSequenceNumberFromHeader）：ShardedHeader /
ShardedChunkHeader / ShardedLogTransactionHeader 取内嵌 sequenceNumber，
其余形态取条目地址。rust 五处散落，其中
aof_replay_coordinator.rs update_max_sequence_number_from_header 与
aof_processor.rs skip_replay 的兜底分支把 ShardedLogTransactionHeader 也
fallback 到条目地址，与 C# 不符（C# 取内嵌）。

三、存储过程参数区编解码两份（P2）：成立。C# 参数区编解码单点在
SessionParseState.SerializeTo / DeserializeFrom（libs/server/Resp/Parser/
SessionParseState.cs，StringInput 与 CustomProcedureInput 共用，布局
[count i32][逐参 (4B len + bytes)]）。rust 两份同构安全切片版：
stored_proc_replay.rs stored_proc_args::encode/decode 与
ReplayInput::encode_to_slice/deserialize 的参数区段；stored_proc_payload
另手写头尺寸判定，与 waof AofHeader::skip_header 重复。

## 改动

1. wedb/waof/src/header.rs
   新增 AofHeader::sequence_number_of(entry, fallback) -> Option<i64>：
   分片三形态取内嵌 sequenceNumber（ShardedLogTransactionHeader 的
   sequenceNumber 位于 sharded 段 FieldOffset 16，解析前 24B 即得），其余
   返回 fallback；头缺失 / 未知类型 / 分片段截断返回 None。
2. wedb/waof/src/args.rs（新文件，lib.rs 导出）
   参数序列区编解码单点 encode_arg_sequence / arg_sequence_len /
   decode_arg_sequence（count 防爆破 capacity 防御 + 逐参边界校验），
   注释映射 SessionParseState.SerializeTo/DeserializeFrom。
3. wedb/wnode/src/aof/aof_processor.rs
   - get_synchronized_operation_params / can_replay / skip_replay 收敛调
     sequence_number_of（skip_replay 顺带修正 ShardedLogTransactionHeader
     取内嵌，对齐 C#）；prepare_key 不同调：其序列号与 header_size 同次
     parse 耦合，拆分徒增重复解析。
   - ReplayInput 参数区编解码改调 waof 单点（encoded_len_for_slices /
     encode_to_slice / deserialize）。
   - object_store_rmw：宏与四 match 分支替换为本地封闭 trait
     ReplayObject（关联 TAG + load/dump/is_empty/apply，四对象类型静态
     实现，无 dyn）+ 泛型 async 单通道 replay_object_channel；顺带消除
     obj_decode 后的 Vec::from 中间克隆。
   - replay_object_channel 注释措辞避开 C# 映射注释格式（check.js 否则
     判与 object_store_rmw 重复映射）。
4. wedb/wnode/src/aof/replaycoordinator/aof_replay_coordinator.rs
   - txn_header_sequence_number 删除，调用点直接调 waof 单点。
   - update_max_sequence_number_from_header 收敛（修正
     ShardedLogTransactionHeader 取内嵌对齐 C#；分片段截断的损坏条目
     以 0 兜底保持原口径）。
5. wedb/wnode/src/aof/replaycoordinator/stored_proc_replay.rs
   - stored_proc_args::encode/decode 收敛为 waof 单点薄委托。
   - stored_proc_payload 改 AofHeader::skip_header 单点。

范围外不动：waof/src/log.rs（group-commit 并发域）、store_rmw 的 expire
换算段（并发域）、wcol from/to_blob 包装（resp 命令面生产 API）。
删除符号 txn_header_sequence_number 为纯 rust 内部辅助（无 C# 映射注释），
不需 check/ignore 登记；check.js 验证零新增缺失/重复。

## 验证结果

1. bun ./js/check.js：0 缺失，0 重复（分支与主目录均验）。
2. ./clippy.sh：3 tasks completed，零警告（禁 allow，分支与主目录均验）。
3. ./test.sh：合并全部并发分支后的主目录 HEAD（4773c33）全量
   2018 passed / 0 failed / 1 skipped，regress 回归门禁 2 passed。
4. 对象四类型 RMW 重放与存储过程重放既有测试全过：
   object_envelope_regression.rs（binary_string_survives_aof_restart /
   object_rename_survives_aof_restart / collection_key_cross_type_semantics）、
   aof_stored_proc_replay.rs（stored_proc_entry_replays_via_recover /
   flush_db_entry_replays_targeted_database）、recover_test.rs、
   vector_replication_replay.rs。

## 工程备忘

全局 cargo config target-dir=/tmp/rust 被多个并发 worktree 共享，同名
crate 产物（如 libwaof.rlib）互相覆盖会导致 name resolution 时灵时不灭
（E0425 找不到刚新增的符号）。并发期编译建议显式指定独立
CARGO_TARGET_DIR；本分支验收均以独立 target 目录复核。
