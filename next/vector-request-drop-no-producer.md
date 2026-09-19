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
