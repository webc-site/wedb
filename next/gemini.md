# gemini 待办

1. [P1] COMMITAOF 提交链空壳
   位置：wedb/wnode/src/resp/admin_commands.rs:144（commit_aof_async 直返 Ok(true) 占位）、:160-176（network_commitaof 仅回固定文案，不走慢路径）；wedb/wnode/src/resp/garnet_api.rs（存储执行域无 AOF commit 端口）
   对标：garnet/libs/server/Resp/AdminCommands.cs:NetworkCOMMITAOF、garnet/libs/server/Storage/Common/StoreWrapper.cs:CommitAOFAsync
   问题：命令不产生物理提交；提交面仅存在于检查点域内部（wedb/wdatabase/src/database_manager_base.rs:263 commit_aof，由 multi_database_manager.rs:325 调用），命令通道未接；亦无测试断言 committed_until 物理推进与 aof_commit_ms 周期推进
   改法：经慢路径通道接通 StoreGarnetApi → AOF commit_to(safe_tail)，补 committed_until 推进与周期提交测试

7. [P2] 集合算子入参切片化
   位置：wedb/wcol/src/resp/input.rs:91（ObjectInput 仍为 operate 封装通道）；wedb/wresp/src/argslice/arg_slice.rs:6-9（ArgSlice 仍为 *const u8 裸指针）
   对标：garnet/libs/server/Objects/Types/GarnetObject.cs:Operate
   问题：operate 链路多一层封装，ArgSlice 裸指针契约带 unsafe 语义
   改法：operate 直接收 &[&[u8]] 切片视图，ArgSlice offset 化消除裸指针

9. [P2] 六个千行巨型文件拆分
   位置：wedb/wnode/src/resp/resp_server_session.rs（3993 行）、wedb/wlua/src/functions.rs（2415）、wedb/wlua/src/runner.rs（2042）、wedb/wnode/src/aof/aof_processor.rs（1915）、wedb/wedb/src/server/cluster_session.rs（2116，就地拆分，不迁 wcluster，见 task/reject/wcluster-premature-split.md）、wedb/whyperlog/src/lib.rs（1605）
   对标：garnet/libs/server/Resp/RespServerSession.cs（C# 以 partial class 拆分）、libs/server/Objects/
   问题：均超 rust_review 1500 行阈值
   改法：拆为模块文件夹：resp_server_session/（string/admin/txn/object_cmds）、functions/（redis/cjson/cmsgpack/bitop）、runner/（resp_convert/lock_guard/executor）、aof_processor/（replay_data/replay_txn/replay_ckpt）、cluster_session/（slot_verify/cmd_cluster/cmd_replication）、whyperlog/（sparse/dense/cardinality）

10. [P2] wnode/src 内嵌集成测试迁移
    位置：wedb/wnode/src（45 个源码文件内嵌 #[cfg(test)]，共 258 处 #[test]）
    对标：garnet/test/standalone/Garnet.test/
    问题：网络泵、AOF 跨模块重放等集成测试混在 src，拖慢增量编译
    改法：src 内只留纯函数与私有结构轻量测试，集成测试移入 wedb/wnode/tests/

11. [P2] ReplayAlignBarrier 多并发轮次测试
    位置：wedb/wnode/src/aof/readconsistency/replay_align_barrier.rs:359（tests 仅 N=2：全员放行、阻塞唤醒、超时弃权、disable/enable）
    对标：garnet/libs/server/AOF/ReplayAlignBarrier.cs
    问题：N>2 多节点并发轮次与周期 rollover 未覆盖
    改法：参数化扩 N>2 并发轮次用例

12. [P2] backpressure_wait_key 复制滞后测试缺失
    位置：wedb/wnode/src/aof/garnet_log.rs:904（实现在位）；现有测试仅 pool 级背压（wedb/wedb/tests/replication_pipeline.rs:218）
    对标：garnet/libs/server/AOF/GarnetLog.cs:BackpressureWaitKey
    问题：按 key 的复制滞后等待无端到端测试
    改法：补滞后触发 wait_key 阻塞、水位推进唤醒的集成用例
