# gemini 待办

1. [P1] COMMITAOF 提交链空壳
   位置：wedb/wnode/src/resp/admin_commands.rs:144（commit_aof_async 直返 Ok(true) 占位）、:160-176（network_commitaof 仅回固定文案，不走慢路径）；wedb/wnode/src/resp/garnet_api.rs（存储执行域无 AOF commit 端口）
   对标：garnet/libs/server/Resp/AdminCommands.cs:NetworkCOMMITAOF、garnet/libs/server/Storage/Common/StoreWrapper.cs:CommitAOFAsync
   问题：命令不产生物理提交；提交面仅存在于检查点域内部（wedb/wdatabase/src/database_manager_base.rs:263 commit_aof，由 multi_database_manager.rs:325 调用），命令通道未接；亦无测试断言 committed_until 物理推进与 aof_commit_ms 周期推进
   改法：经慢路径通道接通 StoreGarnetApi → AOF commit_to(safe_tail)，补 committed_until 推进与周期提交测试

2. [P1] Checkpoint 网络导入面（M4）缺失
   位置：wedb/wedb/src/server/cluster_session.rs（无 CLUSTER SNAPSHOT_DATA / SEND_CKPT_METADATA / SEND_CKPT_FILE_SEGMENT 处理分支，仅 wedb/wnode/src/resp/parser/resp_command.rs:435-438 枚举命名在位）；本地恢复面已备：wedb/wcpr/src/manager.rs:599 recover_checkpoint_components
   对标：garnet/libs/cluster/Server/Replication/ReplicaSyncSession.cs
   问题：三 arm 无任何处理代码，staging 写入 → 恢复组件 → WedbStore 替换在线引擎的导入闭环不通；CheckpointFileType 对齐（HLOG=1, INDEX=4）无对应枚举
   改法：补三 arm 处理与导入闭环；明确不做 kind 2-5（vector/RangeIndex/chunked 帧）、AUTH 透传、ATTACH_SYNC/BEGIN_REPLICA_RECOVER（AOF 直推架构替代）

4. [P2] StoreWrapper.Reset 语义未落地
   位置：wedb/wnode/tests/vector_set_cleanup_vs_reset_race.rs:11（自注「尚未落地」，现测试仅近似锤击）
   对标：garnet/libs/server/Storage/Common/StoreWrapper.cs:Reset
   问题：缺 Pause → Reset → Resume 三段语义，清理任务与重置的竞态回归未覆盖真实路径
   改法：在存储与节点层落地 Reset，补三段锤击测试

5. [P2] 迭代式槽位校验慢路径未落地
   位置：js/check/ignore/server.yml:695-698（NetworkIterativeSlotVerify 豁免在案）；基础面已建：wedb/wedb/src/server/slot_verify.rs:282 multi_key_slot_verify
   对标：garnet/libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:NetworkIterativeSlotVerify
   问题：迭代逐键槽位验证入口缺失，靠 ignore 豁免
   改法：补迭代形态入口，消除豁免

6. [P2] 地理坐标解析双份
   位置：wedb/wcol/src/parse_utils.rs:43 与 wedb/wnode/src/session_parse_state_extensions.rs:631（同名 try_get_geo_lon_lat，后者于 :655-658 重复硬编码经纬度边界常量）
   对标：garnet/libs/server/SessionParseStateExtensions.cs:TryGetGeoLonLat
   问题：同一解析在算法库与节点库各一份，边界常量有漂移风险
   改法：收敛至 wcol::parse_utils，wnode 侧保留错误文案形态薄封装

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
