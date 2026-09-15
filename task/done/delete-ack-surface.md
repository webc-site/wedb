删除自创 ACK 确认面（死代码整面清理）— 已完成

背景

next/net.md 第 9 条与 next/ds.net.md 第 13 条（同问题）：
rust 侧存在 C# 不存在的「副本 → 主端逐记录 ACK」面，生产全链零调用。

核实结论（对照 garnet）

garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:26-56
字段清单：clusterProvider、physicalSublogIdx、garnetClient、localNodeId、remoteNodeId、cts、
startAddress、iter、previousAddress、timePulse 族、backpressure/shipped watermark 族。
无 acked、无 last_ack_timestamp、无逐记录 ACK 方法。

garnet/libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs
方法清单：ResumeReplay/SuspendReplay/Dispose/Consume/Throttle/SignalTimeAdvance/
ValidateSublogIndex/InitializeBackgroundReplayTask/ThrottlePrimary。
无 ACK 构造与上报方法。

生产调用链核实：
handle_replica_ack 仅 tests/replication_stream_e2e.rs、tests/replication_pipeline.rs 调用；
create_replication_ack 仅测试调用；prune_timed_out_replicas 零调用（仅 replica_wire.rs 文档引用）；
acknowledge_inflight 仅 process_ack 内部调用（测试链）→ send_buffer_pool 在途水位生产只增不减；
last_ack_timestamp 生产永不更新 → is_ack_timed_out 一旦被调用恒 true。

删除清单（已全部落地）

wedb/wedb/src/server/replication/aof_sync_task.rs
删 ReplicationAck 结构体；字段 acked_address、last_ack_timestamp、send_buffer_pool；
方法 acked_address、last_ack_timestamp、is_ack_timed_out、process_ack、current_inflight_bytes、
with_buffer_pool；consume 内 track_inflight_send 记账。
单测 test_aof_sync_task_lifecycle_and_ack → test_aof_sync_task_lifecycle_and_throttle
（删 ACK 断言，补 shipped_watermark 断言）。

wedb/wedb/src/server/replication/aof_sync_driver.rs
删方法 acked_address、get_acked_address、process_ack、is_any_task_ack_timed_out；
new 内缓冲池构造改直建任务。单测删 ACK 断言，补 get_shipped_watermark_address 断言。

wedb/wedb/src/server/replication/aof_sync_driver_store.rs
删方法 process_replica_ack、prune_timed_out_replicas；删 wbase::time 导入；
结构体文档删「ACK 确认」表述。

wedb/wedb/src/server/replication/driver_registry.rs（规划外连带）
删 retain 方法：prune_timed_out_replicas 删除后零调用者，属死代码连带清理。

wedb/wedb/src/server/replication/replication_manager.rs
删方法 handle_replica_ack。

wedb/wedb/src/server/replication/replica_replay_driver.rs
删方法 create_replication_ack；删 ReplicationAck/wbase::time 导入；单测删 ACK 断言。

wedb/wedb/src/server/replication/network_buffer.rs
整删 ReplicationSendBufferPool 与 ReplicationSendBuffer（含 track_inflight_send、
acknowledge_inflight、current_inflight_bytes、is_throttled、max_inflight_bytes、
acquire/release/borrow_count、Default、DEFAULT_SEND_BUFFER_SIZE、DEFAULT_RING_BUFFER_SLOTS）
及 5 个池单测。
保留 ReplicationNetworkBufferSettings 与 MAX_CHUNK_SIZE（C# 1:1 对标）及 settings 单测。

水位机制处置
send_buffer_pool 在途水位回收唯一路径是被删的 acknowledge_inflight（生产只增不减），
随 ACK 面整体删除，不留半截机制。背压对齐 C# 既有面：shipped watermark
（consume 推进 shipped_watermark_address → throttle → publish_shipped_address 闸门），
断链感知靠 is_connected（socket 态 + wire 健康面），均已在位，无需新增。

文档修订

replica_wire.rs 模块文档：删「溢流积压由 ACK 超时剔除（prune_timed_out_replicas）治理」，
改为溢流上限 MAX_OVERFLOW_ENTRIES 硬封顶 + 超限断连（通道健康面）。
aof_replication_pump.rs 模块文档与 dispatch_frame 注释：删发送缓冲池记账表述，
改为 AofSyncWire 发送通道承接。
next/net.md 条目 5 改法删「create_replication_ack 面归条目 9 处置」残留引用；
next/ds.net.md 条目 9 位置列表删 create_replication_ack 引用（前代理删条目时的残留）。

测试处置

tests/replication_stream_e2e.rs：删步骤 7 ACK 上报段，更名
test_replication_full_chain_stream；保留全链推流/位点闭环/截断（改受已发位点约束语义）/
节流/断连断言。
tests/replication_pipeline.rs：删步骤 8 ACK 段与 test_network_buffer_pool_reuse_and_backpressure；
test_large_record_chunking_and_inflight_tracking 更名 test_large_record_chunking。
tests/replication_data_source.rs：删步骤 8 在途字节断言与步骤 9 ACK 段。

验证结果

bun ./js/check.js：0 缺失 0 重复（退出 0，check/miss 空）
./clippy.sh：0 警告
./test.sh：全过（2017 passed + regress 2 passed）
删除符号均无 C# 对标函数，无需 js/check/ignore 登记。
