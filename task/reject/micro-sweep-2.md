# micro-sweep-2 拒绝项与甄别结论

## 一 design.md 条 20「监听端口 dyn 消除终局（三端口处理记录）」：拒绝立项（记录性文本非待办）

甄别结论：该条是三条端口处理决策的记录文本（标题即「三端口处理记录」），逐项核实全部已落地、无行动项，按 reject 归档；next/ 原文由主代理清理。

核实证据（dev @ ee156ec 全仓 grep）：

1. VersionShiftFn 调用点组合消除——已落地
   - `wedb/src/server/cluster_provider.rs:423` notify_version_shift_start、`:434` notify_version_shift_end（显式方法，C# checkpointVersionShiftStart/End 委托的 rust 形态）
   - 检查点发起方接线测试 `wedb/tests/checkpoint_wiring.rs:30/:92`
   - 符号 VersionShiftFn 全仓零残留
2. ReplicationSinkFn 信号化拉取消除——已落地
   - `waof/src/wal/log.rs`（原 log.rs:36）ReplicationWakeTx = crossfire MAsyncTx<Array<()>> 容量 1 唤醒信号、`:193` set_replication_wake（满即折叠）
   - `wedb/src/server/replication/aof_replication_pump.rs:147` AofReplicationPump::attach_wake + spawn 增量拉取循环；生产接线 `replica_sync_session.rs:118`
   - 符号 ReplicationSinkFn 全仓零残留
3. StoreEventSink 保留 Arc<dyn Fn>——已落地
   - `wkv/src/store/event.rs:86` StoreEventSink = Arc<dyn Fn(StoreEvent<'_>) + Send + Sync>，事件环拉取方案否决论证在同文件注释

条内「生产接线待 AOF 门控复制面完工补挂」属 AOF 门控复制面立项的既有待办，非本条新增行动项，不构成立项理由。

## 二 浮点格式化三处收敛：主体前置批已落地，仅做残余收口

甄别：wresp/src/resp_memory_writer.rs format_double 为权威单点；wcol/src/resp/output.rs format_double_to / format_double 已转调 wresp::format_double；wnode 全部消费面已走单点。任务下达时的三处重复形态已不存在。
本批仅收口 wcol/src/hash/hash_object_impl.rs 私有 format_double 的 String 中转（ObjectOutput::format_double(...).into_bytes() → zmij::Buffer 直取 as_bytes().to_vec()），消灭 HINCRBYFLOAT 热路径一次堆分配。

## 三 hex 微工具两处：甄别修正为「一处下沉，一处保留」

甄别：两处并非同构重复。
- wacl/src/acl_password.rs hex_val：hex 字符折 4bit 值，二进制解码路径——与 wlua 构成表面相似
- wlua/src/hash_key.rs from_hex：校验 + 小写化文本形态（std is_ascii_hexdigit 内联），ScriptHashKey 保存的是 40 字符 hex 文本而非二进制摘要，无折值实现，无重复定义
处置：wbase 新建 hex 模块（cfg feature "hex"，hex_val 一处定义 + hex_decode 定长解码），wacl 删本地 hex_val 转调 wbase；wlua hash_key.rs 保留不动（判非重复，无 ignore 负担）。
