# 网络协议共识同步迁移深度审查与实施方案

1. [P0] 修复集群启动时 compio runtime 未进入前拉起后台任务导致崩溃的架构缺陷
   位置：wedb/wnode/src/server.rs:218（ServerBootstrap::run_async）
   对标：garnet/libs/host/GarnetServer.cs:Start、garnet/libs/cluster/Server/ClusterProvider.cs:Start
   C# 机制：GarnetServer 在构造与初始化完成网络服务基础架构后，先执行 Provider.RecoverAsync() 恢复存储与检查点状态，再在 Server.Start() 时序中显式调用 clusterProvider.Start() 拉起 Gossip 与复制后台线程，确保运行时与网络上下文完全就绪。
   问题：Rust 采用 Thread-per-core 的 compio 异步运行时，compio::runtime::spawn 依赖当前线程局部已初始化的 Runtime 上下文。ServerBootstrap::run_async 在第 218 行直接调用 self.cluster_provider.start()，此时 Runtime::new()? 尚未执行，主线程无 active compio runtime，内部 GossipManager::start 调用的 spawn 直接 panic 抛出 there is no reactor running 或静默失效，导致 Gossip 与后台探测协程完全无法运行。
   方案：
   调整时序：将 self.cluster_provider.start() 移入 rt.block_on(async move { ... }) 内部，在 session_provider 与存储引擎装配完成、server.run_until_shutdown 执行之前调用。
   生命周期与取消令牌绑定：ClusterProvider::start 接收优雅停机协调器的取消令牌 CancelToken。
   API 签名：
   pub fn start(&self, cancel_token: CancelToken)
   在 GossipManager::start 中：
   spawn(async move {
     while !cancel_token.is_cancelled() {
       this.gossip_step_async().await;
       if compio::time::timeout(this.gossip_delay(), cancel_token.cancelled()).await.is_ok() {
         break;
       }
     }
   }).detach();
   确保后台任务安全拉起，且在服务停机时随协调器优雅退出，杜绝孤儿协程。

2. [P0] 消除 RespClusterIterativeSlotVerify 等重复定义与注释泛滥
   位置：wedb/wedb/src/server/cluster_session.rs:229,1181、cluster_manager.rs:727、slot_verify.rs:380、wnode/src/cluster_session.rs:196（network_iterative_slot_verify）
   对标：garnet/libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:NetworkIterativeSlotVerify
   C# 机制：RespClusterIterativeSlotVerify.cs 是 ClusterSession 的 partial 类，在事务 MULTI/EXEC 的 Prepare 阶段，通过 NetworkIterativeSlotVerify 逐键验证所属槽位，利用 session 上的 cachedVerificationResult 缓存首个错误，后续键若状态漂移则记为 TRYAGAIN，若跨槽则记为 CROSSSLOT，最终由 WriteCachedSlotVerificationMessage 统一落线输出。
   问题：同一 C# 函数映射注释被无序复制标注在 ClusterSession 固有方法、ClusterSessionFace trait 方法、底层算法函数 iterative_slot_verify_step 以及 ClusterManager 门控函数等 5 处，引发 ./js/check.js 重复定义报错，且导致固有方法与 trait 方法双重声明，出现重复调用包装。
   方案：
   单源收敛：删除 ClusterSession 上的固有方法 network_iterative_slot_verify、reset_cached_slot_verification_result、write_cached_slot_verification_message，统一由 impl ClusterSessionFace for ClusterSession 直接实现。
   底层解耦：slot_verify.rs 中的 iterative_slot_verify_step 去除 C# 会话层注释，定位于纯无状态算法辅助函数。
   数据结构状态机闭环：
   pub struct IterativeSlotVerifyCache {
     pub slot: i32,
     pub state: Option<ClusterSlotVerificationState>,
     pub config_version: i64,
     pub initialized: bool,
   }
   状态转移时序：事务开启重置 initialized=false；首键校验写入 slot 与初始 state；后续键槽位不符置 state=CrossSlot；配置版本落后置 state=TryAgain；输出后保持状态直至事务提交或废弃。

3. [P0] 补全副本 AOF 实时存储重放驱动并纠正复制位点推进语义
   位置：wedb/wedb/src/server/replication/cluster_replication_session.rs:223-246（process_primary_stream）、replication_manager.rs:214-224（set_sublog_replication_offset）
   对标：garnet/libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:Consume、ReplicaReplayTask.cs:FullPageBasedBackgroundReplayAsync、ReplicaReplaySession.cs:ProcessPrimaryStream
   C# 机制：主端推流到副本后，副本将数据写入 TsavoriteLog 后，触发 ReplicaReplayDriver 的后台重放任务，通过 AofProcessor 将 AOF 记录逐条重放解析并应用到本地存储引擎。只有在数据成功写入存储后，才推进 SetSublogReplicationOffset(appliedAddress)，确保内存数据与位点严格一致。
   问题：当前 Rust 副本收到主端 AOF 后，仅调用 wal.enqueue_raw 写入本地日志文件，就在 process_primary_stream 尾部直接调用 rm.set_sublog_replication_offset 推进了复制位点（当前代码注释明文承认暂记 enqueued 位点）。副本内存存储（WedbStore）根本没有实时应用这些写入，导致副本内存数据陈旧，读副本读到脏数据，一旦主节点故障发生 Failover 接管，该副本直接丢失全部尚未重放的数据。js/check/ignore/cluster.yml 中把 ReplicaReplayDriver 粗暴 ignore 掩盖了该缺陷。
   方案：
   补全重放驱动数据结构：
   pub struct ReplicaReplayDriver<D: Device> {
     sublog_idx: usize,
     applied_offset: AtomicI64,
     store: Arc<WedbStore<D>>,
     processor: AofProcessor,
   }
   核心 API 签名：
   impl<D: Device> ReplicaReplayDriver<D> {
     pub async fn replay_stream_record(&self, payload: &[u8], begin_addr: i64, next_addr: i64) -> io::Result<()>
     pub fn get_applied_offset(&self) -> i64
   }
   时序步骤：
   1. process_append_log 接收主端 AOF 帧落盘至本地 WalLog；
   2. 调用 ReplicaReplayDriver::replay_stream_record，通过 AofProcessor 将记录反序列化并在 StorageSession 中执行；
   3. 存储写入完成并刷新版本后，原子推进 applied_offset；
   4. 调用 rm.set_sublog_replication_offset(sublog_idx, applied_offset)，使复制位点严格遵循 applied 语义；
   5. 从 js/check/ignore/cluster.yml 中移除 ReplicaReplayDriver.cs 与 ReplicaReplayTask.cs 的不合规 ignore 登记。

4. [P0] 恢复全量检查点快照跨网络传输协议并消除假全量同步
   位置：wedb/wedb/src/server/replication/replica_sync_session.rs:71-73（initiate_replica_sync）、js/check/ignore/cluster.yml:650-685
   对标：garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/SnapshotTransmissionDriver.cs:SendSnapshotFileAsync、ISnapshotDataSource.cs、ReplicaOps/DiskbasedReplication/ReceiveCheckpointHandler.cs:ReceiveFileAsync
   C# 机制：当主备历史不一致或副本请求位点落后于主端 AOF 安全截断线时，判定为 Full Resync。主端通过 SnapshotTransmissionDriver 读取最新快照文件与元数据，通过 CLUSTER SNAPSHOT_START / SNAPSHOT_DATA / SNAPSHOT_END 命令网络推给副本；副本 ReceiveCheckpointHandler 接收落盘后恢复存储基准数据，再从快照覆盖的 AOF 地址接续增量复制。
   问题：当前代码在 FullResync 时直接放弃快照网络传输，强行要求从协商位点直推 AOF。一旦主库运行较久产生 AOF 截断（SafeTruncateAOF），历史 AOF 已被物理清理，新副本挂载时找不到起始日志直接报错，集群无法扩容新从节点。
   方案：
   定义快照传输帧协议：
   pub struct SnapshotChunkHeader {
     pub checkpoint_token: u128,
     pub file_type: u8,
     pub offset: u64,
     pub chunk_len: u32,
     pub is_last: bool,
   }
   核心 API 签名：
   pub struct SnapshotTransmissionDriver {
     checkpoint_store: Arc<RwLock<CheckpointStore>>,
   }
   impl SnapshotTransmissionDriver {
     pub async fn transmit_checkpoint_async(&self, target_client: &GarnetClient, entry: &CheckpointEntry) -> Result<()>
   }
   pub struct ReceiveCheckpointHandler<D: Device> {
     store: Arc<WedbStore<D>>,
     temp_dir: PathBuf,
   }
   impl<D: Device> ReceiveCheckpointHandler<D> {
     pub async fn handle_chunk(&self, header: SnapshotChunkHeader, data: &[u8]) -> Result<bool>
     pub async fn commit_and_recover_store(&self, token: u128) -> Result<()>
   }
   执行步骤：
   1. determine_resync_strategy 判定为 FullResync；
   2. 主端拉取最新 CheckpointEntry，调用 transmit_checkpoint_async 按 64KB 切片流式发送快照数据；
   3. 副本端 ReceiveCheckpointHandler 写入临时目录，分片传输完毕后校验 CRC32 并挂载 wcpr 快照；
   4. 存储基准恢复完成，建立以 store_checkpoint_covered_aof_address 为起点的 AOF 增量复制流；
   5. 剔除 js/check/ignore/cluster.yml 中 SnapshotTransmissionDriver 与 ReceiveCheckpointHandler 的忽略项。

5. [P0] 补全槽迁移复合对象序列化与大键分块重组（ChunkedRecordReassembler）
   位置：wedb/wedb/src/server/migration/migrate_driver.rs:11-17,578-610（parse_migration_payload）、cluster_session.rs:2526-2615（cluster_migrate_slow）
   对标：garnet/libs/cluster/Session/ChunkedRecordReassembler.cs:TryReassembleChunk、RespClusterMigrateCommands.cs:CompleteChunkedRecordReassembly、MigrateSessionCommonUtils.cs:WriteOrSendChunkedRecordAsync
   C# 机制：迁移支持 Hash/Set/List/ZSet 等内存复合对象，以及超出网络单包大小的大值。发送端通过分块切片协议流式传输，目标端由 ChunkedRecordReassembler 维护会话级分块缓冲，全部分块到达后重组还原，并按对象类型反序列化恢复进存储。
   问题：Rust 当前迁移链路仅支持 string（kind=1），遇到任何带有 KeyTag::ObjectEnvelope 的复杂类型直接在 KEYS 迁移时报错拒绝，在 SLOTS 迁移时跳过；且载荷单批硬编码限制 512KB，无大值切片与重组逻辑，无法支撑生产环境复杂数据类型的平滑迁移。
   方案：
   协议升级与分块头设计：
   pub enum MigrationRecordKind {
     String = 1,
     ObjectEnvelope = 2,
     ChunkPiece = 3,
   }
   pub struct ChunkPieceHeader {
     pub chunk_id: u64,
     pub chunk_index: u32,
     pub total_chunks: u32,
     pub total_len: usize,
   }
   重组器结构设计：
   pub struct ChunkedRecordReassembler {
     inflight: papaya::HashMap<u64, InflightChunk>,
   }
   struct InflightChunk {
     received_bytes: usize,
     total_len: usize,
     buffer: Vec<u8>,
     deadline: Instant,
   }
   impl ChunkedRecordReassembler {
     pub fn append_chunk(&self, header: ChunkPieceHeader, data: &[u8]) -> Option<Vec<u8>>
     pub fn purge_stale(&self)
   }
   目标端导入处理：收到完整数据后，若为 ObjectEnvelope，调用 wcol 反序列化并存入 StorageSession，同步设置过期时间戳，保证集合类型完整迁移。

6. [P1] 修复 Gossip 广播发送串行阻塞问题并引入在途任务状态机
   位置：wedb/wedb/src/server/gossip/gossip_manager.rs:268-275（broadcast_gossip_async）
   对标：garnet/libs/cluster/Server/Gossip/Gossip.cs:BroadcastGossipSendAsync、GarnetServerNode.cs:TryGossip
   C# 机制：BroadcastGossipSendAsync 中非阻塞轮询各节点的 TryGossip()。TryGossip 内部检查 gossipTask：空闲则启动异步任务返回 true；运行中则直接跳过；已完成则收获结果并启动下一轮；报错则摘除连接。全广播过程不阻塞主协程。
   问题：Rust 当前 broadcast_gossip_async 采用 while 循环对所有连接串行 await。单个节点网络超时将阻塞主广播循环高达 5000ms（gossip_delay），导致全集群心跳堆积，极易引发误判节点离线或脑裂。
   方案：
   在 NodeConnection 中维护在途原子状态：
   pub struct NodeConnection {
     pub node_id: String,
     in_flight_gossip: AtomicBool,
     last_send: AtomicI64,
   }
   非阻塞并发派发逻辑：
   pub fn broadcast_gossip_step(&self) {
     let mut offset = 0;
     while let Some(conn) = self.connection_store.get_connection_at_offset(offset) {
       offset += 1;
       if conn.in_flight_gossip.compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire).is_ok() {
         let this = self.clone();
         let conn_clone = conn.clone();
         compio::runtime::spawn(async move {
           let ok = this.gossip_to_peer_async(&conn_clone).await;
           conn_clone.in_flight_gossip.store(false, Ordering::Release);
           if !ok {
             this.connection_store.try_remove(&conn_clone.node_id);
           }
         }).detach();
       }
     }
   }
   主循环调用 broadcast_gossip_step 瞬间完成，各节点探测完全异步并发隔离。

7. [P1] 修复 Failover 候选副本探测串行化缺陷并恢复并发竞速与超时机制
   位置：wedb/wedb/src/server/failover/failover_session.rs:218-226（wait_for_first_replica_sync_async）
   对标：garnet/libs/cluster/Server/Failover/PrimaryFailoverSession.cs:WaitForFirstReplicaSyncAsync、DelayToDefaultAsync
   C# 机制：主端向全部候选从节点并发下发位点检查请求，并结合 Task.Delay(failoverTimeout) 构成竞速任务数组，使用 Task.WhenAny 抢占式等待首个位点追平的健康从节点，实现最短停机窗口接管。
   问题：Rust 现存实现改为串行 for 循环逐个 await 检查副本。若首个副本因故障挂起，流程在首个节点卡顿整个超时窗口，无法及时选取其他已经追平的副本，延长故障转移时间。同时 cluster.yml 中错误忽略了 DelayToDefaultAsync。
   方案：
   重构为并发任务竞速模型：
   使用 futures_util::future::select_all 将所有副本的 check_replica_sync_async 包装为 Future 数组；
   配合总超时时间戳 deadline 进行循环 select；
   算法步骤：
   1. 并发发起全量候选副本探测；
   2. select_all 每次收获最早响应的副本结果；
   3. 校验该副本复制位点是否追平 local_offset，追平则立刻中断其余等待直接返回该副本进行接管；
   4. 若未追平且未到总超时，继续等待剩余任务；
   5. 达到 deadline 整体返回超时 None；
   6. 恢复并在代码中对标 DelayToDefaultAsync 超时语义，从 ignore 中移除。

8. [P1] 修复槽迁移目标端导入数据未广播本地 AOF 的一致性缺陷
   位置：wedb/wedb/src/server/cluster_session.rs:2560-2610（cluster_migrate_slow）
   对标：garnet/libs/cluster/Session/RespClusterMigrateCommands.cs:NetworkClusterMigrate
   C# 机制：目标节点接收迁移键值后，通过常规 ClientSession 写入存储，写入动作自动被注入的 AOF 捕获并落盘，同步推流给目标节点的副本，保证分片内主从强一致。
   问题：Rust 在 cluster_migrate_slow 中使用了 StorageSession::new_readonly(batch)，即使调用了 upsert_string，由于是只读包装，缺少 StoreEventSink 事件通知与 AOF 提交通道注入，新迁入的键不会写入本地 WalLog，也不会通知 AofReplicationPump。目标节点的从库不会同步迁入的数据。
   方案：
   写入链路接入 AOF 事件源：
   1. cluster_migrate_slow 改用与写命令相同的 StorageSession 写入实例；
   2. 写入完成后调用 batch.commit_with_aof()；
   3. 触发 StoreEventSink 将写事件序列化写入本地 WalLog；
   4. 调用 provider.primary_replication().pump.notify()，触发推流泵将新迁入的键增量推送给当前节点的所有副本。

9. [P1] 拆分 2615 行巨型单体文件 cluster_session.rs 为高内聚子模块目录
   位置：wedb/wedb/src/server/cluster_session.rs:1-2615
   对标：garnet/libs/cluster/Session/ 下的分类文件结构
   C# 机制：Garnet 在 Session 目录下按功能拆分为 RespClusterBasicCommands.cs、RespClusterSlotManagementCommands.cs、RespClusterMigrateCommands.cs、RespClusterReplicationCommands.cs、RespClusterSlotVerify.cs 等多个部分。
   问题：cluster_session.rs 达到 2615 行，将协议分发、槽位校验、迁移管理、复制子命令、慢路径实现堆砌在一起，单文件体积严重超标，违反代码规范中关于模块内聚性与文件体积的约束。
   方案：
   重构为 cluster_session/ 目录：
   - mod.rs：ClusterSession 结构体、生命周期、公共方法与 ClusterSessionFace 门面实现
   - slot_verify.rs：多键槽位校验、迭代式槽位校验与等待体处理
   - basic_cmds.rs：CLUSTER INFO, NODES, MYID, SLOTS, SHARDS, MEET, FORGET
   - slot_cmds.rs：ADDSLOTS, DELSLOTS, SETSLOT, KEYSLOT, COUNTKEYSINSLOT, GETKEYSINSLOT
   - migrate_cmds.rs：MIGRATE 与 CLUSTER MIGRATE 命令解析与 slow path 执行
   - replication_cmds.rs：REPLICATE, REPLICAS, FAILOVER, APPENDLOG 解析与执行

10. [P1] 拆分 1139 行巨型文件 replication_manager.rs 为独立功能模块
    位置：wedb/wedb/src/server/replication/replication_manager.rs:1-1139
    对标：garnet/libs/cluster/Server/Replication/（ReplicationManager.cs, ReplicationHistoryManager.cs, ReplicationCheckpointManagement.cs）
    C# 机制：C# 将复制管理按历史管理、检查点管理和核心状态机拆分在不同的 partial 类文件中。
    问题：复制历史文件读写、内存检查点管理、安全截断计算、位点等待队列与恢复状态转移集中在 1139 行单文件中，内聚性差。
    方案：
    拆分为 replication/manager/ 目录：
    - mod.rs：ReplicationManager 主定义与对外 API
    - history.rs：ReplicationHistory 维护、ID 轮转与 replication.conf 磁盘落盘
    - checkpoint_mgmt.rs：检查点条目记录、内存索引重置与 safe_truncate 位点计算
    - waiter.rs：OffsetWaiter 等待结构体与精准唤醒通道管理
    - recovery.rs：RecoveryStatus 状态机迁移与锁降级逻辑

11. [P1] 拆分 1184 行巨型文件 cluster_provider.rs
    位置：wedb/wedb/src/server/cluster_provider.rs:1-1184
    对标：garnet/libs/cluster/Server/ClusterProvider.cs
    问题：主从资产装配、角色查询、监控指标统计、纪元推进静止等待全部塞在单一文件中。
    方案：
    拆分为 cluster_provider/ 目录：
    - mod.rs：ClusterProvider 核心结构与 IClusterProvider trait 实现
    - assets.rs：PrimaryReplicationAssets、WalLog、StoreSession 资产注入
    - stats.rs：ReplicationInfo、GossipStats、CheckpointInfo 指标采集
    - epoch.rs：BumpAndWaitForEpochTransition 纪元演进与会话静止协调

12. [P1] 拆分 758 行长文件 migrate_driver.rs
    位置：wedb/wedb/src/server/migration/migrate_driver.rs:1-758
    对标：garnet/libs/cluster/Server/Migration/MigrationDriver.cs、MigrateSessionCommonUtils.cs
    问题：协议帧编解码、显式键迁移驱动、槽位扫描驱动、失败回滚混在一起。
    方案：
    拆分为 migration/driver/ 目录：
    - frame.rs：MigrationRecord 视图与编解码函数
    - keys_driver.rs：run_keys_migration_driver 指定键流式迁移
    - slots_driver.rs：run_slots_migration_task 槽位全量遍历迁移
    - recovery.rs：try_recover_from_failure 槽位状态回滚与远端 STABLE 复位

13. [P1] 清理 js/check/ignore/cluster.yml 中不合理的大面积忽略项
    位置：js/check/ignore/cluster.yml:500-690
    对标：garnet/libs/cluster/Server/Replication/、Migration/
    问题：将核心的 AofSyncDriver、ReplicaReplayDriver、SnapshotTransmissionDriver、ReceiveCheckpointHandler 等关键链路批量 ignore，掩盖了真实实现缺失。
    方案：
    逐项核对 ignore 清单，移除不属于业务无关代码的忽略项；
    将真实缺失的基础设施转入开发待办，确保 check.js 真实反映对标状态。

14. [P2] 优化槽位多键校验挂起重评时的内存分配
    位置：wedb/wedb/src/server/cluster_session.rs:1237-1256（network_multi_key_slot_verify）
    对标：garnet/libs/cluster/Session/SlotVerification/RespClusterSlotVerify.cs:NetworkMultiKeySlotVerify
    问题：槽位未稳定时，每次挂起都会执行 keys.iter().map(|k| k.to_vec()).collect() 堆分配生成 SlotVerifyRequest，高频热路径上产生大量瞬时垃圾。
    方案：
    使用 SmallVec<[Vec<u8>; 4]> 或持有引用切片的轻量请求结构体，绝大多数单键或少键命令挂起时零堆内存分配。

15. [P2] 消除 Gossip 节点连接中的裸超时与套接字句柄泄漏风险
    位置：wedb/wedb/src/server/gossip/gossip_manager.rs:142-167（try_meet_async）
    对标：garnet/libs/cluster/Server/Gossip/GarnetServerNode.cs:TryMeetAsync、Gossip.cs
    问题：MEET 遇到超时或反序列化失败时，部分分支仅从 connection_store 移除键，底层连接未显式 dispose，套接字残留后台直到 GC。
    方案：
    在错误处理分支中强制调用 conn.dispose() 关闭底层 TCP 流，释放操作系统套接字句柄。

16. [P2] 完善 ASKING 与 MOVED 重定向在事务执行阶段的状态一致性闭环
    位置：wedb/wedb/src/server/slot_verify.rs:370-420（iterative_slot_verify_step）
    对标：garnet/libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs:NetworkIterativeSlotVerify
    问题：会话级 session_asking 状态在多键事务与流水线执行时需要精确在命令执行完毕后扣减，当前重置时机若未与 EXEC/DISCARD 联动，会导致后续非事务命令被错误放行。
    方案：
    在 EXEC、DISCARD 执行结束时强制触发 reset_cached_slot_verification_result，确保会话 ASKING 标记不跨事务泄露。

17. [P2] 规范槽迁移过程中的条带写锁与 Sketch 状态探测联动
    位置：wedb/wedb/src/server/migration/migrate_session.rs:131-157（can_access_key）
    对标：garnet/libs/cluster/Server/Migration/MigrateSessionKeyAccess.cs:CanAccessKey
    问题：Sketch 探测与存储引擎写锁未建立原子屏障，Transmitting 到 Deleting 状态切换时可能与并发写入产生可见性交错。
    方案：
    在修改 Sketch 状态为 Deleting 时，先获取对应槽位的条带写锁，排空在途写者后再推进状态与物理删键。

18. [P2] 复制与迁移网络缓冲池的动态配额与系统高负载内存保护
    位置：wedb/wedb/src/server/replication/replication_manager.rs:125、migration_manager.rs:29
    对标：garnet/libs/cluster/Server/Replication/ReplicationNetworkBufferSettings.cs
    问题：缓冲池采用固定 DEFAULT_MAX_POOL_SIZE，在海量连接或超大流量下缺少自适应伸缩与借出泄露告警。
    方案：
    接入 GarnetServerOptions 配置体系，支持配置动态调整缓冲池上限，并增加 buffer_pool_exhausted 监控指标。

19. [P2] 优化 Gossip 配置合并（TryMerge）时的锁争用与单调版本校验
    位置：wedb/wedb/src/server/cluster_manager.rs:270-360（try_merge）
    对标：garnet/libs/cluster/Server/ClusterManager.cs:TryMerge
    问题：Gossip 收包每次都直接请求 active_merge_lock 写锁进行合并判断，高频心跳导致全局锁争用严重。
    方案：
    引入无锁版本快筛：先在只读态下对比 other.config_epoch <= current.config_epoch，仅在远端版本严格单调超前时才获取写锁执行合并。

20. [P2] 完善节点优雅停机时集群会话与复制推流的排空时序
    位置：wedb/wnode/src/shutdown.rs、server.rs:278-284
    对标：garnet/libs/host/GarnetServer.cs:Dispose、libs/cluster/Server/ClusterProvider.cs:Dispose
    问题：停机时先断开 listener，若直接关闭，在途复制流尚未推给从节点的日志可能被直接切断。
    方案：
    停机流程建立严格的三阶段排空：
    Phase 1: 停止监听端口，拒绝外部新客户端连接；
    Phase 2: 触发 AofReplicationPump::flush，等待复制位点推进完毕（限时 3 秒）；
    Phase 3: 执行 cluster_provider.flush_config() 持久化集群状态，最后注销会话与套接字。
