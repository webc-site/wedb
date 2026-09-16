# M4 检查点网络导入面：三 arm 接收 + 全量同步闭环 + CheckpointFileType 对齐（next/gemini.md 条 2 M4 补臂）


## C# 三段握手状态机（disk-based 全量同步，甄别结论的事实基础）

副本                                     主端

1. CLUSTER REPLICATE nodeId
   TryAddReplicaAsync（角色翻转 + BeginRecovery(ClusterReplicate) 恢复锁）
   本地重置：ResetReplicaReplayDriverStore / aofSyncDriverStore.Reset /
   replicationOffset=0 / storeWrapper.Reset / SuspendPrimaryOnlyTasks
   recvCheckpointHandler = new ReceiveCheckpointHandler
2. INITIATE_REPLICA_SYNC nodeId assignedPrimaryId cEntryBytes begin tail
   → 主端 TryBeginDiskbasedSyncAsync 起 ReplicaSyncSession 后台任务，
   副本等检索完成信号，+OK 应答
3. 主端 SendCheckpointAsync（ReplicaSyncSession.cs，后台）：
   AcquireCheckpointEntryAsync：取最新检查点 + TryAddReader 读者锁 +
   注册 AofSyncDriver 占位防 AOF 截断 + OnDemandCheckpoint 兜底重拍
   ValidateMetadata：skipLocalMainStoreCheckpoint 判定（本地无检查点或
   同历史同版本 → 跳过快照下发），失败重试 10 次
   快照下发（SnapshotTransmissionDriver + TsavoriteSnapshotReader）：
   文件源序列 = STORE_HLOG 段流（空 data 收尾）→ STORE_INDEX 段流 →
   STORE_SNAPSHOT 段流（有快照文件才发）→ 元数据源
   （TsavoriteMetadataTransmitSource：STORE_INDEX 元数据 + STORE_SNAPSHOT
   元数据，单消息）；
   传输命令 = ExecuteClusterSnapshotData（SNAPSHOT_DATA token type
   startAddress data，4 参；startAddress=-1 单消息载荷约定）
4. BEGIN_REPLICA_RECOVER recoverStoreFromToken replayAOFMap primaryReplicaId
   cEntryBytes beginAddress checkpointAofBeginAddress（6 参）：
   副本 TryReplicaDiskbasedRecovery：
   RecoverCheckpointAsync(replicaRecover, recoverStoreFromToken, metadata)
   —— 从 token 文件集恢复引擎；replayAOFMap>0 → ReplayAOF；
   Log.Initialize(beginAddress, offset)；replicationOffset = offset；
   PurgeAllCheckpointsExceptEntry；InitializeCheckpointStore；
   TryUpdateMyPrimaryReplId；EndRecovery(CheckpointRecoveredAtReplica)；
   回 replicationOffset（bulk string）
5. 主端 syncFromAofAddress = 应答 → DataLossCheck →
   TryAddReplicationDriver + TryConnectToReplica → APPENDLOG 增量流


## 甄别结论

### 1. 三 arm（SNAPSHOT_DATA / SEND_CKPT_METADATA / SEND_CKPT_FILE_SEGMENT）缺失：成立

上一轮已从 CLUSTER_SUBTABLE 摘除幽灵注册；枚举变体与 strum 映射保留。
C# 接收链 = replicationManager.recvCheckpointHandler（ReceiveCheckpointHandler）
+ FileDataSink（文件段设备写）+ MetadataDataSink（元数据提交）。
SEND_CKPT_METADATA（3 参）/ SEND_CKPT_FILE_SEGMENT（5 参，segmentId 兼容
位不消费）为旧形态命令、SNAPSHOT_DATA 为统一新形态；C# 发送侧只用
SNAPSHOT_DATA，三个 arm 的接收处理同源（ProcessSnapshotData /
ProcessMetadata / ProcessFileSegment）。全部按 C# 落地，无占位。

### 2. CheckpointFileType 枚举值错位：成立

rust 现值 StoreHlog=0/StoreIndex=1，C# 为 NONE=0/STORE_HLOG=1/
STORE_HLOG_OBJ=2/（3 保留）/STORE_INDEX=4/STORE_SNAPSHOT=5/
STORE_SNAPSHOT_OBJ=6/STORE_RANGEINDEX_FLUSH=7/STORE_RANGEINDEX_SNAPSHOT=8。
对齐全 C# 值域；RI 两变体接收臂拒绝（红线不实现 RI 检查点传输，语义等价
C# EnableRangeIndexPreview=false 抛异常分支）；OBJ 两变体 rust 单存储统一
检查点模型无对象存文件，接收臂拒绝（协议违约）。

### 3. BEGIN_REPLICA_RECOVER：按 C# 语义落地（二选一裁决）

导入闭环第三腿，副本非空库一致性的唯一真实入口，必须落地。rust 映射：

- RecoverCheckpointAsync → wcpr 从副本检查点目录按 token 恢复
  （CheckpointManager::recover = recover_checkpoint_components +
  from_recovered 真组件重构），产出的全新 WedbStore 经置换槽接管
  provider.store + cluster.set_store（在线引擎替换）；
- replayAOFMap>0 → 拒绝（协议违约）：rust 主端恒发 0——C# 的
  ComputeAofSyncReplayAddress/ReplayAOF 属「副本引擎回放本地 AOF 衔接
  旧检查点」形态，rust 副本运行期无存储应用链（既有登记的架构边界），
  全量同步语义由「检查点导入 + 授予位点起 AOF 直推」承接；
- Log.Initialize(begin, offset) → wal.safe_initialize(covered, covered)；
- replicationOffset = offset → rm.set_current_replication_offset；
- PurgeAllCheckpointsExceptEntry + InitializeCheckpointStore +
  TryUpdateMyPrimaryReplId → rm 对应面（checkpoint_store 登记 + 检查点
  目录陈旧 token 清理 + try_update_my_primary_repl_id）；
- EndRecovery(CheckpointRecoveredAtReplica) → rm.end_recovery 同态。

接收文件布局（staging = 副本检查点目录，对齐 wcpr 命名，重启恢复收敛）：

- STORE_HLOG 段流 → 副本在线引擎设备文件（C# CreateCheckpointDevice
  STORE_HLOG → GetStoreHLogDevice 同源语义：设备即引擎真身，导入后换引擎、
  重启 recover_latest(checkpoint_dir, 主数据文件) 天然一致）；
  写入走 Device::write_aligned（O_DIRECT/缓冲同路，杜绝双句柄页缓存
  不一致）；末块零填充扇区对齐（validate_aligned_io 契约）
- STORE_INDEX 段流 → checkpoint_dir/index_<token>.ckpt（wcpr
  index_filename，恢复零改名）
- STORE_SNAPSHOT 元数据（startAddress=-1 单消息）→
  checkpoint_dir/checkpoint_<token>.meta（wcpr meta_filename；元数据
  最后落盘 = 提交标记，对齐 wcpr「meta 即发布」协议）
- STORE_INDEX 元数据：登记差异——C# CommitIndexCheckpoint 提交独立
  index 元数据文件；rust wcpr 索引快照自带头+CRC 自描述，无该文件概念，
  主端不发送，副本收到空载荷 no-op、非空载荷拒绝

### 4. ATTACH_SYNC：登记差异（二选一裁决）

C# 唯一调用形态是 DisklessReplication（MainMemoryReplication）：
originNodeRole==REPLICA → TryBeginDisklessSyncAsync（副本接 attach），
否则 TryReplicaDisklessRecovery（主端接 attach）。rust 采用 disk-based
AOF 直推架构（对齐 C# 默认 ReplicaDisklessSync=false 形态），diskless
复制不转写（ReplicaDisklessSync.cs / DisklessReplication/ 全文件 ignore
维持）。不注册、保持 unknown subcommand；ignore 登记保留。

### 5. SYNC：登记差异

CLUSTER SYNC 是迁移/无盘记录流帧通道（MigrationRecordSpanType），红线
kind 2-5 不实现；键迁移链路 MIGRATE payload 已闭环（上一轮验收基线）。
ignore 登记保留，不注册。

### 6. PartialResync 不走 BEGIN_REPLICA_RECOVER：登记差异

C# 每次 attach 均走 BeginReplicaRecover（skipLocal 时 recoverFromToken=
false 从本地检查点恢复）。rust：仅 FullResync 链导入检查点；
PartialResync 维持现有 attach 直推（副本引擎不随 attach 重构，仅导入时
换引擎——架构边界与条 3 replayAOF 差异同源）。

### 7. 主端发送面数据源（wcpr 统一检查点模型映射）

- 主端检查点目录注入 ClusterProvider（装配期，对标 C# storeWrapper 反查
  CheckpointDir 的依赖方向反转）
- STORE_HLOG 段源：store.device 直读 [page_floor(begin), flushed_until)
  （快照只读封印前缀，并发读安全；C# hybridLogFileStartAddress 同口径）
- STORE_INDEX 段源：checkpoint_dir/index_<token>.ckpt 文件字节
- STORE_SNAPSHOT 元数据源：checkpoint_dir/checkpoint_<token>.meta 字节
  （C# GetLogCheckpointMetadata 对标位；wcpr meta 一体承载 hlog/index/
  store 三元数据，STORE_INDEX 元数据不发）
- STORE_SNAPSHOT 文件段：rust FoldOver 模型无独立快照文件，跳过（C#
  snapshotFileEndAddress <= PageHeader.Size 同款跳过分支）
- 段大小：128KiB（对标 FileDataSource.DefaultBatchSize = 1 << 17），段起
  始扇区对齐、末块自然收尾；空载荷收尾帧（EOF 哨兵）


## 对标与改动点

1. wedb/wedb/src/server/replication/checkpoint_entry.rs
   CheckpointFileType 值域对齐 C#（含 RI/OBJ 变体），contains_shared_token
   随新变体收敛。
2. wedb/wedb/src/server/replication/receive_checkpoint_handler.rs（新建）
   ReceiveCheckpointHandler（activeSink 单文件状态机，ProcessSnapshotData /
   ProcessMetadata / ProcessFileSegment，对标 ReceiveCheckpointHandler.cs）
   + FileDataSink（设备段写 + Complete 刷盘，对标 FileDataSink.cs）+
   MetadataDataSink（元数据文件落盘，对标 MetadataDataSink.cs）。
3. wedb/wedb/src/server/replication/replica_diskbased_sync.rs（新建）
   try_replica_diskbased_recovery（对标 ReplicaOps/ReplicaDiskbasedSync.cs:
   TryReplicaDiskbasedRecovery）：恢复门控 → wcpr 恢复 → 引擎置换 →
   wal 对齐 → rm 位点/检查点历史/replid 收敛 → EndRecovery；返回授予位点。
4. wedb/wedb/src/server/replication/snapshot_transmission.rs（新建）
   快照发送驱动（对标 SnapshotTransmissionDriver.cs + TsavoriteSnapshotReader.cs
   + FileDataSource.cs 读块面）：文件源序列编排 + 段切分 + SNAPSHOT_DATA
   发送 + EOF 哨兵。
5. wedb/wedb/src/server/replication/replica_sync_session.rs
   initiate_replica_sync 补 FullResync 检查点下发链：发送面 →
   BEGIN_REPLICA_RECOVER 往返 → DataLossCheck → attach 推流（对标
   SendCheckpointAsync 全链）；determine_resync_strategy 的
   SendCheckpointAsync 文档映射迁移至真实发送函数（check.js 主登记唯一）。
6. wedb/wedb/src/server/cluster_session.rs
   新增四 arm：ClusterSnapshotData / ClusterSendCkptMetadata /
   ClusterSendCkptFileSegment（同步 block_on 落盘）/ ClusterBeginReplicaRecover
   （慢路径承载，对标 BlockingWait）；SEND_CKPT_FILE_SEGMENT segmentId
   兼容位不消费（C# 同注释）。
7. wedb/wedb/src/server/replication/replication_manager.rs
   挂 ReceiveCheckpointHandler（对标 recvCheckpointHandler 字段）+ 复位面；
   SendCheckpointAsync 文档映射迁出 determine_resync_strategy。
8. wedb/wedb/src/server/cluster_provider.rs
   检查点目录注入（try/set_checkpoint_dir）+ 副本引擎置换钩子
   （Arc<dyn Fn> 注入面，对齐 wkv StoreEventSink 先例）。
9. wnode/src/resp/parser/resp_command.rs
   CLUSTER_SUBTABLE 重建四项注册（SNAPSHOT_DATA / SEND_CKPT_METADATA /
   SEND_CKPT_FILE_SEGMENT / BEGIN_REPLICA_RECOVER）；ATTACH_SYNC / SYNC
   维持不注册（登记差异）。
10. wnode/src/service.rs（最小越界）
    在线引擎置换槽：store() 归一读面 + swap_store()，get_session 走置换
    槽（新连接即新引擎，存量会话随批纪元自然收敛——C# epoch 驱逐等价）。
11. wedb/src/main.rs（最小越界）
    装配接线：checkpoint_dir 注入 + 置换钩子注册（Arc<provider> 弱引用
    回调，杜绝循环持有）。
12. wedb/wedb/src/client.rs
    补 BEGIN_REPLICA_RECOVER / SNAPSHOT_DATA 客户端方法（对标
    GarnetClientSessionReplicationExtensions.cs / GarnetClientSession.cs
    ExecuteClusterBeginReplicaRecover / ExecuteClusterSnapshotData）。
13. 测试 wedb/wedb/tests/checkpoint_import.rs（新建）
    副本非空库一致性真实链路：主端建库 + wcpr 快照 + 检查点登记 →
    三 arm 帧接收落盘 → BEGIN_REPLICA_RECOVER → 置换后引擎读到主端键 +
    wal/位点/replid/恢复态断言；SEND_CKPT_* 旧形态臂；段切分与 EOF；
    文件段中断恢复重开（C# 注释的重试重开语义）。


## 范围外仅记录

- wnode 宿主置换槽之外的生命周期（NodeService AOF 监听端口的 store 引用、
  向量域版本原子）随存量会话边界收敛，不随导入热替换——rust 副本运行期
  无存储应用链（既有登记），APPENDLOG 帧直达 wal 不经该面。
- 检查点目录并发：副本 attach 期间不拍本地检查点（C# SuspendPrimaryOnlyTasks
  同语义，副本角色本就不触发周期快照）。
- Command 目录（wresources RespCommandsInfo.json）六命令描述已在场，随
  注册重建自动生效。


## 验收口径

1. ./clippy.sh 零警告（禁 allow）。
2. ./test.sh 全过。
3. bun ./js/check.js 无新增缺失/重复（基线：现存三组
   RespClusterIterativeSlotVerify 重复登记）。
4. CLUSTER_SUBTABLE 四项重建后无「注册而无臂」；导入闭环测试断言到
   「置换后引擎可读主端键」为止，禁半途落盘即成功。
