r8 随机抽样精读 C:集群/复制/对象域 30 函数

方法
在 wedb/wedb/src/server/(replication、cluster_config、cluster_manager、gossip、slot_verify、migration、failover、cluster_session)与 wcol、wext_roaring、wext_json、whyperlog、wnode bitmap 命令面均匀抽 30 个函数,逐一对 garnet C# 对位函数精读。判定只报行为语义可差的点;注释中已声明的刻意差异标注(声明),未声明的差异标注(增量)。r5-repl attach 双泵、r7-redteam 经纪 ns0 已立,不复述。

差异(13)

1. wedb/wedb/src/server/replication/replication_manager.rs:negotiate_resync
   libs/server/AOF/GarnetAppendOnlyFile.cs:ComputeAofSyncReplayAddress(逐子日志内核)+ libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/ReplicaSyncSession.cs:SendCheckpointAsync/ValidateMetadata
   判定:差异(增量为主)
   a) 副本 begin 越过检查点覆盖线分支(rep_begin > ckpt_begin):C# 仅记 info 日志、该子日志不进 replayAOFMap,同步继续按 partial 发 BeginReplicaRecover;rust 置 is_partial_possible=false,经 disk_resync_strategy 折为 FullResync。此转换未在注释声明(注释只声明了下一条的异常收敛)。
   b) 副本 tail 低于覆盖线且非 FastAofTruncate:C# 抛异常中止本次 attach,副本收到错误可重试;rust 折为 FullResync 继续发快照,attach 成功。副本可见结果分叉(声明"无异常通道收敛",但行为面仍是差异)。
   c) 阈值:C# 判 > kFirstValidAofAddress(64),rust 判 > 0。64 源自 Tsavorite 设备头区,rust WalLog 无头区起点 0,属架构适配已声明;但与 a) 组合后 rep_begin 落在 1..=64 的输入在 rust 进 FullResync 臂,C# 不进。
   d) same_history2:C# ReplicaSyncSession.cs:162 写作 string.IsNullOrEmpty(PrimaryReplId2) && PrimaryReplId2.Equals(replicaAssignedPrimaryId)(上游笔误,仅双方皆空才可能为真);rust 改 !is_empty() && 相等。双方皆空边缘下 C# 会用 ReplicationOffset2 钳制回放上限,rust 不钳——修 bug 引入的行为分叉。

2. wedb/wedb/src/server/replication/receive_checkpoint_handler.rs:process_snapshot_data
   libs/cluster/Server/Replication/ReplicaOps/DiskbasedReplication/ReceiveCheckpointHandler.cs:ProcessSnapshotData
   判定:差异(增量)
   a) 单消息(startAddress=-1)STORE_RANGEINDEX_FLUSH:C# 支持,经 RangeIndexFileDataSink.FromMetadata 建 flush 槽(metadata=keyHash 16B ASCII + address i64 LE);rust 直接报错拒绝。
   b) RI 快照头帧载荷编码:C# keyHash 为 16 字节 ASCII,rust key_id 为 u128 LE——双侧自洽但非 1:1,互不解读。
   c) 懒开槽多段类型集:C# 接受 STORE_HLOG/STORE_HLOG_OBJ/STORE_SNAPSHOT/STORE_SNAPSHOT_OBJ/STORE_INDEX 五种流式文件;rust 仅 STORE_HLOG/STORE_INDEX,其余段帧报错。统一检查点模型下当前发送端不触达,自洽;协议面收窄未登记于 ignore 注记。

3. wedb/wedb/src/server/gossip/node_connection.rs:get_most_recent_config
   libs/cluster/Server/Gossip/GarnetServerNode.cs:GetMostRecentConfig
   判定:差异(增量)
   C# 在发送时刻即置 lastConfig=conf(引用比较键),本轮发送/应答失败后下一轮回空 ping,对端滞留旧配置视图直至本地配置再次变化;rust last_sent_config_version 仅在 gossip 应答成功分支推进(gossip_manager.rs:try_gossip Ok 臂),失败后下一轮重发全量。失败恢复语义 rust 更强,但 gossip 字节面与对端收敛时序与 C# 不同(重连风暴场景 rust 放大)。

4. wedb/wedb/src/server/replication/aof_sync_task.rs:send_advance_time_pulse
   libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:SendAdvanceTimePulse
   判定:差异(增量)
   C# timePulseEnabled = serverOptions.MultiLogEnabled(AofPhysicalSublogCount>1 或 AofReplayTaskCount>1):单子日志单任务配置下脉冲链整体静默,线路上不存在 CLUSTER ADVANCE_TIME 帧。rust 脉冲源生产装配恒注入(replica_sync_session.rs/replication_sync_manager.rs 组装 TimePulseSource),单物理日志拓扑下恒发脉冲。线路帧与副本读一致时间推进路径均为 C# 单日志配置所无。tail 扫描域 C# 全子日志 OR 快照,rust 单值,单物理日志下等价。

5. wedb/wedb/src/server/replication/aof_sync_task.rs:throttle
   同文件 C#:Throttle
   判定:差异(增量)
   C# shipped = max(previousAddress, iter.NextAddress):折叠迭代器扫描位,覆盖 null-device 跳页等"推进未消费"情形;rust shipped 只取 previous_address(实发水位),溢流 Queued 帧落网前不计(consume 的 Shipped/Queued 分臂 + ratchet_shipped 补账)。同负载下 rust 背压闸放行晚于 C#,副本 attach 峰值吞吐形状不同。另 C# 有 backpressure.Enabled 门,rust 无门恒判定(幂等,影响小)。

6. wedb/wedb/src/server/replication/replication_manager.rs:diskless_resync_strategy
   libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicaSyncSession.cs:NeedToFullSync
   判定:差异(声明)
   四判据缺一:ReplicaDisklessSyncFullSyncAofThreshold(待回放量超门限升全量)在 rust 无任何实现面。C# 大缺口副本会被升全量保护,rust Partial 回放无上界。与 r6-cli"复制域旋钮零通路"相邻但不同层:此为同步判据本身缺失,非配置通路问题。

7. wedb/wedb/src/server/cluster_config/mod.rs:merge_slot_map
   libs/cluster/Server/ClusterConfig.cs:MergeSlotMap
   判定:差异(声明)
   副本臂 handoff 主未登记本地时:C# assignToWorkerId=0 仍直写槽(worker 0 + STABLE,updated=true),槽挂到保留位;rust 判 handoff_worker_id==0 整轮放弃,保留现状态。rust 防御性修正(注释声明),但两侧 gossip 收敛后的槽表在该边缘可分。另 C# workers[currentOwnerId] 裸索引越界即崩,rust .get 缺失按 epoch=0 视之(防御,不可达态)。

8. wedb/wedb/src/server/migration/frame_import.rs:import_migration_frames
   libs/cluster/Session/RespClusterMigrateCommands.cs:Process
   判定:差异(增量)
   a) 槽门禁点:C# 逐记录 HashSlot→IsImportingSlot,坏记录起置 migrateState=1 跳过余帧,且收尾无条件回 +OK——源端因此认为全部落地并删除 FOUND 键,目标端实际缺键;rust MIGRATE 链头声明槽位一次门禁(库级定槽声明),失败整载荷拒绝并回错误文案。rust 收紧正确,但 C# "半失败仍 OK"的怪癖未保留也未登记为刻意差异。
   b) C# ProcessFileSegment(旧命令)无 token 校验;rust write_active_sink 增 token 漂移拒绝(自洽加固)。

9. wedb/wedb/src/server/slot_verify.rs:iterative_slot_verify_step
   libs/cluster/Session/ClusterSession.cs:MultiKeySlotVerify/VerifyKeysInRange
   判定:差异(声明)
   槽漂移 C# 回 CROSSSLOT,rust 折 TRYAGAIN(键级哈希废除,doc/zh/db.md 4.4 声明)。缓存面:C# verifyResult 保持首键裁决、TRYAGAIN 仅作返回值;rust 将 cache.state 置 TryAgain——最终错误帧等价,仅缓存状态可见面不同。

10. wedb/wedb/src/server/gossip/gossip_manager.rs:sample_gossip_send
    libs/cluster/Server/Gossip/Gossip.cs:GossipSampleSendAsync
    判定:差异(声明)
    C# 成功支 continue 跳过循环尾 count--,单轮全成功净扇出可超 fraction(靠 startTime 推进防重复入选);rust 固定 count 封顶、成功也耗配额。扇出量与 gossip 统计口径不同,注释已声明刻意差异。

11. wedb/wedb/src/server/cluster_session/basic.rs:cluster_gossip_slow
    libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterGossip
    判定:差异(声明)
    应答体:C# 合并前捕获 current 引用,回 pre-merge 配置;rust 合并后读版本号,应答可能已含本帧刚合并的配置。注释声明刻意差异(收敛更快),对端 merge 幂等,方向无害。

12. wedb/wedb/src/server/slot_verify.rs:single_key_read_write_slot_verify
    libs/cluster/Session/ClusterSlotVerification/ClusterSlotVerify.cs:SingleKeyReadWriteSlotVerify
    判定:差异(声明)
    去 internalWriteSession 豁免臂(注释声明:rust 回放不经 RESP 分派);C# waitForStableSlot 自旋等待外提为宿主挂起重评;检查次序 C# 先稳定等待后副本重定向,rust 副本重定向先(等待已外提,可观测等价)。单键裁决矩阵(Stable/Migrating/Importing/ClusterDown/Moved/Ask)逐项一致。

13. wedb/wedb/src/server/failover/replica_failover_session.rs:reset_if_needed
    libs/cluster/Server/Failover/ReplicaFailoverSession.cs:BeginAsyncReplicaFailoverAsync(finally)
    判定:差异(声明)
    abort 打断后主端复位:C# 以已取消 cts 发 ExecuteClusterFailStopWritesAsync(请求可能根本未发出,自弃,注释判其为 C# 缺陷);rust 仍限时发送复位帧并检查 -ERR。abort 场景 rust 更可能把旧主从停写中复位,槽位无主窗口更短——方向更安全,行为不同。

一致(17)

14. wedb/wedb/src/server/replication/aof_sync_task.rs:consume
    同文件 C#:Consume
    判定:一致
    发送帧参数五元组(node_id/子日志/前址/现址/次址)、失败断连且位点不推进、成功刷新脉冲节流窗口均同。Shipped/Queued 双臂与 accepted_address 为溢流泵结构性承接,落网补账后与 C# previousAddress 语义合流;新增 prev 单调防御(不可达态)。

15. wedb/wedb/src/server/migration/migrate_driver/keys.rs:transmit_keys
    libs/cluster/Server/Migration/MigrateOperation.cs:TransmitKeysAsync(+MigrateSessionKeys.cs:ShouldSkipKey)
    判定:一致
    FOUND 标删/NOTFOUND(=LiveValue::Gone)不标删、失败路径不删键、超大单记录切块先冲批、末批冲刷;停等粒度 rust 批级 vs C# 发送缓冲级,等待语义等价。向量集/树流跳装帧由调用方带外通道承接,对位 C# ShouldSkipKey。

16. wedb/wedb/src/server/replication/snapshot_transmission.rs:send_store_checkpoint
    libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/SnapshotTransmissionDriver.cs:SendCheckpointAsync(+TsavoriteSnapshotReader、RangeIndexSnapshotReader/RangeIndexFileDataSource/RangeIndexFileTransmitSource)
    判定:一致
    序 hlog 段流→index 段流→RI 逐文件(头帧 -1/key_id、段流、空载荷收尾)→meta 单消息最后发送(副本提交标记);chunk 1<<17;段起点扇区对齐、终点物理文件长度口径有明确声明。

17. wedb/wedb/src/server/replication/replication_manager.rs:try_update_for_failover
    libs/cluster/Server/Replication/ReplicationHistoryManager.cs:TryUpdateForFailover
    判定:一致
    CommittedUntilAddress(动态提交尾,不读冻结字段)→ FailoverUpdate → FlushConfig → SetPrimaryReplicationId,次序与取数面同。

18. wedb/wedb/src/server/gossip/gossip_manager.rs:try_meet_async
    libs/cluster/Server/Gossip/Gossip.cs:TryMeetAsync
    判定:一致
    已知地址查 id 复用连接/临时键新建、应答版本预检再反序列化、merge 不查信任但查封禁、created 连接成功后以正式 id 交接 gossip 主循环、空应答不计成败不判失败、失败计 MeetRequestsFailed,逐项对位。

19. wedb/wedb/src/server/replication/replica_replay_driver.rs:signal_time_advance(+try_apply_pending_pulse)
    libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:SignalTimeAdvance/TryApplyPendingPulse
    判定:一致
    pending 原子单调、过期脉冲直退、会话线程持重放权时就地应用否则交背景循环 Throttle 消化;应用守卫 GetSublogReplicationOffset != TailAddress 不推进,逐项同。apply_pulse 补全虚拟子日志维有声明(单消费者折叠的等价展开)。

20. wedb/wedb/src/server/failover/replica_failover_session.rs:take_over_as_primary_async
    libs/cluster/Server/Failover/ReplicaFailoverSession.cs:TakeOverAsPrimaryAsync
    判定:一致
    BeginRecovery(ClusterFailover,不升锁)→纪元静止→TryTakeOverForPrimary→TryUpdateForFailover→ResetReplicaReplayDriverStore→ResetSequenceNumberGenerator(C# 有 SublogCount>1 门,rust 无门;单物理日志下 C# 门恒假、rust 恒执行,可观测等价)→InitializeCheckpointStore→纪元→StartPrimaryTasks→finally EndRecovery,锁/次序同。

21. wedb/wedb/wcol/src/hash/hash_object_impl.rs:hash_increment
    libs/server/Objects/Hash/HashObjectImpl.cs:HashIncrement
    判定:一致
    增量先解析后删过期、新字段存增量原文、旧值非整数回错、wrapping 加(C# release 无 checked)、result1 = i32::MIN 占位/1 成功、应答 FromBytes,逐项同。

22. wedb/wedb/wcol/src/set/set_object_impl.rs:set_pop
    libs/server/Objects/Set/SetObjectImpl.cs:SetPop
    判定:一致
    count>=1 批量(min(len) 封顶、随机下标逐枚弹)、NO_COUNT 单枚(空集 nil)、result1 恒 count(C# countDone += count - countDone 怪癖原样保留);随机源 fastrand 替换 RandomNumberGenerator 已声明。

23. wedb/wedb/wcol/src/list/list_object_impl.rs:list_position
    libs/server/Objects/List/ListObjectImpl.cs:ListPosition
    判定:一致
    rank>0 自头 take(maxlen)、rank<0 自尾 maxlen 界(len-maxlen 可负覆盖全表)、count=0 全量、缺省形态标量/显式 COUNT 数组、空命中 nil vs 空数组、noOfFoundItem→result1,双向扫描与断点语义逐项同。

24. wedb/wedb/wcol/src/zset/geo_impl.rs:geo_add
    libs/server/Objects/SortedSetGeo/SortedSetGeoObjectImpl.cs:GeoAdd
    判定:一致
    score==-1 跳过、XX 挡新增不挡既有、NX 挡更新不挡新增、分值变化才改、CH 计 changed 否则 added;坐标解析失败 (0,0) 兜底对齐 C# release 行为(Debug.Assert 出参 0.0),1:1。

25. wedb/wedb/wcol/src/zset/sorted_set_object_impl.rs:sorted_set_add
    libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:SortedSetAdd(+GetOptions)
    判定:一致
    XX/NX 互斥、NX/GT/LT 两两互斥、INCR 仅单对、剩余段空/奇数回 SYNTAX_ERROR(声明为 vendored 行为核对)、score 未变仅清过期、NX/GT/LT 拒更新时 INCR 回 nil、INCR NaN 回错、CH 计数与 INCR 应答出口同。

26. wedb/wedb/whyperlog/src/estimate.rs:count(+count_dense_nc_estimator/count_sparse_nc_estimator/c_tau/c_sigma/round_estimate)
    libs/server/Resp/HyperLogLog/HyperLogLog.cs:Count/CountDenseNCEstimator/CountSparseNCEstimator/cTau/cSigma
    判定:一致
    直方图装配(6 位寄存器 3 字节拆 4/RLE 直方图)、z=mcnt*tau((mcnt-h[q+1])/mcnt) 折半累加 h[q..1]、sigma 小值修正、alpha*mcnt^2/z、缓存卡数失效判;舍入用 round_ties_even 对位 .NET Math.Round 银行家舍入(单点声明)。

27. wedb/wedb/whyperlog/src/lib.rs:update/iterate_update
    同文件 C#:Update/IterateUpdate
    判定:一致
    稠密直更/稀疏可原位增长判定/不可增长返回需申请新空间、MurmurHash2x64A(seed 0) 逐元素、updated 出参语义同。

28. wedb/wedb/wnode/src/resp/bitmap/bitmap_commands.rs:network_string_set_bit
    libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringSetBit(+libs/server/Storage/Session/MainStore/BitmapOps.cs:StringSetBit)
    判定:一致
    offset 负值与 512MB 位上限校验、bit 参数单字符 '0'/'1'、MSB 在前位序、越界按需扩零、应答改写前旧位值;对象键拦截回 WRONGTYPE 为 rust 存储模型等价投影。常量 MAX_BITMAP_PAYLOAD_BYTES/MAX_OFFSET_FOR_BITMAP_LENGTH 与 BitmapManager.cs 逐值相同。

29. wedb/wedb/wext_roaring/src/roaring_bitmap_commands.rs:bit_pos_reader(+bit_pos_not_found/bit_pos_parse_args)
    modules/RoaringBitmap/RoaringBitmapCommands.cs:RBitPos(+RoaringBitmap.cs:BitPos)
    判定:一致
    bit 0/1 与可选 from 解析、错误文案分派;缺键分支 bit==1→-1、bit==0→from(缺省 0) 同 C# NotFound;命中查找按 chunk highKey 二分 + NextSetBit 同构。

30. wedb/wedb/wext_json/src/json_commands/set_get.rs:json_set_updater(+json_set_need_initial_update)
    modules/GarnetJSON/JsonCommands.cs:JsonSET.Updater/NeedInitialUpdate
    判定:一致
    参数 2/3 校验、NX/XX 解析与 SYNTAX_ERROR、Success→OK、ConditionNotMet→null(随协议版本)、错误→errorMessage;缺键 XX 需要初始更新段直回 null 与 C# 空 객체臂可观测等价;根路径 $ 限制 NewObjectAtRoot 同。

统计
30 函数:差异 13(其中增量未声明 5:条 1a/1d、2、3、4、5、8a;声明 8:条 1b/1c、6、7、9、10、11、12、13),一致 17。
覆盖:复制收发(14、19、4、5)、checkpoint 交付(16、2)、gossip 收发(18、10、11、3)、槽校验(12、9)、迁移收发(15、8)、failover(20、13)、对象核心(21、22、23、24、25)、扩展命令(29、30)、HLL(26、27)、bitmap(28)。

视角结论:有增量
增量集中五处:1) negotiate_resync 把 C# "仅记日志继续 partial"的分支折叠成 FullResync,same_history2 修上游笔误引入双方皆空边缘分叉;2) 检查点接收面 RI FLUSH 单消息与流式类型集收窄未登记;3) gossip 全量发送记账时机(成功后 vs 发送时)改变失败恢复行为;4) 单物理日志下时间脉冲恒发,C# MultiLogEnabled=false 静默;5) 迁移接收端 C# "半失败仍 +OK"怪癖未保留未登记(源端可能误删未落地键,保留 rust 收紧属正确决策,但应补 ignore/声明)。背压水位不折叠扫描位(条 5)为吞吐形状差异,建议补压测对照。
