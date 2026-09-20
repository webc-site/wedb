常量审计专项(轮8,视角 = 数值常量与硬编码×全仓横向;已立勿复述:r6-cli 默认值差、r6-doc SKILL 值核实、r4-foundation 常量整合面已立部分、r2-lifecycle 已立的 aof-memory/aof-page-size 回显失真 2 处)。

问题 1:AofReplayDriftCheckFreq 默认 0,C# 两侧均 1——副本回放主动漂移扫描默认臂相反,且 rust 注释把 C# 默认错写成 0
rust 侧 wedb/wconf/src/runtime_server_options.rs:133(Default replay_drift_check_freq: 0)+ :83 文档「默认 0」注释口径错;流转 wedb/wnode/src/aof/garnet_append_only_file.rs:71-72、:297-298;生效门 wedb/wnode/src/aof/readconsistency/read_consistency_manager.rs:64-66(proactive = replay_drift_check_freq > 0 && threshold >= 0 && virtual_sublog_count > 1)
c# 对位 libs/server/Servers/GarnetServerOptions.cs:139(AofReplayDriftCheckFreq = 1)+ libs/host/defaults.conf:173(= 1)+ libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:36、:48-49(ProactiveReplayDriftCheckEnabled 同型门,GarnetServerOptions.cs:1256 即 checkFreq > 0 判定)
判定 数值错(默认值差且影响行为):多物理子日志/多回放任务(虚拟子日志 > 1)部署下,C# 默认按 AofReplayDriftCheckFreq×Threshold 窗口轮转主动扫漂移,rust 默认退化为仅读者等待触发;单子日志默认下两侧同为关闭,故日常不自显。修法:Default 对齐 1 并订正 :83 注释(或登记有意收紧)。r6-cli 问题 1 六旋钮清单未含此项

问题 2:复制域发送字节顶用主存页位代入 AOF 页位式——128MiB,C# 生效值 256MiB,恰好折半
rust 侧 wedb/wconn/src/types.rs:32(MAX_UNFLUSHED_SEND_BYTES = 4 * (2 << 24),:21-31 注释自述「页尺寸取 C# AOF 同式 2 << 页位 = 2 << 24」「AOF 尺寸旋钮读侧接线落地后改接真实页位」)
c# 对位 libs/cluster/Server/Replication/ReplicationNetworkBufferSettings.cs:37(aofSyncSendBufferSize = 2 << AofPageSizeBits())+ libs/client/NetworkWriter.cs:55(BufferSize = 4 页);C# AOF 页默认 "32m"(defaults.conf:158)→ 位 25 → 单页 64MiB × 4 = 256MiB
判定 单位换算错(换算系数取错源):2 << 24 对应 16MiB 主存页,而 rust 自身 AOF 页旋钮默认 aof-page-size "32m"(位 25)已可读(wedb/wnode/src/aof/aof_settings.rs:AofSettings::from_options 已解析三旋钮),「等接线」前提已不成立,常量现值恒为忠实值的一半;影响副本 AOF 同步连接未刷出字节硬顶与写泵拼批分片粒度。修法:改 2 << (aof 页位) 或经 AofSettings 单点注入

问题 3:TCP 监听 backlog 1024,C# 为 512
rust 侧 wedb/wnode/src/net/socket_opt.rs:12(pub const TCP_LISTEN_BACKLOG: i32 = 1024;消费同文件 :74 listen())
c# 对位 libs/server/Servers/GarnetServerTcp.cs:154(listenSocket.Listen(512),TCP 与 unix socket 同一 listenSocket)
判定 数值不一致(未登记的自定放大):突发连接风暴下接受队列深度是 C# 的两倍;无注释登记,1:1 面应对齐 512 或注明差异理由

问题 4:MIGRATE 命令 timeout 参数 <= 0 时兜底 60000ms,C# 无此兜底
rust 侧 wedb/wedb/src/server/migration/migrate_driver/keys.rs:48(const DEFAULT_MIGRATE_TIMEOUT_MS: u64 = 60_000)+ :52-58(wait_dur:timeout_ms <= 0 取 60s)+ :44 CANCEL_POLL_SLICE 25ms 配套
c# 对位 libs/cluster/Session/MigrateCommand.cs:87(TryGetInt(4, out timeout),无下界校验直接透传)+ libs/cluster/Server/Migration/MigrateSession.cs:147(TimeSpan.FromMilliseconds(_timeout))与 MigrateSessionCommonUtils.cs:347(WaitAsync(_timeout));C# timeout=0 即 WaitAsync(Zero) 立即超时
判定 rust 自造(数值自定且改变行为):注释自认口径来自 redis-cli 而非 garnet;MIGRATE 显式带 timeout 0 时 rust 跑满 60s、C# 立即失败收场。若裁定防零值退化,需登记「有意偏离」;否则删兜底直传

问题 5:「默认主存日志页 16MiB」三处独立定值,无单一真源
rust 侧 wedb/wconf/src/node_options.rs:26(DEFAULT_HLOG_PAGE_SIZE,消费:aof_settings.rs:34 MAIN_PAGE_BITS 推 AOF 页下限)+ wedb/whlog/src/config.rs:14(DEFAULT_SERVER_PAGE_SIZE,消费:wkv/src/config.rs:287 页容量 clamp 上界)+ wedb/waof/src/wal/config.rs:5(DEFAULT_BUFFER_SIZE,经 wedb/wconn/src/types.rs:29 注释又作「rust 默认 wal 页 16MiB」引用)
c# 对位 libs/host/defaults.conf:28(PageSize "16m")+ libs/server/Servers/ServerOptions.cs:138(单一 ParseSize(LogMemorySize/PageSize) 读出口)
判定 语义重复字面量(3 处,该收编单点):同一 16MiB 语义分属三个 crate 各自裸定,问题 2 的页位错源正是这种双真源的产物;建议收敛为 wconf 一处导出,whlog/waof 反向引用

问题 6:集群节拍默认值双定义双单位
rust 侧 wedb/wedb/src/args.rs:11(DEFAULT_CLUSTER_NODE_TIMEOUT_MS = 60000,毫秒;消费 cluster_provider/mod.rs:181 播种原子槽)与 wedb/wconf/src/runtime_server_options.rs:103(cluster_timeout: 60,秒;播种 RuntimeServerConfig CONFIG 槽)
c# 对位 libs/server/Servers/GarnetServerOptions.cs:251(ClusterTimeout = 60,唯一真源,ClusterManager.cs:24 经 GetTimeSpan 单点读)
判定 语义重复(同语义 60s 两处两单位):现经 wedb/wnode/src/config_owner.rs:78-83 调停同步,未漂移,但值源未收敛,新增消费点直读任一侧即生成分叉;建议 args 默认改引 wconf 值 × 1000 单点推导

无增量确认:
- wconf 默认值面逐项对齐 defaults.conf:port 6379、slow-log 0/128、max-databases 16、object-scan-count 1000、expired-object-collection 0、expired-key-deletion -1、metrics-sampling 0、gossip delay 5/sample 100、cluster-timeout 60s、aof-size-limit-enforce 5s、compaction max-segments 32/type None、index-resize 60s/50%、readcache-memory 1GiB、revivifiable-fraction 1.0、connection-limit -1、commit-frequency 0、on-demand-checkpoint true、protected-mode true、resp-version 2、sublog 1/replay-task 1、drift-threshold -1、aof 尺寸串 "128m"/"32m"/"1g"(解析即实际,回显同源,已立 2 处回显失真之外未再见新失真点)
- vector 域全对齐:MaxVectorDimensions 65536、MaxRetrieveCount 1e8、MaxFilteringScaleFactor 256、MaxExplorationFactor 1e6(VectorManager.cs:67-92);VSIM count 10/delta 2.0/ef 100/filter-ef 16(RespServerSessionVectors.cs:856-863);ContextMetadata 160B(4×8+64×2,VectorManager.ContextMetadata.cs:40-42)
- hll 域全对齐:alpha 0.7213475204444817、reg_bits 6、sparse 每插值 2B/上限 1<<12/步进 1<<7(whyperlog lib.rs vs HyperLogLog.cs:43-73);sketch slot 1<<15(VirtualSublogReplayState.cs:15)
- geo 域全对齐:52bit/code 11/±180±90/地球半径 6372797.560856(wcol geo_hash.rs vs GeoHash.cs:20-95、:265)
- 布局域全对齐:windex 桶 8 槽(7 数据+1 溢出)、地址位 48、latch 位型(Tsavorite Constants.cs:27-28、LogAddress.cs:14);RI stub 35B(RangeIndexManager.Index.cs:49)与 RI 调参 16MiB/64/1024/128/0(RespServerSessionRangeIndex.cs:44-50 → wbftree TreeTuning::DEFAULT_RI)
- 命令上限:位图载荷 512MiB 单源共享(wbitmap manager.rs:9,SETRANGE set.rs:726 复用,同 BitmapManager.cs:19);RESP 数组上限 1<<20 同 RespCommand.cs:774;ACL GENPASS 64 字符/4096 bits/length 向上取整同 ACLCommands.cs:404-426;.NET 刻度换算(TICKS_PER_MS 10_000、UNIX_EPOCH_TICKS、DateTimeOffset.MaxValue 253402300799/999)精确
- 集群杂项:BUS_PORT_OFFSET 10000 同 ClusterConfig.cs:558;MIN_SECS_BETWEEN_KEYS_TO_OBSERVERS_CLEANS 300 同 CollectionItemBroker.cs:28;failover 默认 600s 同 FailoverSession.cs:85;cluster-config-flush-frequency 默认 0 在位
- 网络顶:通用客户端 4×2MiB=8MiB 同 GarnetClient.cs:149+NetworkWriter 4 页;REPLICA_SYNC 5s/REPL_ATTACH 60s 常量与 C# 默认一致(配置面缺失 r6-cli 问题 1 已立);wbase 缓冲池 small 32MiB/large 128MiB/stripe-cap 8 为文件内登记的嵌入式缩小(C# 预算 1g、small=预算/4、cap 1024,BufferPool.OriginReturn.cs:257-338),属登记偏差非漂移
- C# 有 rust 无(归既定架构/旋钮面,非数值错):ReadCachePageSize "4m"(rust 读缓存复用自适应主存页)、PubSubPageSize "4k"(wpubsub 为 crossfire 通道模型,SKILL 钦定)、FastCommitThrottleFreq 1000 与 CheckpointThrottleFlushDelayMs(whlog 组提交管线无 fast-commit 模式)、MigrationChunkSize(rust 256KiB 分块为自有粒度,C# 按缓冲满即刷无定值)
- 魔数面:varint 1-4/9B 标记掩码、record/位图头部偏移、epoch/latch 移位、pool size-class 编排均单点定义自洽;roaring cookie 12346/12347 与 offset 阈值 4 为 CRoaring 标准格式常量(C# modules/RoaringBitmap 自有序列化格式,差异属 SKILL 格式不兼容钦定面)
- 自加收紧:rust MAX_DATABASES 上限 256 为 C# 无校验下的自加拒启臂(方向为收紧,归 r6-cli 校验面同型裁决);MaxInlineKey 1022/MaxInlineValue min(1m,page/2) 未转写独立旋钮,页模型收敛为整页分配(登记于 whlog config.rs 注释)

视角结论:有增量
