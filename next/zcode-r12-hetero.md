视角 异构集群互操作:C# garnet 节点与 rust 节点混编同一集群
此前轮次查的是 rust 内部自洽,本轮专查「一个 C# 节点 + 一个 rust 节点互相 MEET 后的全交互链」。

前置裁决
SKILL 明文「编码尽量用 bitcode…不需要和 c#格式兼容」;代码侧 wedge 已多处刻意差异(集群配置 v2、MIGRATE 头改形、库级定槽废除键哈希、nodeid u128、FLUSHALL_NS 自增命令)。
裁决:混编不支持,成立。本审查按任务指令转为「混编破绽清单 → 登记为已声明边界」的验证:逐链推演异构交互,判定每步兼容性,核对每条破绽的声明状态。运行期 bug 零(混编不可达,全部在第一道版本门被拦);产出为边界登记清单 + 3 条登记建议。

链1 节点发现与握手
交互:管理员 CLUSTER MEET → 出站连接走主端口(C# 无独立总线口,+10000 仅为 CLUSTER NODES 渲染) → 发 CLUSTER GOSSIP WITHMEET <本端全量配置字节> → 对端版本门 → 合并或拒 → 应答本端配置字节 → 发起端同门校验。
命令面:双侧同字面量。rust wedb/wedb/src/client.rs:gossip_with_meet_async 发 [CLUSTER GOSSIP WITHMEET data];C# garnet/libs/cluster/Server/Gossip/GarnetClientExtensions.cs:GossipWithMeetAsync 同。收端 rust wedb/wedb/src/server/cluster_session/basic.rs:network_cluster_gossip 与 C# garnet/libs/cluster/Session/RespClusterBasicCommands.cs:NetworkClusterGossip 参数形态(1/2 参、WITHMEET 前缀)一致。MEET 均 2 参。
载荷门:rust 版本 2(bitcode,wedb/wedb/src/server/cluster_config/serializer.rs:CLUSTER_CONFIG_VERSION/try_peek_version);C# 版本 1(.NET BinaryWriter,garnet/libs/cluster/Server/ClusterConfigSerializer.cs + ClusterConfig.cs:47)。双侧 TryPeekVersion 对称前置,异版本仅告警降级为空 ping,不解码不合并。
判定:握手命令层兼容;配置载荷双向不互认。混编 MEET 结果:rust→C#,C# 警告「incompatible config version」仍应答 v1,rust 侧 gossip_manager.rs:try_meet_async 版本门拒,rust→C# 反向同,双双保持单例。无崩溃、无脏合并,日志各每轮一条告警。
声明状态:已声明。serializer.rs 头注「v2 起换 bitcode,无向下兼容负担,异版本载荷解码前即被拒绝」。
nodes.conf 互读:双侧同为设备文件二进制 blob(C# ClusterManager.cs:73 nodes.conf 经 ToByteArray 落盘;rust cluster_provider/replication.rs:initialize_cluster_config 同构)。C# 读 rust v2 → InvalidDataException 启动失败;rust 读 C# v1 → Error::Version,boot.rs:239 map_err(?) 启动失败。双向 fail-loud,无静默重引导。判定:不兼容,已声明(v2 注),升级即换格式需 clean-cluster-config 重引导,属声明后果。

链2 槽位表合并
交互:仅当链1 载荷互认才达此链;混编下不可达。仍核语义层互译性。
仲裁:rust wedb/wedb/src/server/cluster_manager.rs:try_merge + cluster_config/mod.rs:merge/merge_slot_map/handle_config_epoch_collision 对位 C# Gossip.cs:TryMerge + ClusterConfig.cs:Merge/MergeSlotMap/HandleConfigEpochCollision,epoch 大者赢、槽位 Stable 认领、副本移交其主,语义同形。
SlotState 互译:wedb/wedb/src/server/hash_slot.rs 与 garnet/libs/cluster/Server/HashSlot.cs 值逐一相同(0..6),MIGRATING eff=LOCAL 语义同;NodeRole 0/1/2 同(Worker.cs vs worker.rs)。
判定:语义层可译;线格式层不互认(链1 已拦)。
备注(声明修正建议,非 bug):epoch 碰撞仲裁键域不同——C# ClusterConfig.cs:1529 对 40 字符 hex 字符串字典序 CompareTo,rust cluster_config/mod.rs:898 对 u128 数值序;rust 注「u128 数值序同为确定性全序…双方各退一步」只对同键域成立,混编下两侧可判出不同赢家。混编不可达,建议在该注释补一句「混编下仲裁不一致,亦为不支持混编的依据之一」。

链3 复制互连(rust 副本挂 C# 主 / C# 副本挂 rust 主)
握手帧层:ATTACH_SYNC 3 元帧、INITIATE_REPLICA_SYNC 5 参帧、APPENDLOG 7/8 元帧、ADVANCE_TIME 4 元帧双侧同形(rust wedb/wconn/src/session.rs:encode_*;C# garnet/libs/client/ClientSession/GarnetClientSessionReplicationExtensions.cs 同名 Execute*)。
协商载荷:SyncMetadata rust bitcode 无版本域(wedb/wedb/src/server/replication/sync_metadata.rs:SyncMetadataWire,origin_node_id 为 u128);C# BinaryWriter ASCII、originNodeId 为 40 hex 字符串(garnet/libs/cluster/Server/Replication/SyncMetadata.cs:ToByteArray)。C# 收端 FromByteArray 无版本门,异构喂入是 BinaryReader 误读(错位串/越界异常/垃圾字段)而非显式拒绝;rust 侧 bitcode::decode 对异构字节同样非结构化失败。判定:不兼容,失败形态不受控但 C# 侧 TryBeginDisklessSyncAsync 按 originNodeId 查配置在册性兜底,未知节点拒绝,无状态损坏。
checkpoint 交付:帧协议同构(SNAPSHOT_DATA 6 元帧、SEND_CHECKPOINT_FILE_SEGMENT/METADATA、头帧/段帧/空载荷收尾,rust snapshot_transmission.rs + receive_checkpoint_handler.rs 对位 C# DiskbasedReplication 三件套);token 字节布局不同(rust u128 to_le_bytes 纯小端 vs C# Guid.ToByteArray 混合端)。载荷为 wcpr/wbftree 设备段与 Tsavorite hlog/index 文件,彻底异构,接收落盘后恢复期 CRC/元数据拒绝,fail-loud。载荷异构面 r10-format 已立项,此处只判帧层与控制层:帧层兼容、载荷不兼容。
AOF 流:帧头(nodeid 字符串 + prev/curr/next 地址 + 载荷 bulk)同形;载荷记录物理键 [NsVarint][DbVarint][KeyTag] 异构,r10-format 问题1 已立(其「AOF 版本门同号不拦」的跨仓搬文件形态即本链异构注入的具体化)。心跳:ADVANCE_TIME 帧兼容,但 sequence_number 语义锚定各自 AOF 记录域,异构下无意义,引用 r10。
判定:控制层兼容、身份与载荷层双向不兼容,第一帧(ATTACH_SYNC/INITIATE_REPLICA_SYNC 元数据)即失败,无半接管。
声明状态:载荷/版本域面已声明(SKILL + r10-format + r4-foundation「bitcode 无版本域」);「SyncMetadata 异构喂入是非结构化垃圾解码而非版本门显式拒绝」为 r4 该票在复制链的具体形态,登记引用即可,不另立。

链4 迁移互连
rust 源→C# 目标:rust 头 4 元 CLUSTER MIGRATE <sourceNodeId> <replace> <slot-list> <payload>(wedb/wedb/src/client.rs:execute_cluster_migrate_async,头注明 C# 头无 slot-list、isVectorSets 位废除、逐键 HashSlot 门禁改头级判槽);C# 目标 garnet/libs/client/ClientSession/GarnetClientSessionMigrationExtensions.cs:SetClusterMigrateHeader 为 6 元(无 slot-list)。C# 收端 parseState 计数不符 → invalidParameters 显式拒。载荷记录 bitcode 异构。判定:帧层不兼容,显式拒。
C# 源→rust 目标:rust 收端 wedb/wedb/src/server/cluster_session/mod.rs:process_cluster_sub_commands → migrate 臂按 4 参校验,C# 3 参头 → 参数错误应答,显式拒。
RESERVE/快照带外流:RESERVE VECTOR_SET_CONTEXTS count 帧双侧同形(C# RespClusterReplicationCommands.cs:NetworkClusterReserve vs rust client.rs:reserve_vector_set_contexts_async);SYNC 复制式迁移帧同形;Sketch 为源端本地结构不上线,sketch.rs 对位 Sketch.cs 同形。带外快照流同链3 结论。
判定:不兼容,全部在头帧参数校验显式失败,无半迁移。
声明状态:已声明。client.rs 头注 + doc/zh/db.md 4.1(库级定槽偏离声明)。

链5 集群总线
FLUSHALL_NS:rust 自增命令(wedb/wnode/src/resp/admin_commands.rs:network_process_cluster_command 收令身份门 + cluster_session/replication.rs:network_cluster_flushall_ns,出站 node_connection.rs:try_flushall_ns_async 期待 +OK)。C# 无 ns 维度无收令臂,收 CLUSTER FLUSHALL_NS 应答未知命令错误 → rust 广播 ack 校验失败。判定:单向(rust 内部)功能,C# 节点永不应答 OK。
声明状态:已声明。admin_commands.rs 注「本命令属 rust 自增多租户管理面(C# 无 ns 维度、无对应收令臂,非移植产物)」+ doc/zh/db.md 3.5/4.5。
ADVANCE_TIME:帧同形已核(链3),异构下序列号域无意义,引用 r10。判定:帧兼容、语义不兼容。
FAIL 传播:CLUSTER FAIL STOPWRITES/REPLICATIONOFFSET 帧双侧同形(rust cluster_session/failover.rs:network_cluster_fail_stop_writes 对位 C# RespClusterFailoverCommands.cs);nodeid 线面 32 hex vs 40 hex,异构对端查表必不中。判定:帧兼容、身份域不兼容,显式拒。声明状态:nodeid u128 已声明(SKILL)。
PUBLISH/SPUBLISH 转发:帧同形(node_connection.rs:try_cluster_publish_async 对位 GarnetClientExtensions.cs:ExecuteClusterPublishNoResponse),频道/消息为不透明字节。判定:兼容(总线中唯一真正可互通的一条,但前置配置合并在链1 已被拦,实际不可达)。

链6 客户端重定向一致性
MOVED/ASK 出帧:wedb/wedb/src/server/slot_verify.rs:write_slot_verification_message 从 config 取 endpoint/port,文本形态与 C# GetSlotVerificationMessage 同;端口语义同(服务端口,CLUSTER NODES 渲染 bus=port+10000 双侧一致,ClusterConfig.cs:558 vs serializer.rs:BUS_PORT_OFFSET);ASKING rust 已实现(wnode/resp/resp_server_session/core.rs:721)。地址解析互通(纯文本 host:port)。
槽值语义:rust MOVED/ASK 槽号 = slot_of(ns,db)(wbase/hash_slot.rs:slot_of,mix64 & 0x3FFF);C# = 键哈希。C# 节点(或 CRC16 智能客户端)收到 rust 的 MOVED 后按自有槽域路由下一键,必错位。KEYSLOT:rust 不解析键,直接回会话库槽(slot_mgmt.rs:network_cluster_keyslot),C# 按键哈希。判定:重定向帧格式与地址互通,槽号域不互通,双向皆然。
声明状态:已声明。doc/zh/db.md 4.1-4.4(废除键哈希、库级定槽、CROSSSLOT 消失)+ hash_slot.rs 注。

登记汇总(混编破绽 → 已声明边界,验证通过)
1 gossip 配置载荷 v1/v2 双向互拒(nodes.conf 同源) — serializer.rs 版本注已声明,双向 fail-loud。
2 SyncMetadata/CheckpointEntry/AOF 载荷/checkpoint 文件异构 — SKILL 编码条款 + r10-format 问题1 + r4-foundation「bitcode 无版本域」已覆盖。
3 MIGRATE 头改形(slot-list 增、isVectorSets 删、逐键门禁废) — client.rs 头注 + db.md 4.1 已声明。
4 库级定槽 vs 键哈希:KEYSLOT/MOVED 槽号域不互通 — db.md 4.1-4.4 已声明。
5 FLUSHALL_NS 为 rust 自增,C# 无对应臂 — admin_commands.rs 注 + db.md 3.5/4.5 已声明。
6 nodeid u128(32 hex)vs C# 字符串(40 hex),贯穿 GOSSIP/FAIL/APPENDLOG/MIGRATE 全部线面 — SKILL「Guid 统一用 u128」已声明。
语义层同形保留面(换编码后即可译):SlotState/NodeRole 枚举值、merge 仲裁、MEET/PUBLISH/SNAPSHOT_DATA/RESERVE/ADVANCE_TIME/FAIL 帧形、总线端口渲染。全部命令层互认、载荷层互拒,失败点集中、显式、无静默损坏。

登记建议(未声明的增量,均为边界成文,非运行期 bug)
1 「不支持与 C# garnet 混编」无一处集中显式声明。现散见各代码注与 db.md 4,缺一句总边界(建议置于 doc/zh/db.md 4 节头或 SKILL 边界段:集群协议域 gossip 配置/复制元数据/迁移载荷/checkpoint 文件与 garnet C# 不互通,混编集群不受支持,单节点替换需 clean-cluster-config 重引导)。
2 cluster_config/mod.rs:handle_config_epoch_collision 注称「u128 数值序同为确定性全序(方向无害)」,应补注:仅同键域成立,C# 侧为 40 hex 字典序,混编下仲裁会双向不一致——此为不支持混编的又一依据,防后人据此误开互译改造。
3 链3 SyncMetadata 异构失败形态(无版本门 → BinaryReader 非结构化垃圾解码)建议在 sync_metadata.rs 头注点名引用 r4-foundation「bitcode 无版本域」票,声明「跨仓喂入按垃圾处理,C# 侧由 originNodeId 在册性兜底」——把未定义行为钉成已定义边界。

视角结论:有增量
