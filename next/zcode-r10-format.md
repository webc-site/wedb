落盘/线上格式单点纪律专项(轮10,横向审计;已立勿复述:r4-foundation bitcode 无版本域、r2-crash AOF CRC 链、r6-doc SKILL 值核实、r8-const 默认值/魔数/页尺寸三真源与 roaring cookie 裁决面)。

格式清单盘点(定义点核验)

AOF 记录帧:语义头族单点 waof/src/aof/header/(basic.rs AofHeader/AofHeaderType、chunk.rs AofChunkHeader、transaction.rs 分片+事务头),to_bytes/parse 同 struct,header/mod.rs 磁盘布局锚点测试按 C# FieldOffset 锁死;写侧单漏斗 wnode/src/aof/garnet_log/single_log_branch.rs:enqueue_with_header/enqueue_span_chunked,读侧统一经 AofHeader::skip_header/sequence_number_of(wnode/src/aof/record_gate.rs、aof_processor.rs 注释自承不自带第二套帧判定)。物理帧(waof/src/wal/header.rs WalFrameHeader 8B=entry_len+crc32)与语义头分层,单点成立。

checkpoint:元数据 wcpr/src/meta.rs CheckpointMeta(bitcode 单格式,FORMAT_VERSION=4 拒旧,integrity_crc32 逐字段摘要独立于序列化布局),文件名 base32 构造单点同文件;索引快照 wcpr/src/index_ckpt/codec.rs(64B 头 encode/decode_opt 同 struct,WEDB_IDX 魔数、INDEX_VERSION、HEADER_CRC_OFFSET、BUCKET_BYTES 单文件),写读流程分件 batch.rs/read.rs 均引 codec 常量。

DbMeta/DbMap:wkv/src/vdb/meta_record.rs DbMetaRecord 六变体键值布局单点(encode=decode 同 enum,roundtrip/截断/前缀错位/未知子类型测试固化),vdb 全域(flush/swap/gc_dead/vdb_load/keyspace)与复制面(wconn kind=8)均经此,无绕行手拼。

信封 wire:wcol/src/object_payload.rs 单点([1B tag][4B count LE][8B 水位 LE][bitcode]),COUNT_BLOB_HEADER/WATERMARKED_BLOB_HEADER 单常量,hash/set/list/zset 四类型 from_blob/to_blob 同文件对称;RESP 渲染与持久化格式分离。

RI 树:存根 wbftree/src/stub.rs RangeIndexStub 35B encode/decode 同点;分块流 wbftree/src/chunk/(serializer/deserializer 同目录,KEY_LEN_BYTES 等常量 mod.rs 单点,流格式文档化);页格式归外部 bf-tree crate,本仓只持文件命名(wbftree/src/manager/mod.rs DATA_FILE_SUFFIX 单点)。

ACL:wacl/src/user.rs UserRecord bitcode 单格式,encode/decode 同文件,无文本旁轨,演进不变量(尾部追加/破坏须加版本域)入注释。

集群 nodes.conf + gossip:同一编解码对(wedb/src/server/cluster_config/serializer.rs to_byte_array/from_byte_array,CLUSTER_CONFIG_VERSION=2 前置版本门 try_peek_version),落盘(cluster_manager.rs write_config_to_device)与 gossip 载荷同源;CLUSTER NODES/SLOTS/SHARDS 文本渲染同文件,RESP 面与持久面不混。

复制/迁移:迁移帧 M1/M2 单点 wconn/src/record.rs(8 类帧 encode/parse 同文件,金字节测试,frame_import.rs 注明"不起第二套解析",全部消费方 import 无复制);SyncMetadata/CheckpointMetadata bitcode 单点(wedb/src/server/replication/sync_metadata.rs、checkpoint_entry.rs);APPENDLOG/ADVANCE_TIME/ATTACH_SYNC RESP 帧前缀常量单点 wconn/src/session.rs;复制 wal 帧发送经 WalRecord::reconstruct_frame 与副本落盘 enqueue_raw 同字节序列。

hlog 记录:wrecord/src/header.rs RecordHeader 16B,内存布局=盘面(编译期断言),RDH_WORD_OFFSET 全仓单点,位段常量 header/bits.rs 单文件。TTL/ETag 旁路 8B 经 wval/src/codec.rs I64Codec 单点。

cursor:SCAN/分层扫描游标为纯内存整数偏移(wnode/src/resp/objects/tiered_collection_ops/scan.rs),无落盘格式,无双格式面。

问题 1:AOF 版本域与 C# 同号(5)而载荷异构——版本门对跨仓装载形同虚设,半兼容半吊子面
问题 AOF 帧头字节面刻意对位 C#(16B 布局、判别值、FieldOffset 锚定),版本号 5 与 C# 相同,门只拒 >5;但载荷层两处实质异构:键为物理键 [NsVarint][DbVarint][KeyTag][用户键](C# 为用户键 + 头内 databaseId 单字节),输入区为 32B 显式分列头(C# 为 3B 判别联合 + parseState 序列)。同仓内格式自洽无碍,但 C# garnet 产出的 AOF(version=5)可无损通过 rust 版本门后在键/输入区被静默误读(错域建键、输入错解),反向亦然——版本门本应承担的「异构格式显式拒绝」职责失效。其余格式各有自持版本域(checkpoint meta v4、索引快照 v1、集群配置 v2),唯 AOF 沿用 C# 同号,版本域策略仓内不一致。
rust 侧 waof/src/aof/header/basic.rs:AofHeader::AOF_HEADER_VERSION(=5)/MAX_SUPPORTED_AOF_HEADER_VERSION(=Self)+ wnode/src/aof/aof_processor.rs:363(唯一版本门)+ wnode/src/aof/replay_input.rs:REPLAY_INPUT_HEADER_SIZE(32B,注释自认与 C# 布局分列)+ wval/src/ns_codec.rs:NamespaceDbCodec::encode_tagged_key(物理键前缀)
c# 对位 garnet/libs/server/AOF/AofHeader.cs:187(AofHeaderVersion = 5)、:193(MaxSupportedAofHeaderVersion)+ libs/server/AOF/AofProcessor.cs:240(同型门)+ libs/server/InputHeader.cs:41-49(RespInputHeader 3B 联合)+ libs/server/AOF/GarnetLog.cs:1272(databaseId 入头)
判定 裁决缺登记(半兼容半吊子,非运行期 bug:仓内读写闭环自洽,风险仅在跨仓/跨代际搬文件)。修法二选一:AOF_HEADER_VERSION 改自持域(如 0xA0 起)使异构文件在版本门显式拒绝;或保留 5 但在 basic.rs 版本常量处登记「与 C# 同号不同构,禁止跨仓搬 AOF 文件」的显式声明。SKILL 钦定不兼容,现缺的是把不兼容做成可判定。

问题 2:RI 复合元记录 [MetaValue 32B][RangeIndexStub 35B] 读侧窗口切分手拼散点,写侧有单点读侧无对偶
问题 写侧单点成立:encode_meta_stub_record(wkv/src/range_index/mod.rs,注释「落盘一律经门面,禁绕行手拼」),存根-only 读侧也有单点 range_index_stub_of(wkv/src/range_index/heal.rs:195-206,自承「复合元记录存根解码单点」)。但需要 (MetaValue, stub) 成对的装载路径未走任何单点,同一段「长度守卫 + 前窗 MetaValue::from_slice + 后窗 RangeIndexStub::decode」切分体手拼三处:load_collection_stub_in_window、load_range_index_stub(wkv/src/range_index/stub.rs:66-81、:125-141)、迁移 claim 重读(wkv/src/range_index/migration.rs:216-221)。布局常量本身单点(META_VALUE_SIZE/RANGE_INDEX_STUB_SIZE),无即时漂移实害;风险在窗口语义(越界判定、错误折叠 Ok(None) 还是 Err)三处各自维护,新增字段或改错误口径时易漂移出第三种行为。
rust 侧 wkv/src/range_index/mod.rs:encode_meta_stub_record + wkv/src/range_index/heal.rs:range_index_stub_of + wkv/src/range_index/stub.rs:load_collection_stub_in_window/load_range_index_stub + wkv/src/range_index/migration.rs(claim 重读)
c# 对位 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:ReadIndex(单点 reinterpret)
判定 真散点(轻度):建议补 meta_and_stub_of(bytes) -> Result<(MetaValue, RangeIndexStub)> 单点,range_index_stub_of 改为其薄封装(仅丢弃 meta),三处装载点收编;写侧 encode 单点与读侧 decode 单点由此成对。

问题 3:AOF 载荷 key/value 4B 长度前缀读写无共享切分体,读侧三处重复
问题 AOF 条目载荷形状「[4B keyLen][key][4B valLen][val](has_chunk_value 型)[input]」:写侧在 single_log_branch.rs 内联拼装(key_len_bytes :90、value_len_bytes :100-103),读侧切分体重复三处——record_gate.rs:peek_entry_key(:84-90)、aof_processor.rs:prepare_key(:219-221,注释自承与 peek_entry_key「同一函数」口径却复制了 4B 切分体)、aof_processor.rs:split_value_input(:249-252)。C# 侧本就散(AofProcessor 直切),此为 1:1 忠实;但 rust 已在 record_gate 立「帧游标单点」门面,prepare_key/split_value_input 未复用,属单点门面下的残余散点。AOF 布局被 C# 冻结,漂移概率低,收敛建议仅为去重。
rust 侧 wnode/src/aof/record_gate.rs:peek_entry_key + wnode/src/aof/aof_processor.rs:prepare_key/split_value_input + wnode/src/aof/garnet_log/single_log_branch.rs:enqueue_with_header
c# 对位 libs/server/AOF/AofProcessor.cs:GetSynchronizedOperationParams/回放反序列化段(同样直切,无单点)
判定 真散点(轻度,1:1 忠实面):可提 aof_load_key(payload) -> Option<&[u8]>、aof_split_value_input 单点供三处共用;不动字节布局。

问题 4:EMPTY_REPLAY_INPUT_BYTES 是编码产物的手工第二静态形态,与编码器无绑定
问题 空命令输入的 AOF 载荷定义了 36B 全零常量([u8; 36])直接入队,语义= ReplayInput 默认形态经 encode_to_slice 的产物(32B 头零 + 4B 参数计数 0)。编码器与常量同文件但无任何绑定(常量无法 const 求值,亦无 roundtrip 断言测试钉死相等);ReplayInput 头布局(如 obj_type 偏移、头部尺寸)一旦演化,该常量静默失配,回放侧把失配字节当合法空输入吞下。现值正确,风险纯预防性。
rust 侧 wnode/src/aof/replay_input.rs:EMPTY_REPLAY_INPUT_BYTES(:24-25,消费 wnode/src/service.rs:168 等)
c# 对位 libs/server/InputHeader.cs(空输入无独立常量,C# 每次经 RespInputHeader 构造)
判定 语义重复字面量(轻度):建议测试层加 assert_eq 断言(EMPTY == ReplayInput::default().serialize 产物)钉死,或运行期 OnceLock 派生;不必改用方式。

文本格式裁决
配置:wconf 单格式 nested_text(node_options.rs「唯一的配置文件格式」,无 redis.conf 兼容层,connection_protection_option.rs:9 显式登记),对位 C# GarnetConf/RedisConf 双格式——不兼容已自洽且已登记。
nodes.conf:C# 本就二进制落盘(ClusterManager.cs:133/188 ClusterUtils.WriteInto(currentConfig.ToByteArray()),非 Redis 文本 nodes.conf);rust 换 bitcode v2 + 版本字节,跨仓不可读但版本门显式拒绝,不兼容已自洽。
日志(文本输出):非持久化格式域,log crate 输出无 C# ILogger 逐字节对位义务,无双格式面。
半兼容总面:即问题 1 的 AOF 版本域——全仓唯一「同号异构、门不拦截」点;其余格式(checkpoint meta v4 / 索引快照 WEDB_IDX v1 / 集群配置 v2 / bitcode 无版本域面)或不兼容已自洽、或归 r4-foundation 已立。

无增量确认:
- waof 语义头族编解码同点且金字节锚定(AofHeader 16B/AofShardedHeader 24B/事务头 50B/58B/AofChunkHeader 28B,header/mod.rs 布局锚点测试);AofEntryType 判别值与载荷形状(has_key/has_chunk_value/has_chunk_input/has_chunk_object_value)单点 entry_type.rs
- waof 参数序列区 [count u32][4B len][bytes] 单点 aof/args.rs(encode/decode 同文件);wresp session_parse_state.rs:serialize_to 同布局另一实现仅服务慢日志快照,不入 AOF 链(注释互指,归既定分层)
- waof::wal 8B 物理帧单点;EMPTY_PAYLOAD_CRC 哨兵防全零歧义;wire 与副本落盘同帧(reconstruct_frame 契约)
- wcpr CheckpointMeta bitcode+FORMAT_VERSION+integrity_crc32 全单点;元数据落盘/读回(manager/create.rs、recover)同一编解码口;复制面 STORE_SNAPSHOT 透传同一 bitcode 字节
- wcpr index_ckpt 桶 64B = windex HashBucket 内存布局(8×8B)直出直入,槽位净化(sanitize_data_slot/resolve_read_cache/sanitize_overflow_slot)codec.rs 单点;ReadCache 指针解析闭包契约与 wkv 同口径
- wval MetaValue 32B 大端单点(偏移常量私有化,仅 META_VALUE_SIZE 外露);StorageEncoding 持久化解码严格单值;I64Codec 单点(TTL/ETag/DbMeta 值域共用)
- wkv DbMetaRecord 六变体单点;ROOT_DBMETA_PREFIX 单点;复制/迁移/回放三链均经 DbMetaRecord 编码(wconn kind=8 只透传字节)
- wcol 信封单点;成员级 TTL 队列(expiration_queue)随 bitcode 载荷一体,无带外第二格式;sonic_rs 仅 JSON 数据类型命令面,不涉持久化
- wbftree 存根 35B 与字段偏移(32/33/34)单点;RI 分块流 serializer/deserializer 常量与 47B 单块原子性契约单点;快照文件归 bf-tree 引擎,本仓零布局知识
- wrecord RecordHeader 单点(编译期断言内存=盘面);Pad 头/零头判定同文件
- wacl UserRecord 单点无文本旁轨;CommandPermissionSet wire_mode/bitmap 单点转写
- 集群:ConfigWire/WorkerWire bitcode 单点,0 号保留位重建口径与 C# skip(1) 对齐;worker.rs 注明「两端同版无兼容负担」;BUS_PORT_OFFSET 单点(serializer.rs,与 r8-const 已核 C# :558 一致)
- 复制:CheckpointFileType 判别值域单点且 from_protocol 拒域外值;SYNC 帧前缀常量单点;RI 流双通道(AOF RangeIndexStreamChunk 0x80 与迁移 kind=4 TREE_STREAM)各有单点且元三元组(.NET Ticks/i64::MAX 哨兵)语义一致,系 C# 同构双通道对位非双格式
- 游标、wtxn 事务暂存、wpubsub 通道、wmetric、wreviv、read_cache 均无独立落盘格式;位图 payload 为裸位串(String 域)无头,SETBIT 面 wbitmap 单点
- ri stream chunk 首/尾/replace 标志位 pack/unpack 同点(wnode/src/range_index/range_index_manager_replication.rs:136-144);token/key_id 16B LE 头帧发送与接收两侧同口径(snapshot_transmission.rs/receive_checkpoint_handler.rs)

视角结论:有增量
