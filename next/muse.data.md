review-data：对照 garnet 数据类型 / redis 命令及参数 / TTL，rust 遗漏缺失

范围：wcol,wval,wkv,wnode/src/resp,wresp；对标 garnet/libs/server/Resp,garnet/libs/server/Objects,ExpirationWithOption.cs,ExpireOption.cs
check.js 口径：实现缺失仅 RespWriteUtils,Lua/NativeMethods,OverflowBucketLockTable 三项非数据面；重复定义无数据面真重复（HashSet/tree_put_batch 等为跨层调用）

1. DELIFEXPIM 有枚举无接线，僵尸命令
问题：wresp/src/command.rs:29 Delifexpim=9 与 C# RespCommand.cs:39 DELIFEXPIM=9 对齐，但 command_table.rs 全表无 DELIFEXPIM 条目，garnet_api/raw.rs,slow.rs 无 C::Delifexpim 分派臂；wkv 仅 ttl.rs:681 注释提及。客户端发必回未知命令；C# 侧为 UnifiedStore RMW 内部条目同样无 RESP 接线，属两边一致的内部命令外泄到枚举。建议：明确为内部 RMW 保留并在 command.rs 写清不对客，或补 parser 条目 + 分派 + network_delifexpim（对标 BasicEtagCommands.cs:NetworkDELIFGREATER 形态）。
rust：wedb/wresp/src/command.rs:29，wedb/wnode/src/resp/parser/command_table.rs（缺条目），wedb/wnode/src/resp/garnet_api/raw.rs（缺分派）
c#：garnet/libs/server/Resp/Parser/RespCommand.cs:39，garnet/libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs:27,67,185,202

2. MIGRATE 有 parser 条目无执行体，发了必错
问题：command_table.rs:132 MIGRATE->Migrate 有名无实，raw.rs,slow.rs 无 C::Migrate 臂，落入未知命令；resp_server_session.rs:1707 把 Migrate 判给 cluster 通道，cluster 侧亦无数据迁移执行（仅 ClusterMigrate 槽迁移）。C# RespCommand.cs:51 同为集群迁移语义（MigrateCommand.cs:168 COPY/REPLACE 为槽迁移 sketch，非 redis 单键 MIGRATE）。若目标是 redis MIGRATE（COPY/REPLACE/KEYS），两边都缺；若是 garnet 槽迁移，rust 缺承接说明。建议注释写清 MIGRATE=槽迁移非 redis 单键迁移。
rust：wedb/wnode/src/resp/parser/command_table.rs:132，wedb/wnode/src/resp/resp_server_session.rs:1707，wedb/wnode/src/resp/garnet_api/raw.rs（缺臂）
c#：garnet/libs/server/Resp/Parser/RespCommand.cs:51，garnet/libs/cluster/Session/MigrateCommand.cs:168

3. COPY/TOUCH 全仓无枚举无 parser，两边同缺，非回归是 redis 兼容缺口
问题：COPY/TOUCH 在 garnet RespCommand.cs 全枚举、RespServerSession.cs 全分派、KeyAdminCommands.cs 全实现中都不存在，rust command.rs，command_table.rs，key_admin_commands/keys.rs,types.rs 同样不存在（keys.rs:1,376 注释自称对标 COPY 实际无 network_copy）。结论：对标 garnet 无遗漏；对照 redis 则 COPY（REPLACE）、TOUCH 全缺。若补，需新增枚举值（注意写命令持久化编号 append-only，见 RespCommand.cs:24-31 注释）+ DUMP/RESTORE 复用 + TTL 拷贝语义。
rust：wedb/wresp/src/command.rs（缺 Copy/Touch），wedb/wnode/src/resp/parser/command_table.rs（缺条目），wedb/wnode/src/resp/key_admin_commands/keys.rs:1,376
c#：garnet/libs/server/Resp/Parser/RespCommand.cs（缺 COPY/TOUCH），garnet/libs/server/Resp/KeyAdminCommands.cs（缺实现），garnet/libs/server/Resp/RespServerSession.cs:868,885

4. COSCAN 线名是 CUSTOMOBJECTSCAN，rust 文档报错写 COSCAN 误导
问题：C# RespCommand.cs:161 枚举名 COSCAN，但线名以 RespCommandHashLookupData.cs:264 Add CUSTOMOBJECTSCAN 登记，RespServerSession.cs:904 同名分派；rust command_table.rs:39 同样只登记 CUSTOMOBJECTSCAN，正确。但 shared_object_commands.rs:42,52,67,184,186,198,498 把 All=>COSCAN、当命令名、报错 COSCAN 与 C# cmdName 口径不一致，客户端发 COSCAN 必 unknown。建议注释报错统一写 CUSTOMOBJECTSCAN，COSCAN 仅作枚举名。
rust：wedb/wnode/src/resp/objects/shared_object_commands.rs:42,52,67,184,186,198,498，wedb/wnode/src/resp/parser/command_table.rs:39
c#：garnet/libs/server/Resp/Parser/RespCommandHashLookupData.cs:264，garnet/libs/server/Resp/RespServerSession.cs:904，garnet/libs/server/Resp/Parser/RespCommand.cs:161

5. GarnetObjectType.RangeIndex=5 是 rust 自造，C# 无此成员
问题：wval tag.rs:137,174 RangeIndex=5 自称 1:1 对标 GarnetObjectType.cs，实则 C# GarnetObjectType.cs:18-54 只有 Null=0,SortedSet=1,List=2,Hash=3,Set=4,All=0xfb，无 RangeIndex；RangeIndex 在 C# 是独立存储形态非对象类型。现状 TYPE/SCAN 把 rangeindex 当类型串回显（tag.rs:202 as_str），与 C# TYPE 口径分叉。建议注释改写为本仓扩展非 1:1，并写清 TYPE 对 RangeIndex 键回显口径。
rust：wedb/wval/src/tag.rs:137,156,174,202，wedb/wnode/src/resp/array_commands.rs:117-125
c#：garnet/libs/server/Objects/Types/GarnetObjectType.cs:18-54
6. SET EXAT/PXAT 拒绝是忠实转写非缺失
问题：set.rs:387,667 SET 只收 EX/PX/KEEPTTL，EXAT/PXAT 落 syntax error；与 C# BasicCommands.cs:628-640,713-740 同口径。wresp options.rs:189 try_get_expiration_option 含 Exat/Pxat 供 GETEX 用，set.rs 复用同一解析器再收窄，正确。GETEX 侧 EX/PX/EXAT/PXAT/PERSIST 全齐（get.rs:283,304,306 对标 BasicCommands.cs:99-181）。无需补功能。
rust：wedb/wnode/src/resp/basic_commands/set.rs:386,667，wedb/wnode/src/resp/basic_commands/get.rs:264,283,304，wedb/wresp/src/options.rs:189
c#：garnet/libs/server/Resp/BasicCommands.cs:614-740,99-181，garnet/libs/server/Resp/RespEnums.cs:6-14

7. 键级 TTL NX/XX/GT/LT 已齐
状态：C# ExpireOption.cs NX=1,XX=2,GT=4,LT=8,XXGT,XXLT；rust options.rs:79-116 同位，ttl.rs:150-169 TtlOpt nx/xx/gt/lt，keys.rs:192-217 双选项合并仅放行 XXGT/XXLT。EXPIRE/PEXPIRE/EXPIREAT/PEXPIREAT + TTL/PTTL/EXPIRETIME/PEXPIRETIME + PERSIST 接线齐（keys.rs:168,254,278,321 对标 KeyAdminCommands.cs:364,458,495,532）。无遗漏。
rust：wedb/wresp/src/options.rs:77，wedb/wkv/src/ttl.rs:150,430,446，wedb/wnode/src/resp/key_admin_commands/keys.rs:168,254,278,321
c#：garnet/libs/server/ExpireOption.cs，garnet/libs/server/ExpirationWithOption.cs，garnet/libs/server/Resp/KeyAdminCommands.cs:364,458,495,532

8. 字段级 TTL 命令全齐，过期堆在 wcol 内联
状态：HHEXPIRE 系列 + HTTL/HPTTL/HEXPIRETIME/HPEXPIRETIME（hash_commands.rs:634,675,716，command_table.rs:88,100,101,107 对标 HashCommands.cs:705-724）；ZEXPIRE 系列 + ZTTL/ZPTTL/ZEXPIRETIME/ZPEXPIRETIME（sorted_set write.rs:641，slow.rs:190,277 对标 SortedSetCommands.cs:1751 起）；HCOLLECT/ZCOLLECT 齐。HashOperation 与 SortedSetOperation 枚举 1:1 齐。wkv ttl.rs 两层载体（KeyTag::Ttl + wcol expiration_queue）与 C# HashObject.cs:562,589/SortedSetObject.cs:713,730 条件一致。无遗漏。
rust：wedb/wnode/src/resp/objects/hash_commands.rs:634,675,716，wedb/wnode/src/resp/objects/sorted_set_commands/write.rs:641，wedb/wcol/src/hash/hash_object.rs:46,74，wedb/wcol/src/zset/sorted_set_object.rs:49,106，wedb/wkv/src/ttl.rs
c#：garnet/libs/server/Resp/Objects/HashCommands.cs，garnet/libs/server/Resp/Objects/SortedSetCommands.cs:1751，garnet/libs/server/Objects/Hash/HashObject.cs:562,589，garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:713,730

9. 参数面抽查无缺：BITFIELD/BITCOUNT/LCS/SCAN/GEO/VECTOR/RI
BITFIELD OVERFLOW WRAP/SAT/FAIL + BITFIELD_RO（bitmap_commands.rs:570，raw.rs:401 对标 BitmapCommands.cs:416,519），BITCOUNT BYTE/BIT（bitmap_commands.rs:159-187 对标 BitmapCommands.cs:219），BITOP DIFF 为 garnet 自有扩展两边都有（bitmap_commands.rs:325,351 对标 BitmapCommands.cs:373）。LCS LEN/IDX/MINMATCHLEN/WITHMATCHLEN 齐（array_commands.rs:583-606 对标 ArrayCommands.cs:426-475）。SCAN MATCH/COUNT/TYPE 齐（array_commands.rs:62-125）。GEOADD NX/XX/CH + GEODIST/GEOHASH/GEOPOS + GEOSEARCH FROMLONLAT/FROMMEMBER BYRADIUS/BYBOX + GEORADIUS STORE/STOREDIST + GEOSEARCHSTORE 齐（geo:151,346,427,517,579）。Vector VADD REDUCE/CAS/NOQUANT/Q8/BIN/XPREQ8/EF/SETATTR/M + VSIM WITHSCORES/WITHATTRIBS/COUNT/EPSILON/EF/FILTER/FILTER-EF/TRUTH/NOTHREAD + VEMB RAW 齐（vectors.rs:17,526,1253）。RI 九命令全齐，RICount/RILen 为本仓 O(1) 计数非 C# 命令注释已写清（range_index.rs:10,671，command_table.rs:160,165 对标 RespServerSessionRangeIndex.cs:31-600）。
rust：wedb/wnode/src/resp/bitmap/bitmap_commands.rs:159,325,570，wedb/wnode/src/resp/array_commands.rs:54,62,571，wedb/wnode/src/resp/objects/sorted_set_geo_commands.rs:151,346,430,517,579，wedb/wnode/src/resp/vector/resp_server_session_vectors.rs:265，wedb/wnode/src/resp/rangeindex/resp_server_session_range_index.rs:410,479,538,592,615,640,671
c#：garnet/libs/server/Resp/Bitmap/BitmapCommands.cs:131,170,206,267,362,416,519，garnet/libs/server/Resp/ArrayCommands.cs:20,41,76,117,207,221,256,345,416，garnet/libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:31,196,247,297,342,420,486,516,600，garnet/libs/server/Resp/Vector/RespServerSessionVectors.cs:14,519,1248,1434,1469,1502,1551,1620,1672,1739,1808,1839

10. 数据类型枚举 wcol 侧 1:1 齐
HashOperation/HSET/HMSET/HGETALL/HRANDFIELD/HSCAN 全齐；ListOperation LPOP..LPOS 全齐含 LMPOP/BLMPOP COUNT（blocking.rs:36,288 对标 ListCommands.cs:185,851）；SetOperation 全齐含 SINTERCARD LIMIT（set_commands.rs:609）；SortedSet ZADD NX/XX/GT/LT/CH/INCR + ZRANGE BYSCORE/BYLEX/REV/LIMIT/WITHSCORES + ZRANGESTORE/ZDIFF/ZINTER/ZUNION WEIGHTS/AGGREGATE 全齐（write.rs:56,121,256,331,474,516）。HLL PFADD/PFCOUNT/PFMERGE 齐（hyper_log_log_commands.rs:353,409,466 对标 HyperLogLogCommands.cs:23,75,104）。
rust：wedb/wcol/src/hash/hash_object.rs:46，wedb/wcol/src/list/list_object.rs:35，wedb/wcol/src/set/set_object.rs:36，wedb/wcol/src/zset/sorted_set_object.rs:49，wedb/wnode/src/resp/objects/list_commands/blocking.rs:36,288，wedb/wnode/src/resp/objects/set_commands.rs:609，wedb/wnode/src/resp/hyperloglog/hyper_log_log_commands.rs:353,409,466
c#：garnet/libs/server/Objects/Hash/HashObject.cs:29，garnet/libs/server/Objects/List/ListObject.cs:20，garnet/libs/server/Objects/Set/SetObject.cs:20，garnet/libs/server/Objects/SortedSet/SortedSetObject.cs:23，garnet/libs/server/Resp/Objects/HashCommands.cs:83,124,167,214，garnet/libs/server/Resp/Objects/SortedSetCommands.cs:22,59,100,146,208,257,302,353,413,539,588,652,696,764,815,912,1007,1052,1175,1248,1347,1477，garnet/libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs:23,75,104

