review-data 待办：对照 garnet 数据类型 / redis 命令及参数 / TTL

check.js 现状
数据面无真符号缺失与重复定义。全面深度审查 wresp、wval、wcol、wkv、wnode/src/resp，对照 garnet/libs/server/Resp、garnet/libs/server/Objects、ExpireOption 等，梳理出 14 项待办与对标确认点，涵盖慢路径降级断流、TTL 单位与参数偏差、大集合升阶重命名与慢路径判定、词元大小写口径及枚举对齐。

1. 慢路径降级分派大面积断流（String / KeyAdmin / Bitmap 命令返回 ASYNC_REQUIRED）
问题：wedb/wnode/src/resp/garnet_api/raw.rs 在 SET 族、GET/MGET/MSET/MSETNX、INCR 族、GETEX/GETDEL、GETRANGE/SUBSTR、STRLEN、APPEND、EXISTS、TTL/PTTL/EXPIRE 族、RENAME/RENAMENX、DUMP/RESTORE、SETBIT/GETBIT/BITCOUNT/BITPOS/BITFIELD 等返回 Ok(false)（如冷数据磁盘缺页、环形缓冲区等待、BfTree 升阶键、TTL 异步裁决）时，将上下文包装为 SlowWait 派发给 slow.rs:exec_slow_impl。但 slow.rs 的 match cmd 仅接入了部分 Object 集合类命令，对上述全部 String、KeyAdmin、Bitmap 命令完全未接入，落入通配符 _ => write_error_raw(&mut output, RESP_ERR_ASYNC_REQUIRED)，向客户端直接报错 -ERR command requires asynchronous completion。
rust：wedb/wnode/src/resp/garnet_api/slow.rs fn exec_slow_impl；wedb/wnode/src/resp/garnet_api/raw.rs fn raw_exec_cmd
对应 C#：garnet/libs/server/Storage/Session/MainStore/MainStoreOps.cs fn CompletePending；garnet/libs/server/Resp/RespServerSession.cs fn ProcessMessage
动作：在 slow.rs 中补齐 String 读写、KeyAdmin、Bitmap 命令的慢路径异步分支，或者将冷页和重试在 storage 读写内部闭环，消除对客抛出的 ASYNC_REQUIRED 异常。

2. RESTORE 命令 TTL 单位分叉与参数缺失
问题：wedb/wnode/src/resp/key_admin_commands/types.rs fn network_restore 仅接收 3 个参数（key, ttl, serialized-value），且将 expiry 视为秒换算为 ticks（expire_after_to_ticks(now_ticks(), expiry)）。标准 Redis 的 RESTORE ttl 参数为毫秒，且支持 [REPLACE] [ABSTTL] [IDLETIME seconds] [FREQ frequency]。C# KeyAdminCommands.cs:NetworkRESTORE:423 同样取 TimeSpan.FromSeconds(expiry) 且仅支持 3 参数（Garnet 本身偏离 Redis 规范）。若客户端按 Redis 规范传入毫秒（如 5000ms），两边均会放大 1000 倍（变为 5000 秒）；且缺少 REPLACE 修饰符导致覆盖已存在键时只能报错 BUSYKEY。
rust：wedb/wnode/src/resp/key_admin_commands/types.rs fn network_restore
对应 C#：garnet/libs/server/Resp/KeyAdminCommands.cs fn NetworkRESTORE
动作：在文档与代码注释中明确标清与 Redis 规范差异（对标 Garnet 维持秒与 3 参数约束）；后续可根据兼容需求支持毫秒判定及可选 REPLACE 参数。

3. RENAME / RENAMENX 大集合升阶键（Meta/BfTree）降级慢路径必报错
问题：wedb/wnode/src/resp/key_admin_commands/keys.rs:425,480 fn network_rename / fn network_renamenx 当探测到 KeyTag::Meta（BfTree 升阶集合或 RangeIndex）时返回 Ok(false)，交由慢路径异步排空重试；但 slow.rs 完全无 Command::Rename / Command::Renamenx 臂，直接落入未知命令并报错 ASYNC_REQUIRED，导致对任何大集合执行 RENAME 必报异常失败。
rust：wedb/wnode/src/resp/key_admin_commands/keys.rs fn network_rename fn network_renamenx；wedb/wnode/src/resp/garnet_api/slow.rs fn exec_slow_impl
对应 C#：garnet/libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs fn RENAME；garnet/libs/server/Resp/KeyAdminCommands.cs fn NetworkRENAME
动作：在 slow.rs 补充 Rename/Renamenx 的慢路径执行；或者针对 Meta/BfTree 句柄实现原地换键（更新元数据键前缀与树根映射），避免降级到不存在的慢路径。

4. MSETNX 慢路径存活判定漏探 Meta 域产生双域脏数据
问题：wedb/wnode/src/resp/garnet_api/slow.rs:115 C::Msetnx 慢路径分支在判断键是否存在时，仅探查了 KeyTag::String 与 KeyTag::ObjectEnvelope 两域，漏探了 KeyTag::Meta。当大集合升阶为 BfTree 且信封被删除后，若该冷键触发慢路径，慢路径误判其不存在，执行 upsert_string 写入 String 值回 1，导致同一个键在存储中同时存在 String 域和 Meta 域，且原 BfTree 成为无主孤儿。
rust：wedb/wnode/src/resp/garnet_api/slow.rs fn exec_slow_impl；wedb/wnode/src/storage/session/common/ttl_sync.rs fn probe_alive_domain_with_prefix
对应 C#：garnet/libs/server/Storage/Session/MainStore/MainStoreOps.cs fn MSET_Conditional
动作：将 slow.rs 的 Msetnx 判定统一改调三域异步单点 StorageSession::exists，覆盖 String/ObjectEnvelope/Meta 三域，消除漏域导致的脏键状态。

5. ZRANK / ZREVRANK WITHSCORE 多余参数未报错
问题：wedb/wnode/src/resp/objects/sorted_set_commands/read.rs:222-237 fn zrank 在参数数量大于 3 时，仅判断第三个参数是否为 WITHSCORE，多余的第 4 及后续参数被静默忽略。C# SortedSetCommands.cs:709 同样仅在 Count == 3 时判断 WITHSCORE。但 Redis 标准对于多余参数要求报错 syntax error。
rust：wedb/wnode/src/resp/objects/sorted_set_commands/read.rs fn zrank
对应 C#：garnet/libs/server/Resp/Objects/SortedSetCommands.cs fn ZRank
动作：加入参数个数严格校验：仅允许 2 个参数或 3 个参数（含 WITHSCORE），大于 3 个参数直接回写 RESP_ERR_GENERIC_SYNTAX_ERROR。

6. LPOS 选项匹配大小写限制（全大写/全小写）
问题：wedb/wcol/src/list/list_object_impl.rs:509-540 在解析 LPOS 的 RANK/COUNT/MAXLEN 选项时，采用精准字节匹配仅支持全大写（b"RANK"）或全小写（b"rank"），客户端若传入混合大小写（如 "Rank"）会直接报错 syntax error。C# ListObject.cs:ReadListPositionInput 同样使用 SequenceEqual 匹配双常数，这是忠实转写 Garnet，但与 Redis 选项完全不区分大小写存在微小差异。
rust：wedb/wcol/src/list/list_object_impl.rs fn operate
对应 C#：garnet/libs/server/Objects/List/ListObject.cs fn ReadListPositionInput
动作：维持与 Garnet 一致并在注释中明确标明该双形态行为；或按 Redis 标准改用 eq_ignore_ascii_case 提高通用客户端兼容度。

7. SCAN TYPE 精确双形态比较与混合大小写回显分叉
问题：wedb/wnode/src/resp/array_commands.rs:113-131 fn parse_scan_filter 对 TYPE 值的过滤比较使用 eq_ignore_ascii_case，允许客户端传入任意混合大小写（如 "ZsEt"）并正常返回匹配集合。而 C# ArrayKeyIterationFunctions.cs:51-86 采用 SequenceEqual 精确比对全大写或全小写（CmdStrings.ZSET / CmdStrings.zset），混合大小写落入未知类型分支并直接回空键列表与游标 0。
rust：wedb/wnode/src/resp/array_commands.rs fn parse_scan_filter；wedb/wnode/src/resp/garnet_api/slow.rs fn exec_slow_impl
对应 C#：garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs fn DbScan；garnet/libs/server/Resp/CmdStrings.cs
动作：若严格对标 Garnet，应将 eq_ignore_ascii_case 改为全大写/全小写精确双形态比较，其余混合大小写置 type_unknown = true 短路回空；若保留混合大小写宽容度，需在注释中改写说明。

8. DELIFEXPIM 内部命令外泄到 RESP 枚举但无接线
问题：wedb/wresp/src/command.rs:29 Delifexpim=9 与 C# RespCommand.cs:39 对齐，但 command_table.rs 无 DELIFEXPIM 注册条目，raw.rs 和 slow.rs 无执行分派臂。C# 侧 DELIFEXPIM 实为 UnifiedStore RMW 内部条目（RMWMethods.cs:27,67），无 RESP 外部接线。
rust：wedb/wresp/src/command.rs Delifexpim；wedb/wnode/src/resp/parser/command_table.rs；wedb/wnode/src/resp/garnet_api/raw.rs
对应 C#：garnet/libs/server/Resp/Parser/RespCommand.cs DELIFEXPIM；garnet/libs/server/Storage/Functions/UnifiedStore/RMWMethods.cs fn InternalRMW
动作：在 command.rs 注释中明确标明 Delifexpim 为内部存储保留命令，对外协议不接线，避免误当成遗漏的公开命令。

9. MIGRATE 单键命令条目有名无实
问题：wedb/wnode/src/resp/parser/command_table.rs:132 注册了 MIGRATE，但 raw.rs/slow.rs 无分派臂；resp_server_session.rs:1707 将其判给 cluster 通道，而 cluster 侧仅支持槽迁移（ClusterMigrate）。客户端若发送 Redis 标准单键 MIGRATE 命令（支持 COPY/REPLACE/KEYS）必定报错。C# RespCommand.cs:51 同样仅为集群槽迁移服务。
rust：wedb/wnode/src/resp/parser/command_table.rs；wedb/wnode/src/resp/resp_server_session.rs fn process_command
对应 C#：garnet/libs/server/Resp/Parser/RespCommand.cs MIGRATE；garnet/libs/cluster/Session/MigrateCommand.cs fn MigrateSlot
动作：在 parser 和会话层注释标清 MIGRATE 仅用于集群槽迁移，对非集群或单键迁移拦截并返回清晰的报错信息。

10. GarnetObjectType.RangeIndex 自造枚举与 C# 类型映射
问题：wedb/wval/src/tag.rs:137,174 将 RangeIndex = 5 列入 GarnetObjectType 枚举，TYPE 命令对其回显 "rangeindex"。C# GarnetObjectType.cs:18-54 仅定义了 Null(0)、SortedSet(1)、List(2)、Hash(3)、Set(4)、All(0xfb)，无 RangeIndex。C# 的 RangeIndex 是底层独立存储形态（RangeIndexRecordType），但在 HandleType（ReadMethods.cs:152）中特判并同样回显 "rangeindex"。
rust：wedb/wval/src/tag.rs enum GarnetObjectType；wedb/wnode/src/resp/array_commands.rs fn network_type
对应 C#：garnet/libs/server/Objects/Types/GarnetObjectType.cs；garnet/libs/server/Storage/Functions/UnifiedStore/ReadMethods.cs fn HandleType
动作：TYPE 命令回显 "rangeindex" 与 C# 行为一致，但应在 tag.rs 注释中明确说明 RangeIndex 是 Rust 统一对象标签的内部扩展，非 C# GarnetObjectType 1:1 成员。

11. CUSTOMOBJECTSCAN 协议线名与报错输出 COSCAN 歧义
问题：C# RespCommand.cs:161 枚举名为 COSCAN，但协议线名以 RespCommandHashLookupData.cs:264 注册为 CUSTOMOBJECTSCAN。Rust command_table.rs:39 正确注册了线名 CUSTOMOBJECTSCAN，但在 shared_object_commands.rs 多处错误打印中使用 "COSCAN"（如报错 ERR unknown command 'COSCAN'）。客户端如果按报错重试 COSCAN 必定失败。
rust：wedb/wnode/src/resp/objects/shared_object_commands.rs fn parse_custom_object_scan；wedb/wnode/src/resp/parser/command_table.rs
对应 C#：garnet/libs/server/Resp/Parser/RespCommandHashLookupData.cs；garnet/libs/server/Resp/RespServerSession.cs
动作：统一将 shared_object_commands.rs 中的报错字符串改为 CUSTOMOBJECTSCAN，与协议注册线名严格一致。

12. 字段级 TTL 与键级 TTL 选项组合支持差异
问题：键级 TTL（EXPIRE/PEXPIRE）在 C# KeyAdminCommands.cs:400 与 Rust keys.rs:192-217 均支持双选项组合 NX/XX 与 GT/LT（放行 XXGT 和 XXLT）。而字段级 TTL（HEXPIRE/ZEXPIRE）在 C# HashCommands.cs:603、SortedSetCommands.cs:1772 以及 Rust hash_commands.rs:634、write.rs:641 中仅支持单个选项，不支持复合选项。两边行为虽保持一致，但该差异属于 Redis 7.4 与 Garnet 的重要语义约束。
rust：wedb/wnode/src/resp/key_admin_commands/keys.rs fn expire_with_options；wedb/wnode/src/resp/objects/hash_commands.rs fn hash_expire；wedb/wnode/src/resp/objects/sorted_set_commands/write.rs fn zadd_expire
对应 C#：garnet/libs/server/Resp/KeyAdminCommands.cs fn NetworkEXPIRE；garnet/libs/server/Resp/Objects/HashCommands.cs fn HashExpire；garnet/libs/server/Resp/Objects/SortedSetCommands.cs fn SetExpiration
动作：在 wresp/src/options.rs 中添加详细文档注释，阐明字段级 TTL 仅单选、键级 TTL 允许双选的设计约束，防止后续重构将字段级 TTL 误放开复合选项。

13. 分层集合成员级 TTL 树内删除堆积墓碑连跑导致递归溢栈
问题：分层集合的成员级 TTL 出账（member_expire / member_persist / member_ttl_probe / collect_expired_members）仍直接向 BfTree 写入逐成员删除记录，导致树内累积连续墓碑；BfTree 的 ScanIter::next 在遍历游标后的每一条墓碑时压一帧递归调用，当墓碑数超过阈值时导致栈溢出使进程崩溃。
rust：wedb/wnode/src/resp/objects/tiered_collection_ops.rs fn tree_del fn member_expire_arm fn collect_expired_members
对应 C#：garnet/libs/server/Objects/Hash/HashObject.cs fn SetExpiration；garnet/libs/server/Objects/SortedSet/SortedSetObject.cs fn SetExpiration
动作：参照集合重写通道将 TTL 出账收敛为「物化求值 + 整值重灌」，使分层树内不留成员级删除墓碑，消除递归栈溢出隐患。

14. now_stopwatch_ticks 取时源落在实时墙钟导致耗时与慢日志失真
问题：wedb/wbase/src/time.rs:42-44 now_stopwatch_ticks 使用 now_nanos()，底层取自 SystemTime::now()（实时域，可被 NTP 回拨），而其注释自称对标 C# Stopwatch.GetTimestamp（单调域）。会话层执行命令耗时统计与慢日志判定使用 now_stopwatch_ticks 做减法并强转 u64，若发生墙钟回拨，将回绕出巨大耗时值污染延迟直方图并漏记慢日志。
rust：wedb/wbase/src/time.rs fn now_stopwatch_ticks；wedb/wnode/src/resp/resp_server_session.rs fn process_message
对应 C#：garnet/libs/server/Metrics/Latency/LatencyMetricsEntrySession.cs；System.Diagnostics.Stopwatch.GetTimestamp
动作：将 now_stopwatch_ticks 改为基于 std::time::Instant（单调域）换算 100ns 刻度，与实时域 now_ticks 彻底物理解耦；耗时差值统一使用 checked/saturating 减法。
