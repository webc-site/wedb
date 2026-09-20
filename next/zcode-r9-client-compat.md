轮9 视角: 真实客户端兼容(主流 Redis 客户端可见差异)

方法: 以 Jedis/Lettuce/go-redis/node-redis/redis-py/StackExchange.Redis/redis-cli 的连接初始化序列(PING/HELLO/CLIENT SETINFO/SELECT/CONFIG GET/INFO/AUTH)与常用操作为尺,推演 rust 侧应答形态,对照 C# garnet。只立 rust 相对 C# garnet 的客户端可见新差异。

已立不重复(历史轮,不复述): CLIENT LIST 镜像视图静态化、CLIENT KILL 对阻塞挂起无效力(r4-client);单命令应答峰值无界、长度头回显字节(r3-protocol);HELLO 认证预门(r3-security,task/done/hello-auth-can-authenticate-gate);EVAL 永久降 RESP2、Lua SELECT 串库(r4-lua);阻塞族经纪会话钉死 ns0(r7-redteam);SETEX/DECRBY/PFMERGE 命令臂(r8-sample-a)。

立项

1. TIME 微秒字段未按 6 位补零,应答帧字节与 C# 不一致
客户端假设: TIME 应答为 [秒, 微秒] 双 bulk string,C# garnet 与真 redis 的微秒字段恒为 6 位零填充十进制串;redis-cli TIME 原样回显,严格按定长/字符串比较的消费方可见差异,按整数解析的消费方无感。
rust: wnode/src/resp/resp_server_session/core.rs:812 process_other_commands 的 Time 臂——usecs=(now_nanos%1e9)/1e3 后经 itoa Buffer::format 直接成帧,无零填充;微秒值不足 100000 时正文与 `$` 长度头同步变短(例 微秒=4210 出 `$4\r\n4210\r\n`)。
C#: libs/server/Resp/BasicCommands.cs:1518 NetworkTIME——`utcTime.ToString("ffffff")` 恒 6 位零填充,`uSeconds.Length` 恒 6(例 `$6\r\n004210\r\n`)。
判定: rust 独有偏差,立项。对齐 C# 须改 6 位补零格式化;秒字段两侧均为不填充十进制,一致。

2. ASKING 补了 C# 没有的参数校验,非标准输入应答文本不同
客户端假设: ASKING 由集群客户端在收到 -ASK 重定向后以裸命令(0 参)随原命令重发,期望 `+OK`;主流客户端恒不传参。
rust: wnode/src/resp/basic_commands/mod.rs:106 network_asking——check_arg_count 限定 0 参,`ASKING x` 回 `ERR wrong number of arguments for 'ASKING' command`;并无条件置 session_asking=2。
C#: libs/server/Resp/BasicCommands.cs:1005 NetworkASKING——无参数校验,任意参数恒 `+OK`;且仅 storeWrapper.serverOptions.EnableCluster 时置 SessionAsking=2。
判定: rust 独有偏差(弱),立项登记。可见面仅非标准带参输入的错误文本;「standalone 下 rust 置位 asking」无消费面(asking 仅集群槽位门读取,session 侧逐命令衰减 core.rs:682 与 C# RespServerSession.cs:736 一致),行为等价。

排除的疑似项(查证后与 C# 一致,防下轮复报)
PING 带消息/多参: C# 分派 libs/server/Resp/RespServerSession.cs:855 `Count == 0 ? NetworkPING() : NetworkArrayPING()`,ArrayCommands.cs:398 NetworkArrayPING 同样 0..=1 参校验+bulk 回显消息;RESP2 订阅态仅裸 PING 出 SUSCRIBE_PONG 双元素数组。rust wnode/src/resp/basic_commands/mod.rs:91 network_ping 逐臂一致。
SELECT 集群门: C# ArrayCommands.cs:130 集群下禁非 0 库,rust 无此门——SKILL 明定的库级定槽架构偏差(集群以 ns->db 定槽),非转写遗漏,不立项。
CLIENT INFO 尾部 pubsub-dropped 字段: rust 扩展(wnode/src/resp/resp_server_session/core.rs:1127 write_client_info_state),代码注释已登记背压差异,非新发现。

无增量确认
握手序列与状态连续性
HELLO 参数臂(protover 严格 i32、2..=3 界、AUTH/SETNAME、arity<=6、syntax-error-option 文案): wnode/src/resp/basic_commands/mod.rs:496 network_hello 对位 libs/server/Resp/BasicCommands.cs:1444 NetworkHELLO。
HELLO 认证先于协议/名字切换,认证失败整命令拒绝(WRONGPASS),免认证档 HELLO AUTH 亦拒: wnode/src/resp/resp_server_session/auth.rs:375 process_hello_command_state 对位 BasicCommands.cs:1774 ProcessHelloCommand;authenticate_user 免认证档返回 false 并落 default 句柄与 C# RespServerSession.cs:425 AuthenticateUser 的 `CanAuthenticate ? success : false` 同构。
HELLO 应答 8 字段序 server/version/garnet_version/proto/id/mode/role/modules,RESP2 双倍数组: auth.rs:443 write_map_len 对位 BasicCommands.cs:1829;version="7.4.3" 单源 core.rs:53。
协议版本切换后订阅态连续: RESP2 订阅门 `isSubscriptionSession && respProtocolVersion == 2 && !IsAllowedInSubscriptionMode` 两侧同式(wnode/src/resp/resp_server_session/core.rs:582 对位 RespServerSession.cs:657);放行集 C# 七命令,rust 多 SUNSUBSCRIBE(上游缺口补全,代码注释已登记);RESP3 订阅态任意命令放行两侧一致;HELLO->CONFIG->SUBSCRIBE 与 SUBSCRIBE->HELLO 降级序列应答逐帧同形。
错误前缀与错误分类
NOAUTH/NOPERM/WRONGTYPE(含 HLL 变体)/WRONGPASS 两变体/EXECABORT/NOSCRIPT 两枚(脚本门 ERR 文案与 EVALSHA miss NOSCRIPT 帧)/BUSYKEY/CLUSTERDOWN/TRYAGAIN: wresp/src/cmd_strings.rs 常量族对位 libs/server/Resp/CmdStrings.cs:199-318 逐条。
事务族错误(NESTED MULTI/EXEC without MULTI/DISCARD without MULTI/WATCH inside MULTI/SELECT-SWAPDB in txn): wnode/src/resp/txn_resp_commands.rs 对位 C# 同名常量。
未知命令 `ERR unknown command`+specific_error(arity/unknown subcommand 三档,CLUSTER/LATENCY 带 HELP 提示,BITOP 语法错): wnode/src/resp/parser/resp_command.rs:549/626 对位 libs/server/Resp/Parser/RespCommand.cs:1335;未知子命令净化截断(rust 128 帽+换行清洗,C# ASCII 直拼)为文案加固,前缀不变。
MOVED/ASK 帧格式 `-<kind> <slot> <ep>:<port>` 单点: wresp/src/cmd_strings.rs:867 write_redirect_error 对位 libs/cluster/Session/SlotVerification/RespClusterSlotVerify.cs 组串。
协议违规三文案+整数溢出文案与 `ERR Protocol Error: {msg}`+断连: wnode/src/resp/resp_server_session/parse.rs violation_* 与 core.rs:427 write_protocol_error 对位 RespParsingException.cs 与 RespServerSession.cs:522-537。
内联命令(非 `*` 帧头)静默跳行不执行: wnode/src/resp/parser/resp_command.rs:507 attempt_skip_line 对位 RespCommand.cs AttemptSkipLine,两侧均不回错。
bulk/null/多批量/整数形态
null 与 null-array 版本分派单点($-1/`_`、*-1/`_`): wresp/src/ext.rs:155/163 write_resp_null_ver/write_resp_null_array_ver 对位 libs/server/Resp/RespServerSessionOutput.cs WriteNull/WriteNullArray;GET miss nilResp 版本感知(libs/server/Storage/Functions/FunctionsState.cs:40)。
EXEC WATCH 脏读出 null 数组、EXECABORT 中止: wnode/src/resp/txn_resp_commands.rs:124/175 对位 C# 同面。
空串/空数组常量($0\r\n\r\n、*0\r\n、:0/:1/:-1/:-2): wresp/src/cmd_strings.rs:13-27 与 CmdStrings.cs:181-186 逐字节一致。
`*0` 空数组帧两侧同为协议违例断连(命令名位读 `$` 头遇 `*` 抛 UnexpectedToken): wresp read 单点对位 RespCommand.cs GetCommand:1267。
阻塞族与连接池
BLPOP/BRPOP/BLMOVE/BRPOPLPUSH/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP 参数臂、timeout 词法(严格 f64、负值、int.MaxValue/1000 上限钳制)逐臂同: wnode/src/session_parse_state_extensions.rs:151 try_get_timeout_bytes 对位 libs/server/SessionParseStateExtensions.cs:813 TryGetTimeout;wnode/src/resp/objects/list_commands/blocking.rs 对位 libs/server/Resp/Objects/ListCommands.cs。
timeout 0=永久、UNBLOCKED/WRONGTYPE/nil(版本分派)三态、应答帧型(六命令尾 switch): wnode/src/resp/objects/list_commands/blocking.rs:374 write_collection_item_result 与 wcol/src/itembroker/item_broker_face.rs:161 BlockedWait::resolve 对位 libs/server/Objects/ItemBroker/CollectionItemBroker.cs:126 GetCollectionItemAsync(TimeSpan.FromMilliseconds(-1))。
客户端超时重连残留: 阻塞挂起期间连接 EOF 不可感知(泵 parked 于 blocked.resolve,不读套接字)、死观察者滞留 keys_to_observers 由 CollectionUpdated 事件或 5 分钟周期清理回收、item 到达后写失败断连——与 C# 网络线程 BlockingWait 整段语义同构(C# 同样在 BlockingWait 期间不感知断连,observer 残留同由 CleanKeysToObservers 回收);除已立的 KILL 无效力外无第二处两侧分歧。wnode/src/resp/resp_server_session/core.rs:399 dispose 与 libs/server/Resp/RespServerSession.cs Dispose 对应;wnode/src/net/handler/mod.rs:95 dispose 注销先于会话释放,对位 C# DisposeMessageConsumer 序。
CLIENT 族辅助面
CLIENT SETINFO(LIB-NAME/LIB-VER,33..=126 属性值校验,非法回 GenericErrInvalidClientAttr)、SETNAME/GETNAME(空串清名,nil 回包)、UNBLOCK(TIMEOUT/ERROR,未阻塞回 0)、KILL 老式 ip:port(无匹配 NO_SUCH_CLIENT,命中 +OK)与新式过滤器(重复过滤器/未知过滤器/SKIPME 默认 true/SLAVE->REPLICA/MAXAGE 严格大于): wnode/src/resp/client_commands.rs 全文件对位 libs/server/Resp/ClientCommands.cs;CLIENT INFO/LIST 字段序 id addr laddr [name] age [user] flags db resp lib-name lib-ver 两侧一致(wnode/src/servers/consumer_registry.rs:300 对位 BasicCommands.cs:1950)。
CLIENT TRACKING、CLIENT REPLY、CLIENT NO-EVICT、RESET: C# 无(garnet 全仓无 tracking/reply/no-evict/reset 命令面),rust 亦无,两侧均回 `ERR unknown command`,主流客户端降级路径一致。两侧都缺,无增量。
CLIENT SETNAME/INFO 校验对二进制安全: try_get_client_name_bytes 的 UTF-8+可打印域与 C# TryGetClientName(GetString 失败即拒)同判,wnode/src/session_parse_state_extensions.rs 对位 SessionParseStateExtensions.cs:103。
发布订阅
SUBSCRIBE/SSUBSCRIBE/PSUBSCRIBE/UNSUBSCRIBE/SUNSUBSCRIBE/PUNSUBSCRIBE/PUBLISH/SPUBLISH/PUBSUB CHANNELS/NUMSUB/NUMPAT 帧型: 五前缀常量、numActiveChannels 计数语义(重复订阅不增、退订减至 0 出订阅态)、无参退订全量列举+空集 nil 名、PUNSUBSCRIBE 空集硬编码 :0、SPUBLISH/SSUBSCRIBE 无集群门回 CLUSTER_DISABLED、--pubsub 关闭禁用文案、自发布自接收帧先于计数应答: wpubsub/src/session_commands.rs 全文件对位 libs/server/Resp/PubSubCommands.cs。
推送帧编码 RESP2 `*`/RESP3 `>` 按当前会话版本逐次分派: session_commands.rs:548 drain_pubsub_frames 对位 C# WritePushLength;推送与命令应答的交错位置为批边界差异,RESP 语义允许任意交错,无兼容影响。
断连清订(dispose->remove_subscription)、重连后客户端自重订、服务端无恢复态: core.rs:407 对位 C# Dispose 尾 RemoveSubscription。
redis-cli 交互面
引号解析后的二进制键(含内嵌 \r\n)经 bulk 长度头解析二进制安全,命令臂无 UTF-8 强校验: wnode/src/resp/resp_server_session/parse.rs get_command_range 与 wresp::read 长度头单点,对位 C# GetCommand/SessionParseState.Read。
HELLO SETNAME/CLIENT SETNAME 非 ASCII 拒绝、CONFIG GET/INFO/ROLE/COMMAND(无参列全目,带参回 unknown subcommand,redis-cli 启动探针 COMMAND DOCS 两侧同为降级路径)。
TIME arity 校验与错误文案一致(仅微秒补零差异,见立项 1)。
认证握手
AUTH 免认证档文案 `ERR Client sent AUTH, but configured authenticator does not accept passwords`、WRONGPASS 按用户名有无分派、AUTH [user] pass 参数臂: wnode/src/resp/resp_server_session/auth.rs:262 network_auth_session 对位 BasicCommands.cs:1536 NetworkAUTH。
未认证会话 ACL 门回 NOAUTH/已认证无权限回 NOPERM/脚本窗 NOSCRIPT 及拒绝计数: core.rs:573-653 对位 RespServerSession.cs:651-715;NoAuth 豁免域 [AUTH..QUIT] 枚举区间 wresp/src/catalog/mod.rs:291 is_no_auth 对位 RespCommand.cs:701(rust SUNSUBSCRIBE=370 置于 QUIT 之后,区间未被扩)。

视角结论:有增量
