轮8 随机抽样精读 A:命令臂域 30 函数逐一对 C#

方法
在 wedb/wnode/src/resp/ 枚举命令执行函数,按命令族均匀抽 30 个(string/hash/list/set/zset/bitmap/hll/geo/keys/txn/pubsub/array),对每个抽中函数在 garnet/libs/server/Resp/ 找 C# 对位方法逐行对照:参数处理顺序、边界判定、回复构造、副作用顺序、错误分支。只报同函数内行为语义可差的点。双侧路径均为相对路径。

差异项(排前)

1 有差异 network_setex_impl(wedb/wnode/src/resp/basic_commands/set.rs:325)
C# 对位 libs/server/Resp/BasicCommands.cs:NetworkSETEX(:533)
过期参数解析宽度不同:C# parseState.TryGetInt → int32(ParseUtils.TryReadInt → TryReadInt32Safe,超 int32 值域直接失败);rust parse_setex_args 用 strict_i64(wbase/src/num.rs:112,全 i64 值域)。
SETEX k 2147483648 v:C# 回 "ERR value is not an integer";rust 解析成功、过期换算经 try_get_absolute_expiry_ticks 防溢出后成功落 TTL,回 +OK。
应答与副作用双差(C# 不写键,rust 写键)。PSETEX 同体,同受累。

2 有差异 network_setexnx(wedb/wnode/src/resp/basic_commands/set.rs:396,parse_set_options :738)
C# 对位 libs/server/Resp/BasicCommands.cs:NetworkSETEXNX(:605,:653)
同根差异:EX/PX 的数值 C# TryGetInt(int32),rust parse_set_options strict_i64。
SET k v EX 2147483648:C# 回 not-integer;rust 回 +OK 并落 TTL。
其余逐项一致:选项大小写不敏感重试、重复过期/存在选项 syntax error、EX/PX 缺值 syntax error、值非正 INVALIDEXP、KEEPTTL+NX 派发 SETEXNX、未知选项 UNK_CMD,次序与文案全对位。

3 有差异 network_increment(wedb/wnode/src/resp/basic_commands/incr.rs:115,parse_incr_args :65)
C# 对位 libs/server/Resp/BasicCommands.cs:NetworkIncrement(:852) + libs/server/Storage/Functions/MainStore/RMWMethods.cs:559(DECRBY 臂)+ PrivateMethods.cs:TryInPlaceUpdateNumber(:396)
DECRBY 取负口径不同:C# `-decrBy` 在 unchecked 上下文(全仓无 CheckForOverflowUnderflow)对 i64::MIN 回绕仍得 i64::MIN;rust parse_incr_args 用 sign().saturating_mul 饱和为 +i64::MAX。
DECRBY k -9223372036854775808、旧值 0:C# 经分支无溢出检测(0+MIN=MIN 合法)写回 -9223372036854775808 回 :1;rust checked_add(0, i64::MAX)=+9223372036854775807 写回。终值符号翻转级发散。旧值>0 时双侧都报 not-integer,一致。
另核对一致:INCR/DECR 多余实参仅校验不消费、溢出与非整数旧值共用 not-integer 且不落写。

4 有差异 hyper_log_log_merge(wedb/wnode/src/resp/hyperloglog/hyper_log_log_commands.rs:503;慢路径 :338)
C# 对位 libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs:HyperLogLogMerge(:96)+ libs/server/Storage/Session/MainStore/HyperLogLogOps.cs:HyperLogLogMerge(:191)
错误路径副作用原子性不同:C# 逐源 GET 后逐源 SET_Conditional(dest)即时合并,任一后续源 WRONGTYPE/非法载荷时 finally 仍 Commit——此前源的合并已持久落盘(dest 缺失则被建键),客户端收到 WRONGTYPE 错误但 dest 已部分变更;rust 先装载全部源、任一源错误即零写返回,dest 保持原状。
PFMERGE dest good_src bad_src:C# dest=good_src 合并结果+错误应答;rust dest 不变+错误应答。
其余核对一致:零源不触达不建键直答 +OK、源缺失跳过、dest TTL 经 RMW 保留。

一致项(按族)

string 族

5 一致 network_set(wedb/wnode/src/resp/basic_commands/set.rs:171)
C# 对位 libs/server/Resp/BasicCommands.cs:NetworkSET(:387)
Count<2 arity 门、>2 转 SETEXNX、+OK 恒答均对位。RI 键 WRONGTYPE 拒写为在册刻意偏差(代码注释声明,C# 对 RI 桩走 promote+DELETE 重写臂);对象键盲写覆写关联 r6-del 在途票,不复述。

6 一致 network_setnx(wedb/wnode/src/resp/basic_commands/set.rs:366)
C# 对位 libs/server/Resp/BasicCommands.cs:NetworkSETNX(:581)
双侧对象键/RI 键上 SET_Conditional WRONGTYPE 均不产生删除重试,回 :0,零副作用对位;存活键 :0、缺失键写入回 :1。

7 一致 network_set_conditional(wedb/wnode/src/resp/basic_commands/set.rs:453)
C# 对位 libs/server/Resp/BasicCommands.cs:NetworkSET_Conditional(:772)
非 GET 臂:SETEXXX XX 族失败 nil、SETEXNX 成功 OK/失败 nil 翻转、SETKEEPTTL 无条件 OK 全对位;对象键 WRONGTYPE→DELETE 重试臂(XX 回 nil、NX 回 OK)净效果一致;KEEPTTL 旧 TTL 回填=C# 记录 Expiration 随 RMW 保留。GET 臂 WRONGTYPE 错误、旧值/nil 对位。差异仅继承第 2 条解析层 i64 口径。

8 一致 network_get_range(wedb/wnode/src/resp/basic_commands/get.rs:215,normalize_range :165)
C# 对位 libs/server/Resp/BasicCommands.cs:NetworkGetRange(:488)+ libs/server/Storage/Functions/MainStore/PrivateMethods.cs:NormalizeRange(:368)
strict_i32 对位 TryGetInt;PrivateMethods NormalizeRange 逐行复刻(含 :387 行 end==len 折 0 怪癖、start>=0 分支不折叠的不对称),缺失键回空 bulk string,found/notfound 计数对位。

hash 族

9 一致 hash_set_by_command(wedb/wnode/src/resp/objects/hash_commands/write.rs:54)
C# 对位 libs/server/Resp/Objects/HashCommands.cs:HashSet(:24)
HSET/HMSET 偶参判定(Count==1 || %2!=1)、HSETNX Count==3、HMSET +OK/其余 result1、NOTFOUND 以空对象执行后正常应答,全对位(rmw 骨架 Missing 分支以空对象执行并透写负载)。

10 一致 hash_get_multiple(wedb/wnode/src/resp/objects/hash_commands/read.rs:75)
C# 对位 libs/server/Resp/Objects/HashCommands.cs:HashGetMultiple(:167)
Count<2 门、NOTFOUND 逐字段 null 数组(write_null_array 逐元素 write_resp_null_ver 双协议)、WRONGTYPE 错误行,对位。

list 族

11 一致 list_push_by_op(wedb/wnode/src/resp/objects/list_commands/write.rs:55)
C# 对位 libs/server/Resp/Objects/ListCommands.cs:ListPush(:21)
Count<2 门、result1 整数应答、LPUSHX/RPUSHX 缺失键 :0 不物化(C# NeedToCreate=false 同),对位。

12 一致 list_move_core(wedb/wnode/src/resp/objects/list_commands/write.rs:350)
C# 对位 libs/server/Resp/Objects/ListCommands.cs:ListMove(:706,:797)+ libs/server/Storage/Session/ObjectStore/ListOps.cs:ListMove(:212)
同键同向/单元素 peek no-op(含免空键丢 TTL 注记)、异键先探目标类型防丢元素、源缺失/空列表 null、旋转一次落库,逐项对位。

13 一致 list_blocking_pop(wedb/wnode/src/resp/objects/list_commands/blocking.rs:67)
C# 对位 libs/server/Resp/Objects/ListCommands.cs:ListBlockingPop(:270)
timeout 解析(try_get_timeout_bytes 对位 TryGetTimeout)、逐键 FIFO 试取、命中 [key,item]、未取到 WriteNullArray(版本分派)、CLIENT UNBLOCK/WRONGTYPE 帧(write_collection_item_result 单源承接 C# 六个阻塞命令尾部 switch),对位。

set 族

14 一致 set_random_member(wedb/wnode/src/resp/objects/set_commands/read.rs:151)
C# 对位 libs/server/Resp/Objects/SetCommands.cs:SetRandomMember(:633)
count 解析 i32 值域、count==0 不触后端回空数组、NOTFOUND 带 count 空数组/无 count null、seed i32 随机,对位。

15 一致 set_intersect_length(wedb/wnode/src/resp/objects/set_commands/read.rs:233)
C# 对位 libs/server/Resp/Objects/SetCommands.cs:SetIntersectLength(:159)+ libs/server/Storage/Session/ObjectStore/SetOps.cs:SetIntersectLength(:938)
numkeys/LIMIT 校验序、limit>0 才参与钳制(C# limit>0 ?: 全量同),对位。

16 一致 set_multi_is_member(wedb/wnode/src/resp/objects/set_commands/read.rs:115)
C# 对位 libs/server/Resp/Objects/SetCommands.cs:SetIsMember(:443,SMISMEMBER 形态)
Count<2 门、NOTFOUND count-1 个 :0 数组,对位。

zset 族

17 一致 sorted_set_add(wedb/wnode/src/resp/objects/sorted_set_commands/write.rs:57)
C# 对位 libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetAdd(:22)
Count<3 门、NX/XX/GT/LT/CH/INCR 选项解析双侧均下沉对象层(载荷参数原样传递)、应答经对象层 payload 透写,对位。

18 一致 sorted_set_rank(wedb/wnode/src/resp/objects/sorted_set_commands/read.rs:247)
C# 对位 libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRank(:60 区段)
Count<2 门、仅 len==3 校验 WITHSCORE 词元(错则 syntax error,len>3 静默忽略)、NOTFOUND null、arg1=includeWithScore,逐项对位。

19 一致 sorted_set_range_store(wedb/wnode/src/resp/objects/sorted_set_commands/write.rs:187)
C# 对位 libs/server/Resp/Objects/SortedSetCommands.cs:SortedSetRangeStore(:208)+ libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetRangeStore(:713)
源缺失→删目标键回 :0、范围参数拒(result1==-1)错误透传且先于目标删除、STORE 族目标 SET 语义清 TTL、回存入元素数,次序对位。

20 一致 write_zset_entries(wedb/wnode/src/resp/objects/sorted_set_commands/write.rs:929)
C# 对位 libs/server/Resp/Objects/SortedSetCommands.cs:966-988/:1135-1165/:1446-1462(ZDIFF/ZINTER/ZUNION 应答段)
RESP2+WITHSCORES 扁平 *2n+bulk 分值、RESP3 头 *n+逐成员 *2+`,num`、空结果恒 *0,有单测锚死,对位。

bitmap 族

21 一致 network_string_set_bit(wedb/wnode/src/resp/bitmap/bitmap_commands.rs:53)
C# 对位 libs/server/Resp/Bitmap/BitmapCommands.cs:NetworkStringSetBit(:131)
Count==3 门、offset TryGetLong+非负+IsValidBitOffset、bit 单字符 '0'/'1'、增长补零、回旧 bit,对位。

22 一致 string_bit_field(wedb/wnode/src/resp/bitmap/bitmap_commands.rs:381,parse_bitfield_args :715)
C# 对位 libs/server/Resp/Bitmap/BitmapCommands.cs:StringBitField(:420 区段)+ StringBitFieldAction
重点核对 OVERFLOW 语义:C# 将 overflow 词元追加为 parseState 末参传存储层,全局作用于整条命令;rust 解析后对全部子命令(含 OVERFLOW 之前的)统一回填——同为全局生效,对位。首子命令错误短路(handle_first_sub_command)、空子命令序列 *0、对象键数组头前拦截(C# 回卷 dcurr 净效果)对位。

hll 族

23 一致 hyper_log_log_add(wedb/wnode/src/resp/hyperloglog/hyper_log_log_commands.rs:392)
C# 对位 libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs:HyperLogLogAdd(:20 区段)
零元素不触存储直答 :0(C# 循环零次 pfaddUpdated==0 同)、非法载荷 WRONGTYPE_HLL、有/无变更 :1/:0。形状差:C# 逐元素 N 次 RMW,rust 单次 RMW 并入全元素——HLL 合并可交换,应答与终态一致。

geo 族

24 一致 geo_add(wedb/wnode/src/resp/objects/sorted_set_geo_commands.rs:430)
C# 对位 libs/server/Resp/Objects/SortedSetGeoCommands.cs:GeoAdd
选项词元扫描(CH/NX/XX 大小写不敏感、遇非选项即断)、NX+XX syntax error、三元组校验 do-while 至少一次(选项吞光实参同报 syntax error 而非空集 :0)、坐标 F6 错误回显(format_f6 有 C# ToString("F6") 逐字节单测),对位。

25 一致 geo_search_commands(wedb/wnode/src/resp/objects/sorted_set_geo_commands.rs:582)
C# 对位 libs/server/Resp/Objects/SortedSetGeoCommands.cs:GeoSearchCommands+ SessionParseStateExtensions.TryGetGeoSearchOptions
六命令形态参数门、FROMMEMBER/FROMLONLAT/BYRADIUS/BYBOX 文法与互斥、COUNT 正整数+ANY、STORE/STOREDIST 与 WITH* 互斥、源缺失存储变体删目标回 :0/读变体空数组,对位。

keys 族

26 一致 network_expire(wedb/wnode/src/resp/key_admin_commands/keys.rs:167,parse_expire_args :326)
C# 对位 libs/server/Resp/KeyAdminCommands.cs:NetworkEXPIRE(:364)
Count 2..=4、TryGetLong 对位 strict_i64(此族 C# 本就是 long,无 SETEX 的 i32 问题)、负值 INVALID_EXPIRE_TIME、NX/XX/GT/LT 组合判定表(XXGT/XXLT 四序合法、其余及重复选项 not-compatible)逐分支核对一致、四路 ticks 换算对位、status!=OK 回 :0。
过去时间戳 rust 物理删键 vs C# 惰性过期:应答(:1)与终态(键消失)一致,属文件内已声明在册差异。

27 一致 rename_sync(wedb/wnode/src/resp/key_admin_commands/keys.rs:413)
C# 对位 libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:RENAME(会话侧 libs/server/Resp/KeyAdminCommands.cs:NetworkRENAME/NetworkRENAMENX)
同键早退(RENAME OK/RENAMENX :1 先于存在性判定)、NOSUCHKEY、新键存在时 RENAMENX :0 不动旧键、TTL/ETag 随键迁移、旧键清退次序,对位;Meta 域(升阶键)整体降级慢路径为本仓分层存储接线,rust 自有面。

txn 族

28 一致 network_skip(wedb/wnode/src/resp/txn_resp_commands.rs:185)
C# 对位 libs/server/Transaction/TxnRespCommands.cs:NetworkSKIP(:105)
未知命令/不允许入事务 UNK_CMD+Abort、arity ±1(含子命令与 BITOP 再偏移)、WATCH 入事务仅报错不中止、SWAPDB 报错中止、SELECT 仅 TryGetInt 成功且 index!=activeDbId 才中止、DEBUG 门、LockKeys 后 +QUEUED、operationCntTxn 递增,次序逐项对位。
另核对 network_multi/network_exec/network_discard/common_watch 与 C#:50-100/:118-204/:209-220/:227 区段状态机与应答一致。

pubsub 族

29 一致 network_subscribe(wedb/wpubsub/src/session_commands.rs:193;会话路由 wedb/wnode/src/resp/resp_server_session/pubsub.rs:45)
C# 对位 libs/server/Resp/PubSubCommands.cs:NetworkSUBSCRIBE(:146 区段)
Count 门、SSUBSCRIBE 无集群 CLUSTER_DISABLED 先于 broker 判定、逐通道 subscribe 帧 [header,channel,count]、仅新订阅递增计数、broker 禁用臂净输出单错误行、isSubscriptionSession=true 尾置,对位。ns 隔离键折叠为 rust 多租户自有面(应答回写裸通道名,用户视角无感)。

array 族

30 一致 network_msetnx(wedb/wnode/src/resp/array_commands.rs:311)
C# 对位 libs/server/Resp/ArrayCommands.cs:NetworkMSETNX(:76)+ libs/server/Storage/Session/MainStore/MainStoreOps.cs:MSET_Conditional(:349)
偶参门、任一键存在回 :0 零写、全缺则全写回 :1(NOTFOUND=成功 同口径)、对象键/升阶键同计存在(C# NX 语义双 store 同)、半提交经 msetnx_resume 补写模式杜绝误答,对位。
另核对同文件 network_del/network_mget:C# 无 arity 门(0 参空循环/:0 空数组)1:1 保留,MGET 对象键答 nil 不报错,对位。

统计
一致 26 / 有差异 4(setex 解析宽度、setexnx 解析宽度、DECRBY 边界取负、PFMERGE 错误原子性)
差异均在极端入参或错误路径显形;4 条均未见轮1-7 已立发现(zcode.review-plan.md 及 next/ 现存文件)登记,排除误报后为增量。
在册偏差(RI 键门 WRONGTYPE、BITCOUNT BIT 口径、SG 批量计数、EXPIRE 过去时间戳物理删)本不被作新增差异计数,已在对应条目标注。

视角结论:有增量
