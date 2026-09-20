轮11 边界值视角:全命令参数边界值字典(按值域横向扫)

方法
建立字典: 0/1/-1、i32::MAX±、i64::MIN/MAX、u64::MAX、空串、1 字节、前导零串、float 特值(NaN/±inf/±0.0/1e308/5e-324/2^53+1)、负 float、科学计数词形、Unicode/二进制键名;对数值型命令族逐族过字典,与 C# 对位,只在字典值可分辨行为处立项。
不复述在档: r8-sample-a(SETEX i32/i64 宽度、DECRBY i64::MIN 符号翻转)、protocol 长度头、r4-foundation(ms 钳制重复)。

增量差异

1. INCRBYFLOAT 结果的浮点格式化与落盘文本
字典值 0.1+0.2(SET k 0.1; INCRBYFLOAT k 0.2)、1e300、5e-324、-0.0。
rust: wedb/wresp/src/resp_memory_writer.rs:format_double(zmij 最短往返 + 剥 ".0"),INCRBYFLOAT 站点 wedb/wnode/src/resp/basic_commands/incr.rs:network_increment_by_float,落盘与应答同串。
C#: garnet/libs/common/NumUtils.cs:CountCharsInDouble/WriteDouble(定点、小数位≤15、Math.Round 逐位截断、value==0 早退无符号),站点 garnet/libs/server/Storage/Functions/MainStore/PrivateMethods.cs:TryInPlaceUpdateNumber(double)。
逐字典值:
- 0.1+0.2: C# 存/回 "0.300000000000000"(15 位小数,数值退化为 0.3);rust 存/回 "0.30000000000000004"。后续再 INCRBYFLOAT 0.1 两侧数值路径发散(C# 0.4,rust 0.4000000000000001)。
- 1e300: C# 301 位定点文本;rust "1e+300"。
- 5e-324: C# CountCharsInDouble 的 2*Double.Epsilon 容差使 fractionalDigits=0 → 回 "0";rust "5e-324"。
- -0.0(SET k -0; INCRBYFLOAT k -0): C# WriteDouble 的 value==0 早退 → 回/存 "0";rust "-0"。
判定: 差异成立。回复帧与存储值双侧不一致,且引发后续累加发散;INCRBYFLOAT 全值域受影响。

2. 浮点应答帧(ZADD INCR/ZINCRBY/ZSCORE/ZMSCORE/ZRANGE WITHSCORES/ZPOPMIN WS/GEODIST/GEOPOS/HINCRBYFLOAT)
字典值 1e16、0.00001(E=-5)、1e-6、1e17。
rust: wresp format_double 统一出帧(zmij f64 FIXED_DEC_EXP=-5..=15,科学计数小写 'e',zmij-1.0.23/src/lib.rs:320),站点 wedb/wcol/src/resp/output.rs:write_double_numeric、wcol/src/zset/sorted_set_object_impl.rs:sorted_set_increment。
C#: value.TryFormat 默认 G 最短往返(garnet/libs/common/RespWriteUtils.cs:TryWriteDoubleBulkString/TryWriteDoubleNumeric;garnet/libs/server/Objects/Hash/HashObjectImpl.cs:HashIncrementFloat 的 result.TryFormat),.NET 规则为 E>-5 且 E<max(位数,17) 用定点,科学计数大写 'E' 指数至少 2 位。
逐字典值:
- 1e16: C# 16<17 → 定点 "10000000000000000";rust "1e+16"。
- 0.00001: C# E=-5 不满足 E>-5 → 科学 "1E-05";rust -5∈[-5,15] → 定点 "0.00001"。
- 1e-6 / 1e17: 双侧均科学计数,但字节不同:C# "1E-06"/"1E+17",rust "1e-06"/"1e+17"(E 大小写)。
判定: 差异成立。凡科学计数值,应答字节必不同;±1e15..1e17 与 1e-5 两个窗口定点/科学选择相反。

3. nan/infinity 词形入参(C# Utf8Parser 特值文法 vs rust 白名单)
C# 解析基座 Utf8Parser(哈 dotnet runtime Utf8Parser.Float.cs:TryParseAsSpecialFloatingPoint)大小写不敏感接受 "nan"(3 字节)与 "infinity"(8 字节,可带号);garnet 的 TryGetDouble/TryParseWithInfinity 由此继承。rust 单点 wedb/wbase/src/num.rs:strict_parse_float 只认 inf/+inf/-inf 3-4 字节白名单,nan 恒拒。
- ZADD k nan m: C# 接纳 score=NaN 落集合应答 :1(非 INCR 臂无 NaN 门,garnet SortedSetObjectImpl.cs:SortedSetAdd);rust 报 not-valid-float(wcol/src/zset/sorted_set_object_impl.rs:322)。
- ZADD k infinity m: C# +inf 落集 :1;rust 报错。
- ZADD k nan m(GT/LT 不涉)与 ZADD INCR k nan m: C# 报 SCORE_NAN;rust 报 not-valid-float,文案不同。
- INCRBYFLOAT k infinity(增量): C# IsInfinity → "ERR increment would produce NaN or Infinity";rust 词形拒 → "ERR value is not a valid float"(incr.rs:parse_incr_by_float_args)。
- INCRBYFLOAT k nan(增量): 两侧最终同报 not-valid-float(C# NaN 入 RMW 得 InvalidTypeError),此值无增量。
- 存量值 "nan"(SET s nan; INCRBYFLOAT s 1): C# TryParseWithInfinity 接纳 → NaNOrInfinityError → "ERR increment would produce NaN or Infinity";rust 旧值解析拒 → "ERR value is not a valid float"(incr.rs:network_increment_by_float)。
- 存量值 "nan"(HINCRBYFLOAT h f 1): C# TryParseWithInfinity 接纳 → NaN + 1 = NaN → TryFormat 落库/回 bulk "NaN"(HashObjectImpl.cs:HashIncrementFloat 无 IsFinite 门);rust 报 hash-value-is-not-float(wcol hash_object_impl.rs:hash_increment_float、分层层 wnode/.../tiered_collection_ops/hash.rs:574)。
- HINCRBYFLOAT h f inf(增量): C# TryGetDouble canBeInfinite=true → "ERR value is NaN or Infinity";rust 对象层 try_parse_f64=strict_f64(..,false) → "ERR value is not a valid float";rust 分层层 strict_f64(..,true) → 与 C# 同文案。rust 两层自身不一致,对象层是偏差方(wcol/src/hash/hash_object_impl.rs:50,387)。
判定: 差异成立。核心是 rust 把 C# 浮点解析锚定为「inf 3-4 字节白名单 + 拒 nan」,漏掉 Utf8Parser 的 nan/infinity 特值文法。

4. HINCRBY 前导零 "007"(增量与存量双侧)
字典值 "007"/"01"/"00"。
rust: 对象层 wedb/wcol/src/hash/hash_object_impl.rs:hash_increment(经 num_utils_try_parse_long=strict_i64),分层层 wnode/wnode/src/resp/objects/tiered_collection_ops/hash.rs:457(strict_i64),两侧注释均锚 "NumUtils.TryParse 失败 → not-integer"。
C#: garnet/libs/server/Objects/Hash/HashObjectImpl.cs:HashIncrement 的 NumUtils.TryParse = Utf8Parser.Int64D(libs/common/NumUtils.cs:TryParse),接受前导零与 + 号,整段消费。
- 增量 "007": C# 解析为 7,新字段存原文 "007" 回 :007,存量按 7 累加;rust strict_i64 拒 → "ERR value is not an integer or out of range."。
- 存量值 "007": C# 按 7 累加;rust 报 "ERR hash value is not an integer."。
判定: 差异成立。INCR 族命令实参走 TryGetLong(allowLeadingZeros:false)拒前导零,rust strict_i64 对位;但 HINCRBY 在 C# 用的是另一基座(Utf8Parser),rust 用同一 strict_i64 覆盖,锚错基座。

5. LPOS RANK 0 / COUNT -1
字典值 RANK 0、COUNT -1。
rust: wedb/wcol/src/list/list_object_impl.rs:list_position,377 行 params.count<0 || params.maxlen<0 || params.rank==0 → 报 "ERR value is not an integer or out of range."。
C#: garnet/libs/server/Objects/List/ListObjectImpl.cs:ListPosition 无零值门,rank==0 落 else(负向)臂自尾扫描且 rank 永不命中 → 缺省形态回 null、带 COUNT 回空数组;COUNT -1 无负值门,直接以 -1 写数组头出畸形帧。
判定: 差异成立(RANK 0:C# 静默 null vs rust 报错;COUNT -1:C# 畸形帧 vs rust 报错)。rust 属「修复 C#」但未在注释登记为有意偏差。

6. EXPIRE 大值秒数(i64::MAX、1e12)
rust: wedb/wnode/src/resp/key_admin_commands/keys.rs:network_expire + wbase/src/convert.rs:expire_after_to_ticks(checked 乘加,超界钳到 i64::MAX ticks)→ 回 :1 落远期 TTL。
C#: garnet/libs/server/Resp/KeyAdminCommands.cs:NetworkEXPIRE → DateTimeOffset.UtcNow.AddSeconds(expiration),结果越过 DateTimeOffset.MaxValue 抛 ArgumentOutOfRangeException → 无正常应答(会话异常通道,连接中止)。阈值约 2.5e11 秒(9999 年),1e12 即触发。
判定: 差异成立。字典值 1e12/i64::MAX:rust :1,C# 异常无应答。秒数≤2.5e11 段两侧一致。

7. ZCOUNT/ZRANGEBYSCORE min/max 空串
rust: wedb/wcol/src/zset/sorted_set_object_impl.rs:try_parse_parameter,val.first() 判 '(' → 空串走 strict_f64 失败 → None → "ERR min or max is not a float"。
C#: garnet/libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:TryParseParameter,val[0] 无长度门 → IndexOutOfRangeException → 会话异常无正常应答。
判定: 差异成立。"(inf" 独占退化、inf/NaN 边界语义两侧一致,唯空串分歧(C# 崩连接 vs rust 错误帧)。

有意偏差(已登记,不计新增)
- EXPIREAT i64::MAX: C# UnixTimestampInSecondsToTicks 无 checked 环绕(garnet/libs/common/ConvertUtils.cs:58);rust expire_at_seconds_to_ticks 钳制,wnode keys.rs 注释已声明 "C# unchecked 环绕对应的确定性降级"。
- HINCRBY 溢出: 两侧同为回绕(C# 未 checked + TryFormat,rust wrapping_add + itoa),含 i64::MIN,无增量。

无增量确认(逐族一句话)
- INCR/DECR/INCRBY 实参与旧值: strict_i64 vs TryGetLong(allowLeadingZeros:false)全字典对位,溢出双侧同报 not-integer 不回绕。
- INCRBYFLOAT 有限值/1e999 增量/±inf 存量: 双侧同文案 NAN_INFINITY_INCR;有限+有限溢出双侧同报 not-valid-float。
- GETRANGE/SUBSTR: strict_i32 vs TryGetInt,NormalizeRange 全怪癖(负起点 end==len 折 0 等)1:1 复刻并有回归锚。
- SETRANGE: 负 offset "ERR offset is out of range"、offset+val>512MB 上限门、边界对位。
- GETDEL: 无数值参数,键缺失 null/WRONGTYPE 对位。
- LTRIM: i32::MIN/MAX 端点归一 1:1(len+i32::MIN 无溢出面)。
- BITPOS: start/end TryGetLong、BIT/BYTE 词形、越界即 -1 的 TryValidateBitPosOffsets 1:1(wbitmap/src/manager.rs)。
- BITFIELD SET/INCRBY: WRAP/SAT/FAIL 三策略含有符号 i64::MIN 回绕、u64::MAX 环、饱和钳,CheckSigned/UnsignedBitfieldOverflow 1:1(wbitmap/src/bitfield/execute.rs)。
- SPOP/SRANDMEMBER count: i32::MIN 双侧同为「无 count」哨兵(显式 -2147483648 与缺省不可分),超 i32 报错一致。
- ZPOPMIN/ZPOPMAX count -1 哨兵(withHeader=false,count=1)双侧一致。
- ZMPOP numkeys/COUNT: 非整数与 <1 同报、语法门次序对位(wnode/.../sorted_set_commands/mod.rs:parse_zmpop_args)。
- ZADD 分值解析有限值/1e999/±inf 词形: 双侧接纳与报错一致(仅 nan/infinity 词形见上第 3 条)。
- GEOADD/GEOSEARCH 坐标: geo_to_long_value 的值域门对 NaN/inf/超界返回 -1,量化数学 1:1(wcol/src/geo/geo_hash.rs)。
- GEODIST: 距离出帧并入第 2 条格式面;单位换算与缺失语义一致。
- GETEX EX/PX/EXAT/PXAT: max_val 门、checked 溢出、负 ticks OVERFLOWEXP、EXAT 过去时归零语义对位。
- SMOVE: 无数值参数,成员字节透明,双侧一致。
- OBJECT 族: 无数值实参(SCAN COUNT 经 OBJECT_SCAN_COUNT_LIMIT 配置钳制),双侧一致。
- SORT: 两侧均未实现该命令,无字典值面。
- Unicode/二进制键名、成员、值: 全链路字节切片透明,无 UTF-8 校验分叉,双侧一致。
- 整数 i64 回复边界: INCR 族 checked 报错、HINCRBY 回绕、LPOS/LPOS INDEX/整数自增回写 itoa 与 TryFormat 一致;u64::MAX 词形各处双侧同报 not-integer。

统计
增量差异 7 条(1/2 为浮点格式化面,覆盖全部浮点应答与 INCRBYFLOAT 落盘;3/4 为解析基座锚错;5/6/7 为单点行为分歧)
有意偏差已登记 2 条(EXPIREAT 钳制、HINCRBY 回绕)
无增量确认 19 族

视角结论: 有增量
