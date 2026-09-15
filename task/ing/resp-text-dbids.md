# resp-text-dbids：RESP 文案字节级对齐 + CONFIG GET RESP3 map 头 + DBID 校验接线

来源：next/glm.md 条 42、44、45 与 next/ds.data.md 条 4（主代理预清理转述）。

## 甄别结论

四条全部成立，逐条对照 C# 源码核实（含甄别提示中两处不精确描述，以下以 C# 权威源码为准）。

### 一、CONFIG GET 应答未接 RESP3 map 头 — 成立

C#：libs/server/ServerConfig.cs:69 NetworkCONFIG_GET 命中参数时调
WriteMapLength(totalCount)（RespServerSessionOutput.cs:164：respProtocolVersion >= 3
写 `%N`，否则写 `*2N`）；零命中恒回 RESP_EMPTYLIST（`*0\r\n`，两协议同）。
同根调用面还有 HELLO（BasicCommands.cs:1829，升级协议后写 map 头）。

rust：config_commands.rs:163 恒 write_map_len_resp2（双倍数组）；HELLO
resp_server_session.rs:2138 同病（注解说"RESP2 退化为双倍数组"实则不分派）。
wresp 已有 RespProtocol trait（Resp2/Resp3 静态分派）但缺运行时按版本分派的
入口函数。

修法：wresp cmd_strings 新增运行时版本分派 write_map_len（一处定义），
CONFIG GET 与 HELLO 接入；network_config_get 增加 resp_protocol_version 参数。

范围外同模式记录（不修改）：acl_commands.rs:462 write_map_len_resp2(3)
（对标 ACLCommands.cs:477 WriteMapLength(3)，同为协议感知辅助函数调用点）。

### 二、SUBSTR 报错名 / PEXPIRETIME 命令名口径 — 成立（两条均修）

SUBSTR：BasicCommands.cs:488 NetworkGetRange 报 cmd.ToString()，即
GETRANGE 报 GETRANGE、SUBSTR 报 SUBSTR（Parser/RespCommand.cs:219 枚举名
SUBSTR）。rust basic_commands.rs:547 unpack_args 恒 "GETRANGE"。
修法：network_get_range 增加 cmd 名参数，garnet_api.rs:890 分派传实名。

PEXPIRETIME：KeyAdminCommands.cs:537 NetworkEXPIRETIME 恒报
nameof(RespCommand.EXPIRETIME)，PEXPIRETIME 参数个数错误也报 EXPIRETIME
（C# quirk）。rust key_admin_commands.rs:477 按形态报实名。
修法：对齐 quirk，恒 "EXPIRETIME"。network_ttl 报实名（command.ToString()）
与 C# 一致，不动。

### 三、集合命令错误文案批 — 成立（逐条已核，含甄别提示修正）

WEIGHTS：C# SortedSetCommands.cs:1107/1299/1398/1522 报模板
GenericErrNotAFloat（"ERR {0} value is not a valid float"）替换 {0}="weight"，
最终文案 "ERR weight value is not a valid float"。甄别提示中"C# 为
ERR value is not a valid float"不含 weight 前缀，系与 RESP_ERR_NOT_VALID_FLOAT
混淆，以模板替换结果为准。rust sorted_set_commands.rs:1556 为
"-ERR weight value is not a float"，缺 "valid"。修正。

ZPOPMIN/ZPOPMAX：C# SortedSetCommands.cs:366 count 非整数或负数报
RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE = "ERR value is out of range, must be
positive."（带句点）。rust:333 报 "ERR value is out of range, must be >= 0"。
改用 cs 既有常量 RESP_ERR_GENERIC_VALUE_IS_OUT_OF_RANGE。

BLMPOP：C# ListCommands.cs:871/899 用 GenericParamShouldBeGreaterThanZero
（"ERR Parameter `{0}` should be greater than 0"）替换 numkeys/count；
注意 LMPOP（:202/:244）用 GenericErrShouldBeGreaterThanZero（无 Parameter
前缀，rust cs::RESP_ERR_GENERIC_NUMKEYS 已对齐，不动）。
rust list_commands.rs:1069/1073 误用 LMPOP 版文案、:1097 为
"ERR count should be greater than 0"。三处改 Parameter 反引号版；
wresp cmd_strings 新增 GENERIC_PARAM_SHOULD_BE_GREATER_THAN_ZERO 模板常量。

GEO lon/lat：C# SessionParseStateExtensions.cs:781 TryGetGeoLonLat 两态：
非浮点 → RESP_ERR_NOT_VALID_FLOAT（"ERR value is not a valid float"）；
越界 → GenericErrLonLat 格式化 "ERR invalid longitude,latitude pair
{lon:F6},{lat:F6}"（带实际坐标，六位小数）。调用点三处：GEOADD 预检
（SortedSetGeoCommands.cs:69）、GEORADIUS 圆心与 FROMLONLAT
（SessionParseStateExtensions.cs:294/366）。
rust wcol::parse_utils::try_get_geo_lon_lat 把两态合并为 Option，
sorted_set_geo_commands.rs:139/200/444 统一回无数值的
"ERR invalid longitude,latitude pair"。修法：wcol 返回两态枚举
（NotFloat / OutOfRange(lon, lat)），错误文案组装单点化（越界格式化串抽
helper，session 版 673 行同源复用），geo_commands 三处调用拆两态，
删除 RESP_ERR_INVALID_LON_LAT 常量。

并发面：sorted_set_commands.rs 的 parse_combine_args numkeys 解析段
（1526-1539）有并发代理在改，本次只改 1556 行字符串字面量与 333 行
sorted_set_pop，不触碰解析逻辑；info_provider 不涉及。

### 四、DBID 校验硬编码 — 成立

C# AdminCommands.cs:1119 TryParseDatabaseId：非整数 →
RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER；dbId > 0 且 EnableCluster →
RESP_ERR_DB_ID_CLUSTER_MODE；dbId >= MaxDatabases || dbId < 0 →
RESP_ERR_DB_INDEX_OUT_OF_RANGE。边界判定顺序：集群门先于范围门
（EnableCluster 时 MaxDatabases 被约束为 1，dbId=1 两门皆拦，语义一致）。

rust admin_commands.rs:38/41 硬编码 const MAX_DATABASES=16 与
const CLUSTER_ENABLED=false：后者使 :626 集群分支永不可达（死代码）；
前者不读会话已装配的 self.max_databases（resp_server_session.rs:355，
SELECT/SWAPDB array_commands.rs:286/327 同源消费），配更多库时
SAVE/BGSAVE/LASTSAVE/COMMITAOF/EXPDELSCAN 的 DBID 被误拒。

装配链现状：RespServerSessionOptions.max_databases 由启动选项投影
（wconf/node_options.rs:349 --max-databases 接线完毕）；集群门用
self.cluster_session.is_some()（None = 单机形态，与 C# EnableCluster
装配 clusterProvider 的门同语义，HELLO/READONLY 等已用同一状态）。

修法：删两常量；try_parse_database_id 改读 self.max_databases 与
self.cluster_session.is_some()，判定顺序对齐 C#。

## 实施顺序

1. wresp cmd_strings：write_map_len 运行时分派 + Parameter 模板常量（一处定义）
2. CONFIG GET / HELLO 接 map 头分派；SUBSTR 实名；PEXPIRETIME quirk
3. 文案批：WEIGHTS / ZPOPMIN / BLMPOP / GEO 两态
4. DBID：删硬编码、接会话字段与集群门
5. 测试：config_commands RESP2/RESP3 字节断言、HELLO map 头、DBID 四态
   （含集群 stub）、新集成测试 resp_error_text_tests.rs 收口文案字节断言

## 验证结果

（完成后追加）
