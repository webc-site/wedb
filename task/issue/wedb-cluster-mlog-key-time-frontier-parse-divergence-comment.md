CLUSTER MLOG_KEY_TIME 副本臂 FRONTIER 参数解析带与 C# 分叉且注释失实：C# ReadBool 仅收单字节 1/0 其余抛 NotANumber 错误面，rust strict_i64 任意整数静默归化非零为 true 非整数静默 false（注释宣称「解析失败按 false」无据）

问题分析：
1 Garnet 契约对齐：C# 副本臂 garnet/libs/cluster/Session/RespClusterReplicationCommands.cs:730 var getFrontier = parseState.Count == 2 ? parseState.GetBool(1) : false——GetBool 直委 ReadBool（garnet/libs/server/Resp/Parser/SessionParseState.cs:490），ParseUtils.cs:208-215 ReadBool 于 TryReadBool 失败即 RespParsingException.ThrowNotANumber 掐错误面；TryReadBool（ParseUtils.cs:224-237）仅收单字节 '1'/'0'。即 C# 第二参收 {"1","0"}，其余（"2"、"-1"、"abc"、"01"）一律 NotANumber 错误帧；参数缺位恒 false。不存在「解析失败按 false」臂。
2 工程现状确证：wedb/wedb/src/server/cluster_session/replication.rs:480-488 副本臂 let frontier = args.get(1).and_then(|a| strict_i64(a)).is_some_and(|v| v != 0);——任意 strict 整数非零即 true（"2"/"42"/"-1" → true），非整数静默 false（"abc" → false，C# 抛错），且码内注释「C# GetBool(1) 解析失败按 false」与 C# 一手形态相悖失实。参数缺位臂两侧同 false 不分叉。
3 逻辑危害确证：纯参数面/应答面对账分叉（宽向加静默吞错向），无数据面危害；对拍席遇 MLOG_KEY_TIME k 2 双侧应答分叉必疑报，失实注释更会误导后续对账轮。查重：§117d 值域轴与 §32 整数文法轴均不经由覆盖（GetBool 非 strict 整数语法，系 bool 词法面），§117 族 failover/§32 RESERVE count 裁例不外延本位，全册 grep MLOG 零在册。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/cluster_session/replication.rs:network_cluster_mlog_key_time 副本臂（:480-488，:482 失实注释）

对应 c# 文件与函数：
garnet/libs/cluster/Session/RespClusterReplicationCommands.cs:MlogKeyTime 副本臂（:730）
garnet/libs/server/Resp/Parser/ParseUtils.cs:ReadBool/TryReadBool（:208-237）
garnet/libs/server/Resp/Parser/SessionParseState.cs:GetBool（:490）

精炼执行方案：
1 裁维持现形（与 §117d 宽向登记先例同向）：订正 :482 注释为「C# 仅收单字节 1/0，其余抛 NotANumber；rust strict_i64 宽向归化，刻意分叉」，deviations.md 按当日册尾顺延新条登记（词法 bool 面分叉，非 §32 整数文法轴外延）
2 备选（评审裁）：对齐 C# 收窄为单字节 b'1'/b'0' 门，其余回 VALUE_IS_NOT_INTEGER 帧——若采纳则注释同步改写
3 验证点：MLOG_KEY_TIME k 1 / k 0 / k 缺位三案双侧对拍锁测（按方案 1 登记级则锁测断言现行为防回改）
