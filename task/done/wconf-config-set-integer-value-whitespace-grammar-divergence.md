终态注记：合入 db07a28，定谳方案2对齐轨——try_set 两整数臂 parse 前 trim_ascii 首尾 ASCII 空白裁剪对齐 C# NumberStyles.Integer 宽收（仅空白折叠不引入全集），弃登记轨（本面无 §32a strict 家族裁定覆盖，恒拒系转写疏漏非刻意分叉，不满足登记册刻意分叉语义），config_commands_tests.rs 补 INT32/INT64 双臂用例防单边回改。

甄别结论：通过（2026-09-29 主控甄别，定级 P3——C# RuntimeServerConfig.cs TrySet :385/:400 NumberStyles.Integer 含 AllowLeadingWhite|AllowTrailingWhite 收首尾空白，rust runtime_server_config.rs:717/:736 str::parse 整数文法恒拒，错误帧文案双侧逐字节同串。修复：执行席先定谳二选一轨道（A 登记 strict 分叉零代码、B trim_ascii 对齐 C#）禁并存，测试双臂覆盖 INT32+INT64）

审核结论：通过（2026-09-29 甲轮35-A，P3 级）。双锚复核成立（rust :716-746 str::parse 恒拒 vs C# :385/:400 NumberStyles.Integer 收空白）；§32 台账系命令参数消费点不含 CONFIG SET 值面，查重干净。双轨定谳结构已备任一轨可落。无修正意见。

原票面：
CONFIG SET 整数值空白文法分叉：C# 收白空格 rust 拒收

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：RuntimeServerConfig.TrySet 的 Int32/Int64 两臂用 int.TryParse/long.TryParse(value, NumberStyles.Integer, InvariantCulture)。NumberStyles.Integer = AllowLeadingWhite | AllowTrailingWhite | AllowLeadingSign，故 CONFIG SET 传 " 100"、"100 "（RESP bulk string 内合法空白）被 C# 接受并落槽回 +OK；"+100"/"-1"/前导零亦被接受。
2. 工程现状确证：rust try_set 的 INT32 臂用 value.parse::<i32>()、INT64 臂用 value.parse::<i64>()，str::parse 的整数文法收前导正负号与前导零但恒拒首尾空白——同样的输入回 ERR Invalid value for '<name>': expected an integer. 且槽位不变。前导零/符号两形双侧一致（§32 前导零 strict 文法裁定的是命令参数 TryGetInt 面，非本 CONFIG SET 值面；deviations.md 与 task/done|reject|issue 五池均未登记本空白形）。
3. 逻辑危害确证：同帧分叉——对含空白值 C# 回 +OK 且槽位生效、rust 回 ERR 且槽位保持；多对 CONFIG SET 连写的错误累积帧随 ErrorsMsgBuilder 顺序在 rust 侧多出一条 C# 不存在的 InvalidInteger，后续以 ; 连接的整帧字节面随之分叉。属验收契约分叉（5.1 参数解析面），非崩溃/数据危害，量级轻（仅客户端显式携带空白时可达，redis-cli 等常规客户端不产生）。

涉及代码：
rust 文件与函数：
wedb/wconf/src/runtime_server_config.rs:RuntimeServerConfig::try_set（INT32 臂 value.parse::<i32>、INT64 臂 value.parse::<i64>）
wedb/wnode/src/resp/config_commands.rs:ServerConfig::network_config_set（string_lossy 归化后传入 try_set 的值面入口）

对应 c# 文件与函数：
garnet/libs/server/Config/RuntimeServerConfig.cs:RuntimeServerConfig.TrySet（case ConfigKind.Int32 / ConfigKind.Int64，int.TryParse/long.TryParse + NumberStyles.Integer）

精炼执行方案：
1. 二选一定谳：按在册 strict 文法家族先例（§32a 前导零 strict 形）维持 rust 现状并落 deviations 台账登记该空白分叉（C# 宽收空白、rust 恒拒，刻意分叉），零代码改动；
2. 或按 C# 对齐：try_set 两整数臂解析前 trim_ascii_once 式首尾 ASCII 空白裁剪（仅空白折叠，不引入 NumberStyles 全集），使 " 100"/"100 " 与 C# 同帧 +OK；
3. 测试验证点：wedb/wnode/tests/config_commands_tests.rs 补两臂用例——CONFIG SET slowlog-log-slower-than " 100"（定谳后断言 +OK 且 CONFIG GET 回显 100，或断言 ERR 帧逐字节）与 aof-sync-max-lag-bytes INT64 臂同形，防两整数臂单边回改。
