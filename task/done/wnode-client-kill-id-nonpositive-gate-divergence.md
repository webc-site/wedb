归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 5e104a9（P4），收口形态：§154 三次让号终案登记＋ID 非正值锁测＋MAXAGE 不越界钉＋§63 划界互指。续排注：本票沙箱席与方案详情见下文。

甄别结论：通过（甄别席 zc-fix-r16-killgate，2026-09-26）定级 P4
1. rust 锚成立：wedb/wnode/src/resp/client_commands.rs:201 确为 strict_i64(value).filter(|&v| v > 0)，非正值同帧回 RESP_ERR_CLIENT_ID_GREATER_THAN_ZERO（wresp/src/cmd_strings.rs:342 常量实存）；MAXAGE 臂 :244 仅 strict_i64 无值域门，与 C# ClientCommands.cs:352-357（MAXAGE 仅 TryReadLong 失败报语法错）双侧同收敛，票面论断属实。
2. C# 锚成立：NetworkCLIENTKILL（ClientCommands.cs:205）ID 臂实测 :271-284（票面 :268-281 有 3 行偏移，内容逐条吻合——:273 TryReadLong 仅解析失败报错 :275，无正值校验）；IsMatch :416 起、:438 id.Value == targetSession.Id；killed=0 → :404 TryWriteInt32(0) 即 :0。分叉真实。
3. 会话 ID 恒正锚成立：Interlocked.Increment(ref lastSessionId) 实存于 garnet/libs/server/Providers/GarnetProvider.cs:61（自 1 起）；票面正文「GarnetServerBase」系笔误，涉及代码栏路径正确，不构成幻觉。
4. 锁测锚成立并订正如票：client_commands_tests.rs:282-286 已锁 ID abc 解析失败臂；kill_by_id 正对照在位（:125/:481/:535）；全 tests grep 无 ID 0 / ID 负值 / MAXAGE 非正值用例，非正值臂确无锁。
5. 查重成立：deviations.md 全册 grep KILL 无 CLIENT KILL ID 值域门条目（连 greater than 0 字样亦零命中）；§63（:839-854）只登 UNBLOCK client_id<0 回 :0 门，异命令异裁非并案；册尾确为 §150，拟号 §151 不撞；ing/reject/done/issue 各池 grep 无同轴票。
6. 架构合规：方案零行为改动，仅 deviations 落册 + 码内锚注释 + tests/ 锁测（对标既有 §149/§150 严向收口登记先例），单向分层、单套机制、无过度设计无假桩；验证闭环（逐字节错误帧断言 + 连接存活 + 正对照复用 kill_by_id）。可执行。

审核结论：通过，定级 P4 登记级（审核席 zcode-r16-review-killid，2026-09-26 双侧现码亲验）。
1. 分叉坐实：rust 侧 wedb/wnode/src/resp/client_commands.rs:201 为 strict_i64(value).filter(|&v| v > 0)，解析成功的 0/负数 ID 同帧回 ERR client-id should be greater than 0（常量 wresp/src/cmd_strings.rs:342，与 C# GenericErrShouldBeGreaterThanZero 格式化串逐字节同文）；C# 侧 ClientCommands.cs ID 臂仅 TryReadLong 解析失败报错，解析成功的 idParsed 无正值校验直接入过滤器，网络会话 ID 恒自 1 起（GarnetProvider.cs:61 Interlocked.Increment(ref lastSessionId)），非正值恒不命中 IsMatch（id.Value == targetSession.Id）→ killed=0 → TryWriteInt32(0) 即 :0。双侧应答分叉真实非幻觉。
2. 查重坐实：deviations.md 全册 grep kill 无 CLIENT KILL ID 值域门条目；§63 只登 CLIENT UNBLOCK 负数 ID :0 门（rust 亦回 :0 双侧同收敛），与本案 KILL 单侧分叉（rust 回错误帧）不同门不同裁；§32b 文法清单无涉。未重复登记。
3. 裁决：维持 rust 现形（真 Redis clientKillCommand 对 ID 0/负数报同文案，严向收口符合本项目无旧兼容取向），登记防回改，禁删 .filter(|&v| v > 0) 滤片。
4. 票面订正（锁面对账，防以讹传讹）：原票称「wnode/tests 零用例」不精确——client_commands_tests.rs:283-286 已逐字节锁 CLIENT KILL ID abc（解析失败臂，双侧同错误帧非分叉面）；真正无锁的是非正值臂（ID 0 / ID -3）与 MAXAGE 负值形。正对照 CLIENT KILL ID 合法单杀 :1 已有 kill_by_id 在位（client_commands_tests.rs:124/:481/:535），新锁测勿重复造轮。
5. 编号注记：本审核时点册尾 §150，落册拟取 §151，撞号让位不覆写，落笔前重核册尾。
6. 执行方案订正：新锁测就近落 client_commands_tests.rs 既有参数错误面块（:275-292 一带），ID 0 与 ID -3 断言错误帧逐字节 -ERR client-id should be greater than 0\r\n 且连接存活；MAXAGE 负值形断言收帧无值域拒（钉双侧同收敛现状）。

CLIENT KILL ID 过滤器非正值门禁双侧分叉未登记（rust 对解析成功的 0/负数 ID 回错误帧，C# 收作恒不匹配过滤器回 :0）

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# NetworkCLIENTKILL 新式过滤器 ID 臂（garnet/libs/server/Resp/ClientCommands.cs:268-281）仅对 ParseUtils.TryReadLong 解析失败（非整数/前导零/超 i64 域）报 ERR client-id should be greater than 0（GenericErrShouldBeGreaterThanZero, CmdStrings.cs:338）；解析成功的 idParsed 不做任何正值校验直接入过滤器，随后仅查重复（id is not null → ERR ... duplicate filter）。会话 ID 恒为正整数（GarnetServerBase 会话计数器自 1 起），故 CLIENT KILL ID 0 / ID -3 在 C# 侧过滤器恒不命中，走杀零会话路径逐条 IsMatch（ClientCommands.cs IsMatch :416 起 id.Value == targetSession.Id）后回 :0。即 C# 可观测契约 =「ID 非正值解析成功 → 应答 :0」。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
rust wedb/wnode/src/resp/client_commands.rs:parse_kill_filters ID 臂（:200-206）为 strict_i64(value).filter(|&v| v > 0)——在 C# 的解析失败门之上新增了「解析成功但非正值」同帧拒绝臂：CLIENT KILL ID 0 / ID -3 回 -ERR client-id should be greater than 0，而 C# 回 :0。该门与真 Redis 对齐（redis clientKillCommand 对 ID 0/负数报 client-id should be greater than 0），属严向收口；但 deviations.md 全册（§1-§150）与 §32b 严格文法清单 25 项均无 CLIENT KILL ID 值域门登记（§63 只登 CLIENT UNBLOCK 负数 ID :0 门），仓内无锁测（全仓 grep CLIENT_ID_GREATER_THAN_ZERO 唯一命中即常量定义本身，wnode/tests 零用例），双侧对拍夹具遇该输入必红且无据可引。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
无运行期数据危害（rust 更严、拒绝面更贴 Redis）。危害纯在治理面：其一，严格逐字节对拍席遇 CLIENT KILL ID 0 / ID -3 用例现形「C# :0 vs rust 错误帧」无台账可引，易误判转写缺陷；其二，后审席可能按「对齐原型」名义删除 >0 滤片回改（回改后 rust 对非正值 ID 静默回 :0，丢真 Redis 值域门），或反向据真 Redis 语义误判 C# 侧缺陷另立错案；其三，与 §63（CLIENT UNBLOCK 负数 ID :0 门）形近易混，两命令同域不同门（UNBLOCK 双侧同收敛 :0；KILL 单侧分叉），不登记则后续 CLIENT 域审查重复撞面。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/client_commands.rs:RespServerSession::parse_kill_filters（ID 臂 strict_i64(...).filter(|&v| v > 0)）
wedb/wresp/src/cmd_strings.rs:RESP_ERR_CLIENT_ID_GREATER_THAN_ZERO（:342，文案与 C# 逐字节同串）

对应 c# 文件与函数：
garnet/libs/server/Resp/ClientCommands.cs:RespServerSession.NetworkCLIENTKILL（ID 过滤器臂 :268-281，仅解析失败报错）
garnet/libs/server/Resp/CmdStrings.cs:GenericErrShouldBeGreaterThanZero（:338）
garnet/libs/server/Resp/Parser/ParseUtils.cs:ParseUtils.TryReadLong（:86-94，allowLeadingZeros:false 与 rust strict_i64 文法同口径）

精炼执行方案：
1. 裁决维持 rust 现形（严向、对齐真 Redis 值域门），按 P4 登记级补 doc/zh/deviations.md 新条目（编号按落册时册尾顺编、撞号让位不覆写）：钉死「CLIENT KILL ID 非正值（0/负数）解析成功时 C# 收作恒不匹配过滤器回 :0、rust 回 ERR client-id should be greater than 0 错误帧」的分叉事实与禁回改声明（严禁删 .filter(|&v| v > 0) 滤片对齐 C# 宽松面）。
2. parse_kill_filters ID 臂处补一行码内注释锚回指该 deviations 条目，与 §63（CLIENT UNBLOCK）划界互指，防两门混淆误改。
3. 测试验证点：wedb/wnode/tests 补锁测一例——CLIENT KILL ID 0 与 ID -3 各断言错误帧逐字节为 -ERR client-id should be greater than 0\r\n 且连接存活、无会话被杀；正对照 CLIENT KILL ID <自身ID> 回 :0 或合法 ID 路径不受影响；MAXAGE 负值形（双侧同收敛、无值域门）一并钉现状防外溢误改。
