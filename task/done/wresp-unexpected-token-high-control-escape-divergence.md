终态注记（2026-09-30 收口）：合入哈希 764d8f7（merge --no-ff fix-wresp-cc），实现 492c082。收口形态：violation_unexpected_token 转义判定臂由 u8::is_ascii_control（仅 0x00-0x1F、0x7F）扩为 matches!(token, 0x00..=0x1F | 0x7F..=0x9F)（Unicode Cc 全域），与 C# RespParsingException.cs:ThrowUnexpectedToken 的 char.IsControl 逐臂对一；转义格式保持 \x 两位小写十六进制不变，可打印臂零改动（0xA0-0xFF 两侧同直排原始字符）；0x80-0x9F 违例断连末帧 ERR Protocol Error 文案由 UTF-8 双字节原始落线收口为与 C# 逐字节全等的四字节 ASCII 转义。测试：resp_command_parse.rs malformed_array_header_raises_violation 补 0x80/0x85/0x9F 三 Cc 上段用例（断言 '\x80'/'\x85'/'\x9f'，含票面两边界 0x80/0x9F 与票面示例值 0x85）及 0x1F/0x7F（旧域仍转义）与 0xA0（Cc 域外走可打印臂）边界回归护栏；既有 'A'/'X'/负长度用例不回摆。定向 cargo check -p wnode --tests 与定向用例在合入基（含前序 exec-abort/shard-notify 同步基点）全绿。

甄别结论:通过(P3,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 甲轮45-G，P3 级）。violation_unexpected_token 仅用 u8::is_ascii_control 判定转义，遗漏 0x80-0x9F 控制字节，与 C# char.IsControl 转义文案逐字节分叉事实确证；属断连前末帧错误文案分叉。执行席遵照：判定区间扩为 matches!(token, 0x00..=0x1F | 0x7F..=0x9F)，保持 \xNN 格式不变，补全单测。

原票面：
协议违例 Unexpected character 文案对 0x80-0x9F 控制字节的转义判定与 C# 分叉


问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# libs/common/Parsing/RespParsingException.cs:ThrowUnexpectedToken(byte token)
以 char.IsControl((char)token) 判定转义：Unicode Cc 类别覆盖 U+0000-U+001F、
U+007F-U+009F，故字节 0x80-0x9F 命中控制字符臂，文案写 ASCII 转义序列
\xNN（四字节字面反斜杠 x 两位小写十六进制），如 0x85 产出
Unexpected character '\x85'.（纯 ASCII）。该文案经 RespServerSession.cs
catch 块 TryWriteError($"ERR Protocol Error: {ex.Message}") 原样上帧。

2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wnode parse.rs violation_unexpected_token 以 u8::is_ascii_control 判定，
该判据仅覆盖 0x00-0x1F 与 0x7F，不含 0x80-0x9F；未命中臂以
(token as char).to_string() 直排，0x85 即排 U+0085，经 abort_error_message
以 UTF-8 落线为双字节 0xC2 0x85，与 C# 的四字节 ASCII 转义 \x85 逐字节
不同。两侧其余区间（0x00-0x1F、0x7F、可打印区、0xA0-0xFF）逐字节同形。
可达性：数组头/长度头/终止符各违例臂把接收缓冲原始字节传入
Error::UnexpectedToken，任何含 0x80-0x9F 字节的畸形帧（如 *2\r\n$<0x85>...）
即触发。deviations.md 在册条目与 task 票池均无此面裁决（§110 系 BITFIELD
参数回显 ASCII 折叠面，非解析器违例哨兵面）。

3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
双侧同为协议违例断连，无 panic、无资源危害；危害面仅限错误帧字节内容：
同批此前应答之后的 ERR Protocol Error 帧文本与 C# 原型不逐字节全等，
偏离仓内「错误契约对齐/应答逐字节全等」判据（对照面为断连前最后一帧）。
低危。


涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/resp_server_session/parse.rs:violation_unexpected_token

对应 c# 文件与函数：
garnet/libs/common/Parsing/RespParsingException.cs:ThrowUnexpectedToken


精炼执行方案：
1. violation_unexpected_token 的判定臂由 is_ascii_control 扩为
matches!(token, 0x00..=0x1F | 0x7F..=0x9F)（Cc 全域），转义格式保持
\x 两位小写十六进制不变；可打印臂不动。
2. 测试验证点：wnode/tests/resp_command_parse.rs malformed_array_header_raises_violation
补 0x80 与 0x9F 两边界用例，断言哨兵文案为
"Unexpected character '\\x85'." / "Unexpected character '\\x9f'."；
既有 0x00-0x1F、0x7F 与可打印用例回归不回摆。
