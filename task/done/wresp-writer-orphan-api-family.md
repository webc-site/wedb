归档注记（主代理 2026-09-26 fix.md 波批合并）：收口 6ea9864（P4，他席先落、本波 wrespwriter 沙箱席零差复验判称已执行未归档，主代理亲验 numstat 四文件净 -155 与票面方案逐条对应、五 API 名代码面零残留、write_map_length(0) 双协议帧锁 :30/:52 在场），收口形态：孤儿写出口族全删＋唯一空 map 机制收敛＋intra-doc 悬空防固，零新提交禁空提。

wresp 写出面孤儿 API 族：RESP3 大数/批量错误等五写入口零生产消费，违零死代码与单一机制纪律

甄别结论：通过（甄别席 zc-fix-r20-wresp-sholing，2026-09-26）定级 P4

甄别核验记录（逐点现码复跑，全程只审不改）：
1. rust 侧消费面全仓穷举 grep（wnode/wcol/wlua/wpubsub/wmetric/wedb/bench/regress 及全部测试）：write_bignum/write_bulk_error/write_empty_map 生产零消费、仅 wresp/tests/writer.rs 自证（:28/:50/:231/:232/:236/:237/:298/:310）；write_null_bulk_string 全仓零引用；wresp write_utf8_bulk_string 仅测试 :63；wnode 门面 write_utf8_bulk_string 零消费；wnode 门面 write_bool 仅 wnode/tests/session_output.rs:41/:45/:93/:97 测试消费。属实。
2. 纯别名亲验：Resp2::write_null_bulk_string（:191-193）出 $-1\r\n、Resp3 臂（:263-265）出 _\r\n，与 write_null（:181-183/:253-255）逐字节同帧；RespWriter::write_utf8_bulk_string（:525-527）与 write_ascii_bulk_string（:518-520）同为 write_bulk_string(chars.as_bytes())。
3. C# 侧亲验：libs 全仓 grep BigNumber 仅 LuaRunner.Strings.cs:57/:197 与 LuaRunner.cs:1036 Lua 类型标记串，BulkError 零命中，NullBulkString 唯一命中 CustomProcedureBase.cs:158（protected static 内部即调 TryWriteNull，全仓零调用死成员）；WriteEmptyMap 唯一服务端消费 BasicCommands.cs:1260 COMMAND DOCS 失败臂 else 分支；RespServerSessionOutput.cs:268 private WriteUtf8BulkString 文件内零调用；RespServerSessionOutput.cs 全文无 WriteBool。C# 无 BigNumber/BulkError/NullBulkString 写出原语属实；C# 同名死成员两处系票面微瑕自陈，方向反佐证。
4. 保留面亲验：RespProtocol::write_bool 本体（trait :150/Resp2 :218/Resp3 :293/RespWriter 包装 :668）活——wnode vectors.rs:210 直接 Resp3::write_bool 不经门面；wlua resp_convert.rs:410-411 经 RespWriter::write_resp3_bool（:674-676）至 Resp3::write_bool。删 wnode 门面 write_bool/write_utf8_bulk_string 不停本体。
5. 空 map 双机制亲验：wnode basic_commands/mod.rs:262-264 失败臂实走 writer.write_map_length(0)，Resp3 出 %0\r\n、Resp2 出 *0\r\n（saturating_mul(2)=0），与 Resp3::write_empty_map（:333-335）/Resp2::write_empty_map（:239-241）逐字节同帧，与 C# WriteEmptyMap（RespMemoryWriter.cs:191 经 TryWriteEmptyMap RespWriteUtils.cs:739）同帧。
6. 非重复非灭失：doc/zh/deviations.md 全册五 API 名与 BigNumber/BulkError 零命中，「孤儿/死代码」命中三条均在删空自愈/到期堆项/段文件回滚语境非本域；task/ing/done/reject/issue 池五 API 名唯一命中本票；doc/ 全目录零引用；wresp lib.rs re-export 不涉五 API。
7. 架构合规与可执行度：三步删除最小（trait 签名+Resp2/Resp3 双臂+RespWriter 包装含文档注释，再 wnode 门面两枚，再测试连带清理并补 write_map_length(0) 双协议锁测）；wresp 0.1.4 发布元数据不构成保留依据（review.md 板块 1 明定彻底无视向下兼容，rust_review 第 12 条 pub 孤儿定期清理）；验收闭环（cargo build 全 workspace + cargo test -p wresp -p wnode + ./test.sh + grep 五 API 名零残留）成立。
8. 执行连带提示：resp_memory_writer.rs:97-105 write_error_frame_to 文档注释内含 Resp2::write_bulk_error 的 intra-doc 链接（:103），删除时须同步改写该句防 rustdoc 悬空断链。
9. 格式：纯文本无加粗/表格/横线，双侧代码路径齐全。

审核结论：通过（席位 zcode-r20-review-orphanapi，2026-09-26，dev 分支）

审核亲验记录（全仓含 wnode/wcol/wlua/wpubsub/wmetric/wedb/wedb_standalone 穷举 grep 双侧亲读）：
1. 五枚 API 零生产消费属实：write_bignum/write_bulk_error/write_empty_map 仅 wresp/tests/writer.rs 自证（:28/:50/:231/:232/:236/:237/:298/:310），write_null_bulk_string 全仓零引用，wresp 侧 write_utf8_bulk_string 仅测试 :63；wnode 门面 write_bool 仅 wnode/tests/session_output.rs:41/:45/:93/:97 消费、门面 write_utf8_bulk_string 零消费。
2. 同帧冗余属实：write_null_bulk_string 双臂与 write_null 逐字节同帧（$-1\r\n / _\r\n）；write_empty_map 与 write_map_length(0) 同帧（*0\r\n / %0\r\n）；write_utf8_bulk_string 与 write_ascii_bulk_string 同为 write_bulk_string(chars.as_bytes())。COMMAND DOCS 失败臂（basic_commands/mod.rs write_command_docs_p）实走 write_map_length(0)。
3. C# 对物属实：BigNumber 全仓唯一命中 LuaRunner.Strings.cs:57 类型标记串；BulkError 零命中；WriteEmptyMap 唯一服务端消费 BasicCommands.cs:1260；WriteUtf8BulkString 服务端零活消费（仅客户端 GarnetClient.cs:700/702/1018）；RespServerSessionOutput.cs 无 WriteBool。
4. 保留面正确：P::write_bool 本体有活消费（vectors.rs:210 Resp3::write_bool、wlua/runner/resp_convert.rs:411 经 write_resp3_bool），wnode 门面 write_resp3_bool 亦活，均不删。
5. 发布属性裁量：wresp 具发布 crate 元数据（0.1.4），但 review.md 板块 1 明定不向下兼容、rust_review 第 12 条明定 pub 孤儿定期清理，删除有纪律依据。
6. 票面两处微瑕不构成反证、方向反佐证：C# CustomProcedureBase.cs:158 实有 WriteNullBulkString（protected static，内部即调 TryWriteNull 出 $-1\r\n，全仓零调用，同为 null 别名死成员）；RespServerSessionOutput.cs:268 实有 private WriteUtf8BulkString（文件内零调用死成员）。
7. deviations.md 查重零登记（全册 grep 五 API 名与孤儿/死代码零命中）。

优化执行方案（供 task/fix.md 直接消费）：
1. wresp/src/resp_memory_writer.rs：删 RespProtocol trait 的 write_bignum、write_bulk_error、write_null_bulk_string、write_empty_map 四方法签名（含文档注释）与 Resp2/Resp3 双臂实现；删 RespWriter 包装 write_bignum(:699-701)、write_bulk_error(:708-710)、write_null_bulk_string(:650-652)、write_empty_map(:632-634)、write_utf8_bulk_string(:522-527)。保留 write_map_length(0) 为唯一空 map 出帧机制；write_bool 本体与 write_resp3_bool 全保留。
2. wnode/src/resp/resp_server_session_output.rs：删门面 write_bool(:183-187) 与 write_utf8_bulk_string(:171-175)。保留 write_resp3_bool 门面。
3. 测试连带清理：wresp/tests/writer.rs 删 test_bignum_and_bulk_error_frames 整测、test_error_frame_single_point_sanitization 内两处 bulk_error 断言块（RESP2 退化臂与 RESP3 长度前缀臂）、test_protocol_aware_lengths 内 write_empty_map 两断言块、test_strings_and_ints 内 write_utf8_bulk_string 断言块；wnode/tests/session_output.rs 删 write_bool 四断言块（RESP2 :1/:0 与 RESP3 #t/#f）。
4. 加固补测（审查档建议采纳）：在 wresp 帧锁或 wnode session_output 测试补 write_map_length(0) 双协议断言（RESP2 *0\r\n、RESP3 %0\r\n），对齐 C# WriteEmptyMap 帧型锁面。
5. 验收：cargo build 全 workspace + cargo test -p wresp -p wnode + ./test.sh 全绿；grep 五 API 名全仓零残留（含文档注释与 re-export）。

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
   C# 写出面全量盘点：RespWriteUtils.cs 与 RespMemoryWriter.cs 根本不存在 BigNumber（( 前缀帧）与 Bulk Error（! 前缀帧）写出原语，全仓唯一 "BigNumber" 命中是 Lua 类型标记字符串（LuaRunner.Strings.cs:57）；WriteNullBulkString 亦不存在（null 双形态只有 WriteNull / WriteNullArray 两口）。WriteEmptyMap 在 C# 有真实消费点（BasicCommands.cs:1260 COMMAND DOCS 失败臂 writer.WriteEmptyMap()）；WriteUtf8BulkString 的消费点全在客户端请求编码面（GarnetClient.cs:700/702/1018），服务端应答面零消费。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
   wedb/wresp/src/resp_memory_writer.rs 定义了超出 C# 写出契约的投机接口族，全仓（wedb/ 全 workspace 含 wnode/wcol/wlua/wpubsub/wmetric/wedb）穷举 grep 亲验零生产调用：
   a) RespProtocol::write_bignum（Resp2/Resp3 双臂 :223-225/:297-303）+ RespWriter::write_bignum（:699-701）——仅 wresp/tests/writer.rs:231/:236 测试自证；
   b) RespProtocol::write_bulk_error（Resp2/Resp3 双臂 :228-231/:306-315）+ RespWriter::write_bulk_error（:708-710）——仅测试 :232/:237/:298/:310 消费；
   c) RespProtocol::write_null_bulk_string（双臂 :190-193/:262-265）+ RespWriter::write_null_bulk_string（:650-652）——全仓零引用（连测试都没有），且 RESP3 形与 write_null 逐字节同为 _\r\n、RESP2 形与 write_null 同为 $-1\r\n，纯冗余别名；
   d) RespProtocol::write_empty_map（双臂 :238-241/:332-335）+ RespWriter::write_empty_map（:631-634）——零生产消费；行为对位点（COMMAND DOCS 失败臂 basic_commands/mod.rs:259 write_command_docs_p）实走 write_map_length(0)（RESP3 %0\r\n / RESP2 *0\r\n，与 C# WriteEmptyMap 逐字节同帧），同帧双机制并立且 C# 命名的那套是死支；
   e) RespWriter::write_utf8_bulk_string（:524-527）——与 write_ascii_bulk_string 同为 chars.as_bytes() 逐字节同帧，服务端零消费；
   f) 连带会话门面两枚同步孤儿：wnode/src/resp/resp_server_session_output.rs write_bool（:185-187，仅 wnode/tests/session_output.rs 消费）与 write_utf8_bulk_string（:173-175，零消费），C# RespServerSessionOutput.cs 分部类无此二员。
   违反 task/review.md 板块 1「零死代码与假桩清退：清理未引用的孤儿逻辑」「接口最小暴露」「全链路唯一机制」，及 .agents/skills/rust_review/SKILL.md 第 12 条「pub API 孤儿（零调用导出函数）定期清理，删除后连带清理测试与文档引用」。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
   无运行期危害（P4 工程纯洁级）：危害在维护面与纪律面——投机保留的 RESP3 全类型写出口诱导后续轮次误以为存在大数/批量错误契约面（C# 无此契约），write_empty_map/write_null_bulk_string 与活机制同帧双轨，违单一机制纪律并扩大审查与测试面。
4. 查重声明：deviations.md 全册（150 条）grep 'write_bignum/write_bulk_error/write_empty_map/write_null_bulk_string/write_utf8_bulk_string/孤儿/死代码' 零登记；r15-contract 线索 14 仅核 write_double 判净、r16-server 第 8 线核帧型族判净但均未扫写出口消费面，本案不与任何在册登记重叠。

涉及代码：
rust 文件与函数：
wedb/wresp/src/resp_memory_writer.rs:RespProtocol::write_bignum / write_bulk_error / write_null_bulk_string / write_empty_map、RespWriter::write_bignum / write_bulk_error / write_null_bulk_string / write_empty_map / write_utf8_bulk_string
wedb/wnode/src/resp/resp_server_session_output.rs:RespServerSession::write_bool / write_utf8_bulk_string

对应 c# 文件与函数：
garnet/libs/common/RespWriteUtils.cs（无 BigNumber/BulkError/NullBulkString 原语的缺失证明面；TryWriteEmptyMap :739）
garnet/libs/common/RespMemoryWriter.cs:WriteEmptyMap（:191-203）/ WriteUtf8BulkString（:428-432，仅客户端面消费）
garnet/libs/server/Resp/BasicCommands.cs:NetworkCOMMANDDOCS（:1260 WriteEmptyMap 唯一服务端消费点）
garnet/libs/server/Resp/RespServerSessionOutput.cs（无 WriteBool/WriteUtf8BulkString 会话分部成员）

精炼执行方案：
1. 删除 RespProtocol trait 的 write_bignum、write_bulk_error、write_null_bulk_string、write_empty_map 四方法及 Resp2/Resp3 双臂实现与 RespWriter 对应包装、write_utf8_bulk_string 包装（trait 签名、实现、文档注释一并清）；保留 write_map_length(0) 单一空 map 出帧机制。
2. 删除 wnode 会话门面 RespServerSession::write_bool 与 write_utf8_bulk_string（P::write_bool 本体保留——vector 回帧 vectors.rs:210 与 wlua resp_convert.rs:411 经 Resp3::write_bool/write_resp3_bool 真实消费）。
3. 测试验证点：连带清理 wresp/tests/writer.rs 中仅覆盖死支的断言（bignum/bulk_error/empty_map/utf8 独立断言行）；保留并确认 test_protocol_aware_lengths 的活机制断言（map/set/push/null 族）与 wnode/tests/session_output.rs 的 write_bool 门面断言随门面一并删除；cargo build + 既有帧锁全绿。
