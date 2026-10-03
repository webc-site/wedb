终态注记: 已合入 main（commit: 40d895f）。收口形态：doc/zh/deviations.md 册尾顺编落册 §190（MGET 缺键/对象键 RESP3 分支返回 _\r\n），订正 wnode/tests/resp3_null_parity.rs 注释锚为 §190，消除对 C# RespServerSessionOutput 锚点误述，保持全仓 RESP3 现代化 null 单源输出一致。

甄别结论:通过(P3,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 甲轮45-D，P3 级）。MGET 在 RESP3 会话下缺键回 _\r\n 而 C# 原型全链恒写协议恒定 $-1\r\n 字节分叉事实确证；属数据结构响应逐字节对标范畴，无崩溃与数据面危害。执行席遵照：二选一定裁，若维持 RESP3 现代 null 形则在 deviations.md 顺编落册并订正测试注记，若对标 C# 则 do_network_mget 缺键/对象键两臂改回 $-1\r\n 并同步改锁测。

原票面：
MGET 缺键 null 帧 RESP3 分支与 C# 原型逐字节分叉（RESP2/RESP3 双协议应答分支覆盖复审，乙轮36-B）

一句话：rust MGET 在 HELLO 3 会话下缺键/对象键臂回 RESP3 null（_\r\n），C# 原型 MGET 全链恒写协议恒定 $-1（\r\n），双分支逐字节对账不相等。

rust 侧：
wedb/wedb/wnode/src/resp/array_commands.rs network_mget 按 is_resp3(self.resp_protocol_version) 分派 RespWriter Resp3/Resp2 类型参，do_network_mget 内 WrongType 与 Missing 两臂均 writer.write_null()，随 P 单态化出帧（Resp3 即 _\r\n）。
wedb/wedb/wresp/src/resp_memory_writer.rs Resp3::write_null 落 _\r\n、Resp2::write_null 落 $-1\r\n。
锁定该形的测试：wedb/wedb/wnode/tests/resp3_null_parity.rs MGET 含缺键臂，断言 RESP3 会话下逐键帧为 _\r\n，其注释称「版本随会话」「对位 C# RespServerSessionOutput.cs:WriteNull 版本裁决」。

C# 侧：
garnet/libs/server/Resp/ArrayCommands.cs NetworkMGET 仅走 scatter-gather 批（MGetReadArgBatch_SG），不经会话 WriteNull。
garnet/libs/server/Resp/MGetReadArgBatch.cs SetOutput 的 pendingNullWrite 臂与 CompletePending 排水 not-found 臂均直调 RespWriteUtils.TryWriteNull。
garnet/libs/common/RespWriteUtils.cs TryWriteNull 恒写 $ -1 \r\n，全文件无 respProtocolVersion 参与；TryWriteResp3Null（_\r\n）在 MGET 链路零消费点。
即 C# MGET 缺键臂对 respProtocolVersion 全盲，RESP3 会话下仍回 $-1\r\n。

对照面（证明 C# 的版本感知 null 是逐点选择而非普适）：
HMGET 缺键经 garnet/libs/server/Objects/Hash/HashObjectImpl.cs HashMultipleGet 的 RespMemoryWriter(respProtocolVersion).WriteNull()，resp3 分支存在，rust HMGET 用 write_resp_null_ver 与之一致；GET 缺键经 BasicCommands 会话级 WriteNull（RespServerSessionOutput.cs 版本分支），rust 一致。MGET 是 C# 侧独有不随版本的 null 位点。

判定：
属真实双分支分叉（仅 HELLO 3 后 MGET 响应数组内 null 元素字节形：C# $-1\r\n 对 rust _\r\n；RESP2 两分支逐字节相等）。功能语义等价、无崩溃无数据危害，定级 P3；按「数据结构响应逐字节全等」契约与 §108/§114 同谱 rust 优侧纯登记先例，须登记裁决（维持 rust 形则补登 deviations 并订正 resp3_null_parity.rs 注释的错误 C# 锚；按 C# 收敛则 do_network_mget 两 null 臂改协议恒定 $-1 并同步改锁测）。

查重：
task/todo 28 张票无 MGET 帧;task/done 仅 wnode-lcs-fast-path-session-metrics-missing.md 触及 do_network_mget 计数面（非帧形）；deviations.md §112（应答文本形态条）判据不可回收不覆盖本面；task/ing/resp-null-protocol-single-source（resp3_null_parity.rs 头注所引验收票）已不在池，其裁决不可回收，本分叉无在册裁决。
