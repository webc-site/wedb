# WithLengthHeader 整数族裁决（整族删除关闭）

来源意见
- next/ds.net.md 原条目 20：wresp WithLengthHeader 族对空数字按半包挂起，改法「补载荷完整但 digits_read==0 → Err 终态，或文档标注不可用于不可信输入」
- next/net.md 原条目 16：wresp 整数 WithLengthHeader 族对空数字按半包挂起，改法「保持刻意差异前提下补终态判定，或文档标注」

裁决
两条意见均按「整族删除」关闭，不补终态判定。理由：给死代码修边界行为是在维护无消费面的 API-parity 残留，与「死接口直接删，禁止留占位」红线相悖。

核实证据
- C# 侧：garnet/libs/common/RespReadUtils.cs:575 TryReadInt32WithLengthHeader、:616 TryReadInt64WithLengthHeader、:657 TryReadUInt64WithLengthHeader。全 garnet grep：TryReadInt64WithLengthHeader 与 TryReadUInt64WithLengthHeader 零生产调用；TryReadInt32WithLengthHeader 仅被 libs/client/RespReadResponseUtils.cs:254 转发（客户端工具面）。rust wconn 客户端应答解析为自有实现，不经该转发面。
- rust 侧：wedb/wresp/src/read.rs 的 try_read_i32_with_length_header / try_read_i64_with_length_header / try_read_u64_with_length_header 全仓 src+tests 零调用；try_read_i32 / try_read_i64（RespReadUtils.cs:TryReadInt32/TryReadInt64 对标）仅被 with_length_header 族内部调用，删族后成死链一并删除（try_read_i32_safe 同链连带）。rust 命令参数解析由 wnode session_parse_state 的 strict_i32/strict_i64 承接（C# ParseUtils.TryGetInt32/TryGetInt64 语义），无需 read.rs 保留该对标本体。
- 意见所述「零数字按半包挂起」的行为缺陷依附于该死族；族已删除，缺陷随之消亡，无需修。
- read.rs 其余 WithLengthHeader 成员（skip/slice/byte_array/bool/span/string/ptr/ptr_with_signed/string_response/string_array/double 族）被 wconn 客户端解析与 wresp/tests/main.rs 在用，全部保留。

落地
- 删 wresp/src/read.rs 五函数：try_read_i32_with_length_header、try_read_i64_with_length_header、try_read_u64_with_length_header、try_read_i32、try_read_i64（连带 try_read_i32_safe）
- js/check/ignore 登记 RespReadUtils.cs：TryReadInt32、TryReadInt32Safe、TryReadInt64、TryReadInt32WithLengthHeader、TryReadInt64WithLengthHeader、TryReadUInt64WithLengthHeader（API-parity，生产消费面已由 strict_i32/strict_i64 与 wconn 自有解析承接）
- 该两条意见不再挂账；后续 MIGRATE/检查点流立项如需整数 WithLengthHeader 读取，随链按 C# RespReadUtils 语义重写
