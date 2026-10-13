终态：合入 4ccd5a6，try_read_byte_slice_array_with_length_header 就地扩元素臂（$-1/_ null 容空切片、+ : - , # 行读借用、* ~ > 嵌套仅消费经 depth 熔断，replies.rs 与既有测试调用点补 depth=1），删 flatten 误判半包死代码，无旧函数副本。

审核结论：通过（2026-10-04 审核席）

审核核实：
1. 缺陷一亲核属实：wedb/wconn/src/parser.rs:128-130 元素闭包 try_read_byte_slice_with_length_header(ptr)?.flatten()，$-1 元素内层 Ok(Some(None)) 经 flatten 成 None，read_array_with（:182-184）按「元素未到齐」整体回滚回 Ok(None)；claim 回 false，dispatch_replies 游标停帧头 break，后续字节只追加尾部，下轮同点重解析必再回 None，确定性永久队头阻塞。另补一形：RESP3 null 元素 _\r\n 经 wresp::read::try_read_signed_length_header（wedb/wresp/src/read.rs:93-97）同判 length=-1 → None，同样死等。
2. 缺陷二亲核属实：wedb/wresp/src/read.rs:100-102 首字节非 '$' 直接 Err(UnexpectedToken)，经 claim/dispatch_replies 的 ? 上抛，读泵（wedb/wconn/src/network/pump.rs:279 调用点）退出拆连。
3. 可触发性亲核：parse_scalar（replies.rs:126-141）与 parse_bytes（replies.rs:102-119）是 ReplyTx::Str / ReplyTx::Bytes 唯一解析臂；公开 API GarnetClient::execute_for_string_result_async / execute_for_bytes_result_async（wedb/wconn/src/client.rs:214/219）接受任意命令，MGET 含缺失键、SMISMEMBER、EXISTS 类数组应答即可送入。
4. C# 契约亲核：garnet/libs/client/GarnetClientProcessReplies.cs:53-58（ProcessReplyAsString）与 :180-187（ProcessReplyAsMemoryByte）case '*' 转调 RespReadResponseUtils.TryReadStringArrayWithLengthHeader（:162 字符串重载含 '*' 嵌套 Join 臂；:212 MemoryPool 重载无嵌套臂），元素 '$' 走客户端门面 TryReadStringWithLengthHeader（:77-91，经 TryReadPtrWithSignedLengthHeader 容 null 返回 true）、'+' 简单串、其余按整数行读。完整帧绝不判半包。
5. 查重：doc/zh/deviations.md 不存在（doc/zh 仅 db.md/collection.md）；task/done|reject|todo|ing 无同面裁决。task/done/wconn-parser-nested-array-unbounded-recursion-stack-abort.md 仅记「bytes/scalar 两形仅 $ 元素臂不涉深度面」，与本票正交。

订正：
1. 票面 C# 引用 garnet/libs/common/RespReadUtils.cs:TryReadStringWithLengthHeader 有误：该 common 件（:842）走 TryReadSpanWithLengthHeader → TryReadUnsignedLengthHeader，不容 $-1。真实 null 容忍件为 garnet/libs/client/RespReadResponseUtils.cs:TryReadStringWithLengthHeader（:77）。
2. 原方案「新增 try_read_first_element_bytes + 保留旧函数为复制帧专用」否决：try_read_byte_slice_array_with_length_header 生产调用方仅 replies.rs 两处（另一处为 tests/session_frame_encode.rs），改道后旧函数只剩测试引用，成死代码且两套数组元素臂并存，违判定标准 3。改为原函数就地扩元素臂，单机制。

客户端 Str/Bytes 应答形遇数组应答：null bulk 元素被误判半包致连接永久队头阻塞，非 bulk 元素被误判协议错致整连接拆除

问题分析：
1. Garnet 契约对齐
C# ProcessReplyAsString 与 ProcessReplyAsMemoryByte 的 case '*' 均转调 RespReadResponseUtils.TryReadStringArrayWithLengthHeader 取首元素。该函数元素臂覆盖 '$'（含 $-1 null，经客户端门面 TryReadStringWithLengthHeader 置 null 返回 true）、'+' 简单串、'*' 嵌套（Join 成串，仅字符串重载）、其余按 TryReadIntegerAsString 读整数行。即标量应答形收到 RESP2 数组（含 null 元素、整数元素、简单串元素）都按帧完整消费并回首元素，不判半包，不拆连接。

2. 工程现状确证
wedb/wconn/src/network/replies.rs 的 parse_scalar 与 parse_bytes 在 '*' 臂调用 RespReadResponseUtils::try_read_byte_slice_array_with_length_header（wedb/wconn/src/parser.rs）。该函数元素读闭包为：
Ok(Self::try_read_byte_slice_with_length_header(ptr)?.flatten())
缺陷一（null 元素）：元素为 $-1\r\n 或 _\r\n 时内层返回 Ok(Some(None))，flatten 后成 None，read_array_with 骨架把 None 解释为「该元素未到齐」，整体回滚返回 Ok(None)。claim 回 false，dispatch_replies 游标停在数组头前 break。后续读事件追加的字节只接在尾部，下一轮从同一游标重解析，必再命中同一 null 元素再回 None，确定性永久等待。
缺陷二（非 bulk 元素）：元素为 :1\r\n 或 +x\r\n 时进入 wresp::read::try_read_ptr_with_signed_length_header → try_read_signed_length_header，首字节与 '$' 不符直接 Err(UnexpectedToken)，经 dispatch_replies 的 ? 上抛，read_pump 返回 Err 退出，整条连接拆除。

3. 逻辑危害确证
公开 API GarnetClient::execute_for_string_result_async / execute_for_bytes_result_async 为通用命令入口（wedb/wconn/src/client.rs），对 MGET 含缺失键（*2\r\n$-1\r\n$1\r\na\r\n）、SMISMEMBER（整数数组）等常见命令即可触发。
缺陷一危害：当前命令应答永不交付，其后全部在途命令被队头阻塞（读泵按 FIFO 认领），read_buf 持续累积后续应答字节无界增长；无超时旋钮时连接永久挂死、调用方 await 永不返回。
缺陷二危害：一条合法应答导致整连接断开，同连接全部在途命令以 ResponseChannelClosed 失败，与 C# 返回首元素行为分叉。

涉及代码：
rust 文件与函数：
wedb/wconn/src/parser.rs:RespReadResponseUtils::try_read_byte_slice_array_with_length_header
wedb/wconn/src/parser.rs:RespReadResponseUtils::read_array_with
wedb/wconn/src/network/replies.rs:parse_scalar
wedb/wconn/src/network/replies.rs:parse_bytes
wedb/wconn/src/network/replies.rs:dispatch_replies
wedb/wresp/src/read.rs:try_read_ptr_with_signed_length_header
wedb/wconn/tests/network_replies.rs
wedb/wconn/tests/session_frame_encode.rs

对应 c# 文件与函数：
garnet/libs/client/GarnetClientProcessReplies.cs:ProcessReplyAsString
garnet/libs/client/GarnetClientProcessReplies.cs:ProcessReplyAsMemoryByte
garnet/libs/client/RespReadResponseUtils.cs:TryReadStringArrayWithLengthHeader
garnet/libs/client/RespReadResponseUtils.cs:TryReadStringWithLengthHeader

精炼执行方案：
1. parser.rs 就地改 try_read_byte_slice_array_with_length_header 元素闭包（不新增读件，不改签名 Result<Option<Option<Vec<&[u8]>>>>，仍单函数服务 parse_scalar/parse_bytes 与复制帧测试），增 depth 参数对齐 try_read_string_array_with_length_header 的熔断形态，按 ptr[0] match：
   '$' → try_read_byte_slice_with_length_header(ptr)?.map(|b| b.unwrap_or_default())（null 记空切片、按完整处理，不再 flatten）
   '+' ':' '-' ',' '#' → try_read_token_span(ptr, ptr[0])（借用行体，零拷贝）
   '_' → try_read_token_span(ptr, b'_')?.map(|_| &[][..])
   '*' '~' '>' → 自递归 depth+1 仅消费，元素记空切片（C# MemoryPool 重载本无嵌套臂，文注写明为 RESP3 扩展偏差）
   其余 → Err(unexpected_token)
   同步订正函数文注（元素集、null 语义、对位 C# 客户端门面 TryReadStringWithLengthHeader）。
2. replies.rs parse_scalar / parse_bytes 调用点补 depth 实参 1，其余取首元素逻辑不动；parse_scalar 的首元素 bulk_text(Some(slice)) 对 null 元素得空串，与 C# 置 null → 空结果同义。
3. 测试闭环：
   wedb/wconn/tests/network_replies.rs 增：Str 形收 *2\r\n$-1\r\n$1\r\na\r\n 交付空串且 read_head 追平整帧；Bytes 形收 *2\r\n:1\r\n:0\r\n 交付 b"1" 且 dispatch_replies 返回 Ok；同缓冲紧随第二条 +OK\r\n 应答被第二个在途项正常认领（验证无队头阻塞）。
   wedb/wconn/tests/session_frame_encode.rs 增：*2\r\n$-1\r\n$1\r\na\r\n 返回 Ok(Some(Some([b"", b"a"]))) 而非 Ok(None)；*1\r\n_\r\n 同理；既有 append_log 往返与半包回滚两例保持通过。
   改完运行 ./test.sh。
