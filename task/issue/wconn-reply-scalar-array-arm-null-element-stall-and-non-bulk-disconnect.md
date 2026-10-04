客户端 Str/Bytes 应答形遇数组应答：null bulk 元素被误判半包致连接永久队头阻塞，非 bulk 元素被误判协议错致整连接拆除

问题分析：
1. Garnet 契约对齐
C# ProcessReplyAsString 与 ProcessReplyAsMemoryByte 的 case '*' 均转调 RespReadResponseUtils.TryReadStringArrayWithLengthHeader 取首元素。该函数元素臂完整覆盖 '$'（含 $-1 null，经 TryReadStringWithLengthHeader 置 null 返回 true）、'+' 简单串、'*' 嵌套（Join 成串，仅字符串重载）、其余按 TryReadIntegerAsString 读整数行。即标量应答形收到任意 RESP2 数组（含 null 元素、整数元素、简单串元素）都按帧完整消费并回首元素，绝不判半包，也不拆连接。

2. 工程现状确证
wedb/wconn/src/network/replies.rs 的 parse_scalar 与 parse_bytes 在 '*' 臂调用 RespReadResponseUtils::try_read_byte_slice_array_with_length_header（wedb/wconn/src/parser.rs）。该函数元素读闭包为：
Ok(Self::try_read_byte_slice_with_length_header(ptr)?.flatten())
缺陷一（null 元素）：元素为 $-1\r\n 时内层返回 Ok(Some(None))，flatten 后成 None，read_array_with 骨架把 None 解释为「该元素未到齐」，整体回滚返回 Ok(None)。claim 回 false，dispatch_replies 游标停在数组头前 break。后续读事件追加的字节只接在尾部，下一轮从同一游标重解析，必再命中同一 null 元素再回 None，形成确定性死循环式等待。
缺陷二（非 bulk 元素）：元素为 :1\r\n 或 +x\r\n 时进入 wresp::read::try_read_ptr_with_signed_length_header → try_read_signed_length_header，首字节与 '$' 不符直接 Err(UnexpectedToken)，经 dispatch_replies 的 ? 上抛，read_pump 返回 Err 退出，整条连接拆除。
另：同函数也被 session_frame_encode 测试作为二进制 APPENDLOG 帧解码臂使用，其「仅 bulk 元素」语义对复制帧正确，但被应答派发复用到标量应答形后语义不匹配。

3. 逻辑危害确证
公开 API GarnetClient::execute_for_string_result_async / execute_for_bytes_result_async 为通用命令入口（wedb/wconn/src/client.rs），对 MGET 含缺失键（*2\r\n$-1\r\n$1\r\na\r\n）、SMISMEMBER（整数数组）等常见命令即可触发。
缺陷一危害：当前命令应答永不交付，且其后全部在途命令被队头阻塞（读泵按 FIFO 认领），read_buf 持续累积后续应答字节无界增长；GarnetClientSession 无超时旋钮（progress 恒 None）时连接永久挂死、调用方 await 永不返回；GarnetClient 仅在开启超时旋钮且发送侧也无进展时才判超时拆连。
缺陷二危害：一条合法应答导致整连接断开，同连接上全部在途命令以 ResponseChannelClosed 失败，与 C# 返回首元素的行为分叉。

涉及代码：
rust 文件与函数：
wedb/wconn/src/network/replies.rs:parse_scalar
wedb/wconn/src/network/replies.rs:parse_bytes
wedb/wconn/src/network/replies.rs:dispatch_replies
wedb/wconn/src/parser.rs:RespReadResponseUtils::try_read_byte_slice_array_with_length_header
wedb/wconn/src/parser.rs:RespReadResponseUtils::read_array_with
wedb/wresp/src/read.rs:try_read_ptr_with_signed_length_header

对应 c# 文件与函数：
garnet/libs/client/GarnetClientProcessReplies.cs:ProcessReplyAsString
garnet/libs/client/GarnetClientProcessReplies.cs:ProcessReplyAsMemoryByte
garnet/libs/client/RespReadResponseUtils.cs:TryReadStringArrayWithLengthHeader
garnet/libs/common/RespReadUtils.cs:TryReadStringWithLengthHeader

精炼执行方案：
1. 在 parser.rs 新增标量形首元素读件（如 try_read_first_element_bytes），复用 read_array_with 骨架，元素臂对齐 C# 元素集：'$'（null 元素记空、按完整处理不回 None）、'+' ':' '-' ',' '#' 走 try_read_token_span、'_' 走 RESP3 null、'*' '~' '>' 嵌套走既有 depth+1 递归仅消费不取值；只保留首元素借用切片，其余元素仅推进游标不收集（零额外 Vec 分配）。
2. parse_scalar 与 parse_bytes 的数组闭包改调该读件；try_read_byte_slice_array_with_length_header 保留为复制帧专用（仅 bulk 元素），并把其元素闭包的 flatten 改为对 null 元素显式按完整处理（null 映射空切片或直接拒收报 UnexpectedToken，二选一并在文注写明），杜绝「完整帧判半包」这一死等形态。
3. 测试：wedb/wconn/tests/network_replies.rs 增三例——Str 形收 *2\r\n$-1\r\n$1\r\na\r\n 交付空串且游标消费完整帧、Bytes 形收 *2\r\n:1\r\n:0\r\n 交付 b"1" 不报错、同缓冲后续追加第二条应答能被正常认领（验证无队头阻塞）；wedb/wconn/tests/session_frame_encode.rs 增 null 元素帧不得返回 Ok(None) 的断言。
