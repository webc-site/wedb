终态：已修复合入（2026-09-30，fix-wconn-depth 分支，合入哈希 f3143ef，实现提交 0dba805）。收口形态：read_array_with 骨架增设 depth 参数单点连接级嵌套计数（顶层 1，字符串数组嵌套臂逐层 +1），超 MAX_NEST_DEPTH=128 返回 Error::UnexpectedToken 沿读泵既有 Err 收场断连；bytes 臂无递归臂、顶层直传 1 过同一骨架熔断，单一机制无冗余、未增大调用栈。tests/parser_nest_depth.rs 经 GarnetClient+假端点生产链路闭环：十万层深嵌套拒收不 abort、128/129 层边界成对锁死、浅嵌套混合 sigil 与 null 嵌套回归；cargo check 与定向 cargo test（wconn parser lib 7 项 + session_frame_encode 3 项 + 新集测 4 项）全绿。范围收口：全仓 grep 复核嵌套集合递归臂（`* ~ >` 元素自递归）仅 wconn parser.rs 一处——wresp 侧无数组嵌套件，bytes/scalar 两臂不经嵌套不涉本面，无遗留同面。

甄别结论:通过(P2,2026-09-30 现码复核,双侧锚亲验成立,五池无重复,deviations 无同面在册裁决)

审核结论：通过（2026-09-30 甲轮45-B，P2 级）。try_read_string_array_with_length_header 对嵌套集合元素无界自递归事实确证，对端深嵌套帧可致栈溢出进程 abort；C# 原型同名件仅认 $/: 元素臂无递归，系 rust 扩展臂缺陷。执行席遵照：read_array_with 骨架或参数增设连接级嵌套深度计数，超限（如 128 层）返回 Error::UnexpectedToken 走正常断连收场；禁止盲目增大调用栈。

原票面：
wconn 应答方向嵌套集合臂无界自递归，深嵌套帧栈溢出整进程 abort（客户端应答方向解析复审）

一句话：wconn/wresp 客户端应答解析的字符串数组臂对嵌套集合元素（* ~ >）无深度上限自递归，对端发约数万层 *1\r\n 嵌套帧（数十 KB 载荷）即栈溢出，进程级 abort 不可捕获；C# 原型同名件只认 $ 与 : 两元素臂、无嵌套无递归，该 panic 面为 rust 扩展臂新引入。

rust 侧：
wedb/wedb/wconn/src/parser.rs try_read_string_array_with_length_header 元素臂 b'*' | b'~' | b'>' 直接递归调用自身（read_array_with 骨架 → read_elem 闭包回入本函数），parse_array_header 只校验 sigil 与长度，全链无深度计数、无嵌套层数上限；每层最小入帧 *1\r\n 仅 4 字节。
wedb/wedb/wconn/src/network/replies.rs parse_array（ReplyTx::Array 应答方向）消费该函数，集群内部客户端应答路径可达；嵌套帧自对端（集群节点）经读泵进入，read_pump 无帧深预检。
栈溢出表现为 guard page SIGSEGV abort，非 Result 可捕获错误，read_pump 的 Err 收场链（断连、在途 oneshot 回传）接不住。

C# 侧：
garnet/libs/common/RespReadUtils.cs:1092 TryReadStringArrayWithLengthHeader 元素循环仅两臂：*ptr == '$' 走 TryReadStringWithLengthHeader，否则走 TryReadIntegerAsString（仅 ':' sigil，异 sigil 掷 RespParsingException 断连收场）；无嵌套集合分支，无任何递归，故 C# 不存在本面。
garnet/libs/client/GarnetClientProcessReplies.cs ProcessReplyAsStringArray 经 RespReadResponseUtils 转调上述 common 件，同无递归面。

对照说明：
parser.rs 头注自述「元素臂扩至 RESP3（- 错误行 / , 浮点 / # 布尔 / _ null / ~ > 嵌套集合），wresp::read 同名件不容这些形态，保留门面本地组合」，嵌套臂属有意的原型外扩展；扩展本身未错，缺的是深度上限（RESP 服务器实现通例为数百层封顶后掷协议错误）。

判定：
属真实 panic 面（网络输入可达的栈溢出 abort，判据内第一类缺陷），定级 P2；触发需对端帧控制权（集群内部互信信道、被控或缺陷节点即可），后果为宿主进程整体崩非连接级收场。修复建议：read_array_with 增连接级深度参数（或线程局部计数），超限（如 128/512 层）返回 Error::UnexpectedToken 走既有断连收场；bytes/scalar 两形（try_read_byte_slice_array_with_length_header 仅 $ 元素臂）不涉本面。

查重：
task/todo 28 张票无 wconn/parser 递归面（wext-roaring、wvector 等票的 dos/alloc 面均不在应答解析方向）；task/issue 7 张票无本面（wresp-unexpected-token-high-control-escape-divergence 票为 wresp 错误文本回显分叉，非递归）；doc/zh/deviations.md 无数组元素臂扩展与递归深度相关在册裁决。
