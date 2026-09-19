优先级：低

wconn replies.rs parse_bytes 锚点挂 ProcessReplyAsMemoryByteArray，实际形态对位 ProcessReplyAsMemoryByte（已入 ignore），锚点错挂待裁决

来源：qcode.rounds.md 第 8 轮 glm 系列 net 条（2026-09-19 08:20 前后收口，产物仅存台账，
本轮清账甄别后仍成立）。

现状（主仓 dev 实测）
- wedb/wconn/src/network/replies.rs:24 锚点注释
  `libs/client/GarnetClientProcessReplies.cs:ProcessReplyAsMemoryByteArray`，:25 fn parse_bytes。
- parse_bytes 返回 `Result<Option<Result<Vec<u8>>>>`，解析单条应答（+OK、token span、
  error、bulk 载荷），消费点仅 :214 `ReplyTx::Bytes(tx)` 单应答通道——形态是
  「一条应答 → 一个字节串」，即 C# ProcessReplyAsMemoryByte 的单元素语义。
- C# 侧两函数均活且形态不同：GarnetClientProcessReplies.cs:147
  ProcessReplyAsMemoryByte（out MemoryResult<byte>，单元素）与 :197
  ProcessReplyAsMemoryByteArray（out MemoryResult<byte>[]，完整数组，:257/:302 消费）。
  rust 无任何「完整数组」解析通道。
- js/check/ignore/client.yml:97 已登记 `GarnetClientProcessReplies.cs: ProcessReplyAsMemoryByte`
  （判「无转写需求」），但 rust parse_bytes 的实际形态恰是它的单元素语义；锚点挂的
  ByteArray 反而没有 1:1 形态对应。ignore 口径与锚点口径互相矛盾，读者无法判断
  parse_bytes 到底对位谁。

修法（二选一，消除矛盾）
- 改锚：parse_bytes 锚点改挂 ProcessReplyAsMemoryByte 并从 ignore/client.yml 移除该项
  （它有真实转写对位，不该在 ignore）；或
- 维持锚点并说明：若下游判定 parse_bytes 是 ByteArray 的泛化承接（Vec 承载单/多数组），
  在锚点注释里写明与 ProcessReplyAsMemoryByte 的边界及 Byte 入 ignore 的理由，
  使两套登记口径自洽。

验收：锚点、ignore 登记与代码形态三者互不矛盾；check.js A 层无新增报错。

甄别结论（2026-09-19 fixloop，C# 侧实测复核）

- 票据成立。C# GarnetClientProcessReplies.cs:147 ProcessReplyAsMemoryByte
  （out MemoryResult<byte>，单元素）：`+OK\r\n` 快路径（*(int*)(ptr+1)==168643407）、
  `+`/`:`/`-`/`$` 单值分支、`*` 数组取首元素 `result = resultArray[0]`（其余 Dispose）。
  :197 ProcessReplyAsMemoryByteArray（out MemoryResult<byte>[]，完整数组）：
  仅接受 `*` 前导，default 直接 ThrowException。
- rust parse_bytes（replies.rs:25）返回 Result<Option<Result<Vec<u8>>>>（单字节串）、
  +OK\r\n 快路径、`*`/`~`/`>` 取首元素（a.and_then(|v| v.first().copied())）——
  与 MemoryByte 单元素语义 1:1 吻合；rust 比 C# 多的 `,`/`#`/`_`/`~`/`>` 是
  parse_scalar/parse_array 共有的 RESP3 统一扩展，非形态分叉。
- 调用链闭环佐证：rust session.rs:93 execute_for_bytes_async 锚挂
  ExecuteForMemoryResultWithCancellationAsync → ReplyTx::Bytes → parse_bytes；
  C# 该 API 即 TaskType.MemoryByteAsync → ProcessReplyAsMemoryByte（:284）。
- ByteArray 消费场景为 ExecuteForMemoryResultArrayAsync / StringGetAsMemoryAsync
  （MGET 多键批量，MemoryResult<byte>[]），rust wconn 无此批量字节通道；
  数组应答统一走 parse_array（对位 ProcessReplyAsStringArray）。

采纳修法一（改锚）：

1. replies.rs:24 锚点 ProcessReplyAsMemoryByteArray → ProcessReplyAsMemoryByte，
   注释写明与 ByteArray 的边界（完整数组批量通道无 rust 对位，数组应答由
   parse_array 承接），并说明 RESP3 扩展超出 C# 原方法射程。
2. client.yml GarnetClientProcessReplies.cs 段：- ProcessReplyAsMemoryByte 改为
   - ProcessReplyAsMemoryByteArray（换位登记，函数级格式与
   CopyErrorToSpan/ProcessReplyAsNumber 一致，段级理由已覆盖 wconn 口径）。
3. 不改任何行为代码；仅注释锚点与 ignore 登记。
