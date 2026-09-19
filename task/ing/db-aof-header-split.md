优先级：低
来源：next/agy.db.md 条 10 立项。取证基线：主仓 dev 当下代码。

问题
waof aof/header.rs 735 行汇集 5 种协议头（基础头、分片头、单日志事务头、分片事务
头、分块大值头）+ 头类型枚举，全部手写位级解析打包同文件，单文件承载整个 AOF 线格式。

取证
- wedb/waof/src/aof/header.rs:28 pub enum AofHeaderType、:72 pub struct AofHeader、
  :283 AofShardedHeader、:330 AofSingleLogTransactionHeader、:390
  AofShardedLogTransactionHeader、:451 AofChunkHeader；文件共 735 行；lib.rs 全量
  re-export（wedb/waof/src/lib.rs pub use aof::header::{...}）。
- C# 对标：garnet/libs/server/AOF/AofHeader.cs——C# 单文件集中定义各类头
（ SALongHashSign / entryType 等位打包），但 rust 侧各头带独立 encode/decode/校验
实现，体量数倍于 C# 声明面。

修法建议
按基础头（AofHeader + AofHeaderType）、事务头（SingleLog/ShardedLog/Sharded 三
Transaction/Header 形态）、大值分块头（AofChunkHeader）拆三个子文件，
header/mod.rs 统一 pub use，对外路径零变化；编解码逻辑保持手写位运算不动
（bitcode 化不适用：线格式须对标 C# AofHeader 位布局语义）。纯搬运。

主代理补录（14:13，agy.db 晚波条 10 反证）：C# garnet/libs/server/AOF/AofHeader.cs 为 335 行单文件汇集全部 7 种头型，且存在跨型 IsChunked/SkipHeader/GetChunkedHeaderRef 契约要求同处。若你甄别认定拆分仍成立，请在落地记录里显式回应该反证（为何 rust 侧拆分不违背 1:1 契约）；若认定不成立，按规程转 task/reject 结案。
