ASYNC 命令只写会话布尔、全仓无读者（甄别拒绝：与在册票重复）

来源：next/qcode10.net.md 条 6（[MED]，原文件已整体拆除）。

拒绝理由：同轮同题单票已在册实施中 —— task/ing/async-command-no-read-side.md
（其自述「来源：qcode 第 10 轮 net 视角审查条 6（原主张文件 next/qcode10.net.md 已剪除
本条，本文是唯一载体）」），覆盖本条目全部内容：pub use_async 字段写侧齐读侧零、
BARRIER 空臂、GET 无异步旁路、C# AsyncProcessor 面 ignore 登记口径、删字段改回
RESP_ERR_ASYNC_REQUIRED 的修法与验收。

浅核（当前工作树）：resp_server_session.rs:261 字段、:464 构造 false、
basic_commands/mod.rs:766/:768 置位点均仍在 —— 问题仍待修，由在册 ing 票承接，
不重复立项。C# 锚点（BasicCommands.cs:70/:1731/:1735、AsyncProcessor.cs:20）存在。
