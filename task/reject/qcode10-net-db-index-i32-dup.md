库号解析域宽与 C# int32 档位分叉（甄别拒绝：与在册票重复）

来源：next/qcode10.net.md 条 3（[LOW]，原文件已整体拆除）。

拒绝理由：同轮同题单票已拆出在册 —— next/qcode10-parse-db-index-i32-parity.md
（其自述「来源：qcode 第 10 轮 net 条 3（next/qcode10.net.md:40-42）」），
主张、证据（wbase/src/num.rs:137-173 parse_db_index u64 域、SELECT/SWAPDB/DBID 三处
消费点）、修法（收口 i32 域 + 逐臂对位文案）与本条目完全一致。

浅核（当前工作树）：parse_db_index 仍为 u64 域（文档自述「支持 0..=u64::MAX」），
问题仍待修，由在册票承接，不重复立项。C# 锚点（ParseUtils.cs TryReadInt int32）存在。
