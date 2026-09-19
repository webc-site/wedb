客户端在途上限形参无语义 .max(CHANNEL_CAP) 只抬不压（甄别拒绝：与在册票重复）

来源：next/qcode10.net.md 条 8（[MED]，原文件已整体拆除）。

拒绝理由：同轮同题单票已拆出在册 —— next/qcode10-client-outstanding-admission-gate.md
（其自述「来源：qcode 第 10 轮 net 条 8（next/qcode10.net.md:71-73）」），覆盖本条目
主体：形参唯一读点 .max(CHANNEL_CAP) 反话语义、泵侧/会话侧直用常量、C# InputGateAsync
准入闸对位与修法。其同族副条（record_latency 写死 false、wconn/src/metrics.rs 五个查询
API 生产零读者）未入该在册票射程，但本条目处置遵循单问题拆票原则，副条不因捆绑而在
本票重开；如需立项另循零消费普查口径核对后单立。

浅核（当前工作树）：wconn/src/client.rs:87 仍为
`mpsc::bounded_async(self.max_outstanding_tasks.max(CHANNEL_CAP))`，
types.rs:12 CHANNEL_CAP=1024 —— 问题仍待修，由在册票承接，不重复立项。
C# 锚点（GarnetClient.cs:135/:167-174/:579-590 InputGateAsync）存在。
