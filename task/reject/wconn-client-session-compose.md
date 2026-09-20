# 拒绝：wconn 客户端架构强制 GarnetClient 组合 GarnetClientSession

判定：票面不成立。裁判为 C# 原作，票面的核心事实主张为误判。

## 票面主张与 C# 原码的逐字核对

票面第 2 条称：“在 C# 原作中，GarnetClient 直接组合包装持有
GarnetClientSession（protected GarnetClientSession session）”。

此句为虚构。全仓 ./garnet 搜索 `GarnetClientSession session` 零命中：

- garnet/libs/client/GarnetClient.cs:32-48 的字段清单里，网络侧只有
  `NetworkWriter networkWriter`、`GarnetClientTcpNetworkHandler networkHandler`、
  `Socket socket`、`TcsWrapper[] tcsArray`，不存在任何 GarnetClientSession 字段。
- GarnetClient 是 `public sealed partial class`（GarnetClient.cs:32），其 14 个
  partial 分部文件（GarnetClient*.cs）中无一引用 GarnetClientSession。
- 全仓 `new GarnetClientSession` 的调用方全部集中在复制与迁移链路
  （garnet/libs/cluster/Server/Replication/.../AofSyncTask.cs、
  ReplicaSyncSession.cs、SnapshotTransmissionDriver.cs、
  Migration/MigrateOperation.cs 等），没有一处出现在 GarnetClient 内部。

C# 中 GarnetClient 与 GarnetClientSession 是两个刻意并行、互不组合的客户端实现：
- GarnetClient（GarnetClient.cs:143 构造）走 NetworkWriter + tcsArray 定长槽的
  异步流水线模型，自带 TimeoutChecker 看门狗（:460），面向压测与集群工厂调用方。
- GarnetClientSession（ClientSession/GarnetClientSession.cs:22 构造）走自己的
  networkPool + GarnetClientSessionTcpNetworkHandler，同步/异步双形态，
  无 timeoutMilliseconds 旋钮，面向复制、AOF 转发、迁移等推送域。

二者在 C# 中就是两套并行入口，共用的是底层网络层（TcpNetworkHandler 基类、
LimitedFixedBufferPool），而非彼此组合。票面把“共用底层”误读成“GarnetClient 组合
GarnetClientSession”，据此要求收敛为单一组合形状，方向与原作相反。

## Rust 现状已符合 C#，且底层已单源

- wedb/wconn/src/client.rs:20 GarnetClient 与 wedb/wconn/src/session.rs:22
  GarnetClientSession 均只持有 end_point、认证字段、可选 tls、可选 network_pool、
  可选 tx 通道，二者都复用同一套 types::{ChannelTx, CommandItem, ReplyTx, roundtrip}
  与 network::stream::OutStream。所谓“独立的第二套运行时泵”并不存在：往返与泵
  逻辑单源收敛在 roundtrip/OutStream，两个结构只是各自薄门面。
- GarnetClient 额外持有的 PumpProgress（client.rs:37）与 timeout_millis
  （client.rs:34）正是对标 C# TimeoutChecker（GarnetClient.cs:460）的看门狗，
  与“网络 I/O、协议握手、命令分发单源”不矛盾，是 C# 本来就有的分层。
- GarnetClientSession 不设超时旋钮（session.rs:18-21 注释），对应 C#
  GarnetClientSession 确无 timeoutMilliseconds，一致。

## 结论

按票面改回“GarnetClient 组合持有 GarnetClientSession”，等于引入 C# 根本不存在的
组合抽象，违背硬性纪律第 3 条“不添加 C# 没有的机制”，且会剥夺 GarnetClientSession
作为独立推送域客户端（复制/迁移）被直接构造的能力。当前 Rust 拓扑与 C# 吻合，
无需改动。判定不成立，不改代码。
