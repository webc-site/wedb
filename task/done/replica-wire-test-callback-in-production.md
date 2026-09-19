优先级：高（死代码/污染扩散：测试专用发送形态无条件编进产线二进制）
分拣注记（源 next/qw.net.md 第 11 轮 net 条 4；浅核 2026-09-19 HEAD 39ea7e58：FrameSink replica_wire.rs:56、CallbackWire :139、AofSyncWire::Callback :613、From :684 在场，CallbackWire 构造点实测全在测试；该条第二半（cfg 别名对一名两义）已由 72a0bf03 随门面删除消解，本票只立内存形态进产线一半；台账查重无同题票，task/ing/encode-cluster-append-log-frame-dead-facade.md 已落地归档 task/done）

replica_wire 的内存回调通道只服务测试，却常驻生产类型面，并把副本会话依赖拽进生产 use

现状事实（主仓 dev，取证 HEAD 39ea7e58）

- 形态面：wedb/wedb/src/server/replication/replica_wire.rs:56-70 FrameSink 五变体
  （Buffer / Reject / Fn / Session / Queue），:72-103 `FrameSink::call`，:108/:114/:120/:129
  四个 Into 转换器，:139-198 CallbackWire，:609-614 AofSyncWire 双臂（Tcp = 生产、
  Callback = 测试），:618-678 逐方法双分派，:684-688 `From<Arc<CallbackWire>>`。
- 生产构造点零命中：AofSyncWire::Callback 的值只由 :684 的 From 产生，该 From 与
  CallbackWire::new 的全部调用者都在测试里——本文件 :691 起的 cfg(test) mod tests
  （:710/:737）、wedb/wedb/src/server/replication/aof_sync_task.rs:434 起的 cfg(test) 用例
  （:473/:492/:508/:528/:540/:588）、集成测试
  wedb/wedb/tests/replication_end_to_end.rs:34+149、
  wedb/wedb/tests/replication_stream_e2e.rs:20+105、
  wedb/wedb/tests/appendlog_reject_disconnect.rs:184-187。
- 集成测试在 tests/ 目录，无法用 cfg(test) 摘除，故只要形态留在 src/ 就恒进产线二进制。
- 依赖污染实证：replica_wire.rs:37 `use wdev::SegmentedDevice;` 与 :38
  `use wnode::MessageConsumerFace;` 只服务内存臂——SegmentedDevice 仅出现在 :63-70 的
  Session 变体与 :120-135 两个 From，MessageConsumerFace 只因 :90-95 调用
  try_consume_messages_into / take_fatal_disconnect（trait 定义在
  wedb/wnode/src/traits.rs:30/:38/:46）才需入 scope。生产 TCP 链本身不需要这两个 crate 面。
- 违背条款：task/review.md:25「删除没用的函数，测试、调试用的函数用 cfg」。
- 原审计同条的第二问题（:691-696 那对 cfg 别名把 encode_cluster_append_log_frame 改名成
  encode_append_log_frame）已随 72a0bf03 删除门面时一并消解，本票不含该部分。

目标形态

产线类型面只剩 TCP 一种通道，与 C# 同形。

1. AofSyncWire 收成单形（仅 Tcp 载荷），:618-678 的逐方法 match 与 :655-660
   `if let Self::Tcp` 特判随之一并展平为直调。
2. FrameSink / CallbackWire / 四个 From 整体搬出产线：首选移入测试支撑 crate
   （本仓已有先例，aof_sync_task.rs:439 用例即依赖 wnode_test::test_sublogs），
   次选 cfg(any(test, feature = "test-wire")) 门 + 自身 dev-dependencies 开该 feature。
   两条路都要求三个 tests/ 集成测试改从新落点引入。
3. 随形态收口，replica_wire.rs:37-38 的两条 use 从生产视图消失。
4. 若最终判定该内存通道无保留价值，则连测试一起改走真 socket（本仓已有
   127.0.0.1:0 与真监听先例），删净 FrameSink/CallbackWire，不留 cfg 中间态。

C# 对位

- garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs 只持
  GarnetClientSession 单一具体通道（:323 建连处），无「测试形态编进产线类型」的第二变体。
- garnet/libs/client/ClientSession/GarnetClientSession.cs:473 ExecuteClusterAppendLog 是
  唯一记录帧出口。
- C# 侧测试替身手法是换 GarnetClientSession 的连接目标/接口实现，不改产线枚举形状。

门禁与验收判据

- cargo check --workspace --all-targets 零错误零警告；./sh/clippy.sh 零警告（未使用变体
  与冗余分派正是本票要消掉的告警面）。
- 三个集成测试 replication_end_to_end / replication_stream_e2e / appendlog_reject_disconnect
  仍全绿，其内存闭环断言（逐帧消费、拒收转断连、溢流水位）不得整片删除或降级为 smoke。
- grep 判据：src/ 生产视图内 CallbackWire / FrameSink 的构造与 match 臂零命中
  （测试落点除外）；replica_wire.rs 的 use 段不再出现 wdev::SegmentedDevice 与
  wnode::MessageConsumerFace。
- 走 feature 路线时依赖必须用 cargo add / cargo add -D 增改（.agents/skills/rust_review/SKILL.md:103
  「依赖用 cargo add 添加，禁止直接编辑 Cargo.toml」），禁手写依赖行。

坑与边界

- 集成测试依赖 `wedb` 的 pub 面，cfg(test) 对它们无效：门一律用
  `cfg(any(test, feature = "test-wire"))`，且该 feature 需在 Cargo.toml 显式声明，
  由 dev-dependencies 自开（否则 tests/ 编译不过）。
- CallbackWire 的 Session 臂承载的是「主端帧直投副本会话」的无 socket 闭环，删形态前
  先确认这三条 e2e 断言在 TCP 形态下可等价复现，避免为了收口而砍掉断流/水位覆盖。
- 不动 TcpSessionWire 的溢流队列与水位回推逻辑（replica_wire.rs:318/:443 一带属
  task/ing/replication-send-path-byte-cap.md 射程，本票只收双臂形状，命中即让位）。
- cluster_replication_session.rs:13 的模块注释自述「内存链路由 CallbackWire 承接」，
  随迁改写，勿留失真指引。
- 不做向下兼容：旧枚举臂直接删，不保留 `#[allow(dead_code)]` 或过渡别名（本仓禁写 allow）。

盘点补记（qw13.invA replica-wire-test-callback-in-production）：dev e75716e 复核，增量：aof_sync_task.rs 侧使用面已收进 #[cfg(test)] mod tests（:426 起，:434 use CallbackWire/FrameSink），但定义仍在生产模块视图：replica_wire.rs:56 pub enum FrameSink、:139 pub struct CallbackWire、:613 Callback 臂与逐方法 match 原样，无 cfg 门、未搬出。修法收敛为「定义面搬出或加 cfg 门」单步。
