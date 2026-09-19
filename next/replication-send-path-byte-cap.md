复制发送链只有条数维度治理、无字节维度上限：慢副本 + 大记录场景每连接驻留内存与 C# 的字节硬顶脱锚

来源：next/glm.net.md 条 5（该文件已分拣清空删除）。逐句按主仓当下代码与 C# 复核后判定成立待做。
取证基线：主仓 /Users/z/git/db/wedb，行号按符号定位。
载体唯一性：并发拆条在 next/ 下另留了一份本条原文照抄的壳（basename
replication-send-buffer-byte-cap.md，只加「优先级：高」头、无取证订正），
以本文件为唯一载体，派单前先剪壳勿双花。

结论

C# 客户端发送侧的驻留内存是按字节封顶的：每连接 4 页环形缓冲，页满即 RETRY_LATER 背压等待；
复制域三类客户端还各自按用途显式声明了发送页尺寸。rust 的副本推送链从溢流队列到命令通道再到
写泵拼批，三层都只数字节之外的「条数」，单帧字节数无任何约束，慢副本场景下每连接稳态驻留等于
一万一千余帧的总字节和，记录一大就线性放大、极端输入无熔断。本单补的是字节维度这一层闸，
不改帧序、不改协议、不改断连语义。

C# 事实（字节顶在哪）

- /Users/z/git/db/wedb/garnet/libs/client/NetworkWriter.cs:55 `BufferSize = 4`（环形页数），
  :99-104 按页分配 `values = new Page[BufferSize]`，:145-210 `TryAllocate` 在页满时经
  :263-267 `NeedToWait`（按 `FlushedUntilAddress` 判定）返回 -1 / RETRY_LATER，
  :220 起的异步包装把该情形挂到 `CompletionEvent flushEvent` 上背压等待，
  发送线程不再继续吃新命令。
- /Users/z/git/db/wedb/garnet/libs/client/GarnetClient.cs:149 `sendPageSize = 1 << 21`（2MB 页），
  即每连接未刷出字节顶 = 4 × 2MB = 8MB；:150 `bufferSize = 1 << 17`、:156
  `networkSendThrottleMax = 8` 限在途发送数；:82-88 两参传入 NetworkWriter 构造。
- 复制域按用途显式配尺寸：/Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/ReplicationNetworkBufferSettings.cs:26-28
  副本同步会话客户端 1MB 发送页、:33-35 InitiateReplicaSync 客户端 128KB、:41-43 AOF 推流客户端
  `aofSyncSendBufferSize => 2 << AofPageSizeBits()`，且 :39 注释写明「双分页尺寸以保证
  命令头 + 页载荷必定落进客户端缓冲」——即 C# 的字节顶直接锚在 AOF 页尺寸这个配置量上。

rust 事实（三层都只数条）

- 溢流队列：/Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_wire.rs:318
  `pub const MAX_OVERFLOW_ENTRIES: usize = 10_000`（注释自述「防止对端假死导致内存无界膨胀」，
  但判据是条数），:464-485 `send_or_enqueue` 在 :477 以 `self.overflow.len() >= MAX_OVERFLOW_ENTRIES`
  作唯一超限判据，超限即 :478 disconnect + BrokenPipe。队列元素是
  :322-333 `WireFrame::AppendLog(OverflowEntry{.. payload ..})`，payload 即整帧载荷，无字节累计量。
- 命令通道：/Users/z/git/db/wedb/wedb/wconn/src/session.rs:51 `mpsc::bounded_async(CHANNEL_CAP)`，
  /Users/z/git/db/wedb/wedb/wconn/src/types.rs:12 `pub(crate) const CHANNEL_CAP: usize = 1024`；
  GarnetClient 侧同口径 /Users/z/git/db/wedb/wedb/wconn/src/client.rs:87
  `bounded_async(self.max_outstanding_tasks.max(CHANNEL_CAP))`（:20-21 注释即「在途命令上限
  即网络泵的命令通道容量下限」）。三层容量判据全是条数。
- 写泵拼批无截断：/Users/z/git/db/wedb/wedb/wconn/src/network/pump.rs:118-135 的
  `while let Some(cur) = pending.take()` 循环把 `rx.try_recv()` 清空的整批逐帧
  `out_buf.extend_from_slice(&cur.frame)`，:136 单次 `stream.write_all(out_buf)`。
  单批峰值即通道积压的总字节和，循环内没有「攒够多少字节先刷一次」的分片。
- 单帧字节无上界：/Users/z/git/db/wedb/wedb/wedb/src/server/replication/aof_replication_pump.rs:212-216
  `let frame = record.reconstruct_frame();` 整帧交 `task.consume`，帧大小即 AOF 记录大小
  （大 value 记录可达 MB 级），链路上没有任何尺寸检查。
- 泵侧贪婪消费同样只看条数：replica_wire.rs:443 `while let Some(next) = overflow.try_pop()`。

后果量化

稳态驻留 ≈ 溢流 10,000 帧 + 通道 1,024 帧的字节和。帧均 1KB 时约 11MB，与 C# 的 8MB 顶同一量级、
偏差可接受；帧均随记录大小放大时（密集 64KB chunk、MB 级 value）按倍数线性放大，
一万帧的 MB 级记录即 GB 级驻留且无熔断。C# 在同样输入下由 TryAllocate 的 RETRY_LATER 把
未刷出字节钉在 4 页之内，多出的部分挂在调用方任务上不占网络缓冲。

修法

1. 一处定义字节预算，两处消费：新增单点常量（建议落
   /Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_wire.rs 的 `TcpSessionWire` 关联常量区，
   紧邻 :318），口径对齐 C# 的「页尺寸 × 页数」——页数取 4，页尺寸取 AOF 侧一次记录帧上限
   （与 C# :41 的 `2 << AofPageSizeBits()` 同式，暂以本仓 wal 记录/页尺寸缺省量取常量），
   不新开配置项。禁在写泵侧另写第二个魔数。
2. 溢流侧按条数与字节双判据：`WireFrame` 增加帧字节数访问口（AppendLog 取 payload 长度 + 头，
   AdvanceTime 取编码常量），`TcpSessionWire` 维护一个累计溢流字节计数（push 增、泵 pop 减，
   与 :477 的 `overflow.len()` 同生命周期同锁域）；`send_or_enqueue` 的超限判定改为
   「条数达 MAX_OVERFLOW_ENTRIES 或 累计字节达预算」，两判据共用同一条断连 + BrokenPipe 路径
   （:478-482），不新增错误种类。错误文案点明是字节超限还是条数超限，便于定位慢副本。
3. 写泵按字节分片刷出：/Users/z/git/db/wedb/wedb/wconn/src/network/pump.rs:118-135 的攒批循环
   内，`out_buf` 达阈值即先 `write_all` 刷出、清空后继续收批（:136 的收尾 write_all 保持不变，
   成为「余量刷出」），使单次在途缓冲有界。阈值由调用方注入或取 wconn 侧单点常量，
   与第 1 步同值；wconn 不引 waledger/wedb 的依赖，故字节预算常量在 wconn 定义、
   复制侧按引用使用，保持单一数值源（禁止两侧各写 4×2MB 的字面量）。
4. 订正自述：replica_wire.rs:316-318 与 :459-463 的注释目前把「防内存无界膨胀」记在条数封顶上，
   补字节维度后同步改写为「条数 + 字节双封顶」，与实现一致。
5. 测试：/Users/z/git/db/wedb/wedb/wedb/tests 现有复制装配用例（replication_assembly_e2e.rs 一族）
   补一条注入大记录 + 挂起对端读的用例，断言溢流在字节阈值处断连而非按条数放行；
   wconn 的写泵分片可在 /Users/z/git/db/wedb/wedb/wconn 单测内以真 loopback 监听断言
   单次 write 不超阈值（模式参照 /Users/z/git/db/wedb/wedb/wconn/src/network/mod.rs:229 一带
   已有的 loopback 用例）。

优先级

功能缺口（背压闸缺一维，可观测后果是内存驻留与 OOM 风险），排在死代码、重复架构、污染扩散之后。
实现与 C# 的字节顶为对齐目标，不引入 C# 没有的自适应或水位算法。

边界（与相邻票不同面，勿合并勿双写）

- 调用方在途命令数准入闸（max_outstanding_tasks 形参无语义）是另一面：
  task/ing/qcode10-client-outstanding-admission-gate.md。那管「能提交多少条命令」，
  本单管「已提交命令的字节在网络上驻留多少」。
- 服务端收包侧的有界性（try_reserve 一类收侧闸门）与本单不同面：那是进方向的界，
  本单是出方向的界，判据不通用，勿在同一改动里顺手合并。
- 字节预算若最终取「2 << aof_page_size_bits」式，则依赖 AOF 页尺寸旋钮真的接到装配：
  该前置在 task/ing/aof-size-knobs-read-side-wiring.md（其现状为三尺寸旋钮读侧零消费）。
  本单不以它为阻塞前提：该票未落地前按当下缺省页尺寸取常量，落地后把同一表达式接上即可。
- 副本侧接收与重放链的内存治理不在本单射程。

盘点补记（qw13.invA replication-send-path-byte-cap）：dev e75716e 复核三层仍纯条数：wedb/src/server/replication/replica_wire.rs:329 MAX_OVERFLOW_ENTRIES=10_000、:493 唯一超限判据 self.overflow.len()；wconn/src/types.rs:15 CHANNEL_CAP=1024；wconn/src/network/pump.rs 拼批循环无字节分片；全仓无 byte_cap/max_unflushed 面。原票可派性结论不变。
