复制发送链补字节维度上限（对标 C# NetworkWriter 4 页环形缓冲的字节硬顶）

判定：票面成立，采纳。

C# 事实核对（已逐行验证）
- garnet/libs/client/NetworkWriter.cs:55 BufferSize = 4 页环形缓冲；:99-104 按 sendPageSize 分页；
  :145-210 TryAllocate 页满时经 :263-267 NeedToWait（按 FlushedUntilAddress 判）返回 -1 RETRY_LATER，
  :220 起异步包装挂到 FlushEvent 上背压等待。未刷出字节顶 = 页数(4) × 每连接 sendPageSize。
- garnet/libs/client/GarnetClient.cs:149 sendPageSize = 1 << 21（通用客户端，4 × 2MB = 8MB）。
- garnet/libs/cluster/Server/Replication/ReplicationNetworkBufferSettings.cs:41 AOF 同步客户端
  aofSyncSendBufferSize = 2 << AofPageSizeBits()（AofPageSize 默认 32m，:106），:26 RSS 1MB、:33 IRS 128KB。
  即复制域按用途显式配发送页尺寸，字节顶锚在 AOF 页尺寸上。

rust 事实核对（已逐行验证，行号按符号定位；文件正被并发会话编辑，合并时以符号复核）
- wedb/wedb/src/server/replication/replica_wire.rs TcpSessionWire::MAX_OVERFLOW_ENTRIES = 10_000，
  send_or_enqueue 唯一超限判据 self.overflow.len() >= MAX_OVERFLOW_ENTRIES，纯条数。
- wedb/wconn/src/types.rs CHANNEL_CAP = 1024；session.rs bounded_async(CHANNEL_CAP)；client.rs 同口径。
- wedb/wconn/src/network/pump.rs write_pump 拼批循环逐帧 out_buf.extend_from_slice，仅最终一次
  write_all；in_flight 满时才中途刷；APPENDLOG 帧是 ReplyTx::None 不入 in_flight，整批积压全进 out_buf，
  单帧字节、单批字节均无上界。
- 全仓无发送侧字节顶常量（byte_cap/max_unflushed 面）。

后果
慢副本 + 大记录（chunk ≤ 页尺寸、MB 级 value）时，溢流 10000 帧 + 通道 1024 帧的字节和线性放大，
极端输入无熔断；C# 由 4 页环形缓冲把未刷出字节钉死。

方案（复用既有断连路径，不新增错误种类、不新增配置项、不引入自适应/水位算法）
1. wconn 单点常量：在 wconn/src/types.rs 新增 pub const MAX_UNFLUSHED_SEND_BYTES。
   口径对标 C#「页数 4 × 发送页尺寸」，页尺寸取 C# AOF 同式 2 << 页位（rust 默认 wal 页 16MiB → 32MiB），
   合计 128MiB。文档注释写明 C# 映射。wconn 不引 waledger/waof 依赖，常量在 wconn 定义、复制侧引用，
   保持单一数值源，禁两处魔数。
2. 写泵按字节分片刷出：wconn/src/network/pump.rs write_pump 增加 flush_threshold_bytes 形参
   （network_loop 内部传 MAX_UNFLUSHED_SEND_BYTES，session/client 调用 network_loop 不变），
   拼批循环内 out_buf 达阈值即 write_all 刷出、清空后继续收批（复用 in_flight 满时的刷出+清空+回接同款写法），
   使单次在途缓冲有界；收尾 write_all 不变。
3. 溢流侧条数 + 字节双判据：TcpSessionWire 增 overflow_bytes: AtomicUsize（入队先增
   后 push、泵 pop 减，防 usize 下溢令闸假性触顶）与 byte_cap: usize 字段
   （产线 connect 恒取 MAX_UNFLUSHED_SEND_BYTES，唯一数值源；单测构造小值廉价触达
   字节判据，与写泵分片阈值注入同形，非新增配置项）；WireFrame 增 resident_bytes()
   （AppendLog 取 payload 长度，AdvanceTime 取恒定小帧；RESP 数组头等固定开销由条数
   封顶承接）。send_or_enqueue 先建帧计量，超限判定改为「条数达 MAX_OVERFLOW_ENTRIES
   或 overflow_bytes 达 byte_cap」，两判据共用同一条 disconnect + BrokenPipe 路径，
   错误文案区分字节/条数便于定位慢副本。
   泵的两处 try_pop（单帧弹取、贪婪消费）弹帧后 fetch_sub 对应字节；pending_frame 续传不重复计量。
4. 订正自述注释：replica_wire.rs 顶部模块注释、MAX_OVERFLOW_ENTRIES 文档、send_or_enqueue 文档，
   改写为「条数 + 字节双封顶」。

测试
- replica_wire.rs 单测：连静默 loopback 端点使会话通道在位，不启常驻泵、在途帧置位
  封死直发臂（溢流只积不排、确定性触顶），注入小 byte_cap 连续 append_log 大 payload
  帧，断言驻留字节计量逐帧累加、字节触顶先于条数断连并返回 BrokenPipe（文案含 byte）；
  另以 byte_cap 放开、小 payload 帧灌满 10000 条断言条数判据照常生效（文案含 entry）。
  （原案「pump_alive 关直发失败入溢流」不成立，见 task/reject/repl-send-bytecap-testsketch.md）
- pump.rs 单测：以真 loopback 调 write_pump，注入小阈值，喂若干 fire-and-forget 帧超阈值，
  对端读尽全部字节，断言分片刷出不丢帧、不乱序、连接健康（TCP 合包不可观测单次 write 边界，
  故以「全量到达」回归分片代码路径）。

边界（勿并入本单）
- 调用方在途命令数准入闸（max_outstanding_tasks）是另一面。
- 服务端收包侧有界性不同面。
- 单帧超过页尺寸由 chunk 分片承接，一帧可超阈值但仍放行（对标 C# 允许单请求占满一页）。
- AOF 尺寸旋钮读侧接线（aof-size-knobs-read-side-wiring）未落地前按默认页尺寸取常量。
