优先级：中（重复/多套架构之「建了没接」：网络缓冲池零借用，客户端读泵随超时丢弃重建缓冲）
分拣注记（源 next/qw.net.md 第 11 轮 net 条 8；浅核 2026-09-19 HEAD 39ea7e58：pump.rs:235/:257 两处 `vec![0u8; READ_CHUNK]` 在场、两池 get/get_ref 全仓零消费点实测命中、drive.rs:79/:92 借还范式在场；台账查重无同题票，仅 task/ing/snapshot-chunk-pooled-borrow.md 处理 AOF 快照分块、readme-crate-map-drift.md 纠正该池「16 分片」虚构口径，均不重叠）

wconn 客户端读泵每 250ms 丢并重建一块 16KB 缓冲；同仓两处 network_pool 建而不借，C# 的收包借还范式在网络面上没落地

现状事实（主仓 dev，取证 HEAD 39ea7e58）

- 稳态限时读：wedb/wconn/src/network/pump.rs:252-260，`progress.is_some()` 时对
  :253 `timeout(RECV_IDLE_PROBE, stream.read(chunk))` 限时，Err 臂 :257
  `chunk = vec![0u8; READ_CHUNK]; continue;`。compio 的 read 取走 chunk 所有权，超时即随
  future 析构，故只能整块重分配。常量：:31 READ_CHUNK = 16 * 1024、
  :35 RECV_IDLE_PROBE = 250ms。progress 在位即复制/gossip 客户端链开了超时旋钮，
  该分支在完全空闲、零字节的稳态下也以 4Hz 恒发。
- 滞留态分支更硬：pump.rs:235 直接 `stream.read(vec![0u8; READ_CHUNK])`，每次迭代就地新分配
  一块并清零，且 :234 注释「滞留窗口极短，重建仅静默异常时发生」对该分支是反的。
- 池在位且已按 C# 形状建档：wedb/wbase/src/pool/limited.rs:3 自述为 C#
  LimitedFixedBufferPool 的 1:1 移植，:205 结构、:268 get、:278 get_ref、:296 purge。
  服务端读侧已真正借用：wedb/wnode/src/net/handler/drive.rs:79 `buffer_pool.get_ref(0)`、
  :81 take_buffer、:92 set_buffer 交还、:103/:131 借出（配套
  wnode/src/net/handler/buffer.rs:20 RecvAppend 的 IoBuf/IoBufMut/SetLen 形态，
  读入 spare_capacity 不清零）。
- wedb 自持两池只建不借：wedb/wedb/src/server/replication/replication_manager.rs:95
  `pub network_pool`（:178 构造），全仓读者仅 :1197 purge 与 :1206-1209 计数上报；
  wedb/wedb/src/server/migration/migration_manager.rs:37 network_pool（:48 构造），
  读者仅 :65 purge、:74-77 计数，其 :33 注释还自称「与 network_pool 同源取」。
  两池 get / get_ref 消费点实测零命中。
- 违背条款：task/review.md:25「数据链路要打通，对标 c#的调用流程，删除没用的函数」——
  现状是「设施齐备、链路未通、并以 stats 名义留存死面」。

目标形态

一口改动同时收两半：把 C# 的「每次收包借缓冲、用完回池」接到客户端读泵上。

1. read_pump 的读块改经池借出（get_ref / take_buffer），读毕按 drive.rs:92 的
   set_buffer 同形交还，取消 pump.rs:200/:235/:257 三处 `vec![0u8; READ_CHUNK]` 字面量；
   借出大小用 READ_CHUNK 对齐，池容量到界自动回落堆分配（抖动有界）。
2. 消掉「超时即丢块」的形态本身，而非只把丢块换成丢池借出：稳态探测改常驻读任务 +
   事件/令牌通知（判成标志与 EOF 由通知唤醒，取消只发生在读已归还之后），使
   pump.rs:257 那条重分配臂整体不存在。
3. pump.rs:234 注释按实际触发口径改写（写明稳态分支每 250ms 触发一次），滞留态分支
   :235 复用同一借还路径。
4. 客户端泵自此是两池的真实消费者；若第 2 步判定不接池（例如池语义与该链不匹配），
   则本票退化为「删两池 + 在 js/check/ignore/garnet/libs/common/Memory/
   LimitedFixedBufferPool.cs.yml 登记不转写消费面」，二者取一，禁以 stats 名义留存。

C# 对位

- garnet/libs/common/Networking/TcpNetworkHandlerBase.cs:34-35 构造显式收
  NetworkBufferSettings + networkPool，:267
  `networkReceiveBufferEntry = networkPool.Get(initialReceiveBufferSize, NetworkReceiveBuffer)`
  一次借用、:132 复用同一 SocketAsyncEventArgs 投递收包。
- garnet/libs/client/GarnetClientTcpNetworkHandler.cs:12 继承该基类，即客户端链与
  服务端链共用同一池化收包范式，不存在「每 250ms 换一块」。
- garnet/libs/common/Memory/LimitedFixedBufferPool.cs:22 类声明、:63 构造
  （maxEntriesPerLevel=16、numLevels=4，层级池，无分片）。

门禁与验收判据

- cargo check --workspace --all-targets 零错误零警告；./sh/clippy.sh 零警告。
- wconn 读泵既有单测与 wedb/tests 复制链 e2e（replication_stream_e2e、
  replication_end_to_end、appendlog_reject_disconnect）全绿。
- 判据一（结构）：pump.rs 内不再出现 `vec![0u8; READ_CHUNK]`（grep 零命中），读块生命周期
  由池句柄持有。
- 判据二（行为）：空闲且开超时监控的客户端连接稳态下零新分配——用一条用例断言
  「N 轮探测后池 borrowed_count 回到基线、allocated_count 不随轮数增长」，或等价的
  分配计数断言；不接受「以注释代替修复」。
- ./js/check.js 无新增缺失；若走删池路线需同批改 ignore 登记。

坑与边界

- compio 的 read 以 owned buffer 入参，future 被取消时缓冲随其析构：直接套池会把
  「丢一块」变成「泄漏一次借用」，borrowed_count 只增不减。故必须先落实第 2 步的
  取消安全形态（常驻读 + 通知），再接池；drive.rs 的先例是阻塞读无超时，不可照抄。
- 只动客户端读泵这一条链，服务端 drive.rs 的借还范式与 wnode 会话缓冲是基准，不改；
  也不把池句柄透传进 GarnetClientSession 的命令面（那是内存池第二套入口，属重复设计）。
- 两池的上界口径（DEFAULT_BUFFER_SIZE / DEFAULT_MAX_POOL_SIZE）若接线，需与 C# 构造
  参数逐一对齐后再写注释，勿沿用现状「注释自称同源、实际无读者」的说法。
- 与在途票的边界：task/ing/snapshot-chunk-pooled-borrow.md 处理 AOF 快照分块的池借用，
  本票只管客户端读泵；task/ing/zero-consumer-dead-surfaces-batch-five.md 与其后批次处理
  pub 死面普查，本票两池子项以「接线」为优先，若改判删除则从本批摘除并入该线，勿双开。
