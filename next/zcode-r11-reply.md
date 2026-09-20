轮11 回复编码与出网缓冲管理(大回复怎么流出网·机制面)

审查范围: wresp 写出原语、wnode 会话输出域(resp_server_session/pump+output+core、net/handler/drive)、wconn 客户端写泵。C# 对位: RespWriteUtils.cs、RespServerSession.SendAndReset/Send、GarnetTcpNetworkSender 页环、NetworkWriter。逐问核实，无票级新缺陷，机制注记见条目 5-8。

1) 缓冲生命周期: 回复从构造到出网的缓冲归属

机制
- 会话 output 为会话自有 Vec，构造 64KB 驻留(wnode/src/resp/resp_server_session/core.rs:67 DEFAULT_OUTPUT_BUFFER_CAPACITY、:336)，命令臂经 wresp 写原语追加，Vec 倍增自然扩容，跨命令复用(批内不清)。
- 批内水位 128KB(同文件:75 OUTPUT_WATERMARK_BYTES，镜像 C# sendBufferSize 1<<17)在命令边界检查(core.rs:696)，达界置让渡哨兵交泵实写。
- 出网交接单点 take_output_into(wnode/src/resp/resp_server_session/pump.rs:161): 泵缓冲空时整段 swap(零拷贝，大缓冲离会话归泵)，非空时 extend+clear。
- 归还/缩回三处闭环: 泵实写后容量超 64KB 即弃大缓冲换新池借出(wnode/src/net/handler/drive.rs:248-250); 会话 swap 进来的泵缓冲恒 ≤64KB，不足 4KB 时补 reserve 64KB(pump.rs:167-171); 接收缓冲整段消费完毕超 64KB 即缩回(core.rs:514)。
- 大回复后久住驻留: 无。单命令峰值缓冲随 swap 流入泵、写出后即弃; 非空 out 支(pubsub 交织)clear 后容量滞留会话，但下一轮 drain 必走空 out swap 支流出，滞留窗口一轮、量级 ≤水位+单命令峰值，非久住。

C# 对位: libs/common/Networking/GarnetTcpNetworkSender.cs:EnterAndGetResponseObject(:120) 从 saeaStack(2*ThrottleMax=16 个 GarnetSaeaBuffer)取 128KB 页，RespServerSession.cs:SendAndReset(:1348) Send 后 GetResponseObject 换新页，页随 SeaaBuffer_Completed 回池——rust 的「swap 出大缓冲写后即弃 + 会话恒持 64KB」与「页满即换页回池」同一生命周期形态。

判定: 机制闭环，无驻留缺陷。r3-perf 已核「输出缓冲复用干净面」，本轮独立复核一致。

2) 部分写与背压

机制
- 满刷时机仅三处: 批收尾、水位让渡(drive.rs:186-251)、空闲读等待期推送帧(drive.rs:331-355)。泵以 stream.write_all 落网(compio 循环写至完)，部分写(内核缓冲满)续传由 write_all 内部循环承担，挂起不占线程。
- 背压传导: 慢客户端令 write_all 挂起 → 本连接任务停住，不再消费新批 → 会话/泵缓冲止于「水位 128KB + 单命令峰值 + 一个池缓冲」，天然按连接隔离，不外溢他连接。
- 命令内增量 flush: 无。水位检查点只在命令边界(core.rs:696)，单命令应答物化全程不交泵——与 r3-protocol 已立「单命令应答峰值无界物化」同根，该项已被 task/reject/zcode-r3-protocol.md 驳回(架构差: C# 固定页 + 同步满刷循环 vs rust 可扩容 Vec + compio)。本轮机制面补记: C# 满刷粒度实为写点——RespWriteUtils 全族 TryWrite 写不下返回 false(libs/common/RespWriteUtils.cs:22-27 等)，调用侧 while(!TryWrite) SendAndReset 循环(libs/server/Resp/ArrayCommands.cs:247-258 NetworkKEYS、libs/server/Metrics/Info/InfoCommand.cs:51-92)在命令执行中途即可满刷; rust 无对位(借用模型下 RespWriter 复查写点不可行，core.rs:69-75 文注已登记)。复核确认驳回理由成立，不翻案不重复立项。
- wconn 写泵(复制/迁移内部客户端出网): 逐项 flush 阈值检查(wconn/src/network/pump.rs:177-179)，阈值 MAX_UNFLUSHED_SEND_BYTES=4*(2<<24)=128MiB(wconn/src/types.rs:32)，常量口径错位已由 r8-const「页位错折半」立案; out_buf 跨批复用、容量滞留至阈值量级，随该票修复收敛，无新增。

C# 对位: libs/common/Networking/GarnetTcpNetworkSender.cs:264 Throttle(SemaphoreSlim，ThrottleMax=8 限制每会话并发在途发送，页耗尽即等)。rust 侧 wbase/src/throttle.rs NetworkSenderThrottle 逐连接构建(wnode/src/net/handler/mod.rs:73)，但写泵内联 await write_all 后才 exit_send(drive.rs:227-236)，在途计数恒 ≤1，节流等待分支永不触发——背压实际由 write_all 挂起承担，行为等价且更紧(C# 在途未 ACK 上限 8 页≈1MB，rust 恒 1 缓冲+水位)，无背压失效。

判定: 背压有效; 命令内无增量 flush 为已立已驳项的机制注记，无新缺陷。

3) 零拷贝面

机制
- GET/GETRANGE/GETEX/GET_SG/MGET: 读路径闭包暴露 &[u8] 值视图，值域直写 output(wnode/src/resp/basic_commands/get.rs:50-51、:105-106、:235-243; wnode/src/resp/array_commands.rs:245-247 do_network_mget)，值内存→输出缓冲恰一次拷贝，无 collect 中转; 降级臂 truncate 撤帧回滚。
- LRANGE 内存臂: 数头落定后逐成员 write_bulk_string 直写(wcol/src/list/list_object_impl.rs:138-174 list_range)。
- 分层(升阶树)臂: LRANGE 有界窗口扫、帧头先落、成员逐条直出(wnode/src/resp/objects/tiered_collection_ops/list.rs:283-304，文注明言旧「全树扫进 Vec<Vec<u8>> 再裁剪」已消除); SMEMBERS/HGETALL 族预留-回填流式直写(同目录 set.rs:139-165、hash.rs:315-443)。
- 全仓无 write_vectored/sendfile/writev(grep 零命中)，单扁平缓冲逐批写出; C# SocketAsyncEventArgs 亦单缓冲，无 vectored 对位可缺。
- 慢路径命令(KEYS/SCAN/DBSIZE 等): SlowWait 产出自有 Vec<u8>(wnode/src/resp/slow_path.rs:32、:116)，经 resolve_slow_wait_into 整块 memcpy 并入泵缓冲(wnode/src/resp/resp_server_session/pump.rs:129-134)——相对 C# 直写页缓冲多一次应答全量拷贝。成因是借用模型强制(跨 await 持有 &mut output 不可行)，且 C# 同样先物化(NetworkKEYS 的 List<byte[]>、INFO 的 GetRespInfo string)，物化主体已由在途票 task/ing/dbsize-keys-single-pass-scan.md 承担，本拷贝随该票重构面收敛，不单独立项。INFO scratch 物化与 C# GetRespInfo string + WriteLargeVerbatimString 同构，非缺陷。

C# 对位: libs/server/Storage/Functions/MainStore 的 SpanByte 直读 + RespWriteUtils.TryWriteBulkString 值域单拷贝直写页缓冲(:160-232 区段); WriteDirectLarge(RespServerSession.cs:1404)分片直写。

判定: 大值回复恒直写无中转，零拷贝面干净; 慢路径 +1 拷贝为在途票覆盖的架构性代价，无新票。

4) RESP3 帧长度前缀: 流式编码的缓冲组织

机制
- 出帧条数先验可得(内存集合 Count)的臂: 直落长度头后流式直写，C# 同形。
- 条数扫完才定的臂: 预留-回填全仓单点 wresp/src/ext.rs:239 reserve_resp_frame_head + :253 backfill_resp_frame_head(位宽差 copy_within 单次移动，计数恒取实际出帧数，错误臂 truncate(base) 撤帧)。
- 消费面穷举(grep 全仓): wnode/src/resp/range_index/resp_server_session_range_index.rs:493/:541(RI.SCAN/RI.RANGE)、tiered_collection_ops/set.rs:147(SMEMBERS)、hash.rs:318/:370/:411(HGETALL/HKEYS/HVALS)、wedb/src/server/cluster_session/slot_mgmt.rs:144(槽键慢扫)，共 7 站全部转调该单点; reply 路径无第二套头补丁实现(copy_within/resize 头移位仅存在于单点内部与 wconn 读缓冲压实)。
- RESP2 退化: map 头落 *2n(wnode 侧经 wresp/src/resp_memory_writer.rs:191-193 Resp2::write_map_len saturating_mul(2))，与 ext.rs:456 测试锚一致。

C# 对位: libs/server/Storage/Session/MainStore/RangeIndexOps.cs ReservedHeaderSize(5 字节，:821/:827 预留、:897 BackfillArrayHeader)——rust 预留位宽同值、回填同型，且把 C# 仅 RI 一处使用的机制推广为分层集合/槽扫共用单点(ext.rs:184-210 文注自述)。

判定: 消费面核实为真单点，无第二套实现，无「先全部物化再拼头」残留。

5) 多会话缓冲隔离

机制
- 每会话自有 recv_buffer(64KB)与 output(64KB)，会话体独占 &mut 访问(wnode/src/resp/resp_session_consumer.rs:33-35 独占式串行访问)，断连随会话析构，不跨会话共享、不入池。
- 发送缓冲: 服务器级单池 LimitedFixedBufferPool(wnode/src/server.rs:356，空闲上限 1024×64KB)，每连接借出一个 PooledRefBuffer 贯穿 drive_loop(drive.rs:156)，RAII 归还，超规缓冲(容量≠池规格)就地弃置不入池(wbase/src/pool/limited.rs:103-106)，池不被大缓冲污染。
- 峰值内存面: 每活跃连接 ≈ 64KB recv + 64KB output + 64KB 泵缓冲 + 128KB 水位瞬态 ≈ 256KB 稳态; C# 每发送器 saeaStack ≤16×128KB 页 + 接收 128KB→1MB 增长，同量级、rust 上界更紧。所有权转移链(泵 take_buffer → IO → set_buffer)两处(wnode drive、wconn pump)均严格借还配对，无泄漏面。

C# 对位: NetworkBufferSettings.cs:58(默认 send/initialReceive 1<<17、maxReceive 1<<20)+ LimitedFixedBufferPool 共享池同构。

判定: 隔离完整，内存面上界与 C# 同阶，无缺陷。

6) 机制注记: 服务端节流器形同虚设

wbase/src/throttle.rs 实现完整(含测试)，但唯一消费点 drive.rs enter_send/exit_send 包夹的是内联 write_all，在途恒 1，ThrottleMax=8 的等待分支不可达。非缺陷(背压由 write_all 承担且更强)，但该组件当前在服务端路径为死逻辑面，若后续出现「攒批异步发送」改造才有用武之地; 登记口径防误判其承担背压职责。

C# 对位: GarnetTcpNetworkSender.cs:264 Throttle 真实限流 8 在途发送。

判定: 行为等价注记，非缺陷。

7) 机制注记: 慢路径/阻塞路径应答并入单点

resolve_slow_wait_into 与 resolve_blocked_wait_into(wnode/src/resp/resp_server_session/pump.rs:109-134)共用「先冲会话存量、再并挂起应答、出向记账 account_output」一形，阻塞臂直写目标缓冲零中转，慢路径臂 ScratchVec 并入(见条目 3)。出向字节记账单点无双计(account_output 仅此三处口径)。

C# 对位: RespServerSession.cs 各阻塞命令尾部 BlockingWait 后 switch 应答段 + Send 唯一出向记账点。

判定: 单点纪律成立，无重复机制。

8) 机制注记: wconn 客户端泵读侧

读缓冲经池一次借出、常驻读请求挂起(wconn/src/network/pump.rs:234-248)，残余段 copy_within 压实不搬字节(:326-330)。复制/迁移记录分片 bounded(wnode/src/aof/aof_chunked_record_reader.rs 值分片)，帧尺寸有界故累积缓冲不会无界。写侧 out_buf 容量滞留随 r8-const 阈值票收敛。

C# 对位: libs/client/NetworkWriter.cs:46 4 页环形缓冲页满即刷(AsyncFlushPageCallback)+ libs/client/GarnetClientProcessReplies.cs 读循环。

判定: 有界，无缺陷。

无增量确认
- 会话 output/recv 生命周期: 增长、复用、swap 归还、64KB 缩回全链核实，无久住大缓冲驻留(复核 r3-perf 干净面结论一致)
- 大回复出网三处满刷点(批尾/水位让渡/推送帧)与 write_all 部分写续传，背压按连接隔离有效
- GET/GETRANGE/MGET/LRANGE/分层集合大值回复恒直写输出缓冲，无 collect 中转
- 预留-回填全仓唯一(7 站消费面全数转调 wresp::ext 单点)，无第二套头补丁
- RESP2 map 头 *2n 退化单点(wresp Resp2::write_map_len)，RESP3 %/~ 头单点
- 无 vectored write/sendfile 使用，与 C# 单缓冲 SocketAsyncEventArgs 对位成立
- 多会话缓冲隔离: 会话自有双缓冲 + 服务器级单池借还，超规缓冲不入池，峰值面与 C# 同阶
- INFO scratch 物化与 C# GetRespInfo string 同构，非双缓冲缺陷
- 慢路径 +1 拷贝、命令内无增量 flush、wconn 阈值口径均为已立/已驳/在途票覆盖项，本轮仅补机制对比，未翻案未重复立项

视角结论:已穷尽
