归档注记（主代理 2026-09-26 fix.md 波批合并）：合入 2febc39（P2），收口形态：SCAN 出帧键逐键一致读 pre/post 包裹（保守口径 §155 登记，让号于 CLIENT KILL §154）＋Live/Degrade 合轨＋跨子日志红绿臂夹具。续排注：本票沙箱席与方案详情见下文。

甄别结论：通过（甄别席 zc-fix-r16-scanconsist，2026-09-26）定级 P2
1 C# 四锚现树亲验成立：RespServerSession.cs:494-508 EnforceConsistentRead 整批
  ProcessMessages(ref consistentReadGarnetApi) 无 SCAN 豁免（票面 :496-508 偏 2 行
  非实质）；ArrayKeyIterationFunctions.cs:90 DbScan 装配逐字命中；:271-277
  ConsistentUnifiedStoreGetDBKeys.Reader pre→base→post 逐记录包裹属实；
  ReadConsistencyManager.cs:298-330 VerifyKeyFreshness 跨子日志 WaitForSequenceNumber
  等待、:388-392 PostSingleKey 单调抬 mssn 属实。
2 rust 五锚亲验成立：scan_cursor（array_key_iteration_functions.rs:211-313）全函数
  grep 无 consistent_read 触点，裸 hlog 扫描；同文件 db_keys:634-656 逐键
  with_consistent_read 在案先例属实；service.rs:2083-2090 multi_log 管理器在场逐会话
  attach（票面行号偏 3 行非实质）；replica_read_session_context.rs:284-307 pre 同步
  签名 role_gate 非副本直通；wkv/session/consistent_read.rs:124-133 single_key_around
  在位；点读族 read_tag_quiet:252 / read_tag_with_size:278 Some(ctx) 接线属实；
  慢臂 resp/garnet_api/slow.rs:612,629 C::Scan 直调 storage.scan_cursor，快路径
  network_scan 仅校验降级——SCAN 唯一未接线面坐实，非架构取舍。
3 查重：deviations.md 全册 grep 零覆盖（§20 系 COUNT/TYPE/游标对齐三面、§80/§129
  系词形与锁窗，均异轴）；task 四池无同轴票。与同批 wnode-tiered-scan-start-
  cursor-overflow 判不同轴不并案：彼系对象族 HSCAN/ZSCAN 分层游标溢出守卫
  （tiered_collection_ops/scan.rs），本票系主键空间 SCAN 一致读协议接线，文件、
  机制、危害面全异，仅共用回归测试文件。
4 方案合规：复用 single_key_around 协议单点与 consistent_read_hash_with_prefix
  （wkv/session/consistent_read.rs:98 在案）既有单源，前缀外提合 transpile 纪律；
  ctx None 零开销、仅包出帧键的保守口径 mssn 只低估不虚高，无新机制无假桩无
  过度设计；测试锚 scan_family_dualstate_frames.rs 与 keyspace_parity_r43.rs 均在案。
5 定级 P2：multi_log 副本拓扑会话前缀一致性击穿（新值/陈旧值混排、幻影漏键），
  非崩溃非数据损坏，限主从一致读启用面。

审核结论：通过（审核席 zcode-r18-review-scanconsist，2026-09-26）

审核亲验记录（双侧现码逐锚复核，票面事实全部属实）：
1 C# 侧四锚亲读属实：RespServerSession.cs TryConsumeMessages 的 EnforceConsistentRead
  放行臂整批 ProcessMessages(ref consistentReadGarnetApi)（SCAN 不豁免）；
  ArrayKeyIterationFunctions.cs DbScan 装配行 IsConsistentReadSession ?
  new ConsistentUnifiedStoreGetDBKeys(readSessionState) : new UnifiedStoreGetDBKeys()；
  ConsistentUnifiedStoreGetDBKeys.Reader 为 pre → base.Reader → post 逐记录包裹
 （base 判 Skip 的记录同样跑 pre/post）；ReadConsistencyManager.VerifyKeyFreshness
  跨虚拟子日志且 mssn >= 子日志重放前沿时 WaitForSequenceNumber 等待追平，
  PostSingleKeyConsistentRead 以刚读键序列号单调抬升 mssn。
2 rust 侧五锚亲读属实：scan_cursor（array_key_iteration_functions.rs:211-313）
  全程无 consistent_read_context 咨询，live_key_at 判活、glob/TYPE 过滤、逐键出帧
  全为裸 hlog 扫描；同文件 db_keys（:634-656）逐键 ctx.with_consistent_read 已接线；
  service.rs:2083-2090 multi_log 拓扑（read_consistency_manager 在场）逐会话 attach
  read_session_state（票面 :2086-2092 行号偏 3 行非实质）；
  replica_read_session_context.rs:284-307 pre 入口 role_gate 非副本直通零等待；
  wkv/session/consistent_read.rs:124-133 single_key_around 协议单点在位。点读族
 （read_tag_quiet :252 / read_tag_with_size :278 等）均 Some(ctx) 才接线；
  SCAN 慢臂（resp/garnet_api/slow.rs C::Scan）直调 storage.scan_cursor——接线
  漏项坐实，唯 SCAN 一面双侧分叉（C# 接 / rust 不接）。
3 危害形态成立：multi_log 副本会话先点读子日志 A 抬 mssn 后 SCAN，pre 缺失致
  扫到子日志 B 键时不等待 B 侧重放追平，单次 SCAN 应答混排 A 新值 / B 陈旧值、
  B 键可因创建条目未重放而整体缺席；post 缺失致 mssn 不抬升、后继点读粘滞
  水位低估。系会话前缀一致性击穿，非架构取舍（KEYS 同文件已接线为先例锚）。
4 查重：deviations.md 全册（§20 SCAN 域为 COUNT/游标形态不同面、§87 BITOP
  折叠读、§88 重放侧组免锁均机制不同源）零在案登记；review_history 全档零撞。
5 方案判定：复用既有 single_key_around 协议单点，单机制零新增协议面；ctx 为
  None（非 multi_log 拓扑）零开销维持现状，multi_log 拓扑非副本时 role_gate
  一次原子读直通（与点读族/KEYS 同口径），主路径无额外开销；票面方案 2 的
  保守等价论证成立——未出帧键客户端未观察其状态、无撕裂前提，mssn 单调
  不虚高，且免对全库死键逐条 pre 等待的扫速塌缩。
6 格式合规：纯文本、双侧路径齐全。

优化执行方案（供 task/fix.md 直接消费）：
1 scan_cursor 入口取一次 let ctx = self.consistent_read_context()（Some 才启用，
  None 分支与现状逐字一致，主路径零开销）。循环内前缀已外提为 prefix_slice，
  出帧键哈希走 consistent_read_hash_with_prefix(prefix_slice, tag, user_key)
  单点（免逐键重读 ns/db 原子变量，与回放侧草图入账同键同标签同哈希；tag 取
  live_key_at outcome 的 Live(tag, _) / Degrade(tag, _) 三域标签，String /
  ObjectEnvelope / Meta 与 AOF 入账域一致）。
2 包裹点为出帧动作：Live 臂 items.push(user_key) 处、Degrade 臂 is_ttl_expired
  异步复判通过后的 push 处，经 ctx 侧单键 pre/post 包裹（pre 同步签名
  pre_single_key_consistent_read，无需异步包裹器）；pre 超时沿既有 wkv Err
  通道上抛，RESP 层 err_frame，与点读族同口径。对齐 C# ConsistentReader 在
  Skip 记录上也跑 pre/post 的差异按保守口径处理：仅包出帧键，mssn 只会低估
  不会虚高，前缀一致性不被破坏（未出帧键客户端未观察，无撕裂前提）。
3 帧内逐键包裹不建议升级为页级批量（pre_batch 协议面向同批显式键列表，
  SCAN 页内键集在遍历中增量发现，改批量属引入第二套接线形态，违反单机制）。
4 测试验证点：multi_log 拓扑两子日志副本会话用例——子日志 A 先点读抬水位后
  SCAN 含 B 子日志键，断言 B 键重放追平前 SCAN 挂起 / 追平后应答（或按第 2
  条保守口径断言 B 键陈旧值不出帧窗口）；单日志 / 主库拓扑 SCAN 行为零回归
 （scan_family_dualstate_frames 与 keyspace_parity_r43 全族既有用例通过）。

以下为原票面：

SCAN 主键空间扫描未接入副本一致读协议，multi-log 副本会话前缀一致性被击穿

问题分析：
1. Garnet 契约对齐（C# 原型行为）：C# 在 multi-log 拓扑副本上 EnforceConsistentRead
   放行时，会话全部命令（含 SCAN）整批切到 consistentReadGarnetApi
   （RespServerSession.cs:496-508 ProcessMessages(ref consistentReadGarnetApi...)），
   其 StorageSession 携带 readSessionState，DbScan 据此装配一致读包装迭代器
   （ArrayKeyIterationFunctions.cs:90 `IsConsistentReadSession ? new
   ConsistentUnifiedStoreGetDBKeys(readSessionState) : ...`）。该包装 Reader
   （:271-277）对每条被扫记录执行 PreSingleKeyConsistentRead(hash) + 基础
   判定 + PostSingleKeyConsistentReadCallback：pre 侧 VerifyKeyFreshness
   （ReadConsistencyManager.cs:298-330）在扫描键落入与上次读不同的虚拟子日志
   且会话水位 mssn 越过该子日志重放前沿时等待重放追平，post 侧用刚读键的
   序列号单调抬升 mssn。由此 SCAN 与点读共享同一会话前缀一致性契约：会话
   已观察到序列号 s 之后，任何后继读（含扫描）不见任何子日志落后于 s 的
   陈旧状态。
2. 工程现状确证：rust 侧该协议机制完备且已接线——service.rs:2086-2092 在
   multi_log 拓扑（aof.read_consistency_manager() 在场）为每个客户端会话
   attach read_session_state，pre 入口 role_gate 副本激活
   （replica_read_session_context.rs:284-296）；点读族（storage_session.rs
   read_tag_with/read_tag_with_size/read_tag_quiet_with_prefix 等）与 KEYS
   （db_keys 逐键 ctx.with_consistent_read）均已走 pre/post 协议
   （wkv/session/consistent_read.rs:124-149 single_key_around 单点）。唯独
   SCAN 的扫描内核 scan_cursor（array_key_iteration_functions.rs:211）从
   头到尾未咨询 consistent_read_context：live_key_at 判活、glob/TYPE 过滤、
   逐键出帧全为裸 hlog 扫描，既不做 pre 等待也不做 post 水位推进。
3. 逻辑危害确证：multi-log 副本上，会话先点读子日志 A 的键观察到序列号
   s（mssn=s），随后 SCAN：(a) pre 缺失——扫描走到子日志 B 的键时不等待
   B 侧重放追平 s，单次 SCAN 应答内即出现 A 键新值（>= s）与 B 键陈旧值
   （远落后 s）混合，B 键甚至可因创建条目尚未重放而整体缺席（幻影漏键），
   正是 C# 一致读协议要消灭的会话前缀撕裂；(b) post 缺失——扫描观察到的
   键序列号不抬升 mssn，后继点读的粘滞水位低估。对照：同文件 KEYS 分支
   已接线、C# 侧 DbScan 与 DBKeys 同走 ConsistentUnifiedStoreGetDBKeys，
   唯 SCAN 一面双侧分叉（C# 接 / rust 不接），系接线漏项非架构取舍。

涉及代码：
rust 文件与函数：
wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:scan_cursor（:211-313 全程无一致读协议）
wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:db_keys（:634-656 逐键 with_consistent_read，接线先例对照锚）
wedb/wnode/src/service.rs:WedbServer::create_consumer（:2086-2092 会话 attach read_session_state）
wedb/wnode/src/aof/readconsistency/replica_read_session_context.rs:ReadSessionState（:284-307 pre/post 协议实现）
wedb/wkv/src/session/consistent_read.rs:single_key_around（:124-133 协议单点）

对应 c# 文件与函数：
garnet/libs/server/Resp/RespServerSession.cs:TryConsumeMessages（:496-508 EnforceConsistentRead 全命令切一致读 API）
garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:DbScan（:90 一致读包装迭代器装配）
garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:ConsistentUnifiedStoreGetDBKeys.Reader（:271-277 逐记录 pre/post 包裹）
garnet/libs/server/AOF/ReadConsistency/ReadConsistencyManager.cs:VerifyKeyFreshness / PostSingleKeyConsistentRead（:298-330 / :388-392）
