来源：next/agy.design.md（review-design：数据链条/死代码/重复机制/常量工具/模块拓扑）分拣裁决档案。
核销时间 2026-09-19，取证基线为主仓 /Users/z/git/db/wedb 分支 dev 当下工作树（行号按符号重取）。
本档案只收「观点不成立、已删行、不动代码」的条目；成立项见 task/ing/ 同名票，
已落地项在回报中给证据不入本档。
并单提示：并发代理已按单题另立 task/reject/design-status-enum-unify.md（条 2）、
design-txn-locktable-anchor-remount.md 与 design-anchor-dup-false-positives.md（条 14 面）、
design-snapshot-triple-struct-unify.md（条 16）、design-wcol-wresp-dep-inversion.md（条 21）、
design-itembroker-move-wnode.md（条 22）、design-null-frame-already-sourced.md（条 10，
本档按「已落地」处置），六处结论与本档一致，归档时可择一保留、勿双写代码改动。
源文件 next/agy.design.md 已由本波剪空（并发代理先行 rm，末态一致）。

条 2 状态与结果枚举跨层三套各自定义 —— 不成立
票面原文：GarnetStatus 在 wnode，StoreResult 在 wkv 会话，ObjLoad 在 wcol，三者描述命中、
缺失、类型错误、异步降级等重叠语义，各层映射靠口头约定；rust：wedb/wnode/src/types.rs
enum GarnetStatus，wedb/wkv/src/session/raw/read.rs enum StoreResult，
wedb/wcol/src/object_payload.rs enum ObjLoad；c#：libs/server/API/GarnetStatus.cs、
libs/storage/Tsavorite/cs/src/core/Index/Common/OperationStatus.cs；
动作：在 wval 或 wbase 建立统一状态转换 trait 与映射契约，消除各层手写映射。
拒绝理由：
1. 三枚举与 C# 三域一一对位，不是分裂：/Users/z/git/db/wedb/wedb/wnode/src/types.rs:10
   GarnetStatus 变体恰为 C# /Users/z/git/db/wedb/garnet/libs/server/API/GarnetStatus.cs:8
   的 OK/NOTFOUND/MOVED/WRONGTYPE 四臂；/Users/z/git/db/wedb/wedb/wkv/src/session/raw/read.rs:22
   StoreResult 三臂即 Tsavorite OperationStatus（SUCCESS/NOTFOUND/RECORD_ON_DISK，
   garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/OperationStatus.cs:21 起）；
   /Users/z/git/db/wedb/wedb/wcol/src/object_payload.rs:125 ObjLoad 的 Degrade 臂
   是本项目分层存储自定义态（SKILL 集合自适应混合分层存储架构），C# 无对应域。
2. 「各层映射靠口头约定」不实：StoreResult→ObjLoad 的判定全部集中在
   /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/object_store_utils.rs 的四支装载函数内
   （:254、:330、:392、:455），是单点而非散落。
3. 修法违反对标与依赖方向：让 wval/wbase（数据与基础层）持有跨层状态转换 trait，
   等于底层感知 wnode 会话语义与 wcol 分层降级语义，是把 C# 的两库分层压平，
   属 SKILL.md「尽量 1:1 对标 c#，不要实现自己的优化」明令避免的额外架构。
同题另见 task/reject/design-status-enum-unify.md（next/muse.design.md 条 15 侧的并单裁决，
结论一致）。

条 14 索引桶闩锁与事务锁表同名同挂锚点 —— 不成立
票面原文：windex HashBucket 四函数为原子位真实现，wtxn TxnLockTable 四函数为下标转发薄壳，
两层同名同挂四组锚点报重复；动作：wtxn 侧去除锚点并注明委托调用，消除 check.js 误报。
拒绝理由：
1. 修法本身被 SKILL 明令禁止：.agents/skills/transpile/SKILL.md:65 对 check.js 重复定义
   「禁止简单的通过修改注释绕过检查」，本条动作恰是改注释消警，不是去重。
2. 事实不构成重复：/Users/z/git/db/wedb/wedb/wtxn/src/txn_lock_table.rs:108、:116、:124、:132
   四函数体各为一行转发（`self.pin().bucket(bucket).try_lock_shared()` 等），
   无任何算法复写；按本次分拣口径「跨层调用薄包装不算重复」。
3. 锚点也并非同挂一处：wtxn 侧四锚指向
   garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Locking/
   OverflowBucketLockTable.cs（txn_lock_table.rs:114、:122 行内可见），
   与 windex HashBucket.cs 的桶内闩是两个 C# 出处，同名的根源是 C# 本身同名方法。
4. 该面的真问题（wtxn 自建恒定 1024 条带表 = 第二套锁架构 + 三处虚构 C# 出处注释）
   已在单问题票在册：next/wtxn-lock-stripe-count-parity.md，落地该票即自然消解本条报的重复，
   无需注释处置。

条 16 快照统计三层同名结构体与双重投影冗余 —— 不成立
票面原文：wkv WedbStore::store_snapshot 输出 StoreSnapshot，wnode 经两级投影转 DbSnapshot 与
AofSnapshot 再送入 wmetric，字段大面积同名重复；动作：快照结构由 wmetric 统一定义。
拒绝理由：
1. 与 C# 分层同构：/Users/z/git/db/wedb/garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs:298
   GetDatabaseStoreStats 逐字段读 `db.Store.*`（CurrentVersion/IndexSize/Log.PageSizeBytes…），
   :366 GetDatabasePersistenceStats 逐字段读 `db.AppendOnlyFile.Log.*`——
   C# 本就是「存储层出数、指标层投影」两层，且两个投影的数据源不同（存储 vs AOF），
   不存在同一份数据被双重投影。
2. rust 侧同形对位：/Users/z/git/db/wedb/wedb/wkv/src/store/stats.rs:45 StoreSnapshot
   （存储层出数）→ /Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/mod.rs:203
   project_db_snapshot（文档注释即锚 GarnetInfoMetrics.cs:GetDatabaseStoreStats）与
   :182 project_aof_snapshot（对位 GetDatabasePersistenceStats），
   两函数各自组装 wmetric::DbSnapshot / wmetric::AofSnapshot 的一个源，字段名相同是
   因为二者承载同一份 C# 指标字段名（INFO 面板文案要求）。
3. 修法造成反向依赖：把 StoreSnapshot 并入 wmetric 意味着 /Users/z/git/db/wedb/wedb/wkv
   要依赖 wmetric（指标 crate），存储层依赖指标层，拓扑倒置；C# 的对应类型（Tsavorite
   StoreSnapshot / 各 Store 属性）恰恰定义在存储侧。
同题另见 next/muse.design.md 条 9（由该文件分拣代理并单处置）。

条 21 wcol 反向依赖 wresp 导致对象层与线协议层倒置 —— 不成立
票面原文：wcol 的 Cargo.toml 直接依赖上层 wresp，仅用于 ObjectOutput 与 RespInputFlags；
动作：将二者移至 wresp 或 wnode，解除 wcol 对 wresp 依赖，还原纯数据对象层。
拒绝理由：
1. 无环即无倒置：/Users/z/git/db/wedb/wedb/wresp/Cargo.toml 不含 wcol/wkv/wval
   （`grep -n "wcol\|wval\|wkv" wresp/Cargo.toml` 零命中），wcol→wresp 是单向依赖，
   不构成拓扑反转。
2. 「纯数据对象层」的前提与 C# 相反：C# 对象实现层直接产出 RESP 帧——
   /Users/z/git/db/wedb/garnet/libs/server/Objects/Hash/HashObjectImpl.cs:28、:41、:63、:122、
   :254、:289、:358、:436、:465、:509 每处操作都 `new RespMemoryWriter(respProtocolVersion,
   ref output.SpanByteAndMemory)` 直写应答；/Users/z/git/db/wedb/garnet/libs/server/Objects/
   Types/ObjectOutput.cs:36 的 ObjectOutput 本身就挂着 SpanByteAndMemory 输出缓冲。
   rust 的 /Users/z/git/db/wedb/wedb/wcol/src/resp/output.rs、/Users/z/git/db/wedb/wedb/
   wcol/src/hash/hash_object_impl.rs 正是这一形态的转写，依赖 wresp 是 1:1 对标结果。
3. 收益为零的搬家：ObjectOutput/RespInputFlags 的定义点已在 wcol（对象层），
   移到 wresp 只是换文件位置，不消任何重复；且 wcol/Cargo.toml:25 的 wresp 依赖已被
   在途单问题票按「依赖本就在场」使用（next/cmd-strings-input-token-single-source.md 修法 2、
   next/resp-frame-literal-single-source.md 单点在场段均以 wcol 能直用 wresp 为前提），
   解除依赖会与两票冲突。

条 22 wcol 承载异步任务调度器与运行时强耦合 —— 不成立
票面原文：CollectionItemBroker 依赖 compio 与 crossfire 运行时，被置于底层集合 crate 内；
动作：将 itembroker 移至 wnode 业务服务层，从 wcol 剥离异步调度与通道依赖。
拒绝理由：
1. 与 C# 拓扑不符：/Users/z/git/db/wedb/garnet/libs/server/Objects/ItemBroker/
   CollectionItemBroker.cs 位于 Objects 层（同目录还有 CollectionItemBrokerEvent.cs、
   CollectionItemObserver.cs、CollectionItemResult.cs），即微软就把取件经纪放在对象层，
   rust 的 wcol/src/itembroker/（collection_item_broker.rs、collection_item_observer.rs、
   collection_item_broker_event.rs、item_broker_face.rs）与之同构；移去 wnode 是主动偏离。
2. 「底层无运行时依赖」在 C# 侧同样不成立：CollectionItemBroker.cs:11 起
   `using System.Threading; using System.Threading.Tasks;`，:31 `AsyncQueue<CollectionItemBrokerEvent>`、
   :34 `ConcurrentDictionary<int, CollectionItemObserver>`、:55 `SingleWriterMultiReaderLock`、
   :57 `CancellationTokenSource`、:60 `ManualResetEventSlim`、:63 `Task mainLoopTask`——
   它是自持异步循环 + 并发容器的对象层组件。rust 用 compio/crossfire/event-listener
   对位这三类设施（wcol/Cargo.toml:29-31）是同一形态的直译。
3. 搬家不消耦合：wcol/Cargo.toml:27-31 的 compio/crossfire/event-listener 消费面实测仅
   itembroker 目录四文件（`grep -rln "compio\|crossfire" wedb/wcol/src` 命中
   item_broker_face.rs、collection_item_observer.rs、collection_item_broker.rs），
   但阻塞族命令的取件协议本身就在对象层（对象信封的 BLPOP/BZPOPMIN 语义），
   移到 wnode 后 wcol 仍需向 wnode 反向暴露通知口，新增一层跨 crate 接口而零收益。
4. 真正的在册缺口是「经纪接线面」而非位置：见
   next/wnode-service-open-node-zero-consumer.md 与 wnode/src/resp/resp_server_session.rs:561
   set_item_broker 的装配链，本条动作与之无关。
