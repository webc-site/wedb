PENDING_LAT 全仓零计时点：存储会话 pending 闭环只计数，ignore 理由却声称延迟计时已由会话层承接

来源：next/glm.net.md 条 1（该文件已分拣清空删除）。逐句按主仓当下代码复核后判定成立待做。
取证基线：主仓 /Users/z/git/db/wedb，行号按符号定位。
载体唯一性：并发拆条在 next/ 下另留了一份本条原文照抄的壳（basename
pending-lat-zero-record-point.md，只加「优先级：中」头、无取证订正），以本文件为唯一载体，
派单前先剪壳勿双花；该壳照抄的原文里 task/done/session-metrics-counter-single-source.md 的引用
已失效（该票当下不存在）。

结论

C# 在存储会话的每一次异步 pending 闭环上都记 PENDING_LAT 延迟，rust 的对位挂点只记了 pending 计数、
没有记延迟，而声称「延迟由会话层承接」的承接面在全仓并不存在。结果是延迟监视开启时
LATENCY HISTOGRAM 的 PENDING_LAT 桶恒为空直方图，C# 可观测的 pending 完成延迟分布在 rust 不可观测。
同时 ignore 条目把一件没做的事写成了已做成的理由，属声明与实现相反。

现状

一、rust 挂点只计数。/Users/z/git/db/wedb/wedb/wnode/src/storage/session/storage_session.rs:122-139
`with_pending_metrics` 的函数体是 `if let Some(metrics) = &self.session_metrics { metrics.incr_total_pending(1) }`
后直接 `f().await`（:135-138），无 start/stop。其 :126-128 的文档注释自述「C# StartPendingMetrics =
incr_total_pending + PENDING_LAT 计时，Stop 仅收表；rust 会话侧延迟表由 RespServerSession 的
latency_metrics 单一承接（存储层不持有会话延迟对象）」。本函数是唯一的 pending 漏斗，生产调用点
八处全在同文件：:157、:166、:193、:202（读面）、:298、:354、:372、:391（写面），
即全部异步闭环都经过这里，也全部不记延迟。

二、所谓承接方只记四类。/Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:1078-1093
`latency_batch_start` 只 `latency.start(LatencyMetricsType::NetRsLat, ..)`；:1098-1118
`latency_batch_stop` 只 stop NetRsLat（含 :1106-1112 慢命令批次切 NetRsLatAdmin）并
record_value NetRsBytes / NetRsOps。全仓 grep `PendingLat|pending_lat|PENDING_LAT` 命中面仅
/Users/z/git/db/wedb/wedb/wmetric/src/latency/latency_metrics_type.rs:14（枚举变体）、:29（ALL 集合）、
:53（cs_name）、:66-67（from_name 解析）与上面那条注释，无任何 start/stop/record 调用点。
计时原语本身是现成的（/Users/z/git/db/wedb/wedb/wmetric/src/latency/garnet_latency_metrics_session.rs:78
`start`、:142 `stop`），只是没人把 PendingLat 接上去。

三、空桶对外可见。/Users/z/git/db/wedb/wedb/wmetric/src/latency/resp_latency_commands.rs:53-54
与 :78 在无参形态下按 `LatencyMetricsType::ALL` 回显全部六类，PENDING_LAT 恒空。

四、ignore 理由与实现相反。/Users/z/git/db/wedb/js/check/ignore/garnet/libs/server/Storage/Session/Metrics.yml
:1-4 写「pending 延迟计时归会话层 latency_metrics 单一承接」、:12-14 写「StopPendingMetrics 的
PENDING_LAT 收表在 wedb 无存储层延迟对象可达面（延迟计时唯由 RespServerSession.latency_metrics 承接）」，
两句均为不实描述。原条目另引 task/done/session-metrics-counter-single-source.md:52 为共犯，
该 done 票当下不存在（/Users/z/git/db/wedb/task/done 无此文件），不再作为证据，以 ignore 文本为准。

C# 参考

- /Users/z/git/db/wedb/garnet/libs/server/Storage/Session/Metrics.cs:10-12 存储会话同持
  `latencyMetrics => LatencyMetrics`（`readonly GarnetLatencyMetricsSession LatencyMetrics`），
  :23-27 `StartPendingMetrics` = `sessionMetrics?.incr_total_pending()` + `latencyMetrics?.Start(PENDING_LAT)`，
  :29-32 `StopPendingMetrics` = `latencyMetrics?.Stop(PENDING_LAT)`。
- /Users/z/git/db/wedb/garnet/libs/server/Storage/Session/MainStore/AdvancedOps.cs:43-45 与 :65-67
  两个 `GET_CompletePending` 重载在 `CompletePendingWithOutputs` 前后内联 Start/Stop。
- 查询名解析 /Users/z/git/db/wedb/garnet/libs/server/SessionParseStateExtensions.cs:75-91（:82-83 即 PENDING_LAT）。

修法

1. 存储会话按 C# Metrics.cs:12 的形态持一枚可选延迟表句柄：`StorageSession` 增
   `latency_metrics: Option<Arc<GarnetLatencyMetricsSession>>` 字段与
   `with_latency_metrics(..)` 装配方法，与既有 `with_session_metrics`
   （/Users/z/git/db/wedb/wedb/wnode/src/storage/session/storage_session.rs:64-67）并列为一条注入轨，
   None 即零开销（对位 C# 的 `latencyMetrics?.`）。
2. `with_pending_metrics` 在 `f().await` 前后各取一次 `now_stopwatch_ticks()` 做 start/stop，
   形如 C# Start 与 Stop 之间的 CompletePending 段；表不在位时不做任何计时与取时。
   本仓 `start/stop` 需显式刻度入参，取时点用
   /Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:1089 与 :1105 的同一 `now_stopwatch_ticks()` 口径，
   不引第三套时钟。
3. 装配面两处下传，与会话指标同轨：慢路径
   /Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/slow.rs:102-108 由 `StoreGarnetApi` 侧字段 clone 下传
   （该字段与 `session_metrics` 同源建在 garnet_api 上）；provider 侧
   /Users/z/git/db/wedb/wedb/wnode/src/service.rs:1598-1611 在 `StoreGarnetApi::new(session)` 交出 session 之前
   先 `session.latency_metrics.clone()`，再与 `with_session_metrics` 一并注入，保持「会话与执行域共持同一对象」
   的既有单点注入语义（延迟表创建点在
   /Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:449-459，门控 `options.latency_monitor`）。
   非会话面（AOF/复制重放、事务过程视图、周期收集）构造不出会话延迟表，自然落 None，与 C#
   这些面 `latencyMetrics` 为 null 同形。
4. 订正声明面：/Users/z/git/db/wedb/js/check/ignore/garnet/libs/server/Storage/Session/Metrics.yml
   的理由删掉「延迟计时唯由 RespServerSession.latency_metrics 承接」这类不实句；
   `StartPendingMetrics` 既然在 rust 由 `with_pending_metrics` 承接全部语义（计数 + 延迟），
   按 check.js 的锚点登记口径把它挂到该函数上；`StopPendingMetrics` 的收表点同样落到
   `with_pending_metrics` 的 stop 半段，不再作为独立 ignore 项保留「不可达」的说法。
   同文件的 `incr_session_notfound`、`incr_session_pending` 两句陈述与实现相符，保持不动。
   storage_session.rs:122-128 的注释同步改写为真实承接关系。
5. 不做的事：TX_PROC_LAT 同样零记录点，但 C# 唯一记录面在自定义命令域
   （/Users/z/git/db/wedb/garnet/libs/server/Custom/CustomRespCommands.cs:26 与 :51 的
   Start/Stop(TX_PROC_LAT)，本仓按 transpile 既定删除自定义命令动态注册管理层），
   枚举保留只为 1:1 的 LATENCY 名字解析表服务，不在本单射程，勿顺手补计时。

优先级

污染扩散（ignore 理由与实现相反、注释自述不存在的承接面）叠加功能缺口（可观测面缺失），
排在死代码与重复架构清理之后、纯打磨之前。第 4 步的声明订正不得晚于第 1-3 步的实现落地：
若决定不做计时，则必须把两处声明改成「明确不承接 PENDING_LAT，理由……」，二者只居其一。

交叉引用

- 同一装配点的会话指标双轨问题（选项侧死轨 vs service.rs 真实轨）已单独立票：
  task/ing/session-metrics-option-dead-track.md。两票共用 service.rs:1598-1611 与
  resp_server_session.rs 的构造段，同批开工可共用一次文件改动，判定各自独立；
  本票只加延迟轨，不引入第二处 session_metrics 创建点。
- 采样与延迟监视的启动期互校验（C# LatencyMonitor 要求 MetricsSamplingFrequency 非零）落在
  task/ing/boot-assembly-projection-single-source.md，本票不重复立校验点。
- 客户端侧延迟直方图（wconn `record_latency` 恒 false、查询 API 无生产读者，
  /Users/z/git/db/wedb/wedb/wconn/src/client.rs:53）是同族不同面：
  task/ing/client-latency-histogram-unwired.md。那票管客户端出向延迟开关，本票管服务端会话
  pending 完成延迟，两套直方图互不相干，勿合并判定。

验收

- 开延迟监视的连接上跑一次走磁盘候选降级（即真正 pending）的读命令，
  LATENCY HISTOGRAM PENDING_LAT 出非空桶；LATENCY HISTOGRAM 无参回显里 PENDING_LAT 不再是空项。
- 延迟监视关闭（`latency_monitor = false`）时 `with_pending_metrics` 零额外取时、零分配，
  pending 计数行为不变。
- /Users/z/git/db/wedb/wedb 全仓 grep `PendingLat` 至少出现于枚举、解析与一处 start/stop 调用点。
- ignore 文本不再声称存在会话层承接，`./js/check.js` 无新增缺失项。
- 集成测试入 /Users/z/git/db/wedb/wedb/wnode/tests（现有延迟面用例见
  /Users/z/git/db/wedb/wedb/wmetric/src/latency/resp_latency_commands.rs:136-148 的单测口径），
  清理 C# 没有的冗余断言。

落地订正（开发棒，分支 pending-lat-timing-site）

一、取证逐条独立复验成立：with_pending_metrics 全文只有 `incr_total_pending(1)` +
`f().await`（八处调用点全在 storage_session.rs：读面四处 + 写面四处），全仓
`PendingLat` 命中仅 wmetric/src/latency/latency_metrics_type.rs:14、:29、:53、:66-67
与本票点名的注释/ignore 文本，零 start/stop；resp_server_session.rs 的
`latency_batch_start`/`latency_batch_stop` 只碰 NetRsLat/NetRsLatAdmin/NetRsBytes/NetRsOps。

二、修法第 3 步的 provider 侧下传口径与代码事实不符，据原意（单一创建点不动、
装配期单点注入、会话与执行域共持同一对象）改道，不改文档正文以留痕：
provider 侧 `StoreGarnetApi::new(session)`（/Users/z/git/db/wedb/wedb/wnode/src/service.rs:1588）
的 `session` 是 wkv 连接级 `StoreSession`（同文件 :1553 `self.store().new_session()`），
不持有 RESP 会话延迟表；延迟表创建点在
/Users/z/git/db/wedb/wedb/wnode/src/resp/resp_server_session.rs:447（门控
`options.latency_monitor`），而会话由 decorate 钩子在 api 交出之后才构造
（service.rs:1600），即执行域构造并被静态虚表擦除（garnet_api/mod.rs 的 `GarnetApi`）
早于延迟表诞生，provider 无从 clone。落地的注入轨因此是「会话回挂执行域」而非
「provider 下传」：

- 执行域新增回挂口 `GarnetApiFace::attach_latency_metrics`（缺省空实现，
  非存储域宿主自然无延迟面，garnet_api/mod.rs:112；StoreGarnetApi 实现在 :629，
  槽位字段 :402，读口 :453），与会话侧既有 `attach_session_metrics` 同族形态；
- 单点回挂落在执行域挂入会话的同一函数
  resp_server_session.rs:543 `set_garnet_api`（单机与集群两条构造路
  resp_session_consumer.rs:46、:66 都经此），持会话自己的 `latency_metrics` 同一 Arc；
- 晚装配换表面 resp_server_session.rs:877 `attach_monitor` 同点重挂，
  杜绝执行域与会话分表；
- 慢路径 storage 构造处与 session_metrics 并列下传
  /Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/slow.rs:110；
- 非会话面不回挂、自然落 None（AOF 重放 aof/garnet_append_only_file.rs:391、
  provider 内部服务面 service.rs:461、事务过程视图 garnet_api/mod.rs:638、
  周期收集 primary_tasks.rs:335、后台降阶 objects/tiered_demote.rs:75），
  与 C# 这些面 `latencyMetrics` 为 null 同形。

三、计时实现按修法第 2 步：storage_session.rs:151 `with_pending_metrics` 在
`f().await` 前后各取一次 `now_stopwatch_ticks()`（与 resp_server_session.rs:1097、:1113
同一计时源），起停 `LatencyMetricsType::PendingLat`；表不在位走 `return f().await`
早退分支，零取时零分配（storage_session.rs:39 字段、:80 装配方法）。

四、声明面订正按修法第 4 步落地：storage_session.rs:140-141 双锚点
（`Metrics.cs:StartPendingMetrics` 与 `Metrics.cs:StopPendingMetrics` 同挂本函数），
:143-150 注释改写为真实承接关系；/Users/z/git/db/wedb/js/check/ignore/garnet/libs/server/
Storage/Session/Metrics.yml 删 `StopPendingMetrics` 条目与「延迟计时唯由
RespServerSession.latency_metrics 承接」「无存储层延迟对象可达面」两句不实描述，
`incr_session_notfound`、`incr_session_pending` 与实现相符保持不动。
check.js 的自动淘汰本就会在锚点在场时剪掉该 ignore 条目，手改与工具收敛态一致。

五、门禁实测（worktree /tmp/fork/pending-lat-timing-site，
CARGO_TARGET_DIR=/tmp/fork/pending-lat-timing-site/target）：
`cargo check -p wnode --tests` 通过、零警告；`bun js/check.js` 相对基线树
（/tmp/fork/pending-lat-baseline @ 分支基点）输出逐字节相同、语料零回写、退出码 0；
全仓 grep `PendingLat` 现为枚举/解析 + storage_session.rs:162、:164 起停调用点 + 测试。
新增集成测试 /Users/z/git/db/wedb/wedb/wnode/tests/pending_latency_timing.rs：
开延迟监视的会话上 flush_and_evict 后跑冷键 HGET（真 pending 闭环），断言会话延迟表
PENDING_LAT 槽出样本、经监视器同款归并口后 LATENCY HISTOGRAM 无参回显含 PENDING_LAT
且不串网络桶；关延迟监视断言会话不建延迟表且同命令应答逐字节一致。
测试执行（test.sh/clippy.sh）按 brief 未跑，留主代理收口。

收尸订正（salvage 棒，落 dev 时对本票前手成果的处理）

一、dev 在落地后已把 garnet_api 的手写静态函数指针虚表收敛为
`pub type GarnetApi = Arc<dyn GarnetApiFace>`，前手为擦除句柄加的那套槽位
（`GarnetApi.attach_latency_metrics` 指针字段、`attach_latency_metrics_fn<T>`
 tramp、Clone 透传）在 dev 上已无载体，落 dev 时整块舍弃，只保留等价两面：
 `GarnetApiFace::attach_latency_metrics` 缺省空实现 + `StoreGarnetApi` 覆写
 （槽位仍 `Mutex<Option<Arc<GarnetLatencyMetricsSession>>>`，`&self` 形参下
 回挂必须内部可变，非为 attach_monitor 而设）。慢路径读口
 `StoreGarnetApi::latency_metrics()` 与 `slow.rs` 下传点不变，注入轨仍单点。

二、`set_garnet_api` 在 dev 已收为 `api: GarnetApi`（无 `impl Into`），
新测试的 `StoreGarnetApi::new(..).into()` 随之改 `Arc::new(..)`（dev 现有
冷降级用例同款）。订正 二 对 provider 侧无从 clone 的判断在 dev 上复验成立
（`service.rs` 仍在 `decorate` 建会话之前构造 `StoreGarnetApi::new(session)`，
session 为 wkv 连接级会话），回挂方向不变。订正内各行号为前手树位，已漂移。

三、前手第五步未跑的新测试由本棒首跑：`cargo test -p wnode --test
pending_latency_timing` 2 passed（真 pending 降级读出 PENDING_LAT 样本、
归并回显含 PENDING_LAT 且不串 NET_RS_LAT；关监视器应答逐字节一致）。
门禁 `cargo check --workspace --all-targets` exit 0 零警告、worktree 内
`bun js/check.js` exit 0 且 ignore 语料零回写。

四、`attach_monitor` 当下仍零生产调用点（唯一读者
wedb/wnode/tests/server_monitor_tests.rs:183，已登记
next/zero-consumer-dead-surfaces-batch-six.md），本票在其内加的同点重挂是
「执行域与会话同表」不变量的对称处置、不构成新接线；该票若判删 attach_monitor，
重挂段随其一并删，PENDING_LAT 主链路（构造期 `set_garnet_api` 回挂）不受影响。

五、票面「现状 一」的「八处调用点即全部异步闭环」有反例，本棒按 brief 未顺手补，
留主代理另立条：C# 记 PENDING_LAT 的点全仓仅三处——`Metrics.cs:23 StartPendingMetrics`
（MainStoreOps/UnifiedStoreOps 十处调用）与 `MainStore/AdvancedOps.cs:38`、`:62` 两个
`GET_CompletePending` 重载（后者只计时、不计数，调用方
Resp/BasicCommands.cs:317、Resp/AsyncProcessor.cs:90、Resp/MGetReadArgBatch.cs:173 全是
MGET 族批量 scatter-gather）。rust 对位 `StorageSession::read_string_batch_into`
（storage_session.rs:305、:306 的 `read_batch_with(..).await`）不经 `with_pending_metrics`，
故 MGET 冷读仍不出 PENDING_LAT 样本；直接套本漏斗会多计 C# 没有的 `incr_total_pending`，
需一枚「仅起停表、不计数」的对位口。TTL 面（`persist`/`pttl_ms`/`expiretime_ms` 的裸 await）
经核对 C# 无 PENDING_LAT 记录点，保持不经漏斗与 C# 同形，非缺口。

六、并发回滚事故与恢复：本棒 merge 273b49ee 落 dev 后，主仓共享工作树的旧树副本被
并发提交 1d0acb66（feat: qw13.invA/B 盘点归档…）以 pathspec 方式整块回退——6 路径
逐字节退回 6aea5715 态、新测试文件被删。恢复按收尸规程不重 merge：五文件
（mod.rs/resp_server_session.rs/storage_session.rs/新测试/Metrics.yml）取
`git checkout 273b49ee -- <path>` 原 blob，slow.rs 因 961c2b1a 的 LASTSAVE 注释在其上
已前行 4 行，故按 diff 重贴 `.with_latency_metrics(self.latency_metrics())` 注入轨，
不留重复。
