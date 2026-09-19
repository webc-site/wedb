# qcode10.design 分拣拒绝台账

来源：next/qcode10.design.md（第 10 轮 design 视角：结构与死码普查增量）分拣。取证基线：主仓
/Users/z/git/db/wedb 当下代码 + /Users/z/git/db/wedb/garnet，全部行号按当下重取（原快照
/tmp/rev10/wedb 的旧行号一律作废；该快照目录现已不存在）。

本台账只收「经复核不成立」与「原报自撤初判」两类；条 1、条 2、条 3、条 5、条 6、条 7、条 8、
条 9、条 10、条 11、条 14、条 15、条 16 均未在此列（判定成立，分票见回报：
task/ing/zero-consumer-dead-surfaces-batch-five.md、task/ing/tiered-collection-ops-file-split.md、
task/ing/cluster-provider-file-split.md、task/ing/json-commands-file-split-anchor-decl.md、
task/ing/wreviv-test-record-alignment-second-source.md；条 3 与条 8 在册的
zero-consumer-dead-surfaces-batch-four.md 已于本轮分拣期间合入并移档 task/done，
grep assert_does_not_exist 与 fn writer_p 当下均零命中）。

## 1. 条 5 内一条主张不成立：wresp/src/read.rs:239 try_read_byte_array_with_length_header「C# 同名单点在生产读路径被真调」

原文摘录
「wresp/src/read.rs:239 try_read_byte_array_with_length_header（全仓 5 次出现 = 两处同名定义
+ 三处用例，无生产调用；C# 同名单点 RespReadUtils.cs:725 在生产读路径被真调）」

拒绝原因
C# 侧断言不实。全仓 grep `ReadByteArrayWithLengthHeader` 于
/Users/z/git/db/wedb/garnet 命中唯一一处，即定义本身
/Users/z/git/db/wedb/garnet/libs/common/RespReadUtils.cs:725
`public static bool TryReadByteArrayWithLengthHeader(out byte[] result, ref byte* ptr, byte* end)`，
无任何调用点。rust 1:1 转写该口并挂 C# 锚点即合规（
/Users/z/git/db/wedb/wedb/wresp/src/read.rs:239），不属死码。
真正的残留是 wconn 门面内的同名第二定义（/Users/z/git/db/wedb/wedb/wconn/src/parser.rs:126，
读者全在同文件 :454/:459/:464 用例），该事实由
task/ing/zero-consumer-dead-surfaces-batch-five.md 类一第 9 项承接，与本条结论不冲突。

## 2. 原报「已核查」段撤销初判 1：wext_json::try_select_node 判为硬死不成立

原文摘录
「wext_json::try_select_node 硬死：C# 同件 …JsonExtensions.cs:55 TrySelectNode 亦只被
test/…/JSONPath/JsonPathExecuteTests.cs:51 消费，1:1 同构，不报。」

复核（当下代码）
定义 /Users/z/git/db/wedb/wedb/wext_json/src/json_path/mod.rs:27 为 json_path 目录模块的对外
再导出项（/Users/z/git/db/wedb/wedb/wext_json/src/lib.rs:14 `pub use json_path::{… try_select_node}`），
与 C# 侧「公开工具口 + 仅测试消费」同形态。结论沿用：不立项。

## 3. 撤销初判 2：NodeArgs.unixsocket / config_export_path / HlogOptions.mutable_percent / ConfigMeta 五字段判为断链系口径错

原文摘录
「…判为断链系口径错（读者在 wconf 自身非展示路径：node_options.rs:585 endpoints()、:791
export_config、:172 mutable_fraction 投影、runtime_server_config.rs:723/:814/:835/:998/:1067），已撤。」

复核（当下代码，行号重取）
endpoints 读端两处在 /Users/z/git/db/wedb/wedb/wconf/src/node_options.rs:587 与 :814；
config_export_path 的真实读写链在同文件 :471 字段、:755 `fn export_config`、:789-:798 的
CLI/文件合并分支（`let (import_path, export_path) = (cli.config.clone(), cli.config_export_path.clone())`
与 `merged.config_export_path = export_path`）；
mutable_percent 在 :100 CLI 声明 → :129 mutable_fraction 字段 → :157 换算，是活投影。
结论：非断链，不立项。该字段的「展示投影缺失」反面事实另票承载
（on-demand 之外的 dir/logdir/unixsocket 只读投影，见
task/ing/runtime-options-read-only-path-fields-unprojected.md 与条 6 判定）。

## 4. 撤销初判 3：ObjectScanCountLimit / SgGet / ExpiredObjectCollectionFreq 三槽判为「进表不出表」不成立

原文摘录
「…三条经核为已接线活链（resp/garnet_api/mod.rs:539、resp/basic_commands/get.rs:42、
primary_tasks.rs:174/:255），撤。」

复核（当下代码，行号重取）
ObjectScanCountLimit 生产读者 /Users/z/git/db/wedb/wedb/wnode/src/resp/objects/shared_object_commands.rs:243；
SgGet 读者 /Users/z/git/db/wedb/wedb/wnode/src/resp/basic_commands/get.rs:42；
ExpiredObjectCollectionFreq 读者 /Users/z/git/db/wedb/wedb/wnode/src/primary_tasks.rs:185 与 :266。
三条均为活链，不立项。同段的 replica-sync-delay 不在此列（该槽确零生产读者，
由 task/ing/runtime-config-hot-reload-consumers.md 承接）。

## 5. 撤销初判 4：cmd_strings 字面量内联复现 11 处 PROD 命中系脚本假阳性

原文摘录
「cmd_strings 文本内联复现 11 处 PROD 命中全落在 config_commands.rs 的 cfg(test) mod
（脚本剥离失败的假阳性），唯一真命中 resp_server_session.rs:1334 与批二条一（PING 臂内联）
同位，不重报。」

理由
普查脚本只剥同文件 `#[cfg(test)] mod {…}`，跨文件门与同文件后段 cfg 区剥离失败，PROD 计数不可信；
唯一真命中与在册批二票同位（重报即双花）。登记以免下轮按同一脚本口径再出一批假阳性。

## 6. 撤销初判 5：1<<20 与 (1<<24)-1 的同值复现不构成双真源

原文摘录
「1<<20 十一处复现：与 C# 同形 divisor（GarnetInfoMetrics.cs:120/:122/:124/:126 逐条
GetTotalMemory(1 << 20)），同构不报；(1<<24)-1 三处分属不同位域（wrecord PAD_KEY_LEN /
wresp catalog ALL / wacl 测试回归钉），非同一真源，不报。」

理由
前者与 C# 逐条同形（转写方针即「对标 C#，不自行合并」）；后者是值相同、语义不同的三个位域掩码，
强行合一反而造出跨 crate 假耦合。不立项。

## 7. 撤销初判 6：wmetric 两份 DEFAULT_LATENCY_TYPES 与 HISTOGRAM_{LOWER,UPPER}_BOUND 与 C# 同构

原文摘录
「wmetric DEFAULT_LATENCY_TYPES 与 HISTOGRAM_{LOWER,UPPER}_BOUND 双文件同名同值：C# 本身亦双份
（GarnetLatencyMetrics.cs:18 与 GarnetLatencyMetricsSession.cs:16），LatencyMetricsEntry 与
EntrySession 同型，同构不报。」

复核（当下代码）
/Users/z/git/db/wedb/wedb/wmetric/src/latency/garnet_latency_metrics.rs:23 与
/Users/z/git/db/wedb/wedb/wmetric/src/latency/garnet_latency_metrics_session.rs:28 各一份，
且各有自身读者（前者 :315、后者 :174 与 :204），
HISTOGRAM_LOWER_BOUND 在 /Users/z/git/db/wedb/wedb/wmetric/src/latency/latency_metrics_entry_session.rs:25。
两态各自成面，与 C# 双份同型，不立项。

## 8. 撤销初判 7：whyperlog::is_valid_hyll 判为「校验口未接线」不成立

原文摘录
「whyperlog::is_valid_hyll 判为「校验口未接线」不成立：生产走同文件 :16 的
is_valid_hyll_len（含 magic 校验），消费点 …/hyper_log_log_commands.rs:54、:170；
且 C# 对应件是 HyperLogLog.cs:189 那个无生产消费者的重载（活的是 :198 版），不报。」

复核（当下代码）
生产校验点 /Users/z/git/db/wedb/wedb/wnode/src/resp/hyperloglog/hyper_log_log_commands.rs:54 走
`is_valid_hyll_len`；`is_valid_hyll`（/Users/z/git/db/wedb/wedb/whyperlog/src/frame.rs:10）
仅测试侧调用（whyperlog/src/lib.rs:267/:286/:590），与 C# 同名重载亦无生产消费者同构。不立项。

## 9. 撤销初判 9：wnode/src/aof/test_support.rs 判为「测试件常驻生产」不成立

原文摘录
「…/tmp/rev10/wedb/wnode/src/aof/mod.rs:15-16 即为 `#[cfg(test)] mod test_support;`，
该文件根本不进生产产物；系脚本只剥同文件 cfg(test) mod 的盲区所致，已从条 5 剔除。」

复核（当下代码）
/Users/z/git/db/wedb/wedb/wnode/src/aof/mod.rs:15-16 逐字为 `#[cfg(test)]` + `mod test_support;`。
不立项；同型跨文件门是本仓死码普查的固定盲区，后续普查须按此口径复核。

## 10. 撤销初判 10：wnode/src/logging.rs:49 LogFormatter::format_time 与 C# 同构

原文摘录
「wnode/src/logging.rs:49 LogFormatter::format_time 不报：C# 同件 libs/common/Logging/LogFormatter.cs:27
的唯一消费者也在测试侧（test/standalone/Garnet.test/NUnitLoggerProvider.cs:94、:107），1:1 同构；
条 5 只保留 MemoryLogger 簇（其在 C# GarnetServer.cs:86-97/:232 有真装配点）。」

理由
测试侧唯一消费者与 C# 完全同形，删之即偏离对标。同段的 MemoryLogger 簇属真缺陷（C# 有装配点），
已由 task/ing/zero-consumer-dead-surfaces-batch-five.md 类三第 18 项承接。

## 11. 撤销初判 11：read_consistency_manager.rs:224 update_physical_sublog_max_sequence_number 与 C# 同构

原文摘录
「…C# 对位 ReadConsistencyManager.cs:188 的非测试消费者只有一处，且在未转译的基准里
（benchmark/BDN.benchmark/Cluster/ConsistentRead/ConsistentReadContext.cs:161），
与 rust「仅 cfg(test) 用例（:478/:479、garnet_append_only_file.rs:523/:524）」同构。」

复核（当下代码）
定义 /Users/z/git/db/wedb/wedb/wnode/src/aof/readconsistency/read_consistency_manager.rs:224；
读者全在测试面：/Users/z/git/db/wedb/wedb/wnode/tests/consistent_read_session.rs:49、
/Users/z/git/db/wedb/wedb/wnode/src/aof/readconsistency/replica_read_session_context.rs:474 与 :475、
read_consistency_manager.rs:478。本仓不转写 benchmark，该口即 C# 公开诊断面，不立项。

## 12. 条 12 合规门读数段：不立项（无待做），但读数本身经复核成立

原文摘录
「合规门本轮全绿（登记读数，防下轮误判为漏扫）… rg '#\[allow\(' / '#\[expect\(' 全仓命中 0；
rg '\b(todo!|unimplemented!)' 命中 0；rg 'std::sync::(Mutex|RwLock)' 命中 0…」

复核（当下代码，/Users/z/git/db/wedb/wedb 全域 *.rs）
四类计数实测均为 0（`#[allow(`、`#[expect(`、`todo!|unimplemented!`、`std::sync::(Mutex|RwLock)`），
读数成立且无待做，本台账登记即其归属：不立票、不改码。若后续轮次任一项非零，按新增回归处理。


判据补录（同台账第二路分拣代理，不改本档载体判定，只修判据与在册票的三处失真）

以下取证同为当下主仓，逐条给出该并回载体票的内容见各条文件:行。

一、条 9 载体 task/ing/tiered-collection-ops-file-split.md 的 C# 对位框架须改：其正文与标题沿原报
「按 C# ObjectStore 会话分界」「一个文件同时承载四类 C# ObjectStore 会话文件」，不实。C# 那四件的
转写落点在本仓按类型已分文件的命令面，证据是各文件自身锚注：
/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/set_commands.rs:6 标
`libs/server/Storage/Session/ObjectStore/SetOps.cs`、hash_commands.rs:713 标 `HashOps.cs:HashPersist`、
sorted_set_geo_commands.rs:646 标 `ObjectStore/SortedSetOps.cs`；而 tiered_collection_ops.rs 承接的是
本仓自有的 wbftree 分层引擎（transpile 需求的自适应分层面，C# 无对位件），其内部锚点在存储函数层
（本文件 :325 标 `libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:79 PostInitialUpdater`）。
拆分依据应改述为本仓「一类一文件」既有惯例 + 该文件内部实测六段分界 + 体量对标（C# 同域最大件
SortedSetOps.cs 1798 行 vs rust 并成 2167 行单件），件制建议按内部分界取八件
（mod + common + member_ttl + hash + set + zset + list + scan），比原报五件更贴实测：
TieredCtx :51、树底座 :75-:180、成员 TTL :181-:350、finish_tiered_arm :350/drain_or_save :369、
hash :392-:907、set :908-:1159、zset :1160-:1614、list :1615-:1893、跨型物化 :1894-:2167。
拆分后 mod.rs 必留的再导出集由十处外部引用决定：list_commands/slow.rs:24、
sorted_set_commands/slow.rs:36、set_commands.rs:883、hash_commands.rs:784、object_store_utils.rs:46
与 :49-:51、tiered_demote.rs:47、shared_object_commands.rs:404、garnet_api/objects.rs:17、
garnet_api/slow.rs:40（另 sorted_set_geo_commands.rs:687 经 object_store_utils 转引）。

二、条 7 载体 task/ing/wreviv-test-record-alignment-single-source.md 的「风险性质」段须纠偏：
「wrecord 改对齐（8→16）后 wreviv 用例仍按 8 断言而全绿」不成立——唯一消费点
/Users/z/git/db/wedb/wedb/wreviv/tests/reviv/free_bin_allocation.rs:31
`let bin_size = TAKE_RECORD_SIZE + RECORD_ALIGNMENT;` 只把该值当分桶尺寸增量参数，用例不含对齐断言
（wreviv/src 全域 grep align 零命中，该池不持对齐概念），故生产对齐值变动不会让任何 wreviv 断言失真。
本项真正治的是测试面第二真源（tests/reviv/support.rs:9 复抄 wrecord/src/header.rs:57）与
support.rs:8 把 C# 生产常量值写进注释的锚点失真；该票「临时改 16 看编译联动」的验收项只证编译面，
不得当作漏检封堵的证据。若判定不该让 wreviv 测试面耦合 wrecord，取另一条收口即可：剪掉 :8 对位锚注、
承认夹具自取增量值（勿两法各做一半）。

三、条 11 载体 task/ing/json-commands-file-split-anchor-decl.md 判据补充：四入口的 C# 锚注不是失真——
/Users/z/git/db/wedb/wedb/wext_json/src/json_commands.rs:365/:370/:375/:380 标的
NeedInitialUpdate/Updater/Reader/AbortWithErrorMessage 在
/Users/z/git/db/wedb/garnet/modules/GarnetJSON/JsonCommands.cs 确有同名（:33、:44、:107、:77/:176/:186/:201），
故无虚构锚点风险；19 条扩展命令体本身不带任何 `.cs:符号` 锚，已合规，第一步走文件头与
match_command :233 名字面量表首的显式声明即可，勿往 js/check/ignore 塞「rust 有而 C# 无」方向的条目
（ignore 台账只治反方向，塞了也无效）。crate 外只见 lib.rs:10-:12 五个名
（COMMAND_INFOS、JsonCommand、JsonCommandInfo、JsonCommands、is_command_registered），
拆分后外部引用应零改动；impl JsonCommands 实测到 :1590 止，其后自由函数 encode_val_resp :1592-:1616
属 RESP 编码面，搬位时勿落在 dispatch 件。

四、条 2 载体（replica-sync-delay）需按当下代码更正一处：其「全仓仅一处消费点、改一处即闭环」不实。
REPLICA_SYNC_DELAY 常量现有两处消费者——副本重放侧
/Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_replay_task.rs:141 与 :200 的 sleep，
以及主端推流泵 /Users/z/git/db/wedb/wedb/wedb/src/server/replication/assembly.rs:79
`pump.start_throttle_loop(replica_replay_task::REPLICA_SYNC_DELAY)`（泵侧实现
aof_replication_pump.rs:85 start_throttle_loop，其 :18-:19 注释自称与 C# BulkConsumeAllAsync 同源）。
接线须同批覆盖两处（泵侧亦改读该运行期槽），否则「配置进、行为不出」只解一半。

五、条 6 的「修法」方向补一条硬证据，防下轮再提：原报要求给 checkpoint_base_directory 补行为读取点
不成立——C# 检查点父目录的真源是构造期选项
（/Users/z/git/db/wedb/garnet/libs/server/Servers/GarnetServerOptions.cs:630
`StoreCheckpointBaseDirectory => Path.Combine(CheckpointBaseDirectory, "Store")`，:693 按 dbId 拼目录名），
运行期表那两列在 C# 亦只是只读投影（libs/server/Config/RuntimeServerConfig.cs:134 unixsocket）；
rust 真源链已在位（/Users/z/git/db/wedb/wedb/wnode/src/service.rs:726 `checkpoint_dir_of(data_path)`）。
照原报补运行期读取点即给同一事实造第二真源，与 runtime-options-read-only-path-fields-unprojected
那张「投影断供」票的正当修法方向相反。

六、批五与批四的现仓落地进度（供派单剪项）：批五类一第 1 项（wlua 两份 script_digest 便捷包装）与
类一第 2 项（BLOCK_HEADER_SIZE 常量与 lib.rs 再导出）已消——全仓 `fn script_digest` 零命中，摘要单点
/Users/z/git/db/wedb/wedb/wlua/src/cache.rs:290 与四个生产消费点（commands.rs:229、:337，
functions/redis.rs:111、:120）在位；`BLOCK_HEADER_SIZE` 全域零命中（wlua/src/managed_allocator.rs:13
的 BlockHeader 是私有结构、非导出常量）。该票其余符号仍活：wbase/src/striped.rs:449/:456/:463、
wnode/src/aof/replaycoordinator/aof_replay_context.rs:35/:43/:80、wconn/src/parser.rs:47/:82/:126/:232、
whyperlog/src/regs.rs:64/:74/:108、whyperlog/src/lib.rs:143 reg_cnt、wlua/src/state.rs 五口与
timeout.rs:164。批四两口（wedb/src/server/replication/aof_sync_driver.rs:609 assert_does_not_exist、
wnode/src/resp/resp_server_session_output.rs:27 writer_p）现仓各仅剩定义行一处命中，未落地；
另注：本档第 9 行指向的 task/ing/zero-consumer-dead-surfaces-batch-four.md 在当前队列已不存在
（应是被认领或归档，按文件名而非路径索引）。

