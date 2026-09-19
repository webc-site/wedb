零生产消费死面第三批：第二套键提取、ArgSliceVector、集群会话别名与 AOF 装配死变体

严重度：MED。取证基线：主仓 dev HEAD，四组符号逐条实读，行号为当下实况。

本单射程与互斥

批一已归档 task/done/zero-consumer-pub-surface-census.md；批二同域前票（slug
zero-consumer-surfaces-batch-two）在册，其射程符号本单一律不碰：
initialize_if、iterate_store、try_set_user、get_info_metrics、expiration_option_from_token、
try_export_resp_commands_data、set_spawner、begin_flush/FlushGuard、basic_commands 的 network_*
分派臂双轨。禁再造第二份普查清单，本单只收上述票未覆盖的四组。

一、wresp 第二套键提取零消费（重复/多套架构，本单最高）

现状：wedb/wresp/src/key_spec.rs:303 try_get_start_index、:338 extract_keys，全仓消费点仅同文件
测试 :544-606；生产键提取另在他处（命令表层）。模块头 :1-5 自称对标 C# 键规格类层次。
C# 参考：garnet/libs/server/Resp/RespCommandKeySpecification.cs:332（abstract ExtractKeys）、
:385/:465/:522 三实现；该类在 C# 的活消费面是命令信息导出
（garnet/libs/server/Resp/RespCommandsInfo.cs:89 KeySpecifications、
garnet/libs/server/Resp/RespCommandInfoSimplifiedStructs.cs:236 TryGetSimpleKeySpec、
garnet/playground/CommandInfoUpdater/RespCommandInfoParser.cs:146 TryReadFromResp），
ExtractKeys 本身在 C# 亦无生产调用点。
处置：键规格结构体保留给导出面（有消费），两提取函数按死码删除并在 js/check/ignore 对应
RespCommandKeySpecification.cs 块登记「C# 同面无生产消费者，rust 不预留」；若日后键门需按规格取键，
届时一次性接线，不留两套无人调的提取实现。

二、ArgSliceVector 整结构零消费

现状：wedb/wresp/src/argslice/arg_slice_vector.rs:12（导出于 :7 pub use），消费仅同文件测试 :67/:78；
同结构无 pinned scratch 复用者。
C# 参考：garnet/libs/server/ArgSlice/ArgSliceVector.cs:17，唯一生产消费点
garnet/libs/cluster/Server/Migration/Sketch.cs:17（readonly ArgSliceVector argSliceVector）。
rust 对位 wedb/wedb/src/server/migration/sketch.rs:13 已在 :8-12 显式声明本仓 Sketch
「不承载 C# argSliceVector / Keys 的收集去重职责」，传输清单由调用方结构承担——即接线需求不存在。
处置：删 arg_slice_vector.rs 与 mod.rs:7 导出，js/check/ignore 登记该 C# 件不转写理由；
禁以「对标完整性」为由留占位实现。

三、with_cluster_session 零调用别名构造器加文档谎指

现状：定义 wedb/wnode/src/resp/resp_session_consumer.rs:74，全仓无生产调用点（集群会话消费者
实际构造走他口）；两处文档仍宣称集群经它装配：wedb/wnode/src/service.rs:844（StorageSessionProvider
类型文档）与 wedb/wedb/src/main.rs:5（基座说明「集群差异仅为 decorate 钩子构造 with_cluster_session」）。
C# 参考：garnet/libs/server/Resp/RespServerSession.cs 的集群切面是构造期注入
（garnet/libs/cluster/Session/ClusterSession.cs 由 RespServerSession 直接派生承载），无「别名构造器」一职，
本件属 rust 自生形态。
处置：二选一——删别名构造器并订正两处文档，或让集群装配真走它使文档为真；文档与实现不得并存两套口径。

四、StorageSessionProvider AOF 死变体加分派文档谎指

现状：定义 wedb/wnode/src/service.rs:1065 open_with_aof、:1168 open_recovered_with_aof；
生产四路分派实走带配置变体（:1276-1289 调 open_recovered_with_config_and_aof /
open_recovered_with_config / open_with_config_and_aof / open_with_config），两死变体调用点仅
wedb/wnode/tests/service_aof.rs:6/:35；文档三处仍以它们为分派落点：
:1254-1256（四路分派说明）、:914-919（字段文档自称由它们点亮）、
wedb/wedb/src/server/boot.rs:121（注释称 boot 经 open_with_aof 构造后注入 set_aof，实际
boot.rs:66 走 open_from_args）。
C# 参考：garnet/libs/server/Storage/Session/StorageSessionProvider.cs 单一构造口，
档位差异由 StoreWrapper serverOptions 承载，无「同族四变体」；本组变体属 rust 自生装配口，
删除不损对位。
处置：删两变体并把三处文档口径改为实走路径，或让 open_from_args_with_config 真按四路分派调它们
（须与 task/ing/boot-assembly-projection-single-source.md 启动装配单点化同域并单，勿两边各改一半）。

优先级：死代码 与 重复/多套架构（第一、二项）> 污染扩散（第三、四项的文档谎指会把后续会话引向不存在的路径）。

验收

上述四组符号 grep 归零或转为「配置进→行为出」的活链；service.rs / main.rs / boot.rs 三处文档
与实际分派一致；js/check/ignore 登记逐条附理由；bun js/check.js 无新增缺失或虚构锚点
（与 task/ing/gate-anchor-drift-reclean.md 复绿判定同一读数收口）；仅涉测试改道，不跑 test.sh/clippy（主代理集中回归）。

落地记录（分支 zero-consumer-b3，合入 dev 66418fd）

一、二组按死码删除：wresp/src/key_spec.rs 的 try_get_start_index / extract_keys 与同文件
两条用例整体移除，键规格结构体与 RESP 导出面保留（活消费在 catalog/simplified.rs 的
try_get_simple_key_spec 折叠口与 commands_info.rs 导入面，生产键提取唯一口径为
wnode/src/key_spec.rs 的 extract_keys_from_slice 族）；wresp/src/argslice/arg_slice_vector.rs
整文件与 mod.rs 的 pub mod / pub use（含 DEFAULT_MAX_ITEM_NUM）删除，argslice 域只留 ArgSlice。
两条锚点按「C# 同面无生产消费者 / 接线需求不存在」登记 js/check/ignore/server.yml
（ArgSliceVector.cs 由原仅 GetEnumerator 的方法级忽略升级为整文件忽略）。

三组取「删别名 + 订正文档」方案：with_cluster_session 删除（全仓零调用，含测试），
wnode/src/service.rs 的 StorageSessionProvider 类型文档与 wedb/src/main.rs 基座说明
改指实走的 RespSessionConsumer::with_cluster（boot.rs:54 与 wnode_test 单点）。

四组取「删两变体 + 统一文档口径」方案：open_with_aof / open_recovered_with_aof 删除，
完整装配文档并入对应的带配置口（生产四路分派 open_from_args_with_config 的四臂即
open_with_config / open_with_config_and_aof / open_recovered_with_config /
open_recovered_with_config_and_aof，配置由 store_config_from_node 单点推导，薄壳无职责）；
service.rs 的 aof / wal / recovered_aof_tail 字段文档与四路分派说明、boot.rs 的 set_aof
注入注释据实改写，另订正 config_owner.rs、replication_manager.rs 两处、recover_test.rs
与 cluster_resp_session.rs 的残留口名，service_aof.rs 两用例更名为
open_with_config_and_aof_*（原已实调该口，用例名与实现不符）。

未纳入本单：StorageSessionProvider::open 与 open_recovered 亦为零调用薄壳
（全仓仅文档引用，无代码消费点），但与启动装配单点化同域（其存在是给「无配置」
口径留位），删除须与装配口径一并收口，留待该票处理，避免两边各改一半。

门禁：CARGO_TARGET_DIR=/tmp/rs-zcb3 cargo check --workspace --all-targets 通过（合入后
主仓复跑同命令通过）；bun js/check.js 读数与基线一致（exit 0，无新增缺失/虚构锚点，
仅行号漂移）；未跑 test.sh 与 clippy.sh。
