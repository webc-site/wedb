优先级：中

2 [MED] CLUSTER MIGRATE 头的 vectorSets 槽位是双向死面：发送端写死 F、接收端仅布尔校验后丢弃
具体问题：C# SetClusterMigrateHeader(sourceNodeId, replace, isVectorSets) 的 6 元素头里
第 5 参承载 isVectorSets，MigrateSession 侧据此选向量集导入路径。rust 的头多带一个
slot-list 成 7 元素（库级定槽既定偏离，已声明），但 vectorSets 位在发送端
wedb/src/client.rs:312 写成常量 `let vector_str: &[u8] = b"F";`，函数签名
execute_cluster_migrate_async 根本不接该参数；而向量集帧与键值帧共用同一
send_payload_and_wait → execute_cluster_migrate_async 出口（keys.rs:182、
migrate_session_vector_set.rs:90、migrate_driver/keys.rs:756），所以向量集迁移真实
发生时线上仍写 F。接收端 migrate.rs:215 只把该位当布尔语法门校验，校验通过后丢弃、
不进 cluster_migrate_slow。后果：6→7 元素头里有一位永久无信息量，且其错误文案
ERR_VECTOR_SET_FLAG 永不触发（发送端只会发 F），C# 靠该位区分的两条导入路径在 rust
退化成帧内 kind=5/6 自描述——这一收敛本身成立，但线上参数位与该收敛没有对齐：
要么把该位真接上（签名加 is_vector_sets，与 C# 三面齐备），要么删该位并同步删
接收端校验与文案。现状是「帧格式声明了、发送端写死了、接收端读而弃」。
rust：wedb/wedb/src/client.rs:301-329 execute_cluster_migrate_async（:312 写死 b"F"、
:296-300 doc 仍把 vectorSets T/F 列为帧参数）；wedb/wedb/src/server/migration/
migrate_driver/keys.rs:163-198 send_payload_and_wait（无 vector_sets 入参）；
接收端 wedb/wedb/src/server/cluster_session/migrate.rs:196-218 network_cluster_migrate
（:213-218 注释自述「仅作布尔校验」）
c#：garnet/libs/client/ClientSession/GarnetClientSessionMigrationExtensions.cs:171-178
SetClusterMigrateHeader（isVectorSets → vectorSetOption T/F，arraySize=6）、:250
调用点显式传 isVectorSets: false；garnet/libs/cluster/Session/
RespClusterMigrateCommands.cs:NetworkClusterMigrate（解析该位并下传导入面）
修法：删该线参数（客户端签名、接收端 args 形状、ERR_VECTOR_SET_FLAG 文案一并删，
头改 6 元素），并在 migrate.rs 的帧格式注释里把「kind=5/6 自描述」写为唯一判据；
或反向补 is_vector_sets 形参并由 transmit_vector_set_frames 置真。禁止现状
「发送写死 + 接收丢弃」。

收口（dev-vecflag 棒复核，代码零改动）：判读为「死」不判「缺」，票面首选修法（删线参数
一支）已随 fix-vectorsets-dead-flag 的 d4462534 合入 dev（合并提交 656759ec，早于本票
16:10 的分拣提交 ea7340bd 22 分钟），即立票时死面已在 dev 消失，故不再重派落地，票转 done。

在 dev HEAD 4d02c044 重取三面，逐项自证旗标已无：
1. 发送端 wedb/wedb/src/client.rs:351-377 execute_cluster_migrate_async(source_node_id,
   replace, slot_list, payload)，:361 只剩 replace_str 的 T/F，票面 :312 的
   `let vector_str: &[u8] = b"F"` 已无；线帧恰 6 元素（CLUSTER、MIGRATE 加 4 参），
   与票面「头改 6 元素」目标形态一致。
2. 单一出口 wedb/wedb/src/server/migration/migrate_driver/keys.rs:165-200
   send_payload_and_wait 五参、无 vector_sets 入参，六个调用点（同文件 :379、:537、:574、
   :774，migrate_session_range_index.rs:42，migrate_session_vector_set.rs:95）同形；
   MigrateTaskSpec（migration/migrate_session.rs:23-36）无残留布尔位。
3. 接收端 wedb/wedb/src/server/cluster_session/migrate.rs:203 恰 4 参 destructuring，多一参即
   wrong number of arguments；票面 :215 的布尔校验与 ERR_VECTOR_SET_FLAG 文案全删，
   全仓 grep 该符号零命中。
4. 帧格式注释 migrate.rs:189-196 与 client.rs:343-350 已把「向量集帧自描述 kind=5/6
   是唯一判据」写为唯一判据，即票面修法的文档侧同步要求。

C# 对位不缺：garnet RespClusterMigrateCommands.cs:74-151 的 vectorSetOption 分支只做
「向量集帧走专用导入段」这一件事，rust 由 frame_import.rs:125-156 按
MigrationFrame::VectorSetIndex / VectorSetElement 单点分派承接（wconn/src/record.rs:300、
:307 解析面与 :564、:580 编码面均有真实生产消费者），语义无缺环，无须反向补旗标。

测试钉已在 dev：wedb/wedb/tests/cluster_migration.rs:916 cluster_migrate_args_shape_convergence
断 4 参形通过、0/2/3/5 参形报参数计数错、C# 旧 4 参形（第 3 位 vectorSets）被头级槽门
整批拒绝；:2610 的接收端帧构造 helper 注明旗标本仓废除。

git grep vectorSets HEAD 剩余命中性质：产线仅 2 处文档注释（client.rs:348 的 isVectorSets、
migrate.rs:193 的 vectorSets，均为「本仓废除」的自述），其余全在 tests/cluster_migration.rs
的测试注释与断言文案；`is_vector_sets` / `vector_str` / `ERR_VECTOR_SET_FLAG` 在 wedb/
各 crate 的 src 与 tests 内零命中，只余本票与 next 台账的叙述文本。

门禁：本票无 rust delta（工作树与 dev HEAD 同树），cargo check 与 bun js/check.js
未跑，未触碰 js/check/ignore 登记。
