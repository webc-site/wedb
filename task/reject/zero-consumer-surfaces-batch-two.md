第二批零生产消费死面与分派臂双轨普查（甄别订正）

来源：task/ing/zero-consumer-surfaces-batch-two.md（承接 qcode 第 7/8/9 轮台账）。
本档只登记该单中被 HEAD 事实与 garnet C# 源码否掉的论点，成立项的落地记录见该单本体。
取证基线：主仓 dev c549409，逐条 grep 全仓（含测试、宏、feature 面）与 ./garnet C# 对位。

订正一（第二节前提失实，但删除结论仍采纳）

原文：「wedb/wnode/src/aof/garnet_log/commit.rs:92 initialize_if 生产零调用（仅
wnode/tests/garnet_log.rs:253），同文件 :66 initialize 有生产者
（replication/replication_manager.rs:1044）」。

复核：:1044 实为 `store.initialize(disk_entry)`，store 是
`let mut store = self.checkpoint_store.write()`（同函数 :1026），即
CheckpointStore::initialize，与 GarnetLog::initialize 无关。全仓 `.initialize(` 与
`.safe_initialize(` 命中逐条归属后：GarnetLog::initialize（commit.rs:64）与
GarnetLog::safe_initialize（commit.rs:41）同样零生产调用（消费面只剩
wnode/tests）。原文的「initialize 有生产者」不成立。

处置：该项射程只含 initialize_if，按射程删除并登记 ignore（GarnetLog.yml）；
门面另两方法（initialize/safe_initialize）不属本单射程，留待下批普查，本单不动、
不顺手扩大删除面（死树 zero-consumer-b2 的「三面同删」草稿因此弃用，见订正六）。

订正二（第三节修法之一不成立，采删除侧）

原文：「修法：rust 集群 MIGRATEG 键枚举与 CLUSTER 侧路径改走该出口（消除第三套
手写扫描），或按实际删除并在 js/check/ignore 登记理由」。

复核：C# IterateStore 是 IterateLookup 的回调式全库扫描原语，生产消费点三处
（garnet/libs/cluster/Session/ClusterCommands.cs:22、:31 与
garnet/libs/cluster/Server/Migration/MigrateOperation.cs:92）。rust 侧这三处各有
活的窄口径出口：CLUSTER COUNTKEYSINSLOT / GETKEYSINSLOT / DELKEYSINSLOT 走
wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs 的
count_keys_in_slot（:284）、get_keys_in_slot（:325）、delete_slot_keys（:256），
迁移与无盘快照走 wedb/wedb/src/server/migration/migrate_driver/slots.rs:148 与
wedb/wedb/src/server/replication/diskless_replication/replication_snapshot_iterator.rs:142
的 get_keys_in_slot。本仓库级定槽（doc/zh/db.md 4.1：Slot = Mixer(namespace,
active_db)，键内容不参与定槽）下，把这三处改走通用全库回调口只会把已收敛的
谓词重新摊平，所谓「第三套手写扫描」恰恰就是被删的 iterate_store 自己
（它经 string_snapshot → collect_records 另起一条 hlog 全库物化路径）。

处置：采删除侧。删 iterate_store 及其私有取数岛 collect_records、string_snapshot
（二者唯一消费者即 iterate_store，键名快照走 string_keys_snapshot 独立在产），
wnode/tests/consistent_read_session.rs 的 iterate_store 段一并撤（db_scan /
db_keys / scan_cursor 三段一致读覆盖保留），并在
js/check/ignore/garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.yml
登记 IterateStore。

订正三（第四节修法不成立，采删除＋ignore）

原文：「现役替换路径为无条件挂载：wedb/wnode/src/resp/acl_commands.rs:699
set_user_handle(handle)……修法：acl_commands.rs:699 按 C# 形态改重试环（并发 ACL
换用户不丢更新），删死口不成——环是真语义」。

复核两句：

1. 现役路径已不是「无条件挂载」。HEAD 的 acl_commands.rs 在 ACL 分派前做 SETUSER
   自改目标预判（:684-696），命令后重读存储刷新连接本地句柄，注释自标「对标 C#
   共享 UserHandle 的 CAS 换新即时生效语义」。原文行号与描述都是旧态。
2. CAS 环在本仓无并发对手。C# 重试环的前提是句柄存活于全局用户字典、多连接共享
   同一 UserHandle（garnet/libs/server/Resp/ACLCommands.cs:226
   `while (!userHandle.TrySetUser(newUser, currentUser))` 与
   garnet/libs/server/ACL/UserHandle.cs:48）。本仓按 doc/zh/db.md「ACL 数据库持久化
   与零全局内存」：全局无用户字典、按需点查 KeyTag::Acl、句柄连接本地持有并随连接
   析构释放；认证器每会话新建（wedb/wnode/src/service.rs:1447-1451
   session_dependencies 每次调用 `Arc::new(Mutex::new(GarnetAclAuthenticator::new(..)))`），
   wedb/wacl/src/user_handle.rs 的 Arc<UserHandle> 无跨连接共享路径。给连接本地句柄
   套 CAS 重试环属凭空加复杂度，且与在飞票
   task/ing/acl-setuser-live-connection-propagation.md（跨连接即时生效域）撞面。

处置：删 try_set_user 与其同文件测试，登记
js/check/ignore/garnet/libs/server/ACL/UserHandle.yml；跨连接传播的缺口由该在飞票
承接，本单不重复派工。

订正四（第六节第一条已在 dev 落地）

原文：「wedb/wresp/src/options.rs:228 expiration_option_from_token：仅同文件测试
:365/:369 消费，生产走既有 token→选项单点，删别名并把测试改走生产出口」。

复核：全仓 `expiration_option_from_token` 零命中，`git log --all -S` 显示该别名已
由前序波次删除并归档（task/reject/qcode10-net-set-expiry-single-point-done.md 第 3
条「死别名已删」）。HEAD 的 options.rs:193 唯一出口 try_get_expiration_option 在产
（set.rs:672、basic_etag_commands.rs:369/:502、session_parse_state_extensions.rs:302
消费）。本单该条无动作，禁再造第二次。

订正五（第七节 start_async 判定不成立）

原文：「:259 get_collection_item_async、:567 start_async 生产零调用（仅
wcol/tests/collection_item_broker_tests.rs:223）」。

复核：start_async 不是死面。它是主循环本体，唯一调用者是同文件 start_main_loop
（:306 起）里 spawner.spawn 闭包内的 `broker.start_async().await`，对标 C#
garnet/libs/server/Objects/ItemBroker/CollectionItemBroker.cs:123
`mainLoopTask = Task.Run(StartAsync)`。grep 只数 `start_async(` 的显式站点会把它
误判为孤儿，实际删除即抹掉整个经纪主循环。

处置：start_async 保留。删除面收敛为 set_spawner（构造后二次注入口，零调用）与
get_collection_item_async（异步门面，生产走 start_wait + wait_result + finish_wait，
见 wcol/src/itembroker/item_broker_face.rs:47/:59 门面只暴露该对）及其私有
get_collection_item_async_inner；C# 锚 GetCollectionItemAsync 移到 start_wait 文档
行（承接者指真，不复用 ignore）。

订正六（一棒遗留 diff 的弃用项）

1. /tmp/fork/zero-consumer-surfaces-batch-two 在 resp_server_session.rs 新增
   `abort_wrong_num_args_to(&mut self, output, cmd_name)` 助手。与既有
   wedb/wnode/src/resp/objects/object_store_utils.rs:186
   `abort_with_wrong_number_of_arguments(&mut self, cmd_name, output)` 同一能力
   （写出参数错误帧＋置 command_error_written），属第二套机制，弃用；本单三处
   错误路径统一改调既有单点（同 shared_object_commands.rs:50、:157 口径）。
2. 同树的 wcol 改动只把 spawner 字段从 Mutex<Option<Arc<Spawner>>> 改成裸字段，
   start_main_loop 仍 `self.spawner.lock()`，编译不过（半成品），弃用其形态，本单
   重做为「字段＋构造器＋start_main_loop＋文档链接」四处一致。
3. /tmp/fork/zero-consumer-b2 把 GarnetLog 的 safe_initialize / initialize /
   initialize_if 三面一起删，并在模块头写「位点初始化三面已删」——越出本单射程
   （本单射程只 initialize_if），且其 ignore 理由声称的承接点未经归属核验，弃用。
4. 同树三份 ignore 草稿的事实性错误已改写：try_export 理由原文「C# 该口自身
   亦无生产调用点（garnet 内 grep 零命中）」不实，真调用点在
   garnet/playground/CommandInfoUpdater/CommonUtils.cs:55（该目录整类已 ignore），
   现按此登记；get_info_metrics 理由补上本仓既有
   js/check/ignore/libs/server/Servers/MetricsApi.cs.yml 已把宿主门面整体登记
   不转写这一硬证据。
