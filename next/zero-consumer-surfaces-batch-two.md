第二批零生产消费死面与分派臂双轨普查收口

来源：qcode 第 7/8/9 轮台账在册、尚未立单的死面项（逐条按主仓 dev HEAD grep 复核，行号为当下实况）。
承接已合并的 zero-consumer-pub-surface 批次与 task/ing/zero-consumer-dead-symbols-cleanup.md，
本单只收其未覆盖的余量，禁造第二份普查清单。

一、wnode 命令方法与分派臂双轨（重复 + 死代码，本单最高）
- 死方法：wedb/wnode/src/resp/basic_commands/mod.rs:91 network_ping、:110 network_asking、:397 network_echo，
  全仓生产零调用（仅 wnode/tests/resp_tests.rs:847-896、tests/resp_admin.rs:24-78）。
- 双轨实现在：wedb/wnode/src/resp/resp_server_session.rs:1325-1336 PING 臂内联 +PONG/bulk 两形态、
  :1337 起 ASKING 臂内联。
- C# 对位是「分派臂只转调方法」：garnet/libs/server/Resp/RespServerSession.cs:855（PING→NetworkPING/
  NetworkArrayPING）、:856（ASKING→NetworkASKING）、:1089（ECHO→NetworkECHO），方法体在
  garnet/libs/server/Resp/BasicCommands.cs:989、:1007、:1425。
  修法：臂改转调，方法体承接 C# 语义，删内联副本。

二、AOF 恢复链第一步零接线
- wedb/wnode/src/aof/garnet_log/commit.rs:92 initialize_if 生产零调用（仅 wnode/tests/garnet_log.rs:253），
  同文件 :66 initialize 有生产者（replication/replication_manager.rs:1044）。
- C# 调用点：garnet/libs/server/Replication/ReplicationManager.cs:548
  `storeWrapper.appendOnlyFile.Log.InitializeIf(ref recoveredSafeAofAddress)`。
  修法：在 rust 对位恢复装配处（wedb/wedb/src/server/replication/ 恢复段）按 C# 同位调用，
  或证实本仓恢复链已由 initialize 单点覆盖后删除该方法并登记 ignore。

三、键空间遍历死转写
- wedb/wnode/src/storage/session/common/array_key_iteration_functions.rs:251 iterate_store 生产零调用
  （仅 wnode/tests/consistent_read_session.rs:113）。
- C# 侧该 API 是活口：garnet/libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:124，
  消费点 garnet/libs/cluster/Session/ClusterCommands.cs:22、:31 与
  garnet/libs/cluster/Server/Migration/MigrateOperation.cs:92。
  修法：rust 集群 MIGRATEG 键枚举与 CLUSTER 侧路径改走该出口（消除第三套手写扫描），或按实际删除并在
  js/check/ignore 登记理由。

四、wacl 死 CAS 口
- wedb/wacl/src/user_handle.rs:39 try_set_user 生产零调用（仅同文件测试 :50）。
- 现役替换路径为无条件挂载：wedb/wnode/src/resp/acl_commands.rs:699 set_user_handle(handle)。
- C# 是 CAS 重试环：garnet/libs/server/ACL/UserHandle.cs:48 TrySetUser 与
  garnet/libs/server/Resp/ACLCommands.cs:226 `while (!userHandle.TrySetUser(newUser, currentUser))`。
  修法：acl_commands.rs:699 按 C# 形态改重试环（并发 ACL 换用户不丢更新），删死口不成——环是真语义。

五、wmetric 死出口
- wedb/wmetric/src/info/garnet_info_metrics.rs:1224 get_info_metrics 全仓零调用点。
- C# 对位 garnet/libs/server/Metrics/Info/GarnetInfoMetrics.cs:629（INFO 段批量产出）。
  修法：INFO 慢路径若已逐段走 get_metric_internal 则删该方法；若 C# 批量语义有对外价值则接线，二选一不留双。

六、wresp 死别名与零消费导出口
- wedb/wresp/src/options.rs:228 expiration_option_from_token：仅同文件测试 :365/:369 消费，
  生产走既有 token→选项单点，删别名并把测试改走生产出口。
- wedb/wresp/src/catalog/data_provider.rs:65 try_export_resp_commands_data：仅测试 :127 消费。
  C# 对位 garnet/libs/server/Resp/RespCommandDataProvider.cs:173（导出到 path + IStreamProvider），
  rust 无落盘导出需求时删除并在 js/check/ignore 登记（形状差异，非缺实现）。

七、wcol itembroker 死注入与异步入口死形态
- wedb/wcol/src/itembroker/collection_item_broker.rs:242 set_spawner 生产零调用（仅 :335 文档自指）；
  :259 get_collection_item_async、:567 start_async 生产零调用（仅 wcol/tests/collection_item_broker_tests.rs:223）。
- C# 对位：阻塞族 notifyItemBroker 见 garnet/libs/server/Storage/Session/ObjectStore/ListOps.cs:67、:83。
  修法：装配期真注入 spawner（一次）并让阻塞族走异步入口，或按删除收口；
  与 task/ing/itembroker-shutdown-dispose.md（dispose/wait_done 接线）同域，开工顺序并单处理，勿两套启动形态。

八、向量清库屏蔽零接线
- wedb/wnode/src/resp/vector/vector_manager_context_metadata.rs:368 begin_flush 生产零调用，
  :462 FlushGuard 无持有者（同文件 :459 注释自称承接 C# 锁令牌语义）。
- 归 task/ing/vector-registry-nsdb-isolation.md 的 FLUSH 域回收联动一并落地（清库三入口缺向量上下文屏蔽），
  本单不重复派工，仅登记交叉引用。

优先级：死代码 > 重复/多套架构（第一项为分派臂双轨）。逐项独立可发，互不阻塞。

验收
- 上述符号 grep 归零或转为「配置进→行为出」的活链；js/check 无新增虚构锚点；
  相关 crate 测试改走生产出口，无因删除产生的 warning。
