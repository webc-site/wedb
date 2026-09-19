事务迭代槽位门同步自旋：compio 线程冻结与 node_timeout=0 永不超时

来源：第 9 轮 net 条 2（MED）。按主仓 dev HEAD 复核成立。

现状
- wedb/wedb/src/server/cluster_manager_slot_gate.rs:328 evaluate_iterative_key_gate 的 :340-367
  can_operate 组装段是同步 `loop`（:356）：`resolve_can_operate` 回 AccessPending/ExistsPending 时
  只在 :363 判超时、:365 `thread::yield_now()` 让出，整段阻塞在调用线程上。
- 该入口的事务/慢命令消费链为同步面：wedb/wtxn/src/txn_slot_verify.rs:28 trait
  `network_iterative_slot_verify`（同步签名，无 async），实现 wedb/wnode/src/cluster_session.rs:169、:386，
  转调点 :275；即事务逐键门评在 compio「一线程一 CPU」装配下自旋，同线程的迁移驱动推进方被饿死，
  两个互等键可成活锁。
- 超时上界可为无限：:355 `deadline = now_ms().saturating_add(node_timeout_ms(..))`，
  而 :38-42 node_timeout_ms 在 provider.cluster_node_timeout() 为 None 时回 u64::MAX
  （wedb/wedb/src/server/cluster_provider.rs:422-427：cluster_node_timeout_ms 为 0 → None；
  测试锁死该哨兵语义 wedb/wedb/tests/cluster_provider_epoch.rs:57-66）→ 0 值配置下 :356 环永不脱出。
- 对照：同文件异步臂已做对——:406 wait_key_gate 以 :431 `sleep(SLOT_VERIFY_POLL_MS)`（:95 =1ms）让出线程，
  单键/多键门评 :213-222 一律回 GateVerdict::Wait 不自旋。即本条是「同一门的两套形态中漏改的第三套」。

C# 参考
- garnet/libs/cluster/Session/SlotVerification/ClusterSlotVerify.cs:77 CanOperateOnKey 内
  :131 WaitForSlotToStabalize 的 `Thread.Yield` 自旋——C# 网络线程与迁移驱动不同线程亲和，让出即有效；
  rust 线程亲和模型下必须转异步挂起（本仓既有口径）。
- 门评调用面 garnet/libs/cluster/Session/SlotVerification/RespClusterIterativeSlotVerify.cs（迭代校验逐键）。

修法
- 一、把事务臂的终评让出改为复用 :406 wait_key_gate 的等待体（同一超时/同一 memo 口径），
  即网络迭代校验遇 Pending 一律转「挂起等待 + 裁决后重驱门评」，删 :365 同步自旋；
  禁新增第二套轮询常量（SLOT_VERIFY_POLL_MS 单点）。
- 二、超时口径与 C# 对齐：node_timeout_ms 的 None 兜底不得回 u64::MAX；无配置时取 C# 缺省 gossip/node-timeout
  实值（本仓缺省见 cluster_provider 装配段），0 值哨兵「无限」若为刻意决策，须在 :38-42 文档注释点名
  且不得覆盖 :356 这类无界自旋环（只允许用于异步等待臂的保守上界）。
- 三、用例：MIGRATING 槽上事务键门评与迁移驱动同线程并存时须推进不死锁；node-timeout=0 时门评按缺省
  超时有界终评（ASK/MOVED/CLUSTERDOWN 现口径不变）。

优先级：污染扩散（同门三套形态中唯一会冻结执行器的一套）。

关联：在途分支 cluster-suspend-await-lock 处理 suspend_config_merge 同步写锁跨 await 的锁形态问题
（另一处门，同属「同步让出转异步挂起」一族），与本条互不覆盖；开工前互查，勿重复改 :406 等待体。
