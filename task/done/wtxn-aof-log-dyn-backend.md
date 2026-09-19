wtxn 事务 AOF 后端由自造泛型 Option<L> 收敛为单一 Option<Arc<dyn TxnAofLog>>（对标 C# 非泛型单一可空 appendOnlyFile 句柄）

本档为 fixloop 认领档。原 next 条目曾被分拣代理删除（8d2aa17b），又被 qcode-my-r4 清账提交带回 next（9d73a6a1），本次以 git mv 重新认领回 task/ing，认领即覆盖，next/ 下不留手。

结论一句话：TransactionManager 自造的 L: TxnAofLog 泛型加 impl TxnAofLog for Arc<T>（blanket 转发）与 impl TxnAofLog for ()（空对象）是 C# 没有的优化层，与仓内既有「删自造泛型/擦除、改单一 dyn 后端」方向相反；收敛为 aof_log: Option<Arc<dyn TxnAofLog>> 后删两处 impl、全链去 <L>，与 C# 事务管理器持单一可空具体日志句柄一一对应。

优先级：高（本条是 task/ing/txn-aof-marker-session-wiring.md 的硬前置；后端不收敛，会话只能承载 TransactionManager<()> 空后端，无法注入真实 GarnetLog）。属「多套/自造抽象」类。

判定基线：整合树分支 fix-wtxn-aof-log-dyn-backend，起点为合入时的最新 dev；在途实现来自死亡代理分支 wave6-a-wtxn-aof-log（单提交 6e18c93e，base ed31ad39，落后 dev 约 244 提交，门禁从未跑过）。判落地只认当前 grep。

一、落地范围
1. wtxn 后端收敛：删 transaction_manager.rs 的 impl<T: TxnAofLog + ?Sized> TxnAofLog for Arc<T> 与 impl TxnAofLog for ()；字段 aof_log: Option<L> 改为 Option<Arc<dyn TxnAofLog>>；TransactionManager<L: TxnAofLog = ()> 去参数为 TransactionManager；new 第三参、Drop impl、enqueue_txn_marker 形参随之收敛。
2. 同域去 <L>：TxnProcedure<L> 改 TxnProcedure（prepare/main/finalize 收 &mut TransactionManager）；TransactionGuard<'a, L> 改 TransactionGuard<'a>；log_proc 与 run_transaction_proc 的 impl TxnProcedure<L> 改 impl TxnProcedure；txn_proc.rs 的 TxnProcResolver<S, L> 改 TxnProcResolver<S>；txn_key_manager.rs 的 impl<L> TransactionManager<L> 改 impl TransactionManager。
3. dev 新增段一并收敛：dev 在此期间给 transaction_manager.rs 加了 TxnProcApi、TxnProcReadApi<L>、TxnWatchApi（txn-proc-storage-api-injection 条），其中 TxnProcReadApi<L> 与 TxnWatchApi 的 impl<L> 是同一自造泛型的新增散落点，合并时必须一并去 <L>，不得留下「新 trait 带 L、旧 trait 不带」两套形态。
4. wnode 事务扩展面去 <L>：resp/txn_resp_commands.rs 的 TxnRespCommandsExt<L> 改 TxnRespCommandsExt，impl<L> TxnRespCommandsExt<L> for TransactionManager<L> 改 impl TxnRespCommandsExt for TransactionManager，network_runtxp/network_runtxp_fast 的 resolver 形参改 impl TxnProcResolver<S>。
5. 测试构造点：None::<()> 改 None（Option<Arc<dyn TxnAofLog>> 可推断）；tests/transaction_tests.rs、tests/transaction_manager_tests.rs 去 wtxn::TxnAofLog 多余导入。其余构造点（wtxn/tests、wcustom/tests、wnode/tests 其他档）第三参本就是 None，类型收敛后可推断，不需改动。

二、边界（不做）
- 不注入真实后端：session_dependencies.rs、service.rs 的 session_dependencies()、resp_server_session.rs 的 attach_transaction_components/inject_dependencies、resp_session_consumer.rs 的装配语义一律不动，恒传 None 的现状留给接线条 txn-aof-marker-session-wiring.md。
- 不改会话字段类型写法：去 <L> 后 resp_server_session.rs 的 txn_manager: Option<TransactionManager> 自动即为可持真实后端的形态，本条不新增注入入口，也不为省改测试另留第二套装配面（重载/builder 均算两套）。
- 不改 trait TxnAofLog 签名（错误透传属 task/ing/txn-aof-marker-error-propagation.md）。
- 不动 compute_sublog_access_vector 语义、不动 GarnetLog 侧 impl TxnAofLog。
- 禁 allow、禁 unsafe、不做向下兼容、不留泛型与 dyn 两套后端并存。

三、验收
- 硬指标 grep 归零：TransactionManager<、TxnRespCommandsExt<、TxnProcResolver<、TxnAofLog for (、None::<()> 在 wedb/ 全仓无命中。
- cargo +nightly clippy -q --tests --all-targets --all-features -- -D warnings -W clippy::absolute_paths 零警告。
- bun js/check.js 缺失 0 / 重复 0；run/commit 的 EnqueueTxn 与 log_proc 的 EnqueueStoredProc 三处映射注释仍与 C# 一一对应。
- ./test.sh 全绿（失败用例隔离复跑 3 次排 flake）。
- 对标锚点：garnet/libs/server/Transaction/TransactionManager.cs:26 无泛型类声明、:49 AofEnabled => appendOnlyFile != null、:106 private readonly GarnetAppendOnlyFile appendOnlyFile、:173 构造注入。

四、定名与口径（供接线条对齐，勿另立）
- 后端句柄不立别名：全仓直写 Option<Arc<dyn TxnAofLog>>（字段与 new 第三参两处），不定 TxnAofLogHandle 之类别名（仓规「不写别名，简化复杂类型定义除外」不适用此处）；接线条的 SessionDependencies 字段同写该型，全仓一种写法。
- 借用取法单点：句柄出借一律 self.aof_log.as_deref()（run/commit/log_proc/compute_sublog_access_vector 四处同形），不写 &**log 之类二次解引用。
- TxnProcResolver 保留首参 S（会话面，对标 C# TryTransactionProc 的 TGarnetApi 泛型位），仅删第二参 L；故 grep TxnProcResolver< 仍有 TxnProcResolver<S> / TxnProcResolver<RespServerSession> 命中属正确形态，判落地用两参式 TxnProcResolver<[^>]*,。
- 测试注入点写法：辅助函数返回类型直书 Arc<dyn TxnAofLog>（返回位天然向上转型），调用处 Some(Arc::clone(..)) 不再逐处 cast 或包壳。

五、落地记录（分支 wtxn-aof-dyn → dev 87c3e80）
按一~四口径全量落地，改动 6 文件、净删 93 行（+46/-139）：wtxn/src/transaction_manager.rs 删 impl<T: TxnAofLog + ?Sized> TxnAofLog for Arc<T> 与 impl TxnAofLog for () 两替身，aof_log 字段与 new 第三参直书 Option<Arc<dyn TxnAofLog>>（不立别名），TransactionManager / TxnProcedure / TransactionGuard / TxnProcReadApi 去参数 L，句柄出借四处统一 self.aof_log.as_deref()；txn_key_manager.rs、txn_proc.rs（TxnProcResolver 只留首参 S）、wnode/src/resp/txn_resp_commands.rs（TxnRespCommandsExt 去 L）、wnode/tests 两处 None::<()> 退为 None、test_log 返回型改 Arc<dyn TxnAofLog>。
硬指标 grep 归零（dev 全仓）：TransactionManager<、TxnRespCommandsExt<、TxnAofLog for (、None::<()>、两参式 TxnProcResolver<[^>]*, 均 0 命中；impl TxnAofLog 只剩 wnode/src/aof/garnet_log/mod.rs 一处（GarnetLog），后端单机制。
门禁（私有 target /tmp/rs-wtxn-aof-dyn）：cargo check --workspace --all-targets 零告警；cargo nextest run -p wtxn --no-fail-fast 36/36 绿；-p wcustom 12/12 绿；wnode 三个事务测试档（transaction_tests / transaction_session_test / transaction_manager_tests）19/20，唯一红 runtxp_resolves_registered_proc_end_to_end 在 dev 基线同码 checkout 复跑同红，系会话 garnet_api 恒未装配走 RESP_ERR_ASYNC_REQUIRED 早退，属边界二所列接线条 txn-aof-marker-session-wiring.md 的既有缺口，非本票引入。
实况校正一处：三、验收「transaction_manager_tests.rs 去 wtxn::TxnAofLog 多余导入」不成立——该档仍经返回型 Arc<dyn TxnAofLog> 与 log.get_physical_sublog_idx / get_replay_task_idx 用该 trait 入作用域，删之即编译不过，故导入保留（transaction_tests.rs 本就未导入）。
