优先级：中
来源：next/agy.db.md 条 3 立项。取证基线：主仓 dev 当下代码，行号为当下实测。

问题
事务状态机核心文件 transaction_manager.rs 内直接定义具体业务数据操作接口
（TxnProcApi 的 get/set/setex/delete/increment/sorted_set_add/sorted_set_remove），
存储过程能力面与事务状态机混居一文件；txn_proc.rs 的 TxnQueuedCommandInfo 用堆分配
String 存命令名（仅错误回显用），事务排队路径每命令一次堆分配。

取证
- wedb/wtxn/src/transaction_manager.rs:125 pub trait TxnProcApi（fn get :127、
  fn set :129、fn setex :132、fn delete :134、fn increment :136、
  fn sorted_set_add :138、fn sorted_set_remove :140）、:144 pub trait TxnProcReadApi、
  :155 pub struct TxnWatchApi（impl TxnProcReadApi :160）。文件共 701 行。
- wedb/wtxn/src/txn_proc.rs:9 pub struct TxnQueuedCommandInfo，:11 pub name: String
  （doc 自注「命令名（错误回显用）」），:13 arity、:15 allowed_in_txn、:17 is_sub_command。
- 消费链：transaction_manager.rs:653 构造 TxnWatchApi 交存储过程执行。
- C# 对标：garnet/libs/server/Transaction/TransactionManager.cs（状态机：WATCH 校验、
  键集加锁、提交回放，不含具体 get/set 业务接口）；业务读写过程定义在
  garnet/libs/server/Custom/CustomTransactionProcedure.cs（ITM 的 Get/Set 等随过程类），
  命令注册信息在 RespCommandsInfo 常量表（非运行期 String）。

修法建议
TxnProcApi / TxnProcReadApi / TxnWatchApi 三接口移出 transaction_manager.rs，收敛到
txn_proc.rs（存储过程能力面单文件）；TxnQueuedCommandInfo.name 由 String 改为
'static str 或命令枚举（解析层命令名本就是静态表项，回显直接借静态串），消除事务
排队堆分配。纯搬运加类型收紧，语义零改动。

----------------------------------------------------------------------

## 判词（开发棒 txn-proc-split）

裁决：判拒，dev 零改动。本棒曾在分支 txn-proc-split 上把搬家做完并过全门禁（实现提交
f81868d），随后盘查文档侧引用时发现同题已被分拣判拒归档，按「双花按主题判、已裁者胜」
收手不并，worktree 与分支已清理。

反转经过：步骤 1 在 HEAD 5c30f0b 复核票面，第一条主张行号全数命中（transaction_manager.rs
701 行，pub trait TxnProcApi:125、pub trait TxnProcReadApi:144、pub struct TxnWatchApi:155
与 impl:160），据此判「成立」开工；做完后才在 grep 文档引用时撞见下面三条拒由。

拒由一：同题已被分拣判拒并归档（双花）
- task/reject/agy.db.md 条 4（即本票「来源：next/agy.db.md 条 3」所指原条，标题
  「TransactionManager 侵入具体业务 API 与堆分配排队命令（部分拒绝）」）已就「三件移出
  transaction_manager.rs」这半条判不成立，三条理由在册：唯一消费方是同文件 TxnProcedure
  三段式（:173）与 run_transaction_proc（:632）、视图与消费者同文件是本仓惯例、搬到
  txn_proc.rs 只是换文件名无收益。
- 兄弟票 task/done/wtxn-queued-command-name-static-str.md「双花登记」段点名本票：
  「并发代理就条 3 另立薄票 next/db-txn-proc-api-out-of-core.md……搬迁半条已判不成立并归档
  task/reject/agy.db.md 条 4……若对方薄票仍被派发，须先剔除搬迁段，两票禁同棒双花」。
  剔除搬迁段后本票剩余 scope 为 0（见拒由二），故整票判拒。

拒由二：第二主张已落地，无重复施工余地
- 票面「txn_proc.rs:11 pub name: String」在 HEAD 已不成立：现 txn_proc.rs:13 是
  `pub name: &'static str`，由 1dade58（refactor(wtxn): 排队命令名与过程名收口 &'static str，
  MULTI 逐命令堆分配归零，dev FF 59511ed）落地，票档 task/done/wtxn-queued-command-name-static-str.md。
- 兄弟票 own 那半条时明写「未搬 transaction_manager.rs（双花薄票的搬迁半条零动作）」，
  即两票的分工本就是：它做堆分配、本票若存在才做搬迁；现两头一头落地一头判拒。

拒由三：票面 C# 对位失实，所指落点与 C# 目录拓扑相反（本棒实测反证，行实如下）
- garnet/libs/server/Transaction/TransactionManager.cs:26 是
  `public sealed unsafe partial class TransactionManager`，全件 grep
  `GarnetStatus Get(`/`Set(` 零命中 —— 票面「状态机件不含具体 get/set 业务接口」这半句对。
- 但票面「业务读写过程定义在 garnet/libs/server/Custom/CustomTransactionProcedure.cs
  （ITM 的 Get/Set 等随过程类）」失实：Increment 在 libs/server/API/IGarnetApi.cs:256/:265、
  SortedSetAdd :372/:382/:392、SortedSetRemove :407/:417/:427，只读界 IGarnetReadApi 与
  IGarnetApi 同文件声明（IGarnetApi.cs:1354，rust 侧搬出块的文档早已注明此口径），
  读即 WATCH 包装在 libs/server/API/GarnetWatchApi.cs；CustomTransactionProcedure.cs 只在
  :73（Prepare<TGarnetReadApi> where : IGarnetReadApi）、:79（Main<TGarnetApi> where :
  IGarnetApi）、:87 以泛型形参消费它们，本体不定义任何读写原语。
- 故按 C# 对位，这三件的归属是 API 面（libs/server/API 的投影，rust 真实落点已在 wnode
  src/storage/session/txn_proc_view.rs:73 impl TxnProcApi），而非票面要求的 txn_proc.rs
  （对位 C# 的 Custom/）。票面的搬法恰是把 API 件塞进过程件，与它所依据的 C# 目录相反。

本棒实测数据（留档，非落地依据）
- 依赖方向反证：搬前 txn_proc.rs → transaction_manager.rs 单向（txn_proc.rs:7 use
  crate::transaction_manager::TransactionManager）；搬后 transaction_manager.rs 须回引
  txn_proc 三件，两文件转互相依赖，内聚不升反降。状态机件仅 701→649 行（−7.4%），
  代价是把派发界与其唯一消费者拆成跨文件跳转。
- 搬家逐字性：搬出块（HEAD 112-165，54 行）与落位块直接对 diff，仅两处必需改动——
  文档内链 [`TxnLockTable`] 补全为 [`crate::txn_lock_table::TxnLockTable`]（裸名跨模块
  失 scope）、TxnWatchApi.api 由模块私有降 pub(crate)（唯一装配点 run_transaction_proc
  跨模块构造，对外仍不可构造；按 task/done/wdev-segmented-device-file-split.md 口径
  「跨域私有项统一降 pub(crate)、禁为拆分升 pub」）。
- 门禁（私有 CARGO_TARGET_DIR=/tmp/ct-tps，未跑主仓 ./test.sh 与 ./sh/clippy.sh）：
  cargo check --workspace --all-targets 合并 dev 前后各一次 exit 0 零告警；
  cargo fmt -p wtxn --check exit 0；bun js/check.js 改动前后 stdout 逐字节相同、
  零 ignore 语料回写、exit 0（重复定义段与实现缺失段无任何 wtxn 条目增减）；
  cargo nextest -p wtxn -p wcustom 49 枚：单跑 49/49 通过（180s 那轮里
  txn_lock_stress::stress_manual_locks_across_threads_without_deadlock 因与 check/wnode
  门禁并发争机超时，它单独跑时 60.8s 通过）；cargo nextest -p wnode 的
  transaction_manager_tests + transaction_session_test + transaction_tests +
  aof_stored_proc_replay 24 枚 23 通过，唯一红 flush_db_entry_replays_targeted_database
  经 git stash 取回改动复跑印证在基点树同样红，属存量红，已登记
  task/ing/r4-red-attribution-fix.md 第 5 枚（6dc1cb6 FlushDb/域路由归因），与本主题无关。
- 并发核验：merge-base 复算 12 个在途分支，wedb/wtxn 内唯一自有改动是 windex-2pl-removal
  的 txn_lock_table.rs（不在射程）；开工时 `git branch --list '*txn*'` 零命中。

处置
本票移 task/reject/，dev 零改动，/tmp/fork/txn-proc-split 与分支 txn-proc-split 清理。
日后若仍要「出核」，正确形态是按 C# 目录另立 wtxn 的 API 面件（对位 IGarnetApi.cs 与
GarnetWatchApi.cs，而非 txn_proc.rs），并先撤销 task/reject/agy.db.md 条 4 的判词、
把本档拒由三的行实补录为反证。
