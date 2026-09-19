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
