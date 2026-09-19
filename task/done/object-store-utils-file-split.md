object_store_utils.rs 四段异源职责混装：抽 rmw_helpers 与分层壳文件

来源：设计审查轮次甄别（原 qcode 条 18，票据已核销删除）。

结论
单文件同时承载四类异源职责：输出/装载辅助、装载保存、对位 C# RMWHelpers.cs 的泛型 RMW 调度器、以及 rust 自有的
分层调度壳；(c)(d) 与 (a)(b) 无内聚关系却顶同一文件行号。判定成立且待做。

现状（HEAD 取证）
- wnode/src/resp/objects/object_store_utils.rs 单文件 1377 行。
- (a) 信封头解析辅助：:59 ElementHeaderKind、:123 parse_elements_header、:156 write_n2_array、
  :168 compute_expiration_ticks。
- (b) 装载保存面：:221 obj_load_typed_sync、:251 obj_load_custom_sync、:385 obj_length_sync、
  :617 obj_save_or_gc_raw、:649 obj_save_or_gc。
- (c) 泛型 RMW 调度器（对应 C# 独立文件 RMWHelpers.cs 边界）：:209 RespRmwDone、:1093 SyncRmwCmd、
  :1103 SyncRmwHandlers、:1145 run_sync_rmw。
- (d) 分层调度壳（rust 自有）：apply_rmw_post_operate ~:946、obj_writeback_tiered ~:1024、
  try_tiered_arm/retire_tiered_dest 段。
- :1225 起文件内 mod abort_tests。

C# 参考
- garnet/libs/server/Objects/RMWHelpers.cs（RMW 广义调度，对应段 c）。
- garnet/libs/server/Storage/Session/StorageSession.cs 对象方法（对应段 b）。

修订方向
纯移动拆分：resp/objects/rmw_helpers.rs 承 (c)，object_store_utils.rs 保留 (a)(b)。段 (d) 的分层壳随
其所属在途改动一并落位（见交叉引用），本票以 (c) 抽取为主目标（C# RMWHelpers.cs 是清晰独立文件边界，与
在途命令臂覆盖正交）。mod.rs 再导出保持 wnode::resp::objects:: 符号路径不变。

交叉引用
段 (d) 与在途分层命令臂覆盖分支（tiered-command-arm-coverage）同函数域，须串接、其合并后重核行号；
apply_rmw_post_operate / obj_writeback_tiered 亦为 task/ing/tiered-background-demote.md 的落点，本票只做
纯移动不改其降阶判定语义。

验收
object_store_utils.rs 显著回落（目标 < 800 行）；git diff 为整块搬迁、无实现差异；外部引用零改动编译通过。

优先级
打磨（文件内聚与并行修改热点）。

实施方案（代理核验后追加，2026-09-19）
核验修正三条：
1 C# 锚点勘误：garnet/libs/server/Objects/RMWHelpers.cs 不存在（原票幻觉）。真实对位是
  garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs（对象 RMW 引擎钩子）加各
  Resp 命令 Network* 方法内重复的 storageSession.RMW 调用形态；rust 的 run_sync_rmw /
  run_async_rmw 正是这二者在命令层的泛型合流。文件名沿用原票定名 rmw_helpers.rs，doc 注明
  真实对标。
2 切线修正：(c) 不可单抽。run_async_rmw（原票划入 (d) 留守）签名直接消费 SyncRmwCmd /
  SyncRmwHandlers；RespRmwDone 被 write_rmw_reply、run_async_rmw、run_sync_rmw 三处共用；
  apply_rmw_post_operate 是同步/异步两骨架共用的收尾状态机。(c) 单抽仅 132 行且制造两文件
  交叉依赖。RMW 执行域整体内聚，一并迁出。交叉引用所列 tiered-command-arm-coverage 分支
  已不存在（未开或已并入），无串接阻塞；task/ing/rmw-atomic-read-modify-write-window.md
  为行为票，落在本拆分之后合并不受影响。
3 行数口径：RMW 执行域约 592 行迁出，留守约 850 行上下（(a)(b) 本体加 abort_tests），
  较原 1450 显著回落；(a) 段命令头解析与 abort 辅助对标 ObjectStoreUtils.cs 与命令文件
  内联校验，留守即本位，不再细切避免过度拆分。

落点（纯移动，函数体逐字搬运，doc 随体）：
- wedb/wnode/src/resp/objects/mod.rs 增 pub mod rmw_helpers;
- 新文件 wedb/wnode/src/resp/objects/rmw_helpers.rs 迁入：RespRmwDone、write_rmw_reply、
  try_tiered_arm、retire_tiered_dest、run_async_rmw、apply_rmw_post_operate、
  obj_writeback_tiered、slow_load_eval、SyncRmwCmd、SyncRmwHandlers、run_sync_rmw
- object_store_utils.rs 保留 (a)(b) 与 abort_tests，头部加
  pub use super::rmw_helpers::{...}（与既有 :28/:46 转发先例同型），消费面 21 文件按
  object_store_utils:: 路径零改动
- obj_save_or_gc_raw 由私有提 pub(super)（run_sync_rmw 跨文件调用，最小可见性）
- 两侧 use 树各自精简，删不带入项

验收口径同原票：cargo check 零 error 零 warning；diff 为整块搬迁无实现差异。
