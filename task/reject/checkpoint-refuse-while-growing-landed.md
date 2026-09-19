检查点发起缺 is_growing 门：扩容迁移窗口拍快照漏条目

结论：已失效（init 起已落地），拒绝开发（fixloop f28 甄别 2026-09-19）

甄别记录
- 票据"现状"与当前 dev HEAD 不符：`is_growing` 门从仓库 init 提交（3c4f74a4，2026-09-19 12:56）
  起已完整落地，非"零命中"。台账原判复核的基点已过时。
- 已在位的完整实现（逐条对照票据"修法"与"验收"）：
  - wedb/wcpr/src/manager/create.rs：`create_checkpoint` 与 `create_checkpoint_with_token`
    两个入口首行均为 `ensure_not_growing(store)?`，位于 `ensure_epoch_unprotected`、Token 签发、
    `lock_ckpt_gate` 之前，拒绝路径零副作用。
  - wedb/wcpr/src/manager/mod.rs：`ensure_not_growing` 定义 + `CprStore::is_growing` trait 端口，
    文档注释含 StateMachineDriver.cs:164-166 单槽 CAS 互斥与 Tsavorite.cs:343-350 的对标说明
    （两处 C# 引用本次已实际核验属实）。
  - wedb/wkv/src/store/cpr_host.rs:303：宿主侧 `is_growing` 实现，透传 resize.rs:76/:113 谓词。
  - wedb/wcpr/tests/cpr/growing_gate.rs：回归用例已覆盖票据要求的全部验收点——扩容相位发布后
    两个入口均回绝 `Error::Host`（既有错误档，未新造变体）、零副作用（检查点目录不创建）、
    相位清除回 REST 后正常路径行为不变。
- 票据诊断的数据丢失机理与 C# 语义分析本身正确，但修复早已存在，无需任何开发。

以下为原票据存档

来源：第 9 轮 db 条 1（HIGH）。台账原判按主仓 dev HEAD 复核成立。

现状
- wedb/wcpr/src/manager/create.rs:68 create_checkpoint 与 :91 create_checkpoint_with_token 两处入口只做
  `ensure_epoch_unprotected(store)`（:75/:97）与进程级闸门 `lock_ckpt_gate()`（:77/:98），
  对索引扩容态零判定；grep `is_growing` 在 wedb/wcpr/ 全目录零命中。
- 扩容态谓词已在位：wedb/wkv/src/store/resize.rs:76 与 :113（`Store::is_growing`），生产消费点只有
  写读路径（wkv/src/session/raw/write/inplace.rs:138、:342，session/raw/batch.rs:283，
  session/raw/read.rs:335，session/raw/modify.rs:24），检查点侧无人问津。
- IN_PROGRESS_GROW 期间哈希条目正被 split_buckets 逐桶搬运，快照捕获点（create.rs:159 起的
  纪元排空屏障之后的 tail 时点）落在搬运中序上：未迁移分块的条目既不进快照、事后 fuzzy 重放也找不回
  （该区已被截断线越界），重启后键永久不可见。

C# 参考
- garnet/libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/StateMachineDriver.cs:164-166
  `Register` 用单槽 `Interlocked.CompareExchange` 天然互斥：已有状态机在跑（含 grow）即返回 false，发起方拿不到槽位。
- garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:343-350 文档明言
  "initiation may fail if we are already taking a checkpoint or performing some other operation such as growing the index"。

修法
- 两个入口（create.rs:68/:91）在 `ensure_epoch_unprotected` 之前加 `store.is_growing()` 拒绝臂，
  返回既有错误档（不新造第二套错误变体），语义与 C#「发起即失败」一致，由调用方（SAVE / 周期快照）按既有
  重试路径处理。
- 拒绝臂补一条集成用例：构造扩容中态（复用 wkv/tests/store/resize.rs:362 的相位发布手法）后发检查点须回绝。

优先级：功能缺口（数据持久性/键丢失），本批最高。

验收
- 扩容窗口发检查点必失败且零副作用（不留半截 token 目录）；REST 态行为逐字节不变。
