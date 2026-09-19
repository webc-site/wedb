SLOTS 迁移删除环缺 copy_option 门：COPY 语义下源端键被误删

拒绝（已失效）：2026-09-19 对照当前 dev HEAD 复核，票据指认的缺陷已完整在位修复，
票面"现状"陈述与现行代码不符，无可改之物。

- wedb/wedb/src/server/migration/migrate_driver/slots.rs:219 已有
  `if !spec.copy_option {` 整段包住 set_status(Deleting) → 纪元静止等待 →
  delete_string 循环；`session.sketch.clear()`（:231）在门外，copy 态照常复位推进，
  与 keys.rs:881 收口同形。
- 更有 slots.rs:285-288 copy 态 untouchable 登记（transferred + ri_keys），
  承接槽头重扫游标收敛——超出本票修法要求。
- 三臂同形确认无漏网：migrate_session_range_index.rs:130、
  migrate_session_vector_set.rs:112、migrate_driver/keys.rs:881 均 `if !spec.copy_option`。
- 删除失败 `let _ =`（slots.rs:228）与 keys.rs:890 同口径留痕，票面亦认可该口径。
- C# 对位核实：garnet MigrateOperation.cs DeleteKeys() 首行
  `if (session._copyOption) return;`，rust 现实现语义一致。

git 历史压缩为 init 提交（3c4f74a4 即含门控），无法追溯票面复核基于的旧态；
以现状为准，本票不成立。

来源：第 9 轮 net 条 1（MED）。按主仓 dev HEAD 复核成立。

现状
- wedb/wedb/src/server/migration/migrate_driver/slots.rs:210 无条件
  `session.sketch.set_status(SketchStatus::Deleting)`，:212-216 纪元静止等待后，
  :217-219 `for key in &transferred { let _ = storage.delete_string(key).await; }`，:220 sketch.clear()。
  全段无 `spec.copy_option` 判：MIGRATE … COPY 走完 SLOTS 通道后源端键照删，语义反成 MOVE。
- 同域三臂都已正确门控，形态现成可抄：
  wedb/wedb/src/server/migration/migrate_driver/keys.rs:865 `if !spec.copy_option { … Deleting → 删除 → clear }`；
  wedb/wedb/src/server/migration/migrate_session_range_index.rs:130；
  wedb/wedb/src/server/migration/migrate_session_vector_set.rs:107。
  即 SLOTS 臂是该族的漏网臂，非另一种设计。
- 次生面：:218 `let _ =` 吞删除失败（与 keys.rs 同位），键未删成而 sketch 已 clear，源端留脏；
  本单一并按 keys.rs 口径处置留痕，不新建第二套错误通道。

C# 参考
- garnet/libs/cluster/Server/Migration/MigrateOperation.cs:231-237 `DeleteKeys()` 首行
  `if (session._copyOption) return;`，再按 `transferOption == TransferOption.SLOTS` 分形态删除——
  copy 门在删除环最外层，SLOTS/KEYS 共用同一出口。
- copy 标志来源 garnet/libs/cluster/Server/Migration/ 会话侧 `_copyOption`（rust 对位
  wedb/wedb/src/server/migration/migrate_session.rs:30 `pub copy_option: bool`）。

修法
- slots.rs:210-220 整段收进 `if !spec.copy_option { … }`，copy 态直接跳过快照清理后的删除步
  （sketch 状态机推进与 keys.rs 同形）；删除失败按 keys.rs 现口径留痕。
- 用例：COPY 迁移后源端键仍可读（含 RI/向量键混合槽）；非 COPY 路径逐字节不变。

优先级：功能缺口（数据可见性正确性），本批高于纯死代码项。

关联：task/ing/migration-frame-import-core.md（导入侧帧管线单源，与本单导出删除侧互不覆盖）。
