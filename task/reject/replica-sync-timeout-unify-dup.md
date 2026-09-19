重复：task/done/repl-timeout.md（复制同步超时口径对齐已落地核销，四套上界收口已由该票完成）

来源：next/qw.net.md 第 11 轮 net 条 6 [MED]「复制链统一超时源只接存储层两处、复制链一处不接
全改硬编码；注释还把 attach 等待记到另一枚从不读的旋钮上」。

逐项复核（当前 dev 工作树），该条所列取证事实已全部过时：
- replication_snapshot_iterator.rs 的 SYNC_FRAME_TIMEOUT(30s) 已不存在（全仓 grep 零命中），
  对应 repl-timeout.md 改动第 7 条「删除局部 SYNC_FRAME_TIMEOUT，三处停等限时单点复用
  REPLICA_SYNC_TIMEOUT」。
- replica_diskless_sync.rs:151 attach 等待已改用 REPL_ATTACH_TIMEOUT（60s 具名单常量），
  :128-129 注释已按实际超时源改写（「attach 级限时取 REPL_ATTACH_TIMEOUT(60s)，对标
  ReplicaDisklessSync.cs:171-174」），原「挂 REPL_ATTACH_TIMEOUT 却从不读」的失真注释已订正，
  对应改动第 3 条。
- diskless_replication/replica_sync_session.rs:298 ATTACH_SYNC 等待已用 REPL_ATTACH_TIMEOUT
  （内联字面量已消除，:54 残留的 REPLICA_SYNC_CMD_TIMEOUT 亦为具名常量非内联 30s），对应
  改动第 4 条。
- 「具名 5s + 具名 30s + 内联 30s + 内联 30s 四套上界」形态已收口为
  REPLICA_SYNC_TIMEOUT(pub crate, replica_wire.rs:248) + REPL_ATTACH_TIMEOUT 两个单点常量。

残余主张（复制链改读 wconf 的 replica_sync_timeout_secs 配置面）与 repl-timeout.md 的显式
边界冲突：该票「边界」一节明确「不建 wconf 配置面（两旋钮入配置转写另计）」，即配置面接线是
已划出的另计事项而非遗漏，不构成本条的新问题成立基础。

结论：dup，命中 task/done/repl-timeout.md；若后续要补 wconf 配置面，应以该票「另计」边界
为起点另立单，不采用本条的「复制链全改读配置旋钮」口径。
