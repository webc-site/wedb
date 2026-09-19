换号物理回收四正确性面寄生于过期键扫描循环（拆票甄别：查重命中，不重复立项）

来源：next/glm.my.md 第 9 轮条目（glm.my.md:14-17，拆票时代理甄别拒绝）。

结论：重复，拒绝开新票。同题既有票 next/wkv-gc-reclaim-decouple-from-scan-loop.md 已完整承接，仍在册未修。

查重命中与对应关系

- 既有票自述来源即「next/glm.my.md（自定义优化上下游打通审查）第 9 轮首条，第 10 轮仍在册」，
  与本条同源同轮。
- 四个正确性面逐项对齐：墓碑注销（pop_reclaimable）、退役租户路由表释放（pin_routing.remove）、
  空闲路由析构（pop_idle_candidates / evict_idle_route）、高低水位熔断（refresh_compact_boost /
  try_compact 首行判定）——两文完全一致。
- 锚点一致：wedb/wkv/src/gc.rs（enabled_by_config 判定、tick 唯一编排、spawn_bftree_reclaimer
  常驻口径注释、try_compact「不受旋钮关闭」失真注释）、wkv/src/config.rs gc.enabled 默认 false、
  C# garnet/libs/server/Databases/DatabaseManagerBase.cs FlushDatabase ShiftBeginAddress 即时回收对位。
- 修法一致：四个面从扫描循环剥离、并入 spawn_bftree_reclaimer 同型常驻循环，gc.enabled 只门控
  过期键扫描；不推翻 expdelscan-bg-scan-mutex 默认禁用裁定、不碰
  wkv-gc-compaction-interval-num-segments-surface 射程。
- glm.my.md 头部第 1 行亦自证「第 9 轮 1 条换号物理回收寄生过期扫描循环……仍在册未修」——
  「在册」即指该 next/ 票。

处置：本条不另立票，由 next/wkv-gc-reclaim-decouple-from-scan-loop.md 继续跟踪至修复。
（本票不附原文照录，原文见既有票的前提/修法/边界三节，语义等价无增量信息。）
