qw11 盘点波 4（next/qw11.inv4.md）open/partial 项核销汇总

来源：next/qw11.inv4.md（盘点波产物，2026-09-19 审计处置）。
审计基线：next/ 现存 15 份 + task/{ing,done,reject} 全目录比对，只读核验。

判定 14 项（open 13 + partial 1）现状：12 核销、1 在途、1 保留。

已完成（task/done，11 项）
- tiered-write-arm-concurrency（原判 partial「删除半+互斥面未做」，现 done → 残面已闭环；
  与 inv3 重复记账）
- txn-aof-marker-error-propagation（原判阻塞链第三环，已解）
- unixsocketperm-config-knob → done/unixsocketperm.md（改名核销）
- vector-preview-production-enable
- wkv-stale-checkpoint-read-cache-anchor → done/wkv-stale-ckpt.md（改名核销，档内自述来源）
- wnode-static-vtable-erase-collapse
- wtxn-aof-log-dyn-backend（原判「无人持单、阻塞头票」，已落地 → 阻塞链整条解除）
- whlog-safe-tail-identity-alias（原判在途让路，现 done）
- wkv-gc-compaction-interval-num-segments-surface（原判在途让路，现 done）
- wkv-gc-reclaim-decouple-from-scan-loop → done/wkv-gc-reclaim-decouple.md（改名核销；
  对应 qcode.rounds 所记并发合入 066dc8a）
- vector-registry-nsdb-isolation（原判本切片最大单，已完成）

已拒绝（task/reject，1 项）
- ttl-purge-watch-version-bump → reject/ttl-purge-watch-version-bump.md
  勘误：inv4 判 open HIGH（WATCH 漏通知），后被对照 garnet C# 逐链甄别推翻
  （「后台过期扫描推版本」「惰性过期即物理删除每次推版本」两前提均不成立，拒绝）。
  以 reject 档为准。附言留痕：wkv/session/raw/write/mod.rs:69-73 头注失真，
  该档已注明留待触及该文件的后续票据顺手订正，不另立票。

在途（task/ing，1 项）
- txn-aof-marker-session-wiring（前置 wtxn-aof-log-dyn-backend 已落，与 inv4 预期一致）

保留 next/（1 项，留待实现波）
- wnode-service-split（唯一门禁 txn-aof-marker-session-wiring 仍在 ing，判定仍有效）

其余
- landed 2 项（tiered-promote-demote-key-ttl、wait-for-commit-chain）为历史，
  reject 侧另有 *-landed 存档；stale 0。
- 原文 top 8 派单清单所列票均已进 done/ing，清单作废随档归档。
