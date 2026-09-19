qw11 盘点波 1（next/qw11.inv1.md）open/partial 项核销汇总

来源：next/qw11.inv1.md（盘点波产物，2026-09-19 审计处置）。
审计基线：next/ 现存 15 份 + task/{ing,done,reject} 全目录比对，只读核验。

判定 15 项（open 14 + partial 1）现状全部核销，next/ 无本切片残留：

已完成（task/done，13 项）
- acl-getuser-resp3-frame-parity（open MED）
- aof-driver-register-pre-transfer（open HIGH）
- aof-size-knobs-read-side-wiring（open MED）
- auth-ns-default-spec-drift（open LOW，纯文档对表）
- bftree-release-detached-guard-recheck（原判「让路/在途」，现已在 done → 完成）
- client-type-remote-node-id-gate（open HIGH）
- cluster-outbound-tls-client（open HIGH）
- cluster-reset-ban-list-parity（open MED）
- cluster-slot-gate-sync-spin（open MED）
- cluster-suspend-await-lock（原判 partial「净贡献未落」，现 done → partial 残面已闭环）
- compact-safe-read-only-address-bound（open HIGH）
- diskless-full-sync-flush-all（open HIGH）
- endpoint-parse-fail-fast-multi-bind → done/endpoint-parse-fail-fast.md（改名核销，
  该档自述来源即本票，含 bind 多地址拆分）

在途（task/ing，2 项）
- aof-store-rmw-dead-replay-arms（盘点判 open 时的第一派单建议，已被认领）
- boot-assembly-projection-single-source（同上）

其余
- landed 7 项为历史判定，不重复记账。
- stale 0，无与现状矛盾的判定，无勘误。
- 原文「建议立即派单清单 top 8 + 次批 + 顺手清」所列票均已进 done/ing，清单整体作废随档归档。
