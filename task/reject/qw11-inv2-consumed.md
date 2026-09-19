qw11 盘点波 2（next/qw11.inv2.md）open/partial 项核销汇总

来源：next/qw11.inv2.md（盘点波产物，2026-09-19 审计处置）。
审计基线：next/ 现存 15 份 + task/{ing,done,reject} 全目录比对，只读核验。

判定 18 项（open 17 + partial 1）现状：14 核销、3 在途、1 保留。

已完成（task/done，14 项）
- connection-exit-shutdown-close-notify → done/connection-exit-graceful-shutdown.md
  （该档自述正主为 glm.net 条 2、本票为其原文照抄壳，壳已剪除）
- diskless-full-sync-flush-all（与 inv1 重复记账，done）
- endpoint-parse-fail-fast-multi-bind → done/endpoint-parse-fail-fast.md（同 inv1）
- flushall-broadcast-parallel-fanout
- gossip-sample-send-quota-declaration
- hlen-o1-bypass-expiry-count
- incr-oldvalue-strict-i64（在册正主落定，reject 侧 -dup 副本不再挂）
- inbound-tls-client-cert-auth
- info-replication-slave-line-endpoint → done/info-repl-slave-line.md（改名核销，档内自述来源）
- info-store-snapshot-channel
- initiate-replica-sync-typo-spread → done/initiate-replica-sync-spelling-drift.md
  （该档自述正主为 glm.net 条 4、本票为壳，壳已剪除）
- lua-acl-check-cmd-catalog-single-source
- lua-call-fast-path-number-arg → done/lua-call-fast-path.md（原判让路 fix/lua-call-fast-path 分支，已落）
- network-connection-limit-accept-guard

在途（task/ing，3 项，均改名认领）
- gate-anchor-drift-reclean（partial，原名认领）
- lua-redis-call-pending-suspend → ing/lua-call-pending-suspend-handoff.md
- msetnx-slow-nx-meta-domain-probe → ing/msetnx-slow-path-meta-domain-probe.md

保留 next/（1 项，留待实现波）
- garnet-api-slow-path-command-split（open，打磨 LOW，仍成立未派）

其余
- landed 4 项为历史判定；stale 0，无勘误。
