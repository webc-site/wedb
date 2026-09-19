qw11 盘点波 3（next/qw11.inv3.md）open/partial 项核销汇总

来源：next/qw11.inv3.md（盘点波产物，2026-09-19 审计处置）。
审计基线：next/ 现存 15 份 + task/{ing,done,reject} 全目录比对，只读核验。

判定 17 项（open 14 + partial 3）现状：15 核销、1 在途、1 保留。

已完成（task/done，13 项）
- papaya-random-seed-anti-dos
- qcode10-client-outstanding-admission-gate
- qcode10-enable-debug-command-knob
- resp3-command-layer-frame-parity
- resync-strategy-store-version
- runtime-config-hot-reload-consumers
- runtime-options-read-only-path-fields-unprojected → done/ro-path-project.md（改名核销，档内自述来源）
- simd-selection-single-claim
- sintercard-i32-filter-lower-bound
- spublish-cross-node-shard-delivery
- subscribe-broker-shutdown-dispose
- tiered-write-arm-concurrency（原判 partial「互斥面/删除半未做」，现 done → 残面已闭环）
- replication-timeout-knob-alignment → done/repl-timeout.md（改名核销，档内自述来源）

已拒绝（task/reject，2 项 partial）
- qcode10.net → reject/qcode10.net.md（整档归档）。inv3 所称「唯一未被认领的残留」
  （wresp RESP_ERR_UBLOCKING_CLINET 第三枚零读者文案）已由
  reject/qcode10-net-err-const-zero-reader-resolved.md 甄别承接：主体修法已落地拒绝，
  残留单枚常量移交零消费普查域（zero-consumer 系列批票口径），明确不另立票 → 无漏项。
- resp-pubsub-acl-e2e-test-parity → reject/resp-pubsub-acl-e2e-test-parity.md
  勘误：inv3 partial 判定（ACL 面半落地、PubSub 大 payload 缺测）被最终甄别推翻——
  reject 判「AI 生成的测试覆盖注水，两点前提均不成立，拒绝实现」。以 reject 档为准。

在途（task/ing，1 项）
- qcode10-parse-db-index-i32-parity

保留 next/（1 项，留待实现波）
- resp-server-session-file-split（open，打磨 LOW，3153 行拆分未动，仍成立）

其余
- landed 5 项为历史判定；stale 0。
- 原文建议派单 top 8 所列票均已进 done/ing/reject，清单作废随档归档。
