第 8 轮修复首波 6 项 + 合规门漂移清账：4 项已修复落地、2 项有票在册、合规门三去向全部在册

审计时间：2026-09-19（fixloop 台账审计）。取证基点：主仓 dev 工作树 grep 实测。

修复首波（台账 :69，08:03 派出，文件域互斥）

- info-keyspace-single-stats-kernel（my HIGH：INFO KEYSPACE 逐库慢路径断链加双统计内核）：
  已修复。wnode/src/resp/info_provider.rs 单内核落地——resp_server_session.rs:1781
  try_info_keyspace_slow_path 分派（garnet_api/raw.rs:458 C::Info 臂）、
  wkv/src/store/keyspace.rs:256 WedbStore::keyspace_stats（一趟分桶扫描）、
  garnet_api/slow.rs:288 注释「单内核 WedbStore::keyspace_stats」；info_provider.rs
  同时定义 InfoSlowScanSource 双源形态（:233/:290）。
- wkv-ri-heal-kernel-single-source（db MED：RI 复合元记录治愈体四抄且超 128B 三口径互斥）：
  已修复。wkv/src/range_index/stub.rs:560 pub(crate) fn patch_stub_record 治愈内核
  单点，:344 注释「C# 的两个 Span 原地静态变更器在 rust 由治愈内核 patch_stub_record
  承接」，:365/:391/:418 三端口均转调内核（与台账 :131 修复分支清单一致）。
- gate-anchor-igarnetreadapi（纯注释锚点，A 层虚构锚点 14 处）：已修复。
  wnode/src/storage/session/txn_proc_view.rs IGarnetReadApi 命中归零；
  wtxn/src/transaction_manager.rs:201/:209 改为映射口径说明注释
  （「IGarnetReadApi 与 IGarnetApi 同文件声明」）。
- gate-allow-await-holding-lock（tests 面 allow 摘除）：已修复。
  全仓 grep `#[allow(` 于 wedb/wkv/wnode/wresp/wconn/wtxn/waof/wcol/wbase/wconf/
  whlog/wrecord/wreviv/wbftree 零命中（与台账 :133「全仓 #[allow( 计数打到 0」一致）。
- wait-for-commit-chain（design HIGH）：有票在册 next/wait-for-commit-chain.md；
  代码侧已见接线（wnode/src/net/handler/drive.rs:199/:315 生产读
  session.wait_for_aof_blocking()，resp_server_session.rs:296/:375 WaitForCommit 投影），
  票据收口归下游，不代管。
- aof-config-read-side-wiring（design HIGH + net MED 合并）：有票在册
  next/aof-size-knobs-read-side-wiring.md（aof_memory_size/aof_page_size/aof_segment_size
  现况：wconf/runtime_server_options.rs:67-71/:121-123 默认三档 + runtime_server_config.rs:67
  格式器；装配侧读点接线与装配期互校验归该票），不代管。

合规门漂移（台账 :56/:67）

- 实现缺失 1（IndexResizeSM）+ 虚构锚点 1（wconn/parser.rs:79）：台账 :92-94 记录
  qcode8.gate.md 回收双清零；后续「实现缺失」扩容的 AllocatorBase/IAllocator/
  ObjectAllocator/SpanByteAllocator/TsavoriteLogAllocator/LightEpoch/
  DoubleTurnstileBarrier/AofAddress/LogAddress/OverflowBucketLockTable 及 test.epoch/
  test.hlog 族（台账 :67「待下波逐族甄别后再登记」）→ 已立案
  next/gate-anchor-drift-reclean.md（标题「实现缺失登记缺口逐族甄别」，票面自记
  19 个家族清单与「台账当时判为非引擎缺件、待逐族甄别后再登记，一直未做」出处）。
- 重复定义提示 12 组 → 已立案 task/ing/cs-anchor-dup-single-mount.md
  （票面自记「承接 next/qcode8.gate.md 条 3」，实测演进为 18 组、原报 13 组中五组已落地）。
- B 层词法断言提示 123 处：台账 :56 明判「条 9 口径外存量」，不属行动项，维持不处置。

结论：首波与合规门全部去向明确（4 修复 + 2 在册票 + 3 合规门去向），无悬置残留。
