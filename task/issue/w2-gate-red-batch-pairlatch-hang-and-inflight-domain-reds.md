立案（波次2 主代理集成门禁 gate-w2 --no-fail-fast 全量，基线 fd8b943 含 zcode-r159c-zpopcnt/zcode-r145c-configx 合入，2026-09-25 22:xx；4612 测 4599 绿 12 红 1 超时 2 skip）。13 例分家如下，唯案一待域主认领，余皆有主或已销。

案一（无主出生红·挂死，P2 候审）：wnode::pair_bucket_order_latch pair_arm_first_slot_latch_pin_fails_closed（tests/pair_bucket_order_latch.rs，init 8545b74 起未动）确定性 180s 超时——全量跑与单点复跑两度挂死（180.035s/180.010s TIMEOUT），非负载 flake。该测系 bound41/lockorder 取闩序收敛回归（pair 臂并入桶升序单机制 + 外部件桶闩钉住 fail-closed 判据），夹具以裸 fast_hash 直算桶号。疑染面：今日合入潮中动 rmw pair/RENAME/装载臂的批次（zcode-r147c-renamedb、zcode-r141c-msetbig、zcode-r139c-etag2 案一 7a88182 动 core.rs EtagResume 尾参）——外部钉闩若因桶号口径或 pair 臂换血失配，fail-closed 判据失灵可挂死。须域主二选一定因：钉闩/桶号直算失配（改夹具口径）或 pair 臂真挂死（真缺陷回滚裁断）。验收底线：该测改绿 + 全量门禁一轮。

案二（在途域主红 10 例，皆波次1 领票会话 task/ing 在办票领地，本席不越界，随其票闭环消红）：
- etag 族 5：etag_conditional_degrade_replay 三例（etag_resume_tail_wire_format Pending≠Full 等）+ ri_cold_etag_slow_single_frame 两例——r139c-etag2 在途，7a88182（案一合入）自带；
- msetnx 1：msetnx_atomic msetnx_fast_arm_expired_residual_single_verdict_source——r141c-msetbig 在途；
- rename 族 3：rename_nx_expired_residual_parity 1 + rename_migration_ttl_window 2——r147c-renamedb 在途（其新测文件已随批入库）。

案三（负载 flake，销案不立）：wnode::aof_recover_parallel_error_arm mid_batch_corruption / poison_before_flush 两例全量跑红、单点复跑双绿，系全量门禁并发负载膨胀已知形（tiered 慢测试根因在案先例），无行为缺陷。

案四（本席已销）：wnode::sorted_set_pop_ttl_reply_header zpopmin 会话锁——1ms 挂账在全量负载下被环前剔除（子代理自报风险成真），5b5a7dd 改 100ms 抗抖，环内跨到期刻压力形归 wcol 结构化锁 member_ttl_pop_reply_header 承压；点名复跑绿。

处置纪律：案一为唯一无主红，欢迎任何席认领；案二各域主在办票闭环时须带自家红例改绿验收，禁跨域代修掩盖。

终局增补（波次2 终门禁 gate-w3 @ 842f6620 no-fail-fast 全量，4685 测 33 红归属复核定案，2026-09-26 00:xx）：
一、bisect 定案（842f6620^1 前态同测复跑）：multi_exec_script_reentry 三例、setrange_append_page_gate、zadd_ttl_purge_not_solidified 在 keybucket 合入前即红，非波次2 引入，归 wlua/setrangeget/zset-ttl 域主（多为彼席合入自带红）。rmw_expired_rebuild_etag_cascade 四例系 43bbc798（zcode-r157c-srethead）引入、rmw_rebuild_side_domain_retire 三例系 e07ead2a（zcode-r147c-incrovf）引入，归其域主。etag 三例、ri_cold 两例、getex、msetnx、rename 两例、zrevlex、vector_read_lock/vector_registry/tiered 四例/cluster_resp_session/engine_swap_hook_bundle SIGSEGV 诸例俱系彼席在途票与其后追加合入（smovettl/exwatch/opt-r8/opt-r93 等）自带或新引入，本席不再逐例追认。aof_recover_parallel_leader_barrier 仍系负载 flake（前批已单测复核销案）。
二、波次2 自有红仅两例且均在 keybucket 域，已派修复棒：exec_embedded_bzpopmin_self_provided_key_no_hang（本票自带测，争用重投臂在自给键场景不收敛，超时 Elapsed）与案一 pairlatch 挂死（夹具已切 scoped 仍挂，修复棒一并诊断定因）。
三、zcode-r153c-restcrc 四锁（restore_ttl_failpath_residual_rollback）终门禁全绿，该票销红。