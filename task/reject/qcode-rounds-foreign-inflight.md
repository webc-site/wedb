qcode.rounds.md 台账在途/等回报项汇总登记（并行会话在途，不代管）

审计时间：2026-09-19（fixloop 台账审计）。以下各项属并行会话在途工作，本审计只登记
不代管、不合并、不派单。台账行动项清零后原文件已删除，本页为其在途部分的唯一存档。

一、基线嫁接代理 qcode-baseline-re（台账 :214-216「已落 15 提交，等其回报后合入」）

- 范围：12 条非 wkv 红（checkpoint_store purge 孤儿文件、checkpoint_wiring
  disk_retention_reader_gate、aof_replay test_acl_replay_loop、aof_shutdown
  dispose_flushes_uncommitted_frames、config_owner_bridge provider_runtime_config_wired、
  object_collect_task replica_gate suspend/resume、resp_blocking blpop_on_tiered、
  resp_sorted_set geo_dist_unit、tiered_background_demote zset_count_dim_deadzone、
  tiered_watch_fence hash_write_arms、transaction_session_test runtxp_e2e、
  ttl_rmw_semantics expire_gt_same_value）。
- 含 scan_tree_in_batches 以 meta.size 为界的批量扫描改造 + --all-features 下三处
  调用点编译破口；抽查未见 #[ignore]/#[allow]/恒真断言（台账 :215-216）。
- ScanIter 深递归条件项（台账 :193-194）：第三方 crate bf-tree range_scan.rs:144 Deleted
  分支自递归，本仓消费点 wbftree/src/service/ops.rs:298/:342；主代理裁决不立案留证据，
  「若复现出真栈溢出再单独立案」——条件挂靠本嫁接代理的改造结果，未触发。

二、wkv 7 条红归并发会话在途（台账 :197-199）

- vdb dbmeta_record_roundtrip、dbmeta_layout cold_resolve_persist、flush_database
  atomic_batch、read_cache 三条 atomic_detach、swap_database swap_pair_record；
  归因：并发会话工作树正在改 wkv/src/vdb.rs 与 wkv/tests/store/flush_database.rs。

三、并发消费波次（台账 :54，07:27 实测）

- /tmp/fork 下 17 条分支/worktree 活跃（wave1-b…wave5-d、checkpoint-disk-retention、
  zero-consumer-pub-surface、session-metrics-counter-single-source、
  wkv-flush-step-call-single-source 等），task/ing 由其维护。

四、审查循环在途（台账 :123/:135）

- 第 9 轮 data/my 补跑、第 10 轮 data/my 补跑未收口；审查轮次与连续计数归审查会话。

五、分支处置残留（台账 :171-183）

- qcode-hashset-anchor(c666e39b)：修复主体已随 dev 2416beba 落地，分支残余可弃，
  无需再等合入窗口。
- advance-time-frame-name(4c728268)：已判重复工弃置，维持。
- test-baseline-green / aof-config-read-side-wiring 嫁接让路面：按台账 :181-183
  口径执行中；aof-config-read-side-wiring 票据在 next/aof-size-knobs-read-side-wiring.md。

六、首波派单余量（台账 :69）

- wait-for-commit-chain（next/wait-for-commit-chain.md）与
  aof-config-read-side-wiring（next/aof-size-knobs-read-side-wiring.md）两票在册；
  前者代码侧已见生产读点（drive.rs:199/:315），收口动作归下游。

后续轮次修复派单前须重查 task/ing + next/ 两队列（台账 :54/:70 口径）。
