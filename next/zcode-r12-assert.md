r12 测试断言强度审查(轮12;与 r2-test 互斥:它查有没有测试,本篇查断言是否真的在验证)

方法
9 个目标 crate tests/ 分层抽样(wnode 13/wkv 6/wedb 5/wbftree 4/whlog 3/wconn 3/wcol 2/waof 2/wresp 2,共 40 个测试函数逐一精读),另全仓扫 should_panic/is_ok 恒真/自引用 expected 模式,并抽 TransactionTests.cs、RespSetTest.cs、ExpiredKeyDeletionTests.cs、BfTreeInteropTests.cs 对照 C# 对位断言。

一、无效(测试看似在守卫,实则守不住)

1. stale_envelope_writeback_never_replaces_promoted_tree
   路径: wedb/wnode/tests/tiered_envelope_writeback_race.rs:127
   问题: #[ignore = "竞态构造时序未闭环,见 next/agy-progress.md T1"]。 ignore 注记自认竞态注入未达成——实测矫正臂应答 2498(计数)而非存储忙错误帧,即该测试若解除 ignore 当场红,它既没验证修复、也没验证现状;且指针 next/agy-progress.md 已不存在( working tree 与 git 历史均无),无人会回来续作。
   影响: 「信封矫正臂写回撞并发升阶致字段集静默丢失(不可线性化)」这一真实缺陷形的回归锁为空。套件绿色与此缺陷是否回归完全无关;生产侧 fail-closed(rmw_helpers obj_writeback_tiered 未封窗臂 re-probe)在位但无测试证明其可达。
   建议: 二选一并落票: (a) 重排注入臂与 re-probe 轮询交错(先 poll 矫正臂过 meta 探针再落 Meta 记录)使断言真触发; (b) 若交错在 compio 单 worker 下不可达,删测试并在 fail-closed 臂处留防复发注释,不能留 ignore 悬空。

2. concurrent_vadd_to_spilled_set_makes_progress
   路径: wedb/wnode/tests/vector_set_concurrent_vadd_disk_spill.rs:68
   问题: #[ignore = "并发 VADD 死锁复现(~9k 次后楔死于 VADD,量化锁竞争);src 修复后移除"]。注记明示 src 侧死锁(C# 注释同款历史死锁)修复未落地,测试被禁用后套件照常绿。测试自身断言质量高(停摆监视 15s 判死、回包帧形、总量下界),但禁用即守卫为零。
   影响: 一个已知的活性死锁躺在主分支,唯一能抓到它的探针处于关断态;「测试过了」在这里恰好掩盖「问题还在」。对标 C# ConcurrentVaddToSpilledSetMakesProgress 在 C# 侧是常跑回归。
   建议: 立票修 src(量化 worker 非阻塞取锁 + 让出),修复后移除 ignore;修前该条应挂到已知缺陷台账而非仅注释。

二、弱(有效但精度不足/对位差;维持可用,建议补强)

3. test_expired_key_deletion_scan
   路径: wedb/wkv/tests/keyspace.rs:216
   问题: deleted == 1 精确,但 scanned 仅 assert!(scanned >= deleted) 下界。C# 对位 ExpiredKeyDeletionTests.cs:TestOnDemandExpiredKeyDeletionScan 双口径均精确: res[0] == expectedKeysToExpire 且 res[1] == TotalNumKeysToCreate(totalRecordsScanned 精确钉死)。
   影响: 扫描窗口口径(误扫邻库/漏扫本库)不受锁;环境受控下完全可以给精确值。
   建议: 仿 C# 钉 scanned 精确值。

4. test_run_once_purges_expired_keys
   路径: wedb/wkv/tests/gc.rs:45
   问题: 同款下界式: last_scan_scanned >= last_scan_deleted、total_scanned >= st.total_scanned,无精确计数。
   影响: 扫描预算/批口径回归(如多扫一轮、跨轮重复计)测不出;删除数与物理状态断言已闭环,故仍有效。
   建议: 首轮记录数可控,改 assert_eq。

5. txn_set_test 族(txn_set_test/txn_execute_test/txn_get_test/txn_get_set_test)
   路径: wedb/wnode/tests/transaction_tests.rs:34/78/108/142
   问题: 只锁状态机(Started→Running→None)、+QUEUED 计数与 *2 应答头;C# 对位 TransactionTests.cs:TxnSetTest 核心断言是 EXEC 后 StringGet 读回提交值(数据可见性)。rust 侧 manager 挂 None store,EXEC 根本无数据面。
   影响: 锁住队列/状态/帧形,未锁「事务真的把数据写进去了」;该面由 wedb/wnode/tests/transaction_session_test.rs:session_provider_multi_set_exec_pipelined 承接,故不立无效,但四个用例的名义(C# 同名)与实际验证面错位。
   建议: 维持,文件头加注「数据可见面见 transaction_session_test」防误读。

6. test_recover_non_existent_file_throws 与 test_memory_only_recover_from_non_existent_file_throws
   路径: wedb/wbftree/tests/interop/snapshot.rs:65/101
   问题: assert!(res.is_err()) 无变体甄别;同文件 test_recover_from_corrupt_snapshot_returns_err 已示范 matches!(Err(Error::Recovery(_))) 正确写法。
   影响: 任意错误(含与本语义无关的 IO 参数错)都过;精度低于同文件水位。
   建议: 改 matches! 锁 Recovery 变体。

7. test_cluster_boot_inprocess_smoke 与 test_cluster_bin_bare_boot_smoke
   路径: wedb/wedb/tests/cluster_boot_smoke.rs:23/60
   问题: 核心断言只有「600/800ms 后未崩」(!handle.is_finished() / try_wait().is_none()),无任何端口探活或 PING 闭环;SIGTERM 臂接受 exit 0 或 signal 15,停机是否真完成不区分。
   影响: 只证明「启动路径没有立即 panic」,gossip/集群装配是否活着不验证。冒烟定位下可接受,备注不立罪。
   建议: 加一次 loopback PING(端口 0 需回读实际端口)把「活着」变成行为断言。

8. advance_time_pulse_frame_resolves_to_cluster_advance_time
   路径: wedb/wedb/tests/advance_time_frame_roundtrip.rs:48
   问题: assert_ne!(cmd, Some(RespCommand::Invalid)) 紧跟 assert_eq!(cmd, Some(ClusterAdvanceTime)) 之后,逻辑恒真(eq 过则 ne 必过),纯冗余。
   影响: 零;整帧字节断言本身极强(注释明说防「前缀断言掩盖」),只是尾巴上挂了个死断言。
   建议: 删 assert_ne 行。

三、有效抽样纪要(31/40;挑代表性面记录锁点)

wkv
- test_keyspace_basic_counts / test_keyspace_expired_not_counted(keyspace.rs): 逐态精确元组 [(0,3,0)]→[(0,3,1)]→[(0,2,1)],多版本去重/墓碑屏蔽全锁。
- purge_with_event_sink_suppresses_writes_and_emits_purge(ttl_purge.rs): 事件恰好一次、四元组逐值、抑制标志零残留;判据刻意取 store.vdb 权威表并 assert_ne 恒等映射防自证(注释明说「杜绝自证」),是全仓防硬编码副本的范本。
- lazy_compaction_frees_garbage_and_advances_begin(compact/lazy_compaction.rs): 紧缩统计 scanned/live_copied/superseded/dead_dropped 四计数逐项 assert_eq + begin 地址精确推进 + 50 键逐一回读。
- test_expired_key_deletion_scan: 除条 3 外,物理 TTL 记录读回、跨库不越权、幂等重扫零删除全锁。

wnode
- expired_key_read_commands_notfound(ttl_fastpath_semantics.rs): GET/TTL/EXISTS 三命令逐字节帧($-1 / -2 / :0)加未过期对照组;锁的是修复点「快路径闭环而非降级」。
- getex_past_absolute_time_keeps_existing_ttl(getex_ttl_semantics.rs): 回归测试带修复前行为描述,kept > now+59s 界定「未被改动」,PERSIST/未来 EXAT 对照组齐。
- smove_missing_member_and_existing_destination_member(resp_set.rs): C# SetMove 语义两侧(SetOps.cs)逐值锁,目标已含成员不重复添加仍回 :1。
- test_upsert_rmw_delete_replay_loop / replica_checkpoint_end_marker_takes_local_checkpoint(aof_replay.rs): 重放计数精确、终值回读;副本臂锁检查点落盘(list_checkpoints==1)、wal 物理截断(begin 推进)、版本闸二次不打点——副作用三面全验。
- simple_hyper_log_log_add_count(hyperloglog.rs): 与 C# HyperLogLogTests 同断言(6 元素估计恰 6)。
- ri_aof_replay_converges_replica(service.rs): 主从四字段逐值一致 + 副本侧独立回读,对标 C# RIAofOnlyRecoveryTest 四项 RI.GET。
- ttl_purge_single_deterministic_entry(service.rs): 分面计数(DbMeta 3/用户域 2)、arg1 粗化值、副本映射继承(vns/route/next_virtual_id 三点)、幂等清除全锁。
- oversized_invalid_payload_copies_no_value(hll_alloc_probe.rs): 分配探针测试先做「探针自检」(窗口内注入 PAYLOAD 级分配必须被看见,否则断言无意义)——防探针失效导致恒过的自指防护,全仓独一份。
- registry_user_key_fails_loud_on_malformed_composite(vector_key_domain_ops.rs): 全仓唯一 should_panic,且带 expected = "不变量破坏"。should_panic 滥用: 零。

wcol
- main_loop_wakes_waiting_observer / collection_update_wakes_multiple_waiting_observers(collection_item_broker_tests.rs): 指派次序(obs1= item-a/obs2= item-b)、映射摘除逐 id 断言。
- zrandmember_distinct_sequence_matches_legacy_shuffle(random_member_sampling.rs): 属「硬编码副本」形态(参照物 legacy_inline_sample 是被删旧实现的内联副本),但为刻意的迁移序列钉死+互异属性断言+第三口径 shared_iterative_sample(对标 C# PickKRandomIndexesIteratively)三重互证,可接受;注意其含义是锁死行为不漂移,未来有意改采样算法须显式换参照。

whlog
- test_inplace_lifecycle / test_revivify_record_at_with_pad(inplace_lifecycle.rs): filler_bytes 精确值、失败臂(键不匹配/超容量)布尔、扫描逐 (addr,key) 元组、tail 地址精确不变。
- test_shift_begin_address_and_truncate: device.get_file_size(0)==0 物理副作用直验,非只看返回值。

waof
- test_commit_to_concurrent_waiters_consistency / test_commit_to_same_target_merges_into_single_leader(wal/concurrent_commit.rs): 唤醒位点 >= 目标 + 静默后 committed==flushed==tail + 扫描总数 16+8*20*4 精确;合并臂 committed_until == target+COMMIT_FRAME_TOTAL_LEN 逐字节位点。

wbftree
- test_snapshot_and_recover_round_trip / test_snapshot_and_recover_scan_after_restore(interop/snapshot.rs): 20 键逐键 Found+值;scan 臂 len==10 与 C# 对位(BfTreeInteropTests.cs:469 同为 Count==10)完全一致,不加罪。
- test_recover_from_corrupt_snapshot_returns_err: 四损坏形态全 matches!(Recovery 变体)。

wresp
- test_error_frame_single_point_sanitization(writer.rs): CRLF 注入切断/LF-only/非 UTF-8 保留/长度帽边界(1+N+2 逐字节)全锁。
- test_protocol_aware_lengths: Resp2/Resp3 全帧形逐字节。

wconn
- coalesced_reply_drain / gate_backpressure(network_tcp.rs): 合包滞留应答就地认领、闸 2 第 4 帧负断言(端点侧 !contains(k4))——负向行为用端点观测,非客户端自说自话。
- in_flight_timeout_fails_commands_and_marks_disconnected(client_timeout.rs): Error::Timeout 变体甄别 + 断连后即刻失败。

wedb
- bump_wait_does_not_block_session_registration / cluster_node_timeout_zero_sentinel_is_infinite(cluster_provider_epoch.rs): 并发注册与静止等待互不阻塞 + 60s/0=哨兵 None/1500ms 三态精确。
- test_single_key_slot_verify_migrating(cluster_slot_verify.rs): Moved/Ask 状态、slot、endpoint/port 四点精确。

四、样本外补查

- should_panic 全仓 tests/+src 只 1 处且带 expected(条 19);滥用: 无。
- assert!(x.is_ok()) 模式全仓 10 处,9 处后随状态/计数断言补强(range_index_stream_replay_tests.rs 的 4 处错误分支 assert!(res.is_err()) 无 ReplicationError 变体甄别,但临时文件清理与 pending 计数断言补位,备注级)。
- 自引用 expected(实现公式复制)主动检索: 除 random_member_sampling 刻意迁移钉死外零命中。
- 恒真断言补充 1 处: wedb/wtxn/tests/txn_lock_stress.rs:311 stress_manual_locks 的 assert!(prev < NUM_THREADS)——merge_plan 每桶去重、8 线程各至多 +1,prev 上限 7,断言结构恒真(排他侧 assert_eq!(prev, 0) 是真断言,仅共享侧这条是死重);建议删或改为记账对账(total 取放守恒)。
- ignore 面: 全仓 #[ignore] 共 3 处,其中 2 处即条 1/2(第三处 resp_command_parse.rs:767 为显式吞吐探针,合理)。

五、C# 对位断言对比结论

- rust 普遍强于 C# 同位测试: C# 大量 AssertEqualUpToExpectedLength 前缀匹配,rust 全帧逐字节;C# 只看返回值处,rust 常加物理副作用验证(文件尺寸、begin/tail 地址、检查点目录、next_virtual_id 水位)。
- 反向差距集中在计数口径: ExpiredKeyDeletionScan 的 totalRecordsScanned C# 精确 rust 下界(条 3/4);事务族数据可见面 C# 端到端 rust 拆到另一文件(条 5)。
- rust 独有的失守形态是 ignore 悬空(条 1/2): C# 侧无对应问题。

统计: 抽 40 / 无效 2 / 弱 7 / 有效 31
视角结论: 有增量
