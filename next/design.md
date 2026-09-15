# design 待办

1. [P1] 命令元数据双源：手写 ACL 目录表 vs 内嵌 JSON
   位置：wresp/src/catalog/data.rs:1（2494 行手写表，消费方 wacl/src/acl_parser.rs、wacl/src/user.rs、wnode/src/resp/resp_commands_info.rs:15 LAST_VALID_COMMAND）
   对标：garnet/libs/server/Resp/RespCommandsInfo.json（wnode/src/resp/resp_commands_info.rs:30 include_str 内嵌，6969 行）
   问题：JSON 与手写表两份真值并存，命令增删需双改，漂移即 ACL 判定与 INFO 输出不一致
   改法：JSON 移入 wresp（或新建 wresources 对齐 C# libs/resources），catalog 改 build.rs 生成或 OnceLock 解析同一份 JSON

2. [P1] NullDevice 配置断头
   位置：wconf/src/runtime_server_options.rs:80 use_aof_null_device、wconf/src/garnet_options.rs:550-553（校验）、wdev/src/null.rs:21 NullDevice
   对标：garnet/libs/storage/Tsavorite/cs/src/core/Device/NullDevice.cs（UseAofNullDevice）
   问题：选项、设备、校验齐全，但 wnode/src 对 NullDevice/aof_null 零命中，装配点不消费，AOF 无设备能力空转
   改法：在 wnode AOF 装配点按选项接 NullDevice，或整链删除（选项 + 校验 + wdev/src/null.rs）

3. [P1] wnode 越层直依赖 wbftree，RangeIndex 双包装
   位置：wnode/src/aof/aof_processor.rs、wnode/src/service.rs、wnode/src/resp/rangeindex/ 8 文件（range_index_chunked_deserializer.rs、range_index_chunked_serializer.rs、range_index_manager_index.rs、range_index_manager_locking.rs、range_index_manager_migration.rs、range_index_manager_replication.rs、range_index_migration_reader.rs、resp_server_session_range_index.rs）
   问题：wnode 绕过 wkv 引擎门面直用 wbftree，层次依赖倒挂
   改法：门控并入 wkv::range_index 暴露面，wnode 仅经 wkv 消费

4. [P1] wedb 集群域零引用孤儿 21 项
   位置：wedb/src/
   对标：garnet/libs/cluster/Server/ClusterManager.cs、ClusterConfig.cs、ClusterProvider.cs、ClusterManagerWorkerState.cs、HashSlot.cs
   问题：C# 形状 API 落地后未接线，全仓（src+tests）引用计数均为 0
   改法：逐个甄别——gossip/failover/bus 端口等有集群语义缺口的补链（需产品决策），纯孤儿删；禁止维持「接口在、无人调」
   - wedb/src/server/gossip/gossip_session.rs:26 handle_gossip（gossip 合并真身在 gossip_manager.rs，入口孤儿）
   - wedb/src/server/cluster_manager.rs:93 init_local（生产/测试均走 try_initialize_local_worker）
   - wedb/src/server/cluster_provider.rs:473 set_seq_reset_hook（钩子机制在，无人注入）
   - wedb/src/server/cluster_manager.rs:73 unsafe_set_config
   - wedb/src/server/cluster_config.rs:378 is_migrating_slot（slot_verify 等处内联重写，收敛到此或删）
   - wedb/src/server/cluster_config.rs:540 get_worker_info_for_gossip
   - wedb/src/server/cluster_config.rs:551 get_slot_count_for_state
   - wedb/src/server/cluster_manager_worker_state.rs:137 list_replicas
   - wedb/src/args.rs:44 cluster_bus_port（bus 端口语义未实现，连带 wbase/src/hash_slot.rs:36 DEFAULT_BUS_PORT_OFFSET）
   - wedb/src/args.rs:51 cluster_config_path
   - wedb/src/server/cluster_config.rs:194 local_node_endpoint
   - wedb/src/server/cluster_config.rs:211 get_local_node_replica_ids
   - wedb/src/server/cluster_config.rs:307 get_remote_node_ids
   - wedb/src/server/cluster_config.rs:359 get_host_name_from_node_id
   - wedb/src/server/cluster_config.rs:520 get_replica_endpoints
   - wedb/src/server/cluster_config.rs:582 get_worker_node_id_from_address_or_hostname
   - wedb/src/server/cluster_session.rs:194 set_replicating
   - wedb/src/server/failover/failover_session.rs:151 failover_timeout_reached（故障转移超时判定，C# 在用，优先补链）
   - wedb/src/server/replication/replication_manager.rs:205 get_sublog_replication_offset
   - wedb/src/server/replication/replica_replay_driver.rs:68 set_replayed_offset
   - wedb/src/server/replication/aof_sync_driver_store.rs:335 assert_does_not_exist

5. [P1] wnode 基础面 C# 形状孤儿约 33 项
   位置：wnode/src/resp/、wnode/src/aof/、wnode/src/
   对标：garnet/libs/server/Resp/BasicCommands.cs、RespServerSession.cs、garnet/libs/server/AOF/Recover/RecoverLogDriver.cs、RecoverReplayTask.cs
   问题：C# 形状方法落地后未被分派接线，全仓（src+tests）引用计数均为 0；READONLY/READWRITE/QUIT 在 C# 集群场景有生产分派
   改法：逐符号甄别二选一——C# 有生产调用链的（READONLY/READWRITE/QUIT、commit_aof、并行恢复任务）补接线打通分派，其余删；甄别时优先核 wedb/src/server/cluster_session.rs 分派表
   - resp 命令面：wnode/src/resp/basic_commands.rs:451 network_get_async、:465 network_get_sg、:1049 network_quit、:1104 network_readonly、:1110 network_readwrite、:1718 parse_get_and_key、:1730 next_command_maybe_get、:1734 try_get_simple_command_info、wnode/src/resp/array_commands.rs:429 network_array_ping、wnode/src/resp/admin_commands.rs:144 commit_aof_async、wnode/src/resp/resp_server_session.rs:1495 network_custom_raw_string_cmd
   - 会话/输出面：wnode/src/resp/resp_server_session.rs:464 set_global_latency_metrics、:1875 debug_send、:2001 create_consistent_read_api、:2040 get_object_output、:2045 get_unified_output、:2070 abort_with_wrong_num_args_or_unknown_subcommand、wnode/src/resp/metrics_commands.rs:137 new_global_latency_metrics、wnode/src/resp/resp_server_session_output.rs:30 with_protocol_writer、:44 process_output、wnode/src/resp/parser/session_parse_state.rs:42 initialize_with_arguments、wnode/src/role_info.rs:27 to_metrics_string、wnode/src/shutdown.rs:80 wait_stopped
   - RangeIndex 复制状态面：wnode/src/resp/rangeindex/range_index_manager_replication.rs:209 pending_stream_reassembly_count、:601 dispose_incomplete_stream_reassembly

6. [P1] wdatabase 面零引用孤儿 10 项
   位置：wedb/wdatabase/src/
   对标：garnet/libs/server/GarnetDatabase.cs（checkpoint/AOF 尺寸策略族）
   问题：C# 形状 checkpoint/锁/统计门面零调用，全仓（src+tests）引用计数均为 0
   改法：逐个甄别——checkpoint 策略族（recover_checkpoint、take_on_demand_checkpoint、task_checkpoint_based_on_aof_size_limit、commit_to_aof）若 C# 生产在用则补接线到 wnode/wedb 装配点，纯孤儿删
   - wdatabase/src/single_database_manager.rs:48 recover_checkpoint
   - wdatabase/src/single_database_manager.rs:86 take_on_demand_checkpoint
   - wdatabase/src/single_database_manager.rs:96 task_checkpoint_based_on_aof_size_limit
   - wdatabase/src/single_database_manager.rs:104 commit_to_aof
   - wdatabase/src/single_database_manager.rs:141 grow_indexes_if_needed
   - wdatabase/src/multi_database_manager.rs:100 try_get_databases_content_write_lock
   - wdatabase/src/multi_database_manager.rs:109 try_get_databases_content_read_lock
   - wdatabase/src/multi_database_manager.rs:163 run_paused_checkpoints_and_release_locks
   - wdatabase/src/database_manager_base.rs:355 get_database_keyspace_stats
   - wdatabase/src/cache_size_tracker.rs:39 add_read_cache_heap_size
8. [P2] SET/SETEX 两跳 AOF 写放大
   位置：wnode/src/service.rs:210（StoreEvent::TtlWrite 汇聚为独立 Pexpireat/Persist 条目，值条目另发）、wnode/src/resp/key_admin_commands.rs:192（SET EX 命令端先算 expire_at_ticks 再写值）
   对标：garnet/libs/server/Resp/BasicCommands.cs:533 NetworkSETEX（input = RespCommand.SETEX，expiry 编入 valMetadata，单条 AOF）
   问题：rust 侧 SET EX 产生值条目 + 过期条目两条 AOF，高频场景写放大翻倍，偏离 C# 单条形态
   改法：值条目随行 expiration（对齐 C# SETEX valMetadata 单条），TtlWrite 事件仅服务独立 EXPIRE/PEXPIRE 命令

9. [P2] 时间戳小面三点散落
   位置：wnode/src/resp/metrics_commands.rs:26 now_stopwatch_ticks、wnode/src/resp/rangeindex/range_index_replication_activities.rs:13 now_ns、wedb/src/server/failover/failover_session.rs:9 直连 coarsetime::Instant
   问题：三处各自手写 coarsetime 换算，易出第四处手写换算率
   改法：wbase::time 一处定义（含 monotonic now），三点同调

10. [P2] 自定义对象命令双执行器
   位置：wnode/src/resp/objects/custom_object_commands.rs:35 try_custom_object_command（同步）vs wnode/src/resp/garnet_api.rs:755 custom_object_slow（异步）
   对标：garnet/libs/server/Custom/CustomRespCommands.cs:TryCustomObjectCommand
   问题：同一四接口分派（装载/NeedInitialUpdate/Updater/Reader/NotFound）两份实现，收敛易丢分支
   改法：抽共用执行器把「装载/回写/删除」原语参数化；对齐 custom_object_slow 的「信封未命中反探 String 域」WRONGTYPE 判定与同步 obj_load_typed_sync 语义

11. [P2] 两个 main 的 (recover, aof) 四路分派重复
   位置：wedb/src/main.rs:99 与 wedb_standalone/src/main.rs:87（同构 match，约 20 行逐字相同）
   问题：同一恢复装配逻辑双份维护
   改法：wnode 提供 open_from_args(args, session_factory) 便利构造，两 main 各一行

13. [P2] 配置文件入口断头
   位置：wconf/src/node_options.rs:222 from_args、:227 from_nested_text_str、:232 from_file（外部全仓零调用，from_file 内部转 from_nested_text_str）
   问题：SKILL 指定 nested_text 为配置格式，但两 main 直接 clap derive，配置文件能力未接
   改法：main 增 --config 分支接线，或明确放弃并删三个函数

14. [P2] expire 换算饱和算术 5 处散落
   位置：wnode/src/resp/key_admin_commands.rs:74-80（EX/PEXPIREAT/EXAT/PXAT 换算与 (i64::MAX - UNIX_EPOCH_TICKS) 上限钳制）、:192（SET EX 同构换算）、wnode/src/aof/aof_processor.rs:1217、:1238、:1253、:1276（重放端同构）
   对标：garnet/libs/common/ConvertUtils.cs（TickConverter 集中换算）
   问题：「秒 → 绝对 Ticks」与溢出钳制公式两端各自手写，公式漂移即重放与原命令不等价，正确性敏感
   改法：wbase::convert（或 wval::ttl 与 TtlCodec 同居）增 expire_after_to_ticks(now_ticks, seconds) 与 absolute_seconds_cap()，两端同调

15. [P2] 跨文件重复错误文案 5 组
   位置：wnode/src/session_parse_state_extensions.rs:55/58/60/61 与 wnode/src/resp/objects/sorted_set_geo_commands.rs:94/95/97/100（GEO 校验文案族两份：ERR radius cannot be negative、ERR need numeric width、ERR height or width cannot be negative、ERR COUNT must be > 0）；wnode/src/resp/garnet_api.rs:79 与 wedb/src/server/cluster_session.rs:1795（ERR slow path storage error）
   对标：garnet/libs/server/Resp/CmdStrings.cs 单点
   问题：同一文案多处 const 定义，改一处漏一处
   改法：GEO 族抽 wnode 域内单点模块，slow path 收归 wresp 或 wbase 常量（wext_json 已删，write-only/read-only 双份已消失）

16. [P2] src 内嵌跨 crate 集成测试迁 tests
   位置：wnode/src/resp/resp_server_session.rs:2725（tests 模块约 1270 行、62 个 #[test]，use wacl/wpubsub/wtxn/waof 全家装配）；同型：wnode/src/session_parse_state_extensions.rs（15 test）、wnode/src/resp/parser/resp_command.rs（13）、wnode/src/aof/garnet_log.rs（11）、wlua/src/lib.rs（12）
   对标：garnet/libs/server/tests/（集成测试独立程序集）
   问题：跨 crate 装配会话/ACL/PubSub/事务的测试占 src 编译单元（全量重编 + 二进制膨胀），与 wnode/tests/ 双轨
   改法：需装配多 crate 运行时或真实存储设备的迁 wnode/tests/（首批 resp_server_session.rs 62 个）；纯算法/纯解析留 src

17. [P2] 删除各 crate 零引用 pub 项 66 项
   位置：见下（判定口径：全仓 src+tests 出现次数 = 定义处，逐符号复核于当前工作树）
   问题：C# 形状接口落地后无人调用的死面
   改法：逐项对标 C#——C# 生产在用的优先补接线，无对应或语义已等价实现的直接删；删净后 cargo check + js/check.js 双零验收
   - wtxn：txn_lock_table.rs:114 lock_exclusive、:119 lock_shared（生产锁路径走 lock_stripe，wtxn/src/txn_key_entry.rs:167）、transaction_manager.rs:329 with_session_id、:342 reset_current
   - wkv：config.rs:212 from_memory_budget、session/mod.rs:235 try_read_in_memory_with_size
   - wconf：server_options.rs:159 pub_sub_page_size_bits、server_config_type.rs:64 ALL_MEMBERS、config_name_comparer.rs:20 hash_code、:34 to_upper_ascii、garnet_options.rs:252 append_only_file_base_directory
   - wresp：cmd_strings.rs:170 GENERIC_ERR_COMMAND_DISALLOWED_WITH_OPTION、:167 GENERIC_ERR_UNKNOWN_SUB_COMMAND_OR_WRONG_NUM_ARGS、:161 GENERIC_ERR_UNSUPPORTED_OPTION、:153 RESP_ERR_GENERIC_AT_LEAST_ONE_KEY、:68 RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER_NO_PERIOD、read.rs:135 try_read_i64、:171 try_read_i32、:367 try_read_i32_with_length_header、:399 try_read_i64_with_length_header、:431 try_read_u64_with_length_header（try_read_i32/i64 仅被死链 with_length_header 内部调用）、:862 get_serialized_record_span、resp_memory_writer.rs:310 with_capacity_p、:603 write_resp2_null_array
   - wpubsub：session_commands.rs:104 set_num_active_channels、:120 drain_mailbox_to
   - whyperlog：lib.rs:49 HllValid（bitflags 类型）、:1044 dense_count_non_zero
   - wbftree：stub.rs:155 clear_tree_handle、service/snapshot.rs:67 cpr_snapshot_by_ptr
   - wepoch：participant.rs:187 user_word_atomic
   - windex：ram/direct_vm.rs:124 slice_mut
   - wcol：itembroker/collection_item_broker.rs:320 move_collection_item_async（C# CollectionItemBroker.MoveAsync）、list/list_object.rs:227 to_items、zset/sorted_set_object.rs:391 to_entries、resp/input.rs:62 set_expired_flag、:67 set_set_get_flag、:72 check_expiry、:83 check_set_get_flag、resp/output.rs:61 has_remove_key、:67 take_payload（wcol 对象 C# 形状读写器面，生产走信封轨后遗留）
   - wlua：limited_allocator.rs:167 get_next_free_block_ref、:173 get_prev_free_block_ref、:193 get_ref_val、:315 contains_ref、:322 is_valid_block_ref、:391 move_to_head_of_free_list、:399 try_coalesce_single_block、:430 get_data_start_ref、:469 update_debug_allocated_bytes、runner.rs:788 reset_compilation、:1686 host_mut
   - wvector：store.rs:279 exists_iid、:334 read_varsize_bytes、:343 read_varsize_id、service.rs:111 into_overflows、:1013 continue_search
   - wmetric：garnet_server_monitor.rs:224 shared_iterations、latency/garnet_latency_metrics_session.rs:106 stop_and_switch、latency/garnet_latency_metrics.rs:195 get_latency_metrics_multi
   - wbase：align.rs:112 is_cacheline_aligned、:118 align_to_cacheline、pool/limited.rs:278 max_send_buffer_content_size、hash_slot.rs:36 DEFAULT_BUS_PORT_OFFSET（连带条目 4 的 cluster_bus_port）
   - wrecord：header.rs:504 set_key_len
   - waof：header.rs:584 OBJECT_ID_OFFSET

18. [P2] 测试孤儿 2 项（生产死、tests 在用）
   位置：wvector/src/store.rs:99 make_physical_key（wedb_standalone/tests 9 处在用）、wtxn/src/transaction_manager.rs:451 is_skipping_operations（wedb_standalone/tests 4 处在用）
   问题：生产链零引用，仅测试自产自销
   改法：接生产链或删函数 + 测试改用生产等价路径


20. 监听端口 dyn 消除终局（三端口处理记录）
   - VersionShiftFn：调用点组合消除。wkv CheckpointManager 不再持 hooks 槽（生产本就零接线，fire 恒 no-op），版本切换通知上提为 ClusterProvider::notify_version_shift_start/end 两个显式方法，检查点发起方在快照前后调用——注意生产接线仍待 AOF 门控复制面完工时补挂（对标 C# checkpointVersionShiftStart/End 委托的 rust 形态）
   - ReplicationSinkFn：信号化拉取消除。WalLog 环形缓冲本身是无锁 MPSC（帧已在共享容器），推流端口降级为容量 1 唤醒信号（crossfire MAsyncTx<Array<()>>，满即折叠）；AofReplicationPump::attach_wake 注册信号 + spawn 增量拉取循环（pump_backlog 从各副本已发位点扫至 safe_tail_address，序=环形缓冲线性化序，与原同栈直推的复制序保证等价）；sync_backlog 的 until 面从 committed 改 safe_tail（与原直推可见面一致，副本提前拿未 commit 帧、位点后 ACK 的语义不变）
   - StoreEventSink：保留 Arc<dyn Fn>（事件环拉取方案否决：借用事件 owned 化违反零拷贝 + 线性化保证退化，详见 wkv/src/store/event.rs 注释）
   - 定理：底层定义端口、上层注入捕获上层类型闭包的场景，dyn 是 Rust 的不动点；能消除的只有「回调可上提为调用者显式步骤」（组合）与「数据已在共享容器、回调可降级为信号」（拉取）两类
