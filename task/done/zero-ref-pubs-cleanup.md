# zero-ref-pubs-cleanup 各 crate 零引用 pub 项清理

来源：next/design.md 原条目 17、18（主代理已预清理条目）与 WithLengthHeader 裁决。
任务下达清单 66 项 + 测试孤儿 2 项，逐项 grep 核实（全仓 src+tests 出现次数 = 定义处）并对照 C# 定删/接线后，执行删除 63 项、保留 1 项（wmetric stop_and_switch）、wconf 2 项与 wbftree 1 项已被预清理。

验收口径
- ./clippy.sh 零警告（禁 allow）
- ./test.sh 全过
- bun ./js/check.js 无新增缺失
- 待删符号全仓 grep 零残留，mod 导出与 lib.rs re-export 同步收缩

## 删除清单（对标结论逐项）

wtxn
- txn_lock_table.rs lock_exclusive / lock_shared：自创便利包装，生产锁路径走 lock_stripe（txn_key_entry.rs）与 lock_key，测试亦用 lock_key
- transaction_manager.rs with_session_id：与 set_session_id 同体 builder 变体
- transaction_manager.rs reset_current：对标 C# TransactionManager.cs:209 无参 Reset()，C# 内部亦零调用（生产走 Reset(bool) 即 rust reset(is_running)）；ignore 登记
- transaction_manager.rs is_skipping_operations（测试孤儿）：C# 生产在用（RespServerSession.cs:507/:518 txnSkip），rust 批处理会话模型无 txnSkip 网络缓冲跳过分支；删函数，transaction_tests.rs txn_command_coverage 的 4 处断言改 state 字段直读（等价路径，C# 测试覆盖点由状态转移断言承接）；ignore 登记

wkv
- config.rs from_memory_budget：与 auto_with_budget 同体（都转调 from_memory_budget_with_keys(memory_bytes, None)），零引用重复面
- session/mod.rs try_read_in_memory_with_size：String-tag 专用薄包装零引用；底层 try_read_tag_in_memory_with_size 生产在用（ttl_sync.rs、raw/read.rs），MEMORY USAGE 走 network_memory_usage 自有链

wconf（pub_sub_page_size_bits / append_only_file_base_directory 已被主代理预清理，不在本批）
- server_config_type.rs ALL_MEMBERS：自创枚举全成员数组（替代 C# Enum 遍历的设想面），零引用
- config_name_comparer.rs hash_code：C# GetHashCode 对标；rust 配置名匹配走 equals 线性比较，无哈希表消费面；ignore 登记
- config_name_comparer.rs to_upper_ascii：equals 已用 eq_ignore_ascii_case 内联，私有辅助对标无消费

wresp
- cmd_strings.rs GENERIC_ERR_COMMAND_DISALLOWED_WITH_OPTION / GENERIC_ERR_UNKNOWN_SUB_COMMAND_OR_WRONG_NUM_ARGS / RESP_ERR_GENERIC_AT_LEAST_ONE_KEY / RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER_NO_PERIOD：C# 侧同为零调用死常量（grep 全 garnet 仅 CmdStrings.cs 定义处），1:1 转写带入；生产文案已由 abort_with_wrong_number_of_arguments / abort_with_unknown_subcommand 等函数形态承接
- cmd_strings.rs GENERIC_ERR_UNSUPPORTED_OPTION：C# 在用（KeyAdminCommands.cs:390/:397），rust 已由 abort_with_unsupported_option 函数承接同文案（wnode expire/GETEX 族调用），常量为冗余第二形态
- resp_memory_writer.rs with_capacity_p：与 new_p 同体容量变体，零引用
- resp_memory_writer.rs write_resp2_null_array：泛型 write_null_array 的固定 RESP2 变体，零引用
- read.rs WithLengthHeader 族整族删除：try_read_i32_with_length_header / try_read_i64_with_length_header / try_read_u64_with_length_header + try_read_i32 + try_read_i64（后两者仅被前二者内部调用，删族后成死链）；C# TryReadInt64WithLengthHeader/TryReadUInt64WithLengthHeader 零生产调用，TryReadInt32WithLengthHeader 仅被 RespReadResponseUtils.cs:254 转发（rust wconn 客户端自有解析，无该消费面）；rust 命令参数解析走 session_parse_state strict_i32/strict_i64 承接；裁决见 task/reject/with-length-header-family.md
- read.rs get_serialized_record_span：C# 生产在用（RespClusterMigrateCommands.cs:131/:215、RespClusterReplicationCommands.cs:565），消费链为 MIGRATE SLOTS 变体与 SEND_CKPT 检查点流（next/glm.md 条 8 / ds.net.md 条 1，均未立项，非只差一环）；删 + ignore 登记待立项随链实现

wpubsub
- session_commands.rs set_num_active_channels：C# PubSubCommands 用局部变量计数，无 session setter 对标；num_active_channels getter 保留（生产在用）
- session_commands.rs drain_mailbox_to：与 drain_mailbox_into 重复变体（排入外部缓冲版），零引用

whyperlog
- lib.rs HllValid：自创 bitflags 辅助（注释自认），零引用
- lib.rs dense_count_non_zero：注释自认 C# 内零调用点的预防性复刻（防后续接入偏差），属占位形态，删除；ignore 登记 HyperLogLog.cs:DenseCountNonZero（理由：C# 亦零调用）

wbftree
- stub.rs clear_tree_handle：C# ClearTreeHandle 生产在用（GarnetRecordTriggers.cs:230、RMWMethods.cs:1532），rust 搬迁自愈走 compact.rs PostCopyToTail 自愈（pre_stage_and_register_pending + set_flushed）与 checkpoint.rs mark_recovered_from_checkpoint（自带句柄清零），rebind_stub 承接在线激活；ignore 登记

wepoch
- participant.rs user_word_atomic：零引用访问器，私有 user_word_ref 内部承接（set/get 在用）

windex
- ram/direct_vm.rs slice_mut：零引用；check_range 保留（as_mut 系方法在用）

wcol（C# 形状读写器面，生产走信封轨遗留）
- itembroker/collection_item_broker.rs move_collection_item_async：C# 生产在用（ListCommands.cs:375 BLMOVE），rust BLMOVE 走 park_broker_wait 登记 + 主循环 try_move_next_list_item 即时移动，等价语义分置两处；ignore 登记 CollectionItemBroker.cs:MoveCollectionItemAsync
- list/list_object.rs to_items、zset/sorted_set_object.rs to_entries：自创物化面（C# 无 ToItems/ToEntries），SCAN/LRANGE/ZRANGE 走各自回调路径
- resp/input.rs set_expired_flag / set_set_get_flag / check_expiry / check_set_get_flag：C# InputHeader 标志机制服务字符串域 RMW 头；rust 对象链信封轨（object_store_utils 只用 new_with_type + empty flags，TTL 在 wkv 层）；ignore 登记 InputHeader.cs 四函数
- resp/output.rs has_remove_key / take_payload：C# 无同名方法（字段直读形态），rust payload pub 字段与删空自愈内联承接

wlua
- limited_allocator.rs get_next_free_block_ref / get_prev_free_block_ref / get_ref_val / contains_ref / is_valid_block_ref / move_to_head_of_free_list / try_coalesce_single_block / get_data_start_ref / update_debug_allocated_bytes：C# LuaLimitedManagedAllocator 遍历/诊断 API 的对标，rust 分配路径已内联承接（合并循环直调 coalesce_pair、debug_allocated_bytes 字段在 alloc/free 直接读写）；ignore 登记九函数
- runner.rs reset_compilation：C# LuaRunner.cs:490 全仓亦零调用；ignore 登记
- runner.rs host_mut：自创访问器零引用，runner 内部直访 self.host

wvector
- store.rs exists_iid：零引用（exists_wid 承接同语义，fsm.rs 在用）
- store.rs read_varsize_bytes / read_varsize_id：零引用（read_varsize_iid 生产在用）
- store.rs make_physical_key（测试孤儿）：删；wedb_standalone/tests/vector_element_key.rs、resp_vector_set.rs 改用生产在用的 namespace_bytes 拼装（同 1:1 语义）
- service.rs into_overflows：与 into_search_output 重复（后者在用）
- service.rs continue_search：C# DiskANNService.cs:ContinueSearch 本身 NotImplementedException，rust 恒 Err 占位 + 零引用，删除；ignore 登记

wmetric（先核 next/glm.md 条 7 指标接线：其对标含 RespServerSession.cs:587-598 段，会复活延迟记录面；不涉及 monitor getter 与 multi 查询面）
- latency/garnet_latency_metrics_session.rs stop_and_switch：保留。C# 生产在用（RespServerSession.cs:591，containsSlowCommand 分支），glm 条 7 接线会复活，不删
- garnet_server_monitor.rs shared_iterations：删。monitor_iterations 字段 pub，会话构造（glm 条 7 接线时）直接 clone 字段即可，getter 非必需
- latency/garnet_latency_metrics.rs get_latency_metrics_multi：删。C# 唯一调用者 MetricsApi.cs:95-99 无业务消费；rust metrics_api.rs get_latency_metrics_all 已用循环单类别 get_latency_metrics 承接同面；ignore 登记多类别重载

wbase（DEFAULT_BUS_PORT_OFFSET 排除：集群 bus 端口待办认领，不动）
- align.rs is_cacheline_aligned / align_to_cacheline：零引用 const 包装，is_aligned/align_up 原语与 CachePadded 保留（striped.rs 在用）
- pool/limited.rs max_send_buffer_content_size：零引用；SEND_BUFFER_OVERHEAD_RESERVE 常量仅此一处消费，连带删除

wrecord
- header.rs set_key_len：零引用 setter；key_len 读面与位段常量保留

waof
- header.rs OBJECT_ID_OFFSET：零引用常量，to_bytes 硬编码偏移已承体检视

## 保留清单

- wmetric stop_and_switch（glm 条 7 复活，见上）
- wbase DEFAULT_BUS_PORT_OFFSET（集群 bus 端口待办认领）
- read.rs 其余 WithLengthHeader 成员（try_skip_byte_array / try_slice / try_read_byte_array / try_read_bool / try_read_span / try_read_string / try_read_ptr_with_signed / try_read_string_response / try_read_string_array / try_read_double / try_read_ptr_with_length_header）：wconn 客户端应答解析与 wresp 测试在用
- RespInputFlags 位常量与 flags 参数：C# RespInputFlags 线格式形状面，empty() 在用

## js/check/ignore 登记项（删后跑 check.js 按实际缺失落）

libs/common/RespReadUtils.cs：TryReadInt32 / TryReadInt64 / TryReadInt32WithLengthHeader / TryReadInt64WithLengthHeader / TryReadUInt64WithLengthHeader / GetSerializedRecordSpan
libs/server/Transaction/TransactionManager.cs：Reset（无参重载）/ IsSkippingOperations
libs/server/Config/ConfigNameComparer.cs：GetHashCode
libs/server/Resp/HyperLogLog/HyperLogLog.cs：DenseCountNonZero
libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs：ClearTreeHandle
libs/server/Objects/ItemBroker/CollectionItemBroker.cs：MoveCollectionItemAsync
libs/server/InputHeader.cs：SetExpiredFlag / SetSetGetFlag / CheckExpiry / CheckSetGetFlag
libs/server/Lua/LuaLimitedManagedAllocator.cs：GetNextFreeBlockRef / GetPrevFreeBlockRef / GetRefVal / ContainsRef / IsValidBlockRef / MoveToHeadOfFreeList / TryCoalesceSingleBlock / GetDataStartRef / UpdateDebugAllocatedBytes
libs/server/Lua/LuaRunner.cs：ResetCompilation
diskann-garnet/DiskANNService.cs：ContinueSearch
libs/server/Metrics/Latency/GarnetLatencyMetrics.cs：GetLatencyMetrics 多类别重载

C# 死常量（CmdStrings.cs 五项中零调用的四个）与 C# 无对标的自创符号无 ignore 负担，以 check.js 实际输出为准修正上述清单。

## 产出文档

- task/reject/with-length-header-family.md：ds.net.md 原 20 条与 net.md 原 16 条按「整族删除」关闭
- task/reject/zero-ref-pubs-cleanup.md：拒绝项（若有）
- 完成后本文移 task/done/ 并追加验证结果

## 分批作业

按 crate 依赖自底向上：wbase/wrecord/waof/wconf → wtxn/wkv/whyperlog/wbftree/wepoch/windex → wresp → wcol/wpubsub/wlua/wvector/wmetric → wedb_standalone/tests 适配 → ignore 登记 → 全量验收。

## 执行结果

- 16 crate 共 35 文件 +77/-743 行，提交 26956c9，合并 dev 后 096d9da 合入主目录
- 删除清单按上文逐项落地，连带清理：
  - wcol input.rs 删 now_ticks 导入、output.rs 删 mem::take 导入
  - whyperlog 删 HllValid 后 cargo remove bitflags 依赖
  - wvector store.rs 删 exists_iid 后连带私有 read_multi_bool、删 make_physical_key 后连带 SmallVec 导入
- 保留：wmetric stop_and_switch（glm 条 7 复活）、wbase DEFAULT_BUS_PORT_OFFSET（bus 端口待办）、read.rs 其余 WithLengthHeader 成员（wconn 与测试在用）、RespInputFlags 位常量（C# 线格式形状面）
- 测试适配：wedb_standalone/tests/vector_element_key.rs、resp_vector_set.rs 增本地 physical_key 助手（生产构件 namespace_bytes 拼装，原 make_physical_key 删除）；transaction_tests.rs txn_command_coverage 改 state 字段直读
- js/check/ignore 登记：server.yml（InputHeader.cs 加 CheckSetGetFlag、LuaRunner.cs:ResetCompilation、LuaLimitedManagedAllocator.cs 九函数、CollectionItemBroker.cs 加 MoveCollectionItemAsync、DiskANNService.cs 加 ContinueSearch）、common.yml 新增 RespReadUtils.cs 四函数（TryReadInt32/TryReadInt64/TryReadInt64WithLengthHeader/TryReadUInt64WithLengthHeader）
- check.js 实际新增缺失即上述 15 函数；TransactionManager.Reset/IsSkippingOperations、ConfigNameComparer.GetHashCode、HyperLogLog.DenseCountNonZero、RangeIndexManager.Index.ClearTreeHandle、GarnetLatencyMetrics 多类别重载因既有 ignore、全局同名映射覆盖或 C# 侧非方法声明未进 miss，无需登记

## 验证结果

- bun ./js/check.js：退出 0，无输出，check/miss 空（零缺失零重复）
- ./clippy.sh：3 任务全过，-D warnings 零警告（一次 E0460 moon 共享缓存元数据冲突，重跑即消，非代码问题）
- ./test.sh：wedb 1992 passed + 1 skipped，regress 2 passed（分支基线、merge dev 后、主目录合并态三处复验）
- 合并：分支先 merge dev（自动合并无冲突，复验通过）后合入主目录 dev，worktree 与分支已清理

状态
- 完成

