优先级：低
分拣注记（qw.design 第 11 轮条 8 拆出；浅核 2026-09-19：24 口符号逐个 grep 全部在场（个别行号漂移：with_revivifiable_fraction :477、live_index_count :539、set_client_lib_info :2375、is_exhausted :141、evaluate_key_gate :194、attach_monitor :876）；台账查重仅三处子项联动须落地时注意：from_der 子项已由 done/inbound-tls-client-cert-auth.md 裁定为测试/自签装配入口并扩参保留，落地时按该裁定处置勿盲删；with_revivifiable_fraction 子项与 next/qw.db.md 条 1（复活池配置面恒关）重叠，本批落地时跳过该子项由该票承接；attach_monitor 子项与 ing/pending-lat-no-timing-site.md 修法（经 attach_monitor 同点重挂）联动，先核该票进度再判接线或删）

零生产消费者 pub 面增量批（新出 24 口，读者全在 tests/ 或同文件 cfg(test) 区；
本轮普查口径见末尾，落地时逐条按其文档锚点判「接线」或「删/cfg」，禁留中间态）
问题（rust 侧事实登记，全仓生产视图引用数 = 定义数）：
wnode/src/rangeindex/range_index_manager_replication.rs:106 const STREAMED_PUBLISH_LOG_ARG、
:178 set_aof_stream_chunk_size、:218 replicate_range_index_del、:442 pending_stream_reassembly_count
（四口同属流式重发域，读者仅 wnode/tests/range_index_replication.rs、range_index_stream_replay_tests.rs）；
wnode/src/aof/aof_backpressure.rs:229 set_counter_log、:237 get_shipped_watermark；
wnode/src/aof/garnet_log/single_log_branch.rs:46 backpressure_wait_vector_async、:252 chunk_buffer_size；
wnode/src/aof/aof_chunked_record_reader.rs:133 get_value_sequence（同文件 :474 消费在 cfg(test) 内）；
wnode/src/aof/readconsistency/replica_read_session_context.rs:374 replica_context_snapshot；
wnode/src/primary_tasks.rs:129 object_collect_running；wnode/src/resp/resp_server_session.rs:872 attach_monitor、
:893 reset_all_latency_metrics、:2351 set_client_lib_info；wnode/src/tls/config.rs:51 from_der；
whlog/src/address.rs:189 validate_invariants；wkv/src/config.rs:497 with_revivifiable_fraction；
wkv/src/session/consistent_read.rs:279 upsert_forbidden、:285 rmw_forbidden、:291 delete_forbidden
（三口同体、只差名字，且 rust 读会话在类型面上已无写能力，属 C# trait 形状残留）；
wval/src/meta.rs:242 write_to_slice（to_bytes 的第二出口，生产直用 to_bytes）；
wbftree/src/manager/mod.rs:531 live_index_count；wcol/src/object_payload.rs:68 obj_encode（obj_decode
生产在产、编码侧只喂用例，另两形态 obj_encode_into/obj_encode_custom_into 有产消）；
wlua/src/runner/executor.rs:149 run_for_runner；wmetric/src/command_stats.rs:77 get_entry；
wresp/src/resp_memory_writer.rs:373 new_p（与批四 writer_p 同族：泛型再转一手的第三形态）、
wresp/src/read.rs:189 try_skip_byte_array_with_length_header、:470 try_read_ptr_with_length_header、
wresp/src/session_parse_state.rs:55 initialize_with_args；
wbase/src/pool/aligned_buf.rs:182 as_allocated_slice；
wedb/src/server/cluster_config/mod.rs:447 get_replica_endpoints、
wedb/src/server/failover/failover_manager.rs:233 wait_failover_done、
wedb/src/server/replication/aof_sync_driver.rs:157 get_task、
wedb/src/server/cluster_manager_slot_gate.rs:124 is_exhausted、:177 evaluate_key_gate
c#：逐条以各函数文档注释锚点为准（本批只立死码事实，避免与批一批五在册符号混判）；
已核两处：garnet/libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs:171+（Upsert/RMW/Delete
为 IFunctions 形参位实现，rust 无对应 trait 位故三口悬空）；garnet/libs/server/Resp/RespServerSession.cs:WriteDirectLarge
（wnode/src/resp/resp_server_session.rs:2263 同名薄壳亦零消费者，第 13 轮 glm.design 已判其形态分叉不立条，此处仅登记不裁）

普查口径（摘自源审查）：零生产消费者 pub 面普查（脚本 /tmp/qw11/zc3.py）：1003 个 *.rs；生产视图按大括号配平剥离
同文件 #[cfg(test)] mod 与跨文件 #[cfg(test)] mod x; 声明，wtest_base/wedb_test/wnode_test/wtxn_test
与全部 tests/ 计入消费侧索引；抽 pub fn/const/static/type/trait/struct/enum/mod 定义并排除
struct/enum 字段位，得定义点索引；生产视图引用数 <= 定义数者命中 177 条，扣批一至批五与
next/、task/ing 在册符号后，新出即本批 24 口 + 同轮第 4、5、6、7 各条。已知盲区：跨 crate 同名
（如 try_get_expire_option 与 wresp::options::try_get_expire_option）会被同名活函数掩盖，故本批
全部改按「限定路径 + 逐文件实读」复核过（rg -n "\b名\b" 排除定义文件后看是否 tests/ 独占）。

分拣补记（next/muse.design.md 条 14 同题增量）：信封编解码 obj_encode 家族已实测为单链薄转调
形态——真源仅 obj_encode_custom_into 与 obj_decode_custom 两口，obj_encode_into/obj_decode/
obj_encode 分别是其类型糖转调；obj_encode_custom 在产（消费点 wnode/src/resp/objects/
custom_object_commands.rs），非零消费；本批在册的零消费死口仍只有 obj_encode。落地删 obj_encode
时可选顺带收敛顶层入口面（六口并为 encode_into/decode 两口），非必做。

盘点补记（qw13.invA zero-consumer-dead-surfaces-batch-six）：dev e75716e 复核抽查仍在场：range_index_manager_replication.rs:115 STREAMED_PUBLISH_LOG_ARG、:187 set_aof_stream_chunk_size、:227 replicate_range_index_del、aof_backpressure.rs:229 set_counter_log/:237 get_shipped_watermark 均零生产消费（set_counter_log 唯一消费在同文件 #[test] 内）。24 面逐条裁定仍待做，票面三处联动裁定不变。

处置记录（fix-dead-batch-six，2026-09-19，裁决=实现）：
- 已被并发会话删除（本批核实时全仓零命中，注明不重复处置）：consistent_read 三口
  upsert_forbidden/rmw_forbidden/delete_forbidden、wval write_to_slice。
- 删（零读者或测试可迁）：STREAMED_PUBLISH_LOG_ARG（rust AOF 显式入队无自动记录抑制位，
  ignore 登记）、set_aof_stream_chunk_size（C# 亦仅 cluster 测试钩子，ignore 登记）、
  replicate_range_index_set/del（票面只列 del；set 同体顺带，读者同 tests——rust 生产对位
  是 service.rs StoreEvent::RangeIndexWrite→enqueue_slices 单点，锚点已迁注 service.rs）、
  backpressure_wait_vector_async（同步版 :281/:380 在产）、chunk_buffer_size（rust 重组
  Vec 动态增长无预分配位，ignore 登记）、get_value_sequence（object_value_bytes 在产承接
  C# GetValueSequence，锚点迁注 object_value_bytes）、replica_context_snapshot（纯 rust
  测试白盒观测，集成断言删除，hash 链语义由 pre/post 协议内部闭环承接）、attach_monitor
  （生产构造期 new() 已从 global 单例建表承接 C# 构造注入；pending-lat-no-timing-site 落地
  形态=trait 载体未采用晚装配重挂，此口为未采用形态残留；garnet_api 两处注释同步改写；
  server_monitor_tests 改 install_global 形态）、reset_all_latency_metrics（对位分叉为
  consumer_registry 句柄直调 reset_all，锚点迁注）、set_client_lib_info（network_clientsetinfo
  命令臂直赋 pub 字段）、validate_invariants（snapshot().validate() 在产 hlog/mod.rs:258）、
  obj_encode（类型糖，测试改 obj_encode_into）、get_entry（C# 全仓零引用死口，ignore 登记，
  测试改 pub entries 直读）、new_p（构造口泛型化：new()/with_capacity() 上移
  impl<P: RespProtocol> 块，别名默认参不参与方法路径推断的调用点显式 turbofish）、
  try_skip_byte_array_with_length_header（C# 仅测试引用死口，ignore 登记）、
  try_read_ptr_with_length_header（C# 在产于 ObjectStore/Common.cs:223/:242 副本回读 SCAN
  重解析，rust 回读结构化不经文本帧，ignore 登记形态差异）、initialize_with_args（字段 pub
  直填，三处测试 helper 展开）、get_replica_endpoints（C# 全仓零引用死口，ignore 登记）、
  get_task（task_ref 在产 aof_replication_pump.rs:202，测试全改 task_ref）、
  is_exhausted（pub 字段 exhausted 直读替代）。
- 保留并注明（测试真在用/C# 对标，票面口径 cfg 收口对集成测试读者不可行）：
  set_counter_log（LogTailHandle::Counter 为域内注释定性的测试替身形态，enum 变体无法
  cfg，删 setter 须连锁删变体重写 6+ 单测）、get_shipped_watermark（节流路径等价性回归
  test_throttle_replica_matches_throttle_all 与最小水位收紧断言的最小观测面；C# 私有数组
  无 getter，rust 以 pub 观测口承接；本批曾裁定删，见此反转理由）、
  pending_stream_reassembly_count（C# internal+RangeIndexStreamReplayTests.cs 测试消费，
  rust 集成测试对标）、object_collect_running（C# TaskManager.IsRunning public+
  TaskManagerTests.cs 测试消费，rust 测试对标）、run_for_runner（C# LuaRunner.cs:RunForRunner
  public，C# 消费=fuzz/test，wlua 测试同位承接）、live_index_count（C# internal+
  RespRangeIndexTests.cs 测试消费，rust 注释已注明）、as_allocated_slice(_mut)
  （C# SectorAlignedBufferPoolTests 对标观测形态）、wait_failover_done（测试收尾同步口，
  event/lock 私有无替代活口；服务关停序列接线属 failover 域票）、evaluate_key_gate
  （集群槽位门宿主面，域已建待接线，cluster_slot_verify_wait.rs 测门评状态机；接线属
  集群域票）、from_der（done/inbound-tls-client-cert-auth.md 既有裁定：测试/自签装配
  入口保留；该票文件现已不在 done/，裁定依票面转述执行）、with_revivifiable_fraction
  （票面拟跳给 next/qw.db.md 条 1——该票已不存在；ing/reviv-copy-reads-to-tail-gate-parity.md
  要求先核实：无票承接且 C# GarnetServerOptions.cs:912 在产（配置装载链），按 C# 可见性
  对标保留，后续配置装载接线时处置）。
- ignore yml 落点：common.yml（RespReadUtils 两函数）、garnet/libs/server/AOF/GarnetLog.yml
  （ChunkBufferSize）、garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Replication.cs.yml
  （新建：StreamedPublishLogArg、SetAofStreamChunkSize）、garnet/libs/server/Metrics/
  CommandStats.cs.yml（新建：GetEntry）、libs_cluster_Server.yml（GetReplicaEndpoints）。
- 验证：worktree cargo check --workspace --tests 0 error 0 warning；主仓合并后
  cargo check --workspace --tests 通过（基线 -p wnode 单包编译存在 custom-obj feature
  组合的既有错误，与本批无关，--workspace 口径不受影响）。合并 commit 见归档提交链。
