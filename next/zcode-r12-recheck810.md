zcode-r12-recheck810 复核轮8-10意见现状(轮12)

方法:15 份复核对象(r8×5、r9×5、r10×5)逐意见条目重走代码链路;P0 六项(spot soak1、chaos 1c/1d、sched2、idem1、const1)亲自重走全链。只读,未跑 test.sh/clippy.sh/cargo。r11 交叉注:r11-boundary/cross/gc 已把 r8-sample-a、r8-c 迁移半失败、r10-idem1、r9-soak1 列为在档不复述,均无翻案。行号为今日 HEAD 实测。

轮8

r8-const

1 AofReplayDriftCheckFreq 默认 0,C# 两侧均 1,注释把 C# 默认错写成 0(深核)
仍在。
证据:wconf/src/runtime_server_options.rs:133 Default replay_drift_check_freq: 0、:85 注释仍写「默认 0」;生效门 wnode/src/aof/readconsistency/read_consistency_manager.rs:65-66(freq > 0 判定)未动;C# 侧 GarnetServerOptions.cs:139 = 1、defaults.conf:173 = 1 亲核无误。多子日志/多回放部署下默认臂仍相反。
2 复制域发送字节顶 128MiB,C# 生效 256MiB
仍在。
证据:wconn/src/types.rs:32 仍 4 * (2 << 24);注释仍留「旋钮接线落地后改接」;C# 锚亲核 ReplicationNetworkBufferSettings.cs:38 2 << AofPageSizeBits()(页 "32m" 位 25 = 64MiB)× 4 页 = 256MiB。aof-page-size 旋钮读侧(aof_settings.rs)早已在位,前提持续不成立。
3 TCP backlog 1024,C# 512
仍在。
证据:wnode/src/net/socket_opt.rs:12 常量未动,无差异登记。
4 MIGRATE timeout <= 0 兜底 60000ms,C# 无兜底
仍在。
证据:wedb/src/server/migration/migrate_driver/keys.rs:48/:53-59 wait_dur 逻辑原文未动,注释仍自认 redis-cli 口径,未登记有意偏离。
5 主存页 16MiB 三处独立定值
仍在。
证据:wconf/node_options.rs:26、whlog/config.rs:14、waof/src/wal/config.rs:5 三常量俱在,未收敛 wconf 单点。
6 集群节拍双定义双单位(60s 两处)
仍在。
证据:wedb/src/args.rs:11 = 60000ms、wconf/runtime_server_options.rs:103 = 60s,仍靠 config_owner 调停同步,值源未收敛。

r8-deps

1 wnode 死依赖 enum_dispatch
仍在。
证据:wnode/Cargo.toml:70 仍在,wnode/src 零引用。
2 SKILL 点名三项逃逸 workspace 单点
仍在。
证据:wedb/Cargo.toml [workspace.dependencies] 无 fearless_simd/nested-text/luau0-src 条目与版本锚注释;wbase/Cargo.toml:59、wconf/Cargo.toml:19、wlua/Cargo.toml:39 仍直写。
3 已有 workspace 条目被直写绕过
仍在。
证据:windex/Cargo.toml:20-21(libc/log)、wpubsub/Cargo.toml:13(event-listener)、wnode/Cargo.toml:37/:39(rustls-pki-types/smallvec)、wbase/Cargo.toml:76-77(dev log/log_init)均直写。
4 跨 crate 重复直写(num_enum、compio-tls)
仍在。
证据:wcol:30、wmetric:18、wresp:23 三处 num_enum 0.7.6;wconn:28、wnode:28 两处 compio-tls 0.10.0,workspace 均无条目。
5 wvector 直引 rand
仍在。
证据:wvector/Cargo.toml:24 rand = "0.9.5" 且无 diskann 边界登记注释(建议的注释未补)。
6 webpki-roots 锁内双版本
仍在。
证据:Cargo.lock 0.26.11 与 1.0.9 共存;wconn/Cargo.toml:37 仍 0.26。
7 regress/bench 版本下限滞后
仍在(wbftree 0.1.4 对实际 0.1.6、wdev 0.1.3 对 0.1.7,regress/bench 两处;bench sonic-rs 0.5.8;regress sonic-rs 已自升 0.5.10,属唯一微动)。

r8-sample-a

1 SETEX/PSETEX 过期参数 strict_i64 vs C# int32
仍在。
证据:wnode/src/resp/basic_commands/set.rs:696 parse_setex_args 仍 strict_i64;r11-boundary 已列在档不复述。
2 SET/SETEXNX 的 EX/PX 同根差异
仍在。
证据:parse_set_options 同文件未动。
3 DECRBY i64::MIN 取负符号翻转
仍在。
证据:wnode/src/resp/basic_commands/incr.rs:76 仍 cmd.sign().saturating_mul(by)。
4 PFMERGE 错误路径原子性(rust 零写 vs C# 部分合并落盘)
仍在。
证据:hyper_log_log_commands.rs slow_hll_merge:任一源 WrongType 即 return,dest 不写回;装载全源后才 rmw_string。
一致 26 条:无动作项,抽查未见翻案。

r8-sample-b

1a 只读区链首 elide 未落地
仍在。
证据:wkv/src/session/raw/write/inplace.rs:198-233 elide_src 仍只在 addr >= read_only_addr 可变区探针内记录。
2a 删除 elision 默认关 vs C# 恒清链
仍在。
证据:wkv/src/session/mod.rs:146/:183 record_elision 默认 false,wkv/src/config.rs:359 enable_revivification 默认 false,合取门(mod.rs:524)默认全关。
4 truncate 钳制未提交地址静默吞
仍在。
证据:waof/src/wal/log.rs:221 safe_until = until_address.min(committed),调用方无感知面未加。
刻意等价 10 条、一致 11 条:无动作项,未见翻案。

r8-sample-c

1a negotiate_resync 把 C#「仅记日志继续 partial」折为 FullResync 未声明
声明已补(差异保留)。
证据:replication_manager.rs negotiate_resync 头注新增「与 C# 的形态差异:…两形态统一收敛为 is_partial_possible = false」;代码行为(:790-796)未变。
1b 同处异常收敛折 FullResync
声明已补(同上头注,1a/1b 一并覆盖)。
1c 阈值 > 0 vs C# > 64
维持(架构适配声明在案,头注已含)。
1d same_history2 修上游笔误引入双方皆空边缘分叉
仍在。
证据:replication_manager.rs:757-758 仍 !is_empty() && 相等;C# 笔误形态未登记裁决。
2 检查点接收面 RI FLUSH 单消息拒绝、类型集收窄未登记
登记已补(差异保留)。
证据:receive_checkpoint_handler.rs 模块头新增两条「登记差异」(STORE_INDEX 元数据、STORE_RANGEINDEX_FLUSH,含拒绝理由「防静默丢帧」);key_id u128 LE 双侧自洽面维持。
3 gossip 记账时机(成功后 vs 发送时)
仍在。
证据:gossip_manager.rs:386-391 last_sent_config_version 仍仅 Ok 分支推进;node_connection.rs:162-165 已注释「仅成功轮次记账」机制但未对 C# 分叉作裁决登记。
4 单物理日志下时间脉冲恒发,C# MultiLogEnabled=false 静默
仍在(强度修正)。
证据:ReplicaSyncSession/AofSyncTask 发送条件已 1:1 对齐 C# 现形态(tail 移动或背压停顿才发、accepted < tail 不发,aof_sync_task.rs:343-395);但脉冲源注入(replica_sync_session.rs:197)仍无 MultiLogEnabled 总闸(C# AofSyncTask.cs:117 = serverOptions.MultiLogEnabled,亲核),单日志有写流量时仍发 C# 所无的 ADVANCE_TIME 帧;「恒发」原表述过强,实为节流门控发送。
5 背压 shipped 不折叠扫描位
仍在。
证据:aof_sync_task.rs throttle 头行仍 shipped = previous_address,Queued 帧落网前不计,补账机制未变。
6 ReplicaDisklessSyncFullSyncAofThreshold 缺失
登记已补(缺席裁决为有意)。
证据:replication_manager.rs:888-894 头注明示「第 4 条在 rust 缺席…不自造第二套门限…待门限配置单独立项时接上」,并已登记 js/check/ignore/server.yml。
7 merge_slot_map handoff 主未登记本地整轮放弃
维持(声明)。
8a MIGRATE 半失败 +OK 怪癖未保留未登记
登记已补(收紧保留)。
证据:frame_import.rs 模块头(:9)列明「协议面差异(migrate 头声明槽门禁、文案前缀、覆写口径、登记槽取值)」;链头一次门禁+失败回错行为未变。
9-13(slot_verify TRYAGAIN、gossip 扇出、应答时机、槽校验次序、failover 复位):均原判「声明」,维持,无动作。
一致 17 条:无动作项,未见翻案。

轮9

r9-soak

1(P0)单文件设备使全部物理回收空操作,两日志随累计写入单调增长(深核)
仍在,无在途票。
证据:装配四点仍全 single_file(service.rs:845/:907/:1284/:1318,全仓 SegmentedDevice::new 分段装配零生产命中);wdev/src/device.rs:294-307 segment_size None 直通 Ok(())、segmented_device/truncate.rs:135 is_none 早退,两处未动;compact.rs Shift/Lookup/熔断推进 begin 与「设备历史段物理回收」文案照旧;database_manager_base.rs 截断文案、WedbStore::truncate(addr.rs:193-201)同。
新增增量(复核揭示):aof-segment-size 旋钮已入配置面(runtime_server_options.rs:71/:129 默认 "1g")并被 AofSettings::from_options 读入做校验(页不得大于段),但 wal_config() 只投影 buffer/page 两项,WalConfig 无段维度——校验面与物理面脱节,配置看似生效实则无物理效应,单文件空操作较原判多一层「假旋钮」误导面。
2 冷分层键后台降阶评估默认不可达(expired-object-collection-freq 默认 0)
仍在。
证据:node_options.rs:76 DEFAULT_EXPIRED_OBJECT_COLLECTION_FREQUENCY_SECS = 0;demote 轮仍搭车该任务(primary_tasks.rs tiered_demote_round 调用点);collection.md 3.3 只登记机制不解决默认值。相邻票 demote-candidate-domain-scan(在途)只降扫描成本,不闭此缺口。
3 停机排空墙钟裸 u64 减法
仍在。
证据:consumer_registry.rs:612/:620 begin = now_ms() 与 now_ms() - begin >= DRAIN_TIMEOUT_MS 原文未动。

r9-load

场景1.1 纪元槽=并发连接硬上限
仍在。
证据:wkv/src/config.rs:324-327 max_sessions 推导式未变。
场景2.1 AOF 环满幻影写/静默缺账
仍在。
证据:inplace.rs:210/:229/:491 notify_write_listener 仍以 ? 上抛(原位写已生效);rmw_helpers.rs 对象臂仍 log::error 吞错族(:264/:271/:300/:423)。
场景3.1 信封态闩内全量序列化
仍在(信封态结构性);树态删除臂在途票 tiered-tree-in-place-delete(与 r10-bigo 问题1 同票)。
场景4.1 复制单泵串行扇出
仍在。
证据:aof_replication_pump.rs:237-238 pump_backlog 仍单循环逐 driver 串行;相邻票 sublog-fanout(在途)解决多物理子日志扇出,boot 仍强制单物理日志,本条形态不变。
场景5.1 单 fsync 域跨租户互 pace
仍在(能力扩展固有架构面,无对应动作)。

r9-chaos

1c(P0)aof-commit-wait 刷盘失败仍回 +OK(深核)
仍在,无在途票。
证据:waof_sublog.rs:418-423 wait_for_commit_async 仍仅 log::error 吞错;drive.rs:222-228 与 :338 推送帧臂仍「等待结果弃用、照常发出应答」;C# 锚亲核 TsavoriteLog.cs:2776-2789 失败设 cannedException + TrySetException、WaitForCommitAsync await 任务即抛,对位错误成立。
新增事实(复核揭示):修向载具已在位——71bf987a 落地 drive.rs:286-296 take_fatal_disconnect 断连闸与 ClusterReplicationSession/RespSessionConsumer 信号通道,提交失败未接入该通道,接线即修。
1d(P0)磁盘满×检查点周期重试环→副本模糊区反复丢弃(深核)
仍在,无在途票。
证据:spawn_aof_size_limit_task(service.rs:586-597)失败仅日志循环照跑(注释仍声明刻意);take_database_checkpoint_async 中 checkpoint_version_shift_start + set_current_version 先于 create_checkpoint_with_token,失败路径不回滚版本、End 不写(函数注释自认「失败路径不到此处」);aof_processor.rs:432-451 新 Start 撞未闭模糊区仍清缓冲仅 Info 留痕(C# AofProcessor.cs:276 同形)。主库每轮失败=副本丢一窗已消费条目,链路与原判一致。
1e bf-tree 落笔 ENOSPC panic
仍在。
证据:外部 bf-tree 0.5.6 src/fs/std_vfs.rs:75 write_at(...).unwrap() 未变(上游 crate,仓内不可直改)。
5a meta 损坏回退缺统一 error 留痕(确认项)
已闭合。
证据:checkpoint-recovery-fallback 票落地,生产恢复链接入 recover_latest(database_manager_base.rs:131/:158),坏 token 容错跳过 + warn 留痕在位。
6c 复制时间脉冲墙钟域错位
仍在。
证据:aof_sync_task.rs:348 仍 now_ms() 墙钟;C# AofSyncTask.cs:255 Environment.TickCount64 亲核。脉冲结构已 1:1 化(见 r8-c 条4),唯钟域未换;相关票 aof-tail-shift-truncate-and-pulse(在途)已落 AofTailWitnessFreq 消费半边,截断半边与钟域未动。
6d CLIENT age/idle 墙钟域
仍在。
证据:client_commands.rs:99/:171 仍 now_ms_i64()。
其余(1a/1b/1f/1g 拒写与吞错面、剧本2 断链组、剧本3 kill 组、剧本4 慢盘组、5b/5c 确认项、6a/6b/6e):原判「良/无增量/同形」,无需动作,未见翻案。

r9-client-compat

1 TIME 微秒未 6 位补零
仍在。
证据:core.rs:816-821 仍 itoa 直接 format(usecs)。
2 ASKING 补参数校验
仍在。
证据:basic_commands/mod.rs:107 仍 check_arg_count!(count == 0)。
排除项与无增量确认面:无动作,未见翻案。

r9-recent

1 efc3a70 Meta 域死记录误判(已自愈)
已修复(维持)。
证据:live_value.rs:98-102 flatten 折叠双层在位。
2 25a7c04 丢区间级早停(已自愈)
已修复(维持)。
证据:whlog/src/walk.rs on_flush 回 bool,flush_records_in_range 收 bool 早退。
3 dfa85c9 SAVE/BGSAVE 死锁(已自愈)
已修复(维持)。
证据:slow.rs:1193/:1201/:1213 take_checkpoint_within_gate + RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS,占闸-还闸配对在位。
4 86164e4 测试编译洞(已自愈)
已修复(维持)。
证据:wcol-expiration-ledger-dedup 票 done,断言走公开观测口。
5 panic=abort 双裁决矛盾(reject/done 并存)
仍在。
证据:task/reject/zcode-r2-error.md 全文仍主张保留 abort,无「已被 panic-abort-policy 反转」回指;wedb/Cargo.toml:238 注释确认 unwind 为现行终态。r9 建议的补回指未做。
6 0fbbcc8 回放通道畸形 obj_type 静默降级 Null(watch 级)
仍在。
证据:range_index_manager_replication.rs:394 仍 from_u8(...).unwrap_or_default(),无 warn 无登记;store-event-garnet-object-type 票只收强类型化,不含降级留痕。

轮10

r10-bigo

问题1 树态 hash/set/list 穿透臂 O(整对象) 五段全量 + AOF 整树重发,doc 仅登记 zset 面
仍在/在途(票 tiered-tree-in-place-delete,task/ing)。
证据:tiered_collection_ops/hash.rs `_ => Ok(false)` 穿透臂与「原树内逐成员出账臂已删」注、set.rs:263-272 Srem/Spop 等穿透臂、list 模块头「一律经物化后整树重灌」均原文在位;collection.md 第 8 节标题仍「分层有序集合范围/排名命令」仅覆盖 zset,hash/set/list 复杂度声明缺口未补。
问题2 AOF 单帧容量门已被装配校验闭合(非问题)
维持(排查结论,无动作)。
已核实双侧同级 17 项:无动作,未见翻案。

r10-sched

1 后台周期任务惰性钉死首会话 worker
仍在。
证据:service.rs get_session 惰性拉起面(pubsub swap 闸 :1700-1703、量化协程、AOF 周期提交、对象收集、索引扩容)结构未动;分摊面仍仅量化协程一处。
2 darwin SO_REUSEPORT 硬倾斜、Linux 静态映射无重平衡(深核)
仍在,无在途票。
证据:socket_opt.rs:57-75 bind_reuseport 的 set_reuseport 覆盖全 unix,cfg 排除表仅 solaris/illumos/cygwin,无 darwin 臂;server.rs start_tcp_workers:468-546 仍顺序绑 nthreads 个 worker,无单 worker 退化、无 accept 转派、无平台差异登记。darwin 开发机上 thread-per-core 名存实亡形态与原判一致。
3 无核钉定、主线程 runtime 兼职重载
仍在。
证据:全仓无 set_affinity/core_affinity 命中;主线程后台任务面未迁移。
4 同核命令耦合饿死窗
仍在。
证据:drive.rs 泵主循环仍同步执行命令无切片/让渡点(本轮新增的仅 fatal_disconnect 闸,与此无关)。

r10-format

1 AOF 版本域同号(5)异构,版本门对跨仓装载失效
仍在。
证据:waof/src/aof/header/basic.rs:81-83 AOF_HEADER_VERSION = 5、MAX = Self 未动,常量处无「同号异构禁跨仓」登记,亦未改自持域。
2 RI 复合元记录读侧窗口切分手拼三处
仍在。
证据:stub.rs:58/:99 两装载点 + migration.rs:216-221 claim 重读仍各自手拼,meta_and_stub 单点未建。
3 AOF 载荷 4B 长度前缀读侧三处重复
仍在。
证据:record_gate.rs:84 peek_entry_key、aof_processor.rs:205 prepare_key、:247 split_value_input 三点俱在。
4 EMPTY_REPLAY_INPUT_BYTES 与编码器无绑定
仍在。
证据:replay_input.rs:25 仍 [0u8; 36] 手工形态,无 roundtrip 断言测试钉死。

r10-memberttl

1 HEXPIRE 条件拒绝臂幻影项修复未入 doc
仍在(代码侧声明充分,doc 侧未落)。
证据:expiry_ledger.rs set_expiration 头注「与 C# 的刻意差异…禁止复刻幻影项」在位;collection.md 仍无成员 TTL 刻意差异小节(全文 grep 偏差/差异仅第 69 行复杂度一处)。
2 SortedSet Equals 重复谓词 1:1 保留,注释「等价于一次」失准
仍在。
证据:sorted_set_object.rs:393 注释原文未改。
3 周期收集两刻意差异(零变更门控、失败粒度)未入 doc
仍在。
证据:primary_tasks.rs 注释在位;collection.md 零登记,合并小节建议未落。

r10-idem

1(P0)副本背景重放任务死亡后 resync 起点取 enqueued 尾,[applied, tail) 重放体静默丢失(深核)
仍在,无在途票。
证据:五点全链复核——replica_replay_task.rs:287-293 processor Err 仍 warn 后 break 任务终止(无 fatal_disconnect 置位、无会话断流);replica_replay_driver.rs:158-181 initialize_background_replay_task 仍 background.is_some() 即吞(仅 dispose 清槽);assembly.rs:183 INITIATE_REPLICA_SYNC 仍报 wal.tail_address()(enqueued);replication_manager.rs negotiate_resync 仍 replay_until = min(rep_tail, committed)(:808-813),applied 位点全程不消费;cluster_replication_session.rs:321 新任务仍从首帧 previousAddress 起扫。本轮新落的 fatal_disconnect 闸(cluster_replication_session.rs:393/:430)只覆盖解析违规与 APPENDLOG 拒收,不覆盖重放任务死亡——修复方向二的载具已在位未接线。r11-cross 已列在档,无翻案。
2 向量集迁移 RESERVE 失败重试永久泄漏
仍在(C# 同形继承)。
证据:sync_transport.rs:171-177 每次传输首步重新预留;replication.rs:405 收端照单全收;vector_manager_context_metadata.rs 仍只有 reserve(:367)无 release/unreserve 原语;try_recover_from_failure(keys.rs:138)仍无目标端补偿。
已核幂等面:无动作,未见翻案。

统计

复核意见条目(立见,不含各文件「无增量确认/一致」面):43
已修复/已闭合:5(r9-recent 条1-4 自愈维持、r9-chaos 5a)
登记/声明面落地(差异保留):5(r8-c 条1a、1b、2、6、8a)
强度修正(判定成立、表述收敛):1(r8-c 条4「恒发」→节流门控发送)
仍在:32
在途(有票):1(r10-bigo 问题1,tiered-tree-in-place-delete);相邻票不闭口的 2(r9-soak 2、r9-load 4.1)
误报/翻案:0
P0 深核六项:全部成立,全部仍在,零在途修复票(soak1、chaos1c、chaos1d、sched2、idem1、const1)

复核揭示的增量(2)
1 aof-segment-size 旋钮校验面与物理面脱节:旋钮入配置与 boot 校验链(页不得大于段),WalConfig 无段维度、设备恒 single_file——配置消费 falsely 有效,单文件物理回收缺口上多一层误导面(r9-soak1 的新形态)。
2 fatal_disconnect 断连通道已落地(drive.rs:286-296 + 两会话实现)但未接两处 P0 修向:AOF 提交失败(r9-chaos 1c 修向「错误应答或断会话」)与副本重放任务死亡(r10-idem1 修复方向二「置 fatal_disconnect 逼 FullResync」)——两票的修复载具已在位,接线即修。

视角结论:有增量
