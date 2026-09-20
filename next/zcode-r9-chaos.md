轮9 混沌工程视角 故障注入剧本推演 next/zcode-r9-chaos.md

方法: 只读代码推演, 不实跑故障。六个剧本沿代码逐步走错误传播; 与 crash 面区别在「故障组合与故障期间行为」, r2-crash/r5-restart/r5-repl/r7-ops 已立发现不复述。每剧本给: 故障点×代码反应序列 / C# 对位差异 / 判定。真缺陷(静默丢/挂死/不自愈/C# 对位错误)才立。

剧本1 磁盘满

1a AOF 入队阶段(环形窗满)
- wedb/waof/src/wal/pipeline.rs:reserve_address :198-233
  预占地址越过 [flushed 扇区界, buffer_size] 窗口即 Err(BufferFull), 不覆写未刷数据, 绝不静默逐出
- wedb/wnode/src/service.rs:on_aof_store_event :111/:126/:378 map_err(Error::AofEnqueue) 上抛
- wedb/wkv/src/store/event.rs:emit_event :176-184 → wedb/wkv/src/session/raw/mod.rs:93/:140/:155 ? 上抛
- 终端: 写命令失败应答错误帧, 拒写不丢写。判: 拒写面, 良
1b AOF 刷盘阶段(设备 ENOSPC)
- wedb/waof/src/wal/flush.rs:flush_and_sync_range :29-40 → wdev flush_range_aligned 返回 Err(Device), 提交水位不动
- wedb/wbase/src/group_commit.rs:run_leader 广播 Broken 给全部 Follower, Leader 身份释放防死锁
- wedb/wnode/src/aof/waof_sublog.rs:commit_flush_async :318-332 仅 log::error + flush_failures 计数, 无上抛(门面签名 () 同 C#)
- 自愈: wedb/wnode/src/primary_tasks.rs:spawn_aof_commit_task :243-278 周期 commit_async 无条件重试; wdev/src/device.rs:flush_range_aligned :109-140 无粘滞错误态, 磁盘腾出后下轮即补刷环形窗内积压。判: 可自愈
1c 【真缺陷】wait 模式刷盘失败仍回 +OK
- 链: 1b 失败后, wedb/wnode/src/aof/waof_sublog.rs:wait_for_commit_async :418-422 仅 log::error 吞错返回; wedb/wnode/src/net/handler/drive.rs:222-228(及 :338 推送帧臂)注明「等待结果一律弃用…照常发出应答」, +OK 照发
- C# 对位: libs/storage/Tsavorite/cs/src/core/TsavoriteLog/TsavoriteLog.cs:2776-2789 提交失败设 cannedException 并 TrySetException, :1866-1882 WaitForCommitAsync await 即重抛 CommitFailureException, 沿 Send 路径使会话故障, 客户端得不到 +OK(除非 TolerateDeviceFailure)。rust 注释「与 C# 同口径」只对齐了「丢弃返回值」, 漏掉 C# 异常传播臂
- 判: 与 C# 对位错误; --aof-commit-wait 下磁盘满+崩溃 = 已确认写丢失窗口。需修复: 等待结果错误时错误应答或断会话
1d 【真缺陷】磁盘满×集群检查点失败重试 → 副本模糊区反复丢弃
- 序: wedb/wnode/src/database/database_manager_base.rs:take_database_checkpoint_async :280-345 先 set_current_version+写 CheckpointStartCommit(:293-305) 再 create_checkpoint_with_token(:325); 磁盘满时 create 失败 ? 上抛, End 标记永不写
- 重试驱动: wedb/wnode/src/service.rs:spawn_aof_size_limit_task :573-597 单次失败仅日志不退出循环; C# StoreWrapper.cs:648-668 catch 在 while 外, 单次异常任务即死且无重拉。rust 此「刻意差异」把单次失败放大为周期失败环
- 副本面: wedb/wnode/src/aof/aof_processor.rs:432-451 遇新 Start 丢弃先前模糊区缓冲仅 Info 留痕(C# AofProcessor.cs:276 同形); record_gate.rs:should_skip_record :40-66 模糊区内新代条目只入缓冲不应用。主库失败后 current_version 不回滚, 后续写入全为「新代」, 副本持续缓冲→下轮 Start 全弃。磁盘满持续 N 轮 = 副本丢 N 窗写入, 且条目已在流中消费, 永不重放, 直到全量重同步
- 判: rust 引入的副本数据损失组合(C# 任务死后只留一个悬挂模糊区, 不反复丢弃)。修向: 检查点失败后不重发 Start(回退版本)或副本对悬挂模糊区超时强制全量
1e wbftree 页分配/刷盘失败
- 分配面为虚偏移(wbftree 依赖 bf-tree 0.5.6 storage.rs:alloc_disk_offset), 磁盘满在落笔时爆
- bf-tree src/fs/std_vfs.rs:75 `write_at(...).unwrap()`(io_uring_vfs.rs:187+ 同) → ENOSPC 即 panic; 仓库 panic=abort(r2-error 已立全局面) → 进程崩
- C# 对位: C# Tsavorite LocalStorageDevice 错误经 Task 异常/日志面传播, 不整进程 abort
- 判: fail-fast 崩溃, 需人工(重启+腾盘); 不静默丢, 但崩溃面比 C# 脆, 具体化 r2-error panic 票
1f hlog 页驱逐写失败(wkv 主存)
- wedb/wkv/src/store/flush.rs:on_flush_address :52-62 错误收拢上抛 → flush_pages_range → evict_pages_for ? 上抛, 写命令报错。判: 拒写面, 良
1g 节点日志写失败
- wedb/wnode/src/logging.rs:FileLoggerOutput::write_line :101-105 `let _ = writeln!` 吞错。C# FileLoggerOutput 同为尽力写。判: 日志丢失可接受, 无增量

剧本2 网络断(主从间)

2a 断链检测时延
- 全链 TCP keepalive: wedb/wnode/src/net/socket_opt.rs:15-19 idle 300s/intvl 10s/cnt 3, 半开黑洞约 5.5 分钟兜底; 主动错误(RST/FIN)即时
- 主端推流: wedb/wedb/src/server/replication/replica_wire.rs:start_pump :281-360 ship 失败即 wire.disconnect(); 溢流双封顶(:196 MAX_OVERFLOW_ENTRIES=10000 + :266 byte_cap=MAX_UNFLUSHED_SEND_BYTES)超限断连, 主端内存有界
- 副端流读无独立心跳, 依赖 TCP 错误/keepalive。C# 对位同形(无应用层 ping)。判: 良
2b 断链期间主库行为
- wedb/wedb/src/server/replication/aof_replication_pump.rs:pump_backlog :222-310 consume Err → store.try_remove_current 驱动退场出册(对标 C# RunAsync finally TryRemove), 截断线与背压闸门不被死副本钉制; 写路径永不因副本失联阻塞
- backlog: AOF 本体即积压, 断链期照常写; 检查点照常截断 → 截断越过来位点后, 重挂被拒
2c 重连与断点续传
- 无自动重连: REPLICAOF 一次性发起(wedb/wedb/src/server/cluster_session/replica_of.rs:19-79; replication.rs:queue_try_replicate_sync 注明「失败即回 -ERR, C# 同口径」), 人工/运维重发
- 续传仲裁: wedb/wedb/src/server/replication/aof_sync_driver.rs:try_add_replication_driver :283-308 start_address 落于 truncated_until 之下且 !allow_data_loss 即拒 → 全量重同步; 窗口内则从断点续扫。判: 与 C# 一致, 良
2d 脑裂窗口(旧主复活)
- 仲裁靠 gossip+config epoch: wedb/wedb/src/server/cluster_config/mod.rs:merge_worker_info :742-763 epoch 低者被覆盖, merge_slot_map :767-855 错位认领置 Offline 给真属主重认领; 升主方 bump epoch(cluster_manager.rs:try_reset_replica :564-573)
- 双主写窗口 = gossip 收敛前的时间窗, 期间两边都可应答本侧槽写; C# 同形(无 quorum, 运维驱动 failover)。判: 与 C# 一致, 无 rust 增量

剧本3 kill -9 各时机(重启行为差异)

3a 检查点中途
- token 预签发+版本先行已入 AOF; 半写 token 无 meta(wcpr rename 原子发布, manager/mod.rs:448-465 find_latest 只认完整 meta) → 重启回退旧检查点+全量 AOF 重放(AOF 未截断, 零丢失); 残留 .tmp/孤儿目录由 purge_all→sweep_checkpoint_residue :496-519 清扫
- 副本侧: Start 后断流, 模糊区悬挂至重连后由新流推进; C# 同形。判: 自愈(基础面 r2-crash/r5-restart 已查, 无增量)
3b AOF 提交中途
- commit 帧随批尾同批原子持久(waof/src/wal/flush.rs:WalCommitStep :159-192; 环满跳帧降级 :181, 恢复侧回退上一帧收敛); 半写批由记录 CRC 截断
- wedb/waof/src/wal/recover.rs:28-126 扫描至最后合法 commit 帧; 无任何帧时回退「最后一条完整记录即已提交」: 方向安全(多算未确认写, 不丢已确认写)。判: 自愈
3c 升阶中途(wcol→wbftree)
- 写序不变量(wedb/wkv/src/range_index/promote.rs:56-66 文档化): 建树 tmp → 先发 RangeIndexStream 数据流 → rename 原子换入 → 落 meta → 删信封
- 各点 kill: 建树期=旧态完整+tmp 残件; 流后换入前=AOF 有流块, 重启经 aof_processor.rs:857-874 RangeIndexStreamChunk 回放重建; 换入后 meta 前=「新树+旧 meta」滞后态, 读臂惰性恢复收敛+计数校正兜底。判: 自愈, 窗口文档化
3d 迁移中途
- 接收端逐帧导入即落本地 AOF; kill 后目标残留部分键, 服务面按 nodes 配置归属; r8-sample-c 已立「迁移半失败 +OK 放弃未登记」, 重启面无新形态, C# 同形。判: 无增量
3e 换号 GC 中途
- 待回收项以 DbMeta GcDeadNs/GcDeadDb 持久; 重启 wedb/wkv/src/store/mod.rs:rebuild_apply_record :655-690 重挂 gc_dead 队列续收; 映射表由 DbMeta 记录重放回建(r2-crash 已立「换号树删除绕安全纪元」运行态缺陷, 本条只确认重启续跑面)。判: 重启面自愈

剧本4 慢盘(IO 变慢 100 倍)

4a hlog 驱逐连锁
- PageNotReady → wedb/wkv/src/session/raw/mod.rs:evict_pages_for :239-278 写路径内联批量驱逐(num_pages/8, clamp 1..64 页); EpochSuspendGuard 挂起本会话纪元防「长 I/O 钉死纪元阻塞全系统页回收」; wait_safe_head_drained 事件等待(whlog/src/hlog/shift.rs:367-376)非忙旋
- 效应: 写吞吐塌缩至盘速, 触发驱逐的命令吸收整批 I/O, 尾延迟暴涨; 无死锁无丢数据。判: 降级良
4b 组提交聚合窗
- waof/src/wal/flush.rs:commit_to :91-123 Leader 持 commit_lock 级联; 慢盘下单批物理合并更多写入(有利), Follower 无上限堆积(group_commit.rs waiters Vec)仅内存成本; 默认 aof_commit_wait=false(wconf/node_options.rs:741, C# 同)默认不吃满盘延迟; wait 模式下命令延迟=盘延迟×排队
- 副本等待者: WaofSublog 成败均 notify(flush_event)防永久沉睡。判: 无挂死
4c AOF 窗口连锁
- 慢盘拖长检查点 → AOF 截断推迟 → 环形窗(buffer_size)耗尽 → 1a BufferFull 拒写(背压而非丢数据)。判: 良
4d watch 版本推进
- 与写同点内存内推进(wnode/src/storage/session/storage_session.rs:665-670 注入的 WatchHook), 不经 AOF/磁盘 → 慢盘不拖 WATCH/EXEC 校验; 事务冲突判定不受影响。判: 无增量
4e 副本推流
- pump 逐批 send, 溢流字节封顶(剧本2a)防无界积压; replica_replay_task 本地重放与网络解耦。判: 良

剧本5 半写扇区(数据损坏)被 CRC 捕获后

5a waof CRC 链已查(前轮), 本轮核下游:
- 检查点索引: 写侧全量 CRC 回写头(wcpr/src/index_ckpt/mod.rs:115-118), 读侧校验 mismatch 即 Err(index_ckpt/read.rs:138-147) → run_recovery_kernel ? 上抛 → 启动失败(配合 r6-cli 已立 fail-on-recovery-error 默认相反的已知面)。判: 拒启非带伤服务, 良
- 检查点 meta: rename 原子发布+目录树 fsync(manager/mod.rs:394-434), 半写不可见; 若 meta 本身位损坏, latest_checkpoint_meta(manager/mod.rs:466-474)静默返回 None 回退旧检查点——旧检查点之后的 AOF 已被截断, 该区间数据静默缺失; rm.recover_async(replication_manager.rs:1260+)有一条 warn 兜底, 数据面恢复链无等值告警。判: 低概率需人工, 登记为确认项(meta 损坏回退缺统一 error 留痕)
- wbftree 页: 引擎内部页校验属 bf-tree 上游; 宿主边界 file_has_cpr_magic/魔数(wbftree/src/manager/replication.rs:213 RI 快照魔数不匹配判检查点失败)拒收。判: 边界有守门
- DbMeta: 记录经 waof CRC 链必挡位翻转; 仅布局漂移(CRC 通过而解码失败)落 wedb/wkv/src/store/mod.rs:rebuild_vdb_visit :613-618 的 warn+忽略——映射静默缺失后同 vid 按需重建空库, 旧物理数据逻辑失联。代码已自评「布局漂移不隐身」但止步 warn。判: 与 CRC 链叠加后概率极低, 确认项(可选升级为恢复失败)

剧本6 时钟跳变(NTP ±1h)

6a TTL/过期: 墙钟域, rust wbase/src/time.rs:now_ticks :80-82 与 C# DateTimeOffset.UtcNow.Ticks(KeyAdminCommands.cs:423-424)同域——±1h 全体提前/滞后过期, 双侧一致。判: 无增量
6b gossip 心跳: rust now_ms(gossip/node_connection.rs:89-107)与 C# DateTimeOffset.UtcNow.Ticks(GarnetServerNode.cs:136-137)同域。判: 无增量
6c 【真缺陷】复制时间脉冲时钟域错位
- rust: wedb/wedb/src/server/replication/aof_sync_task.rs:send_advance_time_pulse :348-369 用 now_ms()(墙钟), 字段注释自称「TickCount64 域」; C# AofSyncTask.cs:256-288 用 Environment.TickCount64(开机单调域)
- NTP -1h: now-last 为负, `now - last < freq` 恒真 → AdvanceTime 脉冲停摆至墙钟追回(最长 1h), 副本读一致时间(空闲流)推进冻结, 读一致等待者悬挂; NTP +1h: 一次性连发, 良性。判: 与 C# 对位错误, 可自愈(1h), 非丢数据; 修向: 改单调域(now_stopwatch_ticks/Instant)
6d 【真缺陷】CLIENT age/idle 时钟域错位
- rust: wedb/wnode/src/resp/client_commands.rs:99/:171 now_ms_i64()(墙钟, core.rs:1168 注释自称「Environment.TickCount64 等价」但实现为墙钟), consumer_registry.rs:423 creation_ticks 同域; C# ClientCommands.cs:145/:379/:483 用 Environment.TickCount64(单调)
- NTP +1h: age/idle 虚增 1h, CLIENT KILL IDLE 误杀在限会话; -1h: idle 杀滞后的同时 age 显示倒退。判: 与 C# 对位错误, 轻; 修向同 6c
6e 超时族: REPLICA_SYNC_TIMEOUT/REPL_ATTACH_TIMEOUT(wedb/wedb/src/server/replication/replica_wire.rs:113/:121)与 wconn 会话均 tokio/compio Duration 计时(单调), 跳变无影响。判: 良

无增量确认清单(已核与 C# 一致或可接受)
- 磁盘满: AOF 入队拒写面、周期提交重试自愈、wdev 无粘滞错误态、hlog 驱逐错误上抛、节点日志吞错
- 网络断: keepalive 检测、溢流双封顶断连、驱动退场出册、一次性 REPLICAOF 无自动重连、断点续传 data-loss 仲裁、gossip epoch 脑裂仲裁
- kill -9: 检查点 meta 原子发布与残留清扫、commit 帧回退与无帧回退方向、升阶写序不变量、GcDead 重启重挂、迁移重启归属面
- 慢盘: 纪元挂起防钉死、事件驱动排水等待、watch 版本不落盘
- 时钟: TTL/gossip 墙钟域与 C# 一致、复制 RPC 超时单调域

视角结论:有增量
