近期提交回归审查（轮9，视角 = 最近合并波引入缺陷；窗口 = 2026-09-20 20:52 init 起 3 天，非合并提交 283 条，逐条甄别后行为类约 45 组全读）。
方法：git log 按域分组，对行为类提交读说明+diff，审「改动自身正确性 + 与周边交互」，重点核对大合并的后续修复链是否自洽、有无补丁摞补丁残留。只读，未跑 test.sh/clippy.sh/cargo。


一、立缺陷（真缺陷，含已自愈回归与文档矛盾）

1) efc3a70 wip 收编升阶集合传输半成品——引入 Meta 域死记录误判缺陷（已由 6781c65 闭合）
改动：把 /tmp/fork 上一棒中断的未审半成品固化进 dev（wconn/record.rs kind=4 帧协议扩流元、migrate_driver/live_value.rs 取值分类泛化 TieredTree、快照内核与接收面改造）。
问题：probe_live_key_kind / read_live_value 的 Meta 域分支把原 `if let Some(Some(obj_type))`（双层 Option，内层 = 记录存活）改成 `.is_some()`（只测外层域存在性）——Meta 键在但记录已死/畸形时被误判 TieredTree 走带外树迁移，与紧邻注释「畸形/死记录 None 落不存在」失配。
证据：`git show efc3a^:wedb/wedb/src/server/migration/migrate_driver/live_value.rs`（改前为双层 match）；6781c65 以 `.flatten()` 修复并新增 tiered_sync_migration.rs 用例 4，提交自述「承接本棒修复的缺陷」。票档随 35dcee5 落 task/done/tiered-key-sync-migration.md，收发两侧终态完整。
判定：回归成立但窗内自愈，链路闭合。「未审、可能不完整」的 wip 直接进 dev 有实际回归成本，此为该流程当窗内的实证样本。

2) 25a7c04 whlog 页内走查单点化——丢失多页区间级早停（已由 e6b58df 闭合）
改动：wkv 的页内记录走查收编为 whlog 单点 for_each_record_in_page / _mut，消除越层触及。
问题：走查内核返回 ()，visit 返回 false 只终止页内循环，flush_records_in_range 继续走后续页——违背「on_flush 返回 false 即刻终止全部走查」契约（原 wkv on_flush_pages 出错中断整段区间的语义在单点化时丢失）。
证据：e6b58df diff 显示改前两内核 `return;`→改为 `return false` 并在 flush_records_in_range 收 bool 早退；提交自述「补全区间级早停，兑现既定契约」。两提交相隔 9 分钟（21:27 / 21:36）。
判定：回归成立但窗内自愈，契约恢复。

3) dfa85c9 检查点闸门收口——SAVE/BGSAVE 占闸语义改错方向引发死锁（已由 1430123 闭合）
改动：take_checkpoint 收口（replica 钩子、集群按需重拍、SAVE/BGSAVE 并闸），撤 RESP 层 CheckpointPauseGuard。
问题：把 C# TakeCheckpointAsync(:122 占闸失败即 false) 错改成 TakeOnDemandCheckpointAsync(:152 while+yield 让渡等待)——闸被外部 TryPauseCheckpoints（运维暂停面）持有时，唯一唤醒源 resume_checkpoints 只能由持闸方触发，SAVE/BGSAVE 无上界死等，save_bgsave_concurrent_mutual_exclusion 死锁超时。
证据：1430123（53 分钟后）改回占闸失败即 Ok(false) → RESP_ERR_CHECKPOINT_ALREADY_IN_PROGRESS，新增 take_checkpoint_within_gate 承接 BGSAVE「同步占闸、后台推进」，让渡重试只留按需入口。终态复核：slow.rs :1196/:1204/:1211/:1216 与 C# NetworkSAVE/NetworkBGSAVE 同位，占闸-还闸配对完整，唤醒链闭环（每占闸成功者必经 resume_checkpoints）。
判定：回归成立但窗内自愈；该域当窗内四连补丁（73f53a5→8b69275→dfa85c9→1430123→2baa614），终态自洽可追踪。

4) 86164e4 wcol ExpiryLedger 单点化——漏改测试断言面致编译洞（已由 2fec4c5 闭合）
改动：hash/zset 双份过期账本收敛单点，宿主薄转调后 has_expirable_items / expiration_times 字段不再对外。
问题：object_serialize_expiration_tests.rs 仍直取已私有化字段，测试族编译红（生产 src 不受影响）。
证据：2fec4c5（26 分钟后）改经公开观测口（is_expired / get_expiration）表达同一断言语义。相隔仅 26 分钟，属同棒收尾而非跨会话补丁。
判定：测试侧回归，窗内闭合，生产代码无损。

5) 1108d3e 与 c8b9f54/1f99ba3——panic=abort 双裁决并存，reject 档未随反转更新（仍未消）
改动：1108d3e（00:35）驳回轮2 zcode-r2-error 条目1「panic=abort 全进程崩」，理由为「Rust 服务端标准配置、静态证明无可达 panic」，落 task/reject/zcode-r2-error.md；同日 03:36 c8b9f54 按 panic-abort-policy 票路径 A 撤 panic="abort" + 泵级 catch_unwind 隔离，1f99ba3 归档 task/done/panic-abort-policy.md。
问题：两份方向相反的体系裁决同时存于 task/reject 与 task/done，reject 文件只字未提被后续裁决推翻——后续按 reject 档执行的人会把 abort 改回去（wedb/Cargo.toml 现注释与 done 档一致，代码终态正确）。
证据：task/reject/zcode-r2-error.md 现文仍主张保留 abort（git log 该文件仅 1108d3e 一笔）；task/done/panic-abort-policy.md 路径 A 落地记录与 c8b9f54 diff（Cargo.toml 撤 abort、drive.rs catch_unwind、vector cleanup 复效）互证。
判定：代码终态正确；任务侧裁决矛盾是真问题，建议在 reject 档补「已被 panic-abort-policy 反转」回指。

6) 0fbbcc8 StoreEvent obj_type 强类型化——AOF 回放通道畸形字节静默降级 Null（watch 级）
改动：StoreEvent/ReplayInput 链 obj_type 从 u8 改 GarnetObjectType。
问题：replay_input.rs 以 `GarnetObjectType::from_u8(input.obj_type).unwrap_or_default()` 收窄——AOF 帧携带非法判别字节（损坏/写端 bug）时旧 u8 直传原样到达，现静默降级 Null(0) 后经 range_index_manager_replication 发布面写 MetaValue.collection_type，降级无日志无标记。
证据：0fbbcc8 diff replay_input.rs 该 hunk；写端 `with_obj_type(obj_type)` 存 `obj_type as u8`（合法值往返无损，仅非法字节触发）。
判定：watch 级——正常路径无损，畸形输入的静默降级点建议留痕（warn 一次）或登记偏离，不计为现行缺陷。


二、无问题判定（重点大合并逐组核过，证据在案）

c388401 读泵游标化：游标跨派发驻留、整段消费完才清零复位、尾部空间不足才一次性 copy_within 前移；orphan 检测与 stalled 判定同步改 `&read_buf[read_head..]` / `len() > read_head` 口径，无旧语义残留；补两条游标流单测。

a474748 双泵竞态两修：单飞闸 per-driver `task_ref(0)` pumping 位 + PumpGuard Drop 防错误路径漏放，信号循环与 attach 补扫共用泵体互斥成立；enqueue_reserved 释放槽位移先、信号后置，容量 1 折叠不丢推进的论证成立（折叠时待处理信号的扫描时刻只会更晚）。后续无再修（同域仅测试归位 247b1c2）。

91c7589(+合并 4dbec5a) bftree 常驻回收驱动补挂：全仓 WedbStore 产点穷举 4 处——open_shared 内置（wkv store/mod.rs:534）、wnode 恢复收口（database_manager_base.rs:164）、副本换入（replica_diskbased_sync.rs:146）、service 两处走 open_shared；与 73f53a5 恢复回退链正交（recover_latest 内部重试不产第二实例，挂载在恢复成功臂单点）。无第五产点漏挂（FLUSHDB/FLUSHNS 换号不产新 store）。

8f23100 换号待释放树安全纪元门控：投递携 expired_at（换算单点 reclaim_expired_at 上移 vdb::gc_dead，与死亡账本同域）、消费单趟分区只摘到期、Drop 末轮同门控残件交启动对账；与先行的 0034d47 临界区优化（contains 短路 + 批量 detach 后单次加锁）无冲突。

60bb6e0 RecoverLogDriver replay_one 门序单点：语义变化点（单任务臂不再置 prefix_consistency_boundary_reached）有 C# RecoverLogDriver.cs:108-118 锚；核查消费面——单任务快路径直扫不经 batch_buffer，该旗标在单任务模式无控制流消费者，安全。

4cfb0fb / db04d77 / 804a99d AOF 重组三件：store_ops 拆分纯移动（函数清单一一对应）；包装层压实在 C# 四级拓扑核实后拒绝删层、仅删 5 个 C# 无对应纯转发并留 reject 档；replay_chunk 为纯格式化。

adfe534 + c612431 + e0e56a7 批量读 TTL 门双轨收敛：两会话同窗各自落地一套门（alive_mask+probe_alive vs stack_gates+整块降级），合并 e0e56a7 显式裁决取 dev 侧机制、废弃副本实现、保留对方测试与注释修正——有意识收敛非无声覆盖；GET 慢路径经 read_string_batch_into 同享一门，「GET 与 MGET 对同一过期键同答」成立。

4d1bca6 分层扫描到期判定先于游标跳过：与 r6-scan 登记的「到期成员偏移跨页重复」口径一致，zset 分值臂顺改 if-let 不改语义，补页级无重复回归。

912e115 降阶限批洗牌：同时修正旧模块头「gxhash 种子随机、逐轮随机重排」的错误断言（deterministic 固定种子下迭代序恒定正是饿死根源）——注释失配与实现一并修复，随机源显式禁用 gxhash。

16fbf05 / 75b6829 / b00dbf7 gossip 三件：get_or_add_entry 返回 (conn, is_new) 消除 contains-then-add TOCTOU；配置快照下沉单点；await_holding_lock 以作用域收窄 config 守卫修复。

f9a248d → da7964a announce 链：先补 hostname 跨 RESET 保留（读 current 再建新 config），后改原地 reset（显式保留 address/port/hostname、复用 64KB slot_map）——后者是前者的形态优化，两者语义等价且后者覆盖前者路径。

72e39e4 / d3da62a / 72b6ed1 / b911575 集群门禁四件：announce-ip/port 配置面、SWAPDB 槽态约束、迁移域解析期拒绝非默认域、FLUSHALL_NS 收令帧身份门（节点间连接或 ns0），均红判据先行（dd4334f/3e674df/a006a0a）后转绿，任务档随行归档。

7aa67bf + d16528a + 666cd0b 认证门禁：foreign_namespace_denied 单点判据 ACL 管理臂/认证臂同口径，认证臂不回落内存认证器拦 0#default 引导换租；HELLO AUTH 门经 authenticator_can_authenticate。666cd0b 本体为任务归档（标题含「add hello auth gate」实为 d16528a 已落，无代码增量）。

d2f0204 lua 窗口恢复：协议版本入口保存/收尾按入口版本成帧（RESP3 不再永久降 RESP2）、活动库同点保存恢复经 try_switch_active_database_session 回写；错误臂 &'static str → Vec<u8> 的剥头透传在 wnode 侧 reply_error_bytes 闭合，set/get 两 vtable 签名同步。

51a1ad0 + 306320a 死链两段式删除：先裁决（RUNTXP 终态=入口在、无静态登记表、回 NO_TRANSACTION_PROCEDURE，非占位）后删除（870 行：三枚举臂、txn_proc_view、transaction_manager 死链、runtxp_slot_verify 测试）；先行引入的 TxnProcIoError（6c93ab1/d674633）随链消亡无孤儿，全仓 grep 零残留。

285dc6f CLIENT LIST/KILL 每批汇聚发布：投影更新收敛到 mirror_session_counters 批收场单点，本会话行 LIST/KILL 入口即时刷新，真值单源仍在会话字段；r4-client「静态视图」条目按此闭合。

34df049 INFO 接线七条：监视器全量采样挂句柄、快路径 found/notfound 漏斗尾收口（deferred 整批转慢路径防双计）、COMMANDSTATS 聚合补员（修正注释与实现失配）——口径声明与 diff 一致，SG/failed_calls 差异以登记处理。

39f3fc6 GETKEYSINSLOT 帧头预留-回填：复用 wresp::ext.rs 唯一单点（上界估宽、错误 truncate(base) 撤帧），删除 splice 前插支，与既有四臂同构不新造机制。

5ce6d62 AOF 提交周期热更：aof_commit_ms u8→i32（-1 手动提交通路补齐）、FastAofTruncate/CommitWait 两组合校验对标 GarnetServer.cs:508-519，测试含负数 CLI 解析。

1a2d7cd / 35dcee5 / 3eeef65 / c85c91b / 247b1c2 / 7963441 测试与 clippy 自动清理件：均测试/格式/任务档，无生产语义。

054be40 挂起登记：fix-session-arg-borrow-split 分支（含 0abbf44）留 /tmp/fork 未入 dev，主仓干净，登记文档在 task/ing——在途区按约不审。


三、流程性观察（不计缺陷）

观察1：合并提交复用分支提交信息（4dbec5a、95955cf、666cd0b 等）使 --oneline 呈同消息双条甚至三条，git log --no-merges 才见真身；本轮甄别中两次误判为重复落地，后续审查轮建议默认过滤 merge。

观察2：当窗内多会话并发同域密度高（批量 TTL 门、检查点闸门、announce、HELLO 门、gossip 各两组以上并行棒），全部以合并裁决或后续修正收口，无静默覆盖；但 25a7c04→e6b58df、dfa85c9→1430123 两次同小时级翻转说明「先合后审」窗口内 dev 短暂处于回归态，依赖后续棒及时兜住。

观察3：wip 提交（efc3a70）自述「未审、可能不完整」仍直接进 dev，当窗内证实引入 1 条真缺陷（条目1）——与任务规程「宁可不并也不把半成品合进主目录」的精神相悖，建议 wip 收编一律先在分支内补审再合并。


统计

窗口非合并提交 283 条，行为类约 45 组全读（复制/集群 14、AOF/存储 12、命令/会话 13、测试/清理 6）。
立缺陷 6 条：已自愈回归 4（efc3a70→6781c65、25a7c04→e6b58df、dfa85c9→1430123、86164e4→2fec4c5）、任务侧裁决矛盾 1（panic=abort reject/done 并存）、watch 1（0fbbcc8 回放降级静默）。
无问题判定 30 组（大合并重点组全部复核通过：读泵游标化、双泵竞态、回收驱动挂载、恢复回退链、换号安全纪元、批量 TTL 门、lua 窗口、死链删除等）。

视角结论:有增量
（近期波引入的 4 条回归均在窗内自愈且终态自洽，但 panic=abort 双裁决矛盾仍未消（建议补 reject 档回指）、0fbbcc8 回放降级静默点在案；wip 直合流程当窗内实证产出过真缺陷。）
