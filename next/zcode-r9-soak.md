zcode-r9-soak 长稳视角(轮9: 跑 30 天不重启,什么会缓慢变坏)

排除面: r6-mem 已立记账/预算/一次性泄漏面(升阶树页环无总闸、zset 双容器记账低报),r5-timers 已立节拍/相位,r2-concurrency 已立锁序/epoch 竞态。本轮聚焦累积型/缓变型退化,与上述不重复。


发现1 (P0) 单文件设备使全部「物理回收」路径空操作,两个日志文件随累计写入量单调增长直至盘满

缓变机制
生产装配唯一入口 open_node_with_config 与 open_wal 均以 SegmentedDevice::single_file 装配(segment_size = None),恢复臂同。Device::truncate_until_address 对 None 直接返回 Ok(()),truncate_until_segment_impl 首行同判直接返回。于是所有上层回收动作只推进 begin_address 内存位点,磁盘一字节不回收:

1. 每次检查点后的 AOF 截断(take_database_checkpoint_async 集群臂 safe_truncate_aof / 单机臂 truncate_until_async)为逻辑空转;日志文案 "Will truncate AOF ... (segments deleted on truncate)" 与单文件下的实际行为不符,形成运维误判面
2. 紧缩 Shift/Lookup 档与死亡虚拟 ID 高低水位熔断旁路推进 begin 后,info 文案声称「由设备 truncate_until_address 物理回收已回收段」,单文件下同为空操作——doc/zh/db.md「偏序 GC 屏障后由日志紧缩物理丢弃」「高低水位熔断防换号旧垃圾膨胀磁盘」两条承诺在生产装配下结构性落空
3. WedbStore::truncate(FLUSHDB unsafe-truncate 臂)同样不缩文件
4. 结果: <data> 下 store 数据文件与 <data>/wal/wal.log 的尺寸 = 进程历史累计写入字节;tail 只进不退,重启后从旧文件尾继续追加,跨重启持续增长;回收内核逻辑(水位判定、回退段数、熔断加速)全部在跑,唯独设备层没有物理半边

证据
wnode/src/service.rs:828 open_node_with_config single_file
wnode/src/service.rs:888 open_wal single_file
wnode/src/service.rs:1264/:1288 恢复臂 single_file
wdev/src/device.rs:294 truncate_until_address 的 segment_size None 直通 Ok(())
wdev/src/segmented_device/truncate.rs:135 truncate_until_segment_impl 首行 is_none 早退
wnode/src/database/database_manager_base.rs:317 截断文案 / :361 truncate_until_async
wkv/src/gc/compact.rs:117-148 Shift/Lookup/熔断推进 begin
wkv/src/store/addr.rs:190 WedbStore::truncate
wnode/src/resp/garnet_api/slow.rs:1065 unsafe_truncate_log 臂

变坏时间量级
wal.log 增速 ≈ 写吞吐对时间积分。1 MB/s 均值 ≈ 2.6 TB/月,天~周量级触盘;50 MB/s 重负载数小时~数天内。store 数据文件同步增长(该半侧默认档 C# 同型,见对位),但 AOF 半侧与熔断/Truncate 承诺是 rust 独有缺口。

C# 对位
C# AOF 与 TsavoriteLog 跑在分段设备上(GarnetServerOptions.SegmentSize 默认 1GB),每次检查点 TruncateUntil/SafeTruncateAOF 按 begin 删段文件,磁盘占用以「检查点边界 + 1 段」为上界;C# store hlog 默认不紧缩同样只涨(与 rust 默认档同型,不另立)。rust 全仓无一处 SegmentedDevice::new 分段装配(grep 全仓仅 single_file),即 C# 的磁盘上界机制整体缺席。


发现2 冷分层键的后台降阶评估默认不可达,树文件 + 页缓存 + 句柄按「曾升阶键数」累积

缓变机制
分层键从死区之下回归内存信封并释放 {stem}.bftree 的唯一冷路径是 tiered_demote_round,它挂在 expired-object-collection-freq 周期任务内;该槽位默认 0 = 任务不拉起。前台懒降阶只覆盖被写触碰的键;删空自愈只覆盖清空的键。于是「升阶过、随后删到双维死区以下、之后无人再碰」的键: 独立树文件、bf-tree 页缓存环、每实例一个 fd 三件套全部常驻,哪怕数据只剩几十条。负载若以「临时大集合建-缩-忘」为常态(排行榜/临时聚合),残留集随时间只增不减。

证据
wnode/src/primary_tasks.rs:336-343 demote 轮宿主与 freq<=0 即退出
wnode/src/resp/objects/tiered_demote.rs 模块头「升阶后再无写入的冷分层键须由后台周期评估回收,否则迟滞死区之下的分层态永不回归内存信封」
wconf/src/runtime_server_config.rs:297 ExpiredObjectCollectionFreq 默认 0
wconf/src/node_options.rs:529 --expired-object-collection-freq 默认 0
wbftree/src/manager/mod.rs 树文件命名/快照路径({stem}.bftree 每实例一文件)

变坏时间量级
视冷化大集合的产生频率,周~月量级可见 fd/文件数缓涨;与 r6-mem「升阶树页环无总闸」互补——r6-mem 立的是单环无预算,本条立的是释放触发默认不可达导致的按键累积(文件+fd+环三件套)。运维开启 expired-object-collection-freq 即可闭合,属「默认值 vs 架构承诺」缺口而非机制缺失。

C# 对位
无对应物: C# 集合恒驻对象域内存,无 per-key 文件/fd/页环,也无降阶概念(garnet 无此架构)。C# 对象删除后容器容量不缩属同型内存缓变,但无磁盘与句柄驻留面。


发现3 停机排空用墙钟差做裸 u64 减法,时钟回拨下提前强杀滞留连接或 debug panic

缓变机制
dispose_active_handlers 以 now_ms() 记起点,循环内 now_ms() - begin >= 5000 判超时,u64 裸减无 saturating。长跑期间 NTP 校正回拨使差值回绕: release(溢出检查关)回绕成巨数恒判超时,5 秒排空窗口瞬间作废、滞留连接被立即强收(本意是留痕后强收,时序前提被破坏);debug 构建直接 panic,叠加 r2 已立的 panic=abort 即全进程崩。仅停机路径,低频但真实。同文件 age 计算已是 i64+max(0) 防护,唯此处漏防。

证据
wnode/src/servers/consumer_registry.rs:608 let begin = now_ms()
wnode/src/servers/consumer_registry.rs:617 now_ms() - begin >= DRAIN_TIMEOUT_MS

变坏时间量级
30 天内 NTP 步进回拨概率不低;仅在停机排空瞬间命中才显形,属低频高危边角。

C# 对位
libs/server/Servers/GarnetServerBase.cs:172/:185 DisposeActiveHandlers 用 Stopwatch.ElapsedMilliseconds,单调域,无此面。


无增量确认(已核实无问题面)

慢日志: Mutex<VecDeque> 环形裁剪有界;id AtomicI64 as i32 截断回绕与 C# int 同型(garnet libs/server/Metrics/Slowlog/SlowlogEntry.cs:11),不立。
指标: SlowLogContainer/GarnetServerMonitor 的 history/global 均为可复位聚合,无按时间堆积的历史序列;延迟直方图定容。
订阅: 三表退订/会话释放 remove_subscription/dispose 清理闭环(wpubsub/src/subscribe_broker.rs:153/:599),邮箱有界丢尾+计数可观测(wpubsub/src/subscriber.rs:98)。
脚本缓存: 会话缓存随连接析构;全局 StoreScriptCache 不封顶为 C# storeScriptCache 同型继承行为,不立。
复活池: 13 桶 × 256 槽定容,min_address 惰性清陈旧槽,truncate 后 purge_below 单点收口(wreviv/src/pool.rs),无长期膨胀。
WATCH/迁移/gossip: WatchVersionMap 定长桶数组;MigrateSessionTaskStore 16384 槽定容有删;gossip 连接表按集群规模、ban list 每轮 cleanup_ban_list 条件删除(wedb/src/server/cluster_manager.rs:639)。
检查点保留: 单机拍新删旧 2 代(CHECKPOINT_RETAIN_GENERATIONS);集群 CheckpointStore delete_outdated_checkpoints 读者闸门淘汰,safely_remove_outdated 恒 true(wedb/src/server/replication/replication_manager.rs:182),无段数缓慢增长面——单文件装配下根本无段(发现1 的另一面)。
计数回绕: epoch u64 fetch_add、全局事务版本 u64、复制 offset i64 地址、checkpoint token u128 带回退地板(wcpr/src/manager/mod.rs:348 next_token_above)、监视迭代 u64,长跑均无实际回绕面;watch_version i64 fetch_add 同理。
时钟域: TTL/过期域 now_ticks(实时)与延迟域 now_stopwatch_ticks(单调 Instant)分离,wbase/src/time.rs 有明确域禁令;coarsetime recent_since_epoch 粗缓存钟全仓零使用,无 10ms 粒度漂移入 TTL 路径;月级 TTL 判定为实时域,C# DateTimeOffset.UtcNow 同型继承。
fd 配对: wdev TLS 句柄表按失效戳 reconcile 驱逐+当场 close,树文件经 dispose/Drop 关闭(wbftree/src/service/mod.rs:183),单文件装配下常驻 fd 恒定(排除发现2 冷树文件);directory fsync 新建段有补。
成员过期账本堆陈旧项惰性清理、对象信封 AOF 全量重灌写放大,均 C# 同型继承,不立。

视角结论: 有增量
