甄别结论：通过（甄别席 zc-fix-r25-甲，2026-09-26）定级 P1
核验记录（现码复跑，非票面背书）：
1 不对称处置现码亲验：aof_replication_pump.rs 扫描臂 `iter.next_frame().await.map_err(io::Error::other)?`（:289 区）仍以 ? 抛离 pump_backlog；泵循环 Err 臂仅 log::warn 后 break（:173-176）——不 try_remove_current、不 dispose、wire 保持；对照同函数 consume 败臂 warn+skipped+try_remove_current+break（:298-313 现读在位）——错误码面处置分叉未灭失。
2 钉死链复验：aof_sync_driver.rs fold_min_addresses（:441-453）对在册驱动取 min、safe_truncate_aof（:469-476）safe_limit.min_exchange(&min_active）、publish 面 min_shipped（:498-506）在位——冻结驱动确实钳死截断线与背压闸门；throttle_all 退场臂依赖 is_connected 现读 :96-97（all tasks connected 判定），TCP 健康时不可达，全仓无逐驱动停滞看门狗，无自愈属实。
3 C# 锚亲验：AofSyncDriver.cs RunAsync catch(Exception)+finally aofSyncDriverStore.TryRemove(this)（:166-177 现读原文）统一退场契约坐实；rust try_remove_current（:415 区）本有同款重报形态，方案仅把扫描错误臂并入既有退场通路，零新机制、不动泵外文件，compio 兼容。
4 查重：deviations.md grep aof_replication_pump/pump_backlog/扫描错误 零命中；§29/§95/§96/§99/§116 与 r16-repl 裁定均未覆盖本出册面（审核席 r25 逐一裁正交，本席复核全册无「扫描错误臂驱逐」在册）；四池无同轴票。
5 格式与可执行度：纯文本、双侧路径齐全；方案三点（就地处置+重报+continue 下一驱动、死 Err 臂同步清理防死分支、fake Device 位点注入三断言）最小闭环。定级 P1：持续性介质错下截断线/闸门被单副本钉死放大为主端写停顿（审核席订正口径维持：瞬时 I/O 错下轮自愈，非无条件挂死不入 P0；无直接丢键非协议错不入 P2）。

审核结论：通过（r25 审核席；P1 维持但表述订正——瞬时 I/O 错下轮扫描可自愈，"永久钳死"仅对持续性介质错/CRC 损坏成立，一旦成立即不可自愈需人工干预。全部锚点亲验属实：pump :289 ? 号抛离、attach_wake Err 臂仅 warn+break、throttle_all 断连臂对健康 wire 不可达、脉冲臂 accepted<tail 静默跳过、全仓无停滞看门狗、safe_truncate min 钳制与闸门水位全量取小不过滤健康态、C# catch+finally TryRemove 双侧非同形；查重 §29/§95/§96/§99/§116 与 r16-repl 裁定 4/5/8 均未覆盖扫描错误臂出册面）

审核裁定执行方案（供 fix 直接消费，替代文末原方案；注意收口后 sync_backlog/attach_wake Err 臂成死分支须同步清理，勿留"错误臂仍在册"误判；瞬时错亦即出册断链系与 C# catch-all 同形的代价，码注声明）：
1. pump_backlog :289 将 ? 收为就地处置：warn（含 remote_node_id 与错误位点，口径对齐 :301-306 consume 臂）→ store.try_remove_current(driver)（既有臂 :415-430 已含 dispose+重报）→ skipped+=1 continue 下一驱动；泵签名收 Ok 化，死 Err 臂同步收敛。
2. 不新建监控不建第二退场机制：出册→断链→副本感知→ensure_replication 重同步全走既有通路，不触泵外文件。
3. 测试：注入指定位点持续 read_range Err 的 fake Device（位点先 flush 出内存窗，仿 whlog flaky_device 形），双健康驱动同册，断言三点——错误驱动出册且另一驱动同轮续转；重报后 safe_truncate_aof 越过原冻结位点；预算耗尽下 wait 方在出册后即放行（对照既有 backpressure 测试形）。

主端推流泵扫描错误臂不驱逐副本驱动，僵尸在册位点永久钉死 AOF 截断线与背压闸门致主端整体写停顿

问题分析：
1. Garnet 契约对齐：C# 原型每副本一条常驻推流任务 AofSyncDriver.RunAsync（garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:147-178）以 catch Exception（:166-169）加 finally aofSyncDriverStore.TryRemove(this)（:170-177）作为统一退场收口；底层扫描推流主体 AofSyncTask.RunAofSyncTaskAsync（同目录 AofSyncTask.cs:306-363）在 :327 建扫描迭代器、:335-339 执行 BulkConsumeAllAsync，扫描推流任何异常在 :341-344 记日志后终止任务本体，finally :345-350 调 garnetClient.Dispose 断开副本连接。即契约形态为：推流链路出现任何致命错误（含 AOF 扫描 I/O 错误），该副本驱动必然出册并断链，副本端感知断连走重同步恢复；出册后经 PublishShippedAddresses 重报水位，截断线与闸门不再被该副本钳制，故障面严格隔离在单副本。
2. 工程现状确证：rust 侧泵体 pump_backlog（wedb/wedb/src/server/replication/aof_replication_pump.rs:256-329）对同类链路两类错误处置不对称。consume 错误臂记日志、计数并 store.try_remove_current 出册（:298-313），与 C# 一致；而扫描迭代器错误臂 iter.next_frame().await.map_err(io::Error::other)?（:289）经 ? 号把错误直接抛离 pump_backlog，attach_wake 常驻循环（:162-181）Err 臂仅 log::warn 后 break（:173-176）等下一次信号再泵——既不 try_remove_current、不 dispose 驱动、wire 保持连接，肇事驱动持续在册且 previous_address 与 shipped_watermark 冻结在错误位点。next_frame 残余可达 Err 面均为 waof 扫描迭代器（wedb/waof/src/wal/iterator.rs）排除一切平滑终止分支后的真致命路径：fetch_window 设备读错误（段被并发删且 offset < begin 的截断竞态已收口 Ok(None) 平滑终止 :329-333，其余设备 I/O 错误上抛 :334）、内存窗内真实损坏 CrcMismatch 上抛（:319）、磁盘权威面 header.verify CRC 校验失败上抛（:378/:394）。此类错误同一位点每轮必复现，驱动永不推进；throttle_all 周期退场臂（wedb/wedb/src/server/replication/aof_sync_driver.rs:583-594）仅在 is_connected 为假时出册，链路 TCP 健康恒真，全仓无逐驱动错误计数或停滞看门狗，无任何自愈路径。
3. 逻辑危害确证：驱动在册且位点冻结后两害叠加。其一，safe_truncate_aof 截断水位被活跃副本最小 previous_address 钳制（wedb/wedb/src/server/replication/aof_sync_driver.rs:469-491，min_exchange :473-476，对位 C# AofSyncDriverStore.cs:71/:125 同款钳制），主端截断点被永久钉死在冻结位点之下，AOF 段文件永不物理回收，磁盘无界增长。其二，背压闸门发布水位被活跃副本最小 shipped_watermark 钳制（wedb/wedb/src/server/replication/aof_sync_driver.rs:497-528 写入 wnode/src/aof/aof_backpressure.rs:269-272），日志尾随写入持续拉大尾差，一旦超过每子日志预算，经 GarnetLog 入队背压等待（wedb/wnode/src/aof/garnet_log/single_log_branch.rs:19-42 调 aof_backpressure.rs wait/wait_slow :160-225 有界 park 循环）的全部后续追加永久挂起——单副本的一处读故障被放大为主节点全量写面冻结，且该副本复制流静默停摆、不判 divergent、不触发重同步。C# 同故障经 RunAsync finally TryRemove 加 garnetClient.Dispose 即时隔离单副本退场、副本重连重协商即恢复，主端截断线与闸门经出册重报（rust try_remove_current 本有同款重报 :424-425）不受扰动，rust 该臂属真实行为分叉而非等价转写。

涉及代码：
rust 文件与函数：
wedb/wedb/src/server/replication/aof_replication_pump.rs:pump_backlog（next_frame 错误 ? 上抛臂）、AofReplicationPump::attach_wake（Err 仅 warn 加 break 臂）
wedb/wedb/src/server/replication/aof_sync_driver.rs:AofSyncDriverStore::safe_truncate_aof、publish_shipped_addresses、throttle_all、try_remove_current
wedb/waof/src/wal/iterator.rs:WalScanIterator::next_frame、fetch_window、mem_decode_failed
wedb/wnode/src/aof/aof_backpressure.rs:AofBackpressure::wait、wait_slow、publish_shipped_address
wedb/wnode/src/aof/garnet_log/single_log_branch.rs:GarnetLog::backpressure_wait_key、backpressure_wait_vector

对应 c# 文件与函数：
garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs:AofSyncDriver.RunAsync（catch 加 finally TryRemove 统一退场）
garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncTask.cs:AofSyncTask.RunAofSyncTaskAsync（扫描推流异常捕获终止、finally garnetClient.Dispose）
garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriverStore.cs:AofSyncDriverStore.SafeTruncateAof、TryRemove、PublishShippedAddresses
garnet/libs/server/AOF/AofBackpressure.cs:AofBackpressure.Wait、PublishShippedAddress

精炼执行方案：
1. pump_backlog 内将 next_frame 错误从 ? 上抛收敛为逐副本就地处置：扫描错误时记 warn（含副本节点与错误详情，口径对齐同函数 consume 错误臂），调 store.try_remove_current(driver) 出册（实例匹配防误删并发重挂新驱动；其内已含 dispose 断链加 publish_shipped_addresses_to_gate 重报，副本端感知断连触发重同步），skipped 计数后 continue 下一个驱动而非中断整轮；PumpGuard 经 Drop 正常释放单飞闸。
2. 不新建第二套恢复机制：出册后位点交接与重同步完全由既有链路承接——副本断连感知、ensure_replication 重协商、trunc_floor 与 sameHistory 钳位、DataLossCheck 降级全量均为现存闭环（r16 已判净），本单改动不触及泵外任何文件；sync_backlog 的 io::Result 外签名保留（attach 期失败仍由调用链 try_remove 兜底）。
3. 测试验证点：以在指定位点持续返回读错误的 fake Device（对照 iterator.rs fetch_window 上抛形态）注入，同册挂载两个健康副本驱动，断言三点：其一，扫描错误即该驱动出册、另一驱动同轮继续转发不受影响；其二，出册重报后 safe_truncate_aof 截断点可越过原冻结位点、闸门水位恢复推进；其三，预算耗尽场景下入队方在出册后即获放行（对照 wnode 既有 aof_backpressure 测试形）。

合入哈希：8bf5553（`git merge fix-aofpump` fast-forward 至 dev）收口形态：pump_backlog 扫描错误臂收为就地 warn+try_remove_current 出册+continue 并入既有退场通路，泵签名 Ok 化、attach_wake/sync_backlog 死 Err 臂同步收敛，tests/aof_pump_scan_error_eviction.rs 三点回归（出册续转/截断线越过冻结位点/闸门等待放行）
