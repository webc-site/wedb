甄别结论：通过（甄别席 J6，2026-09-27，定级 P3——升阶采样先于纪元门入，扩容缝纯性能面）。亲验：index 采样 :1013 先于 enter_gated（:1015），冷读臂外层无守卫（read.rs:1095-1102、batch.rs:192 区 drop(guard) 后调 read_from_disk），扩容缝真实（resize.rs:506-510 先 store 新表后发相位）；:811 对照臂在调用方守卫内形态不一致；复制快照 reader-pin 已合入票与本面正交。C# TryCopyToReadCache 在 InternalRead ephemeral 段（:179 区）成立。落地须守卫与 index 同层提升（审核席坑点提示有效，pump_close_barrier :1027-1031 块外引 &index 亲见）。派沙箱席 c01l。

审核通过（2026-09-27 独立方案审核）：锚点逐点亲验属实——read.rs:1013 采样先于 :1015 enter_gated，且冷读臂外层无守卫（read.rs:1100 与 batch.rs:192 均在调 read_from_disk 前释放守卫）；resize.rs:506 先切表 :507-510 后发相位，缝隙真实（grow_index 跑 spawn_blocking 阻塞线程，采样至进守卫间可整体插入一次完整扩容）；:811 对照臂处于调用方守卫内（read.rs:598 契约，:350/:478/:875/:1094/batch.rs:147 调用链均在守卫下）形态不一致成立；C# 对位 InternalRead.cs:179 与 ContinuePending.cs:186 两处 TryCopyToReadCache 均在 FindTag 装载 hei 的同一 ephemeral 窗（finally EphemeralSUnlock 收尾）。方案可落，落地注意一点：pump_close_barrier 在守卫块外引用 &index（:1029），字面单行交换会破作用域，须提升守卫与 index 同层（如去内层块，对位 :811 臂守卫内采样形态）；val_slice 借自池化设备缓冲（whlog/src/hlog/io.rs:163）非纪元页，挪位无借用冲突。

异步冷读回填臂 RC 晋升索引句柄采样在纪元守卫外，扩容缝可令晋升落入退役旧表

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）
C# TryCopyToReadCache.cs 的 hei 在 InternalRead 全程 ephemeral 保护段内装载（OperationStackContext 钉版本），索引句柄采样与 RC 挂载同保护窗，不存在跨窗退役表写入面。
2. 工程现状确证（Rust 现有实现路径与代码缺陷）
wkv/src/session/raw/read.rs:1012-1023 异步冷读回填臂：let index = self.store.index.load_full() 在 enter_gated() 守卫块之前（:1013 采样、:1015 进守卫）；磁盘 IO 完成恢复执行至重入守卫之间无纪元保护，一次完整扩容可整体插入该缝隙，随后 read_cache.append(key, val, &index, ...) 把 RC 晋升条目 CAS 进已退役旧表。同文件 :811 promote_immutable_read_hit 臂处于调用方守卫内无此窗，形态不一致。
3. 逻辑危害确证（并发/数据丢失/Panic/资源泄露等实际危害）
纯性能面无正确性危害：晋升丢失（下次读再落盘）+ RC 环内孤儿记录占位至页关闭自然回收；主日志链与前驱指针不受影响。C# 对位形态要求采样与挂载同守卫窗。

涉及代码：
rust 文件与函数：
wedb/wkv/src/session/raw/read.rs:read_from_disk 回填臂

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/TryCopyToReadCache.cs:TryCopyToReadCache（ephemeral 保护段内装载）

精炼执行方案：
1 index 采样移入 enter_gated 守卫块内（一行顺序修正），对位 C# 同窗形态
2 测试验证点：扩容注入窗内冷读回填后活跃表可命中 RC 晋升条目（或以现有 RC 回归族加扩容并发用例）

收口记录（收票席 R5 批次，2026-09-28）：合入 4b11a642（验货 3e745ccd/d6f3381f+merge dev 36 提交复查 33+7 定向全绿零警）。收口形态=冷读回填臂去内层块、enter_gated 与 index.load_full 同层提升（对位 :811 守卫内采样臂与 C# InternalRead/ContinuePending ephemeral 同窗装载 hei），泵前 refresh 等形补强，pump_close_barrier 同罩守卫（Some(participant) 双臂口径允许持钉到达），batch.rs 汇入同一回填内核零改动单点对齐。锁测 rc_grow_eviction.rs cold_read_backfill_promotes_into_new_table_across_resize（会合硬同步扣停采样点+PrepareGrow 切表先行，回退 checkout read.rs 实测双断言即红×2，5/5 稳定）。deviations 无需。
