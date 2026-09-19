wkv GC 换号物理回收与过期扫描循环解耦

前提核实（成立）
rust wedb/wkv/src/gc.rs:enabled_by_config 判 cfg.enabled && cfg.scan_interval_ms>0。
rust wedb/wkv/src/config.rs:GcConfig::default 为 enabled:false、scan_interval_ms:0，
默认判否。rust wedb/wkv/src/gc.rs:GcManager::spawn / drive 两循环体在判否时 return 退出，
唯一转调 tick 的入口 run_once 只在循环判真时被调；wedb/wkv/src/store/gc.rs:reconcile_gc_scan
判否时只 stop 不 spawn。故默认部署下 tick 一次不跑。
换号物理回收四面全部只在 tick 内：wedb/wkv/src/gc.rs:tick 调 sweep_vdb 与 try_compact。
sweep_vdb 承担 pop_reclaimable 墓碑注销（wkv/src/vdb.rs:pop_reclaimable）、
pin_routing.remove 退役路由释放、pop_idle_candidates/evict_idle_route 空闲析构；
try_compact 首行 refresh_compact_boost 水位熔断并推进 begin（pop_reclaimable 需 begin 越过 tail_address 方可摘账本）。
唯一无条件常驻的是 wedb/wkv/src/gc.rs:spawn_bftree_reclaimer，只消费 drain_bftree_release
（wkv/src/store/reclaim.rs:drain_bftree_release，bftree 树文件释放队列），不含上述四面。
故默认配置下四面永停，换号墓碑与旧域日志垃圾永驻、退役/空闲路由不析构。前提成立，实施。

C# 对位
每库独立日志下 FLUSHDB 即时 ShiftBeginAddress 物理截断（garnet/libs/server/Databases/
DatabaseManagerBase.cs:FlushDatabase），无后台依赖，默认即生效。rust 共享单日志需紧缩推进
begin 才回收，本应如 C# 般默认生效，却被 gc.enabled（对标 ExpiredKeyDeletionScanFrequencySecs=-1
的扫描开关）顺带关停，属转写引入的错误耦合。

单一机制与改法（只动 gc.rs，最小侵入，不推翻扫描默认禁用、不碰 GcConfig 字段与 compaction_type）
把物理回收内核从 tick 抽出为 reclaim_physical（sweep_vdb + try_compact），tick 转调它，
run_once/drive/spawn/reconcile/start_gc/stop_gc 与全部既有测试语义不变。
扩展既有常驻循环 spawn_bftree_reclaimer：同一 RELEASE_POLL_MS 节拍内，除 drain_bftree_release 外，
经 reclaim_when_scan_idle 兜底推进 reclaim_physical。该方法在 enabled_by_config 为真时直接返回
（此时扫描循环在场，其 tick 已含 reclaim_physical，让位以杜绝双重紧缩），判否时执行物理回收。
净效果：gc.enabled 关闭时由常驻循环回收、开启时由扫描轮回收，物理回收恒被某一驱动推进，
不再随扫描开关关停；gc.enabled 实义只门控「过期键 SCAN 删除」。
修正 spawn_bftree_reclaimer、RELEASE_POLL_MS、try_compact 附近与实现对不上的注释，如实描述新门控边界。

验收
cargo check 零错零警。tests/gc.rs 增一条 enabled=false 下经 reclaim_when_scan_idle
仍弹出到期墓碑、释放退役路由的最小用例。
