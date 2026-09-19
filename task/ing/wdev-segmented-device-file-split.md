优先级：低（全库最大单文件 1437 行，多职责混聚）
来源：next/agy.db.md 条 8。核销 2026-09-19，取证基线 = 主仓 dev 当下 HEAD。

结论一句话
wdev/src/segmented_device.rs 一个文件同时承担路径与句柄池、跨段寻址与对齐、读写主体、
同步刷盘、截断删除、目录恢复扫描、Device trait 实现与 debug 探针面共 1437 行；
按域拆子模块、结构与 trait 实现留门面，纯搬移零语义改动。

现状（主仓 HEAD 实测，wdev/src/segmented_device.rs 共 1437 行）
1. 结构与参数：:60 pub struct SegmentedDevice、:181 pub struct DeviceParams、
   :148 impl Iterator for SegmentEntries。
2. 路径与句柄域：:329 parent_dir、:337 segment_path、:356 segment_entries、
   :390 open_options、:401 try_preallocate、:422 open_file、:497 get_or_open_file、
   :716 handle_capacity、:745 retry_pending_removes。
3. 寻址与对齐域：:373 get_segment_and_offset、:644 within_single_segment
   （扇区对齐校验与跨段切片已单点在 wdev/src/chunk.rs，:32 引入 SegmentChunks /
   segment_mask / segment_shift / validate_aligned_io；读写主体消费点在 :1022 与 :1204-1206）。
4. 同步与截断域：:570 get_file_size、:590 remove_segment、:608 reset、:799 sync、
   :808 sync_data、:813 sync_internal、:664 recover（目录恢复扫描）。
5. IO 主体：:965 read_impl 起（含 :1073 起 impl Device for SegmentedDevice 的转发面）、
   写路径在 :1204 起区段；debug 探针 :913-965（debug_mark_dirty / debug_verify_synced /
   debug_clear_segment / debug_dirty_segments）；内联测试 :1365 起。

C# 参考
1. libs/storage/Tsavorite/cs/src/core/Device/StorageDeviceBase.cs（基类：段映射、命名、
   段生命周期与 OrigFile/BackupFile 语义）
2. libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs（591 行：异步读写与
   句柄使用；ManagedLocalStorageDevice.cs 是其流式变体，本仓不双套实现）
3. 即 C# 的「基类持命名与段映射 / 派生件持 IO」分域，是 rust 拆分的对位依据；
   本仓把三者合成一件是历史堆叠而非对标结论。

修法
1. 目录化 wdev/src/segmented_device/ 或平铺新件（先 ls wdev/src 取该 crate 现有 grain，
   与 chunk.rs 等平铺件保持同一风格，勿为拆而拆出唯一的一个目录模块）：
   句柄与路径域、跨段读写域、截断与刷盘域、恢复扫描域各自成件，
   SegmentedDevice 结构体 + impl Device for SegmentedDevice 留在现门面件（对外 re-export 不变）。
2. 各域以 impl SegmentedDevice 分部实现承载（Rust 同 crate 多文件 impl 是本仓惯例，
   见 wkv/src/store/*.rs 对 WedbStore 的分域 impl），跨域私有项统一降为 pub(crate)，
   禁为拆分而把内部件升为 pub。
3. debug 探针面（:913-965）若已有 cfg 门（grep 现状确认 debug_assertions / 自定义 cfg），
   搬移时保持门控不变；禁新增 allow 属性。
4. 本票不做算法改写：对齐/分片仍走 chunk.rs 单点，get_segment_and_offset 语义不动
   （它与 SegmentChunks 不是重复件，前者是单偏移寻址、后者是区间切片迭代器）。

验收判据
1. wdev/src/segmented_device*（含目录件）内单文件 ≤500 行，且域间无重复实现
   （SegmentedDevice::open_file、SegmentedDevice::get_or_open_file、
   SegmentedDevice::sync_internal、SegmentedDevice::recover 各一处定义）。
2. wdev 对外导出面（Device trait 实现 + SegmentedDevice 的 pub 成员名集合）逐符号不变，
   wkv/whlog/waof 调用点零改动。
3. diff 只呈现搬移与 use 调整（无逻辑行改写）。
4. cargo check 通过（禁在共享 target 跑 test.sh / clippy.sh）。

双花登记
并发代理就条 8 另立同题薄票 next/db-wdev-segmented-device-split.md（自称已与 next/muse.db.md 条 13
两轮同题合并），与本票同改 wdev/src/segmented_device.rs，两票只取一棒：本票为正文载体，
派发时以本票为准并删除该薄票，禁双花。
排棒次序：task/done/wdev-segmented-device-mutator-knobs.md（已归档，主仓 commit 7292fc1）先前已搬动
同文件，本票内行号须按当下 HEAD 重新定位后再拆，勿照抄本票行号。
