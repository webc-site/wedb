# whlog : HybridLog 混合日志

## 项目介绍

whlog 提供 Garnet Tsavorite 风格的 HybridLog 混合日志分配器：基于 wdev 设备 + wepoch 纪元保护的 64 位逻辑地址空间、环形页缓冲与三区滑动状态机。

三区划分：`[read_only, tail)` 可变区（原位更新）、`[head, read_only)` 只读区（内存驻留，更新走追加）、`[begin, head)` 磁盘区。边界状态机约束 `0 <= begin <= safe_head <= head <= safe_read_only <= read_only <= tail`，且 `head <= flushed_until <= tail`（已从内存驱逐的数据必已落盘）。

## 模块组成

- `address`：`AddressManager`，7 个 AtomicU64 边界 + `AddressSnapshot` 可编码快照
- `buffer`：`CircularPageBuffer` 环形页缓冲池，每页独立 RwLock 细粒度并发
- `config`：`HybridLogConfig`；RO 滞后比例定点化（20 位小数），热路径零浮点
- `flush`：`PageFlushRange` + `PendingFlushList`，相邻区间贪心合并与完成跟踪
- `hlog/`：HybridLog 主体——append（追加与换页）、inplace（原位更新 / 墓碑 / RMW / 复活）、io（读路径与批量刷盘）、shift（三区边界滑动与截断）
- `scan`：`ScanIterator` 混合扫描迭代器，零拷贝条目视图
- `output`：`RecordOutput`（Memory / Disk 零拷贝解构）

## 核心 API

- `HybridLog<D: Device>`：new(config, device, epoch)、append、try_update_in_place、try_mark_tombstone_in_place、try_modify_record_in_place、try_modify_record_with_slack、try_revivify_in_chain、revivify_record_at、with_memory_record（同步零拷贝内存点读，未驻留返回 Ok(None) 供降级冷读）、read_record / read_disk_record（async）、flush_page / flush_pages_range / sync / flush_all（async）、iterate_version_chain、recover（async）、scan / scan_iter（推 / 拉模式区间扫描）、shift_read_only_address / shift_read_only_to_tail、shift_head_address、shift_begin_address（async 截断：先冻结只读并补刷 [flushed_until, new_begin)，head 内联推进且 safe_head 经纪元排空跟进、begin 直接推进，最后物理截断设备历史段）、begin_address 等
- `AddressManager` / `AddressSnapshot`
- `CircularPageBuffer`
- `HybridLogConfig`：`DEFAULT_PAGE_SIZE = 64KiB`、`DEFAULT_NUM_PAGES = 16`、`DEFAULT_MUTABLE_FRACTION = 0.5`、`DEFAULT_INITIAL_ADDRESS = 64`、`SECTOR_ALIGNMENT = 4096`、`ro_lag_num_from_fraction()`
- `PendingFlushList` / `PageFlushRange`、`RecordOutput`、`ScanIterator`、`PAD_KEY_LEN`

## 设计要点

- 边界推进：Unsafe 边界先行发布，Safe 边界经纪元排空（BumpCurrentEpoch + 排空动作）后推进
- 崩溃一致性边界 = `flushed_until` 连续前缀 + 调用方 `sync`；刷盘为调用方驱动的单线程批量刷盘（compio 线程每核模型）
- recover 按 flushed_until 前缀强制清零崩溃残留撕裂记录，恢复可见状态严格限于已持久化前缀
- 页级锁仅换页时获取，页内追加无锁；槽位回绕须同时满足已落盘 + 已驱逐 + 纪元排空，否则返回 `PageNotReady` 重试
- 冷读路径带 2 槽直接映射磁盘页缓存，仅装载整页已刷盘且冻结只读的页；连续性启发式（4096B 小读探测）避免随机负载整页读放大
- ScanIterator 构造时一次性快照 read_only；head / flushed_until 刻意不快照以免丢记录；只读区无锁裸读要求调用线程持 LightEpoch

## 测试覆盖

tests/ 覆盖：追加与内存读、原位更新与保护、换页填充、刷盘与冷盘读、RCU 版本链、待刷盘合并、批量刷盘 Direct I/O、混合磁盘内存扫描与早停、recover 快照不变量、恢复续写、shift_read_only_to_tail、并发追加压测、环形回绕驱逐、inplace 生命周期、revivify + Pad、Begin 截断、非持久前缀清洗、残片 Pad、配置校验、冷读精确裁剪、磁盘页缓存与自适应装载、陈旧区间与短写、多段恢复窗口。
