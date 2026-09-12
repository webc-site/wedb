# wdev : 块存储设备层

## 项目介绍

wdev 提供段文件设备（`SegmentedDevice`）、Direct I/O 与设备抽象（`Device`），是 whlog / waof 等上层日志引擎的持久化底座。

设计基于 compio 异步运行时：Linux 走 io_uring，Windows 走 IOCP，macOS 走 kqueue。设备以段（segment）为单位组织文件，段大小为 2 的幂且不小于 sector_size；单段上限 `MAX_SEGMENT_SIZE = 2^62`。段文件命名为 `<base>.<段号>`，段号为 13 字符定长小写 Base32（转写规范偏离 C# 十进制），文件名字典序与段号数值序严格一致。

## 模块组成

- `device`：设备抽象 trait `Device`，定义扇区尺寸、段尺寸、Direct I/O 与读写接口
- `segmented_device`：段文件设备实现，句柄按（设备编号， 段号）Thread-Local 持有 `Rc<File>`（零跨核争用；papaya 仅 Windows 延迟删除队列参与编译）
- `chunk`：扇区与分段切片计算，跨段 / 单段 I/O 边界切片迭代与对齐校验
- `null`：`NullDevice` 空设备，I/O 即时假成功、零物理 I/O
- `sys`：跨平台零依赖硬件探测（CPU 核数、系统内存），探测失败回退 `FALLBACK_CPU_CORES = 4`、`FALLBACK_SYSTEM_MEMORY_BYTES = 4 GiB`
- `error`：错误类型（对齐 / 越界 / 段不存在等 I/O 参数校验错误族）

## 核心 API

- `Device`：设备抽象；`write_aligned` / `read_aligned` 要求 offset / len 为 sector_size 整数倍且缓冲区地址对齐，`read_range` 便捷读取无对齐要求（缓冲 I/O 模式按逻辑范围精确直读）
- `SegmentedDevice`：段文件设备；`dir_sync_count()` 计数器可观测父目录 fsync 次数
- `NullDevice`：测试与基准用空设备
- `detect_cpu_cores()` / `detect_system_memory()`：硬件探测
- 另导出 `wbase::BufferPool` 供调用方直接构建对齐缓冲

## 设计要点

- 持久化契约：`sync` / `sync_data` 为全局屏障，语义对齐 C# `LocalStorageDevice` 的进程级共享句柄表——任一线程调用即覆盖调用发起前全部线程已完成的写入（他线程写入段由调用线程就地补开刷新，句柄永不过线程）；debug 构建以设备级 `dirty_segs` 守护位图（前 128 段）校验契约，release 零成本
- 句柄生命周期：句柄 Thread-Local 持有，随该线程截断驱逐、`reset` 显式清理或线程退出回收；长生命周期工作线程弃用设备前应调用 `Device::reset`（fd 占用上界：线程数 × 在册段数）
- 目录项持久化：新建段时同步 fsync 父目录（Unix），"新段写入 + sync 即持久"同时覆盖段数据与目录项；Windows 平台不支持
- Direct I/O 定型：Linux 首个段打开时探测定型，定型后失败直接上抛，运行中不回退
- 对齐要求：offset / len 须为 sector_size 整数倍，segment_size 须为 2 的幂且 ≥ sector_size

## 测试覆盖

tests/device/ 覆盖：对齐与非法参数、跨段读写 round_trip、边界与溢出防御、sync 持久化与跨线程 sync 契约（幽灵段防御、删段/截断免责）、目录 fsync 生命周期、段恢复与不匹配检测、定长 Base32 段名字典序保序、截断与 reset、容量逐出（分段与单文件有界）、32/64 并发与冷打开竞态、多 OS 线程共享运行时、null 设备。
