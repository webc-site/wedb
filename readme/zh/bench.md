# 嵌入式键值存储性能评测

<p align="center">
  <img src="https://fastly.jsdelivr.net/gh/webc-fs/-@2d/Qj7z0MYvCj0qrGO7c0Ow.svg" alt="嵌入式键值存储性能评测" width="100%">
</p>

| 指标 | [wkv](https://github.com/webc-site/wedb/tree/main/wedb/wkv) | [wbftree](https://github.com/webc-site/wedb/tree/main/wedb/wbftree) | [redb](https://github.com/cberner/redb) | [fjall](https://github.com/fjall-rs/fjall) | [rocksdb](https://github.com/facebook/rocksdb) | [sqlite](https://www.sqlite.org) |
|:---|---:|---:|---:|---:|---:|---:|
| 随机点查 | **13.20X**<br>**326 M/s** | 2.89X<br>71.5 M/s | 1.83X<br>45.3 M/s | 1.79X<br>44.3 M/s | 1.00X<br>24.7 M/s | 1.71X<br>42.2 M/s |
| 范围扫描 | N/A | 9.13X<br>618 M/s | **20.90X**<br>**1.38 G/s** | 2.62X<br>177 M/s | 3.60X<br>244 M/s | 1.00X<br>67.7 M/s |
| 8 线程随机读 | 1.92X<br>38.0 M/s | **9.20X**<br>**182 M/s** | 5.21X<br>103 M/s | 7.20X<br>142 M/s | 4.50X<br>89.1 M/s | 1.00X<br>19.8 M/s |
| 4 线程随机读 | 2.31X<br>63.6 M/s | **5.61X**<br>**155 M/s** | 3.41X<br>94.0 M/s | 3.57X<br>98.3 M/s | 2.54X<br>70.1 M/s | 1.00X<br>27.6 M/s |
| 16 线程随机读 | 1.78X<br>41.6 M/s | **6.41X**<br>**150 M/s** | 3.58X<br>83.8 M/s | 5.64X<br>132 M/s | 3.42X<br>80.0 M/s | 1.00X<br>23.4 M/s |
| 32 线程随机读 | 1.85X<br>40.9 M/s | **6.38X**<br>**141 M/s** | 3.77X<br>83.1 M/s | 5.98X<br>132 M/s | 3.13X<br>69.0 M/s | 1.00X<br>22.1 M/s |
| 记录总数 | 63.59X<br>28ms | **1782881.83X**<br>**0µs** | 1188587.89X<br>1µs | 1.13X<br>1579ms | 1.00X<br>1783ms | 27.67X<br>64ms |
| 单条逐笔写入 | **222.94X**<br>**792 M/s** | 25.69X<br>91.2 M/s | 2.22X<br>7.89 M/s | 2.77X<br>9.84 M/s | 15.16X<br>53.8 M/s | 1.00X<br>3.55 M/s |
| 批量导入 | **19.97X**<br>**610 M/s** | 4.14X<br>127 M/s | 1.00X<br>30.6 M/s | 4.90X<br>150 M/s | 8.05X<br>246 M/s | 1.41X<br>42.9 M/s |
| 事务批量写入 | **145.96X**<br>**584 M/s** | 21.61X<br>86.5 M/s | 3.70X<br>14.8 M/s | 19.96X<br>79.8 M/s | 73.62X<br>295 M/s | 1.00X<br>4.00 M/s |
| 异步批量写入 | **168.63X**<br>**582 M/s** | N/A | 2.27X<br>7.85 M/s | 13.72X<br>47.4 M/s | 14.56X<br>50.3 M/s | 1.00X<br>3.45 M/s |
| 记录删除 | 55.64X<br>278 M/s | 34.16X<br>171 M/s | 1.00X<br>5.00 M/s | 42.87X<br>214 M/s | **61.58X**<br>**308 M/s** | 1.68X<br>8.38 M/s |
| 整理前磁盘占用 | **1.10 GiB** | 1.51 GiB | 2.01 GiB | 1.64 GiB | 1.38 GiB | 1.31 GiB |
| 整理后磁盘占用 | 1.10 GiB | N/A | 1.55 GiB | 1.64 GiB | **1.06 GiB** | 1.31 GiB |
| 内存占用 | 236.42 MiB | **154.14 MiB** | 267.77 MiB | 365.23 MiB | 662.50 MiB | 666.91 MiB |

> 性能指标第一行为相对倍数（以同项最低性能为 1.00X 基准，越大越优），第二行为实际磁盘吞吐；随机点查与范围扫描取 3 次运行中位数；各引擎缓存大小尽可能统一配置。

## 评测配置

| 配置项 | 设定值 |
|:---|:---|
| **键大小** | 24 B |
| **值大小** | 150 B |
| **缓存大小** | 128 MiB |
| **基准数据量** | 6000000 |

### 配置参数与统计说明

- **键值规格**：24B 键 / 150B 值，二进制键与结构体载荷。
- **缓存预算**：统一配置 128 MiB，统筹约束各引擎读缓存 (Block Cache)、缓冲池与写缓冲 (MemTable)。
- **负载规模**：全量导入 6,000,000 条记录 (~1.04 GiB 原始写入)，数据量超出内存缓存 10~30 倍。
- **写入场景**：采用 RocksDB 默认持久化 (WAL 开启，内核缓冲，不逐笔 fsync)；评测单条逐笔、事务批量 (1,000条/批) 与异步批量。
- **查询测试**：随机点查 100,000 项，范围扫描 5,000 次 (步长 10)，多线程覆盖 4~32 线程，取 3 轮中位数。
- **空间与内存**：磁盘统计落盘与紧缩后大小；内存记录测试完成后的常驻物理内存 (RSS / Footprint)。

## 测试环境

| 硬件项 | 规格 |
|:---|:---|
| **CPU 型号** | Apple M2 Max |
| **CPU 核心数** | 12 物理核心 / 12 逻辑核心 |
| **系统架构** | aarch64 |
| **内存容量** | 64.00 GiB |
| **磁盘类型** | NVMe SSD |
| **操作系统** | Darwin 26.5.1 |
| **内核版本** | 25.5.0 |

## 存储架构与持久化说明

基准规范：各引擎默认持久化策略统一对标 **RocksDB 官方默认**（启用 WAL、`sync = false`、操作系统页缓存缓冲，进程崩溃安全，不逐笔硬件 fsync；换页、检查点或显式 flush 时刷盘）。

- **[wkv](https://github.com/webc-site/wedb/tree/main/wedb/wkv)**
  - **存储架构**：Append-Only 混合日志环形缓冲 (HybridLog 内存环形页 + 分段文件存储)
  - **默认事务持久化**：写入 HybridLog 内存环形页与日志缓冲，不逐笔调用 flush_all 物理 fsync，满页后台流水线分段落盘
  - **批量导入持久化**：环形缓冲区流式流水线换页，全部写入完成后统一执行 flush_all 与设备物理 sync
  - **严格物理落盘 (可选 Sync 模式)**：可选严格硬件阻塞落盘 (set_sync(true))，每次 commit 执行 flush_all 强制设备物理 sync
  - **数据校验与一致性**：CRC32 全包校验 + Epoch 安全无锁并发保护
- **[wbftree](https://github.com/webc-site/wedb/tree/main/wedb/wbftree)**
  - **存储架构**：缓存页缓冲池 (Cache-Buffer B-Tree 架构)
  - **默认事务持久化**：写入 Cache-Buffer 缓冲池，内存脏页在缓冲池中累积并批量合并调度落盘，不逐笔调用硬件 fsync
  - **批量导入持久化**：批量脏页合并调度落盘，结束统一执行 flush
  - **严格物理落盘 (可选 Sync 模式)**：内存页缓存快速写入，依托缓冲池淘汰与快照落盘
  - **数据校验与一致性**：页面级 CRC32 校验 + Snapshot 只读隔离
- **[redb](https://github.com/cberner/redb)**
  - **存储架构**：写时复制 B-Tree (Copy-on-Write / MVCC)
  - **默认事务持久化**：采用内核缓冲模式 (Durability::None)，数据写入操作系统内核缓存，跳过逐笔物理 fsync
  - **批量导入持久化**：单次大事务批量写入，全量写入完成后统一执行一次 fsync
  - **严格物理落盘 (可选 Sync 模式)**：可选严格硬件阻塞落盘 (Durability::Immediate)，每次 commit 阻塞执行操作系统物理 fsync
  - **数据校验与一致性**：内置页面校验和 + ACID 事务崩溃一致性
- **[fjall](https://github.com/fjall-rs/fjall)**
  - **存储架构**：单写者 LSM-Tree 架构
  - **默认事务持久化**：写入 WAL 内存/内核缓冲 (PersistMode::Buffer)，不逐笔调用磁盘物理 fsync
  - **批量导入持久化**：单写者事务流式追加，仅在全量写入完成后的最终 commit 执行一次物理 fsync
  - **严格物理落盘 (可选 Sync 模式)**：可选严格硬件阻塞落盘 (PersistMode::SyncAll)，每次 commit 阻塞执行操作系统物理 fsync
  - **数据校验与一致性**：数据块 CRC32 校验和 + WAL 崩溃安全恢复
- **[rocksdb](https://github.com/facebook/rocksdb)**
  - **存储架构**：LSM-Tree + 原生 WriteBatch 批量提交
  - **默认事务持久化**：写入 WAL 文件与 MemTable，操作系统内核缓冲 (WriteOptions 默认 sync=false)，不逐笔调用硬件 fsync
  - **批量导入持久化**：WriteBatch 流式分批写入，全量写入完成后的最终 commit 触发落盘并同步
  - **严格物理落盘 (可选 Sync 模式)**：可选严格硬件阻塞落盘 (WriteOptions::set_sync(true))，每次 commit 强制 WAL 物理 fsync
  - **数据校验与一致性**：SSTable 数据块与 WAL 内置 Checksum 校验
- **[sqlite](https://www.sqlite.org)**
  - **存储架构**：标准 B-Tree + 预写日志 (WAL 模式)
  - **默认事务持久化**：采用 WAL 模式官方推荐配置 (PRAGMA synchronous = NORMAL)，WAL 写入由操作系统内核缓冲，不逐笔调用硬件 fsync
  - **批量导入持久化**：单次大事务批量插入，全量完成后通过 wal_checkpoint(TRUNCATE) 刷盘
  - **严格物理落盘 (可选 Sync 模式)**：可选严格物理落盘 (PRAGMA synchronous = FULL)，每次 commit 强制 WAL 硬件物理 fsync
  - **数据校验与一致性**：页面级校验 + WAL 事务原子性与断电回滚保证

