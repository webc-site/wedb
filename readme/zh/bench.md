# 嵌入式键值存储性能评测

<p align="center">
  <img src="https://fastly.jsdelivr.net/gh/webc-fs/-@oL/kFWlZarGKUXPwhajrMyA.svg" alt="嵌入式键值存储性能评测" width="100%">
</p>

| 指标 | [wkv](https://github.com/webc-site/wedb/tree/main/wedb/wkv) | [wbftree](https://github.com/webc-site/wedb/tree/main/wedb/wbftree) | [redb](https://github.com/cberner/redb) | [fjall](https://github.com/fjall-rs/fjall) | [rocksdb](https://github.com/facebook/rocksdb) | [sqlite](https://www.sqlite.org) |
|:---|---:|---:|---:|---:|---:|---:|
| 随机点查 | **9.83X**<br>**316 M/s** | 2.05X<br>65.7 M/s | 1.53X<br>49.0 M/s | 1.36X<br>43.9 M/s | 1.00X<br>32.1 M/s | 1.39X<br>44.5 M/s |
| 范围扫描 | N/A | 7.46X<br>532 M/s | **15.34X**<br>**1.07 G/s** | 2.26X<br>161 M/s | 3.08X<br>220 M/s | 1.00X<br>71.3 M/s |
| 8 线程随机读 | 1.95X<br>41.1 M/s | 7.65X<br>161 M/s | 4.89X<br>103 M/s | 6.14X<br>129 M/s | **7.67X**<br>**162 M/s** | 1.00X<br>21.1 M/s |
| 4 线程随机读 | 2.12X<br>62.0 M/s | **4.28X**<br>**125 M/s** | 2.97X<br>86.7 M/s | 3.27X<br>95.6 M/s | 3.92X<br>115 M/s | 1.00X<br>29.2 M/s |
| 16 线程随机读 | 1.82X<br>40.5 M/s | 6.11X<br>136 M/s | 3.68X<br>82.1 M/s | 5.37X<br>120 M/s | **6.35X**<br>**142 M/s** | 1.00X<br>22.3 M/s |
| 32 线程随机读 | 1.91X<br>42.1 M/s | 5.74X<br>126 M/s | 3.73X<br>82.1 M/s | 5.36X<br>118 M/s | **7.51X**<br>**165 M/s** | 1.00X<br>22.0 M/s |
| 记录总数 | 60.92X<br>30ms | **1803869.62X**<br>**0µs** | 1169824.66X<br>1µs | 1.20X<br>1509ms | 1.00X<br>1804ms | 27.10X<br>67ms |
| 单条逐笔写入 | **198.88X**<br>**723 M/s** | 19.10X<br>69.5 M/s | 2.03X<br>7.40 M/s | 1.00X<br>3.64 M/s | 15.37X<br>55.9 M/s | 1.18X<br>4.29 M/s |
| 批量导入 | **21.91X**<br>**613 M/s** | 3.94X<br>110 M/s | 1.00X<br>28.0 M/s | 4.83X<br>135 M/s | 7.93X<br>222 M/s | 1.86X<br>52.0 M/s |
| 事务批量写入 | **134.83X**<br>**572 M/s** | 17.48X<br>74.1 M/s | 2.67X<br>11.3 M/s | 36.21X<br>154 M/s | 53.09X<br>225 M/s | 1.00X<br>4.24 M/s |
| 异步批量写入 | **152.79X**<br>**585 M/s** | N/A | 1.99X<br>7.63 M/s | 11.95X<br>45.8 M/s | 11.48X<br>44.0 M/s | 1.00X<br>3.83 M/s |
| 记录删除 | 66.17X<br>297 M/s | 33.41X<br>150 M/s | 1.00X<br>4.49 M/s | 51.03X<br>229 M/s | **66.93X**<br>**301 M/s** | 1.97X<br>8.84 M/s |
| 整理前磁盘占用 | **1.10 GiB** | 1.51 GiB | 2.01 GiB | 1.58 GiB | 1.38 GiB | 1.31 GiB |
| 整理后磁盘占用 | 1.10 GiB | N/A | 1.55 GiB | 1.58 GiB | **1.06 GiB** | 1.31 GiB |
| 内存占用 | 236.64 MiB | **153.86 MiB** | 266.81 MiB | 377.78 MiB | 1.37 GiB | 687.33 MiB |

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

