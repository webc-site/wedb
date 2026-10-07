# 内嵌键值存储性能评测

下面的表格由 [`bench/`](https://github.com/webc-site/wedb/tree/main/bench) 中与 [`redb-bench`](https://github.com/cberner/redb/tree/master/rocksdb-bench) 同构的评测框架产出，主分支每次推送后由 CI 刷新。请勿手工编辑：表格内容从机读结果逐字节重新生成（`node js/readme.js`）。同一批机读结果也以可交互图表形式发布（分平台柱状对比 + 跨提交趋势，列与平台都可切换），见 [WeDB Bench](https://webc-site.github.io/wedb/)。

参评引擎：

| 列名 | 引擎 | 接入方式 |
|:---|:---|:---|
| `hash` | [wkv](https://github.com/webc-site/wedb/tree/main/wedb/wkv) | wedb 自研追加型混合日志 KV |
| `bftree` | [wbftree](https://github.com/webc-site/wedb/tree/main/wedb/wbftree) | wedb 自研有序 bf-tree 索引 |
| `fjall` | [fjall](https://github.com/fjall-rs/fjall) | `fjall` crate，默认持久化档位 |
| `rocksdb` | [RocksDB](https://github.com/facebook/rocksdb) | `rocksdb` crate，`OptimisticTransactionDB` |
| `sqlite` | [SQLite](https://www.sqlite.org) | `rusqlite`，BLOB 主键表 + WAL 模式 |

## 评测参数

| 参数 | 设定 |
|:---|:---|
| **键大小** | 24 B |
| **值大小** | 150 B |
| **缓存预算** | 4 GiB（按物理内存自动收敛） |
| **批量装载 / 有序插入规模** | 5,000,000 / 1,000,000 条 |
| **单条 / nosync / 批量写** | 50,000 / 2,500,000 / 100 批 × 1,000 条 |
| **随机点读 / 区间读** | 100,000 次点读 / 50,000 × 10 元素区间读，取 3 轮中位数 |
| **多线程读** | 4 / 8 / 16 / 32 线程 |
| **随机种子** | 3（各引擎拿到完全相同的键值分布） |

### 口径与方法说明

- **负载一致性**：每个引擎都用同一种子生成的同一份键值分布，因此同一行的各列可直接横向比较。
- **持久化档位**：除 `nosync writes` 外，所有写入段都按事务做持久化提交。`nosync` 段关闭每次提交的 fsync（SQLite 映射为 `synchronous = OFF`，RocksDB 为 `set_sync(false)`），该段结束后恢复持久化写入。
- **进程隔离**：每一列在独立子进程中运行，并受墙钟预算约束。失败、panic 或超时的列整列记为 `N/A`，状态与退出详情写在备注里，不会从表里悄悄消失。
- **行内最优值加粗**（并列全标）。吞吐类行越大越好，`len()` 与两个体积行越小越好。
- **`len()`** 混合了各引擎的计数语义（元数据计数器 vs 全键空间扫描），反映的是计数成本，不能当作性能结论。
- **`pop`** 通过抽样删除外推到完整负载，因为完整弹出会压倒整轮时长；抽样条数记录在机读结果里。
- **体积**为引擎数据目录的递归字节总和（含 WAL、段文件、清单文件），取各自压缩/整理动作前后的值。

## linux-arm64 — Neoverse-N2（4 逻辑核 / 15.6 GiB 内存）

|                                   | wkv        | wbftree    | fjall      | rocksdb        | sqlite     |
|-----------------------------------|------------|------------|------------|----------------|------------|
| bulk load (key/s)                 | **933K**   | 433K       | 214K       | 146K           | 181K       |
| individual writes (txn/s)         | 19.8       | **404K**   | 597        | 4.49K          | 4.02K      |
| small batch writes (key/s)        | 19.6K      | **405K**   | 209K       | 115K           | 15.9K      |
| sorted inserts (key/s)            | 968K       | 622K       | **1.20M**  | 826K           | 260K       |
| nosync writes (txn/s)             | **2.81M**  | N/A        | 371K       | 188K           | 63.3K      |
| len()                             | 1179ms     | **0ms**    | 2082ms     | 2172ms         | 20ms       |
| random reads (key/s)              | **2.78M**  | 285K       | 173K       | 210K           | 282K       |
| random range reads (scan/s)       | **205K**   | 157K       | 57.7K      | 74.9K          | 58.6K      |
| random reads (4 threads) (key/s)  | **9.87M**  | 1.20M      | 857K       | 807K           | 232K       |
| random reads (8 threads) (key/s)  | **10.4M**  | 1.20M      | 879K       | 799K           | 202K       |
| random reads (16 threads) (key/s) | **10.6M**  | 1.19M      | 910K       | 778K           | 197K       |
| random reads (32 threads) (key/s) | **10.4M**  | 1.22M      | 904K       | 800K           | 178K       |
| removals (key/s)                  | **1.82M**  | 597K       | 327K       | 149K           | 182K       |
| retain (key/s)                    | N/A        | **534K**   | 279K       | 237K           | 132K       |
| extract_if (key/s)                | N/A        | **394K**   | 230K       | 270K           | 61.8K      |
| pop (key/s)                       | N/A        | **243K**   | 214K       | 994            | 149K       |
| uncompacted size                  | 1.03 GiB   | 1.53 GiB   | 1.35 GiB   | **556.63 MiB** | 2.17 GiB   |
| compacted size                    | 624.49 MiB | 662.71 MiB | 520.80 MiB | **458.95 MiB** | 562.31 MiB |

- 备注：列 bftree 采于 Neoverse-N2（4 核 / 16 GiB / 缓存 4.00 GiB），机器口径与首列不同
- 备注：列 fjall 采于 Neoverse-N2（4 核 / 16 GiB / 缓存 4.00 GiB），机器口径与首列不同
- 备注：列 rocksdb 采于 Neoverse-N2（4 核 / 16 GiB / 缓存 4.00 GiB），机器口径与首列不同
- 备注：列 sqlite 采于 Neoverse-N2（4 核 / 16 GiB / 缓存 4.00 GiB），机器口径与首列不同

## linux-x64 — AMD EPYC 7763 64-Core Processor（4 逻辑核 / 15.6 GiB 内存）

|                                   | wkv        | wbftree    | fjall      | rocksdb        | sqlite     |
|-----------------------------------|------------|------------|------------|----------------|------------|
| bulk load (key/s)                 | **1.27M**  | 376K       | 348K       | 188K           | 165K       |
| individual writes (txn/s)         | 24.5       | **338K**   | 9.40K      | 3.62K          | 6.07K      |
| small batch writes (key/s)        | 24.4K      | **357K**   | 237K       | 207K           | 15.4K      |
| sorted inserts (key/s)            | 2.11M      | 508K       | **3.11M**  | 1.06M          | 282K       |
| nosync writes (txn/s)             | **3.32M**  | N/A        | 494K       | 115K           | 39.8K      |
| len()                             | 980ms      | **0ms**    | 2406ms     | 2894ms         | 21ms       |
| random reads (key/s)              | **3.45M**  | 309K       | 243K       | 227K           | 192K       |
| random range reads (scan/s)       | **274K**   | 159K       | 56.9K      | 74.7K          | 48.1K      |
| random reads (4 threads) (key/s)  | **8.94M**  | 1.11M      | 783K       | 636K           | 179K       |
| random reads (8 threads) (key/s)  | **9.20M**  | 1.11M      | 814K       | 608K           | 158K       |
| random reads (16 threads) (key/s) | **9.16M**  | 1.12M      | 821K       | 639K           | 162K       |
| random reads (32 threads) (key/s) | **9.08M**  | 1.12M      | 822K       | 624K           | 149K       |
| removals (key/s)                  | **3.12M**  | 494K       | 493K       | 179K           | 150K       |
| retain (key/s)                    | N/A        | **453K**   | 293K       | 241K           | 120K       |
| extract_if (key/s)                | N/A        | **327K**   | 255K       | 263K           | 61.2K      |
| pop (key/s)                       | N/A        | 170K       | **223K**   | 841            | 116K       |
| uncompacted size                  | 1.03 GiB   | 1.53 GiB   | 1.33 GiB   | **556.63 MiB** | 2.17 GiB   |
| compacted size                    | 624.49 MiB | 662.71 MiB | 520.80 MiB | **458.95 MiB** | 562.31 MiB |

- 备注：列 bftree 采于 AMD EPYC 7763 64-Core Processor（4 核 / 16 GiB / 缓存 4.00 GiB），机器口径与首列不同
- 备注：列 sqlite 采于 AMD EPYC 7763 64-Core Processor（4 核 / 16 GiB / 缓存 4.00 GiB），机器口径与首列不同

## macos-arm64 — Apple M1 (Virtual)（3 逻辑核 / 7.0 GiB 内存）

|                                   | wkv        | wbftree    | fjall      | rocksdb        | sqlite     |
|-----------------------------------|------------|------------|------------|----------------|------------|
| bulk load (key/s)                 | **1.24M**  | 367K       | 522K       | 87.0K          | 179K       |
| individual writes (txn/s)         | 117        | **327K**   | 2.25K      | 5.61K          | 1.12K      |
| small batch writes (key/s)        | 88.5K      | 360K       | **383K**   | 107K           | 3.35K      |
| sorted inserts (key/s)            | 1.79M      | 495K       | **3.89M**  | 1.24M          | 349K       |
| nosync writes (txn/s)             | **3.57M**  | N/A        | 441K       | 82.9K          | 9.75K      |
| len()                             | 2196ms     | **0ms**    | 3534ms     | 5188ms         | 233ms      |
| random reads (key/s)              | **2.61M**  | 201K       | 181K       | 83.9K          | 163K       |
| random range reads (scan/s)       | **218K**   | 116K       | 46.3K      | 44.2K          | 23.2K      |
| random reads (4 threads) (key/s)  | **7.61M**  | 477K       | 553K       | 657K           | 40.9K      |
| random reads (8 threads) (key/s)  | **6.02M**  | 564K       | 805K       | 436K           | 12.9K      |
| random reads (16 threads) (key/s) | **8.55M**  | 568K       | 814K       | 311K           | 1.92K      |
| random reads (32 threads) (key/s) | **8.22M**  | 529K       | 780K       | 403K           | 863        |
| removals (key/s)                  | **1.74M**  | 670K       | 558K       | 91.0K          | 38.1K      |
| retain (key/s)                    | N/A        | **705K**   | 326K       | 133K           | 68.7K      |
| extract_if (key/s)                | N/A        | **309K**   | 242K       | 230K           | 42.0K      |
| pop (key/s)                       | N/A        | 180K       | **259K**   | 940            | 118K       |
| uncompacted size                  | 1.03 GiB   | 1.53 GiB   | 1.30 GiB   | **557.48 MiB** | 2.17 GiB   |
| compacted size                    | 624.48 MiB | 662.69 MiB | 520.78 MiB | **458.95 MiB** | 562.30 MiB |

- 备注：缓存预算按物理内存收敛到 3.50 GiB（redb 标准档为 4 GiB）

## windows-x64 — AMD EPYC 7763 64-Core Processor（4 逻辑核 / 16.0 GiB 内存）

|                                   | wkv        | wbftree  | fjall      | rocksdb    | sqlite     |
|-----------------------------------|------------|----------|------------|------------|------------|
| bulk load (key/s)                 | 490K       | **518K** | 200K       | 124K       | 123K       |
| individual writes (txn/s)         | 10.3       | **458K** | 252        | 130        | 190        |
| small batch writes (key/s)        | 10.2K      | **270K** | 126K       | 63.2K      | 479        |
| sorted inserts (key/s)            | 372K       | **874K** | 775K       | 404K       | 112K       |
| nosync writes (txn/s)             | **2.52M**  | N/A      | 449K       | 97.9K      | 2.57K      |
| len()                             | 1358ms     | **0ms**  | 3139ms     | 2476ms     | 23ms       |
| random reads (key/s)              | **2.63M**  | 169K     | 159K       | 199K       | 179K       |
| random range reads (scan/s)       | **150K**   | 104K     | 46.1K      | 58.5K      | 44.3K      |
| random reads (4 threads) (key/s)  | **6.87M**  | 105K     | 580K       | 549K       | 241K       |
| random reads (8 threads) (key/s)  | **6.63M**  | 105K     | 677K       | 553K       | 202K       |
| random reads (16 threads) (key/s) | **6.68M**  | 98.6K    | 501K       | 586K       | 173K       |
| random reads (32 threads) (key/s) | **6.42M**  | 98.2K    | 589K       | 588K       | 112K       |
| removals (key/s)                  | **1.26M**  | 48.7K    | 416K       | 153K       | 45.3K      |
| retain (key/s)                    | N/A        | 15.6K    | 178K       | **215K**   | 25.4K      |
| extract_if (key/s)                | N/A        | 14.2K    | 190K       | **205K**   | 6.54K      |
| pop (key/s)                       | N/A        | 15.3K    | 80.3K      | 646        | **102K**   |
| uncompacted size                  | 1.03 GiB   | **0 B**  | 1.31 GiB   | 557.18 MiB | 2.17 GiB   |
| compacted size                    | 624.49 MiB | **0 B**  | 520.78 MiB | 458.62 MiB | 562.31 MiB |

- 备注：列 bftree 采于 AMD EPYC 9V45 96-Core Processor（4 核 / 16 GiB / 缓存 4.00 GiB），机器口径与首列不同
- 备注：列 fjall 采于 AMD EPYC 7763 64-Core Processor（4 核 / 16 GiB / 缓存 4.00 GiB），机器口径与首列不同
- 备注：列 rocksdb 采于 AMD EPYC 7763 64-Core Processor（4 核 / 16 GiB / 缓存 4.00 GiB），机器口径与首列不同
- 备注：列 sqlite 采于 AMD EPYC 7763 64-Core Processor（4 核 / 16 GiB / 缓存 4.00 GiB），机器口径与首列不同
