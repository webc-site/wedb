# Embedded Key-Value Storage Benchmarks

<p align="center">
  <img src="https://fastly.jsdelivr.net/gh/webc-fs/-@av/Iqtmt6rM4csV3cdkwk2A.svg" alt="Embedded Key-Value Storage Benchmarks" width="100%">
</p>

| Benchmark | [wkv](https://github.com/webc-site/wedb/tree/main/wedb/wkv) | [wbftree](https://github.com/webc-site/wedb/tree/main/wedb/wbftree) | [redb](https://github.com/cberner/redb) | [fjall](https://github.com/fjall-rs/fjall) | [rocksdb](https://github.com/facebook/rocksdb) | [sqlite](https://www.sqlite.org) |
|:---|---:|---:|---:|---:|---:|---:|
| random reads | **9.83X**<br>**316 M/s** | 2.05X<br>65.7 M/s | 1.53X<br>49.0 M/s | 1.36X<br>43.9 M/s | 1.00X<br>32.1 M/s | 1.39X<br>44.5 M/s |
| random range reads | N/A | 7.46X<br>532 M/s | **15.34X**<br>**1.07 G/s** | 2.26X<br>161 M/s | 3.08X<br>220 M/s | 1.00X<br>71.3 M/s |
| random reads (8 threads) | 1.95X<br>41.1 M/s | 7.65X<br>161 M/s | 4.89X<br>103 M/s | 6.14X<br>129 M/s | **7.67X**<br>**162 M/s** | 1.00X<br>21.1 M/s |
| random reads (4 threads) | 2.12X<br>62.0 M/s | **4.28X**<br>**125 M/s** | 2.97X<br>86.7 M/s | 3.27X<br>95.6 M/s | 3.92X<br>115 M/s | 1.00X<br>29.2 M/s |
| random reads (16 threads) | 1.82X<br>40.5 M/s | 6.11X<br>136 M/s | 3.68X<br>82.1 M/s | 5.37X<br>120 M/s | **6.35X**<br>**142 M/s** | 1.00X<br>22.3 M/s |
| random reads (32 threads) | 1.91X<br>42.1 M/s | 5.74X<br>126 M/s | 3.73X<br>82.1 M/s | 5.36X<br>118 M/s | **7.51X**<br>**165 M/s** | 1.00X<br>22.0 M/s |
| len() | 60.92X<br>30ms | **1803869.62X**<br>**0µs** | 1169824.66X<br>1µs | 1.20X<br>1509ms | 1.00X<br>1804ms | 27.10X<br>67ms |
| individual writes | **198.88X**<br>**723 M/s** | 19.10X<br>69.5 M/s | 2.03X<br>7.40 M/s | 1.00X<br>3.64 M/s | 15.37X<br>55.9 M/s | 1.18X<br>4.29 M/s |
| bulk load | **21.91X**<br>**613 M/s** | 3.94X<br>110 M/s | 1.00X<br>28.0 M/s | 4.83X<br>135 M/s | 7.93X<br>222 M/s | 1.86X<br>52.0 M/s |
| batch writes | **134.83X**<br>**572 M/s** | 17.48X<br>74.1 M/s | 2.67X<br>11.3 M/s | 36.21X<br>154 M/s | 53.09X<br>225 M/s | 1.00X<br>4.24 M/s |
| nosync writes | **152.79X**<br>**585 M/s** | N/A | 1.99X<br>7.63 M/s | 11.95X<br>45.8 M/s | 11.48X<br>44.0 M/s | 1.00X<br>3.83 M/s |
| removals | 66.17X<br>297 M/s | 33.41X<br>150 M/s | 1.00X<br>4.49 M/s | 51.03X<br>229 M/s | **66.93X**<br>**301 M/s** | 1.97X<br>8.84 M/s |
| pre-compaction size | **1.10 GiB** | 1.51 GiB | 2.01 GiB | 1.58 GiB | 1.38 GiB | 1.31 GiB |
| compacted size | 1.10 GiB | N/A | 1.55 GiB | 1.58 GiB | **1.06 GiB** | 1.31 GiB |
| memory usage | 236.64 MiB | **153.86 MiB** | 266.81 MiB | 377.78 MiB | 1.37 GiB | 687.33 MiB |

> Performance metrics display relative multiplier in the first line (baseline 1.00X for lowest performance, higher is better) and disk throughput in the second line. Random reads and range scans report median of 3 runs. Cache sizes are configured uniformly where applicable.

## Benchmark Parameters

| Parameter | Setting |
|:---|:---|
| **Key Size** | 24 B |
| **Value Size** | 150 B |
| **Cache Size** | 128 MiB |
| **Benchmark Elements** | 6000000 |

### Configuration & Methodology Notes

- **Key/Value Spec**：24B Key / 150B Value, binary keys and serialized payloads.
- **Cache Budget**：Uniformly configured to 128 MiB, constraining read cache, buffer pool, and write buffer across engines.
- **Workload Scale**：6,000,000 records (~1.04 GiB raw writes), data size 10~30x the memory cache budget.
- **Write Scenarios**：Standardized to RocksDB default durability (WAL enabled, OS cache buffered, no per-commit fsync); benchmarks evaluate individual, batch (1,000 items/batch), and async writes.
- **Read Benchmarks**：100,000 point lookups and 5,000 range scans (step 10), multi-threaded reads (4~32 threads), reporting median of 3 runs.
- **Disk & Memory**：Disk usage measures physical file size pre/post compaction; memory records resident physical memory (RSS / Footprint).

## System Environment

| Hardware | Specification |
|:---|:---|
| **CPU Model** | Apple M2 Max |
| **CPU Cores** | 12 Physical / 12 Logical Cores |
| **Architecture** | aarch64 |
| **Memory** | 64.00 GiB |
| **Disk Type** | NVMe SSD |
| **Operating System** | Darwin 26.5.1 |
| **Kernel Version** | 25.5.0 |

## Storage Architecture & Durability

Baseline Standard: All database engines are uniformly configured to match **RocksDB official defaults** (WAL enabled, `sync = false`, buffered in OS page cache, crash-safe, no per-commit hardware fsync; synced on buffer eviction, checkpoint, or explicit flush).

- **[wkv](https://github.com/webc-site/wedb/tree/main/wedb/wkv)**
  - **Storage Architecture**：Append-Only HybridLog circular buffer (in-memory circular buffer + segmented file storage)
  - **Default Durability**：Writes to HybridLog in-memory circular buffer without per-commit flush_all/fsync, pipeline background page flushing
  - **Bulk Load Ingestion**：Circular buffer streaming pipeline flush, unified flush_all and hardware sync after full completion
  - **Strict Sync Mode (Optional)**：Optional strict hardware blocking sync (set_sync(true)), flush_all and hardware sync on every commit
  - **Verification & Consistency**：Full CRC32 checksums + Epoch lock-free concurrent safe reclamation
- **[wbftree](https://github.com/webc-site/wedb/tree/main/wedb/wbftree)**
  - **Storage Architecture**：Cache-Buffer B-Tree architecture
  - **Default Durability**：Writes to Cache-Buffer pool, dirty pages accumulate and flush via scheduled merges without per-commit fsync
  - **Bulk Load Ingestion**：Dirty pages merged and scheduled for batch flush, explicit flush on completion
  - **Strict Sync Mode (Optional)**：In-memory page buffer fast writes, rely on buffer pool eviction and snapshot flushes
  - **Verification & Consistency**：Page-level CRC32 checksums + Snapshot read-only isolation
- **[redb](https://github.com/cberner/redb)**
  - **Storage Architecture**：Copy-on-Write B-Tree (MVCC)
  - **Default Durability**：Durability::None, data written to OS kernel buffer cache without per-commit fsync
  - **Bulk Load Ingestion**：Single bulk transaction write, single fsync on final commit
  - **Strict Sync Mode (Optional)**：Optional strict hardware blocking sync (Durability::Immediate), fsync on every commit
  - **Verification & Consistency**：Built-in page checksums + ACID crash consistency
- **[fjall](https://github.com/fjall-rs/fjall)**
  - **Storage Architecture**：Single-writer LSM-Tree architecture
  - **Default Durability**：Writes to in-memory WAL buffer (PersistMode::Buffer) without per-commit hardware fsync
  - **Bulk Load Ingestion**：Single-writer txn stream append, single fsync on final commit
  - **Strict Sync Mode (Optional)**：Optional strict hardware blocking sync (PersistMode::SyncAll), fsync on every commit
  - **Verification & Consistency**：CRC32 block checksums + WAL crash recovery
- **[rocksdb](https://github.com/facebook/rocksdb)**
  - **Storage Architecture**：LSM-Tree + native WriteBatch atomic commits
  - **Default Durability**：Writes append to WAL file and MemTable buffered in OS page cache (WriteOptions default sync=false), without per-commit fsync
  - **Bulk Load Ingestion**：WriteBatch stream writes, single sync on final commit
  - **Strict Sync Mode (Optional)**：Optional strict hardware blocking sync (WriteOptions::set_sync(true)), fsync on every commit
  - **Verification & Consistency**：Built-in block and WAL checksums
- **[sqlite](https://www.sqlite.org)**
  - **Storage Architecture**：Standard B-Tree + Write-Ahead Logging (WAL mode)
  - **Default Durability**：Uses WAL mode recommended durability (PRAGMA synchronous = NORMAL), WAL writes buffered in OS cache without per-commit hardware fsync
  - **Bulk Load Ingestion**：Single transaction batch write, commit to WAL buffer, flushed via checkpoint
  - **Strict Sync Mode (Optional)**：Optional strict physical persistence (PRAGMA synchronous = FULL), fsync on every WAL commit
  - **Verification & Consistency**：Page-level verification + WAL atomic transaction rollback

