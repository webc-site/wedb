# Embedded Key-Value Storage Benchmarks

<p align="center">
  <img src="https://fastly.jsdelivr.net/gh/webc-fs/-@mz/koUIDS-Vb93fXh61WzNw.svg" alt="Embedded Key-Value Storage Benchmarks" width="100%">
</p>

| Benchmark | [wkv](https://github.com/webc-site/wedb/tree/main/wedb/wkv) | [wbftree](https://github.com/webc-site/wedb/tree/main/wedb/wbftree) | [redb](https://github.com/cberner/redb) | [fjall](https://github.com/fjall-rs/fjall) | [rocksdb](https://github.com/facebook/rocksdb) | [sqlite](https://www.sqlite.org) |
|:---|---:|---:|---:|---:|---:|---:|
| random reads | **13.20X**<br>**326 M/s** | 2.89X<br>71.5 M/s | 1.83X<br>45.3 M/s | 1.79X<br>44.3 M/s | 1.00X<br>24.7 M/s | 1.71X<br>42.2 M/s |
| random range reads | N/A | 9.13X<br>618 M/s | **20.90X**<br>**1.38 G/s** | 2.62X<br>177 M/s | 3.60X<br>244 M/s | 1.00X<br>67.7 M/s |
| random reads (8 threads) | 1.92X<br>38.0 M/s | **9.20X**<br>**182 M/s** | 5.21X<br>103 M/s | 7.20X<br>142 M/s | 4.50X<br>89.1 M/s | 1.00X<br>19.8 M/s |
| random reads (4 threads) | 2.31X<br>63.6 M/s | **5.61X**<br>**155 M/s** | 3.41X<br>94.0 M/s | 3.57X<br>98.3 M/s | 2.54X<br>70.1 M/s | 1.00X<br>27.6 M/s |
| random reads (16 threads) | 1.78X<br>41.6 M/s | **6.41X**<br>**150 M/s** | 3.58X<br>83.8 M/s | 5.64X<br>132 M/s | 3.42X<br>80.0 M/s | 1.00X<br>23.4 M/s |
| random reads (32 threads) | 1.85X<br>40.9 M/s | **6.38X**<br>**141 M/s** | 3.77X<br>83.1 M/s | 5.98X<br>132 M/s | 3.13X<br>69.0 M/s | 1.00X<br>22.1 M/s |
| len() | 63.59X<br>28ms | **1782881.83X**<br>**0µs** | 1188587.89X<br>1µs | 1.13X<br>1579ms | 1.00X<br>1783ms | 27.67X<br>64ms |
| individual writes | **222.94X**<br>**792 M/s** | 25.69X<br>91.2 M/s | 2.22X<br>7.89 M/s | 2.77X<br>9.84 M/s | 15.16X<br>53.8 M/s | 1.00X<br>3.55 M/s |
| bulk load | **19.97X**<br>**610 M/s** | 4.14X<br>127 M/s | 1.00X<br>30.6 M/s | 4.90X<br>150 M/s | 8.05X<br>246 M/s | 1.41X<br>42.9 M/s |
| batch writes | **145.96X**<br>**584 M/s** | 21.61X<br>86.5 M/s | 3.70X<br>14.8 M/s | 19.96X<br>79.8 M/s | 73.62X<br>295 M/s | 1.00X<br>4.00 M/s |
| nosync writes | **168.63X**<br>**582 M/s** | N/A | 2.27X<br>7.85 M/s | 13.72X<br>47.4 M/s | 14.56X<br>50.3 M/s | 1.00X<br>3.45 M/s |
| removals | 55.64X<br>278 M/s | 34.16X<br>171 M/s | 1.00X<br>5.00 M/s | 42.87X<br>214 M/s | **61.58X**<br>**308 M/s** | 1.68X<br>8.38 M/s |
| pre-compaction size | **1.10 GiB** | 1.51 GiB | 2.01 GiB | 1.64 GiB | 1.38 GiB | 1.31 GiB |
| compacted size | 1.10 GiB | N/A | 1.55 GiB | 1.64 GiB | **1.06 GiB** | 1.31 GiB |
| memory usage | 236.42 MiB | **154.14 MiB** | 267.77 MiB | 365.23 MiB | 662.50 MiB | 666.91 MiB |

> Performance metrics display relative multiplier in the first line (baseline 1.00X for lowest performance, higher is better) and disk throughput in the second line. Random reads and range scans report median of 3 runs. Cache sizes are configured uniformly where applicable. All write sections except nosync perform durable per-transaction fsync; the nosync section disables per-commit fsync via set_sync(false) and restores durable writes afterwards. The total-records (len) row mixes counting semantics — rocksdb/sqlite/fjall count via full-table or LSM iterator scans, wkv counts via an in-memory hash-index bucket-chain scan, while redb/wbftree read a metadata counter directly; multipliers on this row reflect counting-semantics differences, not engine performance, and must not be read as a performance conclusion. All write sections (bulk load, individual writes, batch writes, nosync, removals) are timed once each with no warmup discard window and no repeated sampling, so the figures include first-touch page-fault and cold-cache effects and are not reproducible means. The wbftree uncompacted-size row uses a page-level write-through caliber with no explicit flush step (upstream bf-tree exposes no page-pool drain API); its size is permanently underreported by a sub-0.2% buffer-tail residue relative to a fully solidified snapshot image, a constant that does not scale with dataset size — interpret cross-engine comparisons accordingly.

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
- **Write Scenarios**：Durable per-transaction writes by default (commit triggers fsync, matching the upstream redb-bench default sync=true); benchmarks evaluate individual, batch (1,000 items/batch), plus a nosync section with per-commit fsync disabled for async throughput comparison, restoring durable writes afterwards.
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

Baseline Standard: All database engines perform durable per-transaction writes by default (matching the upstream redb-bench default `sync = true`, every commit triggers a physical fsync); only the nosync section disables per-commit fsync via `set_sync(false)` for async throughput comparison, restoring durable writes afterwards. sqlite's nosync maps to `synchronous = OFF` (no fsync at all), the same semantics as redb `Durability::None`.

- **[wkv](https://github.com/webc-site/wedb/tree/main/wedb/wkv)**
  - **Storage Architecture**：Append-Only HybridLog circular buffer (in-memory circular buffer + segmented file storage)
  - **Default Durability**：Durable by default, flush_all with hardware sync on every commit
  - **Bulk Load Ingestion**：Circular buffer streaming pipeline flush, single flush_all and hardware sync on final commit
  - **nosync Section (set_sync(false))**：Writes to HybridLog circular buffer and journal buffer only, no flush on commit, pipeline background page flushing
  - **Verification & Consistency**：Full CRC32 checksums + Epoch lock-free concurrent safe reclamation
- **[wbftree](https://github.com/webc-site/wedb/tree/main/wedb/wbftree)**
  - **Storage Architecture**：Cache-Buffer B-Tree architecture
  - **Default Durability**：Writes to Cache-Buffer pages, dirty pages are written back to the working file page-by-page on write (page-level write-through) without per-commit fsync
  - **Bulk Load Ingestion**：Bulk imports persist page-by-page during the load; no explicit flush on completion (upstream bf-tree exposes no page-pool drain API), only a handful of tail mini pages stay unflushed
  - **nosync Section (set_sync(false))**：set_sync switching unsupported, section falls back to default writes and reports N/A
  - **Verification & Consistency**：Page-level CRC32 checksums + Snapshot read-only isolation
- **[redb](https://github.com/cberner/redb)**
  - **Storage Architecture**：Copy-on-Write B-Tree (MVCC)
  - **Default Durability**：Durable by default (redb default Durability), blocking fsync on every commit
  - **Bulk Load Ingestion**：Single bulk transaction write, single fsync on final commit
  - **nosync Section (set_sync(false))**：Durability::None, commit skips fsync, data stays in OS kernel buffer cache
  - **Verification & Consistency**：Built-in page checksums + ACID crash consistency
- **[fjall](https://github.com/fjall-rs/fjall)**
  - **Storage Architecture**：Single-writer LSM-Tree architecture
  - **Default Durability**：Durable by default (PersistMode::SyncAll), blocking physical fsync on every commit
  - **Bulk Load Ingestion**：Single-writer txn stream append, single fsync on final commit
  - **nosync Section (set_sync(false))**：PersistMode::Buffer, writes to in-memory WAL buffer only without hardware fsync
  - **Verification & Consistency**：CRC32 block checksums + WAL crash recovery
- **[rocksdb](https://github.com/facebook/rocksdb)**
  - **Storage Architecture**：LSM-Tree + native WriteBatch atomic commits
  - **Default Durability**：Durable by default (WriteOptions sync=true), WAL fsync on every commit
  - **Bulk Load Ingestion**：WriteBatch stream writes, single sync on final commit
  - **nosync Section (set_sync(false))**：WAL writes without fsync, buffered in OS page cache only
  - **Verification & Consistency**：Built-in block and WAL checksums
- **[sqlite](https://www.sqlite.org)**
  - **Storage Architecture**：Standard B-Tree + Write-Ahead Logging (WAL mode)
  - **Default Durability**：Durable by default (PRAGMA synchronous = FULL), hardware fsync on every WAL commit
  - **Bulk Load Ingestion**：Single transaction batch write, fsync on final commit, flushed via wal_checkpoint(TRUNCATE) on completion
  - **nosync Section (set_sync(false))**：PRAGMA synchronous = OFF, no fsync at all, same semantics as redb Durability::None
  - **Verification & Consistency**：Page-level verification + WAL atomic transaction rollback

