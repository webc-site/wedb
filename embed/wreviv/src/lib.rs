#![cfg_attr(docsrs, feature(doc_cfg))]
//! 内存槽位复活回收池（对标 C# Garnet Tsavorite `Revivification/` 目录）
//!
//! 模块对应关系：
//! - [`FreeRecord`] ↔ `FreeRecord.cs`：64 位槽位元信息（48 位地址 + 16 位尺寸）原子打包
//! - [`FreeRecordBin`] ↔ `FreeRecordPool.cs::FreeRecordBin`：定长分桶与 First-Fit / Best-Fit 原子取出
//! - [`FreeRecordPool`] ↔ `FreeRecordPool.cs::FreeRecordPool`：多尺寸分级分桶池与跨桶检索
//! - [`RevivStats`] ↔ `RevivificationStats.cs`：成功 / 失败统计（简化为 put / take / hit / drop 四计数）
//!
//! 与 C# 的刻意差异（同一功能只保留一种实现）：
//! - **分段(segment)机制省略**：C# 分桶内按记录尺寸划分 segment 并以 `GetSegmentStart` 定位插入起点；
//!   本实现采用单一扁平槽位数组 + 轮询写游标，Best-Fit 质量由 `best_fit_scan_limit` 全桶扫描保证。
//! - **CheckEmptyWorker 后台线程省略**：C# 依赖后台线程周期扫描复位 `isEmpty` 标志；本实现以原子
//!   `active_count` 计数直接驱动空桶快速路径，无需任何后台任务。
//! - **oversize 分桶省略**：16 位内联尺寸上限 65535B，超限记录的腾挪属 wedb_hlog 层职责。
//! - **RevivificationManager 门面省略**：`RevivifiableFraction`、`Pause/Resume` 与 min_address 推导
//!   由 wedb_hlog / wedb_store 层以参数形式传入（`min_address`），链内复活由 wedb_hlog 的
//!   `try_revivify_in_chain` 承担。
//! - **单字节填充精度**：C# 以 `Constants.kRecordAlignment`(8B) 对齐记录尺寸后分桶；本实现配合
//!   wrecord 的 FillerWords/FillerRem 单字节精度松弛填充，分桶检索不对齐、按字节粒度匹配。
//!
//! 并发模型裁决：池结构（FreeRecord / FreeRecordBin / FreeRecordPool）全原子无锁，可多线程并发存取；
//! 记录本体的原位复活改写遵循 compio 每核单线程的"单写者 + hlog 页写锁 + epoch 保护"前提，
//! C# 的 `TrySeal` CAS 协议刻意未移植（见 [`FreeRecord`] 文档中的多核扩展路径）。

mod bin;
mod error;
mod pool;
mod record;

pub use bin::{BEST_FIT_SCAN_ALL, FreeRecordBin, USE_FIRST_FIT};
pub use error::{Error, Result};
pub use pool::{
  DEFAULT_BIN_CAPACITY, DEFAULT_BIN_SIZES, FreeRecordPool, RevivAllocation, RevivStats,
};
pub use record::{FreeRecord, SetStatus};
