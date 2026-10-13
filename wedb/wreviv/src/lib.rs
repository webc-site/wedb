#![recursion_limit = "256"]
#![cfg_attr(docsrs, feature(doc_cfg))]
//!
//! 模块对应关系：
//! - [`FreeRecord`] ↔ `FreeRecord.cs`：64 位槽位元信息（48 位地址 + 16 位尺寸）原子打包
//! - [`FreeRecordBin`] ↔ `FreeRecordPool.cs::FreeRecordBin`：定长分桶与 First-Fit / Best-Fit 原子取出
//! - [`FreeRecordPool`] ↔ `FreeRecordPool.cs::FreeRecordPool`：多尺寸分级分桶池与跨桶检索
//! - 统计 ↔ `RevivificationStats.cs`：成功 / 失败统计（简化为 put / take / hit / drop
//!   四计数，直读 [`FreeRecordPool`] 的 pub 原子字段）
//!
//! 与 C# 的刻意差异（同一功能只保留一种实现）：
//! - **配置面收缩**：C# `RevivificationSettings`（`FreeRecordBins` /
//!   `NumberOfBinsToSearch` / `BestFitScanLimit` / `RevivifiableFraction` 等）的可配面在
//!   wedb 收缩为编译期 PowerOf2Bins 预设——生产唯一构造入口 [`FreeRecordPool::new`]
//!   （wkv store 层），跨桶检索固定至多探相邻一档（对标 C# `NumberOfBinsToSearch`
//!   默认 0 = 当期桶 + 相邻上一档）。[`FreeRecordPool::with_bin_sizes_and_scan_limit`]
//!   与 [`FreeRecordPool::find_bin_index`] 仅供对标 C# RevivificationTests 的集成测试
//!   构造单桶 / 小容量 / First-Fit 夹具与验证分桶边界，生产不使用。统计计数器直读
//!   `put_count` 等 pub 原子字段，不提供包装方法。
//! - **分段(segment)机制省略**：C# 分桶内按记录尺寸划分 segment 并以 `GetSegmentStart` 定位插入起点；
//!   本实现采用单一扁平槽位数组 + 轮询写游标，Best-Fit 质量由 `best_fit_scan_limit` 全桶扫描保证。
//! - **CheckEmptyWorker 后台线程省略**：C# 依赖后台线程周期扫描复位 `isEmpty` 标志；本实现以原子
//!   `active_count` 计数直接驱动空桶快速路径，无需任何后台任务。
//! - **oversize 复活臂**：16 位内联尺寸上限 65535B 只封顶普通桶的槽位尺寸字；超上限
//!   而仍在单页容量内的记录经 [`FreeRecordPool::with_oversize`] 接入专用尾桶（对标 C#
//!   `PowerOf2Bins` 预设恒附的 `RecordSize = MaxRecordSize` 单 oversize 桶，
//!   FreeRecordPool.cs:TryPeek/:TryTakeOversize/:GetRecordSize 复读臂）：入池仅存地址
//!   （不写 16 位尺寸），取出时经 wkv store 层从 whlog 记录头读尺寸口注入的
//!   `RecordSizeReader` 闭包复读真实尺寸再判纳。未挂接即无该臂（C# 未配置
//!   oversize 桶的同款形态）。
//! - **RevivificationManager 门面部分收敛**：`revivSuspendCount` 单字段（含
//!   `EnableRevivification` 未启用时的 -1 初值）与 `Pause/Resume/IsEnabled` 一并并入
//!   [`FreeRecordPool`]（[`FreeRecordPool::new`] 的启用位 + [`FreeRecordPool::pause`] /
//!   [`FreeRecordPool::resume`] / [`FreeRecordPool::is_enabled`]，take 入口门控）；
//!   上层链内复活臂与脱钩臂均以 [`FreeRecordPool::is_enabled`] 为唯一启用谓词，
//!   与 C# `RevivificationManager.IsEnabled` 同形；`RevivifiableFraction` 比例门控属配置面，
//!   由 wkv StoreConfig.revivifiable_fraction 持有、上层按
//!   `tail - (tail - read_only) × fraction` 推导 min_address 后以参数传入（对标
//!   `RevivificationManager.GetMinRevivifiableAddress`，推导需要 tail/read_only 等日志水位，
//!   池自身不持有）；链内复活的内核由 wedb_hlog 的 `try_revivify_in_chain` 承担。
//! - **整词对齐填充**：C# 以 `Constants.kRecordAlignment`(8B) 对齐记录尺寸后分桶，本实现
//!   同形：配合 wrecord 的 FillerWords 词粒度（每词 8 字节）松弛填充，记录尺寸恒 8 字节
//!   整词对齐，末桶 65528 即该不变式的池侧收形（pool.rs 编译期断言钉死，别无单字节
//!   余数次级填充口径）。
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
pub use pool::{DEFAULT_BIN_SIZES, FreeRecordPool};
pub(crate) use record::RecordSizeReader;
pub use record::{FreeRecord, SetStatus};
