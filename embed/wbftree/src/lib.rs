//! wbftree: 块级有序存储引擎与 RangeIndex 管理器
//! (1:1 对标 Garnet bftree-garnet 互操作层 + RangeIndexManager 系列 C# 服务层，经 bf-tree crate 纯 Rust 实现)
//!
//! # 模块组成
//! - [`BfTreeService`]：单树生命周期与点读/写入/扫描/CPR 快照 (对标 BfTreeService.cs)
//! - [`RangeIndexManager`]：多树注册表、惰性恢复、刷盘/检查点/截断/复制枚举 (对标 RangeIndexManager.cs*)
//! - [`RangeIndexChunkedSerializer`] / [`RangeIndexChunkedDeserializer`] / [`RangeIndexMigrationReader`]：
//!   迁移分块流协议状态机 (对标 RangeIndexChunkedSerializer/Deserializer/MigrationReader.cs)
//! - [`RangeIndexStub`]：主存储日志中的 35 字节定长存根 (对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:Index.cs)
//!
//! # 并发模型 (thread-per-core 契合，见 sync.md)
//! 全 crate 仅提供同步 API，无运行时依赖，天然契合 compio 线程每核模型：
//! - 点读/写入路径零条带锁：引擎内部叶子闩锁保证并发安全；跨线程共享
//!   `Arc<BfTreeService>`，各核直接操作同一实例，无消息传递开销。
//! - 生命周期变更 (创建/惰性恢复/注销/删除) 才获取 [`RangeIndexLocks`] 键哈希
//!   条带写锁 (128 缓存行对齐条带，消除伪共享)；热路径绝不持锁跨引擎调用。
//! - 在线索引用 papaya 无锁字典管理，读侧 pin 快照一致，与写侧互不阻塞。
//! - CPR 快照与点写并发安全 (对标 C# 非阻塞并发 CPR)：直接依赖 bf-tree 引擎
//!   的 CPR 阶段协议 (在途写者按快照版本自行拷贝触碰页)，快照不阻塞写、写不
//!   阻塞快照；同一树的并发快照互斥由 [`TreeEntry`] 的 per-tree claim 承担
//!   (引擎对并发快照静默 no-op，宿主必须串行化)。
//! - 「屏障计数 + 在途写者计数」双 AtomicUsize 的 SeqCst store-buffering 配对
//!   (见 `service::WriteGuard`) 仅用于换树/释放等生命周期窗口 ([`BfTreeService::recover_in_place`]
//!   / [`BfTreeService::dispose_quiesced`]) 的写静稳；屏障排空超过 30s 以
//!   [`Error::Timeout`] 显式上抛 (C# 依赖 LightEpoch 排空无超时，本实现同步等待
//!   无 epoch 兜底，必须显式暴露)。
//! - 删除树延迟到 `Arc` 引用归零：扫描/点读持引擎 Arc 者可安全跑完 (对标
//!   C# LightEpoch 延迟释放语义，此处由引用计数天然承担)；删除路径另经
//!   [`BfTreeService::dispose_quiesced`] 屏障排空在途写者后才释放树并删除
//!   数据文件，杜绝「写入已成功应答却落入正被 unlink 的 inode」的撕裂窗口
//!   (对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:DisposeTreeUnderLock 经 storeEpoch 排空后才删文件的语义)。
//!
//! # 键语义与名字空间隔离 (见 sync.md)
//! - 键全程 `&[u8]` 二进制安全，零拷贝透传引擎。
//! - 128 位键 ID 由 `gxhash128(key, 专用种子域)` 派生 (与 C# XxHash128→Guid
//!   刻意不逐位兼容，跨架构比特稳定即可)；文件名前缀即该 ID 的 26 字符小写
//!   Base32 编码，摘要域与用户数据域隔离，复合键派生不会与用户键混淆。
//!
//! # 预留未接线 (flush 体系)
//! [`RangeIndexManager::on_flush`] / `on_flush_address` 及配套的刷盘文件恢复、
//! `on_truncate` 回收、`enumerate_files_for_replication` 文件级复制枚举，对标
//! C# GarnetRecordTriggers.OnFlush / OnTruncate / EnumerateFilesForReplication
//! 体系，为文件级增量复制预留；当前宿主 (wkv/wnode) 复制走 WAL 重放，
//! 尚未在 whlog 页转只读处接线触发，属公开预留 API 而非死代码。

#![cfg_attr(docsrs, feature(doc_cfg))]

mod chunk;
pub(crate) mod error;
mod manager;
mod service;
mod stub;
mod types;

pub use chunk::{
  MIN_CHUNK_SIZE, RangeIndexChunkedDeserializer, RangeIndexChunkedSerializer,
  RangeIndexMigrationReader, compute_checksum, compute_checksum_with_seed,
};
pub use error::{Error, Result};
pub use manager::{
  CacheAlignedLock, DEFAULT_MIGRATION_CHUNK_SIZE, INDEX_SIZE_BYTES, NUM_LOCK_STRIPES,
  RangeIndexFileEntry, RangeIndexLocks, RangeIndexManager, TreeEntry,
};
pub use service::{BfTreeService, WriteBarrierGuard, file_has_cpr_magic};
pub use stub::{RANGE_INDEX_STUB_SIZE, RangeIndexStub};
pub use types::{
  BfTreeConfig, BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, ScanRecord,
  ScanReturnField, StorageBackend, StorageBackendType, TreeTuning,
};
// TreeEntry.hash_prefix 字段类型的转发再导出：下游无需依赖 wbase 即可具名该字段类型
pub use wbase::base32::Base32Buf128;
