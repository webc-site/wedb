//! wbftree: 块级有序存储引擎与 RangeIndex 管理器
//! (1:1 对标 Garnet bftree-garnet 互操作层 + RangeIndexManager 系列 C# 服务层，经 bf-tree crate 纯 Rust 实现)
//!
//! # 模块组成
//! - [`BfTreeService`]：单树生命周期与点读/扫描/排序批量装载内核/CPR 快照 (对标 BfTreeService.cs)
//! - [`RangeIndexManager`]：多树注册表、惰性恢复、刷盘/检查点/截断/复制枚举 (对标 RangeIndexManager.cs*)
//! - [`RangeIndexChunkedSerializer`] / [`RangeIndexChunkedDeserializer`] / [`RangeIndexMigrationReader`]：
//!   迁移分块流协议状态机 (对标 RangeIndexChunkedSerializer/Deserializer/MigrationReader.cs)
//! - [`RangeIndexStub`]：主存储日志中的 35 字节定长存根 (对标 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:RangeIndexStub)
//!
//! # 并发模型 (thread-per-core 契合，见 sync.md)
//! 全 crate 仅提供同步 API，无运行时依赖，天然契合 compio 线程每核模型：
//! - 点读/写入路径零条带锁：引擎内部叶子闩锁保证并发安全；跨线程共享
//!   `Arc<BfTreeService>`，各核直接操作同一实例，无消息传递开销。
//! - 生命周期变更 (创建/惰性恢复/注销/删除) 才获取键哈希条带写锁
//!   (128 缓存行对齐条带，消除伪共享)；热路径绝不持锁跨引擎调用。
//! - 在线索引用 papaya 无锁字典管理，读侧 pin 快照一致，与写侧互不阻塞。
//! - CPR 快照与点写并发安全 (对标 C# 非阻塞并发 CPR)：直接依赖 bf-tree 引擎
//!   的 CPR 阶段协议 (在途写者按快照版本自行拷贝触碰页)，快照不阻塞写、写不
//!   阻塞快照；同一树的并发快照互斥由 [`TreeEntry`] 的 per-tree claim 承担
//!   (引擎对并发快照静默 no-op，宿主必须串行化)。
//! - 本层 (BfTreeService) 无写入屏障：实例的树指针构造后不变更 (恢复一律走
//!   [`BfTreeService::recover_from_cpr_snapshot`] 新建实例，1:1 对标 C#
//!   RestoreTree 的「新建树 + 注册表登记」)。写静稳等待 (检查点屏障、快照
//!   claim) 均属 [`RangeIndexManager`] 的 checkpoint 体系 (snapshot_pending
//!   原子量)，不在本层：读屏障等待与快照 claim 自旋均为**无超时**串行等待 (均对标
//!   C# `Thread.Yield`)——claim 自旋采用退避阶梯 (spin → yield → 微睡) 免烧核，
//!   认领释放由 RAII 守卫兜底，杜绝以超时判失败导致大树慢盘快照长期饥饿。
//! - 删除树延迟到 `Arc` 引用归零：扫描/点读持引擎 Arc 者可安全跑完 (对标
//!   C# LightEpoch 延迟释放语义，此处由引用计数天然承担)；「排空在途读者后
//!   才释放树并删除数据文件」的删除路径语义由
//!   [`RangeIndexManager::dispose_tree_under_lock`] 承担 (对标
//!   libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:DisposeTreeUnderLock
//!   经 storeEpoch 排空后才删文件的语义)。
//!
//! # 键语义与名字空间隔离 (见 sync.md)
//! - 键全程 `&[u8]` 二进制安全，零拷贝透传引擎。
//! - 树身份 = f(物理域, 用户键)：128 位键 ID 由 `gxhash128(id_key, 专用种子域)`
//!   对**树身份键**派生（与 C# XxHash128→Guid 刻意不逐位兼容，跨架构比特稳定
//!   即可）。身份键由宿主在域上下文中单点派生——wkv 侧为物理 Meta 键
//!   `[vns varint][vdb varint][KeyTag::Meta][user_key]`（C# RangeIndexManager
//!   单实例单域，KeyId=XxHash128(keyBytes) 无需域编码；rust 多库共享单日志为
//!   自定义面，跨库同名键在树注册、升阶换入、claim 封堵、FLUSHDB 回收四面的
//!   隔离全靠身份含域）。文件名前缀即该 ID 的 26 字符小写 Base32 编码，摘要域
//!   与用户数据域隔离，复合键派生不会与用户键混淆。
//!
//! # flush / truncate 与复制文件面接线现状
//! [`RangeIndexManager::on_flush_address`] 已在宿主接线：wkv/src/store/flush.rs
//! ::on_flush_pages 于 whlog 页转只读（刷盘）前原位触发，存根置 IsFlushed +
//! 刷盘快照落盘（1:1 对标 C# GarnetRecordTriggers.cs:OnFlush 与
//! RangeIndexManager.cs:SnapshotTreeForFlush）；`on_truncate` 已在
//! wkv/src/store/addr.rs 日志安全截断处接线回收刷盘快照文件（对标
//! GarnetRecordTriggers.cs:OnTruncate）。主从全量同步的文件面只下发检查点快照树：
//! [`RangeIndexManager::enumerate_checkpoint_snapshots`] 供宿主
//! wedb/src/server/replication/snapshot_transmission.rs 逐文件三段发送，副本侧以
//! [`RangeIndexManager::checkpoint_snapshot_path_in`] 派生落盘路径；刷盘快照不参与
//! 传输，仅由 `on_truncate` 按日志地址回收（C# 文件级复制枚举的 flush 地址窗分支
//! 在 rust 无恢复面消费者，不实现，理由登记于
//! js/check/ignore/libs/server/Resp/RangeIndex/RangeIndexManager.yml）。

#![cfg_attr(docsrs, feature(doc_cfg))]

mod chunk;
pub(crate) mod error;
mod manager;
mod service;
mod stub;
mod types;

pub use chunk::{
  DEFAULT_FILE_READ_BUFFER_SIZE, MIN_CHUNK_SIZE, RangeIndexChunkedDeserializer,
  RangeIndexChunkedSerializer, RangeIndexMigrationReader,
};
pub use error::{ERR_INDEX_ALREADY_EXISTS, ERR_MEMORY_TREE_MIGRATION, Error, Result};
pub use manager::{
  DEFAULT_MIGRATION_CHUNK_SIZE, DetachedTree, INDEX_SIZE_BYTES, RangeIndexManager, TreeEntry,
};
#[cfg(debug_assertions)]
pub use manager::{
  DELETE_INDEX_FAIL_INJECT, GET_OR_OPEN_PAUSE_INJECT, GET_OR_OPEN_PAUSE_KEY_HASH,
  GET_OR_OPEN_PAUSED, GET_OR_OPEN_RESUME, PUBLISH_FAIL_INJECT,
};
pub use service::BfTreeService;
#[cfg(debug_assertions)]
pub use service::SCAN_FAIL_INJECT;
pub use stub::{RANGE_INDEX_STUB_SIZE, RangeIndexStub};
pub use types::{
  BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, ScanRecord, ScanReturnField,
  StorageBackendType, TreeTuning,
};
