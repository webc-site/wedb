//! RangeIndex 分块流式序列化与反序列化状态机 (1:1 对标 Garnet RangeIndexChunkedSerializer 与 RangeIndexChunkedDeserializer)
//!
//! # 流格式
//!
//! 单条 RangeIndex 迁移记录的完整字节流：
//!
//! ```text
//! [4-byte keyLen (LE)][key bytes][8-byte fileBytes (LE)][file payload][8-byte checksum (LE)][4-byte stubLen (LE)][stub bytes]
//! ```
//!
//! # 分块规则 (传输层契约)
//!
//! 序列化侧由调用方按任意块长切 `move_next` 输出；键数据与文件正文可跨任意多块，
//! 但以下三处具有**单块原子性**（与 C# 逐条对应，[`MIN_CHUNK_SIZE`] = 47 字节即
//! 由最长的单块结构 trailer 决定）：
//!
//! 1. `keyLen` 头部必须完整位于一个块内；
//! 2. `fileBytes` 头部必须完整位于一个块内；
//! 3. **trailer（checksum + stubLen + stub 共 47 字节）必须整体在一个块内到达，
//!    且流在该块末尾恰好结束**——[`RangeIndexChunkedDeserializer`] 在 trailer 阶段
//!    严格校验「剩余 35 字节即 stub」，传输层若把 trailer 重新分块拆散（哪怕拆成
//!    连续的合法块）会被判为协议损坏而非等待续块：状态机无法区分「拆散的 trailer」
//!    与「截断/带尾随字节的畸形流」，两者都表现为同一不可恢复错误。
//!    违反原因经 [`RangeIndexChunkedDeserializer::take_error`](RangeIndexChunkedDeserializer::take_error)
//!    获取。[`RangeIndexMigrationReader`] 按 `dest.len() >= MIN_CHUNK_SIZE` 输出
//!    天然满足该契约；接收端按 47 字节以上缓冲循环调 `read_next_chunk` 亦然。

/// 键长度字段长度 (4 字节小端无符号整数)
pub const KEY_LEN_BYTES: usize = 4;
/// 文件总长度字段长度 (8 字节小端无符号整数)
pub const FILE_LEN_BYTES: usize = 8;
/// 校验和字段长度 (8 字节小端无符号整数)
pub const CHECKSUM_BYTES: usize = 8;
/// 存根长度字段长度 (4 字节小端无符号整数)
pub const STUB_LEN_BYTES: usize = 4;

mod deserializer;
mod migration_reader;
mod serializer;

pub use deserializer::RangeIndexChunkedDeserializer;
pub use migration_reader::{DEFAULT_FILE_READ_BUFFER_SIZE, RangeIndexMigrationReader};
pub use serializer::{MIN_CHUNK_SIZE, RangeIndexChunkedSerializer};
