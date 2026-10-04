//! 接收读缓冲探测阈值（探测域）
//!
//! 在 garnet 中的相对路径: `libs/common/Networking/NetworkHandler.cs`（bytesRead 累积读取模型）
//!
//! 追加式接收读缓冲本体在 [`wbase::primed::PrimedVec`]（单一实现，读泵与
//! TLS 臂共用，此处不再有 wnode 局部副本）

/// 读取前缓冲预留最小空闲容量阈值（不足时向前平移或扩容）
pub(super) const MIN_READ_SPACE: usize = 4096;
/// 握手识别首批最小所需字节数
pub(super) const MIN_HANDSHAKE_BYTES: usize = 4;
