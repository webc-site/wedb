//! AOF 语义头族（对标 libs/server/AOF/AofHeader.cs / AofChunkHeader.cs，
//! GarnetAppendOnlyFile 语义层；与 8B 物理帧头 wal::header::WalFrameHeader 分层）
//!
//! 按协议头职责分文件：basic.rs 基础头 + 头类型枚举、transaction.rs 分片头 +
//! 两类事务头、chunk.rs 分块大值帧头；跨头共用的 const 定长写入原语与跨头
//! 线格式锚点测试留在本文件，对外路径经 pub use 保持不变。
//!
//! 自研依据: AOF 头族（C# 对应 AOF 头元数据，本仓 bitcode 单格式）

mod basic;

mod chunk;

mod transaction;

pub use basic::{AofHeader, AofHeaderType};
pub use chunk::AofChunkHeader;
pub use transaction::{
  AofShardedHeader, AofShardedLogTransactionHeader, AofSingleLogTransactionHeader,
};

/// const 上下文定长写入原语：把 src 拷入 out[off..off+N]
///
/// 序列化布局 = 各字段 LE 编码按 C# StructLayout 显式 FieldOffset 落位；
/// const fn 无法调用 copy_from_slice，各头序列化统一经此原语按偏移写入，
/// 消除散落的手写字节循环
#[inline]
const fn write_at<const N: usize>(out: &mut [u8], off: usize, src: [u8; N]) {
  let mut i = 0;
  while i < N {
    out[off + i] = src[i];
    i += 1;
  }
}
