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

#[cfg(test)]
mod tests {
  use wbase::store_type::REPLAY_TASK_ACCESS_VECTOR_BYTES;

  use super::{
    AofChunkHeader, AofHeader, AofHeaderType, AofShardedHeader, AofShardedLogTransactionHeader,
    AofSingleLogTransactionHeader,
  };

  /// 磁盘字节布局锚点：roundtrip 只能发现 parse/to_bytes 对称性错位，
  /// 此处按 C# FieldOffset 逐字段断言绝对偏移，锁死序列化格式
  #[test]
  fn test_header_disk_layout_anchors() {
    // AofHeader：偏移 0=version、1=flags、2=opType、3=procedureId/databaseId union、
    // 4=storeVersion、12=sessionID
    let mut h = AofHeader::new();
    h.op_type = 0x07;
    h.procedure_id = 0x09;
    h.store_version = 0x0102_0304_0506_0708;
    h.session_id = 0x0a0b_0c0d;
    let b = h.to_bytes();
    assert_eq!(b[0], AofHeader::AOF_FORMAT_VERSION);
    assert_eq!(b[1], 0);
    assert_eq!(b[2], 0x07);
    assert_eq!(b[3], 0x09);
    assert_eq!(b[4..12], 0x0102_0304_0506_0708u64.to_le_bytes());
    assert_eq!(b[12..16], 0x0a0b_0c0du32.to_le_bytes());

    // procedure_id 为 0 时 union 字节写 database_id
    h.procedure_id = 0;
    h.database_id = 0x0e;
    assert_eq!(h.to_bytes()[3], 0x0e);

    // AofShardedHeader：sequenceNumber @16
    let sh = AofShardedHeader {
      basic: h,
      sequence_number: -2,
    };
    let sb = sh.to_bytes();
    assert_eq!(&sb[..16], &h.to_bytes()[..]);
    assert_eq!(sb[16..24], (-2i64).to_le_bytes());

    // AofSingleLogTransactionHeader：participantCount @16、位图 @18
    let mut vector = [0u8; REPLAY_TASK_ACCESS_VECTOR_BYTES];
    vector[0] = 0xAA;
    vector[31] = 0x55;
    let st = AofSingleLogTransactionHeader {
      basic: h,
      participant_count: -3,
      replay_task_access_vector: vector,
    };
    let stb = st.to_bytes();
    assert_eq!(&stb[..16], &h.to_bytes()[..]);
    assert_eq!(stb[16..18], (-3i16).to_le_bytes());
    assert_eq!(stb[18..50], vector);

    // AofShardedLogTransactionHeader：participantCount @24、位图 @26
    let sht = AofShardedLogTransactionHeader {
      sharded: sh,
      participant_count: 7,
      replay_task_access_vector: vector,
    };
    let shtb = sht.to_bytes();
    assert_eq!(&shtb[..24], &sh.to_bytes()[..]);
    assert_eq!(shtb[24..26], 7i16.to_le_bytes());
    assert_eq!(shtb[26..58], vector);

    // AofChunkHeader：长度三元组 @0/4/8、objectId @12、keyHash @20
    let ch = AofChunkHeader {
      overflow_key_length: 1,
      overflow_value_length: 2,
      input_length: 3,
      object_id: 4,
      key_hash: -5,
    };
    let cb = ch.to_bytes();
    assert_eq!(cb[..4], 1u32.to_le_bytes());
    assert_eq!(cb[4..8], 2u32.to_le_bytes());
    assert_eq!(cb[8..12], 3u32.to_le_bytes());
    assert_eq!(cb[12..20], 4u64.to_le_bytes());
    assert_eq!(cb[20..28], (-5i64).to_le_bytes());
  }

  #[test]
  fn test_skip_header_offsets() {
    for (t, size) in [
      (AofHeaderType::BasicHeader, 16),
      (AofHeaderType::ShardedHeader, 24),
      (AofHeaderType::SingleLogTransactionHeader, 50),
      (AofHeaderType::ShardedLogTransactionHeader, 58),
      (AofHeaderType::BasicChunkHeader, 44),
      (AofHeaderType::ShardedChunkHeader, 52),
    ] {
      assert_eq!(t.total_size(), size);
      let mut h = AofHeader::new();
      h.set_header_type(t);
      assert_eq!(AofHeader::skip_header(&h.to_bytes()), Some(size));
    }
  }

  #[test]
  fn test_chunk_header_ref() {
    let mut h = AofHeader::new();
    h.set_header_type(AofHeaderType::BasicChunkHeader);
    let mut entry = h.to_bytes().to_vec();
    let chunk = AofChunkHeader {
      overflow_key_length: 8,
      overflow_value_length: 0,
      input_length: 4,
      object_id: 7,
      key_hash: -1,
    };
    entry.extend_from_slice(&chunk.to_bytes());

    let (offset, parsed) = AofHeader::get_chunked_header_ref(&entry).unwrap();
    assert_eq!(offset, 16);
    assert_eq!(parsed, chunk);

    // 非分块类型返回 None。
    let mut plain = AofHeader::new();
    plain.set_header_type(AofHeaderType::BasicHeader);
    assert!(AofHeader::get_chunked_header_ref(&plain.to_bytes()).is_none());
  }

  #[test]
  fn test_header_parse_truncated_boundaries() {
    let short_bytes = [0u8; 15];
    assert!(AofHeader::parse(&short_bytes).is_none());
    assert!(AofShardedHeader::parse(&[0u8; 23]).is_none());
    assert!(AofSingleLogTransactionHeader::parse(&[0u8; 49]).is_none());
    assert!(AofShardedLogTransactionHeader::parse(&[0u8; 57]).is_none());
    assert!(AofChunkHeader::parse(&[0u8; 27]).is_none());
    assert!(AofHeader::skip_header(&short_bytes).is_none());
  }
}
