use std::io::{self, Cursor, Read};

use waof::AofAddress;

use crate::server::{replication::checkpoint_entry::CheckpointEntry, worker::NodeRole};

/// 写入 7-bit 变长整数 + UTF-8 字节（对标 C# BinaryWriter.Write(string)）
fn write_cs_string(w: &mut Vec<u8>, s: &str) {
  let bytes = s.as_bytes();
  let mut len = bytes.len();
  while len >= 0x80 {
    w.push(len as u8 | 0x80);
    len >>= 7;
  }
  w.push(len as u8);
  w.extend_from_slice(bytes);
}

/// 读取 7-bit 变长整数 + UTF-8 字节（对标 C# BinaryReader.ReadString()）
fn read_cs_string<R: Read>(r: &mut R) -> io::Result<String> {
  let mut len = 0usize;
  let mut shift = 0;
  loop {
    let mut b = [0u8; 1];
    r.read_exact(&mut b)?;
    len |= ((b[0] & 0x7f) as usize) << shift;
    if (b[0] & 0x80) == 0 {
      break;
    }
    shift += 7;
    if shift > 35 {
      return Err(io::Error::new(
        io::ErrorKind::InvalidData,
        "string length overflow",
      ));
    }
  }
  let mut buf = vec![0u8; len];
  r.read_exact(&mut buf)?;
  String::from_utf8(buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// libs/cluster/Server/Replication/SyncMetadata.cs:SyncMetadata
///
/// 节点间主备同步协商元数据（字段公开直构，对标 C# 逐字段构造）
#[derive(Debug, Clone)]
pub struct SyncMetadata {
  pub full_sync: bool,
  pub origin_node_role: NodeRole,
  pub origin_node_id: String,
  pub current_primary_repl_id: String,
  pub current_store_version: i64,
  pub current_aof_begin_address: AofAddress,
  pub current_aof_tail_address: AofAddress,
  pub current_replication_offset: AofAddress,
  pub checkpoint_entry: Option<CheckpointEntry>,
}

impl SyncMetadata {
  /// libs/cluster/Server/Replication/SyncMetadata.cs:ToByteArray
  ///
  /// 序列化为二进制
  pub fn to_byte_array(&self) -> Vec<u8> {
    let mut out = Vec::with_capacity(256);
    out.push(u8::from(self.full_sync));
    out.push(self.origin_node_role as u8);
    write_cs_string(&mut out, &self.origin_node_id);
    write_cs_string(&mut out, &self.current_primary_repl_id);
    out.extend_from_slice(&self.current_store_version.to_le_bytes());

    out.extend_from_slice(&self.current_aof_begin_address.serialize());
    out.extend_from_slice(&self.current_aof_tail_address.serialize());
    out.extend_from_slice(&self.current_replication_offset.serialize());

    if let Some(ref entry) = self.checkpoint_entry {
      let entry_bytes = entry.to_byte_array();
      out.extend_from_slice(&(entry_bytes.len() as i32).to_le_bytes());
      out.extend_from_slice(&entry_bytes);
    } else {
      out.extend_from_slice(&0i32.to_le_bytes());
    }

    out
  }

  /// libs/cluster/Server/Replication/SyncMetadata.cs:FromByteArray
  ///
  /// 从二进制字节反序列化
  pub fn from_byte_array(serialized: &[u8]) -> io::Result<Self> {
    if serialized.len() < 20 {
      return Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "SyncMetadata payload too short",
      ));
    }
    let mut cursor = Cursor::new(serialized);
    let mut b1 = [0u8; 1];
    cursor.read_exact(&mut b1)?;
    let full_sync = b1[0] != 0;

    cursor.read_exact(&mut b1)?;
    let role_val = b1[0];
    let origin_node_role = match role_val {
      0 => NodeRole::Primary,
      1 => NodeRole::Replica,
      _ => NodeRole::Unassigned,
    };

    let origin_node_id = read_cs_string(&mut cursor)?;
    let current_primary_repl_id = read_cs_string(&mut cursor)?;

    let mut b8 = [0u8; 8];
    cursor.read_exact(&mut b8)?;
    let current_store_version = i64::from_le_bytes(b8);

    let pos = cursor.position() as usize;
    let rem = &serialized[pos..];

    let current_aof_begin_address = AofAddress::deserialize(rem);
    let s1 = current_aof_begin_address.span_len();
    if rem.len() < s1 {
      return Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "truncated current_aof_begin_address",
      ));
    }
    let rem2 = &rem[s1..];

    let current_aof_tail_address = AofAddress::deserialize(rem2);
    let s2 = current_aof_tail_address.span_len();
    if rem2.len() < s2 {
      return Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "truncated current_aof_tail_address",
      ));
    }
    let rem3 = &rem2[s2..];

    let current_replication_offset = AofAddress::deserialize(rem3);
    let s3 = current_replication_offset.span_len();
    if rem3.len() < s3 {
      return Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "truncated current_replication_offset",
      ));
    }
    let rem4 = &rem3[s3..];
    if rem4.len() < 4 {
      return Err(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "missing checkpoint entry length",
      ));
    }

    let ckpt_len = i32::from_le_bytes(rem4[..4].try_into().unwrap()) as usize;
    let checkpoint_entry = if ckpt_len > 0 && rem4.len() >= 4 + ckpt_len {
      CheckpointEntry::from_byte_array(&rem4[4..4 + ckpt_len])
    } else {
      None
    };

    Ok(Self {
      full_sync,
      origin_node_role,
      origin_node_id,
      current_primary_repl_id,
      current_store_version,
      current_aof_begin_address,
      current_aof_tail_address,
      current_replication_offset,
      checkpoint_entry,
    })
  }
}

#[cfg(test)]
mod tests {
  use super::*;
  use crate::server::replication::checkpoint_entry::CheckpointMetadata;

  #[test]
  fn test_sync_metadata_roundtrip() {
    let mut meta = CheckpointMetadata::new(1);
    meta.store_version = 10;
    meta.store_hlog_token = 0xabcdef;
    meta.store_index_token = 0x123456;
    meta.store_checkpoint_covered_aof_address = AofAddress::create(1, 50);
    let entry = CheckpointEntry::new(meta);

    let sync = SyncMetadata {
      full_sync: true,
      origin_node_role: NodeRole::Primary,
      origin_node_id: "node-origin-1".to_string(),
      current_primary_repl_id: "primary-repl-id-1".to_string(),
      current_store_version: 10,
      current_aof_begin_address: AofAddress::create(1, 0),
      current_aof_tail_address: AofAddress::create(1, 2048),
      current_replication_offset: AofAddress::create(1, 1024),
      checkpoint_entry: Some(entry),
    };

    let bytes = sync.to_byte_array();
    let decoded = SyncMetadata::from_byte_array(&bytes).expect("decode failed");

    assert_eq!(sync.full_sync, decoded.full_sync);
    assert_eq!(sync.origin_node_role, decoded.origin_node_role);
    assert_eq!(sync.origin_node_id, decoded.origin_node_id);
    assert_eq!(
      sync.current_primary_repl_id,
      decoded.current_primary_repl_id
    );
    assert_eq!(sync.current_store_version, decoded.current_store_version);
    assert_eq!(
      sync.current_aof_begin_address,
      decoded.current_aof_begin_address
    );
    assert_eq!(
      sync.current_aof_tail_address,
      decoded.current_aof_tail_address
    );
    assert_eq!(
      sync.current_replication_offset,
      decoded.current_replication_offset
    );
    assert_eq!(
      sync.checkpoint_entry.as_ref().unwrap().metadata,
      decoded.checkpoint_entry.as_ref().unwrap().metadata
    );
  }
}
