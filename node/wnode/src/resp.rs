//! RESP2 协议帧编码
//!
//! 自 embed/wkv 迁入的 RI.* 命令帧编码器（原 `wkv::encode_ri_*`），
//! 用于 WAL 预写日志与主从复制流的协议载荷。协议字节只在本模块产生，
//! 引擎层（embed）不感知任何协议格式。

use itoa::Buffer;
use wkv::{StorageBackend, TreeTuning};

/// 向 RESP 缓冲区追加一段 Bulk String 数据帧: `$len\r\ndata\r\n`
#[inline]
fn append_resp_bulk(buf: &mut Vec<u8>, data: &[u8], itoa_buf: &mut Buffer) {
  let len_str = itoa_buf.format(data.len());
  buf.push(b'$');
  buf.extend_from_slice(len_str.as_bytes());
  buf.extend_from_slice(b"\r\n");
  buf.extend_from_slice(data);
  buf.extend_from_slice(b"\r\n");
}

/// 向 RESP 缓冲区追加一段数字选项参数帧: `$opt_len\r\nOPT\r\n$val_len\r\nval\r\n`
#[inline]
fn append_resp_num_arg(buf: &mut Vec<u8>, opt_tag: &[u8], val: usize, b: &mut Buffer) {
  buf.extend_from_slice(opt_tag);
  let len = b.format(val).len();
  buf.extend_from_slice(b.format(len).as_bytes());
  buf.extend_from_slice(b"\r\n");
  buf.extend_from_slice(b.format(val).as_bytes());
  buf.extend_from_slice(b"\r\n");
}

/// 编码 RI.SET 命令为 RESP 协议字节帧 (用于 WAL 预写日志与主从复制流)
pub fn encode_ri_set(key: &[u8], field: &[u8], value: &[u8]) -> Vec<u8> {
  let mut b = Buffer::new();
  let estimated_len = 36 + key.len() + field.len() + value.len();
  let mut buf = Vec::with_capacity(estimated_len);
  buf.extend_from_slice(b"*4\r\n$6\r\nRI.SET\r\n");
  append_resp_bulk(&mut buf, key, &mut b);
  append_resp_bulk(&mut buf, field, &mut b);
  append_resp_bulk(&mut buf, value, &mut b);
  buf
}

/// 编码 RI.DEL 命令为 RESP 协议字节帧 (用于 WAL 预写日志与主从复制流)
pub fn encode_ri_del(key: &[u8], field: &[u8]) -> Vec<u8> {
  let mut b = Buffer::new();
  let estimated_len = 29 + key.len() + field.len();
  let mut buf = Vec::with_capacity(estimated_len);
  buf.extend_from_slice(b"*3\r\n$6\r\nRI.DEL\r\n");
  append_resp_bulk(&mut buf, key, &mut b);
  append_resp_bulk(&mut buf, field, &mut b);
  buf
}

/// 编码 RI.CREATE 命令为 RESP 协议字节帧 (用于 WAL 预写日志与主从复制流)
pub fn encode_ri_create(
  key: &[u8],
  storage_backend: &StorageBackend,
  tuning: TreeTuning,
) -> Vec<u8> {
  let num_args = if tuning.leaf_page_size > 0 { 13 } else { 11 };
  let mut out = Vec::with_capacity(128 + key.len());
  let mut b = Buffer::new();

  out.push(b'*');
  out.extend_from_slice(b.format(num_args).as_bytes());
  out.extend_from_slice(b"\r\n$9\r\nRI.CREATE\r\n");

  append_resp_bulk(&mut out, key, &mut b);

  if *storage_backend == StorageBackend::Memory {
    out.extend_from_slice(b"$6\r\nMEMORY\r\n");
  } else {
    out.extend_from_slice(b"$4\r\nDISK\r\n");
  }

  append_resp_num_arg(&mut out, b"$9\r\nCACHESIZE\r\n$", tuning.cache_size, &mut b);
  append_resp_num_arg(&mut out, b"$9\r\nMINRECORD\r\n$", tuning.min_record_size, &mut b);
  append_resp_num_arg(&mut out, b"$9\r\nMAXRECORD\r\n$", tuning.max_record_size, &mut b);
  append_resp_num_arg(&mut out, b"$9\r\nMAXKEYLEN\r\n$", tuning.max_key_len, &mut b);

  if tuning.leaf_page_size > 0 {
    append_resp_num_arg(&mut out, b"$8\r\nPAGESIZE\r\n$", tuning.leaf_page_size, &mut b);
  }

  out
}

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn ri_set_frame() {
    let buf = encode_ri_set(b"k1", b"f1", b"v1");
    assert_eq!(
      &buf,
      b"*4\r\n$6\r\nRI.SET\r\n$2\r\nk1\r\n$2\r\nf1\r\n$2\r\nv1\r\n"
    );
  }

  #[test]
  fn ri_del_frame() {
    let buf = encode_ri_del(b"k1", b"f1");
    assert_eq!(&buf, b"*3\r\n$6\r\nRI.DEL\r\n$2\r\nk1\r\n$2\r\nf1\r\n");
  }

  #[test]
  fn ri_create_frame_without_leaf_page() {
    let tuning = TreeTuning::default();
    let buf = encode_ri_create(b"idx", &StorageBackend::Std, tuning);
    let text = String::from_utf8(buf).unwrap();
    assert!(text.starts_with("*11\r\n$9\r\nRI.CREATE\r\n$3\r\nidx\r\n$4\r\nDISK\r\n"));
    assert!(text.contains("$9\r\nCACHESIZE\r\n$"));
    assert!(!text.contains("PAGESIZE"));
  }

  #[test]
  fn ri_create_frame_with_leaf_page() {
    let tuning = TreeTuning {
      leaf_page_size: 4096,
      ..TreeTuning::default()
    };
    let buf = encode_ri_create(b"idx", &StorageBackend::Memory, tuning);
    let text = String::from_utf8(buf).unwrap();
    assert!(text.starts_with("*13\r\n$9\r\nRI.CREATE\r\n$3\r\nidx\r\n$6\r\nMEMORY\r\n"));
    assert!(text.contains("$8\r\nPAGESIZE\r\n$4\r\n4096\r\n"));
  }
}
