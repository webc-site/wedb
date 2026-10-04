use wedb::server::migration::chunk_reassembler::ChunkReassembler;

#[test]
fn chunk_reassembler_appends_until_final_chunk() {
  let mut r = ChunkReassembler::new();
  assert!(r.append(b"ab", true).is_none());
  assert!(r.append(b"cd", true).is_none());
  assert_eq!(r.append(b"ef", false), Some(b"abcdef".to_vec()));
  // 末块后复位：下一条记录从空流开始
  assert!(r.append(b"z", false).is_some());
}

#[test]
fn chunk_reassembler_reset_drops_partial_stream() {
  let mut r = ChunkReassembler::new();
  assert!(r.append(b"half", true).is_none());
  r.reset();
  assert_eq!(r.append(b"ok", false), Some(b"ok".to_vec()));
}
