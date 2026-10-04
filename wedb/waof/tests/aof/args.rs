use waof::{arg_sequence_len, decode_arg_slices, encode_arg_sequence};

fn decode_arg_sequence(bytes: &[u8]) -> Option<Vec<Vec<u8>>> {
  decode_arg_slices(bytes).map(|slices| slices.into_iter().map(<[u8]>::to_vec).collect())
}

#[test]
fn arg_sequence_roundtrip() {
  let args = vec![b"k".to_vec(), b"v".to_vec(), vec![]];
  let mut buf = vec![0u8; arg_sequence_len(&args)];
  let written = encode_arg_sequence(&args, &mut buf);
  assert_eq!(written, buf.len());
  assert_eq!(decode_arg_sequence(&buf).unwrap(), args);

  let slices = decode_arg_slices(&buf).unwrap();
  assert_eq!(slices, vec![&b"k"[..], &b"v"[..], &b""[..]]);

  // 截断即 None
  assert!(decode_arg_slices(&buf[..buf.len() - 1]).is_none());
  assert!(decode_arg_slices(&[]).is_none());
  assert!(decode_arg_sequence(&buf[..buf.len() - 1]).is_none());
  assert!(decode_arg_sequence(&[]).is_none());
}
