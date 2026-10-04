use std::sync::Arc;

use wnode::resp::objects::sorted_set_commands::blocking::write_popped_pairs;

/// ZMPOP/BZMPOP 弹出对分值版本分派
///（C# SortedSetMPop :493-517 的 WriteDoubleNumeric；*2/*n 包装双版本同形）
#[test]
fn popped_pairs_dual_protocol() {
  let popped = vec![(1.5_f64, Arc::from(&b"m"[..]))];

  let mut out2 = Vec::new();
  write_popped_pairs(b"k", &popped, &mut out2, 2);
  assert_eq!(
    out2,
    b"*2\r\n$1\r\nk\r\n*1\r\n*2\r\n$1\r\nm\r\n$3\r\n1.5\r\n"
  );

  let mut out3 = Vec::new();
  write_popped_pairs(b"k", &popped, &mut out3, 3);
  assert_eq!(out3, b"*2\r\n$1\r\nk\r\n*1\r\n*2\r\n$1\r\nm\r\n,1.5\r\n");
}
