use wbase::ns_prefix::NsPrefix;

#[test]
fn single_segment_roundtrip() {
  let p = NsPrefix::new(7);
  assert_eq!(p.as_slice(), b"7:");
  let iso = p.isolate(b"news");
  assert_eq!(iso, b"7:news");
  assert_eq!(p.strip(&iso), Some(&b"news"[..]));
  // 他域键不匹配本前缀
  let other = NsPrefix::new(42).isolate(b"news");
  assert_eq!(p.strip(&other), None);
}

#[test]
fn joined_segments_roundtrip() {
  let p = NsPrefix::new(7).join(0);
  assert_eq!(p.as_slice(), b"7:0:");
  let iso = p.isolate(b"q");
  assert_eq!(iso, b"7:0:q");
  assert_eq!(p.strip(&iso), Some(&b"q"[..]));
  // 同 ns 不同 db 的折叠键互不命中
  let pdb = NsPrefix::new(7).join(1).isolate(b"q");
  assert_eq!(p.strip(&pdb), None);
  assert_eq!(NsPrefix::new(7).join(1).strip(&iso), None);
}

#[test]
fn extreme_domains_are_unambiguous() {
  let (ns, db) = (u64::MAX, u64::MAX);
  let p = NsPrefix::new(ns).join(db);
  let iso = p.isolate(b"k");
  assert_eq!(iso.len(), 20 + 1 + 20 + 1 + 1);
  assert_eq!(p.strip(&iso), Some(&b"k"[..]));
  // 无前导零：跨段拼接不产生前导碰撞
  let p2 = NsPrefix::new(1).join(2);
  assert_eq!(p2.isolate(b"3"), b"1:2:3");
  assert_eq!(NsPrefix::new(12).strip(&p2.isolate(b"3")), None);
}
