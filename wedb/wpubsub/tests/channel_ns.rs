use wbase::glob::glob_match;
use wpubsub::channel_ns::ChannelNsPrefix;

#[test]
fn prefix_is_stripped_symmetrically() {
  let p = ChannelNsPrefix::new(7);
  let iso = p.isolate(b"news");
  assert_eq!(iso, b"7:news");
  assert_eq!(p.strip(&iso), Some(&b"news"[..]));
  // 他 ns 键不匹配本前缀
  let other = ChannelNsPrefix::new(42).isolate(b"news");
  assert_eq!(p.strip(&other), None);
}

#[test]
fn prefix_is_glob_safe_and_unambiguous() {
  // 数字与定界符均非 glob 元字符：前缀段按字面匹配
  let p42 = ChannelNsPrefix::new(42);
  let pat = p42.isolate(b"news.*");
  // 同 ns 命中
  assert!(glob_match(&pat, &p42.isolate(b"news.tech")));
  // 跨 ns 不命中（含数字前导歧义：ns 4 的 "2x" 与 ns 42 的 "x"）
  assert!(!glob_match(
    &pat,
    &ChannelNsPrefix::new(4).isolate(b"2news.tech")
  ));
  assert!(!glob_match(&pat, &ChannelNsPrefix::new(4).isolate(b"2x")));
  assert!(!glob_match(
    &ChannelNsPrefix::new(4).isolate(b"2x"),
    &p42.isolate(b"x")
  ));
}
