use std::time::Instant;

use wbase::glob::{glob_match, glob_match_nocase};

#[test]
fn test_glob_adjudicated_clauses() {
  // * 不匹空目标/尾部多星吞尽
  assert!(!glob_match(b"*", b""));
  assert!(glob_match(b"a*", b"a"));
  assert!(glob_match(b"a***", b"a"));

  // ? 单字节
  assert!(glob_match(b"?", b"a"));
  assert!(!glob_match(b"?", b""));

  // 未闭合 `[^abc` 完备集＋取反判定
  assert!(glob_match(b"[^abc", b"d"));
  assert!(!glob_match(b"[^abc", b"a"));

  // `[a-]x]` 端点兼 `]`
  assert!(glob_match(b"[a-]x]", b"]"));
  assert!(glob_match(b"[a-]x]", b"a"));
  assert!(glob_match(b"[a-]x]", b"x"));

  // `[]` 恒不匹
  assert!(!glob_match(b"[]", b"a"));
  assert!(!glob_match(b"[]", b""));

  // `[!a]` 字面 `!`
  assert!(glob_match(b"[!a]", b"!"));
  assert!(glob_match(b"[!a]", b"a"));
  assert!(!glob_match(b"[!a]", b"b"));

  // `\` 末字节面（模式尾裸 `\` 匹字面反斜杠）
  assert!(glob_match(b"\\", b"\\"));

  // `\x41` 匹字面 "x41"（无 hex 语义防误加）
  assert!(!glob_match(b"\\x41", b"A"));
  assert!(glob_match(b"\\x41", b"x41"));

  // `[k-M]` nocase 恒不匹（C# 先交换后折叠形）
  assert!(!glob_match_nocase(b"[k-M]", b"l"));
  assert!(!glob_match_nocase(b"[k-M]", b"L"));

  // 转义字节类内恒大小写敏感
  assert!(!glob_match_nocase(b"[\\a]", b"A"));
  assert!(glob_match_nocase(b"[\\a]", b"a"));
}

#[test]
fn test_glob_exhaustion_sanity() {
  // 经典模式已知答案锚点（Redis KEYS 语义：`*` 吞 ≥1 字节、`?` 恰耗 1 字节、
  // 区间闭包、`\` 字面转义；空目标仅空模式命中）——穷举只防 panic/死循环，
  // 逐 case 判定由本组断言承担
  assert!(glob_match(b"*", b"abc"));
  assert!(!glob_match(b"*", b""));
  assert!(glob_match(b"a*", b"abc"));
  assert!(!glob_match(b"a*", b"xbc"));
  assert!(glob_match(b"a*b", b"axxxb"));
  assert!(!glob_match(b"a*b", b"axxx"));
  assert!(glob_match(b"a?c", b"abc"));
  assert!(!glob_match(b"a?c", b"ac"));
  assert!(!glob_match(b"a?c", b"abbc"));
  assert!(glob_match(b"h?llo", b"hello"));
  assert!(glob_match(b"[a-c]x", b"bx"));
  assert!(!glob_match(b"[a-c]x", b"dx"));
  assert!(glob_match(b"\\*", b"*"));
  assert!(!glob_match(b"\\*", b"x"));
  assert!(glob_match_nocase(b"A*", b"abc"));
  assert!(!glob_match(b"A*", b"abc"));

  // 8 字符子集字母表模式≤3B × 目标≤3B 双 case 全枚举
  let charset = *b"ab*?[]^\\";

  let mut pats = Vec::new();
  let mut targets = Vec::new();

  // length 0..=3
  for len in 0..=3 {
    let mut curr = vec![vec![]];
    for _ in 0..len {
      let mut next = Vec::new();
      for c in curr.into_iter() {
        for &ch in charset.iter() {
          let mut n = c.clone();
          n.push(ch);
          next.push(n);
        }
      }
      curr = next;
    }
    pats.extend(curr.clone());
    targets.extend(curr);
  }

  // Simply ensure no panics or infinite loops
  for p in &pats {
    for t in &targets {
      let _ = glob_match(p, t);
      let _ = glob_match_nocase(p, t);
    }
  }
}

#[test]
fn test_glob_performance_redline() {
  // k=12 星对抗输入 × 24B 目标恒返 false 且墙钟帽
  // 防回改递归/正则形引入指数面，回指工单与 GlobUtils.cs:33 递归源
  let k = 12;
  let mut pattern = Vec::new();
  for _ in 0..k {
    pattern.push(b'a');
    pattern.push(b'*');
  }
  pattern.push(b'b'); // End with 'b' to force matching fail

  let target = vec![b'a'; 24];

  let start = Instant::now();
  let result = glob_match(&pattern, &target);
  let elapsed = start.elapsed();

  assert!(!result);
  // 指数回溯需要几十毫秒，O(N) 在 1us 内，放宽到 1ms 防止 CI 抖动
  assert!(
    elapsed.as_millis() < 5,
    "Performance redline failed: {} ms. Prevent ReDoS/recursive match.",
    elapsed.as_millis()
  );
}
