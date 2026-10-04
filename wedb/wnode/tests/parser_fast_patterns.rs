//! 固定形状热命令模式表集成测试
//! （对应 libs/server/Resp/Parser/RespCommandSimdPatterns.cs）

use wnode::resp::parser::fast_patterns::{
  FAST_PATTERN_GROUPS, FAST_PATTERN_TABLE, pattern_matches,
};

/// 派生忠实性：定长 16B 候选逐字节等于表内变长帧 + 零填尾，
/// 组展平后与表同序同数，组掩码宽度即本组各项帧长（16 字节档免掩码）
#[test]
fn test_derived_candidates_mirror_table() {
  let flattened: Vec<&[u8; 16]> = FAST_PATTERN_GROUPS
    .iter()
    .flat_map(|group| group.candidates.iter())
    .collect();
  assert_eq!(
    flattened.len(),
    FAST_PATTERN_TABLE.len(),
    "候选总数须与表同数"
  );
  assert!(
    FAST_PATTERN_GROUPS
      .windows(2)
      .all(|pair| pair[0].base + pair[0].candidates.len() == pair[1].base),
    "组 base 须连续无缝"
  );
  for (idx, (pattern, ..)) in FAST_PATTERN_TABLE.iter().enumerate() {
    assert_eq!(
      &flattened[idx][..pattern.len()],
      *pattern,
      "第 {idx} 项帧字节须与表一致"
    );
    assert!(
      flattened[idx][pattern.len()..]
        .iter()
        .all(|byte| *byte == 0),
      "第 {idx} 项尾部须零填"
    );
    let group = FAST_PATTERN_GROUPS
      .iter()
      .find(|group| (group.base..group.base + group.candidates.len()).contains(&idx))
      .unwrap();
    match group.mask {
      Some(mask) => assert_eq!(
        mask.iter().filter(|byte| **byte == 0xFF).count(),
        pattern.len(),
        "第 {idx} 项掩码宽度须为帧长"
      ),
      None => assert_eq!(pattern.len(), 16, "免掩码组须为 16 字节全等档"),
    }
  }
}

#[test]
fn test_pattern_matches_boundary() {
  let buffer = b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n";
  assert!(pattern_matches(buffer, 0, b"*2\r\n$3\r\nGET\r\n"));
  assert!(!pattern_matches(buffer, 0, b"*3\r\n$3\r\nSET\r\n"));
  // 越界保护
  assert!(!pattern_matches(
    buffer,
    buffer.len() - 2,
    b"*2\r\n$3\r\nGET\r\n"
  ));
}
