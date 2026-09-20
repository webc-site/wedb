//! 固定形状热命令模式表（对标 libs/server/Resp/Parser/RespCommandSimdPatterns.cs）
//!
//! 表本体仍是变长帧字面量（人读可核、与 C# `RespPattern(argCount, cmd)` 逐项对齐）；
//! 比对用的定长 16B 模式与各长度档掩码由 `FAST_PATTERN_TABLE` 编译期派生
//! （`padded` / `mask_for`），对位 C# 的零填 `buf.Clear()` 与 `s_mask13/14/15`，
//! 不存在第二份常量誊写。派生组界由编译期布局锁定校验把死，表动则编译失败。

use wbase::simd::MaskedGroup;
use wresp::command::RespCommand;

/// 固定形状热命令模式表：`( RESP 帧前缀, 命令, 参数个数 )`
///
/// 帧字节逐项对齐 RespCommandSimdPatterns.cs 的 RespPattern(argCount, cmd)：
/// `*N` 的 N = 参数个数 + 1（数组元素总数，含命令名）；13..15 字节模式在
/// C# 以掩码忽略模式长度之后的字节，16 字节模式（6 字符命令）为全等比较。
///
/// `const`（非 `static`）：定长 16B 派生数组须在编译期读表内容。
pub(crate) const FAST_PATTERN_TABLE: &[(&[u8], RespCommand, u8)] = &[
  // 13 字节：3 字符命令
  (b"*2\r\n$3\r\nGET\r\n", RespCommand::Get, 1),
  (b"*3\r\n$3\r\nSET\r\n", RespCommand::Set, 2),
  (b"*2\r\n$3\r\nDEL\r\n", RespCommand::Del, 1),
  (b"*2\r\n$3\r\nTTL\r\n", RespCommand::Ttl, 1),
  // 14 字节：4 字符命令
  (b"*1\r\n$4\r\nPING\r\n", RespCommand::Ping, 0),
  (b"*2\r\n$4\r\nINCR\r\n", RespCommand::Incr, 1),
  (b"*2\r\n$4\r\nDECR\r\n", RespCommand::Decr, 1),
  (b"*1\r\n$4\r\nEXEC\r\n", RespCommand::Exec, 0),
  (b"*2\r\n$4\r\nPTTL\r\n", RespCommand::Pttl, 1),
  // 15 字节：5 字符命令
  (b"*1\r\n$5\r\nMULTI\r\n", RespCommand::Multi, 0),
  (b"*3\r\n$5\r\nSETNX\r\n", RespCommand::Setnx, 2),
  (b"*4\r\n$5\r\nSETEX\r\n", RespCommand::Setex, 3),
  // 16 字节：6 字符命令（无掩码，全等）
  (b"*2\r\n$6\r\nEXISTS\r\n", RespCommand::Exists, 1),
  (b"*2\r\n$6\r\nGETDEL\r\n", RespCommand::Getdel, 1),
  (b"*3\r\n$6\r\nAPPEND\r\n", RespCommand::Append, 2),
  (b"*3\r\n$6\r\nINCRBY\r\n", RespCommand::Incrby, 2),
  (b"*3\r\n$6\r\nDECRBY\r\n", RespCommand::Decrby, 2),
  (b"*4\r\n$6\r\nPSETEX\r\n", RespCommand::Psetex, 3),
];

/// 长度档组界（表内起始下标 / 组内项数），与 C# SimdFastParse 的
/// 13 → 14 → 15 → 16 判定序同序，命中优先级仍由表序决定
const RUN13_START: usize = 0;
const RUN13_COUNT: usize = 4;
const RUN14_START: usize = RUN13_START + RUN13_COUNT;
const RUN14_COUNT: usize = 5;
const RUN15_START: usize = RUN14_START + RUN14_COUNT;
const RUN15_COUNT: usize = 3;
const RUN16_START: usize = RUN15_START + RUN15_COUNT;
const RUN16_COUNT: usize = 6;

/// 逐位与掩码：前 `len` 字节保留、其后清零（C# `s_mask13/14/15`）
///
/// 16 字节档不占掩码——`MaskedGroup::mask = None` 即全 16 字节全等。
/// MRU 槽的消费长度掩码同源（C# `_cachedMaskN` 由 `UpdateCommandCache` 写入）
pub(crate) const fn mask_for(len: usize) -> [u8; 16] {
  let mut mask = [0u8; 16];
  let mut i = 0;
  while i < len {
    mask[i] = 0xFF;
    i += 1;
  }
  mask
}

/// 由 `FAST_PATTERN_TABLE` 的 `[START, START + N)` 段派生定长 16B 模式
/// （模式长度之后零填，与 C# `RespPattern` 的 `buf.Clear()` 同形；
/// 掩码清零的正是这些尾部字节）
const fn padded<const START: usize, const N: usize>() -> [[u8; 16]; N] {
  let mut run = [[0u8; 16]; N];
  let mut i = 0;
  while i < N {
    let pattern = FAST_PATTERN_TABLE[START + i].0;
    assert!(
      pattern.len() <= 16,
      "SIMD pattern overflow: 帧前缀超 16 字节"
    );
    let mut j = 0;
    while j < pattern.len() {
      run[i][j] = pattern[j];
      j += 1;
    }
    i += 1;
  }
  run
}

/// 组内项数校验：`[start, start + count)` 各项帧长同为 `pattern_len`
const fn run_pattern_len_is(start: usize, count: usize, pattern_len: usize) -> bool {
  let mut i = 0;
  while i < count {
    if FAST_PATTERN_TABLE[start + i].0.len() != pattern_len {
      return false;
    }
    i += 1;
  }
  true
}

// 表布局锁定（编译期）：18 项、按 13/14/15/16 字节成段同序。
// 增删项或改序即编译失败 → 派生数组与组界不可能与表脱节
const _: () = {
  assert!(RUN16_START + RUN16_COUNT == FAST_PATTERN_TABLE.len());
  assert!(run_pattern_len_is(RUN13_START, RUN13_COUNT, 13));
  assert!(run_pattern_len_is(RUN14_START, RUN14_COUNT, 14));
  assert!(run_pattern_len_is(RUN15_START, RUN15_COUNT, 15));
  assert!(run_pattern_len_is(RUN16_START, RUN16_COUNT, 16));
};

/// 各长度档掩码：宽度取自表内本档首项的实际帧长，与上方布局锁定的同档校验
/// 互为呼应（不另誊字面量）
const MASK_13: [u8; 16] = mask_for(FAST_PATTERN_TABLE[RUN13_START].0.len());
const MASK_14: [u8; 16] = mask_for(FAST_PATTERN_TABLE[RUN14_START].0.len());
const MASK_15: [u8; 16] = mask_for(FAST_PATTERN_TABLE[RUN15_START].0.len());

static PADDED_13: [[u8; 16]; RUN13_COUNT] = padded::<RUN13_START, RUN13_COUNT>();
static PADDED_14: [[u8; 16]; RUN14_COUNT] = padded::<RUN14_START, RUN14_COUNT>();
static PADDED_15: [[u8; 16]; RUN15_COUNT] = padded::<RUN15_START, RUN15_COUNT>();
static PADDED_16: [[u8; 16]; RUN16_COUNT] = padded::<RUN16_START, RUN16_COUNT>();

/// 模式表臂的比对入参：四组候选（每组一个长度档），命中回传 `FAST_PATTERN_TABLE`
/// 的全局下标
///
/// 一次 16B 载入 + 至多 3 次按位与 + 18 次整向量全等（`wbase::simd::first_masked_eq`
/// 的向量核），与 C# SimdFastParse 逐组判定序同型
pub(crate) static FAST_PATTERN_GROUPS: &[MaskedGroup<'static>] = &[
  MaskedGroup {
    mask: Some(MASK_13),
    candidates: &PADDED_13,
    base: RUN13_START,
  },
  MaskedGroup {
    mask: Some(MASK_14),
    candidates: &PADDED_14,
    base: RUN14_START,
  },
  MaskedGroup {
    mask: Some(MASK_15),
    candidates: &PADDED_15,
    base: RUN15_START,
  },
  MaskedGroup {
    mask: None,
    candidates: &PADDED_16,
    base: RUN16_START,
  },
];

/// 模式比较（标量等价：仅比较模式长度内的字节，模式之后的输入字节不作约束）
///
/// 向量化后仅剩单数字帧标量快路径一个消费者（对位 C# 的标量掩码技巧，保持原形）
#[inline]
pub(crate) fn pattern_matches(buffer: &[u8], start: usize, pattern: &[u8]) -> bool {
  buffer.len() >= start + pattern.len() && &buffer[start..start + pattern.len()] == pattern
}

#[cfg(test)]
mod tests {
  use wbase::simd::first_masked_eq;

  use super::{FAST_PATTERN_GROUPS, FAST_PATTERN_TABLE, pattern_matches};

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

  /// 比较核逐输入等价：向量核与转写前的标量 `pattern_matches` 逐项首中同判
  ///
  /// 表臂命中后落到 `fast_parse_command` 的标量快路径上结果同形，故解析级
  /// 集成用例区分不了两臂——本用例直呼向量核与标量参考逐输入核对命中序位
  #[test]
  fn test_masked_kernel_matches_scalar_reference() {
    // 转写前形态：按表序逐项 pattern_matches（含缓冲区长度门）取首中
    let scalar_scan = |window: &[u8; 16]| {
      FAST_PATTERN_TABLE
        .iter()
        .position(|(pattern, ..)| pattern_matches(window, 0, pattern))
    };

    // 表内 18 项 × 尾部填充字节取值：命中序位即表序位，与尾部内容无关
    for (idx, (pattern, ..)) in FAST_PATTERN_TABLE.iter().enumerate() {
      for fill in [0x00u8, 0xFF, b'Z', b'\r'] {
        let mut window = [fill; 16];
        window[..pattern.len()].copy_from_slice(pattern);
        assert_eq!(
          first_masked_eq(&window, FAST_PATTERN_GROUPS),
          Some(idx),
          "第 {idx} 项 fill={fill:#04x} 应命中"
        );
        assert_eq!(scalar_scan(&window), Some(idx), "第 {idx} 项标量参考同判");
      }
    }

    // 逐字节位翻转（18 项 × 16 位置 × 8 位）：掩码内失配、掩码外仍命中，
    // 命中序位与标量参考逐位同判
    for (idx, (pattern, ..)) in FAST_PATTERN_TABLE.iter().enumerate() {
      for position in 0..16 {
        for bit in 0..8 {
          let mut window = [0u8; 16];
          window[..pattern.len()].copy_from_slice(pattern);
          window[position] ^= 1 << bit;
          assert_eq!(
            first_masked_eq(&window, FAST_PATTERN_GROUPS),
            scalar_scan(&window),
            "第 {idx} 项第 {position} 字节翻第 {bit} 位"
          );
        }
      }
    }

    // 伪随机语料（LCG，进程内确定性）：全噪声窗口与帧前缀嫁接随机尾窗口
    let mut state = 0x2545_F491_4F6C_DD1Du64;
    let mut next = move || {
      state = state
        .wrapping_mul(6364136223846793005)
        .wrapping_add(1442695040888963407);
      (state >> 33) as u8
    };
    for _ in 0..4096 {
      let mut window = [0u8; 16];
      for byte in window.iter_mut() {
        *byte = next();
      }
      assert_eq!(
        first_masked_eq(&window, FAST_PATTERN_GROUPS),
        scalar_scan(&window),
        "全随机窗口 {window:?}"
      );
    }
    for _ in 0..4096 {
      let pattern = FAST_PATTERN_TABLE[(next() as usize) % FAST_PATTERN_TABLE.len()].0;
      let mut window = [0u8; 16];
      for byte in window.iter_mut() {
        *byte = next();
      }
      let keep = (next() as usize) % 17;
      window[..keep.min(pattern.len())].copy_from_slice(&pattern[..keep.min(pattern.len())]);
      assert_eq!(
        first_masked_eq(&window, FAST_PATTERN_GROUPS),
        scalar_scan(&window),
        "前缀嫁接窗口 {window:?}"
      );
    }
  }
}
