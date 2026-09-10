//! 高性能无分配 Glob 通配符匹配算法 (对齐 Redis / Garnet 规范)
//!
//! 采用非递归贪心有限状态机单次遍历，实现 O(N) 典型时间复杂度与 O(1) 栈空间，零堆分配，杜绝 ReDoS 与递归栈溢出。
//! 支持 `*`（多字符通配）、`?`（单字符通配）、`[...]`（字符集合与区间，`^` 前缀取反）及 `\` 转义。

/// 字节比较辅助：支持可选大小写忽略 (const fn)
#[inline(always)]
const fn byte_eq(a: u8, b: u8, ignore_case: bool) -> bool {
  if ignore_case {
    a.eq_ignore_ascii_case(&b)
  } else {
    a == b
  }
}

/// 区间包含辅助：支持可选大小写忽略与逆序区间自适应 (const fn)
///
/// 端点交换严格对齐 Garnet GlobUtils.Match：先按原始字节序交换，再小写化，
/// 不做小写化后的回退排序——ignoreCase 下逆序字母区间（如 `[k-M]`）经小写化产生
/// 逆序端点（m..k），与 C# 一致判恒不匹配。
#[inline(always)]
const fn in_range(ch: u8, low: u8, high: u8, ignore_case: bool) -> bool {
  let (low, high) = if low <= high {
    (low, high)
  } else {
    (high, low)
  };
  if ignore_case {
    let ch = ch.to_ascii_lowercase();
    let low = low.to_ascii_lowercase();
    let high = high.to_ascii_lowercase();
    ch >= low && ch <= high
  } else {
    ch.wrapping_sub(low) <= high - low
  }
}

/// 解析并匹配中括号字符集 `[...]` (const fn)
///
/// 返回 `(是否匹配, 消耗的模式串字节数)`。
/// 规则逐字节对齐 Microsoft Garnet `GlobUtils.Match`：
/// - 仅 `^` 为取反前缀；`'!'` 为字面集合成员，不取反（与 Redis stringmatchlen 一致）
/// - 区间分支消费 `start '-' end` 三字节后继续扫描：`end` 为 `]` 时不兼作集合终止，
///   如 `[a-]x]` 的集合为 {]..a} ∪ {x}；若区间后模式串即结束，则由未闭合规则收尾
/// - 未闭合的 `[` 消耗剩余全部模式串，按已扫描字符集（含反转）判定
/// - 反斜杠转义字节按字面参与匹配且恒为大小写敏感比较
#[inline]
const fn match_bracket(pat: &[u8], target_byte: u8, ignore_case: bool) -> (bool, usize) {
  let mut i = 1;
  let mut invert = false;
  if i < pat.len() && pat[i] == b'^' {
    invert = true;
    i += 1;
  }

  let mut matched = false;
  while i < pat.len() && pat[i] != b']' {
    if pat[i] == b'\\' && i + 1 < pat.len() {
      // 转义字节与目标字节直接比较，不忽略大小写
      if !matched && pat[i + 1] == target_byte {
        matched = true;
      }
      i += 2;
    } else if pat.len() - i >= 3 && pat[i + 1] == b'-' {
      let start = pat[i];
      let end = pat[i + 2];
      if !matched && in_range(target_byte, start, end, ignore_case) {
        matched = true;
      }
      // 右端点 `]` 仅作为区间上界消费，循环继续寻找真实集合终止 `]`
      i += 3;
    } else {
      if !matched && byte_eq(pat[i], target_byte, ignore_case) {
        matched = true;
      }
      i += 1;
    }
  }

  // 未闭合字符集：消耗剩余全部
  if i >= pat.len() {
    return (matched != invert, pat.len());
  }
  (matched != invert, i + 1)
}

/// 支持可选大小写忽略的 Glob 通配符匹配 (const fn)
///
/// 采用非递归贪心状态机单次遍历，实现 O(N) 线性时间复杂度与 O(1) 栈空间，零堆分配，杜绝 ReDoS 与递归栈溢出。
/// 语义对齐 Microsoft Garnet `GlobUtils.Match`：主循环入口要求双串非空，
/// 故目标为空时仅空模式命中，`("*", "")` 返回 false（与 Redis `stringmatchlen` 一致）；
/// 目标非空时尾部多余 `*` 在目标耗尽后跳过。
#[inline]
pub const fn glob_match_opt(pattern: &[u8], target: &[u8], ignore_case: bool) -> bool {
  if target.is_empty() {
    return pattern.is_empty();
  }

  let mut p = 0;
  let mut t = 0;
  let mut star_p: Option<usize> = None;
  let mut star_t = 0;

  while t < target.len() {
    if p < pattern.len() {
      match pattern[p] {
        b'*' => {
          while p + 1 < pattern.len() && pattern[p + 1] == b'*' {
            p += 1;
          }
          // 尾部星号吞尽剩余目标，O(1) 直接命中
          if p + 1 == pattern.len() {
            return true;
          }
          star_p = Some(p);
          p += 1;
          star_t = t;
          continue;
        }
        b'?' => {
          p += 1;
          t += 1;
          continue;
        }
        b'[' => {
          let (matched, consumed) = match_bracket(pattern.split_at(p).1, target[t], ignore_case);
          if matched {
            p += consumed;
            t += 1;
            continue;
          }
        }
        b'\\' => {
          let pat_byte = if p + 1 < pattern.len() {
            pattern[p + 1]
          } else {
            b'\\'
          };
          if byte_eq(pat_byte, target[t], ignore_case) {
            p += if p + 1 < pattern.len() { 2 } else { 1 };
            t += 1;
            continue;
          }
        }
        c => {
          if byte_eq(c, target[t], ignore_case) {
            p += 1;
            t += 1;
            continue;
          }
        }
      }
    }

    // 字符不匹配，若此前存在星号通配，则回溯星号匹配范围
    if let Some(sp) = star_p {
      p = sp + 1;
      star_t += 1;
      t = star_t;
    } else {
      return false;
    }
  }

  // 目标串耗尽，跳过模式串尾部所有多余的 '*'
  while p < pattern.len() && pattern[p] == b'*' {
    p += 1;
  }

  p == pattern.len()
}

/// Redis / Garnet 规范通配符匹配（默认大小写敏感, const fn）
///
/// libs/server/GlobUtils.cs:Match
#[inline(always)]
pub const fn glob_match(pattern: &[u8], target: &[u8]) -> bool {
  glob_match_opt(pattern, target, false)
}

/// Redis / Garnet 规范通配符匹配（大小写不敏感, const fn）
#[inline(always)]
pub const fn glob_match_nocase(pattern: &[u8], target: &[u8]) -> bool {
  glob_match_opt(pattern, target, true)
}
