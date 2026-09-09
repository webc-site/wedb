//! 高性能无重复随机下标抽样算法（支持位掩码天然保序、栈上去重与 Floyd 抽样）

use whasher::{HashSet, HashSetExt};

/// 栈上定长数组去重排序最大容量（64 个下标，消除中小规模抽样堆分配）
pub const SAMPLE_STACK_CAP: usize = 64;

/// 高性能升序无重复抽样：从 `[0, total)` 中抽取 `count` 个互不相同的下标，输出天然升序。
///
/// 算法层级：
/// 1. $N \le 64$: 单个 `u64` 位掩码，零堆分配，通过位扫描天然输出严格升序索引。
/// 2. $64 < N \le 512$: 栈上 `[u64; 8]` 位掩码，正反向双重优化，通过位扫描天然输出升序索引。
/// 3. $N > 512$: 当 $K \le 64$ 时使用栈上小数组线性查重；当 $K > 64$ 时使用 Floyd 算法。
#[inline]
pub fn sample_distinct_indices(total: usize, count: usize) -> Vec<usize> {
  if count == 0 {
    return Vec::new();
  }
  if count >= total {
    return (0..total).collect();
  }
  if count == 1 {
    return vec![fastrand::usize(0..total)];
  }

  // 场景 1: N <= 64，单字位掩码，正反向双重优化，天然升序
  if total <= 64 {
    let mut mask = 0u64;
    if count.saturating_mul(2) <= total {
      let mut picked = 0;
      while picked < count {
        let idx = fastrand::usize(0..total);
        let bit = 1u64 << idx;
        if (mask & bit) == 0 {
          mask |= bit;
          picked += 1;
        }
      }
    } else {
      let exclude = total - count;
      let mut excluded = 0;
      while excluded < exclude {
        let idx = fastrand::usize(0..total);
        let bit = 1u64 << idx;
        if (mask & bit) == 0 {
          mask |= bit;
          excluded += 1;
        }
      }
      let valid_mask = if total == 64 {
        !0u64
      } else {
        (1u64 << total) - 1
      };
      mask = (!mask) & valid_mask;
    }
    let mut res = Vec::with_capacity(count);
    while mask != 0 {
      let tz = mask.trailing_zeros() as usize;
      res.push(tz);
      mask &= mask - 1;
    }
    return res;
  }

  // 场景 2: 64 < N <= 512，栈上 8 字位掩码，天然升序
  if total <= 512 {
    let mut mask = [0u64; 8];
    if count.saturating_mul(2) <= total {
      let mut picked = 0;
      while picked < count {
        let idx = fastrand::usize(0..total);
        let word = idx / 64;
        let bit = 1u64 << (idx % 64);
        if (mask[word] & bit) == 0 {
          mask[word] |= bit;
          picked += 1;
        }
      }
    } else {
      let exclude = total - count;
      let mut excluded = 0;
      while excluded < exclude {
        let idx = fastrand::usize(0..total);
        let word = idx / 64;
        let bit = 1u64 << (idx % 64);
        if (mask[word] & bit) == 0 {
          mask[word] |= bit;
          excluded += 1;
        }
      }
      for (i, word) in mask.iter_mut().enumerate() {
        let base = i * 64;
        if base >= total {
          *word = 0;
        } else if base + 64 > total {
          let valid_bits = total - base;
          let valid_mask = if valid_bits == 64 {
            !0u64
          } else {
            (1u64 << valid_bits) - 1
          };
          *word = (!*word) & valid_mask;
        } else {
          *word = !*word;
        }
      }
    }

    let mut res = Vec::with_capacity(count);
    for (i, &word) in mask.iter().enumerate() {
      let mut w = word;
      let base = i * 64;
      while w != 0 {
        let tz = w.trailing_zeros() as usize;
        res.push(base + tz);
        w &= w - 1;
      }
    }
    return res;
  }

  // 场景 3: N > 512，若 K <= 64 则栈上定长数组去重排序
  if count <= SAMPLE_STACK_CAP {
    let mut stack_buf = [0usize; SAMPLE_STACK_CAP];
    let mut picked = 0;
    while picked < count {
      let r = fastrand::usize(0..total);
      if !stack_buf[..picked].contains(&r) {
        stack_buf[picked] = r;
        picked += 1;
      }
    }
    stack_buf[..count].sort_unstable();
    return stack_buf[..count].to_vec();
  }

  // 场景 4: N > 512 且 K > 64，使用 Floyd 算法
  if count.saturating_mul(2) <= total {
    let mut picked = HashSet::with_capacity(count);
    for j in (total - count)..total {
      let t = fastrand::usize(0..=j);
      if !picked.insert(t) {
        picked.insert(j);
      }
    }
    let mut v: Vec<usize> = picked.into_iter().collect();
    v.sort_unstable();
    v
  } else {
    let exclude = total - count;
    let mut excluded = HashSet::with_capacity(exclude);
    for j in (total - exclude)..total {
      let t = fastrand::usize(0..=j);
      if !excluded.insert(t) {
        excluded.insert(j);
      }
    }
    let mut sorted_ex: Vec<usize> = excluded.into_iter().collect();
    sorted_ex.sort_unstable();

    let mut v = Vec::with_capacity(count);
    let mut cur = 0;
    for ex in sorted_ex {
      if ex > cur {
        v.extend(cur..ex);
      }
      cur = ex + 1;
    }
    if cur < total {
      v.extend(cur..total);
    }
    v
  }
}
