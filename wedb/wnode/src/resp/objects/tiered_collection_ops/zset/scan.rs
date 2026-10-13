//! 分层 zset 树内扫描内核：流式留存窗口、分值/字典序区间界、单趟流式遍历
//! 与字典序断点选择（range 读臂共享面）

use std::{
  cmp::{Ordering, Reverse},
  collections::BinaryHeap,
  ops::Range,
  sync::Arc,
};

use wbftree::{BfTreeService, ScanReturnField};
use wcol::{
  types::member_ttl::decode_member,
  zset::{
    comparer::SortedSetComparer,
    sorted_set_object::{SortedSetEntry, SortedSetObject},
    sorted_set_object_impl::SpecialRanges,
  },
};
use wval::expiry_elapsed;

use super::super::common::{scan_all_from_head, score_of_payload};

/// 分层 zset 树内流式留存窗口（[`zset_scan_select`] 的内存上界）
///
/// 判序单点复用 wcol [`SortedSetEntry`] 的 `Ord`（委托
/// [`SortedSetComparer`]，.NET `Double.CompareTo` 口径），与对象层内存
/// `SortedSet` 同序——树内只存 member → 分值，无分序索引，故「序」只能在
/// 扫描侧由同一比较器重建，严禁另立第二套排序结构。
#[derive(Copy, Clone)]
pub(super) enum ZSetWindow {
  /// 全量留存后排序：应答本身即 O(N) 的形态（`ZRANGE k 0 -1`、无 LIMIT 的
  /// ZRANGEBYSCORE / ZRANGEBYLEX）
  All,
  /// 只留序最小 k 条，升序回（有界窗口正向；k == 1 即「序最小者」= 断点求解）
  Head(usize),
  /// 只留序最大 k 条，降序回（有界窗口 REV / 反向形态）
  Tail(usize),
  /// 不留存，仅计数（ZCOUNT / ZRANK / ZLEXCOUNT 与 byIndex 的存活计数趟）
  Count,
}

/// [`zset_scan_select`] 回传束
pub(super) struct ZSetScanOut {
  /// 留存条目：Head/All 升序、Tail 降序、Count 恒空
  pub picked: Vec<SortedSetEntry>,
  /// 存活且命中谓词的条目数（Count 形态的应答值）
  pub matched: u64,
  /// 存活条目总数（与谓词无关：byIndex 的 `set_count` 与 ZREVRANK 的
  /// `Count - rank - 1` 同基准）
  pub alive: u64,
  /// 全树序最大条目的分值，**含到期成员**（对位对象层 `sorted_set.last()`：
  /// C# 该守卫不过滤到期，ZCOUNT 外层守卫唯一取值面）
  pub last_score: Option<f64>,
}

/// 分值判序单点（[`SortedSetComparer`] 的空成员回退臂，等价 .NET
/// `Double.CompareTo`：NaN 小于一切非 NaN，±0.0 相等）
#[inline]
fn zset_score_order(a: f64, b: f64) -> Ordering {
  const EMPTY: &[u8] = &[];
  SortedSetComparer::compare((&a, EMPTY), (&b, EMPTY))
}

/// 分值区间界（解析单点复用 wcol [`SortedSetObject::try_parse_parameter`]）
///
/// 谓词口径逐字对位 C# `GetElementsInRangeByScore` / `SortedSetCount` 循环：
/// 下界 = `GetViewBetween((minValue, null), …)` 哨兵的序裁剪（按 CompareTo，
/// 非 raw `>=`：±0.0 与 NaN 面不等价），上界 = 循环内 raw `>` / `==` 断点判据。
#[derive(Clone)]
pub(super) struct ZScoreBounds {
  pub(super) min: f64,
  pub(super) min_excl: bool,
  pub(super) max: f64,
  pub(super) max_excl: bool,
}

impl ZScoreBounds {
  /// 两界解析（ZCOUNT 臂与 ZRANGE BYSCORE 段同式，与 [`ZLexBounds::parse`] 对称）：
  /// 任一词形非法即 None，错误帧由调用方按各自臂口径落 NOT_VALID_FLOAT
  pub fn parse(min_span: &[u8], max_span: &[u8]) -> Option<Self> {
    let (Some((min, min_excl)), Some((max, max_excl))) = (
      SortedSetObject::try_parse_parameter(min_span),
      SortedSetObject::try_parse_parameter(max_span),
    ) else {
      return None;
    };
    Some(Self {
      min,
      min_excl,
      max,
      max_excl,
    })
  }

  pub(super) fn pass(&self, score: f64) -> bool {
    zset_score_order(score, self.min) != Ordering::Less
      && !(self.min_excl && score == self.min)
      && !(score > self.max || (self.max_excl && score == self.max))
  }
}

/// 字典序区间界（解析单点复用 wcol [`SortedSetObject::try_parse_lex_parameter`]，
/// REV 交换对位 C# `GetElementsInRangeByLex` 首段三换）
#[derive(Clone)]
pub(super) struct ZLexBounds<'a> {
  min: &'a [u8],
  min_excl: bool,
  min_inf: SpecialRanges,
  max: &'a [u8],
  max_excl: bool,
  max_inf: SpecialRanges,
}

impl<'a> ZLexBounds<'a> {
  /// 两界解析 + REV 交换；任一界词形非法 → None（C# 以 i32::MAX 上抛）
  pub fn parse(min_span: &'a [u8], max_span: &'a [u8], reverse: bool) -> Option<Self> {
    let ((min, min_excl, min_inf), (max, max_excl, max_inf)) = (
      SortedSetObject::try_parse_lex_parameter(min_span)?,
      SortedSetObject::try_parse_lex_parameter(max_span)?,
    );
    Some(if reverse {
      Self {
        min: max,
        min_excl: max_excl,
        min_inf: max_inf,
        max: min,
        max_excl: min_excl,
        max_inf: min_inf,
      }
    } else {
      Self {
        min,
        min_excl,
        min_inf,
        max,
        max_excl,
        max_inf,
      }
    })
  }

  /// C# 早空判据：min 为 `+`、max 为 `-`
  pub(super) fn always_empty(&self) -> bool {
    self.min_inf == SpecialRanges::InfiniteMax || self.max_inf == SpecialRanges::InfiniteMin
  }

  /// 下界过滤（成员字节序，对位 C# `SequenceCompareTo`；`-∞` 恒真）
  fn pass_min(&self, member: &[u8]) -> bool {
    if self.min_inf == SpecialRanges::InfiniteMin {
      return true;
    }
    let ord = member.cmp(self.min);
    !(ord == Ordering::Less || (ord == Ordering::Equal && self.min_excl))
  }

  /// 上界过滤（C# `take_while` 的判据面；`+∞` 恒真 ⇒ 不存在断点）
  fn pass_max(&self, member: &[u8]) -> bool {
    if self.max_inf == SpecialRanges::InfiniteMax {
      return true;
    }
    let ord = member.cmp(self.max);
    !(ord == Ordering::Greater || (ord == Ordering::Equal && self.max_excl))
  }
}

/// 分层 zset 树内单趟流式遍历内核：范围选择、区间计数、名次计数三族共用
///
/// 树内记录以 member 为键（member → 8B f64 大端分值 + 可选 TTL 头），本内核自
/// 树头一趟线性扫过全部记录，按 `(分值, 成员)` 序留存至多 `window` 条，
/// **内存随窗口增长而非随键基数增长**——这正是本票消除的「一条 ZRANGE 触发
/// 千万级成员全量反序列化 + 重建 `SortedSetObject`」。计数形态（[`ZSetWindow::
/// Count`]）内存 O(1)。
///
/// 到期成员只过滤不出产（C# 循环首臂 `IsExpired → continue`），本内核**不落
/// 任何删除记录**：维持「树内零墓碑」写形不变量（见本模块头注；物理出账归
/// ZCARD / ZCOLLECT 臂的 expire_sweep_or_rebuild，锁内单趟扫描 + 有到期才
/// 整值重灌，树内同样零墓碑），故本族读臂一律走共享读锁、
/// `dirty` 恒假、不推进 WATCH 栅栏。但 [`ZSetScanOut::last_score`] 含到期成员，
/// 与对象层 `sorted_set.last()` 同基准。
///
/// 栈深口径同 [`exec_tiered_scan`]：底层游标对墓碑的尾递归连跑不受本臂截断
/// 约束，安全性来自写形不变量而非本扫描的窗口。
///
/// 分值载荷非 8B = 编码损坏 → fail-fast `Err(())`（与 [`tiered_materialize_blob`]
/// 的 zset 臂同口径，严禁静默剔除成员后照常应答）。
pub(super) fn zset_scan_select(
  tree: &BfTreeService,
  now: i64,
  window: ZSetWindow,
  mut pred: impl FnMut(f64, &[u8]) -> bool,
) -> Result<ZSetScanOut, ()> {
  let mut picked: Vec<SortedSetEntry> = Vec::new();
  // Head 用最大堆（超容量弹最大 ⇒ 恒留序最小 k 条），Tail 用反序堆（同构造
  // 对偶 ⇒ 恒留序最大 k 条，`into_sorted_vec` 即降序）
  let mut head: BinaryHeap<SortedSetEntry> = BinaryHeap::new();
  let mut tail: BinaryHeap<Reverse<SortedSetEntry>> = BinaryHeap::new();
  let mut matched = 0_u64;
  let mut alive = 0_u64;
  let mut last_score: Option<f64> = None;
  let mut corrupt = false;
  // 扫描 Err 经 [`scan_all_from_head`]（内部 `scan_count` 单点）上抛（ZCOUNT/
  // ZRANK/ZRANGE 各臂应答尚未落帧），严禁折叠——计数臂折叠会把存储故障答成 0 计数
  scan_all_from_head(tree, ScanReturnField::KeyAndValue, |k, v| {
    let (expiry, payload) = decode_member(v);
    let Some(score) = score_of_payload(payload) else {
      log::error!(
        "zset_scan_select: corrupted zset score payload, member='{}'",
        String::from_utf8_lossy(k)
      );
      corrupt = true;
      return false;
    };
    if last_score.is_none_or(|cur| zset_score_order(score, cur) == Ordering::Greater) {
      last_score = Some(score);
    }
    if expiry_elapsed(expiry, now) {
      return true;
    }
    alive += 1;
    if !pred(score, k) {
      return true;
    }
    matched += 1;
    match window {
      ZSetWindow::All => picked.push(SortedSetEntry {
        score,
        member: Arc::from(k),
      }),
      ZSetWindow::Head(cap) if cap > 0 => {
        head.push(SortedSetEntry {
          score,
          member: Arc::from(k),
        });
        if head.len() > cap {
          head.pop();
        }
      }
      ZSetWindow::Tail(cap) if cap > 0 => {
        tail.push(Reverse(SortedSetEntry {
          score,
          member: Arc::from(k),
        }));
        if tail.len() > cap {
          tail.pop();
        }
      }
      _ => {}
    }
    true
  })?;
  if corrupt {
    return Err(());
  }
  let picked = match window {
    ZSetWindow::Head(_) => head.into_sorted_vec(),
    ZSetWindow::Tail(_) => tail.into_sorted_vec().into_iter().map(|e| e.0).collect(),
    // All：树内扫描序为 member 序，须按 (分值, 成员) 单源 Ord 重排，与内存态
    // `SortedSetObject`（C# 内存 `SortedSet` 同序）及本内核 Head/Tail 堆序一致；
    // Count 形态 picked 恒空，排序无副作用
    _ => {
      picked.sort();
      picked
    }
  };
  Ok(ZSetScanOut {
    picked,
    matched,
    alive,
    last_score,
  })
}

/// REV / LIMIT → （留存窗口, 跳数, 取数）单点换算
///
/// 对位 C# 两区间块的同一段：`offset < 0 || count == 0` → 空结果；`count < 0`
/// → 取到末尾（窗口退化为全量）；否则正向取序最小 `offset + count` 条、反向取
/// 序最大 `offset + count` 条，再跳过 `offset` 条——与 C#「全量收集后
/// `skip(offset).take(count)`」逐条同序，堆留存集是它的前缀。
///
/// 折位消费契约（单判据源）：「负 offset 或 count 0」折返 `(Head(0), 0, 0)`，
/// 其中 `take == 0` 恒为折位唯一签名（常规臂 `take ≥ 1`，off/take 同折于本
/// 换算单点）；两消费命令臂（byScore 段与 byLex 段）入口一律以 `take == 0`
/// 即回空帧（对位 C# GetElementsInRangeByScore :1095-1099 与
/// GetElementsInRangeByLex :1004-1010 早退，先于任何扫描与损坏判定），
/// 严禁消费侧再写 `offset < 0 || count == 0` 二次判据（免判据散落多处，
/// 后席只补一径）。
pub(super) fn zset_limit_window(
  reverse: bool,
  valid_limit: bool,
  limit: (i64, i64),
) -> (ZSetWindow, usize, usize) {
  if !valid_limit {
    return (ZSetWindow::All, 0, usize::MAX);
  }
  if limit.0 < 0 || limit.1 == 0 {
    return (ZSetWindow::Head(0), 0, 0);
  }
  let off = limit.0 as usize;
  if limit.1 < 0 {
    return (ZSetWindow::All, off, usize::MAX);
  }
  let take = limit.1 as usize;
  let window = if reverse {
    ZSetWindow::Tail(off.saturating_add(take))
  } else {
    ZSetWindow::Head(off.saturating_add(take))
  };
  (window, off, take)
}

/// 留存集 → 输出方向与 `skip/take` 切片（免二次拷贝）
///
/// Head/All 升序、Tail 降序已由 [`zset_scan_select`] 保证；仅全量形态需按 REV
/// 显式倒置（C# `scored_elements.reverse()` / `all.reverse()` 同臂）。
pub(super) fn zset_windowed_pick(
  mut picked: Vec<SortedSetEntry>,
  window: ZSetWindow,
  reverse: bool,
  off: usize,
  take: usize,
) -> (Vec<SortedSetEntry>, Range<usize>) {
  if reverse && matches!(window, ZSetWindow::All) {
    picked.reverse();
  }
  let start = off.min(picked.len());
  let end = start.saturating_add(take).min(picked.len());
  (picked, start..end)
}

/// 字典序区间树内流式选择（ZRANGEBYLEX 族与 ZLEXCOUNT 共用内核）
///
/// C# `GetElementsInRangeByLex` 的上界是 `take_while`（真 break），而树内扫描序
/// 是 member 序、输出口径是 `(分值, 成员)` 序：同一条目「成员越界」与「分值
/// 靠后」互相交错，越界断点**不可**表达为局部谓词（反例 `{("z",1),("a",2)}`
/// 取 `[a`/`[y`：C# 在 (1,"z") 处 break，(2,"a") 虽在字典窗口内亦不得出现）。
/// 故先以 [`ZSetWindow::Head(1)`] 一趟求出断点 `= 序最小者 ∈ {存活 ∧ 过下界 ∧
/// 未过上界}`（内存 O(1)），第二趟把「序 < 断点」并入谓词选择；上界为 `+`
/// 时 break 永不触发，断点趟直接跳过。两趟均为页级顺序扫，无成员级堆分配。
///
/// 返回 `(留存集, 输出区间, 命中总数)`：区间供范围命令切片，命中总数供
/// ZLEXCOUNT（[`ZSetWindow::Count`] 形态下区间恒空）。
///
/// 入口 `always_empty` 短路为共享核的恒空兜底（C#:1004-1008 首两条件）：
/// ZLEXCOUNT 消费者无 limit 输入，须据此回 0 计数且不触内容——命令臂
///（byLex 段）已以同一判据源并臂先行短路，本处对命令路径为幂等兜底。
pub(super) fn zset_lex_select(
  tree: &BfTreeService,
  now: i64,
  bounds: ZLexBounds<'_>,
  window: ZSetWindow,
  reverse: bool,
  off: usize,
  take: usize,
) -> Result<(Vec<SortedSetEntry>, Range<usize>, u64), ()> {
  if bounds.always_empty() {
    return Ok((Vec::new(), 0..0, 0));
  }
  let barrier = if bounds.max_inf == SpecialRanges::InfiniteMax {
    None
  } else {
    zset_scan_select(tree, now, ZSetWindow::Head(1), |_, member| {
      bounds.pass_min(member) && !bounds.pass_max(member)
    })?
    .picked
    .into_iter()
    .next()
  };
  let scan = zset_scan_select(tree, now, window, |score, member| {
    bounds.pass_min(member)
      && bounds.pass_max(member)
      && barrier.as_ref().is_none_or(|t| {
        SortedSetComparer::compare((&score, member), (&t.score, t.member.as_ref()))
          == Ordering::Less
      })
  })?;
  let matched = scan.matched;
  let (picked, range) = zset_windowed_pick(scan.picked, window, reverse, off, take);
  Ok((picked, range, matched))
}

/// 成员分值点读（到期视同不存在，对位 C# `SortedSetObject.TryGetScore`）
///
/// 与分层 ZSCORE / ZMSCORE 臂同一解码口径（8B f64 大端 + 可选 TTL 头）。以树内
/// 顺序扫描（member 为升序键，命中或越过即停）替代 `read_callback` 点读，与本
/// 模块其余读臂同走 [`scan_all_from_head`] 单源扫描入口——ZRANK 臂随后即接一次
/// 计数扫描，点读 + 扫描在同一服务上交错会命中 bf-tree 游标定位的 mini-page 合并
/// 缺陷，故全程只用扫描入口。`Ok(None)` = 成员不存在；扫描 Err 经其内部的
/// [`scan_count`] 单点上抛（与「成员不存在」严格分态，不得折叠成 null 应答）
pub(super) fn tree_member_score(
  tree: &BfTreeService,
  member: &[u8],
  now: i64,
) -> Result<Option<f64>, ()> {
  let mut score_opt = None;
  scan_all_from_head(tree, ScanReturnField::KeyAndValue, |k, v| {
    match k.cmp(member) {
      // 键升序：越过目标即判不存在，停止扫描
      Ordering::Greater => false,
      // 命中：解码分值（到期视同不存在），停止扫描
      Ordering::Equal => {
        let (expiry, payload) = decode_member(v);
        if !expiry_elapsed(expiry, now) {
          score_opt = score_of_payload(payload);
        }
        false
      }
      Ordering::Less => true,
    }
  })?;
  Ok(score_opt)
}
