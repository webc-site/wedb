//! 排序批量装载内核 (树内写入唯一真值路径)
//!
//! rust 侧工程优化：C# 互操作层 BfTreeService 只有两个单条 `Insert` 重载
//! (libs/native/bftree-garnet/BfTreeService.cs)，无记录级批量写入接口；其
//! 「非逐条建树」能力是文件级流式换入 (RangeIndexChunkedDeserializer 收树文件
//! 字节流 → RangeIndexManager 的 RestoreTree 打开整树)，与 rust 侧「内存容器
//! 就地升阶灌入」不同源。故本内核按 .agents/skills/transpile/SKILL.md
//! 「批量接口 (Batching API) 单次折叠机制」三条子准则实现：
//! - 单次借用：整批只做一次 [`BfTreeService::with_tree`] (本层「条带写锁 + 纪元」
//!   的对应物，见 service/mod.rs 单句柄单视图说明)，消除逐条 N 次原子借用；
//! - 栈上排序：批次按键升序、同键按输入序排列后下刷，连续键集中命中同一叶页，
//!   页内定位由引擎 `LeafNode::insert` 的 lower_bound 二分承担，页填充到近满才
//!   分裂，压降页分裂与环形缓冲逐出抖动 (引擎乱序插入每次都自根下降并可能触发
//!   CircularBufferFull 逐出)；
//! - 单次计数：返回去重后落刷条数 / 真实新增键数，宿主据此一次性回写 meta.size。
//!
//! 单条 [`insert`](BfTreeService::insert) 是本内核的一元素特例，全仓不存在第二条
//! 树内写入路径。

use std::result::Result as StdResult;

use bf_tree::{BfTree, LeafInsertResult, LeafReadResult};
use smallvec::SmallVec;

use super::{BfTreeService, ops::with_read_buffer};
use crate::types::BfTreeInsertResult;

/// 栈上排序缓冲的 inline 容量 (下标数组，8B × 96 = 768B 栈；键值本体零搬运零拷贝，
/// 超出仅溢出下标，覆盖 RI 批量写与单条写入的常态批次)
const ORDER_INLINE: usize = 96;

impl BfTreeService {
  /// 排序批量装载：整批一次引擎借用，按键升序下刷，返回去重后实际落刷条数。
  ///
  /// 语义与「按输入顺序逐条 [`insert`](BfTreeService::insert)」完全等价：同批重复
  /// 键仅输入序末位落刷 (排序对同键保留原始相对序)，树内终态与逐条覆盖一致。
  /// 前置校验先于任何写入：任一条目值为空即整批 `InvalidKV` 拒绝且零副作用
  /// (引擎叶子插入 `debug_assert!(!value.is_empty())`，release 构建下空值行为未定义)。
  ///
  /// 空批次返回 0 且零借用。
  #[inline]
  pub fn bulk_load<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    &self,
    entries: &[(K, V)],
  ) -> StdResult<u64, BfTreeInsertResult> {
    self.write_batch(entries, false)
  }

  /// 排序批量 upsert：与 [`bulk_load`](Self::bulk_load) 同内核，额外逐条前查存在性，
  /// 返回真实新增键数 (宿主 meta.size 增量依据；引擎 `LeafInsertResult` 不报覆盖，
  /// 前查是 O(1) 计数规约的必要成本)。
  #[inline]
  pub fn upsert<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    &self,
    entries: &[(K, V)],
  ) -> StdResult<u64, BfTreeInsertResult> {
    self.write_batch(entries, true)
  }

  /// 树内写入唯一内核：栈上排序批次 → 单次借用引擎 → 按序下刷。
  ///
  /// `probe_new` 为真时逐条点读判存在性并只计新增，否则恒计落刷条数。
  fn write_batch<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    &self,
    entries: &[(K, V)],
    probe_new: bool,
  ) -> StdResult<u64, BfTreeInsertResult> {
    if entries.is_empty() {
      return Ok(0);
    }

    // 排序对象是输入下标而非法键值本体：零搬运，且同键比较落回下标即天然
    // 保持输入相对序 (重复键后者胜语义的来源)
    let mut order: SmallVec<[usize; ORDER_INLINE]> = SmallVec::with_capacity(entries.len());
    for (idx, (_, val)) in entries.iter().enumerate() {
      if val.as_ref().is_empty() {
        return Err(BfTreeInsertResult::InvalidKV);
      }
      order.push(idx);
    }
    order.sort_unstable_by(|&a, &b| {
      entries[a]
        .0
        .as_ref()
        .cmp(entries[b].0.as_ref())
        .then(a.cmp(&b))
    });

    self
      .with_tree(|tree| {
        // 存在性前查与点读同口径复用统一缓冲选路 (整批一次选路，非逐条)
        if probe_new {
          with_read_buffer(self.max_record_size(), |buf| {
            Self::flush_sorted(tree, entries, &order, Some(buf))
          })
        } else {
          Self::flush_sorted(tree, entries, &order, None)
        }
      })
      .unwrap_or(Err(BfTreeInsertResult::InvalidArguments))
  }

  /// 已排序批次的下刷循环：同键相邻且按输入序排列，仅每组末位落刷；
  /// `probe_buf` 为 `Some` 时逐条前查存在性并计真实新增键数，否则计落刷条数
  fn flush_sorted<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    tree: &BfTree,
    entries: &[(K, V)],
    order: &[usize],
    mut probe_buf: Option<&mut [u8]>,
  ) -> StdResult<u64, BfTreeInsertResult> {
    let probe = probe_buf.is_some();
    let mut counted = 0u64;
    let mut iter = order.iter().copied().peekable();
    while let Some(idx) = iter.next() {
      let (key, val) = (&entries[idx].0, &entries[idx].1);
      // 同批重复键去重保末值：下一条同键即本条被覆盖，跳过落刷
      if iter
        .peek()
        .is_some_and(|&next| entries[next].0.as_ref() == key.as_ref())
      {
        continue;
      }
      let (k, v) = (key.as_ref(), val.as_ref());
      // 引擎不回报是否覆盖已有键，upsert 的新增判定须前查 (与点读同一缓冲口径)
      let exists = probe_buf
        .as_deref_mut()
        .is_some_and(|buf| matches!(tree.read(k, buf), LeafReadResult::Found(_)));
      match tree.insert(k, v) {
        LeafInsertResult::Success => counted += if probe { u64::from(!exists) } else { 1 },
        LeafInsertResult::InvalidKV(_) => return Err(BfTreeInsertResult::InvalidKV),
      }
    }
    Ok(counted)
  }
}
