//! 解析态参数汇集面（借用零拷贝视图 [`collect_arg_views`] 与单分配物化
//! [`ArgStore`] 两口，借用形态与 `&mut self` 形态分派落点各自单源）。

use smallvec::SmallVec;
use wresp::session_parse_state::SessionParseState;

use super::core::RespServerSession;

/// 参数收集的栈上内联容量：不多于此数目的参数完全零堆分配（RESP 命令参数
/// 个数常见 1-3，借用面与物化面共用此单源）
pub(super) const ARG_INLINE: usize = 8;

/// 解析态参数视图批量收集（借用零拷贝；parse_state 与接收缓冲显式传参，
/// 借用按字段拆分，可与 `self.output` 等正交字段的可变借用并存）
///
/// 唯一的借用收集入口：被调方为 `&self` / 字段级借用（集群切面、只读探针、
/// 纯写出函数）时优先本口。
pub(crate) fn collect_arg_views<'a>(
  parse_state: &SessionParseState,
  recv_buffer: &'a [u8],
) -> SmallVec<[&'a [u8]; ARG_INLINE]> {
  let count = parse_state.count;
  let mut args = SmallVec::with_capacity(count);
  args.extend((0..count).map(|i| parse_state.arg_in(recv_buffer, i)));
  args
}

/// 解析态参数的单分配物化容器
///
/// C# 分派臂把 parseState 直递命令实现，零物化；rust 侧多数命令处理器取
/// `&mut self`（会话方法族），接收缓冲的借用与之不可共存，故在此一次性物化：
/// 全部参数字节落单次分配，视图表借用本容器（栈上 SmallVec）因而与被调方的
/// `&mut self` 正交。借用能够流到的落点直用 [`collect_arg_views`]，不经本容器。
pub(crate) struct ArgStore {
  /// 参数字节按 [`Self::lens`] 顺序紧密排布
  data: Vec<u8>,
  /// 各参数长度
  lens: SmallVec<[usize; ARG_INLINE]>,
}

impl ArgStore {
  /// 参数视图表（零字节拷贝；仅参数数超 [`ARG_INLINE`] 时多一次指针表分配）
  pub(crate) fn views(&self) -> SmallVec<[&[u8]; ARG_INLINE]> {
    let mut off = 0usize;
    self
      .lens
      .iter()
      .map(|&len| {
        let arg = &self.data[off..off + len];
        off += len;
        arg
      })
      .collect()
  }
}

impl RespServerSession {
  /// 汇集解析态参数为单分配物化（见 [`ArgStore`]；被调方取 `&mut self` 的
  /// 分派落点用）
  ///
  /// 借用能够流到的落点一律直用 [`collect_arg_views`]，不经本入口。
  pub(crate) fn collect_args_store(&self) -> ArgStore {
    let args = (0..self.parse_state.count).map(|i| self.parse_state.arg_in(&self.recv_buffer, i));
    // 两遍索引（首遍只读长度、不触碰字节）换 data 的恰容量单次分配
    let lens: SmallVec<[usize; ARG_INLINE]> = args.clone().map(<[u8]>::len).collect();
    let mut data = Vec::with_capacity(lens.iter().sum());
    for arg in args {
      data.extend_from_slice(arg);
    }
    ArgStore { data, lens }
  }
}
