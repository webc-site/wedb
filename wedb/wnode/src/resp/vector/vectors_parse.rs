//! 向量命令共享取参骨架（VADD/VSIM 参数解析的公共承接，自
//! resp_server_session_vectors.rs 拆出）：格式/量化/度量选项表、参数游标
//! [`Cur`]、向量取参 [`RespServerSessionVectors::vector_operand`] 与
//! 各档非法文案表（逐字保持各命令原文案）。

use std::borrow::Cow;

use wbase::num::{strict_f32, strict_i32};
use wresp::{options::equals_ignore_case, wrong_num_args};
use wvector::{VectorDistanceMetricType, VectorQuantType, VectorValueType, store::StoreCallbacks};

use super::{
  resp_server_session_vectors::{
    ERR_FP32_MULTIPLE_OF_4, ERR_INVALID_VECTOR_SPEC, ERR_VALUES_COUNT_MUST_BE_POSITIVE,
    ERR_VALUES_MUST_BE_FLOAT, ERR_VSIM_EXPECTED_KIND, RespServerSessionVectors, VectorReply,
  },
  vector_manager::MAX_VECTOR_DIMENSIONS,
};

/// VADD 的 M 取值边界（libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVADD 的 MinM/MaxM）。
pub(super) const MIN_M: i32 = 4;
pub(super) const MAX_M: i32 = 4_096;

/// VSIM 默认结果数（NetworkVSIM 的 DefaultResultSetSize 语义：count ??= 10）。
pub(super) const DEFAULT_VSIM_COUNT: i32 = 10;
/// VSIM 默认搜索探索因子（count ??= 10 / searchExplorationFactor ??= 100）。
pub(super) const DEFAULT_VSIM_EF: i32 = 100;
/// VSIM 默认过滤过取放大（maxFilteringEffort ??= 16）。
pub(super) const DEFAULT_VSIM_FILTER_EF: i32 = 16;
/// VSIM 默认距离截断上限（对齐 C# RespServerSessionVectors.cs:860 epsilon ?? 2f）。
pub(super) const DEFAULT_VSIM_EPSILON: f32 = 2.0;

/// VADD 参数个数不足文案（`wrong_num_args!` 编译期展开；入口与取参共用单点）。
pub(super) const WNA_VADD: &[u8] = wrong_num_args!("VADD").as_bytes();
/// VSIM 参数个数不足文案（同上）。
pub(super) const WNA_VSIM: &[u8] = wrong_num_args!("VSIM").as_bytes();

/// VADD 默认建索引探索因子（对齐 C# buildExplorationFactor ??= 200）。
pub(super) const DEFAULT_VADD_BUILD_EF: i32 = 200;
/// VADD 默认每层链数（对齐 C# numLinks ??= 16）。
pub(super) const DEFAULT_VADD_NUM_LINKS: i32 = 16;

/// 向量取参形态（[`VALUE_KINDS`] 表载荷；ELE 由 VSIM 单独识别，不入表）。
#[derive(Copy, PartialEq, Clone)]
enum ValueKind {
  /// FP32 原始字节
  Fp32,
  /// VALUES num v1..vn 文本浮点
  Values,
  /// XU8 原始字节
  Xu8,
  /// XI8 原始字节
  Xi8,
}

/// 格式关键字 → 取参形态（表序即 VADD 原 if-chain 识别序；关键字互斥故顺序无关）。
const VALUE_KINDS: &[(&[u8], ValueKind)] = &[
  (b"FP32", ValueKind::Fp32),
  (b"VALUES", ValueKind::Values),
  (b"XU8", ValueKind::Xu8),
  // XB8 为 XU8 的向后兼容别名，推荐 XU8
  (b"XB8", ValueKind::Xu8),
  (b"XI8", ValueKind::Xi8),
];

/// 量化器选项关键字 → 量化类型（表序即原识别序；重复指定判据由调用点承接）。
pub(super) const QUANT_OPTS: &[(&[u8], VectorQuantType)] = &[
  (b"NOQUANT", VectorQuantType::NoQuant),
  (b"Q8", VectorQuantType::Q8),
  (b"BIN", VectorQuantType::Bin),
  (b"XNOQUANT_U8", VectorQuantType::XnoQuantU8),
  // XPREQ8 为 XNOQUANT_U8 的向后兼容别名，推荐 XNOQUANT_U8
  (b"XPREQ8", VectorQuantType::XnoQuantU8),
  (b"XNOQUANT_I8", VectorQuantType::XnoQuantI8),
  (b"XBIN_I8", VectorQuantType::XbinI8),
  (b"XBIN_U8", VectorQuantType::XbinU8),
];

/// 距离度量关键字 → 度量类型（VADD XDISTANCE_METRIC 值参）。
pub(super) const METRIC_OPTS: &[(&[u8], VectorDistanceMetricType)] = &[
  (b"L2", VectorDistanceMetricType::L2),
  (b"COSINE", VectorDistanceMetricType::Cosine),
  (b"IP", VectorDistanceMetricType::InnerProduct),
  (
    b"XCOSINE_NORMALIZED",
    VectorDistanceMetricType::XCosineNormalized,
  ),
];

/// 编译期选项表命中：`arg` 大小写无关命中表内关键字即取其载荷。
#[inline]
pub(super) fn lookup<T: Copy>(table: &[(&[u8], T)], arg: &[u8]) -> Option<T> {
  table
    .iter()
    .find(|(kw, _)| equals_ignore_case(arg, kw))
    .map(|(_, v)| *v)
}

/// 值格式每维字节数（FP32 4 字节，XU8/XI8 单字节）。
#[inline]
const fn dim_bytes(value_type: VectorValueType) -> usize {
  match value_type {
    VectorValueType::FP32 => 4,
    _ => 1,
  }
}

/// X 系量化器（与 REDUCE 互斥，对齐 C# 调 storageApi 前的 BadParams 判定）。
#[inline]
pub(super) const fn is_x_quant(quant: VectorQuantType) -> bool {
  matches!(
    quant,
    VectorQuantType::XbinU8
      | VectorQuantType::XbinI8
      | VectorQuantType::XnoQuantU8
      | VectorQuantType::XnoQuantI8
  )
}

/// 向量取参的非法文案集（VADD/VSIM 各成一表，逐字保持原命令文案）。
pub(super) struct OperandErrs {
  /// 关键字后取尽缺参
  pub(super) missing: &'static [u8],
  /// FP32 字节数非每维字节数倍数
  pub(super) align: &'static [u8],
  /// VALUES 数量非法
  pub(super) count: &'static [u8],
  /// VALUES 单值非法
  pub(super) float: &'static [u8],
  /// 格式关键字未知
  pub(super) kind: &'static [u8],
}

/// VADD 向量取参文案（缺参走 wrong-num-args，余下四项原文案统一 invalid spec）。
pub(super) const VADD_OPERAND: OperandErrs = OperandErrs {
  missing: WNA_VADD,
  align: ERR_INVALID_VECTOR_SPEC,
  count: ERR_INVALID_VECTOR_SPEC,
  float: ERR_INVALID_VECTOR_SPEC,
  kind: ERR_INVALID_VECTOR_SPEC,
};

/// VSIM 向量取参文案（各色非法文案逐一区别于 VADD，禁合并）。
pub(super) const VSIM_OPERAND: OperandErrs = OperandErrs {
  missing: WNA_VSIM,
  align: ERR_FP32_MULTIPLE_OF_4,
  count: ERR_VALUES_COUNT_MUST_BE_POSITIVE,
  float: ERR_VALUES_MUST_BE_FLOAT,
  kind: ERR_VSIM_EXPECTED_KIND,
};

/// 向量取参产出（VADD/VSIM 共用形态）。
pub(super) struct Operand<'a> {
  /// 值格式（ELE 形态为 Invalid）
  pub(super) value_type: VectorValueType,
  /// 向量字节（原始字节借参数缓冲、VALUES 文本浮点落堆；ELE 为空）
  pub(super) values: Cow<'a, [u8]>,
  /// 维度（值格式步长折算）
  pub(super) dims: usize,
  /// ELE 形态的查询元素（仅 VSIM 产出）
  pub(super) element: Option<&'a [u8]>,
}

/// 参数游标：命令入口「识别关键字 → 取值 → 校验 → 前进」骨架的承接。
/// 缺失/非法应答文案一律由调用点供给，逐字保持各命令原文案。
pub(super) struct Cur<'a> {
  args: &'a [&'a [u8]],
  ix: usize,
}

impl<'a> Cur<'a> {
  #[inline]
  pub(super) fn new(args: &'a [&'a [u8]], ix: usize) -> Self {
    Self { args, ix }
  }

  /// 当前位置参数（取尽为空切片，永不命中表内关键字）。
  #[inline]
  pub(super) fn peek(&self) -> &'a [u8] {
    self.args.get(self.ix).copied().unwrap_or_default()
  }

  /// 剩余参数未取尽（选项循环的续跑判据）。
  #[inline]
  pub(super) fn more(&self) -> bool {
    self.ix < self.args.len()
  }

  /// 当前位置是否命中关键字 `kw`（不前进）。
  #[inline]
  pub(super) fn at(&self, kw: &[u8]) -> bool {
    equals_ignore_case(self.peek(), kw)
  }

  /// 跳过当前位置（无值参的开关型选项）。
  #[inline]
  pub(super) fn skip(&mut self) {
    self.ix += 1;
  }

  /// 取当前参并前进；取尽回空串（C# 对缺参不做显式校验的形态）。
  #[inline]
  pub(super) fn next_or_empty(&mut self) -> &'a [u8] {
    let arg = self.peek();
    self.ix += 1;
    arg
  }

  /// 取当前参并前进；取尽即回 `missing` 文案应答。
  pub(super) fn next(&mut self, missing: &'static [u8]) -> Result<&'a [u8], VectorReply> {
    let arg = self
      .args
      .get(self.ix)
      .copied()
      .ok_or_else(|| VectorReply::err(missing))?;
    self.ix += 1;
    Ok(arg)
  }

  /// 取当前关键字后的值参并前进两位；缺失回 `missing`。
  pub(super) fn value(&mut self, missing: &'static [u8]) -> Result<&'a [u8], VectorReply> {
    self.skip();
    self.next(missing)
  }

  /// 当前关键字后跟严格 i32 值参：缺失回 `missing`、非法或不满足 `ok` 回
  /// `bad`（`strict_i32` 对齐 C# `parseState.TryGetInt`，前导零拒收系 rust
  /// 严格收口，见 doc/zh/deviations.md §32）。
  #[inline]
  pub(super) fn i32_value(
    &mut self,
    missing: &'static [u8],
    ok: impl Fn(i32) -> bool,
    bad: &'static [u8],
  ) -> Result<i32, VectorReply> {
    strict_i32(self.value(missing)?)
      .filter(|&v| ok(v))
      .ok_or_else(|| VectorReply::err(bad))
  }

  /// 当前关键字后跟严格 f32 值参（应答口径同 [`Self::i32_value`]）。
  #[inline]
  pub(super) fn f32_value(
    &mut self,
    missing: &'static [u8],
    ok: impl Fn(f32) -> bool,
    bad: &'static [u8],
  ) -> Result<f32, VectorReply> {
    strict_f32(self.value(missing)?, true)
      .filter(|&v| ok(v))
      .ok_or_else(|| VectorReply::err(bad))
  }
}

impl<S: StoreCallbacks> RespServerSessionVectors<S> {
  /// 向量取参骨架（VADD/VSIM 共用）：格式关键字命中 [`VALUE_KINDS`] 后按
  /// 形态取参——原始字节（FP32/XU8/XI8）零拷贝借接收缓冲参数、VALUES 文本
  /// 浮点落堆；非法文案一律由 `errs` 供给，逐字保持各命令原文案。
  /// 维度上限以 usize 比对（上限远小于 isize::MAX，与原 i32/usize 两种
  /// 写法在全部可达输入上同判）。
  pub(super) fn vector_operand<'a>(
    &self,
    cur: &mut Cur<'a>,
    errs: &OperandErrs,
  ) -> Result<Operand<'a>, VectorReply> {
    let Some(kind) = lookup(VALUE_KINDS, cur.peek()) else {
      return Err(VectorReply::err(errs.kind));
    };
    if kind == ValueKind::Values {
      let n = strict_i32(cur.value(errs.missing)?)
        .filter(|n| *n > 0)
        .ok_or_else(|| VectorReply::err(errs.count))?;
      if n as usize > MAX_VECTOR_DIMENSIONS as usize {
        return Err(self.abort_too_many_dimensions());
      }
      if cur.ix + n as usize > cur.args.len() {
        return Err(VectorReply::err(errs.missing));
      }
      let mut floats = Vec::with_capacity(n as usize * 4);
      for _ in 0..n {
        let f =
          strict_f32(cur.next(errs.float)?, true).ok_or_else(|| VectorReply::err(errs.float))?;
        floats.extend_from_slice(&f.to_le_bytes());
      }
      return Ok(Operand {
        value_type: VectorValueType::FP32,
        values: Cow::Owned(floats),
        dims: n as usize,
        element: None,
      });
    }
    // 原始字节形态：每维字节数由格式定（FP32 另校验 4 字节对齐）
    let value_type = match kind {
      ValueKind::Fp32 => VectorValueType::FP32,
      ValueKind::Xu8 => VectorValueType::XU8,
      ValueKind::Xi8 => VectorValueType::XI8,
      ValueKind::Values => VectorValueType::Invalid,
    };
    let bytes = cur.value(errs.missing)?;
    let step = dim_bytes(value_type);
    if step > 1 && bytes.len() % step != 0 {
      return Err(VectorReply::err(errs.align));
    }
    let dims = bytes.len() / step;
    if dims > MAX_VECTOR_DIMENSIONS as usize {
      return Err(self.abort_too_many_dimensions());
    }
    Ok(Operand {
      value_type,
      values: Cow::Borrowed(bytes),
      dims,
      element: None,
    })
  }

  /// 维度上限错误。
  pub(super) fn abort_too_many_dimensions(&self) -> VectorReply {
    VectorReply::Error(
      format!("ERR vector exceeds maximum of {MAX_VECTOR_DIMENSIONS} dimensions")
        .into_bytes()
        .into(),
    )
  }
}
