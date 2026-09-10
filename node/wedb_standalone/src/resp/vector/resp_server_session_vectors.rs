//! Vector Set 命令的网络层（对标 libs/server/Resp/Vector/RespServerSessionVectors.cs）
//!
//! C# 为 RespServerSession 的 partial，直接消费 parseState / storageApi /
//! networkSender；Rust 侧 RespServerSession 属并行域，此处以
//! `&[&[u8]]` 参数面 + [`VectorReply`] 应答面承接同一命令语义
//! （选项解析、重复项报错、默认值、RESP2/RESP3 应答布局）。
//!
//! 数值解析复用 [`crate::resp::parser::session_parse_state`] 的
//! `strict_i32`/`strict_f32`（逐项对齐 C# `parseState.TryGetInt`/
//! `TryGetFloat` 的严格语义），错误文案与命令应答对齐 C# 字面量。

use std::sync::Arc;

use super::{
  vector_manager::{
    MAX_EXPLORATION_FACTOR, MAX_FILTERING_SCALE_FACTOR, MAX_RETRIEVE_COUNT, MAX_VECTOR_DIMENSIONS,
    VectorManager, VectorManagerResult,
  },
  vector_manager__index::Index,
  vector_manager__locking::CreateIndexParams,
  vector_types::{VectorDistanceMetricType, VectorQuantType, VectorValueType},
};
use crate::{
  objects::types::object_output::ObjectOutput,
  resp::{
    cmd_strings::GENERIC_ERR_WRONG_NUM_ARGS,
    parser::session_parse_state::{strict_f32, strict_i32},
  },
  storage::session::common::array_key_iteration_functions::cluster_slot,
};

/// VADD 的 M 取值边界（libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVADD 的 MinM/MaxM）。
const MIN_M: i32 = 4;
const MAX_M: i32 = 4_096;

/// VSIM 默认结果数（NetworkVSIM 的 DefaultResultSetSize 语义：count ??= 10）。
const DEFAULT_VSIM_COUNT: i32 = 10;
/// VSIM 默认搜索探索因子（count ??= 10 / searchExplorationFactor ??= 100）。
const DEFAULT_VSIM_EF: i32 = 100;
/// VSIM 默认过滤过取放大（maxFilteringEffort ??= 16）。
const DEFAULT_VSIM_FILTER_EF: i32 = 16;

// ── 错误文案常量（逐字节对齐 C# 字面量；自带 RESP 前缀原样写出） ──

/// NetworkVADD/NetworkVSIM 统一预览未启用文案。
const ERR_VECTOR_SET_DISABLED: &[u8] = b"ERR Vector Set (preview) commands are not enabled";
/// AbortVectorSetWrongType：对齐 Redis 行为、不指名具体类型（注意：无句点，
/// 与 CmdStrings.RESP_ERR_WRONG_TYPE 的带句点版本不同）。
const ERR_VECTOR_SET_WRONG_TYPE: &[u8] =
  b"WRONGTYPE Operation against a key holding the wrong kind of value";
const ERR_INVALID_VECTOR_SPEC: &[u8] = b"ERR invalid vector specification";
const ERR_REDUCE_MUST_BE_POSITIVE: &[u8] = b"REDUCE dimension must be > 0";
const ERR_REDUCE_EXCEEDS_DIMS: &[u8] = b"ERR REDUCE dimension must be <= vector dimensions";
const ERR_INVALID_OPTION_AFTER_ELEMENT: &[u8] = b"ERR invalid option after element";
const ERR_QUANT_SPECIFIED_TWICE: &[u8] = b"Quantization specified multiple times";
const ERR_EF_RANGE: &[u8] = b"ERR EF must be an integer between 1 and 1000000";
const ERR_M_RANGE: &[u8] = b"ERR M must be an integer between 4 and 4096";
const ERR_INVALID_DISTANCE_METRIC: &[u8] = b"ERR invalid XDISTANCE_METRIC";
const ERR_EMPTY_VECTOR_SET_KEY: &[u8] = b"ERR Vector Set key cannot be empty";
const ERR_QUANT_MISMATCH: &[u8] = super::vector_manager::ERR_QUANTIZATION_MISMATCH;
const ERR_FP32_MULTIPLE_OF_4: &[u8] = b"FP32 values must be multiple of 4-bytes in size";
const ERR_VALUES_COUNT_MUST_BE_POSITIVE: &[u8] = b"VALUES count must > 0";
const ERR_VALUES_MUST_BE_FLOAT: &[u8] = b"VALUES value must be valid float";
const ERR_VSIM_EXPECTED_KIND: &[u8] = b"VSIM expected ELE, FP32, or VALUES";
const ERR_COUNT_RANGE: &[u8] = b"ERR COUNT must be an integer between 0 and 100000000";
const ERR_EPSILON_MUST_BE_POSITIVE: &[u8] = b"EPSILON must be float > 0";
const ERR_FILTER_EF_RANGE: &[u8] = b"ERR FILTER-EF must be an integer between 4 and 256";
const ERR_UNKNOWN_OPTION: &[u8] = b"Unknown option";
/// 元素不在集合中（C# VectorSetElementSimilarity MissingElement 出参文案）。
pub(crate) const ERR_ELEMENT_NOT_IN_SET: &[u8] = b"Element not in Vector Set";
const ERR_VEMB_UNEXPECTED_OPTION: &[u8] = b"Unexpected option to VEMB";
const ERR_KEY_NOT_FOUND: &[u8] = b"ERR Key not found";
const ERR_VLINKS_UNEXPECTED_OPTION: &[u8] = b"ERR Unexpected option";
const ERR_EXPECTED_INTEGER_COUNT: &[u8] = b"ERR expected integer count";
/// 超出服务端上下文配额（C# 为 GarnetException；Rust 防御性转为错误应答）。
const ERR_MAX_ALLOCATIONS_EXCEEDED: &[u8] =
  b"ERR Maximum Vector Set allocations exceeded, cannot issue new context";

/// 命令应答（RESP 数据模型）。
#[derive(Debug, Clone, PartialEq)]
pub enum VectorReply {
  /// 简单字符串（+...）。
  Simple(Vec<u8>),
  /// 错误（-...，含完整前缀）。
  Error(Vec<u8>),
  /// 整数。
  Integer(i64),
  /// 批量字符串（None = NULL）。
  Bulk(Option<Vec<u8>>),
  /// 数组。
  Array(Vec<VectorReply>),
  /// 空（NULL）数组 `*-1\r\n`（对齐 RespWriteUtils.TryWriteNullArray）。
  NullArray,
  /// RESP3 映射（RESP2 退化为键值交错的扁平数组）。
  Map(Vec<(VectorReply, VectorReply)>),
  /// RESP3 双精度浮点。
  Double(f64),
  /// 布尔（RESP3）。
  Boolean(bool),
}

impl VectorReply {
  /// 编码为 RESP2 字节（Double 退化为 bulk 字符串、Map 退化为双倍长度数组）。
  pub fn encode_resp2(&self, out: &mut Vec<u8>) {
    match self {
      VectorReply::Simple(s) => {
        out.push(b'+');
        out.extend_from_slice(s);
        out.extend_from_slice(b"\r\n");
      }
      VectorReply::Error(e) => {
        out.push(b'-');
        out.extend_from_slice(e);
        out.extend_from_slice(b"\r\n");
      }
      VectorReply::Integer(i) => {
        out.extend_from_slice(format!(":{i}\r\n").as_bytes());
      }
      VectorReply::Bulk(v) => match v {
        Some(v) => {
          out.extend_from_slice(format!("${}\r\n", v.len()).as_bytes());
          out.extend_from_slice(v);
          out.extend_from_slice(b"\r\n");
        }
        None => out.extend_from_slice(b"$-1\r\n"),
      },
      VectorReply::NullArray => out.extend_from_slice(b"*-1\r\n"),
      VectorReply::Map(pairs) => {
        out.extend_from_slice(format!("*{}\r\n", pairs.len() * 2).as_bytes());
        for (k, v) in pairs {
          k.encode_resp2(out);
          v.encode_resp2(out);
        }
      }
      VectorReply::Double(d) => {
        let text = ObjectOutput::format_double(*d);
        out.extend_from_slice(format!("${}\r\n{}\r\n", text.len(), text).as_bytes());
      }
      VectorReply::Boolean(b) => {
        out.extend_from_slice(if *b { b"$1\r\n1\r\n" } else { b"$0\r\n0\r\n" });
      }
      VectorReply::Array(items) => {
        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
        for item in items {
          item.encode_resp2(out);
        }
      }
    }
  }

  /// 编码为 RESP3 字节（Double 为 `,`、Boolean 为 `#`、Map 为 `%`）。
  pub fn encode_resp3(&self, out: &mut Vec<u8>) {
    match self {
      VectorReply::Double(d) => {
        out.push(b',');
        out.extend_from_slice(ObjectOutput::format_double(*d).as_bytes());
        out.extend_from_slice(b"\r\n");
      }
      VectorReply::Boolean(b) => {
        out.extend_from_slice(if *b { b"#t\r\n" } else { b"#f\r\n" });
      }
      VectorReply::Map(pairs) => {
        out.extend_from_slice(format!("%{}\r\n", pairs.len()).as_bytes());
        for (k, v) in pairs {
          k.encode_resp3(out);
          v.encode_resp3(out);
        }
      }
      VectorReply::Array(items) => {
        out.extend_from_slice(format!("*{}\r\n", items.len()).as_bytes());
        for item in items {
          item.encode_resp3(out);
        }
      }
      other => other.encode_resp2(out),
    }
  }
}

/// 大小写不敏感比较（对齐 EqualsUpperCaseSpanIgnoringCase）。
fn eq_ignore_case(a: &[u8], b: &[u8]) -> bool {
  a.eq_ignore_ascii_case(b)
}

/// Vector Set 命令处理（会话承接层）。
pub struct RespServerSessionVectors {
  /// 向量集合管理器。
  pub manager: Arc<VectorManager>,
}

impl RespServerSessionVectors {
  /// 创建命令处理层。
  pub fn new(manager: Arc<VectorManager>) -> Self {
    Self { manager }
  }

  /// 键 → 索引记录读取。
  fn read_index(&self, key: &[u8]) -> Option<Index> {
    let stored = self.manager.read_stored_index(key)?;
    Index::from_bytes(&stored)
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:AbortVectorSetWrongType
  ///
  /// 键存在但不是 Vector Set（索引记录尺寸/格式非法）的错误路径。
  pub fn abort_vector_set_wrong_type(&self, key: &[u8]) -> Option<VectorReply> {
    if self.manager.read_stored_index(key).is_some() {
      Some(VectorReply::Error(ERR_VECTOR_SET_WRONG_TYPE.to_vec()))
    } else {
      None
    }
  }

  /// Vector Set 预览未启用的统一拒绝。
  fn abort_disabled(&self) -> VectorReply {
    VectorReply::Error(ERR_VECTOR_SET_DISABLED.to_vec())
  }

  /// 参数数量错误（对齐 CmdStrings.GenericErrWrongNumArgs 模板）。
  fn abort_wrong_number_of_arguments(cmd: &str) -> VectorReply {
    VectorReply::Error(GENERIC_ERR_WRONG_NUM_ARGS.replace("{0}", cmd).into_bytes())
  }

  // ======================== VADD ========================

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVADD
  ///
  /// `VADD key [REDUCE dim] (FP32 | XU8 | XI8 | VALUES num) vector element
  ///   [CAS] [NOQUANT | Q8 | BIN | XNOQUANT_U8 | XPREQ8 | XNOQUANT_I8 | XBIN_I8 | XBIN_U8]
  ///   [EF build-exploration-factor] [SETATTR attributes] [M numlinks]
  ///   [XDISTANCE_METRIC L2 | COSINE | IP | XCOSINE_NORMALIZED]`
  pub fn network_vadd(&self, args: &[&[u8]]) -> VectorReply {
    self.network_vadd_impl(args, false)
  }

  /// VADD 内部实现（`resp3` 选择成功/重复应答的布尔或整数形态）。
  pub fn network_vadd_impl(&self, args: &[&[u8]], resp3: bool) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() < 4 {
      return Self::abort_wrong_number_of_arguments("VADD");
    }

    let key = args[0];
    let mut cur_ix = 1usize;

    // REDUCE dim
    let mut reduce_dims = 0u32;
    if eq_ignore_case(args[cur_ix], b"REDUCE") {
      cur_ix += 1;
      // C# TryGetInt（严格 i32）；缺失/非法/非正统一报 REDUCE 文案
      let v = args.get(cur_ix).and_then(|a| strict_i32(a));
      let Some(v) = v.filter(|v| *v > 0) else {
        return VectorReply::Error(ERR_REDUCE_MUST_BE_POSITIVE.to_vec());
      };
      reduce_dims = v as u32;
      cur_ix += 1;
    }

    // 向量格式分派：FP32 / VALUES num / XU8|XB8 / XI8
    let Some(&kind) = args.get(cur_ix) else {
      return Self::abort_wrong_number_of_arguments("VADD");
    };
    let value_type;
    let values: Vec<u8>;
    let vector_dims: i32;
    if eq_ignore_case(kind, b"FP32") {
      cur_ix += 1;
      let Some(as_bytes) = args.get(cur_ix) else {
        return Self::abort_wrong_number_of_arguments("VADD");
      };
      if as_bytes.len() % 4 != 0 {
        return VectorReply::Error(ERR_INVALID_VECTOR_SPEC.to_vec());
      }
      vector_dims = (as_bytes.len() / 4) as i32;
      if vector_dims > MAX_VECTOR_DIMENSIONS as i32 {
        return self.abort_too_many_dimensions();
      }
      value_type = VectorValueType::FP32;
      values = as_bytes.to_vec();
      cur_ix += 1;
    } else if eq_ignore_case(kind, b"VALUES") {
      cur_ix += 1;
      let Some(count_raw) = args.get(cur_ix) else {
        return Self::abort_wrong_number_of_arguments("VADD");
      };
      let Some(n) = strict_i32(count_raw).filter(|n| *n > 0) else {
        return VectorReply::Error(ERR_INVALID_VECTOR_SPEC.to_vec());
      };
      cur_ix += 1;
      if n > MAX_VECTOR_DIMENSIONS as i32 {
        return self.abort_too_many_dimensions();
      }
      if cur_ix + n as usize > args.len() {
        return Self::abort_wrong_number_of_arguments("VADD");
      }
      value_type = VectorValueType::FP32;
      let mut floats = Vec::with_capacity(n as usize * 4);
      for _ in 0..n {
        let Some(f) = args.get(cur_ix).and_then(|a| strict_f32(a, true)) else {
          return VectorReply::Error(ERR_INVALID_VECTOR_SPEC.to_vec());
        };
        floats.extend_from_slice(&f.to_le_bytes());
        cur_ix += 1;
      }
      vector_dims = n;
      values = floats;
    } else if eq_ignore_case(kind, b"XU8") || eq_ignore_case(kind, b"XB8") {
      // XB8 为向后兼容别名，推荐 XU8
      cur_ix += 1;
      let Some(as_bytes) = args.get(cur_ix) else {
        return Self::abort_wrong_number_of_arguments("VADD");
      };
      vector_dims = as_bytes.len() as i32;
      if vector_dims > MAX_VECTOR_DIMENSIONS as i32 {
        return self.abort_too_many_dimensions();
      }
      value_type = VectorValueType::XU8;
      values = as_bytes.to_vec();
      cur_ix += 1;
    } else if eq_ignore_case(kind, b"XI8") {
      cur_ix += 1;
      let Some(as_bytes) = args.get(cur_ix) else {
        return Self::abort_wrong_number_of_arguments("VADD");
      };
      vector_dims = as_bytes.len() as i32;
      if vector_dims > MAX_VECTOR_DIMENSIONS as i32 {
        return self.abort_too_many_dimensions();
      }
      value_type = VectorValueType::XI8;
      values = as_bytes.to_vec();
      cur_ix += 1;
    } else {
      return VectorReply::Error(ERR_INVALID_VECTOR_SPEC.to_vec());
    }

    if reduce_dims as i32 > vector_dims {
      return VectorReply::Error(ERR_REDUCE_EXCEEDS_DIMS.to_vec());
    }

    // 元素键
    let Some(&element) = args.get(cur_ix) else {
      return Self::abort_wrong_number_of_arguments("VADD");
    };
    cur_ix += 1;

    // 选项循环（C#：元素后顺序未指定，逐一识别）
    let mut cas_seen = false;
    let mut quant: Option<VectorQuantType> = None;
    let mut build_ef: Option<i32> = None;
    let mut attributes: Option<&[u8]> = None;
    let mut num_links: Option<i32> = None;
    let mut distance_metric: Option<VectorDistanceMetricType> = None;

    while cur_ix < args.len() {
      let opt = args[cur_ix];
      // REDUCE 在元素之后无论何种写法均非法
      if eq_ignore_case(opt, b"REDUCE") {
        return VectorReply::Error(ERR_INVALID_OPTION_AFTER_ELEMENT.to_vec());
      }
      if eq_ignore_case(opt, b"CAS") {
        if cas_seen {
          return VectorReply::Error(b"CAS specified multiple times".to_vec());
        }
        // CAS 仅识别不处理
        cas_seen = true;
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"NOQUANT") {
        if quant.is_some() {
          return VectorReply::Error(ERR_QUANT_SPECIFIED_TWICE.to_vec());
        }
        quant = Some(VectorQuantType::NoQuant);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"Q8") {
        if quant.is_some() {
          return VectorReply::Error(ERR_QUANT_SPECIFIED_TWICE.to_vec());
        }
        quant = Some(VectorQuantType::Q8);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"BIN") {
        if quant.is_some() {
          return VectorReply::Error(ERR_QUANT_SPECIFIED_TWICE.to_vec());
        }
        quant = Some(VectorQuantType::Bin);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"XNOQUANT_U8") || eq_ignore_case(opt, b"XPREQ8") {
        // XPREQ8 为向后兼容别名，推荐 XNOQUANT_U8
        if quant.is_some() {
          return VectorReply::Error(ERR_QUANT_SPECIFIED_TWICE.to_vec());
        }
        quant = Some(VectorQuantType::XNoQuant_U8);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"XNOQUANT_I8") {
        if quant.is_some() {
          return VectorReply::Error(ERR_QUANT_SPECIFIED_TWICE.to_vec());
        }
        quant = Some(VectorQuantType::XNoQuant_I8);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"XBIN_I8") {
        if quant.is_some() {
          return VectorReply::Error(ERR_QUANT_SPECIFIED_TWICE.to_vec());
        }
        quant = Some(VectorQuantType::XBin_I8);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"XBIN_U8") {
        if quant.is_some() {
          return VectorReply::Error(ERR_QUANT_SPECIFIED_TWICE.to_vec());
        }
        quant = Some(VectorQuantType::XBin_U8);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"EF") {
        if build_ef.is_some() {
          return VectorReply::Error(b"EF specified multiple times".to_vec());
        }
        cur_ix += 1;
        let Some(v) = args.get(cur_ix) else {
          return VectorReply::Error(ERR_INVALID_OPTION_AFTER_ELEMENT.to_vec());
        };
        let Some(v) = strict_i32(v).filter(|v| *v > 0 && *v <= MAX_EXPLORATION_FACTOR as i32)
        else {
          return Self::abort_ef_range();
        };
        build_ef = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"SETATTR") {
        if attributes.is_some() {
          return VectorReply::Error(b"SETATTR specified multiple times".to_vec());
        }
        cur_ix += 1;
        let Some(attr) = args.get(cur_ix) else {
          return VectorReply::Error(ERR_INVALID_OPTION_AFTER_ELEMENT.to_vec());
        };
        attributes = Some(attr);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"M") {
        if num_links.is_some() {
          return VectorReply::Error(b"M specified multiple times".to_vec());
        }
        cur_ix += 1;
        let Some(v) = args.get(cur_ix) else {
          return VectorReply::Error(ERR_INVALID_OPTION_AFTER_ELEMENT.to_vec());
        };
        let Some(v) = strict_i32(v).filter(|v| (MIN_M..=MAX_M).contains(v)) else {
          return Self::abort_m_range();
        };
        num_links = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"XDISTANCE_METRIC") {
        if distance_metric.is_some() {
          return VectorReply::Error(b"XDISTANCE_METRIC specified multiple times".to_vec());
        }
        cur_ix += 1;
        let Some(metric) = args.get(cur_ix) else {
          return VectorReply::Error(ERR_INVALID_OPTION_AFTER_ELEMENT.to_vec());
        };
        distance_metric = Some(if eq_ignore_case(metric, b"L2") {
          VectorDistanceMetricType::L2
        } else if eq_ignore_case(metric, b"COSINE") {
          VectorDistanceMetricType::Cosine
        } else if eq_ignore_case(metric, b"IP") {
          VectorDistanceMetricType::InnerProduct
        } else if eq_ignore_case(metric, b"XCOSINE_NORMALIZED") {
          VectorDistanceMetricType::XCosineNormalized
        } else {
          return VectorReply::Error(ERR_INVALID_DISTANCE_METRIC.to_vec());
        });
        cur_ix += 1;
      } else {
        return VectorReply::Error(ERR_INVALID_OPTION_AFTER_ELEMENT.to_vec());
      }
    }

    if key.is_empty() {
      return VectorReply::Error(ERR_EMPTY_VECTOR_SET_KEY.to_vec());
    }

    // 默认值（对齐 C#：Q8 / 200 / 16 / L2）
    let quant = quant.unwrap_or(VectorQuantType::Q8);
    let build_ef = build_ef.unwrap_or(200) as u32;
    let num_links = num_links.unwrap_or(16) as u32;
    let distance_metric = distance_metric.unwrap_or(VectorDistanceMetricType::L2);

    // X 系量化器与 REDUCE 互斥：C# 在调storageApi 前以此判 BadParams（自定义
    // 文案为空 → 回落 quantization mismatch 文案）
    if matches!(
      quant,
      VectorQuantType::XBin_U8
        | VectorQuantType::XBin_I8
        | VectorQuantType::XNoQuant_U8
        | VectorQuantType::XNoQuant_I8
    ) && reduce_dims != 0
    {
      return VectorReply::Error(ERR_QUANT_MISMATCH.to_vec());
    }

    // 向量维度（供 manager 校验）
    let dims = values.len() as u32
      / match value_type {
        VectorValueType::FP32 => 4,
        _ => 1,
      };

    // 读或创建索引记录（缺失或需重建时按选项建原生索引，对齐 C# ReadOrCreateVectorIndex）
    let params = CreateIndexParams {
      hash_slot: cluster_slot(key),
      dims,
      reduce_dims,
      quant,
      build_exploration_factor: build_ef,
      num_links,
      distance_metric,
    };
    let (index, _lock) = match self.manager.read_or_create_vector_index(key, Some(&params)) {
      Ok(acquired) => acquired,
      Err(_) => return VectorReply::Error(ERR_MAX_ALLOCATIONS_EXCEEDED.to_vec()),
    };

    // 经 manager 执行插入（重复/参数不匹配校验在 try_add 内）
    let stored = index.to_bytes();
    match self.manager.try_add(
      key,
      &stored,
      element,
      value_type,
      &values,
      attributes.unwrap_or(b""),
      reduce_dims,
      quant,
      num_links,
      distance_metric,
    ) {
      Ok(VectorManagerResult::OK) => {
        // 对齐 C#：成功 → RESP3 布尔真 / RESP2 整数 1
        if resp3 {
          VectorReply::Boolean(true)
        } else {
          VectorReply::Integer(1)
        }
      }
      Ok(VectorManagerResult::Duplicate) => {
        // 对齐 C#：重复 → RESP3 布尔假 / RESP2 整数 0
        if resp3 {
          VectorReply::Boolean(false)
        } else {
          VectorReply::Integer(0)
        }
      }
      Ok(VectorManagerResult::BadParams) => {
        VectorReply::Error(VectorManager::error_msg(VectorManagerResult::BadParams).to_vec())
      }
      Ok(other) => VectorReply::Error(VectorManager::error_msg(other).to_vec()),
      Err(e) => VectorReply::Error(e.message),
    }
  }

  /// 维度上限错误。
  fn abort_too_many_dimensions(&self) -> VectorReply {
    VectorReply::Error(
      format!("ERR vector exceeds maximum of {MAX_VECTOR_DIMENSIONS} dimensions").into_bytes(),
    )
  }

  /// EF 范围错误。
  fn abort_ef_range() -> VectorReply {
    VectorReply::Error(ERR_EF_RANGE.to_vec())
  }

  /// M 范围错误。
  fn abort_m_range() -> VectorReply {
    VectorReply::Error(ERR_M_RANGE.to_vec())
  }

  // ======================== VSIM ========================

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVSIM
  ///
  /// `VSIM key (ELE | FP32 | XU8 | XI8 | VALUES num) (vector | element)
  ///   [WITHSCORES] [WITHATTRIBS] [COUNT num] [EPSILON delta] [EF factor]
  ///   [FILTER expression] [FILTER-EF effort] [TRUTH] [NOTHREAD]`
  pub fn network_vsim(&self, args: &[&[u8]]) -> VectorReply {
    self.network_vsim_impl(args, false)
  }

  /// VSIM 内部实现（`resp3` 选择应答协议版本）。
  pub fn network_vsim_impl(&self, args: &[&[u8]], resp3: bool) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() < 3 {
      return Self::abort_wrong_number_of_arguments("VSIM");
    }

    let key = args[0];
    let kind = args[1];
    let mut cur_ix = 2usize;

    let mut element: Option<&[u8]> = None;
    let mut value_type = VectorValueType::Invalid;
    let mut values: Vec<u8> = Vec::new();

    if eq_ignore_case(kind, b"ELE") {
      // C# 对缺失的元素参数不做显式校验（空切片语义），此处以空串承接
      element = Some(args.get(cur_ix).copied().unwrap_or(b""));
      cur_ix += 1;
    } else if eq_ignore_case(kind, b"FP32") {
      let Some(as_bytes) = args.get(cur_ix) else {
        return Self::abort_wrong_number_of_arguments("VSIM");
      };
      if as_bytes.len() % 4 != 0 {
        return VectorReply::Error(ERR_FP32_MULTIPLE_OF_4.to_vec());
      }
      if as_bytes.len() / 4 > MAX_VECTOR_DIMENSIONS as usize {
        return self.abort_too_many_dimensions();
      }
      value_type = VectorValueType::FP32;
      values = as_bytes.to_vec();
      cur_ix += 1;
    } else if eq_ignore_case(kind, b"XU8") || eq_ignore_case(kind, b"XB8") {
      let Some(as_bytes) = args.get(cur_ix) else {
        return Self::abort_wrong_number_of_arguments("VSIM");
      };
      if as_bytes.len() > MAX_VECTOR_DIMENSIONS as usize {
        return self.abort_too_many_dimensions();
      }
      value_type = VectorValueType::XU8;
      values = as_bytes.to_vec();
      cur_ix += 1;
    } else if eq_ignore_case(kind, b"XI8") {
      let Some(as_bytes) = args.get(cur_ix) else {
        return Self::abort_wrong_number_of_arguments("VSIM");
      };
      if as_bytes.len() > MAX_VECTOR_DIMENSIONS as usize {
        return self.abort_too_many_dimensions();
      }
      value_type = VectorValueType::XI8;
      values = as_bytes.to_vec();
      cur_ix += 1;
    } else if eq_ignore_case(kind, b"VALUES") {
      let Some(count_raw) = args.get(cur_ix) else {
        return Self::abort_wrong_number_of_arguments("VSIM");
      };
      let Some(n) = strict_i32(count_raw).filter(|n| *n > 0) else {
        return VectorReply::Error(ERR_VALUES_COUNT_MUST_BE_POSITIVE.to_vec());
      };
      if n > MAX_VECTOR_DIMENSIONS as i32 {
        return self.abort_too_many_dimensions();
      }
      cur_ix += 1;
      if cur_ix + n as usize > args.len() {
        return Self::abort_wrong_number_of_arguments("VSIM");
      }
      value_type = VectorValueType::FP32;
      for _ in 0..n {
        let Some(f) = args.get(cur_ix).and_then(|a| strict_f32(a, true)) else {
          return VectorReply::Error(ERR_VALUES_MUST_BE_FLOAT.to_vec());
        };
        values.extend_from_slice(&f.to_le_bytes());
        cur_ix += 1;
      }
    } else {
      return VectorReply::Error(ERR_VSIM_EXPECTED_KIND.to_vec());
    }

    // 选项（默认值对齐 C#：count=10 / delta=2 / EF=100 / FILTER-EF=16）
    let mut with_scores = false;
    let mut with_attribs = false;
    let mut count: Option<i32> = None;
    let mut epsilon: Option<f32> = None;
    let mut ef: Option<i32> = None;
    let mut filter: Option<&[u8]> = None;
    let mut filter_ef: Option<i32> = None;
    let mut truth = false;
    let mut no_thread = false;

    while cur_ix < args.len() {
      let opt = args[cur_ix];
      if eq_ignore_case(opt, b"WITHSCORES") {
        if with_scores {
          return VectorReply::Error(b"WITHSCORES specified multiple times".to_vec());
        }
        with_scores = true;
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"WITHATTRIBS") {
        if with_attribs {
          return VectorReply::Error(b"WITHATTRIBS specified multiple times".to_vec());
        }
        with_attribs = true;
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"COUNT") {
        if count.is_some() {
          return VectorReply::Error(b"COUNT specified multiple times".to_vec());
        }
        cur_ix += 1;
        let Some(v) = args.get(cur_ix) else {
          return Self::abort_wrong_number_of_arguments("VSIM");
        };
        let Some(v) = strict_i32(v).filter(|v| *v >= 0 && *v <= MAX_RETRIEVE_COUNT as i32) else {
          return Self::abort_count_range();
        };
        count = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"EPSILON") {
        if epsilon.is_some() {
          return VectorReply::Error(b"EPSILON specified multiple times".to_vec());
        }
        cur_ix += 1;
        let Some(v) = args.get(cur_ix) else {
          return Self::abort_wrong_number_of_arguments("VSIM");
        };
        let Some(v) = strict_f32(v, true).filter(|v| *v > 0.0) else {
          return VectorReply::Error(ERR_EPSILON_MUST_BE_POSITIVE.to_vec());
        };
        epsilon = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"EF") {
        if ef.is_some() {
          return VectorReply::Error(b"EF specified multiple times".to_vec());
        }
        cur_ix += 1;
        let Some(v) = args.get(cur_ix) else {
          return Self::abort_wrong_number_of_arguments("VSIM");
        };
        let Some(v) = strict_i32(v).filter(|v| *v > 0 && *v <= MAX_EXPLORATION_FACTOR as i32)
        else {
          return Self::abort_ef_range();
        };
        ef = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"FILTER") {
        if filter.is_some() {
          return VectorReply::Error(b"FILTER specified multiple times".to_vec());
        }
        cur_ix += 1;
        let Some(f) = args.get(cur_ix) else {
          return Self::abort_wrong_number_of_arguments("VSIM");
        };
        filter = Some(f);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"FILTER-EF") {
        if filter_ef.is_some() {
          return VectorReply::Error(b"FILTER-EF specified multiple times".to_vec());
        }
        cur_ix += 1;
        let Some(v) = args.get(cur_ix) else {
          return Self::abort_wrong_number_of_arguments("VSIM");
        };
        let Some(v) = strict_i32(v).filter(|v| *v >= 4 && *v <= MAX_FILTERING_SCALE_FACTOR as i32)
        else {
          return Self::abort_filter_ef_range();
        };
        filter_ef = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"TRUTH") {
        if truth {
          return VectorReply::Error(b"TRUTH specified multiple times".to_vec());
        }
        // TODO 语义与 C# 一致：仅识别
        truth = true;
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"NOTHREAD") {
        if no_thread {
          return VectorReply::Error(b"NOTHREAD specified multiple times".to_vec());
        }
        // C# 忽略 NOTHREAD
        no_thread = true;
        cur_ix += 1;
      } else {
        return VectorReply::Error(ERR_UNKNOWN_OPTION.to_vec());
      }
    }
    let _ = (truth, no_thread);
    // EPSILON / FILTER-EF 参与检索（对齐 C# 传参语义：maxFilteringEffort ??= 16
    // 放大过滤候选队列；delta 截断最大距离 —— 缺省不截断，原生库默认 2f 不可复刻）
    let delta = epsilon.unwrap_or(f32::INFINITY);
    let filter_effort = filter_ef.unwrap_or(DEFAULT_VSIM_FILTER_EF).max(0) as usize;

    let count = count.unwrap_or(DEFAULT_VSIM_COUNT);

    // 键不存在：对齐 C# NOTFOUND → 空数组（非错误）
    let Some(stored) = self.manager.read_stored_index(key) else {
      return VectorReply::Array(Vec::new());
    };

    let result = match element {
      Some(elem) => self.manager.element_similarity(
        &stored,
        elem,
        count.max(0) as usize,
        ef.unwrap_or(DEFAULT_VSIM_EF).max(0) as usize,
        filter.unwrap_or(b""),
        filter_effort,
        delta,
        with_attribs,
      ),
      None => self.manager.value_similarity(
        &stored,
        value_type,
        &values,
        count.max(0) as usize,
        ef.unwrap_or(DEFAULT_VSIM_EF).max(0) as usize,
        filter.unwrap_or(b""),
        filter_effort,
        delta,
        with_attribs,
      ),
    };

    let output = match result {
      Ok(out) => out,
      Err(e) => return VectorReply::Error(e.message),
    };

    // 拆包命中
    let ids: Vec<&[u8]> = super::vector_manager::unpack_length_prefixed(&output.output_ids);
    let attrs: Vec<&[u8]> =
      super::vector_manager::unpack_length_prefixed(&output.output_attributes);

    if resp3 {
      Self::write_resp3_result(
        count as usize,
        &ids,
        &output.output_distances,
        &output.filter_bitmap,
        with_attribs.then_some(&attrs),
        with_scores,
      )
    } else {
      Self::write_resp2_result(
        count as usize,
        &ids,
        &output.output_distances,
        &output.filter_bitmap,
        with_attribs.then_some(&attrs),
        with_scores,
      )
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:WriteRESP3Result
  ///
  /// RESP3：无 score/attr 时为普通数组；否则为 map（id → score/attr/[score, attr]），
  /// 过滤未过项剔除，空属性写 NULL。
  pub fn write_resp3_result(
    count: usize,
    ids: &[&[u8]],
    distances: &[f32],
    filter_bitmap: &[u8],
    attributes: Option<&Vec<&[u8]>>,
    with_scores: bool,
  ) -> VectorReply {
    let has_filter = !filter_bitmap.is_empty();
    let with_attribs = attributes.is_some();
    // C#：有位图时输出上限 = popcount(bitmap)，否则全部命中；再与 count 取小
    let total_found = ids.len();
    let output_count = if has_filter {
      filter_bitmap
        .iter()
        .map(|b| b.count_ones() as usize)
        .sum::<usize>()
        .min(count)
    } else {
      total_found.min(count)
    };

    let mut plain = Vec::new();
    let mut map = Vec::new();
    let mut written = 0usize;
    for (result_index, id) in ids.iter().enumerate().take(total_found) {
      if written >= output_count {
        break;
      }
      // 过滤位图：未通过项跳过
      if has_filter && (filter_bitmap[result_index >> 3] >> (result_index & 7)) & 1 == 0 {
        continue;
      }
      let score = VectorReply::Double(f64::from(
        distances.get(result_index).copied().unwrap_or(0.0),
      ));
      let attr_reply = match attributes
        .and_then(|attrs| attrs.get(result_index))
        .copied()
      {
        Some(a) if !a.is_empty() => VectorReply::Bulk(Some(a.to_vec())),
        // RESP3：空属性写 NULL
        _ => VectorReply::Bulk(None),
      };
      if !with_scores && !with_attribs {
        plain.push(VectorReply::Bulk(Some(id.to_vec())));
      } else {
        // 分数与属性齐备时以二元素数组为 map 值（顺序：score → attr）
        let value = if with_scores && with_attribs {
          VectorReply::Array(vec![score, attr_reply])
        } else if with_scores {
          score
        } else {
          attr_reply
        };
        map.push((VectorReply::Bulk(Some(id.to_vec())), value));
      }
      written += 1;
    }
    if !with_scores && !with_attribs {
      VectorReply::Array(plain)
    } else {
      VectorReply::Map(map)
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:WriteRESP2Result
  ///
  /// RESP2：扁平数组（WITHSCORES 时 id/score 成对；WITHATTRIBS 附加属性；
  /// 二者齐备时长度为三倍），空属性写空 bulk 字符串。
  pub fn write_resp2_result(
    count: usize,
    ids: &[&[u8]],
    distances: &[f32],
    filter_bitmap: &[u8],
    attributes: Option<&Vec<&[u8]>>,
    with_scores: bool,
  ) -> VectorReply {
    let has_filter = !filter_bitmap.is_empty();
    let with_attribs = attributes.is_some();
    let total_found = ids.len();
    let output_count = if has_filter {
      filter_bitmap
        .iter()
        .map(|b| b.count_ones() as usize)
        .sum::<usize>()
        .min(count)
    } else {
      total_found.min(count)
    };

    let mut items = Vec::new();
    let mut written = 0usize;
    for (result_index, id) in ids.iter().enumerate().take(total_found) {
      if written >= output_count {
        break;
      }
      if has_filter && (filter_bitmap[result_index >> 3] >> (result_index & 7)) & 1 == 0 {
        continue;
      }
      items.push(VectorReply::Bulk(Some(id.to_vec())));
      if with_scores {
        items.push(VectorReply::Double(f64::from(
          distances.get(result_index).copied().unwrap_or(0.0),
        )));
      }
      if with_attribs && let Some(attr) = attributes.and_then(|attrs| attrs.get(result_index)) {
        items.push(VectorReply::Bulk(Some(attr.to_vec())));
      }
      written += 1;
    }
    VectorReply::Array(items)
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVEMB
  ///
  /// `VEMB key element [RAW]` → 嵌入向量数组；RAW 时输出
  /// [量化器名, 原始量化字节, 范数, (Q8 量化范围)]。
  pub fn network_vemb(&self, args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() < 2 || args.len() > 3 {
      return Self::abort_wrong_number_of_arguments("VEMB");
    }
    let raw = if args.len() == 3 {
      if !eq_ignore_case(args[2], b"RAW") {
        return VectorReply::Error(ERR_VEMB_UNEXPECTED_OPTION.to_vec());
      }
      true
    } else {
      false
    };

    // C#：键/元素缺失统一写空数组
    let Some(stored) = self.manager.read_stored_index(args[0]) else {
      return VectorReply::Array(Vec::new());
    };

    if raw {
      return match self.manager.try_get_raw_embedding(&stored, args[1]) {
        Some((bytes, quant, norm, range)) => {
          // 量化器名映射：BIN/XBIN_* → bin；Q8/XNOQUANT_* → q8；NOQUANT → fp32
          let quant_name: &[u8] = match quant {
            VectorQuantType::Bin | VectorQuantType::XBin_I8 | VectorQuantType::XBin_U8 => b"bin",
            VectorQuantType::Q8 | VectorQuantType::XNoQuant_U8 | VectorQuantType::XNoQuant_I8 => {
              b"q8"
            }
            VectorQuantType::NoQuant => b"fp32",
            VectorQuantType::Invalid => b"fp32",
          };
          let mut items = vec![
            VectorReply::Simple(quant_name.to_vec()),
            VectorReply::Bulk(Some(bytes)),
            VectorReply::Double(norm),
          ];
          // 仅 Q8 追加量化范围
          if quant == VectorQuantType::Q8 {
            items.push(VectorReply::Double(range.unwrap_or(0.0)));
          }
          VectorReply::Array(items)
        }
        None => VectorReply::Array(Vec::new()),
      };
    }

    match self.manager.try_get_embedding(&stored, args[1]) {
      Some(embedding) => VectorReply::Array(
        embedding
          .into_iter()
          .map(|v| VectorReply::Double(f64::from(v)))
          .collect(),
      ),
      None => VectorReply::Array(Vec::new()),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVCARD
  pub fn network_vcard(&self, args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() != 1 {
      return Self::abort_wrong_number_of_arguments("VCARD");
    }
    let Some(index) = self.read_index(args[0]) else {
      return VectorReply::Integer(0);
    };
    VectorReply::Integer(self.manager.service.card(index.context) as i64)
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVDIM
  pub fn network_vdim(&self, args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() != 1 {
      return Self::abort_wrong_number_of_arguments("VDIM");
    }
    let Some(index) = self.read_index(args[0]) else {
      // 对齐 C# NOTFOUND → "ERR Key not found"
      return VectorReply::Error(ERR_KEY_NOT_FOUND.to_vec());
    };
    VectorReply::Integer(i64::from(index.dimensions))
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVGETATTR
  pub fn network_vgetattr(&self, args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() != 2 {
      return Self::abort_wrong_number_of_arguments("VGETATTR");
    }
    let Some(index) = self.read_index(args[0]) else {
      // 对齐 C# NOTFOUND → null
      return VectorReply::Bulk(None);
    };
    match self.manager.service.get_attribute(index.context, args[1]) {
      Some(attr) => VectorReply::Bulk(Some(attr)),
      None => VectorReply::Bulk(None),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVINFO
  ///
  /// `VINFO key` → 14 项元信息（quant-type/distance-metric/input-vector-dimensions/
  /// reduced-dimensions/build-exploration-factor/num-links/size）。
  pub fn network_vinfo(&self, args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() != 1 {
      return Self::abort_wrong_number_of_arguments("VINFO");
    }
    let Some(index) = self.read_index(args[0]) else {
      // 对齐 C# NOTFOUND → null 数组
      return VectorReply::NullArray;
    };
    // 对齐 C# 小写枚举名（Invalid 在 C# 侧抛异常；此处防御性报错）
    let quant: &[u8] = match index.quant_type {
      VectorQuantType::NoQuant => b"f32",
      VectorQuantType::Bin => b"bin",
      VectorQuantType::Q8 => b"q8",
      VectorQuantType::XNoQuant_U8 => b"xnoquant_u8",
      VectorQuantType::XNoQuant_I8 => b"xnoquant_i8",
      VectorQuantType::XBin_I8 => b"xbin_i8",
      VectorQuantType::XBin_U8 => b"xbin_u8",
      VectorQuantType::Invalid => {
        return VectorReply::Error(b"ERR Invalid VectorQuantType".to_vec());
      }
    };
    let metric: &[u8] = match index.distance_metric {
      VectorDistanceMetricType::Cosine => b"cosine",
      VectorDistanceMetricType::InnerProduct => b"inner-product",
      VectorDistanceMetricType::L2 => b"l2",
      VectorDistanceMetricType::XCosineNormalized => b"cosine-normalized",
    };
    let bulk_u32 = |v: u32| VectorReply::Bulk(Some(v.to_string().into_bytes()));
    VectorReply::Array(vec![
      VectorReply::Simple(b"quant-type".to_vec()),
      VectorReply::Simple(quant.to_vec()),
      VectorReply::Simple(b"distance-metric".to_vec()),
      VectorReply::Simple(metric.to_vec()),
      VectorReply::Simple(b"input-vector-dimensions".to_vec()),
      bulk_u32(index.dimensions),
      VectorReply::Simple(b"reduced-dimensions".to_vec()),
      bulk_u32(index.reduce_dims),
      VectorReply::Simple(b"build-exploration-factor".to_vec()),
      bulk_u32(index.build_exploration_factor),
      VectorReply::Simple(b"num-links".to_vec()),
      bulk_u32(index.num_links),
      VectorReply::Simple(b"size".to_vec()),
      VectorReply::Bulk(Some(
        self
          .manager
          .service
          .card(index.context)
          .to_string()
          .into_bytes(),
      )),
    ])
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVISMEMBER
  pub fn network_vismember(&self, args: &[&[u8]]) -> VectorReply {
    self.network_vismember_impl(args, false)
  }

  /// VISMEMBER 内部实现（RESP3 以布尔应答）。
  pub fn network_vismember_impl(&self, args: &[&[u8]], resp3: bool) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() != 2 {
      return Self::abort_wrong_number_of_arguments("VISMEMBER");
    }
    let member = self
      .read_index(args[0])
      .is_some_and(|index| self.manager.is_member(&index.to_bytes(), args[1]));
    match (member, resp3) {
      (true, true) => VectorReply::Boolean(true),
      (false, true) => VectorReply::Boolean(false),
      (true, false) => VectorReply::Integer(1),
      (false, false) => VectorReply::Integer(0),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVLINKS
  ///
  /// `VLINKS key element [WITHSCORES]`。C# 侧输出为 TODO（恒 +OK）；
  /// 此处返回层 0 邻接的实际元素（超集语义），键/元素缺失写 null。
  pub fn network_vlinks(&self, args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() != 2 && args.len() != 3 {
      return Self::abort_wrong_number_of_arguments("VLINKS");
    }
    if args.len() == 3 && !eq_ignore_case(args[2], b"WITHSCORES") {
      return VectorReply::Error(ERR_VLINKS_UNEXPECTED_OPTION.to_vec());
    }
    let Some(index) = self.read_index(args[0]) else {
      return VectorReply::Bulk(None);
    };
    match self.manager.service.links_of(index.context, args[1]) {
      Some(links) => VectorReply::Array(
        links
          .into_iter()
          .map(|l| VectorReply::Bulk(Some(l)))
          .collect(),
      ),
      None => VectorReply::Bulk(None),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVRANDMEMBER
  ///
  /// C# 侧输出为 TODO（恒 +OK）；此处返回实际取样元素（超集语义）。
  pub fn network_vrandmember(&self, args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.is_empty() || args.len() > 2 {
      return Self::abort_wrong_number_of_arguments("VRANDMEMBER");
    }
    let count = match args.get(1) {
      Some(raw) => {
        let Some(v) = strict_i32(raw) else {
          return VectorReply::Error(ERR_EXPECTED_INTEGER_COUNT.to_vec());
        };
        v
      }
      None => 1,
    };
    let Some(index) = self.read_index(args[0]) else {
      // 对齐 C# NOTFOUND：指定 count → 空数组；未指定 → null
      return if args.len() == 2 {
        VectorReply::Array(Vec::new())
      } else {
        VectorReply::Bulk(None)
      };
    };
    let samples = self
      .manager
      .service
      .sample(index.context, count.max(0) as usize);
    if count == 1 && args.len() == 1 {
      return match samples.into_iter().next() {
        Some(s) => VectorReply::Bulk(Some(s)),
        None => VectorReply::Bulk(None),
      };
    }
    VectorReply::Array(
      samples
        .into_iter()
        .map(|v| VectorReply::Bulk(Some(v)))
        .collect(),
    )
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVREM
  pub fn network_vrem(&self, args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() != 2 {
      return Self::abort_wrong_number_of_arguments("VREM");
    }
    let Some(index) = self.read_index(args[0]) else {
      return VectorReply::Integer(0);
    };
    let removed = self.manager.try_remove(&index.to_bytes(), args[1]);
    VectorReply::Integer(match removed {
      VectorManagerResult::OK => 1,
      _ => 0,
    })
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVSETATTR
  pub fn network_vsetattr(&self, args: &[&[u8]]) -> VectorReply {
    self.network_vsetattr_impl(args, false)
  }

  /// VSETATTR 内部实现（RESP3 以布尔应答；缺失元素 → 假 / 0，非错误）。
  pub fn network_vsetattr_impl(&self, args: &[&[u8]], resp3: bool) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() != 3 {
      return Self::abort_wrong_number_of_arguments("VSETATTR");
    }
    let found = self.read_index(args[0]).is_some_and(|index| {
      self
        .manager
        .try_set_attribute(&index.to_bytes(), args[1], args[2])
    });
    match (found, resp3) {
      (true, true) => VectorReply::Boolean(true),
      (false, true) => VectorReply::Boolean(false),
      (true, false) => VectorReply::Integer(1),
      (false, false) => VectorReply::Integer(0),
    }
  }
}

/// COUNT 范围错误。
impl RespServerSessionVectors {
  fn abort_count_range() -> VectorReply {
    VectorReply::Error(ERR_COUNT_RANGE.to_vec())
  }

  fn abort_filter_ef_range() -> VectorReply {
    VectorReply::Error(ERR_FILTER_EF_RANGE.to_vec())
  }
}

#[cfg(test)]
mod tests {
  use std::str::from_utf8;

  use super::{
    super::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_types::{VectorDistanceMetricType, VectorIdFormat, VectorQuantType},
    },
    *,
  };

  fn session() -> RespServerSessionVectors {
    let manager = Arc::new(VectorManager::new(VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    }));
    RespServerSessionVectors::new(manager)
  }

  fn s(bytes: &[u8]) -> &str {
    from_utf8(bytes).unwrap()
  }

  fn err_text(r: VectorReply) -> String {
    match r {
      VectorReply::Error(e) => s(&e).to_owned(),
      other => panic!("期望错误应答，实际 {other:?}"),
    }
  }

  #[test]
  fn vadd_parse_and_defaults() {
    let sess = session();

    // 参数不足
    let r = sess.network_vadd(&[b"k", b"FP32"]);
    assert!(err_text(r).contains("wrong number of arguments"));

    // VALUES 形式 + 默认 Q8/200/16/L2（成功应答为整数 1）
    let r = sess.network_vadd(&[b"k", b"VALUES", b"2", b"1.5", b"-2.5", b"elem1", b"CAS"]);
    assert_eq!(r, VectorReply::Integer(1));
    let index = sess.read_index(b"k").unwrap();
    assert_eq!(index.dimensions, 2);
    assert_eq!(index.quant_type, VectorQuantType::Q8);
    assert_eq!(index.num_links, 16);
    assert_eq!(index.build_exploration_factor, 200);
    assert_eq!(index.distance_metric, VectorDistanceMetricType::L2);
    assert!(sess.manager.is_member(&index.to_bytes(), b"elem1"));

    // 重复元素 → 整数 0（非错误，对齐 C# Duplicate 分支）
    let r = sess.network_vadd(&[b"k", b"VALUES", b"2", b"1.5", b"-2.5", b"elem1"]);
    assert_eq!(r, VectorReply::Integer(0));

    // FP32 形式 + 全选项（新键）
    let r = sess.network_vadd(&[
      b"k2",
      b"FP32",
      &f32_bytes(&[3.0, 4.0]),
      b"elem2",
      b"NOQUANT",
      b"EF",
      b"64",
      b"SETATTR",
      b"{\"a\":1}",
      b"M",
      b"8",
      b"XDISTANCE_METRIC",
      b"COSINE",
    ]);
    assert_eq!(r, VectorReply::Integer(1));
    let index = sess.read_index(b"k2").unwrap();
    assert_eq!(index.quant_type, VectorQuantType::NoQuant);
    assert_eq!(index.num_links, 8);
    assert_eq!(index.build_exploration_factor, 64);
    assert_eq!(index.distance_metric, VectorDistanceMetricType::Cosine);
    assert_eq!(
      sess.manager.service.get_attribute(index.context, b"elem2"),
      Some(b"{\"a\":1}".to_vec())
    );

    // REDUCE + FP32：降维集合
    let r = sess.network_vadd(&[
      b"kr",
      b"REDUCE",
      b"1",
      b"FP32",
      &f32_bytes(&[3.0, 4.0]),
      b"e0",
    ]);
    assert_eq!(r, VectorReply::Integer(1));
    let index = sess.read_index(b"kr").unwrap();
    assert_eq!(index.reduce_dims, 1);

    // REDUCE 超过向量维度
    let r = sess.network_vadd(&[
      b"kr2",
      b"REDUCE",
      b"5",
      b"FP32",
      &f32_bytes(&[1.0, 2.0]),
      b"e",
    ]);
    assert_eq!(
      err_text(r),
      "ERR REDUCE dimension must be <= vector dimensions"
    );

    // X 系量化 + REDUCE → quantization mismatch（对齐 C# BadParams 空文案回落）
    let r = sess.network_vadd(&[
      b"kq",
      b"REDUCE",
      b"1",
      b"FP32",
      &f32_bytes(&[1.0, 2.0]),
      b"e",
      b"XNOQUANT_U8",
    ]);
    assert_eq!(
      err_text(r),
      "ERR asked quantization mismatch with existing vector set"
    );

    // 重复选项报错（量化 / EF / M / SETATTR / CAS / XDISTANCE_METRIC）
    let v1 = f32_bytes(&[1.0]);
    let dup_sets: Vec<Vec<&[u8]>> = vec![
      vec![b"k", b"FP32", &v1, b"x", b"NOQUANT", b"Q8"],
      vec![b"k", b"FP32", &v1, b"x", b"EF", b"8", b"EF", b"9"],
      vec![b"k", b"FP32", &v1, b"x", b"M", b"8", b"M", b"9"],
      vec![b"k", b"FP32", &v1, b"x", b"SETATTR", b"a", b"SETATTR", b"b"],
      vec![b"k", b"FP32", &v1, b"x", b"CAS", b"CAS"],
      vec![
        b"k",
        b"FP32",
        &v1,
        b"x",
        b"XDISTANCE_METRIC",
        b"L2",
        b"XDISTANCE_METRIC",
        b"COSINE",
      ],
    ];
    for dup in dup_sets {
      let r = sess.network_vadd(&dup);
      assert!(matches!(r, VectorReply::Error(_)), "重复选项应报错: {r:?}");
    }
    assert_eq!(
      err_text(sess.network_vadd(&[b"k", b"FP32", &v1, b"x", b"NOQUANT", b"Q8"])),
      "Quantization specified multiple times"
    );

    // M 越界
    let r = sess.network_vadd(&[b"k", b"FP32", &f32_bytes(&[1.0]), b"x", b"M", b"2"]);
    assert_eq!(err_text(r), "ERR M must be an integer between 4 and 4096");

    // EF 越界 + 非法度量 + 未知选项 + 元素后 REDUCE
    assert_eq!(
      err_text(sess.network_vadd(&[b"k", b"FP32", &v1, b"x", b"EF", b"0"])),
      "ERR EF must be an integer between 1 and 1000000"
    );
    assert_eq!(
      err_text(sess.network_vadd(&[b"k", b"FP32", &v1, b"x", b"XDISTANCE_METRIC", b"DOT"])),
      "ERR invalid XDISTANCE_METRIC"
    );
    assert_eq!(
      err_text(sess.network_vadd(&[b"k", b"FP32", &v1, b"x", b"WHAT"])),
      "ERR invalid option after element"
    );
    assert_eq!(
      err_text(sess.network_vadd(&[b"k", b"FP32", &v1, b"x", b"REDUCE", b"2"])),
      "ERR invalid option after element"
    );

    // 向量格式非法 + 空键
    assert_eq!(
      err_text(sess.network_vadd(&[b"k", b"FP32", b"123", b"x"])),
      "ERR invalid vector specification"
    );
    assert_eq!(
      err_text(sess.network_vadd(&[b"", b"FP32", &v1, b"x"])),
      "ERR Vector Set key cannot be empty"
    );

    // 严格整数：前导零拒绝（C# allowLeadingZeros: false）
    let r = sess.network_vadd(&[b"k", b"VALUES", b"02", b"1.0", b"2.0", b"e"]);
    assert_eq!(err_text(r), "ERR invalid vector specification");
  }

  #[test]
  fn vsim_options_and_output() {
    let sess = session();

    sess
      .manager
      .try_add(
        b"vs",
        &seed_index(&sess, b"vs", 2),
        b"near",
        VectorValueType::FP32,
        &f32_bytes(&[1.0, 0.0]),
        b"{\"n\":1}",
        0,
        VectorQuantType::NoQuant,
        8,
        VectorDistanceMetricType::L2,
      )
      .unwrap();
    sess
      .manager
      .try_add(
        b"vs",
        &sess.manager.read_stored_index(b"vs").unwrap(),
        b"far",
        VectorValueType::FP32,
        &f32_bytes(&[9.0, 9.0]),
        b"{\"n\":2}",
        0,
        VectorQuantType::NoQuant,
        8,
        VectorDistanceMetricType::L2,
      )
      .unwrap();

    // RESP2 + WITHSCORES（id/score 交错扁平数组）
    let r = sess.network_vsim(&[
      b"vs",
      b"FP32",
      &f32_bytes(&[1.0, 0.0]),
      b"WITHSCORES",
      b"COUNT",
      b"2",
    ]);
    let mut encoded = Vec::new();
    r.encode_resp2(&mut encoded);
    let text = s(&encoded);
    assert!(text.starts_with("*4\r\n"), "扁平数组 id/score 成对: {text}");

    // RESP3 + WITHSCORES → map（id → distance）
    let r3 = sess.network_vsim_impl(
      &[
        b"vs",
        b"FP32",
        &f32_bytes(&[1.0, 0.0]),
        b"WITHSCORES",
        b"COUNT",
        b"2",
      ],
      true,
    );
    let mut encoded3 = Vec::new();
    r3.encode_resp3(&mut encoded3);
    let text3 = s(&encoded3);
    assert!(text3.starts_with("%2\r\n"), "RESP3 map 头: {text3}");

    // RESP3 无选项 → 普通数组
    let r3 = sess.network_vsim_impl(&[b"vs", b"ELE", b"near", b"COUNT", b"1"], true);
    let mut encoded3 = Vec::new();
    r3.encode_resp3(&mut encoded3);
    assert!(
      s(&encoded3).starts_with("*1\r\n"),
      "RESP3 普通数组: {}",
      s(&encoded3)
    );

    // FILTER 过滤 far
    let r = sess.network_vsim(&[
      b"vs",
      b"FP32",
      &f32_bytes(&[1.0, 0.0]),
      b"FILTER",
      b".n > 1",
      b"COUNT",
      b"2",
    ]);
    let mut encoded = Vec::new();
    r.encode_resp2(&mut encoded);
    let text = s(&encoded);
    assert!(text.contains("far") && !text.contains("near"));

    // ELE 形式
    let r = sess.network_vsim(&[b"vs", b"ELE", b"near", b"COUNT", b"1"]);
    let mut encoded = Vec::new();
    r.encode_resp2(&mut encoded);
    assert!(s(&encoded).contains("near"));

    // COUNT 重复
    let r = sess.network_vsim(&[
      b"vs",
      b"FP32",
      &f32_bytes(&[1.0, 0.0]),
      b"COUNT",
      b"1",
      b"COUNT",
      b"2",
    ]);
    assert_eq!(err_text(r), "COUNT specified multiple times");

    // 未知选项（对齐 C# "Unknown option"）
    assert_eq!(
      err_text(sess.network_vsim(&[b"vs", b"ELE", b"near", b"WHAT"])),
      "Unknown option"
    );

    // 非法 kind（对齐 C# "VSIM expected ELE, FP32, or VALUES"）
    assert_eq!(
      err_text(sess.network_vsim(&[b"vs", b"WHAT", b"x"])),
      "VSIM expected ELE, FP32, or VALUES"
    );

    // VALUES 分量非法（对齐 C# "VALUES value must be valid float"）
    assert_eq!(
      err_text(sess.network_vsim(&[b"vs", b"VALUES", b"2", b"1.0", b"abc"])),
      "VALUES value must be valid float"
    );

    // COUNT 选项缺值 → 参数数量错误（对齐 C# AbortWithWrongNumberOfArguments）
    assert!(
      err_text(sess.network_vsim(&[b"vs", b"ELE", b"near", b"COUNT"]))
        .contains("wrong number of arguments")
    );

    // 元素不在集合中 → "Element not in Vector Set"
    assert_eq!(
      err_text(sess.network_vsim(&[b"vs", b"ELE", b"ghost"])),
      "Element not in Vector Set"
    );

    // 缺键 → 空数组（对齐 C# NOTFOUND → TryWriteEmptyArray）
    assert_eq!(
      sess.network_vsim(&[b"nope", b"ELE", b"x"]),
      VectorReply::Array(Vec::new())
    );

    // EPSILON / FILTER-EF 解析与越界
    assert_eq!(
      err_text(sess.network_vsim(&[b"vs", b"ELE", b"near", b"EPSILON", b"-1"])),
      "EPSILON must be float > 0"
    );
    assert_eq!(
      err_text(sess.network_vsim(&[b"vs", b"ELE", b"near", b"FILTER-EF", b"3"])),
      "ERR FILTER-EF must be an integer between 4 and 256"
    );
  }

  #[test]
  fn auxiliary_commands() {
    let sess = session();
    sess
      .manager
      .try_add(
        b"aux",
        &seed_index(&sess, b"aux", 2),
        b"e1",
        VectorValueType::FP32,
        &f32_bytes(&[5.0, 6.0]),
        b"{\"tag\":\"x\"}",
        0,
        VectorQuantType::NoQuant,
        8,
        VectorDistanceMetricType::L2,
      )
      .unwrap();
    sess
      .manager
      .try_add(
        b"aux",
        &sess.manager.read_stored_index(b"aux").unwrap(),
        b"e2",
        VectorValueType::FP32,
        &f32_bytes(&[1.0, 1.0]),
        b"",
        0,
        VectorQuantType::NoQuant,
        8,
        VectorDistanceMetricType::L2,
      )
      .unwrap();

    assert_eq!(sess.network_vcard(&[b"aux"]), VectorReply::Integer(2));
    assert_eq!(sess.network_vdim(&[b"aux"]), VectorReply::Integer(2));
    assert_eq!(
      sess.network_vismember(&[b"aux", b"e1"]),
      VectorReply::Integer(1)
    );
    assert_eq!(
      sess.network_vismember(&[b"aux", b"zz"]),
      VectorReply::Integer(0)
    );
    assert_eq!(
      sess.network_vismember_impl(&[b"aux", b"e1"], true),
      VectorReply::Boolean(true)
    );

    // VEMB / VEMB RAW（NoQuant → fp32 通道）
    let emb = sess.network_vemb(&[b"aux", b"e1"]);
    let mut encoded = Vec::new();
    emb.encode_resp2(&mut encoded);
    let text = s(&encoded);
    assert!(text.contains('5'), "嵌入值包含 5: {text}");

    // VEMB RAW：NoQuant → [fp32, 原始 f32 字节, 范数]（原始字节非 UTF-8，按结构断言）
    let raw = sess.network_vemb(&[b"aux", b"e1", b"RAW"]);
    let VectorReply::Array(items) = raw else {
      panic!("RAW 应为数组: {raw:?}");
    };
    assert_eq!(items.len(), 3);
    assert_eq!(items[0], VectorReply::Simple(b"fp32".to_vec()));
    assert_eq!(items[1], VectorReply::Bulk(Some(f32_bytes(&[5.0, 6.0]))));

    // VEMB 非法选项
    assert_eq!(
      err_text(sess.network_vemb(&[b"aux", b"e1", b"BAD"])),
      "Unexpected option to VEMB"
    );
    // VEMB 缺失元素 → 空数组（对齐 C# NOTFOUND）
    assert_eq!(
      sess.network_vemb(&[b"aux", b"ghost"]),
      VectorReply::Array(Vec::new())
    );

    // VGETATTR / VSETATTR（成功 1 / 缺失 0，非错误）
    assert_eq!(
      sess.network_vgetattr(&[b"aux", b"e1"]),
      VectorReply::Bulk(Some(b"{\"tag\":\"x\"}".to_vec()))
    );
    assert_eq!(
      sess.network_vsetattr(&[b"aux", b"e1", b"{}"]),
      VectorReply::Integer(1)
    );
    assert_eq!(
      sess.network_vsetattr(&[b"aux", b"ghost", b"{}"]),
      VectorReply::Integer(0)
    );
    assert_eq!(
      sess.network_vgetattr(&[b"aux", b"e1"]),
      VectorReply::Bulk(Some(b"{}".to_vec()))
    );

    // VLINKS（缺失元素 → null）
    assert!(matches!(
      sess.network_vlinks(&[b"aux", b"e1"]),
      VectorReply::Array(_)
    ));
    assert_eq!(
      sess.network_vlinks(&[b"aux", b"ghost"]),
      VectorReply::Bulk(None)
    );
    assert_eq!(
      err_text(sess.network_vlinks(&[b"aux", b"e1", b"BAD"])),
      "ERR Unexpected option"
    );

    // VRANDMEMBER
    assert!(matches!(
      sess.network_vrandmember(&[b"aux", b"2"]),
      VectorReply::Array(_)
    ));
    assert_eq!(
      err_text(sess.network_vrandmember(&[b"aux", b"x"])),
      "ERR expected integer count"
    );
    assert_eq!(
      sess.network_vrandmember(&[b"ghost", b"2"]),
      VectorReply::Array(Vec::new())
    );
    assert_eq!(
      sess.network_vrandmember(&[b"ghost"]),
      VectorReply::Bulk(None)
    );

    // VINFO（对齐 C# 布局：14 项 + 小写枚举名）
    let info = sess.network_vinfo(&[b"aux"]);
    let mut encoded = Vec::new();
    info.encode_resp2(&mut encoded);
    let text = s(&encoded);
    assert!(text.starts_with("*14\r\n"), "VINFO 14 项: {text}");
    assert!(text.contains("input-vector-dimensions"));
    assert!(text.contains("reduced-dimensions"));
    assert!(text.contains("f32"));
    assert!(text.contains("l2"));
    assert_eq!(sess.network_vinfo(&[b"ghost"]), VectorReply::NullArray);

    // VREM
    assert_eq!(sess.network_vrem(&[b"aux", b"e2"]), VectorReply::Integer(1));
    assert_eq!(sess.network_vrem(&[b"aux", b"e2"]), VectorReply::Integer(0));
    assert_eq!(sess.network_vcard(&[b"aux"]), VectorReply::Integer(1));

    // VDIM 缺键 → "ERR Key not found"
    assert_eq!(
      err_text(sess.network_vdim(&[b"ghost"])),
      "ERR Key not found"
    );
  }

  #[test]
  fn disabled_and_reply_encoding() {
    let manager = Arc::new(VectorManager::new(VectorManagerOptions {
      is_enabled: false,
      ..Default::default()
    }));
    let sess = RespServerSessionVectors::new(manager);

    for r in [
      sess.network_vadd(&[b"k", b"FP32", &[0; 4], b"e"]),
      sess.network_vsim(&[b"k", b"ELE", b"e"]),
      sess.network_vcard(&[b"k"]),
      sess.network_vrem(&[b"k", b"e"]),
    ] {
      assert!(matches!(r, VectorReply::Error(_)), "未启用应拒绝");
    }

    // WRONGTYPE 文案（无句点，对齐 AbortVectorSetWrongType）
    assert!(sess.abort_vector_set_wrong_type(b"somekey").is_none());

    // RESP2/RESP3 编码差异（含 map / null array）
    let reply = VectorReply::Array(vec![
      VectorReply::Bulk(Some(b"a".to_vec())),
      VectorReply::Double(2.0),
      VectorReply::Boolean(true),
    ]);
    let mut r2 = Vec::new();
    reply.encode_resp2(&mut r2);
    assert_eq!(s(&r2), "*3\r\n$1\r\na\r\n$1\r\n2\r\n$1\r\n1\r\n");
    let mut r3 = Vec::new();
    reply.encode_resp3(&mut r3);
    assert_eq!(s(&r3), "*3\r\n$1\r\na\r\n,2\r\n#t\r\n");

    let map = VectorReply::Map(vec![(
      VectorReply::Bulk(Some(b"id".to_vec())),
      VectorReply::Double(1.5),
    )]);
    let mut m2 = Vec::new();
    map.encode_resp2(&mut m2);
    assert_eq!(s(&m2), "*2\r\n$2\r\nid\r\n$3\r\n1.5\r\n");
    let mut m3 = Vec::new();
    map.encode_resp3(&mut m3);
    assert_eq!(s(&m3), "%1\r\n$2\r\nid\r\n,1.5\r\n");

    let mut na = Vec::new();
    VectorReply::NullArray.encode_resp2(&mut na);
    assert_eq!(s(&na), "*-1\r\n");
  }

  #[test]
  fn result_writers_honor_bitmap_and_count() {
    let ids = vec![b"a".as_slice(), b"b".as_slice(), b"c".as_slice()];
    let distances = [0.1f32, 0.2, 0.3];
    // 位图：a、c 通过
    let bitmap = [0b101u8];

    // RESP2：输出上限 = min(count, popcount)
    let out =
      RespServerSessionVectors::write_resp2_result(10, &ids, &distances, &bitmap, None, false);
    let mut encoded = Vec::new();
    out.encode_resp2(&mut encoded);
    assert_eq!(s(&encoded), "*2\r\n$1\r\na\r\n$1\r\nc\r\n");

    // RESP3 无 score/attr → 普通数组
    let out =
      RespServerSessionVectors::write_resp3_result(10, &ids, &distances, &bitmap, None, false);
    let mut encoded = Vec::new();
    out.encode_resp3(&mut encoded);
    assert_eq!(s(&encoded), "*2\r\n$1\r\na\r\n$1\r\nc\r\n");

    // RESP3 + WITHSCORES + WITHATTRIBS → map，值为 [score, attr] 数组
    let attrs = vec![b"x".as_slice(), b"".as_slice(), b"z".as_slice()];
    let out = RespServerSessionVectors::write_resp3_result(
      10,
      &ids,
      &distances,
      &bitmap,
      Some(&attrs),
      true,
    );
    let mut encoded = Vec::new();
    out.encode_resp3(&mut encoded);
    let text = s(&encoded);
    // b 被位图剔除；a → [score, attr] 数组；c 的属性 z 原样写出
    // （f32 距离升 f64 后为最短往返表示 0.10000000149011612，对齐 C# double.TryFormat）
    assert!(text.starts_with("%2\r\n"), "{text}");
    assert!(
      text.contains("$1\r\na\r\n*2\r\n,0.10000000149011612\r\n$1\r\nx\r\n"),
      "{text}"
    );
    assert!(
      text.contains("$1\r\nc\r\n*2\r\n,0.30000001192092896\r\n$1\r\nz\r\n"),
      "{text}"
    );

    // 空属性在 RESP3 下写 NULL
    let attrs_one = vec![b"".as_slice()];
    let out = RespServerSessionVectors::write_resp3_result(
      10,
      &[b"e".as_slice()],
      &[0.5],
      &[],
      Some(&attrs_one),
      true,
    );
    let mut encoded = Vec::new();
    out.encode_resp3(&mut encoded);
    assert_eq!(s(&encoded), "%1\r\n$1\r\ne\r\n*2\r\n,0.5\r\n$-1\r\n");

    // count 截断
    let out = RespServerSessionVectors::write_resp2_result(1, &ids, &distances, &[], None, false);
    let mut encoded = Vec::new();
    out.encode_resp2(&mut encoded);
    assert_eq!(s(&encoded), "*1\r\n$1\r\na\r\n");
  }

  fn f32_bytes(vals: &[f32]) -> Vec<u8> {
    vals.iter().flat_map(|v| v.to_le_bytes()).collect()
  }

  fn seed_index(sess: &RespServerSessionVectors, key: &[u8], dims: u32) -> [u8; 56] {
    // 直接登记 NoQuant 索引（绕过 VADD 的 Q8 默认，便于距离断言）
    let context = sess.manager.next_vector_set_context(0).unwrap();
    // 原生索引同步建立（C# 侧由 ReadOrCreateVectorIndex 保证）
    sess.manager.service.create_index(
      context,
      dims,
      0,
      VectorQuantType::NoQuant,
      64,
      8,
      VectorDistanceMetricType::L2,
    );
    let index = Index {
      context,
      index_ptr: 1,
      dimensions: dims,
      reduce_dims: 0,
      num_links: 8,
      build_exploration_factor: 64,
      quant_type: VectorQuantType::NoQuant,
      distance_metric: VectorDistanceMetricType::L2,
      ..Index::default()
    };
    let bytes = index.to_bytes();
    sess.manager.write_stored_index(key, &bytes);
    bytes
  }

  // 引用以保持与 C# 出参形态的对应关系可见
  #[test]
  fn id_format_is_length_prefixed() {
    assert_eq!(VectorIdFormat::I32LengthPrefixed as i32, 1);
  }
}
