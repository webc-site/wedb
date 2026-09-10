//! Vector Set 命令的网络层（对标 libs/server/Resp/Vector/RespServerSessionVectors.cs）
//!
//! C# 为 RespServerSession 的 partial，直接消费 parseState / storageApi /
//! networkSender；Rust 侧 RespServerSession 属并行域，此处以
//! `&[&[u8]]` 参数面 + [`VectorReply`] 应答面承接同一命令语义
//! （选项解析、重复项报错、默认值、RESP2/RESP3 应答布局）。

use std::{str, sync::Arc};

use super::{
  disk_ann_service::DiskAnnInsertResult,
  vector_manager::{
    MAX_EXPLORATION_FACTOR, MAX_FILTERING_SCALE_FACTOR, MAX_RETRIEVE_COUNT, MAX_VECTOR_DIMENSIONS,
    VectorManager, VectorManagerResult,
  },
  vector_manager__index::Index,
  vector_manager__locking::CreateIndexParams,
  vector_types::{VectorDistanceMetricType, VectorIdFormat, VectorQuantType, VectorValueType},
};
use crate::storage::session::common::array_key_iteration_functions::cluster_slot;

/// VADD 的 M 取值边界（对齐 C# MinM/MaxM）。
const MIN_M: i64 = 4;
const MAX_M: i64 = 4_096;

/// 命令应答（RESP 数据模型）。
#[derive(Debug, Clone, PartialEq)]
pub enum VectorReply {
  /// 简单字符串（+OK）。
  Simple(Vec<u8>),
  /// 错误（-...，含完整前缀）。
  Error(Vec<u8>),
  /// 整数。
  Integer(i64),
  /// 批量字符串（None = NULL）。
  Bulk(Option<Vec<u8>>),
  /// 数组。
  Array(Vec<VectorReply>),
  /// RESP3 双精度浮点。
  Double(f64),
  /// 布尔（RESP3）。
  Boolean(bool),
}

impl VectorReply {
  /// 编码为 RESP2 字节（Double 退化为 bulk 字符串）。
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
      VectorReply::Double(d) => {
        let text = fmt_double(*d);
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

  /// 编码为 RESP3 字节（Double 为 `,`、Boolean 为 `#`）。
  pub fn encode_resp3(&self, out: &mut Vec<u8>) {
    match self {
      VectorReply::Double(d) => {
        out.push(b',');
        out.extend_from_slice(fmt_double(*d).as_bytes());
        out.extend_from_slice(b"\r\n");
      }
      VectorReply::Boolean(b) => {
        out.extend_from_slice(if *b { b"#t\r\n" } else { b"#f\r\n" });
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

/// f64 → RESP 分数字符串（整数省小数，对齐 Redis 输出习惯）。
fn fmt_double(d: f64) -> String {
  if d.fract() == 0.0 && d.abs() < 1e15 {
    format!("{}", d as i64)
  } else {
    format!("{d}")
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
      Some(VectorReply::Error(
        b"WRONGTYPE Operation against a key holding the wrong kind of value.".to_vec(),
      ))
    } else {
      None
    }
  }

  /// Vector Set 预览未启用的统一拒绝。
  fn abort_disabled(&self) -> VectorReply {
    VectorReply::Error(b"ERR Vector Set (preview) commands are not enabled".to_vec())
  }

  /// 参数数量错误的统一文案。
  fn abort_wrong_number_of_arguments(cmd: &str) -> VectorReply {
    VectorReply::Error(format!("ERR wrong number of arguments for '{cmd}' command").into_bytes())
  }

  // ======================== VADD ========================

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVADD
  ///
  /// `VADD key [REDUCE dim] (FP32 | XU8 | XI8 | VALUES num) vector element
  ///   [CAS] [NOQUANT | Q8 | BIN | XNOQUANT_U8 | XPREQ8 | XNOQUANT_I8 | XBIN_I8 | XBIN_U8]
  ///   [EF build-exploration-factor] [SETATTR attributes] [M numlinks]
  ///   [XDISTANCE_METRIC L2 | COSINE | IP | XCOSINE_NORMALIZED]`
  pub fn network_vadd(&self, args: &[&[u8]]) -> VectorReply {
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
      let Some(v) = parse_int(args.get(cur_ix)) else {
        return VectorReply::Error(b"REDUCE dimension must be > 0".to_vec());
      };
      if v <= 0 {
        return VectorReply::Error(b"REDUCE dimension must be > 0".to_vec());
      }
      reduce_dims = v as u32;
      cur_ix += 1;
    }

    // 向量格式分派：FP32 / VALUES num / XU8|XB8 / XI8
    let Some(&kind) = args.get(cur_ix) else {
      return Self::abort_wrong_number_of_arguments("VADD");
    };
    let value_type;
    let values: Vec<u8>;
    if eq_ignore_case(kind, b"FP32") {
      cur_ix += 1;
      let Some(as_bytes) = args.get(cur_ix) else {
        return Self::abort_wrong_number_of_arguments("VADD");
      };
      if as_bytes.len() % 4 != 0 {
        return VectorReply::Error(b"ERR invalid vector specification".to_vec());
      }
      if as_bytes.len() / 4 > MAX_VECTOR_DIMENSIONS as usize {
        return self.abort_too_many_dimensions();
      }
      value_type = VectorValueType::FP32;
      values = as_bytes.to_vec();
      cur_ix += 1;
    } else if eq_ignore_case(kind, b"VALUES") {
      cur_ix += 1;
      let Some(n) = parse_int(args.get(cur_ix)) else {
        return VectorReply::Error(b"ERR invalid vector specification".to_vec());
      };
      if n <= 0 {
        return VectorReply::Error(b"ERR invalid vector specification".to_vec());
      }
      cur_ix += 1;
      if n as u32 > MAX_VECTOR_DIMENSIONS {
        return self.abort_too_many_dimensions();
      }
      if cur_ix + n as usize > args.len() {
        return Self::abort_wrong_number_of_arguments("VADD");
      }
      value_type = VectorValueType::FP32;
      let mut floats = Vec::with_capacity(n as usize);
      for _ in 0..n {
        match parse_float(args.get(cur_ix)) {
          // Redis 语义：VALUES 分量为单精度浮点
          Some(f) => floats.extend_from_slice(&(f as f32).to_le_bytes()),
          None => return VectorReply::Error(b"ERR invalid vector specification".to_vec()),
        }
        cur_ix += 1;
      }
      values = floats;
    } else if eq_ignore_case(kind, b"XU8") || eq_ignore_case(kind, b"XB8") {
      // XB8 为向后兼容别名，推荐 XU8
      cur_ix += 1;
      let Some(as_bytes) = args.get(cur_ix) else {
        return Self::abort_wrong_number_of_arguments("VADD");
      };
      if as_bytes.len() > MAX_VECTOR_DIMENSIONS as usize {
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
      if as_bytes.len() > MAX_VECTOR_DIMENSIONS as usize {
        return self.abort_too_many_dimensions();
      }
      value_type = VectorValueType::XI8;
      values = as_bytes.to_vec();
      cur_ix += 1;
    } else {
      return VectorReply::Error(b"ERR invalid vector specification".to_vec());
    }

    // 元素键
    let Some(&element) = args.get(cur_ix) else {
      return Self::abort_wrong_number_of_arguments("VADD");
    };
    cur_ix += 1;

    // 选项循环
    let mut cas_seen = false;
    let mut quant: Option<VectorQuantType> = None;
    let mut build_ef: Option<i64> = None;
    let mut attributes: Option<&[u8]> = None;
    let mut num_links: Option<i64> = None;
    let mut distance_metric: Option<VectorDistanceMetricType> = None;

    while cur_ix < args.len() {
      let opt = args[cur_ix];
      if eq_ignore_case(opt, b"CAS") {
        if cas_seen {
          return VectorReply::Error(b"CAS specified multiple times".to_vec());
        }
        // CAS 仅识别不处理
        cas_seen = true;
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"NOQUANT") {
        quant = Some(VectorQuantType::NoQuant);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"Q8") {
        quant = Some(VectorQuantType::Q8);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"BIN") {
        quant = Some(VectorQuantType::Bin);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"XNOQUANT_U8") || eq_ignore_case(opt, b"XPREQ8") {
        // XPREQ8 为向后兼容别名，推荐 XNOQUANT_U8
        quant = Some(VectorQuantType::XNoQuant_U8);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"XNOQUANT_I8") {
        quant = Some(VectorQuantType::XNoQuant_I8);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"XBIN_I8") {
        quant = Some(VectorQuantType::XBin_I8);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"XBIN_U8") {
        quant = Some(VectorQuantType::XBin_U8);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"EF") {
        if build_ef.is_some() {
          return VectorReply::Error(b"EF specified multiple times".to_vec());
        }
        cur_ix += 1;
        let Some(v) = parse_int(args.get(cur_ix)) else {
          return Self::abort_ef_range();
        };
        if v <= 0 || v as usize > MAX_EXPLORATION_FACTOR {
          return Self::abort_ef_range();
        }
        build_ef = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"SETATTR") {
        if attributes.is_some() {
          return VectorReply::Error(b"SETATTR specified multiple times".to_vec());
        }
        cur_ix += 1;
        let Some(attr) = args.get(cur_ix) else {
          return Self::abort_wrong_number_of_arguments("VADD");
        };
        attributes = Some(attr);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"M") {
        if num_links.is_some() {
          return VectorReply::Error(b"M specified multiple times".to_vec());
        }
        cur_ix += 1;
        let Some(v) = parse_int(args.get(cur_ix)) else {
          return Self::abort_m_range();
        };
        if !(MIN_M..=MAX_M).contains(&v) {
          return Self::abort_m_range();
        }
        num_links = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"XDISTANCE_METRIC") {
        if distance_metric.is_some() {
          return VectorReply::Error(b"DISTANCE METRIC specified multiple times".to_vec());
        }
        cur_ix += 1;
        let Some(metric) = args.get(cur_ix) else {
          return Self::abort_wrong_number_of_arguments("VADD");
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
          return VectorReply::Error(b"ERR invalid distance metric".to_vec());
        });
        cur_ix += 1;
      } else {
        return VectorReply::Error(
          format!("ERR Unknown argument '{}'", String::from_utf8_lossy(opt)).into_bytes(),
        );
      }
    }

    // 默认值（对齐 C#：Q8 / 200 / 16 / L2）
    let quant = quant.unwrap_or(VectorQuantType::Q8);
    let build_ef = build_ef.unwrap_or(200) as u32;
    let num_links = num_links.unwrap_or(16) as u32;
    let distance_metric = distance_metric.unwrap_or(VectorDistanceMetricType::L2);

    // 读或创建索引记录（缺失时按选项建桩）
    let dims = match value_type {
      VectorValueType::FP32 => (values.len() / 4) as u32,
      VectorValueType::XU8 | VectorValueType::XI8 => values.len() as u32,
      VectorValueType::Invalid => {
        return VectorReply::Error(b"ERR invalid vector specification".to_vec());
      }
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
      Err(_) => return VectorReply::Error(b"ERR Maximum Vector Set allocations exceeded".to_vec()),
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
        // 首次插入成功后补齐元素登记（DIM/CARD 查询路径依赖）
        if self.manager.service.card(index.context) == 1 {
          let _ = DiskAnnInsertResult::True;
        }
        VectorReply::Simple(b"OK".to_vec())
      }
      Ok(VectorManagerResult::Duplicate) => {
        VectorReply::Error(VectorManager::error_msg(VectorManagerResult::Duplicate).to_vec())
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
    VectorReply::Error(
      format!("ERR EF must be an integer between 1 and {MAX_EXPLORATION_FACTOR}").into_bytes(),
    )
  }

  /// M 范围错误。
  fn abort_m_range() -> VectorReply {
    VectorReply::Error(format!("ERR M must be an integer between {MIN_M} and {MAX_M}").into_bytes())
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
  fn network_vsim_impl(&self, args: &[&[u8]], resp3: bool) -> VectorReply {
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
      element = Some(args.get(cur_ix).copied().unwrap_or(b""));
      cur_ix += 1;
    } else if eq_ignore_case(kind, b"FP32") {
      let Some(as_bytes) = args.get(cur_ix) else {
        return Self::abort_wrong_number_of_arguments("VSIM");
      };
      if as_bytes.len() % 4 != 0 {
        return VectorReply::Error(b"FP32 values must be multiple of 4-bytes in size".to_vec());
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
      let Some(n) = parse_int(args.get(cur_ix)) else {
        return VectorReply::Error(b"VALUES count must > 0".to_vec());
      };
      if n <= 0 {
        return VectorReply::Error(b"VALUES count must > 0".to_vec());
      }
      if n as u32 > MAX_VECTOR_DIMENSIONS {
        return self.abort_too_many_dimensions();
      }
      cur_ix += 1;
      if cur_ix + n as usize > args.len() {
        return Self::abort_wrong_number_of_arguments("VSIM");
      }
      value_type = VectorValueType::FP32;
      for _ in 0..n {
        match parse_float(args.get(cur_ix)) {
          Some(f) => values.extend_from_slice(&f.to_le_bytes()),
          None => return VectorReply::Error(b"ERR invalid vector specification".to_vec()),
        }
        cur_ix += 1;
      }
    } else {
      return Self::abort_wrong_number_of_arguments("VSIM");
    }

    // 选项
    let mut with_scores = false;
    let mut with_attribs = false;
    let mut count: Option<i64> = None;
    let mut epsilon: Option<f64> = None;
    let mut ef: Option<i64> = None;
    let mut filter: Option<&[u8]> = None;
    let mut filter_ef: Option<i64> = None;
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
        let Some(v) = parse_int(args.get(cur_ix)) else {
          return Self::abort_count_range();
        };
        if !(0..=MAX_RETRIEVE_COUNT as i64).contains(&v) {
          return Self::abort_count_range();
        }
        count = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"EPSILON") {
        if epsilon.is_some() {
          return VectorReply::Error(b"EPSILON specified multiple times".to_vec());
        }
        cur_ix += 1;
        let Some(v) = parse_float(args.get(cur_ix)) else {
          return VectorReply::Error(b"EPSILON must be float > 0".to_vec());
        };
        if v <= 0.0 {
          return VectorReply::Error(b"EPSILON must be float > 0".to_vec());
        }
        epsilon = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"EF") {
        if ef.is_some() {
          return VectorReply::Error(b"EF specified multiple times".to_vec());
        }
        cur_ix += 1;
        let Some(v) = parse_int(args.get(cur_ix)) else {
          return Self::abort_ef_range();
        };
        if v <= 0 || v as usize > MAX_EXPLORATION_FACTOR {
          return Self::abort_ef_range();
        }
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
        let Some(v) = parse_int(args.get(cur_ix)) else {
          return Self::abort_filter_ef_range();
        };
        if !(4..=MAX_FILTERING_SCALE_FACTOR as i64).contains(&v) {
          return Self::abort_filter_ef_range();
        }
        filter_ef = Some(v);
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"TRUTH") {
        if truth {
          return VectorReply::Error(b"TRUTH specified multiple times".to_vec());
        }
        truth = true;
        cur_ix += 1;
      } else if eq_ignore_case(opt, b"NOTHREAD") {
        if no_thread {
          return VectorReply::Error(b"NOTHREAD specified multiple times".to_vec());
        }
        no_thread = true;
        cur_ix += 1;
      } else {
        return VectorReply::Error(
          format!("ERR Unknown argument '{}'", String::from_utf8_lossy(opt)).into_bytes(),
        );
      }
    }
    let _ = (truth, no_thread, epsilon, filter_ef);

    let count = count.unwrap_or(64) as usize;

    // 键存在性
    let Some(_index) = self.read_index(key) else {
      return VectorReply::Error(b"ERR no such key".to_vec());
    };

    let result = match element {
      Some(elem) => self.manager.element_similarity(
        &self.manager.read_stored_index(key).unwrap_or([0; 56]),
        elem,
        count,
        ef.unwrap_or(0).max(0) as usize,
        filter.unwrap_or(b""),
        with_attribs,
      ),
      None => self.manager.value_similarity(
        &self.manager.read_stored_index(key).unwrap_or([0; 56]),
        value_type,
        &values,
        count,
        ef.unwrap_or(0).max(0) as usize,
        filter.unwrap_or(b""),
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
        output.found,
        &ids,
        &output.output_distances,
        &output.filter_bitmap,
        with_attribs.then_some(&attrs),
        with_scores,
        output.id_format,
      )
    } else {
      Self::write_resp2_result(
        output.found,
        &ids,
        &output.output_distances,
        &output.filter_bitmap,
        with_attribs.then_some(&attrs),
        with_scores,
        output.id_format,
      )
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:WriteRESP3Result
  ///
  /// RESP3：每命中项为 [id, score]（按需附属性），过滤未过项剔除。
  #[allow(clippy::too_many_arguments)]
  pub fn write_resp3_result(
    count: usize,
    ids: &[&[u8]],
    distances: &[f32],
    filter_bitmap: &[u8],
    attributes: Option<&Vec<&[u8]>>,
    with_scores: bool,
    _id_format: VectorIdFormat,
  ) -> VectorReply {
    let mut items = Vec::new();
    for i in 0..count {
      // 过滤位图：非空时仅输出通过项
      if !filter_bitmap.is_empty() && i < filter_bitmap.len() * 8 {
        let bit = (filter_bitmap[i >> 3] >> (i & 7)) & 1;
        if bit == 0 {
          continue;
        }
      }
      let Some(id) = ids.get(i) else { break };
      let mut entry = vec![VectorReply::Bulk(Some(id.to_vec()))];
      if with_scores {
        entry.push(VectorReply::Double(
          distances.get(i).copied().unwrap_or(0.0) as f64,
        ));
      }
      if let Some(attrs) = attributes
        && let Some(attr) = attrs.get(i)
      {
        entry.push(VectorReply::Bulk(Some(attr.to_vec())));
      }
      items.push(VectorReply::Array(entry));
    }
    VectorReply::Array(items)
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:WriteRESP2Result
  ///
  /// RESP2：扁平数组（WITHSCORES 时 id/score 成对；WITHATTRIBS 附加属性）。
  #[allow(clippy::too_many_arguments)]
  pub fn write_resp2_result(
    count: usize,
    ids: &[&[u8]],
    distances: &[f32],
    filter_bitmap: &[u8],
    attributes: Option<&Vec<&[u8]>>,
    with_scores: bool,
    _id_format: VectorIdFormat,
  ) -> VectorReply {
    let mut items = Vec::new();
    for i in 0..count {
      if !filter_bitmap.is_empty() && i < filter_bitmap.len() * 8 {
        let bit = (filter_bitmap[i >> 3] >> (i & 7)) & 1;
        if bit == 0 {
          continue;
        }
      }
      let Some(id) = ids.get(i) else { break };
      items.push(VectorReply::Bulk(Some(id.to_vec())));
      if with_scores {
        items.push(VectorReply::Double(
          distances.get(i).copied().unwrap_or(0.0) as f64,
        ));
      }
      if let Some(attrs) = attributes
        && let Some(attr) = attrs.get(i)
      {
        items.push(VectorReply::Bulk(Some(attr.to_vec())));
      }
    }
    VectorReply::Array(items)
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVEMB
  ///
  /// `VEMB key element` → 嵌入向量（元素以数组形式回出）。
  pub fn network_vemb(&self, args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() != 2 {
      return Self::abort_wrong_number_of_arguments("VEMB");
    }
    let Some(embedding) = self.manager.try_get_embedding(
      &self.manager.read_stored_index(args[0]).unwrap_or([0; 56]),
      args[1],
    ) else {
      return VectorReply::Error(b"ERR element not found in vector set".to_vec());
    };
    VectorReply::Array(
      embedding
        .into_iter()
        .map(|v| VectorReply::Double(f64::from(v)))
        .collect(),
    )
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
      return VectorReply::Error(b"ERR no such key".to_vec());
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
      return VectorReply::Bulk(None);
    };
    match self.manager.service.get_attribute(index.context, args[1]) {
      Some(attr) => VectorReply::Bulk(Some(attr)),
      None => VectorReply::Bulk(None),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVINFO
  ///
  /// `VINFO key` → 集合元信息（维度 / 量化 / 度量 / M / EF / CARD）。
  pub fn network_vinfo(&self, args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() != 1 {
      return Self::abort_wrong_number_of_arguments("VINFO");
    }
    let Some(index) = self.read_index(args[0]) else {
      return VectorReply::Error(b"ERR no such key".to_vec());
    };
    let quant = match index.quant_type {
      VectorQuantType::NoQuant => "NOQUANT",
      VectorQuantType::Q8 => "Q8",
      VectorQuantType::Bin => "BIN",
      VectorQuantType::XNoQuant_U8 => "XNOQUANT_U8",
      VectorQuantType::XNoQuant_I8 => "XNOQUANT_I8",
      VectorQuantType::XBin_I8 => "XBIN_I8",
      VectorQuantType::XBin_U8 => "XBIN_U8",
      VectorQuantType::Invalid => "INVALID",
    };
    let metric = match index.distance_metric {
      VectorDistanceMetricType::L2 => "L2",
      VectorDistanceMetricType::Cosine => "COSINE",
      VectorDistanceMetricType::InnerProduct => "IP",
      VectorDistanceMetricType::XCosineNormalized => "XCOSINE_NORMALIZED",
    };
    VectorReply::Array(vec![
      VectorReply::Bulk(Some(b"dimensions".to_vec())),
      VectorReply::Integer(i64::from(index.dimensions)),
      VectorReply::Bulk(Some(b"quant-type".to_vec())),
      VectorReply::Bulk(Some(quant.as_bytes().to_vec())),
      VectorReply::Bulk(Some(b"distance-metric".to_vec())),
      VectorReply::Bulk(Some(metric.as_bytes().to_vec())),
      VectorReply::Bulk(Some(b"num-links".to_vec())),
      VectorReply::Integer(i64::from(index.num_links)),
      VectorReply::Bulk(Some(b"build-exploration-factor".to_vec())),
      VectorReply::Integer(i64::from(index.build_exploration_factor)),
      VectorReply::Bulk(Some(b"cardinality".to_vec())),
      VectorReply::Integer(self.manager.service.card(index.context) as i64),
    ])
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVISMEMBER
  pub fn network_vismember(&self, args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() != 2 {
      return Self::abort_wrong_number_of_arguments("VISMEMBER");
    }
    let Some(index) = self.read_index(args[0]) else {
      return VectorReply::Integer(0);
    };
    VectorReply::Integer(i64::from(
      self.manager.is_member(&index.to_bytes(), args[1]),
    ))
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVLINKS
  pub fn network_vlinks(&self, args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() < 2 {
      return Self::abort_wrong_number_of_arguments("VLINKS");
    }
    let Some(index) = self.read_index(args[0]) else {
      return VectorReply::Error(b"ERR no such key".to_vec());
    };
    match self.manager.service.links_of(index.context, args[1]) {
      Some(links) => VectorReply::Array(
        links
          .into_iter()
          .map(|l| VectorReply::Bulk(Some(l)))
          .collect(),
      ),
      None => VectorReply::Error(b"ERR element not found in vector set".to_vec()),
    }
  }

  /// libs/server/Resp/Vector/RespServerSessionVectors.cs:NetworkVRANDMEMBER
  pub fn network_vrandmember(&self, args: &[&[u8]]) -> VectorReply {
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.is_empty() {
      return Self::abort_wrong_number_of_arguments("VRANDMEMBER");
    }
    let Some(index) = self.read_index(args[0]) else {
      return VectorReply::Bulk(None);
    };
    let count = args.get(1).and_then(|a| parse_int(Some(a))).unwrap_or(1);
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
    if !self.manager.is_enabled {
      return self.abort_disabled();
    }
    if args.len() != 3 {
      return Self::abort_wrong_number_of_arguments("VSETATTR");
    }
    let Some(index) = self.read_index(args[0]) else {
      return VectorReply::Error(b"ERR no such key".to_vec());
    };
    if self
      .manager
      .try_set_attribute(&index.to_bytes(), args[1], args[2])
    {
      VectorReply::Simple(b"OK".to_vec())
    } else {
      VectorReply::Error(b"ERR element not found in vector set".to_vec())
    }
  }
}

/// 可选参数解析为 i64。
fn parse_int(arg: Option<&&[u8]>) -> Option<i64> {
  let bytes = *arg?;
  str::from_utf8(bytes).ok()?.parse::<i64>().ok()
}

/// 可选参数解析为 f64。
fn parse_float(arg: Option<&&[u8]>) -> Option<f64> {
  let bytes = *arg?;
  str::from_utf8(bytes)
    .ok()?
    .parse::<f64>()
    .ok()
    .filter(|v| v.is_finite())
}

/// COUNT 范围错误。
impl RespServerSessionVectors {
  fn abort_count_range() -> VectorReply {
    VectorReply::Error(
      format!("ERR COUNT must be an integer between 0 and {MAX_RETRIEVE_COUNT}").into_bytes(),
    )
  }

  fn abort_filter_ef_range() -> VectorReply {
    VectorReply::Error(
      format!("ERR FILTER-EF must be an integer between 4 and {MAX_FILTERING_SCALE_FACTOR}")
        .into_bytes(),
    )
  }
}

#[cfg(test)]
mod tests {
  use super::{
    super::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_types::{VectorDistanceMetricType, VectorQuantType},
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
    str::from_utf8(bytes).unwrap()
  }

  #[test]
  fn vadd_parse_and_defaults() {
    let sess = session();

    // 参数不足
    let r = sess.network_vadd(&[b"k", b"FP32"]);
    assert!(
      s(&match r {
        VectorReply::Error(e) => e,
        _ => panic!(),
      })
      .contains("wrong number of arguments")
    );

    // VALUES 形式 + 默认 Q8/200/16/L2
    let r = sess.network_vadd(&[b"k", b"VALUES", b"2", b"1.5", b"-2.5", b"elem1", b"CAS"]);
    assert_eq!(r, VectorReply::Simple(b"OK".to_vec()));
    let index = sess.read_index(b"k").unwrap();
    assert_eq!(index.dimensions, 2);
    assert_eq!(index.quant_type, VectorQuantType::Q8);
    assert_eq!(index.num_links, 16);
    assert_eq!(index.build_exploration_factor, 200);
    assert_eq!(index.distance_metric, VectorDistanceMetricType::L2);
    assert!(sess.manager.is_member(&index.to_bytes(), b"elem1"));

    // FP32 形式 + 全选项（新键：既有集合的量化定义不可变更，同键换量化会被 C# 语义拒绝）
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
    assert_eq!(r, VectorReply::Simple(b"OK".to_vec()));
    let index = sess.read_index(b"k2").unwrap();
    assert_eq!(index.quant_type, VectorQuantType::NoQuant);
    assert_eq!(index.num_links, 8);
    assert_eq!(index.build_exploration_factor, 64);
    assert_eq!(index.distance_metric, VectorDistanceMetricType::Cosine);
    assert_eq!(
      sess.manager.service.get_attribute(index.context, b"elem2"),
      Some(b"{\"a\":1}".to_vec())
    );

    // 重复选项报错
    let v1 = f32_bytes(&[1.0]);
    let dup_sets: Vec<Vec<&[u8]>> = vec![
      vec![b"k", b"FP32", &v1, b"x", b"EF", b"8", b"EF", b"9"],
      vec![b"k", b"FP32", &v1, b"x", b"M", b"8", b"M", b"9"],
      vec![b"k", b"FP32", &v1, b"x", b"SETATTR", b"a", b"SETATTR", b"b"],
    ];
    for dup in dup_sets {
      let r = sess.network_vadd(&dup);
      assert!(matches!(r, VectorReply::Error(_)), "重复选项应报错");
    }

    // M 越界
    let r = sess.network_vadd(&[b"k", b"FP32", &f32_bytes(&[1.0]), b"x", b"M", b"2"]);
    assert!(
      s(&match r {
        VectorReply::Error(e) => e,
        _ => panic!(),
      })
      .contains("M must be")
    );

    // 向量格式非法
    let r = sess.network_vadd(&[b"k", b"FP32", b"123", b"x"]);
    assert!(
      s(&match r {
        VectorReply::Error(e) => e,
        _ => panic!(),
      })
      .contains("invalid vector")
    );
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

    // RESP2 + WITHSCORES
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

    // RESP3
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
    assert!(s(&encoded3).contains(','), "RESP3 double 输出");

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
    assert!(
      s(&match r {
        VectorReply::Error(e) => e,
        _ => panic!(),
      })
      .contains("COUNT specified multiple times")
    );

    // 缺键
    let r = sess.network_vsim(&[b"nope", b"ELE", b"x"]);
    assert!(
      s(&match r {
        VectorReply::Error(e) => e,
        _ => panic!(),
      })
      .contains("no such key")
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

    // VEMB
    let emb = sess.network_vemb(&[b"aux", b"e1"]);
    let mut encoded = Vec::new();
    emb.encode_resp2(&mut encoded);
    let text = s(&encoded);
    assert!(text.contains('5'), "嵌入值包含 5: {text}");

    // VGETATTR / VSETATTR
    assert_eq!(
      sess.network_vgetattr(&[b"aux", b"e1"]),
      VectorReply::Bulk(Some(b"{\"tag\":\"x\"}".to_vec()))
    );
    assert_eq!(
      sess.network_vsetattr(&[b"aux", b"e1", b"{}"]),
      VectorReply::Simple(b"OK".to_vec())
    );
    assert_eq!(
      sess.network_vgetattr(&[b"aux", b"e1"]),
      VectorReply::Bulk(Some(b"{}".to_vec()))
    );

    // VLINKS
    assert!(matches!(
      sess.network_vlinks(&[b"aux", b"e1"]),
      VectorReply::Array(_)
    ));

    // VRANDMEMBER
    assert!(matches!(
      sess.network_vrandmember(&[b"aux", b"2"]),
      VectorReply::Array(_)
    ));

    // VINFO
    let info = sess.network_vinfo(&[b"aux"]);
    let mut encoded = Vec::new();
    info.encode_resp2(&mut encoded);
    assert!(s(&encoded).contains("NOQUANT"));
    assert!(s(&encoded).contains("L2"));

    // VREM
    assert_eq!(sess.network_vrem(&[b"aux", b"e2"]), VectorReply::Integer(1));
    assert_eq!(sess.network_vrem(&[b"aux", b"e2"]), VectorReply::Integer(0));
    assert_eq!(sess.network_vcard(&[b"aux"]), VectorReply::Integer(1));
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

    // RESP2/RESP3 编码差异
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
}
