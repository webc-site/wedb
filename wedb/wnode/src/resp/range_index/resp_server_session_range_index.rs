//! RESP 网络层范围索引命令（对标 libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs）
//!
//! C# 为 RespServerSession 的 partial：解析网络参数 → 委派存储 API →
//! 写 RESP 应答。Rust 侧沿用命令域统一形态：`parse_state` 参数切片 +
//! wkv 存储会话 + 输出缓冲显式传参；RI 操作走 wkv 异步存储路径，故
//! 命令为 async。C# 的 `EnableRangeIndexPreview` 预览门在 rust 恒开
//!（RI 引擎是自适应分层存储的承重组件，见 doc/zh/deviations.md
//!「RI 预览门恒开」条），无 None 拒绝臂。
//!
//! 命令面扩展（按 .agents/skills/transpile/SKILL.md「O(1) 复杂度计数规约」
//! 授权）：本文件在 C# 九命令之外多一个 RI.COUNT，作为 MetaValue.size 的
//! O(1) 直读出口；RI.LEN 不是第二个命令，它在主分发表（`parser/resp_command.rs`
//! PRIMARY_TABLE）与 RI.COUNT 同枚举双名，解析期归一。
//!
//! 错误映射（对标 C# 双层文案）：CREATE/SET 走 storage 层 errorMsg 原样写出
//! （"ERR index already exists" / "ERR key+value size must be ..." / SET 索引
//! 缺失的 "ERR no such range index"），即 RangeIndexError 的 Display；GET/DEL/
//! SCAN/RANGE/COUNT/CONFIG/METRICS 的索引缺失为网络层硬编码 "ERR range index not
//! found"（Display 同文案承接）；SCAN/RANGE 内存模式不支持为网络层按命令
//! 硬编码文案（"ERR RI.SCAN/RANGE is not supported for MEMORY-mode indexes"，
//! RANGE 处覆写变体 Display 的 SCAN 文案），WRONGTYPE 走 CmdStrings 同款文案。
//!
//! 协议错误差异：RI.CREATE 数值选项非法时 C# 抛 RespParsingException →
//! catch 块回 `ERR Protocol Error: ...` 后 DisposeNetworkSender 断开会话
//! （RespServerSession.cs:522-540）；rust RI 族经 exec_slow 异步闭环
//! （无会话可变面，parse_violation 哨兵不可达），文案字节级对齐而连接
//! 保持，断连语义为已知差异。

use std::result::Result as StdResult;

use wbase::num::strict_i32;
use wbftree::{RangeIndexManager, RangeIndexStub, ScanReturnField, StorageBackendType, TreeTuning};
use wdev::Device;
use wkv::{RangeIndexError, StoreSession};
use wresp::{
  Result,
  check_args::{check_arg_count, unpack_args, unpack_args_rest},
  cmd_strings::{self as cs, abort_with_error_message},
  ext::{RESP_FRAME_HEAD_RESERVED, RespVecExt, backfill_resp_frame_head, reserve_resp_frame_head},
};

use crate::resp::resp_server_session::RespServerSession;

/// RI.CREATE 选项解析产物（数值域为原始 i64，校验后转引擎口径）
#[derive(Debug, Clone, PartialEq, Eq)]
struct RiCreateOptions {
  /// 存储后端字节（0 = Disk，1 = Memory）
  backend_byte: u8,
  cache_size: i64,
  min_record_size: i64,
  max_record_size: i64,
  max_key_len: i64,
  leaf_page_size: i64,
}

impl RiCreateOptions {
  /// C# 默认值起点（RespServerSessionRangeIndex.cs:44-50 Defaults，
  /// 引用存储层单点 TreeTuning::DEFAULT_RI，值 16MiB / 64 / 1024 / 128）
  const fn new() -> Self {
    Self {
      backend_byte: 0,
      cache_size: TreeTuning::DEFAULT_RI.cache_size as i64,
      min_record_size: TreeTuning::DEFAULT_RI.min_record_size as i64,
      max_record_size: TreeTuning::DEFAULT_RI.max_record_size as i64,
      max_key_len: TreeTuning::DEFAULT_RI.max_key_len as i64,
      leaf_page_size: 0,
    }
  }

  /// 校验并折算引擎调优参数（数值选项须 > 0；MINRECORD ≤ MAXRECORD；
  /// PAGESIZE 0 = 按 MAXRECORD 自动推导；CACHESIZE ≥ 4 × 叶页）
  fn validate(&self) -> StdResult<TreeTuning, &'static str> {
    if self.cache_size <= 0
      || self.min_record_size <= 0
      || self.max_record_size <= 0
      || self.max_key_len <= 0
      || self.leaf_page_size < 0
    {
      return Err("ERR numeric options must be greater than zero");
    }
    if self.min_record_size > self.max_record_size {
      return Err("ERR MINRECORD must not exceed MAXRECORD");
    }
    // 容量关系守卫（协议级快败）：CACHESIZE ≥ 4 × 叶页，即引擎
    // `Config::validate` 的 cache-only 比例判定（bf-tree 0.5.6 config.rs
    // circular buffer size 检查段：cache-only ≥ 4× 叶页、Disk ≥ 2×）。建树
    // 后端在本时点虽可知，但 cache-only 与否由引擎构造面联动（Memory 恒
    // cache-only、Disk 按数据路径），统一取两模式交集的严规则 4x、单一文案，
    // 保证守卫通过的组合在两种后端下均过引擎 validate——InvalidConfig 穿透
    // 帧在本命令面不可达，引擎串无从直出协议面（引擎 ConfigError 的文案翻译
    // 收 wbftree `config_error_to_string` 单点，仅供恢复/升阶运维面）。
    // 拒绝回定向错误帧，先于预留记账与旧世代工件清理拦下必然失败的建树。
    // C# 原型（RespServerSessionRangeIndex.cs:136-141）仅查 > 0，容量不足
    // 组合由 native 层引擎 validate 拒回 NULL、错误文案泛化——rust 侧入口
    // 补定向文案守卫系裁决过的合理偏离（doc/zh/deviations.md 第 83 条）。
    // 校验收 wnode 单点，wkv 建树臂不加第二套；PAGESIZE 显式给出按显式值判，
    // 缺省按 MAXRECORD 派生（wkv resolve_tuning 同一口径）
    let leaf_page_size = if self.leaf_page_size > 0 {
      self.leaf_page_size
    } else {
      RangeIndexManager::compute_leaf_page_size(self.max_record_size as usize) as i64
    };
    if self.cache_size < 4 * leaf_page_size {
      return Err("ERR CACHESIZE must be at least 4 times the leaf page size");
    }
    Ok(TreeTuning {
      cache_size: self.cache_size as usize,
      min_record_size: self.min_record_size as usize,
      max_record_size: self.max_record_size as usize,
      max_key_len: self.max_key_len as usize,
      leaf_page_size: self.leaf_page_size as usize,
    })
  }
}

/// RI.CREATE 数值选项解析失败两态（C# GetLong → RespParsingException，
/// SessionParseState.cs:402 + ParseUtils.cs:64 + RespReadUtils.cs:126）
enum RiNumError {
  /// 非数字 / 尾随垃圾 / u64 溢出 → C# ThrowNotANumber（回显原始参数）
  NotANumber(Vec<u8>),
  /// u64 域内但超 i64 → C# ThrowIntegerOverflow（数字串不含符号）
  Overflow { digits: String },
}

/// RI.CREATE 数值选项解析（C# parseState.GetLong 口径：allowLeadingZeros
/// 默认 true，前导零合法；失败两态见 [`RiNumError`]）
fn ri_option_long(parse_state: &[&[u8]], idx: usize) -> StdResult<i64, String> {
  let raw = parse_state[idx];
  let (digits, negative) = match raw {
    // C# ReadLong：length == 0 直接 ThrowNotANumber（ParseUtils.cs:69-71）
    [] | [b'+'] | [b'-'] => return Err(ri_num_error_text(RiNumError::NotANumber(raw.to_vec()))),
    [b'+', rest @ ..] => (rest, false),
    [b'-', rest @ ..] => (rest, true),
    rest => (rest, false),
  };
  // C# TryReadUInt64：中途非数字 / u64 溢出 → TryReadInt64 失败 →
  // ReadLong 外层 ThrowNotANumber（尾随垃圾 bytesRead != length 同此）
  let mut number = 0_u64;
  for &d in digits {
    match (d as char).to_digit(10) {
      Some(v) => match number
        .checked_mul(10)
        .and_then(|n| n.checked_add(u64::from(v)))
      {
        Some(n) => number = n,
        None => return Err(ri_num_error_text(RiNumError::NotANumber(raw.to_vec()))),
      },
      None => return Err(ri_num_error_text(RiNumError::NotANumber(raw.to_vec()))),
    }
  }
  let overflow = RiNumError::Overflow {
    digits: String::from_utf8_lossy(digits).into_owned(),
  };
  if negative {
    if number > i64::MIN.unsigned_abs() {
      return Err(ri_num_error_text(overflow));
    }
    if number == i64::MIN.unsigned_abs() {
      return Ok(i64::MIN);
    }
  } else if number > i64::MAX as u64 {
    return Err(ri_num_error_text(overflow));
  }
  Ok(if negative {
    -(number as i64)
  } else {
    number as i64
  })
}

/// 两态协议错误文案（C# RespServerSession.cs:522 catch 块
/// `ERR Protocol Error: {ex.Message}` 前缀；断连副作用见模块文档）
fn ri_num_error_text(e: RiNumError) -> String {
  let prefix = cs::ERR_PROTOCOL_ERROR_PREFIX;
  match e {
    RiNumError::NotANumber(arg) => {
      format!(
        "{prefix}Unable to parse number: {}",
        String::from_utf8_lossy(&arg)
      )
    }
    RiNumError::Overflow { digits } => {
      format!("{prefix}Unable to parse integer. The given number is larger than allowed: {digits}")
    }
  }
}

/// 解析 RI.CREATE 可选关键字参数（idx 从 1 起；C# while 循环逐分支形态）
fn parse_ricreate_options(parse_state: &[&[u8]]) -> StdResult<RiCreateOptions, String> {
  let mut options = RiCreateOptions::new();
  let mut idx = 1;
  while idx < parse_state.len() {
    let arg = parse_state[idx];
    if arg.eq_ignore_ascii_case(b"MEMORY") {
      options.backend_byte = 1;
      idx += 1;
    } else if arg.eq_ignore_ascii_case(b"DISK") {
      options.backend_byte = 0;
      idx += 1;
    } else {
      // 带值选项：选项名 → 值 → 前进 2；值缺失即定向错误（C# 文案逐项保留）
      let (slot, opt_name) = if arg.eq_ignore_ascii_case(b"CACHESIZE") {
        (&mut options.cache_size, "CACHESIZE")
      } else if arg.eq_ignore_ascii_case(b"MINRECORD") {
        (&mut options.min_record_size, "MINRECORD")
      } else if arg.eq_ignore_ascii_case(b"MAXRECORD") {
        (&mut options.max_record_size, "MAXRECORD")
      } else if arg.eq_ignore_ascii_case(b"MAXKEYLEN") {
        (&mut options.max_key_len, "MAXKEYLEN")
      } else if arg.eq_ignore_ascii_case(b"PAGESIZE") {
        (&mut options.leaf_page_size, "PAGESIZE")
      } else {
        return Err("ERR unknown option".into());
      };
      idx += 1;
      if idx >= parse_state.len() {
        return Err(format!("ERR {opt_name} requires a value"));
      }
      *slot = ri_option_long(parse_state, idx)?;
      idx += 1;
    }
  }
  Ok(options)
}

/// 解析 RI.SCAN / RI.RANGE 的 FIELDS 选项（默认 KEY|VALUE 双返）
#[inline]
fn parse_fields_option(args: &[&[u8]]) -> ScanReturnField {
  if let [fields, val, ..] = args
    && fields.eq_ignore_ascii_case(b"FIELDS")
  {
    if val.eq_ignore_ascii_case(b"KEY") {
      return ScanReturnField::Key;
    }
    if val.eq_ignore_ascii_case(b"VALUE") {
      return ScanReturnField::Value;
    }
  }
  ScanReturnField::KeyAndValue
}

/// 范围索引存储层错误帧统一收口（WRONGTYPE → RESP_ERR_WRONG_TYPE，其余 → Display）
#[inline]
fn handle_ri_error(output: &mut Vec<u8>, err: RangeIndexError) {
  match err {
    RangeIndexError::WrongType => abort_with_error_message(output, cs::RESP_ERR_WRONG_TYPE),
    e => abort_with_error_message(output, &e.to_string()),
  }
}

/// 写单条扫描记录 RESP 帧：KEY/VALUE 模式为批量字符串，BOTH 为 2 元素内嵌数组
/// （1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:TryWriteRecordResp；
/// rust 侧 output 为无上界 Vec，无需 C# 的 grow-in-place 换堆路径）
#[inline]
fn write_scan_record_resp(
  output: &mut Vec<u8>,
  key: &[u8],
  value: &[u8],
  return_field: ScanReturnField,
) {
  match return_field {
    ScanReturnField::Key => output.write_resp_bulk_string(key),
    ScanReturnField::Value => output.write_resp_bulk_string(value),
    ScanReturnField::KeyAndValue => {
      output.write_resp_array_len(2);
      output.write_resp_bulk_string(key);
      output.write_resp_bulk_string(value);
    }
  }
}

/// 写 RI.CONFIG 应答：6 字段 × 2 = 12 元素交替数组
///
/// 数值字段（cache_size u64、其余 u32）经 [`RespVecExt::write_resp_int_as_bulk_string`]
/// 单点写出（itoa 栈上格式化，零临时 `String`）。C# 本处是
/// `RespServerSessionRangeIndex.cs:558-582` 的 `cacheSize.ToString()` +
/// `TryWriteAsciiBulkString`；按 .agents/skills/rust_review/SKILL.md「数字转字符串用
/// itoa」的改进口径替换格式化手段，帧字节逐位不变。
fn write_config_resp(output: &mut Vec<u8>, stub: &RangeIndexStub) {
  output.write_resp_array_len(12);
  output.write_resp_bulk_string(b"storage_backend");
  output.write_resp_bulk_string(if stub.storage_backend == 0 {
    b"DISK"
  } else {
    b"MEMORY"
  });
  output.write_resp_bulk_string(b"cache_size");
  output.write_resp_int_as_bulk_string(stub.cache_size);
  output.write_resp_bulk_string(b"min_record_size");
  output.write_resp_int_as_bulk_string(stub.min_record_size);
  output.write_resp_bulk_string(b"max_record_size");
  output.write_resp_int_as_bulk_string(stub.max_record_size);
  output.write_resp_bulk_string(b"max_key_len");
  output.write_resp_int_as_bulk_string(stub.max_key_len);
  output.write_resp_bulk_string(b"leaf_page_size");
  output.write_resp_int_as_bulk_string(stub.leaf_page_size);
}

/// 写 RI.METRICS 应答：4 字段 × 2 = 8 元素交替数组
///
/// `tree_handle`（u64 句柄）同走 [`RespVecExt::write_resp_int_as_bulk_string`] 单点，
/// 对位 C# `RespServerSessionRangeIndex.cs:634` 的 `treeHandle.ToString()` +
/// `TryWriteAsciiBulkString`（无符号口径不变，帧字节逐位不变）。
fn write_metrics_resp(
  output: &mut Vec<u8>,
  tree_handle: u64,
  is_live: bool,
  is_flushed: bool,
  is_recovered: bool,
) {
  output.write_resp_array_len(8);
  output.write_resp_bulk_string(b"tree_handle");
  output.write_resp_int_as_bulk_string(tree_handle);
  output.write_resp_bulk_string(b"is_live");
  output.write_resp_bulk_string(if is_live { b"true" } else { b"false" });
  output.write_resp_bulk_string(b"is_flushed");
  output.write_resp_bulk_string(if is_flushed { b"true" } else { b"false" });
  output.write_resp_bulk_string(b"is_recovered");
  output.write_resp_bulk_string(if is_recovered { b"true" } else { b"false" });
}

/// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRICREATE
///
/// RI.CREATE key [MEMORY | DISK] [CACHESIZE n] [MINRECORD n] [MAXRECORD n]
/// [MAXKEYLEN n] [PAGESIZE n]；重复创建报 "ERR index already exists"
pub async fn network_ricreate<D: Device>(
  parse_state: &[&[u8]],
  session: &StoreSession<D>,
  output: &mut Vec<u8>,
) -> Result<bool> {
  check_arg_count!(parse_state, 1.., output, "RI.CREATE");
  let key = parse_state[0];

  let options = match parse_ricreate_options(parse_state) {
    Ok(options) => options,
    Err(message) => {
      abort_with_error_message(output, &message);
      return Ok(true);
    }
  };
  let tuning = match options.validate() {
    Ok(tuning) => tuning,
    Err(message) => {
      abort_with_error_message(output, message);
      return Ok(true);
    }
  };

  let backend = StorageBackendType::from_u8(options.backend_byte);
  match session.range_index_create(key, backend, tuning).await {
    Ok(()) => output.extend_from_slice(cs::RESP_OK),
    Err(e) => handle_ri_error(output, e),
  }
  Ok(true)
}

/// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRISET
///
/// RI.SET key field value
pub async fn network_riset<D: Device>(
  parse_state: &[&[u8]],
  session: &StoreSession<D>,
  output: &mut Vec<u8>,
) -> Result<bool> {
  let Some([key, field, value]) = unpack_args(parse_state, output, "RI.SET") else {
    return Ok(true);
  };

  match session.range_index_set(key, field, value).await {
    Ok(()) => output.extend_from_slice(cs::RESP_OK),
    // 索引不存在 → C# storage 层 errorMsg "ERR no such range index"
    // (RangeIndexOps.cs:212/220)，网络层 errorMsg.Length > 0 分支原样写出——
    // 与 GET/DEL 等网络层硬编码的 "ERR range index not found" 是两条不同文案
    Err(RangeIndexError::NotFound) => {
      abort_with_error_message(output, "ERR no such range index");
    }
    Err(e) => handle_ri_error(output, e),
  }
  Ok(true)
}

/// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRIGET
///
/// RI.GET key field：命中回批量字符串；键或字段缺失回 null 批量字符串
pub async fn network_riget<D: Device>(
  parse_state: &[&[u8]],
  session: &StoreSession<D>,
  resp_version: u8,
  output: &mut Vec<u8>,
) -> Result<bool> {
  let Some([key, field]) = unpack_args(parse_state, output, "RI.GET") else {
    return Ok(true);
  };

  // 闭包借用树页切片直写网络输出缓冲，单次点查零中间堆分配
  // （对标 C# RangeIndexGet 的 ReadByPtrInto 直写 output；内层块限定借用生存期）
  let got = {
    let mut on_val = |v: Option<&[u8]>| {
      if let Some(val) = v {
        output.write_resp_bulk_string(val);
        true
      } else {
        false
      }
    };
    session.range_index_get_with(key, field, &mut on_val).await
  };
  match got {
    Ok(true) => {}
    // 字段不存在 → null（C# RangeIndexResult.NotFound → WriteNull）
    Ok(false) => output.write_resp_null_ver(resp_version),
    Err(e) => handle_ri_error(output, e),
  }
  Ok(true)
}

/// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRIDEL
///
/// RI.DEL key field（删除树内字段；整键删除走标准 DEL）
pub async fn network_ridel<D: Device>(
  parse_state: &[&[u8]],
  session: &StoreSession<D>,
  output: &mut Vec<u8>,
) -> Result<bool> {
  let Some([key, field]) = unpack_args(parse_state, output, "RI.DEL") else {
    return Ok(true);
  };

  match session.range_index_del(key, field).await {
    Ok(_) => output.write_resp_int(1),
    Err(e) => handle_ri_error(output, e),
  }
  Ok(true)
}

/// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRISCAN
///
/// RI.SCAN key start COUNT n [FIELDS KEY|VALUE|BOTH]
pub async fn network_riscan<D: Device>(
  parse_state: &[&[u8]],
  session: &StoreSession<D>,
  output: &mut Vec<u8>,
) -> Result<bool> {
  let Some(([key, start, count_tok, count_raw], rest)) =
    unpack_args_rest(parse_state, output, "RI.SCAN")
  else {
    return Ok(true);
  };

  if !count_tok.eq_ignore_ascii_case(b"COUNT") {
    abort_with_error_message(output, "ERR syntax error, expected COUNT");
    return Ok(true);
  }
  // C# TryGetInt（int32）：非整数（含溢出）或 <=0 同报（:361-364）；前导零拒收系 rust 严格收口，见 doc/zh/deviations.md §32
  let Some(count) = strict_i32(count_raw).filter(|c| *c > 0) else {
    abort_with_error_message(output, "ERR invalid count");
    return Ok(true);
  };
  let return_field = parse_fields_option(rest);

  // 流式回调直写网络输出缓冲：乐观预留数组头，扫完回填实际计数
  // （对标 C# RangeIndexScan → WriteScanToOutput；零 Vec<ScanRecord> 中转拷贝）。
  // 回调经内层块限定借用生存期，避免跨 match 持锁阻塞错误分支写 output
  let base = reserve_resp_frame_head(output, RESP_FRAME_HEAD_RESERVED);
  let mut records = 0usize;
  let scanned = {
    let mut on_record = |k: &[u8], v: &[u8]| {
      records += 1;
      write_scan_record_resp(output, k, v, return_field);
      true
    };
    session
      .range_index_scan_stream(key, start, count as usize, return_field, &mut on_record)
      .await
  };
  match scanned {
    Ok(_) => backfill_resp_frame_head(output, base, RESP_FRAME_HEAD_RESERVED, records, |buf, n| {
      buf.write_resp_array_len(n)
    }),
    Err(e) => {
      // 中途树错误：丢弃本命令已写的部分帧再回错误（前置检查已覆盖
      // NotFound/MemoryMode，此路径仅余底层 I/O 故障）
      output.truncate(base);
      handle_ri_error(output, e);
    }
  }
  Ok(true)
}

/// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRIRANGE
///
/// RI.RANGE key start end [FIELDS KEY|VALUE|BOTH]：闭区间 [start, end]
pub async fn network_rirange<D: Device>(
  parse_state: &[&[u8]],
  session: &StoreSession<D>,
  output: &mut Vec<u8>,
) -> Result<bool> {
  let Some(([key, start, end], rest)) = unpack_args_rest(parse_state, output, "RI.RANGE") else {
    return Ok(true);
  };
  let return_field = parse_fields_option(rest);

  // 流式回调直写网络输出缓冲（对标 C# RangeIndexRange → WriteScanToOutput）
  let base = reserve_resp_frame_head(output, RESP_FRAME_HEAD_RESERVED);
  let mut records = 0usize;
  let scanned = {
    let mut on_record = |k: &[u8], v: &[u8]| {
      records += 1;
      write_scan_record_resp(output, k, v, return_field);
      true
    };
    session
      .range_index_range_stream(key, start, end, return_field, &mut on_record)
      .await
  };
  match scanned {
    Ok(_) => backfill_resp_frame_head(output, base, RESP_FRAME_HEAD_RESERVED, records, |buf, n| {
      buf.write_resp_array_len(n)
    }),
    // 内存模式不支持 → C# 网络层按命令硬编码文案（行 458 "ERR RI.RANGE ..."）；
    // 错误变体 Display 带的是 RI.SCAN 文案，此处按 C# 网络层口径覆写
    Err(RangeIndexError::MemoryModeNotSupported) => {
      output.truncate(base);
      abort_with_error_message(
        output,
        "ERR RI.RANGE is not supported for MEMORY-mode indexes",
      );
    }
    Err(e) => {
      output.truncate(base);
      handle_ri_error(output, e);
    }
  }
  Ok(true)
}

/// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRIEXISTS
///
/// RI.EXISTS key：非 RI 键一律 :0（不回 WRONGTYPE）
pub async fn network_riexists<D: Device>(
  parse_state: &[&[u8]],
  session: &StoreSession<D>,
  output: &mut Vec<u8>,
) -> Result<bool> {
  let Some([key]) = unpack_args(parse_state, output, "RI.EXISTS") else {
    return Ok(true);
  };

  let exists = session.range_index_exists(key).await.unwrap_or(false);
  output.write_resp_int(i64::from(exists));
  Ok(true)
}

/// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRICONFIG
///
/// RI.CONFIG key：索引配置 12 元素交替数组
pub async fn network_riconfig<D: Device>(
  parse_state: &[&[u8]],
  session: &StoreSession<D>,
  output: &mut Vec<u8>,
) -> Result<bool> {
  let Some([key]) = unpack_args(parse_state, output, "RI.CONFIG") else {
    return Ok(true);
  };

  match session.range_index_config(key).await {
    Ok(stub) => write_config_resp(output, &stub),
    Err(e) => handle_ri_error(output, e),
  }
  Ok(true)
}

/// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRIMETRICS
///
/// RI.METRICS key：树句柄与生命周期标志 8 元素交替数组
pub async fn network_rimetrics<D: Device>(
  parse_state: &[&[u8]],
  session: &StoreSession<D>,
  output: &mut Vec<u8>,
) -> Result<bool> {
  let Some([key]) = unpack_args(parse_state, output, "RI.METRICS") else {
    return Ok(true);
  };

  match session.range_index_metrics(key).await {
    Ok(m) => write_metrics_resp(
      output,
      m.tree_handle,
      m.is_live,
      m.is_flushed,
      m.is_recovered,
    ),
    Err(e) => handle_ri_error(output, e),
  }
  Ok(true)
}

/// RI.COUNT key：索引元素计数（O(1) 复杂度计数规约的 RESP 出口）
///
/// 本仓自定义命令，C# 无对应处理器（`RespServerSessionRangeIndex.cs` 只有
/// CREATE/SET/GET/DEL/SCAN/RANGE/EXISTS/CONFIG/METRICS 九个，
/// `RespCommand.cs` 的 RI 族亦无 RICOUNT/RILEN），故不写伪对标映射；
/// 结构与本文件 CONFIG/METRICS 同型（RI 门 → 参数计数 → 错误映射）。
/// 计数实现唯一：`wkv::range_index::ops.rs:range_index_count` 一次主存读
/// 直取 MetaValue.size，不触树、不扫树。RI.LEN 在解析期归一到同一
/// `RespCommand::Ricount`（主分发表双名，与 SLAVEOF/SECONDARYOF 同型），
/// 因此这里不存在第二套计数入口。
pub async fn network_ricount<D: Device>(
  parse_state: &[&[u8]],
  session: &StoreSession<D>,
  output: &mut Vec<u8>,
) -> Result<bool> {
  let Some([key]) = unpack_args(parse_state, output, "RI.COUNT") else {
    return Ok(true);
  };

  match session.range_index_count(key).await {
    Ok(count) => {
      output.write_resp_int(count as i64);
    }
    Err(e) => handle_ri_error(output, e),
  }
  Ok(true)
}

impl RespServerSession {
  #[inline]
  pub async fn network_ricreate<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    network_ricreate(parse_state, session, output).await
  }

  #[inline]
  pub async fn network_riset<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    network_riset(parse_state, session, output).await
  }

  #[inline]
  pub async fn network_riget<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    network_riget(parse_state, session, self.resp_protocol_version, output).await
  }

  #[inline]
  pub async fn network_ridel<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    network_ridel(parse_state, session, output).await
  }

  #[inline]
  pub async fn network_riscan<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    network_riscan(parse_state, session, output).await
  }

  #[inline]
  pub async fn network_rirange<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    network_rirange(parse_state, session, output).await
  }

  #[inline]
  pub async fn network_riexists<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    network_riexists(parse_state, session, output).await
  }

  #[inline]
  pub async fn network_riconfig<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    network_riconfig(parse_state, session, output).await
  }

  #[inline]
  pub async fn network_rimetrics<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    network_rimetrics(parse_state, session, output).await
  }

  #[inline]
  pub async fn network_ricount<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    network_ricount(parse_state, session, output).await
  }
}

#[cfg(test)]
mod tests {
  use super::*;

  /// ri_option_long 对标 C# parseState.GetLong 两态（ParseUtils.cs:64 +
  /// RespReadUtils.cs:126，allowLeadingZeros 默认 true）
  #[test]
  fn ri_option_long_matches_csharp_getlong() {
    // 合法（含前导零与符号——C# GetLong 无前导零拒绝）
    assert_eq!(ri_option_long(&[b"0"], 0).ok(), Some(0));
    assert_eq!(ri_option_long(&[b"007"], 0).ok(), Some(7));
    assert_eq!(ri_option_long(&[b"-007"], 0).ok(), Some(-7));
    assert_eq!(ri_option_long(&[b"+5"], 0).ok(), Some(5));
    assert_eq!(
      ri_option_long(&[b"9223372036854775807"], 0).ok(),
      Some(i64::MAX)
    );
    assert_eq!(
      ri_option_long(&[b"-9223372036854775808"], 0).ok(),
      Some(i64::MIN)
    );

    // 非数字 / 尾随垃圾 / u64 溢出 → ThrowNotANumber（回显原始参数）
    for raw in ["abc", "12x", "", "99999999999999999999"] {
      let err = ri_option_long(&[raw.as_bytes()], 0).expect_err(raw);
      assert_eq!(
        err,
        format!("ERR Protocol Error: Unable to parse number: {raw}")
      );
    }

    // u64 域内超 i64 → ThrowIntegerOverflow（数字串不含符号）
    let err = ri_option_long(&[b"9223372036854775808"], 0).expect_err("overflow");
    assert_eq!(
      err,
      "ERR Protocol Error: Unable to parse integer. The given number is larger than allowed: 9223372036854775808"
    );
    let err = ri_option_long(&[b"-9223372036854775809"], 0).expect_err("overflow");
    assert_eq!(
      err,
      "ERR Protocol Error: Unable to parse integer. The given number is larger than allowed: 9223372036854775809"
    );
  }

  /// RI.CREATE 选项解析：数值走 GetLong 两态，值缺失/未知选项文案不变
  #[test]
  fn parse_ricreate_options_two_state_errors() {
    let ok = parse_ricreate_options(&[b"k", b"CACHESIZE", b"007"]).expect("ok");
    assert_eq!(ok.cache_size, 7);

    let err = parse_ricreate_options(&[b"k", b"MINRECORD", b"xyz"]).expect_err("not a number");
    assert_eq!(err, "ERR Protocol Error: Unable to parse number: xyz");

    let err = parse_ricreate_options(&[b"k", b"PAGESIZE"]).expect_err("missing value");
    assert_eq!(err, "ERR PAGESIZE requires a value");

    let err = parse_ricreate_options(&[b"k", b"WHATEVER"]).expect_err("unknown");
    assert_eq!(err, "ERR unknown option");
  }
}
