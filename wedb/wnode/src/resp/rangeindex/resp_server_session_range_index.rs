//! RESP 网络层范围索引命令（对标 libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs）
//!
//! C# 为 RespServerSession 的 partial：解析网络参数 → 委派存储 API →
//! 写 RESP 应答。Rust 侧沿用命令域统一形态：`parse_state` 参数切片 +
//! wkv 存储会话 + 输出缓冲显式传参；RI 操作走 wkv 异步存储路径，故
//! 命令为 async。`store_wrapper.rangeIndexManager is null` 的预览关闭门
//! 以 `ri: Option<&RangeIndexManager>` 承接（None = 未启用）。
//!
//! 错误映射（对标 C# 双层文案）：CREATE/SET 走 storage 层 errorMsg 原样写出
//! （"ERR index already exists" / "ERR key+value size must be ..." / SET 索引
//! 缺失的 "ERR no such range index"），即 RangeIndexError 的 Display；GET/DEL/
//! SCAN/RANGE/CONFIG/METRICS 的索引缺失为网络层硬编码 "ERR range index not
//! found"（Display 同文案承接）；SCAN/RANGE 内存模式不支持为网络层按命令
//! 硬编码文案（"ERR RI.SCAN/RANGE is not supported for MEMORY-mode indexes"，
//! RANGE 处覆写变体 Display 的 SCAN 文案），WRONGTYPE 走 CmdStrings 同款文案。

use std::result::Result as StdResult;

use wdev::Device;
use wkv::{RangeIndexError, RangeIndexStub, ScanRecord, ScanReturnField, StoreSession, TreeTuning};
use wresp::{
  RespSliceExt, RespVecExt, Result,
  cmd_strings::{self as cs, abort_with_error_message, abort_with_wrong_number_of_arguments},
};

use super::range_index_manager::RangeIndexManager;
use crate::resp::resp_server_session::RespServerSession;

/// 预览命令未启用时的统一错误（C# AbortWithErrorMessage 文案）
const RI_DISABLED: &str = "ERR Range Index (preview) commands are not enabled";

/// RI.CREATE 默认调优（C# 同款：16MiB / 64 / 1024 / 128 / PAGESIZE 自动推导）
const DEFAULT_CACHE_SIZE: i64 = 16 * 1024 * 1024;
const DEFAULT_MIN_RECORD: i64 = 64;
const DEFAULT_MAX_RECORD: i64 = 1024;
const DEFAULT_MAX_KEY_LEN: i64 = 128;

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
  /// C# 默认值起点
  const fn new() -> Self {
    Self {
      backend_byte: 0,
      cache_size: DEFAULT_CACHE_SIZE,
      min_record_size: DEFAULT_MIN_RECORD,
      max_record_size: DEFAULT_MAX_RECORD,
      max_key_len: DEFAULT_MAX_KEY_LEN,
      leaf_page_size: 0,
    }
  }

  /// 校验并折算引擎调优参数（数值选项须 > 0；MINRECORD ≤ MAXRECORD；
  /// PAGESIZE 0 = 按 MAXRECORD 自动推导）
  fn validate(&self) -> StdResult<TreeTuning, &'static str> {
    if self.cache_size <= 0
      || self.min_record_size <= 0
      || self.max_record_size <= 0
      || self.max_key_len <= 0
    {
      return Err("ERR numeric options must be greater than zero");
    }
    if self.min_record_size > self.max_record_size {
      return Err("ERR MINRECORD must not exceed MAXRECORD");
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

/// 解析 RI.CREATE 可选关键字参数（idx 从 1 起；C# while 循环逐分支形态）
fn parse_ricreate_options(parse_state: &[&[u8]]) -> StdResult<RiCreateOptions, &'static str> {
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
      if arg.eq_ignore_ascii_case(b"CACHESIZE") {
        idx += 1;
        if idx >= parse_state.len() {
          return Err("ERR CACHESIZE requires a value");
        }
        options.cache_size = parse_state[idx].try_parse_i64().unwrap_or(0);
        idx += 1;
      } else if arg.eq_ignore_ascii_case(b"MINRECORD") {
        idx += 1;
        if idx >= parse_state.len() {
          return Err("ERR MINRECORD requires a value");
        }
        options.min_record_size = parse_state[idx].try_parse_i64().unwrap_or(0);
        idx += 1;
      } else if arg.eq_ignore_ascii_case(b"MAXRECORD") {
        idx += 1;
        if idx >= parse_state.len() {
          return Err("ERR MAXRECORD requires a value");
        }
        options.max_record_size = parse_state[idx].try_parse_i64().unwrap_or(0);
        idx += 1;
      } else if arg.eq_ignore_ascii_case(b"MAXKEYLEN") {
        idx += 1;
        if idx >= parse_state.len() {
          return Err("ERR MAXKEYLEN requires a value");
        }
        options.max_key_len = parse_state[idx].try_parse_i64().unwrap_or(0);
        idx += 1;
      } else if arg.eq_ignore_ascii_case(b"PAGESIZE") {
        idx += 1;
        if idx >= parse_state.len() {
          return Err("ERR PAGESIZE requires a value");
        }
        options.leaf_page_size = parse_state[idx].try_parse_i64().unwrap_or(0);
        idx += 1;
      } else {
        return Err("ERR unknown option");
      }
    }
  }
  Ok(options)
}

/// 解析 RI.SCAN / RI.RANGE 的 FIELDS 选项（默认 KEY|VALUE 双返）
fn parse_fields_option(
  parse_state: &[&[u8]],
  name_idx: usize,
  value_idx: usize,
) -> ScanReturnField {
  if parse_state.len() > value_idx
    && parse_state.len() > name_idx
    && parse_state[name_idx].eq_ignore_ascii_case(b"FIELDS")
  {
    let value = parse_state[value_idx];
    if value.eq_ignore_ascii_case(b"KEY") {
      return ScanReturnField::Key;
    }
    if value.eq_ignore_ascii_case(b"VALUE") {
      return ScanReturnField::Value;
    }
  }
  ScanReturnField::KeyAndValue
}

/// 写扫描记录应答：KEY/VALUE 模式为批量字符串序列，BOTH 为 2 元素内嵌数组
/// （C# storageApi.RangeIndexScan 经 RespWriteUtils 落帧的同款字节形态）
fn write_scan_records(output: &mut Vec<u8>, records: &[ScanRecord], return_field: ScanReturnField) {
  output.write_resp_array_len(records.len());
  for record in records {
    match return_field {
      ScanReturnField::Key => output.write_resp_bulk_string(&record.key),
      ScanReturnField::Value => output.write_resp_bulk_string(&record.value),
      ScanReturnField::KeyAndValue => {
        output.write_resp_array_len(2);
        output.write_resp_bulk_string(&record.key);
        output.write_resp_bulk_string(&record.value);
      }
    }
  }
}

/// 写 RI.CONFIG 应答：6 字段 × 2 = 12 元素交替数组
fn write_config_resp(output: &mut Vec<u8>, stub: &RangeIndexStub) {
  output.write_resp_array_len(12);
  output.write_resp_bulk_string(b"storage_backend");
  output.write_resp_bulk_string(if stub.storage_backend == 0 {
    b"DISK"
  } else {
    b"MEMORY"
  });
  output.write_resp_bulk_string(b"cache_size");
  output.write_resp_bulk_string(stub.cache_size.to_string().as_bytes());
  output.write_resp_bulk_string(b"min_record_size");
  output.write_resp_bulk_string(stub.min_record_size.to_string().as_bytes());
  output.write_resp_bulk_string(b"max_record_size");
  output.write_resp_bulk_string(stub.max_record_size.to_string().as_bytes());
  output.write_resp_bulk_string(b"max_key_len");
  output.write_resp_bulk_string(stub.max_key_len.to_string().as_bytes());
  output.write_resp_bulk_string(b"leaf_page_size");
  output.write_resp_bulk_string(stub.leaf_page_size.to_string().as_bytes());
}

/// 写 RI.METRICS 应答：4 字段 × 2 = 8 元素交替数组
fn write_metrics_resp(
  output: &mut Vec<u8>,
  tree_handle: u64,
  is_live: bool,
  is_flushed: bool,
  is_recovered: bool,
) {
  output.write_resp_array_len(8);
  output.write_resp_bulk_string(b"tree_handle");
  output.write_resp_bulk_string(tree_handle.to_string().as_bytes());
  output.write_resp_bulk_string(b"is_live");
  output.write_resp_bulk_string(if is_live { b"true" } else { b"false" });
  output.write_resp_bulk_string(b"is_flushed");
  output.write_resp_bulk_string(if is_flushed { b"true" } else { b"false" });
  output.write_resp_bulk_string(b"is_recovered");
  output.write_resp_bulk_string(if is_recovered { b"true" } else { b"false" });
}

impl RespServerSession {
  /// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRICREATE
  ///
  /// RI.CREATE key [MEMORY | DISK] [CACHESIZE n] [MINRECORD n] [MAXRECORD n]
  /// [MAXKEYLEN n] [PAGESIZE n]；重复创建报 "ERR index already exists"
  pub async fn network_ricreate<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    ri: Option<&RangeIndexManager>,
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    if ri.is_none() {
      abort_with_error_message(output, RI_DISABLED);
      return Ok(true);
    }
    if parse_state.is_empty() {
      abort_with_wrong_number_of_arguments(output, "RI.CREATE");
      return Ok(true);
    }
    let key = parse_state[0];

    let options = match parse_ricreate_options(parse_state) {
      Ok(options) => options,
      Err(message) => {
        abort_with_error_message(output, message);
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

    let backend = if options.backend_byte == 1 {
      wkv::StorageBackend::Memory
    } else {
      wkv::StorageBackend::Std
    };
    match session.range_index_create(key, backend, tuning).await {
      Ok(()) => output.extend_from_slice(cs::RESP_OK),
      Err(RangeIndexError::WrongType) => {
        abort_with_error_message(output, cs::RESP_ERR_WRONG_TYPE);
      }
      // 已存在 / 长度越界等： errorMsg 文案即错误内容（C# errorMsg 路径）
      Err(e) => abort_with_error_message(output, &e.to_string()),
    }
    Ok(true)
  }

  /// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRISET
  ///
  /// RI.SET key field value
  pub async fn network_riset<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    ri: Option<&RangeIndexManager>,
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    if ri.is_none() {
      abort_with_error_message(output, RI_DISABLED);
      return Ok(true);
    }
    if parse_state.len() != 3 {
      abort_with_wrong_number_of_arguments(output, "RI.SET");
      return Ok(true);
    }
    let (key, field, value) = (parse_state[0], parse_state[1], parse_state[2]);

    match session.range_index_set(key, field, value).await {
      Ok(()) => output.extend_from_slice(cs::RESP_OK),
      Err(RangeIndexError::WrongType) => {
        abort_with_error_message(output, cs::RESP_ERR_WRONG_TYPE);
      }
      // 索引不存在 → C# storage 层 errorMsg "ERR no such range index"
      // (RangeIndexOps.cs:212/220)，网络层 errorMsg.Length > 0 分支原样写出——
      // 与 GET/DEL 等网络层硬编码的 "ERR range index not found" 是两条不同文案
      Err(RangeIndexError::NotFound) => {
        abort_with_error_message(output, "ERR no such range index");
      }
      // InvalidKV 长度越界等：errorMsg 文案即错误内容（C# errorMsg 路径）
      Err(e) => abort_with_error_message(output, &e.to_string()),
    }
    Ok(true)
  }

  /// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRIGET
  ///
  /// RI.GET key field：命中回批量字符串；键或字段缺失回 null 批量字符串
  pub async fn network_riget<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    ri: Option<&RangeIndexManager>,
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    if ri.is_none() {
      abort_with_error_message(output, RI_DISABLED);
      return Ok(true);
    }
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "RI.GET");
      return Ok(true);
    }
    let (key, field) = (parse_state[0], parse_state[1]);

    match session.range_index_get(key, field).await {
      Ok(Some(value)) => output.write_resp_bulk_string(&value),
      // 字段不存在 → null（C# RangeIndexResult.NotFound → WriteNull）
      Ok(None) => output.write_resp_null(),
      Err(RangeIndexError::WrongType) => {
        abort_with_error_message(output, cs::RESP_ERR_WRONG_TYPE);
      }
      // 索引不存在 → "ERR range index not found"（含于 Display，走统一上抛）
      Err(e) => abort_with_error_message(output, &e.to_string()),
    }
    Ok(true)
  }

  /// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRIDEL
  ///
  /// RI.DEL key field（删除树内字段；整键删除走标准 DEL）
  pub async fn network_ridel<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    ri: Option<&RangeIndexManager>,
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    if ri.is_none() {
      abort_with_error_message(output, RI_DISABLED);
      return Ok(true);
    }
    if parse_state.len() != 2 {
      abort_with_wrong_number_of_arguments(output, "RI.DEL");
      return Ok(true);
    }
    let (key, field) = (parse_state[0], parse_state[1]);

    match session.range_index_del(key, field).await {
      Ok(_) => output.write_resp_int(1),
      Err(RangeIndexError::WrongType) => {
        abort_with_error_message(output, cs::RESP_ERR_WRONG_TYPE);
      }
      Err(e) => abort_with_error_message(output, &e.to_string()),
    }
    Ok(true)
  }

  /// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRISCAN
  ///
  /// RI.SCAN key start COUNT n [FIELDS KEY|VALUE|BOTH]
  pub async fn network_riscan<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    ri: Option<&RangeIndexManager>,
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    if ri.is_none() {
      abort_with_error_message(output, RI_DISABLED);
      return Ok(true);
    }
    if parse_state.len() < 4 {
      abort_with_wrong_number_of_arguments(output, "RI.SCAN");
      return Ok(true);
    }
    let (key, start) = (parse_state[0], parse_state[1]);

    if !parse_state[2].eq_ignore_ascii_case(b"COUNT") {
      abort_with_error_message(output, "ERR syntax error, expected COUNT");
      return Ok(true);
    }
    let Some(count) = parse_state[3].try_parse_i64().filter(|c| *c > 0) else {
      abort_with_error_message(output, "ERR invalid count");
      return Ok(true);
    };
    let return_field = parse_fields_option(parse_state, 4, 5);

    match session
      .range_index_scan(key, start, count as usize, return_field)
      .await
    {
      Ok(records) => write_scan_records(output, &records, return_field),
      Err(RangeIndexError::WrongType) => {
        abort_with_error_message(output, cs::RESP_ERR_WRONG_TYPE);
      }
      Err(e) => abort_with_error_message(output, &e.to_string()),
    }
    Ok(true)
  }

  /// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRIRANGE
  ///
  /// RI.RANGE key start end [FIELDS KEY|VALUE|BOTH]：闭区间 [start, end]
  pub async fn network_rirange<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    ri: Option<&RangeIndexManager>,
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    if ri.is_none() {
      abort_with_error_message(output, RI_DISABLED);
      return Ok(true);
    }
    if parse_state.len() < 3 {
      abort_with_wrong_number_of_arguments(output, "RI.RANGE");
      return Ok(true);
    }
    let (key, start, end) = (parse_state[0], parse_state[1], parse_state[2]);
    let return_field = parse_fields_option(parse_state, 3, 4);

    match session
      .range_index_range(key, start, end, return_field)
      .await
    {
      Ok(records) => write_scan_records(output, &records, return_field),
      Err(RangeIndexError::WrongType) => {
        abort_with_error_message(output, cs::RESP_ERR_WRONG_TYPE);
      }
      // 内存模式不支持 → C# 网络层按命令硬编码文案（行 458 "ERR RI.RANGE ..."）；
      // 错误变体 Display 带的是 RI.SCAN 文案，此处按 C# 网络层口径覆写
      Err(RangeIndexError::MemoryModeNotSupported) => {
        abort_with_error_message(
          output,
          "ERR RI.RANGE is not supported for MEMORY-mode indexes",
        );
      }
      Err(e) => abort_with_error_message(output, &e.to_string()),
    }
    Ok(true)
  }

  /// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRIEXISTS
  ///
  /// RI.EXISTS key：非 RI 键一律 :0（不回 WRONGTYPE）
  pub async fn network_riexists<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    ri: Option<&RangeIndexManager>,
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    if ri.is_none() {
      abort_with_error_message(output, RI_DISABLED);
      return Ok(true);
    }
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "RI.EXISTS");
      return Ok(true);
    }

    let exists = session
      .range_index_exists(parse_state[0])
      .await
      .unwrap_or(false);
    output.write_resp_int(i64::from(exists));
    Ok(true)
  }

  /// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRICONFIG
  ///
  /// RI.CONFIG key：索引配置 12 元素交替数组
  pub async fn network_riconfig<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    ri: Option<&RangeIndexManager>,
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    if ri.is_none() {
      abort_with_error_message(output, RI_DISABLED);
      return Ok(true);
    }
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "RI.CONFIG");
      return Ok(true);
    }

    match session.range_index_config(parse_state[0]).await {
      Ok(stub) => write_config_resp(output, &stub),
      Err(RangeIndexError::WrongType) => {
        abort_with_error_message(output, cs::RESP_ERR_WRONG_TYPE);
      }
      Err(e) => abort_with_error_message(output, &e.to_string()),
    }
    Ok(true)
  }

  /// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRIMETRICS
  ///
  /// RI.METRICS key：树句柄与生命周期标志 8 元素交替数组
  pub async fn network_rimetrics<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    ri: Option<&RangeIndexManager>,
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    if ri.is_none() {
      abort_with_error_message(output, RI_DISABLED);
      return Ok(true);
    }
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "RI.METRICS");
      return Ok(true);
    }

    match session.range_index_metrics(parse_state[0]).await {
      Ok(m) => write_metrics_resp(
        output,
        m.tree_handle,
        m.is_live,
        m.is_flushed,
        m.is_recovered,
      ),
      Err(RangeIndexError::WrongType) => {
        abort_with_error_message(output, cs::RESP_ERR_WRONG_TYPE);
      }
      Err(e) => abort_with_error_message(output, &e.to_string()),
    }
    Ok(true)
  }

  /// libs/server/Resp/RangeIndex/RespServerSessionRangeIndex.cs:NetworkRILEN
  ///
  /// RI.LEN key：返回索引中键值对总数（O(1) 元数据直读）
  pub async fn network_rilen<D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    ri: Option<&RangeIndexManager>,
    session: &StoreSession<D>,
    output: &mut Vec<u8>,
  ) -> Result<bool> {
    if ri.is_none() {
      abort_with_error_message(output, RI_DISABLED);
      return Ok(true);
    }
    if parse_state.len() != 1 {
      abort_with_wrong_number_of_arguments(output, "RI.LEN");
      return Ok(true);
    }

    match session.range_index_len(parse_state[0]).await {
      Ok(len) => output.write_resp_int(len as i64),
      Err(RangeIndexError::WrongType) => {
        abort_with_error_message(output, cs::RESP_ERR_WRONG_TYPE);
      }
      Err(e) => abort_with_error_message(output, &e.to_string()),
    }
    Ok(true)
  }
}
