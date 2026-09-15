use itoa::Buffer;
use wbase::num::{strict_i32, strict_i64};
use wresp::{
  Resp2, Resp3, RespProtocol, RespVecExt, RespWriter, check_arg_count, cmd_strings as cs,
  cmd_strings::{
    RESP_ERR_GENERIC, RESP_ERR_GENERIC_INVALIDCURSOR, RESP_ERR_GENERIC_SYNTAX_ERROR,
    RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_ERR_WRONG_TYPE, abort_with_error_message,
    write_error_raw,
  },
  unpack_args,
};
use wval::GarnetObjectType;

use crate::{
  resp::resp_server_session::RespServerSession,
  storage::session::{
    common::{
      array_key_iteration_functions::ScanTypeFilter,
      ttl_sync::{probe_alive, read_adjudicated_envelope_sync, read_adjudicated_user_sync},
    },
    storage_session::StorageSession,
  },
};

/// SCAN 过滤参数（C# NetworkSCAN 局部变量组的结构化承接）
///
/// 快路径校验段与慢路径执行段共用同一解析单源 [`parse_scan_filter`]
pub(crate) struct ScanFilter {
  /// 游标（C# cursorFromInput；地址游标口径，0 = 从头）
  pub cursor: i64,
  /// 匹配模式（默认 `*`；C# patternArgSlice）
  pub pattern: Vec<u8>,
  /// 模式为 `*` 时全量放行（C# allKeys）
  pub all_keys: bool,
  /// 单页计数（默认 10；负/零钳 0——C# countValue 直传扫描层
  /// `acceptedCount >= count`，首条匹配后即停）
  pub count: usize,
  /// 已知 TYPE 参数映射的过滤类型（C# matchType）
  pub type_filter: Option<ScanTypeFilter>,
  /// TYPE 参数出现过（此时单页无上限，C# `long.MaxValue` 同口径）
  pub type_given: bool,
  /// TYPE 值不属于五类已知类型（C# DbScan 对非空未知 typeObject 直接
  /// 回空列表 + 游标 0，ArrayKeyIterationFunctions.cs:82-84）
  pub type_unknown: bool,
}

/// 解析 SCAN 参数（cursor + MATCH/COUNT/TYPE 选项）
///
/// 校验口径 1:1 对标 C# NetworkSCAN：cursor 非法/负值、选项缺参、COUNT
/// 非整数均返回完整 RESP 错误行；未知选项静默跳过（C# if/else-if 链无
/// else 分支）；TYPE 匹配为大小写敏感双字面量（C# SequenceEqual），
/// 未知 TYPE 值由慢路径直接回空结果（C# DbScan 提前返回同口径）。
/// [`RespServerSession::network_scan`] 的共享解析单源（快路径校验段与
/// 慢路径执行段同一入口，单次实现）
pub(crate) fn parse_scan_filter(args: &[&[u8]]) -> Result<ScanFilter, &'static str> {
  let Some(cursor) = strict_i64(args.first().copied().unwrap_or(b"")) else {
    return Err(RESP_ERR_GENERIC_INVALIDCURSOR);
  };
  if cursor < 0 {
    return Err(RESP_ERR_GENERIC_INVALIDCURSOR);
  }

  let mut filter = ScanFilter {
    cursor,
    pattern: b"*".to_vec(),
    all_keys: true,
    count: 10,
    type_filter: None,
    type_given: false,
    type_unknown: false,
  };
  let mut token_idx = 1;
  while token_idx < args.len() {
    let param = args[token_idx];
    token_idx += 1;

    if param.eq_ignore_ascii_case(b"MATCH") {
      if token_idx >= args.len() {
        return Err(RESP_ERR_GENERIC_SYNTAX_ERROR);
      }
      filter.pattern = args[token_idx].to_vec();
      filter.all_keys = filter.pattern.as_slice() == b"*";
      token_idx += 1;
    } else if param.eq_ignore_ascii_case(b"COUNT") {
      if token_idx >= args.len() {
        return Err(RESP_ERR_GENERIC_SYNTAX_ERROR);
      }
      // C# TryGetLong 仅校验整数性；负/零钳 0，扫描层 max(1) 后首条
      // 匹配即停（C# acceptedCount >= count 同语义）
      match strict_i64(args[token_idx]) {
        Some(n) => filter.count = n.max(0) as usize,
        None => return Err(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER),
      }
      token_idx += 1;
    } else if param.eq_ignore_ascii_case(b"TYPE") {
      if token_idx >= args.len() {
        return Err(RESP_ERR_GENERIC_SYNTAX_ERROR);
      }
      filter.type_given = true;
      // C# SequenceEqual 大小写敏感，仅认双字面量（zset/ZSET 等）
      filter.type_filter = match args[token_idx] {
        b"zset" | b"ZSET" => Some(ScanTypeFilter::Object(GarnetObjectType::SortedSet)),
        b"list" | b"LIST" => Some(ScanTypeFilter::Object(GarnetObjectType::List)),
        b"set" | b"SET" => Some(ScanTypeFilter::Object(GarnetObjectType::Set)),
        b"hash" | b"HASH" => Some(ScanTypeFilter::Object(GarnetObjectType::Hash)),
        b"string" | b"STRING" => Some(ScanTypeFilter::String),
        // 未知类型：C# DbScan 对非空未知 typeObject 回空列表 + 游标 0
        _ => {
          filter.type_unknown = true;
          None
        }
      };
      token_idx += 1;
    }
    // 未知选项：C# if/else-if 链无 else，静默跳过（仅消费参数名本身）
  }
  Ok(filter)
}

impl RespServerSession {
  /// libs/server/Resp/ArrayCommands.cs:NetworkDEL
  ///
  /// C# 无 arity 校验（0 参即空循环回 :0），1:1 保留。
  pub fn network_del<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let mut deleted_count = 0i64;
    for key in parse_state {
      match store.try_delete_sync(key) {
        Ok(Ok(deleted)) => deleted_count += deleted as i64,
        // 环形页翻转 / 复合对象元数据：须降级完整异步路由，本次不产生输出
        Ok(Err(_)) => return Ok(false),
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      }
    }

    output.write_resp_int(deleted_count);
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkMGET
  ///
  /// C# 无 arity 校验（0 参即 `*0`），1:1 保留。
  /// 0 堆分配流式写出：直读内存切片写入 RespWriter 缓冲，遇异步降级整体回滚截断。
  pub fn network_mget<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if self.resp_protocol_version >= 3 {
      let mut writer = RespWriter::<_, Resp3>::new_ref_p(output);
      Self::do_network_mget(parse_state, store, &mut writer)
    } else {
      let mut writer = RespWriter::<_, Resp2>::new_ref(output);
      Self::do_network_mget(parse_state, store, &mut writer)
    }
  }

  #[inline]
  fn do_network_mget<P: RespProtocol, D: wdev::Device>(
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'_, D>,
    writer: &mut RespWriter<&mut Vec<u8>, P>,
  ) -> wresp::Result<bool> {
    let start_len = writer.len();
    writer.write_array_length(parse_state.len());
    for key in parse_state {
      // 双域读：String 域命中写值；信封域命中（对象键）写 nil（Redis MGET
      // 对非字符串键同答 nil，不报错）
      let res = read_adjudicated_user_sync(store, key, |v| writer.write_bulk_string(v));
      match res {
        Ok(Some(Some(Ok(())))) => {}
        Ok(Some(Some(Err(())))) => writer.write_null(),
        Ok(Some(None)) => writer.write_null(),
        Ok(None) => {
          writer.buf_mut().truncate(start_len);
          return Ok(false);
        }
        Err(_) => writer.write_null(),
      }
    }
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkMSET
  pub fn network_mset<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(
      parse_state.len() >= 2 && parse_state.len().is_multiple_of(2),
      output,
      "MSET"
    );

    // 对齐 NetworkSET：Ok(Err(page_id)) 为环形页翻转/异步闭环信号，
    // 吞掉即静默丢写，须整体降级（此时尚未写出任何应答，可安全重试）
    for chunk in parse_state.as_chunks::<2>().0 {
      match store.try_upsert_sync(chunk[0], chunk[1]) {
        Ok(Ok(_)) => {}
        Ok(Err(_)) => return Ok(false),
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      }
    }

    output.write_resp_simple_string("OK");
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkMSETNX
  pub fn network_msetnx<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(
      !parse_state.is_empty() && parse_state.len().is_multiple_of(2),
      output,
      "MSETNX"
    );

    // 检查是否有任何键已存在（双域：对象键同计存在，C# NX 语义）
    for chunk in parse_state.as_chunks::<2>().0 {
      match probe_alive(store, chunk[0]) {
        Ok(Some(true)) => {
          output.write_resp_int(0);
          return Ok(true);
        }
        Ok(Some(false)) | Err(_) => {}
        Ok(None) => return Ok(false),
      }
    }

    // 均不存在，写入所有键值
    for chunk in parse_state.as_chunks::<2>().0 {
      match store.try_upsert_sync(chunk[0], chunk[1]) {
        Ok(Ok(_)) => {}
        Ok(Err(_)) => return Ok(false),
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      }
    }

    output.write_resp_int(1);
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkSELECT
  ///
  /// C# 校验序：arity → 整数 → 集群门槛（非 0 库禁切）→ MaxDatabases 上界
  /// → 切库（activeDbId 已是目标库或 TrySwitchActiveDatabaseSession 成功）。
  /// 切库经 [`wkv::StoreSession::set_active_db`] 原子改写会话前缀：批处理
  /// 纪元内物理键编码每次按需重算前缀（`session_prefix()` 读原子变量），
  /// 纪元守卫仅保护内存直读，切库无 NewEpoch 交叉，安全。
  pub fn network_select<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    unpack_args!(parse_state, output, "SELECT", [index_raw]);

    // C# TryGetInt 校验（CmdStrings.RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER）
    let Some(index) = strict_i32(index_raw) else {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      return Ok(true);
    };
    // C#：集群模式不允许选择非 0 库（挂接集群切面即集群形态）
    if index != 0 && self.cluster_session.is_some() {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_SELECT_CLUSTER_MODE);
      return Ok(true);
    }
    // 负下标或超出 MaxDatabases 上界均越界（C# RESP_ERR_DB_INDEX_OUT_OF_RANGE）
    if !(0..self.max_databases).contains(&index) {
      abort_with_error_message(output, cs::RESP_ERR_DB_INDEX_OUT_OF_RANGE);
      return Ok(true);
    }
    // C#：index == activeDbId || TrySwitchActiveDatabaseSession(index)
    if index == self.active_db_id || self.try_switch_active_database_session(index) {
      store.session.set_active_db(index as u64);
      output.extend_from_slice(cs::RESP_OK);
    } else {
      // C# Debug.Fail 兜底路径：allowMultiDb 关闭时非当前库不可选
      abort_with_error_message(output, cs::RESP_ERR_SELECT_UNSUCCESSFUL);
    }
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkSWAPDB
  ///
  /// C# 校验序：arity → 集群门槛 → 两个整数（invalid first/second DB index）
  /// → 负下标与 MaxDatabases 上界 → storeWrapper.TrySwapDatabases 全局换库。
  /// 同库交换 C# 短路成功；真实换库在 wkv 前缀模型下须跨库搬移键值
  /// （`MultiDatabaseManager::try_swap_databases` 异步域承接），同步执行域
  /// 无法闭环，按降级约定返回 `Ok(false)` 绝不误答 +OK。
  pub fn network_swapdb(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    unpack_args!(parse_state, output, "SWAPDB", [idx1_raw, idx2_raw]);

    if self.cluster_session.is_some() {
      abort_with_error_message(output, cs::RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE);
      return Ok(true);
    }
    let Some(idx1) = strict_i32(idx1_raw) else {
      abort_with_error_message(output, cs::RESP_ERR_INVALID_FIRST_DB_INDEX);
      return Ok(true);
    };
    let Some(idx2) = strict_i32(idx2_raw) else {
      abort_with_error_message(output, cs::RESP_ERR_INVALID_SECOND_DB_INDEX);
      return Ok(true);
    };
    if !(0..self.max_databases).contains(&idx1) || !(0..self.max_databases).contains(&idx2) {
      abort_with_error_message(output, cs::RESP_ERR_DB_INDEX_OUT_OF_RANGE);
      return Ok(true);
    }
    // C# TrySwapDatabases：同库交换短路 +OK（无搬移语义）
    if idx1 == idx2 {
      output.extend_from_slice(cs::RESP_OK);
      return Ok(true);
    }
    // 异库交换：跨库键值搬移须异步闭环（同步域不得静默伪成功）
    Ok(false)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkDBSIZE
  pub fn network_dbsize(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, empty, output, "DBSIZE");
    // 全库扫描无法在同步快路径完成，对标 C# 降级异步路径执行
    Ok(false)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkKEYS
  pub fn network_keys(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    unpack_args!(parse_state, output, "KEYS", [_pattern]);
    // 键空间扫描无法在同步快路径完成，降级异步路径执行
    Ok(false)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkSCAN
  ///
  /// 同步段仅做参数校验（统一解析器 [`parse_scan_filter`]，与慢路径
  /// 异步执行段共用同一解析单源）；扫描本身在慢路径执行器闭环
  /// （[`crate::resp::slow_path::SlowWait`]）
  pub fn network_scan(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, !empty, output, "SCAN");
    // 参数校验通过后，游标扫描降级异步路径执行
    match parse_scan_filter(parse_state) {
      Ok(_) => Ok(false),
      Err(err) => {
        write_error_raw(output, err);
        Ok(true)
      }
    }
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkTYPE
  pub fn network_type<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    unpack_args!(parse_state, output, "TYPE", [key]);
    // 双域读（对标 C# ReadMethods.cs:HandleType 的 ValueIsObject 分支）：
    // String 域命中 → string；信封域命中按内层标签映射 zset/list/hash/set；
    // 两域皆缺 → none
    match read_adjudicated_user_sync(store, key, |v| v.len()) {
      Ok(Some(Some(Ok(_)))) => {
        output.write_resp_simple_string("string");
      }
      Ok(Some(Some(Err(())))) => {
        match read_adjudicated_envelope_sync(store, key, |raw| {
          raw.first().copied().and_then(GarnetObjectType::from_u8)
        }) {
          // 信封域命中已由双探确认；内层标签缺省兜底 none；磁盘候选降级
          Ok(Some(Some(Some(obj_type)))) => output.write_resp_simple_string(obj_type.as_str()),
          Ok(Some(Some(None))) | Ok(Some(None)) | Err(_) => output.write_resp_simple_string("none"),
          Ok(None) => return Ok(false),
        }
      }
      Ok(Some(None)) | Err(_) => {
        output.write_resp_simple_string("none");
      }
      Ok(None) => return Ok(false),
    }
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:WriteOutputForScan
  pub fn write_output_for_scan(cursor_value: i64, keys: &[&[u8]], output: &mut Vec<u8>) {
    output.write_resp_array_len(2);
    let mut cur_buf = Buffer::new();
    let cur_str = cur_buf.format(cursor_value);
    output.write_resp_bulk_string(cur_str.as_bytes());
    output.write_resp_array_len(keys.len());
    for key in keys {
      output.write_resp_bulk_string(key);
    }
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkLCS
  pub fn network_lcs<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    unpack_args!(parse_state, output, "LCS", [key1, key2], rest);

    let mut len_only = false;
    let mut with_idx = false;
    let mut min_match_len = 0usize;
    let mut with_match_len = false;

    let mut idx = 0;
    while idx < rest.len() {
      let opt = rest[idx];
      if opt.eq_ignore_ascii_case(b"LEN") {
        len_only = true;
      } else if opt.eq_ignore_ascii_case(b"IDX") {
        with_idx = true;
      } else if opt.eq_ignore_ascii_case(b"MINMATCHLEN") {
        idx += 1;
        if idx >= rest.len() {
          cs::write_error_raw(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
          return Ok(true);
        }
        // C# TryGetInt（有符号）：负值钳 0 而非报错
        let Some(min_len) = strict_i32(rest[idx]) else {
          cs::write_error_raw(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
          return Ok(true);
        };
        min_match_len = min_len.max(0) as usize;
      } else if opt.eq_ignore_ascii_case(b"WITHMATCHLEN") {
        with_match_len = true;
      } else {
        cs::write_error_raw(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }
      idx += 1;
    }

    if len_only && with_idx {
      cs::write_error_raw(
        output,
        "ERR If you want both the length and indexes, please just use IDX.",
      );
      return Ok(true);
    }

    // 双域读：String 域命中即字符串（值内容任意）；信封域命中（list 等对象键）
    // 报 WRONGTYPE
    let read_string_val = |key| match read_adjudicated_user_sync(store, key, |v| v.to_vec()) {
      Ok(Some(Some(Ok(v)))) => Ok(Some(v)),
      Ok(Some(Some(Err(())))) => Err(true),
      Ok(Some(None)) | Err(_) => Ok(None),
      Ok(None) => Err(false),
    };

    let s1 = match read_string_val(key1) {
      Ok(v) => v,
      Err(true) => {
        write_error_raw(output, RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Err(false) => return Ok(false),
    };
    let s2 = match read_string_val(key2) {
      Ok(v) => v,
      Err(true) => {
        write_error_raw(output, RESP_ERR_WRONG_TYPE);
        return Ok(true);
      }
      Err(false) => return Ok(false),
    };

    let resp3 = self.resp_protocol_version >= 3;
    match (s1.as_deref(), s2.as_deref()) {
      (Some(v1), Some(v2)) => {
        if len_only {
          let len = StorageSession::<D>::compute_lcs_length(v1, v2, min_match_len);
          output.write_resp_int(len as i64);
        } else if with_idx {
          let (total, matches) =
            StorageSession::<D>::compute_lcs_with_indices(v1, v2, min_match_len);
          StorageSession::<D>::write_lcs_matches(&matches, with_match_len, total, output, resp3);
        } else {
          let lcs = StorageSession::<D>::compute_lcs(v1, v2, min_match_len);
          output.write_resp_bulk_string(&lcs);
        }
      }
      _ => {
        if len_only {
          output.write_resp_int(0);
        } else if with_idx {
          StorageSession::<D>::write_lcs_matches(&[], with_match_len, 0, output, resp3);
        } else {
          output.write_resp_bulk_string(b"");
        }
      }
    }
    Ok(true)
  }
}
