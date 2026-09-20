use itoa::Buffer;
use wbase::{
  hash_slot::slot_of,
  num::{DbIndexError, parse_db_index, strict_i32, strict_i64},
};
use wresp::{
  check_args::{check_arg_count, unpack_args, unpack_args_rest},
  cmd_strings as cs,
  cmd_strings::{
    RESP_ERR_GENERIC, RESP_ERR_GENERIC_INVALIDCURSOR, RESP_ERR_GENERIC_SYNTAX_ERROR,
    RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_ERR_WRONG_TYPE, abort_with_error_message,
    write_error_raw,
  },
  ext::RespVecExt,
  resp_memory_writer::{Resp2, Resp3, RespProtocol, RespWriter},
};
use wval::{GarnetObjectType, KeyTag};

use crate::{
  resp::{
    basic_commands::{RiWriteGate, ri_write_gate},
    resp_server_session::RespServerSession,
    vector::vector_manager::VectorManager,
  },
  storage::session::{
    common::{
      TagRead, UserRead,
      array_key_iteration_functions::ScanTypeFilter,
      read_envelope_sync, read_tag_sync, read_user_sync, read_user_sync_with_prefix,
      ttl_sync::{meta_collection_type_of, probe_alive_with_prefix},
    },
    storage_session::StorageSession,
  },
};

/// libs/server/Resp/CmdStrings.cs:RESP_ERR_LENGTH_AND_INDEXES
///
/// C# 常量不带 `ERR` 前缀（RespWriteUtils.TryWriteError 直加 `-` 成帧），
/// 1:1 保留 garnet quirk：应答为 `-If you want ... IDX.\r\n`
const RESP_ERR_LENGTH_AND_INDEXES: &str =
  "If you want both the length and indexes, please just use IDX.";

/// SCAN 过滤参数（C# NetworkSCAN 局部变量组的结构化承接）
///
/// 快路径校验段与慢路径执行段共用同一解析单源 [`parse_scan_filter`]
#[derive(Debug, PartialEq, Eq)]
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
/// else 分支）；TYPE 取值按 C# DbScan 双形态精确比对（全大写/全小写各一，
/// ArrayKeyIterationFunctions.cs:57-76），混合大小写及其它未知值归未知类型，
/// 由慢路径直接回空结果（C# DbScan 提前返回同口径）。选项名本身（MATCH/COUNT/
/// TYPE）大小写不敏感（C# EqualsUpperCaseSpanIgnoringCase 同口径）。
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

    if param.eq_ignore_ascii_case(cs::MATCH) {
      if token_idx >= args.len() {
        return Err(RESP_ERR_GENERIC_SYNTAX_ERROR);
      }
      filter.pattern = args[token_idx].to_vec();
      filter.all_keys = filter.pattern.as_slice() == b"*";
      token_idx += 1;
    } else if param.eq_ignore_ascii_case(cs::COUNT) {
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
    } else if param.eq_ignore_ascii_case(cs::TYPE) {
      if token_idx >= args.len() {
        return Err(RESP_ERR_GENERIC_SYNTAX_ERROR);
      }
      filter.type_given = true;
      let type_arg = args[token_idx];
      // C# DbScan 双形态精确比对（SequenceEqual 全大写/全小写各一），混合大小写
      // 及其它值走未知臂回空。string 的 C# 常量名 stringt 但值为 "string"。
      filter.type_filter = if type_arg == b"zset" || type_arg == b"ZSET" {
        Some(ScanTypeFilter::Object(GarnetObjectType::SortedSet))
      } else if type_arg == b"list" || type_arg == b"LIST" {
        Some(ScanTypeFilter::Object(GarnetObjectType::List))
      } else if type_arg == b"set" || type_arg == b"SET" {
        Some(ScanTypeFilter::Object(GarnetObjectType::Set))
      } else if type_arg == b"hash" || type_arg == b"HASH" {
        Some(ScanTypeFilter::Object(GarnetObjectType::Hash))
      } else if type_arg == b"string" || type_arg == b"STRING" {
        Some(ScanTypeFilter::String)
      } else {
        // 未知类型（含混合大小写、stream 等）：C# DbScan 对非空未知 typeObject 回空列表 + 游标 0
        filter.type_unknown = true;
        None
      };
      token_idx += 1;
    }
    // 未知选项：C# if/else-if 链无 else，静默跳过（仅消费参数名本身）
  }
  Ok(filter)
}

/// 信封内层标签 → Redis TYPE 类型串单点
///
/// 内建段走 [`GarnetObjectType`] 小写名；扩展段统一走
/// [`custom_objects::custom_object_type_name`] 编译期静态清单（C# modules
/// 注册名，使 TYPE 与 EXISTS 对扩展对象键的存活口径一致——C# HandleType
/// 对 custom object 的 ValueObject 四类型 switch 无 default 臂输出零字节
/// quirk，rust 刻意差异：回注册名）；未知标签仍 none（畸形信封防御臂）
const fn envelope_object_type_name(tag: u8) -> Option<&'static str> {
  if let Some(obj_type) = GarnetObjectType::from_u8(tag) {
    return Some(obj_type.as_str());
  }
  super::custom_objects::custom_object_type_name(tag)
}

impl RespServerSession {
  /// libs/server/Resp/ArrayCommands.cs:NetworkDEL
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:DELETE_MainStore
  ///
  /// C# 无 arity 校验（0 参即空循环回 :0），1:1 保留。向量集清退下沉至 wkv
  /// 用户键删除单点（双域判未命中后经 [`crate::storage::session::storage_session::vector_registry_delete_hook`]
  /// 摘除登记表项，对标 C# MainStore RemoveKey 回调 → VectorManager.RequestDeletion，
  /// GarnetRecordTriggers.OnDispose 的 Deleted 臂），本层不再另配第二套清退判据。
  pub fn network_del<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    // 循环前缀外提（transpile SKILL 工程准则；rust 工程优化无 c# 对应）：
    // 单命令执行窗口内 ns/db 原子变量不可变（RESP 命令原子性，批内 SELECT
    // 不可能插入 DEL 循环），循环零前缀重读与 Varint 重算
    let prefix = store.session_prefix();
    let prefix_slice = prefix.as_slice();
    let mut deleted_count = 0i64;
    for key in parse_state {
      // 单点删除：返回值已含登记表缺席收口（向量集键命中即 true，计 1）
      let deleted = match store.try_delete_sync_with_prefix(prefix_slice, key) {
        Ok(Ok(deleted)) => deleted,
        // 环形页翻转 / 复合对象元数据：须降级完整异步路由，本次不产生输出
        Ok(Err(_)) => return Ok(false),
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      };
      deleted_count += i64::from(deleted);
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
      Self::do_network_mget(
        parse_state,
        store,
        self.session_metrics.as_deref(),
        &mut writer,
      )
    } else {
      let mut writer = RespWriter::<_, Resp2>::new_ref(output);
      Self::do_network_mget(
        parse_state,
        store,
        self.session_metrics.as_deref(),
        &mut writer,
      )
    }
  }

  #[inline]
  fn do_network_mget<P: RespProtocol, D: wdev::Device>(
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'_, D>,
    metrics: Option<&wmetric::SessionMetricsHandle>,
    writer: &mut RespWriter<&mut Vec<u8>, P>,
  ) -> wresp::Result<bool> {
    let start_len = writer.len();
    writer.write_array_length(parse_state.len());
    // 循环前缀外提（transpile SKILL 工程准则；rust 工程优化无 c# 对应）：
    // 单命令执行窗口内 ns/db 原子变量不可变（批内 SELECT 不可能插入 MGET
    // 循环），循环零前缀重读与 Varint 重算
    let prefix = store.session_prefix();
    let prefix_slice = prefix.as_slice();
    // 逐键命中/未命中本地累加、非 deferred 收尾一次入账（终值与 C# 批量
    // GET 循环逐键累加同口径；deferred 整批转慢路径重执，即时入账会双计）
    let (mut found, mut notfound) = (0u64, 0u64);
    for key in parse_state {
      // 双域读：String 域命中写值；信封域命中（对象键）写 nil（Redis MGET
      // 对非字符串键同答 nil，不报错）
      match read_user_sync_with_prefix(store, prefix_slice, key, None, |v| {
        writer.write_bulk_string(v)
      }) {
        Ok(UserRead::Hit(())) => found += 1,
        // WrongType 与 C# MainStoreOps GET 判定同不计（仅 Missing 计未命中）；
        // MGET 对对象键同答 nil 不报错
        Ok(UserRead::WrongType) => writer.write_null(),
        Ok(UserRead::Missing) => {
          notfound += 1;
          writer.write_null();
        }
        Ok(UserRead::Deferred) => {
          writer.buf_mut().truncate(start_len);
          return Ok(false);
        }
        Err(_) => writer.write_null(),
      }
    }
    if let Some(metrics) = metrics {
      metrics.incr_total_found(found);
      metrics.incr_total_notfound(notfound);
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

    // RI 键门折叠前预检（复用 ri_write_gate 单点，判据不
    // 另起第二套）：任一键为存活 RangeIndex 整命令拒 WRONGTYPE——预检先于任
    // 何写入，零键落库无半提交；Deferred（元记录有磁盘候选）沿用既有出口
    // 整体降级慢路径，由执行臂异步对偶门复裁决
    for chunk in parse_state.as_chunks::<2>().0 {
      match ri_write_gate(store, chunk[0], output) {
        RiWriteGate::Pass => {}
        RiWriteGate::Blocked => return Ok(true),
        RiWriteGate::Deferred => return Ok(false),
      }
    }

    // 批量接口单次折叠（transpile SKILL 工程准则；rust 工程优化无 c# 对应，
    // 折叠先例对标 C# MainStoreOps.cs:MSET_Conditional 全键锁内批量 SET）：
    // 批外层复用纪元守卫零新增 enter、会话前缀单次外提、借用对排序去重保末值
    //（MSET 重复键后者胜）。对齐 NetworkSET：Ok(Err(page_id)) 为环形页翻转/
    // 异步闭环信号，吞掉即静默丢写，须整体降级（已写键随慢路径整命令重放幂等，
    // 此时尚未写出任何应答，可安全重试）
    let prefix = store.session_prefix();
    let prefix_slice = prefix.as_slice();
    let pairs = parse_state.as_chunks::<2>().0.iter().map(|c| (c[0], c[1]));
    match store.try_upsert_batch_sync_with_prefix(prefix_slice, pairs) {
      Ok(Ok(())) => {}
      Ok(Err(_)) => return Ok(false),
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }

    output.write_resp_simple_string("OK");
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkMSETNX
  ///
  /// 全有或全无条件批量写，存储侧映射
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:MSET_Conditional
  ///（全键排他锁内
  /// EXISTS 判定 + 锁内批量 SET + Commit）：compio 单线程协作调度下同步
  /// 段（无 await 点）天然串行，判定与写入之间无插入窗口。任一环节须
  /// 异步闭环时整体降级慢路径（[`Self::msetnx_resume`] 携带续跑模式），
  /// 绝不以半提交状态应答。
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

    // 循环前缀外提（transpile SKILL 工程准则；rust 工程优化无 c# 对应）：
    // 判定与写入两循环共用同一外提前缀，零逐键重读与 Varint 重算
    let prefix = store.session_prefix();
    let prefix_slice = prefix.as_slice();

    // 检查是否有任何键已存在（三域存活探针：对象信封与升阶键 Meta 元记录
    // 同计存在，C# NX 语义）。降级（Ok(None)：磁盘候选 / TTL 待裁决）发生
    // 时尚未写入任何键，整体移交慢路径完整裁决，安全重放
    self.msetnx_resume = false;
    for chunk in parse_state.as_chunks::<2>().0 {
      match probe_alive_with_prefix(store, prefix_slice, chunk[0]) {
        Ok(Some(true)) => {
          output.write_resp_int(0);
          return Ok(true);
        }
        // C# EXISTS 非 NOTFOUND 即判存在；rust 存储错误与 EXISTS 命令同
        // 口径回错（NetworkEXISTS），不得吞作"不存在"继续写入
        Ok(Some(false)) => {}
        Ok(None) => return Ok(false),
        Err(_) => {
          output.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      }
    }

    // 均不存在，批量折叠写入所有键值（排序去重保末值，重复键后者胜，与
    // 逐键 SET 幂等等价）；环形页翻转 / TTL 清除等异步闭环信号出现时，
    // 前面键已持久写入而判定已整体通过：置补写标记整体移交慢路径续写
    //（exec_slow MSETNX 补写模式，重写已写键同值幂等），禁止半提交误答 :0
    let pairs = parse_state.as_chunks::<2>().0.iter().map(|c| (c[0], c[1]));
    match store.try_upsert_batch_sync_with_prefix(prefix_slice, pairs) {
      Ok(Ok(())) => {}
      Ok(Err(_)) => {
        self.msetnx_resume = true;
        return Ok(false);
      }
      Err(_) => {
        output.write_resp_error(RESP_ERR_GENERIC);
        return Ok(true);
      }
    }

    output.write_resp_int(1);
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkSELECT
  ///
  /// C# 校验序：arity → 整数 → MaxDatabases 上界 → 切库（TrySwitchActiveDatabaseSession）。
  /// 切库经 [`wkv::StoreSession::set_active_db`] 原子改写会话前缀：批处理
  /// 物理键编码前缀按命令边界重算（批量命令在单命令窗口内一次外提，
  /// 见 [`Self::network_mget`]/[`Self::network_mset`]/[`Self::network_del`]），
  /// 纪元守卫仅保护内存直读，切库无 NewEpoch 交叉，安全。
  pub fn network_select<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([index_raw]) = unpack_args(parse_state, output, "SELECT") else {
      return Ok(true);
    };

    let index = match parse_db_index(index_raw) {
      // 线面按 C# int32 档收口，内部库 ID 仍 u64（Ok 档非负，as u64 升位无损）
      Ok(idx) => idx as u64,
      Err(DbIndexError::NotInteger) => {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      }
      Err(DbIndexError::OutOfRange) => {
        abort_with_error_message(output, cs::RESP_ERR_DB_INDEX_OUT_OF_RANGE);
        return Ok(true);
      }
    };
    if !self.try_switch_active_database_session(index) {
      abort_with_error_message(output, cs::RESP_ERR_DB_INDEX_OUT_OF_RANGE);
      return Ok(true);
    }
    // 冷库挂起面：切库报告映射未装载时挂起磁盘点查装载，+OK 交由 SlowWait
    // 闭环（装载完成并重放切库后原样应答，后续命令消费到的一定是装载后上下文）
    if let Some((ns, db)) = self.take_cold_ctx() {
      self.park_cold_context_load(store.session.store(), ns, db, cs::RESP_OK.to_vec());
      return Ok(true);
    }
    store.session.set_active_db(index);
    output.extend_from_slice(cs::RESP_OK);
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkSWAPDB
  ///
  /// C# 校验序：arity → 集群门槛 → 两个整数（invalid first/second DB index）
  /// → 负下标与 MaxDatabases 上界 → storeWrapper.TrySwapDatabases 全局换库。
  /// 集群门禁按 doc/zh/db.md SWAPDB 条款改库槽位归属 + 槽态判定（偏离 C#
  /// 一刀切，槽位真值源 `Slot = Mixer(namespace, db)` 单点
  /// wbase::hash_slot::slot_of）：校验序相应调整为 arity → 两个整数 → 上界 →
  /// 归属门禁 → 同库短路——门禁需库号参与，门槛必然后置于下标解析。两库槽位
  /// 均由本地节点掌管且处于 Stable 态才放行（同库交换亦须本节点持有该槽），
  /// 分属不同节点、本节点不持有或任一槽位处于 MIGRATING/IMPORTING 迁移窗口
  /// 即拦截回 RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE——O(1) 换号只换
  /// logic_db→vdb 指针、物理数据原地，迁移窗口内换库会将在途搬迁数据与另一库
  /// 归属对调，必须与在途搬迁互斥。真实换库为 wkv O(1) 虚库 ID
  /// 原子互换（`StoreSession::swap_databases`，零物理搬移），命令面同步执行段
  /// 按降级约定返回 `Ok(false)` 绝不误答 +OK，异步域由 `swap_command_slow`
  /// 承接闭环。
  pub fn network_swapdb(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([idx1_raw, idx2_raw]) = unpack_args(parse_state, output, "SWAPDB") else {
      return Ok(true);
    };

    let idx1 = match parse_db_index(idx1_raw) {
      // 线面 i32 档，内部库 ID 仍 u64；超档字面量走 NotInteger，
      // 按 C# NetworkSWAPDB 回 invalid first DB index 档
      Ok(idx) => idx as u64,
      Err(DbIndexError::NotInteger) => {
        abort_with_error_message(output, cs::RESP_ERR_INVALID_FIRST_DB_INDEX);
        return Ok(true);
      }
      Err(DbIndexError::OutOfRange) => {
        abort_with_error_message(output, cs::RESP_ERR_DB_INDEX_OUT_OF_RANGE);
        return Ok(true);
      }
    };
    let idx2 = match parse_db_index(idx2_raw) {
      Ok(idx) => idx as u64,
      Err(DbIndexError::NotInteger) => {
        abort_with_error_message(output, cs::RESP_ERR_INVALID_SECOND_DB_INDEX);
        return Ok(true);
      }
      Err(DbIndexError::OutOfRange) => {
        abort_with_error_message(output, cs::RESP_ERR_DB_INDEX_OUT_OF_RANGE);
        return Ok(true);
      }
    };
    if idx1 >= self.max_databases || idx2 >= self.max_databases {
      abort_with_error_message(output, cs::RESP_ERR_DB_INDEX_OUT_OF_RANGE);
      return Ok(true);
    }
    // 集群归属门禁（doc/zh/db.md：单机直接执行；集群校验两库槽位是否均由
    // 当前本地节点掌管且处于 Stable 态——分属不同物理节点、或任一库槽位
    // 处于 MIGRATING/IMPORTING 迁移窗口（换号与在途搬迁互斥）均拦截）
    if let Some(provider) = self.cluster_provider.as_ref()
      && provider.is_cluster_enabled()
    {
      let ns = self.namespace;
      if !(provider.is_slot_local_stable(slot_of(ns, idx1))
        && provider.is_slot_local_stable(slot_of(ns, idx2)))
      {
        abort_with_error_message(output, cs::RESP_ERR_GENERIC_SWAPDB_CLUSTER_MODE);
        return Ok(true);
      }
    }
    // C# TrySwapDatabases：同库交换短路 +OK（无搬移语义）
    if idx1 == idx2 {
      output.extend_from_slice(cs::RESP_OK);
      return Ok(true);
    }
    // 异库交换：虚库 ID 互换由异步慢路径 swap_command_slow 闭环（同步域不得静默伪成功）
    Ok(false)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkDBSIZE
  pub fn network_dbsize(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    check_arg_count!(parse_state, ..=0, output, "DBSIZE");
    // 全库扫描无法在同步快路径完成，对标 C# 降级异步路径执行
    Ok(false)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkKEYS
  pub fn network_keys(
    &mut self,
    parse_state: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([_pattern]) = unpack_args(parse_state, output, "KEYS") else {
      return Ok(true);
    };
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
    check_arg_count!(parse_state, 1.., output, "SCAN");
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
    vector: Option<&VectorManager>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([key]) = unpack_args(parse_state, output, "TYPE") else {
      return Ok(true);
    };
    if vector.is_some_and(|vm| {
      vm.read_stored_index(store.session_prefix().as_slice(), key)
        .is_some()
    }) {
      output.write_resp_simple_string("vectorset");
      return Ok(true);
    }
    // 三域读（对标 C# ReadMethods.cs:HandleType 的 ValueIsObject 分支）：
    // String 域命中 → string；Meta 域命中（升阶键）按 MetaValue.collection_type
    // 映射；信封域命中按内层标签映射 zset/list/hash/set 与扩展注册名
    //（[`envelope_object_type_name`]）；三域皆缺 → none
    match read_user_sync(store, key, None, |_| ()) {
      Ok(UserRead::Hit(())) => {
        output.write_resp_simple_string("string");
      }
      Ok(UserRead::WrongType) => {
        // 升阶键：Meta 元记录存活且带集合类型，直读类型名
        match read_tag_sync(store, key, KeyTag::Meta, meta_collection_type_of) {
          Ok(TagRead::Hit(Some(obj_type))) => {
            output.write_resp_simple_string(obj_type.as_str());
          }
          Ok(TagRead::Deferred) => return Ok(false),
          // Meta 域缺失/死记录：落信封域读（对象信封键口径，含扩展注册名）
          _ => match read_envelope_sync(store, key, |raw| {
            raw.first().copied().and_then(envelope_object_type_name)
          }) {
            // 信封域命中已由双探确认；内层标签缺省兜底 none；磁盘候选降级
            Ok(TagRead::Hit(Some(name))) => {
              output.write_resp_simple_string(name);
            }
            Ok(TagRead::Hit(None)) | Ok(TagRead::Missing) | Err(_) => {
              output.write_resp_simple_string("none");
            }
            Ok(TagRead::Deferred) => return Ok(false),
          },
        }
      }
      Ok(UserRead::Missing) | Err(_) => {
        output.write_resp_simple_string("none");
      }
      Ok(UserRead::Deferred) => return Ok(false),
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
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:LCS
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:LCSInternal
  ///（读两键 + LCS 计算内核：LEN/IDX/默认三形态与缺键空应答皆在本函数闭环）
  pub fn network_lcs<'a, D: wdev::Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(([key1, key2], rest)) = unpack_args_rest(parse_state, output, "LCS") else {
      return Ok(true);
    };

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
      cs::write_error_raw(output, RESP_ERR_LENGTH_AND_INDEXES);
      return Ok(true);
    }

    // 双域读五态：String 域命中即值 / 确证缺键 / 信封域命中（list 等对象键）
    // 报 WRONGTYPE / 磁盘候选整体降级慢路径 / 存储 IO 失败。IO 失败与缺键
    // 严禁合流——合流即伪应答空 LCS（C# storageApi.LCS 的 StringGet 异常
    // 上抛会话 catch 报错，不产出伪结果）
    enum StrRead {
      /// String 域命中
      Val(Vec<u8>),
      /// 确证缺键（两域皆缺或已过期）
      Missing,
      /// 对象键 → WRONGTYPE
      WrongType,
      /// 磁盘候选 / TTL 待异步裁决，整体降级慢路径
      Deferred,
      /// 存储 IO 失败（RESP_ERR_SLOW_PATH_STORAGE，与 exec_slow 同一口径）
      IoFail,
    }
    let read_string_val = |key| match read_user_sync(store, key, None, |v| v.to_vec()) {
      Ok(UserRead::Hit(v)) => StrRead::Val(v),
      Ok(UserRead::Missing) => StrRead::Missing,
      Ok(UserRead::WrongType) => StrRead::WrongType,
      Ok(UserRead::Deferred) => StrRead::Deferred,
      Err(_) => StrRead::IoFail,
    };

    let mut vals = [None, None];
    for (slot, key) in vals.iter_mut().zip([key1, key2]) {
      match read_string_val(key) {
        StrRead::Val(v) => *slot = Some(v),
        StrRead::Missing => {}
        StrRead::WrongType => {
          write_error_raw(output, RESP_ERR_WRONG_TYPE);
          return Ok(true);
        }
        StrRead::Deferred => return Ok(false),
        StrRead::IoFail => {
          write_error_raw(output, cs::RESP_ERR_SLOW_PATH_STORAGE);
          return Ok(true);
        }
      }
    }

    let resp3 = self.resp_protocol_version >= 3;
    match (vals[0].as_deref(), vals[1].as_deref()) {
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

#[cfg(test)]
mod tests {
  use super::*;

  #[test]
  fn test_parse_scan_filter_cursor_validation() {
    // C# TryGetLong 失败（非整数）与负值均回 invalid cursor
    assert_eq!(
      parse_scan_filter(&[b"abc"]).unwrap_err(),
      RESP_ERR_GENERIC_INVALIDCURSOR
    );
    assert_eq!(
      parse_scan_filter(&[b"-1"]).unwrap_err(),
      RESP_ERR_GENERIC_INVALIDCURSOR
    );
    assert_eq!(
      parse_scan_filter(&[b"0", b"COUNT", b"x"]).unwrap_err(),
      RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER
    );

    // 合法游标与默认值
    let res = parse_scan_filter(&[b"0"]).unwrap();
    assert_eq!(res.cursor, 0);
    assert_eq!(res.count, 10);
    assert!(res.all_keys);
  }

  #[test]
  fn test_parse_scan_filter_type_exact_forms() {
    // C# DbScan 双形态精确比对：仅全大写或全小写命中，选项名本身大小写不敏感
    let res = parse_scan_filter(&[b"0", b"type", b"zset"]).unwrap();
    assert_eq!(
      res.type_filter,
      Some(ScanTypeFilter::Object(GarnetObjectType::SortedSet))
    );
    assert!(!res.type_unknown);

    let res = parse_scan_filter(&[b"0", b"TYPE", b"LIST"]).unwrap();
    assert_eq!(
      res.type_filter,
      Some(ScanTypeFilter::Object(GarnetObjectType::List))
    );

    let res = parse_scan_filter(&[b"0", b"TyPe", b"SET"]).unwrap();
    assert_eq!(
      res.type_filter,
      Some(ScanTypeFilter::Object(GarnetObjectType::Set))
    );

    let res = parse_scan_filter(&[b"0", b"type", b"hash"]).unwrap();
    assert_eq!(
      res.type_filter,
      Some(ScanTypeFilter::Object(GarnetObjectType::Hash))
    );

    let res = parse_scan_filter(&[b"0", b"type", b"STRING"]).unwrap();
    assert_eq!(res.type_filter, Some(ScanTypeFilter::String));

    // 混合大小写不匹配 C# 双常量（SequenceEqual 全大写/全小写）→ 未知类型回空
    let res = parse_scan_filter(&[b"0", b"type", b"zSet"]).unwrap();
    assert_eq!(res.type_filter, None);
    assert!(res.type_unknown);

    let res = parse_scan_filter(&[b"0", b"type", b"HaSh"]).unwrap();
    assert_eq!(res.type_filter, None);
    assert!(res.type_unknown);

    let res = parse_scan_filter(&[b"0", b"type", b"StRiNg"]).unwrap();
    assert_eq!(res.type_filter, None);
    assert!(res.type_unknown);

    // 其它未知类型（如 stream 两形态）
    let res = parse_scan_filter(&[b"0", b"type", b"stream"]).unwrap();
    assert_eq!(res.type_filter, None);
    assert!(res.type_unknown);

    let res = parse_scan_filter(&[b"0", b"type", b"STREAM"]).unwrap();
    assert_eq!(res.type_filter, None);
    assert!(res.type_unknown);

    // TYPE 缺少参数
    assert_eq!(
      parse_scan_filter(&[b"0", b"type"]).unwrap_err(),
      RESP_ERR_GENERIC_SYNTAX_ERROR
    );
  }
}

/// 批量操作慢路径执行臂（exec_slow 冷键分派；嵌套模块保持对同步段零侵入）
///
/// 对标 libs/server/Resp/ArrayCommands.cs 的 NetworkMGET/NetworkMSET/
/// NetworkDEL 在 Tsavorite pending 读 / 环形页翻转后 CompletePending 重放
/// 的异步形态。`Err(())` 为存储 IO 失败，由 exec_slow 统一应答
/// RESP_ERR_SLOW_PATH_STORAGE
pub(crate) mod slow {
  use wresp::{
    cmd_strings::RESP_ERR_WRONG_TYPE,
    ext::RespVecExt,
    resp_memory_writer::{Resp2, Resp3, RespProtocol, RespWriter},
  };

  use crate::storage::session::storage_session::StorageSession;

  /// DEL / UNLINK 慢路径执行臂
  ///
  /// C# 无 arity 校验（0 参即空循环回 :0），1:1 保留。`delete_string` 内部
  /// `try_delete_sync` 与快路径共用同一 wkv 用户键删除单点，向量集清退随
  /// 缺席观测钩子收口，快慢两臂计数口径一致（对标 C# 主存 DELETE 单回调）
  pub(crate) async fn del(
    storage: &StorageSession<'_, impl wdev::Device>,
    refs: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    let mut deleted_count = 0_i64;
    for key in refs {
      let deleted = storage.delete_string(key).await.map_err(|_| ())?;
      deleted_count += deleted as i64;
    }
    output.write_resp_int(deleted_count);
    Ok(())
  }

  /// MGET 慢路径执行臂
  ///
  /// String 域读：命中写值；缺失写 nil（Redis MGET 对非字符串键同答 nil，
  /// 不报错；对位 C# 主存单域读）；TTL 过期键经异步读惰性清除后视同缺失
  pub(crate) async fn mget(
    storage: &StorageSession<'_, impl wdev::Device>,
    refs: &[&[u8]],
    resp_version: u8,
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    if resp_version >= 3 {
      let mut writer = RespWriter::<_, Resp3>::new_ref_p(output);
      mget_inner(storage, refs, &mut writer).await?;
    } else {
      let mut writer = RespWriter::<_, Resp2>::new_ref(output);
      mget_inner(storage, refs, &mut writer).await?;
    }
    Ok(())
  }

  async fn mget_inner<P: RespProtocol>(
    storage: &StorageSession<'_, impl wdev::Device>,
    refs: &[&[u8]],
    writer: &mut RespWriter<&mut Vec<u8>, P>,
  ) -> Result<(), ()> {
    writer.write_array_length(refs.len());
    storage
      .read_string_batch_into(refs, writer.buf_mut())
      .await
      .map_err(|_| ())
  }

  /// MSET 慢路径执行臂
  ///
  /// 批量折叠（与快路径 [`crate::resp::RespServerSession::network_mset`] 同一
  /// 折叠入口 `try_upsert_batch_sync`，快慢路径性能契约一致）：会话前缀单次
  /// 外提、借用对排序去重保末值（MSET 重复键后者胜）、单次折叠写。任一键
  /// 存储错误时已写键保持、整臂报错；同步折叠降级（环形页翻转 / TTL 清退）
  /// 时已写键保持，剩余键逐键 `upsert_string` 异步兜底重放全量（重放值与
  /// 已写键同值幂等；SET 语义自动清新键残留 TTL，与快路径同口径）。
  /// C# NetworkMSET 逐键 SET 亦无事务回滚，折叠为 transpile SKILL 批量接口
  /// 单次折叠工程准则
  pub(crate) async fn mset(
    storage: &StorageSession<'_, impl wdev::Device>,
    refs: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    // RI 键门折叠前预检（异步对偶 [`StorageSession::ri_write_gate_async`]，
    // 与快路径同判据）：任一键为存活 RangeIndex 整命令拒 WRONGTYPE，零键落库
    for chunk in refs.as_chunks::<2>().0 {
      if storage.ri_write_gate_async(chunk[0]).await.map_err(|_| ())? {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        return Ok(());
      }
    }
    let pairs = refs.as_chunks::<2>().0.iter().map(|c| (c[0], c[1]));
    match storage.batch.try_upsert_batch_sync(pairs) {
      Ok(Ok(())) => {}
      Ok(Err(_)) => {
        for [key, val] in refs.as_chunks::<2>().0 {
          storage.upsert_string(key, val).await.map_err(|_| ())?;
        }
      }
      Err(_) => return Err(()),
    }
    output.write_resp_simple_string("OK");
    Ok(())
  }
}
