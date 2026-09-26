use itoa::Buffer;
use smallvec::SmallVec;
use wbase::{
  hash_slot::slot_of,
  num::{strict_i32, strict_i64},
};
use wdev::Device;
use wresp::{
  check_args::{check_arg_count, parse_db_index_arg, unpack_args, unpack_args_rest},
  cmd_strings as cs,
  cmd_strings::{
    RESP_ERR_GENERIC, RESP_ERR_GENERIC_INVALIDCURSOR, RESP_ERR_GENERIC_SYNTAX_ERROR,
    RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, RESP_ERR_WRONG_TYPE, abort_with_error_message,
    write_error_raw,
  },
  ext::{RespVecExt, is_resp3},
  resp_memory_writer::{Resp2, Resp3, RespProtocol, RespWriter},
};
use wval::{GarnetObjectType, KeyTag};

use crate::{
  resp::{
    basic_commands::{RiWriteGate, ri_write_gate},
    resp_server_session::{MsetnxResume, RespServerSession},
    vector::vector_manager::VectorManager,
  },
  storage::session::{
    common::{
      TagRead, UserRead,
      array_key_iteration_functions::ScanTypeFilter,
      read_envelope_sync, read_tag_sync, read_tag_sync_with_prefix, read_user_sync,
      read_user_sync_with_prefix,
      ttl_sync::{meta_collection_type_of, probe_alive_with_registry},
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
  /// 回空列表 + 游标 0，ArrayKeyIterationFunctions.cs:82-84）。末值覆盖
  /// 语义：对位 C# typeParameterValue 局部直赋（ArrayCommands.cs:305-311），
  /// 每个 TYPE 词元解析前清位重判，前次非法值可被后次合法 TYPE 覆清；
  /// 空串 TYPE 归本标志系 §20 b 在册刻意偏离（见解析臂注释），边界不变
  pub type_unknown: bool,
}

/// SCAN 已知选项种类（三选项一律携一元组参数，取参与缺参语法错共用骨架）
#[derive(Clone, Copy)]
enum ScanOpt {
  Match,
  Count,
  Type,
}

/// [`ScanTypeFilter::Object`] 简构（保持比对表表项单行）
const fn obj(t: GarnetObjectType) -> ScanTypeFilter {
  ScanTypeFilter::Object(t)
}

/// SCAN TYPE 取值双形态精确比对表（C# DbScan 全大写/全小写各一，
/// ArrayKeyIterationFunctions.cs:57-76；string 的 C# 常量名 stringt 但值为 "string"）。
/// 混合大小写及表外值（含空串，§20 b 在册偏离）归未知臂回空
const SCAN_TYPES: &[(&[u8], &[u8], ScanTypeFilter)] = &[
  (b"string", b"STRING", ScanTypeFilter::String),
  (b"list", b"LIST", obj(GarnetObjectType::List)),
  (b"set", b"SET", obj(GarnetObjectType::Set)),
  (b"hash", b"HASH", obj(GarnetObjectType::Hash)),
  (b"zset", b"ZSET", obj(GarnetObjectType::SortedSet)),
];
/// 编译期自检：五类已知类型各双形态，表项数恒 5（新增类型须同步扩表）
const _: () = assert!(SCAN_TYPES.len() == 5);

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
  // C# TryGetLong 失败（非整数）与负值同回 invalid cursor，双臂折叠单帧校验
  let Some(cursor) = strict_i64(args.first().copied().unwrap_or(b"")).filter(|c| *c >= 0) else {
    return Err(RESP_ERR_GENERIC_INVALIDCURSOR);
  };

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
    // 选项名本身大小写不敏感（C# EqualsUpperCaseSpanIgnoringCase 同口径）；
    // 未知选项静默跳过（C# if/else-if 链无 else，仅消费参数名本身）
    let opt = if param.eq_ignore_ascii_case(b"MATCH") {
      ScanOpt::Match
    } else if param.eq_ignore_ascii_case(b"COUNT") {
      ScanOpt::Count
    } else if param.eq_ignore_ascii_case(b"TYPE") {
      ScanOpt::Type
    } else {
      continue;
    };
    // 已知选项一律携一元组参数：共享取参骨架，缺参回语法错
    let Some(&value) = args.get(token_idx) else {
      return Err(RESP_ERR_GENERIC_SYNTAX_ERROR);
    };
    token_idx += 1;
    match opt {
      ScanOpt::Match => {
        filter.pattern = value.to_vec();
        filter.all_keys = value == b"*";
      }
      ScanOpt::Count => {
        // 刻意偏离：C# TryGetLong 仅校验整数性，若 count<=0 直传，
        // 在底层扫描由于 acceptedCount(0) >= count(<=0) 立即返回 true，
        // 导致未扫描即中断；外层若无键匹配（keys.Count == 0）会硬写游标 0，
        // 此处修复性规整，负/零钳 0，并在扫描层 max(1) 保证至少扫描 1 条。
        // 已登记 doc/zh/deviations.md 第 20 条 a，勿按 C# 改回。
        let Some(n) = strict_i64(value) else {
          return Err(RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        };
        filter.count = n.max(0) as usize;
      }
      ScanOpt::Type => {
        filter.type_given = true;
        // 末值覆盖（对位 C# ArrayCommands.cs:305-311：typeParameterValue 为
        // 局部 ReadOnlySpan 直赋，每个 TYPE 词元整体覆盖前值，不留标志位）：
        // 解析本词元类型值前先清未知标志，前次非法 TYPE 不粘滞，判型只取
        // 末次词元（末值合法覆清、末值非法由下方未知臂照常置位）
        filter.type_unknown = false;
        // 双形态精确比对走 const 表 [`SCAN_TYPES`]；未知类型（含混合大小写、
        // stream、空串等表外值）落 None：C# DbScan 对非空未知 typeObject 回空
        // 列表 + 游标 0。刻意偏离：C# 空串 TYPE 会透传并忽略类型过滤，此处统一
        // 视为空集。已登记 doc/zh/deviations.md 第 20 条 b，勿按 C# 改回。
        filter.type_filter = SCAN_TYPES
          .iter()
          .find(|(lo, up, _)| value == *lo || value == *up)
          .map(|(_, _, f)| *f);
        filter.type_unknown = filter.type_filter.is_none();
      }
    }
  }
  Ok(filter)
}

/// LCS 选项（C# NetworkLCS 局部变量组的结构化承接）
///
/// 快路径校验段与慢路径执行段共用同一解析单源 [`parse_lcs_options`]
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct LcsOptions {
  /// `LEN`（只回长度）
  pub len_only: bool,
  /// `IDX`（回匹配段索引矩阵）
  pub with_idx: bool,
  /// `MINMATCHLEN`（段长下限；C# TryGetInt 有符号，负值钳 0）
  pub min_match_len: usize,
  /// `WITHMATCHLEN`（IDX 矩阵每段附段长）
  pub with_match_len: bool,
}

/// 解析 LCS 选项（LEN / IDX / MINMATCHLEN <n> / WITHMATCHLEN）
///
/// 校验口径 1:1 对标 C# NetworkLCS：选项名大小写不敏感；未知选项与
/// MINMATCHLEN 缺参回语法错；MINMATCHLEN 非整数回 value-not-integer；
/// LEN 与 IDX 互斥（[`RESP_ERR_LENGTH_AND_INDEXES`]）
pub(crate) fn parse_lcs_options(rest: &[&[u8]]) -> Result<LcsOptions, &'static str> {
  let mut opts = LcsOptions::default();
  let mut idx = 0;
  while idx < rest.len() {
    let token = rest[idx];
    if token.eq_ignore_ascii_case(b"LEN") {
      opts.len_only = true;
    } else if token.eq_ignore_ascii_case(b"IDX") {
      opts.with_idx = true;
    } else if token.eq_ignore_ascii_case(b"MINMATCHLEN") {
      idx += 1;
      if idx >= rest.len() {
        return Err(cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
      }
      // C# TryGetInt（有符号）：负值钳 0 而非报错；前导零拒收系 rust 严格收口，见 doc/zh/deviations.md §32
      let Some(min_len) = strict_i32(rest[idx]) else {
        return Err(cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
      };
      opts.min_match_len = min_len.max(0) as usize;
    } else if token.eq_ignore_ascii_case(b"WITHMATCHLEN") {
      opts.with_match_len = true;
    } else {
      return Err(cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
    }
    idx += 1;
  }
  if opts.len_only && opts.with_idx {
    return Err(RESP_ERR_LENGTH_AND_INDEXES);
  }
  Ok(opts)
}

/// LCS 应答整形单点（[`RespServerSession::network_lcs`] 快路径与
/// [`slow::lcs`] 慢路径共漏斗，LEN/IDX/默认三形态与缺键空帧逐字节一致）
fn write_lcs_output<D: Device>(
  v1: Option<&[u8]>,
  v2: Option<&[u8]>,
  opts: &LcsOptions,
  resp3: bool,
  output: &mut Vec<u8>,
) {
  match (v1, v2) {
    (Some(v1), Some(v2)) => {
      if opts.len_only {
        let len = StorageSession::<D>::compute_lcs_length(v1, v2, opts.min_match_len);
        output.write_resp_int(len as i64);
      } else if opts.with_idx {
        let (total, matches) =
          StorageSession::<D>::compute_lcs_with_indices(v1, v2, opts.min_match_len);
        StorageSession::<D>::write_lcs_matches(&matches, opts.with_match_len, total, output, resp3);
      } else {
        let lcs = StorageSession::<D>::compute_lcs(v1, v2, opts.min_match_len);
        output.write_resp_bulk_string(&lcs);
      }
    }
    _ => {
      if opts.len_only {
        output.write_resp_int(0);
      } else if opts.with_idx {
        StorageSession::<D>::write_lcs_matches(&[], opts.with_match_len, 0, output, resp3);
      } else {
        output.write_resp_bulk_string(b"");
      }
    }
  }
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
  /// libs/server/API/GarnetApiUnifiedCommands.cs:DELETE
  ///（C# NetworkDEL 逐键循环调 `api.DELETE(key)` 单键删除 API，rust 无该
  /// API 包装层，循环内删除原语与单键 DELETE 语义折叠于本函数删除臂）
  ///
  /// C# 无 arity 校验（0 参即空循环回 :0），1:1 保留。向量集清退下沉至 wkv
  /// 用户键删除单点（双域判未命中后经 [`crate::storage::session::storage_session::vector_registry_delete_hook`]
  /// 摘除登记表项，对标 C# MainStore RemoveKey 回调 → VectorManager.RequestDeletion，
  /// GarnetRecordTriggers.OnDispose 的 Deleted 臂），本层不再另配第二套清退判据。
  pub fn network_del<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    self.del_deleted_count = 0;
    // 循环前缀外提（transpile SKILL 工程准则；rust 工程优化无 c# 对应）：
    // 单命令执行窗口内 ns/db 原子变量不可变（RESP 命令原子性，批内 SELECT
    // 不可能插入 DEL 循环），循环零前缀重读与 Varint 重算
    let prefix = store.session_prefix();
    let prefix_slice = prefix.as_slice();
    let mut deleted_count = 0i64;
    for key in parse_state {
      // 逐键短窗（票 zcode-r32-rmwmatrix 立项一：对标 C# InternalDelete.cs:60
      // FindTagAndTryEphemeralXLock——C# NetworkDEL 逐键 api.DELETE 每键独立
      // 进记录闩域，rust 逐键取窗即该形态 1:1 对位；盲删落在他者读算写间隙
      // 即「DEL :1 而键以新值复活」非可串行化，RENAME 双窗亦凭同址桶闩互斥）。
      // 失闩沿既有 Ok(false) 降级慢路径：保留快路径已删除键计数（沿 MSETNX
      // resume 尾参先例），慢路径继承计数续传
      let Some(_window) = store.try_rmw_window(key) else {
        self.del_deleted_count = deleted_count;
        return Ok(false);
      };
      // 单点删除：返回值已含登记表缺席收口（向量集键命中即 true，计 1）
      let deleted = match store.try_delete_sync_with_prefix(prefix_slice, key) {
        Ok(Ok(deleted)) => deleted,
        // 环形页翻转 / 复合对象元数据：须降级完整异步路由，本次不产生输出；
        // 保留快路径已删除键计数（沿 MSETNX resume 尾参先例），慢路径继承计数续传
        Ok(Err(_)) => {
          self.del_deleted_count = deleted_count;
          return Ok(false);
        }
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
  pub fn network_mget<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    if is_resp3(self.resp_protocol_version) {
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
  fn do_network_mget<P: RespProtocol, D: Device>(
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
        // 对齐 C# MGetReadArgBatch.SetStatus（非 Found 即计入 notfound）：
        // 信封域命中（WrongType）与缺键计入 notfound；MGET 对对象键同答
        // nil 不报错
        Ok(UserRead::WrongType) => {
          notfound += 1;
          writer.write_null();
        }
        Ok(UserRead::Missing) => {
          notfound += 1;
          writer.write_null();
        }
        Ok(UserRead::Deferred) => {
          writer.buf_mut().truncate(start_len);
          return Ok(false);
        }
        // 存储错误不得伪装键缺席（C# 磁盘收割异常上抛掐断连接，绝无
        // 「nil 混入数组」形态）：整命令回滚至数组头前换错误帧，逐键
        // found/notfound 一并不入账（与 Deferred 回滚同机制）；错误帧走
        // [`RespVecExt::write_resp_error`] 单点，与 GET 同线面字节
        Err(_) => {
          let buf = writer.buf_mut();
          buf.truncate(start_len);
          buf.write_resp_error(RESP_ERR_GENERIC);
          return Ok(true);
        }
      }
    }
    if let Some(metrics) = metrics {
      metrics.incr_total_found(found);
      metrics.incr_total_notfound(notfound);
    }
    Ok(true)
  }

  /// libs/server/Resp/ArrayCommands.cs:NetworkMSET
  pub fn network_mset<'a, D: Device>(
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

    // 全键读改写窗口（票 zcode-r32-rmwmatrix 立项一：对标 C# MainStoreOps.cs
    // MSET_Conditional 全键排他锁内批量 SET——折叠先例注释自引的「全键锁内」
    // 即本窗，判定与批量写收敛一键组闩域；沿 network_msetnx 同款
    // [`wkv::BatchStoreSession::try_rmw_window_sorted`] 桶序取闩，哈希升序
    // 全序定序与他命令键组闩交叉无循环等待面）。失闩沿 Ok(false) 降级慢路径
    // 同序持窗重放。取窗先于预检（票 zcode-r141c-msetbig 案二）：窗外预检与
    // 本核取窗之间他核会话可持同键闩完成集合写/promote 落下 RangeIndex 元
    // 记录，批内核 Meta 在场探针只降级不裁决，降级前缀键已提交、慢臂窗内门
    // 改答 WRONGTYPE——半提交假拒击穿「零键落库」契约；全仓字符串写臂皆「先
    // 取窗、窗内门、后写」（SET 共同体 apply_set_with_expiry 契约同款），
    // 本快臂序反系孤例，就此收口
    // 键值对视图（取窗/预检/批量写三处共用，免逐处重展 as_chunks）
    let chunks = parse_state.as_chunks::<2>().0;
    let Some(_windows) = store.try_rmw_window_sorted(chunks.iter().map(|c| c[0])) else {
      return Ok(false);
    };

    // RI 键门窗内预检（复用 ri_write_gate 单点，判据不另起第二套）：任一键
    // 为存活 RangeIndex 整命令拒 WRONGTYPE——窗内预检、窗内裁决，预检先于任
    // 何写入，零键落库无半提交；Deferred（元记录有磁盘候选）弃窗沿用既有
    // 出口整体降级慢路径，由慢臂持窗后异步对偶门复裁决闭环
    for chunk in chunks {
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
    let pairs = chunks.iter().map(|c| (c[0], c[1]));
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
  /// EXISTS 判定 + 锁内批量 SET + Commit）：判定与批量写收敛进全键桶序
  /// 读改写窗口（[`wkv::BatchStoreSession::try_rmw_window_sorted`]），与 C#
  /// 全键排他事务锁同形——thread-per-core 多 worker 跨核并发下同步段无
  /// await 只保证单 worker 内不可分割，跨 worker 插入窗口由键组闩封堵。
  /// 任一环节须异步闭环时整体降级慢路径（[`Self::msetnx_resume`] 携带续跑
  /// 模式），绝不以半提交状态应答。
  pub fn network_msetnx<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    vector: Option<&VectorManager>,
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
    // 键值对视图（取窗/判定/落笔三循环共用，免逐处重展 as_chunks）
    let chunks = parse_state.as_chunks::<2>().0;

    // 全键读改写窗口（票 zcode-r15-generic 发现一，对标 C# MainStoreOps.cs:349
    // MSET_Conditional 全键排他锁内「EXISTS 判定 + 锁内批量 SET + Commit」：
    // 判定与写入一体）：桶序取闩（哈希升序定序，与他命令键组闩交叉无循环
    // 等待面），闩内完成判定与批量写全序列，杜绝「逐键判定通过后、批量写
    // 落库前」他 worker 会话 SET 插入的全有或全无契约破坏。失闩沿既有
    // Ok(false) 降级慢路径同序持窗重放，绝不自旋等闩
    let Some(_windows) = store.try_rmw_window_sorted(chunks.iter().map(|c| c[0])) else {
      return Ok(false);
    };

    // 检查是否有任何键已存在（闩窗内折叠存活探针单源：三域 + 向量登记表
    // 第四态，对象信封与升阶键 Meta 同计存在，C# NX 语义；对标 C# Reader
    // 主存单记录——向量索引与 String 同槽同探针，MainStoreOps.cs:375 EXISTS
    // 对存活向量记录恒判在）。票 zcode-r161c-msetnx 案一：NX 存在性判定
    // 唯一收口于本窗内折叠，派发层不再另出终态应答。降级（Ok(None)：
    // 磁盘候选 / TTL 待裁决）发生时尚未写入任何键，整体移交慢路径完整
    // 裁决，安全重放
    self.msetnx_resume = MsetnxResume::Replay;
    for chunk in chunks {
      match probe_alive_with_registry(store, prefix_slice, chunk[0], vector) {
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

    // 判定段放行后逐键 upsert 落笔（SET 语义覆写，对标 C# MSET_Conditional
    // 判后锁内无条件 SET 循环）：NX 判定的唯一仲裁源=闩窗内存活探针判定段，
    // 物理在场不作第二判据（票 zcode-r141c-msetbig 案一，与 SETNX 窗内探针
    // 判死直 upsert 同型；共守「闩窗内存活探针=唯一 NX 判据」纪律，与
    // ing/zcode-r131c-dumprest 案一 RESTORE 位互见勿起第三形）——闩窗全程
    // 持有，探针判死的过期残留记录由 upsert 原位覆写并自带旧 TTL 清退自愈，
    // 绝不再回假 :0。重复键「written 复写」分支与条件回滚面随之仅保留存储
    // 错误 Err 臂；回滚比对基准随末次写入值前移，禁残留中间值（票
    // zcode-r37-lockfix 发现 B：回滚禁裸删吞并发已确认写——已写键逐键重读
    // String 域，内容即本命令所写才删）。返回是否存在删除降级（环形页翻转）
    // 残留，调用方据此置 Rollback 态降级慢路径持窗收尾，杜绝「已写键残留而
    // 应答失败」的原子性破面（C# MSET_Conditional 全键锁内折叠无回滚形态，
    // 本臂为 rust 快慢路径分工的自有收口）
    let mut written: smallvec::SmallVec<[(&[u8], &[u8]); 8]> =
      SmallVec::with_capacity(chunks.len());
    // 回滚已写键：入窗复验待删内容确系本次所写再删，避免吞掉并发盲写 SET 值；
    // 妥善处理删除错误与降级（降级置 MsetnxResume::Rollback 转慢路径）
    let rollback = |written: &[(&[u8], &[u8])]| -> bool {
      let mut degraded = false;
      for (k, v) in written {
        let is_our_write = match read_tag_sync_with_prefix(
          store,
          prefix_slice,
          k,
          KeyTag::String,
          |cur| cur == *v,
        ) {
          Ok(TagRead::Hit(matches)) => matches,
          Ok(TagRead::Missing) => false,
          Ok(TagRead::Deferred) => true,
          Err(_) => false,
        };
        if is_our_write {
          match store.try_delete_sync_with_prefix(prefix_slice, k) {
            Ok(Ok(_)) => {}
            Ok(Err(_)) => degraded = true,
            Err(_) => {}
          }
        }
      }
      degraded
    };

    for chunk in chunks {
      let key = chunk[0];
      let val = chunk[1];
      if let Some(pos) = written.iter_mut().find(|(wk, _)| *wk == key) {
        pos.1 = val;
      } else {
        written.push((key, val));
      }
      match store.try_upsert_sync(key, val) {
        Ok(Ok(_)) => {}
        // 环形页翻转/异步闭环信号（非错误）：判定已整体通过、已写键
        // 保持，置 Continue 交慢路径跳过判定续写全部键值（upsert 同值幂等）
        Ok(Err(_)) => {
          self.msetnx_resume = MsetnxResume::Continue;
          return Ok(false);
        }
        Err(_) => {
          if rollback(&written) {
            self.msetnx_resume = MsetnxResume::Rollback;
            return Ok(false);
          }
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
  /// C# 校验序：arity → 整数 → MaxDatabases 上界 → 切库（TrySwitchActiveDatabaseSession）。
  /// 切库经 [`wkv::StoreSession::set_active_db`] 原子改写会话前缀：批处理
  /// 物理键编码前缀按命令边界重算（批量命令在单命令窗口内一次外提，
  /// 见 [`Self::network_mget`]/[`Self::network_mset`]/[`Self::network_del`]），
  /// 纪元守卫仅保护内存直读，切库无 NewEpoch 交叉，安全。
  pub fn network_select<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some([index_raw]) = unpack_args(parse_state, output, "SELECT") else {
      return Ok(true);
    };

    // 线面按 C# int32 档收口，内部库 ID 仍 u64（[`parse_db_index_arg`]）
    let Some(index) =
      parse_db_index_arg(index_raw, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER, output)
    else {
      return Ok(true);
    };
    if !self.try_switch_active_database_session(index) {
      abort_with_error_message(output, cs::RESP_ERR_DB_INDEX_OUT_OF_RANGE);
      return Ok(true);
    }
    // 冷库挂起面：切库报告映射未装载时挂起磁盘点查装载，+OK 交由 SlowWait
    // 闭环（装载完成并重放切库后原样应答并物化标量，后续命令消费到的一定是
    // 装载后上下文；装载失败即弃暂存，active_db_id 保持旧库零撕裂）
    if let Some((ns, db)) = self.cold_pending_ctx() {
      // 事务窗围栏文案按 SELECT 域传入（deviations §58b 事务窗零停泊红线，
      // 同库排队准入后重放撞冷库窄窗即回 SELECT_IN_TXN 族帧）
      self.park_cold_context_load(
        store.session.store(),
        ns,
        db,
        cs::RESP_OK.to_vec(),
        output,
        cs::RESP_ERR_SELECT_IN_TXN_UNSUPPORTED,
      );
      return Ok(true);
    }
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

    // 逐库短路解析（[`parse_db_index_arg`]）：超档字面量走 NotInteger，
    // 按 C# NetworkSWAPDB 各回 invalid first/second DB index 档
    let Some(idx1) = parse_db_index_arg(idx1_raw, cs::RESP_ERR_INVALID_FIRST_DB_INDEX, output)
    else {
      return Ok(true);
    };
    let Some(idx2) = parse_db_index_arg(idx2_raw, cs::RESP_ERR_INVALID_SECOND_DB_INDEX, output)
    else {
      return Ok(true);
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
  /// libs/server/API/GarnetApiUnifiedCommands.cs:TYPE
  ///（C# NetworkTYPE 调 `api.TYPE(key)` → storageSession.Read_UnifiedStore
  ///（HandleType 判定内核）；rust 无该 API 包装层，判定与应答折叠于本函数）
  pub fn network_type<'a, D: Device>(
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
    //（[`envelope_object_type_name`]）；三域皆缺 → none；任一域存储错误独立
    // 回错误帧（C# IO 异常沿调用栈上抛断连，status 枚举域只有 NOTFOUND/
    // WRONGTYPE 绝无 IO 错误折 none——与 GET/STRLEN 同口径 RESP_ERR_GENERIC，
    // 快慢路径与同域命令三态互斥）
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
          Ok(TagRead::Hit(None)) | Ok(TagRead::Missing) => {
            match read_envelope_sync(store, key, |raw| {
              raw.first().copied().and_then(envelope_object_type_name)
            }) {
              // 信封域命中已由双探确认；内层标签缺省兜底 none；磁盘候选降级
              Ok(TagRead::Hit(Some(name))) => {
                output.write_resp_simple_string(name);
              }
              Ok(TagRead::Hit(None)) | Ok(TagRead::Missing) => {
                output.write_resp_simple_string("none");
              }
              Ok(TagRead::Deferred) => return Ok(false),
              // 信封域存储错误不得伪装 none（对齐 slow::type_cmd Err 上抛形态）
              Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
            }
          }
          // Meta 域存储错误不得伪装 none / 不得借信封域兜底吞错
          Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
        }
      }
      Ok(UserRead::Missing) => {
        output.write_resp_simple_string("none");
      }
      // String 域存储错误不得伪装「键不存在」（与 GET 同口径）
      Err(_) => output.write_resp_error(RESP_ERR_GENERIC),
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
  pub fn network_lcs<'a, D: Device>(
    &mut self,
    parse_state: &[&[u8]],
    store: &wkv::BatchStoreSession<'a, D>,
    output: &mut Vec<u8>,
  ) -> wresp::Result<bool> {
    let Some(([key1, key2], rest)) = unpack_args_rest(parse_state, output, "LCS") else {
      return Ok(true);
    };
    // 选项解析单源（慢路径执行臂共用 [`parse_lcs_options`]）
    let opts = match parse_lcs_options(rest) {
      Ok(opts) => opts,
      Err(err) => {
        write_error_raw(output, err);
        return Ok(true);
      }
    };

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

    write_lcs_output::<D>(
      vals[0].as_deref(),
      vals[1].as_deref(),
      &opts,
      is_resp3(self.resp_protocol_version),
      output,
    );
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

  /// 多 TYPE 词元末值覆盖（票 zcode-r161c-scantype §五）：C#
  /// typeParameterValue 局部直赋末值覆盖（ArrayCommands.cs:305-311），
  /// DbScan 仅依据末值判型（ArrayKeyIterationFunctions.cs:57-86），
  /// 前次非法 TYPE 置位的 type_unknown 不得粘滞吞掉末次合法值
  #[test]
  fn parse_scan_filter_duplicate_type_last_valid_overrides() {
    // 末次合法：前次 stream 的粘滞标志被覆清，判型取末值 hash
    let res = parse_scan_filter(&[b"0", b"type", b"stream", b"type", b"hash"]).unwrap();
    assert_eq!(
      res.type_filter,
      Some(ScanTypeFilter::Object(GarnetObjectType::Hash))
    );
    assert!(!res.type_unknown);
    assert!(res.type_given);

    // 末次非法：末值覆盖照常生效——type_filter 覆为 None、标志置位，
    // 与单形非法 TYPE 同口径（慢路径早退回空列表 + 游标 0）
    let res = parse_scan_filter(&[b"0", b"type", b"hash", b"type", b"stream"]).unwrap();
    assert_eq!(res.type_filter, None);
    assert!(res.type_unknown);

    // §20 b 边界不动：空串 TYPE 单形仍归 type_unknown（在册刻意偏离，
    // C# 空串透传忽略过滤）；空串后被合法 TYPE 覆盖时末值语义照常覆清
    let res = parse_scan_filter(&[b"0", b"type", b""]).unwrap();
    assert!(res.type_unknown);
    let res = parse_scan_filter(&[b"0", b"type", b"", b"type", b"zset"]).unwrap();
    assert_eq!(
      res.type_filter,
      Some(ScanTypeFilter::Object(GarnetObjectType::SortedSet))
    );
    assert!(!res.type_unknown);
  }
}

/// 批量操作慢路径执行臂（exec_slow 冷键分派；嵌套模块保持对同步段零侵入）
///
/// 对标 libs/server/Resp/ArrayCommands.cs 的 NetworkMGET/NetworkMSET/
/// NetworkDEL 在 Tsavorite pending 读 / 环形页翻转后 CompletePending 重放
/// 的异步形态。`Err(())` 为存储 IO 失败，由 exec_slow 统一应答
/// RESP_ERR_SLOW_PATH_STORAGE
pub(crate) mod slow {
  use wdev::Device;
  use wresp::{
    check_args::unpack_args_rest,
    cmd_strings::{RESP_ERR_WRONG_TYPE, write_error_raw},
    ext::{RespVecExt, is_resp3},
    resp_memory_writer::{Resp2, Resp3, RespProtocol, RespWriter},
  };
  use wval::KeyTag;

  use super::{envelope_object_type_name, parse_lcs_options, write_lcs_output};
  use crate::{
    resp::{basic_commands::slow::clear_vector_registry, vector::vector_manager::VectorManager},
    storage::session::{
      common::{UserReadAsync, ttl_sync::meta_collection_type_of},
      storage_session::StorageSession,
    },
  };

  /// DEL / UNLINK 慢路径执行臂
  ///
  /// C# 无 arity 校验（0 参即空循环回 :0），1:1 保留。`delete_string` 内部
  /// `try_delete_sync` 与快路径共用同一 wkv 用户键删除单点，向量集清退随
  /// 缺席观测钩子收口，快慢两臂计数口径一致（对标 C# 主存 DELETE 单回调）。
  /// `initial_count` 继承快路径降级前已物理删除的键计数（SlowWait 快照尾参带入），
  /// 快路径已删键在重放中得 false（计数 +0），未删键/降级键在慢路径正常删除计数，
  /// 确保整命令应答准确等于实际删除键总数
  pub(crate) async fn del(
    storage: &StorageSession<'_, impl Device>,
    refs: &[&[u8]],
    initial_count: i64,
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    let mut deleted_count = initial_count;
    for key in refs {
      // 逐键短窗（快路径 network_del 同一窗口契约：对标 C# InternalDelete.cs:60
      // 逐键记录闩；本臂让核等闩，预算耗尽按本臂存储错误应答）
      let _window = storage.batch.rmw_window(key).await.map_err(|_| ())?;
      let deleted = storage.delete_string(key).await.map_err(|_| ())?;
      deleted_count += i64::from(deleted);
    }
    output.write_resp_int(deleted_count);
    Ok(())
  }

  /// MGET 慢路径执行臂
  ///
  /// String 域读：命中写值；缺失写 nil（Redis MGET 对非字符串键同答 nil，
  /// 不报错；对位 C# 主存单域读）；TTL 过期键经异步读惰性清除后视同缺失
  pub(crate) async fn mget(
    storage: &StorageSession<'_, impl Device>,
    refs: &[&[u8]],
    resp_version: u8,
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    if is_resp3(resp_version) {
      let mut writer = RespWriter::<_, Resp3>::new_ref_p(output);
      mget_inner(storage, refs, &mut writer).await?;
    } else {
      let mut writer = RespWriter::<_, Resp2>::new_ref(output);
      mget_inner(storage, refs, &mut writer).await?;
    }
    Ok(())
  }

  /// 数组头先行、批量流式补元素（对标 C# NetworkMGET 先 `TryWriteArrayLength`
  /// 再 `ReadWithPrefetch` + `CompletePending` 顺序补写应答）
  ///
  /// 批量读口以 `Err` 中止时，本臂写入的数组头与首个磁盘候选之前已先行交付的
  /// 元素一并回滚（wkv `session/raw/batch.rs:read_batch_raw_with` 次序契约：部
  /// 分结果不可回滚、调用方须整体丢弃），错误交由 `exec_slow_impl` 的
  /// `C::Mget` 臂在干净的 output 上落单条完整错误帧。
  /// 回滚形态与快路径 `do_network_mget` 的 `Deferred` 出口同一机制
  ///（`writer.len()` 记点 + `buf_mut().truncate`）。
  async fn mget_inner<P: RespProtocol>(
    storage: &StorageSession<'_, impl Device>,
    refs: &[&[u8]],
    writer: &mut RespWriter<&mut Vec<u8>, P>,
  ) -> Result<(), ()> {
    let start_len = writer.len();
    writer.write_array_length(refs.len());
    if storage
      .read_string_batch_into(refs, writer.buf_mut())
      .await
      .is_err()
    {
      writer.buf_mut().truncate(start_len);
      return Err(());
    }
    Ok(())
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
  /// 单次折叠工程准则。
  ///
  /// 向量登记清退（票 zcode-r163c-setguard 案一）：`set_vector_guard` 命中
  /// Degrade 至此臂后，取窗成功且 RI 门全数通过时逐键复用
  /// [`clear_vector_registry`] 单源摘除登记表（对标 C# ArrayCommands.cs:59-63
  /// 排他锁内 DELETE+SET 重投——清退与覆写同一临界区），落毕键恒 string 域、
  /// 登记零残骸；失败 arity / RI 拒 / 失窗早退于清退之前，零副作用契约不变
  pub(crate) async fn mset(
    storage: &StorageSession<'_, impl Device>,
    vector: Option<&VectorManager>,
    refs: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    // 键值对视图（取窗/预检/清退/折叠/兜底重放共用，免逐处重展 as_chunks）
    let chunks = refs.as_chunks::<2>().0;
    // RI 键门折叠前预检（异步对偶 [`StorageSession::ri_write_gate`]，
    // 与快路径同判据）：任一键为存活 RangeIndex 整命令拒 WRONGTYPE，零键落库
    // 全键读改写窗口（快路径 network_mset 同一窗口契约：对标 C#
    // MSET_Conditional 全键排他锁；桶序取闩无循环等待面，闩内完成折叠批写，
    // 杜绝批写跨键交叠他 worker 读算写间隙）。失闩预算耗尽按本臂存储错误应答
    let Ok(_windows) = storage
      .batch
      .rmw_window_sorted(chunks.iter().map(|c| c[0]))
      .await
    else {
      return Err(());
    };
    for chunk in chunks {
      if storage.ri_write_gate(chunk[0]).await.map_err(|_| ())? {
        output.write_resp_error(RESP_ERR_WRONG_TYPE);
        return Ok(());
      }
    }
    // 窗内逐键清退登记表（持窗临界区内、批量落笔之前；判据与清退全走既有
    // 单源壳，未命中零操作幂等），杜绝已答 +OK 的覆写留下双域幽灵残骸
    for chunk in chunks {
      clear_vector_registry(storage, vector, chunk[0]).await;
    }
    let pairs = chunks.iter().map(|c| (c[0], c[1]));
    match storage.batch.try_upsert_batch_sync(pairs) {
      Ok(Ok(())) => {}
      Ok(Err(_)) => {
        for [key, val] in chunks {
          storage.upsert_string(key, val).await.map_err(|_| ())?;
        }
      }
      Err(_) => return Err(()),
    }
    output.write_resp_simple_string("OK");
    Ok(())
  }

  /// TYPE 慢路径执行臂
  ///
  /// C# NetworkTYPE → storageApi.TYPE → Read_UnifiedStore：单次读 pending 就地
  /// CompletePendingForUnifiedStoreSession 闭环，终态恒类型名或 none。rust 慢
  /// 路径对偶：磁盘候选在异步读内闭环（无降级态），三域判型与应答形态同快路径
  /// [`crate::resp::RespServerSession::network_type`] 逐字节一致（vector 登记特
  /// 判前置，String / 升阶 Meta / 对象信封三域，内层标签缺省兜底 none）
  pub(crate) async fn type_cmd(
    storage: &StorageSession<'_, impl Device>,
    vector: Option<&VectorManager>,
    refs: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    let Some([key]) = <[&[u8]; 1]>::try_from(refs).ok() else {
      // 可达性防御：分派前快路径 unpack_args 已校验 arity，降级快照不失真
      return Err(());
    };
    // vector 登记特判前置（登记表纯内存，无降级面，与快路径同单点）
    let prefix = storage.batch.session_prefix();
    if vector.is_some_and(|vm| vm.read_stored_index(prefix.as_slice(), key).is_some()) {
      output.write_resp_simple_string("vectorset");
      return Ok(());
    }
    // 三域异步判型（String 域命中 → string；升阶键 Meta 元记录带集合类型直读
    // 类型名；信封域按内层标签映射含扩展注册名；三域皆缺 / 已过期 → none）
    match storage.read_user(key, |_| ()).await {
      Ok(UserReadAsync::Hit(())) => output.write_resp_simple_string("string"),
      Ok(UserReadAsync::Missing) => output.write_resp_simple_string("none"),
      Ok(UserReadAsync::WrongType) => {
        match storage
          .read_tag_with(key, KeyTag::Meta, meta_collection_type_of)
          .await
        {
          Ok(Some(Some(obj_type))) => output.write_resp_simple_string(obj_type.as_str()),
          // Meta 域缺失/死记录：落信封域读（对象信封键口径，含扩展注册名）；
          // 信封命中已由双探确认，内层标签缺省兜底 none
          Ok(_) => match storage
            .read_tag_with(key, KeyTag::ObjectEnvelope, |raw| {
              raw.first().copied().and_then(envelope_object_type_name)
            })
            .await
          {
            Ok(Some(Some(name))) => output.write_resp_simple_string(name),
            Ok(_) => output.write_resp_simple_string("none"),
            Err(_) => return Err(()),
          },
          Err(_) => return Err(()),
        }
      }
      Err(_) => return Err(()),
    }
    Ok(())
  }

  /// LCS 慢路径执行臂
  ///
  /// C# NetworkLCS → storageApi.LCS → MainStoreOps.LCS/LCSInternal：双键单次读
  /// pending 就地闭环，终态恒结果帧或 WRONGTYPE。rust 慢路径对偶：双键异步读
  /// 后复用既有纯函数 [`StorageSession::compute_lcs_length`] /
  /// [`StorageSession::compute_lcs_with_indices`] / [`StorageSession::compute_lcs`]
  /// （零新机制），选项解析与快路径同一单源 [`parse_lcs_options`]，应答同快路径
  /// [`crate::resp::RespServerSession::network_lcs`] 逐字节一致（含 WRONGTYPE /
  /// 缺键空帧 / LEN / IDX 形态）。IO 失败与缺键严禁合流——合流即伪应答空 LCS
  pub(crate) async fn lcs<D: Device>(
    storage: &StorageSession<'_, D>,
    refs: &[&[u8]],
    resp_version: u8,
    output: &mut Vec<u8>,
  ) -> Result<(), ()> {
    let Some(([key1, key2], rest)) = unpack_args_rest(refs, output, "LCS") else {
      return Ok(());
    };
    let opts = match parse_lcs_options(rest) {
      Ok(opts) => opts,
      Err(err) => {
        write_error_raw(output, err);
        return Ok(());
      }
    };
    // 双键异步读：String 域命中即值 / 确证缺键（两域皆缺或已过期，TTL 过期键经
    // 异步读惰性清除后视同缺失）/ 对象键 → WRONGTYPE / 存储 IO 失败（与 exec_slow
    // RESP_ERR_SLOW_PATH_STORAGE 同一口径）
    let mut vals = [None, None];
    for (slot, key) in vals.iter_mut().zip([key1, key2]) {
      match storage.read_user(key, |v| v.to_vec()).await {
        Ok(UserReadAsync::Hit(v)) => *slot = Some(v),
        Ok(UserReadAsync::Missing) => {}
        Ok(UserReadAsync::WrongType) => {
          write_error_raw(output, RESP_ERR_WRONG_TYPE);
          return Ok(());
        }
        Err(_) => return Err(()),
      }
    }

    write_lcs_output::<D>(
      vals[0].as_deref(),
      vals[1].as_deref(),
      &opts,
      is_resp3(resp_version),
      output,
    );
    Ok(())
  }
}
