//! 有序集合 RESP 命令（对标 libs/server/Resp/Objects/SortedSetCommands.cs）
//!
//! 命令层只做参数校验与编解码：语义全部下沉到
//! [`wcol::zset::sorted_set_object::SortedSetObject`] 的
//! operate 直收切片通道（与 C# GarnetObjectBase.Operate 分层一致），
//! 存取经与 storage 会话域共享的 `[类型标签][载荷]` 信封
//! （见 [`crate::resp::objects::object_store_utils`]）。
//!
//! 目录化拆分：
//! - [`read`]：ZRANGE, ZRANK, ZSCORE, ZCOUNT, ZLEXCOUNT, ZMSCORE, ZCARD, ZSCAN 等读命令；
//! - [`write`]：ZADD, ZINCRBY, ZREM, ZREMRANGEBYSCORE, ZREMRANGEBYRANK, ZREMRANGEBYLEX,
//!   ZDIFF, ZINTER, ZUNION, ZRANDMEMBER 等写与集合运算命令；
//! - [`blocking`]：BZPOPMIN, BZPOPMAX, BZMPOP 等阻塞与弹出命令；
//! - [`slow`]：慢路径执行臂。

// ============ 族内收尾宏单源（read / write 子域共用，须先于 mod 声明定义） ============

macro_rules! zset_load_or_bail {
  ($store:expr, $key:expr, $output:expr, $missing:expr) => {
    $crate::obj_load_or_bail!(zset_load_sync, $store, $key, $output, $missing)
  };
}

macro_rules! zset_windowed_load {
  ($store:expr, $key:expr, $output:expr, mut $name:ident, $missing:expr) => {
    $crate::obj_windowed_load!(zset_load_sync, $store, $key, $output, mut $name, $missing);
  };
}

macro_rules! load_many_or_bail {
  ($store:expr, $keys:expr, $output:expr) => {
    $crate::load_many_or_bail!($store, $keys, $output)
  };
}

/// 只读 rmw 命令通道收尾单源：键取 parse_state[0]、余参原样交 operate，
/// 装载 → operate → 回写判定全在 `zset_rmw` 内闭环（零变更只读操作经
/// should_write_back 的 is_read_only 臂不落库），仅降级臂转 `Ok(false)`
/// 异步重放，其余分支应答已完整
macro_rules! zset_rmw_read_or_bail {
  ($self:expr, $store:expr, $op:expr, $parse_state:expr, $output:expr) => {{
    let key = $parse_state[0];
    match $self.zset_rmw($store, key, $op, &$parse_state[1..], (0, 0), $output) {
      Rmw::Degrade => Ok(false),
      // AofFail 防御臂（读骨架不可达，写面可达处已逐个显式收口）：与存储
      // 硬失败同帧闭环，禁 `_` 吞态冒答成功（AofEnqueue 契约）
      Rmw::AofFail => {
        $crate::resp::objects::object_store_utils::write_rmw_aof_fail_frame($output);
        Ok(true)
      }
      _ => Ok(true),
    }
  }};
}

mod blocking;
mod read;
pub(crate) mod slow;
mod write;

use memchr::memmem;
use wbase::num::{strict_f64, strict_i32};
use wcol::{
  ObjectOutput, ObjectOutputFlags,
  zset::sorted_set_object::{SortedSetObject, SortedSetOperation},
};
use wdev::Device;
use wresp::{check_args::check_arg_count, cmd_strings as cs, ext::RespVecExt};
use wval::GarnetObjectType;

pub use self::write::RemoveRangeKind;
pub(crate) use self::{
  blocking::{pop_up_to, write_popped_pairs},
  write::{
    CombineKind, combine_sets, diff_sets, intersect_card, parse_combine_args, parse_diff_args,
    write_lex_result, write_zset_entries,
  },
};
use crate::resp::{
  objects::object_store_utils::{
    GarnetObjectPayload, ObjLoad, SyncRmwCmd, SyncRmwHandlers, SyncRmwOutcome, SyncStoreWindow,
    obj_load_typed_sync, obj_save_or_gc, obj_writeback_recheck_sync, run_sync_rmw,
    store_writeback_clear_ttl,
  },
  resp_server_session::RespServerSession,
};

pub(crate) type ZsetLoad = ObjLoad<SortedSetObject>;
pub(crate) type Rmw = SyncRmwOutcome;

/// 经对象层 operate 通道执行操作，返回结构化输出
///（协议版本按会话协商版本透传，C# respProtocolVersion）
/// 同步装载有序集合（信封解码，与 storage 会话域同一 `[标签][载荷]` 格式）
#[inline]
pub(crate) fn zset_load_sync(
  store: &wkv::BatchStoreSession<impl Device>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> ZsetLoad {
  obj_load_typed_sync(
    store,
    key,
    GarnetObjectType::SortedSet,
    output,
    SortedSetObject::from_blob,
  )
}

/// 变更回写：空集合整键回收（对齐 storage 层 finalize_removal 与 set 命令域收尾）
#[inline]
pub(crate) fn zset_save_or_gc(
  store: &wkv::BatchStoreSession<impl Device>,
  key: &[u8],
  obj: &SortedSetObject,
) -> wkv::Result<bool> {
  obj_save_or_gc(
    store,
    key,
    GarnetObjectType::SortedSet,
    obj,
    obj.sorted_set_dict.is_empty(),
    |o| o.to_blob(),
  )
}

/// rmw 回写判定
///
/// - 只读操作不落库；例外：对象层已发生 TTL 惰性剔除（mutated_by_ttl）时
///   升格写回——C# 对象常驻 Tsavorite 对象缓存，ZCARD 读路径的
///   DeleteExpiredItems 就地剔除经 checkpoint 序列化落盘，Rust 信封无常驻
///   对象层，以剔除后回写等价闭环，杜绝已剔除成员重装载复活；
/// - 错误回复（WRONGTYPE 标志或 `-` 行）不落库——分臂陈述：非「无状态变更」，
///   ZADD 族数据段错臂出帧前序合法对已就地改 obj（对象层无回滚臂），随本门
///   整体丢弃（原子性收口，见 deviations §142，禁按 C# 部分提交形回改）；
///   缺键不建键（防幻键）；
/// - 缺失键操作后仍为空则保持缺失（对齐 GarnetObject.NeedToCreate 初值判定矩阵）；
/// - 仅回填 result1 的操作（ZREM/ZREMRANGEBYLEX/ZREMRANGEBYRANK/ZREMRANGEBYSCORE/
///   ZPOPMIN/ZPOPMAX）以移除计数为准：零变更（命中 0 条 / 弹出 0 条）不落库
///   不广播，杜绝全量重序列化写放大与 AOF 增量污染；TTL 惰性剔除（装载即剔除
///   或操作前堆序剔除，mutated_by_ttl）升格写回固化矫正。
pub(crate) fn should_write_back(
  op: SortedSetOperation,
  out: &ObjectOutput<'_>,
  obj: &SortedSetObject,
  existed: bool,
) -> bool {
  if out.payload_view().first() == Some(&b'-') || (!existed && obj.sorted_set_dict.is_empty()) {
    return false;
  }
  match op {
    SortedSetOperation::Zrem
    | SortedSetOperation::Zremrangebylex
    | SortedSetOperation::Zremrangebyrank
    | SortedSetOperation::Zremrangebyscore
    | SortedSetOperation::Zpopmin
    | SortedSetOperation::Zpopmax => {
      (out.result1 > 0 && out.result1 != i32::MAX as i64) || obj.mutated_by_ttl()
    }
    // ZCARD 物化矫正臂（信封水位越线承接）：ZCARD 只回填 result1 不写负载，
    // out.written() 对其恒假、不得并入默认臂——剔除实际发生（mutated_by_ttl）
    // 升格写回一次矫正；全成员到期剔空（REMOVE_KEY）即删空自愈。矫正后水位
    // 前移，回归 O(1) 快道（collection.md §6.3）
    SortedSetOperation::Zcard => {
      obj.mutated_by_ttl() || out.output_flags.contains(ObjectOutputFlags::REMOVE_KEY)
    }
    // 只读操作零状态变更不落库（mutated_by_ttl 剔除升格写回维持原判定）
    _ => out.written() && (!is_read_only(op) || obj.mutated_by_ttl()),
  }
}

/// 只读操作（rmw 不落库）
pub(crate) fn is_read_only(op: SortedSetOperation) -> bool {
  matches!(
    op,
    SortedSetOperation::Zcard
      | SortedSetOperation::Zscore
      | SortedSetOperation::Zmscore
      | SortedSetOperation::Zcount
      | SortedSetOperation::Zrange
      | SortedSetOperation::Zrank
      | SortedSetOperation::Zrevrank
      | SortedSetOperation::Zlexcount
      | SortedSetOperation::Zrandmember
      | SortedSetOperation::Zttl
      | SortedSetOperation::Zscan
  )
}

impl RespServerSession {
  /// 读-改-写骨架：装载 → operate → 变更回写（带增量 WAL 广播）→ 负载输出
  ///（协议版本取会话协商版本，C# respProtocolVersion）
  #[inline]
  pub(crate) fn zset_rmw(
    &self,
    store: &wkv::BatchStoreSession<impl Device>,
    key: &[u8],
    op: SortedSetOperation,
    args: &[&[u8]],
    args12: (i32, i32),
    output: &mut Vec<u8>,
  ) -> Rmw {
    let (arg1, arg2) = args12;
    let resp_version = self.resp_protocol_version;
    run_sync_rmw(
      store,
      SyncRmwCmd {
        key,
        tag: GarnetObjectType::SortedSet,
        op,
        args,
        arg1,
        arg2,
      },
      output,
      SyncRmwHandlers::new(
        SortedSetObject::from_blob,
        SortedSetObject::new,
        |o: &SortedSetObject| o.sorted_set_dict.is_empty(),
        |o: &SortedSetObject| o.to_blob(),
        |obj, op, args, output| run_operate(obj, op, args, arg1, arg2, resp_version, output),
        should_write_back,
      ),
    )
  }
}

/// 成对负载解析（ZRANGESTORE/GEOSEARCHSTORE 回读：member score member score ...）
///
/// 兼容两种形态：RESP2 扁平序列；GEOSEARCHSTORE 的每项前置 `*2` 嵌套数组头。
/// 单次游标推进逐 bulk 读取，任一项非法即终止（残缺项不入库）
pub(crate) fn parse_pairs_payload(payload: &[u8]) -> Vec<(Vec<u8>, f64)> {
  let mut pos = 0;
  // 外层数组头 *<n>
  skip_array_header(payload, &mut pos);

  let mut pairs = Vec::new();
  while let Some(member) = next_bulk(payload, &mut pos) {
    // 第二项：bulk string 形式的分值
    let Some(score) = next_bulk(payload, &mut pos) else {
      break;
    };
    pairs.push((member.to_vec(), strict_f64(score, true).unwrap_or(0.0)));
  }
  pairs
}

/// 跳过至多一个数组头 `*<n>\r\n`（外层头与逐项 `*2` 嵌套头各跳一次，
/// 逐层剥落即还原扁平序列）
fn skip_array_header(payload: &[u8], pos: &mut usize) {
  if payload.get(*pos) == Some(&b'*')
    && let Some(line_end) = find_crlf(payload, *pos)
  {
    *pos = line_end + 2;
  }
}

/// 游标读取一个批量字符串 `$<len>\r\n<bytes>\r\n`（前置数组头至多跳一个）；
/// 词形非法或长度越界即 `None`（解析终止，游标不动）
fn next_bulk<'p>(payload: &'p [u8], pos: &mut usize) -> Option<&'p [u8]> {
  skip_array_header(payload, pos);
  if payload.get(*pos) != Some(&b'$') {
    return None;
  }
  let line_end = find_crlf(payload, *pos)?;
  let len = strict_i32(&payload[*pos + 1..line_end]).filter(|&v| v >= 0)? as usize;
  let start = line_end + 2;
  let end = start + len;
  if end + 2 > payload.len() {
    return None;
  }
  *pos = end + 2;
  payload.get(start..end)
}

fn find_crlf(payload: &[u8], from: usize) -> Option<usize> {
  let slice = payload.get(from..)?;
  memmem::find(slice, b"\r\n").map(|pos| from + pos)
}

// ============ 选项词元游标（集合运算 / GEOSEARCH 族选项文法共用单源） ============

/// RESP 选项段游标：单次前向扫描 + 大小写不敏感关键字命中的紧凑 DFA，
/// 替代各命令重复手写的下标递增与多 `Option` 解构
#[derive(Clone, Copy)]
pub(crate) struct OptCursor<'p> {
  /// 待扫词元段（调用方已切除命令前缀）
  args: &'p [&'p [u8]],
  /// 游标位（恒 ≤ args.len()）
  idx: usize,
}

impl<'p> OptCursor<'p> {
  pub(crate) const fn new(args: &'p [&'p [u8]]) -> Self {
    Self { args, idx: 0 }
  }

  /// 选项段已扫尽
  pub(crate) const fn done(&self) -> bool {
    self.idx >= self.args.len()
  }

  /// 当前词元（不前进）
  pub(crate) fn peek(&self) -> Option<&'p [u8]> {
    self.args.get(self.idx).copied()
  }

  /// 取并前进 `n` 个词元（零拷贝切片直取）；不足则不前进
  pub(crate) fn take(&mut self, n: usize) -> Option<&'p [&'p [u8]]> {
    let got = self.args.get(self.idx..self.idx + n)?;
    self.idx += n;
    Some(got)
  }

  /// 取并前进恰 `N` 个词元（定长形态，便于数组解构）；不足则不前进
  pub(crate) fn take_arr<const N: usize>(&mut self) -> Option<[&'p [u8]; N]> {
    let got = <[&'p [u8]; N]>::try_from(self.args.get(self.idx..self.idx + N)?).ok()?;
    self.idx += N;
    Some(got)
  }

  /// 取并前进单个词元；不足则不前进
  pub(crate) fn one(&mut self) -> Option<&'p [u8]> {
    Some(self.take_arr::<1>()?[0])
  }

  /// 关键字命中（大小写不敏感）即前进
  pub(crate) fn eat(&mut self, keyword: &[u8]) -> bool {
    if self.peek().is_some_and(|t| t.eq_ignore_ascii_case(keyword)) {
      self.idx += 1;
      return true;
    }
    false
  }
}

// ============ 同步落笔三态收尾（写臂 / STORE 覆写族 / 弹出族共用单源） ============

/// 同步写回落笔结果
pub(crate) enum Wb {
  /// 写回成功（`clear_ttl` 形已按 SET 语义完成 TTL 清退），待调用方补正常应答
  Done,
  /// 存储争用 / 磁盘候选未就绪：走既有 `Ok(false)` 异步重放通道
  Degrade,
  /// 写回 IO 失败或 TTL 尾笔残留：generic 错误帧已写出，应答终结
  Failed,
}

impl Wb {
  /// 非成功态即终结应答（`Some(false)` 转异步重放、`Some(true)` 应答已完整），
  /// 成功返回 `None` 供调用方续写正常应答
  #[inline]
  pub(crate) const fn terminal(self) -> Option<bool> {
    match self {
      Wb::Done => None,
      Wb::Degrade => Some(false),
      Wb::Failed => Some(true),
    }
  }
}

/// 信封写回（空集合整键回收）+ 可选 SET 语义尾笔：`clear_ttl` 时非空结果随写
/// 清既有 key 级 TTL（票 zcode-r122c-setstore1 序纪律：写回先行、清退随后，
/// 失败臂零清退即原态；空结果 TTL 已随删空臂级联清退，免尾笔）
pub(crate) fn writeback_sync<D: Device>(
  store: &wkv::BatchStoreSession<'_, D>,
  key: &[u8],
  obj: &SortedSetObject,
  clear_ttl: bool,
  output: &mut Vec<u8>,
) -> Wb {
  match zset_save_or_gc(store, key, obj) {
    Ok(true) => {
      if clear_ttl && !obj.sorted_set_dict.is_empty() && !store_writeback_clear_ttl(store, key) {
        output.write_resp_error(cs::RESP_ERR_GENERIC);
        return Wb::Failed;
      }
      Wb::Done
    }
    Ok(false) => Wb::Degrade,
    Err(_) => {
      output.write_resp_error(cs::RESP_ERR_GENERIC);
      Wb::Failed
    }
  }
}

/// 装载型写臂落笔（弹出 / GEOADD 剔空自愈等同形态）：按装载态复验域归属 →
/// 信封写回（无 TTL 尾笔）
pub(crate) fn rmw_writeback_sync<D: Device>(
  store: &wkv::BatchStoreSession<'_, D>,
  key: &[u8],
  obj: &SortedSetObject,
  output: &mut Vec<u8>,
) -> Wb {
  if !obj_writeback_recheck_sync(store, key, true) {
    return Wb::Degrade;
  }
  writeback_sync(store, key, obj, false, output)
}

/// STORE 覆写族目标键同步收尾（ZRANGESTORE / ZDIFFSTORE / Z*STORE 同核）：
/// 取目标键 rmw 窗跨「信封写回 → TTL 清退」全程（对面 DEL/SET 交叠即拒写重放，
/// 走既有 `Ok(false)` 异步通道），落笔前按开窗时刻存活域复验归属
pub(crate) fn store_overwrite<D: Device>(
  store: &wkv::BatchStoreSession<'_, D>,
  dst: &[u8],
  obj: &SortedSetObject,
  output: &mut Vec<u8>,
) -> Wb {
  let Some(window) = SyncStoreWindow::begin(store, dst) else {
    return Wb::Degrade;
  };
  if !window.recheck() {
    return Wb::Degrade;
  }
  writeback_sync(store, dst, obj, true, output)
}

// ============ 族内参数推导单源（快慢路径共用） ============
// 推导体为无 IO 纯解析 + 失败帧直写 output（同输入快慢应答逐字节一致），
// 慢分派不再对快路径已校验参数做第二份推导。

/// ZRANK / ZREVRANK 的 WITHSCORE 词元推导单源（快慢路径共用；解析失败时
/// 已写出错误应答并返回 None），返回是否带 WITHSCORE
///
/// 判定序对标 C# SortedSetCommands.cs 的 SortedSetRank：arity ≥ 2 →
/// 仅 len==3 校验 WITHSCORE（大小写不敏感，非法即 syntax error），len>3
/// 静默忽略多余参数（includeWithScore 保持 false）
pub(crate) fn parse_rank_with_score(
  cmd_name: &'static str,
  parse_state: &[&[u8]],
  output: &mut Vec<u8>,
) -> Option<bool> {
  check_arg_count!(parse_state, 2.., output, cmd_name, return None);
  if parse_state.len() == 3 && !parse_state[2].eq_ignore_ascii_case(cs::WITHSCORE) {
    cs::abort_with_error_message(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
    return None;
  }
  Some(parse_state.len() == 3)
}

/// ZMPOP / BZMPOP 参数推导单源（快慢路径共用；解析失败时已写出错误应答并
/// 返回 None），返回 (键切片, 低分优先, count)
///
/// ZMPOP: numkeys key \[key ...\] MIN|MAX \[COUNT count\]
/// BZMPOP: timeout numkeys key \[key ...\] MIN|MAX \[COUNT count\]
///（timeout 词元不在本内核射程，由调用方先行解析与校验）
///
/// 判定序对标 C# SortedSetCommands.cs（SortedSetMPop 与 SortedSetBlockingMPop）：
/// numkeys 非整数（含溢出）与 <1 同报 → 定长形态 MIN|MAX 必带、COUNT 形态恰多
/// 2 参 → MIN/MAX 大小写门 → COUNT 词元大小写门 → count 非整数与 <1 同报。
/// 错误帧两命令不同源：ZMPOP 报 NOT_INTEGER，BZMPOP 报 `Parameter` 反引号版
pub(crate) fn parse_zmpop_args<'a>(
  parse_state: &'a [&'a [u8]],
  is_blocking: bool,
  output: &mut Vec<u8>,
) -> Option<(&'a [&'a [u8]], bool, i32)> {
  super::shared_object_commands::parse_mpop_args(
    parse_state,
    is_blocking,
    super::shared_object_commands::MpopArgsConfig {
      cmd_name: if is_blocking { "BZMPOP" } else { "ZMPOP" },
      num_keys_non_blocking_err: cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
      count_non_blocking_err: cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER,
    },
    |t| {
      if t.eq_ignore_ascii_case(b"MIN") {
        Some(true)
      } else if t.eq_ignore_ascii_case(b"MAX") {
        Some(false)
      } else {
        None
      }
    },
    output,
  )
}

// operate 通道执行单源已收敛至 rmw_helpers（原本地副本删除）
pub(crate) use crate::resp::objects::object_store_utils::run_operate;
