//! 自适应分层集合引擎：基于 wbftree 的分页分层态操作（Hash/Set/ZSet/List）
//!
//! 当集合规模达到升阶阈值（条目数 >= 65,536 或 体积 >= 4MB）时，
//! 自动就地升阶转换为基于 wbftree 的独立页级持久化树，基于 B+ 树页级冷热置换
//! 实现千万级海量存储，彻底消除全量反序列化读放大。
//!
//! 写形不变量「树内墓碑恒低」：本模块**没有**按成员往树里逐条落删除记录的命令臂。
//! 删除重的写命令（HDEL / SREM / SPOP / ZREM / LPOP / RPOP）与 ZPOPMIN、
//! ZREMRANGE* 等族内其余未支持操作同径，一律穿透至 `rmw_helpers::run_async_rmw`
//! 的物化降级通道 → wcol 对象层单源求值 → `apply_rmw_post_operate` 整值写回
//! （删空即整键消亡，否则 `promote_collection_to_bftree` 的 `bulk_load` 重建，
//! 零墓碑）。该形与 C# 一致：C# 集合对象整值常驻对象域、删即就地改内存对象、
//! 删空即整键消亡（garnet/libs/server/Storage/Functions/ObjectStore/
//! RMWMethods.cs:188-215 `PostCopyUpdater` → `value.Operate` → `HasRemoveKey`），
//! 既无按成员分记录的树、也就无墓碑连跑。
//!
//! 本不变量是硬要求而非优化：底层 bf-tree 的 `ScanIter::next` 对墓碑记录是
//! **尾递归自调**（rustc 无 TCO），跳过一条墓碑压一帧（实测 ≈680B/帧），且墓碑
//! 判定排在界键比较之前、`scan_cnt` 只在存活分支递减 ⇒ 游标后的连续墓碑必须在
//! 单次 `next()` 内跳完，COUNT / 上界键 / 早停回调一概截不断，8MiB 默认栈的安全
//! 边 ≈ 连续墓碑 8000 条。故任何一次扫描面调用（分层 HSCAN/SSCAN/ZSCAN 的
//! [`exec_tiered_scan`]、push 双臂用的 [`list_head_seq`]、后台降阶物化轮
//! [`tiered_materialize_blob`]）的栈深度都只由「树内最长墓碑连跑」决定，与本臂
//! count 无关（实测与机理全档：task/reject/tiered-zset-demote-stack.md，判据与
//! 落地次序：task/ing/tiered-zset-demote-bf-tree-recursion-stack-overflow.md）。
//! 原第三处客户端同险面「`tiered_list_arm` 的 LPOP/RPOP 有界取臂」已随本不变量
//! 一并消失（弹出臂删除，见 [`tiered_list_arm`] 末穿透注释）。
//! 残余墓碑来源仅剩成员级 TTL 物理出账一处（批量出账 [`collect_expired_members`]
//! 与逐成员出账 `member_expire_arm` / `member_ttl_probe` / `member_persist_arm`，
//! 后者由 HEXPIRE / HTTL / HPERSIST 族按成员下压）：该族是分层态自有形态（C# 侧
//! 成员级到期是常驻内存的 expiration 字典、出账零 I/O），无对应重灌写形可依，
//! 且批量出账的宿主是 HLEN / ZCARD 的 O(1) 计数直读臂——实测该面**仍可**留长连
//! 跑并在默认栈下溢出（复现与处置选项见 [`collect_expired_members`] 文档），是否
//! 连它一并收敛到重灌面（代价：计数臂在到期水位命中那次由 O(1) 变 O(N)）属主代
//! 理裁决项，不在本票内自行加阈值常量或栈大小掩盖。

use core::str;
use std::{str::from_utf8, sync::Arc};

use itoa::Buffer as ItoaBuffer;
use wbase::{
  convert::{
    milliseconds_from_diff_ticks, seconds_from_diff_ticks, unix_time_in_milliseconds_from_ticks,
    unix_time_in_seconds_from_ticks,
  },
  num::{strict_f64, strict_i32, strict_i64},
  time::now_ticks,
};
use wbftree::{
  BfTreeDeleteResult, BfTreeInsertResult, BfTreeReadResult, BfTreeService, RangeIndexStub,
  ScanReturnField,
};
use wcol::{
  SET_MEMBER_DUMMY_VALUE,
  hash::hash_object::{HashExpireResult, HashOperation},
  list::list_object::ListOperation,
  set::set_object::SetOperation,
  types::{
    garnet_object::LIST_SEQ_BASE,
    member_ttl::{decode_member, encode_member_into, encoded_len, member_expired_at},
  },
  zset::sorted_set_object::SortedSetOperation,
};
use wdev::Device;
use wkv::{BatchStoreSession, RangeIndexError, StoreSession, TreeGuard, validate_bftree_record};
use wresp::{
  cmd_strings as cs,
  ext::RespVecExt,
  options::{ExpirationWithOption, ExpireOption},
  resp_memory_writer::format_double,
};
use wval::{GarnetObjectType, MetaValue};
use zmij::Buffer as ZmijBuffer;

/// 分层态操作上下文（元记录 + 树存根的可变借用束 + 变更脏标记）
///
/// `dirty` 由本模块树内写漏斗 [`tree_put`] / [`tree_del`] 在实际写入成功时置位，
/// 批量漏斗 [`tree_put_batch`] 的置脏交由调用臂按命令语义判定（覆盖写与重复
/// 成员在「新增计数」上分叉，见其文档），是分层写臂 WATCH 版本栅栏推进的唯一
/// 判据（一处定义，见 [`finish_tiered_arm`]）
pub(crate) struct TieredCtx<'a> {
  pub meta: &'a mut MetaValue,
  pub stub: &'a mut RangeIndexStub,
  /// 本次命令是否实际变更了树内容
  pub dirty: bool,
}

impl<'a> TieredCtx<'a> {
  /// 新建分层命令上下文（脏标记初值为假：读臂不得推进版本栅栏）
  #[inline]
  pub(crate) fn new(meta: &'a mut MetaValue, stub: &'a mut RangeIndexStub) -> Self {
    Self {
      meta,
      stub,
      dirty: false,
    }
  }
}

/// 分层态集合命令通用参数包（收敛 exec_tiered_* 与 tiered_*_arm 入参，消除 too-many-arguments）
pub(crate) struct TieredCollectionArgs<'a, Op> {
  pub op: Op,
  pub args12: (i32, i32),
  pub args: &'a [&'a [u8]],
  pub resp_protocol_version: u8,
}

impl<'a, Op> TieredCollectionArgs<'a, Op> {
  #[inline]
  pub fn new(op: Op, args12: (i32, i32), args: &'a [&'a [u8]], resp_protocol_version: u8) -> Self {
    Self {
      op,
      args12,
      args,
      resp_protocol_version,
    }
  }
}

/// 分层树内写入漏斗（置脏的唯一入口之一）：仅实际写入成功才标脏
///
/// 记录经 [`wcol::types::member_ttl`] 单点 codec 编码（`expiry = None` 落裸
/// 载荷形态），Set / List 无字段级 TTL 恒传 `None`
#[inline]
fn tree_put(
  ctx: &mut TieredCtx<'_>,
  tree: &BfTreeService,
  member: &[u8],
  value: &[u8],
  expiry: Option<i64>,
) -> BfTreeInsertResult {
  let mut record = Vec::with_capacity(encoded_len(value.len(), expiry));
  encode_member_into(value, expiry, &mut record);
  let res = tree.insert(member, &record);
  ctx.dirty |= res == BfTreeInsertResult::Success;
  res
}

/// 分层写臂「插成功才计数」唯一判据：tree_put 实际落树成功才回报真。
///
/// C# 内存对象插入是无失败纯字典写（libs/server/Objects/Hash/HashObjectImpl.cs 内
/// HashSet 一族，该符号锚点 1:1 挂在 `wcol::hash::hash_object_impl::HashObject::hash_set`，
/// 本判据位不复挂），计数与写入天然同步；rust 树写面存在 wbftree 长度契约失败面
/// （BfTreeInsertResult::InvalidKV/InvalidArguments），计数与应答一律以本判据为准，
/// 禁止 `let _ =` 吞掉（置脏判据在 tree_put 内已按 Success 收口，不受影响）
#[inline]
fn tree_put_ok(
  ctx: &mut TieredCtx<'_>,
  tree: &BfTreeService,
  member: &[u8],
  value: &[u8],
  expiry: Option<i64>,
) -> bool {
  tree_put(ctx, tree, member, value, expiry) == BfTreeInsertResult::Success
}

/// 分层写臂长度契约预校验漏斗（对标 wkv RI 面 range_index_set_batch「预校验先于
/// 任何写入」口径）：任一成员违反存根长度契约即写出同款 InvalidKV 应答并回报假，
/// 调用臂直接收尾——整命令零树内副作用。校验经 [`validate_bftree_record`] 单点，
/// 编码记录长度经 [`encoded_len`] 与写树 codec 同源，杜绝第二套判定
#[inline]
fn tiered_precheck(
  ctx: &TieredCtx<'_>,
  member: &[u8],
  payload_len: usize,
  expiry: Option<i64>,
  output: &mut Vec<u8>,
) -> bool {
  match validate_bftree_record(ctx.stub, member, encoded_len(payload_len, expiry)) {
    Ok(()) => true,
    Err(e) => {
      cs::write_error_raw(output, &e.to_string());
      false
    }
  }
}

/// 预校验通过后树内仍拒写的异常态兜底应答（仅树引擎配置与存根契约偏离可达）：
/// 部分写入已发生但计数与实存保持一致，回错误而非假成功，与 wkv RI 面
/// ri_set_batch 的 Internal 偏离兜底同族口径
#[inline]
fn tree_put_rejected(output: &mut Vec<u8>) {
  cs::write_error_raw(
    output,
    &RangeIndexError::Internal("分层树写被拒：树配置与存根长度契约偏离".to_string()).to_string(),
  );
}

/// 分层树内删除漏斗（置脏的唯一入口之二）：仅实际删除成功才标脏
#[inline]
fn tree_del(ctx: &mut TieredCtx<'_>, tree: &BfTreeService, member: &[u8]) -> BfTreeDeleteResult {
  let res = tree.delete(member);
  ctx.dirty |= res == BfTreeDeleteResult::Success;
  res
}

/// 分层树内批量写入漏斗：编码整批经排序批量 upsert 内核一次下刷
/// （栈上排序集中命中叶页，消除逐条 N 次引擎借用），返回真实新增键数——
/// 到期旧记录在树即不计新增，与逐条前探 `tree_member_state` 等价的
/// 「插成功才计数」批量判据（前查由内核单次借用内承担）
///
/// 置脏归调用方判定（本漏斗不置脏）：新增计数与「树内容是否实际变更」在
/// 覆盖写族上天然分叉，只有命令语义能定夺——
/// - HSET/HMSET 覆盖既有字段：计数为 0 但值字节被替换，C#
///   libs/server/Objects/Hash/HashObjectImpl.cs:HashSet 变更分支同样重写记录
///   → 写成功即置脏；
/// - SADD 重复成员：C# libs/server/Objects/Set/SetObjectImpl.cs:Set 对已存
///   成员零字典写（计数与写入天然同步），树内容逐位不变 → 计数为 0 即不置脏
///
/// 调用方须先经 [`tiered_precheck`] 全量预校验（任一成员越契约即整体失败、
/// 零树内副作用），内核批内 `InvalidKV` 因前置校验不可达，`Err` 仅兜底树配置
/// 偏离态：不计数入账、由调用臂回 [`tree_put_rejected`] 错误，杜绝已落成员
/// 计数与应答的第二次背离
#[inline]
fn tree_put_batch(
  tree: &BfTreeService,
  entries: &[(&[u8], &[u8])],
) -> Result<u64, BfTreeInsertResult> {
  let mut recs: Vec<(&[u8], Vec<u8>)> = Vec::with_capacity(entries.len());
  for (member, payload) in entries {
    let mut record = Vec::with_capacity(encoded_len(payload.len(), None));
    encode_member_into(payload, None, &mut record);
    recs.push((member, record));
  }
  tree.upsert(&recs)
}

/// 分层树内批量删除漏斗（置脏）：单次排序批量删除内核（前查 + 删，单次引擎
/// 借用），返回真实删除数——「删成功才计数」的结果驱动判据（同批重复键去重
/// 只删一次不重复计数，到期旧记录在树亦被删亦计，与逐条前探口径一致）
///
/// **唯一消费方是成员级 TTL 物理出账**（[`collect_expired_members`]）：命令级
/// 删除（HDEL/SREM/SPOP/ZREM/LPOP/RPOP）已无树内逐成员删除臂，一律走整值重灌
/// （见本模块头注「墓碑恒低」）。本漏斗留下的墓碑即全模块残余项，其规模上限
/// 与出账批的键序连跑长度同阶，权衡与实测边界见 collect_expired_members 文档
#[inline]
fn tree_del_batch(ctx: &mut TieredCtx<'_>, tree: &BfTreeService, members: &[&[u8]]) -> u64 {
  let deleted = tree.bulk_delete(members);
  ctx.dirty |= deleted > 0;
  deleted
}

/// 树内成员点查状态：`None` = 记录不在树；`Some((到期刻度, 已到期))` = 在树
#[inline]
fn tree_member_state(tree: &BfTreeService, member: &[u8], now: i64) -> Option<(Option<i64>, bool)> {
  let mut state = None;
  tree.read_callback(member, |res, raw| {
    if res == BfTreeReadResult::Found {
      let (expiry, _) = decode_member(raw);
      state = Some((expiry, expiry.is_some_and(|ticks| ticks < now)));
      true
    } else {
      false
    }
  });
  state
}

/// 读成员记录解码载荷（堆拷贝；HEXPIRE/HPERSIST 原值保持重写所需）
fn tree_payload(tree: &BfTreeService, member: &[u8]) -> Option<Vec<u8>> {
  let mut out = None;
  tree.read_callback(member, |res, raw| {
    if res == BfTreeReadResult::Found {
      out = Some(decode_member(raw).1.to_vec());
      true
    } else {
      false
    }
  });
  out
}

/// HEXPIRE/ZEXPIRE 族共享内核（hash·zset 两臂同型循环体一处定义）：
/// 到期成员视同不存在并物理出账（-2，C# hash_expire 入口先
/// DeleteExpiredItems）、NX/XX/GT/LT 闸门拒 0（libs/server/Objects/Hash/
/// HashObject.cs:SetExpiration）、过去时刻物理出账回 2、原值保持挂新刻度
/// 推进水位回 1。返回 (结果码, 是否发生树/meta 变更)
fn member_expire_arm(
  ctx: &mut TieredCtx<'_>,
  tree: &BfTreeService,
  member: &[u8],
  ticks: i64,
  option: ExpireOption,
  now: i64,
) -> (i32, bool) {
  match tree_member_state(tree, member, now) {
    Some((_, true)) => {
      // 结果驱动出账：删除成功才扣减（写锁内成员确认在树，恒成功；防御口径
      // 仍取真实结果，杜绝计数虚减固化）
      if tree_del(ctx, tree, member) == BfTreeDeleteResult::Success {
        ctx.meta.dec_size(1);
      }
      (HashExpireResult::KeyNotFound as i32, true)
    }
    Some((current, false)) => {
      let denied = match current {
        Some(cur) => {
          option.contains(ExpireOption::NX)
            || (option.contains(ExpireOption::GT) && ticks <= cur)
            || (option.contains(ExpireOption::LT) && ticks >= cur)
        }
        None => option.contains(ExpireOption::XX) || option.contains(ExpireOption::GT),
      };
      if denied {
        (HashExpireResult::ExpireConditionNotMet as i32, false)
      } else if ticks <= now {
        // 过去时刻物理出账：结果驱动扣减（同到期臂口径）
        if tree_del(ctx, tree, member) == BfTreeDeleteResult::Success {
          ctx.meta.dec_size(1);
        }
        (HashExpireResult::KeyAlreadyExpired as i32, true)
      } else {
        // 重写须有原值：读不到（记录已消失的异常态）即放弃重写回 KeyNotFound
        // 口径，禁 unwrap_or_default 以空载荷复活幽灵成员（size 不补记）
        let Some(payload) = tree_payload(tree, member) else {
          return (HashExpireResult::KeyNotFound as i32, false);
        };
        let _ = tree_put(ctx, tree, member, &payload, Some(ticks));
        ctx.meta.note_expiry(ticks);
        (HashExpireResult::ExpireUpdated as i32, true)
      }
    }
    None => (HashExpireResult::KeyNotFound as i32, false),
  }
}

/// HTTL/ZTTL 族共享内核：-2 不存在/已到期（到期物理出账）、-1 无 TTL、
/// 正值为原始到期刻度（单位换算由调用方处理）。返回 (刻度, 是否出账)
fn member_ttl_probe(
  ctx: &mut TieredCtx<'_>,
  tree: &BfTreeService,
  member: &[u8],
  now: i64,
) -> (i64, bool) {
  match tree_member_state(tree, member, now) {
    Some((expiry, false)) => (expiry.unwrap_or(-1), false),
    Some((Some(_), true)) => {
      // 结果驱动出账：删除成功才扣减
      if tree_del(ctx, tree, member) == BfTreeDeleteResult::Success {
        ctx.meta.dec_size(1);
      }
      (-2, true)
    }
    _ => (-2, false),
  }
}

/// HTTL/ZTTL 族到期刻度 → 应答单位换算单点（秒/毫秒 × 相对/绝对时间戳，
/// 对标 C# HashTimeToLive 的四分支 Utf8 换算）
fn expiry_tick_to_reply(ticks: i64, is_milliseconds: bool, is_timestamp: bool, now: i64) -> i64 {
  if is_timestamp {
    if is_milliseconds {
      unix_time_in_milliseconds_from_ticks(ticks)
    } else {
      unix_time_in_seconds_from_ticks(ticks)
    }
  } else if is_milliseconds {
    milliseconds_from_diff_ticks(ticks, now)
  } else {
    seconds_from_diff_ticks(ticks, now)
  }
}

/// HPERSIST/ZPERSIST 族共享内核：-2 不存在/已到期（到期物理出账）、
/// -1 无 TTL、1 已移除（重写裸载荷形态）。返回 (结果码, 是否变更)
fn member_persist_arm(
  ctx: &mut TieredCtx<'_>,
  tree: &BfTreeService,
  member: &[u8],
  now: i64,
) -> (i32, bool) {
  match tree_member_state(tree, member, now) {
    Some((Some(_), false)) => {
      // 重写须有原值：读不到（记录已消失的异常态）即放弃重写回 KeyNotFound
      // 口径，禁 unwrap_or_default 以空载荷复活幽灵成员（零计数零变更）
      let Some(payload) = tree_payload(tree, member) else {
        return (HashExpireResult::KeyNotFound as i32, false);
      };
      let _ = tree_put(ctx, tree, member, &payload, None);
      (HashExpireResult::ExpireUpdated as i32, true)
    }
    Some((Some(_), true)) => {
      // 结果驱动出账：删除成功才扣减
      if tree_del(ctx, tree, member) == BfTreeDeleteResult::Success {
        ctx.meta.dec_size(1);
      }
      (HashExpireResult::KeyNotFound as i32, true)
    }
    _ => (HashExpireResult::KeyNotFound as i32, false),
  }
}

/// 分层树字段级到期收集唯一内核（计数校正臂 / 周期对象收集任务 / 显式
/// HCOLLECT·ZCOLLECT 分层臂共用，杜绝第二套收集逻辑）
///
/// 水位快路径：`now < meta.next_expiry` 时树内不存在已到期成员，零树访问
/// 零写闭环（`Ok(false)`）。水位命中才全扫一遍：确认并物理删除全部已到期
/// 成员（对齐 C# 对象层读路径 `DeleteExpiredItems`：libs/server/Objects/Hash/
/// HashObjectImpl.cs 各操作入口先清后算），重算最早到期水位并按删除数扣减
/// `meta.size`（O(1) 计数抵扣的单点落账），meta 回写与 WATCH 推进由调用方
/// 统一收尾。返回是否有树/meta 变更。
///
/// 记账口径：树内「已到期未删除」成员由 `size` 承载、经本内核一次性出账——
/// 无成员级确认态标量（member 级状态无法无损汇入单一标量，刻意不设），两态
/// 计数等价由「读臂过滤 + 计数臂校正 + 周期收集兜底」三层闭环保证。
///
/// 残余项（唯一仍留树墓碑的写形）：本内核经 [`tree_del_batch`] 逐成员落墓碑，
/// 故出账批在键序上的连跑长度即后续扫描面的栈深度上界（≈680B/帧，8MiB 默认栈
/// 实测安全边 ≈8000 条，见 task/reject/tiered-zset-demote-stack.md 一.表）。
/// 同型的逐成员出账（`member_expire_arm` 等，HEXPIRE / ZEXPIRE 族）实测确可触发
/// 溢出：66000 成员分层 zset 上 `ZEXPIREAT key 100 MEMBERS 56000 m0..m55999`
/// （过去时刻 → 逐成员物理出账）后一条 `ZSCAN key 0 COUNT 10` 在默认 8MiB 主线程
/// 栈 SIGABRT「has overflowed its stack」，本票改动前后同形（属既有残余，非本票
/// 引入），而命令级删除同形态（ZREM 前缀 56000）改动后已不炸。
/// 不并入整值重灌面的理由：消费方含 HLEN / ZCARD 等 O(1) 计数直读臂与周期收集
/// 任务（doc/zh/collection.md 大键 O(1) 计数规约第 3 条），重灌要求整对象物化
/// 与建树，令「水位已过期」后的首次计数从 O(1) 变 O(N)；且成员级 TTL 是分层态
/// 自有形态（C# HashObject 的 expiration 字典常驻内存、出账零 I/O），无对应
/// C# 写形可依。可选收敛形（待主代理裁决，本票未自行落）：本内核既已为出账付过
/// 一次整树扫描，出账后随同一次 bulk_load 重建（既有 `promote_collection_to_bftree`
/// 面，渐近同阶 O(N)）即可令残余项归零；逐成员臂则须整族改走重灌，代价另计。
/// 本票不加阈值常量掩盖
pub(crate) fn collect_expired_members(ctx: &mut TieredCtx<'_>, tree: &BfTreeService) -> bool {
  let now = now_ticks();
  if now < ctx.meta.next_expiry {
    return false;
  }
  let mut expired_keys: Vec<Vec<u8>> = Vec::new();
  let mut next_expiry = i64::MAX;
  let _ =
    tree.scan_with_count_callback(&[0u8], usize::MAX, ScanReturnField::KeyAndValue, |k, v| {
      match decode_member(v).0 {
        Some(ticks) if ticks < now => expired_keys.push(k.to_vec()),
        Some(ticks) => next_expiry = next_expiry.min(ticks),
        None => {}
      }
      true
    });
  // 结果驱动出账：批量删除内核前查 + 删，按真实删除数扣减（写锁内扫描确认集
  // 与真实删除恒等，防御口径仍取真实值；置脏经 tree_del_batch 漏斗单点）
  let removed = {
    let keys: Vec<&[u8]> = expired_keys.iter().map(|k| k.as_slice()).collect();
    tree_del_batch(ctx, tree, &keys)
  };
  let moved = ctx.meta.next_expiry != next_expiry;
  ctx.meta.next_expiry = next_expiry;
  if removed > 0 {
    ctx.meta.dec_size(removed);
  }
  removed > 0 || moved
}

/// 分层写臂 WATCH 版本栅栏统一收尾（一处定义，覆盖四个命令臂的全部返回出口）：
/// 判据唯一取 `ctx.dirty`（树内容实际变更），不以应答形态作第二判据
///
/// 对标 C# 对象域写钩子 functionsState.watchVersionMap.IncrementVersion
/// （libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:79 PostInitialUpdater、
/// :100 InPlaceUpdater、:125 HasRemoveKey 删空臂、:200 PostCopyUpdater 与
/// ObjectStore/UpsertMethods.cs:48/:58/:68、ObjectStore/DeleteMethods.cs:21/:30）：
/// C# 集合对象常驻对象域，任意写命令均经上述钩子推进；rust 分层臂是这些钩子在
/// wbftree 态的替代实现，故栅栏在此同点位补齐。键口径为用户键裸字节（与
/// [`BatchStoreSession::bump_watch_version`] 下游 version_map_watch_hook 的
/// TxnKeyEntryComparison::key_hash 及 wtxn WATCH 登记同表同哈希，非物理 Meta 键）。
///
/// 零推进的三类出口与 C# 同口径：
/// - 读臂与被拒臂（HSETNX 命中已存在 / ZADD NX·GT·LT 不落 / SRANDMEMBER 空表）
///   未走写漏斗，dirty 恒假——对齐 C# 纯读走 Read 面不 IncrementVersion；
/// - `Ok(false)` 穿透臂按构造不触碰树（本模块置脏入口只有 [`tree_put`] /
///   [`tree_del`] / [`tree_del_batch`] 与批量 upsert 臂的显式判定，均在闭环
///   应答前），删除重命令（HDEL/SREM/SPOP/ZREM/LPOP/RPOP）全经此穿透，其推进由
///   run_async_rmw 物化降级臂经 apply_rmw_post_operate 单点承接，两条路径互斥
///   无双计；
/// - 页级存根治愈（RIPROMOTE / RIRESTORE 只改瞬态树句柄与 Flushed / Recovered
///   位，零逻辑内容变更）刻意不推进——C# 原位臂同判据（MainStore/RMWMethods.cs
///   :949 RIPROMOTE、:954 RIRESTORE 均返回 IPUResult.NotUpdated，而 :427 推进
///   仅挂 Succeeded 臂）；C# 复制到尾部的 PostCopyUpdater（:1501/:1504）无差别
///   推进属其锁表实现副作用：rust 该路径由读面 acquire_tree_read 首访触发，
///   若照样推进则一条纯读即可误杀他会话 WATCH 事务（假阳性 abort），
///   与本栅栏「变更即通知、未变不误杀」的双向契约相违，故两子路径统一不推进。
///
/// `Err(())` 且已置脏（树内写已生效而元记录写失败）仍推进：树内容已实际变更，
/// 不推进即 WATCH 漏通知（版本号看似未变而数据已改），正是本条缺陷的风险本质。
#[inline]
fn finish_tiered_arm<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &TieredCtx<'_>,
  handled: Result<bool, ()>,
) -> Result<bool, ()> {
  if ctx.dirty {
    session.bump_watch_version(key);
  }
  handled
}

/// 分层树删减后统一收尾（严格删空生命周期单点）：条目计数减至 0 → 先释放
/// 树守卫再 drain 整键回收（含随键 TTL 清理，杜绝幽灵空元记录与孤儿 TTL）；
/// 否则元记录 + 存根回写
///
/// `drop` 顺序在函数体内固定：删空臂必须先放树守卫再 drain（写臂持条带独占
/// 写锁，drain 侧 lifecycle 的 delete_index 自取同键条带写锁，守卫未放即互锁）。
/// 命令级删除族已无本 helper 的消费方（HDEL/SREM/SPOP/ZREM/LPOP/RPOP 全走整值
/// 重灌面，见本模块头注「墓碑恒低」），现仅成员级 TTL 物理出账的收集臂
/// （[`exec_tiered_collect`]）经此收口，不得手抄 if/else 双收尾；非删空分支的
/// meta 回写在守卫仍持有时执行，互斥窗口完整覆盖「装载 → 树写 → 计数 → 回写」
async fn drain_or_save<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  tree_guard: TreeGuard<'_>,
) -> Result<(), ()> {
  if ctx.meta.size == 0 {
    drop(tree_guard);
    // 删空臂键消亡：keep_ttl=false 随键清 TTL，杜绝幽灵空元记录与孤儿 TTL
    session
      .handle_bftree_drain_and_delete(key, false)
      .await
      .map_err(|_| ())?;
  } else {
    session
      .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
      .await
      .map_err(|_| ())?;
  }
  Ok(())
}

/// 分层臂树守卫获取单点：写臂取条带独占写锁并锁内刷新元记录副本，读臂
/// 维持共享读锁
///
/// 写臂刷新消除「装载于锁外（rmw_helpers 路由探测）→ 各持快照 → 收尾
/// save_bftree_meta_stub 整体覆写」的同键并发丢更新（size / next_expiry
/// 增量被后写者抹掉），使互斥窗口完整覆盖多步写臂全程；独占串行对位 C#
/// 对象域同键写经 Tsavorite 记录锁（TsavoriteKV.cs RMW InPlaceUpdater 前置
/// 记录 X 锁），纯读臂共享锁对位 C# RangeIndexManager.Locking.cs 数据操作面。
///
/// `None` = 写臂取锁后元记录已非 live（键被并发排空回收；写锁保证此刻起
/// 无人能再动该键，判定即终态），调用臂穿透（四族 arm `Ok(false)` 物化
/// 降级 / 收集执行体 `Ok(None)` 键非分层态）
async fn tiered_guard<'s, D: Device>(
  session: &'s BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  write: bool,
) -> Result<Option<TreeGuard<'s>>, ()> {
  if !write {
    return session
      .acquire_tree_read(key, ctx.stub)
      .await
      .map(|g| Some(TreeGuard::Read(g)))
      .map_err(|_| ());
  }
  let guard = session
    .acquire_tree_write(key, ctx.stub)
    .await
    .map(TreeGuard::Write)
    .map_err(|_| ())?;
  if !session
    .refresh_tiered_meta(key, ctx.meta)
    .await
    .map_err(|_| ())?
  {
    return Ok(None);
  }
  Ok(Some(guard))
}

/// 哈希族写面判定（一处定义）：多步写臂与含 collect_expired_members 校正面
/// 的读命令（Hlen/Hgetall/Hkeys/Hvals 到期成员物理出账）均取独占写锁；
/// 纯读臂（Hget/Hmget/Hexists/Hstrlen）与穿透臂（HDEL / HRANDFIELD 等）维持共享读锁
fn hash_needs_write(op: HashOperation) -> bool {
  matches!(
    op,
    HashOperation::Hset
      | HashOperation::Hmset
      | HashOperation::Hsetnx
      | HashOperation::Hincrby
      | HashOperation::Hincrbyfloat
      | HashOperation::Hexpire
      | HashOperation::Httl
      | HashOperation::Hpersist
      | HashOperation::Hlen
      | HashOperation::Hgetall
      | HashOperation::Hkeys
      | HashOperation::Hvals
  )
}

/// 集合族写面判定（一处定义）：SADD 写臂取独占写锁；SREM / SPOP 与 SRANDMEMBER、
/// 纯读 / 穿透臂一律共享读锁（删除重族无树内臂，见本模块头注「墓碑恒低」）
fn set_needs_write(op: SetOperation) -> bool {
  matches!(op, SetOperation::Sadd)
}

/// 有序集合族写面判定（一处定义）：Zcard 含 collect_expired_members 校正面
/// 亦写；纯读（Zscore/Zmscore）、ZREM 与其余穿透臂维持共享读锁
fn zset_needs_write(op: SortedSetOperation) -> bool {
  matches!(
    op,
    SortedSetOperation::Zadd
      | SortedSetOperation::Zincrby
      | SortedSetOperation::Zexpire
      | SortedSetOperation::Zttl
      | SortedSetOperation::Zpersist
      | SortedSetOperation::Zcard
  )
}

/// 列表族写面判定（一处定义）：四 push 写臂取独占写锁（LPUSH 序号分配依赖锁内
/// 刷新后的 meta.size，免装载快照错位）；LPOP / RPOP 无树内臂，与纯读、其余
/// 穿透臂一律共享读锁（见本模块头注「墓碑恒低」）
fn list_needs_write(op: ListOperation) -> bool {
  matches!(
    op,
    ListOperation::Rpush | ListOperation::Rpushx | ListOperation::Lpush | ListOperation::Lpushx
  )
}

/// 执行分层态哈希命令（WATCH 栅栏由 [`finish_tiered_arm`] 统一收尾）
pub(crate) async fn exec_tiered_hash<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, HashOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let handled = tiered_hash_arm(session, key, ctx, call, output).await;
  finish_tiered_arm(session, key, ctx, handled)
}

/// 分层态哈希命令树内主体（读写臂分派，见 [`exec_tiered_hash`]）
async fn tiered_hash_arm<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, HashOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let TieredCollectionArgs {
    op,
    args12,
    args,
    resp_protocol_version,
  } = call;
  // 写臂独占互斥（锁内刷新元记录），读臂共享锁（判定一处定义）
  let Some(tree_guard) = tiered_guard(session, key, ctx, hash_needs_write(op)).await? else {
    return Ok(false);
  };
  let tree = tree_guard.tree();

  match op {
    HashOperation::Hset | HashOperation::Hmset | HashOperation::Hsetnx => {
      let is_nx = op == HashOperation::Hsetnx;
      if is_nx {
        if args.len() != 2 {
          return Err(());
        }
        let field = args[0];
        let val = args[1];
        // C# contains_key 口径：到期字段视同不存在 → 插入（物理覆盖替换，size
        // 不变即死→活转换；存活字段命中则拒写）
        let now = now_ticks();
        if !tiered_precheck(ctx, field, val.len(), None, output) {
          return Ok(true);
        }
        match tree_member_state(tree, field, now) {
          Some((_, false)) => {
            output.write_resp_int(0);
            return Ok(true);
          }
          state => {
            // 插成功才计数、才回 1（libs/server/Objects/Hash/HashObjectImpl.cs
            // :SetIfNotExists 计数与写入天然同步的分层等价判据）
            if !tree_put_ok(ctx, tree, field, val, None) {
              tree_put_rejected(output);
              return Ok(true);
            }
            if state.is_none() {
              ctx.meta.size += 1;
            }
            session
              .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
              .await
              .map_err(|_| ())?;
          }
        }
        output.write_resp_int(1);
        return Ok(true);
      }

      // 覆盖写清字段 TTL（C# HashSet 变更分支移除 expiration 条目，
      // libs/server/Objects/Hash/HashObjectImpl.cs:HashSet）；到期旧记录物理
      // 覆盖（size 不变，等价 C# 先 DeleteExpiredItems 再 Add 的净计数）
      // 预校验先于任何写入（RI 批量口径：任一字段越契约即整体失败，零树内副作用）
      for chunk in args.as_chunks::<2>().0 {
        if !tiered_precheck(ctx, chunk[0], chunk[1].len(), None, output) {
          return Ok(true);
        }
      }
      // 批量折叠：编码整批经排序批量 upsert 内核一次下刷（栈上排序集中命中
      // 叶页），返回值即真实新增键数——到期旧记录在树不计新增，与逐条前探
      // tree_member_state 的「插成功才计数」判据等价（前查由内核单次借用承担）
      let entries: Vec<(&[u8], &[u8])> = args
        .as_chunks::<2>()
        .0
        .iter()
        .map(|chunk| (chunk[0], chunk[1]))
        .collect();
      let new_fields = match tree_put_batch(tree, &entries) {
        Ok(n) => n,
        Err(_) => {
          tree_put_rejected(output);
          return Ok(true);
        }
      };
      // 置脏判据：写成功即树内容变更（覆盖字段计数为 0 但值字节被替换）
      ctx.dirty |= !entries.is_empty();

      if new_fields > 0 {
        ctx.meta.size += new_fields;
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
      }

      if op == HashOperation::Hmset {
        output.extend_from_slice(cs::RESP_OK);
      } else {
        output.write_resp_int(new_fields as i64);
      }
      Ok(true)
    }

    HashOperation::Hget => {
      if args.is_empty() {
        return Err(());
      }
      let field = args[0];
      let now = now_ticks();
      // 到期字段视同不存在（C# TryGetValue 口径，libs/server/Objects/Hash/
      // HashObject.cs:ContainsKey/TryGetValue 语义）
      let mut alive = false;
      tree.read_callback(field, |res, raw| {
        if res == BfTreeReadResult::Found {
          if !member_expired_at(raw, now) {
            alive = true;
            output.write_resp_bulk_string(decode_member(raw).1);
          }
          true
        } else {
          false
        }
      });
      if !alive {
        output.write_resp_null_ver(resp_protocol_version);
      }
      Ok(true)
    }

    HashOperation::Hmget => {
      let now = now_ticks();
      output.write_resp_array_len(args.len());
      for &field in args {
        let mut alive = false;
        tree.read_callback(field, |res, raw| {
          if res == BfTreeReadResult::Found {
            if !member_expired_at(raw, now) {
              alive = true;
              output.write_resp_bulk_string(decode_member(raw).1);
            }
            true
          } else {
            false
          }
        });
        if !alive {
          output.write_resp_null_ver(resp_protocol_version);
        }
      }
      Ok(true)
    }

    HashOperation::Hexists => {
      if args.is_empty() {
        return Err(());
      }
      let now = now_ticks();
      let exists = matches!(tree_member_state(tree, args[0], now), Some((_, false)));
      output.write_resp_int(if exists { 1 } else { 0 });
      Ok(true)
    }

    HashOperation::Hlen => {
      // 计数校正（见 collect_expired_members）：水位命中即物理出账到期成员，
      // 直读恒精确（collection.md 大键 O(1) 计数规约第 3 条）
      if collect_expired_members(ctx, tree) {
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
      }
      output.write_resp_int(ctx.meta.size as i64);
      Ok(true)
    }

    HashOperation::Hstrlen => {
      if args.is_empty() {
        return Err(());
      }
      let now = now_ticks();
      let mut len = 0usize;
      tree.read_callback(args[0], |res, raw| {
        if res == BfTreeReadResult::Found && !member_expired_at(raw, now) {
          len = decode_member(raw).1.len();
        }
        res == BfTreeReadResult::Found
      });
      output.write_resp_int(len as i64);
      Ok(true)
    }

    HashOperation::Hgetall => {
      // 输出面先校正（存活全集就位后再写数组头，保证 RESP 头与项数一致），
      // 校正后树内已无到期成员（同刻 now 全扫出账），流式直出
      if collect_expired_members(ctx, tree) {
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
      }
      // 帧头与实体同源：先流式扫树收集到 scratch 并计数，再按实际字段对数写
      // 协议感知 map 头（对标 HashObjectImpl.cs:HashGetAll WriteMapLength(Count())），
      // RESP3 写 `%<对数>`、RESP2 退化为 `*<2×对数>` 数组，杜绝 meta.size 漂移错位
      let mut scratch: Vec<u8> = Vec::new();
      let mut pairs = 0usize;
      let _ =
        tree.scan_with_count_callback(&[0u8], usize::MAX, ScanReturnField::KeyAndValue, |k, v| {
          scratch.write_resp_bulk_string(k);
          scratch.write_resp_bulk_string(decode_member(v).1);
          pairs += 1;
          true
        });
      cs::write_map_len(output, pairs, resp_protocol_version);
      output.extend_from_slice(&scratch);
      Ok(true)
    }

    HashOperation::Hkeys => {
      if collect_expired_members(ctx, tree) {
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
      }
      // 帧头与实际条目同源（消除 meta.size 漂移错位）：数组语义（HKEYS RESP3 仍数组）
      let mut scratch: Vec<u8> = Vec::new();
      let mut n = 0usize;
      let _ = tree.scan_with_count_callback(&[0u8], usize::MAX, ScanReturnField::Key, |k, _| {
        scratch.write_resp_bulk_string(k);
        n += 1;
        true
      });
      output.write_resp_array_len(n);
      output.extend_from_slice(&scratch);
      Ok(true)
    }

    HashOperation::Hvals => {
      if collect_expired_members(ctx, tree) {
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
      }
      // 帧头与实际条目同源（消除 meta.size 漂移错位）：数组语义（HVALS RESP3 仍数组）
      let mut scratch: Vec<u8> = Vec::new();
      let mut n = 0usize;
      let _ = tree.scan_with_count_callback(&[0u8], usize::MAX, ScanReturnField::Value, |_, v| {
        scratch.write_resp_bulk_string(decode_member(v).1);
        n += 1;
        true
      });
      output.write_resp_array_len(n);
      output.extend_from_slice(&scratch);
      Ok(true)
    }

    HashOperation::Hincrby => {
      if args.len() < 2 {
        return Err(());
      }
      let field = args[0];
      let incr_slice = args[1];
      let Some(incr) = strict_i64(incr_slice) else {
        return Err(());
      };
      let now = now_ticks();
      let mut cur_val = 0i64;
      let mut is_new = true;
      // 现存值非数字（Found 且未到期却解析失败）：对齐 C# HashIncrement /
      // 对象层 hash_increment，回 RESP_ERR_HASH_VALUE_IS_NOT_INTEGER 且不写
      // （不落库、不动 size、不推进 dirty，与 HSETNX 命中已存在同属未走写漏斗出口）
      let mut bad_value = false;
      // 存活成员增量保留既有 TTL（C# HashIncrement 不动 expiration）；到期
      // 旧记录视同不存在（C# 增量入口先 DeleteExpiredItems）
      let mut old_expiry = None;
      tree.read_callback(field, |res, raw| {
        if res == BfTreeReadResult::Found {
          let (expiry, payload) = decode_member(raw);
          if member_expired_at(raw, now) {
            return true;
          }
          is_new = false;
          old_expiry = expiry;
          // 现存值用对象层同口径的 `i64::parse`（from_utf8 + parse）判定可解析性
          match from_utf8(payload).ok().and_then(|s| s.parse::<i64>().ok()) {
            Some(n) => cur_val = n,
            None => bad_value = true,
          }
        }
        res == BfTreeReadResult::Found
      });
      if bad_value {
        cs::write_error_raw(output, cs::RESP_ERR_HASH_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      }
      // 新字段：存/回增量原文（对齐对象层 add(incr_slice)+write_integer_from_bytes，
      // 与内存态逐字节一致，如输入 "5" 存 "5"）；wrapping 溢出语义与对象层同
      if is_new {
        // 预校验先于写入（RI 单点口径，零树内副作用）；插成功才计数回值
        if !tiered_precheck(ctx, field, incr_slice.len(), old_expiry, output) {
          return Ok(true);
        }
        if !tree_put_ok(ctx, tree, field, incr_slice, old_expiry) {
          tree_put_rejected(output);
          return Ok(true);
        }
        ctx.meta.size += 1;
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
        output.resp_writer2().write_integer_from_bytes(incr_slice);
        return Ok(true);
      }
      let new_val = cur_val.wrapping_add(incr);
      let mut itoa_buf = ItoaBuffer::new();
      let formatted = itoa_buf.format(new_val);
      if !tree_put_ok(ctx, tree, field, formatted.as_bytes(), old_expiry) {
        tree_put_rejected(output);
        return Ok(true);
      }
      output.write_resp_int(new_val);
      Ok(true)
    }

    HashOperation::Hincrbyfloat => {
      if args.len() < 2 {
        return Err(());
      }
      let field = args[0];
      let incr_slice = args[1];
      // 入参增量经 strict_f64(.., false) 拒 ±inf 词形（对象层 num_utils_try_parse_double
      // + incr.is_infinite → RESP_ERR_GENERIC_NAN_INFINITY 的入参门，两态一致）
      let Some(incr) = strict_f64(incr_slice, false) else {
        return Err(());
      };
      let now = now_ticks();
      let mut cur_val = 0.0f64;
      let mut is_new = true;
      let mut old_expiry = None;
      // 现存值两态错误（对齐 C# HashIncrementFloat 双门）：非浮点 → NOT_FLOAT；
      // 可解析但为 ±inf → NAN_INFINITY_INCR；均回错且不写、不动 size、不推进 dirty
      let mut err: Option<&'static str> = None;
      tree.read_callback(field, |res, raw| {
        if res == BfTreeReadResult::Found {
          let (expiry, payload) = decode_member(raw);
          if member_expired_at(raw, now) {
            return true;
          }
          is_new = false;
          old_expiry = expiry;
          // try_parse_with_infinity 口径 = strict_f64(.., true)（允许 inf 词形，
          // 与对象层 try_parse_with_infinity(&hash_value) 单源一致），据此区分
          // 「非浮点」与「现存值为无穷」两态
          match strict_f64(payload, true) {
            Some(n) if n.is_infinite() => err = Some(cs::RESP_ERR_GENERIC_NAN_INFINITY_INCR),
            Some(n) => cur_val = n,
            None => err = Some(cs::RESP_ERR_HASH_VALUE_IS_NOT_FLOAT),
          }
        }
        res == BfTreeReadResult::Found
      });
      if let Some(msg) = err {
        cs::write_error_raw(output, msg);
        return Ok(true);
      }
      // 新字段：存/回增量原文（对齐对象层 add(incr_slice)+write_bulk_string，
      // 与内存态逐字节一致，如输入 "0.10" 存 "0.10"、回 bulk "0.10"）
      if is_new {
        // 预校验先于写入（RI 单点口径，零树内副作用）；插成功才计数回值
        if !tiered_precheck(ctx, field, incr_slice.len(), old_expiry, output) {
          return Ok(true);
        }
        if !tree_put_ok(ctx, tree, field, incr_slice, old_expiry) {
          tree_put_rejected(output);
          return Ok(true);
        }
        ctx.meta.size += 1;
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
        output.write_resp_bulk_string(incr_slice);
        return Ok(true);
      }
      let new_val = cur_val + incr;
      let mut zmij_buf = ZmijBuffer::new();
      let formatted = format_double(new_val, &mut zmij_buf);
      if !tree_put_ok(ctx, tree, field, formatted.as_bytes(), old_expiry) {
        tree_put_rejected(output);
        return Ok(true);
      }
      output.write_resp_bulk_string(formatted.as_bytes());
      Ok(true)
    }

    // HEXPIRE / HPEXPIRE / HEXPIREAT / HPEXPIREAT 树内原地臂（arg1/arg2 为
    // ExpirationWithOption 压缩字，libs/server/Objects/Hash/HashObjectImpl.cs:
    // HashExpire + HashObject.cs:SetExpiration 闸门口径）
    HashOperation::Hexpire => {
      let e = ExpirationWithOption::from_word_head_tail(args12.0, args12.1);
      let ticks = e.expiration_time_in_ticks();
      let option = e.expire_option();
      let now = now_ticks();
      let mut changed = false;
      output.write_resp_array_len(args.len());
      for &field in args {
        let (result, arm_changed) = member_expire_arm(ctx, tree, field, ticks, option, now);
        changed |= arm_changed;
        output.write_resp_int(i64::from(result));
      }
      if changed {
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
      }
      Ok(true)
    }

    // HTTL / HPTTL / HEXPIRETIME / HPEXPIRETIME 树内原地臂
    //（libs/server/Objects/Hash/HashObjectImpl.cs:HashTimeToLive：-2 不存在 /
    // 已过期（先 DeleteExpiredItems 物理出账）、-1 无过期，正值为请求单位换算）
    HashOperation::Httl => {
      let (is_milliseconds, is_timestamp) = (args12.0 == 1, args12.1 == 1);
      let now = now_ticks();
      let mut removed = false;
      output.write_resp_array_len(args.len());
      for &field in args {
        let (raw, expired) = member_ttl_probe(ctx, tree, field, now);
        removed |= expired;
        let result = if raw >= 0 {
          expiry_tick_to_reply(raw, is_milliseconds, is_timestamp, now)
        } else {
          raw
        };
        output.write_resp_int(result);
      }
      if removed {
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
      }
      Ok(true)
    }

    // HPERSIST 树内原地臂（libs/server/Objects/Hash/HashObjectImpl.cs:
    // HashPersist → HashObject.cs:Persist：-2 不存在/已过期（物理出账）、
    // -1 无过期、1 已移除）
    HashOperation::Hpersist => {
      let now = now_ticks();
      let mut changed = false;
      output.write_resp_array_len(args.len());
      for &field in args {
        let (result, arm_changed) = member_persist_arm(ctx, tree, field, now);
        changed |= arm_changed;
        output.write_resp_int(i64::from(result));
      }
      if changed {
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
      }
      Ok(true)
    }

    // 未支持操作一律穿透（Ok(false)）：由 run_async_rmw 物化降级通道接手，
    // 杜绝静默兜底输出与命令语义无关的应答——HCOLLECT / HRANDFIELD 族经此
    // 落 wcol 对象层单源求值，WATCH 推进同臂由 apply_rmw_post_operate 承接。
    // HDEL 亦在此穿透（无树内逐成员删除臂）：删除重的命令一律走「物化 →
    // 对象层单源求值 → 整值重灌（bulk_load 重建）」，见本模块头注「墓碑恒低」
    _ => Ok(false),
  }
}

/// 执行分层态集合命令（WATCH 栅栏由 [`finish_tiered_arm`] 统一收尾）
pub(crate) async fn exec_tiered_set<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  op: SetOperation,
  args: &[&[u8]],
  output: &mut Vec<u8>,
  resp_protocol_version: u8,
) -> Result<bool, ()> {
  let handled = tiered_set_arm(session, key, ctx, op, args, output, resp_protocol_version).await;
  finish_tiered_arm(session, key, ctx, handled)
}

/// 分层态集合命令树内主体（读写臂分派，见 [`exec_tiered_set`]）
async fn tiered_set_arm<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  op: SetOperation,
  args: &[&[u8]],
  output: &mut Vec<u8>,
  resp_protocol_version: u8,
) -> Result<bool, ()> {
  // 写臂独占互斥（锁内刷新元记录），读臂共享锁（判定一处定义）
  let Some(tree_guard) = tiered_guard(session, key, ctx, set_needs_write(op)).await? else {
    return Ok(false);
  };
  let tree = tree_guard.tree();

  match op {
    SetOperation::Sadd => {
      // 预校验先于任何写入（RI 批量口径，Set 成员恒以 dummy 值裸编码落树，
      // 任一成员越契约即整体失败、零树内副作用）
      for &member in args {
        if !tiered_precheck(ctx, member, SET_MEMBER_DUMMY_VALUE.len(), None, output) {
          return Ok(true);
        }
      }
      // 批量折叠：dummy 值整批经排序批量 upsert 内核一次下刷（栈上排序集中
      // 命中叶页），返回值即真实新增数——同批重复成员去重只计一次，与逐条
      // contains_key 前探的「插成功才计数」判据等价（libs/server/Objects/Set/
      // SetObjectImpl.cs:Set 纯字典写计数的分层等价判据）
      let entries: Vec<(&[u8], &[u8])> = args
        .iter()
        .map(|&member| (member, SET_MEMBER_DUMMY_VALUE))
        .collect();
      let added = match tree_put_batch(tree, &entries) {
        Ok(n) => n,
        Err(_) => {
          tree_put_rejected(output);
          return Ok(true);
        }
      };
      // 置脏判据：仅真实新增才变更树内容——重复成员落树记录逐位相同（dummy
      // 值定长编码），C# SetObjectImpl.cs:Set 对已存成员零字典写、零变更
      ctx.dirty |= added > 0;
      if added > 0 {
        ctx.meta.size += added;
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
      }
      output.write_resp_int(added as i64);
      Ok(true)
    }

    SetOperation::Sismember => {
      if args.is_empty() {
        return Err(());
      }
      let exists = tree.contains_key(args[0]);
      output.write_resp_int(if exists { 1 } else { 0 });
      Ok(true)
    }

    SetOperation::Smismember => {
      output.write_resp_array_len(args.len());
      for &member in args {
        let exists = tree.contains_key(member);
        output.write_resp_int(if exists { 1 } else { 0 });
      }
      Ok(true)
    }

    SetOperation::Scard => {
      output.write_resp_int(ctx.meta.size as i64);
      Ok(true)
    }

    SetOperation::Smembers => {
      // 帧头与实体同源 + 协议感知 set 头（对标 SetObjectImpl.cs:SetMembers
      // WriteSetLength(Set.Count)，RESP3 写 `~<n>`、RESP2 退化 `*<n>`）
      let mut scratch: Vec<u8> = Vec::new();
      let mut n = 0usize;
      let _ = tree.scan_with_count_callback(&[0u8], usize::MAX, ScanReturnField::Key, |k, _| {
        scratch.write_resp_bulk_string(k);
        n += 1;
        true
      });
      cs::write_set_len(output, n, resp_protocol_version);
      output.extend_from_slice(&scratch);
      Ok(true)
    }

    SetOperation::Srandmember => {
      // count 解析（libs/server/Resp/Objects/SetCommands.cs:SetRandomMember：
      // 非整数 → RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER；负数合法取 |count| 可重复）
      let count_arg = args.first().map(|raw| (raw, strict_i32(raw)));
      if let Some((_, parsed)) = &count_arg
        && parsed.is_none()
      {
        cs::write_error_raw(output, cs::RESP_ERR_GENERIC_VALUE_IS_NOT_INTEGER);
        return Ok(true);
      }
      let count = count_arg.as_ref().and_then(|(_, v)| *v);

      // 随机起点：fastrand 随机起始键定位树内扫描位（对象层为随机索引抽取；
      // 树内顺序流式近似，返回集合无序契约下语义等价）
      let start_key = fastrand::u64(..).to_be_bytes();
      let scan = |tree: &Arc<wbftree::BfTreeService>,
                  start: &[u8],
                  n: usize,
                  out: &mut Vec<(Vec<u8>, Vec<u8>)>| {
        let _ = tree.scan_with_count_callback(start, usize::MAX, ScanReturnField::Key, |k, _| {
          if out.len() < n {
            out.push((k.to_vec(), Vec::new()));
            true
          } else {
            false
          }
        });
      };

      // C# NO_COUNT 语义：count_arg 缺省（None）即 int.MinValue 的无 count 形态
      let nc = count.unwrap_or(i32::MIN);
      // 请求/可返条目基数（对标 SetObjectImpl.cs:SetRandomMember 各分支）
      let n = match count {
        None => 1,
        Some(c) if c > 0 => (c as u64).min(ctx.meta.size) as usize,
        Some(0) => 0,
        Some(c) => c.unsigned_abs() as usize,
      };

      // 空集出口
      if ctx.meta.size == 0 {
        if nc == i32::MIN || nc > 0 {
          // SetRandomMember：无 count 或 count>0 → WriteSetLength(0)；
          // count<=0（含负数与 0，落入 C# else）→ WriteNull
          cs::write_set_len(output, 0, resp_protocol_version);
        } else {
          output.write_resp_null_ver(resp_protocol_version);
        }
        return Ok(true);
      }

      let mut members: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(n);
      scan(tree, &start_key, n, &mut members);
      if members.len() < n {
        // 随机起点后段不足：回绕自树头补扫（负 count 可重复语义亦经此补足）
        scan(tree, &[0u8], n, &mut members);
      }
      match count {
        None => {
          if let Some((m, _)) = members.first() {
            output.write_resp_bulk_string(m);
          } else {
            output.write_resp_null_ver(resp_protocol_version);
          }
        }
        // count>0 互异：set 头（C# WriteSetLength）
        Some(c) if c > 0 => {
          cs::write_set_len(output, members.len(), resp_protocol_version);
          for (m, _) in &members {
            output.write_resp_bulk_string(m);
          }
        }
        // count<=0（含 0 与负 count 可重复）：数组头（C# WriteArrayLength）
        Some(_) => {
          output.write_resp_array_len(members.len());
          for (m, _) in &members {
            output.write_resp_bulk_string(m);
          }
        }
      }
      Ok(true)
    }

    // 未支持操作一律穿透（Ok(false)）：由 run_async_rmw 物化降级通道接手，
    // 杜绝静默兜底输出与命令语义无关的应答。删除重的 SREM / SPOP 同在此穿透
    // （无树内逐成员删除臂），见本模块头注「墓碑恒低」
    SetOperation::Srem
    | SetOperation::Spop
    | SetOperation::Sscan
    | SetOperation::Smove
    | SetOperation::Sunion
    | SetOperation::Sunionstore
    | SetOperation::Sdiff
    | SetOperation::Sdiffstore
    | SetOperation::Sinter
    | SetOperation::Sinterstore => Ok(false),
  }
}

/// 执行分层态有序集合命令（WATCH 栅栏由 [`finish_tiered_arm`] 统一收尾）
pub(crate) async fn exec_tiered_zset<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, SortedSetOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let handled = tiered_zset_arm(session, key, ctx, call, output).await;
  finish_tiered_arm(session, key, ctx, handled)
}

/// 分层态有序集合命令树内主体（读写臂分派，见 [`exec_tiered_zset`]）
async fn tiered_zset_arm<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, SortedSetOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let TieredCollectionArgs {
    op,
    args12,
    args,
    resp_protocol_version,
  } = call;
  // 写臂独占互斥（锁内刷新元记录），读臂共享锁（判定一处定义）
  let Some(tree_guard) = tiered_guard(session, key, ctx, zset_needs_write(op)).await? else {
    return Ok(false);
  };
  let tree = tree_guard.tree();

  match op {
    SortedSetOperation::Zadd => {
      use wresp::options::{SortedSetAddOption, try_get_sorted_set_add_option};

      // ---- GetOptions 选项段（libs/server/Objects/SortedSet/
      // SortedSetObjectImpl.cs:GetOptions：XX&NX 互斥、NX/GT/LT 两两互斥、
      // INCR 仅单对、剩余 token 非空且成对）
      let mut options = SortedSetAddOption::NONE;
      let mut curr = 0usize;
      while curr < args.len()
        && let Some(opt) = try_get_sorted_set_add_option(args[curr])
      {
        options |= opt;
        curr += 1;
      }
      let options_error: Option<&'static str> = if options.contains(SortedSetAddOption::XX)
        && options.contains(SortedSetAddOption::NX)
      {
        Some(cs::RESP_ERR_XX_NX_NOT_COMPATIBLE)
      } else if (options.contains(SortedSetAddOption::GT)
        && options.contains(SortedSetAddOption::LT))
        || ((options.contains(SortedSetAddOption::GT) || options.contains(SortedSetAddOption::LT))
          && options.contains(SortedSetAddOption::NX))
      {
        Some(cs::RESP_ERR_GT_LT_NX_NOT_COMPATIBLE)
      } else if options.contains(SortedSetAddOption::INCR) && args.len() - curr > 2 {
        Some(cs::RESP_ERR_INCR_SUPPORTS_ONLY_SINGLE_PAIR)
      } else {
        None
      };
      if let Some(err) = options_error {
        cs::write_error_raw(output, err);
        return Ok(true);
      }
      if curr == args.len() || !(args.len() - curr).is_multiple_of(2) {
        cs::write_error_raw(output, cs::RESP_ERR_GENERIC_SYNTAX_ERROR);
        return Ok(true);
      }

      // 预校验先于任何写入（RI 批量口径：任一成员越契约即整体失败、零树内副作用；
      // (score, member) 成对排布，成员即 pair[1]，分值恒 8B 裸记录落树，成员长度
      // 决定契约；分值解析失败仍由主循环保有原口径）
      for pair in args[curr..].as_chunks::<2>().0 {
        if !tiered_precheck(ctx, pair[1], size_of::<f64>(), None, output) {
          return Ok(true);
        }
      }

      // ---- SortedSetAdd 主循环（SortedSetObjectImpl.cs:SortedSetAdd：
      // 新增恒计数 / CH 计变更 / NX 过滤已存在 / GT/LT 分值比较 /
      // INCR 增量回 bulk string）；「插成功才计数」判据统一走 tree_put_ok
      let mut added_or_changed = 0_i64;
      let mut incr_result = 0_f64;
      let mut new_members = 0u64;
      let mut put_rejected = false;
      let now = now_ticks();
      // ZADD 收尾记账单点：新成员计数入账与元记录回写一处判定，提前出口与正常
      // 出口共用——保证任一出口处「计数与树内实存一一对应」（解析/NaN 等命令
      // 中途错误提前回包时，已落树的成员计数不得随早退丢失，否则 size 虚减
      // 反噬删空自愈判据）
      macro_rules! commit_new_members {
        () => {
          if new_members > 0 {
            ctx.meta.size += new_members;
            session
              .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
              .await
              .map_err(|_| ())?;
          }
        };
      }
      while curr < args.len() {
        let Some(score) = strict_f64(args[curr], true) else {
          commit_new_members!();
          cs::write_error_raw(output, cs::RESP_ERR_NOT_VALID_FLOAT);
          return Ok(true);
        };
        curr += 1;
        let member = args[curr];
        curr += 1;

        // 到期成员视同不存在（C# SortedSetAdd 入口先 DeleteExpiredItems）；
        // 物理覆盖对到期旧记录等价先删后加（size 不变），对存活旧记录任一
        // 写入分支都清字段 TTL（C# TryRemoveExpiration，含同分值分支）
        let state = tree_member_state(tree, member, now);
        let mut alive_score = None;
        if matches!(state, Some((_, false))) {
          tree.read_callback(member, |res, raw| {
            if res == BfTreeReadResult::Found {
              let (_, payload) = decode_member(raw);
              if payload.len() == 8
                && let Ok(arr) = <[u8; 8]>::try_from(payload)
              {
                alive_score = Some(f64::from_be_bytes(arr));
              }
            }
            res == BfTreeReadResult::Found
          });
        }

        match alive_score {
          None => {
            // 真新成员 / 已到期旧记录：XX 置位则不新增（到期视同不存在）
            if options.contains(SortedSetAddOption::XX) {
              continue;
            }
            incr_result = score;
            if tree_put_ok(ctx, tree, member, &score.to_be_bytes(), None) {
              if state.is_none() {
                new_members += 1;
              }
              added_or_changed += 1;
            } else {
              put_rejected = true;
            }
          }
          Some(old_score) => {
            let mut score = score;
            if options.contains(SortedSetAddOption::INCR) {
              score += old_score;
              incr_result = score;
              if score.is_nan() {
                commit_new_members!();
                cs::write_error_raw(output, cs::RESP_ERR_GENERIC_SCORE_NAN);
                return Ok(true);
              }
            }
            if score == old_score {
              // 同分值：仅清 TTL（零计数变更，C# 相同分支 TryRemoveExpiration）
              if !tree_put_ok(ctx, tree, member, &score.to_be_bytes(), None) {
                put_rejected = true;
              }
              continue;
            }
            // NX 置位，或 GT/LT 置位且现存分值高于/低于新分值 → 不更新
            if options.contains(SortedSetAddOption::NX)
              || (options.contains(SortedSetAddOption::GT) && old_score > score)
              || (options.contains(SortedSetAddOption::LT) && old_score < score)
            {
              if options.contains(SortedSetAddOption::INCR) {
                commit_new_members!();
                output.write_resp_null_ver(resp_protocol_version);
                return Ok(true);
              }
              continue;
            }
            if tree_put_ok(ctx, tree, member, &score.to_be_bytes(), None) {
              if options.contains(SortedSetAddOption::CH) {
                added_or_changed += 1;
              }
            } else {
              put_rejected = true;
            }
          }
        }
      }

      commit_new_members!();
      if put_rejected {
        tree_put_rejected(output);
        return Ok(true);
      }
      if options.contains(SortedSetAddOption::INCR) {
        // 分值数值（对标 SortedSetObjectImpl.cs:SortedSetAdd INCR 分支
        // WriteDoubleNumeric，RESP3 `,val`、RESP2 bulk）
        cs::write_double_numeric(output, incr_result, resp_protocol_version);
      } else {
        output.write_resp_int(added_or_changed);
      }
      Ok(true)
    }

    SortedSetOperation::Zscore => {
      if args.is_empty() {
        return Err(());
      }
      let member = args[0];
      let now = now_ticks();
      let mut score_opt = None;
      tree.read_callback(member, |res, raw| {
        if res == BfTreeReadResult::Found && !member_expired_at(raw, now) {
          let (_, payload) = decode_member(raw);
          if payload.len() == 8
            && let Ok(arr) = <[u8; 8]>::try_from(payload)
          {
            score_opt = Some(f64::from_be_bytes(arr));
          }
        }
        res == BfTreeReadResult::Found
      });
      if let Some(score) = score_opt {
        cs::write_double_numeric(output, score, resp_protocol_version);
      } else {
        output.write_resp_null_ver(resp_protocol_version);
      }
      Ok(true)
    }

    SortedSetOperation::Zmscore => {
      let now = now_ticks();
      output.write_resp_array_len(args.len());
      for &member in args {
        let mut score_opt = None;
        tree.read_callback(member, |res, raw| {
          if res == BfTreeReadResult::Found && !member_expired_at(raw, now) {
            let (_, payload) = decode_member(raw);
            if payload.len() == 8
              && let Ok(arr) = <[u8; 8]>::try_from(payload)
            {
              score_opt = Some(f64::from_be_bytes(arr));
            }
          }
          res == BfTreeReadResult::Found
        });
        if let Some(score) = score_opt {
          cs::write_double_numeric(output, score, resp_protocol_version);
        } else {
          output.write_resp_null_ver(resp_protocol_version);
        }
      }
      Ok(true)
    }

    SortedSetOperation::Zcard => {
      // 计数校正（同分层 Hlen 臂，见 collect_expired_members）
      if collect_expired_members(ctx, tree) {
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
      }
      output.write_resp_int(ctx.meta.size as i64);
      Ok(true)
    }

    SortedSetOperation::Zincrby => {
      if args.len() < 2 {
        return Err(());
      }
      let Some(incr) = strict_f64(args[0], false) else {
        return Err(());
      };
      let member = args[1];
      let now = now_ticks();
      let mut cur_score = 0.0f64;
      let mut is_new = true;
      // 存活成员增量保留既有 TTL（C# SortedSetIncrby 不动 expiration）；到期
      // 旧记录视同不存在（C# 增量入口先 DeleteExpiredItems）
      let mut old_expiry = None;
      tree.read_callback(member, |res, raw| {
        if res == BfTreeReadResult::Found {
          let (expiry, payload) = decode_member(raw);
          if member_expired_at(raw, now) {
            return true;
          }
          is_new = false;
          old_expiry = expiry;
          if payload.len() == 8
            && let Ok(arr) = <[u8; 8]>::try_from(payload)
          {
            cur_score = f64::from_be_bytes(arr);
          }
        }
        res == BfTreeReadResult::Found
      });
      let new_score = cur_score + incr;
      let score_record = new_score.to_be_bytes();
      // 预校验先于写入（RI 单点口径，零副作用）；编码记录长度随 TTL 头形态换算
      if !tiered_precheck(ctx, member, score_record.len(), old_expiry, output) {
        return Ok(true);
      }
      // 「插成功才计数」：写成才计新成员并入账回分值（libs/server/Objects/
      // SortedSet/SortedSetObjectImpl.cs:SortedSetIncrby 的等价判据）
      if !tree_put_ok(ctx, tree, member, &score_record, old_expiry) {
        tree_put_rejected(output);
        return Ok(true);
      }
      if is_new {
        ctx.meta.size += 1;
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
      }
      // 分值数值（对标 SortedSetObjectImpl.cs:SortedSetIncrby WriteDoubleNumeric）
      cs::write_double_numeric(output, new_score, resp_protocol_version);
      Ok(true)
    }

    // ZEXPIRE / ZEXPIREAT / ZPEXPIRE / ZPEXPIREAT 树内原地臂（arg1/arg2 为
    // ExpirationWithOption 压缩字，libs/server/Objects/SortedSet/
    // SortedSetObjectImpl.cs:SortedSetExpire + SortedSetObject.cs:SetExpiration
    // 闸门口径，与 hash Hexpire 臂同型）
    SortedSetOperation::Zexpire => {
      let e = ExpirationWithOption::from_word_head_tail(args12.0, args12.1);
      let ticks = e.expiration_time_in_ticks();
      let option = e.expire_option();
      let now = now_ticks();
      let mut changed = false;
      output.write_resp_array_len(args.len());
      for &member in args {
        let (result, arm_changed) = member_expire_arm(ctx, tree, member, ticks, option, now);
        changed |= arm_changed;
        output.write_resp_int(i64::from(result));
      }
      if changed {
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
      }
      Ok(true)
    }

    // ZTTL / ZPTTL / ZEXPIRETIME / ZPEXPIRETIME 树内原地臂（同 Httl 口径）
    SortedSetOperation::Zttl => {
      let (is_milliseconds, is_timestamp) = (args12.0 == 1, args12.1 == 1);
      let now = now_ticks();
      let mut removed = false;
      output.write_resp_array_len(args.len());
      for &member in args {
        let (raw, expired) = member_ttl_probe(ctx, tree, member, now);
        removed |= expired;
        let result = if raw >= 0 {
          expiry_tick_to_reply(raw, is_milliseconds, is_timestamp, now)
        } else {
          raw
        };
        output.write_resp_int(result);
      }
      if removed {
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
      }
      Ok(true)
    }

    // ZPERSIST 树内原地臂（同 Hpersist 口径）
    SortedSetOperation::Zpersist => {
      let now = now_ticks();
      let mut changed = false;
      output.write_resp_array_len(args.len());
      for &member in args {
        let (result, arm_changed) = member_persist_arm(ctx, tree, member, now);
        changed |= arm_changed;
        output.write_resp_int(i64::from(result));
      }
      if changed {
        session
          .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
          .await
          .map_err(|_| ())?;
      }
      Ok(true)
    }

    // 未支持操作一律穿透（Ok(false)）：由 run_async_rmw 物化降级通道接手，
    // 杜绝静默兜底输出与命令语义无关的应答——ZPOPMIN / ZPOPMAX / ZREMRANGEBY*
    // / GEOADD / ZRANGESTORE 等写族经此落 wcol 对象层单源真实删改，WATCH 推进
    // 同臂由 apply_rmw_post_operate 承接（旧兜底臂把它们应答成整表 ZRANGE 形态
    // 且零树删除，客户端见成功而数据未动，是比漏栅栏更重的语义缺陷）。
    // ZREM 同在此穿透（无树内逐成员删除臂，见本模块头注「墓碑恒低」）
    _ => Ok(false),
  }
}

/// 分层列表头端序号（C# `LinkedList.First` 指针的分层态对位）
///
/// 树最左键一次定位：`scan_cnt=1` 只界定**返回条数**、不界定**遍历条数**——
/// 底层单趟游标对每条记录先做墓碑判定（`leaf_node.rs` 的 `is_absent()` 臂排在
/// `bound_key` 比较之前并直接返回 Deleted），再对存活记录扣减 scan_cnt，故本臂
/// 「一次下降 + 一条记录即返回」的 O(1) 只对**无墓碑树**成立；游标之后若存在连续
/// 墓碑，那一段必须在**单次 `next()` 内**逐条跳完（实测每跳一条压一帧 ≈680B，
/// 早停回调与 count 都来不及生效），页读次数与栈深度皆随连跑长度增长
/// （实测分档与机理：task/reject/tiered-zset-demote-stack.md 二.表 S1..S4 与一.表）。
/// 故本臂的栈安全性完全依赖「树内墓碑恒低」这一写形不变量，而该不变量由删除不
/// 逐成员落树、统一走整值重灌面保证（见 `rmw_helpers::apply_rmw_post_operate`）。
///
/// 序号编码单点在 wcol：`ListObject::export_entries`
/// （wcol/src/types/garnet_object.rs:243）以 `(LIST_SEQ_BASE + 位次)` 的 u128
/// 16B 大端导出元素，升阶与重灌同源；本函数与之同基准。曾有的第二形态
/// （按 u64 位次 8B 导出、信封枚举内另写一份 16B 映射）已删——位置索引与
/// 序号窗口不可混用，混用即令 push 分配的序号与既有键错开整段。
///
/// 尾端无需第二次定位：分层列表序号占用区恒为连续区间
/// `[头, 头 + meta.size - 1]`，因三个写入面都保持连续排布——
/// - 升阶 bulk_load（`IGarnetObject::export_entries` 逐类型委派，
///   wcol/src/types/garnet_object.rs:453）自 `LIST_SEQ_BASE` 起按元素序连续排布；
/// - RPUSH 自尾 +1 向上、LPUSH 自头 -n 向下（本函数唯一消费方，见
///   [`tiered_list_arm`] 的 push 双臂）；
/// - 两端摘除（LPOP/RPOP）与中段删改（LREM/LTRIM/LINSERT/LSET）均无树内臂，
///   一律经 [`tiered_materialize_blob`] 物化后整树重灌，重灌仍走连续排布。
///
/// 故不把头尾游标另存进元记录：那会把「树 + 已持久化 meta.size」本可推出的
/// 派生量变成第二份真值源（写侧任一维护点漏改即与树漂移、序号错乱），且
/// 两个 u128 字段会把全体集合类型共用的 24B [`MetaValue`] 撑到 64B（u128 强制
/// 16 字节对齐，wval/src/meta.rs:72 的 `repr(C, align(8))` 布局与单缓存行双条
/// 记录口径一并作废）。C# 侧游标是链表节点的内存指针、从不落盘，本函数按同一
/// 语义在树内直取，元记录仍只持久化 `size` 一处计数。
///
/// 空树（`MetaValue::is_live` 已挡掉 size 为 0 的元记录，正常不可达）回落
/// `LIST_SEQ_BASE`，与升阶排布同基准。
#[inline]
fn list_head_seq(tree: &BfTreeService) -> u128 {
  let mut head = LIST_SEQ_BASE;
  let _ = tree.scan_with_count_callback(&[0u8], 1, ScanReturnField::Key, |k, _| {
    if let Ok(arr) = <[u8; 16]>::try_from(k) {
      head = u128::from_be_bytes(arr);
    }
    false
  });
  head
}

/// 执行分层态列表命令（WATCH 栅栏由 [`finish_tiered_arm`] 统一收尾）
pub(crate) async fn exec_tiered_list<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, ListOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let handled = tiered_list_arm(session, key, ctx, call, output).await;
  finish_tiered_arm(session, key, ctx, handled)
}

/// 分层态列表命令树内主体（读写臂分派，见 [`exec_tiered_list`]）
async fn tiered_list_arm<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  call: TieredCollectionArgs<'_, ListOperation>,
  output: &mut Vec<u8>,
) -> Result<bool, ()> {
  let TieredCollectionArgs {
    op,
    args12,
    args,
    resp_protocol_version,
  } = call;
  let (arg1, arg2) = args12;
  // 写臂独占互斥（锁内刷新元记录），读臂共享锁（判定一处定义）
  let Some(tree_guard) = tiered_guard(session, key, ctx, list_needs_write(op)).await? else {
    return Ok(false);
  };
  let tree = tree_guard.tree();

  match op {
    ListOperation::Rpush | ListOperation::Rpushx => {
      // 序号 u128 16B 大端（保序）：RPUSH 自尾 +1 向上连续分配，尾 = 头 + size - 1
      // （头端由 [`list_head_seq`] 一次定位，等价 C# ListPush 的 list.AddLast
      // ListObjectImpl.cs:229-244 O(1)，不再扫树求 cur_max）；LIST_SEQ_BASE 距
      // u128 上界留有 2^128 量级空间，向上永不环绕
      let base = list_head_seq(tree) + ctx.meta.size as u128;
      // 预校验先于任何写入（RI 批量口径：任一元素越契约即整体失败、零树内副作用；
      // 序号恒 16B 键，落树记录为元素编码后长）
      for (idx, &item) in args.iter().enumerate() {
        let seq = (base + idx as u128).to_be_bytes();
        if !tiered_precheck(ctx, &seq, item.len(), None, output) {
          return Ok(true);
        }
      }
      // 「插成功才计数」：pushed 与树内实存一一对应，size 即应答长度
      let mut pushed = 0u64;
      let mut put_rejected = false;
      for (idx, &item) in args.iter().enumerate() {
        let seq = (base + idx as u128).to_be_bytes();
        if tree_put_ok(ctx, tree, &seq, item, None) {
          pushed += 1;
        } else {
          put_rejected = true;
        }
      }
      ctx.meta.size += pushed;
      session
        .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
        .await
        .map_err(|_| ())?;
      if put_rejected {
        tree_put_rejected(output);
        return Ok(true);
      }
      output.write_resp_int(ctx.meta.size as i64);
      Ok(true)
    }

    ListOperation::Lpush | ListOperation::Lpushx => {
      // LPUSH 自头 -n 向下连续分配（等价 C# ListPush 的 list.AddFirst
      // ListObjectImpl.cs:229-244 O(1)，不再扫树求 cur_min）：u128 低半区距
      // LIST_SEQ_BASE 留有 2^64 空间，saturating_sub 兜住下溢，绝不回绕覆盖
      // 尾端元素（删空即 drain_and_delete 销毁整树，下次推入重回基准，
      // 故该半区实际永不耗尽）
      let base = list_head_seq(tree).saturating_sub(args.len() as u128);
      // 命令内多参序：C# ListPush 与对象层 list_push
      // （wcol/src/list/list_object_impl.rs:248-266）同为「一参一次 AddFirst」的
      // 循环 ⇒ LPUSH k a b 落 [b, a, 旧…]，故序号自低向高按 args 逆序分配
      // （末参占最小序号 = 新头端），与内存态两态一致
      // 预校验先于任何写入（RI 批量口径，同 RPUSH 臂；序号恒 16B 键）
      for (idx, &item) in args.iter().rev().enumerate() {
        let seq = (base + idx as u128).to_be_bytes();
        if !tiered_precheck(ctx, &seq, item.len(), None, output) {
          return Ok(true);
        }
      }
      // 「插成功才计数」：pushed 与树内实存一一对应，size 即应答长度
      let mut pushed = 0u64;
      let mut put_rejected = false;
      for (idx, &item) in args.iter().rev().enumerate() {
        let seq = (base + idx as u128).to_be_bytes();
        if tree_put_ok(ctx, tree, &seq, item, None) {
          pushed += 1;
        } else {
          put_rejected = true;
        }
      }
      ctx.meta.size += pushed;
      session
        .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
        .await
        .map_err(|_| ())?;
      if put_rejected {
        tree_put_rejected(output);
        return Ok(true);
      }
      output.write_resp_int(ctx.meta.size as i64);
      Ok(true)
    }

    ListOperation::Llen => {
      output.write_resp_int(ctx.meta.size as i64);
      Ok(true)
    }

    ListOperation::Lindex => {
      let len = ctx.meta.size as i64;
      let idx = if arg1 < 0 {
        len + i64::from(arg1)
      } else {
        i64::from(arg1)
      };
      if idx < 0 || idx >= len {
        output.write_resp_null_ver(resp_protocol_version);
        return Ok(true);
      }
      // 顺序定位第 idx 个（0-based；负索引已折算）
      let mut skipped = 0i64;
      let mut found: Option<Vec<u8>> = None;
      let _ = tree.scan_with_count_callback(&[0u8], usize::MAX, ScanReturnField::Value, |_, v| {
        if skipped < idx {
          skipped += 1;
          return true;
        }
        found = Some(decode_member(v).1.to_vec());
        false
      });
      match found {
        Some(v) => output.write_resp_bulk_string(&v),
        None => output.write_resp_null_ver(resp_protocol_version),
      }
      Ok(true)
    }

    ListOperation::Lrange => {
      let start_idx = i64::from(arg1);
      let stop_idx = i64::from(arg2);
      let mut items = Vec::new();
      let _ = tree.scan_with_count_callback(&[0u8], usize::MAX, ScanReturnField::Value, |_, v| {
        items.push(decode_member(v).1.to_vec());
        true
      });
      let len = items.len() as i64;
      let start = if start_idx < 0 {
        (len + start_idx).max(0) as usize
      } else {
        start_idx.min(len) as usize
      };
      let stop = if stop_idx < 0 {
        (len + stop_idx).max(0) as usize
      } else {
        stop_idx.min(len.saturating_sub(1)) as usize
      };
      if start > stop || start >= items.len() {
        output.extend_from_slice(cs::RESP_EMPTYLIST);
      } else {
        let slice = &items[start..=stop.min(items.len().saturating_sub(1))];
        output.write_resp_array_len(slice.len());
        for item in slice {
          output.write_resp_bulk_string(item);
        }
      }
      Ok(true)
    }

    // 未支持操作一律穿透（Ok(false)）：由 run_async_rmw 物化降级通道接手，
    // 杜绝静默兜底输出与命令语义无关的应答。LPOP / RPOP 同在此穿透（弹出臂已
    // 摘除，树内无逐成员删除，见本模块头注「墓碑恒低」）
    _ => Ok(false),
  }
}

/// 分层树物化为内存信封载荷（对标 C# 单记录对象域：Garnet 对象常驻存储层，
/// 任意命令对任意规模对象语义恒定；rust 分层态未实现树内原语的操作经本通道
/// 一次性物化回 wcol 对象，走对象层单源求值后按升阶/降阶判据回写）
///
/// `Ok(Some(blob))` 物化载荷（与信封剥壳后格式一致，可直接喂 `from_blob`）；
/// `Ok(None)` 键非分层态（调用方维持既有装载路径）；`Err(())` 存储 IO 失败或
/// 树记录载荷损坏（fail-fast 中止物化，调用方不得回写，杜绝固化成员丢失）
pub(crate) async fn tiered_materialize_blob<D: Device>(
  session: &StoreSession<D>,
  key: &[u8],
  tag: GarnetObjectType,
) -> Result<Option<Vec<u8>>, ()> {
  use wcol::{
    hash::hash_object::HashObject, list::list_object::ListObject,
    object_payload::GarnetObjectPayload, set::set_object::SetObject,
    zset::sorted_set_object::SortedSetObject,
  };

  let Some((meta, stub)) = session.load_collection_stub(key).await.map_err(|_| ())? else {
    return Ok(None);
  };
  if meta.collection_type != tag {
    return Ok(None);
  }
  let tree_guard = session
    .acquire_tree_read(key, &stub)
    .await
    .map_err(|_| ())?;
  let tree = tree_guard.tree();

  // 物化承载字段级 TTL：挂 TTL 成员还原进对象 expiration 结构（未到期），
  // 已到期成员剔除不复活（对齐 C# 对象构造函数装载口径：SortedSetObject.cs
  // 的 ExpirationBitMask 分支 canAddItem = expiration >= UtcNow.Ticks）；
  // 往返「内存态 → 升阶 → 物化 → 再升阶」逐字段 TTL 保真
  let now = now_ticks();
  let blob = match tag {
    GarnetObjectType::Hash => {
      let mut obj = HashObject::new();
      let _ =
        tree.scan_with_count_callback(&[0u8], usize::MAX, ScanReturnField::KeyAndValue, |k, v| {
          let (expiry, payload) = decode_member(v);
          if expiry.is_some_and(|ticks| ticks < now) {
            return true;
          }
          let item = k.to_vec();
          obj.update_size(&item, payload, true);
          obj.hash.insert(item.clone(), payload.to_vec());
          if let Some(ticks) = expiry {
            obj.insert_expiration(item, ticks);
          }
          true
        });
      obj.to_blob()
    }
    GarnetObjectType::Set => {
      let mut obj = SetObject::new();
      let _ = tree.scan_with_count_callback(&[0u8], usize::MAX, ScanReturnField::Key, |k, _| {
        obj.set.insert(k.to_vec());
        true
      });
      obj.to_blob()
    }
    GarnetObjectType::SortedSet => {
      let mut obj = SortedSetObject::from_entries(Vec::with_capacity(meta.size as usize));
      // 树记录分值载荷恒为编码侧单源落下的 8B f64 大端；非 8B 即编码损坏或
      // codec 缺陷——fail-fast 中止物化（Err 上抛，调用方不写回），与
      // exec_tiered_scan 扫描臂的显式失败口径共用错误面，严禁静默剔除成员后
      // 照常回写固化丢失（对照 C# GarnetObjectSerializer.DeserializeInternal
      // 抛异常可见失败）
      let mut corrupt = false;
      let _ =
        tree.scan_with_count_callback(&[0u8], usize::MAX, ScanReturnField::KeyAndValue, |k, v| {
          let (expiry, payload) = decode_member(v);
          if expiry.is_some_and(|ticks| ticks < now) {
            return true;
          }
          let Ok(arr) = <[u8; 8]>::try_from(payload) else {
            log::error!(
              "tiered_materialize_blob: corrupted zset score payload, key='{}' member={:?}",
              String::from_utf8_lossy(key),
              k
            );
            corrupt = true;
            return false;
          };
          let score = f64::from_be_bytes(arr);
          let member = k.to_vec();
          obj.update_size(&member, true);
          obj.sorted_set_dict.insert(member.clone(), score);
          if let Some(ticks) = expiry {
            obj.insert_expiration(member, ticks);
          }
          true
        });
      if corrupt {
        return Err(());
      }
      obj.to_blob()
    }
    GarnetObjectType::List => {
      let mut obj = ListObject::new();
      let _ =
        tree.scan_with_count_callback(&[0u8], usize::MAX, ScanReturnField::KeyAndValue, |_, v| {
          // 树内键为序号（大端保序），扫描序即元素序
          obj.list.push_back(decode_member(v).1.to_vec());
          true
        });
      obj.to_blob()
    }
    _ => return Ok(None),
  };
  Ok(Some(blob))
}

/// 分层键字段级到期收集执行体（显式 HCOLLECT / ZCOLLECT 单键、`*` 全库周期
/// 对象收集任务与计数慢路径校正共用；树内删除漏斗 [`collect_expired_members`]
/// 唯一内核）
///
/// `Ok(Some(size))` 分层键已处理并返回校正后存活计数（含零到期零写）；
/// `Ok(None)` 键不在分层态或类型不符；`Err(())` 存储 IO 失败
pub(crate) async fn exec_tiered_collect<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
) -> Result<Option<u64>, ()> {
  let Some((mut meta, mut stub)) = session.load_collection_stub(key).await.map_err(|_| ())? else {
    return Ok(None);
  };
  if meta.collection_type != tag {
    return Ok(None);
  }
  let mut ctx = TieredCtx::new(&mut meta, &mut stub);
  // 收集执行体是写臂（到期成员物理出账）：独占写锁 + 锁内刷新元记录，
  // 键已被并发排空回收则按「键非分层态」穿透
  let Some(tree_guard) = tiered_guard(session, key, &mut ctx, true).await? else {
    return Ok(None);
  };
  let changed = collect_expired_members(&mut ctx, tree_guard.tree());
  if changed {
    drain_or_save(session, key, &mut ctx, tree_guard).await?;
    // 到期成员物理出账即客户端可见变更，推进 WATCH 版本栅栏（对标 C#
    // HashCollect 走 RMW 写钩子 IncrementVersion；零到期零写不推进）
    session.bump_watch_version(key);
  }
  Ok(Some(ctx.meta.size))
}

/// 分层态 SCAN 族（HSCAN/SSCAN/ZSCAN）树内游标扫描
///
/// 对标 SharedObjectCommands.cs 的 ObjectScan 分层态臂——C# 对任意规模
/// 对象恒可用，升阶键不得回内部错误。游标复刻 wcol 对象层 scan 口径（
/// hash_object.rs:scan / set_object.rs:scan / sorted_set_object.rs:scan 单源）：
/// 起始游标 = 已扫条目计数，单轮收集匹配项至 count 截断（成员+值对计数），
/// 扫至树尾光标归零；MATCH 全局通配、COUNT 钳制 OBJECT_SCAN_COUNT_LIMIT、
/// NOVALUES（zset 对齐对象层忽略该旗标）逐项一致。树内流式输出，内存
/// O(单轮 items)，不做全量物化
///
/// 栈深度口径（勿按「COUNT 截断」误读为成本有界）：起始游标以「跳过 start 条」
/// 实现，故本臂自树头起遍历，**遍历**条数与单帧深度都不受 COUNT 约束——底层游标
/// 对墓碑的尾递归自调必须在单次 `next()` 内跳完游标后的整段连跑（≈680B/条），
/// 回调早停与 count 均来不及生效（实测 task/reject/tiered-zset-demote-stack.md
/// 二.表 S4）。本臂的安全性来自「树内墓碑恒低」的写形不变量，不来自本函数的截断
///
/// `Ok(true)` 已闭环应答；`Ok(false)` 键非分层态（调用方维持既有路径）；
/// `Err(())` 存储 IO 失败
pub(crate) async fn exec_tiered_scan<D: Device>(
  session: &StoreSession<D>,
  key: &[u8],
  object_type: GarnetObjectType,
  args: &[&[u8]],
  scan_count_limit: i32,
  output: &mut Vec<u8>,
  resp_protocol_version: u8,
) -> Result<bool, ()> {
  use wbase::glob::glob_match;
  use wcol::types::scan_input::read_scan_input;

  let Some((meta, stub)) = session.load_collection_stub(key).await.map_err(|_| ())? else {
    return Ok(false);
  };
  if meta.collection_type != object_type {
    cs::write_error_raw(output, cs::RESP_ERR_WRONG_TYPE);
    return Ok(true);
  }

  // ReadScanInput 单源解析（与对象层同一解析函数）
  let params = match read_scan_input(args, scan_count_limit) {
    Ok(params) => params,
    Err(msg) => {
      // 错误文本为 ASCII 常量（INVALIDCURSOR/SYNTAX/NOT_INTEGER）
      cs::write_error_raw(output, str::from_utf8(msg).unwrap_or(""));
      return Ok(true);
    }
  };
  let start = params.cursor;
  // 对象层截断口径：hash/zset 每成员占 2 项（成员 + 分值/值）
  let want = match object_type {
    GarnetObjectType::Hash if !params.is_no_value => params.count * 2,
    GarnetObjectType::SortedSet => params.count * 2,
    _ => params.count,
  };
  let return_field = if object_type == GarnetObjectType::Hash && params.is_no_value {
    ScanReturnField::Key
  } else {
    ScanReturnField::KeyAndValue
  };

  let tree_guard = session
    .acquire_tree_read(key, &stub)
    .await
    .map_err(|_| ())?;
  let tree = tree_guard.tree();

  // 纯读面不回写 meta（水位由计数臂 / 收集执行体收敛），到期成员只过滤
  // 不出产出（C# 对象层 Scan 前经 DeleteExpiredItems 的过滤等价）
  let now = now_ticks();
  // None 项 = 分值文本化失败（对齐对象层 RESP null 项回写）
  let mut items: Vec<Option<Vec<u8>>> = Vec::new();
  let mut skipped = 0_i64;
  let mut scanned = 0_i64;
  let mut expired = 0_i64;
  let _ = tree.scan_with_count_callback(&[0u8], usize::MAX, return_field, |k, v| {
    // 起始游标之前的条目跳过（不产出，推进已扫计数）
    if skipped < start {
      skipped += 1;
      return true;
    }
    if object_type != GarnetObjectType::List && member_expired_at(v, now) {
      // 到期成员计入光标基数（对齐 C# scan 的 expiredKeysCount 口径，
      // hash_object.rs:scan 尾段 cursor + expired_keys_count == len 判定）
      expired += 1;
      return true;
    }
    if params.pattern.is_empty() || glob_match(params.pattern, k) {
      match object_type {
        GarnetObjectType::SortedSet => {
          let (_, payload) = decode_member(v);
          items.push(Some(k.to_vec()));
          // 分值文本化失败（±inf/NaN）以 None 表达（C# Utf8Formatter 失败写 null）
          items.push(if payload.len() == 8 {
            let score = f64::from_be_bytes(<[u8; 8]>::try_from(payload).unwrap());
            if score.is_finite() {
              let mut fbuf = ZmijBuffer::new();
              Some(format_double(score, &mut fbuf).as_bytes().to_vec())
            } else {
              None
            }
          } else {
            None
          });
        }
        GarnetObjectType::Hash => {
          items.push(Some(k.to_vec()));
          if !params.is_no_value {
            items.push(Some(decode_member(v).1.to_vec()));
          }
        }
        _ => items.push(Some(k.to_vec())),
      }
    }
    scanned += 1;
    // C# 以相等判断截断（负 COUNT 恒不命中 → 全量遍历；count=0 首个
    // 未命中条目即停的上游怪癖一并 1:1 保留）
    (items.len() as i64) != want
  });

  // 扫至树尾光标归零（对象层 cursor + expired_keys_count == size 口径）
  let next_cursor = if start + scanned + expired >= meta.size as i64 {
    0
  } else {
    start + scanned
  };

  output.write_resp_array_len(2);
  let mut cur_buf = ItoaBuffer::new();
  output.write_resp_bulk_string(cur_buf.format(next_cursor).as_bytes());
  if items.is_empty() {
    output.extend_from_slice(cs::RESP_EMPTYLIST);
  } else {
    output.write_resp_array_len(items.len());
    for item in &items {
      match item {
        Some(bytes) => output.write_resp_bulk_string(bytes),
        None => output.write_resp_null_ver(resp_protocol_version),
      }
    }
  }
  Ok(true)
}
