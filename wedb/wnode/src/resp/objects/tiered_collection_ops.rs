//! 自适应分层集合引擎：基于 wbftree 的分页分层态操作（Hash/Set/ZSet/List）
//!
//! 当集合规模达到升阶阈值（条目数 >= 65,536 或 体积 >= 4MB）时，
//! 自动就地升阶转换为基于 wbftree 的独立页级持久化树，基于 B+ 树页级冷热置换
//! 实现千万级海量存储，彻底消除全量反序列化读放大。
//!
//! 写形不变量「树内零墓碑」：本模块**没有**任何按成员往树里落删除记录的写臂
//! （旧 `tree_del` / `tree_del_batch` 漏斗已随成员级 TTL 面收口删净）。删除重的
//! 写命令（HDEL / SREM / SPOP / ZREM / LPOP / RPOP）与成员级 TTL 面
//! （HEXPIRE / HTTL / HPERSIST / ZEXPIRE / ZTTL / ZPERSIST 族）一律穿透至
//! `rmw_helpers::run_async_rmw` 的物化降级通道 → wcol 对象层单源求值 →
//! `apply_rmw_post_operate` 整值写回（删空即整键消亡，否则
//! `promote_collection_to_bftree` 先建快照后原子换入重灌，零墓碑）。该形与 C#
//! 一致：C# 集合对象整值常驻对象域、删即就地改内存对象、删空即整键消亡
//!（garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:188-215
//! `PostCopyUpdater` → `value.Operate` → `HasRemoveKey`），既无按成员分记录的
//! 树、也就无墓碑连跑。
//!
//! 本不变量是硬要求而非优化：底层 bf-tree 的 `ScanIter::next` 对墓碑记录是
//! **尾递归自调**（rustc 无 TCO），跳过一条墓碑压一帧（实测 ≈680B/帧），且墓碑
//! 判定排在界键比较之前、`scan_cnt` 只在存活分支递减 ⇒ 游标后的连续墓碑必须在
//! 单次 `next()` 内跳完，COUNT / 上界键 / 早停回调一概截不断，8MiB 默认栈的安全
//! 边 ≈ 连续墓碑 8000 条。故任何一次扫描面调用（分层 HSCAN/SSCAN/ZSCAN 的
//! [`exec_tiered_scan`]、push 双臂用的 [`list_head_seq`]、后台降阶物化轮
//! [`tiered_materialize_blob`]）的栈深度都不由树内墓碑决定——树内根本没有墓碑
//!（机理锚点：wbftree `ScanIter::next` 墓碑尾递归自调与紧上界键不截断遍历的
//! 实测档案，见 `list_head_seq` 文注；裁决 B 收口命令级删除、本票收口成员级
//! TTL 残余源，两层收口后树内删除记录恒零）。
//!
//! 成员级 TTL 的惰性出账面（计数校正臂 HLEN / ZCARD、输出面校正臂 HGETALL /
//! HKEYS / HVALS、显式 HCOLLECT / ZCOLLECT 与周期对象收集任务）不穿透物化，
//! 而走本模块的到期重灌内核 [`expire_sweep_or_rebuild`]：水位命中时锁内单趟
//! 扫描收集存活全集，放守卫后整值重灌（先建快照后原子换入，与裁决 B 同一
//! 换入原语），树内同样零墓碑；水位未命中（`now <= next_expiry`，含水位刻度
//! 当刻）零树访问、直读恒精确。计数复杂度契约（已裁决口径，见
//! doc/zh/collection.md 大键 O(1) 计数规约第 3 条分层态补则）：稳态水位内
//! HLEN/ZCARD O(1) 直读 `MetaValue.size`；水位越过即首个计数命令 O(N) 物理
//! 出账一次（扫 + 有到期才重灌），水位前移后回归 O(1)——到期辅助索引会被
//! 索引侧墓碑与双写放大击穿「树内零墓碑」写形，单批限截断则必须有界删树，
//! 均不采；C# 同面亦非 O(1)（HashObject/SortedSetObject.Count 遍历
//! expirationTimes 字典，O(T) 内存只读）。

use core::str;
use std::{
  cmp::{Ordering, Reverse},
  collections::BinaryHeap,
  mem::swap,
  ops::Range,
  str::from_utf8,
  sync::Arc,
};

use itoa::Buffer as ItoaBuffer;
use wbase::{
  num::{strict_f64, strict_i32, strict_i64},
  time::now_ticks,
};
use wbftree::{
  BfTreeInsertResult, BfTreeReadResult, BfTreeService, RangeIndexStub, ScanReturnField,
};
use wcol::{
  SET_MEMBER_DUMMY_VALUE,
  hash::hash_object::HashOperation,
  list::list_object::ListOperation,
  set::set_object::SetOperation,
  types::{
    garnet_object::LIST_SEQ_BASE,
    member_ttl::{decode_member, encode_member_into, encoded_len, member_expired_at},
  },
  zset::{
    comparer::SortedSetComparer,
    sorted_set_object::{SortedSetEntry, SortedSetObject, SortedSetOperation, SortedSetRangeOpts},
    sorted_set_object_impl::{
      RangeArgError, SpecialRanges, parse_range_options, write_sorted_set_result_payload,
    },
  },
};
use wdev::Device;
use wkv::{BatchStoreSession, RangeIndexError, StoreSession, TreeGuard, validate_bftree_record};
use wresp::{cmd_strings as cs, ext::RespVecExt, resp_memory_writer::format_double};
use wval::{GarnetObjectType, MetaValue};
use zmij::Buffer as ZmijBuffer;

/// 分层态操作上下文（元记录 + 树存根的可变借用束 + 变更脏标记）
///
/// `dirty` 由本模块树内写漏斗 [`tree_put`] 在实际写入成功时置位、批量漏斗
/// [`tree_put_batch`] 的置脏交由调用臂按命令语义判定（覆盖写与重复成员在
/// 「新增计数」上分叉，见其文档）、到期重灌内核 [`expire_sweep_or_rebuild`]
/// 在实际出账时置位，是分层写臂 WATCH 版本栅栏推进的唯一判据（一处定义，
/// 见 [`finish_tiered_arm`]）
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

/// 分层树内批量写入漏斗：编码整批经排序批量 upsert 内核一次下刷
/// （栈上排序集中命中叶页，消除逐条 N 次引擎借用），返回真实新增键数——
/// 到期旧记录在树即不计新增，与逐条前探 `tree_member_state` 等价的
/// 「插成功才计数」批量判据（前查由内核单次借用内承担）
///
/// 置脏归调用方判定（本漏斗不置脏）：新增计数与「树内容是否实际变更」在
/// 覆盖写族上天然分叉，只有命令语义能定夺——
/// - HSET 新增/覆写字段：对标 C# HashObjectImpl.cs 的 HashSet 变更分支
///   同样重写记录 → 写成功即置脏；
/// - SADD 重复成员：C# SetObjectImpl.cs 的 Set 对已存
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

/// 升阶 / 重灌条目集的最早成员到期水位单点（条目为树内记录形态，经
/// [`wcol::types::member_ttl`] 单点 codec 解码；无挂 TTL 成员回 `i64::MAX`）。
/// 唯一调用方是 [`promote_collection_to_bftree`](wkv) 的水位入参（重灌换树
/// 不换内容，水位必须随灌入批在同一元记录落盘内前移，杜绝「重灌后假水位
/// MAX 骗过计数校正与周期收集」的正确性缺口）
pub(crate) fn earliest_expiry(entries: &[(Vec<u8>, Vec<u8>)]) -> i64 {
  entries
    .iter()
    .filter_map(|(_, record)| decode_member(record).0)
    .min()
    .unwrap_or(i64::MAX)
}

/// 分层树字段级到期单趟扫描内核（唯一，计数校正臂 / 输出面校正臂 / 显式
/// HCOLLECT·ZCOLLECT / 周期对象收集任务共用，杜绝第二套收集逻辑）
///
/// 水位快路径：`now <= meta.next_expiry` 时树内不存在已到期成员（成员刻度
/// `ticks < now` 严格判过期，水位刻度 `== now` 的成员要到下一刻度才到期），
/// 零树访问直回 `None`（`Ok(false)` 等价口径）。水位越过才全扫一遍：收集
/// **存活全集**（树内原始记录形态，含未到期 TTL 头——既是输出面数据源，
/// 也是整值重灌的灌入批）并计数到期成员，重算最早到期水位写回
/// `ctx.meta.next_expiry`。判定收在 `<=` 而非 `<`，是计数 O(1) 契约的
/// off-by-one 收口：若在 `now == next_expiry`（无一到期）也开扫，水位原值
/// 不动，该刻度窗口内每条计数命令都重复 O(N) 全扫——收口后「扫 ⇒ 必有
/// 成员实际到期」成为不变量，每到期纪元至多一扫（契约口径见
/// doc/zh/collection.md 大键 O(1) 计数规约第 3 条分层态补则）。
///
/// 记账口径：树内「已到期未删除」成员由 `size` 承载、由调用方经
/// [`expire_sweep_or_rebuild`] 一次性出账——无成员级确认态标量（member 级
/// 状态无法无损汇入单一标量，刻意不设），两态计数等价由「读臂过滤 + 计数臂
/// 校正 + 周期收集兜底」三层闭环保证。
/// 到期全扫产物：`(存活条目全集, 到期被剔除计数)`
type SweptLiveEntries = (Vec<(Vec<u8>, Vec<u8>)>, u64);

fn sweep_expired_members(
  ctx: &mut TieredCtx<'_>,
  tree: &BfTreeService,
) -> Option<SweptLiveEntries> {
  let now = now_ticks();
  // `<=`：水位刻度成员此刻尚未到期（`ticks < now` 严格），零树访问直回；
  // 越过水位才必有到期可出账（off-by-one 收口，见上文水注文）
  if now <= ctx.meta.next_expiry {
    return None;
  }
  let mut live: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
  let mut expired = 0u64;
  let mut next_expiry = i64::MAX;
  let _ =
    tree.scan_with_count_callback(&[0u8], usize::MAX, ScanReturnField::KeyAndValue, |k, v| {
      match decode_member(v).0 {
        // 到期成员不入存活集（对齐 C# 对象层读路径 DeleteExpiredItems 的
        // 「先清后算」口径，出账经重灌完成，树内零墓碑）
        Some(ticks) if ticks < now => expired += 1,
        Some(ticks) => {
          next_expiry = next_expiry.min(ticks);
          live.push((k.to_vec(), v.to_vec()));
        }
        None => live.push((k.to_vec(), v.to_vec())),
      }
      true
    });
  ctx.meta.next_expiry = next_expiry;
  Some((live, expired))
}

/// 到期重灌执行结果：水位越过（`now > next_expiry`，必有成员已到期）后守卫
/// 与存活全集的去向
enum SweepOutcome<'a> {
  /// 水位未越过：零树访问，树守卫原样奉还（调用方走流式快路径直读）
  Below(TreeGuard<'a>),
  /// 水位越过：单趟全扫已完成，存活全集经 `drain_live` 闭包单次遍历交出
  ///（输出面收集单点，免二次扫树）；`expired > 0` 时已整值重灌出账（守卫已
  /// 释、计数已扣、置脏），`expired == 0` 时仅水位前移回写（树内容零变更，
  /// 仅水位落后者：HPERSIST 后残低的旧水位一次扫正）
  Swept { expired: u64 },
}

/// 分层到期出账统一执行体（树内零墓碑的出账形态，替代旧的逐成员树内删除）：
/// 单趟扫描 → `drain_live` 交出存活全集 → 有到期即**整值重灌**（放守卫 →
/// promote 先建后拆：内核建树快照后经 publish 原子换入，旧树全程可读），
/// 与命令级删除（HDEL/ZREM 等）走的 `apply_rmw_post_operate` 同一换入原语。
///
/// `drain_live` 在重灌前对存活全集恰好一次只读遍历（计数臂传 `|_| {}` 即弃，
/// 输出面臂在此收集应答数据——重灌会 move 走全集且旧树随之销毁，闭包是输出
/// 面读取存活数据的唯一窗口），零克隆零二次扫树。
///
/// 出账代价（读放大与锁窗口，与旧「批量树内删除」形对比）：水位越过那次计数
/// 由 O(1) 变 O(N)——锁内单趟全扫收集存活集，放锁后 O(N_live) 重灌写；水位
/// 未越过恒零树访问。旧形锁内扫 O(N) + 批量删 O(E)（E = 到期数），新形重灌
/// 恒 O(N_live)——出账批接近全集（如整树同时到期）时两形同阶，零星到期时
/// 新形贵出存活集写放大，这是换取「树内墓碑恒零 ⇒ 扫描栈深度自变量消失」
/// 的既定裁决代价（见模块头注）；`<=` 判定收口后该 O(N) 每到期纪元至多
/// 一次，契约口径见 doc/zh/collection.md 大键 O(1) 计数规约第 3 条分层态补则。
///
/// 删空自愈（严格删空生命周期）：存活全集为空 → `keep_ttl=false` 随键清 TTL
/// 整键回收（对齐 [`wkv handle_bftree_drain_and_delete`] 删键臂）；非空重灌
/// 键全程存活，换入臂不碰 TTL 旁路。`drop` 顺序固定：重灌前必先放树守卫
///（写臂持条带独占写锁，promote 换入侧自取同键条带写锁，守卫未放即互锁）。
/// 重灌后 `ctx.meta` / `ctx.stub` 为旧树副本（promote 已落新
/// 元记录），调用方不得再 `save_bftree_meta_stub` 覆写，仅可应答内存态 size
///（`dec_size` 后与 promote 落盘的 bulk_load 去重计数一致）
async fn expire_sweep_or_rebuild<'s, D: Device>(
  session: &'s BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  tree_guard: TreeGuard<'s>,
  drain_live: impl FnOnce(&[(Vec<u8>, Vec<u8>)]),
) -> Result<SweepOutcome<'s>, ()> {
  let Some((live, expired)) = sweep_expired_members(ctx, tree_guard.tree()) else {
    return Ok(SweepOutcome::Below(tree_guard));
  };
  let tag = ctx.meta.collection_type;
  if expired > 0 {
    drain_live(&live);
    ctx.meta.dec_size(expired);
    ctx.dirty = true;
    drop(tree_guard);
    if live.is_empty() {
      // 删空臂键消亡：keep_ttl=false 随键清 TTL，杜绝幽灵空元记录与孤儿 TTL
      session
        .handle_bftree_drain_and_delete(key, false)
        .await
        .map_err(|_| ())?;
    } else {
      // 整值重灌：先建后拆原子换入（promote replace=true 旧树全程可读，
      // 发布失败旧状态原样保留），键存活不碰 TTL 旁路，水位用扫描期已
      // 重算的 ctx.meta.next_expiry
      session
        .promote_collection_to_bftree(key, tag, live, ctx.meta.next_expiry, true)
        .await
        .map_err(|_| ())?;
    }
    return Ok(SweepOutcome::Swept { expired });
  }
  // 零到期水位前移：仅元记录回写（守卫持有窗口内完成，互斥覆盖装载→回写）
  session
    .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
    .await
    .map_err(|_| ())?;
  Ok(SweepOutcome::Swept { expired: 0 })
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
/// - `Ok(false)` 穿透臂按构造不触碰树（本模块置脏入口只有 [`tree_put`] 与
///   批量 upsert 臂的显式判定，均在闭环应答前），删除重命令（HDEL/SREM/SPOP/
///   ZREM/LPOP/RPOP）与成员级 TTL 面（HEXPIRE/HTTL/HPERSIST/ZEXPIRE/ZTTL/
///   ZPERSIST 族）全经此穿透，其推进由 run_async_rmw 物化降级臂经
///   apply_rmw_post_operate 单点承接，两条路径互斥无双计；
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

/// 哈希族写面判定（一处定义）：多步写臂与含 [`expire_sweep_or_rebuild`] 校正面
/// 的读命令（Hlen/Hgetall/Hkeys/Hvals 到期成员出账重灌）均取独占写锁；
/// 纯读臂（Hget/Hmget/Hexists/Hstrlen）与穿透臂（HDEL / HEXPIRE / HTTL /
/// HPERSIST / HRANDFIELD 等，经物化降级整值重灌）维持共享读锁
fn hash_needs_write(op: HashOperation) -> bool {
  matches!(
    op,
    HashOperation::Hset
      | HashOperation::Hmset
      | HashOperation::Hsetnx
      | HashOperation::Hincrby
      | HashOperation::Hincrbyfloat
      | HashOperation::Hlen
      | HashOperation::Hgetall
      | HashOperation::Hkeys
      | HashOperation::Hvals
  )
}

/// 集合族写面判定（一处定义）：SADD 写臂取独占写锁；SREM / SPOP 与 SRANDMEMBER、
/// 纯读 / 穿透臂一律共享读锁（删除重族无树内臂，见本模块头注「树内零墓碑」）
fn set_needs_write(op: SetOperation) -> bool {
  matches!(op, SetOperation::Sadd)
}

/// 有序集合族写面判定（一处定义）：Zcard 含 [`expire_sweep_or_rebuild`] 校正面
/// 亦写；纯读（Zscore/Zmscore）、ZREM、ZEXPIRE / ZTTL / ZPERSIST 与其余穿透臂
/// 维持共享读锁（经物化降级整值重灌）
fn zset_needs_write(op: SortedSetOperation) -> bool {
  matches!(
    op,
    SortedSetOperation::Zadd | SortedSetOperation::Zincrby | SortedSetOperation::Zcard
  )
}

/// 列表族写面判定（一处定义）：四 push 写臂取独占写锁（LPUSH 序号分配依赖锁内
/// 刷新后的 meta.size，免装载快照错位）；LPOP / RPOP 无树内臂，与纯读、其余
/// 穿透臂一律共享读锁（见本模块头注「树内零墓碑」）
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
    // arg1/arg2 压缩字已无树内消费臂（HEXPIRE 族穿透物化降级，压缩字由
    // run_async_rmw 的 run_op 闭包捕获透传对象层），显式弃绑防未用告警
    args12: _,
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
      // 计数校正（见 expire_sweep_or_rebuild）：水位越过即物理出账到期成员
      //（树内零墓碑，有到期才重灌），O(N) 每到期纪元至多一次，其余时刻直读
      // `size` 恒精确 O(1)（collection.md 大键 O(1) 计数规约第 3 条分层态补则）
      let _ = expire_sweep_or_rebuild(session, key, ctx, tree_guard, |_| {}).await?;
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
      // 帧头与实体同源：按实际字段对数写协议感知 map 头（对标
      // HashObjectImpl.cs:HashGetAll WriteMapLength(Count())），RESP3 写
      // `%<对数>`、RESP2 退化为 `*<2×对数>` 数组，杜绝 meta.size 漂移错位
      let mut scratch: Vec<u8> = Vec::new();
      let mut pairs = 0usize;
      let outcome = expire_sweep_or_rebuild(session, key, ctx, tree_guard, |live| {
        // 水位命中：存活全集在重灌前经闭包交出（到期成员已被扫描剔除），零二次扫树
        for (field, record) in live {
          scratch.write_resp_bulk_string(field);
          scratch.write_resp_bulk_string(decode_member(record).1);
          pairs += 1;
        }
      })
      .await?;
      if let SweepOutcome::Below(guard) = outcome {
        // 水位未命中：守卫原样奉还，树内无到期成员，流式直出
        let _ = guard.tree().scan_with_count_callback(
          &[0u8],
          usize::MAX,
          ScanReturnField::KeyAndValue,
          |k, v| {
            scratch.write_resp_bulk_string(k);
            scratch.write_resp_bulk_string(decode_member(v).1);
            pairs += 1;
            true
          },
        );
      }
      cs::write_map_len(output, pairs, resp_protocol_version);
      output.extend_from_slice(&scratch);
      Ok(true)
    }

    HashOperation::Hkeys => {
      // 帧头与实际条目同源（消除 meta.size 漂移错位）：数组语义（HKEYS RESP3 仍数组）
      let mut scratch: Vec<u8> = Vec::new();
      let mut n = 0usize;
      let outcome = expire_sweep_or_rebuild(session, key, ctx, tree_guard, |live| {
        for (field, _) in live {
          scratch.write_resp_bulk_string(field);
          n += 1;
        }
      })
      .await?;
      if let SweepOutcome::Below(guard) = outcome {
        let _ = guard.tree().scan_with_count_callback(
          &[0u8],
          usize::MAX,
          ScanReturnField::Key,
          |k, _| {
            scratch.write_resp_bulk_string(k);
            n += 1;
            true
          },
        );
      }
      output.write_resp_array_len(n);
      output.extend_from_slice(&scratch);
      Ok(true)
    }

    HashOperation::Hvals => {
      // 帧头与实际条目同源（消除 meta.size 漂移错位）：数组语义（HVALS RESP3 仍数组）
      let mut scratch: Vec<u8> = Vec::new();
      let mut n = 0usize;
      let outcome = expire_sweep_or_rebuild(session, key, ctx, tree_guard, |live| {
        for (_, record) in live {
          scratch.write_resp_bulk_string(decode_member(record).1);
          n += 1;
        }
      })
      .await?;
      if let SweepOutcome::Below(guard) = outcome {
        let _ = guard.tree().scan_with_count_callback(
          &[0u8],
          usize::MAX,
          ScanReturnField::Value,
          |_, v| {
            scratch.write_resp_bulk_string(decode_member(v).1);
            n += 1;
            true
          },
        );
      }
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

    // 未支持操作一律穿透（Ok(false)）：由 run_async_rmw 物化降级通道接手，
    // 杜绝静默兜底输出与命令语义无关的应答——HCOLLECT / HRANDFIELD 族经此
    // 落 wcol 对象层单源求值，WATCH 推进同臂由 apply_rmw_post_operate 承接。
    // HDEL 与成员级 TTL 面（HEXPIRE / HTTL / HPERSIST 族，原树内逐成员出账臂
    // 已删）亦在此穿透：删除与到期出账一律走「物化 → 对象层单源求值 →
    // 整值重灌（bulk_load 重建）」，树内零墓碑，见本模块头注
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
    // （无树内逐成员删除臂），见本模块头注「树内零墓碑」
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
    // arg1 为 ZRANK / ZREVRANK 的 WITHSCORE 位、arg2 为 ZRANGE 族选项位（见下方
    // 范围与排名臂）；成员级 TTL 面（ZEXPIRE 族）已穿透物化降级，其压缩字仍由
    // run_async_rmw 的 run_op 闭包捕获透传对象层，本臂不再消费
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
      // 计数校正（同分层 Hlen 臂，见 expire_sweep_or_rebuild）：水位越过即
      // 物理出账（树内零墓碑，有到期才重灌），O(N) 每到期纪元至多一次，其余
      // 时刻直读 `size` 恒精确 O(1)
      let _ = expire_sweep_or_rebuild(session, key, ctx, tree_guard, |_| {}).await?;
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

    // ZCOUNT 树内读臂（libs/server/Objects/SortedSet/SortedSetObjectImpl.cs:
    // SortedSetCount）：单趟流式扫描计数，内存 O(1)（原经 slow_load_eval 全量
    // 物化 → 千万级集合一次 O(N) 内存抖动）
    SortedSetOperation::Zcount => {
      if args.len() < 2 {
        return Err(());
      }
      let (Some((min_value, min_exclusive)), Some((max_value, max_exclusive))) = (
        SortedSetObject::try_parse_parameter(args[0]),
        SortedSetObject::try_parse_parameter(args[1]),
      ) else {
        cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
        return Ok(true);
      };
      let bounds = ZScoreBounds {
        min: min_value,
        min_excl: min_exclusive,
        max: max_value,
        max_excl: max_exclusive,
      };
      let scan = zset_scan_select(tree, now_ticks(), ZSetWindow::Count, |score, _| {
        bounds.pass(score)
      })?;
      // C# 外层守卫 `minValue <= sortedSet.Max.Score`：raw `<=` 且 Max **含到期
      // 成员**（对象层该臂不先 DeleteExpiredItems），NaN 面唯一差异处
      // （min 为 NaN 时守卫短路为 0，谓词本身会把 NaN 成员计入）
      let count = match scan.last_score {
        Some(last) if min_value <= last => scan.matched,
        _ => 0,
      };
      output.write_resp_int(count as i64);
      Ok(true)
    }

    // ZLEXCOUNT 树内读臂（SortedSetObjectImpl.cs:SortedSetRemoveOrCountRangeByLex
    // 的 ZLEXCOUNT 分支：`GetElementsInRangeByLex(min, max, false, false, (0,0))`
    // 命中数，零删除）
    SortedSetOperation::Zlexcount => {
      if args.len() < 2 {
        return Err(());
      }
      let Some(bounds) = ZLexBounds::parse(args[0], args[1], false) else {
        // 对象层由调用方按 result1 == int.MaxValue 回同一错误文本
        cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_STRING);
        return Ok(true);
      };
      let (_picked, _range, matched) = zset_lex_select(
        tree,
        now_ticks(),
        bounds,
        ZSetWindow::Count,
        false,
        0,
        usize::MAX,
      )?;
      output.write_resp_int(matched as i64);
      Ok(true)
    }

    // ZRANGE / ZREVRANGE / ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZRANGEBYLEX /
    // ZREVRANGEBYLEX 树内读臂（arg2 = SortedSetRangeOpts 位，与对象层 run_operate
    // 同约定；SortedSetObjectImpl.cs:SortedSetRange 三形态逐臂对位）：
    // 参数段解析与结果负载均复用 wcol 单源函数，选择走 [`zset_scan_select`]
    // 流式内核，内存只随窗口增长，不再全量物化
    SortedSetOperation::Zrange => {
      let range_opts = SortedSetRangeOpts::from_bits_truncate(args12.1 as u8);
      let (options, resp_ver) = match parse_range_options(args, range_opts, resp_protocol_version) {
        Ok(v) => v,
        // 命令层 arity 保证 min/max 两段 → Incomplete 不可达；分层侧不得输出
        // 半帧（对象层该臂零输出回包），防御性走存储错误面
        Err(RangeArgError::Incomplete) => return Err(()),
        Err(err) => {
          err.write_reply(output);
          return Ok(true);
        }
      };
      let (min_span, max_span) = (args[0], args[1]);
      let now = now_ticks();

      // ---- 区间块 1：byIndex 或 byScore（C# 同段进入条件 !byLex || byScore）
      if (!options.by_score && !options.by_lex) || options.by_score {
        let (Some((min_value, min_exclusive)), Some((max_value, max_exclusive))) = (
          SortedSetObject::try_parse_parameter(min_span),
          SortedSetObject::try_parse_parameter(max_span),
        ) else {
          cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_FLOAT);
          return Ok(true);
        };
        let (picked, range) = if options.by_score {
          let mut bounds = ZScoreBounds {
            min: min_value,
            min_excl: min_exclusive,
            max: max_value,
            max_excl: max_exclusive,
          };
          // C# do_reverse 先交换边界再正序扫、后 reverse 输出（等价于按窗口
          // 方向直接取序最大 k 条）
          if options.reverse {
            swap(&mut bounds.min, &mut bounds.max);
            swap(&mut bounds.min_excl, &mut bounds.max_excl);
          }
          let (window, off, take) =
            zset_limit_window(options.reverse, options.valid_limit, options.limit);
          let scan = zset_scan_select(tree, now, window, |score, _| bounds.pass(score))?;
          zset_windowed_pick(scan.picked, window, options.reverse, off, take)
        } else {
          // byIndex：LIMIT 不支持（C# 同臂），负索引归一与钳制以存活总数为
          // 基准，故除 `0 -1` 全量快速路径外需先走一趟计数
          if options.valid_limit {
            cs::write_error_raw(output, cs::RESP_ERR_LIMIT_NOT_SUPPORTED);
            return Ok(true);
          }
          if min_value == 0.0 && max_value == -1.0 {
            let scan = zset_scan_select(tree, now, ZSetWindow::All, |_, _| true)?;
            zset_windowed_pick(scan.picked, ZSetWindow::All, options.reverse, 0, usize::MAX)
          } else {
            let set_count = zset_scan_select(tree, now, ZSetWindow::Count, |_, _| true)?.alive;
            if min_value > (set_count as f64) - 1.0 {
              (Vec::new(), 0..0)
            } else {
              let (mut min_index, mut max_index) = (min_value as i64, max_value as i64);
              if min_index < 0 {
                min_index += set_count as i64;
              }
              if max_index < 0 {
                max_index += set_count as i64;
              } else if max_index >= set_count as i64 {
                max_index = set_count as i64 - 1;
              }
              if (min_index < 0 && max_index < 0) || min_index > max_index {
                (Vec::new(), 0..0)
              } else {
                let min_index = min_index.max(0);
                let n = (max_index - min_index + 1) as usize;
                let window = if options.reverse {
                  ZSetWindow::Tail(min_index as usize + n)
                } else {
                  ZSetWindow::Head(min_index as usize + n)
                };
                let scan = zset_scan_select(tree, now, window, |_, _| true)?;
                zset_windowed_pick(scan.picked, window, options.reverse, min_index as usize, n)
              }
            }
          }
        };
        write_sorted_set_result_payload(
          output,
          options.with_scores,
          range.len(),
          resp_ver,
          picked[range].iter().map(|e| (e.score, e.member.as_slice())),
        );
      }

      // ---- 区间块 2：byLex（与块 1 相互独立，BYSCORE+BYLEX 并置时 C# 写两份
      // 回复；本块解析失败须回退本命令负载起点重写错误，对标 writer.ResetPosition）
      if options.by_lex {
        let out = match ZLexBounds::parse(min_span, max_span, options.reverse) {
          Some(bounds) => {
            let (window, off, take) =
              zset_limit_window(options.reverse, options.valid_limit, options.limit);
            zset_lex_select(tree, now, bounds, window, options.reverse, off, take)?
          }
          None => {
            output.clear();
            cs::write_error_raw(output, cs::RESP_ERR_MIN_MAX_NOT_VALID_STRING);
            return Ok(true);
          }
        };
        let (picked, range, _) = out;
        write_sorted_set_result_payload(
          output,
          options.with_scores,
          range.len(),
          resp_ver,
          picked[range].iter().map(|e| (e.score, e.member.as_slice())),
        );
      }
      Ok(true)
    }

    // ZRANK / ZREVRANK 树内读臂（SortedSetObjectImpl.cs:SortedSetRank）：
    // arg1 == 1 附带分值；名次 = 序在目标之前的存活成员数，反向以
    // `存活总数 - 名次 - 1` 换算（C# Count() 同基准，两侧皆不含到期成员）
    SortedSetOperation::Zrank | SortedSetOperation::Zrevrank => {
      let with_score = args12.0 == 1;
      // 与冷路径同口径取成员（命令层保证段数，缺失防御性按空成员）
      let member = args.first().copied().unwrap_or(&[]);
      let now = now_ticks();
      // C# TryGetScore：到期成员视同不存在（先点读定位分值，再单趟流式计名次）
      let Some(score) = tree_member_score(tree, member, now) else {
        output.write_resp_null_ver(resp_protocol_version);
        return Ok(true);
      };
      let target = (score, member);
      let scan = zset_scan_select(tree, now, ZSetWindow::Count, |s, m| {
        SortedSetComparer::compare((&s, &m), (&target.0, &target.1)) == Ordering::Less
      })?;
      let mut rank = scan.matched as i64;
      if op == SortedSetOperation::Zrevrank {
        rank = scan.alive as i64 - rank - 1;
      }
      if with_score {
        output.write_resp_array_len(2);
        output.write_resp_int(rank);
        cs::write_double_numeric(output, score, resp_protocol_version);
      } else {
        output.write_resp_int(rank);
      }
      Ok(true)
    }

    // 未支持操作一律穿透（Ok(false)）：由 run_async_rmw 物化降级通道接手，
    // 杜绝静默兜底输出与命令语义无关的应答——ZPOPMIN / ZPOPMAX / ZREMRANGEBY*
    // / GEOADD / ZRANGESTORE 等写族经此落 wcol 对象层单源真实删改，WATCH 推进
    // 同臂由 apply_rmw_post_operate 承接（旧兜底臂把它们应答成整表 ZRANGE 形态
    // 且零树删除，客户端见成功而数据未动，是比漏栅栏更重的语义缺陷）。
    // ZREM 与成员级 TTL 面（ZEXPIRE / ZTTL / ZPERSIST 族，原树内逐成员出账臂
    // 已删）同在此穿透：删除与到期出账一律走整值重灌，树内零墓碑（见模块头注）。
    // ZRANDMEMBER / ZDIFF / ZUNION / ZINTER / ZRANGESTORE 等多键与随机采样面
    // 亦维持物化通道（非本票射程，代价与限流口径见 doc/zh/collection.md）。
    _ => Ok(false),
  }
}

/// 分层 zset 树内流式留存窗口（[`zset_scan_select`] 的内存上界）
///
/// 判序单点复用 wcol [`SortedSetEntry`] 的 `Ord`（委托
/// [`SortedSetComparer`]，.NET `Double.CompareTo` 口径），与对象层内存
/// `SortedSet` 同序——树内只存 member → 分值，无分序索引，故「序」只能在
/// 扫描侧由同一比较器重建，严禁另立第二套排序结构。
#[derive(Debug, Clone, Copy)]
enum ZSetWindow {
  /// 全量留存后排序：应答本身即 O(N) 的形态（`ZRANGE k 0 -1`、无 LIMIT 的
  /// ZRANGEBYSCORE / ZRANGEBYLEX）
  All,
  /// 只留序最小 k 条，升序回（有界窗口正向；k == 1 即「序最小者」= 断点求解）
  Head(usize),
  /// 只留序最大 k 条，降序回（有界窗口 REV / 反向形态）
  Tail(usize),
  /// 不留存，仅计数（ZCOUNT / ZRANK / ZLEXCOUNT 与 byIndex 的存活计数趟）
  Count,
}

/// [`zset_scan_select`] 回传束
struct ZSetScanOut {
  /// 留存条目：Head/All 升序、Tail 降序、Count 恒空
  picked: Vec<SortedSetEntry>,
  /// 存活且命中谓词的条目数（Count 形态的应答值）
  matched: u64,
  /// 存活条目总数（与谓词无关：byIndex 的 `set_count` 与 ZREVRANK 的
  /// `Count - rank - 1` 同基准）
  alive: u64,
  /// 全树序最大条目的分值，**含到期成员**（对位对象层 `sorted_set.last()`：
  /// C# 该守卫不过滤到期，ZCOUNT 外层守卫唯一取值面）
  last_score: Option<f64>,
}

/// 分值判序单点（[`SortedSetComparer`] 的空成员回退臂，等价 .NET
/// `Double.CompareTo`：NaN 小于一切非 NaN，±0.0 相等）
#[inline]
fn zset_score_order(a: f64, b: f64) -> Ordering {
  const EMPTY: &[u8] = &[];
  SortedSetComparer::compare((&a, EMPTY), (&b, EMPTY))
}

/// 分值区间界（解析单点复用 wcol [`SortedSetObject::try_parse_parameter`]）
///
/// 谓词口径逐字对位 C# `GetElementsInRangeByScore` / `SortedSetCount` 循环：
/// 下界 = `GetViewBetween((minValue, null), …)` 哨兵的序裁剪（按 CompareTo，
/// 非 raw `>=`：±0.0 与 NaN 面不等价），上界 = 循环内 raw `>` / `==` 断点判据。
#[derive(Debug, Clone, Copy)]
struct ZScoreBounds {
  min: f64,
  min_excl: bool,
  max: f64,
  max_excl: bool,
}

impl ZScoreBounds {
  fn pass(&self, score: f64) -> bool {
    zset_score_order(score, self.min) != Ordering::Less
      && !(self.min_excl && score == self.min)
      && !(score > self.max || (self.max_excl && score == self.max))
  }
}

/// 字典序区间界（解析单点复用 wcol [`SortedSetObject::try_parse_lex_parameter`]，
/// REV 交换对位 C# `GetElementsInRangeByLex` 首段三换）
#[derive(Debug, Clone, Copy)]
struct ZLexBounds<'a> {
  min: &'a [u8],
  min_excl: bool,
  min_inf: SpecialRanges,
  max: &'a [u8],
  max_excl: bool,
  max_inf: SpecialRanges,
}

impl<'a> ZLexBounds<'a> {
  /// 两界解析 + REV 交换；任一界词形非法 → None（C# 以 i32::MAX 上抛）
  fn parse(min_span: &'a [u8], max_span: &'a [u8], reverse: bool) -> Option<Self> {
    let ((min, min_excl, min_inf), (max, max_excl, max_inf)) = (
      SortedSetObject::try_parse_lex_parameter(min_span)?,
      SortedSetObject::try_parse_lex_parameter(max_span)?,
    );
    Some(if reverse {
      Self {
        min: max,
        min_excl: max_excl,
        min_inf: max_inf,
        max: min,
        max_excl: min_excl,
        max_inf: min_inf,
      }
    } else {
      Self {
        min,
        min_excl,
        min_inf,
        max,
        max_excl,
        max_inf,
      }
    })
  }

  /// C# 早空判据：min 为 `+`、max 为 `-`
  fn always_empty(&self) -> bool {
    self.min_inf == SpecialRanges::InfiniteMax || self.max_inf == SpecialRanges::InfiniteMin
  }

  /// 下界过滤（成员字节序，对位 C# `SequenceCompareTo`；`-∞` 恒真）
  fn pass_min(&self, member: &[u8]) -> bool {
    if self.min_inf == SpecialRanges::InfiniteMin {
      return true;
    }
    let ord = member.cmp(self.min);
    !(ord == Ordering::Less || (ord == Ordering::Equal && self.min_excl))
  }

  /// 上界过滤（C# `take_while` 的判据面；`+∞` 恒真 ⇒ 不存在断点）
  fn pass_max(&self, member: &[u8]) -> bool {
    if self.max_inf == SpecialRanges::InfiniteMax {
      return true;
    }
    let ord = member.cmp(self.max);
    !(ord == Ordering::Greater || (ord == Ordering::Equal && self.max_excl))
  }
}

/// 分层 zset 树内单趟流式遍历内核：范围选择、区间计数、名次计数三族共用
///
/// 树内记录以 member 为键（member → 8B f64 大端分值 + 可选 TTL 头），本内核自
/// 树头一趟线性扫过全部记录，按 `(分值, 成员)` 序留存至多 `window` 条，
/// **内存随窗口增长而非随键基数增长**——这正是本票消除的「一条 ZRANGE 触发
/// 千万级成员全量反序列化 + 重建 `SortedSetObject`」。计数形态（[`ZSetWindow::
/// Count`]）内存 O(1)。
///
/// 到期成员只过滤不出产（C# 循环首臂 `IsExpired → continue`），本内核**不落
/// 任何删除记录**：维持「树内墓碑恒低」写形不变量（物理出账仍归 ZCARD /
/// ZCOLLECT 的 [`collect_expired_members`]），故本族读臂一律走共享读锁、
/// `dirty` 恒假、不推进 WATCH 栅栏。但 [`ZSetScanOut::last_score`] 含到期成员，
/// 与对象层 `sorted_set.last()` 同基准。
///
/// 栈深口径同 [`exec_tiered_scan`]：底层游标对墓碑的尾递归连跑不受本臂截断
/// 约束，安全性来自写形不变量而非本扫描的窗口。
///
/// 分值载荷非 8B = 编码损坏 → fail-fast `Err(())`（与 [`tiered_materialize_blob`]
/// 的 zset 臂同口径，严禁静默剔除成员后照常应答）。
fn zset_scan_select(
  tree: &BfTreeService,
  now: i64,
  window: ZSetWindow,
  mut pred: impl FnMut(f64, &[u8]) -> bool,
) -> Result<ZSetScanOut, ()> {
  let mut picked: Vec<SortedSetEntry> = Vec::new();
  // Head 用最大堆（超容量弹最大 ⇒ 恒留序最小 k 条），Tail 用反序堆（同构造
  // 对偶 ⇒ 恒留序最大 k 条，`into_sorted_vec` 即降序）
  let mut head: BinaryHeap<SortedSetEntry> = BinaryHeap::new();
  let mut tail: BinaryHeap<Reverse<SortedSetEntry>> = BinaryHeap::new();
  let mut matched = 0_u64;
  let mut alive = 0_u64;
  let mut last_score: Option<f64> = None;
  let mut corrupt = false;
  let _ =
    tree.scan_with_count_callback(&[0u8], usize::MAX, ScanReturnField::KeyAndValue, |k, v| {
      let (expiry, payload) = decode_member(v);
      let Ok(arr) = <[u8; 8]>::try_from(payload) else {
        log::error!(
          "zset_scan_select: corrupted zset score payload, member='{}'",
          String::from_utf8_lossy(k)
        );
        corrupt = true;
        return false;
      };
      let score = f64::from_be_bytes(arr);
      if last_score.is_none_or(|cur| zset_score_order(score, cur) == Ordering::Greater) {
        last_score = Some(score);
      }
      if expiry.is_some_and(|ticks| ticks < now) {
        return true;
      }
      alive += 1;
      if !pred(score, k) {
        return true;
      }
      matched += 1;
      match window {
        ZSetWindow::All => picked.push(SortedSetEntry {
          score,
          member: k.to_vec(),
        }),
        ZSetWindow::Head(cap) if cap > 0 => {
          head.push(SortedSetEntry {
            score,
            member: k.to_vec(),
          });
          if head.len() > cap {
            head.pop();
          }
        }
        ZSetWindow::Tail(cap) if cap > 0 => {
          tail.push(Reverse(SortedSetEntry {
            score,
            member: k.to_vec(),
          }));
          if tail.len() > cap {
            tail.pop();
          }
        }
        _ => {}
      }
      true
    });
  if corrupt {
    return Err(());
  }
  let picked = match window {
    ZSetWindow::Head(_) => head.into_sorted_vec(),
    ZSetWindow::Tail(_) => tail.into_sorted_vec().into_iter().map(|e| e.0).collect(),
    // All：树内扫描序为 member 序，须按 (分值, 成员) 单源 Ord 重排，与内存态
    // `SortedSetObject`（C# 内存 `SortedSet` 同序）及本内核 Head/Tail 堆序一致；
    // Count 形态 picked 恒空，排序无副作用
    _ => {
      picked.sort();
      picked
    }
  };
  Ok(ZSetScanOut {
    picked,
    matched,
    alive,
    last_score,
  })
}

/// REV / LIMIT → （留存窗口, 跳数, 取数）单点换算
///
/// 对位 C# 两区间块的同一段：`offset < 0 || count == 0` → 空结果；`count < 0`
/// → 取到末尾（窗口退化为全量）；否则正向取序最小 `offset + count` 条、反向取
/// 序最大 `offset + count` 条，再跳过 `offset` 条——与 C#「全量收集后
/// `skip(offset).take(count)`」逐条同序，堆留存集是它的前缀。
fn zset_limit_window(
  reverse: bool,
  valid_limit: bool,
  limit: (i64, i64),
) -> (ZSetWindow, usize, usize) {
  if !valid_limit {
    return (ZSetWindow::All, 0, usize::MAX);
  }
  if limit.0 < 0 || limit.1 == 0 {
    return (ZSetWindow::Head(0), 0, 0);
  }
  let off = limit.0 as usize;
  if limit.1 < 0 {
    return (ZSetWindow::All, off, usize::MAX);
  }
  let take = limit.1 as usize;
  let window = if reverse {
    ZSetWindow::Tail(off.saturating_add(take))
  } else {
    ZSetWindow::Head(off.saturating_add(take))
  };
  (window, off, take)
}

/// 留存集 → 输出方向与 `skip/take` 切片（免二次拷贝）
///
/// Head/All 升序、Tail 降序已由 [`zset_scan_select`] 保证；仅全量形态需按 REV
/// 显式倒置（C# `scored_elements.reverse()` / `all.reverse()` 同臂）。
fn zset_windowed_pick(
  mut picked: Vec<SortedSetEntry>,
  window: ZSetWindow,
  reverse: bool,
  off: usize,
  take: usize,
) -> (Vec<SortedSetEntry>, Range<usize>) {
  if reverse && matches!(window, ZSetWindow::All) {
    picked.reverse();
  }
  let start = off.min(picked.len());
  let end = start.saturating_add(take).min(picked.len());
  (picked, start..end)
}

/// 字典序区间树内流式选择（ZRANGEBYLEX 族与 ZLEXCOUNT 共用内核）
///
/// C# `GetElementsInRangeByLex` 的上界是 `take_while`（真 break），而树内扫描序
/// 是 member 序、输出口径是 `(分值, 成员)` 序：同一条目「成员越界」与「分值
/// 靠后」互相交错，越界断点**不可**表达为局部谓词（反例 `{("z",1),("a",2)}`
/// 取 `[a`/`[y`：C# 在 (1,"z") 处 break，(2,"a") 虽在字典窗口内亦不得出现）。
/// 故先以 [`ZSetWindow::Head(1)`] 一趟求出断点 `= 序最小者 ∈ {存活 ∧ 过下界 ∧
/// 未过上界}`（内存 O(1)），第二趟把「序 < 断点」并入谓词选择；上界为 `+`
/// 时 break 永不触发，断点趟直接跳过。两趟均为页级顺序扫，无成员级堆分配。
///
/// 返回 `(留存集, 输出区间, 命中总数)`：区间供范围命令切片，命中总数供
/// ZLEXCOUNT（[`ZSetWindow::Count`] 形态下区间恒空）。
fn zset_lex_select(
  tree: &BfTreeService,
  now: i64,
  bounds: ZLexBounds<'_>,
  window: ZSetWindow,
  reverse: bool,
  off: usize,
  take: usize,
) -> Result<(Vec<SortedSetEntry>, Range<usize>, u64), ()> {
  if bounds.always_empty() {
    return Ok((Vec::new(), 0..0, 0));
  }
  let barrier = if bounds.max_inf == SpecialRanges::InfiniteMax {
    None
  } else {
    zset_scan_select(tree, now, ZSetWindow::Head(1), |_, member| {
      bounds.pass_min(member) && !bounds.pass_max(member)
    })?
    .picked
    .into_iter()
    .next()
  };
  let scan = zset_scan_select(tree, now, window, |score, member| {
    bounds.pass_min(member)
      && bounds.pass_max(member)
      && barrier.as_ref().is_none_or(|t| {
        SortedSetComparer::compare((&score, &member), (&t.score, &t.member)) == Ordering::Less
      })
  })?;
  let matched = scan.matched;
  let (picked, range) = zset_windowed_pick(scan.picked, window, reverse, off, take);
  Ok((picked, range, matched))
}

/// 成员分值点读（到期视同不存在，对位 C# `SortedSetObject.TryGetScore`）
///
/// 与分层 ZSCORE / ZMSCORE 臂同一解码口径（8B f64 大端 + 可选 TTL 头）。以树内
/// 顺序扫描（member 为升序键，命中或越过即停）替代 `read_callback` 点读，与本
/// 模块其余读臂同一 `scan_with_count_callback` 入口——ZRANK 臂随后即接一次计数
/// 扫描，点读 + 扫描在同一服务上交错会命中 bf-tree 游标定位的 mini-page 合并
/// 缺陷，故全程只用扫描入口。
fn tree_member_score(tree: &BfTreeService, member: &[u8], now: i64) -> Option<f64> {
  let mut score_opt = None;
  let _ =
    tree.scan_with_count_callback(&[0u8], usize::MAX, ScanReturnField::KeyAndValue, |k, v| {
      match k.cmp(member) {
        // 键升序：越过目标即判不存在，停止扫描
        Ordering::Greater => false,
        // 命中：解码分值（到期视同不存在），停止扫描
        Ordering::Equal => {
          let (expiry, payload) = decode_member(v);
          if expiry.is_none_or(|ticks| ticks >= now)
            && let Ok(arr) = <[u8; 8]>::try_from(payload)
          {
            score_opt = Some(f64::from_be_bytes(arr));
          }
          false
        }
        Ordering::Less => true,
      }
    });
  score_opt
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
/// 故本臂的栈安全性完全依赖「树内零墓碑」这一写形不变量，而该不变量由删除不
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
    // 摘除，树内无逐成员删除，见本模块头注「树内零墓碑」）
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
/// 对象收集任务与计数慢路径校正共用；到期重灌内核 [`expire_sweep_or_rebuild`]
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
  // 收集执行体是写臂（到期成员出账重灌）：独占写锁 + 锁内刷新元记录，
  // 键已被并发排空回收则按「键非分层态」穿透
  let Some(tree_guard) = tiered_guard(session, key, &mut ctx, true).await? else {
    return Ok(None);
  };
  let old_expiry = ctx.meta.next_expiry;
  // 守卫在 Below 出口原样奉还后即释放；Swept 出口重灌/回写已在内完成
  let changed = match expire_sweep_or_rebuild(session, key, &mut ctx, tree_guard, |_| {}).await? {
    SweepOutcome::Below(_) => false,
    SweepOutcome::Swept { expired } => expired > 0 || ctx.meta.next_expiry != old_expiry,
  };
  if changed {
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
/// 二.表 S4）。本臂的安全性来自「树内零墓碑」的写形不变量，不来自本函数的截断
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
