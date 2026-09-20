use wbase::time::now_ticks;
use wbftree::{
  BfTreeInsertResult, BfTreeReadResult, BfTreeService, RangeIndexStub, ScanReturnField,
};
use wcol::types::member_ttl::{decode_member, encode_member_into, encoded_len};
use wdev::Device;
use wkv::{
  BatchStoreSession, RangeIndexError, TieredCollectionNotification, TreeGuard,
  validate_bftree_record,
};
use wresp::cmd_strings as cs;
use wval::{GarnetObjectType, MetaValue};

/// 分层扫描 Err 上抛单点（一处定义，全族树内扫描唯一门）
///
/// `scan_with_count_callback` 的 `Err` 携带底层 range_index 迭代失败或
/// catch_unwind 兜底（wbftree service/ops.rs:scan_callback），必须原样上抛：
/// 各臂以 `?` 穿透本门后沿既有慢路径存储错误漏斗统一闭环成 RESP 错误帧
///（RESP_ERR_SLOW_PATH_STORAGE），与存储 IO 失败同一出口。严禁 `let _ =`
/// 折叠——折叠即 r6 红缺陷复现（读促销踩脏 mini page 期间 HGETALL 双端
/// 返 `*0` 而无任何报错），空集/截断应答会把存储故障伪装成数据缺失。
///
/// 生产态扫描失败面极窄（引擎配置偏离 / 页损坏 / panic 兜底），日志单点
/// 落 `error!` 便于归因，不携带键内容
#[inline]
pub(super) fn scan_count(scan: wbftree::Result<usize>) -> Result<usize, ()> {
  scan.map_err(|e| {
    log::error!("tiered scan failed: {e}");
  })
}

/// 分层态操作上下文（元记录 + 树存根的可变借用束 + 变更脏标记）
///
/// `dirty` 由本模块树内写漏斗 [`tree_put`] 在实际写入成功时置位、批量漏斗
/// [`tree_put_batch`] 的置脏交由调用臂按命令语义判定（覆盖写与重复成员在
/// 「新增计数」上分叉，见其文档）、到期重灌内核 [`expire_sweep_or_rebuild`]
/// 在实际出账时置位，是分层写臂 WATCH 版本栅栏推进的唯一判据（一处定义，
/// 见 [`finish_tiered_arm`]）。
///
/// 不变式（全置脏点共同自持）：**`dirty` ⟺ 树内容已实际变更**。各点置脏
/// 时点一律在实际树写生效之后——到期出账臂的置脏在自迁移封窗登记成功之后
///（claim 失败即存储忙拒绝，树内容零变更，保持 dirty 为假，杜绝 Err+dirty
/// 假推进误夭折并发 WATCH 事务），臂内更早的 `?` 早退（扫描失败 / claim
/// 失败）因此天然不触栅栏
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
pub(super) fn tree_put_ok(
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
pub(super) fn tiered_precheck(
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
pub(super) fn tree_put_rejected(output: &mut Vec<u8>) {
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
pub(super) fn tree_put_batch(
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
pub(super) fn tree_member_state(
  tree: &BfTreeService,
  member: &[u8],
  now: i64,
) -> Option<(Option<i64>, bool)> {
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
/// 到期全扫产物：`(存活条目全集, 到期被剔除计数)`；扫描 Err 经 [`scan_count`]
/// 上抛（`Err(())`）——此处若静默，部分/空存活集会被整值重灌固化，比输出臂
/// 截断更重（数据销毁），故为本门最早收口的位置
type SweptLiveEntries = (Vec<(Vec<u8>, Vec<u8>)>, u64);

fn sweep_expired_members(
  ctx: &mut TieredCtx<'_>,
  tree: &BfTreeService,
) -> Result<Option<SweptLiveEntries>, ()> {
  let now = now_ticks();
  // `<=`：水位刻度成员此刻尚未到期（`ticks < now` 严格），零树访问直回；
  // 越过水位才必有到期可出账（off-by-one 收口，见上文水注文）
  if now <= ctx.meta.next_expiry {
    return Ok(None);
  }
  let mut live: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
  let mut expired = 0u64;
  let mut next_expiry = i64::MAX;
  scan_count(tree.scan_with_count_callback(
    &[0u8],
    usize::MAX,
    ScanReturnField::KeyAndValue,
    |k, v| {
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
    },
  ))?;
  ctx.meta.next_expiry = next_expiry;
  Ok(Some((live, expired)))
}

/// 到期重灌执行结果：水位越过（`now > next_expiry`，必有成员已到期）后守卫
/// 与存活全集的去向
pub(super) enum SweepOutcome<'a> {
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
/// 键全程存活，换入臂不碰 TTL 旁路。`drop` 顺序固定：自迁移封窗（安全换入窗）
/// 先行——claim 在守卫（条带独占写锁）仍在手时登记，此刻无在途同键写者，登记
/// 即与后续全部写臂的探测门串行；随后放守卫进入「建树快照 → 换入」长窗，窗内
/// 并发同键写臂被四探测门 MigrationBusy 拒，杜绝「已 ACK 落旧树随 replace=true
/// 换入被整树顶替」的静默丢失形；窗内无写提交，AOF 镜像序仍 = 树内提交序
///（快照含扫描前全部已提交写）。守卫 RAII 成对释放：换入失败 / IO 失败 / panic
/// 展开一律出窗即释，杜绝泄漏令键永久不可见。重灌后 `ctx.meta` / `ctx.stub`
/// 为旧树副本（promote 已落新元记录），调用方不得再 `save_bftree_meta_stub`
/// 覆写，仅可应答内存态 size（`dec_size` 后与 promote 落盘的 bulk_load 去重
/// 计数一致）
pub(super) async fn expire_sweep_or_rebuild<'s, D: Device>(
  session: &'s BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  tree_guard: TreeGuard<'s>,
  drain_live: impl FnOnce(&[(Vec<u8>, Vec<u8>)]),
) -> Result<SweepOutcome<'s>, ()> {
  let Some((live, expired)) = sweep_expired_members(ctx, tree_guard.tree())? else {
    return Ok(SweepOutcome::Below(tree_guard));
  };
  let tag = ctx.meta.collection_type;
  if expired > 0 {
    // 封窗登记先于放守卫（claim 失败 = 同键正被 RENAME / 另一自迁移窗持有，
    // 存储忙失败交客户端重试；守卫随退出释放，对方等锁不互锁）。登记必须先于
    // dec_size / 置脏：claim 失败臂树内容零变更（扫描只读、重灌未启动、删空
    // 未落），dirty 不得为真——「Err+dirty 仍推进」的 WATCH 判据前提是树内容
    // 已实际变更，存储忙拒绝不是树写，假推进即误夭折并发 WATCH 事务
    let Some(_swap_in_window) = session.try_swap_in_window(key) else {
      return Err(());
    };
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
    // 出窗即释 claim（RAII），随后调用方的 Swept 重装载/重取守卫不再被拒
    return Ok(SweepOutcome::Swept { expired });
  }
  // 零到期水位前移：仅元记录回写（守卫持有窗口内完成，互斥覆盖装载→回写）
  session
    .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
    .await
    .map_err(|_| ())?;
  Ok(SweepOutcome::Swept { expired: 0 })
}

/// 分层计数臂读锁稳态快路径与水位越过升级出账（HLEN / ZCARD 共用内核）：
/// 计数是 O(1) 契约面，不得常态占据独占写锁——水位内（`now <= next_expiry`）
/// 树内零到期成员，共享读锁内重读元记录直读 `size` 恒精确；水位越过（必有
/// 成员到期）才放读锁升级独占写锁走 [`expire_sweep_or_rebuild`] 物理出账。
/// 出账形态不解耦后台异步：出账完成前 `size` 仍含未出账到期成员，首个计数
/// 命令同步出账一次、每到期纪元至多一扫的裁决语义原样保留（契约口径见
/// doc/zh/collection.md 大键 O(1) 计数规约第 3 条分层态补则）
///
/// 锁内重读元记录（[`tiered_guard`] 读臂不刷新，快照是锁外装载值）把应答
/// 新鲜度推近发起时刻：装载快照的水位可能被 HEXPIRE 族整值重灌收紧、size
/// 可能被并发写臂推高，直读快照即应答装载时刻的陈旧值；读锁与写臂「树写
/// → meta 回写」全程窗口互斥，重读值与树内容互一致。重读自带迁移 claim
/// 判定，封窗内计数按存储忙拒绝——与改前写锁臂同形，锁形态收敛不引入读面
/// 新语义。`Ok(false)`（键被并发排空回收）与写锁终态判定同口径，计数回零
pub(super) async fn tiered_count<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &mut TieredCtx<'_>,
  read_guard: TreeGuard<'_>,
) -> Result<u64, ()> {
  if !session
    .refresh_tiered_meta(key, ctx.meta)
    .await
    .map_err(|_| ())?
  {
    return Ok(0);
  }
  if now_ticks() <= ctx.meta.next_expiry {
    return Ok(ctx.meta.size);
  }
  // 水位越过：先放读锁再取独占写锁（持读锁等写锁即自锁死）；tiered_guard
  // 锁内再刷新兼作双检——并发计数臂可能已出账前移水位，出账内核水位内
  // Below 直读即短路
  drop(read_guard);
  let Some(write_guard) = tiered_guard(session, key, ctx, true).await? else {
    return Ok(0);
  };
  let _ = expire_sweep_or_rebuild(session, key, ctx, write_guard, |_| {}).await?;
  Ok(ctx.meta.size)
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
/// 该判据前提「树内容已实际变更」由各置脏点自持（见 [`TieredCtx`] 头注不变式：
/// 置脏必在实际树写生效之后；存储忙拒绝等树内容零变更的失败臂不得置脏）。
/// 镜像侧同一形态由 [`save_tiered_meta`] 失败臂 `break 'arm` 落臂尾
/// [`emit_tiered_mirror`] 收口，与 WATCH 推进同窗兜底（见 emit 头注不变式）。
#[inline]
pub(super) fn finish_tiered_arm<D: Device>(
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

/// 分层稳态写命令镜像载荷（RESP 命令语义：判别类型 + 族内操作码 + arg1/arg2
/// 压缩字 + 原始参数）。镜像的是命令而非最终值——副本/恢复端按主端同一判定
/// 单点路由分层臂逐条重放天然收敛，INCRBY 族 delta 与主端同序幂等一致
pub(super) struct TieredMirror<'a> {
  pub obj_type: GarnetObjectType,
  pub op_code: u8,
  pub arg1: i32,
  pub arg2: i32,
  pub args: &'a [&'a [u8]],
}

/// 树内稳态写命令的镜像载荷构造（写命令 `Some`，读臂/计数校正臂/穿透臂
/// `None`——镜像面刻意窄于 `*_needs_write` 锁面：HLEN/ZCARD/HGETALL 族到期
/// 出账置脏的树变更已由 promote 重灌流整树镜像，命令本身无写语义不产命令
/// 记录，重放一条 HLEN 只会空转）
#[inline]
pub(super) fn tiered_write_mirror<'a, Op: Copy + Into<u8>>(
  obj_type: GarnetObjectType,
  op: Op,
  args12: (i32, i32),
  args: &'a [&'a [u8]],
  is_steady_write: impl FnOnce(Op) -> bool,
) -> Option<TieredMirror<'a>> {
  is_steady_write(op).then(|| TieredMirror {
    obj_type,
    op_code: op.into(),
    arg1: args12.0,
    arg2: args12.1,
    args,
  })
}

/// 分层稳态写命令镜像入账（一处定义，四族 arm 收尾共用）：`ctx.dirty`（与
/// WATCH 置脏同判据）且命令属稳态写面时发 `StoreEvent::TieredCollectionWrite`
/// 入账 ObjectStoreRMW 条目，补齐「升阶承诺 AOF 镜像、主从一致」在稳态写面的
/// 断链——此前升阶流只镜像升阶时点内容，此后树内写既不发数据事件也不发命令
/// 记录，副本树永久缺字段、meta.size 滞后。
///
/// 不变式（本模块镜像机制的完整性契约，四族写臂必须共同维护）：**树已变更
/// ⟺ 镜像已入账**。「树内写已生效」的**所有**退出路径（含元记录落盘失败的
/// IO 失败臂）都必须经过本 emit——WATCH 侧有 [`finish_tiered_arm`] 对
/// Err+dirty 的兜底推进，镜像侧无，失败臂若以 `?` 从函数直返即绕过臂尾
/// emit，主端树已变更而 AOF/副本缺条目（重启重放后树内容与 meta.size 滞后）。
/// 故树写生效后的落盘失败一律经 [`save_tiered_meta`] 记错后以
/// `break 'arm Err(())` 落到臂尾本 emit 再上抛，严禁 `?` 直返。
///
/// 调用位约定（正确性前提，调用方必须遵守）：在各族 arm 的**树守卫存续窗口
/// 内**调用——同键并发写臂被条带独占写锁串行，本函数入账完成后守卫才 drop
/// 放行下一写臂，AOF 序与树内提交序严格一致；锁外收尾（finish_tiered_arm）
/// 与 emit 之间存在调度窗口，多核下并发同键写可乱序入账使覆写序颠倒，故
/// 镜像必须在锁内。对标 C# WriteLogRMW 在记录锁内置 NeedAofLog 的同窗口形态
///（libs/server/Storage/Functions/ObjectStore/PrivateMethods.cs:WriteLogRMW，
/// RMWMethods.cs:83/:102/:127 三钩子全量置位）。
///
/// 入队失败仅告警不上抛：树内写已生效，与 RI.SET 逐条镜像先例同口径
///（wkv range_index/ops.rs），拒绝应答反而制造主从应答背离；重放面可自
/// 树状态与后续条目收敛
#[inline]
pub(super) fn emit_tiered_mirror<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &TieredCtx<'_>,
  mirror: Option<TieredMirror<'_>>,
) {
  let Some(mirror) = mirror else { return };
  if !ctx.dirty {
    return;
  }
  let (ns, db) = session.virtual_domain();
  let notif = TieredCollectionNotification {
    ns,
    db,
    key,
    obj_type: mirror.obj_type,
    op_code: mirror.op_code,
    arg1: mirror.arg1,
    arg2: mirror.arg2,
    args: mirror.args,
  };
  if let Err(e) = session.notify_tiered_collection_write(&notif) {
    log::error!("分层稳态写 AOF 入队失败: {e}");
  }
}

/// 分层稳态写臂元记录落盘单点（一处定义，四族写臂共用，禁散落直调
/// `save_bftree_meta_stub`）
///
/// 不变式载体（见 [`emit_tiered_mirror`] 头注）：**树已变更 ⟺ 镜像已入账**。
/// 调用本漏斗时树内写已生效（`ctx.dirty` 必真），落盘失败**不得**以 `?` 从
/// 函数直返——直返绕过 `'arm` 块之后的臂尾 emit，主端树已变更而 AOF/副本缺
/// 条目。本漏斗失败仅记错误日志并向调用臂交回 `Err(())`，调用臂以
/// `break 'arm Err(())` 收口：先落臂尾 emit（镜像入账）再沿既有存储错误
/// 漏斗闭环成 RESP 错误帧；meta 落盘失败本身由既有「重放自树状态与后续
/// 条目收敛」口径兜住（与本模块 emit 入队失败同口径）。对标 C# WriteLogRMW
/// 与写生效同窗置 NeedAofLog，无「写生效后第二失败点跳过日志」形态
///（libs/server/Storage/Functions/ObjectStore/PrivateMethods.cs 的 WriteLogRMW,正式锚点
/// 归本模块 emit_tiered_mirror）
#[inline]
pub(super) async fn save_tiered_meta<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  ctx: &TieredCtx<'_>,
) -> Result<(), ()> {
  session
    .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
    .await
    .map_err(|e| {
      log::error!(
        "tiered meta save failed (tree write effective, mirror emit pending), key='{}': {e}",
        String::from_utf8_lossy(key)
      );
    })
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
pub(super) async fn tiered_guard<'s, D: Device>(
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
