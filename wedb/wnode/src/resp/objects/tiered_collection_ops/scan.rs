use core::str;
use std::sync::Arc;

use wbase::time::now_ticks;
use wbftree::ScanReturnField;
use wcol::types::{
  member_ttl::{decode_member, member_expired_at},
  scan_converge_cursor,
};
use wdev::Device;
use wkv::{BatchStoreSession, Error, StoreSession, SwapInWindowGuard};
use wresp::{
  cmd_strings as cs,
  ext::{RespVecExt, backfill_resp_frame_head, reserve_resp_frame_head, resp_frame_head_len},
};
use wval::GarnetObjectType;

use super::common::{
  TieredCtx, expire_sweep_or_rebuild, finish_tiered_arm, load_collection_stub_for_read,
  scan_all_from_head, score_of_payload, tiered_guard,
};

/// 分层树物化为内存信封载荷（对标 C# 单记录对象域：Garnet 对象常驻存储层，
/// 任意命令对任意规模对象语义恒定；rust 分层态未实现树内原语的操作经本通道
/// 一次性物化回 wcol 对象，走对象层单源求值后按升阶/降阶判据回写）
///
/// meta/stub 由调用方预载传入：只读物化（慢路径求值）传门禁装载
/// [`load_collection_stub`](wkv::StoreSession::load_collection_stub) 结果即可；
/// 写回面（求值后经 [`apply_rmw_post_operate`](super::super::rmw_helpers)
/// 换入/清退）一律走封窗变体 [`tiered_materialize_blob_sealed`]，窗内自装载
/// 交 [`load_collection_stub_in_window`](wkv::StoreSession::load_collection_stub_in_window)
/// 预载，杜绝被自身 claim 拒绝。`Ok(Some(blob))` 物化载荷（与信封剥壳后格式
/// 一致，可直接喂 `from_blob`）；`Ok(None)` 键非分层态或类型不符（调用方维持
/// 既有装载路径）；`Err(())` 存储 IO 失败或树记录载荷损坏（fail-fast 中止
/// 物化，调用方不得回写，杜绝固化成员丢失）
pub(crate) async fn tiered_materialize_blob<D: Device>(
  session: &StoreSession<D>,
  key: &[u8],
  tag: GarnetObjectType,
  meta: &wval::MetaValue,
  stub: &mut wbftree::RangeIndexStub,
) -> Result<Option<Vec<u8>>, ()> {
  use wcol::{
    hash::hash_object::HashObject,
    list::list_object::ListObject,
    object_payload::GarnetObjectPayload,
    set::set_object::SetObject,
    zset::sorted_set_object::{SortedSetEntry, SortedSetObject},
  };

  if meta.collection_type != tag {
    return Ok(None);
  }
  let tree_guard = session.acquire_tree_read(key, stub).await.map_err(|_| ())?;
  let tree = tree_guard.tree();

  // 物化承载字段级 TTL：挂 TTL 成员还原进对象 expiration 结构（未到期），
  // 已到期成员剔除不复活（对齐 C# 对象构造函数装载口径：SortedSetObject.cs
  // 的 ExpirationBitMask 分支 canAddItem = expiration >= UtcNow.Ticks）；
  // 往返「内存态 → 升阶 → 物化 → 再升阶」逐字段 TTL 保真。扫描 Err 经
  // [`scan_all_from_head`] 单点上抛（fail-fast 中止物化，调用方不写回）——静默截断的
  // 物化载荷会被整值写回固化成员丢失，比输出臂截断更重
  let now = now_ticks();
  let blob = match tag {
    GarnetObjectType::Hash => {
      let mut obj = HashObject::new();
      scan_all_from_head(tree, ScanReturnField::KeyAndValue, |k, v| {
        let (expiry, payload) = decode_member(v);
        if expiry.is_some_and(|ticks| ticks < now) {
          return true;
        }
        // 一处分配多处持有：散列与过期账本共享同一 Arc
        let item = Arc::from(k.to_vec());
        obj.update_size(&item, payload, true);
        obj.hash.insert(item.clone(), payload.to_vec());
        if let Some(ticks) = expiry {
          obj.insert_expiration(item, ticks);
        }
        true
      })?;
      obj.to_blob()
    }
    GarnetObjectType::Set => {
      let mut obj = SetObject::new();
      scan_all_from_head(tree, ScanReturnField::Key, |k, _| {
        obj.set.insert(k.to_vec());
        true
      })?;
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
      scan_all_from_head(tree, ScanReturnField::KeyAndValue, |k, v| {
        let (expiry, payload) = decode_member(v);
        if expiry.is_some_and(|ticks| ticks < now) {
          return true;
        }
        let Some(score) = score_of_payload(payload) else {
          log::error!(
            "tiered_materialize_blob: corrupted zset score payload, key='{}' member={:?}",
            String::from_utf8_lossy(key),
            k
          );
          corrupt = true;
          return false;
        };
        // 一处分配多处持有：有序视图、散列与过期账本共享同一 Arc
        let member = Arc::from(k.to_vec());
        obj.update_size(&member, true);
        obj.sorted_set.insert(SortedSetEntry {
          score,
          member: member.clone(),
        });
        obj.sorted_set_dict.insert(member.clone(), score);
        if let Some(ticks) = expiry {
          obj.insert_expiration(member, ticks);
        }
        true
      })?;
      if corrupt {
        return Err(());
      }
      obj.to_blob()
    }
    GarnetObjectType::List => {
      let mut obj = ListObject::new();
      scan_all_from_head(tree, ScanReturnField::KeyAndValue, |_, v| {
        // 树内键为序号（大端保序），扫描序即元素序
        obj.list.push_back(decode_member(v).1.to_vec());
        true
      })?;
      obj.to_blob()
    }
    _ => return Ok(None),
  };
  Ok(Some(blob))
}

/// 写回面物化封窗单点（物化降级臂唯一入口，一处定义、调用点转引）：登记自迁移
/// 安全换入窗（[`StoreSession::try_swap_in_window`]，同键并发稳态写自此被四探测
/// 门拒绝）后未门禁装载 + 全扫物化，守卫随载荷交出——调用方必须持守卫跨过
/// 「对象层求值 → 写回收尾（promote 换入 / 懒降阶清退 / 删空排空）」全程，杜绝
/// 「已 ACK 落旧树随 replace=true 换入被整树顶替」的丢失形与镜像 AOF 乱序。
///
/// `Ok(None)` = 键不在分层态（claim 窗内键不可被并发排空——DEL/判活被 load_meta
/// 门禁视同不存在；窗内 None 仅余本命令臂先行删空自愈的键消亡态，调用方落信封
/// 通道按 Missing 新建承接）；`Err(())` = 同键 claim 被并发迁移持有（存储忙，
/// 客户端重试收敛）或存储 IO 失败
pub(crate) async fn tiered_materialize_blob_sealed<D: Device>(
  session: &StoreSession<D>,
  key: &[u8],
  tag: GarnetObjectType,
) -> Result<Option<(Vec<u8>, SwapInWindowGuard)>, ()> {
  let Some(window) = session.try_swap_in_window(key) else {
    return Err(());
  };
  let Some((meta, mut stub)) = session
    .load_collection_stub_in_window(key)
    .await
    .map_err(|_| ())?
  else {
    // 键已消亡（封窗前树内臂删空穿透）：出窗失败，交调用方信封通道承接
    return Ok(None);
  };
  Ok(
    tiered_materialize_blob(session, key, tag, &meta, &mut stub)
      .await?
      .map(|blob| (blob, window)),
  )
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
  // List 到期豁免单点（与 [`exec_tiered_scan`] 扫描臂 `!= List` 豁免同源，
  // 一份数据一副面孔）：List 树记录载荷是元素本身，不是 member+expiry 帧
  // （`wcol::types::member_ttl` 模块头「Set / List 记录恒为裸载荷」），恒
  // 「零字段级到期」——出账内核 [`expire_sweep_or_rebuild`] 的逐记录
  // `decode_member` 刻度判定对 List 恒不适用，故在进入该内核前按 tag 单点
  // 分流（裁决禁止在 slow.rs 计数臂另写第二套 List 判定）
  if tag == GarnetObjectType::List {
    return exec_list_collect_watermark_repair(session, key, meta, stub).await;
  }
  let mut ctx = TieredCtx::new(&mut meta, &mut stub);
  // 收集执行体是写臂（到期成员出账重灌）：独占写锁 + 锁内刷新元记录，
  // 键已被并发排空回收则按「键非分层态」穿透
  let Some(tree_guard) = tiered_guard(session, key, &mut ctx, true).await? else {
    return Ok(None);
  };
  // 守卫在 Below 出口原样奉还后即释放；Swept 出口重灌/回写已在内完成。
  // 推进判据统一取 `ctx.dirty`（与 [`finish_tiered_arm`] 同一判据单点）：
  // Below / 零到期仅水位前移（FOLLOW-UP 内部元记录收敛，树内容零变更）/ 页
  // 缓存预算耗尽推迟臂均不置脏，零推进——纯内部修复不得误夭折并发 WATCH
  // 事务（对齐既有「零到期零写不推进」文注）；到期成员物理出账重灌置脏即
  // 客户端可见变更，推进 WATCH 版本栅栏（对标 C# HashCollect 走 RMW 写钩子
  // IncrementVersion）。失败臂经 finish 收尾兜底：删空墓碑已落盘 / 重灌已
  // 换入的失败树内容已实际变更，漏推进即 WATCH 漏通知（版本看似未变而数据
  // 已改），与四族臂同判据收口后再上抛存储错误
  if expire_sweep_or_rebuild(session, key, &mut ctx, tree_guard, |_| {})
    .await
    .is_err()
  {
    let _ = finish_tiered_arm(session, key, &ctx, Err(()));
    return Err(());
  }
  if ctx.dirty {
    session.bump_watch_version(key);
  }
  Ok(Some(ctx.meta.size))
}

/// List 分层收集臂豁免执行体（不出账不扫树，水位纯修复）：List 恒「零字段级
/// 到期」，本函数把计数慢路径 Degrade 分支对 List 的语义收敛为纯水位自愈——
///
/// - 水位内（`now <= next_expiry`，正常态 `next_expiry` 恒 `i64::MAX`，见
///   [`super::common::earliest_expiry`] 无挂 TTL 全 None 归 MAX）：零树访问
///   直读 `size`，与 [`super::common::sweep_expired_members`] 水位快路径同判据；
/// - 水位越线（`next_expiry` 被伪造成过去 / 异常写低，LLEN 才会落 Degrade 进
///   本臂）：归 `i64::MAX` 后经 `save_bftree_meta_stub` 回写元记录（与
///   [`super::common::expire_sweep_or_rebuild`] 零到期臂同一回写形态，写锁
///   守卫持有窗口内完成互斥覆盖装载→回写）。不出账不置脏：树内容零变更，
///   不推进 WATCH 版本栅栏（对齐既有「零到期零写不推进」判据）。
///
/// 不豁免的后果（本票缺陷本质）：出账内核逐记录 `decode_member` 解刻度，树内
/// 任何 `0x01` 首字节且总长 ≥ 9B 的 List 记录（手工构造 / 历史形态 / 后续
/// 新增臂）其元素载荷前 8B 被宽松解码读成远古假刻度，`ticks < now` 即被判
/// 「已到期」物理出账——真实成员静默丢失而客户端零写入。
///
/// 写锁内的锁内刷新（[`tiered_guard`] write 臂）兼作双检：并发臂可能已前移
/// 水位，刷新后水位内即零写直回（与 [`super::common::tiered_count`] 升级臂
/// 同形）。`Ok(None)` = 键被并发排空回收（写锁终态判定，与收集执行体穿透
/// 口径一致）；`Err(())` = 存储 IO 失败（水位回写失败上抛，禁吞错伪装成功）
async fn exec_list_collect_watermark_repair<D: Device>(
  session: &BatchStoreSession<'_, D>,
  key: &[u8],
  mut meta: wval::MetaValue,
  mut stub: wbftree::RangeIndexStub,
) -> Result<Option<u64>, ()> {
  if now_ticks() <= meta.next_expiry {
    return Ok(Some(meta.size));
  }
  let mut ctx = TieredCtx::new(&mut meta, &mut stub);
  let Some(_tree_guard) = tiered_guard(session, key, &mut ctx, true).await? else {
    return Ok(None);
  };
  if now_ticks() <= ctx.meta.next_expiry {
    return Ok(Some(ctx.meta.size));
  }
  ctx.meta.next_expiry = i64::MAX;
  session
    .save_bftree_meta_stub(key, ctx.meta, ctx.stub)
    .await
    .map_err(|_| ())?;
  Ok(Some(ctx.meta.size))
}

/// 分层态 SCAN 族（HSCAN/SSCAN/ZSCAN）树内游标扫描
///
/// 对标 SharedObjectCommands.cs 的 ObjectScan 分层态臂——C# 对任意规模
/// 对象恒可用，升阶键不得回内部错误。游标复刻 wcol 对象层 scan 口径（
/// hash_object.rs:scan / set_object.rs:scan / sorted_set_object.rs:scan 单源）：
/// 起始游标 = 已扫条目计数，单轮收集匹配项至 count 截断（成员+值对计数），
/// 扫至树尾光标归零；MATCH 全局通配、COUNT 钳制 OBJECT_SCAN_COUNT_LIMIT、
/// NOVALUES（zset 对齐对象层忽略该旗标）逐项一致。树内流式直写出帧
/// （帧头预留-回填，同族 SMEMBERS/HGETALL/LRANGE 先例），无逐条目堆物化
/// 中间容器，对位 C# out List 引用收集的零拷贝形态
///
/// 栈深度口径（勿按「COUNT 截断」误读为成本有界）：起始游标以「跳过 start 条」
/// 实现，故本臂自树头起遍历，**遍历**条数与单帧深度都不受 COUNT 约束——底层游标
/// 对墓碑的尾递归自调必须在单次 `next()` 内跳完游标后的整段连跑（≈680B/条），
/// 回调早停与 count 均来不及生效（实测 task/reject/tiered-zset-demote-stack.md
/// 二.表 S4）。本臂的安全性来自「树内零墓碑」的写形不变量，不来自本函数的截断
///
/// 游标序域切换说明：集合在跨升阶（信封到树）或懒降阶（树到信封）过程中，游标序域
/// 会在哈希序与字典序之间切换，因此在这期间并发的流式 SCAN 迭代不保证游标不重不漏。
///
/// `Ok(true)` 已闭环应答；`Ok(false)` 装载即键不存在 / 非分层态（门禁探测
/// 与 claim 在册回退装载经读臂装载单点
/// [`load_collection_stub_for_read`] 收口，调用方维持既有路径）；
/// `Err(())` 存储 IO 失败
pub(crate) async fn exec_tiered_scan<D: Device>(
  session: &StoreSession<D>,
  key: &[u8],
  object_type: GarnetObjectType,
  args: &[&[u8]],
  scan_count_limit: i32,
  output: &mut Vec<u8>,
  _resp_protocol_version: u8,
) -> Result<bool, ()> {
  use wbase::glob::glob_match;
  use wcol::types::scan_input::read_scan_input;

  let Some((mut meta, mut stub)) = load_collection_stub_for_read(session, key).await? else {
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
  // 扫描返回字段恒取键值对：到期过滤（member_expired_at）必须在真实值域上
  // 求值，Key 模式回调值恒为空切片会使 NOVALUES 臂的字段级到期剔除失效
  // （对标 C# HashObject.cs:Scan 368-425 行——IsExpired 过滤位于产出之前，与
  // isNoValue 旗标正交）。NOVALUES 语义由下方输出臂单点承载（不 push 值项），
  // 值切片为读缓冲借用零额外堆分配
  let return_field = ScanReturnField::KeyAndValue;

  let tree_guard = session
    .acquire_tree_read(key, &mut stub)
    .await
    .map_err(|_| ())?;
  let tree = tree_guard.tree();

  // 锁窗内元记录刷新单点（与 [`tiered_guard`] 读臂逐臂同形，复用
  // `refresh_tiered_meta` 既有机制，禁第二套；对标 C# ObjectScan 记录锁内
  // 装载即扫描契约 SharedObjectCommands.cs:ObjectScan → objectContext.Read）：
  // 装载与取锁之间键可能已迁移 / 排空 / 增删，total（游标收敛与帧头预留上界）
  // 一律以锁内新值为准，杜绝陈旧 size 驱动游标提前归零致跨页漏扫
  // （修复型分叉已登 deviations §129，claim 回退残余形同条登记）
  let mut key_vanished = false;
  match session
    .refresh_tiered_meta(key, &mut meta, Some(&mut stub))
    .await
  {
    // 刷新成功：meta 与树内容在共享读锁窗口内互一致
    Ok(true) => {}
    // 键消亡：应答缺失语义（size 记 0，同 tiered_guard 读臂口径，
    // size 为 0 的读臂不触碰树，出 [0, 空] ≅ 对象缺失）
    Ok(false) => {
      meta.size = 0;
      key_vanished = true;
    }
    // 迁移 claim 在册：回退装载快照照常扫（读面不扩大忙拒面，见 tiered_guard 头注；
    // 残余 items_upper 上界失配由预留-回填帧头兜底，wresp ext.rs 收窄/加宽两向合法）
    Err(Error::MigrationBusy) => {}
    // 真实存储 IO 失败沿既有漏斗上抛——此时未落帧，无需撤帧
    Err(_) => return Err(()),
  }

  // 起始游标越过总量入遍历前早退守卫（对位 C# 对象层 Scan 三臂同型早退
  // HashObject.cs:373 / SetObject.cs:195 / SortedSetObject.cs:471
  // `if (Count < start) { cursor = 0; return; }`，内存态三臂守卫在位，本臂
  // 漏抄——票 task/ing/wnode-tiered-scan-start-cursor-overflow.md）：
  // start 越过锁窗内刷新后 meta.size（与尾段收敛 total 同源）时全部存活条目
  // 恒走 skipped 臂空走全树，且尾段 scan_converge_cursor 判定
  // `cursor + expired >= total` 裸加法在 start 逼近 i64::MAX 且到期成员驻留
  // （expired >= 1）时溢出——debug panic / release 回绕负值恒不命中，回显
  // 垃圾游标死循环。守卫并入下方 key_vanished 臂同款收敛：不触碰树、n = 0、
  // expired = 0，scan_converge_cursor(start, 0, size) 因 start > size 恒真
  // 自然归零，经预留-回填帧头出 [0, 空] ≅ 内存态守卫早退应答（双态透明），
  // 零新机制零新帧形。claim 回退臂（MigrationBusy 装载快照）同被覆盖：
  // 快照 size 越界亦早退，早退是正常应答非错误帧，不扩大忙拒面。
  // 勿删：非可删防御分支，删除即恢复溢出死循环面。
  let start_beyond = (meta.size as i64) < start;

  // 纯读面不回写 meta（水位由计数臂 / 收集执行体收敛），到期成员只过滤
  // 不出产出（C# 对象层 Scan 前经 DeleteExpiredItems 的过滤等价）；扫描 Err
  // 经 [`scan_all_from_head`] 上抛前先 `output.truncate(base)` 撤帧（连同预留头回到
  // 臂进入点，应答未落帧，上游闭环成 RESP 错误帧，wresp 预留-回填不变量 2），
  // 严禁折成空游标页——存储故障不得伪装成「扫描完毕」
  let now = now_ticks();

  // 落帧序（帧头预留-回填直写，与对象层 scan_operate_shared 同款、同族
  // SMEMBERS/HGETALL/LRANGE 先例）：外层 *2 头直写 → 游标帧位预留（游标
  // 终值 <= meta.size 恒成立，位宽按 digits(size) 上界估算）→ 条目数组头
  // 预留（成对形态按 2×size 上界）→ 扫描回调内按对象型直写 → 回填实际
  // 出帧计数与游标终值（回填序先条目头后游标位，后者搬移已定型条目帧整段）
  let base = output.len();
  output.write_resp_array_len(2);
  let write_cursor = |buf: &mut Vec<u8>, n: usize| buf.write_resp_int_as_bulk_string(n);
  let cursor_reserved = resp_frame_head_len(meta.size as usize, write_cursor);
  let cursor_base = reserve_resp_frame_head(output, cursor_reserved);
  let write_items = |buf: &mut Vec<u8>, n: usize| buf.write_resp_array_len(n);
  let items_upper = match object_type {
    GarnetObjectType::Hash if !params.is_no_value => (meta.size as usize).saturating_mul(2),
    GarnetObjectType::SortedSet => (meta.size as usize).saturating_mul(2),
    _ => meta.size as usize,
  };
  let items_reserved = resp_frame_head_len(items_upper, write_items);
  let items_base = reserve_resp_frame_head(output, items_reserved);

  // 出帧条目计数（截断比较口径：发出计数 != want 即续扫，1:1 对位旧
  // items.len() != want 的「发出计数」形态）
  let mut n = 0usize;
  let mut skipped = 0_i64;
  let mut scanned = 0_i64;
  let mut expired = 0_i64;
  // 键消亡（size 记 0）臂与起始游标越界臂均不触碰树（tiered_guard 读臂同口径），
  // 回调整体跳过，经下方预留-回填帧头自然出 [0, 空] 应答 ≅ 对象缺失/守卫早退，
  // 不走错误帧
  if !(key_vanished || start_beyond)
    && scan_all_from_head(tree, return_field, |k, v| {
      // List 豁免：List 分层值域非 member_ttl 编码（索引+元素裸格式），
      // 按到期解码必假阳性，整体跳过过滤；Set 成员值为
      // SET_MEMBER_DUMMY_VALUE（b"1111"）裸哨兵，首字节不在 member_ttl
      // 旗标域 0/1 内，member_expired_at 恒假——豁免语义天然成立，无需
      // 并入 List 豁免臂（单源判据见 wcol member_ttl::member_expired_at）
      if object_type != GarnetObjectType::List && member_expired_at(v, now) {
        // 到期成员计入光标基数（对齐 C# scan 的 expiredKeysCount 口径，供尾段
        // [`scan_converge_cursor`] 单点判定：cursor + expired >= total 归零收敛）
        expired += 1;
        return true;
      }
      // 起始游标之前的有效存活条目跳过（不产出，推进已跳过计数）
      if skipped < start {
        skipped += 1;
        return true;
      }
      if params.pattern.is_empty() || glob_match(params.pattern, k) {
        match object_type {
          GarnetObjectType::SortedSet => {
            let (_, payload) = decode_member(v);
            output.write_resp_bulk_string(k);
            // 分值统一走 write_resp_double_bulk_string 文本化（非有限值输出
            // "inf"/"-inf"，与内存态 ZSCAN 及 ZRANGE 单源收口，见 doc/zh/deviations.md §80；
            // 订正旧注：C# Utf8Formatter 恒成功，null 项系上游不可达死臂）
            output.write_resp_double_bulk_string(score_of_payload(payload).unwrap_or(0.0));
            n += 2;
          }
          GarnetObjectType::Hash => {
            output.write_resp_bulk_string(k);
            // NOVALUES 臂不写值项（只出字段，截断比较口径同步减半）
            if !params.is_no_value {
              output.write_resp_bulk_string(decode_member(v).1);
              n += 2;
            } else {
              n += 1;
            }
          }
          _ => {
            output.write_resp_bulk_string(k);
            n += 1;
          }
        }
      }
      scanned += 1;
      // C# 以相等判断截断（负 COUNT 恒不命中 → 全量遍历；count=0 首个
      // 未命中条目即停的上游怪癖一并保留）。极值点 1:1 失真已登记：C#
      // int32 侧 COUNT=-2147483648 翻倍回绕为 0 致首条未命中即停，rust
      // i64 加宽不复刻该回绕（登记见 doc/zh/deviations.md 第 20 条 d)，
      // 勿按 C# 回改）
      (n as i64) != want
    })
    .is_err()
  {
    output.truncate(base);
    return Err(());
  }

  // 扫至树尾光标归零：走 [`scan_converge_cursor`] 单点判定（本轮结束游标
  // start + scanned 叠加到期垫数 >= total 即归零），与内存态 hash/zset 同口径，
  // 杜绝双态应答分叉。经上方 start 越界守卫后操作数恒有界（start <= size、
  // scanned <= size，加法两侧 <= 2×size），i64 溢出物理不可能，无需饱和算术
  let next_cursor = scan_converge_cursor(start + scanned, expired, meta.size as i64);

  backfill_resp_frame_head(output, items_base, items_reserved, n, write_items);
  backfill_resp_frame_head(
    output,
    cursor_base,
    cursor_reserved,
    next_cursor as usize,
    write_cursor,
  );
  Ok(true)
}
