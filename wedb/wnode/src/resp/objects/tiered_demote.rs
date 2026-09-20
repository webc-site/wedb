//! 分层态后台懒降阶评估轮（doc/zh/collection.md 3.2/3.3「后台紧缩异步降阶」半边）
//!
//! 前台写收尾（rmw_helpers::apply_rmw_post_operate 懒降阶臂）只覆盖被写触碰
//! 的分层键；升阶后再无写入的冷分层键须由后台周期评估回收树文件与页缓存，否则
//! 迟滞死区之下的分层态永不回归内存信封。评估点接进既有周期对象收集节拍
//! （[`crate::primary_tasks`] 的 ObjectCollectTaskAsync 对标任务），不放第二套调度器、
//! 不新增配置旋钮；双入口共享本模块唯一执行体（对标仓内 collect_expired 内核与
//! EXPDELSCAN 命令的双入口单内核先例）。
//!
//! 单一机制纪律：
//! - 跨全量域扫描：预筛对物理键直接解码 (vns, vdb)（[`NamespaceDbCodec`] 单点），
//!   不做会话前缀过滤，全库 ns/db 的分层键同轮入围——SKILL「同一个 Namespace 的
//!   不同 DB 可以是不同槽位」的多租户设计对全体键兑现迟滞死区承诺；单候选评估
//!   与写回前先 [`wkv::StoreSession::set_virtual_context`] 落条目物理域，装载、
//!   物化、信封写回与树清退的键拼装全部出自会话域单点，落域即全链路换域，与
//!   内置 GC 过期删除的逐键落域先例同型（wkv gc/ttl_sweep）；
//! - 预筛只走元记录直读（hlog 单趟扫描携带的 MetaValue 头 32B，条目维
//!   `meta.size <= wcol::TIERED_DEMOTE_THRESHOLD`，零树访问、单轮限批截断）；
//! - 体积维决策复用同一谓词单点 [`wcol::IGarnetObject::should_demote`]（count AND
//!   heap_bytes 双维），经分层物化单源通道 [`tiered_materialize_blob`] 构造内存
//!   对象后判定，绝不新增第二套降阶阈值判断，绝不全树扫体积
//!   （MetaValue 无体积标量，wval/src/meta.rs）；
//! - 死亡域守卫：解出域经 [`wkv::VirtualDbManager::is_dead_domain`] 判死即跳过
//!   （预筛一处 + 落盘前复判一处）——FLUSHDB/FLUSHNS 换号退役、紧缩未及回收的
//!   旧域 Meta 记录仍滞留 hlog，写回即向死亡域脏写（登记面先例
//!   wkv store/reclaim.rs 死亡域拒登；紧缩面 wkv/src/compact.rs 同豁免同源）；
//!   不做 hopeless 负缓存：候选集逐轮新建；gxhash deterministic 固定种子下
//!   去重表迭代序逐轮恒定（插入序 = hlog 稳定追加序），限批若按迭代序恒取
//!   头部，恒败谓词的死区体积键会永久挤占全部席位、饿死真正可降阶的冷键，
//!   故截断前对候选集洗牌随机出列（[`demote_batch`]），16 次全树物化本就是
//!   限批常量明定的轮次 I/O 预算（体积维不进预筛本身是 MetaValue 无体积
//!   标量的既定设计，见 task/reject/my-demote-volume-prefilter.md）；
//! - 落地写回复用前台懒降阶臂同一组合：obj_save 信封写回 +
//!   handle_bftree_drain_and_delete 树清退（含元记录墓碑、旁表注销与
//!   RangeIndexDrop AOF 入账；keep_ttl=true——键换域存活，不碰随键 TTL 旁路，
//!   杜绝一次降阶静默抹掉 EXPIRE 并经 TtlWrite(expire_at=None) 镜像成 Persist
//!   扩散到从库与 AOF 回放面；对标 C# 对象记录重写原样前移 HasExpiration）；
//!   WATCH 栅栏口径逐字同前台 else 臂——
//!   信封写回臂由 wkv 用户键写入口恰一次推进，树清退臂不重复推进（一命令一推进）；
//! - 竞态防护：自迁移安全换入窗（try_swap_in_window，claim 先于基线读）+
//!   落盘前重读元记录校验 key_id/size 双重裁决，窗内前台同键写臂被探测门
//!   MigrationBusy 拒（含 key_id/size 双不变的 content-only 覆写——旧栅栏的
//!   穿透形），漂移即本轮放弃（零副作用、不落盘、下轮再看），杜绝「后台已
//!   清树、前台仍向树写」与「前台已 ACK 写随旧树降阶销毁」的双向丢失；
//!   形态与前台迁移臂同 AOF/复制语义，重启与从库回放后一致。
//!
//! 对标 C#：分层引擎为仓内自定义架构（garnet 集合恒驻对象域，无降阶对应物），
//! 降阶判据唯一对标 doc/zh/collection.md 第 3 章；后台周期清扫的调度形态对标
//! libs/server/StoreWrapper.cs:ObjectCollectTaskAsync（:722 频率节拍 +
//! CancellationToken）与 libs/server/Databases/DatabaseManagerBase.cs:
//! ExecuteObjectCollection（:343 专用收集会话逐键处理）——副本角色挂起与频率
//! 槽位禁用即退出均复用宿主任务的既有门检（C# 单租户单域无跨域对位；多租户
//! 多库为 rust 自定义设计，doc/zh/db.md）。

use std::sync::Arc;

use fastrand::Rng;
use log::{info, warn};
use wbase::map::HashMap as GxHashMap;
use wcol::{
  HashObject, IGarnetObject, ListObject, SetObject, SortedSetObject, TIERED_DEMOTE_THRESHOLD,
  object_payload::GarnetObjectPayload,
};
use wdev::Device;
use wkv::WedbStore;
use wval::{GarnetObjectType, KeyTag, META_VALUE_SIZE, MetaValue, NamespaceDbCodec};

use super::tiered_collection_ops::tiered_materialize_blob;
use crate::storage::session::storage_session::StorageSession;

/// 单轮后台降阶评估的分层键上限：每候选一次全树物化（O(N) 页读），
/// 限批只界定**轮次 I/O 次数**（候选数 × 一次全树物化），低负载时段推进
/// （doc/zh/collection.md 3.3）。
///
/// 本上限**不界定栈深度**，勿据「成本有界」再推出「物化扫树深度有界」：全树物化
/// 走 wbftree 游标，而底层 `ScanIter::next` 对墓碑是尾递归自调（rustc 无 TCO），
/// 单次 `next()` 的栈用量 = 游标之后的连续墓碑条数 × ≈680B，与限批、与 count、
/// 与上界键均无关（实测：task/reject/tiered-zset-demote-stack.md 一.帧表、二.表
/// S2/S4）。深度不变量因此只能由写形给出：分层集合的删除不逐成员落树墓碑，
/// 一律经整值重灌面重建（`rmw_helpers::apply_rmw_post_operate` →
/// `promote_collection_to_bftree` → `bulk_load`），使树内零墓碑。
const DEMOTE_MAX_KEYS_PER_ROUND: usize = 16;

/// 扫描阶段候选映射表：(vns, vdb, user_key) -> Option<GarnetObjectType>
type DemoteCandidateMap = GxHashMap<(u64, u64, Vec<u8>), Option<GarnetObjectType>>;

/// 一轮降阶评估统计（观测面：候选数与降阶数可观测，零命中静默）
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TieredDemoteStats {
  /// 预筛命中的分层键候选数（≤ [`DEMOTE_MAX_KEYS_PER_ROUND`]）
  pub candidates: usize,
  /// 本轮实际降阶回内存信封的键数
  pub demoted: usize,
  /// 判定不通过或竞态基线漂移而本轮放弃的键数（零副作用）
  pub aborted: usize,
}

/// 单轮分层键后台降阶评估（周期对象收集任务与手动驱动共用的唯一执行体）
///
/// 扫描跨全量活跃域：预筛对物理键直接解码，无会话域约束；单候选评估与写回前
/// 先落条目物理域（见模块头「跨全量域扫描」）。会话形态沿用周期收集轮先例：
/// 独立批处理纪元内完成，版本推进实际经引擎级写面钩子收敛到共享版本表（与
/// [`wcol`] 域既有后台写臂 exec_tiered_collect 同型）。
pub async fn tiered_demote_round<D: Device>(store: &Arc<WedbStore<D>>) -> TieredDemoteStats {
  let Ok(session) = store.new_session() else {
    return TieredDemoteStats::default();
  };
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  let candidates = match collect_demote_candidates(store).await {
    Ok(v) => v,
    Err(e) => {
      warn!("后台降阶评估扫描失败，留待下轮: {e}");
      return TieredDemoteStats::default();
    }
  };
  let mut stats = TieredDemoteStats {
    candidates: candidates.len(),
    demoted: 0,
    aborted: 0,
  };
  for (vns, vdb, key, tag) in candidates {
    // 落条目物理域再评估与写回：会话域即条目域，全链路键拼装单点换域
    storage.batch.set_virtual_context(vns, vdb);
    match demote_candidate(store, &storage, vns, vdb, &key, tag).await {
      Ok(true) => stats.demoted += 1,
      // Ok(false) = 键态漂移/判定不通过/竞态基线不符：本轮零副作用，下轮再看
      Ok(false) => stats.aborted += 1,
      Err(()) => {
        // 存储 IO 失败：单键留痕中断本候选，错误不放大为轮次失败，下轮自愈重试
        warn!(
          "后台降阶评估单键写回失败，留待下轮: key={:?}",
          String::from_utf8_lossy(&key)
        );
        stats.aborted += 1;
      }
    }
  }
  if stats.candidates > 0 {
    info!(
      "后台分层键降阶评估轮完成: 候选={}, 降阶={}, 本轮放弃={}",
      stats.candidates, stats.demoted, stats.aborted
    );
  }
  stats
}

/// 分层键候选预筛（O(1) 条目维、零树访问，不扫树）
///
/// 与周期收集内核 [`crate::resp::garnet_api::object_collect_all`] 共用 whlog
/// 单一遍历引擎（`ScanIterator::next_ref`），截断口径按各自成本分流：本预筛
/// 单趟 hlog 扫描 + 单轮限批截断（截断前洗牌随机出列，见 [`demote_batch`]；
/// 候选须逐键全树物化，故名额更紧），周期收集
/// 走地址游标分批流式（键仅 O(1) 头部计数直读，无须限批）。物理键经
/// [`NamespaceDbCodec::decode_tagged_key`] 直接解出 (vns, vdb, tag, user_key)，
/// 无会话前缀约束（跨全量域扫描，对标 wkv/src/compact.rs 紧缩谓词先例）；
/// 直读记录内 MetaValue 头 32B，按非 RangeIndex、存活、条目计数 ≤ 降阶低水位
/// 预筛。hlog 追加序下同键取最新元记录（后见覆盖、墓碑判不命中），杜绝陈旧
/// 低计数记录挤占限批名额；换号退役的死亡域在预筛直读即剔除（见模块头
/// 「死亡域守卫」），**评估轮的 I/O 次数**有界——该有界性不覆盖候选物化扫树的
/// 栈深度（每候选一次全树游标，单次 `next()` 深度 = 游标后连续墓碑条数 × ≈680B，
/// 与限批无关，见 [`DEMOTE_MAX_KEYS_PER_ROUND`] 与
/// task/reject/tiered-zset-demote-stack.md 一.帧表）。残余陈旧窗口（本趟扫描后
/// 键即被前台写推高、或写回前撞上换号）由落地前 load_collection_stub 最新读、
/// 竞态基线与死亡域复判三重裁决，零误降零脏写。
async fn collect_demote_candidates<D: Device>(
  store: &Arc<WedbStore<D>>,
) -> wkv::Result<Vec<DemoteCandidate>> {
  // 追加序单趟扫描 latest-wins：域内用户键 → 最新元记录的类型预筛判定
  let mut latest = DemoteCandidateMap::default();
  store
    .hlog()
    .scan(store.begin_address(), store.tail_address(), |_addr, rec| {
      let Ok((vns, vdb, tag, user_key)) = NamespaceDbCodec::decode_tagged_key(rec.key()) else {
        return Ok(true);
      };
      if tag != KeyTag::Meta {
        return Ok(true);
      }
      // 死亡域守卫（预筛臂）：换号退役旧域的滞留记录直读即剔除，不占限批名额
      if store.vdb.is_dead_domain(vns, vdb) {
        return Ok(true);
      }
      let slot = latest.entry((vns, vdb, user_key.to_vec())).or_insert(None);
      *slot = if rec.is_tombstone() {
        None
      } else {
        let value = rec.value();
        // 不足元记录头长的短记录非元记录形态，判不命中放行（collect_keys 同口径）
        MetaValue::from_slice(&value[..value.len().min(META_VALUE_SIZE)])
          .ok()
          .filter(|meta| {
            !meta.is_range_index()
              // is_live：size > 0（删空自愈保证存活元记录计数非零）
              && meta.is_live()
              // 条目维预筛（AND 判定之必要非充分条件），体积维留待物化后单点谓词
              && meta.size <= TIERED_DEMOTE_THRESHOLD as u64
          })
          .filter(|meta| {
            matches!(
              meta.collection_type,
              GarnetObjectType::Hash
                | GarnetObjectType::Set
                | GarnetObjectType::SortedSet
                | GarnetObjectType::List
            )
          })
          .map(|meta| meta.collection_type)
      };
      Ok(true)
    })
    .await?;
  let all: Vec<DemoteCandidate> = latest
    .into_iter()
    .filter_map(|((vns, vdb, key), cand)| cand.map(|tag| (vns, vdb, key, tag)))
    .collect();
  Ok(demote_batch(all, &mut Rng::new()))
}

/// 单个降阶候选：(物理域 vns, 物理域 vdb, 用户键, 集合类型)。域对来自物理键
/// 前缀解码，评估与写回前须先落域（set_virtual_context），会话域即条目域
type DemoteCandidate = (u64, u64, Vec<u8>, GarnetObjectType);

/// 限批出列：截断前洗牌消解确定性饿死（见模块头「不做 hopeless 负缓存」），
/// 每轮恰一次 O(n) 交换，候选键自去重表移动入列、无复制分配。随机源用
/// fastrand（与 wbase papaya 种子承接同源同熵，进程级非确定、不落盘），显式
/// 入参以便固定种子单测复现；禁用 gxhash 充当随机源（deterministic 种子正是
/// 迭代序恒定、饿死成形的根源）
fn demote_batch(mut cands: Vec<DemoteCandidate>, rng: &mut fastrand::Rng) -> Vec<DemoteCandidate> {
  rng.shuffle(&mut cands);
  cands.truncate(DEMOTE_MAX_KEYS_PER_ROUND);
  cands
}

/// 单候选键降阶评估与落地（判定不通过/态漂移零副作用；`Err(())` 存储 IO 失败）
///
/// 自迁移安全换入窗（[`StoreSession::try_swap_in_window`]）覆盖「基线读 → 物化
/// 全扫 → 竞态栅栏 → 信封写回 + 树清退」全程：claim 先于基线读登记——窗内前台
/// 同键写臂被四探测门 MigrationBusy 拒，key_id/size 双不变的 content-only 写
///（HSET 覆写存活成员）恰是旧栅栏的穿透形，封窗后自基线起即被排除，杜绝
/// 「后台已降阶清树、前台已 ACK 写随旧树销毁」的丢失形；窗内无写提交，降阶流
/// AOF 序仍与树内提交序一致。装载一律走未门禁形态
/// [`load_collection_stub_in_window`](wkv::StoreSession::load_collection_stub_in_window)
/// （claim 持有者自装载不得被自身封窗拒绝）。claim 被占（并发迁移）= 本轮放弃
/// 零副作用，下轮再看
async fn demote_candidate<D: Device>(
  store: &Arc<WedbStore<D>>,
  storage: &StorageSession<'_, D>,
  vns: u64,
  vdb: u64,
  key: &[u8],
  tag: GarnetObjectType,
) -> Result<bool, ()> {
  let Some(_swap_in_window) = storage.batch.try_swap_in_window(key) else {
    return Ok(false);
  };
  // 预读元记录（含 probe_alive 惰性过期裁决与存活核对），建立竞态基线
  let Some((meta_before, stub_before)) = storage
    .batch
    .load_collection_stub_in_window(key)
    .await
    .map_err(|_| ())?
  else {
    return Ok(false);
  };
  if meta_before.collection_type != tag {
    return Ok(false);
  }
  // 扫描窗口内键已被前台写推高开外（陈旧记录假候选）：最新计数直读复核，
  // 零树访问即出局
  if meta_before.size > TIERED_DEMOTE_THRESHOLD as u64 {
    return Ok(false);
  }
  // 物化单源通道：树全扫还原信封载荷（四族支持，已到期成员剔除不复活；
  // 封窗内预载基线 meta/stub，不重复装载）
  let Some(blob) =
    tiered_materialize_blob(&storage.batch, key, tag, &meta_before, &stub_before).await?
  else {
    return Ok(false);
  };
  // 双维单点判定；空对象（物化剔除到期成员后为零）本轮跳过，交由既有删空自愈
  // 臂处理，后台不造空信封
  if !demote_target(tag, &blob) {
    return Ok(false);
  }
  // 落盘前竞态栅栏：重读元记录校验 key_id/size 与预读基线一致，漂移即放弃本轮
  // （封窗后的纵深防御：正常无写者可越门，防 claim 外残余形态）
  let Some((meta_now, _)) = storage
    .batch
    .load_collection_stub_in_window(key)
    .await
    .map_err(|_| ())?
  else {
    return Ok(false);
  };
  if meta_now.key_id != meta_before.key_id || meta_now.size != meta_before.size {
    return Ok(false);
  }
  // 落盘前死域复判（守卫第二臂）：扫描与写回间隙撞上 FLUSHDB/FLUSHNS 换号，
  // 域已退役即零副作用放弃——紧缩未回收的滞留记录绝不向死亡域脏写
  if store.vdb.is_dead_domain(vns, vdb) {
    return Ok(false);
  }
  // 前台懒降阶臂同一写回组合（rmw_helpers::apply_rmw_post_operate else 臂）：
  // obj_save 信封整值写回（wkv 用户键写入口恰一次推进 WATCH 栅栏并自带入账），
  // 树清退仅回收残留物理页与注销登记，不重复推进（一命令一推进）；
  // keep_ttl=true 与前台 else 臂同口径：键换域存活，键级 TTL 不随树清退脱落。
  // 封窗守卫随成功/失败/panic 展开一律出函数即释
  storage.obj_save(key, tag, &blob).await.map_err(|_| ())?;
  storage
    .batch
    .handle_bftree_drain_and_delete(key, true)
    .await
    .map_err(|_| ())?;
  Ok(true)
}

/// 双维单点谓词适配：按标签构造内存对象并走 [`IGarnetObject::should_demote`]
/// （即 wcol::should_demote，count AND heap_bytes），本模块零阈值散点；
/// 物化载荷解码 fail-fast——畸形（内部往返本不应出现，出现即编解码缺陷）
/// 落错误日志并按不降阶保守处理，绝不回退空对象
fn demote_target(tag: GarnetObjectType, payload: &[u8]) -> bool {
  let decoded = match tag {
    GarnetObjectType::Hash => {
      HashObject::from_blob(payload).map(|o| (o.is_empty(), o.should_demote()))
    }
    GarnetObjectType::Set => {
      SetObject::from_blob(payload).map(|o| (o.is_empty(), o.should_demote()))
    }
    GarnetObjectType::SortedSet => {
      SortedSetObject::from_blob(payload).map(|o| (o.is_empty(), o.should_demote()))
    }
    GarnetObjectType::List => {
      ListObject::from_blob(payload).map(|o| (o.is_empty(), o.should_demote()))
    }
    _ => return false,
  };
  match decoded {
    Some((empty, demote)) => !empty && demote,
    None => {
      log::error!("tiered_demote: corrupted materialized payload, tag={tag:?}");
      false
    }
  }
}

#[cfg(test)]
mod tests {
  use fastrand::Rng;

  use super::*;

  /// 构造标记候选序：前 `dead` 个为死区键（过 meta.size 预筛、恒败物化后谓词、
  /// 元记录不改写下轮仍入围），其余为冷键；用户键首字节作标记位
  fn marked_keys(dead: usize, cold: usize) -> Vec<DemoteCandidate> {
    (0..dead + cold)
      .map(|i| {
        let marker = if i < dead { b'D' } else { b'C' };
        (0u64, 0u64, vec![marker, i as u8], GarnetObjectType::Hash)
      })
      .collect()
  }

  /// 限额内候选全保留（集合不变、仅次序随机）——候选不超限批时无饿死面
  #[test]
  fn batch_within_limit_keeps_all() {
    let cands = marked_keys(0, 8);
    let mut out = demote_batch(cands.clone(), &mut Rng::with_seed(42));
    out.sort_by(|a, b| a.2.cmp(&b.2));
    assert_eq!(out, cands);
  }

  /// 超限额恰取限额、无重复、不引入集合外候选
  #[test]
  fn batch_over_limit_truncates_exactly() {
    let cands = marked_keys(0, 64);
    let out = demote_batch(cands.clone(), &mut Rng::with_seed(42));
    assert_eq!(out.len(), DEMOTE_MAX_KEYS_PER_ROUND);
    assert!(out.iter().all(|c| cands.contains(c)), "出列候选须出自输入");
    let mut keys: Vec<&[u8]> = out.iter().map(|c| c.2.as_slice()).collect();
    keys.sort_unstable();
    keys.dedup();
    assert_eq!(keys.len(), DEMOTE_MAX_KEYS_PER_ROUND, "出列候选不得重复");
  }

  /// 死区键占满迭代序头部时冷键仍能出列：洗牌出列序与输入序独立，固定种子
  /// 保证断言确定可复现。单种子冷键全灭概率 = 1/C(24,16) ≈ 1.4e-6，64 个
  /// 种子下出列种子数下界 56 在 10σ 之外，零 flaky
  #[test]
  fn cold_keys_not_starved_when_deadzone_fills_head() {
    let cands = marked_keys(DEMOTE_MAX_KEYS_PER_ROUND, 8);
    let hits = (0..64u64)
      .filter(|s| {
        demote_batch(cands.clone(), &mut Rng::with_seed(*s))
          .iter()
          .any(|c| c.2[0] == b'C')
      })
      .count();
    assert!(hits >= 56, "死区占满头部时冷键出列种子数不足: {hits}/64");
  }
}
