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
//! - 预筛只走元记录直读（hlog 单趟扫描携带的 MetaValue 头 32B，条目维
//!   `meta.size <= wcol::TIERED_DEMOTE_THRESHOLD`，零树访问、单轮限批截断）；
//! - 体积维决策复用同一谓词单点 [`wcol::IGarnetObject::should_demote`]（count AND
//!   heap_bytes 双维），经分层物化单源通道 [`tiered_materialize_blob`] 构造内存
//!   对象后判定，绝不新增第二套降阶阈值判断，绝不全树扫体积
//!   （MetaValue 无体积标量，wval/src/meta.rs）；
//! - 落地写回复用前台懒降阶臂同一组合：obj_save 信封写回 +
//!   handle_bftree_drain_and_delete 树清退（含元记录墓碑、旁表注销与
//!   RangeIndexDrop AOF 入账；keep_ttl=true——键换域存活，不碰随键 TTL 旁路，
//!   杜绝一次降阶静默抹掉 EXPIRE 并经 TtlWrite(expire_at=None) 镜像成 Persist
//!   扩散到从库与 AOF 回放面；对标 C# 对象记录重写原样前移 HasExpiration）；
//!   WATCH 栅栏口径逐字同前台 else 臂——
//!   信封写回臂由 wkv 用户键写入口恰一次推进，树清退臂不重复推进（一命令一推进）；
//! - 竞态防护：落盘前重读元记录校验 key_id/size 与预读基线一致，不一致即本轮放弃
//!   （零副作用、不落盘、下轮再看），杜绝「后台已清树、前台仍向树写」的条目数
//!   可观测丢失更新；形态与前台迁移臂同 AOF/复制语义，重启与从库回放后一致。
//!
//! 对标 C#：分层引擎为仓内自定义架构（garnet 集合恒驻对象域，无降阶对应物），
//! 降阶判据唯一对标 doc/zh/collection.md 第 3 章；后台周期清扫的调度形态对标
//! libs/server/StoreWrapper.cs:ObjectCollectTaskAsync（:722 频率节拍 +
//! CancellationToken）与 libs/server/Databases/DatabaseManagerBase.cs:
//! ExecuteObjectCollection（:343 专用收集会话逐键处理）——副本角色挂起与频率
//! 槽位禁用即退出均复用宿主任务的既有门检。

use std::sync::Arc;

use gxhash::HashMap as GxHashMap;
use log::{info, warn};
use wcol::{
  HashObject, IGarnetObject, ListObject, SetObject, SortedSetObject, TIERED_DEMOTE_THRESHOLD,
  object_payload::GarnetObjectPayload,
};
use wdev::Device;
use wkv::WedbStore;
use wval::{GarnetObjectType, KeyTag, META_VALUE_SIZE, MetaValue};

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
/// 会话形态沿用周期收集轮先例：独立批处理纪元内完成，版本推进实际经引擎级
/// 写面钩子收敛到共享版本表（与 [`wcol`] 域既有后台写臂 exec_tiered_collect 同型）。
/// 会话默认域（主库 ns0/db0）与既有周期收集扫描口径一致。
pub async fn tiered_demote_round<D: Device>(store: &Arc<WedbStore<D>>) -> TieredDemoteStats {
  let Ok(session) = store.new_session() else {
    return TieredDemoteStats::default();
  };
  let batch = session.enter_batch();
  let storage = StorageSession::new_readonly(batch);
  let candidates = match collect_demote_candidates(&storage).await {
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
  for (key, tag) in candidates {
    match demote_candidate(&storage, &key, tag).await {
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
/// 单趟 hlog 扫描 + 单轮限批截断（候选须逐键全树物化，故名额更紧），周期收集
/// 走地址游标分批流式（键仅 O(1) 头部计数直读，无须限批）。直读记录内
/// MetaValue 头 32B，按非 RangeIndex、存活、条目计数 ≤ 降阶低水位预筛。hlog
/// 追加序下同键取最新元记录（后见覆盖、墓碑判不命中），杜绝陈旧低计数记录
/// 挤占限批名额；去重命中集按单轮上限截断，**评估轮的 I/O 次数**有界——该有界性
/// 不覆盖候选物化扫树的栈深度（每候选一次全树游标，单次 `next()` 深度 = 游标后
/// 连续墓碑条数 × ≈680B，与限批无关，见 [`DEMOTE_MAX_KEYS_PER_ROUND`] 与
/// task/reject/tiered-zset-demote-stack.md 一.帧表）。残余陈旧窗口
/// （本趟扫描后键即被前台写推高）由落地前 load_collection_stub 最新读与竞态
/// 基线双重裁决，零误降。
async fn collect_demote_candidates<D: Device>(
  storage: &StorageSession<'_, D>,
) -> wkv::Result<Vec<(Vec<u8>, GarnetObjectType)>> {
  let store = Arc::clone(&storage.batch.store);
  let prefix = storage.batch.session_prefix();
  let prefix_slice = prefix.as_slice();
  // 追加序单趟扫描 latest-wins：键 → 最新元记录的预筛判定结果
  let mut latest: GxHashMap<Vec<u8>, Option<GarnetObjectType>> = GxHashMap::default();
  store
    .hlog()
    .scan(store.begin_address(), store.tail_address(), |_addr, rec| {
      let key = rec.key();
      if let Some(rest) = key.strip_prefix(prefix_slice)
        && let Some((&key_tag, user_key)) = rest.split_first()
        && key_tag == KeyTag::Meta.as_u8()
      {
        let slot = latest.entry(user_key.to_vec()).or_insert(None);
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
      }
      Ok(true)
    })
    .await?;
  Ok(
    latest
      .into_iter()
      .filter_map(|(key, tag)| tag.map(|tag| (key, tag)))
      .take(DEMOTE_MAX_KEYS_PER_ROUND)
      .collect(),
  )
}

/// 单候选键降阶评估与落地（判定不通过/态漂移零副作用；`Err(())` 存储 IO 失败）
async fn demote_candidate<D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
) -> Result<bool, ()> {
  // 预读元记录（含 probe_alive 惰性过期裁决与存活核对），建立竞态基线
  let Some((meta_before, _)) = storage
    .batch
    .load_collection_stub(key)
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
  // 物化单源通道：树全扫还原信封载荷（四族支持，已到期成员剔除不复活）
  let Some(blob) = tiered_materialize_blob(&storage.batch, key, tag).await? else {
    return Ok(false);
  };
  // 决策复用同一谓词单点；空对象（物化剔除到期成员后为零）本轮跳过，
  // 交由既有删空自愈臂处理，后台不造空信封
  if !demote_target(tag, &blob) {
    return Ok(false);
  }
  // 落盘前竞态栅栏：重读元记录校验 key_id/size 与预读基线一致，漂移即放弃本轮
  // （对照 wkv load_collection_stub 口径；杜绝「后台已清树、前台仍向树写」）
  let Some((meta_now, _)) = storage
    .batch
    .load_collection_stub(key)
    .await
    .map_err(|_| ())?
  else {
    return Ok(false);
  };
  if meta_now.key_id != meta_before.key_id || meta_now.size != meta_before.size {
    return Ok(false);
  }
  // 前台懒降阶臂同一写回组合（rmw_helpers::apply_rmw_post_operate else 臂）：
  // obj_save 信封整值写回（wkv 用户键写入口恰一次推进 WATCH 栅栏并自带入账），
  // 树清退仅回收残留物理页与注销登记，不重复推进（一命令一推进）；
  // keep_ttl=true 与前台 else 臂同口径：键换域存活，键级 TTL 不随树清退脱落
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
