//! 升阶/降阶收尾状态机：RMW 对象操作后收尾（分层感知统一状态机）、STORE 族
//! 冷路径目标键收尾与冷漏斗窗内升阶臂

use wcol::{object_payload::GarnetObjectPayload, types::garnet_object::IGarnetObject};
use wdev::Device;
use wkv::Error;
use wval::{GarnetObjectType, KeyTag};

use super::{
  super::{
    object_store_utils::{envelope_overflow, obj_save_recheck_async},
    tiered_collection_ops::earliest_expiry,
  },
  drain::{bftree_drain, retire_tiered_dest},
  obj_current_domain_async,
  pageswap::obj_save_pageswap_replay,
};
use crate::storage::session::storage_session::StorageSession;

/// STORE 族冷路径目标键收尾公共臂（zset / set / geo 三臂一处定义，禁第二套
/// 窗序；同步快路径对位为 [`SyncStoreWindow`] + `del_ttl_sync` 的窗内清退形）
///
/// 目标键 rmw 窗句柄由调用方装载前预取承接（票 wnode-geo-store-selfref-load-
/// outside-window / wnode-set-store-selfref-load-outside-window，装载型写臂
/// §87 先窗后装纪律；同键同单窗禁双取，本臂不再自取），跨单套窗序：
/// 1. 存活域快照 + 落笔复验：窗内对面 DEL/SET 交叠即 `Err(())` 按存储忙拒写
///    （fail-closed，绝不盲写复活已删键或造双域并存）；
/// 2. 非空结果写回归档判链（票 wnode-store-dest-cold-upgrade-bypass，与同步
///    漏斗 [`obj_save_or_gc`] 同款 wcol 单源谓词、同判序——先条目数/体积维
///    [`should_promote`](wcol::types::IGarnetObject::should_promote) 后超页维
///    [`envelope_overflow`]，禁新造第二套门限）：命中即禁直写信封，窗内就地走
///    与 [`apply_rmw_post_operate`] 升阶分支同型的升阶漏斗
///    （[`dest_cold_promote_arm`]：封窗 claim + bftree+Meta 先建后拆换入 +
///    SET 语义随写清 TTL）；升阶未成且信封装得回落信封写回（同
///    apply_rmw_post_operate 回落臂形），升阶未成且超页 / 封窗竞态拒写一律
///    `Err(())` 转既有拒写/重试通道——超页信封直写被 whlog RecordTooLarge 拒
///    吐错误帧（同命令 C# 成功回基数，可用性分叉），且经异步 upsert_tag 臂
///    落位的超页信封永无升阶修复点，严禁在位。未命中判链 →
///    信封写回（页翻转瞬态原地重试闭环 [`obj_save_pageswap_replay`]）并随写
///    清退键级 TTL（[`StorageSession::clear_ttl`]，SET 语义对标 C# STORE 族
///    「Delete dst → ZADD」收尾；若开窗存活域为 String
///    则先 `delete_string` 清退旧 String 域记录，杜绝双域并存）；空结果删空回收
///    （`delete_string` 级联清 TTL，对标 C# EXPIRE(destination, 0)）——各臂的
///    TTL 动作都落在复验通过后的持窗临界区内，源装载/错误透传臂天然零清退；
/// 3. 窗释放后 [`retire_tiered_dest`] 按结果分流清退树态残留（其按用户键取闩，
///    窗内调用必自锁）；升阶换入成功臂跳过——新树即终态，清退反噬新树，
///    旧树已由 replace 换入先建后拆承接。
pub(crate) async fn store_dest_cold_common<O, D>(
  storage: &StorageSession<'_, D>,
  dst: &[u8],
  result: &O,
  window: wkv::RmwWindow<'_, '_, D>,
) -> Result<(), ()>
where
  O: GarnetObjectPayload + IGarnetObject,
  D: Device,
{
  let loaded = obj_current_domain_async(storage, dst)
    .await
    .map_err(|_| ())?;
  if !obj_save_recheck_async(storage, dst, loaded)
    .await
    .unwrap_or(false)
  {
    return Err(());
  }
  let is_empty = GarnetObjectPayload::is_empty(result);
  let mut promoted = false;
  if is_empty {
    storage.delete_string(dst).await.map_err(|_| ())?;
  } else {
    if loaded == Some(KeyTag::String) {
      storage.delete_string(dst).await.map_err(|_| ())?;
    }
    // 判链序与同步漏斗 obj_save_or_gc 同构：条目数/体积维先行（零序列化开销），
    // 超页维才需序列化载荷；升阶成功臂严禁提前全量序列化（同
    // apply_rmw_post_operate 口径）
    let by_threshold = result.should_promote();
    let mut blob = if by_threshold {
      Vec::new()
    } else {
      result.to_blob()
    };
    if by_threshold || envelope_overflow(&storage.batch, dst, &blob) {
      promoted = dest_cold_promote_arm(storage, dst, O::OBJECT_TAG, result, loaded).await?;
      if !promoted {
        // 升阶未成（页缓存总闸 / 契约闸 / 换入 / 落盘失败，旧态分毫未动）回落：
        // 信封装得下续写信封（同 apply_rmw_post_operate 回落臂形）；超页
        // fail-closed 拒写转既有重试通道，严禁直写超页信封
        if blob.is_empty() {
          blob = result.to_blob();
        }
        if envelope_overflow(&storage.batch, dst, &blob) {
          log::error!(
            "store_dest_cold_common 升阶未成且信封超页，fail-closed 拒写: key='{}'",
            String::from_utf8_lossy(dst)
          );
          return Err(());
        }
      }
    }
    // 非升阶成功臂统一信封写回并随写清退键级 TTL（落点与收敛前两臂逐 path 等价；
    // SET 语义对标 C# STORE 族「Delete dst → ZADD」收尾）：信封段经页翻转瞬态
    // 原地重试闭环（C# pending 重试对位，票 wnode-collect-fallback-blind-write-
    // after-recheck），重试窗复验不过仍存储忙，TTL 只在信封写闭环后清退
    if !promoted {
      obj_save_pageswap_replay(storage, dst, loaded, O::OBJECT_TAG, &blob).await?;
      storage.clear_ttl(dst).await.map_err(|_| ())?;
    }
  }
  // 释窗后清退：分层残留清退按用户键取闩，窗内调用同桶自锁；`keep_ttl` 按
  // 结果分流——非空结果信封已接管（键换域存活，只清 Meta+树、绝不触碰刚写回
  // 的信封），删空回收整键消亡（两域齐清）；升阶换入成功臂跳过（见头注 3）
  drop(window);
  if !promoted {
    retire_tiered_dest(storage, dst, !is_empty).await?;
  }
  Ok(())
}

/// STORE 族冷漏斗窗内升阶臂（与 [`apply_rmw_post_operate`] 升阶分支同型、
/// 经 [`promote_to_bftree`] 单点：先建后拆 replace 换入 bftree+Meta、水位同帧
/// 落盘、WATCH 恰一次推进，判据全耗 wcol 单源谓词，零第二套门限）
///
/// 自迁移封窗（[`StoreSession::try_swap_in_window`](wkv) claim）先于落笔登记：
/// 本 rmw 窗只挡同键对象层写臂，分层树内稳态写臂是另一锁面（wbftree 树与元
/// 记录）不在射程——不封窗则「已 ACK 落旧树随 replace 换入被整树顶替」的
/// 静默丢失形在位（同 [`tiered_materialize_blob_sealed`] 裁决）；claim 竞态
/// （并发 RENAME / 另一自迁移窗）即升阶在本窗不可执行，`Err(())` 转既有
/// 拒写/重试通道，绝不盲写。`Ok(true)` = 已换入且键级 TTL 随写窗内清退（SET
/// 语义，与信封臂随写清退（[`obj_save_pageswap_replay`] + clear_ttl）同判定
/// 点；升阶迁移臂本身不动 TTL 旁路，须显式清退）；`Ok(false)` = 升阶未执行，
/// 调用方回落。
/// 封窗守卫随本函数退出 RAII 释放——必先于调用方窗后 [`retire_tiered_dest`]
/// 臂（其装载探测门会拒自身 claim）
async fn dest_cold_promote_arm<O, D>(
  storage: &StorageSession<'_, D>,
  dst: &[u8],
  tag: GarnetObjectType,
  result: &O,
  loaded: Option<KeyTag>,
) -> Result<bool, ()>
where
  O: IGarnetObject,
  D: Device,
{
  let _swap_in_window = match storage.batch.try_swap_in_window(dst) {
    Some(guard) => guard,
    None => return Err(()),
  };
  // 既存分层目标键（开窗存活域 = Meta）整树顶替走 replace 换入（旧树先建后拆
  // 承接清退）；其余域首升阶 false（信封记录由 promote 内核随元记录落盘删除）
  if !matches!(
    promote_to_bftree(storage, dst, tag, result, loaded == Some(KeyTag::Meta)).await,
    PromoteOutcome::Done
  ) {
    return Ok(false);
  }
  storage.clear_ttl(dst).await.map_err(|_| ())?;
  Ok(true)
}

/// 升阶结果：`Done` 已换入；`BudgetExhausted` 页缓存总闸拒绝（建树闸在实例化
/// 之前拒绝，scratch 与引擎实例零创建，旧态完整无触碰）；`Failed` 其余失败
///（契约闸装载被拒 / 流 / 换入 / 落盘）。两非 `Done` 态同入调用方信封回落臂
///（信封装得下回落续写、超页 fail-closed），`Failed` 漏接回落即「未写树、未回
/// 信封、ACK 成功」零持久化假成功
enum PromoteOutcome {
  Done,
  BudgetExhausted,
  Failed,
}

/// 集合升阶 / 重灌为 BfTree 分页树（单点复用收口）
///
/// 页缓存总闸拒绝（[`PromoteOutcome::BudgetExhausted`]）单独呈报：建树闸在
/// 引擎实例化之前拒绝，此刻 scratch 工作文件零创建、注册表与旧态零触碰。
/// 契约闸整批拒与 IO 硬失败归入 [`PromoteOutcome::Failed`]，与总闸同入调用方
/// 信封回落臂（升阶臂回落信封写回 / 到期重灌臂推迟重灌或上抛）
#[inline]
async fn promote_to_bftree<D: Device, O: IGarnetObject>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  obj: &O,
  tiered: bool,
) -> PromoteOutcome {
  let entries = obj.export_entries();
  // 水位随灌入批同帧落盘（export_entries 写时过滤已到期成员，剩余挂 TTL
  // 刻度经 earliest_expiry 单点提取），杜绝重灌后假水位 MAX 骗过计数校正
  let next_expiry = earliest_expiry(&entries);
  // 升阶 / 重灌统一先建后拆：promote 内内核建树快照后原子换入（tiered=true
  // 即 replace 换树，旧树全程可读，发布失败只删残快照、旧状态原样保留），
  // 不再有 drain-destroy 蒸发窗口；键存活不动 TTL 旁路（对标 C#
  // ObjectStore/VarLenInputMethods.cs:42 GetRMWModifiedFieldInfo 记录重写
  // 前移 HasExpiration，零 TTL 事件）
  if let Err(e) = storage
    .batch
    .promote_collection_to_bftree(key, tag, entries, next_expiry, tiered)
    .await
  {
    if matches!(e, Error::CacheBudgetExhausted) {
      log::warn!(
        "apply_rmw_post_operate promote 页缓存预算耗尽: key='{}'",
        String::from_utf8_lossy(key)
      );
      return PromoteOutcome::BudgetExhausted;
    }
    log::error!("apply_rmw_post_operate promote err: {e:?}");
    return PromoteOutcome::Failed;
  }
  // 升阶 / 重灌迁移臂：promote_collection_to_bftree 仅 upsert_raw 元记录 +
  // delete_raw 信封（wkv range_index/stub.rs），同样不经用户键写入口 →
  // 显式恰一次推进。对标 C# ObjectStore/RMWMethods.cs:79 PostInitialUpdater
  // 与 :200 PostCopyUpdater（对象复制落盘即 IncrementVersion）：C# 无分层
  // 引擎、集合恒驻对象域，等价状态变更一律经该两钩子推进
  storage.bump_watch_version(key);
  PromoteOutcome::Done
}

/// RMW 对象操作后收尾（分层感知统一状态机）：
/// - 空对象 → 删空自愈（分层树 drain 随键清 TTL，keep_ttl=false / 信封域删键）；
/// - 超升阶阈值或物化重灌 → 先建后拆换入重灌（promote 内建快照原子替换
///   旧树，键全程可读；TTL 旁路不动）；
/// - 分层态改动后跌回迟滞死区之下 → 懒降阶（信封写回 + 树清退，同样
///   keep_ttl=true）；
/// - 其余 → 信封写回
///
/// 键级 TTL 分流判据（对标 C# 对象记录重写从不脱落 HasExpiration——
/// ObjectStore/VarLenInputMethods.cs:42 GetRMWModifiedFieldInfo 把过期字段
/// 从源记录前移到修改后记录，且零发 TTL 事件；删除臂则记录与过期同亡）：
/// 键在本次收尾后仍存活（升阶/降阶迁移臂）即保留 TTL 旁路记录，键消亡
///（删空自愈臂）才随键清除；杜绝一次迁移静默抹掉 EXPIRE 并把清除经
/// TtlWrite(expire_at=None) 镜像成 Persist 扩散到从库与 AOF 回放面
///
/// WATCH 版本栅栏分工（一命令一推进）：删空 drain 臂与 promote 重灌臂仅经
/// wkv 物理键原语（delete_raw / upsert_raw），故本层显式推进一次；
/// 信封写回臂与 delete_string 臂已由 wkv 用户键写入口收口，本层绝不重复推进；
/// 分层态原生树内写臂的推进在 tiered_collection_ops::finish_tiered_arm 单点
/// 完成（本函数不参与该臂，两条路径互斥无双计）
///
#[derive(Clone)]
pub(crate) struct RmwPostTarget<'a> {
  pub key: &'a [u8],
  pub tag: GarnetObjectType,
  pub tiered: bool,
  pub loaded: Option<KeyTag>,
}

/// 集合升阶 / 换树 / 信封写回单点收口（单点单源，杜绝多处手写写回）
pub(crate) async fn apply_rmw_post_operate<D, O>(
  storage: &StorageSession<'_, D>,
  target: RmwPostTarget<'_>,
  obj: &O,
  serialize: impl FnOnce(&O) -> Vec<u8>,
  is_empty: impl FnOnce(&O) -> bool,
) -> Result<(), ()>
where
  D: Device,
  O: IGarnetObject,
{
  let key = target.key;
  let tag = target.tag;
  let tiered = target.tiered;
  let loaded = target.loaded;
  if is_empty(obj) {
    if tiered {
      bftree_drain(storage, key, false).await?;
      // 删空迁移臂（键消亡，keep_ttl=false 随键清 TTL 杜绝孤儿）：drain 仅
      // delete_raw + del_ttl + 索引注销（wkv range_index/stub.rs），零用户键
      // 写入口 → 此处显式恰一次推进。
      // 对标 C# ObjectStore/RMWMethods.cs:125 InPlaceUpdaterWorker 的
      // output.HasRemoveKey 删空臂与 DeleteMethods.cs:21/:30 删除臂
      storage.bump_watch_version(key);
    } else {
      storage.delete_string(key).await.map_err(|_| ())?;
    }
  } else if obj.should_promote() || (tiered && !obj.should_demote()) {
    // 升阶或物化换树分支：直接使用 export_entries，无需且严禁提前全量序列化 payload。
    // 升阶未成（BudgetExhausted 页缓存总闸 / Failed 契约闸装载被拒·流·换入·落盘）
    // 且信封装得下时回落信封写回（升阶态保持键存活、写已持久化照常 ACK；分层态
    // 附加强制懒降阶清树，与下方懒降阶臂同一写回形）；信封超页装不下则 fail-closed
    // 命令失败——旧态（信封/树/元记录）分毫未动，杜绝半态迁移。Failed 必须与
    // BudgetExhausted 同入本回落臂：漏接即本分支既不写树也不写信封却放行 Ok，
    // 命令假成功静默丢写（升阶契约闸整批拒时每次触阈写都命中）
    if !matches!(
      promote_to_bftree(storage, key, tag, obj, tiered).await,
      PromoteOutcome::Done
    ) {
      let payload = serialize(obj);
      if envelope_overflow(&storage.batch, key, &payload) {
        log::error!(
          "apply_rmw_post_operate 升阶未成且信封超页，fail-closed 保持旧态: key='{}'",
          String::from_utf8_lossy(key)
        );
        return Err(());
      }
      // 页翻转降级（同步臂未落写入）走原地重试闭环：每轮「复验（同一判定核
      // 单点）→ 同步快写」零让核，瞬态等待不判败（C# pending 重试对位，
      // 票 wnode-collect-fallback-blind-write-after-recheck；复验不过仍
      // 存储忙，三向洞封堵不回退）
      obj_save_pageswap_replay(storage, key, loaded, tag, &payload).await?;
      if tiered {
        // 强制懒降阶臂：键换域存活（信封已写回），keep_ttl=true 树清退不碰 TTL 旁路；
        // WATCH 栅栏不在此推进（obj_save 已由 wkv 用户键写入口恰一次推进，同懒降阶臂）
        storage
          .batch
          .handle_bftree_drain_and_delete(key, true)
          .await
          .map_err(|_| ())?;
      }
    }
  } else {
    // 普通内存信封写回分支：此时才调用 serialize(obj) 并判定 envelope_overflow
    let payload = serialize(obj);
    if envelope_overflow(&storage.batch, key, &payload) {
      // 信封超页升阶臂：预算耗尽无回落余地（信封本就装不下）→ fail-closed，
      // 旧态完整无丢失；其余失败同上抛，禁漏过当成功（命令假成功即丢写）
      match promote_to_bftree(storage, key, tag, obj, tiered).await {
        PromoteOutcome::Done => {}
        PromoteOutcome::BudgetExhausted | PromoteOutcome::Failed => return Err(()),
      }
    } else {
      // 页翻转降级走原地重试闭环（同回落臂，票 wnode-collect-fallback-blind-
      // write-after-recheck）
      obj_save_pageswap_replay(storage, key, loaded, tag, &payload).await?;
      if tiered {
        // 懒降阶臂：键换域存活（信封已写回），keep_ttl=true 树清退不碰 TTL 旁路
        storage
          .batch
          .handle_bftree_drain_and_delete(key, true)
          .await
          .map_err(|_| ())?;
        // WATCH 栅栏不在此重复推进：上方 obj_save 已经 wkv 用户键写入口
        // （try_upsert_tag_sync_unprotected_with_prefix / upsert_tag）恰一次推进，
        // 树清退仅回收残留物理页，不再另计一次（一命令一推进）
      }
    }
  }
  Ok(())
}
