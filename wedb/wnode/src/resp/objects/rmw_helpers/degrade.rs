//! 降级重载与写回收尾：Degrade 态只读物化装载核、装载型命令慢路径公共体、
//! 分层感知写回收尾与异步档复验收口

use wcol::{
  object_payload::{GarnetObjectPayload, ObjLoad},
  types::garnet_object::IGarnetObject,
};
use wdev::Device;
use wval::{GarnetObjectType, KeyTag};

use super::{
  super::{
    object_store_utils::obj_load_typed,
    tiered_collection_ops::{load_collection_stub_for_read, tiered_materialize_blob},
  },
  cold_promote::{RmwPostTarget, apply_rmw_post_operate},
  load_stub, obj_writeback_recheck_async,
};
use crate::storage::session::storage_session::StorageSession;

/// 慢路径写回收尾（分层感知，装载型命令统一漏斗）
///
/// 空对象 → 删空自愈（分层树 drain 随键清 TTL / 信封域删键）；
/// 超升阶阈值 → 重灌树；分层态改动后跌回迟滞死区之下 → 信封写回 + 树清退
///（懒降阶）；升阶/降阶迁移臂键存活不清 TTL（分流口径见
/// apply_rmw_post_operate 头注）；其余 → 信封写回（对标 C# WriteLogUpsert 单漏斗；
/// StorageSession::obj_save 自带入账）。`Err(())` 存储 IO 失败
///
/// 第六写回路径封堵（未封窗臂 fail-closed）：`sealed=false` 调用方持「Meta
/// 缺席时刻」装载的信封快照（obj_load_custom 先验 Meta、缺席才落信封），
/// 写回前 re-probe 探得分层态 =「信封装载 → 写回」间隙有并发升阶落 meta——
/// 信封视图相对树恒陈旧（缺升阶命令自身字段与间隙内稳态树写），以陈旧内容
/// 承接分层写回（promote replace=true 整树顶替 / 懒降阶清树 / 删空排空）不可
/// 线性化：可复活已 ACK 删除、丢弃已 ACK 写入，封窗也救不回内容陈旧。故该
/// 分支按存储忙拒绝交客户端重试，分层写回唯一合法入口是持
/// [`SwapInWindowGuard`](wkv::SwapInWindowGuard) 守卫的封窗臂（sealed=true，
/// 共用同一封窗原语，禁各调用方散补第二套判定）
pub(crate) async fn obj_writeback_tiered<D, O>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  obj: &O,
  sealed: bool,
  loaded: Option<KeyTag>,
) -> Result<(), ()>
where
  D: Device,
  O: IGarnetObject + GarnetObjectPayload,
{
  // `sealed` 直传分层收尾 `tiered` 判：sealed = 调用方持自迁移封窗守卫（物化
  // 自树），键必为树态且门禁装载会被自身 claim 拒绝（MigrationBusy），故跳过
  // 装载直接按分层收尾；未封窗臂门禁装载探测：并发迁移窗内按存储忙失败
  //（禁穿透），探得分层态即 fail-closed（见头注第六写回路径封堵，禁以信封
  // 陈旧视图承接分层写回）
  if !sealed && load_stub(&storage.batch, key).await?.is_some() {
    return Err(());
  }
  apply_rmw_post_operate(
    storage,
    RmwPostTarget {
      key,
      tag,
      tiered: sealed,
      loaded,
    },
    obj,
    |o| o.to_blob(),
    IGarnetObject::is_empty,
  )
  .await
}

/// 装载型写臂删空回收/信封写回收尾·异步档单点收口（list/set/zset/geo 四臂
/// 同构收编；对标 C# libs/server/Storage/Session/ObjectStore/Common.cs
/// SaveObject 单点）：非封窗（信封域）落笔前先按装载态复验域归属
/// （[`obj_writeback_recheck_async`]，票 load-type-rmw-window 异步档），窗内
/// DEL/SET 交叠即 `Err(())` 按存储忙拒写；封窗臂（[`wkv::SwapInWindowGuard`]）
/// 物化语义域已钉死，免复验，`sealed` 直传 [`obj_writeback_tiered`] 分层判。
/// 类型标签单源 `O::OBJECT_TAG`（wcol 载荷契约关联常量），禁再传形参
#[inline]
pub(crate) async fn obj_writeback_rechecked_async<D, O>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  obj: &O,
  sealed: bool,
  existed: bool,
) -> Result<(), ()>
where
  D: Device,
  O: IGarnetObject + GarnetObjectPayload,
{
  if !sealed {
    obj_writeback_recheck_async(storage, key, existed).await?;
  }
  // 封窗臂树态域钉死 Meta；未封窗臂信封既存态 = existed（与复验同参同源）
  obj_writeback_tiered(
    storage,
    key,
    O::OBJECT_TAG,
    obj,
    sealed,
    sealed
      .then_some(KeyTag::Meta)
      .or_else(|| existed.then_some(KeyTag::ObjectEnvelope)),
  )
  .await
}

/// Degrade 态只读物化装载核（[`slow_load_eval`] 专用）：读臂装载单点
/// [`load_collection_stub_for_read`]（MigrationBusy 回退装载快照照常物化，读面
/// 忙拒面不扩大——try_tiered_arm 头注在册验收同一裁决）+ 只读物化；装载
/// None 或物化 `Ok(None)`（键在两探针 await 让渡窗内消亡 / 懒降阶摘 Meta /
/// FLUSHDB 换号——`tiered_materialize_blob` 头注契约「调用方维持既有装载
/// 路径」）重跑一次 [`obj_load_typed`]：键态至此已定，出 Missing / WrongType /
/// Present 三确态由调用方承接；再度 Degrade 原样透传交调用方维持 Err 折算
/// （一次为限防病态循环，且防把「零存储错误帧」偷换成「忙时报假缺失」，
/// 缺失帧仅限三确态）。物化载荷畸形仍 fail-fast Err（真实损坏，不回退空对象）
async fn degrade_reload<T, D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  output: &mut Vec<u8>,
  deserialize: &impl Fn(&[u8]) -> Option<T>,
) -> Result<ObjLoad<T>, ()> {
  if let Some((meta, mut stub)) = load_collection_stub_for_read(&storage.batch, key).await?
    && let Some(blob) =
      tiered_materialize_blob(&storage.batch, key, tag, &meta, &mut stub, None).await?
  {
    // 物化载荷解码 fail-fast：畸形即落错中止，不回退空对象
    let Some(obj) = deserialize(&blob) else {
      log::error!(
        "slow_load_eval: corrupted materialized payload, key='{}' tag={:?}",
        String::from_utf8_lossy(key),
        tag
      );
      return Err(());
    };
    return Ok(ObjLoad::Present(obj));
  }
  obj_load_typed(storage, key, tag, output, deserialize)
    .await
    .map_err(|_| ())
}

/// 装载型命令慢路径公共体：异步装载 → Missing 短路应答 / Present 求值
///
/// 对位同步段 `load_sync + run_operate` 形态命令（HGETALL/LRANGE/ZRANGE
/// 等）的 `HashLoad::Missing` 短路分支：`on_missing` 写同步入口同款应答，
/// `eval` 承载 operate 求值与应答整形。`Err(())` 为存储 IO 失败
pub(crate) async fn slow_load_eval<T, D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  output: &mut Vec<u8>,
  deserialize: impl Fn(&[u8]) -> Option<T>,
  on_missing: impl FnOnce(&mut Vec<u8>),
  eval: impl AsyncFnOnce(&mut T, &mut Vec<u8>),
) -> Result<(), ()> {
  let loaded = match obj_load_typed(storage, key, tag, output, &deserialize)
    .await
    .map_err(|_| ())?
  {
    // 异步域 Degrade 唯一来源为分层 Meta 命中：物化回内存信封再求值
    //（C# 对象层语义恒定，无规模上限；树内扫描原语落地前以物化通道闭环）。
    // 只读物化不封窗（eval 零写回，无换入丢失面），装载窗内键态迁移经
    // [`degrade_reload`] 承接；写回面（换入/清退）必须走
    // tiered_materialize_blob_sealed 封窗变体
    ObjLoad::Degrade => degrade_reload(storage, key, tag, output, &deserialize).await?,
    loaded => loaded,
  };
  match loaded {
    ObjLoad::Present(mut obj) => {
      eval(&mut obj, output).await;
    }
    // 类型不符：WRONGTYPE 错误帧已由装载核写出（含重装载臂），维持既有应答
    ObjLoad::WrongType => {}
    ObjLoad::Missing => on_missing(output),
    // 重装载后再度 Degrade（键态仍未收敛，如窗内消亡后又并发升阶）：维持
    // Err 折算交慢路径 err_frame 漏斗，绝不折缺失帧（键实际分层存活，
    // 报缺失即假 NOTFOUND）
    ObjLoad::Degrade => return Err(()),
  }
  Ok(())
}
