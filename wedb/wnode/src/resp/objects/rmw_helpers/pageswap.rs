//! 页交换回放与信封计数矫正：obj_save 页翻转瞬态原地重试闭环、HLEN/ZCARD
//! 水位越线的信封态计数慢路径矫正

use wcol::{
  HashObject, SortedSetObject,
  object_payload::{GarnetObjectPayload, ObjLoad},
  types::garnet_object::IGarnetObject,
};
use wdev::Device;
use wval::{GarnetObjectType, KeyTag};

use super::{
  super::object_store_utils::{obj_load_typed, obj_save_recheck_async},
  degrade::obj_writeback_tiered,
};
use crate::storage::session::storage_session::StorageSession;

/// 页翻转瞬态原地重试预算（轮数）：obj_save 降级臂每轮内联官方驱逐推进
/// （至少一页），可变区页数有限，预算内重试必收敛；超限判存储忙防活锁
const PAGE_SWAP_REPLAY_BUDGET: usize = 64;

/// 信封写回的页翻转瞬态原地重试闭环（一处定义，C# InternalRMW pending 对位：
/// libs/storage/Tsavorite InternalRMW.cs 遇 hlog 尾部空间不足回 pending，
/// CompletePending 驱动 evict 后**同一条 RMW 循环重试**成功回基数——瞬态等待
/// 而非判败正是 C# 语义；驱逐推进由 [`StorageSession::obj_save`] 降级臂内联
/// 官方驱逐原语（upsert_raw 翻转臂同款循环步）承接，让核窗恒在落笔之前）。
///
/// 每轮「存活域复验（判定核 [`obj_save_recheck_async`] 单点）→ 同步快路径
/// 重写」之间零让核：同步落笔与复验终判同处批处理纪元同步段，对面 DEL/SET
///（物理记录键桶闩）在两步之间无可观测插入窗，洞面结构性不存在（有洞的只有
/// 旧「复验终判 → 异步 upsert_tag 落笔」闭环形，await 间隙无记录闩——票
/// wnode-collect-fallback-blind-write-after-recheck 三向洞封堵不回退）。
/// 复验不过与预算耗尽一律 `Err(())` 按存储忙拒绝，fail-closed 交客户端重试
pub(super) async fn obj_save_pageswap_replay<D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  loaded: Option<KeyTag>,
  tag: GarnetObjectType,
  payload: &[u8],
) -> Result<(), ()> {
  // 首发（调用方落笔前复验刚过，零让核继承，不重复复验）；判页翻转降级即入循环
  if storage.obj_save(key, tag, payload).await.map_err(|_| ())? {
    return Ok(());
  }
  for _ in 0..PAGE_SWAP_REPLAY_BUDGET {
    if !obj_save_recheck_async(storage, key, loaded)
      .await
      .unwrap_or(false)
    {
      return Err(());
    }
    // obj_save 降级臂内联官方驱逐推进（每轮至少推进一页），可变区有限页数内
    // 必然腾出可写空间
    if storage.obj_save(key, tag, payload).await.map_err(|_| ())? {
      return Ok(());
    }
  }
  log::error!(
    "obj_save 页翻转重试预算耗尽，按存储忙拒绝: key='{}'",
    String::from_utf8_lossy(key)
  );
  Err(())
}

/// 信封态计数慢路径矫正（HLEN 水位越线经 hash_length 降级由本臂承接，与 ZCARD 同步
/// 物化矫正臂 sorted_set_length_purged 同口径）：异步装载信封对象 →
/// purge_expired_len 堆序惰性剔除（collection.md §6.3 唯一剔除内核）→ 剔除
/// 实际发生（mutated_by_ttl，含装载即剔除的信封陈旧过期）升格写回一次矫正；
/// 全成员到期剔空落 [`apply_rmw_post_operate`] 空对象臂即删空自愈
///
/// 仅哈希/有序集合信封携带到期水位域（计数探针只对这两类降级到本臂）；
/// `Ok(Some(len))` 已矫正并返回存活计数，`Ok(None)` 键缺失/类型不符（错误
/// 帧已由装载探针写出），`Err(())` 存储 IO 失败
pub(crate) async fn envelope_length_correct<D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  output: &mut Vec<u8>,
) -> Result<Option<usize>, ()> {
  match tag {
    GarnetObjectType::Hash => {
      envelope_length_correct_by(storage, key, tag, output, HashObject::from_blob, |obj| {
        let len = obj.purge_expired_len();
        (len, obj.mutated_by_ttl())
      })
      .await
    }
    GarnetObjectType::SortedSet => {
      envelope_length_correct_by(
        storage,
        key,
        tag,
        output,
        SortedSetObject::from_blob,
        |obj| {
          let len = obj.purge_expired_len();
          (len, obj.mutated_by_ttl())
        },
      )
      .await
    }
    // 无水位域类型不降级到本臂
    _ => Ok(None),
  }
}

/// [`envelope_length_correct`] 泛型核：`purge` 返回 (矫正后计数, 是否实际剔除)
///
/// 写回面保护与 RMW 骨架同构（计数矫正写回面登记，见模块头注）：装载前取
/// [`wkv::RmwWindow`] 用户键桶排他闩（[`run_async_rmw`] 让核等待臂同款；HLEN 水位
/// 越线经 hash_length 降级由 envelope_length_correct 异步域承接，与 ZCARD 同步
/// 矫正臂 sorted_set_length_purged 经 run_sync_rmw 持的 try_rmw_window 即同一锁源），
/// 跨「装载 → purge → 写回」全程持窗，挡住并发
/// 同键 RMW 写臂（HSET/HDEL/HEXPIRE 族）的交错整值顶替；落笔前经
/// [`obj_save_recheck_async`] 终态复验封堵窗口期并发 DEL / SET（对面取物理
/// 记录键桶闩，不取本窗）造成的键复活 / 双域并存 / 已 ACK 写丢失，复验不过
/// 按存储忙拒绝交客户端重试，绝不以陈旧快照盲写——与 run_async_rmw 让核等待
/// 臂同款双保护，纯复用既有机制，不新增第二套裁决
async fn envelope_length_correct_by<O, D>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  output: &mut Vec<u8>,
  deserialize: impl Fn(&[u8]) -> Option<O>,
  purge: impl FnOnce(&mut O) -> (usize, bool),
) -> Result<Option<usize>, ()>
where
  O: IGarnetObject + GarnetObjectPayload,
  D: Device,
{
  // 同键读改写原子窗口（异步域让核等待臂）：先于装载取，覆盖全程
  let _window = storage.batch.rmw_window(key).await.map_err(|e| {
    log::error!("envelope_length_correct_by rmw_window failed: {e:?}");
  })?;

  let mut obj = match obj_load_typed(storage, key, tag, output, deserialize).await {
    Ok(ObjLoad::Present(obj)) => obj,
    // 异步域闭环后无降级态；Missing/类型不符（并发可达非防御：本域 await
    // 让核点与对面物理记录键桶闩的 DEL / DEL+SET 竞态，重装载即得二态）
    // 一律维持调用方既有应答
    Ok(ObjLoad::Missing | ObjLoad::WrongType | ObjLoad::Degrade) => return Ok(None),
    Err(_) => return Err(()),
  };
  let (len, mutated) = purge(&mut obj);
  if mutated || IGarnetObject::is_empty(&obj) {
    let unchanged = obj_save_recheck_async(storage, key, Some(KeyTag::ObjectEnvelope))
      .await
      .map_err(|e| {
        log::error!("envelope_length_correct_by obj_save_recheck_async err: {e:?}");
      })?;
    if !unchanged {
      return Err(());
    }
    obj_writeback_tiered(storage, key, tag, &obj, false, Some(KeyTag::ObjectEnvelope)).await?;
  }
  Ok(Some(len))
}
