//! 对象 RMW 执行域：同步/异步泛型骨架、分层感知收尾状态机、慢路径调度壳
//!
//! 对标 garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs（对象 RMW
//! 引擎钩子：NeedInitialUpdate / InitialUpdater / InPlaceUpdater / CopyUpdater 在
//! 记录锁内对 IGarnetObject 执行 op）与各 Resp 命令 Network* 方法内重复的
//! storageSession.RMW 调用形态——rust 侧以泛型骨架一次合流两形态，命令文件只交
//! 装载/序列化/operate 回调。自 object_store_utils.rs 纯移动迁出（该文件保留
//! 信封头解析辅助与装载保存面）。
//!
//! 写回面保护统一登记：本域一切「装载 → 求值 → 写回」变更面（[`run_sync_rmw`] /
//! [`run_async_rmw`] RMW 骨架，及信封计数矫正写回面 [`envelope_length_correct_by`]）
//! 共用同一套窗口与复验判定——装载前持 [`wkv::RmwWindow`] 用户键桶排他闩挡并发
//! 同键 RMW 写臂交错顶替，落笔前经 [`obj_save_recheck_sync`] / [`obj_save_recheck_async`]
//! （object_store_utils 单点裁决核）复验域归属挡对面 DEL / SET 交错，复验不过一律
//! 弃写交既有降级 / 存储忙信号。禁任何写回面另立第二套裁决。
//!
//! 目录化拆分：[`cold_promote`] 升阶/降阶收尾状态机、[`drain`] 树清退、
//! [`pageswap`] 页翻转回放与信封计数矫正、[`degrade`] Degrade 物化重载与写回
//! 收尾、[`handlers`] 同步/异步 RMW 执行骨架与算子装配。

mod cold_promote;
mod degrade;
mod drain;
mod handlers;
mod pageswap;

pub(crate) use cold_promote::store_dest_cold_common;
pub(crate) use degrade::{obj_writeback_rechecked_async, slow_load_eval};
pub use handlers::{
  CollectionOp, RmwOp, SyncRmwCmd, SyncRmwHandlers, collection_rmw_handlers, run_sync_rmw,
};
pub(crate) use handlers::{
  SealedLoad, collection_sync_rmw, load_sealed_tri, load_typed_sealed, run_async_rmw, run_operate,
};
pub(crate) use pageswap::envelope_length_correct;
use smallvec::SmallVec;
use wbftree::RangeIndexStub;
use wcol::object_payload::ObjLoad;
use wdev::Device;
use wkv::{BatchStoreSession, StoreSession};
use wresp::{
  cmd_strings::{RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, write_error_raw},
  ext::RespVecExt,
};
use wval::{GarnetObjectType, KeyTag, MetaValue};

use super::{
  object_store_utils::{obj_save_recheck_async, obj_save_recheck_sync},
  tiered_collection_ops::{TieredCtx, load_collection_stub_for_read},
};
use crate::storage::session::{
  common::ttl_sync::{del_ttl_sync, probe_alive_domain_with_prefix},
  storage_session::StorageSession,
};

/// 对象读改写命令的 RESP 回执（四类对象共用）
#[derive(Debug, Copy, PartialEq, Eq, Clone)]
pub struct RespRmwDone {
  /// 执行数值结果（如新增/删除元素数）
  pub result1: i64,
  /// 协议响应负载是否已写出；若为 false 则 result1 由外层直接写出为整数响应
  pub payload_written: bool,
}

/// [`run_sync_rmw`] 同步骨架终态五态：[`ObjLoad`] 四态 + AofFail 终态错误信号
///
/// AofFail 对位 `wkv::error::AofEnqueue` 契约（error.rs「主存写入已生效，AOF
/// 缺条目，调用方须以错误拒绝该命令防主从发散」，票
/// wnode-objrmw-aof-enqueue-swallow-matrix）：信封写回已生效后增量条目入队
/// 失败，**严禁**借 Degrade 信号转异步整体重放（HINCRBY/ZINCRBY/LPUSH 等
/// 非幂等算子会二次施加），调用臂须撤帧落错误应答；内存写不回滚（发散可见
/// 可感知，客户端得错误而非假成功，口径同 r167c-aoffail）
#[derive(Debug, Copy, PartialEq, Eq, Clone)]
pub enum SyncRmwOutcome {
  /// 磁盘候选/写回失败：命令须降级异步重放（未写任何输出）
  Degrade,
  /// 键存在但信封类型不符（WRONGTYPE）
  WrongType,
  /// 键缺失（未找到/无候选/放弃写入）
  Missing,
  /// 写已生效但 AOF 增量条目入队失败（终态拒绝，禁重放）
  AofFail,
  /// 命中/执行完成并产出载荷或结果
  Present(RespRmwDone),
}

impl From<ObjLoad<RespRmwDone>> for SyncRmwOutcome {
  /// 异步对偶骨架（[`run_async_rmw`]）与装载口的四态原样映射；AofFail 仅由
  /// 同步骨架写后入账失败产生（异步臂经 obj_save `?` 上抛 wkv::Error 承接）
  fn from(load: ObjLoad<RespRmwDone>) -> Self {
    match load {
      ObjLoad::Degrade => Self::Degrade,
      ObjLoad::WrongType => Self::WrongType,
      ObjLoad::Missing => Self::Missing,
      ObjLoad::Present(done) => Self::Present(done),
    }
  }
}

/// 异步对象 RMW 执行骨架的统一应答收尾：负载未写出时补整数（result1）
///
/// 与各命令同步入口 `Rmw::Present` 臂 `if !payload_written` 的整数补写
/// 同口径；`+OK` 形态（HMSET）等特殊应答由调用方在返回后覆盖
#[inline]
pub(crate) fn write_rmw_reply(done: RespRmwDone, output: &mut Vec<u8>) {
  if !done.payload_written {
    output.write_resp_int(done.result1);
  }
}

/// 对象族命令层对 [`SyncRmwOutcome::AofFail`] 的统一错误帧收尾（AofEnqueue
/// 契约：写已生效、镜像缺条目即拒绝命令，禁假成功帧；帧形与本域存储硬失败
/// 臂 `write_resp_error(RESP_ERR_GENERIC)` 同族，票
/// wnode-objrmw-aof-enqueue-swallow-matrix）
#[inline]
pub(crate) fn write_rmw_aof_fail_frame(output: &mut Vec<u8>) {
  output.write_resp_error(RESP_ERR_GENERIC);
}

/// 慢路径 refs 首键/余参切分单源（hash / set / zset / list 四族 slow 分派
/// 共用）：返回 (首键, 余参)，空 refs 缺键兜底空串、无参兜底空切片
#[inline]
pub(crate) fn split_refs<'a>(refs: &'a [&'a [u8]]) -> (&'a [u8], &'a [&'a [u8]]) {
  (
    refs.first().copied().unwrap_or(&[]),
    refs.get(1..).unwrap_or(&[]),
  )
}

/// AofFail 尾臂收口单点（[`write_rmw_aof_fail_frame`] + 「终态拒绝已闭环」
/// `Ok(true)` 返回值的成对样板；对象族命令 match 尾臂由此单源，调用形如
/// `Rmw::AofFail => rmw_aof_fail_ok(output),`）。`E` 泛型承各域 Result 别名
///（wresp::Result / wkv::Result）
#[inline]
pub(crate) fn rmw_aof_fail_ok<E>(output: &mut Vec<u8>) -> Result<bool, E> {
  write_rmw_aof_fail_frame(output);
  Ok(true)
}

/// 分层态门禁装载探测的 IO 折算单点（[`StoreSession::load_collection_stub`]
/// 的 `wkv::Error` → 本域 `Err(())` 存储忙信号统一收口；读臂需要
/// MigrationBusy 回退形态的走 [`load_collection_stub_for_read`]，禁混用）
#[inline]
async fn load_stub<D: Device>(
  session: &StoreSession<D>,
  key: &[u8],
) -> Result<Option<(MetaValue, RangeIndexStub)>, ()> {
  session.load_collection_stub(key).await.map_err(|_| ())
}

/// 分层四族慢路径调度壳单点收口：探测 → WRONGTYPE 门 → 树内原生臂 → 穿透
///
/// 骨架顺序与各族壳体现行严格一致：`load_collection_stub` 探测分层态（非分层
/// 键返回 `Ok(false)` 落冷路径）→ `collection_type` 不符写 WRONGTYPE 即闭环
/// （返回 `Ok(true)`，调用方直接返回）→ `op_opt` 为本族树内未覆盖命令
/// （翻译表留在壳体）同样 `Ok(false)` 穿透：落下方对象层通道
///（run_async_rmw 物化降级闭环），杜绝静默兜底输出与命令语义无关的应答。
/// `exec` 闭包承载族内 [`TieredCtx`] 臂调用与族特有收尾（List/ZSet 写命令
/// notify、List arg 通道），其 `Ok(true)` 即已闭环应答。`Err(())` 存储 IO 失败
pub(crate) async fn try_tiered_arm<Op, D: Device, Exec>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  op_opt: Option<Op>,
  needs_write: impl Fn(&Op) -> bool,
  output: &mut Vec<u8>,
  exec: Exec,
) -> Result<bool, ()>
where
  Exec: AsyncFnOnce(&mut TieredCtx<'_>, Op, &mut Vec<u8>) -> Result<bool, ()>,
{
  // 优先检查 BfTree 分页分层态。迁移 claim 在册（`Err(MigrationBusy)`）×
  // 共享读锁臂（`needs_write(op) == false`）回退未门禁装载照常执行：换入前
  // 旧树内容自洽，[`tiered_guard`](tiered_collection_ops::common) 读臂
  // MigrationBusy 回退的同一裁决延展到路由门——读面忙拒面不扩大（并发升阶 /
  // 物化的自迁移开窗期间 LRANGE/SCARD 等轮询读不得折存储错误帧，票
  // wnode-tieredread-stalemeta 验收「零存储错误帧」），写臂与写锁臂
  //（HGETALL/HKEYS/HVALS）仍显式忙拒；残余「装载于窗前、窗内换树」失配由
  // 读臂锁内刷新回退 + LRANGE 预留-回填帧头兜底（同票两案并用裁决）。
  // 读臂路由装载裁决收口于单点 [`load_collection_stub_for_read`]（SCAN 族
  // exec_tiered_scan 同一函数，禁第二套）；写臂/写锁臂维持门禁装载显式忙拒
  let loaded = if op_opt.as_ref().is_some_and(|op| !needs_write(op)) {
    load_collection_stub_for_read(&storage.batch, key).await?
  } else {
    load_stub(&storage.batch, key).await?
  };
  let Some((mut meta, mut stub)) = loaded else {
    return Ok(false);
  };
  if meta.collection_type != tag {
    write_error_raw(output, RESP_ERR_WRONG_TYPE);
    return Ok(true);
  }
  let Some(op) = op_opt else {
    // 未支持操作穿透：落下方对象层通道（run_async_rmw 物化降级闭环）
    return Ok(false);
  };
  exec(&mut TieredCtx::new(&mut meta, &mut stub), op, output).await
}

// ============ 装载型写命令族统一保护面（票 wnode-load-type-write-bypass-rmw-window-recheck） ============
//
// 骨架外手写臂（run_sync_rmw/run_async_rmw 不可表达的 Missing 短路应答形、
// 双键移动形、STORE 覆写形）复用与骨架完全同源的窗口与复验判定核，
// 禁任何臂另立第二套裁决（见模块头注登记）。

/// 装载型双键写臂同步双窗（LMOVE/SMOVE 形）：键组排他闩经全仓多键臂唯一
/// 桶升序单机制 [`wkv::BatchStoreSession::try_rmw_window_sorted`]（票
/// zcode-r135c-lockorder 案一：本臂曾自立字节字典序第二套取闩序，与 sorted
/// 臂（RENAME/MSET 族）在同一 `try_lock_key_bucket` 物理桶闩上双序对撞，
/// 热键对互逐重放风暴；C# 双键臂与多键臂同源单序——ListOps.cs:229-231
/// ListMove 双键经 `SaveKeyEntryToLock` 登记后由 TxnKeyEntryComparison.cs:23-24
/// 桶序整组排序，全仓无字典序手写臂）。双键退化为两槽计划，同键/同桶碰撞
/// 由计划折叠单点顺带去重；任一窗自旋预算内未取到即整体 RAII 放闩回 None
/// （较旧臂的部分持窗形态收严为全有全无），调用方沿用既有 `Ok(false)` 异步
/// 重放通道；事务锁模式下回空窗组（与 sorted 臂同款让闩语义）
pub(crate) fn try_sync_rmw_window_pair<'s, 'a, 'k, D: Device>(
  store: &'s BatchStoreSession<'a, D>,
  key1: &'k [u8],
  key2: &'k [u8],
) -> Option<SmallVec<[wkv::RmwWindow<'s, 'k, D>; 8]>> {
  store.try_rmw_window_sorted([key1, key2])
}

/// 装载型双键写臂异步双窗（让核等待臂，[`try_sync_rmw_window_pair`] 的异步
/// 对位）：同经 [`wkv::BatchStoreSession::rmw_window_sorted`] 桶升序单机制
/// 取闩；[`wkv::RmwWindow`] 预算耗尽错误原样上抛，调用方按存储忙应答
///（fail-closed，绝不盲写）
pub(crate) async fn rmw_window_pair_async<'s, 'a, 'k, D: Device>(
  storage: &'s StorageSession<'a, D>,
  key1: &'k [u8],
  key2: &'k [u8],
) -> wkv::Result<SmallVec<[wkv::RmwWindow<'s, 'k, D>; 8]>> {
  storage.batch.rmw_window_sorted([key1, key2]).await
}

/// 装载型写臂落笔前终态复验·同步档单点收口（判定核 [`obj_save_recheck_sync`]）：
/// `existed` = 装载时键既存（信封域）；新建写回传 false（域须仍为缺席才可落笔）。
/// 探针磁盘候选与存储错误一律判不复通过（`false`），调用方走既有降级/重试通道
#[inline]
pub(crate) fn obj_writeback_recheck_sync<D: Device>(
  store: &BatchStoreSession<'_, D>,
  key: &[u8],
  existed: bool,
) -> bool {
  obj_save_recheck_sync(store, key, existed.then_some(KeyTag::ObjectEnvelope)).unwrap_or(false)
}

/// 装载型写臂落笔前终态复验·异步档单点收口（判定核 [`obj_save_recheck_async`]）：
/// 复验不过（含探针磁盘候选与存储错误）一律 `Err(())` 按存储忙交回客户端重试，
/// 与 [`run_async_rmw`] 让核等待臂同款出口
#[inline]
pub(crate) async fn obj_writeback_recheck_async<D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  existed: bool,
) -> Result<(), ()> {
  if obj_save_recheck_async(storage, key, existed.then_some(KeyTag::ObjectEnvelope))
    .await
    .unwrap_or(false)
  {
    Ok(())
  } else {
    Err(())
  }
}

/// STORE/覆写族目标键同步双保护句柄（SINTERSTORE 族 / GEOSEARCHSTORE 形）：
/// 覆写语义无「装载快照」可比对，装载态取开窗时刻存活域探针（探针不可裁决即
/// 开窗失败，调用方走既有 `Ok(false)` 异步重放通道），落笔前按同域复验，
/// 窗口期内对面 DEL/SET 交叠即拒写重放，杜绝双域并存盲写
pub(crate) struct SyncStoreWindow<'s, 'a, 'k, D: Device> {
  _window: wkv::RmwWindow<'s, 'k, D>,
  store: &'s BatchStoreSession<'a, D>,
  key: &'k [u8],
  loaded: Option<KeyTag>,
}

impl<'s, 'a, 'k, D: Device> SyncStoreWindow<'s, 'a, 'k, D> {
  pub(crate) fn begin(store: &'s BatchStoreSession<'a, D>, key: &'k [u8]) -> Option<Self> {
    let _window = store.try_rmw_window(key)?;
    let prefix = store.session_prefix();
    let loaded = probe_alive_domain_with_prefix(store, prefix.as_slice(), key).ok()??;
    // 异构域目标键拒写单点收口（票 zcode-r15-zset 发现一 / zcode-r32-retirematrix）：
    // 若 loaded 不是 None 且不是 Some(KeyTag::ObjectEnvelope)（即旧键存在且为 String 或 Meta 等异构域），
    // 同步臂直写 ObjectEnvelope 会导致双域并存并破坏单域不变式。此处整臂转既有
    // `Ok(false)` 异步慢路径（STORE 族 `store_dest_cold_common` 收尾单点在写临界区清退旧域，
    // 窗后 `retire_tiered_dest` 清退残留树），同步臂禁做裸域清退，不引入第二套机制
    if !matches!(loaded, None | Some(KeyTag::ObjectEnvelope)) {
      return None;
    }
    Some(Self {
      _window,
      store,
      key,
      loaded,
    })
  }

  /// 落笔前复验：开窗时刻存活域 == 当前存活域才可写
  pub(crate) fn recheck(&self) -> bool {
    obj_save_recheck_sync(self.store, self.key, self.loaded).unwrap_or(false)
  }
}

/// STORE 族同步臂「写回先行、清退随后」尾笔清退单点（票 zcode-r122c-setstore1）
///
/// 与冷臂（store_dest_cold_common 信封写回臂的随写清退）同序同机制：信封写回
/// Ok(true)
/// 落定后、同一持窗临界区内清既有 key 级 TTL（SET 语义对标 C# STORE 族
/// 「值随 Delete 整写、expiration 归零」终态，
/// garnet/libs/server/Storage/Session/ObjectStore/SetOps.cs:422、
/// SortedSetGeoOps.cs:181——清退脱钩先于写回的旧序在写故障窗破「失败即原态」，
/// 禁复犯）。前置条件同 [`del_ttl_sync`] 契约：调用方持目标键 rmw 窗。
///
/// 三态收尾：`true` = 清退闭环（含 dst 本无 TTL：探针未命中零写入零推进）；
/// `false` = 尾笔残留（环形页翻转降级 / 存储错误）——值已写、TTL 未清，系与
/// 冷臂尾笔 clear_ttl 失败上抛同型的罕见残留形（C# 单记录
/// 整写形态下不可观测），本臂告警可见并回 `false` 由调用方按冷臂同款 fail-loud
/// 错误帧应答（禁静默成功、禁降级重放致值双写），已登 deviations 台账 §125（工单
/// zcode-r122c-setstore1）。禁引入第二把 TTL 闩或新原语。
///
/// 禁序判据豁免申报（store_ttl_clear_critical_section 判据 1 同步臂对位）：
/// 写回与清退同处同步段单线程持窗临界区、零 await 挂起点，清退尾笔紧随写回
/// 闭环，「TTL 已亡 ∧ 信封未写」禁序态在两笔之间无可观测窗口，执行期探针
/// 无从命中，豁免以本单点序纪律 + 调用方失败注入回归锁死。
pub(crate) fn store_writeback_clear_ttl<D: Device>(
  store: &BatchStoreSession<'_, D>,
  dst: &[u8],
) -> bool {
  match del_ttl_sync(store, dst) {
    Ok(true) => true,
    Ok(false) | Err(_) => {
      log::error!(
        "STORE 族同步臂尾笔 TTL 清退未落（值已写、SET 语义清退残留，票 zcode-r122c-setstore1 deviation 登记面）: key='{}'",
        String::from_utf8_lossy(dst)
      );
      false
    }
  }
}

/// [`SyncStoreWindow`] 的异步档装载态探针（与 [`obj_save_recheck_async`] 同探测序：
/// 同步三域探针可裁决即直返，磁盘候选交异步读内核闭环终态，无降级态）
pub(crate) async fn obj_current_domain_async<D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
) -> wkv::Result<Option<KeyTag>> {
  let prefix = storage.batch.session_prefix();
  if let Some(current) = probe_alive_domain_with_prefix(&storage.batch, prefix.as_slice(), key)? {
    return Ok(current);
  }
  storage
    .probe_alive_domain_with_prefix(prefix.as_slice(), key)
    .await
}
