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

use std::marker::PhantomData;

use smallvec::SmallVec;
use wbase::time::now_ticks;
use wbftree::RangeIndexStub;
use wcol::{
  HashObject, ObjectOutput, SortedSetObject,
  object_payload::{GarnetObjectPayload, ObjLoad},
  types::garnet_object::IGarnetObject,
};
use wdev::Device;
use wkv::{BatchStoreSession, Error, StoreSession, SwapInWindowGuard};
use wresp::{
  cmd_strings::{RESP_ERR_GENERIC, RESP_ERR_WRONG_TYPE, write_error_raw},
  ext::RespVecExt,
};
use wval::{GarnetObjectType, KeyTag, MetaValue};

use super::{
  object_store_utils::{
    envelope_overflow, obj_load_typed, obj_load_typed_sync, obj_save_or_gc_raw,
    obj_save_recheck_async, obj_save_recheck_sync,
  },
  tiered_collection_ops::{
    TieredCollectionArgs, TieredCtx, earliest_expiry, exec_tiered_by_op_code,
    load_collection_stub_for_read, tiered_materialize_blob, tiered_materialize_blob_sealed,
  },
};
use crate::storage::session::{
  common::ttl_sync::{del_ttl_sync, probe_alive_domain_with_prefix},
  storage_session::StorageSession,
};

/// 对象读改写命令的 RESP 回执（四类对象共用）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

/// bftree drain 清退的 `Error::Swapped` 折算单点：Swapped 残留先显式推进
/// WATCH 恰一次再报忙，其余错误直接报忙（fail-closed 交既有拒写/重试通道）。
/// 仅供需要 Swapped 折算的两臂使用；obj_save 已入账的懒降阶臂维持裸
/// `map_err` 不重复推进（一命令一推进，见 [`apply_rmw_post_operate`] 头注）
#[inline]
async fn bftree_drain<D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  keep_ttl: bool,
) -> Result<(), ()> {
  if let Err(e) = storage
    .batch
    .handle_bftree_drain_and_delete(key, keep_ttl)
    .await
  {
    if matches!(e, Error::Swapped(_)) {
      storage.bump_watch_version(key);
    }
    return Err(());
  }
  Ok(())
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
  // （HGETALL/HKEYS/HVALS）仍显式忙拒；残余「装载于窗前、窗内换树」失配由
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

/// STORE 族目标键清退收尾：目标键若原为分层态，按接管形态分流清退残留树
///（SINTERSTORE / ZINTERSTORE 族统一漏斗）
///
/// 分流口径（票 zcode-r15-zset 发现一，对齐 [`apply_rmw_post_operate`] 两分支
/// 既有口径，一处定义）：非空结果信封写回已接管数据面、键换域存活 →
/// `keep_ttl=true` 只墓碑元记录 + 注销树，绝不触碰刚写回的信封（删键臂
/// `keep_ttl=false` 会先对信封域写幂等墓碑，把接管态数据一并抹掉）；删空
/// 回收整键消亡 → `keep_ttl=false` 两域齐清 + 随键 TTL/ETag 旁路级联清退，
/// 杜绝孤儿
pub(crate) async fn retire_tiered_dest<D: Device>(
  storage: &StorageSession<'_, D>,
  dst: &[u8],
  keep_ttl: bool,
) -> Result<(), ()> {
  if load_stub(&storage.batch, dst).await?.is_some() {
    bftree_drain(storage, dst, keep_ttl).await
  } else {
    Ok(())
  }
}

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
///    信封写回并随写清退键级 TTL（[`StorageSession::obj_save_clear_ttl`]，
///    SET 语义对标 C# STORE 族「Delete dst → ZADD」收尾；若开窗存活域为 String
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
    // 非升阶成功臂统一信封写回并随写清退键级 TTL（落点与收敛前两臂逐 path 等价）
    if !promoted {
      storage
        .obj_save_clear_ttl(dst, O::OBJECT_TAG, &blob)
        .await
        .map_err(|_| ())?;
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
/// 语义，与信封臂 [`StorageSession::obj_save_clear_ttl`] 同判定点；升阶迁移臂
/// 本身不动 TTL 旁路，须显式清退）；`Ok(false)` = 升阶未执行，调用方回落。
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

/// 通用异步对象 RMW 执行骨架：异步装载 → operate → 变更异步回写 → 负载输出
///
/// [`run_sync_rmw`] 的慢路径对位（exec_slow 冷键闭环）：磁盘候选经
/// StorageSession 异步读闭环后仅余 Missing/Present/WrongType 三态；升阶键经
/// 分层原生臂就地执行，未支持操作物化降级走对象层单源；写回调用的写端口
/// 为异步段对象写回唯一漏斗（StorageSession::obj_save，信封整值入账对标
/// C# WriteLogUpsert；空对象整键回收），不重复发同步段的增量条目。
/// `Err(())` 为存储 IO 失败，调用方统一应答 RESP_ERR_SLOW_PATH_STORAGE
pub(crate) async fn run_async_rmw<
  Obj: IGarnetObject,
  Op: Copy + Into<u8>,
  D: Device,
  Deser,
  Def,
  IsEmpty,
  Ser,
  RunOp,
  ShouldWrite,
>(
  storage: &StorageSession<'_, D>,
  cmd: SyncRmwCmd<'_, Op>,
  output: &mut Vec<u8>,
  handlers: SyncRmwHandlers<Obj, Op, Deser, Def, IsEmpty, Ser, RunOp, ShouldWrite>,
) -> Result<ObjLoad<RespRmwDone>, ()>
where
  Deser: Fn(&[u8]) -> Option<Obj>,
  Def: FnOnce() -> Obj,
  IsEmpty: Fn(&Obj) -> bool,
  Ser: FnOnce(&Obj) -> Vec<u8>,
  RunOp: for<'o> FnOnce(&mut Obj, Op, &[&[u8]], &'o mut Vec<u8>) -> ObjectOutput<'o>,
  ShouldWrite: FnOnce(Op, &ObjectOutput<'_>, &Obj, bool) -> bool,
{
  // 1. 优先检查是否处于 BfTree 分页分层态
  if let Some((mut meta, mut stub)) = load_stub(&storage.batch, cmd.key).await? {
    if meta.collection_type != cmd.tag {
      write_error_raw(output, RESP_ERR_WRONG_TYPE);
      return Ok(ObjLoad::WrongType);
    }
    // 分层态原生臂支持的操作就地执行（四族「操作码转换 → exec_tiered_* 装配」
    // 分派共享单点 exec_tiered_by_op_code，与重放端 tiered_replay_arm 同核，
    // 禁第二形态 match 漂移）；未支持操作 / 操作码越覆盖面（Ok(None)）穿透
    //（Ok(false)）走物化降级通道，杜绝静默兜底输出与命令语义无关的应答。
    // 会话协议版本透传至分层输出段，帧型与内存态 100% 一致（见
    // tiered_collection_ops 的 map/set/null/双精度写出）
    let resp_protocol_version = storage.resp_version;
    let handled = exec_tiered_by_op_code(
      &storage.batch,
      cmd.key,
      cmd.tag,
      &mut TieredCtx::new(&mut meta, &mut stub),
      TieredCollectionArgs::new(
        cmd.op.into(),
        (cmd.arg1, cmd.arg2),
        cmd.args,
        resp_protocol_version,
      ),
      output,
    )
    .await?
    .unwrap_or(false);
    // 未支持操作（None / Some(false)）：物化降级（穿透至下方对象层单源通道）
    if handled {
      return Ok(ObjLoad::Present(RespRmwDone {
        result1: 0,
        payload_written: true,
      }));
    }

    // 物化降级（自迁移封窗）：封窗单点 [`tiered_materialize_blob_sealed`] 登记
    // 安全换入窗（同键并发稳态写臂自此被四探测门 MigrationBusy 拒）后全扫物化，
    // 守卫持跨「对象层 run_operate 求值 → apply_rmw_post_operate 换入/清退」
    // 全程——语义对标 C# 对象层单源（对象求值与写回在记录锁内完成），杜绝窗内
    // 并发树内稳态写已 ACK 落旧树、随 replace=true 换入被整树顶替的静默丢失形
    // 与镜像 AOF 乱序（窗内无写提交，镜像序仍 = 树内提交序）。
    // Ok(None) = 树内臂到期出账后整键删空自愈（如 HSET 折叠臂全到期批的
    // 出账前置），键已消亡：出窗（守卫随 None 释放）落下方通用对象层通道按
    // Missing 新建承接（对标 C# DeleteExpiredItems 清空后 Add 的净字典计数），
    // 不再按存储错误降级。版本栅栏口径：出账删空的推进已由 finish_tiered_arm
    // 按置脏恰一次完成，下方新建写回臂（obj_save / promote）对应「重建」这一
    // 第二次真实变更各自推进，无同一变更的双计
    'materialize: {
      let Some((blob, _swap_in_window)) =
        tiered_materialize_blob_sealed(&storage.batch, cmd.key, cmd.tag).await?
      else {
        break 'materialize;
      };
      // 物化载荷解码 fail-fast：畸形即落错中止本命令，严禁回退空对象后写回销毁原键
      let Some(mut obj) = (handlers.deserialize)(&blob) else {
        log::error!(
          "run_async_rmw: corrupted materialized payload, key='{}' tag={:#04x}",
          String::from_utf8_lossy(cmd.key),
          cmd.tag as u8
        );
        return Err(());
      };
      let existed = true;
      // operate 直写会话输出尾段；写回失败回退挂载点再落错（慢路径统一应答
      // 前清场，杜绝残留负载与错误帧拼帧）
      let mut obj_out = (handlers.run_op)(&mut obj, cmd.op, cmd.args, output);
      let result1 = obj_out.result1;

      if (handlers.should_write)(cmd.op, &obj_out, &obj, existed)
        && apply_rmw_post_operate(
          storage,
          cmd.key,
          cmd.tag,
          &obj,
          true,
          handlers.serialize,
          handlers.is_empty,
        )
        .await
        .is_err()
      {
        obj_out.reset();
        return Err(());
      }

      // 封窗守卫随 return 释放：换入/清退已完成，窗内写臂自此重新放行
      return Ok(ObjLoad::Present(RespRmwDone {
        result1,
        payload_written: obj_out.written(),
      }));
    }
  }

  // 同键读改写原子窗口（异步域让核等待臂，对标 C# 锁冲突转 pending 重试）：
  // 先于装载取，覆盖「异步装载 → operate → 整值写回」全程，与 run_sync_rmw
  // 的同步臂同锁源同判据；分层树内臂是另一锁面（wbftree 树与 Meta 记录），
  // 不在本窗口射程（见票边界），故窗口只挂对象层单源通道。对面 DEL/SET 不取本窗
  // （物理记录键与用户键两把不同基，取之即双锁序死锁面），交叠裁决由写回前
  // [`obj_save_recheck_async`] 终态复验承接
  let _window = storage.batch.rmw_window(cmd.key).await.map_err(|e| {
    log::error!("run_async_rmw rmw_window failed: {e:?}");
  })?;

  let (mut obj, existed) =
    match obj_load_typed(storage, cmd.key, cmd.tag, output, handlers.deserialize)
      .await
      .map_err(|e| {
        log::error!("run_async_rmw obj_load_typed err: {e:?}");
      })? {
      // 异步读闭环后不存在降级态；防御性按存储错误应答（同 exec_slow
      // DEBUG 臂"防御内部错序"口径）
      ObjLoad::Degrade => {
        log::error!("run_async_rmw got ObjLoad::Degrade!");
        return Err(());
      }
      ObjLoad::WrongType => return Ok(ObjLoad::WrongType),
      ObjLoad::Missing => {
        // Missing 短路钩子（与 run_sync_rmw 同钩同帧，快慢双臂同果）：直出
        // 常量应答即返，跳过空对象求值与写回判定
        if let Some(on_missing) = handlers.on_missing {
          on_missing(output);
          return Ok(ObjLoad::Missing);
        }
        ((handlers.default_obj)(), false)
      }
      ObjLoad::Present(o) => (o, true),
    };

  // operate 直写会话输出尾段；写回失败回退挂载点再落错（同上清场口径）
  let mut obj_out = (handlers.run_op)(&mut obj, cmd.op, cmd.args, output);
  let result1 = obj_out.result1;

  if (handlers.should_write)(cmd.op, &obj_out, &obj, existed) {
    // 落笔前终态复验（键复活 / 双域并存封堵，见 [`obj_save_recheck_async`] 头注）：
    // 本窗口只挡 RMW 方，对面 DEL 与 SET 取物理记录键桶闩，与本窗口的用户键桶是
    // 两个不同基，窗口期内可自由墓碑信封 / 清退信封并写字符串；本臂「异步装载 →
    // operate → 写回」之间还跨读内核让核点，交叠面比同步臂更宽，装载时的旧视图
    // 绝不允许未经复验即落笔。复验不通过（含探针磁盘候选与存储错误）一律弃写：
    // 本臂已是终态重放面，无更深降级通道，与写回失败同按存储忙信号交回客户端
    // 重试（[`obj_writeback_tiered`] 未封窗臂 fail-closed 同口径，不新增错误形态）
    let loaded = existed.then_some(KeyTag::ObjectEnvelope);
    let unchanged = obj_save_recheck_async(storage, cmd.key, loaded)
      .await
      .map_err(|e| {
        log::error!("run_async_rmw obj_save_recheck_async err: {e:?}");
      })?;
    if !unchanged {
      obj_out.reset();
      return Err(());
    }
    if apply_rmw_post_operate(
      storage,
      cmd.key,
      cmd.tag,
      &obj,
      false,
      handlers.serialize,
      handlers.is_empty,
    )
    .await
    .is_err()
    {
      obj_out.reset();
      return Err(());
    }
  }

  Ok(ObjLoad::Present(RespRmwDone {
    result1,
    payload_written: obj_out.written(),
  }))
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
pub(crate) async fn apply_rmw_post_operate<D, O>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  obj: &O,
  tiered: bool,
  serialize: impl FnOnce(&O) -> Vec<u8>,
  is_empty: impl FnOnce(&O) -> bool,
) -> Result<(), ()>
where
  D: Device,
  O: IGarnetObject,
{
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
      storage.obj_save(key, tag, &payload).await.map_err(|e| {
        log::error!("apply_rmw_post_operate 回落信封写回 obj_save err: {e:?}");
      })?;
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
      storage.obj_save(key, tag, &payload).await.map_err(|e| {
        log::error!("apply_rmw_post_operate obj_save err: {e:?}");
      })?;
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
    key,
    tag,
    obj,
    sealed,
    |o| o.to_blob(),
    IGarnetObject::is_empty,
  )
  .await
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
    // 异步域闭环后无降级态；Missing/类型不符（不可达：计数探针已判型）
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
    obj_writeback_tiered(storage, key, tag, &obj, false).await?;
  }
  Ok(Some(len))
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
  match obj_load_typed(storage, key, tag, output, &deserialize)
    .await
    .map_err(|_| ())?
  {
    // 异步域 Degrade 唯一来源为分层 Meta 命中：物化回内存信封再求值
    //（C# 对象层语义恒定，无规模上限；树内扫描原语落地前以物化通道闭环）。
    // 只读物化不封窗（eval 零写回，无换入丢失面），走门禁装载 + 纯物化核；
    // 写回面（换入/清退）必须走 tiered_materialize_blob_sealed 封窗变体
    ObjLoad::Degrade => {
      let Some((meta, mut stub)) = load_stub(&storage.batch, key).await? else {
        return Err(());
      };
      match tiered_materialize_blob(&storage.batch, key, tag, &meta, &mut stub).await? {
        Some(blob) => {
          // 物化载荷解码 fail-fast：畸形即落错中止，不回退空对象
          let Some(mut obj) = deserialize(&blob) else {
            log::error!(
              "slow_load_eval: corrupted materialized payload, key='{}' tag={:?}",
              String::from_utf8_lossy(key),
              tag
            );
            return Err(());
          };
          eval(&mut obj, output).await;
          Ok(())
        }
        None => Err(()),
      }
    }
    ObjLoad::WrongType => Ok(()),
    ObjLoad::Missing => {
      on_missing(output);
      Ok(())
    }
    ObjLoad::Present(mut obj) => {
      eval(&mut obj, output).await;
      Ok(())
    }
  }
}

/// 写回面封窗装载核出参三态（四族慢路径装载站点共享判定码）
pub(crate) enum SealedLoad<O> {
  /// WRONGTYPE 错误行已写出
  WrongType,
  /// 键缺失（未写任何输出，调用方定短路应答）
  Missing,
  /// 已装载（守卫 = 分层物化封窗，须持至写回收尾；信封域装载为 `None`）
  Present(O, Option<SwapInWindowGuard>),
}

/// 写回面单键封窗装载核（物化降级臂装配一处定义、调用点转引）：门禁分派
/// [`obj_load_typed`]，Degrade 臂转引 [`tiered_materialize_blob_sealed`]
/// 登记自迁移安全换入窗后全扫物化 + 解码 fail-fast + 统一错误日志；其余臂
/// 原样回传，WrongType/Missing/Present 臂的差异映射留在调用方
///
/// 读侧只读物化对应物为 [`slow_load_eval`]（不封窗，eval 零写回）；本核供
/// 「装载 → 求值 → 写回」全程持窗的写回面装载站点（四族 slow.rs）。`Err(())`
/// 为存储 IO 失败或物化载荷畸形（fail-fast 落错，调用方不写回）
/// 四族 operate 通道执行单源（原 hash/set/list/zset 各一份的同形 run_operate
/// 收口）：op 经 `Into<u8>` 窄化（wcol 四枚举 `From<Op> for u8` 均为 `op as u8`），
/// 经 [`IGarnetObject::operate`] u16 转发臂收窄回 u8 固有通道，协议版本透传
#[inline]
pub(crate) fn run_operate<'o, O: IGarnetObject, Op: Copy + Into<u8>>(
  obj: &mut O,
  op: Op,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  resp_version: u8,
  output: &'o mut Vec<u8>,
) -> ObjectOutput<'o> {
  let mut obj_out = ObjectOutput::mount(output);
  obj.operate(
    u16::from(op.into()),
    args,
    arg1,
    arg2,
    &mut obj_out,
    resp_version,
  );
  obj_out
}

/// 单键封窗装载三态映射单源（原 zset/list 两份慢臂同形 load_typed 收口）：
/// `Ok(None)` = WRONGTYPE 错误行已写出、`Ok(Some(None))` = MISSING（调用方定
/// 短路应答）、`Ok(Some(Some((对象, 封窗守卫))))` = Present，守卫随对象交
/// 调用方持跨求值与写回收尾（信封域装载守卫为 None）
pub(crate) async fn load_sealed_tri<O: GarnetObjectPayload, D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  output: &mut Vec<u8>,
) -> Result<Option<Option<(O, Option<SwapInWindowGuard>)>>, ()> {
  Ok(
    match load_typed_sealed(storage, key, O::OBJECT_TAG, output).await? {
      SealedLoad::WrongType => None,
      SealedLoad::Missing => Some(None),
      SealedLoad::Present(o, window) => Some(Some((o, window))),
    },
  )
}

pub(crate) async fn load_typed_sealed<O: GarnetObjectPayload, D: Device>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  output: &mut Vec<u8>,
) -> Result<SealedLoad<O>, ()> {
  match obj_load_typed(storage, key, tag, output, O::from_blob)
    .await
    .map_err(|_| ())?
  {
    // 异步域 Degrade 唯一来源为分层 Meta 命中：物化回内存对象（封窗变体：
    // 自迁移安全换入窗自物化扫描起登记，杜绝窗内并发树内稳态写随 replace=true
    // 换入被整树顶替的已 ACK 丢失形），守卫交调用方持跨求值与写回收尾
    ObjLoad::Degrade => {
      let Some((blob, window)) = tiered_materialize_blob_sealed(&storage.batch, key, tag).await?
      else {
        return Err(());
      };
      // 物化载荷解码 fail-fast：畸形落错中止，不回退空对象销毁原键
      match O::from_blob(&blob) {
        Some(obj) => Ok(SealedLoad::Present(obj, Some(window))),
        None => {
          log::error!(
            "load_typed_sealed: corrupted materialized payload, key='{}' tag={:?}",
            String::from_utf8_lossy(key),
            tag
          );
          Err(())
        }
      }
    }
    ObjLoad::WrongType => Ok(SealedLoad::WrongType),
    ObjLoad::Missing => Ok(SealedLoad::Missing),
    ObjLoad::Present(o) => Ok(SealedLoad::Present(o, None)),
  }
}

/// 同步对象 RMW 命令输入参数
pub struct SyncRmwCmd<'a, Op> {
  pub key: &'a [u8],
  pub tag: GarnetObjectType,
  pub op: Op,
  pub args: &'a [&'a [u8]],
  pub arg1: i32,
  pub arg2: i32,
}

/// 同步对象 RMW 处理策略集合
pub struct SyncRmwHandlers<Obj, Op, Deser, Def, IsEmpty, Ser, RunOp, ShouldWrite> {
  pub deserialize: Deser,
  pub default_obj: Def,
  pub is_empty: IsEmpty,
  pub serialize: Ser,
  pub run_op: RunOp,
  pub should_write: ShouldWrite,
  /// Missing 短路钩子（对位 [`slow_load_eval`] 的 `on_missing` 形制）：`Some`
  /// 且装载为 Missing 时直出常量帧、跳过 `default_obj`+`run_op` 求值与写回
  /// 判定——C# NOTFOUND 恒常量帧形（如 HashCommands.cs:148/:513 空数组），
  /// 杜绝空对象求值在 RESP3 出 `%0` 与 C# `*0` 分叉；缺省 `None` 维持
  /// HTTL/HGET/HMGET 等既有臂的空对象求值矩阵零改动
  pub on_missing: Option<fn(&mut Vec<u8>)>,
  pub phantom: PhantomData<fn() -> (Obj, Op)>,
}

impl<Obj, Op, Deser, Def, IsEmpty, Ser, RunOp, ShouldWrite>
  SyncRmwHandlers<Obj, Op, Deser, Def, IsEmpty, Ser, RunOp, ShouldWrite>
where
  Deser: FnOnce(&[u8]) -> Option<Obj>,
  Def: FnOnce() -> Obj,
  IsEmpty: Fn(&Obj) -> bool,
  Ser: FnOnce(&Obj) -> Vec<u8>,
  RunOp: for<'o> FnOnce(&mut Obj, Op, &[&[u8]], &'o mut Vec<u8>) -> ObjectOutput<'o>,
  ShouldWrite: FnOnce(Op, &ObjectOutput<'_>, &Obj, bool) -> bool,
{
  #[inline]
  pub fn new(
    deserialize: Deser,
    default_obj: Def,
    is_empty: IsEmpty,
    serialize: Ser,
    run_op: RunOp,
    should_write: ShouldWrite,
  ) -> Self {
    Self {
      deserialize,
      default_obj,
      is_empty,
      serialize,
      run_op,
      should_write,
      on_missing: None,
      phantom: PhantomData,
    }
  }

  /// 挂接 Missing 短路常量帧钩子（缺省不挂 = 空对象求值矩阵，见 `on_missing`
  /// 字段注）
  #[inline]
  pub fn with_on_missing(mut self, on_missing: Option<fn(&mut Vec<u8>)>) -> Self {
    self.on_missing = on_missing;
    self
  }
}

/// 通用同步对象 RMW 执行骨架：装载 → operate → 变更回写（带增量 WAL 广播）→ 负载输出
///
/// C# 对象存 RMW 四钩子在记录锁内执行 op 的等价骨架（`try_rmw_window` 桶闩即
/// C# 记录 XLock；load→default_obj/缺建、run_op→对活对象改值、写回→CAS 落链），
/// 逐枚举挂准（异步对偶 [`run_async_rmw`] 同一判定序）：
/// libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:NeedInitialUpdate
/// （键缺席是否建对象：装载 `ObjLoad::Missing` 臂 → `default_obj` + should_write 判写）；
/// libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:InPlaceUpdater
/// （对既有活对象就地改值：`run_op(&mut obj, ..)` 臂）；
/// libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:CopyUpdater
/// （改后整值经 [`apply_rmw_post_operate`]/obj_save 序列化重投新信封；
/// C# 源记录过期清退臂由装载口 TTL 惰性清除前置闭环）
pub fn run_sync_rmw<
  Obj: IGarnetObject,
  Op: Copy + Into<u8>,
  D: Device,
  Deser,
  Def,
  IsEmpty,
  Ser,
  RunOp,
  ShouldWrite,
>(
  store: &BatchStoreSession<'_, D>,
  cmd: SyncRmwCmd<'_, Op>,
  output: &mut Vec<u8>,
  handlers: SyncRmwHandlers<Obj, Op, Deser, Def, IsEmpty, Ser, RunOp, ShouldWrite>,
) -> SyncRmwOutcome
where
  Deser: FnOnce(&[u8]) -> Option<Obj>,
  Def: FnOnce() -> Obj,
  IsEmpty: Fn(&Obj) -> bool,
  Ser: FnOnce(&Obj) -> Vec<u8>,
  RunOp: for<'o> FnOnce(&mut Obj, Op, &[&[u8]], &'o mut Vec<u8>) -> ObjectOutput<'o>,
  ShouldWrite: FnOnce(Op, &ObjectOutput<'_>, &Obj, bool) -> bool,
{
  // 同键读改写原子窗口：跨「装载信封对象 → operate → 整值序列化写回」全程持
  // 本键桶排他闩（对标 C# ObjectStore/RMWMethods.cs 的 NeedInitialUpdate /
  // InPlaceUpdater / CopyUpdater 全在记录锁内对 IGarnetObject 执行 op），杜绝
  // 并发 HSET/SADD/ZADD 同键不同字段时后写者整值抹掉前写者字段；自旋预算内
  // 不得闩即降级，由 run_async_rmw 的让核等待臂承接
  let Some(_window) = store.try_rmw_window(cmd.key) else {
    return SyncRmwOutcome::Degrade;
  };

  let (mut obj, existed) =
    match obj_load_typed_sync(store, cmd.key, cmd.tag, output, handlers.deserialize) {
      ObjLoad::Degrade => return SyncRmwOutcome::Degrade,
      ObjLoad::WrongType => return SyncRmwOutcome::WrongType,
      ObjLoad::Missing => {
        // Missing 短路钩子（C# NOTFOUND 恒常量帧形）：直出应答即返，跳过
        // 空对象求值与写回判定；缺省无钩子走 default_obj + run_op 求值矩阵
        if let Some(on_missing) = handlers.on_missing {
          on_missing(output);
          return SyncRmwOutcome::Missing;
        }
        ((handlers.default_obj)(), false)
      }
      ObjLoad::Present(o) => (o, true),
    };

  // operate 直写会话输出尾段；升阶/写回失败先回退挂载点再返回 Degrade
  //（慢路径整体重放，残留负载会与重放应答拼帧）
  let mut obj_out = (handlers.run_op)(&mut obj, cmd.op, cmd.args, output);
  let result1 = obj_out.result1;

  if (handlers.should_write)(cmd.op, &obj_out, &obj, existed) {
    // 落笔前终态复验（键复活 / 双域并存封堵，判据与票号背景见
    // [`obj_save_recheck_sync`] 头注）：本窗口只挡 RMW 方，对面 DEL/SET 走物理记录键
    // 桶闩（与本窗口的用户键桶两个不同基），窗口期内可自由墓碑信封、清退信封并写
    // 字符串，装载时的旧视图绝不允许未经复验即落笔。不复通过（含探针磁盘候选与
    // 存储错误，本臂无法裁决）一律弃写，与写回失败同款借既有降级信号整体转异步
    // 重放（重放按当前态重新装载求值，应答与新状态自洽，绝不复活已 ACK 删除的旧值）
    if !obj_writeback_recheck_sync(store, cmd.key, existed) {
      obj_out.reset();
      return SyncRmwOutcome::Degrade;
    }
    let empty = (handlers.is_empty)(&obj);
    if !empty && obj.should_promote() {
      obj_out.reset();
      return SyncRmwOutcome::Degrade;
    }
    // 写回走无入账内核（payload 先行编码一次，删空臂传空载荷）：增量条目
    // ObjectStoreRMW 由下方显式通知单独承接（对标 C# WriteLogRMW），与信封
    // 整值写通知（obj_save_notified 收口）互斥，杜绝双份入账
    let payload = if empty {
      Vec::new()
    } else {
      (handlers.serialize)(&obj)
    };
    // 信封超页前置判（升阶容量门，见 envelope_overflow）：与上方 should_promote
    // 门同款 Degrade——交异步漏斗 apply_rmw_post_operate 走既有升阶臂，杜绝
    // 同步臂先撞 RecordTooLarge 的无效往返
    if !empty && envelope_overflow(store, cmd.key, &payload) {
      obj_out.reset();
      return SyncRmwOutcome::Degrade;
    }
    match obj_save_or_gc_raw(store, cmd.key, cmd.tag, &payload, empty) {
      Ok(true) => {
        // 事件时间戳：真 .NET Ticks（与 wkv::ObjectRmwNotification 契约同域，
        // 对标 Garnet 对象 RMW 输入的时间戳）；key 为信封物理键
        //（KeyTag::ObjectEnvelope），与存储记录域一致
        let raw_key = store.session_tag_key(KeyTag::ObjectEnvelope, cmd.key);
        let notif = wkv::ObjectRmwNotification {
          key: &raw_key,
          obj_type: cmd.tag,
          op_code: cmd.op.into(),
          timestamp_ticks: now_ticks(),
          arg1: cmd.arg1,
          arg2: cmd.arg2,
          args: cmd.args,
        };
        // AOF 入队失败按 error.rs AofEnqueue 契约以终态 AofFail 上抛拒绝本
        // 命令（票 wnode-objrmw-aof-enqueue-swallow-matrix，同族先例
        // r167c-aoffail）：信封写已生效不回滚（撤帧不撤内存，发散显式可见），
        // 严禁吞错冒答 Present 假成功，亦严禁借 Degrade 转异步重放——慢臂
        // 整体重放会对 HINCRBY/ZINCRBY/LPUSH 等非幂等算子二次施加
        if let Err(e) = store.notify_object_rmw(&notif) {
          log::error!("对象 RMW AOF 入队失败，命令按 AofEnqueue 契约拒绝: {e}");
          obj_out.reset();
          return SyncRmwOutcome::AofFail;
        }
      }
      Ok(false) | Err(_) => {
        obj_out.reset();
        return SyncRmwOutcome::Degrade;
      }
    }
  }

  SyncRmwOutcome::Present(RespRmwDone {
    result1,
    payload_written: obj_out.written(),
  })
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
/// （fail-closed，绝不盲写）
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
/// 与冷臂 [`StorageSession::obj_save_clear_ttl`] 同序同机制：信封写回 Ok(true)
/// 落定后、同一持窗临界区内清既有 key 级 TTL（SET 语义对标 C# STORE 族
/// 「值随 Delete 整写、expiration 归零」终态，
/// garnet/libs/server/Storage/Session/ObjectStore/SetOps.cs:422、
/// SortedSetGeoOps.cs:181——清退脱钩先于写回的旧序在写故障窗破「失败即原态」，
/// 禁复犯）。前置条件同 [`del_ttl_sync`] 契约：调用方持目标键 rmw 窗。
///
/// 三态收尾：`true` = 清退闭环（含 dst 本无 TTL：探针未命中零写入零推进）；
/// `false` = 尾笔残留（环形页翻转降级 / 存储错误）——值已写、TTL 未清，系与
/// 冷臂 `obj_save_clear_ttl` 尾笔 clear_ttl 失败上抛同型的罕见残留形（C# 单记录
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
