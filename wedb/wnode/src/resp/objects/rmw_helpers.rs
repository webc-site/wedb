//! 对象 RMW 执行域：同步/异步泛型骨架、分层感知收尾状态机、慢路径调度壳
//!
//! 对标 garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs（对象 RMW
//! 引擎钩子：NeedInitialUpdate / InitialUpdater / InPlaceUpdater / CopyUpdater 在
//! 记录锁内对 IGarnetObject 执行 op）与各 Resp 命令 Network* 方法内重复的
//! storageSession.RMW 调用形态——rust 侧以泛型骨架一次合流两形态，命令文件只交
//! 装载/序列化/operate 回调。自 object_store_utils.rs 纯移动迁出（该文件保留
//! 信封头解析辅助与装载保存面）。

use std::marker::PhantomData;

use wbase::time::now_ticks;
use wcol::{
  ObjectOutput,
  object_payload::{GarnetObjectPayload, ObjLoad},
  types::garnet_object::IGarnetObject,
};
use wdev::Device;
use wkv::BatchStoreSession;
use wresp::{
  cmd_strings::{RESP_ERR_WRONG_TYPE, write_error_raw},
  ext::RespVecExt,
};
use wval::{GarnetObjectType, KeyTag};

use super::{
  object_store_utils::{obj_load_typed_async, obj_load_typed_sync, obj_save_or_gc_raw},
  tiered_collection_ops::{
    TieredCollectionArgs, TieredCtx, earliest_expiry, exec_tiered_hash, exec_tiered_list,
    exec_tiered_set, exec_tiered_zset, tiered_materialize_blob,
  },
};
use crate::storage::session::storage_session::StorageSession;

/// 对象读改写命令的 RESP 回执（四类对象共用）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RespRmwDone {
  /// 执行数值结果（如新增/删除元素数）
  pub result1: i64,
  /// 协议响应负载是否已写出；若为 false 则 result1 由外层直接写出为整数响应
  pub payload_written: bool,
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
  output: &mut Vec<u8>,
  exec: Exec,
) -> Result<bool, ()>
where
  Exec: AsyncFnOnce(&mut TieredCtx<'_>, Op, &mut Vec<u8>) -> Result<bool, ()>,
{
  // 优先检查 BfTree 分页分层态
  let Some((mut meta, mut stub)) = storage
    .batch
    .load_collection_stub(key)
    .await
    .map_err(|_| ())?
  else {
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

/// STORE 族目标键清退收尾：目标键若原为分层态，信封写回 / 删空回收已接管
/// 数据面，清退残留树（SINTERSTORE / ZINTERSTORE 族统一漏斗）
pub(crate) async fn retire_tiered_dest<D: Device>(
  storage: &StorageSession<'_, D>,
  dst: &[u8],
) -> Result<(), ()> {
  if storage
    .batch
    .load_collection_stub(dst)
    .await
    .map_err(|_| ())?
    .is_some()
  {
    // STORE 族目标清退属删旧键接管语义（新数据面已由信封写回/删空回收接管），
    // keep_ttl=false 维持既有随旧态清 TTL 的口径不变
    storage
      .batch
      .handle_bftree_drain_and_delete(dst, false)
      .await
      .map_err(|_| ())?;
  }
  Ok(())
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
  if let Some((mut meta, mut stub)) = storage
    .batch
    .load_collection_stub(cmd.key)
    .await
    .map_err(|_| ())?
  {
    if meta.collection_type != cmd.tag {
      write_error_raw(output, RESP_ERR_WRONG_TYPE);
      return Ok(ObjLoad::WrongType);
    }
    // 分层态原生臂支持的操作就地执行；未支持操作穿透（Ok(false)）走物化
    // 降级通道，杜绝静默兜底输出与命令语义无关的应答
    // 会话协议版本透传至分层输出段，帧型与内存态 100% 一致（见
    // tiered_collection_ops 的 map/set/null/双精度写出）
    let resp_protocol_version = storage.resp_protocol_version();
    let handled = match cmd.tag {
      GarnetObjectType::Hash => {
        if let Ok(op) = (cmd.op.into()).try_into() {
          exec_tiered_hash(
            &storage.batch,
            cmd.key,
            &mut TieredCtx::new(&mut meta, &mut stub),
            TieredCollectionArgs::new(op, (cmd.arg1, cmd.arg2), cmd.args, resp_protocol_version),
            output,
          )
          .await
        } else {
          Ok(false)
        }
      }
      GarnetObjectType::Set => {
        if let Ok(op) = (cmd.op.into()).try_into() {
          exec_tiered_set(
            &storage.batch,
            cmd.key,
            &mut TieredCtx::new(&mut meta, &mut stub),
            op,
            cmd.args,
            output,
            resp_protocol_version,
          )
          .await
        } else {
          Ok(false)
        }
      }
      GarnetObjectType::SortedSet => {
        if let Ok(op) = (cmd.op.into()).try_into() {
          exec_tiered_zset(
            &storage.batch,
            cmd.key,
            &mut TieredCtx::new(&mut meta, &mut stub),
            TieredCollectionArgs::new(op, (cmd.arg1, cmd.arg2), cmd.args, resp_protocol_version),
            output,
          )
          .await
        } else {
          Ok(false)
        }
      }
      GarnetObjectType::List => {
        if let Ok(op) = (cmd.op.into()).try_into() {
          exec_tiered_list(
            &storage.batch,
            cmd.key,
            &mut TieredCtx::new(&mut meta, &mut stub),
            TieredCollectionArgs::new(op, (cmd.arg1, cmd.arg2), cmd.args, resp_protocol_version),
            output,
          )
          .await
        } else {
          Ok(false)
        }
      }
      _ => Ok(false),
    };
    match handled {
      Ok(true) => {
        return Ok(ObjLoad::Present(RespRmwDone {
          result1: 0,
          payload_written: true,
        }));
      }
      Err(()) => return Err(()),
      // 未支持操作（Ok(false)）：物化降级（穿透至下方对象层单源通道）
      Ok(false) => {}
    }

    // 物化降级：树全扫还原内存信封对象，经对象层 run_operate 求值，语义与
    // C# 对象层单源一致（TODO: 范围/随机型操作逐命令树内实现，消除物化
    // O(N) 读与重灌写）
    let blob = tiered_materialize_blob(&storage.batch, cmd.key, cmd.tag)
      .await?
      .ok_or(())?;
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

    return Ok(ObjLoad::Present(RespRmwDone {
      result1,
      payload_written: obj_out.written(),
    }));
  }

  let (mut obj, existed) =
    match obj_load_typed_async(storage, cmd.key, cmd.tag, output, handlers.deserialize)
      .await
      .map_err(|e| {
        log::error!("run_async_rmw obj_load_typed_async err: {e:?}");
      })? {
      // 异步读闭环后不存在降级态；防御性按存储错误应答（同 exec_slow
      // DEBUG 臂"防御内部错序"口径）
      ObjLoad::Degrade => {
        log::error!("run_async_rmw got ObjLoad::Degrade!");
        return Err(());
      }
      ObjLoad::WrongType => return Ok(ObjLoad::WrongType),
      ObjLoad::Missing => ((handlers.default_obj)(), false),
      ObjLoad::Present(o) => (o, true),
    };

  // operate 直写会话输出尾段；写回失败回退挂载点再落错（同上清场口径）
  let mut obj_out = (handlers.run_op)(&mut obj, cmd.op, cmd.args, output);
  let result1 = obj_out.result1;

  if (handlers.should_write)(cmd.op, &obj_out, &obj, existed)
    && apply_rmw_post_operate(
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

  Ok(ObjLoad::Present(RespRmwDone {
    result1,
    payload_written: obj_out.written(),
  }))
}

/// RMW 对象操作后收尾（分层感知统一状态机）：
/// - 空对象 → 删空自愈（分层树 drain 随键清 TTL，keep_ttl=false / 信封域删键）；
/// - 超升阶阈值或物化重灌 → 树重建灌入（键仍存活，drain 只墓碑元记录不碰 TTL
///   旁路，keep_ttl=true）；
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
      storage
        .batch
        .handle_bftree_drain_and_delete(key, false)
        .await
        .map_err(|_| ())?;
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
    if tiered {
      // 重灌前清退旧树：键全程存活，keep_ttl=true 只墓碑元记录、不碰 TTL
      // 旁路（对标 C# 记录重写前移 HasExpiration，零 TTL 事件）
      storage
        .batch
        .handle_bftree_drain_and_delete(key, true)
        .await
        .map_err(|_| ())?;
    }
    let entries = obj.export_entries();
    // 水位随灌入批同帧落盘（export_entries 写时过滤已到期成员，剩余挂 TTL
    // 刻度经 earliest_expiry 单点提取），杜绝重灌后假水位 MAX 骗过计数校正
    let next_expiry = earliest_expiry(&entries);
    if let Err(e) = storage
      .batch
      .promote_collection_to_bftree(key, tag, entries, next_expiry)
      .await
    {
      log::error!("apply_rmw_post_operate promote err: {e:?}");
      return Err(());
    }
    // 升阶 / 重灌迁移臂：promote_collection_to_bftree 仅 upsert_raw 元记录 +
    // delete_raw 信封（wkv range_index/stub.rs），同样不经用户键写入口 →
    // 显式恰一次推进。对标 C# ObjectStore/RMWMethods.cs:79 PostInitialUpdater
    // 与 :200 PostCopyUpdater（对象复制落盘即 IncrementVersion）：C# 无分层
    // 引擎、集合恒驻对象域，等价状态变更一律经该两钩子推进
    storage.bump_watch_version(key);
  } else {
    storage
      .obj_save(key, tag, &serialize(obj))
      .await
      .map_err(|e| {
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
  Ok(())
}

/// 慢路径写回收尾（分层感知，装载型命令统一漏斗）
///
/// 空对象 → 删空自愈（分层树 drain 随键清 TTL / 信封域删键）；
/// 超升阶阈值 → 重灌树；分层态改动后跌回迟滞死区之下 → 信封写回 + 树清退
///（懒降阶）；升阶/降阶迁移臂键存活不清 TTL（分流口径见
/// apply_rmw_post_operate 头注）；其余 → 信封写回（对标 C# WriteLogUpsert 单漏斗；
/// StorageSession::obj_save 自带入账）。`Err(())` 存储 IO 失败
pub(crate) async fn obj_writeback_tiered<D, O>(
  storage: &StorageSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  obj: &O,
) -> Result<(), ()>
where
  D: Device,
  O: IGarnetObject + GarnetObjectPayload,
{
  let tiered = storage
    .batch
    .load_collection_stub(key)
    .await
    .map_err(|_| ())?
    .is_some();
  apply_rmw_post_operate(
    storage,
    key,
    tag,
    obj,
    tiered,
    |o| o.to_blob(),
    IGarnetObject::is_empty,
  )
  .await
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
  match obj_load_typed_async(storage, key, tag, output, &deserialize)
    .await
    .map_err(|_| ())?
  {
    // 异步域 Degrade 唯一来源为分层 Meta 命中：物化回内存信封再求值
    //（C# 对象层语义恒定，无规模上限；树内扫描原语落地前以物化通道闭环）
    ObjLoad::Degrade => match tiered_materialize_blob(&storage.batch, key, tag).await? {
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
    },
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
      phantom: PhantomData,
    }
  }
}

/// 通用同步对象 RMW 执行骨架：装载 → operate → 变更回写（带增量 WAL 广播）→ 负载输出
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
) -> ObjLoad<RespRmwDone>
where
  Deser: FnOnce(&[u8]) -> Option<Obj>,
  Def: FnOnce() -> Obj,
  IsEmpty: Fn(&Obj) -> bool,
  Ser: FnOnce(&Obj) -> Vec<u8>,
  RunOp: for<'o> FnOnce(&mut Obj, Op, &[&[u8]], &'o mut Vec<u8>) -> ObjectOutput<'o>,
  ShouldWrite: FnOnce(Op, &ObjectOutput<'_>, &Obj, bool) -> bool,
{
  let (mut obj, existed) =
    match obj_load_typed_sync(store, cmd.key, cmd.tag, output, handlers.deserialize) {
      ObjLoad::Degrade => return ObjLoad::Degrade,
      ObjLoad::WrongType => return ObjLoad::WrongType,
      ObjLoad::Missing => ((handlers.default_obj)(), false),
      ObjLoad::Present(o) => (o, true),
    };

  // operate 直写会话输出尾段；升阶/写回失败先回退挂载点再返回 Degrade
  //（慢路径整体重放，残留负载会与重放应答拼帧）
  let mut obj_out = (handlers.run_op)(&mut obj, cmd.op, cmd.args, output);
  let result1 = obj_out.result1;

  if (handlers.should_write)(cmd.op, &obj_out, &obj, existed) {
    let empty = (handlers.is_empty)(&obj);
    if !empty && obj.should_promote() {
      obj_out.reset();
      return ObjLoad::Degrade;
    }
    // 写回走无入账内核（payload 先行编码一次，删空臂传空载荷）：增量条目
    // ObjectStoreRMW 由下方显式通知单独承接（对标 C# WriteLogRMW），与信封
    // 整值写通知（obj_save_notified 收口）互斥，杜绝双份入账
    let payload = if empty {
      Vec::new()
    } else {
      (handlers.serialize)(&obj)
    };
    match obj_save_or_gc_raw(store, cmd.key, cmd.tag, &payload, empty) {
      Ok(true) => {
        // 事件时间戳：真 .NET Ticks（与 wkv::ObjectRmwNotification 契约同域，
        // 对标 Garnet 对象 RMW 输入的时间戳）；key 为信封物理键
        //（KeyTag::ObjectEnvelope），与存储记录域一致
        let raw_key = store.session_tag_key(KeyTag::ObjectEnvelope, cmd.key);
        let notif = wkv::ObjectRmwNotification {
          key: &raw_key,
          obj_type: cmd.tag as u8,
          op_code: cmd.op.into(),
          timestamp_ticks: now_ticks(),
          arg1: cmd.arg1,
          arg2: cmd.arg2,
          args: cmd.args,
        };
        // AOF 入队失败降级为重放面可收敛的告警（对象已入内存域）
        if let Err(e) = store.notify_object_rmw(&notif) {
          log::error!("对象 RMW AOF 入队失败: {e}");
        }
      }
      Ok(false) | Err(_) => {
        obj_out.reset();
        return ObjLoad::Degrade;
      }
    }
  }

  ObjLoad::Present(RespRmwDone {
    result1,
    payload_written: obj_out.written(),
  })
}
