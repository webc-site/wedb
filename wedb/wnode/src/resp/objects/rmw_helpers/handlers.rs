//! 同步/异步 RMW 执行骨架与算子装配：SyncRmwCmd 输入包、RmwOp 执行契约、
//! 四族公共装配器、run_sync_rmw / run_async_rmw 双骨架与封窗装载核

use std::marker::PhantomData;

use wcol::{
  ObjectOutput,
  object_payload::{GarnetObjectPayload, ObjLoad},
  types::garnet_object::IGarnetObject,
};
use wdev::Device;
use wkv::{BatchStoreSession, SwapInWindowGuard};
use wresp::cmd_strings::{RESP_ERR_WRONG_TYPE, write_error_raw};
use wval::{GarnetObjectType, KeyTag};

use super::{
  super::{
    object_store_utils::{
      envelope_overflow, obj_load_typed, obj_load_typed_sync, obj_save_or_gc_raw,
      obj_save_recheck_async,
    },
    tiered_collection_ops::{
      TieredCollectionArgs, TieredCtx, exec_tiered_by_op_code, tiered_materialize_blob_sealed,
    },
  },
  RespRmwDone, SyncRmwOutcome,
  cold_promote::{RmwPostTarget, apply_rmw_post_operate},
  load_stub, obj_writeback_recheck_sync,
};
use crate::storage::session::storage_session::StorageSession;

/// 同步对象 RMW 命令输入参数
pub struct SyncRmwCmd<'a, Op> {
  pub key: &'a [u8],
  pub tag: GarnetObjectType,
  pub op: Op,
  pub args: &'a [&'a [u8]],
  pub arg1: i32,
  pub arg2: i32,
}

/// 对象 RMW 算子执行契约：具体算子类型派发（CollectionOp / 测试自建模，如
/// rmw_writeback_revalidate 的 TestGateOp），消解嵌套类型复杂度。无闭包
/// blanket impl——现役构造点（collection_rmw_handlers 与测试）均传具体算子，
/// 预留闭包形态待异步对偶 run_async_rmw 出现高阶需求时再立
pub trait RmwOp<Obj, Op> {
  fn run<'o>(
    &mut self,
    obj: &mut Obj,
    op: Op,
    args: &[&[u8]],
    output: &'o mut Vec<u8>,
  ) -> ObjectOutput<'o>;
}

/// 集合对象公共分发算子（Hash / Set / List / ZSet）
#[derive(Clone)]
pub struct CollectionOp<Op> {
  pub arg1: i32,
  pub arg2: i32,
  pub resp_version: u8,
  pub phantom: PhantomData<fn(Op)>,
}

impl<Obj: IGarnetObject, Op: Copy + Into<u8>> RmwOp<Obj, Op> for CollectionOp<Op> {
  #[inline]
  fn run<'o>(
    &mut self,
    obj: &mut Obj,
    op: Op,
    args: &[&[u8]],
    output: &'o mut Vec<u8>,
  ) -> ObjectOutput<'o> {
    run_operate(
      obj,
      op,
      args,
      self.arg1,
      self.arg2,
      self.resp_version,
      output,
    )
  }
}

/// 同步对象 RMW 处理策略集合
pub struct SyncRmwHandlers<Obj, Op, RunOp, ShouldWrite> {
  pub deserialize: fn(&[u8]) -> Option<Obj>,
  pub default_obj: fn() -> Obj,
  pub is_empty: fn(&Obj) -> bool,
  pub serialize: fn(&Obj) -> Vec<u8>,
  pub run_op: RunOp,
  pub should_write: ShouldWrite,
  /// Missing 短路钩子（对位 `slow_load_eval` 的 `on_missing` 形制）：`Some`
  /// 且装载为 Missing 时直出常量帧、跳过 `default_obj`+`run_op` 求值与写回
  /// 判定——C# NOTFOUND 恒常量帧形（如 HashCommands.cs:148/:513 空数组），
  /// 杜绝空对象求值在 RESP3 出 `%0` 与 C# `*0` 分叉；缺省 `None` 维持
  /// HTTL/HGET/HMGET 等既有臂的空对象求值矩阵零改动
  pub on_missing: Option<fn(&mut Vec<u8>)>,
  pub phantom: PhantomData<fn() -> (Obj, Op)>,
}

impl<Obj, Op, RunOp, ShouldWrite> SyncRmwHandlers<Obj, Op, RunOp, ShouldWrite>
where
  RunOp: RmwOp<Obj, Op>,
  ShouldWrite: FnOnce(Op, &ObjectOutput<'_>, &Obj, bool) -> bool,
{
  #[inline]
  pub fn new(
    deserialize: fn(&[u8]) -> Option<Obj>,
    default_obj: fn() -> Obj,
    is_empty: fn(&Obj) -> bool,
    serialize: fn(&Obj) -> Vec<u8>,
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

/// 集合类型（Hash / Set / List / ZSet）公共同步 RMW 骨架装配器：
///
/// 4 种对象同形收口——`deserialize`/`default_obj`/`is_empty`/`to_blob` 均直
/// 借 [`GarnetObjectPayload`]/[`IGarnetObject`] 现成签名零重写；`run_op` 由
/// 各集合命令枚举的分发函数直接捕获 `(arg1, arg2, resp_version)` 闭包化，
/// 一行装配，hash 的 on_missing 钩子由调用点 `.with_on_missing` 可选挂。
pub fn collection_rmw_handlers<Obj, Op, ShouldWrite>(
  arg1: i32,
  arg2: i32,
  resp_version: u8,
  should_write: ShouldWrite,
) -> SyncRmwHandlers<Obj, Op, CollectionOp<Op>, ShouldWrite>
where
  Obj: GarnetObjectPayload + IGarnetObject + Default,
  Op: Copy + Into<u8>,
  ShouldWrite: FnOnce(Op, &ObjectOutput<'_>, &Obj, bool) -> bool,
{
  SyncRmwHandlers::new(
    Obj::from_blob,
    Obj::default,
    <Obj as GarnetObjectPayload>::is_empty,
    Obj::to_blob,
    CollectionOp {
      arg1,
      arg2,
      resp_version,
      phantom: PhantomData,
    },
    should_write,
  )
}

/// 集合命令同步 RMW 装配单源（List / Set / ZSet 三壳共用）：[`SyncRmwCmd`]
/// 输入包 + [`collection_rmw_handlers`] 装配 + [`run_sync_rmw`] 执行骨架
/// 一站式收口，命令壳仅补对象标签、操作枚举与回写判定器（协议版本由会话
/// 协商值经参数下发）
#[inline]
pub(crate) fn collection_sync_rmw<Obj, Op, D, ShouldWrite>(
  resp_version: u8,
  store: &BatchStoreSession<'_, D>,
  cmd: SyncRmwCmd<'_, Op>,
  output: &mut Vec<u8>,
  should_write: ShouldWrite,
) -> SyncRmwOutcome
where
  Obj: GarnetObjectPayload + IGarnetObject + Default,
  Op: Copy + Into<u8>,
  D: Device,
  ShouldWrite: FnOnce(Op, &ObjectOutput<'_>, &Obj, bool) -> bool,
{
  let SyncRmwCmd { arg1, arg2, .. } = cmd;
  run_sync_rmw(
    store,
    cmd,
    output,
    collection_rmw_handlers(arg1, arg2, resp_version, should_write),
  )
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
pub fn run_sync_rmw<Obj: IGarnetObject, Op: Copy + Into<u8>, D: Device, RunOp, ShouldWrite>(
  store: &BatchStoreSession<'_, D>,
  cmd: SyncRmwCmd<'_, Op>,
  output: &mut Vec<u8>,
  mut handlers: SyncRmwHandlers<Obj, Op, RunOp, ShouldWrite>,
) -> SyncRmwOutcome
where
  RunOp: RmwOp<Obj, Op>,
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
  let mut obj_out = handlers.run_op.run(&mut obj, cmd.op, cmd.args, output);
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
        // key 为信封物理键（KeyTag::ObjectEnvelope），与存储记录域一致
        let raw_key = store.session_tag_key(KeyTag::ObjectEnvelope, cmd.key);
        let notif = wkv::ObjectRmwNotification {
          key: &raw_key,
          obj_type: cmd.tag,
          op_code: cmd.op.into(),
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
  RunOp,
  ShouldWrite,
>(
  storage: &StorageSession<'_, D>,
  cmd: SyncRmwCmd<'_, Op>,
  output: &mut Vec<u8>,
  mut handlers: SyncRmwHandlers<Obj, Op, RunOp, ShouldWrite>,
) -> Result<ObjLoad<RespRmwDone>, ()>
where
  RunOp: RmwOp<Obj, Op>,
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
      let mut obj_out = handlers.run_op.run(&mut obj, cmd.op, cmd.args, output);
      let result1 = obj_out.result1;

      if (handlers.should_write)(cmd.op, &obj_out, &obj, existed)
        && apply_rmw_post_operate(
          storage,
          RmwPostTarget {
            key: cmd.key,
            tag: cmd.tag,
            tiered: true,
            loaded: Some(KeyTag::Meta),
          },
          &obj,
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
  //（物理记录键与用户键两把不同基，取之即双锁序死锁面），交叠裁决由写回前
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
  let mut obj_out = handlers.run_op.run(&mut obj, cmd.op, cmd.args, output);
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
    let unchanged = match obj_save_recheck_async(storage, cmd.key, loaded).await {
      Ok(v) => v,
      // 探针 Err（内存快照 / 异步读通 IO 失败）同按清场纪律先复位再落错，
      // 与下方 unchanged=false、apply_rmw_post_operate 失败两臂及物化臂同形
      Err(e) => {
        log::error!("run_async_rmw obj_save_recheck_async err: {e:?}");
        obj_out.reset();
        return Err(());
      }
    };
    if !unchanged {
      obj_out.reset();
      return Err(());
    }
    if apply_rmw_post_operate(
      storage,
      RmwPostTarget {
        key: cmd.key,
        tag: cmd.tag,
        tiered: false,
        loaded,
      },
      &obj,
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
/// 登记自迁移安全换入窗后全扫物化 + 解码 fail-fast + 统一错误日志；物化
/// `Ok(None)`（窗内键消亡）映射 [`SealedLoad::Missing`] 交调用方既有
/// Missing 臂承接（信封通道按缺新建 / on_missing 短路帧——与 run_async_rmw
/// 物化臂 break 'materialize 落信封通道同一消费形态，写回复验
/// obj_writeback_rechecked_async 终态复验兜底并发交叠）；其余臂原样回传，
/// WrongType/Missing/Present 臂的差异映射留在调用方
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
        // 窗内键消亡（头注契约「调用方落信封通道按 Missing 新建承接」）：
        // 映射 Missing 交调用方既有臂，绝不折存储错误帧——装载体按缺席
        // 承接而非按存储忙拒绝
        return Ok(SealedLoad::Missing);
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
