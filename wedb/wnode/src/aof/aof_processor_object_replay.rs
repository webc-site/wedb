//! AOF 对象存重放通道（对标 libs/server/AOF/AofProcessor.cs 的
//! ObjectStoreUpsert / ObjectStoreRMW / ObjectStoreDelete 段）。
//!
//! C# ObjectStoreRMW 经 Tsavorite objectContext 以 GarnetObjectType 泛型
//! 多态应用到四内存对象；rust 侧以 [`ReplayObject`] 静态分发承接
//! （信封域读出 → operate → 删空自愈 / 回写信封），无运行时查表。
//! ObjectStoreRMW 条目两类两域：分层稳态写镜像条目（
//! StoreEvent::TieredCollectionWrite 入账形态）物理键恒 Meta 域，信封 RMW
//! 条目（ObjectRmw 通知形态）物理键恒 ObjectEnvelope 域——[`object_store_rmw`]
//! 入口先验物理键标签路由（`KeyContextGuard` 已零成本解出），Meta 域交
//! `tiered_replay_arm` 分层臂（缺态留痕跳过，绝不落信封通道），信封域维持
//! 既有信封通道；树内四族「操作码转换 → exec_tiered_* 装配」分派转引
//! tiered_collection_ops::exec_tiered_by_op_code 共享单点。

use wcol::{
  HashObject, ListObject, ObjectOutput, SetObject, SortedSetObject,
  object_payload::{GarnetObjectPayload, obj_decode},
};
use wdev::Device;
use wval::{GarnetObjectType, KeyTag};

use super::{aof_processor::AofReplayError, replay_input::ReplayInputRef};
use crate::{
  resp::objects::tiered_collection_ops::{TieredCollectionArgs, TieredCtx, exec_tiered_by_op_code},
  storage::session::storage_session::StorageSession,
};

/// libs/server/AOF/AofProcessor.cs:ObjectStoreUpsert
pub async fn object_store_upsert<D: Device>(
  session: &StorageSession<'_, D>,
  key: &[u8],
  value: &[u8],
) -> Result<(), AofReplayError> {
  // 对象信封：[tag u8][4B count][payload]（C# 回放经 GarnetObjectSerializer
  // 解出对象再交对象存序列化回写；rust 信封记录本身即存值真身，
  // 零拷贝直写单源，对象读径统一走 from_blob 剥壳）
  if value.is_empty() {
    return Err("ObjectStoreUpsert 值缺少类型标签".to_string().into());
  }
  session
    .upsert_tag(key, KeyTag::ObjectEnvelope, value)
    .await
    .map_err(AofReplayError::Store)
}

/// libs/server/AOF/AofProcessor.cs:ObjectStoreRMW
///
/// 条目路由先验物理键标签（`KeyContextGuard` 解物理键时零成本解出，路由
/// 判定一处）：两类条目在 AOF 内本可零成本区分，禁仅凭 load_collection_stub
/// 探测误路由——
/// - Meta 域：分层稳态写镜像条目（service.rs StoreEvent::TieredCollectionWrite
///   单点物化），交 [`tiered_replay_arm`]；stub 缺失 / 型不符 / 操作码越覆盖面
///   一律留痕跳过，**绝不落信封通道**（信封空对象重建在树态键旁造幻影信封，
///   对盘同步快照留痕跳过键更会物化半截幻影，见 tiered_replay_arm 文注）；
/// - ObjectEnvelope 域：既有信封 RMW 条目（run_sync_rmw 的 notify_object_rmw），
///   维持下方信封通道，升阶前历史条目不受影响；
/// - 其余标签：AOF 流损坏或写 / 放两端演化失配，log error 跳过，同样不落
///   信封通道。
pub async fn object_store_rmw<D: Device>(
  session: &StorageSession<'_, D>,
  tag: KeyTag,
  key: &[u8],
  input: &[u8],
) -> Result<(), AofReplayError> {
  let r_input = ReplayInputRef::deserialize(input).ok_or("ObjectStoreRMW input 损坏")?;

  // 确定对象类型（生产写入端与规范 AOF 条目恒写 obj_type）
  let obj_type =
    GarnetObjectType::from_u8(r_input.obj_type).ok_or("ObjectStoreRMW 未知对象类型")?;

  let sub_id = r_input.sub_id;
  let arg1 = r_input.arg1 as i32;
  let arg2 = r_input.arg2 as i32;
  let args = &r_input.args;

  match tag {
    // 分层稳态写镜像条目（Meta 域）：分层臂闭环（含留痕跳过），恒不落信封通道
    KeyTag::Meta => {
      return tiered_replay_arm(session, key, obj_type, sub_id, arg1, arg2, args).await;
    }
    // 既有信封 RMW 条目（ObjectEnvelope 域）：落下方信封通道
    KeyTag::ObjectEnvelope => {}
    // 非镜像非信封：AOF 流损坏 / 演化失配，留痕跳过
    _ => {
      log::error!(
        "ObjectStoreRMW replay: unexpected physical key tag {tag:?}, key='{}'",
        String::from_utf8_lossy(key)
      );
      return Ok(());
    }
  }

  let resp_version = session.resp_version;
  match obj_type {
    GarnetObjectType::Hash => {
      replay_object_channel::<HashObject, D>(session, key, sub_id, args, arg1, arg2, resp_version)
        .await
    }
    GarnetObjectType::Set => {
      replay_object_channel::<SetObject, D>(session, key, sub_id, args, arg1, arg2, resp_version)
        .await
    }
    GarnetObjectType::List => {
      replay_object_channel::<ListObject, D>(session, key, sub_id, args, arg1, arg2, resp_version)
        .await
    }
    GarnetObjectType::SortedSet => {
      replay_object_channel::<SortedSetObject, D>(
        session,
        key,
        sub_id,
        args,
        arg1,
        arg2,
        resp_version,
      )
      .await
    }
    _ => Err("ObjectStoreRMW 不支持的对象类型".into()),
  }
}

/// 越覆盖面发散残留的留痕出口（与判型不符 / stub 缺失同一处置：跳过不重建）
#[inline]
fn beyond_tiered_arm(key: &[u8], op_code: u8) -> Result<(), AofReplayError> {
  log::error!(
    "tiered rmw replay: op_code {} beyond tiered steady-write arm, key='{}'",
    op_code,
    String::from_utf8_lossy(key)
  );
  Ok(())
}

/// Meta 域镜像条目 stub 缺失的留痕出口（发散残留处置，绝不落信封通道）
///
/// 预期形态文注：盘同步快照对升阶键留痕跳过（replication_snapshot_iterator
/// 「键保留源端、副本不含该批键」），续推 AOF 自锚点起——锚后该键的稳态写
/// 镜像条目在副本 `load_collection_stub` 探空，属 diskless 留痕契约已知面：
/// 升阶键在副本**整体缺失**（树与信封俱无），后续全量同步 / 升阶流条目自会
/// 按源端口径收敛。此刻若按空对象落信封通道，即物化出只含锚后字段的半截
/// 幻影信封对象，与源端千万级分层树 TYPE / HLEN 全发散——故留痕跳过，
/// 严禁重建。
#[inline]
fn missing_tiered_stub_skip(key: &[u8], op_code: u8) -> Result<(), AofReplayError> {
  log::error!(
    "tiered rmw replay: mirror entry for missing tiered stub (diskless trace-skip residue), key='{}', op_code {op_code}",
    String::from_utf8_lossy(key)
  );
  Ok(())
}

/// 分层稳态写镜像条目重放臂（副本/恢复端与主端稳态写臂的对接单点）
///
/// 入口前提：条目物理键已验为 Meta 域（object_store_rmw 路由单点），故本臂
/// **恒不回落信封通道**——`Ok(())` 即条目已闭环（执行或留痕跳过），无第二
/// 去向。判定与主端 [`crate::resp::objects::rmw_helpers::try_tiered_arm`]
/// 同一单点：`load_collection_stub` 探分层态 + `collection_type` 判型
///（RI.SET 先例 RangeIndexWrite 的回放判型门在 `load_range_index_stub` 内
/// 要求 collection_type == RangeIndex，会把分层键拒成 WrongType，故不可复用）。
/// - stub 缺失：盘同步快照留痕跳过键的锚后镜像（diskless 留痕契约已知面，
///   升阶键整体缺失，见 [`missing_tiered_stub_skip`] 文注），留痕跳过；
/// - 型不符 / 操作码不可转 / 越覆盖面 / 穿透：主端镜像条目只产自树内稳态写臂
///   **生效**的命令（dirty 判据），上述形态意味着 AOF 流损坏或主从发散残留，
///   显式留痕后跳过——绝不落信封通道（信封空对象重建会在树态键旁造幻影
///   信封）；
/// - 命中：经 [`exec_tiered_by_op_code`] 共享分派单点按主端 `exec_tiered_*`
///   同一臂逐条重放（应答负载挂本地 scratch 弃用；WATCH 推进与镜像 emit 由
///   重放会话 pause_aof_listeners 闸收口，副本重放零自激）。
///
/// `Err(())`（存储 IO 失败）上抛中止恢复，与信封通道失败语义同口径
async fn tiered_replay_arm<D: Device>(
  session: &StorageSession<'_, D>,
  key: &[u8],
  tag: GarnetObjectType,
  op_code: u8,
  arg1: i32,
  arg2: i32,
  args: &[&[u8]],
) -> Result<(), AofReplayError> {
  let Some((mut meta, mut stub)) = session
    .batch
    .load_collection_stub(key)
    .await
    .map_err(AofReplayError::Store)?
  else {
    return missing_tiered_stub_skip(key, op_code);
  };
  if meta.collection_type != tag {
    log::error!(
      "tiered rmw replay: collection_type {:?} != entry obj_type {tag:?}, key='{}'",
      meta.collection_type,
      String::from_utf8_lossy(key)
    );
    return Ok(());
  }
  let mut scratch = Vec::new();
  let routed = exec_tiered_by_op_code(
    &session.batch,
    key,
    tag,
    &mut TieredCtx::new(&mut meta, &mut stub),
    TieredCollectionArgs::new(op_code, (arg1, arg2), args, session.resp_version),
    &mut scratch,
  )
  .await;
  match routed {
    // 树内臂已执行（含被拒/未置脏的零副作用臂——镜像条目不产自这些臂，此处
    // 只代表命令语义在副本树上已闭环）
    Ok(Some(true)) => Ok(()),
    Err(()) => Err(AofReplayError::Replay(format!(
      "tiered rmw replay storage IO failed, key='{}'",
      String::from_utf8_lossy(key)
    ))),
    // 操作码不可转 / 判别类型越覆盖面（None）与转换成功却越树内覆盖面穿透
    //（Some(false)）：镜像条目只产自稳态写臂生效命令，上述形态即发散残留，
    // 留痕跳过，绝不落信封通道
    Ok(None | Some(false)) => beyond_tiered_arm(key, op_code),
  }
}

/// libs/server/AOF/AofProcessor.cs:ObjectStoreDelete
pub async fn object_store_delete<D: Device>(
  session: &StorageSession<'_, D>,
  key: &[u8],
) -> Result<(), AofReplayError> {
  session
    .delete_string(key)
    .await
    .map_err(|e| format!("ObjectStoreDelete replay failed: {e}"))?;
  Ok(())
}

/// 四对象类型重放单通道约束（本地封闭 trait，继承 GarnetObjectPayload；
/// 对标 C# AofProcessor.ObjectStoreRMW<TObjectContext> 经 Tsavorite
/// objectContext 的泛型单通道，静态分发）。
trait ReplayObject: GarnetObjectPayload {
  /// RESP 语义操作（委托各对象固有 operate）
  fn apply(
    &mut self,
    sub_id: u8,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) -> bool;
}

impl ReplayObject for HashObject {
  #[inline]
  fn apply(
    &mut self,
    sub_id: u8,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) -> bool {
    self.operate(sub_id, args, arg1, arg2, output, resp_protocol_version)
  }
}

impl ReplayObject for SetObject {
  #[inline]
  fn apply(
    &mut self,
    sub_id: u8,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) -> bool {
    self.operate(sub_id, args, arg1, arg2, output, resp_protocol_version)
  }
}

impl ReplayObject for ListObject {
  #[inline]
  fn apply(
    &mut self,
    sub_id: u8,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) -> bool {
    self.operate(sub_id, args, arg1, arg2, output, resp_protocol_version)
  }
}

impl ReplayObject for SortedSetObject {
  #[inline]
  fn apply(
    &mut self,
    sub_id: u8,
    args: &[&[u8]],
    arg1: i32,
    arg2: i32,
    output: &mut ObjectOutput,
    resp_protocol_version: u8,
  ) -> bool {
    self.operate(sub_id, args, arg1, arg2, output, resp_protocol_version)
  }
}

/// 整对象回放泛型单通道（C# AofProcessor.ObjectStoreRMW 经 Tsavorite
/// objectContext 多态的对象应用段，rust 侧以 [`ReplayObject`] 静态分发：
/// 信封域现载荷 → [`GarnetObjectPayload::from_blob`] → [`ReplayObject::apply`] →
/// 删空走双域删除自愈，非空回写信封；键缺失按空对象重建，与 ObjectStoreRMW
/// 重放会话 NeedToCreate=true 口径一致）。
/// 载荷畸形（tag 正确 wire 损坏）fail-fast 显式回错中止恢复，对标 C# 回放经
/// GarnetObjectSerializer.DeserializeInternal 抛异常中断恢复的可见失败语义，
/// 严禁静默回退空对象后被 is_empty 臂折成整键删除销毁数据。
async fn replay_object_channel<T: ReplayObject, D: Device>(
  session: &StorageSession<'_, D>,
  key: &[u8],
  sub_id: u8,
  args: &[&[u8]],
  arg1: i32,
  arg2: i32,
  resp_version: u8,
) -> Result<(), AofReplayError> {
  let loaded = session
    .read_tag_with(key, KeyTag::ObjectEnvelope, |r| {
      match obj_decode(r, T::OBJECT_TAG) {
        // 标签不符按既有口径视同缺失（重建语义不变）
        None => Ok(None),
        Some(payload) => T::from_blob(payload).map(Some).ok_or_else(|| {
          AofReplayError::Replay(format!(
            "corrupted {} envelope payload for key '{}'",
            T::OBJECT_TAG as u8,
            String::from_utf8_lossy(key)
          ))
        }),
      }
    })
    .await
    .map_err(AofReplayError::Store)?;
  let mut obj = loaded.transpose()?.flatten().unwrap_or_default();
  // 回放仅取对象副作用（应答负载弃用），挂本地 sink 不触会话输出
  obj.apply(
    sub_id,
    args,
    arg1,
    arg2,
    &mut ObjectOutput::mount(&mut Vec::new()),
    resp_version,
  );
  if obj.is_empty() {
    session
      .delete_string(key)
      .await
      .map_err(AofReplayError::Store)?;
  } else {
    let blob = obj.to_blob();
    session
      .obj_save(key, T::OBJECT_TAG, &blob)
      .await
      .map_err(AofReplayError::Store)?;
  }
  Ok(())
}
