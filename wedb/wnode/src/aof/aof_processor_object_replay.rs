//! AOF 对象存重放通道（对标 libs/server/AOF/AofProcessor.cs 的
//! ObjectStoreUpsert / ObjectStoreRMW / ObjectStoreDelete /
//! UnifiedStoreStringUpsert 段）。
//!
//! C# ObjectStoreRMW 经 Tsavorite objectContext 以 GarnetObjectType 泛型
//! 多态应用到四内存对象；rust 侧以 [`ReplayObject`] 静态分发承接
//! （信封域读出 → operate → 删空自愈 / 回写信封），无运行时查表。

use wcol::{
  HashObject, ListObject, ObjectOutput, SetObject, SortedSetObject,
  object_payload::{GarnetObjectPayload, obj_decode},
};
use wdev::Device;
use wval::{GarnetObjectType, KeyTag};

use super::{aof_processor::AofReplayError, replay_input::ReplayInputRef};
use crate::storage::session::storage_session::StorageSession;

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
pub async fn object_store_rmw<D: Device>(
  session: &StorageSession<'_, D>,
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
  let resp_version = session.resp_protocol_version();

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

/// libs/server/AOF/AofProcessor.cs:UnifiedStoreStringUpsert
pub async fn unified_store_string_upsert<D: Device>(
  session: &StorageSession<'_, D>,
  key: &[u8],
  value: &[u8],
) -> Result<(), AofReplayError> {
  // 统一存字符串面上 upsert 与主存 upsert 同形（wkv 单一面）。C# 的
  // RENAME 向量特例（arg1 == VectorManager.RecordType → HandleVectorSet-
  // RenameCopy）在本仓落在 StoreRMW 向量分支（向量集 RENAME 经合成
  // StoreRMW 条目入 AOF，见 VectorManager::replicate_vector_set_rename），
  // UnifiedStoreStringUpsert 重放面无向量形态
  super::aof_processor::AofProcessor::store_upsert(session, KeyTag::String, key, value).await
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
