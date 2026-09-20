//! 单键迁移资格与活值探测分类读取逻辑（对标 libs/cluster/Server/Migration/MigrateScanFunctions.cs:StoreScan）

use wbase::{convert::unix_time_in_milliseconds_from_ticks, time::now_ticks};
use wconn::record::MigrateVal;
use wdev::Device;
use wnode::{
  resp::vector::{
    vector_manager::{INDEX_SIZE_BYTES, VectorManager},
    vector_manager_locking::registry_key,
  },
  storage::session::{common::ttl_sync::meta_collection_type_of, storage_session::StorageSession},
};
use wval::{GarnetObjectType, KeyTag};

use crate::error::Result;

/// 暂不支持迁移的键登记条目（键 + 类型说明）
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedKey<'a> {
  /// 键字节
  pub key: &'a [u8],
  /// 类型说明标签
  pub kind_label: &'static str,
}

/// 信封内层标签是否为可迁移集合对象（Hash/Set/List/SortedSet 四类；
/// RangeIndex 与未知类型显式不支持，一处定义供收发两端共用）
#[inline]
pub fn migratable_object_type(tag: u8) -> bool {
  matches!(
    GarnetObjectType::from_u8(tag),
    Some(
      GarnetObjectType::SortedSet
        | GarnetObjectType::List
        | GarnetObjectType::Hash
        | GarnetObjectType::Set
    )
  )
}

/// 暂不支持迁移的内层类型说明标签（清单登记用）
#[inline]
pub fn unsupported_label(tag: u8) -> &'static str {
  match GarnetObjectType::from_u8(tag) {
    Some(GarnetObjectType::RangeIndex) => "rangeindex",
    _ => "unknown",
  }
}

/// 单键迁移资格分类（轻量探测，零值载荷物化）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LiveKeyKind {
  /// 可迁移（String 记录）
  String,
  /// 可迁移（合规集合对象信封）
  ObjectEnvelope,
  /// wbftree 页存储集合键（RangeIndex 与升阶分层集合共用带外分块流迁移）
  TieredTree,
  /// 向量集键（带外上下文预留 + 元素重放迁移）
  VectorSet,
  /// 暂不支持迁移（未知信封类型）
  Unsupported(&'static str),
  /// 键不存在 / 已过期 / 竞态消亡
  Gone,
}

/// 轻量探测单键迁移资格（零载荷拷贝，闭包内只读标签判型）
pub async fn probe_live_key_kind<D: Device>(
  storage: &StorageSession<'_, D>,
  vm: Option<&VectorManager>,
  key: &[u8],
) -> Result<LiveKeyKind> {
  // 1. string 域（只读存在性，不物化值字节）
  if storage
    .read_tag_with(key, KeyTag::String, |_| ())
    .await?
    .is_some()
  {
    return Ok(LiveKeyKind::String);
  }
  // 2. 对象信封域：闭包内只读首字节内层标签，零物化
  if let Some(tag) = storage
    .read_tag_with(key, KeyTag::ObjectEnvelope, |raw| {
      raw.first().copied().unwrap_or(0)
    })
    .await?
  {
    return Ok(if migratable_object_type(tag) {
      LiveKeyKind::ObjectEnvelope
    } else {
      LiveKeyKind::Unsupported(unsupported_label(tag))
    });
  }
  // 3. 集合元记录域（RangeIndex 与升阶分层集合键共用物理域）：闭包内单次读
  //    解析 MetaValue 判型（判据单点 meta_collection_type_of，畸形/死记录
  //    None 落不存在）——该域任一存活元记录均以 wbftree 页存储承载，
  //    统一走带外分块流迁移（流元随帧携载，类型无关）。read_tag_with 闭包
  //    返回 Option，外套域存在性 Option，flatten 折叠双层：仅存活元记录判真
  if storage
    .read_tag_with(key, KeyTag::Meta, meta_collection_type_of)
    .await?
    .flatten()
    .is_some()
  {
    return Ok(LiveKeyKind::TieredTree);
  }
  // 4. 向量集域（索引记录驻留向量管理器登记表；按本会话域复合寻址，
  // 与三域探测同前缀口径，杜绝跨域同名键误分类）
  let prefix = storage.batch.session_prefix();
  if vm.is_some_and(|vm| vm.read_migrated_index(prefix.as_slice(), key).is_some()) {
    return Ok(LiveKeyKind::VectorSet);
  }
  Ok(LiveKeyKind::Gone)
}

/// 单键迁移资格分类（存储读取产物）
pub enum LiveValue {
  /// 可迁移：值 + TTL 毫秒
  Migratable(MigrateVal, i64),
  /// wbftree 页存储集合键（RangeIndex 与升阶分层集合共用带外分块流迁移）
  TieredTree,
  /// 向量集键（带外上下文预留 + 元素重放迁移）
  VectorSet,
  /// 暂不支持迁移（未知信封类型）
  Unsupported(&'static str),
  /// 键不存在 / 已过期 / 竞态消亡
  Gone,
}

/// libs/cluster/Server/Migration/MigrateScanFunctions.cs:Reader
///
/// 读取单键迁移资格与活值（string 域 → 信封域 → 元记录/带外域逐级判定，
/// TTL 惰性过期裁决内置于域读取）。`vm` 为本端向量集合管理器（向量集索引
/// 记录驻留域内登记表，存储域不可见；None = 装配未注入，向量集不可达）
///
/// （C# 扫描回调 StoreScan.Reader 的逐记录分类分发语义——过期/墓碑过滤后
/// 按命名空间记录、RangeIndex、向量集、可迁移键分流——在此以逐键读取形态
/// 承接；C# 原型见 libs/cluster/Server/Migration/MigrateScanFunctions.cs:StoreScan）
///
/// Reader 内三处副作用不在本件：取消检查（ThrowIfCancelled）由调用侧停等
/// 与驱动循环检查点承接、槽位归属过滤（Contains(slot)）由 rust 槽位索引
/// 枚举天然满足、收键入 sketch（TryHashAndStore）由调用方在传输批前落
/// （slots.rs 的 hash_and_store / migrate_session_vector_set.rs 同形）。
/// Reader 的 hasNamespace 分支（向量集元素按命名空间收键）对位本件第 4 步
/// 登记表域判定。
pub async fn read_live_value<D: Device>(
  storage: &StorageSession<'_, D>,
  vm: Option<&VectorManager>,
  key: &[u8],
) -> Result<LiveValue> {
  // 1. string 域（命中即活值；过期键域内不可见）
  if let Some(val) = storage.read_string(key).await? {
    return Ok(LiveValue::Migratable(
      MigrateVal::Str(val),
      expire_unix_ms(storage, key).await?,
    ));
  }
  // 2. 对象信封域：闭包只读判标签，仅合规类型物化整值
  if let Some((tag, env_opt)) = storage
    .read_tag_with(key, KeyTag::ObjectEnvelope, |raw| {
      let tag = raw.first().copied().unwrap_or(0);
      (tag, migratable_object_type(tag).then(|| raw.to_vec()))
    })
    .await?
  {
    return Ok(if let Some(env) = env_opt {
      LiveValue::Migratable(MigrateVal::Env(env), expire_unix_ms(storage, key).await?)
    } else {
      LiveValue::Unsupported(unsupported_label(tag))
    });
  }
  // 3. 集合元记录域（RangeIndex 与升阶分层集合键共用物理域，判据同探测侧
  //    单点）：该域任一存活元记录均以 wbftree 页存储承载，统一走带外分块
  //    流迁移（发送侧 snapshot_range_index_and_create_reader 泛化后按树快照
  //    字节流式发送，类型与 TTL 随流元携载）。flatten 折叠域/存活双层 Option，
  //    仅存活元记录判真（死/畸形落不存在）
  if storage
    .read_tag_with(key, KeyTag::Meta, meta_collection_type_of)
    .await?
    .flatten()
    .is_some()
  {
    return Ok(LiveValue::TieredTree);
  }
  // 4. 向量集域（索引记录驻留向量管理器登记表；本会话域复合寻址）
  let prefix = storage.batch.session_prefix();
  if vm.is_some_and(|vm| vm.read_migrated_index(prefix.as_slice(), key).is_some()) {
    return Ok(LiveValue::VectorSet);
  }
  Ok(LiveValue::Gone)
}

/// 键 TTL 毫秒（活值读取后调用；无 TTL 记录返回 0；换算委托 wbase::convert 单点）
async fn expire_unix_ms<D: Device>(storage: &StorageSession<'_, D>, key: &[u8]) -> Result<i64> {
  Ok(match storage.batch.ttl_of(key).await? {
    Some(exp) if exp > now_ticks() => unix_time_in_milliseconds_from_ticks(exp),
    _ => 0,
  })
}

/// 预检键清单，返回其中「暂不支持迁移」的键（仅未知信封内层类型）。
/// 供 KEYS 迁移入口在触达远端前对混合键请求整体拒绝并列明清单——绝不静默
/// 跳键。wbftree 页存储集合键（RangeIndex 与升阶分层集合）与向量集键不再
/// 列入拒绝清单（带外通道已打通），由调用方收集后走对应带外迁移。
pub async fn probe_unsupported_keys<'a, D: Device, K: AsRef<[u8]>>(
  storage: &StorageSession<'_, D>,
  vm: Option<&VectorManager>,
  keys: &'a [K],
) -> Result<Vec<UnsupportedKey<'a>>> {
  let mut unsupported = Vec::new();
  for key in keys {
    let key_ref = key.as_ref();
    if let LiveKeyKind::Unsupported(kind_label) = probe_live_key_kind(storage, vm, key_ref).await? {
      unsupported.push(UnsupportedKey {
        key: key_ref,
        kind_label,
      });
    }
  }
  Ok(unsupported)
}

/// 收集键清单中的向量集键及其源端索引记录（KEYS 迁移发现面）
pub async fn collect_vector_set_keys<D: Device, K: AsRef<[u8]>>(
  storage: &StorageSession<'_, D>,
  vm: Option<&VectorManager>,
  keys: &[K],
) -> Result<Vec<(Vec<u8>, [u8; INDEX_SIZE_BYTES])>> {
  let Some(vm) = vm else {
    return Ok(Vec::new());
  };
  let mut vector_sets = Vec::new();
  // 会话域复合收集：产物即登记表复合键（与 SLOTS 链枚举口径一致，
  // 帧传输/源端删除共用复合键域）
  let prefix = storage.batch.session_prefix();
  for key in keys {
    let key_ref = key.as_ref();
    let Some(index) = vm.read_migrated_index(prefix.as_slice(), key_ref) else {
      continue;
    };
    if let LiveKeyKind::VectorSet = probe_live_key_kind(storage, Some(vm), key_ref).await? {
      let rk = registry_key(prefix.as_slice(), key_ref);
      vector_sets.push((rk.as_slice().to_vec(), index));
    }
  }
  Ok(vector_sets)
}
