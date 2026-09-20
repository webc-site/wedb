//! 向量登记表持久化与恢复回建（对标 C# 索引/元数据记录入主存 + 恢复趟回建）
//!
//! C# 的向量索引记录（`RecordType == VectorManager.RecordType`）与上下文元数据
//! 记录（`METADATA_NAMESPACE`）驻 Tsavorite 主存，随检查点持久；恢复趟
//! （GarnetRecordTriggers.cs:OnRecoverySnapshotRead）逐记录喂
//! [`VectorManager::sanitize_and_track_ingested_record_if_applicable`] 暂存，
//! 恢复完成点（SingleDatabaseManager.cs:RecoverVectorSets →
//! VectorManager.cs:ReconcileRecoveredState）一次性收口。
//!
//! rust 的登记表（`key_index_registry`）与元数据驻内存，本模块以
//! [`KeyTag::VectorRegistry`] 旁路记录写透 wkv 存储域达成同等持久性，并在
//! 恢复完成点经 `hlog` 全区间扫描回建——扫描出的登记记录喂同一 sanitize 臂
//! （C# 恢复趟回调的对位形态），收口即 [`VectorManager::reconcile_recovered_state`]。
//!
//! 物理键布局（编码单点在本文件，写入与回建两侧共用）：
//! `[NsVarint][DbVarint][KeyTag::VectorRegistry][1B 子标签][负载]`
//! - 子标签 [`INDEX_SUBTAG`]：负载 = 集合用户键，值 = Index 字节（32B）；
//! - 子标签 [`METADATA_SUBTAG`]：负载 = i32 LE 元数据下标，值 = ContextMetadata
//!   字节（20B）；元数据为全局单例，恒落根域前缀。

use std::sync::Arc;

use wbase::future::blocking_wait;
use wdev::Device;
use wkv::WedbStore;
use wval::{KeyTag, NamespaceDbCodec, SessionPrefixBuf, TaggedKeyBuf};
use wvector::store::StoreCallbacks;

use super::{
  vector_manager::{
    CONTEXT_METADATA_SIZE, INDEX_SIZE_BYTES, METADATA_NAMESPACE, RECORD_TYPE, VectorManager,
  },
  vector_manager_locking::{domain_prefix, registry_key, split_registry_key},
};

/// 索引登记记录子标签。
pub const INDEX_SUBTAG: u8 = 0x01;
/// 上下文元数据记录子标签。
pub const METADATA_SUBTAG: u8 = 0x02;

/// 登记旁路记录持久化承接（rust 工程自创，无 c# 对应：C# 索引/元数据记录经
/// Tsavorite 主存天然入检查点；rust 登记表驻内存，经本接口写透 wkv 存储域
/// KeyTag::VectorRegistry 旁路记录达成同等持久性）。
///
/// 装配期经 [`VectorManager::attach_registry_store`] 一次性注入；未注入时
/// 写透零开销旁路（纯内存登记表形态，向量域测试夹具与嵌入式裸装配沿用）。
pub trait RegistryPersistence: Send + Sync + 'static {
  /// 写入（upsert 语义）。
  fn put(&self, physical_key: &[u8], value: &[u8]);
  /// 摘除（墓碑语义；返回是否命中仅作观测，调用方不分支）。
  fn remove(&self, physical_key: &[u8]);
}

/// 索引登记复合键 → 旁路记录物理键（写入单点，登记表复合键域入、物理键域出）。
///
/// 复合键必由 `registry_key` 构造（全仓唯一拼装入口的不变量），解码失败即
/// 不变量破坏，显式失败不静默降级。
#[inline]
pub fn index_registry_physical_key(rk: &[u8]) -> TaggedKeyBuf {
  let (domain, user_key) = split_registry_key(rk);
  let prefix = domain_prefix(domain);
  let mut payload = Vec::with_capacity(1 + user_key.len());
  payload.push(INDEX_SUBTAG);
  payload.extend_from_slice(user_key);
  NamespaceDbCodec::encode_with_session_prefix(prefix.as_slice(), KeyTag::VectorRegistry, &payload)
}

/// 上下文元数据下标 → 旁路记录物理键（元数据全局单例恒落根域前缀）。
#[inline]
pub fn metadata_registry_physical_key(index: i32) -> TaggedKeyBuf {
  let mut payload = [0u8; 1 + 4];
  payload[0] = METADATA_SUBTAG;
  payload[1..].copy_from_slice(&index.to_le_bytes());
  NamespaceDbCodec::encode_with_session_prefix(
    SessionPrefixBuf::ROOT.as_slice(),
    KeyTag::VectorRegistry,
    &payload,
  )
}

/// 旁路记录物理键解码三态（回建扫描单点消费）。
#[derive(Debug)]
enum RegistryRecord {
  /// 索引登记：复合键（前缀 + 用户键，`registry_key` 形态直拼回）。
  Index(TaggedKeyBuf),
  /// 上下文元数据：元数据数组下标。
  Metadata(i32),
}

/// 旁路记录物理键解码（标签、子标签、负载长度不合法即 None，扫描侧跳过）。
fn decode_registry_physical(physical: &[u8]) -> Option<RegistryRecord> {
  let prefix_len = NamespaceDbCodec::session_prefix_len_from_slice(physical)?;
  let prefix = &physical[..prefix_len];
  let rest = physical[prefix_len..].strip_prefix(&[KeyTag::VectorRegistry.as_u8()][..])?;
  let (subtag, payload) = rest.split_first()?;
  match *subtag {
    INDEX_SUBTAG => {
      // 登记表复合键 = 前缀 + 用户键（registry_key 全仓唯一拼装入口直拼回）
      Some(RegistryRecord::Index(registry_key(prefix, payload)))
    }
    METADATA_SUBTAG => {
      let (&index, empty) = payload.split_first_chunk::<4>()?;
      empty
        .is_empty()
        .then_some(RegistryRecord::Metadata(i32::from_le_bytes(index)))
    }
    _ => None,
  }
}

/// 恢复完成点的向量登记回建（C# RecoverVectorSets 的扫描段对位）。
///
/// 全区间扫描恢复出的日志（恢复装配期写者未启动，满足 `scan` 的冻结写入
/// 契约），收集链首存活的 [`KeyTag::VectorRegistry`] 旁路记录；扫描收尾后
/// （scan 迭代持页读锁，回调内严禁写 store——写透会自锁在页写锁上）逐条
/// 喂 [`VectorManager::sanitize_and_track_ingested_record_if_applicable`]——
/// 索引登记记录清指针并暂存恢复上下文、写回内存登记表镜像，元数据记录
/// 暂存恢复元数据。
///
/// 收口由调用方接 [`VectorManager::reconcile_recovered_state`]（C#
/// ReconcileRecoveredState 对位：还原元数据数组、清理未恢复上下文）。
///
/// 返回 `Some(恢复登记条数)`；`None` 表示登记数据损坏（重复元数据记录或
/// 扫描失败），调用方须显式报错，禁止静默吞成半截恢复。
pub async fn rebuild_registry_from_store<S: StoreCallbacks, D: Device>(
  store: &WedbStore<D>,
  vm: &VectorManager<S>,
) -> Option<usize> {
  if !vm.is_enabled() {
    return Some(0);
  }
  let index = store.index.load();
  let begin = store.begin_address();
  let tail = store.tail_address();

  // 段 1：扫描收集（值字节拷出；sanitize 臂需 &mut 清指针，盘上原记录不动，
  // 每次恢复重复清指针幂等无害——C# 原位改写日志页为效率优化，非语义必需）
  let mut pending: Vec<(RegistryRecord, Vec<u8>)> = Vec::new();
  let scan = store
    .hlog()
    .scan(begin, tail, |addr, rec| {
      // 链首校验：仅该键当前最新版本进入回建（多版本/墓碑链/旧版记录一律
      // 跳过，杜绝元数据同下标多版本误报重复）；墓碑直接跳过（sanitize 臂
      // 的 tombstone=true 分支即空操作，等价短路）
      if rec.is_tombstone() || index.find_tag(rec.key()) != Some(addr) {
        return Ok(true);
      }
      let Some(record) = decode_registry_physical(rec.key()) else {
        return Ok(true);
      };
      let value = rec.value();
      if value.len() > INDEX_SIZE_BYTES.max(CONTEXT_METADATA_SIZE) {
        return Ok(true);
      }
      pending.push((record, value.to_vec()));
      Ok(true)
    })
    .await;
  if let Err(err) = scan {
    log::error!("向量登记回建扫描失败: {err:?}");
    return None;
  }

  // 段 2：登记回建（页锁已释放，写透安全）
  let mut recovered: usize = 0;
  for (record, mut value) in pending {
    match &record {
      RegistryRecord::Index(rk) => {
        if !vm.sanitize_and_track_ingested_record_if_applicable(
          false,
          None,
          RECORD_TYPE,
          rk.as_slice(),
          &mut value,
        ) {
          return None;
        }
        // 登记条目写回内存登记表（sanitize 臂已清指针）。C# 主存记录
        // 即登记（恢复趟无此步）；rust 登记表为主存旁路记录的内存镜像，
        // 回建即重建镜像——写透单点 put_stored_index 幂等回写旁路记录
        let Ok(bytes) = <[u8; INDEX_SIZE_BYTES]>::try_from(value.as_slice()) else {
          continue;
        };
        vm.put_stored_index(rk.as_slice(), &bytes);
        recovered += 1;
      }
      RegistryRecord::Metadata(idx) => {
        if !vm.sanitize_and_track_ingested_record_if_applicable(
          false,
          Some(&[METADATA_NAMESPACE]),
          0,
          &idx.to_le_bytes(),
          &mut value,
        ) {
          return None;
        }
      }
    }
  }
  Some(recovered)
}

/// 登记旁路记录持久化的 wkv 存储域承接（写透钩子的宿主实现，装配期绑定
/// 存储会话；与 [`super::vector_store_callbacks::WedbVectorStoreCallbacks`]
/// 同型同域：热路径同步直写、失败回退阻塞收割）。
pub(crate) struct WedbRegistryPersistence<D: Device> {
  session: Arc<wkv::StoreSession<D>>,
}

impl<D: Device> WedbRegistryPersistence<D> {
  /// 绑定存储会话构造。
  pub fn new(session: Arc<wkv::StoreSession<D>>) -> Self {
    Self { session }
  }
}

impl<D: Device> RegistryPersistence for WedbRegistryPersistence<D> {
  fn put(&self, physical_key: &[u8], value: &[u8]) {
    match self.session.try_upsert_raw_sync(physical_key, value) {
      Ok(Ok(_)) => {}
      // 冷区键同步臂不接：回退阻塞收割一次异步追加（对齐向量存储回调 write 臂）
      _ => {
        let session = &self.session;
        let key = physical_key.to_vec();
        let value = value.to_vec();
        if blocking_wait(session.upsert_raw(&key, &value)).is_err() {
          log::error!("向量登记旁路记录写透失败: {key:?}");
        }
      }
    }
  }

  fn remove(&self, physical_key: &[u8]) {
    let session = &self.session;
    let key = physical_key.to_vec();
    if blocking_wait(session.delete_raw(&key)).is_err() {
      log::error!("向量登记旁路记录摘除失败: {key:?}");
    }
  }
}
