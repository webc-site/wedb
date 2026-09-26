//! 向量登记表持久化与恢复回建（对标 C# 索引/元数据记录入主存 + 恢复趟回建）
//!
//! C# 的向量索引记录（`RecordType == VectorManager.RecordType`）与上下文元数据
//! 记录（`MetadataNamespace`）驻 Tsavorite 主存，随检查点持久；恢复趟
//! （GarnetRecordTriggers.cs:OnRecoverySnapshotRead）逐记录喂
//! VectorManager.cs:SanitizeAndTrackIngestedRecordIfApplicable 暂存，
//! 恢复完成点（SingleDatabaseManager.cs:RecoverVectorSets →
//! VectorManager.cs:ReconcileRecoveredState）一次性收口。
//!
//! rust 的登记表（`key_index_registry`）与元数据驻内存，本模块以
//! [`KeyTag::VectorRegistry`] 旁路记录写透 wkv 存储域达成同等持久性，并在
//! 恢复完成点经 `hlog` 全区间扫描回建——扫描出的登记记录由
//! [`decode_registry_physical`] 解出强类型 [`VectorRegistrySubTag`] 后，直接喂
//! 对应的恢复处理方法（[`VectorManager::recovered_vector_set_index_key`] 与
//! [`VectorManager::recovered_context_metadata`]），收口即
//! [`VectorManager::reconcile_recovered_state`]。
//!
//! 物理键布局（编码单点在本文件，写入与回建两侧共用；子标签强类型枚举
//! [`VectorRegistrySubTag`] 定义于 wval `tag.rs`，与本文件写透/回建编码和
//! wkv 紧缩豁免臂共用同一真值源）：
//! `[NsVarint][DbVarint][KeyTag::VectorRegistry][1B 子标签][负载]`
//! - 子标签 [`VectorRegistrySubTag::Index`]：负载 = 集合用户键，值 = Index 字节（32B）；
//!   前缀随所属逻辑域走，域退役时经紧缩死域判定正常退役（根库 FLUSHDB 后
//!   (0,0) 坐标的 Index 记录即本库自身向量集的预期退役对象）。
//! - 子标签 [`VectorRegistrySubTag::Metadata`]：负载 = i32 LE 元数据下标，值 = ContextMetadata
//!   字节（20B）；元数据为全局单例，恒落根域前缀 (0,0)——换号不迁位，其生死
//!   由登记表回收通道（RegistryReclaim 命中域 + All/reset）自治，豁免于 wkv
//!   紧缩业务死域判定（compact.rs is_deleted 豁免臂，zcode-r149c-flushsnap
//!   案一：根库 FLUSHDB 退役 vdb 0 的窗口经 gc_dead 键 0 库级角色第一比对臂
//!   会直击该记录，误伤即全库向量上下文 in_use/slots 持久面丢失、重启回建
//!   趟静默失据）。
//!
//! 键自带域前缀（[`index_registry_physical_key`] 由记录自身域拼装、元数据恒
//! [`SessionPrefixBuf::ROOT`]），故写透与回建**都与当前执行域绑定的会话域无
//! 关**：任何绑定会话皆可读写同一旁路记录。据此 [`WedbRegistryPersistence`]
//! 与向量存储回调同型改为无状态，会话经
//! [`with_active_vector_session`](super::vector_store_callbacks::with_active_vector_session)
//! 取用；恢复回建的写段与收口则自持专用会话（对标 C# 恢复趟由
//! `RecoverVectorSets` 自备会话上下文，而非借用某条连接的线程静态会话）。

use std::{
  future::{Future, ready},
  marker::PhantomData,
  pin::Pin,
  sync::Arc,
};

use wbase::map::HashSet;
use wdev::Device;
use wkv::WedbStore;
use wval::{KeyTag, NamespaceDbCodec, SessionPrefixBuf, TaggedKeyBuf, VectorRegistrySubTag};
use wvector::store::StoreCallbacks;

use super::{
  vector_manager::{CONTEXT_METADATA_SIZE, INDEX_SIZE_BYTES, VectorManager},
  vector_manager_locking::{domain_prefix, registry_key, split_registry_key},
  vector_store_callbacks::{
    AssertSessionSend, OwnedActiveVectorSession, SessionRef, with_active_vector_session,
  },
};

/// 登记旁路记录持久化承接（rust 工程自创，无 c# 对应：C# 索引/元数据记录经
/// Tsavorite 主存天然入检查点；rust 登记表驻内存，经本接口写透 wkv 存储域
/// KeyTag::VectorRegistry 旁路记录达成同等持久性）。
///
/// 装配期经 [`VectorManager::attach_registry_store`] 一次性注入；未注入时
/// 写透零开销旁路（纯内存登记表形态，向量域测试夹具与嵌入式裸装配沿用）。
///
/// # 异步契约（手动装箱，弃 async-trait / RPITIT）
///
/// 本 trait 以 `Arc<dyn RegistryPersistence>` 擦除注入（设备类型泛型不进
/// [`VectorManager`]），`dyn` 面不可用 RPITIT，装箱 future 是唯一 async
/// 形态；`async-trait` 宏展开与本手动装箱同构且零额外能力，弃用。
/// 写透必须在 async 上下文闭环（`put`/`remove` 冷区臂 `.await` 引擎异步口，
/// 无任何内联收割），而调用点（create/recreate_index_locked 等独占锁同步
/// 段）原为 parking_lot 同步锁——守卫跨 await 的死锁论证（持锁任务挂起 →
/// 等锁任务 block 线程 → 持锁任务永无 poll）已随向量集合条带锁改 async
/// 锁（[`super::vector_manager_locking::VectorSetLocks`]）整体解除：异步锁
/// 等待方在 await 点让出线程，守卫随 async 栈帧跨 `.await` 存活。快路径
/// （`try_upsert_raw_sync` 命中）在装箱 future 首 poll 内同步闭环，无额外
/// 挂起开销。
///
/// # Send 通行证
///
/// 装箱 future 的 `Send` 约束与 [`StoreCallbacks`] 同型：内部持有的会话
/// 引用经 [`SessionRef`] 通行证承接（compio 任务不迁线程，future
/// 只在其所属任务线程上 poll，`Send` 纯为类型级通行证）。
pub trait RegistryPersistence: Send + Sync + 'static {
  /// 写入（upsert 语义；冷区臂真异步，首 poll 内取定会话引用后跨 await 自持）。
  /// 返回是否落盘成功。
  fn put(
    &self,
    physical_key: &[u8],
    value: &[u8],
  ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>>;
  /// 摘除（墓碑语义；冷区臂真异步，首 poll 内取定会话引用后跨 await 自持）。
  /// 返回写透是否成功（无会话 / 落盘失败为 false，log::error 各臂自落）：
  /// 失败信号必达调用方，由 `remove_stored_index` 单点累计结构化计数——
  /// 对标 C# ReplicateVectorSetRemove 失败 throw GarnetException 的错误
  /// 必达口径（rust 以 bool 返回承接 throw，控制面不引入异常）。
  fn remove(&self, physical_key: &[u8]) -> Pin<Box<dyn Future<Output = bool> + Send + '_>>;
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
  payload.push(VectorRegistrySubTag::Index.as_u8());
  payload.extend_from_slice(user_key);
  NamespaceDbCodec::encode_with_session_prefix(prefix.as_slice(), KeyTag::VectorRegistry, &payload)
}

/// 上下文元数据下标 → 旁路记录物理键（元数据全局单例恒落根域前缀）。
#[inline]
pub fn metadata_registry_physical_key(index: i32) -> TaggedKeyBuf {
  let mut payload = [0u8; 1 + 4];
  payload[0] = VectorRegistrySubTag::Metadata.as_u8();
  payload[1..].copy_from_slice(&index.to_le_bytes());
  NamespaceDbCodec::encode_with_session_prefix(
    SessionPrefixBuf::ROOT.as_slice(),
    KeyTag::VectorRegistry,
    &payload,
  )
}

/// 旁路记录物理键解码三态（回建扫描单点消费）。
#[derive(Debug, PartialEq)]
pub enum RegistryRecord {
  /// 索引登记：复合键（前缀 + 用户键，`registry_key` 形态直拼回）。
  Index(TaggedKeyBuf),
  /// 上下文元数据：元数据数组下标。
  Metadata(i32),
}

/// 旁路记录物理键解码（标签、子标签、负载长度不合法即 None，扫描侧跳过；
/// 子标签经 [`VectorRegistrySubTag::from_u8`] 强类型解析，match 全枚举穷尽）。
pub fn decode_registry_physical(physical: &[u8]) -> Option<RegistryRecord> {
  let prefix_len = NamespaceDbCodec::session_prefix_len_from_slice(physical)?;
  let prefix = &physical[..prefix_len];
  let rest = physical[prefix_len..].strip_prefix(&[KeyTag::VectorRegistry.as_u8()][..])?;
  let (subtag, payload) = rest.split_first()?;
  match VectorRegistrySubTag::from_u8(*subtag)? {
    VectorRegistrySubTag::Index => {
      // 登记表复合键 = 前缀 + 用户键（registry_key 全仓唯一拼装入口直拼回）
      Some(RegistryRecord::Index(registry_key(prefix, payload)))
    }
    VectorRegistrySubTag::Metadata => {
      let (&index, empty) = payload.split_first_chunk::<4>()?;
      empty
        .is_empty()
        .then_some(RegistryRecord::Metadata(i32::from_le_bytes(index)))
    }
  }
}

/// 恢复完成点的向量登记回建（C# RecoverVectorSets 的扫描段对位）。
///
/// 全区间扫描恢复出的日志（恢复装配期写者未启动，满足 `scan` 的冻结写入
/// 契约），收集链首存活的 [`KeyTag::VectorRegistry`] 旁路记录；扫描收尾后
/// （scan 迭代持页读锁，回调内严禁写 store——写透会自锁在页写锁上）按
/// [`decode_registry_physical`] 解出的 [`RegistryRecord`] 直接调对应恢复方法：
/// 索引登记记录清指针并经 [`VectorManager::recovered_vector_set_index_key`]
/// 暂存恢复上下文、写回内存登记表镜像，元数据记录经
/// [`VectorManager::recovered_context_metadata`] 暂存恢复元数据。
///
/// 收口由调用方接 [`VectorManager::reconcile_recovered_state`]（C#
/// ReconcileRecoveredState 对位：还原元数据数组、清理未恢复上下文）。
///
/// 返回 `Some(恢复登记条数)`；`None` 表示回建不可行（登记数据损坏——重复
/// 元数据记录或扫描失败；或自备会话资源不可得），调用方须显式报错拒启，
/// 禁止静默吞成半截恢复。
pub async fn rebuild_registry_from_store<S: StoreCallbacks, D: Device>(
  store: &Arc<WedbStore<D>>,
  vm: &VectorManager<S>,
) -> Option<usize> {
  if !vm.is_enabled() {
    return Some(0);
  }
  let index = store.index.load();
  let begin = store.begin_address();
  let tail = store.tail_address();

  // 段 1：扫描收集（值字节拷出；回建臂需 &mut 清指针，盘上原记录不动，
  // 每次恢复重复清指针幂等无害——C# 原位改写日志页为效率优化，非语义必需）
  let mut pending: Vec<(RegistryRecord, Vec<u8>)> = Vec::new();
  let scan = store
    .hlog()
    .scan(begin, tail, |addr, rec| {
      // 链首校验：仅该键当前最新版本进入回建（多版本/墓碑链/旧版记录一律
      // 跳过，杜绝元数据同下标多版本误报重复）；墓碑直接跳过（C# 恢复趟
      // 回调的 tombstone 短路分支即空操作，等价短路）
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

  // 段 2：登记回建（页锁已释放，写透安全）。扫描段与本段之间有 `.await`，
  // 故绑定守卫不得作为跨段状态——本段自持专用会话（对标 C# 恢复趟专用
  // 会话上下文），随本 async 栈帧跨写透 `.await` 存活（与量化/清理臂同型：
  // compio 任务不迁线程，会话引用于 put 首 poll 一次取出后自持，槽位重绑
  // 不影响已取出的引用）。会话由手头 store 直接自备、不经专用会话工厂：
  // 恢复回建运行于 open_recovered_* 装配口的 from_parts 之前，工厂此时尚
  // 未装配，而登记写透承接已在 node_components 注入，经工厂取会话必落空
  let Ok(_bound) = store
    .new_session()
    .map(OwnedActiveVectorSession::new)
    .map_err(|err| log::error!("向量登记回建自备会话失败（{err:?}），拒绝半截恢复"))
  else {
    return None;
  };
  let mut recovered: usize = 0;
  // 回建集的复合键全集（残影清退判据，扫描收集链一次登记）
  let recovered_keys: HashSet<Vec<u8>> = pending
    .iter()
    .filter_map(|(record, _)| match record {
      RegistryRecord::Index(rk) => Some(rk.as_slice().to_vec()),
      RegistryRecord::Metadata(_) => None,
    })
    .collect();
  for (record, mut value) in pending {
    match record {
      RegistryRecord::Index(rk) => {
        // 句柄属于写入进程——先清指针（C# 恢复趟 RecordType 臂语义：即使
        // Vector Set 未启用也须清，否则稍后启用时沿指针访问已死亡的地址
        // 空间；本入口已在 is_enabled 守卫之后），再暂存恢复上下文
        VectorManager::<S>::clear_index_pointer(&mut value);
        vm.recovered_vector_set_index_key(&value);
        // 登记条目写回内存登记表（上一步已清指针）。C# 主存记录
        // 即登记（恢复趟无此步）；rust 登记表为主存旁路记录的内存镜像，
        // 回建即重建镜像——写透单点 put_stored_index 幂等回写旁路记录
        let Ok(bytes) = <[u8; INDEX_SIZE_BYTES]>::try_from(value.as_slice()) else {
          continue;
        };
        if vm.put_stored_index(&rk, &bytes).await {
          recovered += 1;
        } else {
          log::error!("向量登记回建写回失败: {rk:?}");
        }
      }
      RegistryRecord::Metadata(idx) => {
        if !vm.recovered_context_metadata(&idx.to_le_bytes(), &value) {
          return None;
        }
      }
    }
  }
  // 残影镜像清退（工单 zcode-r137c-snaplock2 宗二）：镜像必等于日志回建集
  // ——C# 记录存于主存、恢复形制下整表随 store 重置，副本同步前本地旧集与
  // 主端已删集的旧镜像随旧表记录一并消失；rust 镜像独立于引擎实例，引擎
  // 置换后指向已弃置物理实例的旧登记条目（虚体、DiskANN 原生句柄悬挂、
  // VADD 复用时与主端 context 编号冲突写透回环）须在此刻经既有删除单点
  // 收口摘除（条带独占锁协议与 reclaim_registry_domain 同型：锁内复核 +
  // request_deletion 弃索引 + 登记表摘除）。启动形态镜像恒空，本段零成本
  let stale: Vec<Vec<u8>> = vm
    .key_index_registry
    .pin()
    .iter()
    .filter(|(rk, _)| !recovered_keys.contains(*rk))
    .map(|(rk, _)| rk.as_slice().to_vec())
    .collect();
  for rk in stale {
    let _lock = vm.vector_set_locks.acquire_exclusive(&rk).await;
    vm.delete_vector_set_of(&rk).await;
  }
  Some(recovered)
}

/// 登记旁路记录持久化的 wkv 存储域承接（写透钩子的宿主实现；与
/// [`super::vector_store_callbacks::WedbVectorStoreCallbacks`] 同型：**无状态**，
/// 会话经线程槽取当前执行域绑定者——快路径同步直写、冷区臂 `.await` 引擎
/// 异步口，无内联收割）。
///
/// 物理键自带域前缀，故写透落位与绑定会话的 (ns, db) 无关；绑定的意义仅在
/// 于取得「本执行域可写的会话」（wkv 参与者纪元槽按线程使用）。
pub(crate) struct WedbRegistryPersistence<D: Device> {
  _device: PhantomData<D>,
}

impl<D: Device> WedbRegistryPersistence<D> {
  /// 构造无状态写透承接（会话由调用侧在执行域内绑定）。
  pub const fn new() -> Self {
    Self {
      _device: PhantomData,
    }
  }
}

impl<D: Device> Default for WedbRegistryPersistence<D> {
  fn default() -> Self {
    Self::new()
  }
}

impl<D: Device> RegistryPersistence for WedbRegistryPersistence<D> {
  fn put(
    &self,
    physical_key: &[u8],
    value: &[u8],
  ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
    // 同步段拷贝负载（登记物理键约数十字节、值 32/20 字节定长）：装箱
    // future 只借用 self，调用点临时键缓冲不随 future 跨 await 悬垂
    let physical_key = physical_key.to_vec();
    let value = value.to_vec();
    // 同步调用点取会话引用（未绑定/错绑即按失败口径报错，不静默改用
    // 他人会话）；引用经通行证随本 future 跨冷区 `.await` 自持，调用方
    // 绑定守卫无须存活到 await
    let Some(SessionRef(session)) = with_active_vector_session(SessionRef::<D>) else {
      super::vector_store_callbacks::report_missing_session("registry put");
      return Box::pin(ready(false));
    };
    Box::pin(AssertSessionSend(async move {
      match session.try_upsert_raw_sync(&physical_key, &value) {
        Ok(Ok(_)) => true,
        // 冷区键同步臂不接：`.await` 引擎异步追加闭环（对齐向量存储回调
        // write 臂；契约见 trait 注释的异步化论证）
        _ => {
          let ok = session.upsert_raw(&physical_key, &value).await.is_ok();
          if !ok {
            log::error!("向量登记旁路记录写透失败: {physical_key:?}");
          }
          ok
        }
      }
    }))
  }

  fn remove(&self, physical_key: &[u8]) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
    let physical_key = physical_key.to_vec();
    let Some(SessionRef(session)) = with_active_vector_session(SessionRef::<D>) else {
      super::vector_store_callbacks::report_missing_session("registry remove");
      log::error!("向量登记旁路记录摘除无会话: {physical_key:?}");
      return Box::pin(ready(false));
    };
    Box::pin(AssertSessionSend(async move {
      // 快路径同步墓碑优先（冷区页翻转 / 冷数据确认才走异步臂）
      match session.try_delete_raw_sync(&physical_key) {
        Ok(Ok(_)) => true,
        _ => {
          let ok = session.delete_raw(&physical_key).await.is_ok();
          if !ok {
            log::error!("向量登记旁路记录摘除失败: {physical_key:?}");
          }
          ok
        }
      }
    }))
  }
}
