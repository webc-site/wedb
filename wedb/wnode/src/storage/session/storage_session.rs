//! 存储会话（对标 C# `sealed partial class StorageSession`，libs/server/Storage/Session/StorageSession.cs）
//!
//! C# 侧 StorageSession 按 MainStore/ObjectStore/UnifiedStore 拆为多个 partial 文件；
//! Rust 侧以单一结构体 + 跨文件 `impl` 块承担同等职责。底层统一走 wkv
//! `BatchStoreSession`（纪元守卫持有者），同步快路径未闭环时降级 wkv 异步路径，
//! 对标 C# BasicContext 内部 CompletePending 语义。

use std::{
  future::Future,
  sync::{
    Arc,
    atomic::{AtomicU8, AtomicU64, Ordering::Relaxed},
  },
};

use gxhash::HashMap as GxHashMap;
use parking_lot::Mutex;
use wbase::time::now_ticks;
use wdev::Device;
use wkv::{BatchStoreSession, ConsistentReadContext, ConsistentReadFunctions, TtlOpt};
use wtxn::{TxnKeyEntryComparison, WatchVersionMap};

/// WATCH 版本读数（监视时刻 [`WatchVersionMap`] 桶计数快照）
type WatchVersion = u64;

/// 单键 WATCH 登记：键哈希 + 监视时刻版本（对标 C# WatchedKeySlice 的 hash/version 对）
#[derive(Debug, Clone, Copy)]
struct WatchedKey {
  /// 键哈希（与 [`TxnKeyEntryComparison::key_hash`] 同面，写方推进同桶）
  hash: u64,
  /// 监视时刻的版本表读数
  version: WatchVersion,
}

pub use crate::types::StoreType;

/// 存储会话：wserver 执行存储操作的内部层
pub struct StorageSession<'a, D: Device, CR: ConsistentReadFunctions = ()> {
  /// 底层批处理会话（对标 C# stringBasicContext/objectBasicContext 共用的底层会话）
  pub batch: BatchStoreSession<'a, D>,
  /// WATCH 版本表（C# FunctionsState.watchVersionMap 的会话侧可达面；与
  /// 所属 [`crate::databases::garnet_database::GarnetDatabase`] 同实例共享）
  version_map: Arc<WatchVersionMap>,
  /// 会话命中计数
  pub session_found: AtomicU64,
  /// 会话未命中计数
  pub session_notfound: AtomicU64,
  /// 会话 pending（异步闭环）计数
  pub session_pending: AtomicU64,
  /// pending 起始毫秒时间戳（0 表示未在计时）
  pub(super) pending_start_ms: AtomicU64,
  /// pending 累计等待毫秒
  pub pending_total_ms: AtomicU64,
  /// RESP 协议版本
  resp_version: AtomicU8,
  /// WATCH 登记表：用户键 -> (键哈希, 监视时刻版本)
  watch_versions: Mutex<GxHashMap<Box<[u8]>, WatchedKey>>,
  /// 副本一致读状态机（对标 C# StorageSession.readSessionState）
  pub read_session_state: Option<Arc<CR>>,
}

impl<'a, D: Device> StorageSession<'a, D, ()> {
  /// 基于底层批处理会话创建存储会话（版本表与所属库共享同实例）
  pub fn new(batch: BatchStoreSession<'a, D>, version_map: Arc<WatchVersionMap>) -> Self {
    Self::new_with_read_session(batch, version_map, None)
  }
}

impl<'a, D: Device, CR: ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// 基于底层批处理会话与一致读状态机创建存储会话
  pub fn new_with_read_session(
    batch: BatchStoreSession<'a, D>,
    version_map: Arc<WatchVersionMap>,
    state: Option<Arc<CR>>,
  ) -> Self {
    Self {
      batch,
      version_map,
      session_found: AtomicU64::new(0),
      session_notfound: AtomicU64::new(0),
      session_pending: AtomicU64::new(0),
      pending_start_ms: AtomicU64::new(0),
      pending_total_ms: AtomicU64::new(0),
      resp_version: AtomicU8::new(2),
      watch_versions: Mutex::new(GxHashMap::default()),
      read_session_state: state,
    }
  }

  /// 绑定读一致性状态机（对标 C# StorageSession 构造）
  pub fn with_read_session_state(mut self, state: Option<Arc<CR>>) -> Self {
    self.read_session_state = state;
    self
  }

  /// 设置读一致性状态机
  pub fn set_read_session_state(&mut self, state: Option<Arc<CR>>) {
    self.read_session_state = state;
  }

  /// 是否为一致读会话（对标 C# StorageSession.IsConsistentReadSession）
  #[inline]
  pub fn is_consistent_read_session(&self) -> bool {
    self.read_session_state.is_some()
  }

  /// 获取一致读会话上下文（当且仅当会话启用读一致性时返回 Some，对标 C# StorageSession.consistentReadContext）
  ///
  /// libs/server/Storage/Session/StorageSession.cs:consistentReadContext
  #[inline]
  pub fn consistent_read_context(&self) -> Option<ConsistentReadContext<'_, D, CR>> {
    self
      .read_session_state
      .as_ref()
      .map(|rss| self.batch.consistent_read(rss.as_ref()))
  }

  /// 当前 RESP 协议版本
  #[inline]
  pub fn resp_protocol_version(&self) -> u8 {
    self.resp_version.load(Relaxed)
  }

  /// 记录 WATCH：登记键哈希与监视时刻的版本表读数（对标 C#
  /// WatchedKeysContainer.AddWatch 的 versionMap.ReadVersion 快照）
  pub fn watch_key(&self, key: &[u8]) {
    let mut reg = self.watch_versions.lock();
    if !reg.contains_key(key) {
      let hash = TxnKeyEntryComparison::key_hash(key) as u64;
      let version = self.version_map.read_version(hash);
      reg.insert(key.into(), WatchedKey { hash, version });
    }
  }

  /// 查询键的 WATCH 登记版本（未登记返回 None）
  pub fn watched_version(&self, key: &[u8]) -> Option<WatchVersion> {
    self.watch_versions.lock().get(key).map(|w| w.version)
  }

  /// 校验全部被监视键自登记以来未被任何会话修改（对标 C#
  /// WatchedKeysContainer.ValidateWatchVersion：写面经
  /// `Self::bump_watch_version` 推进同一张版本表，读数不变即干净）
  pub fn validate_watch_version(&self) -> bool {
    let registry = self.watch_versions.lock();
    registry
      .values()
      .all(|w| self.version_map.read_version(w.hash) == w.version)
  }

  /// 清空本会话的 WATCH 登记（对标 EXEC/DISCARD/UNWATCH 后的版本表释放）
  pub fn clear_watches(&self) {
    self.watch_versions.lock().clear();
  }

  /// 推进键的 WATCH 版本（写面挂钩，对标 C# Tsavorite functions 面的
  /// functionsState.watchVersionMap.IncrementVersion：MainStore
  /// UpsertMethods.PostInitialWriter / DeleteMethods.InitialDeleter /
  /// RMWMethods.PostInitialUpdater+InPlaceUpdater 在完成实际写入后调用）
  ///
  /// 挂点为本会话四个写入口（值写 / 删除 / TTL 变更）与 lua 脚本同步写
  /// 路径（storage_scripting_api 的 upsert_sync / delete_sync）；objectstore
  /// 各 ops 经 obj_save / rmw_object_store_operation / finalize_removal 全部
  /// 漏斗至此，RESP 命令层（main_store_ops / bitmap_ops / hyper_log_log_ops
  /// 等）经 upsert_string / delete_string 亦全部覆盖本挂点。
  #[inline]
  pub(crate) fn bump_watch_version(&self, key: &[u8]) {
    self
      .version_map
      .increment_version(TxnKeyEntryComparison::key_hash(key) as u64);
  }

  /// 在会话 pending（异步闭环）统计指标守卫下执行异步操作
  #[inline]
  pub(crate) async fn with_pending_metrics<F, Fut, T>(&self, f: F) -> T
  where
    F: FnOnce() -> Fut,
    Fut: Future<Output = T>,
  {
    self.incr_session_pending();
    self.start_pending_metrics();
    let res = f().await;
    self.stop_pending_metrics();
    res
  }

  /// 读字符串键值（零拷贝闭包版：快路径内存直读，磁盘候选异步闭环）
  ///
  /// 对标 C# StringBasicContext.Read 内部 CompletePending：`Ok(None)`（磁盘候选）
  /// 时降级 wkv 异步 `read_with`，对调用方呈现同步闭环语义。
  pub async fn read_string_with<R>(
    &self,
    key: &[u8],
    f: impl Fn(&[u8]) -> R,
  ) -> wkv::Result<Option<R>> {
    let opt = if let Some(ctx) = self.consistent_read_context() {
      match ctx.try_read_sync_unprotected(key, &f)? {
        Some(r) => r,
        None => self.with_pending_metrics(|| ctx.read_with(key, &f)).await?,
      }
    } else {
      match self.batch.try_read_sync(key, &f)? {
        Some(r) => r,
        None => {
          self
            .with_pending_metrics(|| self.batch.read_with(key, &f))
            .await?
        }
      }
    };
    self.record_read_outcome(opt.is_some());
    Ok(opt)
  }

  /// 读字符串键值（拷贝版）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:GET
  pub async fn read_string(&self, key: &[u8]) -> wkv::Result<Option<Vec<u8>>> {
    self.read_string_with(key, |v| v.to_vec()).await
  }

  /// 写字符串键值（SET 语义：同步快路径优先，环形缓冲翻转 / TTL 清除异步闭环）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:SET
  pub async fn upsert_string(&self, key: &[u8], val: &[u8]) -> wkv::Result<()> {
    match self.batch.try_upsert_sync(key, val)? {
      Ok(_) => {}
      // 降级 wkv 异步闭环（BatchStoreSession 官方封装，等价于退出批处理纪元后重写）
      Err(_) => {
        self
          .with_pending_metrics(|| self.batch.upsert(key, val))
          .await
          .map(|_| ())?;
      }
    }
    // 值写入完成即推进版本（C# PostInitialWriter 对位）
    self.bump_watch_version(key);
    Ok(())
  }

  /// 删除键（同步快路径优先，磁盘异步闭环）
  pub async fn delete_string(&self, key: &[u8]) -> wkv::Result<bool> {
    // C# InitialDeleter 无条件推进版本（缺席键的墓碑追加同样计入，保守方向）
    self.bump_watch_version(key);
    match self.batch.try_delete_sync(key)? {
      Ok(deleted) => Ok(deleted),
      Err(_) => self.with_pending_metrics(|| self.batch.delete(key)).await,
    }
  }

  /// 设置键级绝对过期时间（.NET Ticks，对标 C# EXPIRE 族在 RESP 边界换算为
  /// ticks 后经 UnifiedInput 携带的同域语义；KeyAdminCommands.cs:421-427）
  pub async fn expire_at_ticks(&self, key: &[u8], expire_at_ticks: i64) -> wkv::Result<i32> {
    // >0 = TTL 已写（1）或过期即删（2）：键状态实际变化才推进版本（C# EXPIRE
    // 经 RMW InPlaceUpdater/PostInitialUpdater，未命中不 Incremment 同向）
    let applied = self
      .batch
      .expire_at(key, expire_at_ticks, TtlOpt::NONE)
      .await?;
    if applied > 0 {
      self.bump_watch_version(key);
    }
    Ok(applied)
  }

  /// 以相对时长设置过期（TimeSpan 口径的 ticks；内部换算绝对 ticks）
  pub async fn expire_in_ticks(&self, key: &[u8], ttl_ticks: i64) -> wkv::Result<i32> {
    let expire_at = now_ticks().saturating_add(ttl_ticks);
    self.expire_at_ticks(key, expire_at).await
  }

  /// 移除键级 TTL（对标 PERSIST）
  pub async fn persist_key(&self, key: &[u8]) -> wkv::Result<i32> {
    // 返回 1 = TTL 记录已删（键元数据变化，C# PERSIST 走 RMW 同向计版本）
    let applied = self.batch.persist(key).await?;
    if applied > 0 {
      self.bump_watch_version(key);
    }
    Ok(applied)
  }

  /// 查询键剩余生存毫秒（无 TTL 记录返回 -1，键不存在返回 -2；RESP 出参边界，
  /// 内部 .NET Ticks 经 wkv pttl_ms 换算，见 ConvertUtils.MillisecondsFromDiffUtcNowTicks）
  pub async fn pttl_ms(&self, key: &[u8]) -> wkv::Result<i64> {
    self.batch.pttl_ms(key).await
  }

  /// 查询键绝对过期 Unix 毫秒时间戳（语义同 pttl；内部 ticks 经
  /// `unix_time_in_milliseconds_from_ticks` 换算为出参毫秒）
  pub async fn expiretime_ms(&self, key: &[u8]) -> wkv::Result<i64> {
    self.batch.expiretime_ms(key).await
  }

  /// 命中/未命中计数收纳点
  #[inline]
  pub(crate) fn record_read_outcome(&self, found: bool) {
    if found {
      self.session_found.fetch_add(1, Relaxed);
    } else {
      self.session_notfound.fetch_add(1, Relaxed);
    }
  }
}

impl<'a, D: Device, CR: ConsistentReadFunctions> StorageSession<'a, D, CR> {
  /// 更新会话的 RESP 协议版本（影响响应编码格式）
  ///
  /// libs/server/Storage/Session/StorageSession.cs:UpdateRespProtocolVersion
  pub fn update_resp_protocol_version(&self, resp_protocol_version: u8) {
    self.resp_version.store(resp_protocol_version, Relaxed);
  }
}
