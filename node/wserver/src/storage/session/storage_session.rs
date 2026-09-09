//! 存储会话（对标 C# `sealed partial class StorageSession`，libs/server/Storage/Session/StorageSession.cs）
//!
//! C# 侧 StorageSession 按 MainStore/ObjectStore/UnifiedStore 拆为多个 partial 文件；
//! Rust 侧以单一结构体 + 跨文件 `impl` 块承担同等职责。底层统一走 wkv
//! `BatchStoreSession`（纪元守卫持有者），同步快路径未闭环时降级 wkv 异步路径，
//! 对标 C# BasicContext 内部 CompletePending 语义。

use std::sync::atomic::{AtomicU8, AtomicU64, Ordering::Relaxed};

use gxhash::HashMap as GxHashMap;
use parking_lot::Mutex;
use wdev::Device;
use wkv::{BatchStoreSession, TtlOpt};

/// WATCH 写入版本代理说明：
/// C# 侧 WATCH 依赖 `WatchVersionMap`（libs/server/Transaction/WatchVersionMap.cs）
/// 提供"键 -> 写入版本"映射；Rust 侧 wkv 未暴露按键版本入口，本域以 wkv 写日志
/// 尾地址（tail_address）作为单调版本代理——无关写入也会推进尾地址，属保守
/// 过估（只会多中止事务，不会漏检冲突），符合 Redis WATCH 语义的安全方向。
type WatchVersion = u64;

/// 键空间类型（libs/server/Cluster/StoreType.cs:StoreType）
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u8)]
pub enum StoreType {
  /// 未指定
  None = 0,
  /// 主存（原始字符串）
  Main = 1,
  /// 对象存（数据结构）
  Object = 2,
  /// 全部存储
  All = 3,
}

/// 存储会话：wserver 执行存储操作的内部层
pub struct StorageSession<'a, D: Device> {
  /// 底层批处理会话（对标 C# stringBasicContext/objectBasicContext 共用的底层会话）
  pub batch: BatchStoreSession<'a, D>,
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
  /// WATCH 登记表：用户键 -> 监视时的写日志尾地址
  watch_versions: Mutex<GxHashMap<Vec<u8>, WatchVersion>>,
}

impl<'a, D: Device> StorageSession<'a, D> {
  /// 基于底层批处理会话创建存储会话
  pub fn new(batch: BatchStoreSession<'a, D>) -> Self {
    Self {
      batch,
      session_found: AtomicU64::new(0),
      session_notfound: AtomicU64::new(0),
      session_pending: AtomicU64::new(0),
      pending_start_ms: AtomicU64::new(0),
      pending_total_ms: AtomicU64::new(0),
      resp_version: AtomicU8::new(2),
      watch_versions: Mutex::new(GxHashMap::default()),
    }
  }

  /// 当前 RESP 协议版本
  #[inline]
  pub fn resp_protocol_version(&self) -> u8 {
    self.resp_version.load(Relaxed)
  }

  /// 记录 WATCH：登记键在监视时刻的写日志尾地址作为版本代理
  pub fn watch_key(&self, key: &[u8]) {
    let version = self.batch.store.tail_address();
    self.watch_versions.lock().insert(key.to_vec(), version);
  }

  /// 查询键的 WATCH 登记版本（未登记返回 None）
  pub fn watched_version(&self, key: &[u8]) -> Option<WatchVersion> {
    self.watch_versions.lock().get(key).copied()
  }

  /// 清空本会话的 WATCH 登记（对标 EXEC/DISCARD/UNWATCH 后的版本表释放）
  pub fn clear_watches(&self) {
    self.watch_versions.lock().clear();
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
    match self.batch.try_read_sync(key, &f)? {
      Some(r) => {
        self.record_read_outcome(r.is_some());
        Ok(r)
      }
      None => {
        self.session_pending.fetch_add(1, Relaxed);
        let r = self.batch.read_with(key, f).await?;
        self.record_read_outcome(r.is_some());
        Ok(r)
      }
    }
  }

  /// 读字符串键值（拷贝版）
  pub async fn read_string(&self, key: &[u8]) -> wkv::Result<Option<Vec<u8>>> {
    self.read_string_with(key, |v| v.to_vec()).await
  }

  /// 写字符串键值（SET 语义：同步快路径优先，环形缓冲翻转 / TTL 清除异步闭环）
  pub async fn upsert_string(&self, key: &[u8], val: &[u8]) -> wkv::Result<()> {
    match self.batch.try_upsert_sync(key, val)? {
      Ok(_) => Ok(()),
      // 降级 wkv 异步闭环（BatchStoreSession 官方封装，等价于退出批处理纪元后重写）
      Err(_) => self.batch.upsert(key, val).await.map(|_| ()),
    }
  }

  /// 删除键（同步快路径优先，磁盘异步闭环）
  pub async fn delete_string(&self, key: &[u8]) -> wkv::Result<bool> {
    match self.batch.try_delete_sync(key)? {
      Ok(deleted) => Ok(deleted),
      Err(_) => self.batch.delete(key).await,
    }
  }

  /// 设置键级绝对过期时间（毫秒 Unix 时间戳，对标 SessionFunctionsUtils TTL 记录写入）
  pub async fn expire_at_ms(&self, key: &[u8], expire_at_ms: u64) -> wkv::Result<i32> {
    self.batch.expire_at(key, expire_at_ms, TtlOpt::NONE).await
  }

  /// 以相对毫秒设置过期（内部换算绝对时间戳）
  pub async fn expire_in_ms(&self, key: &[u8], ttl_ms: u64) -> wkv::Result<i32> {
    let now = coarsetime::Clock::now_since_epoch().as_millis();
    self.expire_at_ms(key, now.saturating_add(ttl_ms)).await
  }

  /// 移除键级 TTL（对标 PERSIST）
  pub async fn persist_key(&self, key: &[u8]) -> wkv::Result<i32> {
    self.batch.persist(key).await
  }

  /// 查询键剩余生存毫秒（无 TTL 记录返回 -1，键不存在返回 -2）
  pub async fn pttl_ms(&self, key: &[u8]) -> wkv::Result<i64> {
    self.batch.pttl_ms(key).await
  }

  /// 查询键绝对过期时间戳（毫秒，语义同 pttl）
  pub async fn expiretime_ms(&self, key: &[u8]) -> wkv::Result<i64> {
    self.batch.expiretime_ms(key).await
  }

  /// 命中/未命中计数收纳点
  #[inline]
  fn record_read_outcome(&self, found: bool) {
    if found {
      self.session_found.fetch_add(1, Relaxed);
    } else {
      self.session_notfound.fetch_add(1, Relaxed);
    }
  }
}

impl<'a, D: Device> StorageSession<'a, D> {
  /// 更新会话的 RESP 协议版本（影响响应编码格式）
  ///
  /// libs/server/Storage/Session/StorageSession.cs:UpdateRespProtocolVersion
  pub fn update_resp_protocol_version(&self, resp_protocol_version: u8) {
    self.resp_version.store(resp_protocol_version, Relaxed);
  }
}
