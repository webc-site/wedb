//! ACL 用户规则底层存储访问层（对标 doc/zh/db.md §3）
//!
//! 物理键：`[NsVarint] + [DbVarint: 0] + [KeyTag::Acl: 0x0D] + [username]`，
//! 值：用户规则的 bitcode 紧凑编码字节（`User::to_bytes`，结构化承载口令哈希、
//! 命令分类与白名单位图，可经 `User::from_bytes` 直构还原；非文本行格式）。
//!
//! ACL 记录恒定落于 db 0，与会话活跃库无关；故所有读写显式以
//! `SessionPrefixBuf::new(ns, 0)` 前缀定位，不经会话前缀（会话前缀随 SELECT
//! 漂移，会误落他库）。存储引擎为 ACL 唯一真实数据源，无全局用户字典。
//!
//! 同步分派段的冷记录落盘回读与流式扫描一律经 [`blocking_wait`] 单点收割（全仓
//! 唯一同步收割口，本文件不自建驱动器）。

use wbase::future::blocking_wait;
use wdev::Device;
use wkv::{StoreResult, StoreSession};
use wval::{KeyTag, NamespaceDbCodec, SessionPrefixBuf, TaggedKeyBuf};

use crate::storage::session::common::array_key_iteration_functions::scan_err;

/// ACL 记录固定库位（db 0）
pub(crate) const ACL_DB: u64 = 0;

/// ACL 用户规则存储访问句柄（绑定单个 `StoreSession`）
pub struct AclStore<'a, D: Device> {
  session: &'a StoreSession<D>,
}

impl<'a, D: Device> AclStore<'a, D> {
  /// 绑定存储会话（会话与存储执行域同一实例，共享底层 `WedbStore`）
  pub fn new(session: &'a StoreSession<D>) -> Self {
    Self { session }
  }

  /// 底层存储会话（冷上下文挂起装载面的会话可达出口）
  pub(crate) fn storage(&self) -> &'a StoreSession<D> {
    self.session
  }

  /// 目标命名空间在 db 0 的会话前缀
  #[inline]
  fn prefix(ns: u64) -> SessionPrefixBuf {
    SessionPrefixBuf::new(ns, ACL_DB)
  }

  /// 完整物理键（冷读回退与 AOF 条目同布局）
  #[inline]
  fn physical_key(ns: u64, username: &[u8]) -> TaggedKeyBuf {
    NamespaceDbCodec::encode_tagged_key(ns, ACL_DB, KeyTag::Acl, username)
  }

  /// 同步点查用户规则字节
  ///
  /// 内存直读优先（绝大多数场景纳秒级闭环）；冷记录（RecordOnDisk）降级
  /// 阻塞式落盘回读——批处理纪元守卫在阻塞前释放，绝不持守卫等待驱逐。
  pub fn read(&self, ns: u64, username: &[u8]) -> wkv::Result<Option<Vec<u8>>> {
    let prefix = Self::prefix(ns);
    let batch = self.session.enter_batch();
    let res = batch.session.try_read_tag_sync_unprotected_with_prefix(
      prefix.as_slice(),
      username,
      KeyTag::Acl,
      |v| v.to_vec(),
    );
    drop(batch);
    match res? {
      StoreResult::Success(v) => Ok(Some(v)),
      StoreResult::NotFound => Ok(None),
      StoreResult::RecordOnDisk => {
        let phys = Self::physical_key(ns, username);
        blocking_wait(self.session.read_raw(&phys))
      }
    }
  }

  /// 同步落盘用户规则字节（内存写优先；环形页翻转降级阻塞式异步闭环）
  pub fn write(&self, ns: u64, username: &[u8], value: &[u8]) -> wkv::Result<()> {
    let prefix = Self::prefix(ns);
    let batch = self.session.enter_batch();
    let res = batch.session.try_upsert_tag_sync_unprotected_with_prefix(
      prefix.as_slice(),
      username,
      KeyTag::Acl,
      value,
    );
    drop(batch);
    match res? {
      Ok(_) => Ok(()),
      Err(_) => {
        let phys = Self::physical_key(ns, username);
        blocking_wait(self.session.upsert_raw(&phys, value))?;
        Ok(())
      }
    }
  }

  /// 同步墓碑删除用户规则（返回是否确有删除；环形页翻转降级阻塞式异步闭环）
  pub fn delete(&self, ns: u64, username: &[u8]) -> wkv::Result<bool> {
    let prefix = Self::prefix(ns);
    let batch = self.session.enter_batch();
    let res = batch.session.try_delete_tag_sync_unprotected_with_prefix(
      prefix.as_slice(),
      username,
      KeyTag::Acl,
    );
    drop(batch);
    match res? {
      Ok(deleted) => Ok(deleted),
      Err(_) => {
        let phys = Self::physical_key(ns, username);
        blocking_wait(self.session.delete_raw(&phys))
      }
    }
  }

  /// 流式扫描指定命名空间的全部存活 ACL 记录
  ///
  /// 逐条回调 `(用户名, 规则字节)`，回调返回 `false` 提前终止。不全量装载：
  /// 调用方在回调内自取所需。扫描区间为调用时刻的 `[begin, tail)`，起扫后追加
  /// 的记录落在区间外，故本遍所见即该时刻起的一份可见集快照。同键多版本经索引
  /// 链首地址校验去重（仅最新版本可入），墓碑与链尾旧版本一律跳过——与
  /// `array_key_iteration_functions::scan_cursor` 同口径。扫描期内被并发更新的
  /// 键，其区间内旧版本因链首已不指向本条而落选，等价于 C# 侧
  /// `ConcurrentDictionary` 的弱一致枚举窗口。
  pub async fn for_each_user<F>(&self, ns: u64, mut on_user: F) -> wkv::Result<()>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    let prefix = Self::prefix(ns);
    let prefix_slice = prefix.as_slice();
    let store = &self.session.store;
    let begin = store.begin_address();
    let end = store.tail_address();
    store
      .hlog()
      .scan(begin, end, |addr, rec| {
        let key = rec.key();
        let Some(user_key) =
          NamespaceDbCodec::strip_session_prefix_with_tag(key, prefix_slice, KeyTag::Acl)
        else {
          return Ok(true);
        };
        // 链首地址校验：仅该用户当前最新版本可入（旧版本/墓碑链一律跳过）
        if store.index.load().find_tag(key) != Some(addr) {
          return Ok(true);
        }
        if rec.is_tombstone() {
          return Ok(true);
        }
        Ok(on_user(user_key, rec.value()))
      })
      .await
      .map_err(scan_err)?;
    Ok(())
  }

  /// [`Self::for_each_user`] 的同步分派段形态（ACL LIST/USERS 在命令分派
  /// 段闭环，不经慢路径通道）：应答需先写数组长度，故调用方以本内核**单遍**
  /// 收成小快照（用户名 / 已渲染正文），再由同一快照写符头与正文，不设第二套
  /// 扫描通道、也不重扫两遍
  pub fn for_each_user_blocking<F>(&self, ns: u64, on_user: F) -> wkv::Result<()>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    blocking_wait(self.for_each_user(ns, on_user))
  }
}
