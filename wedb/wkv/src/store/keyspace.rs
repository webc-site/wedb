use std::sync::Arc;

use gxhash::{GxBuildHasher, HashSet};
use wbase::time::now_ticks;
use wdev::Device;
use wval::{KeyTag, MetaValue, NamespaceDbCodec};

use super::WedbStore;
use crate::{
  error::Result,
  session::StoreSession,
  ttl::{TTL_VALUE_LEN, TtlProbe},
};

impl<D: Device> WedbStore<D> {
  /// 扫描内存区并物理删除已过期键（对标 Garnet ExpiredKeyDeletionScan / EXPDELSCAN）
  ///
  /// 参数 `db_id`: None 表示默认数据库 0，Some(db) 过滤指定数据库；
  /// 返回 `(num_expired_keys_deleted, total_records_scanned)`。
  pub async fn expired_key_deletion_scan(
    self: &Arc<Self>,
    db_id: Option<u64>,
  ) -> Result<(u64, u64)> {
    let from = self.hlog.read_only_address();
    let until = self.hlog.tail_address();
    let session = self.new_session()?;
    // 过期判定基准：.NET Ticks（与 TTL 记录值同域）
    let now = now_ticks();
    let target_db = db_id.unwrap_or(0);
    let mut scanned = 0u64;
    let mut to_expire: HashSet<(u64, Box<[u8]>)> = HashSet::with_hasher(GxBuildHasher::default());

    self
      .hlog
      .scan(from, until, |_, rec| {
        scanned += 1;
        if rec.is_tombstone() {
          return Ok(true);
        }
        if let Some((ns, db, user_key)) = StoreSession::<D>::user_key_from_ttl_key(rec.key)
          && db == target_db
          && let Ok(be) = <[u8; TTL_VALUE_LEN]>::try_from(rec.value)
          // 到期判定取严格小于（读路径口径，与 probe_ttl/check_expired 一致）
          && i64::from_be_bytes(be) < now
        {
          // 同步内存探针双检初筛：若最新态已不过期（续期）或已删除（墓碑），跳过无谓收集
          session.set_context(ns, db);
          if matches!(session.probe_ttl(user_key, now), TtlProbe::Pass) {
            return Ok(true);
          }
          to_expire.insert((ns, Box::from(user_key)));
        }
        Ok(true)
      })
      .await?;

    let mut deleted = 0u64;
    for (ns, key) in to_expire {
      session.set_context(ns, target_db);
      if session.check_expired(&key).await? {
        deleted += 1;
      }
    }
    Ok((deleted, scanned))
  }

  /// 扫描全日志收集用户面键候选（String / Meta / ObjectEnvelope，去重）
  ///
  /// `scope = Some((ns, db))` 限定单库，`None` 收全部域；返回去重后的
  /// `(ns, db, 用户键)` 三元组（用户键为物理键剥
  /// `[NsVarint][DbVarint][KeyTag]` 前缀原文）。同键多历史版本去重后仅
  /// 保留一份，逐键删除以最新态为准（经哈希索引，免疫历史版本重复）。
  ///
  /// 单一收集内核供 [`Self::flush_database`] / [`Self::flush_all_databases`]
  /// 共用；全区间 `[begin_address, tail_address)` 扫描（含磁盘冷区，
  /// [`HybridLog::scan`] 自动读盘），与 [`Self::keyspace_stats`] 同口径。
  async fn collect_user_keys(
    &self,
    scope: Option<(u64, u64)>,
  ) -> Result<HashSet<(u64, u64, Box<[u8]>)>> {
    let mut keys: HashSet<(u64, u64, Box<[u8]>)> = HashSet::with_hasher(GxBuildHasher::default());
    self
      .hlog
      .scan(
        self.hlog.begin_address(),
        self.hlog.tail_address(),
        |_, rec| {
          if !rec.is_tombstone()
            && let Ok((ns, db, tag, user_key)) = NamespaceDbCodec::decode_tagged_key(rec.key())
            && tag.is_user_visible()
            && scope.is_none_or(|s| s == (ns, db))
          {
            keys.insert((ns, db, Box::from(user_key)));
          }
          Ok(true)
        },
      )
      .await?;
    Ok(keys)
  }

  /// 清空指定数据库全部用户域数据，返回删除键数
  ///
  /// 在 garnet 中的相对路径:libs/server/StoreWrapper.cs:FlushDatabase
  ///
  /// C# 每库独立 Tsavorite 实例，FlushDatabase 即
  /// `db.Store.Log.ShiftBeginAddress(db.Store.Log.TailAddress)` 整段截断
  /// （libs/server/Databases/DatabaseManagerBase.cs:FlushDatabase）；rust
  /// 共享单日志以 `[NsVarint][DbVarint]` 前缀物理隔离各库，无法按库截断，
  /// 等价实现为域扫描收集 + 逐键完整删除（[`StoreSession::delete`]：随键
  /// TTL/ETag 旁路清理 + 集合 Meta 版本栅栏秒删 + wbftree 树文件排空释放 +
  /// 对象信封双域删除），每键删除各自原子，引擎结构与其它库数据不受影响。
  pub async fn flush_database(self: &Arc<Self>, ns: u64, db_id: u64) -> Result<usize> {
    let session = self.new_session()?;
    session.set_context(ns, db_id);
    let mut deleted = 0usize;
    for (_, _, key) in self.collect_user_keys(Some((ns, db_id))).await? {
      if session.delete(&key).await? {
        deleted += 1;
      }
    }
    Ok(deleted)
  }

  /// 清空全部数据库用户域数据，返回删除键数
  ///
  /// 在 garnet 中的相对路径:libs/server/Databases/MultiDatabaseManager.cs:FlushAllDatabases
  ///
  /// C# 逐活跃库 FlushDatabase（每库独立 Tsavorite 整段截断）的共享存储面
  /// 等价：单次全量扫描收集全部域的用户面键，逐键切换会话域完整删除。
  pub async fn flush_all_databases(self: &Arc<Self>) -> Result<usize> {
    let session = self.new_session()?;
    let mut deleted = 0usize;
    for (ns, db, key) in self.collect_user_keys(None).await? {
      session.set_context(ns, db);
      if session.delete(&key).await? {
        deleted += 1;
      }
    }
    Ok(deleted)
  }

  /// 统计 keyspace 存活键数量与其中设置 TTL 的数量（对标 Garnet
  /// `StoreWrapper.GetKeyspaceStats` / `INFO KEYSPACE` 命令）
  ///
  /// 返回 `(key_count, expire_count)`：`key_count` 为存活（未过期）用户键数，
  /// 按 `(ns, db, 用户键)` 去重（同一键多个历史版本只计一次）；`expire_count`
  /// 为其中设置了 TTL 记录的存活键数。
  ///
  /// 实现为全日志两阶段扫描（对标 Garnet `KeyspaceStats` 的哈希索引 lookup
  /// 迭代；wedb 的 windex 无按键遍历 API，索引槽位仅承载 (bucket, tag) 链头，
  /// tag 碰撞键须经 prev_address 链回溯才能枚举，退化为乱序版日志扫描，故择优
  /// 顺序扫描）：
  /// 1. 顺序扫描 `[begin_address, tail_address)` 全区间（含磁盘冷区，
  ///    [`HybridLog::scan`] 自动读盘），跳过墓碑与非用户面物理键（仅收
  ///    String/Meta 标签；集合子键与 TTL 旁路记录不作候选），收集去重候选
  ///    `(ns, db, 用户键)`；
  /// 2. 逐候选经会话读路径（哈希索引取最新态，免疫复活导致的地址乱序）复判：
  ///    字符串记录命中或对象信封记录命中，或集合元记录存在且
  ///    size > 0 任一成立视为存活；是否有 TTL 以
  ///    TTL 记录最新版判定（ttl_of）；已过期键两栏均不计。探针为纯读，不触发
  ///    惰性物理清除（区别于 contains_key / check_expired），统计零写副作用。
  ///
  /// 并发防护对标 Garnet `KeyspaceScanLock`：专用扫描会话懒建复用，并发调用
  /// 后到者降级为一次性临时会话（读路径无共享可变状态，无正确性风险）。
  pub async fn keyspace_stats(self: &Arc<Self>) -> Result<(u64, u64)> {
    let session = self.keyspace_scan_session.take(self)?;
    let from = self.hlog.begin_address();
    let until = self.hlog.tail_address();
    // 过期判定基准：.NET Ticks（与 TTL 记录值同域）
    let now = now_ticks();
    let mut candidates: HashSet<(u64, u64, Box<[u8]>)> =
      HashSet::with_hasher(GxBuildHasher::default());

    self
      .hlog
      .scan(from, until, |_, rec| {
        if rec.is_tombstone() {
          return Ok(true);
        }
        if let Ok((ns, db, tag, user_key)) = NamespaceDbCodec::decode_tagged_key(rec.key)
          && matches!(tag, KeyTag::String | KeyTag::Meta | KeyTag::ObjectEnvelope)
        {
          candidates.insert((ns, db, Box::from(user_key)));
        }
        Ok(true)
      })
      .await?;

    let mut key_count = 0u64;
    let mut expire_count = 0u64;
    for (ns, db, key) in &candidates {
      session.set_context(*ns, *db);
      // 存活判定：字符串记录命中、对象信封记录命中，或集合元记录存在且
      // size > 0（幽灵元记录不计）
      let str_k = session.session_string_key(key);
      let alive = session.read_raw_with(&str_k, |_| ()).await?.is_some();
      let alive = if alive {
        true
      } else {
        let env_k = session.session_tag_key(KeyTag::ObjectEnvelope, key);
        if session.read_raw_with(&env_k, |_| ()).await?.is_some() {
          true
        } else {
          let meta_k = session.session_meta_key(key);
          match session.read_raw(&meta_k).await? {
            Some(bytes) => matches!(MetaValue::read_size(&bytes), Ok(size) if size > 0),
            None => false,
          }
        }
      };
      if !alive {
        continue;
      }
      // 是否有 TTL 与是否过期均以 TTL 记录最新版判定；已过期键两栏均不计
      let ttl = session.ttl_of(key).await?;
      if ttl.is_some_and(|exp| exp < now) {
        continue;
      }
      key_count += 1;
      if ttl.is_some() {
        expire_count += 1;
      }
    }

    self.keyspace_scan_session.restore(session);
    Ok((key_count, expire_count))
  }
}
