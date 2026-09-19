//! 冷租户与冷库点查装载（doc/zh/db.md「冷租户按需加载与零全局常驻内存」）
//!
//! 映射权威在磁盘 KeyTag::DbMeta：内存未命中先点查磁盘既有映射装载回建
//! （绝不换号），点查未命中才分配新号并原子持久化——杜绝「冷租户一经访问
//! 即盲分配新号、旧域数据判死丢失」。同步域内严禁 await 磁盘，本模块即
//! 同步原语（[`crate::vdb::VirtualDbManager`] 内存面）与磁盘权威之间的
//! 异步会话解析单点；严格会话（RESP 连接）由协议层挂起本入口后重放上下文。

use std::sync::Arc;

use wdev::Device;
use wval::{KeyTag, TaggedKeyBuf};

use super::WedbStore;
use crate::{
  error::Result,
  session::StoreSession,
  vdb::{DbMetaRecord, ROOT_DBMETA_PREFIX},
};

impl<D: Device> WedbStore<D> {
  /// 构造根域前缀下的 DbMeta 完整物理键（系统元数据固定落 (ns 0, db 0) 前缀；
  /// 供 `read_raw_with` 按完整键点查，写侧走 persist_dbmeta 载荷口径）
  fn dbmeta_key(payload: &[u8]) -> TaggedKeyBuf {
    StoreSession::<D>::session_tag_key_with_prefix(
      ROOT_DBMETA_PREFIX.as_slice(),
      KeyTag::DbMeta,
      payload,
    )
  }

  /// 点查磁盘 NS_MAP 记录：[0x01][logic_ns: 8B be] -> [vns: 8B be]
  async fn probe_ns_mapping(
    &self,
    session: &StoreSession<D>,
    logic_ns: u64,
  ) -> Result<Option<u64>> {
    let key = Self::dbmeta_key(DbMetaRecord::key_ns_map(logic_ns).as_slice());
    Ok(
      session
        .read_raw_with(&key, DbMetaRecord::decode_value)
        .await?
        .flatten(),
    )
  }

  /// 点查磁盘 DB_MAP 记录：[0x02][vns: 8B be][logic_db: 8B be] -> [vdb: 8B be]
  async fn probe_db_mapping(
    &self,
    session: &StoreSession<D>,
    vns: u64,
    logic_db: u64,
  ) -> Result<Option<u64>> {
    let key = Self::dbmeta_key(DbMetaRecord::key_db_map(vns, logic_db).as_slice());
    Ok(
      session
        .read_raw_with(&key, DbMetaRecord::decode_value)
        .await?
        .flatten(),
    )
  }

  /// 解析逻辑命名空间映射：内存命中直返，未命中先点查磁盘装载既有映射
  /// （退役旧号防复活：命中且未判死方装载），未命中才分配新号并持久化
  async fn resolve_ns(&self, session: &StoreSession<D>, logic_ns: u64) -> Result<u64> {
    if let Some(&vns) = self.vdb.ns_map.pin().get(&logic_ns) {
      return Ok(vns);
    }
    if let Some(vns) = self.probe_ns_mapping(session, logic_ns).await?
      && !self.vdb.is_dead_ns(vns)
    {
      self.vdb.insert_ns_mapping(logic_ns, vns);
      return Ok(vns);
    }
    let (vns, created) = self.vdb.get_or_create_ns(logic_ns);
    if created {
      // 全新租户（点查未命中即磁盘从无记录）：路由表权威全量
      self.vdb.mark_route_authoritative(vns);
      if logic_ns != 0 {
        // persist_dbmeta 收记录引用（原子批退化单条形态，内部固定根域前缀 +
        // DbMeta 标签），与 probe 读面同键；直传完整键会被双重加缀成
        // probe/rebuild 皆不可见的孤儿记录
        let rec = DbMetaRecord::NsMap { logic_ns, vns };
        session.persist_dbmeta(&rec).await?;
      }
    }
    Ok(vns)
  }

  /// 解析逻辑库映射：内存命中直返，未命中先点查磁盘装载（退役旧号防复活），
  /// 未命中才分配新号并持久化
  async fn resolve_db(
    &self,
    session: &StoreSession<D>,
    logic_ns: u64,
    vns: u64,
    logic_db: u64,
  ) -> Result<u64> {
    if let Some(vdb) = self.vdb.route_vdb_of(vns, logic_db) {
      return Ok(vdb);
    }
    if let Some(vdb) = self.probe_db_mapping(session, vns, logic_db).await?
      && !self.vdb.is_dead_domain(vns, vdb)
    {
      self.vdb.insert_db_mapping(vns, logic_db, vdb);
      return Ok(vdb);
    }
    let (vdb, created) = self.vdb.get_or_create_db(logic_ns, logic_db);
    if created && (logic_ns != 0 || logic_db != 0) {
      // 键载荷口径同 resolve_ns：persist_dbmeta 内部固定根域前缀 + DbMeta 标签
      let rec = DbMetaRecord::DbMap { vns, logic_db, vdb };
      session.persist_dbmeta(&rec).await?;
    }
    Ok(vdb)
  }

  /// 解析 (ns, db) 至物理 (vns, vdb)：内存命中零 I/O 直返；未命中先点查
  /// 磁盘 DbMeta 装载既有映射（不换号），点查未命中才分配新号并原子持久化。
  ///
  /// 严格会话（RESP 连接）上下文切换挂起面：协议层在 `set_context` 报告
  /// 未装载时挂起本调用，完成后重放上下文（磁盘为映射权威，任何路径都
  /// 不得绕过点查直接盲分配）
  pub async fn resolve_context(self: &Arc<Self>, ns: u64, db: u64) -> Result<(u64, u64)> {
    // 快路径：双命中零 I/O 直返
    if let Some(&vns) = self.vdb.ns_map.pin().get(&ns)
      && let Some(vdb) = self.vdb.route_vdb_of(vns, db)
    {
      return Ok((vns, vdb));
    }
    let session = self.vdb_load_session.take(self)?;
    session.set_context(0, 0);
    let out = async {
      let vns = self.resolve_ns(&session, ns).await?;
      let vdb = self.resolve_db(&session, ns, vns, db).await?;
      Ok((vns, vdb))
    }
    .await;
    self.vdb_load_session.restore(session);
    out
  }

  /// 解析逻辑命名空间映射（冷装载面）：内存命中直返，未命中点查磁盘装载
  /// 既有 vns（不换号），未命中分配新号并持久化；`flush_namespace` 换号前
  /// 与副本 FlushNs 回放共用，保证映射权威在磁盘
  pub async fn resolve_ns_mapping(self: &Arc<Self>, ns: u64) -> Result<u64> {
    if let Some(&vns) = self.vdb.ns_map.pin().get(&ns) {
      return Ok(vns);
    }
    let session = self.vdb_load_session.take(self)?;
    session.set_context(0, 0);
    let out = self.resolve_ns(&session, ns).await;
    self.vdb_load_session.restore(session);
    out
  }
}
