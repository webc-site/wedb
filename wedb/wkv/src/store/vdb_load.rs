//! 冷租户与冷库点查装载（doc/zh/db.md「冷租户按需加载与零全局常驻内存」）
//!
//! 映射权威在磁盘 KeyTag::DbMeta：内存未命中先点查磁盘既有映射装载回建
//! （绝不换号），点查未命中才分配新号并原子持久化——杜绝「冷租户一经访问
//! 即盲分配新号、旧域数据判死丢失」。同步域内严禁 await 磁盘，本模块即
//! 同步原语（[`crate::vdb::VirtualDbManager`] 内存面）与磁盘权威之间的
//! 异步会话解析单点；严格会话（RESP 连接）由协议层挂起本入口后重放上下文。

use std::sync::Arc;

use wbase::cfg::MAX_DATABASES_MAX;
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
  pub async fn probe_ns_mapping(
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
  pub async fn probe_db_mapping(
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

  /// 冷租户库级路由快照点查回建（物理域 → 逻辑域反查的前置装载单点）
  ///
  /// 冷租户条款下非根域库级路由表在启动 / 检查点恢复时**刻意不装载**
  /// （[`crate::store::WedbStore::rebuild_apply_record`] 的 DbMap / DbSwap 臂仅对
  /// 根域 immortal 常驻执行），而早于恢复基线的 `KeyTag::DbMeta` 镜像条目又被 AOF
  /// 版本闸挡在应用面之外（`wnode/aof/record_gate.rs:should_skip_record` 先于
  /// `replay_op` 的 DbMeta 分派返回），副本检查点基线后重启即处于「映射权威在磁盘、
  /// 内存快照为空」形态。库级定槽按逻辑域现算（`doc/zh/db.md` 4.1），回放面按条目
  /// 物理域反查逻辑域在此形态下必无格可查——本入口即该缺口的显式承接。
  ///
  /// 工序：以磁盘 DbMeta 为权威，在**协议层逻辑库地址空间** `0..MAX_DATABASES_MAX`
  /// 内逐格点查既有 0x02 记录，命中即经 [`VirtualDbManager::insert_db_mapping`]
  /// 装载在册格（与 [`Self::resolve_db`] 同一装载原语与同一判死门
  /// `is_dead_domain`），全程零取号、零落盘——回放面绝不本地二次映射
  /// （`doc/zh/db.md`「从库完全继承主库的映射体系」）。
  ///
  /// 枚举空间的完备性论证（上界单点，非拍脑袋常量）：0x02 记录的 logic_db 侧
  /// 只能由会话上下文产生，而切库唯一入口
  /// `wnode/src/resp/resp_server_session.rs:try_switch_active_database_session`
  /// 以 `db_id >= self.max_databases` 拦截，`max_databases` 又由
  /// `wconf/src/node_options.rs:NodeArgs::validate` 收口在
  /// `wbase::cfg::MAX_DATABASES_MIN..=MAX_DATABASES_MAX`——故一切合法在册记录的
  /// logic_db 恒落于此协议绝对上界内，按上界枚举即覆盖全部格。该上下界一处定义于
  /// `wbase::cfg` 基座：上层配置库的启动定界与本存储层的回建枚举共用同一判据，
  /// 存储层不因此反向依赖配置层，本处亦不另立第二套上界定义。
  ///
  /// 全量格探针（现上界 256）成本可忽略：未在册库号在
  /// `wkv/src/session/raw/read.rs:read_raw_with_reader` 的
  /// `read_probe` + `drive_mem_read` 同步环即判为不存在（Done(None)），零日志读、
  /// 零 await 落地。
  ///
  /// 完整枚举成功才落 [`VirtualDbManager::mark_route_authoritative`]：枚举覆盖全部
  /// 在册库号，此后该租户快照即本运行期权威全量，同一租户至多发生一趟回建，
  /// 后续反查与切库解析一律内存命中；点查硬错误上抛时不落标记，半途态不被误判为
  /// 全量。根域与本机新建租户（`resolve_ns` / `set_context` 首映射）已具权威，
  /// 恒零 I/O 直返。
  pub async fn load_routes_of_vns(self: &Arc<Self>, vns: u64) -> Result<()> {
    if self.vdb.is_route_authoritative(vns) {
      return Ok(());
    }
    let session = self.vdb_load_session.take(self)?;
    session.set_context(0, 0);
    let out = async {
      let space = u64::try_from(MAX_DATABASES_MAX).expect("库号上界为定值正常量，升位无损");
      let mut hit: Vec<(u64, u64)> = Vec::new();
      for logic_db in 0..space {
        if let Some(vdb) = self.probe_db_mapping(&session, vns, logic_db).await? {
          hit.push((logic_db, vdb));
        }
      }
      for (logic_db, vdb) in hit {
        if !self.vdb.is_dead_domain(vns, vdb) {
          self.vdb.insert_db_mapping(vns, logic_db, vdb);
        }
      }
      self.vdb.mark_route_authoritative(vns);
      Ok(())
    }
    .await;
    self.vdb_load_session.restore(session);
    out
  }

  /// 全量活跃域枚举（跨域扫描原语）：在册租户 × 在册库逐域展开，冷租户先经
  /// [`Self::load_routes_of_vns`] 磁盘回建路由（幂等，已权威零 I/O 直返），
  /// 换号退役域经 [`VirtualDbManager::is_dead_domain`] 剔除——产出即本节点
  /// 当前持有映射的完整域清单（含仅存向量集登记、数据已被清空的空域）。
  ///
  /// 消费面（无盘全量同步快照扇出）以「窗口外预热 + 窗口内重取」两连调使用：
  /// 首调承担全部磁盘 I/O 把冷路由装载为权威，窗口内二调纯内存命中——窗口
  /// 内绝不磁盘等待。映射权威为磁盘 DbMeta，全新租户路由天然权威全量
  /// （[`VirtualDbManager::mark_route_authoritative`]），窗口内新建租户无须
  /// 回建即可枚举
  pub async fn active_domains(self: &Arc<Self>) -> Result<Vec<ActiveDomain>> {
    let tenants: Vec<(u64, u64)> = self
      .vdb
      .active_vns
      .pin()
      .iter()
      .map(|(&vns, &logic_ns)| (vns, logic_ns))
      .collect();
    let mut domains = Vec::new();
    for (vns, ns) in tenants {
      self.load_routes_of_vns(vns).await?;
      for (vdb, db) in self.vdb.registered_dbs(vns) {
        if self.vdb.is_dead_domain(vns, vdb) {
          continue;
        }
        domains.push(ActiveDomain { vns, vdb, ns, db });
      }
    }
    Ok(domains)
  }
}

/// 单个活跃域：物理域 `(vns, vdb)` 与逻辑域 `(ns, db)` 域对
///
/// 物理域供直设会话上下文（[`crate::StoreSession::set_virtual_context`]，
/// 从库完全继承主库映射体系），逻辑域供库级定槽（doc/zh/db.md 4.1
/// `slot_of(ns, db)`）与逻辑上下文切换
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ActiveDomain {
  /// 物理命名空间号
  pub vns: u64,
  /// 物理库号
  pub vdb: u64,
  /// 逻辑命名空间号
  pub ns: u64,
  /// 逻辑库号
  pub db: u64,
}
