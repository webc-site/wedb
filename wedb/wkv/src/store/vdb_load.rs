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
  pub(super) async fn probe_ns_mapping(
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
    let probed = self.probe_ns_mapping(session, logic_ns).await?;
    if let Some(vns) = probed.filter(|v| !self.vdb.is_dead_ns(*v)) {
      // 占位式装载（在座即采纳，绝不回卷）：probe await 窗内并发 FLUSHNS
      // 换代或并发冷装载已装映射时采纳在座值，落回取号路径时 created 恒假、
      // 绝不重复持久化
      let winner = self.vdb.adopt_ns_mapping(logic_ns, vns);
      // 装载后判死复核（死值毒格终身不可收敛）：死亡登记落在 probe 与装载
      // 两相邻同步操作之间即判死成立。在座死号只可能是本插值（换代新号恒
      // 活）——ns_map 已被 flush 改指新号则保护在座权威映射只摘死号逆表，
      // 改走取号路径（命中换代现值）；残余窗收窄至装载与复核两相邻同步操
      // 作间（纳秒级），绝不虚称绝对收敛
      if !self.vdb.is_dead_ns(winner) {
        return Ok(winner);
      }
      // 条件摘格（compute 单键原子，仅当格仍指 winner 才摘）：get→remove
      // 两步在换指新值落位于两步之间时会被按键误摘 flush 已换指的权威值，
      // 跌落取号路径分配新号持久化即与落盘 NsMap 批发散
      use wbase::map::MapOperation;
      self
        .vdb
        .ns_map
        .pin()
        .compute(logic_ns, |entry| match entry {
          Some((_, &v)) if v == winner => MapOperation::Remove,
          Some((_, &v)) => MapOperation::Abort(v),
          None => MapOperation::Abort(winner),
        });
      self.vdb.active_vns.pin().remove(&winner);
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
  /// 未命中才分配新号并持久化。返回生效 `(vns, vdb)`——换代窗口内生效 vns
  /// 可能已非入参值，调用方必须采纳回传对，绝不复用入参 vns 拼域对
  async fn resolve_db(
    &self,
    session: &StoreSession<D>,
    logic_ns: u64,
    vns: u64,
    logic_db: u64,
  ) -> Result<(u64, u64)> {
    if let Some(vdb) = self.vdb.route_vdb_of(vns, logic_db) {
      return Ok((vns, vdb));
    }
    let probed = self.probe_db_mapping(session, vns, logic_db).await?;
    if let Some(vdb) = probed.filter(|v| !self.vdb.is_dead_domain(vns, *v)) {
      // 占位式装载（在座即采纳，绝不回卷）：probe await 窗内并发 SWAPDB /
      // 回放换指已提交时采纳在座值——SWAPDB 无死亡登记，死值复核臂对其
      // 不触发，后写覆盖（insert_db_mapping 的 set 语义）会打回已落盘的
      // 换指新值且本运行期无自愈；采纳在座值后继会话自经代数 bump 刷新
      let winner = self.vdb.adopt_db_mapping(vns, logic_db, vdb);
      if !self.vdb.is_dead_domain(vns, winner) {
        return Ok((vns, winner));
      }
      // 插后判死成立：probe await 窗内并发换号已换代（仅单格换指不孤
      // 儿化路由表，死值毒格有人读、终身不可收敛）。持元数据串行闸重探——
      // FLUSHDB/FLUSHNS 全事务（换指 → record_dead → persist）在闸内原子
      // 完成，重探必读换代后权威值；约束：swap 锁内重导调用本函数时走活
      // 值臂不触此闸（其 probe 已读换代后盘面，判死恒假）
      let _guard = self.lock_dbmeta().await;
      match self.probe_db_mapping(session, vns, logic_db).await {
        // FLUSHDB 形：换代事务改写本格盘上权威值，原位覆盖收口
        Ok(Some(correct)) if !self.vdb.is_dead_domain(vns, correct) => {
          self.vdb.insert_db_mapping(vns, logic_db, correct);
          return Ok((vns, correct));
        }
        // FLUSHNS 形或盘面缺席：FLUSHNS 只落 NsMap/墓碑/水位批，不改写旧
        // vns 的盘上 0x02 记录，重探读回即退役死值——先撤本插毒格（仅当
        // 仍指装载值 winner，第三方改指则保留在座权威值），跌落取/建收口
        Ok(_) => {
          self.vdb.remove_db_mapping_if(vns, logic_db, winner);
        }
        // 重探硬错误：闸内换代已终态，摘格前必判死——提交则墓碑在册（死值
        // 毒格滞留会被无判死门快路径采纳），撤本插毒格后上抛；persist 硬
        // 失败的回滚补偿（rollback_flush_db 销账复活）则格已还原为活值，
        // 无条件摘格会误删权威活格（权威路由表面 is_cold_db 判假，后续同步
        // 取建以新号覆盖盘面即孤儿化复活域），保留格只上抛
        Err(e) => {
          if self.vdb.is_dead_domain(vns, winner) {
            self.vdb.remove_db_mapping_if(vns, logic_db, winner);
          }
          return Err(e);
        }
      }
    }
    // 取/建收口：生效 vns 取 ns_map 现值（FLUSHNS 窗口内入参旧 vns 已退役
    // 换代，落盘 DbMap 键携退役旧号即孤儿记录——重启冷装载点查
    // [0x02][现号] 必落空，新域数据随孤儿号蒸发；生效 vns 回传调用方，
    // 绝不容 (旧 vns, 新 vdb) 错配对钉死会话上下文）；常态路径现值即入参
    // 值，逐值同旧形
    let (vns, _) = self.vdb.get_or_create_ns(logic_ns);
    let (vdb, created) = self.vdb.get_or_create_db_for_vns(vns, logic_db);
    if created && (logic_ns != 0 || logic_db != 0) {
      // 键载荷口径同 resolve_ns：persist_dbmeta 内部固定根域前缀 + DbMeta 标签
      let rec = DbMetaRecord::DbMap { vns, logic_db, vdb };
      session.persist_dbmeta(&rec).await?;
    }
    Ok((vns, vdb))
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
      // 生效域对以 resolve_db 回传为准（换代窗口内生效 vns 可能已换代，
      // 见其文档），绝不复用 resolve_ns 返回值拼对
      self.resolve_db(&session, ns, vns, db).await
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
  /// 内逐格点查既有 0x02 记录，命中即经 [`crate::vdb::VirtualDbManager::insert_db_mapping`]
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
  /// 完整枚举成功才落 [`crate::vdb::VirtualDbManager::mark_route_authoritative`]：枚举覆盖全部
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
      // probe await 窗收口（同 resolve_ns）：锁内复核判死再回插 + 落权威——
      // 窗内 FLUSHNS/FLUSHDB 退役本 vns 即整批跳过（权威标记也不落死号）
      let _guard = self.lock_dbmeta().await;
      if self.vdb.is_dead_ns(vns) {
        return Ok(());
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
  /// 换号退役域经 [`crate::vdb::VirtualDbManager::is_dead_domain`] 剔除——产出即本节点
  /// 当前持有映射的完整域清单（含仅存向量集登记、数据已被清空的空域）。
  ///
  /// 消费面（无盘全量同步快照扇出）以「窗口外预热 + 窗口内重取」两连调使用：
  /// 首调承担全部磁盘 I/O 把冷路由装载为权威，窗口内二调纯内存命中——窗口
  /// 内绝不磁盘等待。映射权威为磁盘 DbMeta，全新租户路由天然权威全量
  /// （[`crate::vdb::VirtualDbManager::mark_route_authoritative`]），窗口内新建租户无须
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
