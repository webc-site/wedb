//! SWAPDB 跨库交换数据面内核
//!
//! 在 garnet 中的相对路径:libs/server/Databases/MultiDatabaseManager.cs:TrySwapDatabases
//!
//! C# 两库为独立 Tsavorite 实例，SWAPDB 交换 GarnetDatabase 容器指针；
//! rust 基于双层虚拟化数据库架构，通过槽位级单元格路由表（每逻辑库一格
//! `Arc<ArcSwap<u64>>`，doc/zh/db.md 1.4）原子交换虚拟数据库 ID 映射：
//! 1. 两格各自单指令 ArcSwap 换指 virtual_db_id，零物理数据搬移、零整表克隆；
//!    互换全程在换号串行锁内成对完成，持久化面以单条 0x06 成对记录一次
//!    追加上盘即原子，杜绝双库指向同一虚拟 ID 的盘上撕裂中间态；
//! 2. 推进全局 generation 标量，促使活跃连接感知并刷新；
//! 3. 持换号元数据串行锁全程，DbMeta 成对映射原子批落盘（KeyTag::DbMeta
//!    0x0E）：单条 0x06 成对记录先行落地即成一致互换（任意崩溃前缀无撕裂
//!    双指向），两条 0x02 映射随落保冷装载点查同址收敛，断电重启一致性
//!    由 [`crate::store::WedbStore::rebuild_vdb_visit`] 的 0x06 臂承接（恢复链单趟扫描与冷启动重建共用）；
//! 4. 会话门控：活跃会话数 > 1 时按 Garnet 契约拒绝换库，防并发读写竞态。

use wdev::Device;

use crate::{
  error::Result,
  session::StoreSession,
  vdb::{DbMetaRecord, ROOT_VIRTUAL_ID},
};

impl<D: Device> StoreSession<D> {
  /// 交换两库全部用户域数据（SWAPDB 数据面内核）
  ///
  /// O(1) 虚库 ID 单元格互换，彻底消除物理搬移与整表克隆；盘上成对记录
  /// 单条原子，杜绝中间态撕裂。换号编排体全程持
  /// 元数据串行锁（[`crate::store::WedbStore::lock_dbmeta`]，garnet 每库独立实例无对位）：
  /// 消除与并发 flush 换号事务的记录流交错及读旧值竞态。
  pub async fn swap_databases(&self, db_id1: i64, db_id2: i64) -> Result<()> {
    if db_id1 == db_id2 {
      return Ok(());
    }
    let ns = self.namespace();
    let logic_db1 = db_id1.max(0) as u64;
    let logic_db2 = db_id2.max(0) as u64;

    // 冷装载优先：两库既有映射先点查磁盘装载（映射权威在磁盘 DbMeta，
    // 冷租户换库绝不盲分配换号），未命中（全新库）才由解析面创建持久化
    let (vns, _) = self.store.resolve_context(ns, logic_db1).await?;
    self.store.resolve_context(ns, logic_db2).await?;

    // 换号事务全程：锁内单元格 O(1) 互换 → 原子批 persist
    let _dbmeta_guard = self.store.lock_dbmeta().await;

    // 路由快照取用（冷装载已完成，此处恒在册命中；缺席防御性回插空表）
    let routing = self.store.vdb.routing_for(vns);

    // 槽位单元格 O(1) 互换：读双格现指向后各自单格换指——零整表克隆、零 CAS
    // 重试；换号事务全程持元数据串行锁，锁内无并发换号者。互换中任一瞬态下
    // 每库只指向自身换号前/后两个合法域之一（绝不错读他库新域），且随后
    // 代数 bump 令活跃会话刷新收敛
    let (final_vdb1, final_vdb2) = (
      routing.table.get(logic_db1).unwrap_or(ROOT_VIRTUAL_ID),
      routing.table.get(logic_db2).unwrap_or(ROOT_VIRTUAL_ID),
    );
    routing.table.set(logic_db1, final_vdb2);
    routing.table.set(logic_db2, final_vdb1);

    // 推进全局版本代数并刷新当前会话上下文
    self.store.vdb.bump_generation();
    self.set_context(self.namespace(), self.active_db());

    // 成对映射原子批（safe-order [0x06 成对记录, 0x02 映射×2]，doc/zh/db.md
    // 「即时原子提交」段）：单条 0x06 一次追加即原子互换，键载荷携双库、值
    // 携双新指向，重启重建经 0x06 臂成对覆盖生效；其后两条 0x02 记录与成对
    // 记录同值随落，令非根域冷装载点查（仅按 [0x02] 键读最新映射）同样收敛
    // 到互换后指针，绝不静默回退换号
    let swap_rec = DbMetaRecord::DbSwap {
      vns,
      logic_db1,
      logic_db2,
      swapped_db1: final_vdb2,
      swapped_db2: final_vdb1,
    };
    let map_rec1 = DbMetaRecord::DbMap {
      vns,
      logic_db: logic_db1,
      vdb: final_vdb2,
    };
    let map_rec2 = DbMetaRecord::DbMap {
      vns,
      logic_db: logic_db2,
      vdb: final_vdb1,
    };
    self
      .persist_dbmeta_batch(&[Some(swap_rec), Some(map_rec1), Some(map_rec2)])
      .await?;

    Ok(())
  }
}
