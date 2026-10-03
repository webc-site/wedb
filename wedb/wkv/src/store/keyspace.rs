use std::{
  io,
  sync::{Arc, atomic::Ordering},
};

use wbase::{
  map::{GxBuildHasher, HashMap},
  time::now_ticks,
};
use wdev::Device;
use whlog::Error;
use wval::{
  KeyTag, MetaValue, NamespaceDbCodec, SessionPrefixBuf, TaggedKeyBuf, VectorRegistrySubTag,
};

use super::WedbStore;
use crate::{
  error,
  error::Result,
  gc::{ExpiredKeySet, ScanBudget, collect_expired},
  range_index::range_index_blocking,
  ttl::is_expired,
  vdb::{DbMetaRecord, GcDeadEntry, ROOT_VIRTUAL_ID, reclaim_expired_at},
};

/// 一次换号事务的 DbMeta 落盘载荷形态（[`WedbStore::commit_swap`] 的唯一入参）
///
/// 两形态穷尽 FLUSHDB/FLUSHNS 主库放射的换号收尾，原子批安全顺序
/// `[新映射?, 旧域退役墓碑?, 0x05 分配水位?]`（doc/zh/db.md「即时原子提交」段）
/// 与「水位只在真正取号时抬升」两条不变式因此只有一份：
/// - `Swapped`：既有格换指（已取新号）——新映射 + 旧域墓碑 + 抬升后的水位；
/// - `FirstMap`：库/空间首映射、无旧域可退——新映射 + 水位，无墓碑。
///
/// 副本不本地换号：换号批次（含墓碑与水位）经 KeyTag::DbMeta 镜像条目同步，
/// 回放应用走 [`WedbStore::apply_dbmeta_record`] 单点
enum SwapRecords {
  Swapped {
    map: DbMetaRecord,
    dead: DbMetaRecord,
  },
  FirstMap {
    map: DbMetaRecord,
  },
}

impl<D: Device> WedbStore<D> {
  /// 扫描内存区并物理删除已过期键（对标 Garnet ExpiredKeyDeletionScan / EXPDELSCAN）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ExpiredKeyDeletionScan
  /// libs/server/Databases/IDatabaseManager.cs:ExpiredKeyDeletionScan
  /// libs/server/Databases/SingleDatabaseManager.cs:ExpiredKeyDeletionScan
  /// libs/server/StoreWrapper.cs:ExpiredKeyDeletionScan
  ///（四层 C# 入口——基类 protected 助手与抽象声明、单库 override、
  /// StoreWrapper 按库转发——随共享单存储双轨折叠为本单内核）
  ///
  /// 候选收集走与内置 GC 共享的 [`crate::gc::collect_expired`] 内核（对标 C#
  /// StoreExpiredKeyDeletionScan 单内核双入口：后台 ExpiredKeyDeletionScanTaskAsync
  /// 与本命令共用 ExpiredKeysBase.Reader）；本入口独有全量预算（记录与候选上限
  /// 双 MAX，与 C# ScanCursor 全窗口同口径）、(ns,db) 目标库过滤（缺省 0 库，
  /// 避免跨库越权扫描）与命令路径错误传播语义（区别于后台 GC 的容错删除）。
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
    let mut to_expire = ExpiredKeySet::with_hasher(GxBuildHasher::default());
    let (scanned, ..) = collect_expired(
      &session,
      self,
      from..until,
      now,
      ScanBudget {
        max_records: u64::MAX,
        max_picks: usize::MAX,
      },
      &mut to_expire,
      &|ns, db| self.vdb.matches_logic_db(ns, db, target_db),
    )
    .await?;

    // 收集完成后统一物理删除：逐键双检，走与用户 DEL 完全一致的路径；
    // 错误传播至命令侧应答（后台 GC 同段为 warn 容错，语义差异保留各自入口）
    let mut deleted = 0u64;
    for (ns, db, key) in to_expire {
      // 直设落域显式携版本轨入账逻辑域（to_expire 存的是物理对，活域正查、
      // 死域孤域代位，见 version_domain_of 单点）
      let (lns, ldb) = self.vdb.version_domain_of(ns, db);
      session.set_virtual_context(ns, db, lns, ldb);
      if session.check_expired(&key).await? {
        deleted += 1;
      }
    }
    Ok((deleted, scanned))
  }

  /// 清空指定数据库全部用户域数据，返回 `(vns, Option<旧 vdb>)`（虚拟命名
  /// 空间号与换号前旧虚拟库号；`None` 即 FirstMap——库首映射无旧域可退役）
  ///
  /// 在 garnet 中的相对路径:libs/server/StoreWrapper.cs:FlushDatabase
  ///
  /// C# 每库独立 Tsavorite 实例，FlushDatabase 即
  /// `db.Store.Log.ShiftBeginAddress(db.Store.Log.TailAddress)` 整段截断；
  /// rust 基于双层虚拟化数据库 ID (virtual_db_id) 映射实现 O(1) 秒级换号：
  /// 分配新虚拟 ID 对路由表目标逻辑库单元格单格原子换指（槽位级
  /// `Arc<ArcSwap<u64>>`，零整表克隆零物理扫描），旧 ID 写入 GC 墓碑
  /// （KeyTag::DbMeta 0x0E）；内存换号段耗时 < 1μs，其后的 DbMeta 原子批
  /// 同步落盘不在此承诺内，
  /// 底层数据由后台延时 GC 与 Compactor 物理回收。
  /// 返回值供 AOF 广播面生产 FlushDb 条目（域值与数据条目物理键前缀同域），
  /// 换号事务内原子取号，杜绝调用方前后取号竞态。
  ///
  /// 换号联动树回收：DbMeta 退役墓碑随换号批落盘后按 (vns, 旧 vdb) 取走换号回收旁表键集，
  /// 逐键 detach_tree 同步摘除注册表（杜绝同名重建索引被 IndexExists 拦截的
  /// 卡死与双泄漏）并就地 dispose 引擎（页环占用与摘除点归还的配额同步归零，
  /// 绝不随删除期限滞留整树），数据文件删除移交内置 GC 轮次
  /// （[`Self::drain_bftree_release`]）在后台线程执行——从库回放臂经此
  /// 不 await 任何文件删除，不阻塞复制流水线（doc/zh/db.md
  /// 主从异步屏障）。时序「先持久化换号、安全纪元过后才物理删树」：投递条目
  /// 携带与墓碑同口径的删除期限，期限内 unlink 绝不发生，换号批持久化镜像
  /// 丢失的崩溃回滚不至旧域树已消失（中途崩溃最坏留孤儿文件，启动对账
  /// [`Self::reclaim_dead_domain_bftrees`] 承接），绝不出现路由存活而树已销毁。
  ///
  /// 换号全程持元数据串行锁（[`Self::lock_dbmeta`]，garnet 无对位）：CAS 换号
  /// 与原子批落盘为一个编排体，杜绝并发换号事务的记录流交错出盘上末态发散；
  /// 换号是 <1μs 内存操作 + 批 persist 的复合体，锁只在管理命令面，不触用户
  /// 数据热路径。
  ///
  /// 回收严格按域编排，严禁追加全局 `range_index.clear_all`：其作用域是跨域
  /// 共享的 RangeIndexManager 全局（摘除全部域注册 + 删全部树文件与检查点
  /// 快照），单库清库波及他域在用 RI 树——他域活树文件被删后经惰性激活重建
  /// 空树，索引数据静默清零。
  pub async fn flush_database(self: &Arc<Self>, ns: u64, db_id: u64) -> Result<(u64, Option<u64>)> {
    // 冷装载优先：租户既有映射先点查磁盘装载（映射权威在磁盘 DbMeta，
    // 主库冷租户清库绝不盲分配换号）
    self.resolve_context(ns, db_id).await?;

    // 换号事务全程：锁内 CAS 换号 → 原子批 persist → 树回收投递
    let _dbmeta_guard = self.lock_dbmeta().await;
    let (expired_at, tail_addr) = self.swap_stamp();
    let (vns, new_vdb, old_vdb_opt) = self.vdb.flush_db(ns, db_id, expired_at, tail_addr);

    // 原子批安全顺序 [新映射, 旧域退役墓碑?, 0x05 分配水位]（doc/zh/db.md
    // 「即时原子提交」段）：固定根域前缀经 persist_dbmeta_batch 单点落盘，
    // 同步快路径翻页条目集中异步回放保达，命令应答即含全批持久化承诺；
    // 任意崩溃前缀最坏旧域泄漏，绝不数据复活或撞号（水位收尾抬升兜底）
    let map_rec = DbMetaRecord::DbMap {
      vns,
      logic_db: db_id,
      vdb: new_vdb,
    };
    let commit_res = self
      .commit_swap(match old_vdb_opt {
        Some(old_vdb) => SwapRecords::Swapped {
          map: map_rec,
          dead: DbMetaRecord::GcDeadDb {
            expired_at,
            vns,
            old_vdb,
            tail_address: tail_addr,
          },
        },
        None => SwapRecords::FirstMap { map: map_rec },
      })
      .await;

    if let Err(err) = commit_res {
      self.vdb.rollback_flush_db(vns, db_id, old_vdb_opt);
      return Err(err);
    }

    if let Some(old_vdb) = old_vdb_opt {
      let keys = self.take_bftree_domain(vns, old_vdb);
      self.reclaim_bftree_keys(vns, old_vdb, keys).await;
    }

    // 返回 (vns, Option<换号前旧 vdb>)：None 即 FirstMap（库首映射，无旧域
    // 可退役）——调用面按 None 跳过 Flush 广播条目与登记表回收，哨兵域值
    // 自生产端灭绝（换号条目域载荷唯一合法语义是换号前旧域，doc/zh/db.md
    // 1.3；FirstMap 本无 0x03/0x04 墓碑，清库语义由 DbMeta 镜像批完整承接）
    Ok((vns, old_vdb_opt))
  }

  /// DbMeta 镜像记录回放应用单点（doc/zh/db.md「从库完全继承主库的映射体系，
  /// 不进行本地二次映射」的 AOF 复制流承接面）
  ///
  /// 主库全部 DbMeta 落盘（换号批 / 首映射 / SWAPDB 成对记录）经存储事件镜像
  /// 为 StoreUpsert 条目；回放侧解码出 [`DbMetaRecord`] 后经本入口应用：内存
  /// 装载复用重建内核 [`Self::rebuild_apply_record`]（根域 immortal 全量、非根域
  /// 0 常驻点查回建，与启动重建同一口径），分配水位单调抬升越过记录号（杜绝
  /// 本地取号与主库未来取号撞号），判死变体联动旧域树回收投递，最后经
  /// `persist_dbmeta` 落盘本节点磁盘（幂等覆写；回放全程
  /// `pause_aof_listeners` 抑制再镜像，杜绝自激放大）——副本重启后映射从本节点
  /// 磁盘装载，与 AOF 截断位点解耦。
  ///
  /// 同一条目重复应用幂等：映射/墓碑为同键同载荷覆写，水位只升不降，树回收
  /// 旁表取走后为空（主库恢复重放自己 AOF 尾段即此形态）。
  pub async fn apply_dbmeta_record(self: &Arc<Self>, rec: DbMetaRecord) -> Result<()> {
    let _dbmeta_guard = self.lock_dbmeta().await;
    let vid = self.rebuild_apply_record(rec);
    if vid > 0 {
      self.vdb.bump_watermark(vid + 1);
    }
    // 映射变体补齐非根域在册格的覆盖换指：重建内核从空装载可跳过非根域，
    // 但副本该租户活跃时内存格已在册，主库换号新指必须覆盖（单格幂等 set，
    // 装载原语与重建/点查装载同源），否则换代重解析后仍解析到旧域。
    // 应用即换代：在册会话纪元缓存重解析（换号 / SWAPDB 换指后旧指向失效；
    // 首映射虽无旧会话，多一次原子加无碍）；墓碑与水位不换代
    match rec {
      DbMetaRecord::NsMap { logic_ns, vns } => {
        self.vdb.insert_ns_mapping(logic_ns, vns);
        self.vdb.bump_generation();
      }
      DbMetaRecord::DbMap { vns, logic_db, vdb } => {
        if vns != ROOT_VIRTUAL_ID {
          self.vdb.insert_db_mapping(vns, logic_db, vdb);
        }
        self.vdb.bump_generation();
      }
      DbMetaRecord::DbSwap {
        vns,
        logic_db1,
        logic_db2,
        swapped_db1,
        swapped_db2,
      } => {
        // 死域门（vns 级判死 = gc_dead 既有账本，与点查装载防复活
        // vdb_load.rs 的 is_dead_domain 口径对齐）：已退役租户的滞留 DbSwap
        // 镜像条目留痕跳过，绝不复活死亡域映射重写账本；提前返回同时
        // 跳过本节点落盘臂，死亡域零新写映射
        if vns != ROOT_VIRTUAL_ID && self.vdb.is_dead_ns(vns) {
          log::warn!(
            "apply_dbmeta_record: 死亡租户 vns={vns} 的 DbSwap 条目留痕跳过（退役后滞留镜像）"
          );
          return Ok(());
        }
        if vns != ROOT_VIRTUAL_ID {
          self.vdb.insert_db_mapping(vns, logic_db1, swapped_db1);
          self.vdb.insert_db_mapping(vns, logic_db2, swapped_db2);
        }
        self.vdb.bump_generation();
      }
      _ => {}
    }
    // 判死变体联动：旧物理域的换号回收旁表键集摘除后投待释放队列（与主库
    // flush 放射同一编排，时序「先持久化换号、安全纪元过后才物理删树」）
    match rec {
      DbMetaRecord::GcDeadDb { vns, old_vdb, .. } => {
        let keys = self.take_bftree_domain(vns, old_vdb);
        self.reclaim_bftree_keys(vns, old_vdb, keys).await;
      }
      DbMetaRecord::GcDeadNs { old_vns, .. } => {
        for (dvns, dvdb, keys) in self.take_bftree_domains_of_vns(old_vns) {
          self.reclaim_bftree_keys(dvns, dvdb, keys).await;
        }
      }
      _ => {}
    }
    self.new_session()?.persist_dbmeta(&rec).await
  }

  /// DbMeta 墓碑注销镜像回放单点（GC 退役完成的 StoreDelete 条目应用面）
  ///
  /// 主库内置 GC 物理回收完成后注销磁盘墓碑（`delete_dbmeta` → tombstone 事件
  /// → 镜像条目）；副本跟随注销：内存死亡账本离册 + 命名空间级退役连带释放废弃
  /// 租户路由快照 + 本节点磁盘墓碑删除（幂等）。与 [`crate::gc`] 的到期注销
  /// 共用本单点——副本 GC 默认禁用（对标 C# ExpiredKeyDeletionScanFrequencySecs
  /// = -1），磁盘墓碑生命周期完全跟随主库镜像，否则账本随历史换号无限膨胀
  pub async fn apply_dbmeta_tombstone(self: &Arc<Self>, key_payload: &[u8]) -> Result<()> {
    let Some((vid, ns_kind)) = DbMetaRecord::dead_tombstone_of(key_payload) else {
      // 非墓碑载荷的 DbMeta 删除（当前布局不存在）静默跳过，与重建面
      // rebuild_vdb_visit 的墓碑臂同口径
      return Ok(());
    };
    self.vdb.gc_dead.remove(&vid);
    if ns_kind {
      // 彻底释放废弃租户路由快照表，内存归零
      self.vdb.db_routing.pin().remove(&vid);
    }
    self.new_session()?.delete_dbmeta(key_payload).await
  }

  /// 清空指定命名空间下的全部数据库用户域数据，返回 `(new_vns, Option<旧 vns>)`
  /// （新虚拟命名空间号与换号前旧虚拟命名空间号；`None` 即 FirstMap——ns 首映射
  /// 无旧域可退役。O(1) 虚拟命名空间换号 + 延时 GC）
  ///
  /// 多租户隔离清库：仅针对该 `ns` 分配新 virtual_ns_id，旧空间压入延时 GC，
  /// 绝对不触碰其他命名空间，亦不截断全域混合日志。返回值供 AOF 广播面生产
  /// FlushNs 条目（域值与数据条目物理键前缀同域）。
  ///
  /// 换号联动树回收：DbMeta 退役墓碑随换号批落盘后取走旧 vns 全部域的旁表键集延迟销毁
  /// （时序同 [`Self::flush_database`]）；回收严格按命名空间编排，严禁全局
  /// `range_index.clear_all`（误伤他命名空间在用 RI 树，同 [`Self::flush_database`]
  /// 文档说明）。
  pub async fn flush_namespace(self: &Arc<Self>, ns: u64) -> Result<(u64, Option<u64>)> {
    // 冷装载优先：命名空间既有映射先点查磁盘装载（主库冷租户清空间
    // 绝不盲分配换号）
    self.resolve_ns_mapping(ns).await?;

    // 换号事务全程持元数据串行锁，编排与批语义同 [`Self::flush_database`]
    let _dbmeta_guard = self.lock_dbmeta().await;
    let (expired_at, tail_addr) = self.swap_stamp();
    let (new_vns, old_vns_opt) = self.vdb.flush_ns(ns, expired_at, tail_addr);

    // 原子批安全顺序 [新映射, 旧空间退役墓碑?, 0x05 分配水位]，同
    // [`Self::flush_database`]：单点 persist_dbmeta_batch，应答含全批持久化
    let map_rec = DbMetaRecord::NsMap {
      logic_ns: ns,
      vns: new_vns,
    };
    let commit_res = self
      .commit_swap(match old_vns_opt {
        Some(old_vns) => SwapRecords::Swapped {
          map: map_rec,
          dead: DbMetaRecord::GcDeadNs {
            expired_at,
            old_vns,
            tail_address: tail_addr,
          },
        },
        None => SwapRecords::FirstMap { map: map_rec },
      })
      .await;

    if let Err(err) = commit_res {
      self.vdb.rollback_flush_ns(ns, new_vns, old_vns_opt);
      return Err(err);
    }

    if let Some(old_vns) = old_vns_opt {
      for (dvns, dvdb, keys) in self.take_bftree_domains_of_vns(old_vns) {
        self.reclaim_bftree_keys(dvns, dvdb, keys).await;
      }
    }

    // 返回 (new_vns, Option<换号前旧 vns>)：None 即 FirstMap（ns 首映射，
    // 无旧域可退役），调用面按 None 跳过广播与回收（同 [`Self::flush_database`]
    // 哨兵灭绝单机制）
    Ok((new_vns, old_vns_opt))
  }

  /// 换号事务取样单点（须在 [`Self::lock_dbmeta`] 串行锁内调用，主库两放射
  /// flush_database / flush_namespace 同源；唯一豁免：AOF 回放屏障
  /// retire_dead_domain / retire_dead_namespace 两腿——其 synchronized_under_barrier
  /// Leader 独占段全员停车，串行强度不低于本锁，取样无并发换号交错）
  ///
  /// 返回 `(旧域回收期限 expired_at, 取样的日志尾地址 tail_address)`：两者必须
  /// 同刻同锁内取——墓碑记录的尾地址据此判定「该域死亡前已入盘的记录段」，
  /// 与回收期限错代即可能提前物理回收在用数据。
  #[inline]
  fn swap_stamp(&self) -> (i64, u64) {
    let now = now_ticks();
    let tail_address = self.hlog.tail_address();
    (
      reclaim_expired_at(now, self.config.gc.db_gc_reclaim_delay_secs),
      tail_address,
    )
  }

  /// 退役指定物理库域（AOF FlushDb 回放屏障 + 盘上权威回建 + fail-closed 上抛）
  ///
  /// 两段式（doc/zh/db.md「映射权威在磁盘，绝不换号」「从库完全继承主库的映射
  /// 体系，不进行本地二次映射」；禁本臂取号换格——本地发明的号未落盘即成无主
  /// 幽灵域，永久泄漏且任何回收臂不认）：
  /// (i) 在册路由表尚有逻辑库格指向旧域（条目未被先行 DbMeta 换指）时，先按
  /// 盘上 0x02 权威点查回建（[`Self::probe_db_mapping`]），命中值非旧域即经
  /// [`crate::vdb::VirtualDbManager::insert_db_mapping`] 单点装载（零取号、
  /// 零落盘，与冷装载面同一装载原语）；
  /// (ii) 判死腿（`gc_dead`）与树回收腿（[`Self::take_bftree_domain`] +
  /// [`Self::reclaim_bftree_keys`]，树回收经异步卸载离核，回放任务不触条带
  /// 停车档）为屏障语义，**无条件执行且先于任何上抛**，杜绝 Err 丢失屏障回收；
  /// (iii) 回建后内存格仍指旧域（盘上仍指旧域或确无 0x02 记录——本节点对该
  /// 域无可用映射权威）即显式失败上抛留痕，文案携 `vns`/`logic_db`/`old_vdb`
  /// 三值（对标反查面先例「显式失败上抛留痕，禁静默冒充」口径）。
  /// 判死/树回收与幂等面：格已换新或租户不在册即空转，重复应用零副作用。
  pub async fn retire_dead_domain(self: &Arc<Self>, vns: u64, old_vdb: u64) -> Result<()> {
    // (i) 盘上权威回建：仅当内存格仍指旧域才点查该格所属 logic_db 的 0x02
    let mut probe_err = None;
    if let Some(logic_db) = self.stuck_logic_db_of(vns, old_vdb) {
      match self.vdb_load_session.take(self) {
        Ok(session) => {
          session.set_context(0, 0);
          let out: Result<()> = async {
            if let Some(new_vdb) = self.probe_db_mapping(&session, vns, logic_db).await?
              && new_vdb != old_vdb
            {
              self.vdb.insert_db_mapping(vns, logic_db, new_vdb);
              self.vdb.bump_generation();
            }
            Ok(())
          }
          .await;
          self.vdb_load_session.restore(session);
          if let Err(e) = out {
            probe_err = Some(e);
          }
        }
        Err(e) => {
          probe_err = Some(e);
        }
      }
    }
    // (ii) 判死腿：无条件先行（上抛不得丢屏障）
    if !self.vdb.is_dead_domain(vns, old_vdb) {
      let (expired_at, tail_address) = self.swap_stamp();
      self.vdb.gc_dead.insert(
        old_vdb,
        GcDeadEntry {
          expired_at,
          tail_address,
          vns: Some(vns),
        },
      );
    }
    let keys = self.take_bftree_domain(vns, old_vdb);
    self.reclaim_bftree_keys(vns, old_vdb, keys).await;
    // (iii) fail-closed：回建后仍指旧域即显式失败上抛（禁折成静默空转/仅告警）
    if let Some(e) = probe_err {
      return Err(e);
    }
    if let Some(logic_db) = self.stuck_logic_db_of(vns, old_vdb) {
      return Err(error::Error::Io(io::Error::other(format!(
        "retire_dead_domain: 盘上 0x02 权威回建未成功（仍指旧域或无记录），\
         内存格仍指已退役旧域——回放臂禁本地取号二次映射，显式失败留痕: \
         vns={vns}, logic_db={logic_db}, old_vdb={old_vdb}"
      ))));
    }
    Ok(())
  }

  /// 在册路由表中仍指向指定物理域的首个逻辑库号（回建点查入口与
  /// fail-closed 判据共用单点；租户路由快照不在册即 `None` 空转）
  fn stuck_logic_db_of(&self, vns: u64, old_vdb: u64) -> Option<u64> {
    self
      .vdb
      .db_routing
      .pin()
      .get(&vns)
      .and_then(|routing| {
        routing
          .table
          .snapshot()
          .into_iter()
          .find(|&(_, vdb)| vdb == old_vdb)
      })
      .map(|(logic_db, _)| logic_db)
  }

  /// 退役指定物理命名空间（AOF FlushNs 回放屏障 + 盘上权威回建 + fail-closed 上抛）
  ///
  /// 两段式与屏障腿顺序为 [`Self::retire_dead_domain`] 的 Ns 腿镜像：回建
  /// 点查盘上 0x01 NS_MAP 权威（[`Self::probe_ns_mapping`] 单点，键布局不外文复刻），
  /// 命中值非旧空间即经
  /// [`crate::vdb::VirtualDbManager::insert_ns_mapping`] 单点装载
  /// （零取号、零落盘）；判死腿与旧空间全部域树回收腿无条件先行；回建后
  /// 内存 ns 映射仍指旧空间即显式失败上抛，文案携 `logic_ns`/`old_vns` 留痕
  pub async fn retire_dead_namespace(self: &Arc<Self>, old_vns: u64) -> Result<()> {
    // (i) 盘上权威回建：仅当内存 ns 映射仍指旧空间才点查其 logic_ns 的 0x01
    let mut probe_err = None;
    if let Some(logic_ns) = self.stuck_logic_ns_of(old_vns) {
      match self.vdb_load_session.take(self) {
        Ok(session) => {
          session.set_context(0, 0);
          let out: Result<()> = async {
            if let Some(new_vns) = self.probe_ns_mapping(&session, logic_ns).await?
              && new_vns != old_vns
            {
              self.vdb.insert_ns_mapping(logic_ns, new_vns);
              self.vdb.bump_generation();
            }
            Ok(())
          }
          .await;
          self.vdb_load_session.restore(session);
          if let Err(e) = out {
            probe_err = Some(e);
          }
        }
        Err(e) => {
          probe_err = Some(e);
        }
      }
    }
    // (ii) 判死腿 + 旧空间全部域的树回收腿：无条件先行（上抛不得丢屏障）
    if !self.vdb.is_dead_ns(old_vns) {
      let (expired_at, tail_address) = self.swap_stamp();
      self.vdb.gc_dead.insert(
        old_vns,
        GcDeadEntry {
          expired_at,
          tail_address,
          vns: None,
        },
      );
    }
    for (dvns, dvdb, keys) in self.take_bftree_domains_of_vns(old_vns) {
      self.reclaim_bftree_keys(dvns, dvdb, keys).await;
    }
    // (iii) fail-closed：回建后仍指旧空间即显式失败上抛（禁折成静默空转/仅告警）
    if let Some(e) = probe_err {
      return Err(e);
    }
    if let Some(logic_ns) = self.stuck_logic_ns_of(old_vns) {
      return Err(error::Error::Io(io::Error::other(format!(
        "retire_dead_namespace: 盘上 0x01 权威回建未成功（仍指旧空间或无记录），\
         内存 ns 映射仍指已退役旧空间——回放臂禁本地取号二次映射，显式失败留痕: \
         logic_ns={logic_ns}, old_vns={old_vns}"
      ))));
    }
    Ok(())
  }

  /// 内存命名空间映射仍指向指定旧空间时的逻辑空间号（回建点查入口与
  /// fail-closed 判据共用单点；反查表无该空间即 `None` 空转）
  fn stuck_logic_ns_of(&self, old_vns: u64) -> Option<u64> {
    let logic_ns = self.vdb.logic_ns_of(old_vns)?;
    (self.vdb.vns_of_ns(logic_ns) == Some(old_vns)).then_some(logic_ns)
  }

  /// DbMeta 换号事务落盘单点（主库放射与回放射四类换号共用）：把一次换号的
  /// [`SwapRecords`] 经专用会话原子批落盘（固定根域前缀 + `KeyTag::DbMeta`）
  async fn commit_swap(self: &Arc<Self>, records: SwapRecords) -> Result<()> {
    let items: [Option<DbMetaRecord>; 3] = match records {
      SwapRecords::Swapped { map, dead } => [Some(map), Some(dead), Some(self.next_id_rec())],
      SwapRecords::FirstMap { map } => [Some(map), None, Some(self.next_id_rec())],
    };
    self.new_session()?.persist_dbmeta_batch(&items).await
  }

  /// 0x05 分配水位记录（换号取号后即时采样，随原子批收尾抬升）
  #[inline]
  fn next_id_rec(&self) -> DbMetaRecord {
    DbMetaRecord::NextId {
      next_virtual_id: self.vdb.next_virtual_id.load(Ordering::Relaxed),
    }
  }

  /// 清空全部数据库用户域数据（O(1) 物理截断）
  ///
  /// 在 garnet 中的相对路径:libs/server/Databases/MultiDatabaseManager.cs:FlushAllDatabases
  ///
  /// 对标 C# 物理截断 O(1)：仅 shift_begin_address(tail)（C# FlushDatabase
  /// 全程无任何索引清空面），哈希索引表绝不在役原位置零——在途桶闩持有者的
  /// 锁字由持有者自身配对维护（C# HashBucket ReleaseSharedLatch /
  /// ReleaseExclusiveLatch 同款纪律），低于新 begin 的死条目交由既有惰性清退
  /// 单机制收敛：写路径 `find_or_create_tag_by_hash_with_min_addr` /
  /// `find_tag_entry_by_hash_with_min_addr` 的 min_valid_addr CAS 置零臂
  /// （对标 C# TsavoriteBase.cs:FindTagOrFreeInternal）与读路径「低于 begin
  /// 判不存在」臂；range_index.clear_all() 只作用于 RI 旁表（树注册与数据
  /// 文件面，无桶闩锁字），区别于哈希索引表。旁表随 vdb.reset 一并 clear
  /// 整表（不逐键编排，销毁单一来源无双重释放）。
  ///
  /// 入口持 lock_dbmeta 串行闸覆盖 截断前重挂 -> shift_begin_address ->
  /// range_clear -> vdb.reset -> clear_bftree_domains 全编排，与既有四持闸者
  /// 同形复用单机制，杜绝并发换号时死亡登记蒸发与旧号复用撞号；编排内无
  /// lock_dbmeta 再入，互不嵌套契约不破。
  ///
  /// 编排内嵌套获取 lock_acl（全仓单向锁序点 lock_dbmeta → lock_acl——lock_acl
  /// 持有面 SETUSER/DELUSER 分派段全程不触 lock_dbmeta，反向路径不存在）：
  /// 恒根域住户重挂的 scan 与 shift 之间，并发 SETUSER/DELUSER 落盘的增量
  /// ACL 记录地址低于截断线会被物理吞掉，认证真源静默缺损，故重挂全程与
  /// ACL 管理写互斥。
  ///
  /// C# FLUSHALL 只清键空间数据、绝不动 ACL 用户注册表（C# 用户句柄驻
  /// AccessControlList._userHandles 与 Tsavorite 存储彻底分离，
  /// StoreWrapper.FlushAllDatabases 仅转发 databaseManager）；rust ACL 记录
  /// 驻存储即认证唯一真源，物理截断前经 [`Self::remount_truncation_survivors`]
  /// 重挂保真，重挂完成按 DELUSER 同判据推进 ACL 代数消除旧句柄静默窗。
  pub async fn flush_all_databases(self: &Arc<Self>) -> Result<()> {
    let _dbmeta_guard = self.lock_dbmeta().await;
    let _acl_guard = self.lock_acl().await;
    let tail = self.hlog.tail_address();
    let acl_remounted = self.remount_truncation_survivors(tail).await?;
    self.shift_begin_address(tail).await?;
    // clear_all 逐树 dispose（树槽位写锁停车档）+ 全目录文件删除属重操作，
    // 整段经既有阻塞卸载通道离核（detach 族同款纪律，async 任务内直调即
    // 同核互候永挂）；await 期间持 lock_dbmeta/lock_acl 为两串行闸的既有
    // 跨 await 持有形态（编排内无再入，互不嵌套契约不破）
    let mgr = Arc::clone(&self.range_index);
    range_index_blocking(move || mgr.clear_all()).await??;
    self.vdb.reset();
    self.clear_bftree_domains();
    // 重挂即 ACL 改权事实（与 DELUSER 确有删除才推进同判据）：在途挂载会话
    // 下一拍收敛——记录在场重挂无感、截断窗前已删用户 revoke
    if acl_remounted {
      self.bump_acl_generation();
    }
    Ok(())
  }

  /// FLUSHALL 物理截断前的恒根域住户重挂（返回是否重挂了 ACL 用户记录）
  ///
  /// C# 每库独立 Tsavorite 实例与 ACL 用户字典物理分离，FlushAllDatabases
  /// 截断绝不波及用户注册表；rust 单日志多库共享，ACL 用户规则（认证唯一
  /// 真源）与向量上下文元数据单例恒驻日志逻辑域根，无豁免臂的整段截断即
  /// 静默销毁——读路径低于 begin 恒 NotFound 不可逆，AUTH 全租户 WRONGPASS
  /// 永锁。故截断前单遍顺序扫描 `[begin, tail)` 收集链首存活住户（链首
  /// 校验经 [`crate::session::StoreSession::find_tag_cooperative`] 先分裂协同
  /// 后采样活跃表，命中本地址 + 非墓碑，口径同 `for_each_user` 扫描面）：
  /// - `KeyTag::Acl`：全部命名空间的用户规则记录（恒驻 db 0 物理前缀，不经
  ///   vdb 虚号映射，vdb.reset 零号态不迁移）；
  /// - `KeyTag::VectorRegistry`·Metadata 子标签：向量上下文元数据全局单例
  ///   （恒落根域前缀 (0,0)，与 compact 紧缩豁免臂同格同判据；Index 子标签
  ///   随数据域退役，不在重挂集）。
  ///
  /// 逐条 upsert 至当前日志尾（新地址 >= 截断线 tail），原记录与副本物理分居
  /// 截断线两侧，shift 后仅副本存活——O(1) 截断主体不变，ACL 无第二套内存
  /// 镜像。扫描属控制面冷路径，量级同 INFO KEYSPACE 全日志扫描既有先例；
  /// scan 迭代持页读锁，回调内严禁写日志面（upsert 等追加须待扫描收口后的
  /// 回写段执行，收集与回写两段分离）；链首探针经 find_tag_cooperative 先
  /// 协同迁移键所在分块再采样活跃表（索引面迁移，与 `for_each_user` 扫描
  /// 回调内同位同形）——扩容迁移窗内未迁分块桶在新表恒空，裸 `find_tag`
  /// 采得 `None` 会把活住户误判链首不符剔除不重挂，截断后认证真源物理丢失
  /// 不可逆。
  ///
  /// 0x05 分配水位盘上镜像同被截断而内存水位由 vdb.reset 保留（去回绕），
  /// 经 persist_dbmeta 单点补挂——重启重建 rebuild_apply_record 折叠后水位
  /// 不回绕，内存-盘上一致。DbMeta 换号映射/墓碑不重挂：FLUSHALL 零号态
  /// 语义由 vdb.reset 承接。
  ///
  /// 并发边界：ACL 记录写面（SETUSER/DELUSER）已由调用方 lock_acl 互斥封死；
  /// 向量元数据写透（RegistryPersistence::put）不经该锁，竞态窗内最坏以
  /// 旧值副本居尾覆盖新值，计数面下一拍写透自愈，不损认证面。
  async fn remount_truncation_survivors(self: &Arc<Self>, tail: u64) -> Result<bool> {
    let begin = self.hlog.begin_address();
    let mut residents: Vec<(TaggedKeyBuf, Vec<u8>)> = Vec::new();
    let mut acl_seen = false;
    // 会话上提扫描段前：链首校验探针挂 StoreSession 协同单点（勿在
    // WedbStore 上另开第二套协同探针）
    let session = self.new_session()?;
    self
      .hlog
      .scan(begin, tail, |addr, rec| {
        // 链首地址校验：仅该键当前最新版本可入（旧版本/墓碑链一律跳过）。
        // 探针经 find_tag_cooperative 协同单点（先分裂协同后采样，与点读
        // read_probe / 扫描族活键判定一套机制），迁移内核错误沿闭包 Result
        // 短路上抛，严禁折成剔除（折叠即截断幸住户物理丢失）
        if rec.is_tombstone()
          || session
            .find_tag_cooperative(rec.key())
            .map_err(live_probe_err)?
            != Some(addr)
        {
          return Ok(true);
        }
        let keep = match NamespaceDbCodec::decode_tagged_key(rec.key()) {
          Ok((_, _, KeyTag::Acl, _)) => {
            acl_seen = true;
            true
          }
          Ok((_, _, KeyTag::VectorRegistry, payload)) => {
            payload.first() == Some(&VectorRegistrySubTag::Metadata.as_u8())
          }
          _ => false,
        };
        if keep {
          residents.push((TaggedKeyBuf::from(rec.key()), rec.value().to_vec()));
        }
        Ok(true)
      })
      .await?;
    for (key, value) in &residents {
      session.upsert_raw_tail(key, value, tail).await?;
    }
    // 0x05 分配水位补挂（换号取号同源采样，随截断线幸存）
    session
      .persist_dbmeta_tail(&self.next_id_rec(), tail)
      .await?;
    Ok(acl_seen)
  }

  /// INFO KEYSPACE 统计单内核：按在册库逐库统计存活键数与其中带 TTL 键数
  ///
  /// 在 garnet 中的相对路径:libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:KeyspaceStats
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:GetKeyspaceStats
  /// libs/server/Databases/DatabaseManagerBase.cs:GetDatabaseKeyspaceStats
  /// libs/server/Databases/IDatabaseManager.cs:GetKeyspaceStats
  /// libs/server/Databases/MultiDatabaseManager.cs:GetKeyspaceStats
  /// libs/server/Databases/SingleDatabaseManager.cs:GetKeyspaceStats
  ///（四层 GetKeyspaceStats 入口——接口抽象、基类抽象与 protected 扫描助手、
  /// Single/Multi 两实现——随共享单存储双轨折叠为本单内核；C# KeyspaceScanLock
  /// 专用扫描会话的并发防护由本入口的单趟扫描会话编排承接，见下）
  ///
  /// 调用链：`GarnetInfoMetrics.cs:PopulateKeyspaceInfo` →
  /// `libs/server/StoreWrapper.cs:GetKeyspaceStats` →
  /// `libs/server/Databases/MultiDatabaseManager.cs:GetKeyspaceStats` →
  /// `libs/server/Databases/DatabaseManagerBase.cs:GetDatabaseKeyspaceStats` → 本内核。
  ///
  /// C# 侧 INFO KEYSPACE 只遍历 `GetDatabasesSnapshot` 的在册库，逐库取该库专用
  /// 扫描会话（`db.KeyspaceScanStorageSession` + `db.KeyspaceScanLock`，会话按
  /// `db.Id` 构造）各扫一遍**自己独立的** Tsavorite 实例，总扫描量为 ΣN_db；wedb
  /// 单物理日志多库共享、物理键前缀刚性隔离，故逐库语义以**一趟**全日志扫描按
  /// `(vns, vdb)` 分桶承接——总量与 C# 等量等复杂度，杜绝「逐库重扫全日志 + 每库
  /// 建全库键表」的 O(在册库数 × 日志量) 放大。
  ///
  /// 在册库口径同 C# `GetDatabasesSnapshot`：仅枚举 `ns` 已装载路由表内的逻辑库
  /// （[`VirtualDbManager::registered_dbs`] 纯内存只读点查），绝不盲分配虚库号、
  /// 绝不落 KeyTag::DbMeta（只读命令的数据面带写副作用即改写存储状态），亦不改
  /// 任何连接会话上下文；冷库（映射在磁盘未装载）不属在册集，与 C# 未实例化库
  /// 同理不出现在快照里。
  ///
  /// 返回按逻辑库号升序的 `(逻辑库号, 活键数, 带 TTL 活键数)` 行集，含零键库行
  ///（「仅列出至少持有一个键的库」的剔除口径随 C# 留在展示侧
  /// `GarnetInfoMetrics.cs` 的调用面）。
  ///
  /// 实现为全日志两阶段扫描（对标 Garnet `KeyspaceStats` 的哈希索引 lookup
  /// 迭代；wedb 的 windex 无按键遍历 API，索引槽位仅承载 (bucket, tag) 链头，
  /// tag 碰撞键须经 prev_address 链回溯才能枚举，退化为乱序版日志扫描，故择优
  /// 顺序扫描）：
  /// 1. 顺序扫描 `[begin_address, tail_address)` 全区间（含磁盘冷区，
  ///    [`HybridLog::scan`] 自动读盘），跳过墓碑与非用户面物理键（仅收
  ///    String/Meta/ObjectEnvelope 标签；集合子键与 TTL 旁路记录不作候选），按
  ///    在册桶收集去重候选 `(vns, vdb, 用户键)`；
  /// 2. 逐候选经会话读路径（哈希索引取最新态，免疫复活导致的地址乱序）复判：
  ///    字符串记录命中或对象信封记录命中，或集合元记录存在且
  ///    size > 0 任一成立视为存活；是否有 TTL 以
  ///    TTL 记录最新版判定（ttl_of）；已过期键两栏均不计。探针为纯读，不触发
  ///    惰性物理清除（区别于 contains_key / check_expired），统计零写副作用。
  ///
  /// 并发防护对标 Garnet `KeyspaceScanLock`：专用扫描会话懒建复用（本入口单趟
  /// 扫描即覆盖全部在册库，故为单会话而非逐库会话表），并发调用后到者降级为
  /// 一次性临时会话（读路径无共享可变状态，无正确性风险）。
  pub async fn keyspace_stats(self: &Arc<Self>, ns: u64) -> Result<Vec<(u64, u64, u64)>> {
    // 在册库快照（只读甄别）：租户未在册或路由快照未装载即无库可报
    let Some(vns) = self.vdb.vns_of_ns(ns) else {
      return Ok(Vec::new());
    };
    let registered = self.vdb.registered_dbs(vns);
    if registered.is_empty() {
      return Ok(Vec::new());
    }
    // 行按逻辑库号升序（C# databases.OrderBy(db => db.Id)）；vdb -> 行下标
    // 反查表把日志记录解码出的物理库号归到所属逻辑库行
    let mut by_logic: Vec<(u64, u64)> = registered
      .into_iter()
      .map(|(vdb, logic_db)| (logic_db, vdb))
      .collect();
    by_logic.sort_unstable();
    let mut rows: Vec<(u64, u64, u64)> = by_logic
      .iter()
      .map(|&(logic_db, _)| (logic_db, 0, 0))
      .collect();
    let bucket_of: HashMap<u64, usize> = by_logic
      .iter()
      .enumerate()
      .map(|(i, &(_, vdb))| (vdb, i))
      .collect();

    let session = self.keyspace_scan_session.take(self)?;
    let from = self.hlog.begin_address();
    let until = self.hlog.tail_address();
    // 过期判定基准：.NET Ticks（与 TTL 记录值同域）
    let now = now_ticks();
    let mut candidates = ExpiredKeySet::with_hasher(GxBuildHasher::default());

    self
      .hlog
      .scan(from, until, |_, rec| {
        if rec.is_tombstone() {
          return Ok(true);
        }
        // 解码出的 (ns, db) 为物理口径 (vns, vdb)：只收本租户在册桶，桶外
        //（他租户域、退役换号旧域、未装载冷库）一并跳过——在册桶天然不含退役
        // 旧号（换号即逻辑库号 → 新虚库号覆盖），无需二次判死
        if let Ok((rec_vns, rec_vdb, tag, user_key)) = NamespaceDbCodec::decode_tagged_key(rec.key)
          && rec_vns == vns
          && bucket_of.contains_key(&rec_vdb)
          && tag.is_user_visible()
        {
          candidates.insert((rec_vns, rec_vdb, Box::from(user_key)));
        }
        Ok(true)
      })
      .await?;

    for (cns, cdb, key) in &candidates {
      let prefix = SessionPrefixBuf::new(*cns, *cdb);
      // 存活判定：字符串记录命中、对象信封记录命中，或集合元记录
      // MetaValue::is_live 存活（RangeIndex 恒活 + size > 0；与 contains_key /
      // contains_key_ignore_ttl 同源单点——空 RangeIndex 计入活键，
      // EXISTS/TTL 族已收敛口径不得在统计面分叉）
      let str_k =
        NamespaceDbCodec::encode_with_session_prefix(prefix.as_slice(), KeyTag::String, key);
      let alive = session.read_raw_with(&str_k, |_| ()).await?.is_some();
      let alive = if alive {
        true
      } else {
        let env_k = NamespaceDbCodec::encode_with_session_prefix(
          prefix.as_slice(),
          KeyTag::ObjectEnvelope,
          key,
        );
        if session.read_raw_with(&env_k, |_| ()).await?.is_some() {
          true
        } else {
          let meta_k =
            NamespaceDbCodec::encode_with_session_prefix(prefix.as_slice(), KeyTag::Meta, key);
          session
            .read_raw_with(
              &meta_k,
              // 纯读闭包：判活不落任何写入，统计零写约束不变
              |bytes| match MetaValue::from_slice(bytes) {
                Ok(meta) => meta.is_live(),
                Err(_) => false,
              },
            )
            .await?
            .unwrap_or(false)
        }
      };
      if !alive {
        continue;
      }
      // 是否有 TTL 与是否过期均以 TTL 记录最新版判定；已过期键两栏均不计
      let ttl = session.raw_ttl_with_prefix(prefix.as_slice(), key).await?;
      if is_expired(ttl, now) {
        continue;
      }
      // 反查表命中必成立（候选即按在册桶收集）；越界写行号即统计口径漂移，
      // 故取而非直用 unchecked
      if let Some(&i) = bucket_of.get(cdb) {
        rows[i].1 += 1;
        if ttl.is_some() {
          rows[i].2 += 1;
        }
      }
    }

    self.keyspace_scan_session.restore(session);
    Ok(rows)
  }
}

/// remount 扫描闭包协同探针错误折叠单点（§35 口径）：
/// [`crate::session::StoreSession::find_tag_cooperative`] 上抛的迁移内核错误
/// 沿 whlog Err 通道传出扫描闭包，外层 `?` 复原 wkv::Error 上抛
/// [`WedbStore::flush_all_databases`]，严禁折成剔除——折叠即截断幸住户物理
/// 丢失（ACL 认证真源不可逆灭失）。与 wnode
/// array_key_iteration_functions::live_probe_err 同轨同形（wkv 层无法复用
/// wnode crate 私有项，依赖单向分层）
fn live_probe_err(e: error::Error) -> whlog::Error {
  Error::Io(io::Error::other(e.to_string()))
}
