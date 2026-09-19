use std::sync::{Arc, atomic::Ordering};

use gxhash::{GxBuildHasher, HashMap};
use wbase::{convert::expire_after_to_ticks, time::now_ticks};
use wdev::Device;
use wval::{I64Codec, KeyTag, MetaValue, NamespaceDbCodec, SessionPrefixBuf};

use super::WedbStore;
use crate::{
  error::Result,
  gc::{ExpiredKeySet, ScanBudget, collect_expired},
  ttl::is_expired,
  vdb::{DbMetaRecord, GcDeadEntry, ROOT_VIRTUAL_ID},
};

/// 换号旧域/旧空间的回收截止 ticks：`now_ticks + db_gc_reclaim_delay_secs` 秒
///
/// 秒→tick 一律走 [`expire_after_to_ticks`] 单点（时长换算与饱和加法均在其中），
/// 严禁在存储层裸乘刻度字面量或在 u64 域内乘完再 `as i64` 静默收窄（debug 构建
/// 溢出 panic、release 构建环绕成过去时刻）。配置秒数超出 i64 值域时钳到
/// i64::MAX = 截止永不到期：宁可旧域滞留磁盘，也不透支「严防幽灵读取」的
/// 安全纪元窗（提前回收才是数据可见性事故）
#[inline]
fn reclaim_expired_at(now_ticks: i64, delay_secs: u64) -> i64 {
  expire_after_to_ticks(now_ticks, i64::try_from(delay_secs).unwrap_or(i64::MAX))
}

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
      session.set_virtual_context(ns, db);
      if session.check_expired(&key).await? {
        deleted += 1;
      }
    }
    Ok((deleted, scanned))
  }

  /// 清空指定数据库全部用户域数据，返回 `(vns, domain_db)`（FlushDb 条目域
  /// 载荷值：虚拟命名空间号与换号前旧虚拟库号）
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
  /// 换号联动树回收：DbMeta 双墓碑落盘后按 (vns, 旧 vdb) 取走换号回收旁表键集，
  /// 逐键 detach_tree 同步摘除注册表（杜绝同名重建索引被 IndexExists 拦截的
  /// 卡死与双泄漏）后投递待释放队列，旧域 RI/升阶树的纪元屏障排空与数据文件
  /// 删除移交内置 GC 轮次（[`Self::drain_bftree_release`]）在后台线程执行——
  /// 从库回放臂经此不 await 任何物理释放，不阻塞复制流水线（doc/zh/db.md
  /// 主从异步屏障）。时序「先持久化换号后回收树」：中途崩溃最坏留孤儿文件
  /// （启动对账 [`Self::reclaim_dead_domain_bftrees`] 承接），绝不出现路由
  /// 存活而树已销毁。
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
  pub async fn flush_database(self: &Arc<Self>, ns: u64, db_id: u64) -> Result<(u64, u64)> {
    // 冷装载优先：租户既有映射先点查磁盘装载（映射权威在磁盘 DbMeta，
    // 主库冷租户清库绝不盲分配换号）
    let (vns, _) = self.resolve_context(ns, db_id).await?;

    // 换号事务全程：锁内 CAS 换号 → 原子批 persist → 树回收投递
    let _dbmeta_guard = self.lock_dbmeta().await;
    let (expired_at, tail_addr) = self.swap_stamp();
    let (new_vdb, old_vdb_opt) = self.vdb.flush_db(ns, db_id, expired_at, tail_addr);

    // 原子批安全顺序 [新映射, 旧域退役墓碑?, 0x05 分配水位]（doc/zh/db.md
    // 「即时原子提交」段）：固定根域前缀经 persist_dbmeta_batch 单点落盘，
    // 同步快路径翻页条目集中异步回放保达，命令应答即含全批持久化承诺；
    // 任意崩溃前缀最坏旧域泄漏，绝不数据复活或撞号（水位收尾抬升兜底）
    let map_rec = DbMetaRecord::DbMap {
      vns,
      logic_db: db_id,
      vdb: new_vdb,
    };
    self
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
      .await?;

    if let Some(old_vdb) = old_vdb_opt {
      self.reclaim_bftree_keys(self.take_bftree_domain(vns, old_vdb));
    }

    // 条目域值 (vns, 载荷 db)：db = 换号前旧 vdb；库首映射（无旧号，RESP 面
    // 不可达）时以新号作非零哨兵——旧号不存在则该库从未有数据条目前缀，从库
    // 换号空域幂等，且绝不误清根域 (vns, 0)
    Ok((vns, old_vdb_opt.unwrap_or(new_vdb)))
  }

  /// DbMeta 镜像记录回放应用单点（doc/zh/db.md「从库完全继承主库的映射体系，
  /// 不进行本地二次映射」的 AOF 复制流承接面）
  ///
  /// 主库全部 DbMeta 落盘（换号批 / 首映射 / SWAPDB 成对记录）经存储事件镜像
  /// 为 StoreUpsert 条目；回放侧解码出 [`DbMetaRecord`] 后经本入口应用：内存
  /// 装载复用重建内核 [`Self::rebuild_apply_record`]（根域 immortal 全量、非根域
  /// 0 常驻点查回建，与启动重建同一口径），分配水位单调抬升越过记录号（杜绝
  /// 本地取号与主库未来取号撞号），判死变体联动旧域树回收投递，最后经
  /// [`Self::persist_dbmeta`] 落盘本节点磁盘（幂等覆写；回放全程
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
        if vns != ROOT_VIRTUAL_ID {
          self.vdb.insert_db_mapping(vns, logic_db1, swapped_db1);
          self.vdb.insert_db_mapping(vns, logic_db2, swapped_db2);
        }
        self.vdb.bump_generation();
      }
      _ => {}
    }
    // 判死变体联动：旧物理域的换号回收旁表键集摘除后投待释放队列（与主库
    // flush 放射同一编排，时序「先持久化换号后回收树」）
    match rec {
      DbMetaRecord::GcDeadDb { vns, old_vdb, .. } => {
        self.reclaim_bftree_keys(self.take_bftree_domain(vns, old_vdb));
      }
      DbMetaRecord::GcDeadNs { old_vns, .. } => {
        self.reclaim_bftree_keys(self.take_bftree_domains_of_vns(old_vns));
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

  /// 清空指定命名空间下的全部数据库用户域数据，返回 `(domain_vns, 0)`
  /// （FlushNs 条目域载荷值，O(1) 虚拟命名空间换号 + 延时 GC）
  ///
  /// 多租户隔离清库：仅针对该 `ns` 分配新 virtual_ns_id，旧空间压入延时 GC，
  /// 绝对不触碰其他命名空间，亦不截断全域混合日志。返回值供 AOF 广播面生产
  /// FlushNs 条目（域值与数据条目物理键前缀同域）。
  ///
  /// 换号联动树回收：DbMeta 双墓碑落盘后取走旧 vns 全部域的旁表键集延迟销毁
  /// （时序同 [`Self::flush_database`]）；回收严格按命名空间编排，严禁全局
  /// `range_index.clear_all`（误伤他命名空间在用 RI 树，同 [`Self::flush_database`]
  /// 文档说明）。
  pub async fn flush_namespace(self: &Arc<Self>, ns: u64) -> Result<(u64, u64)> {
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
    self
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
      .await?;

    if let Some(old_vns) = old_vns_opt {
      self.reclaim_bftree_keys(self.take_bftree_domains_of_vns(old_vns));
    }

    // 条目域值 = 换号前旧 vns；ns 首次映射（无旧号，RESP 面不可达）时以新号
    // 作非零哨兵——旧号不存在则该 ns 从未有数据条目前缀，从库换号空域幂等
    Ok((old_vns_opt.unwrap_or(new_vns), 0))
  }

  /// 换号事务取样单点（须在 [`Self::lock_dbmeta`] 串行锁内调用，主库两放射同源）
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

  /// 退役指定物理库域（AOF FlushDb 回放屏障与兜底判死）
  ///
  /// 仅当旧域尚未判死时登记本地 GC 死亡账本并联动树回收，
  /// 映射面（租户表/路由表/分配水位）全程零改动，不进行本地二次映射
  pub fn retire_dead_domain(&self, vns: u64, old_vdb: u64) {
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
    self.reclaim_bftree_keys(self.take_bftree_domain(vns, old_vdb));
  }

  /// 退役指定物理命名空间（AOF FlushNs 回放屏障与兜底判死）
  ///
  /// 仅当旧空间尚未判死时登记本地 GC 死亡账本并联动树回收，
  /// 映射面（租户表/路由表/分配水位）全程零改动，不进行本地二次映射
  pub fn retire_dead_namespace(&self, old_vns: u64) {
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
    self.reclaim_bftree_keys(self.take_bftree_domains_of_vns(old_vns));
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
  /// 对标 C# 物理截断 O(1)：shift_begin_address(tail) + index.clear() +
  /// range_index.clear_all()；旁表随 vdb.reset 一并 clear 整表（不逐键编排，
  /// 销毁单一来源无双重释放）。全域清空场景全局 clear_all 语义正确：全部域
  /// 同步死亡无存活租户可误伤，且兼清理旁表逐键回收不覆盖的检查点快照面
  /// （cpr_dir 快照目录），区别于按库/按命名空间换号的严格按域编排。
  pub async fn flush_all_databases(self: &Arc<Self>) -> Result<()> {
    let tail = self.hlog.tail_address();
    self.shift_begin_address(tail).await?;
    self.index.load().clear();
    self.range_index.clear_all()?;
    self.vdb.reset();
    self.clear_bftree_domains();
    Ok(())
  }

  /// INFO KEYSPACE 统计单内核：按在册库逐库统计存活键数与其中带 TTL 键数
  ///
  /// 在 garnet 中的相对路径:libs/server/Storage/Session/Common/ArrayKeyIterationFunctions.cs:KeyspaceStats
  ///
  /// 调用链：`GarnetInfoMetrics.cs:PopulateKeyspaceInfo` →
  /// `StoreWrapper.cs:GetKeyspaceStats` →
  /// `Databases/MultiDatabaseManager.cs:GetKeyspaceStats` →
  /// `Databases/DatabaseManagerBase.cs:GetDatabaseKeyspaceStats` → 本内核。
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
      // 存活判定：字符串记录命中、对象信封记录命中，或集合元记录存在且
      // size > 0（幽灵元记录不计）
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
              |bytes| matches!(MetaValue::read_size(bytes), Ok(size) if size > 0),
            )
            .await?
            .unwrap_or(false)
        }
      };
      if !alive {
        continue;
      }
      // 是否有 TTL 与是否过期均以 TTL 记录最新版判定；已过期键两栏均不计
      let ttl_k = NamespaceDbCodec::encode_with_session_prefix(prefix.as_slice(), KeyTag::Ttl, key);
      let ttl = session
        .read_raw_with(&ttl_k, I64Codec::decode)
        .await?
        .flatten();
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
