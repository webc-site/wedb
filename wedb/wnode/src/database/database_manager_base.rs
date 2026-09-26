//! 数据库管理共享基座（对标 libs/server/Databases/DatabaseManagerBase.cs）
//!
//! C# 侧为抽象基类，承载检查点 / AOF / 恢复 / 清空的跨模式共享实现；Rust 侧
//! 为 [`DatabaseManagerBase`]，方法统一以 [`GarnetDatabase`] 为操作对象，
//! 由 Single / Multi 管理器组合复用。AOF 记录严格遵循 Garnet 标准 AofHeader + AofEntryType 布局。

use std::{
  future::Future,
  io::Error,
  marker::PhantomData,
  path::PathBuf,
  sync::{
    Arc, OnceLock,
    atomic::Ordering::{AcqRel, Acquire, Relaxed, Release},
  },
};

use waof::AofAddress;
use wbase::time::now_ms;
use wcpr::{self, CheckpointType};
use wdev::Device;
use wkv::{Error as WkvError, VERSION_MASK, WedbStore, store::grow_index_blocking};

use super::garnet_database::GarnetDatabase;
use crate::cluster_provider::ClusterProviderHandle;

/// 快照恢复产物：恢复出的存储句柄（多库共享场景由调用方接管重挂）
pub type RecoveredStore<D> = Option<Arc<WedbStore<D>>>;

/// 检查点版本号映射（优先高 64 位，回退低 64 位）
#[inline]
pub const fn checkpoint_version(token: u128) -> i64 {
  let ver = (token >> 64) as i64;
  if ver != 0 { ver } else { token as i64 }
}

/// 检查点保留代数（对标 C# 检查点管理器 removeOutdated 环容量
/// `DeviceLogCommitCheckpointManager.cs:19` 的 `const byte indexTokenCount = 2`
/// ——环满两代即拍新删旧；C# 的 logTokenCount = 1 是「索引/日志异 Token」
/// 时代的配对余量，rust 统一检查点模型一个 Token 承载 index/hlog/RangeIndex
/// 一套文件，无错配形态，取两环上界 2 同形）。
///
/// C# 该值是编译期常数而非配置旋钮（garnet 全域 Options.cs 无 checkpoint-keep
/// 类项，只有决定**是否**自动清理的形态位 removeOutdated），rust 据而不另立
/// 配置槽位：保留数由本常量承载，形态位由基座的 `cluster` 句柄在位与否承担
/// （与检查点版本切换标记同一判据，对标 C# `GarnetServer.cs:396` 的
/// removeOutdated = !EnableCluster）
pub const CHECKPOINT_RETAIN_GENERATIONS: usize = 2;

/// 共享基座：快照目录
pub struct DatabaseManagerBase<D: Device> {
  /// 默认检查点目录（库未显式指定时使用）
  pub checkpoint_dir: PathBuf,
  /// 集群提供者句柄（检查点版本切换标记的复制域出口；与 flush_gate 同点装配，
  /// None = 单机形态无标记面，对标 C# 委托挂 ReplicationLogCheckpointManager，
  /// standalone provider 无 replication manager 故不写标记）
  cluster: OnceLock<ClusterProviderHandle>,
  _marker: PhantomData<D>,
}

impl<D: Device> DatabaseManagerBase<D> {
  /// 以默认检查点目录与默认策略创建基座
  pub fn new(checkpoint_dir: PathBuf) -> Self {
    Self {
      checkpoint_dir,
      cluster: OnceLock::new(),
      _marker: PhantomData,
    }
  }

  /// 注入集群提供者句柄（集群装配期一次，与 flush_gate 同点；对标 C# 构造期把
  /// checkpointVersionShiftStart/End 委托挂到检查点管理器）。None（未注入）=
  /// 单机形态，检查点内核不写版本切换标记
  pub fn attach_cluster_provider(&self, cluster: ClusterProviderHandle) {
    let _ = self.cluster.set(cluster);
  }

  /// 取库或新建（多库模式的映射由管理器覆写）
  ///
  /// 单库语义：恒返回 db0。
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:TryGetOrAddDatabase
  pub fn try_get_or_add_database(
    &self,
    db: &Arc<GarnetDatabase<D>>,
  ) -> wkv::Result<(Arc<GarnetDatabase<D>>, bool)> {
    Ok((Arc::clone(db), false))
  }

  /// 尝试暂停检查点（占用检查点锁，成功返回 true；已被占用返回 false）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:TryPauseCheckpoints
  pub fn try_pause_checkpoints(&self, db: &GarnetDatabase<D>) -> bool {
    !db.checkpoint_paused.swap(true, AcqRel)
  }

  /// 恢复检查点调度（释放检查点锁）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ResumeCheckpoints
  pub fn resume_checkpoints(&self, db: &GarnetDatabase<D>) {
    db.checkpoint_paused.store(false, Release);
    // 精准唤醒一个让渡等待者（按需入口 acquire_checkpoint_gate 的挂起端）
    db.checkpoint_gate_resume.notify(1);
  }

  /// 恢复数据库检查点：从指定（或最新有效）令牌恢复出全新存储句柄
  ///
  /// `recover_from_token` 优先（显式 Token 单发：副本在线导入与历史版本恢复
  /// 依赖复制域指定 Token 的精确语义，回退会产生版本低于复制域要求的引擎，
  /// 语义破坏）；None 即启动恢复（主/副本重启同链），走
  /// [`WedbStore::recover_latest`] 由新到旧回退跳过损坏检查点（对标 C#
  /// GetClosestHybridLogCheckpointInfo 的容错跳过语义）——最新有效版承接，
  /// 旧代条目由 AOF 重放按版本基线追平。
  /// wkv 恢复产出全新 [`WedbStore`]，存储句柄替换由管理器初始化路径接管。
  /// 恢复成功即将存储版本推进至**实际恢复**令牌（回退轮经
  /// [`WedbStore::recovered_checkpoint_token`] 取得，find_latest 在回退后已
  /// 不代表恢复版本；对标 C# `RecoverAsync` 返回 storeVersion、
  /// `store.CurrentVersion` 成为 AOF 重放的版本基线——`ShouldSkipRecord`
  /// 跳过低版本条目）。
  ///
  /// 恢复成功后追加「清未用」尾段，见 [`Self::purge_unrecovered_checkpoints`]。
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:RecoverDatabaseCheckpointAsync
  pub async fn recover_database_checkpoint_async(
    &self,
    db: &GarnetDatabase<D>,
    recover_from_token: Option<u128>,
  ) -> wkv::Result<RecoveredStore<D>> {
    let has_checkpoint = match recover_from_token {
      Some(_) => true,
      None => wcpr::find_latest_checkpoint(&db.checkpoint_dir)?.is_some(),
    };
    if !has_checkpoint {
      return Ok(None);
    }
    let store = Arc::new(match recover_from_token {
      Some(t) => WedbStore::recover(&db.checkpoint_dir, t, Arc::clone(&db.device)).await?,
      None => WedbStore::recover_latest(&db.checkpoint_dir, Arc::clone(&db.device)).await?,
    });
    // 常驻回收驱动随实例挂载（幂等；恢复产出的全新引擎不经 open_shared，
    // 唯一生产挂点在 open_shared 时死亡账本到期项永无人消费）：bftree 待释放
    // 队列与死亡账本续扫不受 gc.enabled 门禁，与下方 start_gc 的过期键扫描
    // 循环解耦（主/副本重启同链，doc/zh/db.md 重启自动恢复未完成 GC 队列）
    wkv::spawn_bftree_reclaimer(&store);
    let t = store
      .recovered_checkpoint_token()
      .expect("恢复成功必携带恢复来源 Token");
    // 版本基线推进：共享存储模型下当前在用 store 亦对齐至恢复版本，
    // 后续 AOF 重放跳过 checkpoint 已覆盖的旧代条目
    store.set_current_version(checkpoint_version(t));
    db.store().set_current_version(checkpoint_version(t));
    self.purge_unrecovered_checkpoints(db, t);
    db.update_last_save(now_ms());
    Ok(Some(store))
  }

  /// 恢复后「清未用」：物理删除目录内未被本次恢复选中的全部快照
  ///
  /// 对标 C# 检查点管理器的 `OnRecovery`
  ///（`DeviceLogCommitCheckpointManager.cs:337-365`：首行 `if (!removeOutdated)
  /// return;`，随后把 `GetLogCheckpointTokens` / `GetIndexCheckpointTokens` 中
  /// 一切不等于本次恢复 Token 的快照逐个 Delete，原文注释 "Purge all log/index
  /// checkpoints that were not used for recovery"）——未被选中的快照对本次恢复
  /// 之后的写入链已无意义（版本基线已抬至恢复 Token，旧代条目重放即被
  /// `ShouldSkipRecord` 跳过），留盘即永久占用。
  ///
  /// 形态位与 [`Self::take_database_checkpoint_async`] 第 7 步同源（`cluster`
  /// 句柄缺位 = C# `GarnetServer.cs:396` 的 removeOutdated = !EnableCluster
  /// 为真）：集群形态让位复制域 CheckpointStore 单轨，本口按其回收。
  ///
  /// 与按代回收的分工：本口按**身份**筛——保留被选中那一版、删其余全部（显式
  /// 恢复历史 Token 时更新的代同样作废），故不复用 [`wcpr::purge_outdated`] 的
  /// 按条数口径：按条数留的是「最新 keep 个」，显式恢复旧 Token 时会误删在用版。
  ///
  /// 启动期形态位说明（与 C# 的唯一结构差）：rust 的启动恢复先于
  /// `wedb/src/server/boot.rs:105` 的 cluster 句柄注入（管理器由 `open_from_args`
  /// 自建），故集群宿主的启动恢复亦走本段；删集与集群轨自身的启动清理段重合——
  /// `replication_manager.rs:1130` `initialize_checkpoint_store` 的 seed 同取
  /// `find_latest_checkpoint` 那一 Token，经 `CheckpointStore::initialize` →
  /// `purge_all_checkpoints_except_entry` 留同一版删其余，两段不构成并行的第二套
  /// 回收（运行期恢复入口形态位已就位，集群形态按位让位）。
  ///
  /// best-effort：删除失败只 warn，不回滚已成功的恢复（C# 的 `deviceFactory.Delete`
  /// 同为不影响恢复结果的清理尾段），下一轮检查点按代回收再收。
  fn purge_unrecovered_checkpoints(&self, db: &GarnetDatabase<D>, recovered: u128) {
    if self.cluster.get().is_some() {
      return;
    }
    let Ok(tokens) = wcpr::list_checkpoints(&db.checkpoint_dir) else {
      return;
    };
    for stale in tokens {
      if stale == recovered {
        continue;
      }
      // 恢复基线严格性断言：对一切更早前代，恢复 Token 的版本投影必须严格更高
      //（签发闸门高位钳制保证）——基线与前代相等则旧代 AOF 条目对版本闸
      // （is_old_version_record）不可见，已固化数据重复重放
      debug_assert!(
        stale > recovered || checkpoint_version(recovered) > checkpoint_version(stale),
        "恢复基线版本须严格高于前代: recovered {recovered:#x} vs stale {stale:#x}"
      );
      if let Err(e) = wcpr::purge_checkpoint(&db.checkpoint_dir, stale) {
        log::warn!(
          "Failed purging checkpoint {stale:#x} unrecovered by recovery in {}: {e}",
          db.checkpoint_dir.display()
        );
      }
    }
  }

  /// 恢复数据库 AOF：设备面日志恢复（磁盘段位点扫描）+ 全量重放
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:RecoverDatabaseAOFAsync
  ///（C# = `db.AppendOnlyFile.Log.RecoverAsync()` 仅恢复位点；重放由
  /// `AofProcessor.Recover` 从 BeginAddress 全量扫描、以 `ShouldSkipRecord`
  /// 版本过滤承接。C# RecoverDatabaseAOFAsync 自身无 try/catch，其续行门控
  /// FailOnRecoveryError 生效默认关、在更上层消费（catch 吞错带部分数据起库）；
  /// rust 侧该旗标零代码消费、全仓无装配层裁决点——本口设备面 `?` 即终点，
  /// 恢复失败恒拒启、无续行门（刻意收紧，见 deviations.md §122），
  /// 阻断脏位点下的 replay_database_aof）
  pub async fn recover_database_aof_async(&self, db: &GarnetDatabase<D>) -> wkv::Result<u64> {
    let Some(aof) = &db.aof else {
      return Ok(0);
    };
    // waof→wkv 无 From 通道（禁跨 crate 加依赖造第二套错误边）：经 Io 字符串
    // 单点降级，Display 保留完整因果链，脏位点下绝不续跑重放
    aof
      .log()
      .recover_async()
      .await
      .map_err(|e| WkvError::Io(Error::other(e.to_string())))?;
    self.replay_database_aof(db, u64::MAX).await
  }

  /// 重放数据库 AOF（至 `until` 地址；u64::MAX = 尾部），返回重放条数
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ReplayDatabaseAOF
  pub async fn replay_database_aof(&self, db: &GarnetDatabase<D>, until: u64) -> wkv::Result<u64> {
    let Some(aof) = &db.aof else {
      return Ok(0);
    };
    let store = db.store();
    let _pause = store.pause_aof_listeners();
    let replayed = Arc::clone(aof).replay_database_aof(db, until).await?;
    db.update_last_save(now_ms());
    Ok(replayed)
  }

  /// 拍数据库检查点（TakeCheckpointAsync 的 full 判定 + InitiateCheckpointAsync 五步）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:TakeCheckpointAsync 与
  /// libs/server/Databases/DatabaseManagerBase.cs:InitiateCheckpointAsync
  ///（498-545）的合流内核：
  /// 1. full 维度经统一检查点模型结构性消除，每轮恒等价 C# Checkpoint.Full
  ///    代际轮（对位 DatabaseManagerBase.cs:InitiateCheckpointAsync 的 true 臂，
  ///    与下方「full 恒 true 对齐 rust 统一检查点模型」自书同口径；严禁按
  ///    FullCheckpointLogInterval 文案回接第二套增量形态）；
  /// 2. 预知签发 Token（快照目录下界之上，高位钳制保证版本严格递增）；
  /// 3. 覆盖位点采样先行（C# InitiateCheckpointAsync :503-519 同序）：集群
  ///    形态由复制域 OnCheckpointInitiated 给出检查点覆盖的 AOF 地址，单机
  ///    形态直取 AOF 尾地址（C# else 分支 TailAddress + SetCurrentSafeAofAddress，
  ///    安全地址由本方法末尾 update_last_save 承接）——covered ≤ 开窗 ≤ 版本
  ///    推进的锚点全序保证位点闸与版本闸互补闭环；
  /// 4. 开启 hlog 版本推进窗口并推进版本号（对标 C# PREPARE
  ///    进入 IN_PROGRESS 时 Version 推进与 startLogicalAddress 捕获同点：窗口
  ///    开启瞬间 tail 即本轮检查点模糊区地板 index_start）；
  /// 5. 执行快照（WedbStore.create_checkpoint_with_token，随参传入第 4 步捕获的
  ///    index_start 模糊区地板；快照期间前台写入携带新版本并被标记窗置位
  ///    IN_NEW_VERSION，恢复内核据同一地板回滚剔除、由 AOF 重放恰一次承接）；
  /// 6. AOF 边界持久化 + 截断：快照发布后经
  ///    [`wcpr::publish_checkpoint_aof_address`] 把 covered 补写进检查点元数据
  ///    （对标 C# GarnetCheckpointManager.GetCookie 将 CurrentSafeAofAddress
  ///    序列化进检查点 cookie），随后 AddNewCheckpointEntry（集群 && AOF）登记
  ///    检查点条目并安全截断——异步截断口经 SlowFuture 擦除壳承载、内核处
  ///    await 收口（见下方截断层形态二分）；单机形态 TruncateUntil + Commit
  ///    （物理截断 + 刷盘，
  ///    与数据记录同一物理 AOF 域——域统一后截断位点对单一日志成立）；
  /// 7. 记录保存点（update_last_save）；
  /// 8. 快照保留回收（单机形态）：拍新删旧至 [`CHECKPOINT_RETAIN_GENERATIONS`]
  ///    代（对标 C# 检查点状态机 REST 段的 CleanupIndexCheckpoint /
  ///    CleanupLogCheckpoint）；集群形态由复制域 CheckpointStore 读者闸门单轨
  ///    接管，本步不触发（形态判据同版本切换标记）。
  ///
  /// 两层闸门关系：本方法不含 [`checkpoint_paused`](GarnetDatabase::checkpoint_paused)
  /// 暂停闸（调用方职责——生产链一律经 [`SingleDatabaseManager::take_checkpoint`]
  /// 收口入口或 AOF 限长任务的显式对进入）；wcpr 实例闸 `acquire`（宿主
  /// `WedbStore::ckpt_gate` 持）只串行
  /// 第 5 步 `create_checkpoint_with_token` 内部快照段，罩不住其外的版本推进
  /// （第 4 步）、移位标记入账（第 3 步与尾前步）、AOF 截断（第 6 步）与回收
  /// （第 8 步）——两检查点的这些段互斥由上层暂停闸唯一保证。
  pub async fn take_database_checkpoint_async(&self, db: &GarnetDatabase<D>) -> wkv::Result<bool> {
    // 统一检查点模型恒快照形态（FoldOver 增量形态不移植，见方法头注）
    let cp_type = CheckpointType::Snapshot;

    // 预知签发 Token 并提前推进版本号：快照期间写入即携带新版本号，
    // 杜绝快照窗口内写入携带旧版本而在崩溃恢复时被 AOF 重放误跳过
    let floor = wcpr::find_latest_checkpoint(&db.checkpoint_dir)?.unwrap_or(0);
    let token = wcpr::next_token_above(floor);
    let new_version = checkpoint_version(token);
    // 版本推进无静默失效断言：签发闸门（wcpr::issue_token_after 高位钳制）保证
    // token 高 64 位逐代严格递增，new_version 必须严格高于引擎当前版本——否则
    // 快照窗口内写入仍携带旧版本，AOF 新旧代边界（is_old_version_record /
    // is_new_version_record）划分失效，旧代条目重复重放（对标 C# VersionChangeSM
    // `nextState.Version = start.Version + 1` 的严格代际推进）
    // 在线引擎单次取口（版本推进窗、快照与还窗必须同一实例——窗口中途
    // 被副本全量恢复换持时，旧窗标记绝不允许泄漏到新引擎）
    let store = db.store();
    debug_assert!(
      new_version > store.current_version(),
      "检查点版本必须严格递增: new {new_version} <= current {}",
      store.current_version()
    );
    // 版本切换开始（对标 C# GlobalBeforeEnteringState IN_PROGRESS →
    // CheckpointVersionShiftStart）：主库向 AOF 追加 CheckpointStartCommit 标记，
    // 副本据此进入模糊区缓冲新代条目。单机形态（未注入 cluster 句柄）不写标记
    if let Some(cluster) = self.cluster.get() {
      cluster.checkpoint_version_shift_start(new_version);
    }
    // 覆盖位点采样先行（对标 C# InitiateCheckpointAsync :503-519：covered
    // 采样于同步段开头、先于 Checkpoint.Full 的状态机版本推进）。旧序
    // （开窗→推版本→采样 covered）在版本推进与采样之间留窗：窗口内前台写入
    // 携带新版本却位点落在 covered 之前，快照发布后 AOF 被截断至 covered
    // 将其删除，恢复内核又按 IN_NEW_VERSION_BIT 把快照内副本回滚剔除——
    // 截断面与回滚面双重删除致静默丢数据。采样先行恢复锚点全序
    // covered ≤ 开窗 ≤ 版本推进，自此「位点 < covered ⇒ 版本恒旧」不变式
    // 成立：截断面（truncate 至 covered）删除的条目必被恢复重放的版本闸
    // 跳过（其效果已物化进快照），版本闸对截断面完备，无丢失窗
    // 占位按物理子日志数开维（对标 C# InitiateCheckpointAsync :503
    // `AofAddress.Create(AofPhysicalSublogCount, 0)`），集群臂整体覆盖
    let mut covered = db.aof.as_ref().map_or_else(AofAddress::default, |aof| {
      AofAddress::create(aof.log().size() as i32, 0)
    });
    if let Some(aof) = &db.aof {
      // covered 取源按形态二分（对标 C# :509 EnableCluster 二分）：集群句柄
      // 在位 → 复制域经 on_checkpoint_initiated 给出覆盖位点（PRIMARY 取当前
      // 复制位点并更新提交安全地址，REPLICA 取检查点开始标记位点）；否则直取
      // AOF 原生尾地址向量（C# else 分支 TailAddress——向量本身即全子日志尾，
      // + SetCurrentSafeAofAddress，安全地址由本方法末尾 update_last_save
      // 承接）。严禁折叠为子日志 0 标量：多物理子日志各自独立写入与地址
      // 空间，标量折叠令截断臂对其余子日志下发 0 目标，段文件永不回收
      if let Some(cluster) = self.cluster.get() {
        cluster.on_checkpoint_initiated(&mut covered);
      } else {
        covered = aof.log().tail_address();
      }
      if (0..covered.length() as usize).any(|i| covered.get(i).is_some_and(|a| a > 0)) {
        // C# DatabaseManagerBase.cs:515 文案为 "files deleted after next commit"
        //（逻辑截断 + 提交面删段组合）；rust 提交面不删段，截断走唯一物理回收
        // 真身 truncate_until_async 即时删段，故文案据实反映"检查点落盘后立即回收"
        log::info!(
          "Will truncate AOF to {} right after checkpoint (segments deleted on truncate), db_id = {}",
          covered.to_aof_string(),
          db.id
        );
      }
    }

    // 版本推进窗口与覆盖位点依序定锚（对标 C# 状态机 PREPARE→IN_PROGRESS 段
    // phase 推进与 startLogicalAddress=fuzzyStart 捕获同点完成）：begin_version_shift
    // 内部「load tail（模糊区地板，即版本推进那一刻 tail）→ 单原子 store 窗口开
    // 哨兵+新版本」一次发布——检查点元数据、写路径 IN_NEW_VERSION 标记窗口与
    // AOF 版本戳三者同源同字（对标 C# SystemState 单原子字），恢复内核据同一
    // 地板回滚剔除、由 AOF 重放恰一次承接。窗口晚于 covered 采样开启，保证窗口
    // 写入（undo 剔除面）位点恒 ≥ covered——AOF 截断面绝不吞窗口条目，回滚剔除
    // 与重放承接严格互补
    let index_start = store.begin_version_shift(new_version as u64 & VERSION_MASK);

    // 快照段失败亦收口标记窗（版本推进窗口与快照事务同生命周期；失败轮检查点
    // 未发布，位窗空转无消费方，但绝不允许泄漏到下一轮之外造成无主标记）
    let cp_res = store
      .create_checkpoint_with_token(&db.checkpoint_dir, cp_type, token, index_start)
      .await;
    store.end_version_shift();
    cp_res?;

    // 版本切换结束（对标 C# GlobalBeforeEnteringState WAIT_FLUSH →
    // CheckpointVersionShiftEnd）：快照落盘后、截断前追加 CheckpointEndCommit
    // 标记，副本据此退出模糊区并重放缓冲条目。失败路径不到此处（create_checkpoint_with_token
    // 以 ? 上抛），语义同 C# WAIT_FLUSH 仅在快照成功后进入
    if let Some(cluster) = self.cluster.get() {
      cluster.checkpoint_version_shift_end(new_version);
    }

    if db.aof.is_some() {
      // AOF 边界随检查点元数据持久化：取快照发起时的 covered（快照窗口内
      // 的并发写入使当前尾地址大于覆盖边界，C# CurrentSafeAofAddress 同为
      // 发起时 TailAddress）；快照已提交，补写失败即向上传播不静默
      // 补写完整覆盖位点向量（对标 C# GetCookie 多子日志分支
      // CurrentSafeAofAddress.Serialize(writer) 逐位序列化），严禁
      // get(0) 标量——丢弃子日志 1..N 位点即丢失其恢复期对齐基线
      let covered_vec: Vec<u64> = (0..covered.length() as usize)
        .map(|i| covered.get(i).unwrap_or_default() as u64)
        .collect();
      wcpr::publish_checkpoint_aof_address(&db.checkpoint_dir, token, &covered_vec).await?;
    }

    // 截断层按形态二分（对标 C# :536 的 EnableCluster && EnableAOF 与
    // else 分支层级：C# 该段与 AppendOnlyFile 判空正交，rust 集群形态恒启
    // AOF——复制域依赖 AOF 流，句柄在位即 C# 该合取式为真）：
    // 集群句柄在位 → 复制域登记 CheckpointEntry 历史并经 SafeTruncateAOF
    // 截断（full 恒 true 对齐 rust 统一检查点模型，与副本 attach 按需链
    // take_on_demand_checkpoint 同口径）；否则 TruncateUntil + Commit
    //（C# else 分支，AppendOnlyFile 判空等价）。物理回收真身仍只有
    // truncate_until 一处（safe_truncate_aof 内部亦走它），不造第二套
    if let Some(cluster) = self.cluster.get() {
      if let Some(slow) = cluster.add_new_checkpoint_entry(true, covered, token, token) {
        slow.await;
      }
    } else if let Some(aof) = &db.aof {
      aof.log().truncate_until_async(&covered).await;
      aof.log().commit_async().await;
    }

    db.update_last_save(now_ms());

    // 快照保留回收（对标 C# 检查点状态机 REST 段的 CleanupIndexCheckpoint /
    // CleanupLogCheckpoint 环形 tokenHistory 拍新删旧）：仅单机形态执行——
    // `cluster` 句柄缺位即 C# 侧 removeOutdated = !EnableCluster 为真的同一
    // 形态位（与上方版本切换标记同源判据）。集群形态下快照淘汰由复制域
    // CheckpointStore 读者闸门单轨接管（它按在途读者停手，本处的按条数纯
    // unlink 无此感知力，两轨并行必误删在传快照），故此处让位不触发。
    //
    // SAVE / BGSAVE / AOF 体积超限三条生产链共走本内核，不回收即检查点目录
    // 随打点次数单调增长直至磁盘耗尽；失败只 warn 不回滚已发布快照（C# 的
    // 设备删除同为 best-effort），下一轮检查点再收。回收面只覆盖已发布
    // Token（meta 未落的在途快照不在 list 内），并发下一轮的半成品不受波及
    if self.cluster.get().is_none()
      && let Err(e) = wcpr::purge_outdated(&db.checkpoint_dir, CHECKPOINT_RETAIN_GENERATIONS)
    {
      log::warn!(
        "Failed purging outdated checkpoints in {}: {e}",
        db.checkpoint_dir.display()
      );
    }

    Ok(true)
  }

  /// 按需检查点（管理器入口）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:TakeOnDemandCheckpointAsync
  pub async fn take_on_demand_checkpoint_async(
    &self,
    db: &GarnetDatabase<D>,
    entry_ms: u64,
  ) -> wkv::Result<bool> {
    if db.last_save_ms() > entry_ms {
      return Ok(false);
    }
    self.take_database_checkpoint_async(db).await
  }

  /// 索引溢出超阈值时执行单索引扩容判定与动作（单索引判定内核）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:GrowIndexIfNeededAsync
  ///
  /// 判定 `index_size < index_max_size 且 overflow_count > index_size * threshold / 100`。
  /// 若满足条件则触发翻倍扩容回调。返回索引是否已达上限（`index_size >= index_max_size`）。
  pub async fn grow_index_if_needed<F, G>(
    &self,
    index_max_size: usize,
    overflow_count: u64,
    resize_threshold: i64,
    index_size_retriever: F,
    grow_action: G,
  ) -> wkv::Result<bool>
  where
    F: Fn() -> usize,
    G: Future<Output = wkv::Result<bool>>,
  {
    let current_size = index_size_retriever();
    log::debug!(
      "IndexAutoGrowTask: checking index size {current_size} against max {index_max_size} with overflow {overflow_count}"
    );

    // u128 中间量：threshold 为 i64 CLI 直通（极端值 × usize 索引规模在
    // u64 域回绕出小阈值导致误扩容），u128 乘积域恒不溢出
    let threshold = resize_threshold.max(0) as u64;
    let thresholded_overflow = u128::from(current_size as u64) * u128::from(threshold) / 100;
    if current_size < index_max_size && u128::from(overflow_count) > thresholded_overflow {
      log::info!(
        "IndexAutoGrowTask: overflowCount {overflow_count} ratio more than threshold {threshold}%. Doubling index size..."
      );
      grow_action.await?;
    }

    let final_size = index_size_retriever();
    if final_size < index_max_size {
      return Ok(false);
    }

    // 扩容后复核：确认最终规模确已触及上限（与入场检查日志区分语义）
    log::debug!(
      "IndexAutoGrowTask: post-grow recheck: index size {final_size} reached max {index_max_size} with overflow {overflow_count}"
    );
    Ok(true)
  }

  /// 检查并按需扩容指定数据库的主存储索引
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:GrowIndexesIfNeededAsync
  pub async fn grow_indexes_if_needed(
    &self,
    db: &GarnetDatabase<D>,
    index_max_size: usize,
    resize_threshold: i64,
  ) -> wkv::Result<bool> {
    if db.store_index_maxed_out.load(Acquire) {
      return Ok(true);
    }

    let store = db.store();
    let overflow_count = store.active_index().overflow_pool.allocated_count();
    let maxed_out = self
      .grow_index_if_needed(
        index_max_size,
        overflow_count,
        resize_threshold,
        || store.active_index().size,
        // 扩容含纪元排空忙等与全量分块迁移自旋（大索引秒级），经 compio
        // spawn_blocking 卸载至阻塞线程，reactor 宿主核继续调度其他任务
        grow_index_blocking(Arc::clone(&store)),
      )
      .await?;

    if maxed_out {
      db.store_index_maxed_out.store(true, Release);
    }

    Ok(maxed_out)
  }

  /// AOF 提交：物理刷盘推进 committed_until 至 safe_tail
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:CompactionCommitAofAsync
  ///
  /// C# 该内核与 Single/MultiDatabaseManager.CommitToAofAsync 共同动作即
  /// `db.AppendOnlyFile.Log.CommitAsync()`（物理刷盘推进提交位点，不动
  /// LastSave 时间戳——其仅检查点域推进），rust 合一处
  /// 内核供检查点 compaction 与命令通道（COMMITAOF）复用。
  /// 副本角色收口在子日志提交面唯一角色闸（WaofSublog::commit_flush_async
  /// 副本臂改道 flush-only 纯刷盘不写本地帧），本命令面读同一角色源，
  /// 不设第二角色判定
  pub async fn commit_aof(&self, db: &GarnetDatabase<D>) -> wkv::Result<()> {
    if let Some(aof) = &db.aof {
      aof.log().commit_async().await;
    }
    Ok(())
  }

  /// 全库清空数据（O(1) 物理截断 + AOF 截断至尾）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:FlushAllDatabases
  ///
  /// 原按库 [`wkv::WedbStore::flush_database`] 截断形态（ns=0 硬编码 +
  /// truncate_aof 门控）已并入 [`super::single_database_manager::SingleDatabaseManager`]
  /// flush 三入口（换号清库 + SafeFlushAOF 广播；共享单 AOF 换号路径不物理
  /// 截断，旧记录经逻辑条目 + 延时 GC 承接，C# 集群 safeTruncateAof=false
  /// 分支同形）。SWAPDB 搬移走 wkv `StoreSession::swap_databases` 内核
  /// （经写端口镜像 AOF，绝不截断），不经此路。
  pub async fn flush_all_databases(&self, db: &GarnetDatabase<D>) -> wkv::Result<()> {
    // 换号元数据串行闸在 wkv 单点持有（flush_all_databases 入口持 lock_dbmeta），宿主层不重复加闸仅做 AOF 截断跟随
    db.store().flush_all_databases().await?;
    if let Some(aof) = &db.aof {
      // 截断目标直取 AOF 原生尾地址向量（各子日志向各自真实尾水位回收，
      // 严禁折叠为子日志 0 标量——向量尺寸 1 时 truncate_until_async 对
      // 子日志 1..N 取 unwrap_or(0) 恒不删段）
      let until = aof.log().tail_address();
      aof.log().truncate_until_async(&until).await;
    }
    Ok(())
  }

  /// 重置数据库（拆除重建族：数据清空 + AOF 位点归零 + 复位保存点）
  ///
  /// libs/server/Databases/DatabaseManagerBase.cs:ResetDatabase
  ///
  /// C# 为 `db.Store.Reset()`（TailAddress > 64 时日志地址归零、分配器
  /// 拆除重建）、`Aof Log.Reset()`（位点归零）与 LastSave 归零；rust
  /// 共享存储模型下数据清空等价为 wkv O(1) 虚拟换号，语义差
  /// 由 AOF 段承载——`GarnetLog::reset_async`（经 appendOnlyFile.log()
  /// 直达）位点归零（对标 `Log.Reset()`），区别于
  /// [`Self::flush_all_databases`] 的截断至尾。
  pub async fn reset_database(&self, db: &GarnetDatabase<D>) -> wkv::Result<()> {
    db.store().flush_database(0, db.id.max(0) as u64).await?;
    if let Some(aof) = &db.aof {
      aof.log().reset_async().await;
    }
    db.last_save_ms.store(0, Relaxed);
    db.store_index_maxed_out.store(false, Release);
    Ok(())
  }

  /// 采集单库混合日志内存分布扫描
  ///
  /// 在 garnet 中的相对路径: libs/server/Databases/DatabaseManagerBase.cs:CollectHybridLogStatsForDb
  ///
  /// C# 对主/对象存储各扫一遍（`CollectHybridLogStats(db, db.Store, ...)`）；
  /// wedb 单物理日志 + wcol 信封统一值域，仅 main store 形态，扫描内核
  /// 收敛在 [`WedbStore::hlog_scan_metrics`]（区域 × 状态 × (条数, 字节)）。
  pub async fn collect_hybrid_log_stats_for_db(
    &self,
    db: &GarnetDatabase<D>,
  ) -> wkv::Result<wkv::HybridLogScanMetrics> {
    db.store().hlog_scan_metrics().await
  }

  /// 全库混合日志内存分布统计（基座视角：单库一份）
  ///
  /// 转发至 wkv 扫描内核 [`wkv::WedbStore::hlog_scan_metrics`]，C#
  /// CollectHybridLogStats 的严格映射单点随内核。
  pub async fn collect_hybrid_log_stats(
    &self,
    db: &GarnetDatabase<D>,
  ) -> wkv::Result<Vec<(i64, wkv::HybridLogScanMetrics)>> {
    Ok(vec![(
      db.id,
      self.collect_hybrid_log_stats_for_db(db).await?,
    )])
  }
}
