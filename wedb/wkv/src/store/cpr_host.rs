//! CPR 检查点宿主契约实现与恢复期单趟扫描内核
//! (快照面 1:1 对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:TakeFullCheckpointAsync 与 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotAllTreesForCheckpoint；
//!  恢复面 1:1 对标 libs/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs:RecoverHybridLogAsync 单趟扫描派发
//!  libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnRecoverySnapshotRead)

use std::{
  path::Path,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

use wbase::addr::clean_address;
use wbftree::RangeIndexManager;
use wcpr::Error as WcprError;
use wdev::Device;
use wval::{KeyTag, NamespaceDbCodec};

use super::{KEY_ID_ASSIGN_MARGIN, WedbStore, resize::ResizePhase};
use crate::{
  config::StoreConfig,
  error::{Error, Result},
  range_index::{mark_recovered_patch, patch_stub_record, range_index_stub_of},
};

/// wkv::Error → wcpr::Error 宿主端口映射（零字符串化，全链路类型化）
///
/// wcpr 可表达的基础设施错误（Device/Index/Hlog/Epoch/Io/Cpr）逐变体透明转发；
/// wcpr 不依赖 wbftree 与本 crate（依赖无环），BfTree 快照、RangeIndex 恢复、
/// 配置校验等宿主专属错误经 `wcpr::Error::Host` 透明透传
fn cpr_err(e: Error) -> wcpr::Error {
  match e {
    Error::Device(e) => e.into(),
    Error::Epoch(e) => e.into(),
    Error::HLog(e) => e.into(),
    Error::Index(e) => e.into(),
    Error::Io(e) => e.into(),
    Error::Cpr(e) => e,
    other => WcprError::Host(other.to_string()),
  }
}

impl<D: Device> WedbStore<D> {
  /// 开启 hlog 版本推进窗口并推进存储版本，返回模糊区地板（窗口开启瞬间的
  /// tail 逻辑地址，单原子发布，见 [`whlog::HybridLog::begin_version_shift`]）
  ///
  /// 对标 C# 检查点状态机版本推进相位入口（OnCheckpoint(VersionShift) 后
  /// phase < REST 起全部新记录携带 InNewVersion、版本经 _systemState 单原子
  /// 推进）：宿主调用本口后须等值配对 [`Self::end_version_shift`]，返回值即
  /// 本轮检查点的 `index_start_logical_address`——版本推进瞬间的 tail，恢复
  /// 内核 undoNextVersion 回滚与模糊区重插的同一下界。写侧 AOF 版本戳自
  /// [`Self::write_window_snapshot`] 同字读出，与本口的单原子发布全序衔接。
  #[inline]
  pub fn begin_version_shift(&self, new_version: u64) -> u64 {
    self.hlog.begin_version_shift(new_version)
  }

  /// 关闭 hlog 版本推进窗口（检查点快照段返回后无条件收口，见
  /// [`Self::begin_version_shift`]）
  #[inline]
  pub fn end_version_shift(&self) {
    self.hlog.end_version_shift();
  }

  /// 版本推进窗口开启态查询（写侧脱钩/复活抑制谓词，见
  /// [`whlog::HybridLog::is_version_shift_open`]）
  #[inline]
  pub fn is_version_shift_open(&self) -> bool {
    self.hlog.is_version_shift_open()
  }

  /// 当前模糊区地板读数（窗口关闭期为 0，即本轮检查点 `index_start_logical_address`
  /// 的同源读口，见 [`whlog::HybridLog::version_shift_floor`]）；取槽下界抬升消费
  #[inline]
  pub fn version_shift_floor(&self) -> u64 {
    self.hlog.version_shift_floor()
  }

  /// 创建持久化 Checkpoint 快照
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:TakeFullCheckpointAsync
  ///
  /// 委托 wcpr 实例闸门（[`Self::ckpt_gate`]，对标 C# GarnetDatabase.CheckpointingLock
  /// 逐实例锁）内安全入口（闸内计算目录下界，杜绝锁外读陈旧目录）
  #[inline]
  pub async fn create_checkpoint(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    cp_type: wcpr::CheckpointType,
  ) -> wcpr::Result<wcpr::CheckpointMeta> {
    let meta = wcpr::create_checkpoint(self, &self.ckpt_gate, checkpoint_dir, cp_type).await?;
    self
      .last_checkpointed_version
      .store(self.current_version() as u64, Ordering::SeqCst);
    Ok(meta)
  }

  /// 使用指定 Token 创建持久化 Checkpoint 快照
  ///
  /// 版本切换通知与版本号推进由调用方在快照发起前经预知 Token 显式执行，
  /// 确保快照期间写入即携带新版本号（对标 C# Tsavorite FullCheckpointSM/VersionChangeSM
  /// PREPARE 阶段推进版本；调用点组合杜绝运行时动态分发）。
  /// `index_start_logical_address` 同由调用方给出：必须是版本推进（含
  /// [`Self::begin_version_shift`]）瞬间的 tail，令标记窗口、模糊区地板与检查点
  /// 元数据三者同源，恢复内核据其执行 undoNextVersion 回滚
  #[inline]
  pub async fn create_checkpoint_with_token(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    cp_type: wcpr::CheckpointType,
    token: u128,
    index_start_logical_address: u64,
  ) -> wcpr::Result<wcpr::CheckpointMeta> {
    let meta = wcpr::create_checkpoint_with_token(
      self,
      &self.ckpt_gate,
      checkpoint_dir,
      cp_type,
      token,
      index_start_logical_address,
    )
    .await?;
    self
      .last_checkpointed_version
      .store(self.current_version() as u64, Ordering::SeqCst);
    Ok(meta)
  }

  /// 从指定 Checkpoint 进行崩溃恢复，重构并实例化全新的 WedbStore
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:RecoverAsync
  ///
  /// vdb 映射重建已在 [`Self::from_recovered`] 的恢复期单趟扫描
  /// （[`Self::run_recovery_pass`]）内随日志扫描完成，返回的句柄即
  /// 「映射面已就绪」的凭据；后即行启动对账：回收旁表中已换号死亡域的
  /// 恢复期注册（pending 条目与孤儿数据文件），同名重建索引不被 IndexExists 拦截
  #[inline]
  pub async fn recover(
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
    device: Arc<D>,
  ) -> wcpr::Result<Self> {
    let store: Self = wcpr::recover(checkpoint_dir, token, device).await?;
    store.reclaim_dead_domain_bftrees();
    Ok(store)
  }

  /// 从目录中最新的有效 Checkpoint 执行崩溃恢复
  ///
  /// 对账语义同 [`Self::recover`]
  #[inline]
  pub async fn recover_latest(
    checkpoint_dir: impl AsRef<Path>,
    device: Arc<D>,
  ) -> wcpr::Result<Self> {
    let store: Self = wcpr::recover_latest(checkpoint_dir, device).await?;
    store.reclaim_dead_domain_bftrees();
    Ok(store)
  }

  /// 恢复来源检查点 Token（冷启动 None）
  ///
  /// recover / recover_latest 两路统一在 [`Self::from_recovered`] 一次性记录；
  /// 回退链（recover_latest）选中更早 Token 时，find_latest 已不代表实际恢复
  /// 版本，宿主的版本基线推进与「清未用」回收以本出口为准
  #[inline]
  pub fn recovered_checkpoint_token(&self) -> Option<u128> {
    self.recovered_token.get().copied()
  }

  /// 恢复检查点的 AOF 覆盖位点向量（冷启动空切片 = 无下界）
  ///
  /// 恢复 Token 元数据 `checkpoint_aof_address`（快照发起时各物理子日志的
  /// AOF 覆盖边界，对标 C# 检查点 cookie 的 CurrentSafeAofAddress /
  /// RecoveredSafeAofAddress 全向量形态）。AOF 重放扫描以此为位点下界，按
  /// 条目所属物理子日志逐位比对：位点严格小于其所属子日志下界的条目必先于
  /// 快照发起、其效果已物化进快照，重放跳过（版本闸对其天然成立——covered
  /// ≤ 开窗 ≤ 版本推进锚点全序保证位点 < covered ⇒ 版本恒旧；位点闸为截断
  /// 前异常宕机时的纵深防御，防版本戳异常条目的重复重放）。补写窗内崩溃
  /// （字段 None）退化为空，全量重放由版本闸单面承接
  #[inline]
  pub fn recovered_aof_floor(&self) -> &[u64] {
    self
      .recovered_aof_floor
      .get()
      .map(Vec::as_slice)
      .unwrap_or(&[])
  }

  /// 遍历并为所有在线与待激活的 RangeIndex 执行 CPR 检查点快照落盘（调用 snapshot_all_trees_to_dir）
  pub fn take_range_index_checkpoints(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
  ) -> Result<usize> {
    Ok(
      self
        .range_index
        .snapshot_all_trees_to_dir(checkpoint_dir.as_ref(), token)?,
    )
  }

  /// 设置 RangeIndex 检查点屏障（对标 C# CheckpointTrigger::VersionShift →
  /// SetCheckpointBarrier）：wcpr 检查点在一致性截断点捕获之前调用，阻塞全部
  /// BfTree 树写入直至快照段内部配对清屏，保证 RI 快照与 hlog tail 刚性对齐
  pub fn set_range_index_checkpoint_barrier(&self) {
    self.range_index.set_checkpoint_barrier();
  }

  /// 清除 RangeIndex 检查点屏障（幂等；wcpr 失败清场路径兜底）
  pub fn clear_range_index_checkpoint_barrier(&self) {
    self.range_index.clear_checkpoint_barrier();
  }

  /// 恢复期唯一一次有序日志扫描（wcpr::CprRecover::from_recovered 契约的
  /// 宿主侧驱动点）：单趟 `[begin, tail)` 扫描同趟完成旧实现三次独立全扫的
  /// 全部工作。C# 侧 Recovery/Recovery.cs 的 RecoverHybridLogAsync 单趟扫描经
  /// ClearBitsOnPage 派发 GarnetRecordTriggers.cs 的 OnRecoverySnapshotRead 逐记录
  /// 回调，该融合形态的两枚符号锚点 1:1 挂在扫描内核
  /// `wcpr::manager::recover::run_recovery_kernel`（本函数只驱动内核并做 RI 桩
  /// 结算），此处不复挂：
  ///
  /// 1. **模糊区回滚（undoNextVersion）与重插** + 逐记录素材收集：由
  ///    [`wcpr::run_recovery_kernel`] 驱动，回调 [`RecoveryPassVisitor`] 同趟完成
  ///    DbMeta 映射重建（复用冷启动独立重建 [`Self::rebuild_vdb_async`] 的同一单条
  ///    工序 [`Self::rebuild_vdb_visit`] 与同一收尾 [`Self::finish_vdb_rebuild`]）
  ///    并收集 RI 桩候选；被回滚的新版记录不回调，其效果由宿主 AOF 重放恰一次承接；
  /// 2. **RI 桩结算**：对扫描收集到的存活桩候选按物理键分组，逐键反查恢复出的
  ///    哈希索引槽位（桶 + 溢出链纯内存查表，与「全索引枚举 ∩ 候选地址集合」
  ///    同果）执行存根自愈与注册。旧实现在此处逐索引条目
  ///    `read_record` 随机读日志（O(N) 次设备读），现收敛为扫描期一次顺序读 +
  ///    结算期内存查表，无任何按-key 点读残留。
  ///
  /// 结算判活唯一取「被索引引用」：候选帧地址须恰为该键索引槽位当前指向地址
  ///  才落笔——空槽 / tentative 槽位被 [`windex::HashIndex::find_tag`] 天然滤除，
  ///  盲追加墓碑重插后槽位已迁往墓碑地址、原位墓碑不再作候选，都落不到死帧上，
  ///  与旧遍历的全部过滤（空槽、tentative、addr < begin）等价；ReadCache 折链
  ///  分支删除——快照写出前 sanitize_data_slot 已把 RC 条目重写为主日志地址
  ///  并清退残留 RC 槽位，恢复出的索引不可能含 RC 地址，且此刻读缓存尚未启用。
  ///  绝不按「扫描序最后一条/最高地址」判活：复活池可能让索引指向更低地址的
  ///  复活记录、同时高位残留旧桩死帧（SEALED 位纯易失，盘上帧无从判别），该判据
  ///  会让存活帧漏落 mark_recovered、死帧反被改写重挂；与槽位对齐后「恢复路由
  ///  所指必为快照恢复态」，与 C# 快照区逐记录无条件置位
  ///  （GarnetRecordTriggers.cs:OnRecoverySnapshotRead →
  ///  RangeIndexManager.Index.cs:MarkRecoveredFromCheckpoint，死帧上的位无读方）
  ///  可观测语义一致，全链路仅此一套判活机制。
  pub(super) async fn run_recovery_pass(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
    index_start: u64,
  ) -> Result<usize> {
    let dir = checkpoint_dir.as_ref();

    // 1. 先从目标检查点目录预置所有 .bftree 快照物理文件并注册 pending 条目
    //    （纯文件目录操作，不涉日志，先于扫描保持旧实现次序）
    let mut count = self.range_index.recover_all_trees_from_dir(dir, token)?;

    // 2. 单趟有序扫描：(0,0) 根域先强制初始化（与 rebuild_vdb_async 同口径），
    //    写者已冻结仍按扫描内核纪元契约进入临界区
    self.vdb.get_or_create_db(0, 0);
    let begin_addr = self.begin_address();
    let tail_addr = self.hlog.tail_address();
    let mut visitor = RecoveryPassVisitor {
      store: self,
      max_vid: 0,
      healed_count: 0,
      stubs_to_heal: Vec::new(),
    };
    {
      let participant = self.epoch.register()?;
      let _guard = self.barrier_enter(&participant);
      let index = self.active_index();
      wcpr::run_recovery_kernel(
        &self.hlog,
        &index,
        begin_addr,
        index_start,
        tail_addr,
        // undoNextVersion 默认启用（对标 C# RecoveryOptions.undoNextVersion
        // 缺省 true）：模糊区内携带纪元位的新版记录回滚剔除，效果由 AOF 重放承接
        true,
        &mut visitor,
      )
      .await?;
    }
    let RecoveryPassVisitor {
      max_vid,
      healed_count,
      mut stubs_to_heal,
      ..
    } = visitor;

    count += healed_count;

    // 3. RI 桩自愈结算：唯一判活「被索引引用」——候选按物理键分组后逐键反查
    //    恢复出的活跃哈希索引槽位，仅槽位当前指向的候选地址落笔标记
    //    （handle=0 + recovered）；未被索引引用的陈旧死帧一律跳过，不改写、
    //    不追加重挂（复活池低地址指向场景，见函数头注释判活论述）
    if !stubs_to_heal.is_empty() {
      let index = self.active_index();
      stubs_to_heal.sort_unstable_by(|a, b| a.1.cmp(&b.1));
      for group in stubs_to_heal.chunk_by(|a, b| a.1 == b.1) {
        let meta_k = &group[0].1;
        let Some(live) = index.find_tag(meta_k) else {
          continue;
        };
        let Some(&(addr, _)) = group.iter().find(|(a, _)| *a == live) else {
          continue;
        };
        let modified = self
          .hlog
          .try_modify_resident_record_in_place(addr, meta_k, |val| {
            Some(patch_stub_record(val, mark_recovered_patch))
          })
          .map_err(|e| cpr_err(Error::HLog(e)))?;
        if modified != Some(true) {
          // 驻留页驱逐的罕见冷数据：读出记录、修补存根、追加至尾部并 CAS 挂载
          if let Ok(rec) = self.hlog.read_record(addr).await
            && let Ok(val) = rec.value()
          {
            let mut frame = val.to_vec();
            if patch_stub_record(&mut frame, mark_recovered_patch)
              && let Ok((new_addr, _)) = self.hlog.append(meta_k, frame.as_slice(), addr, false)
            {
              index.update_address(meta_k, addr, new_addr);
            }
          }
        }
      }
    }

    // 4. vdb 重建收尾：水位折叠与代数推进成对（与 rebuild_vdb_async 同一
    //    收尾函数，顺序写死）
    self.finish_vdb_rebuild(max_vid);
    Ok(count)
  }
}

/// 恢复期融合扫描的逐记录收集器（[`wcpr::RecoveryVisitor`] 的 wkv 宿主实现）
struct RecoveryPassVisitor<'a, D: Device> {
  store: &'a WedbStore<D>,
  /// vdb 分配水位折叠值（与 rebuild_vdb_async 的 max_vid 同源同义）
  max_vid: u64,
  /// 扫描中自愈注册成功计数
  healed_count: usize,
  /// 待自愈的 RI 存根候选 `(逻辑地址, 物理键)`（结算段按「被索引引用」判活落笔）
  stubs_to_heal: Vec<(u64, Vec<u8>)>,
}

impl<D: Device> wcpr::RecoveryVisitor for RecoveryPassVisitor<'_, D> {
  fn on_record(&mut self, addr: u64, key: &[u8], value: &[u8], is_tombstone: bool) {
    // DbMeta：与冷启动独立重建共用单条工序，按扫描序原位应用
    self
      .store
      .rebuild_vdb_visit(addr, key, value, is_tombstone, &mut self.max_vid);

    // 墓碑直接返回：其键的判活在结算段由「被索引引用」自然承接——盲追加墓碑
    // 重插后槽位已迁往墓碑地址、原位墓碑本身不作候选，无需旧式按-key 剔除
    if is_tombstone {
      return;
    }

    // RI 桩候选识别门与旧遍历记录侧判定完全一致（KeyTag::Meta 载荷 + 35B 桩布局）
    if let Some(stub) = range_index_stub_of(value)
      && let Ok((vns, vdb, KeyTag::Meta, user_key)) = NamespaceDbCodec::decode_tagged_key(key)
    {
      // 在 RangeIndexManager 中注册 pending 条目 (tree=None，惰性恢复)
      if self.store.range_index.register_pending(key) {
        self.healed_count += 1;
      }
      // 恢复期登记换号回收旁表
      self.store.register_bftree_key(vns, vdb, user_key);
      if stub.tree_handle != 0 || !stub.is_recovered() {
        self.stubs_to_heal.push((addr, key.to_vec()));
      }
    }
  }
}

impl<D: Device> wcpr::CprStore for WedbStore<D> {
  type Device = D;

  #[inline]
  fn hlog(&self) -> &whlog::HybridLog<D> {
    &self.hlog
  }

  #[inline]
  fn index(&self) -> Arc<windex::HashIndex> {
    self.active_index()
  }

  #[inline]
  fn is_growing(&self) -> bool {
    self.is_growing()
  }

  /// 扩容中止遗留撕裂索引待重建上报（票 zcode-r135c-rehash 案二）：检查点
  /// 入口 `ensure_not_growing` 据此在标记存续期拒发索引快照，杜绝撕裂空表
  /// 被落盘固化成永久数据缺失
  #[inline]
  fn index_rebuild_pending(&self) -> bool {
    self.index_rebuild_pending()
  }

  /// 进入检查点临界区：CAS 把扩容相位由 Rest 原子推进为 Checkpoint
  ///
  /// 对标 C# StateMachineDriver.cs:167-190 单槽注册——检查点状态机与扩容状态机
  /// 抢同一槽位，槽位被占（扩容 PrepareGrow/InProgressGrow 进行中，或检查点
  /// 已在临界区）CAS 立即失败返回 `Error::Host` 拒绝，即「发起即失败」。
  /// 自此至配对 [`Self::exit_checkpoint`] 期间，`grow_index` 的
  /// Rest→PrepareGrow CAS 天然失败返回 `Ok(false)`，快照窗口与切表窗口互斥，
  /// 索引快照 `index_meta.size` 与 `store_meta.index_size` 不再撕裂
  #[inline]
  fn enter_checkpoint(&self) -> wcpr::Result<()> {
    self
      .resize
      .phase
      .compare_exchange(
        ResizePhase::Rest as u8,
        ResizePhase::Checkpoint as u8,
        Ordering::AcqRel,
        Ordering::Acquire,
      )
      .map(|_| ())
      .map_err(|_| {
        WcprError::Host(
          "哈希索引扩容或另一检查点占用状态机槽位，拒绝发起 Checkpoint（单槽互斥），稍后重试"
            .into(),
        )
      })
  }

  /// 退出检查点临界区：仅当槽位仍为本检查点所置 Checkpoint 相位时条件复位 Rest
  ///
  /// 对标 C# StateMachineDriver.cs:345-362 单次清槽契约——清槽者恒为槽位当前
  /// 持有者，绝不踢出后来注册者。临界期内 `grow_index` 的 Rest→PrepareGrow CAS
  /// 恒失败，但成功路径显式退出后、`CkptPhaseGuard` Drop 二次复位前，扩容（不
  /// 走检查点闸门）可抢占槽位进入 PrepareGrow：无条件 store 会把 PrepareGrow
  /// 打回 Rest 而扩容状态机仍在运行，事务屏障（`try_acquire_txn`/`barrier_enter`
  /// 以 phase == PrepareGrow 为拦截判据）与切表序随之失效。故复位改为仅清自身
  /// 所置相位的 CAS，对 Rest/PrepareGrow/InProgressGrow 一律 no-op，显式退出与
  /// Drop 兜底双调幂等。内存序统一 SeqCst：与 [`crate::store::WedbStore::grow_index`]
  /// 相位 CAS 及 `IndexResizeState::try_acquire_txn` 的 Dekker 配对共享同一全序，
  /// 不留「读到被覆盖前的陈旧相位」推理缝
  #[inline]
  fn exit_checkpoint(&self) {
    let _ = self.resize.phase.compare_exchange(
      ResizePhase::Checkpoint as u8,
      ResizePhase::Rest as u8,
      Ordering::SeqCst,
      Ordering::SeqCst,
    );
  }

  #[inline]
  fn epoch(&self) -> &wepoch::LightEpoch {
    &self.epoch
  }

  #[inline]
  fn tail_address(&self) -> u64 {
    self.tail_address()
  }

  #[inline]
  fn begin_address(&self) -> u64 {
    self.begin_address()
  }

  #[inline]
  fn head_address(&self) -> u64 {
    self.head_address()
  }

  #[inline]
  fn shift_read_only_address(&self, target: u64) {
    self.shift_read_only_address(target);
  }

  #[inline]
  async fn flush_all(&self) -> wcpr::Result<()> {
    self.flush_all().await.map_err(cpr_err)
  }

  #[inline]
  fn entry_count(&self) -> usize {
    self.entry_count()
  }

  #[inline]
  fn skip_read_cache_with_wait(&self, slot: &AtomicU64) -> u64 {
    // 端口体单点：直转带等待的走查内核（对标 C# SkipReadCache 每步判定当前位置 +
    // RestartChain 重读哈希项）。链头以**重读槽位**取得——清洗方以槽位 CAS 换指主日志
    // 地址后重探即得新值，故存活槽位永不折成 0；紧缩面 `wkv/src/compact.rs` 同一内核
    self.read_cache.skip_read_cache_with_wait(
      || clean_address(slot.load(Ordering::Acquire)),
      || self.epoch.drain(),
    )
  }

  #[inline]
  fn take_range_index_checkpoints(&self, dir: &Path, token: u128) -> wcpr::Result<usize> {
    self
      .take_range_index_checkpoints(dir, token)
      .map_err(cpr_err)
  }

  #[inline]
  fn set_range_index_checkpoint_barrier(&self) {
    self.set_range_index_checkpoint_barrier();
  }

  #[inline]
  fn clear_range_index_checkpoint_barrier(&self) {
    self.clear_range_index_checkpoint_barrier();
  }

  #[inline]
  fn checkpoint_store_meta(&self) -> wcpr::StoreMeta {
    wcpr::StoreMeta {
      index_size: self.active_index().size,
      page_size: self.config.page_size,
      num_pages: self.config.num_pages,
      mutable_fraction: self.config.mutable_fraction,
      max_sessions: self.config.max_sessions,
      enable_revivification: self.config.enable_revivification,
      enable_read_cache: self.config.enable_read_cache,
      read_cache_num_pages: self.config.read_cache_num_pages,
      range_index_dir: self
        .config
        .range_index_dir
        .as_ref()
        .map(|p| p.to_string_lossy().into_owned()),
      next_key_id: self.next_key_id.load(Ordering::Relaxed),
    }
  }
}

impl<D: Device> wcpr::CprRecover for WedbStore<D> {
  /// 从恢复组件装配引擎（容量契约：恢复配置完全由检查点 StoreMeta 决定）
  ///
  /// `index_size`/`page_size`/`num_pages` 等核心容量一律取自持久化 StoreMeta
  /// 重建配置，索引按快照原样恢复重建（wcpr 已做 meta 与快照严格相等校验）——
  /// 恢复入口没有"用户本次配置"通道，绝不按调用方意愿缩表。
  /// [`Self::from_components`] 内部再做 config 与实际索引容量一致性预检兜底。
  async fn from_recovered(
    recovered: wcpr::RecoveredCheckpoint<D>,
    checkpoint_dir: &Path,
    device: Arc<D>,
  ) -> wcpr::Result<Self> {
    let recovered_token = recovered.meta.token;
    let mut config = StoreConfig::new(
      recovered.meta.store_meta.index_size,
      recovered.meta.store_meta.page_size,
      recovered.meta.store_meta.num_pages,
      recovered.meta.store_meta.mutable_fraction,
    )
    .map_err(cpr_err)?
    .with_max_sessions(recovered.meta.store_meta.max_sessions)
    .map_err(cpr_err)?;

    if recovered.meta.store_meta.enable_revivification {
      config = config.with_revivification(true);
    }
    if recovered.meta.store_meta.enable_read_cache {
      config = config
        .with_read_cache(true)
        .with_read_cache_pages(recovered.meta.store_meta.read_cache_num_pages)
        .map_err(cpr_err)?;
    }
    if let Some(p) = &recovered.meta.store_meta.range_index_dir {
      config = config.with_range_index_dir(p);
    }

    let store = Self::from_components(
      config,
      recovered.index,
      recovered.hlog,
      recovered.epoch,
      device,
    )
    .map_err(cpr_err)?;
    // 恢复来源 Token 一次性记录（OnceLock 保首，重复装配的 set 失败不可能——
    // 实例为本函数新建）。recover / recover_latest 两路统一经此携带，宿主
    // 版本基线与「清未用」回收以它为准
    let _ = store.recovered_token.set(recovered_token);
    // 恢复检查点的 AOF 覆盖位点向量一次性记录（创建时未知 AOF 域、由宿主
    // 快照发布后补写；补写窗内崩溃则 None 退化为空——AOF 重放扫描位点下界
    // 的唯一持久来源，按物理子日志逐位读，见 recovered_aof_floor 读口）
    let _ = store.recovered_aof_floor.set(
      recovered
        .meta
        .checkpoint_aof_address
        .clone()
        .unwrap_or_default(),
    );
    store.raise_key_id_floor(
      recovered
        .meta
        .store_meta
        .next_key_id
        .saturating_add(KEY_ID_ASSIGN_MARGIN),
    );
    // 物理删段地板 = 恢复检查点重放窗下界（启动恢复与副本在线导入
    // recover_from_token 同路覆盖；对标 C# OnRecovery 后由下一检查点发布的
    // CleanupLogCheckpoint 接管：此处只抬地板不补删，窗下存量残留段待下一
    // 发布点补收）
    store
      .hlog
      .raise_delete_floor(recovered.meta.hlog_meta.begin_address);
    // 恢复期唯一一次有序扫描内核（wcpr::CprRecover::from_recovered 契约：
    // 主机恰好调用一次）：模糊区重插 + RI 桩自愈注册 + DbMeta 映射重建合一
    store
      .run_recovery_pass(
        checkpoint_dir,
        recovered.meta.token,
        recovered.meta.index_start_logical_address,
      )
      .await
      .map_err(cpr_err)?;

    Ok(store)
  }

  /// 回退清场（[`wcpr::CprRecover::discard_partial_recovery`] 的宿主实现）：
  /// 删除失败轮已从快照拷贝的 RI 树工作文件
  ///
  /// 本轮注册的 pending 条目随失败实例析构自动摘除，无需处理；磁盘工作文件
  /// 按 Token 候选目录口径定位转调 [`RangeIndexManager::discard_staged_data_files`]。
  /// range_index_dir 缺席（临时目录形态）时恢复轮工作目录为进程私有临时目录，
  /// 进程内无跨轮残留，无事可做
  fn discard_partial_recovery(checkpoint_dir: &Path, token: u128, store_meta: &wcpr::StoreMeta) {
    let Some(ri_dir) = store_meta.range_index_dir.as_deref() else {
      return;
    };
    // 与 WedbStore::init_range_index 同构派生：{range_index_dir}/rangeindex
    // 工作根、{range_index_dir}/checkpoints 快照根
    let ri_dir = Path::new(ri_dir);
    let removed = RangeIndexManager::discard_staged_data_files(
      &ri_dir.join("rangeindex"),
      checkpoint_dir,
      &ri_dir.join("checkpoints"),
      token,
    );
    if removed > 0 {
      log::warn!("恢复回退清场：摘除失败轮预置的 RI 工作文件 {removed} 个 (token={token:#x})");
    }
  }
}
