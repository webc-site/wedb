//! CPR 检查点宿主契约实现与恢复期单趟扫描内核
//! (快照面 1:1 对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:TakeFullCheckpointAsync 与 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotAllTreesForCheckpoint；
//!  恢复面 1:1 对标 libs/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs:RecoverHybridLogAsync 单趟扫描派发
//!  libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnRecoverySnapshotRead)

use std::{
  path::Path,
  sync::{Arc, atomic::Ordering},
};

use gxhash::HashMap;
use wcpr::Error as WcprError;
use wdev::Device;
use windex::{HashBucket, HashBucketEntry};
use wval::NamespaceDbCodec;

use super::{KEY_ID_ASSIGN_MARGIN, WedbStore};
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
  /// 创建持久化 Checkpoint 快照
  ///
  /// libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Tsavorite.cs:TakeFullCheckpointAsync
  ///
  /// 委托 wcpr 进程级闸门内安全入口（闸门内计算目录下界，杜绝锁外读陈旧目录）
  #[inline]
  pub async fn create_checkpoint(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    cp_type: wcpr::CheckpointType,
  ) -> wcpr::Result<wcpr::CheckpointMeta> {
    wcpr::create_checkpoint(self, checkpoint_dir, cp_type).await
  }

  /// 使用指定 Token 创建持久化 Checkpoint 快照
  ///
  /// 版本切换通知与版本号推进由调用方在快照发起前经预知 Token 显式执行，
  /// 确保快照期间写入即携带新版本号（对标 C# Tsavorite FullCheckpointSM/VersionChangeSM
  /// PREPARE 阶段推进版本；调用点组合杜绝运行时动态分发）
  #[inline]
  pub async fn create_checkpoint_with_token(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    cp_type: wcpr::CheckpointType,
    token: u128,
  ) -> wcpr::Result<wcpr::CheckpointMeta> {
    wcpr::create_checkpoint_with_token(self, checkpoint_dir, cp_type, token).await
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
  /// 1. **模糊区重插** + 逐记录素材收集：由 [`wcpr::run_recovery_kernel`] 驱动，
  ///    回调 [`RecoveryPassVisitor`] 同趟完成 DbMeta 映射重建（复用冷启动
  ///    独立重建 [`Self::rebuild_vdb_async`] 的同一单条工序
  ///    [`Self::rebuild_vdb_visit`] 与同一收尾 [`Self::finish_vdb_rebuild`]）
  ///    并收集 RI 桩候选；
  /// 2. **RI 桩结算**：对恢复出的哈希索引（桶 + 溢出链）做纯内存枚举，与收集
  ///    到的地址集合取交集执行存根自愈与注册。旧实现在此处逐索引条目
  ///    `read_record` 随机读日志（O(N) 次设备读），现收敛为扫描期一次顺序读 +
  ///    结算期内存查表，无任何按-key 点读残留。
  ///
  /// 结算枚举保留旧遍历的全部过滤（空槽、tentative、addr < begin）；ReadCache
  /// 折链分支删除——快照写出前 sanitize_data_slot 已把 RC 条目重写为主日志地址
  /// 并清退残留 RC 槽位，恢复出的索引不可能含 RC 地址，且此刻读缓存尚未启用。
  /// 地址集合交集而非「扫描序最后一条」判活：复活池可能让索引指向更低地址的
  /// 复活记录，同时高位残留旧桩框架，只有「被索引引用」这一判据与旧遍历等价。
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
    let participant = self.epoch.register()?;
    let _guard = self.barrier_enter(&participant);
    let index = self.active_index();
    let mut visitor = RecoveryPassVisitor {
      store: self,
      max_vid: 0,
      stub_records: HashMap::default(),
    };
    wcpr::run_recovery_kernel(
      &self.hlog,
      &index,
      begin_addr,
      index_start,
      tail_addr,
      &mut visitor,
    )
    .await?;
    let RecoveryPassVisitor {
      max_vid,
      stub_records,
      ..
    } = visitor;

    // 3. RI 桩结算：与旧遍历同一枚举顺序（桶序 → 槽位序 → 溢出链），同一
    //    注册/治愈副作用序列，仅把逐条随机读换成扫描期收集结果的内存交集
    for bucket in index.buckets.iter() {
      let mut curr_bucket = bucket;
      loop {
        for item in curr_bucket.entries.iter().take(HashBucket::DATA_ENTRIES) {
          let raw = item.load(Ordering::Acquire);
          if raw == 0 {
            continue;
          }
          let entry = HashBucketEntry::from_raw(raw);
          if entry.is_tentative() {
            continue;
          }
          let addr = entry.address();
          if addr < begin_addr {
            continue;
          }
          let Some((key, val)) = stub_records.get(&addr) else {
            continue;
          };
          let Some(user_key) = NamespaceDbCodec::decode_meta_user_key(key) else {
            continue;
          };

          // 存根自愈：转调 wkv 唯一治愈内核（对标 C# in-span 单点变更器
          // libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:MarkRecoveredFromCheckpoint），
          // 在等长堆副本上就地改 35B 存根窗口——值体长度无上限，存根之后的扩展
          // 字段原样保留，杜绝旧实现在此的 [u8;128] 私有副本与 Vec 回退双口径。
          // 内核返回 false = 持久化字节已是自愈态 (句柄已清零 + 恢复位已置)，
          // 零写跳过：多轮恢复的重复回写纯属浪费——原位改写退化为同址重写，
          // 失败路径还会多出一次追加 + 索引地址更新；跳过仅省写副作用，注册
          // 副作用照常执行
          let mut healed = val.to_vec();
          if patch_stub_record(&mut healed, mark_recovered_patch)
            && !self.hlog.try_update_in_place(addr, key, &healed)?
          {
            let new_addr = self.hlog.append(key, &healed, addr, false)?;
            index.update_address(key, addr, new_addr);
          }

          // 在 RangeIndexManager 中注册 pending 条目 (tree=None，惰性恢复)
          if self.range_index.register_pending(user_key) {
            count += 1;
          }

          // 恢复期登记换号回收旁表：域 (vns, vdb) 自物理键前缀解出。登记与
          // DbMeta 映射重建在本趟扫描中互不读取对方状态（登记为纯旁表追加，
          // 重建为映射/账本写入），判死统一留到 reclaim_dead_domain_bftrees
          // 据角色精确对账销毁（否则死亡域残留 pending 注册令同名重建被拦截）
          if let Ok((vns, vdb, ..)) = NamespaceDbCodec::decode_tagged_key(key) {
            self.register_bftree_key(vns, vdb, user_key);
          }
        }

        let overflow_idx = curr_bucket.overflow_index();
        if overflow_idx == 0 {
          break;
        }
        match index.overflow_pool.get(overflow_idx) {
          Some(next) => curr_bucket = next,
          None => break,
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
  /// RI 桩候选：记录地址 -> (键, 值) 原始字节；结算期与恢复出的索引地址集合
  /// 取交集，只治愈确被索引引用的那一条
  stub_records: gxhash::HashMap<u64, (Vec<u8>, Vec<u8>)>,
}

impl<D: Device> wcpr::RecoveryVisitor for RecoveryPassVisitor<'_, D> {
  fn on_record(&mut self, addr: u64, key: &[u8], value: &[u8], is_tombstone: bool) {
    // DbMeta：与冷启动独立重建共用单条工序，按扫描序原位应用
    self
      .store
      .rebuild_vdb_visit(addr, key, value, is_tombstone, &mut self.max_vid);
    // RI 桩候选识别门与旧遍历记录侧判定完全一致（KeyTag::Meta 载荷 + 35B 桩布局）
    if NamespaceDbCodec::decode_meta_user_key(key).is_some() && range_index_stub_of(value).is_some()
    {
      self
        .stub_records
        .insert(addr, (key.to_vec(), value.to_vec()));
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
  fn skip_read_cache(&self, addr: u64) -> u64 {
    // 端口体单点：见 crate::read_cache 的 skip_read_cache_addr（检查点 resolve_slot
    // 已有断链重读复核，cleanse_page 承诺页复用前恢复槽位至主日志地址）
    self.read_cache.skip_read_cache_addr(addr)
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
    store.raise_key_id_floor(
      recovered
        .meta
        .store_meta
        .next_key_id
        .saturating_add(KEY_ID_ASSIGN_MARGIN),
    );
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
}
