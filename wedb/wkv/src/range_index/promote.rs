use std::{
  result::Result as StdResult,
  sync::{Arc, atomic::Ordering},
};

use wbftree::{
  BfTreeInsertResult, Error as WbftreeError, RangeIndexManager, RangeIndexStub, StorageBackendType,
  TreeTuning,
};
use wdev::Device;
use whasher::fast_hash;
use wval::{GarnetObjectType, KeyTag, MetaValue, TaggedKeyBuf};

use super::{
  RangeIndexError, discard_snapshot_file,
  heal::{TailPatchOutcome, clear_flushed_patch, patch_stub_record, transfer_out_patch},
  range_index_blocking,
};
use crate::{
  error::{CollectionError, Error, Result},
  session::StoreSession,
  store::StoreEvent,
};

/// 自迁移安全换入窗守卫（RAII）：持有期间同键迁移 claim 在册（封堵语义见
/// [`StoreSession::try_swap_in_window`]），Drop 幂等释放。只持管理器句柄与树
/// 身份键（物理 Meta 键，跨库同名键按物理域隔离），零生命周期参数——可随物化
/// 载荷穿线交调用方持跨「求值 → 写回收尾」，守卫存活即封窗存活
pub struct SwapInWindowGuard {
  manager: Arc<RangeIndexManager>,
  id_key: TaggedKeyBuf,
}

impl Drop for SwapInWindowGuard {
  fn drop(&mut self) {
    self.manager.release_migration_claim(&self.id_key);
  }
}

impl<D: Device> StoreSession<D> {
  /// 将集合就地升阶为 BfTree 分页分层态（首升阶与分层重灌共用同一建树+换入内核）
  ///
  /// 先建后拆单内核 [`build_collection_tree_snapshot`](wbftree::RangeIndexManager)：
  /// 建树与整批装载合并在一次阻塞线程卸载内完成（全批次单次引擎借用、条目栈上
  /// 排序后集中命中相邻页，杜绝逐条 insert 的 N 次借用与页缓存抖动），产物为
  /// migration-tmp 下的 CPR 快照文件；随后在键条带写锁内经
  /// [`publish_tree_from_snapshot_locked`](wbftree::RangeIndexManager) rename 原子
  /// 换入正式数据路径。`replace` 分流两态：首升阶 false（无旧树，IndexExists
  /// 防重门在换入锁内裁决）；分层重灌 true（旧树锁内摘除并延迟释放，换入窗口
  /// 无文件缺失间隙，杜绝旧「先 drain 销毁再原位重建」形态下 drain 成功、
  /// promote 失败的键蒸发窗口）。meta.size 由装载内核返回的去重条数一次性回写
  /// (单次元数据落盘)。65536+ 条目升阶与分层重灌路径同源，重灌经
  /// obj_writeback_tiered → apply_rmw_post_operate 汇入本函数。
  ///
  /// 写序不变量：建树 → 先发 RangeIndexStream 数据流 → 原子换入 → 落元记录 →
  /// 删信封。数据流先行于换入与 meta：入队失败即弃快照残件上抛，此刻新树未换入、
  /// 旧态（旧树 / 旧 meta / 信封 / 键级 TTL）分毫未动，回滚即原态；副本据流块以
  /// 同 replace 标志重放发布（重灌流不再前导 RangeIndexDrop，旧树由 replace 换入
  /// 承接）。换入后至元记录落盘前崩溃仅余「新树 + 旧 meta」滞后态——键仍可读
  /// （读臂惰性恢复按数据文件收敛、计数校正兜底 size 滞后），崩溃窗口自「数据
  /// 丢失」收敛为「新旧快照之一」，对齐 C# 对象记录重写单日志记录原子、
  /// HasExpiration 前移零 TTL 事件的口径（ObjectStore/VarLenInputMethods.cs:
  /// GetRMWModifiedFieldInfo）。
  ///
  /// `next_expiry` 为灌入批的最早成员到期刻度（调用方单点算好传入：升阶/重灌臂
  /// 经 wnode [`earliest_expiry`](wnode::resp::objects::tiered_collection_ops) /
  /// 分层到期重灌臂经扫描期已重算的水位；`i64::MAX` = 无成员挂 TTL）。重灌是
  /// 换树不换内容，水位若在重建时归 MAX，成员级 TTL 计数校正（HLEN/ZCARD 的
  /// `now <= next_expiry` 快路径）与周期收集任务会被「无 TTL」假水位骗过，已
  /// 到期成员永不出账、计数虚高——故水位必须随灌入批在同一元记录落盘内前移。
  pub async fn promote_collection_to_bftree(
    &self,
    key: &[u8],
    obj_type: GarnetObjectType,
    entries: Vec<(Vec<u8>, Vec<u8>)>,
    next_expiry: i64,
    replace: bool,
  ) -> Result<()> {
    // 升阶建树调参：min_record_size 取引擎硬下限 2 (集合条目常短于 RI.CREATE
    // 的 64B 引擎记录下限，详见 TreeTuning::DEFAULT_RI_COLLECTION 注释)
    let mut tuning = TreeTuning::DEFAULT_RI_COLLECTION;
    RangeIndexManager::resolve_tuning(&mut tuning);

    // 建树 + 装载 + CPR 快照：scratch 树全程不入注册表、不触目标键数据文件，
    // 旧树（重灌态）原态可读
    let mgr = Arc::clone(&self.store.range_index);
    let build_key = key.to_vec();
    let entry_count = entries.len();
    let (snapshot_path, count) = range_index_blocking(move || {
      mgr
        .build_collection_tree_snapshot(&entries, &tuning)
        .map_err(|e| -> Error {
          match e {
            // 装载被拒整树作废：内核前置校验保证失败发生在任何写入之前，scratch
            // 工作文件已由建树内核就地回收，注册表与旧态零触碰
            WbftreeError::LoadRejected(BfTreeInsertResult::InvalidKV) => {
              log::error!(
                "promote_collection_to_bftree 装载被拒 (键值违反长度契约): key={build_key:?}, entries={entry_count}"
              );
              CollectionError::KeyTooLong.into()
            }
            WbftreeError::LoadRejected(res) => {
              log::error!(
                "promote_collection_to_bftree 装载失败 (引擎参数非法): key={build_key:?}, entries={entry_count}, res={res:?}"
              );
              CollectionError::InvalidArgument("invalid arguments for tree insert").into()
            }
            e => e.into(),
          }
        })
    })
    .await??;

    // 数据通道发布存根：树句柄取 0 的瞬态形态——消费侧（副本回放 / 迁移接收）
    // 一律 rebind_stub 以在线树句柄重绑，流的事实源是调参与后端字段（同迁移流口径）
    let stream_stub = RangeIndexStub::from_tuning(0, &tuning, StorageBackendType::Disk);

    // 数据通道事件先行于换入与 meta 落盘：sink 同步读取快照文件分块灌入 AOF，
    // 副本据流块重建为树态。先发流后写 meta，杜绝「副本见 meta 却缺数据」的空树
    // 幻影（源侧 RI.SET 经元记录门禁与本流串行，无需额外加锁）。入队失败即删快照
    // 残件上抛——此刻新树未换入、旧态分毫未动，回滚即原态；emit 静默缺条目为禁区，
    // 本地有树而副本无数据是发散。
    let (ns, db) = self.virtual_domain();
    if let Err(e) = self.store.emit_event(StoreEvent::RangeIndexStream {
      ns,
      db,
      key,
      obj_type,
      stub: stream_stub.encode(),
      file_path: &snapshot_path,
      replace,
      // 灌入批水位随流透传副本（调用方单点算好传入），副本发布元记录不再落
      // i64::MAX 假水位——否则副本计数门恒真，到期成员永不出账、主从发散
      next_expiry,
    }) {
      // emit 失败 = 流块未入账，副本未发布，无需补偿；仅弃快照残件上抛——
      // 此刻新树未换入、旧态分毫未动，回滚即原态
      discard_snapshot_file(&snapshot_path, "升阶");
      return Err(e);
    }

    // 原子换入：条带写锁内裁决 IndexExists（首升阶防重门）→ 摘旧树延迟释放
    // （重灌态）→ rename 快照顶替数据文件 + 目录 fsync 双屏障 → 恢复注册。
    // 树身份键 = 物理 Meta 键（跨库同名键的换入防重门与数据文件按物理域隔离）
    let mgr = Arc::clone(&self.store.range_index);
    let pub_key = self.session_meta_key(key);
    let pub_snap = snapshot_path.clone();
    let published = range_index_blocking(move || {
      let _xlock = mgr.acquire_exclusive_for_delete(fast_hash(&pub_key));
      mgr.publish_tree_from_snapshot_locked(&pub_key, &pub_snap, replace)
    })
    .await?;
    let tree = published.inspect_err(|_e| {
      // 换入失败旧树必在位（快照源校验前置于摘除之前，见发布内核）：弃快照
      // 残件 + 补偿摘除副本幻影（emit 已成功，副本已按流块实时发布树态，主端
      // 回滚须同步摘除，杜绝主从长期发散、重启回放复现幻影）后上抛令升阶命令
      // 失败，禁静默；残件另有 migration-tmp 启动清扫兜底
      discard_snapshot_file(&snapshot_path, "升阶");
      self.compensate_stream_drop(key);
    })?;

    // 升阶树纳入换号回收旁表（FLUSHDB 后同名集合再升阶不被 IndexExists 拦截；
    // 重灌臂因不再前导 drain 而全程在册，set 插入幂等）
    self.register_bftree_key(key);

    let stub = RangeIndexStub::from_tuning(tree.native_ptr(), &tuning, StorageBackendType::Disk);
    let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
    let meta = MetaValue::new_with_expiry(key_id, obj_type, count, next_expiry);
    let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, key);

    // 换入后元记录落盘硬错上抛，禁静默（落盘工序列 save_bftree_meta_stub 门面
    // 单点）：主端仅余「新树 + 旧 meta」滞后态，键仍可读（惰性恢复按数据文件
    // 收敛），错误面必须可见；副本已按流块发布树态元记录 → 补偿摘除幻影后上抛
    if let Err(e) = self.save_bftree_meta_stub(key, &meta, &stub).await {
      self.compensate_stream_drop(key);
      return Err(e);
    }

    // 信封删除 IO 硬错上抛令升阶命令失败，禁 `let _ =` 静默 Ok：静默吞错正是
    // 「信封旧快照 + Meta 存根 + 树」双态残留的最廉价入口（错误面必须可见）；
    // 此刻 meta 已落盘无法回滚，残留由排空回收单点的信封幂等墓碑兜底收敛。
    // 重灌态信封本就不存在，delete_raw 探针落空零写零入账（幂等）
    self.delete_raw(&env_k).await?;
    // 信封墓碑入账由 delete_raw 的写监听端口恰一次完成（sink 的 Write 臂
    // 放行 ObjectEnvelope 墓碑为 StoreDelete 条目），回放侧按域范围仅删信封，
    // 不动 publish 先行的树态元记录——杜绝升阶后旧信封在副本残留成幻影。

    Ok(())
  }

  /// 自迁移安全换入窗登记（出账重灌臂 / 物化降级通道 / 后台懒降阶的共用封写原语，
  /// 一处定义，调用点转引，禁各臂各写一套）：把「建树快照 → 求值 → 换入/清退」
  /// 的放锁长窗视作一次自迁移——同键迁移 claim 在册期间，并发同键写臂在四探测门
  /// （load_collection_stub / refresh_tiered_meta / load_meta / range_index_create）
  /// 一律被 MigrationBusy / 键暂不可见拒绝，窗内不存在「已 ACK 落旧树随
  /// replace=true 换入被整树顶替」的静默丢失形；快照在 claim 后读取，含全部已
  /// 提交写，AOF 镜像序仍 = 树内提交序。
  ///
  /// 返回 `None` = 同键 claim 已被持有（并发 RENAME / 另一自迁移窗）：try 失败
  /// 即退不持钥等待，无死锁面，调用方按存储忙失败交客户端重试。守卫 RAII 成对
  /// 释放：成功臂、`?` 早退失败臂与 panic 展开一律 Drop 释 claim（幂等），杜绝
  /// 单侧泄漏令键永久不可见。锁纪律：登记本身零锁零 await；持条带锁臂（到期
  /// 出账）须在守卫释放前登记——此刻无在途同键写者，登记即与后续全部写臂串行。
  /// claim 判据取树身份键 = 物理 Meta 键（会话域内派生单点），跨库同名键互不
  /// 封堵
  pub fn try_swap_in_window(&self, key: &[u8]) -> Option<SwapInWindowGuard> {
    let id_key = self.session_meta_key(key);
    self
      .store
      .range_index
      .try_claim_migration(&id_key)
      .then(|| SwapInWindowGuard {
        manager: Arc::clone(&self.store.range_index),
        id_key,
      })
  }

  /// 存根提升至日志尾部并清除 Flushed 标记
  ///
  /// libs/server/Storage/Session/MainStore/RangeIndexOps.cs:PromoteRangeIndexToTail
  /// libs/server/Storage/Functions/MainStore/RMWMethods.cs:NeedInitialUpdate
  /// libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs:GetRMWInitialFieldInfo
  /// libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs:GetRMWModifiedFieldInfo
  ///
  /// 当存根位于只读区且被置位 IsFlushed 时，通过存储层原子 RMW (RIPROMOTE) 重新提升回可变区尾部，
  /// 清除 IsFlushed 标记并转移所有权 (设置源记录 IsTransferred，对标 C# PostCopyUpdater)。
  pub async fn promote_range_index_to_tail(&self, key: &[u8]) -> StdResult<(), RangeIndexError> {
    let meta_k = self.session_meta_key(key);

    // 1. InPlaceUpdater 等价：可变区页写锁内读-验-改 (若已在可变区，原位清除 Flushed 标记)
    if self
      .heal_stub_in_place(&meta_k, clear_flushed_patch)
      .await?
    {
      return Ok(());
    }

    // 2. CopyUpdater 等价：候选链定位当前记录 (含磁盘冷区) → 构造提升记录追加至尾部 → CAS 挂载
    if let TailPatchOutcome::Appended {
      src_addr,
      cas_ok: true,
      src_stub,
    } = self.tail_patch_record(&meta_k, clear_flushed_patch).await?
    {
      self.post_promote_tail_patch(key, &meta_k, src_addr, src_stub);
    }
    Ok(())
  }

  /// RIPROMOTE CAS 成功后的源记录所有权转移后处理 (1:1 对标 C#
  /// RMWMethods.cs:PostCopyUpdater RIPROMOTE 分支)
  ///
  /// - 源存根 TreeHandle == 0 (冷态：驱逐后/恢复后) 时预登记 pending，让下一
  ///   检查点捕获尾部新帧；
  /// - 源记录在可变区时置位 is_transferred，防止过期源记录被误驱逐摘除注册或
  ///   误快照陈旧视图。句柄清零与置位由治愈内核 [`patch_stub_record`] +
  ///   位变更器 [`transfer_out_patch`] 单点承接（C# RangeIndexManager.Index.cs 内
  ///   ClearTreeHandle 与 SetTransferredFlag 两枚的符号锚点挂在该位变更器，
  ///   本编排点不复挂）。
  fn post_promote_tail_patch(
    &self,
    key: &[u8],
    meta_k: &[u8],
    src_addr: u64,
    src_stub: Option<RangeIndexStub>,
  ) {
    let _guard = self.enter_gated();
    if src_stub.is_some_and(|s| s.tree_handle == 0) {
      // 树身份键 = 物理 Meta 键（预置登记按物理域隔离，跨库同名键不互挡）
      let id_key = self.session_meta_key(key);
      let _ = self
        .store
        .range_index
        .pre_stage_and_register_pending(&id_key, src_addr);
    }
    if !self.store.hlog.is_on_disk(src_addr) {
      let _ = self
        .store
        .hlog
        .try_modify_record_in_place(src_addr, meta_k, |src_val| {
          Some(patch_stub_record(src_val, transfer_out_patch))
        });
    }
  }
}
