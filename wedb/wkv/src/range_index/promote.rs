use std::{
  result::Result as StdResult,
  sync::{Arc, atomic::Ordering},
};

use wbase::keyfmt::log_key;
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
  range_index_blocking, validate_bftree_record,
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
  /// 写序不变量：契约闸 → 建树 → 先发 RangeIndexStream 数据流 → 原子换入 →
  /// 落元记录 → 删信封。契约闸前置：装载前逐条过 [`validate_bftree_record`]
  /// 与稳态写臂同一受理上限（引擎页容量不折 key.len()，与存根契约两套上限，
  /// 漏闸即「能升阶、不能续写」），任一越限整批拒、零半树（闸在建树之前，
  /// scratch 与引擎实例零创建），调用臂按 CacheBudgetExhausted 同型回落信封
  /// 态续写；建树 → 数据流先行于换入与 meta：入队失败即弃快照残件上抛，此刻
  /// 新树未换入、
  /// 旧态（旧树 / 旧 meta / 信封 / 键级 TTL）分毫未动，回滚即原态；副本据流块以
  /// 同 replace 标志重放发布（重灌流不再前导 RangeIndexDrop，旧树由 replace 换入
  /// 承接）。换入后至元记录落盘前崩溃仅余「新树 + 旧 meta」滞后态——键仍可读
  /// （读臂惰性恢复按数据文件收敛、计数校正兜底 size 滞后），崩溃窗口自「数据
  /// 丢失」收敛为「新旧快照之一」，对齐 C# 对象记录重写单日志记录原子、
  /// HasExpiration 前移零 TTL 事件的口径（ObjectStore/VarLenInputMethods.cs:
  /// GetRMWModifiedFieldInfo）。换入之后的失败（save meta / 信封删除）一律经
  /// [`Error::Swapped`] 分级包装上抛——树物理面已实际变更，分层臂 WATCH 判据
  /// 据此保持置脏；换入之前的失败（建树 / 流入队 / 换入）原样上抛，旧态零变更。
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

    // 契约存根（树句柄取 0 的瞬态形态——消费侧（副本回放 / 迁移接收）一律
    // rebind_stub 以在线树句柄重绑，流的事实源是调参与后端字段，同迁移流口径）。
    // 同时是下方建树前契约闸的判据载体：闸与稳态写臂读同一份 tuning 派生存根，
    // 受理上限天然同源（Copy 定长结构，随闭包零成本穿线程）
    let stream_stub = RangeIndexStub::from_tuning(0, &tuning, StorageBackendType::Disk);

    // 建树 + 装载 + CPR 快照：scratch 树全程不入注册表、不触目标键数据文件，
    // 旧树（重灌态）原态可读
    let mgr = Arc::clone(&self.store.range_index);
    let store = Arc::clone(&self.store);
    let build_key = key.to_vec();
    let entry_count = entries.len();
    let (snapshot_path, count) = range_index_blocking(move || {
      // 契约闸前置（建树侧收口走 validate_bftree_record 单点，禁第二套长度判定）：
      // 引擎页容量只按裸记录受理（不折 key.len()），稳态写臂却按折 key 后的存根
      // 契约受理——不设本闸即「能升阶、不能续写」的两套上限。建树前逐条过与
      // 稳态写臂同一的 validate_bftree_record，任一越限**整批拒**：此刻 scratch
      // 工作文件零创建、引擎实例零注册，旧树 / 信封 / meta 分毫未动，半树换入
      // 结构性不可达；调用臂按 CacheBudgetExhausted 同型回落信封态续写
      for (tree_key, record) in &entries {
        validate_bftree_record(&stream_stub, tree_key, record.len()).map_err(|e| {
          log::warn!(
            "promote_collection_to_bftree 契约闸整批拒升阶: key={}, entries={entry_count}, {e}",
            log_key(&build_key)
          );
          Error::from(e)
        })?;
      }
      mgr
        .build_collection_tree_snapshot(&entries, &tuning)
        // 预算耗尽拒绝臂自愈（冷树回收接线，对位 C# GarnetRecordTriggers.cs:OnEvict
        // → DisposeTreeUnderLock(deleteFiles:false) 的即时释放上半场）：scratch
        // 预留被拒即同步驱动一轮冷树回收再重试一次建树，仍失败方按下方映射
        // 回落信封态。本闭包已在 `range_index_blocking`（spawn_blocking 线程池）
        // 承接，同步回收的重 I/O 不触异步反应器；拒绝时注册表与旧态零触碰
        // （见 build_collection_tree_snapshot 入口闸注），重试无副作用
        .or_else(|e| match e {
          WbftreeError::CacheBudgetExhausted => {
            store.recycle_cold_bftrees();
            mgr.build_collection_tree_snapshot(&entries, &tuning)
          }
          other => Err(other),
        })
        .map_err(|e| -> Error {
          match e {
            // 预算耗尽单点映射：建树闸在实例化之前拒绝，scratch 工作文件与
            // 引擎实例零创建、注册表与旧态零触碰，调用方回落信封态即可
            WbftreeError::CacheBudgetExhausted => {
              log::warn!(
                "promote_collection_to_bftree 页缓存预算耗尽，集合保持信封态: key={}, entries={entry_count}",
                log_key(&build_key)
              );
              Error::CacheBudgetExhausted
            }
            // 装载被拒整树作废：内核前置校验保证失败发生在任何写入之前，scratch
            // 工作文件已由建树内核就地回收，注册表与旧态零触碰
            WbftreeError::LoadRejected(BfTreeInsertResult::InvalidKV) => {
              log::error!(
                "promote_collection_to_bftree 装载被拒 (键值违反长度契约): key={}, entries={entry_count}",
                log_key(&build_key)
              );
              CollectionError::KeyTooLong.into()
            }
            WbftreeError::LoadRejected(res) => {
              log::error!(
                "promote_collection_to_bftree 装载失败 (引擎参数非法): key={}, entries={entry_count}, res={res:?}",
                log_key(&build_key)
              );
              CollectionError::InvalidArgument("invalid arguments for tree insert").into()
            }
            e => e.into(),
          }
        })
    })
    .await??;

    // 数据通道事件先行于换入与 meta 落盘：sink 同步读取快照文件分块灌入 AOF，
    // 副本据流块重建为树态。先发流后写 meta，杜绝「副本见 meta 却缺数据」的空树
    // 幻影（源侧 RI.SET 经元记录门禁与本流串行，无需额外加锁）。入队失败即删快照
    // 残件上抛——此刻新树未换入、旧态分毫未动，回滚即原态；emit 静默缺条目为禁区，
    // 本地有树而副本无数据是发散。
    let (ns, db) = self.virtual_domain();
    if let Err(e) = self.store.emit_event(
      self.aof_session_id,
      StoreEvent::RangeIndexStream {
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
      },
    ) {
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
    let pub_id = pub_key.clone();
    let published = range_index_blocking(move || {
      let _xlock = mgr.acquire_exclusive_for_delete(fast_hash(&pub_id));
      mgr.publish_tree_from_snapshot_locked(&pub_id, &pub_snap, replace)
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

    // 换入后元记录落盘失败按 replace 两态分流（1:1 对标 C#
    // RangeIndexOps.cs:RangeIndexCreate「存根落盘失败即销毁刚建 BfTree」，复用
    // range_index_create 同一 unregister + delete_index 回滚机制，禁第三套清理路径）：
    // - 首升阶（!replace）：主存本无旧 meta，残留态实为「孤儿新树 + 内存态信封」
    //   而非滞后可读态——孤儿条目常驻注册表会令该键后续重试升阶在换入锁内被
    //   IndexExists 永久拦截，冻结为不可写不可升阶。失败臂逆序回滚至纯信封态：
    //   注销换号旁表 + 摘除孤儿树并清理数据文件（换入世代判据兜底迟滞 unlink
    //   不误删重试换入的新文件）；副本已按流块发布树态，本地回滚须同配
    //   RangeIndexDrop 补偿摘除，杜绝主从发散与重启回放复现幻影；
    // - 重灌（replace）：旧 meta 在位、新树已原子顶替数据文件，主端仅余「新树 +
    //   旧 meta」滞后态，键仍可读（惰性恢复按数据文件收敛、计数校正兜底 size
    //   滞后）——误删已换入新树即丢数据，保持既有行为仅补偿摘除副本幻影。
    // 两态失败均属「换入已生效后的失败」（[`Error::Swapped`] 分级），分层臂 WATCH
    // 判据据此保持置脏（推进真实反映树变更）
    if let Err(e) = self.save_bftree_meta_stub(key, &meta, &stub).await {
      if !replace {
        self.unregister_bftree_key(key);
        let _ = self.store.range_index.delete_index(&pub_key);
        // 镜像点收口后（票 zcode-r34-writekernel 条目三）save 内部的 meta
        // upsert 在写监听失败前已完成索引挂载（「通知失败 = 已生效 + 镜像
        // 缺失」全仓契约）——回滚链须同步清退已挂载的元记录，键方能回到纯
        // 信封态；旧「分配后即镜像」时序下元记录未挂载，本删除为幂等探针
        // 落空零写，两时序下皆收敛。删除自身失败按 Swapped 分级上抛（树已
        // 注销，元记录残留由读面探针与后台回收兜底，错误面保持可见）
        let meta_k = self.session_meta_key(key);
        self.delete_raw(&meta_k).await.map_err(Error::swapped)?;
      }
      self.compensate_stream_drop(key);
      return Err(Error::swapped(e));
    }

    // 信封删除 IO 硬错上抛令升阶命令失败，禁 `let _ =` 静默 Ok：静默吞错正是
    // 「信封旧快照 + Meta 存根 + 树」双态残留的最廉价入口（错误面必须可见）；
    // 此刻 meta 已落盘无法回滚，残留由排空回收单点的信封幂等墓碑兜底收敛。
    // 重灌态信封本就不存在，delete_raw 探针落空零写零入账（幂等）。
    // 换入已生效（[`Error::Swapped`] 分级），同上保持置脏
    self.delete_raw(&env_k).await.map_err(Error::swapped)?;
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

    // CopyUpdater 等价 (1:1 对标 C# RMWMethods.cs:NeedCopyUpdate 的 RIPROMOTE 分支：
    // 恒走 CopyUpdater 提升至可变区尾部，并在 PostCopyUpdater 中转移所有权与执行冷态预置)：
    // 候选链定位当前记录 (含磁盘冷区) → 构造提升记录追加至尾部 → CAS 挂载
    if let TailPatchOutcome::Appended {
      src_addr,
      cas_ok: true,
    } = self.tail_patch_record(&meta_k, clear_flushed_patch).await?
    {
      self.transfer_out_source_stub(&meta_k, src_addr);
    }
    Ok(())
  }

  /// CAS 成功后的源存根所有权转移后处理单点 (1:1 对标 C#
  /// RMWMethods.cs:PostCopyUpdater RIPROMOTE 分支；紧缩搬迁臂
  /// `CompactSession::transfer_out_source` 同一编排并轨，严禁第三套判定)
  ///
  /// - 树冷态（注册表无在线实例 **且** 源存根句柄为零——C# 判据 handle==0，
  ///   驱逐/恢复/摘树生命周期钩子必清零句柄，见下段差额说明）时以源记录地址
  ///   预置 data.bftree 并预登记 pending，让下一检查点捕获尾部新帧；
  /// - 源记录在可变区时置位 is_transferred，防止过期源记录被误驱逐摘除注册或
  ///   误快照陈旧视图。句柄清零与置位由治愈内核 [`patch_stub_record`] +
  ///   位变更器 [`transfer_out_patch`] 单点承接（C# RangeIndexManager.Index.cs 内
  ///   ClearTreeHandle 与 SetTransferredFlag 两枚的符号锚点挂在该位变更器，
  ///   本编排点不复挂）。
  ///
  /// 冷态判据的 rust 差额（注册表复查 + 句柄，缺一即错）：C# 侧存根 TreeHandle
  /// 即活跃性事实源——live transfer（句柄非零、树在册）当场清零源句柄、绝无
  /// 陈旧句柄形态；handle==0 唯一等价冷态。rust 两处偏离：其一，句柄可能陈旧
  /// 为假活（FLUSHDB 换号摘树未清账本内句柄——安全纪元内回滚复用的既定形态，
  /// doc/zh/db.md 安全纪元双覆盖），单按句柄即误跳过预置、冷读重开陈旧工作
  /// 文件丢刷盘件里的权威数据；其二，句柄也可能假零而树仍在册（检查点恢复态
  /// 存根 handle=0 配在线树），单按注册表即对活引擎误预置、用滞后刷盘件覆盖
  /// 在用数据文件。故预置触发 = 「源存根句柄为零 或 注册表无在线树」，且预置
  /// 内核（RangeIndexManager::pre_stage_and_register_pending）锁内对在册条目
  /// （在线或 pending）短路跳过复制，双保险封死误覆盖活引擎文件窗。
  ///
  /// `id_key` 为树身份键 = 物理 Meta 键：RIPROMOTE 侧由调用方按会话域派生，
  /// 紧缩搬迁侧直接传记录物理键（Tag 即 Meta，零解码换键——身份含域与树注册
  /// 同源）。全编排同步零 I/O，持纪元保护内闭环（调用方已持守卫时可重入）。
  pub(crate) fn transfer_out_source_stub(&self, id_key: &[u8], src_addr: u64) {
    let _guard = self.enter_gated();
    if self.store.range_index.get_tree(id_key).is_none() {
      let _ = self
        .store
        .range_index
        .pre_stage_and_register_pending(id_key, src_addr);
    }
    if !self.store.hlog.is_on_disk(src_addr) {
      // 可变区原位置位；只读区（紧缩搬迁源恒驻于此，RIPROMOTE 只读区源同盲区）
      // 降级页驻留原位内核——页写锁与读者互斥，CAS 已成功、源逻辑上已被尾部
      // 新帧取代，原位改安全（对标 C# GarnetRecordTriggers.cs:PostCopyToTail
      // 注释「Mutating it in place is safe because the source is logically
      // superseded by dst」）。`!is_on_disk` 即地址仍驻内存环形窗（低于
      // begin_address 的磁盘冷区已由前置臂排除），resident 臂恒可落笔
      if let Ok(None) = self
        .store
        .hlog
        .try_modify_record_in_place(src_addr, id_key, |src_val| {
          Some(patch_stub_record(src_val, transfer_out_patch))
        })
      {
        let _ = self
          .store
          .hlog
          .try_modify_resident_record_in_place(src_addr, id_key, |src_val| {
            Some(patch_stub_record(src_val, transfer_out_patch))
          });
      }
    }
  }
}
