use std::{
  fs::remove_file,
  result::Result as StdResult,
  sync::{Arc, atomic::Ordering},
};

use wbase::addr::is_read_cache;
use wbftree::{
  BfTreeInsertResult, BfTreeService, RANGE_INDEX_STUB_SIZE, RangeIndexManager, RangeIndexStub,
  StorageBackendType, TreeTuning,
};
use wdev::Device;
use whasher::fast_hash;
use wval::{GarnetObjectType, KeyTag, META_VALUE_SIZE, MetaValue};

use super::{
  RangeIndexError, TreeReadGuard, TreeWriteGuard, encode_meta_stub_record, range_index_blocking,
  wait_tree_checkpoint,
};
use crate::{
  error::{CollectionError, Error, Result},
  session::{CopyToTailOutcome, StoreSession},
  store::StoreEvent,
};

#[inline]
pub fn rebind_stub(stub: &mut RangeIndexStub, tree: &BfTreeService) {
  stub.tree_handle = tree.native_ptr();
  stub.reset_flags();
}

impl<D: Device> StoreSession<D> {
  pub async fn save_bftree_meta_stub(
    &self,
    key: &[u8],
    meta: &MetaValue,
    stub: &RangeIndexStub,
  ) -> Result<()> {
    let meta_k = self.session_meta_key(key);
    let val = encode_meta_stub_record(meta, stub);
    self.upsert_raw(&meta_k, &val).await?;
    Ok(())
  }

  /// 树态键排空回收单点：双物理域原子墓碑（信封域 + 元记录）+ BfTree 树实例注销 +
  /// 换号旁表回收 + RangeIndexDrop AOF 入账；随键 TTL 由 `keep_ttl` 分流（口径详见
  /// `drain_and_delete_collection_meta` 文档：删键臂 false 清 TTL
  /// 杜绝孤儿，降阶迁移臂 true 只墓碑元记录、不碰 TTL 旁路，对标 C#
  /// 对象记录重写原样前移 HasExpiration、零发 TTL 事件）。
  ///
  /// 删键臂（keep_ttl=false）连带对信封域写幂等墓碑，两域回收一处收口：
  /// 升阶是非原子三步写（先发流块、再落元记录、后删信封），崩溃、AOF 截断
  /// 部分回放或信封删除 IO 失败都会留下「信封旧快照 + Meta 存根 + 树」双态
  /// 残留；双态期命令路由 Meta 优先读写无误，但排空臂若只清 Meta 域，删空后
  /// 命令回落信封域探测（`contains_key` / `read_tag_with` 双域臂），已删空集合
  /// 以升阶时刻的完整旧数据幽灵复活。C# 集合恒驻对象域单一物理域，删记录即
  /// 连尾随字段同亡、绝无第二域残留（libs/server/Storage/Functions/
  /// ObjectStore/RMWMethods.cs:InPlaceUpdaterWorker 的 HasRemoveKey →
  /// ExpireAndStop 与 DeleteMethods 删除臂）；本仓分层态键消亡必须两域齐清，
  /// 落实 SKILL.md「严格删空生命周期与原子墓碑，杜绝幽灵空元记录」。墓碑先于
  /// 元记录落笔：命令窗口内回落域先死、路由域后死，杜绝中途幽灵读。迁移臂
  /// （keep_ttl=true）绝不触碰信封域：键全程存活，降阶臂随后 obj_save 写回新
  /// 信封，墓碑即自相残杀；分层重灌臂经 promote 直接换树、不再经本函数（先建
  /// 后拆，见 promote_collection_to_bftree）。纯 RangeIndex 键
  /// 无信封记录，delete_raw 哈希探针落空零写零入账（幂等），与 DEL 复合臂
  /// [`crate::session::StoreSession::delete`] 的双域墓碑共用 delete_raw 同一
  /// 原语，不新增第二条信封删除路径。
  pub async fn handle_bftree_drain_and_delete(&self, key: &[u8], keep_ttl: bool) -> Result<()> {
    // 删键臂信封域幂等墓碑（回落域先于路由域消亡）
    if !keep_ttl {
      let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, key);
      self.delete_raw(&env_k).await?;
    }
    self.drain_and_delete_collection_meta(key, keep_ttl).await?;
    let mgr = Arc::clone(&self.store.range_index);
    let del_key = key.to_vec();
    let _ = range_index_blocking(move || mgr.delete_index(&del_key)).await??;
    // 删除面唯一收敛点的旁表逆操作：树已销毁，同步注销换号回收登记
    self.unregister_bftree_key(key);
    // AOF 入队失败不中断 RI 操作（复制/重建面可自树状态收敛），告警可见
    // 事件域取会话物理域（与本键记录前缀同源），入账键方与记录落域一致
    let (ns, db) = self.virtual_domain();
    if let Err(e) = self
      .store
      .emit_event(StoreEvent::RangeIndexDrop { ns, db, key })
    {
      log::error!("RangeIndexDrop AOF 入队失败: {e}");
    }
    Ok(())
  }

  /// 读取任意集合（RangeIndex 或 FlattenedTree 集合）存根及元数据
  pub async fn load_collection_stub(
    &self,
    key: &[u8],
  ) -> Result<Option<(MetaValue, RangeIndexStub)>> {
    let meta_k = self.session_meta_key(key);
    let Some(bytes) = self.read_raw(&meta_k).await? else {
      return Ok(None);
    };
    if bytes.len() < META_VALUE_SIZE {
      return Ok(None);
    }
    let meta = MetaValue::from_slice(&bytes[..META_VALUE_SIZE])?;
    if !self.probe_alive(key).await? {
      return Ok(None);
    }
    let is_alive = meta.is_live();
    if !is_alive {
      return Ok(None);
    }
    if bytes.len() < META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE {
      return Ok(None);
    }
    let stub =
      RangeIndexStub::decode(&bytes[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE])?;
    Ok(Some((meta, stub)))
  }

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
            wbftree::Error::LoadRejected(BfTreeInsertResult::InvalidKV) => {
              log::error!(
                "promote_collection_to_bftree 装载被拒 (键值违反长度契约): key={build_key:?}, entries={entry_count}"
              );
              CollectionError::KeyTooLong.into()
            }
            wbftree::Error::LoadRejected(res) => {
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
      obj_type: obj_type.as_u8(),
      stub: stream_stub.encode(),
      file_path: &snapshot_path,
      replace,
    }) {
      if snapshot_path.exists()
        && let Err(re) = remove_file(&snapshot_path)
      {
        log::warn!("升阶快照残件删除失败，migration-tmp 启动清扫兜底: {re}");
      }
      return Err(e);
    }

    // 原子换入：条带写锁内裁决 IndexExists（首升阶防重门）→ 摘旧树延迟释放
    // （重灌态）→ rename 快照顶替数据文件 + 目录 fsync 双屏障 → 恢复注册。
    let mgr = Arc::clone(&self.store.range_index);
    let pub_key = key.to_vec();
    let pub_snap = snapshot_path.clone();
    let published = range_index_blocking(move || {
      let _xlock = mgr.acquire_exclusive_for_delete(fast_hash(&pub_key));
      mgr.publish_tree_from_snapshot_locked(&pub_key, &pub_snap, replace)
    })
    .await?;
    let tree = published.map_err(|e| {
      // 换入失败旧树必在位（快照源校验前置于摘除之前，见发布内核），删快照残件
      // 后上抛令升阶命令失败，禁静默；残件另有 migration-tmp 启动清扫兜底
      if snapshot_path.exists()
        && let Err(re) = remove_file(&snapshot_path)
      {
        log::warn!("升阶快照残件删除失败，migration-tmp 启动清扫兜底: {re}");
      }
      e
    })?;

    // 升阶树纳入换号回收旁表（FLUSHDB 后同名集合再升阶不被 IndexExists 拦截；
    // 重灌臂因不再前导 drain 而全程在册，set 插入幂等）
    self.register_bftree_key(key);

    let stub = RangeIndexStub::from_tuning(tree.native_ptr(), &tuning, StorageBackendType::Disk);
    let meta_k = self.session_meta_key(key);
    let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
    let meta = MetaValue::new_with_expiry(key_id, obj_type, count, next_expiry);
    let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, key);

    let val = encode_meta_stub_record(&meta, &stub);
    // 换入后元记录落盘硬错上抛，禁静默：此刻仅余「新树 + 旧 meta」滞后态，键仍
    // 可读（惰性恢复按数据文件收敛），错误面必须可见
    self.upsert_raw(&meta_k, &val).await?;

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

  /// 读取 RangeIndex 存根及元数据（支持防重入与类型安全检查）
  ///
  /// libs/server/Resp/Parser/RespCommand.cs:IsRangeIndexCommand
  ///
  /// 热路径时间复杂度优化：RI 点操作每次调用本函数，旧实现经 load_meta 读一次
  /// 元记录后再 read_raw 重复读同一记录（2 次主存 I/O）；现改为单次 read_raw
  /// 同帧解析 MetaValue + TTL 守卫 + 类型检查 + 存根解码（1 次主存 I/O），
  /// 语义与 load_meta 口径一致（过期视同不存在）。
  ///
  /// RI 命令打非 RI 键的门禁在此收口（C# 由存储层记录类型判别回 WrongType，
  /// 本函数是 rust 侧 RI 点操作的唯一装载入口，判据为记录自己的物理域事实，
  /// 不吃命令位图）：String 域与集合信封域命中一律 `WrongType`（C#
  /// ValueIsObject 先于类型白名单），非 RangeIndex 的存活元记录同样
  /// `WrongType`，三域皆缺才是索引缺失（`Ok(None)` → 调用方答 no such index）。
  pub async fn load_range_index_stub(
    &self,
    key: &[u8],
  ) -> StdResult<Option<(MetaValue, RangeIndexStub)>, RangeIndexError> {
    let meta_k = self.session_meta_key(key);
    let Some(bytes) = self.read_raw(&meta_k).await? else {
      // 无 RI 元记录：同名普通字符串键 / 集合信封键命中皆 WRONGTYPE，非类型不符
      // 而是压根不是索引
      if self.read(key).await?.is_some() {
        return Err(RangeIndexError::WrongType);
      }
      let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, key);
      if self.contains_key_raw(&env_k).await? {
        return Err(RangeIndexError::WrongType);
      }
      return Ok(None);
    };
    if bytes.len() < META_VALUE_SIZE {
      return Ok(None);
    }
    let meta = MetaValue::from_slice(&bytes[..META_VALUE_SIZE])
      .map_err(|e| RangeIndexError::Internal(e.to_string()))?;
    // TTL 守卫 (与 load_meta 口径一致)：过期集合视同不存在
    if !self.probe_alive(key).await? {
      return Ok(None);
    }
    if meta.collection_type != GarnetObjectType::RangeIndex {
      return Err(RangeIndexError::WrongType);
    }
    if bytes.len() < META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE {
      return Ok(None);
    }
    let stub =
      RangeIndexStub::decode(&bytes[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE])?;
    Ok(Some((meta, stub)))
  }

  /// 获取在线 BfTree 实例及其条带共享读锁 (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.Locking.cs:ReadRangeIndex 与 ReadRangeIndexLock)
  ///
  /// 先在无锁/共享锁状态下快速命中（稳态 O(1)：一次 volatile 读 + 一次注册表
  /// 查找），若存根已被刷盘 (IsFlushed = true) 则先通过 RIPROMOTE 重新提升至日志尾部并清除
  /// 刷盘标记；若树未激活则释放读锁后调用 get_or_open_tree（文件 I/O + 快照解析
  /// 属重操作，卸载 compio 阻塞线程，避免慢恢复停摆整核），激活完成后执行
  /// RIRESTORE 存根回写（新句柄写回 + 清 Recovered 位，对标 C# RestoreTree 尾段
  /// 的 RIRESTORE RMW），随后重新获取共享读锁。整个数据读取/修改操作在其 RAII
  /// 读锁保护下安全执行。
  pub async fn acquire_tree_read(
    &self,
    key: &[u8],
    stub: &RangeIndexStub,
  ) -> StdResult<TreeReadGuard<'_>, RangeIndexError> {
    let key_hash = fast_hash(key);
    let mut current_stub = *stub;
    loop {
      // 屏障异步等待：外层 VersionShift 屏障跨 await 持有，写者挂起让出 reactor
      // （同步忙自旋会在持有窗口霸占 compio worker 造成检查点 I/O 永不收割的死锁）
      wait_tree_checkpoint(&self.store.range_index, key).await?;
      let read_lock = self.store.range_index.locks().read(key_hash);
      if let Some(tree) = self.store.range_index.get_tree(key) {
        if current_stub.is_flushed() {
          drop(read_lock);
          self.promote_range_index_to_tail(key).await?;
          current_stub.set_flushed(false);
          continue;
        }
        return Ok(TreeReadGuard::new(tree, read_lock));
      }
      drop(read_lock);

      if current_stub.is_flushed() {
        self.promote_range_index_to_tail(key).await?;
        current_stub.set_flushed(false);
        continue;
      }

      // 惰性恢复慢路径：卸载阻塞线程 (条带锁在 manager 内部自取自放，不跨线程边界)
      let mgr = Arc::clone(&self.store.range_index);
      let restore_key = key.to_vec();
      let restore_stub = current_stub;
      let tree = range_index_blocking(move || mgr.get_or_open_tree(&restore_key, &restore_stub))
        .await?
        .map_err(RangeIndexError::from)?;

      // 惰性激活补登换号回收旁表：重启后未经检查点恢复的树首访激活时旁表
      // 尚无记录，若不补登则后续 FLUSHDB 取不到该键，同名重建被 IndexExists 拦截
      self.register_bftree_key(key);

      // RIRESTORE 存根回写：不持任何条带锁 (对标 C# RestoreTree 释放 X 锁后再发
      // RIRESTORE RMW 的分裂设计——锁内发 RMW 会与延迟 OnFlush 自死锁)
      self.restore_range_index_stub(key, &tree).await?;
    }
  }

  /// 获取在线 BfTree 实例及其条带独占写锁（多步写臂互斥面）
  /// 对应 C# RangeIndexManager.AcquireExclusiveForDelete 独占守卫形态；
  /// 语义差异：C# 独占锁仅承载生命周期操作——DEL/
  /// 淘汰/检查点快照/惰性恢复，RI 数据写每命令单树操作走共享锁即可；rust 分层
  /// 集合写臂是「探测 → 树写 → 计数 → meta 回写」多步序列，共享锁下两臂可交错
  /// 互踩（删除结果丢弃、meta 覆写丢更新），故数据写面升格独占，对位 C# 对象域
  /// 同键写经 Tsavorite 记录锁串行（TsavoriteKV.cs RMW InPlaceUpdater 前置记录
  /// X 锁），纯读臂维持共享锁）
  ///
  /// 骨架与 [`acquire_tree_read`] 同型：检查点屏障在无锁状态下异步等待（让出
  /// reactor 不自旋霸占 compio worker）；存根被刷盘先放锁再晋升重试；树未激活
  /// 先放锁再卸载阻塞线程恢复（恢复路径的条带锁在 manager 内部自取自放，不跨
  /// 线程边界，持本锁进阻塞卸载会与其自取的锁互锁）；RIRESTORE 回写不持条带锁
  /// （锁内发 RMW 与延迟 OnFlush 自死锁，分裂设计同读面）。
  pub async fn acquire_tree_write(
    &self,
    key: &[u8],
    stub: &RangeIndexStub,
  ) -> StdResult<TreeWriteGuard<'_>, RangeIndexError> {
    let key_hash = fast_hash(key);
    let mut current_stub = *stub;
    loop {
      wait_tree_checkpoint(&self.store.range_index, key).await?;
      let write_lock = self
        .store
        .range_index
        .acquire_exclusive_for_delete(key_hash);
      if let Some(tree) = self.store.range_index.get_tree(key) {
        if current_stub.is_flushed() {
          drop(write_lock);
          self.promote_range_index_to_tail(key).await?;
          current_stub.set_flushed(false);
          continue;
        }
        return Ok(TreeWriteGuard::new(tree, write_lock));
      }
      drop(write_lock);

      if current_stub.is_flushed() {
        self.promote_range_index_to_tail(key).await?;
        current_stub.set_flushed(false);
        continue;
      }

      // 惰性恢复慢路径：卸载阻塞线程（条带锁自取自放，不跨线程边界）
      let mgr = Arc::clone(&self.store.range_index);
      let restore_key = key.to_vec();
      let restore_stub = current_stub;
      let tree = range_index_blocking(move || mgr.get_or_open_tree(&restore_key, &restore_stub))
        .await?
        .map_err(RangeIndexError::from)?;

      // 惰性激活补登换号回收旁表（同读面口径）
      self.register_bftree_key(key);

      // RIRESTORE 存根回写：不持任何条带锁（分裂设计同读面）
      self.restore_range_index_stub(key, &tree).await?;
    }
  }

  /// 写臂锁内刷新分层元记录副本
  ///
  /// 分层写臂的 meta 装载发生在条带锁之外（rmw_helpers 路由探测），两臂并发时
  /// 各持装载快照、收尾 [`Self::save_bftree_meta_stub`] 整体覆写会互相丢更新
  /// （size / next_expiry 增量被后写者抹掉）。本函数在独占写锁内重读元记录覆盖
  /// 调用方副本，使互斥窗口完整覆盖「装载 → 树写 → 计数 → 回写」。
  ///
  /// 返回假 = 键已被并发排空回收（无记录 / 非 live 元记录），调用臂放弃写面
  /// 穿透重建（写锁保证此刻起无人能再动该键，非 live 判定即终态）。
  pub async fn refresh_tiered_meta(&self, key: &[u8], meta: &mut MetaValue) -> Result<bool> {
    let meta_k = self.session_meta_key(key);
    let Some(bytes) = self.read_raw(&meta_k).await? else {
      return Ok(false);
    };
    if bytes.len() < META_VALUE_SIZE {
      return Ok(false);
    }
    let fresh = MetaValue::from_slice(&bytes[..META_VALUE_SIZE])?;
    if !fresh.is_live() {
      return Ok(false);
    }
    *meta = fresh;
    Ok(true)
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
      let _ = self
        .store
        .range_index
        .pre_stage_and_register_pending(key, src_addr);
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

  /// 可变区原位治愈快路径 (InPlaceUpdater 等价，RIPROMOTE/RIRESTORE 共用)
  ///
  /// find_tag 命中可变区记录时，页写锁内读-验-改：治愈内核 [`patch_stub_record`]
  /// 就地改存根窗口，零复制零分配。`try_modify_record_in_place` 返回 Ok(None)
  /// 仅表示记录不在可变区 (含 RC 链头 / 墓碑 / Tag 碰撞)。返回 Ok(true) 原位闭环
  /// (含命中但已治愈的零写)；Ok(false) 降级候选链慢路径
  async fn heal_stub_in_place(
    &self,
    meta_k: &[u8],
    patch: impl FnOnce(&mut RangeIndexStub) -> bool,
  ) -> StdResult<bool, RangeIndexError> {
    let _guard = self.enter_gated();
    let Some(addr) = self.store.index.load().find_tag(meta_k) else {
      return Ok(false);
    };
    if is_read_cache(addr) {
      return Ok(false);
    }
    let closed = self
      .store
      .hlog
      .try_modify_record_in_place(addr, meta_k, |val| Some(patch_stub_record(val, patch)))
      .map_err(Error::from)?;
    Ok(closed.is_some())
  }

  /// 候选链定位当前记录并追加治愈帧 (CopyUpdater 等价，RIPROMOTE/RIRESTORE 共用)
  ///
  /// 骨架、纪元纪律与 CAS 收尾（含败帧回复活池，杜绝旧实现「落败即遗弃」的
  /// 槽位泄漏）全部转调 copy-to-tail 内核 [`StoreSession::copy_record_to_tail`]，
  /// 本函数仅保留 RangeIndex 侧协议适配：位变更器交 wkv 唯一治愈内核
  /// [`patch_stub_record`] 在等长堆副本上就地改 35B 存根窗口后追加至日志尾部
  /// (对标 C# TryCopyToTail 先分配新尾记录再拷值，值体长度无上限)，plan 返回
  /// None = 非存活 RangeIndex 元记录 / 已治愈，零写闭环；同时携带源记录治愈前
  /// 的存根状态供 RIPROMOTE 所有权转移判定
  async fn tail_patch_record(
    &self,
    meta_k: &[u8],
    patch: impl Fn(&mut RangeIndexStub) -> bool,
  ) -> StdResult<TailPatchOutcome, RangeIndexError> {
    let outcome = self
      .copy_record_to_tail(meta_k, false, false, |record| {
        let Ok(val) = record.value() else {
          return None;
        };
        // 源记录存根状态取自治愈前的原帧 (RIPROMOTE 所有权转移判定依赖)
        let src_stub = range_index_stub_of(val);
        let mut frame = val.to_vec();
        if !patch_stub_record(&mut frame, |stub| patch(stub)) {
          return None;
        }
        Some((frame, src_stub))
      })
      .await?;
    Ok(match outcome {
      CopyToTailOutcome::Miss => TailPatchOutcome::Miss,
      CopyToTailOutcome::Closed => TailPatchOutcome::Closed,
      CopyToTailOutcome::Appended {
        src_addr,
        cas_ok,
        ctx: src_stub,
        ..
      } => TailPatchOutcome::Appended {
        src_addr,
        cas_ok,
        src_stub,
      },
    })
  }

  /// 惰性激活后的存根回写 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RestoreRangeIndexStub
  /// ——回写落地的存根字段变更本体见 wbftree::RangeIndexStub::recreate_index)
  ///
  /// C# 在 RestoreTree 恢复并注册树实例后，经 RIRESTORE RMW 把恢复树的新句柄写回
  /// 存根并清除 IsRecovered 位，本方法承接同一语义：
  /// - 快路径为可变区原位读-改-写 (InPlaceUpdater 等价，页写锁内读-验-改，零追加零 CAS)；
  /// - 只读/磁盘区降级候选链定位 + 补丁追加 + CAS 挂载 (CopyUpdater 等价)；
  /// - 墓碑 / 非 RangeIndex 元记录零写跳过 (RIRESTORE.NeedInitialUpdate=false：键已被
  ///   并发删除时 RMW 返回 NOTFOUND，绝不复活存根)；
  /// - 内部维护写旁路写监听 (对标 C# RIRESTORE 不入 AOF——瞬态句柄非用户写效果，
  ///   追加走 append_record_compacted，原位直调 hlog 无监听通知)；
  /// - 幂等：句柄已绑定当前树且 Recovered 位已清时零写。
  ///
  /// 清 Recovered 位的意义 (对标 C# RecreateIndex 注释)：使后续淘汰重开选择反映
  /// 激活后写入的刷盘快照而非过期的检查点快照——未清位时 get_or_open_tree 的恢复
  /// 源选择会永久绕过刷盘文件 (见 wbftree lifecycle 的 IsRecovered 分流)。
  ///
  /// 对标差异：C# 存根句柄是热路径直接调用的原生指针，重启后由 OnDiskRead 清零
  /// (InvalidateStub)；本实现路由一律走注册表、句柄仅作标识，故以「句柄 ≠ 当前树
  /// native_ptr」的补丁条件等价覆盖跨重启陈旧句柄的清理。C# 在 RIRESTORE 失败时把
  /// 整个 RestoreTree 视作失败 (客户端见 NOTFOUND)；本实现显式上抛 Internal——激活
  /// 已成功而回写失败属存储 I/O 故障，静默吞掉会掩盖持久化态与运行态的偏离。
  async fn restore_range_index_stub(
    &self,
    key: &[u8],
    tree: &BfTreeService,
  ) -> StdResult<(), RangeIndexError> {
    let meta_k = self.session_meta_key(key);
    let native = tree.native_ptr();

    // 1. InPlaceUpdater 等价：可变区页写锁内读-验-改。原位治愈闭环即返回；
    //    记录不在可变区 (含 RC 链头 / 墓碑 / Tag 碰撞) 交由候选链慢路径定位
    if self
      .heal_stub_in_place(&meta_k, |stub| recreate_patch(stub, native))
      .await?
    {
      return Ok(());
    }

    // 2. CopyUpdater 等价：候选链定位 + 补丁追加 + CAS 挂载。CAS 失败 (并发写
    //    移动链头)：败帧由 copy-to-tail 内核回复活池回收；治愈幂等，下次激活重试
    //    (对标 C# CopyUpdater CAS 败者不重试同帧)；RIRESTORE 无 PostCopyUpdater
    //    后处理——句柄是瞬态标识，源记录无需转移语义
    let _ = self
      .tail_patch_record(&meta_k, |stub| recreate_patch(stub, native))
      .await?;
    Ok(())
  }
}

/// RIPROMOTE/RIRESTORE 慢路径 (CopyUpdater 等价) 结果
enum TailPatchOutcome {
  /// 候选链无存活目标记录，零写
  Miss,
  /// 命中但零写闭环：墓碑 / 非 RI 元记录 / 已治愈
  Closed,
  /// 已追加治愈帧并尝试 CAS 挂载
  Appended {
    /// 命中的源记录地址 (PostCopyUpdater 所有权转移目标)
    src_addr: u64,
    /// 索引 CAS 是否成功
    cas_ok: bool,
    /// 源记录存根状态 (RIPROMOTE 所有权转移判定依赖)
    src_stub: Option<RangeIndexStub>,
  },
}

/// RangeIndex 复合元记录（`[MetaValue 32B][RangeIndexStub 35B][可选扩展]`）的存根解码单点
///
/// 在 garnet 中的相对路径: libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:ReadIndex
/// （C# 先按 `DataHeader.RecordType == RangeIndexRecordType` 判别记录类型，再
/// `Unsafe.As` 把值体首段 reinterpret 为存根；本仓记录类型事实由
/// [`MetaValue::is_range_index`] 承载，RI 元记录唯一编码口为
/// [`encode_meta_stub_record`]）
///
/// 返回 Some = 目标记录，`[META_VALUE_SIZE, +RANGE_INDEX_STUB_SIZE)` 窗口必在界内；
/// None = 非目标记录（定长不足 / Meta 解码失败 / 非 RangeIndex 类型），调用方零写跳过
#[inline]
pub(crate) fn range_index_stub_of(val: &[u8]) -> Option<RangeIndexStub> {
  if val.len() < META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE {
    return None;
  }
  let meta = MetaValue::from_slice(&val[..META_VALUE_SIZE]).ok()?;
  if !meta.is_range_index() {
    return None;
  }
  RangeIndexStub::decode(&val[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE]).ok()
}

/// wkv 唯一的 RangeIndex 存根治愈内核：解码 → 位变更 → 就地回填 35B 存根窗口
///
/// 本内核只承接「解码—就地回填」骨架，锚点仅挂 C# 侧无独立 rust 位变更器的两枚：
/// 在 garnet 中的相对路径: libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:SetFlushedFlag
/// 与 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:InvalidateStub
/// （清 Flushed / 标记恢复 / 重绑句柄 / 所有权转出四枚的符号锚点 1:1 挂在下方对应
/// 位变更器 [`clear_flushed_patch`] / [`mark_recovered_patch`] / [`recreate_patch`] /
/// [`transfer_out_patch`]，本内核不复挂；
/// C# 的 in-span 单点变更器族：全部经
/// `ref var stub = ref Unsafe.As<byte, RangeIndexStub>(ref valueSpan[0])` 就地改位，
/// 无任何「复制整值体」的第二形态；变更器只被
/// libs/server/Storage/Functions/GarnetRecordTriggers.cs:130/:157/:248 与
/// RMWMethods 的 RIPROMOTE/RIRESTORE 分支转调。wbftree 侧最后一个零调用切片位写入器
/// （`RangeIndexStub::slice_set_flushed`）已随本内核收口删除——
/// rust 位变更一律经本内核 + 下方四个位变更器，杜绝跨 crate 双口径）
///
/// 值体长度口径（超容量策略取堆侧，杜绝旧实现三种互斥口径）：内核只对存根窗口
/// 落笔，Meta 段与其后扩展字节原样保留，**不设任何上限**——C# 侧
/// `Debug.Assert(valueSpan.Length >= RangeIndexStub.Size)`（Index.cs:153）给出的是
/// 下界，压根不存在「值体超容量」概念，旧 rust 的 `[u8; 128]` 栈缓冲与
/// `val.len().min(128)` 是转写自造，并对 >128B 值体分裂出「Vec 回退 / 静默截断
/// 丢扩展 / 判 None 放弃治愈」三口径。确需新帧的调用点（RIPROMOTE/RIRESTORE 慢
/// 路径、紧缩搬迁、恢复原位失败降级）按 `val.to_vec()` 精确等长复制后交本内核
/// 就地改位，对标 C# TryCopyToTail 先分配新尾记录再拷值
/// (libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/
/// TryCopyToTail.cs:24)；原位路径（可变区页写锁、刷盘置位）零复制零分配。
/// 不采「超界拒绝并报错」：C# 无此对位，且会把 >上限的存活索引永久卡在 Flushed
/// 态反复晋升。
///
/// 返回 true = 已回填（值体被改）；false = 零写跳过（非目标记录，或 `patch`
/// 判定已治愈）
#[inline]
pub(crate) fn patch_stub_record(
  val: &mut [u8],
  patch: impl FnOnce(&mut RangeIndexStub) -> bool,
) -> bool {
  let Some(mut stub) = range_index_stub_of(val) else {
    return false;
  };
  if !patch(&mut stub) {
    return false;
  }
  val[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE].copy_from_slice(&stub.encode());
  true
}

/// 清除 Flushed 位 (在 garnet 中的相对路径:
/// libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:ClearFlushedFlag，
/// RIPROMOTE 尾部新帧与紧缩搬迁共用)
///
/// 已清零时返回 false 零写（幂等）
#[inline]
pub(crate) fn clear_flushed_patch(stub: &mut RangeIndexStub) -> bool {
  if !stub.is_flushed() {
    return false;
  }
  stub.set_flushed(false);
  true
}

/// 标记「已从检查点快照恢复」并清零句柄 (在 garnet 中的相对路径:
/// libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:MarkRecoveredFromCheckpoint)
///
/// 持久化字节已是该态时返回 false 零写：多轮恢复的重复回写纯属浪费——原位改写
/// 退化为同址重写，失败路径还多出一次追加 + 索引地址更新
#[inline]
pub(crate) fn mark_recovered_patch(stub: &mut RangeIndexStub) -> bool {
  if stub.tree_handle == 0 && stub.is_recovered() {
    return false;
  }
  stub.mark_recovered_from_checkpoint();
  true
}

/// 重绑在线树句柄并清 Recovered 位 (在 garnet 中的相对路径:
/// libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:RecreateIndex，RIRESTORE 回写)
///
/// 句柄已绑定当前树且恢复位已清时返回 false 零写（幂等）
#[inline]
fn recreate_patch(stub: &mut RangeIndexStub, new_tree_handle: u64) -> bool {
  if stub.tree_handle == new_tree_handle && !stub.is_recovered() {
    return false;
  }
  stub.recreate_index(new_tree_handle);
  true
}

/// 源存根所有权转出：句柄清零 + 置 Transferred 位
/// (在 garnet 中的相对路径:
/// libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:ClearTreeHandle 与
/// libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:SetTransferredFlag，
/// PostCopyToTail 活跃转移分支)
#[inline]
fn transfer_out_patch(stub: &mut RangeIndexStub) -> bool {
  if stub.tree_handle == 0 && stub.is_transferred() {
    return false;
  }
  stub.tree_handle = 0;
  stub.set_transferred(true);
  true
}

#[cfg(test)]
mod tests {
  use super::*;

  /// 构造测试基准存根 (句柄/缓存/契约长度均为可辨识值)
  fn base_stub(tree_handle: u64) -> RangeIndexStub {
    RangeIndexStub::new(
      tree_handle,
      4096,
      4,
      1024,
      128,
      4096,
      StorageBackendType::Disk,
    )
  }

  /// 编码 RI 元记录 (Meta + 存根定长栈编码)
  fn record(stub: &RangeIndexStub, size: u64) -> Vec<u8> {
    let meta = MetaValue::new(1, GarnetObjectType::RangeIndex, size);
    encode_meta_stub_record(&meta, stub).to_vec()
  }

  /// 取治愈后帧内的存根 (存根窗口界内性由内核保证)
  fn stub_in(val: &[u8]) -> RangeIndexStub {
    RangeIndexStub::decode(&val[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE])
      .expect("治愈帧存根必须可解码")
  }

  /// RIPROMOTE / 紧缩搬迁共用的清 Flushed 治愈：就地改位，Meta 段与句柄等其余字段原样保留
  #[test]
  fn clear_flushed_patch_heals_in_place() {
    let mut stub = base_stub(0xdead);
    stub.set_flushed(true);
    let src = record(&stub, 3);
    let mut healed = src.clone();
    assert!(patch_stub_record(&mut healed, clear_flushed_patch));
    assert_eq!(&healed[..META_VALUE_SIZE], &src[..META_VALUE_SIZE]);
    let out = stub_in(&healed);
    assert!(!out.is_flushed());
    assert_eq!(out.tree_handle, 0xdead);
    assert_eq!(out.cache_size, 4096);
  }

  /// 值体长度无上限：存根之后的扩展字节（含超旧 [u8;128] 栈缓冲的长扩展）逐字保留，
  /// 杜绝旧实现 `val.len().min(128)` 静默截断丢尾
  #[test]
  fn patch_preserves_extension_beyond_legacy_stack_cap() {
    let ext: Vec<u8> = (0..200u32).map(|i| i as u8).collect();
    let mut stub = base_stub(0xabcd);
    stub.set_flushed(true);
    stub.set_transferred(true);
    let mut healed = record(&stub, 5);
    healed.extend_from_slice(&ext);
    let total = healed.len();
    assert!(patch_stub_record(&mut healed, clear_flushed_patch));
    assert_eq!(healed.len(), total, "治愈不得改变值体长度");
    assert_eq!(
      &healed[META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE..],
      &ext[..],
      "扩展字段必须逐字保留"
    );
    let out = stub_in(&healed);
    assert!(!out.is_flushed());
    assert!(out.is_transferred(), "非目标位不得被顺带改写");
  }

  /// 治愈内核幂等：未置 Flushed 的记录零写跳过
  #[test]
  fn clear_flushed_patch_idempotent_when_not_flushed() {
    let mut src = record(&base_stub(7), 1);
    assert!(!patch_stub_record(&mut src, clear_flushed_patch));
  }

  /// RIRESTORE 治愈：跨重启陈旧句柄重绑当前树并清恢复位，其余字段原样保留
  #[test]
  fn recreate_patch_rebinds_handle_and_clears_recovered() {
    let mut stub = base_stub(0x11);
    stub.set_recovered(true);
    let src = record(&stub, 2);
    let mut healed = src.clone();
    assert!(patch_stub_record(&mut healed, |s| recreate_patch(s, 0x22)));
    assert_eq!(&healed[..META_VALUE_SIZE], &src[..META_VALUE_SIZE]);
    let out = stub_in(&healed);
    assert_eq!(out.tree_handle, 0x22);
    assert!(!out.is_recovered());
    assert_eq!(out.cache_size, 4096);
  }

  /// RIRESTORE 治愈幂等：句柄已绑定当前树且恢复位已清时零写跳过
  #[test]
  fn recreate_patch_idempotent_when_bound_and_clear() {
    let mut src = record(&base_stub(9), 2);
    assert!(!patch_stub_record(&mut src, |s| recreate_patch(s, 9)));
  }

  /// 恢复期治愈 (对标 C# MarkRecoveredFromCheckpoint)：句柄清零 + 置恢复位；已是该态零写
  #[test]
  fn mark_recovered_patch_zeroes_handle_and_is_idempotent() {
    let mut src = record(&base_stub(0x1234), 1);
    assert!(patch_stub_record(&mut src, mark_recovered_patch));
    let out = stub_in(&src);
    assert_eq!(out.tree_handle, 0);
    assert!(out.is_recovered());
    assert!(
      !patch_stub_record(&mut src, mark_recovered_patch),
      "二次恢复零写"
    );
  }

  /// 所有权转出治愈 (对标 C# ClearTreeHandle + SetTransferredFlag)；已是该态零写
  #[test]
  fn transfer_out_patch_clears_handle_and_is_idempotent() {
    let mut src = record(&base_stub(0x99), 1);
    assert!(patch_stub_record(&mut src, transfer_out_patch));
    let out = stub_in(&src);
    assert_eq!(out.tree_handle, 0);
    assert!(out.is_transferred());
    assert!(!patch_stub_record(&mut src, transfer_out_patch));
  }

  /// 公共校验面：非 RangeIndex 元记录与定长不足的记录一律零写跳过
  #[test]
  fn patch_rejects_non_range_index_and_short_record() {
    let mut flushed = base_stub(1);
    flushed.set_flushed(true);
    // 非 RI 记录（集合信封 / 字符串等形态的 Meta + 任意尾字节）一律拒绝：
    // 存根窗口只对 RI 记录落笔
    let hash_meta = MetaValue::new(1, GarnetObjectType::Hash, 7);
    let mut not_ri = encode_meta_stub_record(&hash_meta, &flushed).to_vec();
    assert!(!patch_stub_record(&mut not_ri, clear_flushed_patch));
    assert!(!patch_stub_record(&mut not_ri, mark_recovered_patch));
    assert!(range_index_stub_of(&not_ri).is_none());
    assert_eq!(
      not_ri,
      encode_meta_stub_record(&hash_meta, &flushed).to_vec()
    );
    // 空索引（size=0 的存活 RI 元记录，RI.CREATE 即此态）同样纳入治愈
    let mut empty = record(&flushed, 0);
    assert!(patch_stub_record(&mut empty, clear_flushed_patch));
    // 定长不足
    let mut short = [0u8; 8];
    assert!(!patch_stub_record(&mut short, clear_flushed_patch));
    assert!(range_index_stub_of(&short).is_none());
  }
}
