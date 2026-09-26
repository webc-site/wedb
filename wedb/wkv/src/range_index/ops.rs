use std::{
  result::Result as StdResult,
  sync::{Arc, atomic::Ordering},
};

use wbftree::{
  Error as WbftreeError, RangeIndexManager, RangeIndexStub, ScanReturnField, StorageBackendType,
  TreeTuning,
};
use wdev::Device;
use wval::{GarnetObjectType, KeyTag, MetaValue};

use super::{RangeIndexError, RangeIndexMetrics, range_index_blocking};
use crate::{
  error::{CollectionError, Error, Result},
  ri::RiTreeOps,
  session::StoreSession,
  store::StoreEvent,
};

/// 0 值取默认的微小解析器 (创建时把解析后的实际值固化进存根，绝不为 0)
#[inline]
const fn nz_or(v: usize, d: usize) -> usize {
  if v > 0 { v } else { d }
}

impl<D: Device> StoreSession<D> {
  /// RI 点操作装载单点：`load_range_index_stub` 缺席（None）统一收敛 NotFound
  async fn load_ri_stub_or_notfound(
    &self,
    key: &[u8],
  ) -> StdResult<(MetaValue, RangeIndexStub), RangeIndexError> {
    self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)
  }
  /// 创建新的 RangeIndex 索引 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexCreate)
  ///
  /// libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:CreateIndex 的承接：
  /// C# 把 TreeHandle + BfTree 调优参数 + 清零标志位固化进值体存根（35B），
  /// rust 对位为下方第 4 步 `RangeIndexStub::from_tuning(tree.native_ptr(), ..)`
  /// 构建存根并经 save_bftree_meta_stub 单点落盘。
  ///
  /// 键位按物理域三态裁决：String 域命中 / 集合信封域命中 / 存活非 RI 元记录
  /// 一律 `WrongType`（C# 该处由存储层记录类型判别回 WrongType，"index already
  /// exists" 只属于已存活的 RI 元记录），存活 RI 元记录才是 `AlreadyExists`
  pub async fn range_index_create(
    &self,
    key: &[u8],
    storage_backend: StorageBackendType,
    tuning: TreeTuning,
  ) -> StdResult<(), RangeIndexError> {
    // 树身份键 = 物理 Meta 键（会话域内派生单点）：树注册、数据文件、claim
    // 判定按物理域隔离，跨库同名索引互不撞面（wbftree key_id_of 身份契约）
    let id_key = self.session_meta_key(key);
    // 0. RENAME 迁移 claim 判定：迁移窗内显式拒绝（MigrationBusy 锁忙/重试
    //    语义）。无此早退，claim 命中会令下方 load_meta 视同不存在而走重建
    //    路径——在 dst 旧树域叠建新树（冷树注册表缺席时裸建 clobber 数据文件），
    //    换一种已 ACK 写丢失形
    if self.store.range_index.migration_claimed(&id_key) {
      return Err(Error::MigrationBusy.into());
    }

    // 1. 物理域三态裁决（对标 C#：RICREATE 经 RMW 落存储层，先判 ValueIsObject
    //    与记录类型，非 RI 记录一律 WrongType，唯存活 RI 元记录才是重复创建）
    //    String 域命中：普通字符串键不可当索引用
    if self.read(key).await?.is_some() {
      return Err(RangeIndexError::WrongType);
    }
    //    Meta 域命中：存活 RI 记录 = 重复创建，存活其他集合类型 = 类型不符，
    //    死记录（过期视同不存在）放行重建
    if let Some(meta) = self.load_meta(key).await?
      && meta.is_live()
    {
      return if meta.collection_type == GarnetObjectType::RangeIndex {
        Err(RangeIndexError::AlreadyExists)
      } else {
        Err(RangeIndexError::WrongType)
      };
    }
    //    集合信封域命中：C# ValueIsObject 先于类型白名单
    let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, key);
    if self.contains_key_raw(&env_k).await? {
      return Err(RangeIndexError::WrongType);
    }

    // 2. 解析调优参数：0 值取默认并在创建时固化进存根 (对标 C# RespServerSessionRangeIndex.cs:44-50
    //    Defaults，单点 TreeTuning::DEFAULT_RI——后续长度校验与惰性恢复重建都拿
    //    真实值，绝不为 0；否则全零存根会让 set 的长度校验把一切写入拒之门外)
    let mut create_tuning = TreeTuning {
      cache_size: nz_or(tuning.cache_size, TreeTuning::DEFAULT_RI.cache_size),
      min_record_size: nz_or(
        tuning.min_record_size,
        TreeTuning::DEFAULT_RI.min_record_size,
      ),
      max_record_size: nz_or(
        tuning.max_record_size,
        TreeTuning::DEFAULT_RI.max_record_size,
      ),
      max_key_len: nz_or(tuning.max_key_len, TreeTuning::DEFAULT_RI.max_key_len),
      leaf_page_size: tuning.leaf_page_size,
    };
    RangeIndexManager::resolve_tuning(&mut create_tuning);

    // 3. 在底层 RangeIndexManager 中创建并托管 BfTree 实例
    //    (数据文件创建 + 环形缓冲分配属重操作，卸载阻塞线程保护 compio 核)
    let mgr = Arc::clone(&self.store.range_index);
    let create_key = id_key.clone();
    let create_backend = storage_backend;
    let store = Arc::clone(&self.store);
    let tree = range_index_blocking(move || {
      let first = mgr.create_bftree(&create_key, create_backend, create_tuning);
      // 预算耗拒绝臂自愈（冷树回收接线，对位 C# GarnetRecordTriggers.cs:OnEvict
      // → DisposeTreeUnderLock(deleteFiles:false) 的即时释放上半场）：预留被拒
      // 即同步驱动一轮冷树回收（仅摘 is_on_disk 且冷满迟滞窗的树，见
      // [`crate::store::WedbStore::recycle_cold_bftrees`] 语义），腾出常驻页环后重试一次
      // 预留；仍失败方原样上抛回客户端报错。本臂已在 `range_index_blocking`
      // （spawn_blocking 线程池）承接，同步回收的重 I/O 不触异步反应器
      if matches!(first, Err(WbftreeError::CacheBudgetExhausted)) {
        store.recycle_cold_bftrees();
        mgr.create_bftree(&create_key, create_backend, create_tuning)
      } else {
        first
      }
    })
    .await?
    .map_err(RangeIndexError::from)?;

    // 换号回收旁表登记：树身份含域后主存仍无键遍历 API 无法按域反查，创建时
    // 以会话当前域登记用户键（一参收口，见 session register_bftree_key），供
    // FLUSHDB/FLUSHNS 换号联动延迟销毁（vdb::bftree_release）
    self.register_bftree_key(key);

    // 4. 构建定长 35 字节 RangeIndexStub 并持久化入主日志库（落盘工序列
    //    save_bftree_meta_stub 门面单点）
    let stub = RangeIndexStub::from_tuning(tree.native_ptr(), &create_tuning, storage_backend);

    let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
    let meta = MetaValue::new(key_id, GarnetObjectType::RangeIndex, 0);

    if let Err(e) = self.save_bftree_meta_stub(key, &meta, &stub).await {
      // 事务回滚：清理此前在内存中注册及磁盘生成的孤儿文件
      // (尽力而为：主错误已优先上抛，回滚自身的屏障超时属进程级故障，不再覆盖)
      self.unregister_bftree_key(key);
      let _ = self.store.range_index.delete_index(&id_key);
      return Err(RangeIndexError::from(e));
    }

    // 事件域取会话物理域（与 meta 记录键前缀同源），入账键与落域一致。
    // AOF 入账失败按 error.rs AofEnqueue 契约以「已生效 + 镜像缺失」上抛
    // 拒绝本命令（同 promote/migration 冒泡臂单一机制），杜绝副本/重放缺条目
    // 静默发散
    let (ns, db) = self.virtual_domain();
    self
      .store
      .emit_event(
        self.aof_session_id,
        StoreEvent::RangeIndexCreate {
          ns,
          db,
          key,
          backend: &storage_backend,
          tuning: create_tuning,
        },
      )
      .map_err(RangeIndexError::from)
  }

  /// 设置字段值 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexSet)
  ///
  /// 写臂锁形态：多步序列「装载 → 树写 → 计数 → meta 回写」持条带独占写锁
  /// ([`Self::acquire_tree_write`]) 全程互斥，并在锁内经 [`Self::refresh_tiered_meta`]
  /// 刷新最新元数据与存根——排队写者绝不基于锁前旧采样覆写最新计数（共享读锁
  /// 下并发读改写窗口的 meta.size 丢更新即本函历史缺陷，破坏 O(1) RI.COUNT
  /// 契约）。刷新返回假 = 键已被并发排空回收，终态拒绝 NotFound，禁穿透重建。
  ///
  /// WATCH 版本栅栏（对标 C# 主存/对象域写钩子 functionsState.watchVersionMap.
  /// IncrementVersion，见 libs/server/Storage/Functions/MainStore/RMWMethods.cs
  /// 与 ObjectStore/RMWMethods.cs:79/:100/:200）：树内容实际变更即在独占守卫
  /// 释放后恰一次推进，元记录回写失败臂同样推进（树已改而不推进即 WATCH 漏
  /// 通知，事务隔离破损）；树写零变更的错误臂（NotFound / InvalidKV / 树内失败）
  /// 零推进，对齐 C# 纯读与拒绝臂不 IncrementVersion。
  pub async fn range_index_set(
    &self,
    key: &[u8],
    field: &[u8],
    value: &[u8],
  ) -> StdResult<(), RangeIndexError> {
    let (mut meta, mut stub) = self.load_ri_stub_or_notfound(key).await?;

    validate_ri_kv(&stub, field, value)?;

    // 树内容是否实际变更（置真必在 ri_set 成功之后，作 WATCH 推进唯一判据）
    let mut applied = false;
    let res: StdResult<(), RangeIndexError> = {
      let tree = self.acquire_tree_write(key, &mut stub).await?;
      // 独占锁内刷新：互斥窗口完整覆盖「装载 → 树写 → 计数 → 回写」
      if !self
        .refresh_tiered_meta(key, &mut meta, Some(&mut stub))
        .await?
      {
        return Err(RangeIndexError::NotFound);
      }
      // 锁内刷新后按最新存根复核长度契约（锁前窗口索引可能被删空重建换参，
      // 锁前预校验结论不作数），违例零树写即拒
      validate_ri_kv(&stub, field, value)?;
      match tree.ri_set(field, value) {
        Ok(is_new) => {
          applied = true;
          if is_new {
            meta.inc_size(1);
            match self.save_bftree_meta_stub(key, &meta, &stub).await {
              Ok(()) => Ok(()),
              Err(e) => Err(e.into()),
            }
          } else {
            Ok(())
          }
        }
        Err(CollectionError::KeyTooLong) => Err(invalid_kv_error(&stub, field, value.len())),
        Err(e) => Err(e.into()),
      }
      // tree 独占守卫随本块尾析构释放
    };
    if applied {
      // 释放独占锁后推进观察者栅栏，再入队 AOF
      self.bump_watch_version(key);
    }
    res?;
    // AOF 入账失败按 error.rs AofEnqueue 契约以「已生效 + 镜像缺失」上抛
    // 拒绝本命令（副本/重放缺条目即主从发散，禁吞错回成功）
    let (ns, db) = self.virtual_domain();
    self.store.emit_event(
      self.aof_session_id,
      StoreEvent::RangeIndexWrite {
        ns,
        db,
        key,
        field,
        val: value,
        delete: false,
      },
    )?;
    Ok(())
  }

  /// 批量设置字段值，返回写入后真实新增的元素数
  ///
  /// rust 侧工程优化批量入口 (C# RangeIndexOps 无对应批量接口——迁移/复制均走
  /// 文件块流换入，见 libs/server/Storage/Session/MainStore/RangeIndexOps.cs 与
  /// libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs；
  /// 按 .agents/skills/transpile/SKILL.md 批量接口单次折叠机制实现)：
  /// 单次折叠元记录加载与长度预校验、树会话获取 (acquire_tree_write 独占写锁
  /// 一次，锁内 refresh_tiered_meta 刷新后互斥窗口覆盖「装载 → 树写 → 计数 →
  /// 回写」全程，杜绝共享读锁下并发覆写 meta.size 丢更新)、meta.size 单次增量
  /// 回写 (1 次 RMW 落盘，消除 N 次元数据写放大)；
  /// 树内写入经 ri_set_batch 栈上索引排序集中命中相邻页压降页分裂。
  ///
  /// 预校验先于任何写入：任一条目违反长度契约即整体失败返回零副作用错误；
  /// 同批重复字段取末值，与逐条 [`Self::range_index_set`] 最终态一致。事件
  /// 按输入原序逐条发射 (AOF 复制端逐条重放收敛同态，对标单点语义)。
  ///
  /// WATCH 版本栅栏同单点写臂：树内容实际变更（含覆盖写与异常兜底臂的部分
  /// 写入）即在独占守卫释放后显式恰一次推进，零变更错误臂零推进。
  pub async fn range_index_set_batch(
    &self,
    key: &[u8],
    entries: &[(&[u8], &[u8])],
  ) -> StdResult<usize, RangeIndexError> {
    let (mut meta, mut stub) = self.load_ri_stub_or_notfound(key).await?;

    for &(field, value) in entries {
      validate_ri_kv(&stub, field, value)?;
    }

    // 树内容是否实际变更（ri_set_batch 成功即覆写已落树，作 WATCH 推进判据）
    let mut applied = false;
    let res: StdResult<usize, RangeIndexError> = {
      let tree = self.acquire_tree_write(key, &mut stub).await?;
      // 独占锁内刷新：互斥窗口完整覆盖「装载 → 树写 → 计数 → 回写」
      if !self
        .refresh_tiered_meta(key, &mut meta, Some(&mut stub))
        .await?
      {
        return Err(RangeIndexError::NotFound);
      }
      // 锁内刷新后按最新存根复核长度契约（同单点写臂：重建换参即重判），
      // 违例零树写整体拒绝
      for &(field, value) in entries {
        validate_ri_kv(&stub, field, value)?;
      }
      match tree.ri_set_batch(entries) {
        Ok(inserted) => {
          applied = true;
          if inserted > 0 {
            meta.inc_size(inserted as u64);
            match self.save_bftree_meta_stub(key, &meta, &stub).await {
              Ok(()) => Ok(inserted),
              Err(e) => Err(e.into()),
            }
          } else {
            Ok(inserted)
          }
        }
        Err(CollectionError::KeyTooLong) => {
          // 预校验已按存根契约放行，此处仅为树配置与存根偏离的异常态兜底；
          // 部分写入已发生（置真补推栅栏），语义同单点路径的存储层兜底
          applied = true;
          Err(RangeIndexError::Internal(
            "ri_set_batch 树配置与存根长度契约偏离".to_string(),
          ))
        }
        Err(e) => Err(RangeIndexError::from(e)),
      }
      // tree 独占守卫随本块尾析构释放
    };
    if applied {
      // 释放独占锁后推进观察者栅栏，再入队 AOF
      self.bump_watch_version(key);
    }
    let inserted = res?;
    let (ns, db) = self.virtual_domain();
    for &(field, value) in entries {
      // AOF 入账失败按 error.rs AofEnqueue 契约以「已生效 + 镜像缺失」上抛
      // 拒绝本命令（副本/重放缺条目即主从发散，禁吞错回成功）
      self.store.emit_event(
        self.aof_session_id,
        StoreEvent::RangeIndexWrite {
          ns,
          db,
          key,
          field,
          val: value,
          delete: false,
        },
      )?;
    }
    Ok(inserted)
  }

  /// 读取字段值，经闭包零拷贝借阅树页切片 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexGet)
  ///
  /// C# 经 ReadByPtrInto 把 BfTree 值直写网络输出缓冲（零中间拷贝）；rust 侧
  /// 由调用方在闭包内消费 `&[u8]` 视图直写 output，达成同一零拷贝形态。
  /// 闭包入参 `None` = 字段不存在（C# RangeIndexResult.NotFound），缺失语义
  /// 由返回值 `R` 自行承载。
  pub async fn range_index_get_with<R>(
    &self,
    key: &[u8],
    field: &[u8],
    f: impl FnOnce(Option<&[u8]>) -> R,
  ) -> StdResult<R, RangeIndexError> {
    let (_, mut stub) = self.load_ri_stub_or_notfound(key).await?;

    let tree = self.acquire_tree_read(key, &mut stub).await?;

    tree
      .ri_get_callback(field, f)
      .map_err(RangeIndexError::from)
  }

  /// 读取字段值 (深拷贝便捷封装，仅供测试断言；RESP 热路径走 [`Self::range_index_get_with`] 零拷贝直写)
  pub async fn range_index_get(
    &self,
    key: &[u8],
    field: &[u8],
  ) -> StdResult<Option<Vec<u8>>, RangeIndexError> {
    self
      .range_index_get_with(key, field, |v| v.map(<[u8]>::to_vec))
      .await
  }

  /// 删除字段 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexDel)
  ///
  /// 删空自愈（.agents/skills/transpile/SKILL.md 严格删空生命周期条 /
  /// doc/zh/collection.md 3.3 六类集合含 RangeIndex 统一口径）：实删后计数归零
  /// 即走树态键排空回收单点 [`Self::handle_bftree_drain_and_delete`]（信封域幂等
  /// 墓碑 + 元记录原子墓碑 + 随键 TTL 清理 + BfTree 树实例注销 + 换号旁表登记
  /// 注销 + RangeIndexDrop 入账），与四族分层漏斗 wnode `drain_or_save` 同一内核，
  /// 杜绝幽灵空元记录、孤儿 TTL 与树文件句柄永驻。判据取本次实删后的
  /// `meta.size`，不经
  /// `MetaValue::is_live`（RI 类型恒活，正是空删不自愈的放大根源）。非删空臂
  /// （size > 0）行为零变更：元记录 + 存根回写。
  ///
  /// 两臂互斥收尾：删空臂绝不回写元记录——`save_bftree_meta_stub` 会把刚墓碑化
  /// 的记录复活成幽灵空索引。
  ///
  /// 锁次序同 `drain_or_save`：独占树写守卫在上方块尾显式 drop 先行释放，
  /// drain 侧 `delete_index` 会再取同条带写锁，守卫未放即自死锁；排空在途
  /// 读者同样要求条带写锁已交出。
  ///
  /// WATCH 版本栅栏：实删成功（树内容实际变更）即在守卫释放后恰一次推进，
  /// 两臂同权——删空臂仅经 wkv 物理键原语（delete_raw / del_ttl），非删空臂
  /// 仅经 save_bftree_meta_stub，均不经用户键写入口收口，故本层按 wnode
  /// `apply_rmw_post_operate` 同款显式补齐（旧实现非删空臂不推进致 WATCH
  /// 事务隔离破损，即本函历史缺陷）；字段不存在（removed=false）零变更零推进。
  ///
  /// 复制面 AOF 先后顺序固定：删空臂 drain 的整键墓碑（RangeIndexDrop →
  /// StoreDelete）先行入账，函数尾部 RangeIndexWrite(delete=true) 字段条目后至
  ///（与四族同步 RMW 骨架 `run_sync_rmw` 的「墓碑写回先入账、增量条目后通知」同
  /// 序）。副本据此确定性收敛：删空臂各物理域消亡逐域入账（信封域墓碑由
  /// delete_raw 同栈的 ObjectEnvelope StoreDelete 条目承接、TTL/ETag 旁路经
  /// TtlWrite/EtagWrite 确定性条目承接），Meta 条目在回放面只排空 Meta 域，
  /// 随后字段条目命中 NotFound 由 `handle_range_index_del_replay` 静默跳过，
  /// 绝不残留；主臂与回放臂共用本函数，删空自愈在两侧同源生效。
  pub async fn range_index_del(
    &self,
    key: &[u8],
    field: &[u8],
  ) -> StdResult<bool, RangeIndexError> {
    let (mut meta, mut stub) = self.load_ri_stub_or_notfound(key).await?;

    // 实删判定与是否删空只在独占锁内取刷新后的最新计数，杜绝共享读锁下
    // 两写者同读旧 size 各自递减、双双错过 size == 0 的自愈漏删面
    let mut removed = false;
    let mut drained = false;
    let res: StdResult<(), RangeIndexError> = {
      let tree = self.acquire_tree_write(key, &mut stub).await?;
      // 独占锁内刷新：互斥窗口完整覆盖「装载 → 树删 → 计数 → 回写」
      if !self
        .refresh_tiered_meta(key, &mut meta, Some(&mut stub))
        .await?
      {
        return Err(RangeIndexError::NotFound);
      }
      match tree.ri_del(field) {
        Ok(true) => {
          removed = true;
          meta.dec_size(1);
          drained = meta.size == 0;
          let saved = if drained {
            // 删空臂绝不回写元记录（save 会把刚墓碑化的记录复活成幽灵空索引）
            Ok(())
          } else {
            self.save_bftree_meta_stub(key, &meta, &stub).await
          };
          // 防死锁析构排序：drain 前必须显式交出独占条带守卫（内部
          // delete_index 再取同条带写锁）；本臂未走 drain，同样先放锁再收尾
          drop(tree);
          match saved {
            Ok(()) => Ok(()),
            Err(e) => Err(e.into()),
          }
        }
        Ok(false) => {
          drop(tree);
          Ok(())
        }
        Err(e) => {
          drop(tree);
          Err(e.into())
        }
      }
    };
    if removed {
      if drained {
        // 删空自愈：接既有排空回收内核（keep_ttl=false 随键清 TTL 杜绝孤儿），
        // 不新造第二套清理编排、不加后台周期任务；守卫已交出，delete_index
        // 自取条带写锁无自死锁
        let drained_res = self.handle_bftree_drain_and_delete(key, false).await;
        // drain 失败经 Error::Swapped 分级：元记录墓碑后键已死、物理面已变更，
        // 栅栏照常推进；墓碑前失败键未消亡，同样推进无害（实删已生效）
        self.bump_watch_version(key);
        drained_res?;
      } else {
        // 非删空实删同样推进观察者栅栏（补齐旧实现漏推进的缺陷面）
        self.bump_watch_version(key);
      }
    }
    res?;
    // AOF 入账失败按 error.rs AofEnqueue 契约以「已生效 + 镜像缺失」上抛
    // 拒绝本命令（副本/重放缺条目即主从发散，禁吞错回成功）
    let (ns, db) = self.virtual_domain();
    self.store.emit_event(
      self.aof_session_id,
      StoreEvent::RangeIndexWrite {
        ns,
        db,
        key,
        field,
        val: &[],
        delete: true,
      },
    )?;
    Ok(true)
  }

  /// 基于数量的流式范围扫描 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexScan，
  /// 内部栈缓冲区零分配回调，透传切片引用，O(1) 空间复杂度)
  pub async fn range_index_scan_stream<F>(
    &self,
    key: &[u8],
    start: &[u8],
    count: usize,
    return_field: ScanReturnField,
    on_record: F,
  ) -> StdResult<usize, RangeIndexError>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    let (meta, mut stub) = self.load_ri_stub_or_notfound(key).await?;

    if stub.storage_backend == StorageBackendType::Memory.to_u8() {
      return Err(RangeIndexError::MemoryModeNotSupported);
    }

    if count == 0 || meta.size == 0 {
      return Ok(0);
    }

    let tree = self.acquire_tree_read(key, &mut stub).await?;

    Ok(tree.ri_scan_with_field(start, count, return_field, on_record)?)
  }

  /// 闭区间流式范围扫描 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexRange，
  /// 内部栈缓冲区零分配回调，透传切片引用，O(1) 空间复杂度)
  pub async fn range_index_range_stream<F>(
    &self,
    key: &[u8],
    start: &[u8],
    end: &[u8],
    return_field: ScanReturnField,
    on_record: F,
  ) -> StdResult<usize, RangeIndexError>
  where
    F: FnMut(&[u8], &[u8]) -> bool,
  {
    let (meta, mut stub) = self.load_ri_stub_or_notfound(key).await?;

    if stub.storage_backend == StorageBackendType::Memory.to_u8() {
      return Err(RangeIndexError::MemoryModeNotSupported);
    }

    if meta.size == 0 || start > end {
      return Ok(0);
    }

    let tree = self.acquire_tree_read(key, &mut stub).await?;

    Ok(tree.ri_range_with_field(start, end, return_field, on_record)?)
  }

  /// 获取 RangeIndex 元素计数（O(1) 复杂度计数规约的唯一实现：
  /// `load_range_index_stub` 一次主存读直取 MetaValue.size，不触树、
  /// 不调用 `acquire_tree_read`，严禁扫树兜底）。RESP 面 RI.COUNT 与其
  /// 同命令双名 RI.LEN 均接本函数。
  pub async fn range_index_count(&self, key: &[u8]) -> StdResult<usize, RangeIndexError> {
    let (meta, _) = self.load_ri_stub_or_notfound(key).await?;
    Ok(meta.size as usize)
  }

  /// 检查索引是否存在且为 RangeIndex (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexExists)
  pub async fn range_index_exists(&self, key: &[u8]) -> Result<bool> {
    if let Some(meta) = self.load_meta(key).await?
      && meta.is_range_index()
    {
      return Ok(true);
    }
    Ok(false)
  }

  /// 获取索引配置 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexConfig)
  pub async fn range_index_config(&self, key: &[u8]) -> StdResult<RangeIndexStub, RangeIndexError> {
    let (_, stub) = self.load_ri_stub_or_notfound(key).await?;
    Ok(stub)
  }

  /// 获取索引指标与运行状态 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexMetrics)
  pub async fn range_index_metrics(
    &self,
    key: &[u8],
  ) -> StdResult<RangeIndexMetrics, RangeIndexError> {
    let (_, stub) = self.load_ri_stub_or_notfound(key).await?;

    // is_live 只以注册表为准：stub.tree_handle 仅作标识且可能为陈旧值
    // (崩溃恢复改写注册表前)，据此推断会误报。树身份键 = 物理 Meta 键，
    // 跨库同名键的在线状态按物理域隔离
    let id_key = self.session_meta_key(key);
    let (tree_handle, is_live) = match self.store.range_index.get_tree(&id_key) {
      Some(tree) => (tree.native_ptr(), true),
      None => (0, false),
    };

    Ok(RangeIndexMetrics {
      tree_handle,
      is_live,
      is_flushed: stub.is_flushed(),
      is_recovered: stub.is_recovered(),
    })
  }
}

/// wbftree 记录长度契约校验单点 (对标 C# RangeIndexSet 的 InvalidKV 错误文案，
/// RI 面单点/批量、wnode 分层集合写臂预校验与 wkv 升阶建树口闸共用，禁止各处
/// 另写长度判定)。`record_len` 为实际落树记录长度：RI 面即原值长，分层面为
/// member_ttl 编码后长。
///
/// 空键与空记录两门是引擎受理面的精确镜像：bf-tree `insert` 拒空键
/// (tree.rs「Key too small, at least one byte」)、批量内核 `write_batch` 前置
/// 拒空值——缺此两门即「过预校验、树内拒写」，空成员（树键侧：set/zset 成员、
/// hash 字段）ZADD 部分提交后回错误帧、SADD/HSET 回配置偏离失真文案，含空成员
/// 集合触阈升阶 O(N) 建树尝试永不收敛。空载荷侧（list 元素、hash 值）经
/// member_ttl 编码记录恒 ≥ 1B，不触两门、引擎受理，与内存态一致。RI 面同输入
/// 原本也经引擎拒后以 KeyTooLong→invalid_kv_error 映射回同文案，补门仅前置化
/// 零语义回归；分层态空成员自此与超长成员同待遇，内存态受理面（C# 对象层
/// 无条件受理空成员）的双态分叉沿「两上限不同源」同类目先例
#[inline]
pub fn validate_bftree_record(
  stub: &RangeIndexStub,
  key: &[u8],
  record_len: usize,
) -> StdResult<(), RangeIndexError> {
  if key.is_empty()
    || record_len == 0
    || key.len() > stub.max_key_len as usize
    || key.len() + record_len < stub.min_record_size as usize
    || key.len() + record_len > stub.max_record_size as usize
  {
    return Err(invalid_kv_error(stub, key, record_len));
  }
  Ok(())
}

/// 存根长度契约校验 (RI 面便捷形：落树记录即原值)
#[inline]
fn validate_ri_kv(
  stub: &RangeIndexStub,
  field: &[u8],
  value: &[u8],
) -> StdResult<(), RangeIndexError> {
  validate_bftree_record(stub, field, value.len())
}

/// 构造 InvalidKV 错误 (携带存根契约与实际长度，供 C# 同款错误文案渲染)
#[inline]
fn invalid_kv_error(stub: &RangeIndexStub, field: &[u8], record_len: usize) -> RangeIndexError {
  RangeIndexError::InvalidKV {
    min_record_size: stub.min_record_size,
    max_record_size: stub.max_record_size,
    max_key_len: stub.max_key_len,
    total_len: field.len() + record_len,
    key_len: field.len(),
  }
}
