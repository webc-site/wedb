use std::{
  result::Result as StdResult,
  sync::{Arc, atomic::Ordering},
};

use wbftree::{RangeIndexManager, RangeIndexStub, ScanReturnField, StorageBackendType, TreeTuning};
use wdev::Device;
use wval::{GarnetObjectType, KeyTag, MetaValue};

use super::{RangeIndexError, RangeIndexMetrics, encode_meta_stub_record, range_index_blocking};
use crate::{
  error::{CollectionError, Result},
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
  /// 创建新的 RangeIndex 索引 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexCreate)
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
    let create_key = key.to_vec();
    let create_backend = storage_backend;
    let tree =
      range_index_blocking(move || mgr.create_bftree(&create_key, create_backend, create_tuning))
        .await?
        .map_err(RangeIndexError::from)?;

    // 换号回收旁表登记：树键无域前缀、主存无键遍历 API 无法按域反查，创建时
    // 以会话当前域登记（一参收口，见 session register_bftree_key），供
    // FLUSHDB/FLUSHNS 换号联动延迟销毁（store::reclaim）
    self.register_bftree_key(key);

    // 4. 构建定长 35 字节 RangeIndexStub 并持久化入主日志库
    let stub = RangeIndexStub::from_tuning(tree.native_ptr(), &create_tuning, storage_backend);

    let meta_k = self.session_meta_key(key);
    let key_id = self.store.next_key_id.fetch_add(1, Ordering::Relaxed);
    let meta = MetaValue::new(key_id, GarnetObjectType::RangeIndex, 0);

    let val = encode_meta_stub_record(&meta, &stub);

    if let Err(e) = self.upsert_raw(&meta_k, &val).await {
      // 事务回滚：清理此前在内存中注册及磁盘生成的孤儿文件
      // (尽力而为：主错误已优先上抛，回滚自身的屏障超时属进程级故障，不再覆盖)
      self.unregister_bftree_key(key);
      let _ = self.store.range_index.delete_index(key);
      return Err(RangeIndexError::from(e));
    }

    // 事件域取会话物理域（与 meta 记录键前缀同源），入账键与落域一致
    let (ns, db) = self.virtual_domain();
    if let Err(e) = self.store.emit_event(StoreEvent::RangeIndexCreate {
      ns,
      db,
      key,
      backend: &storage_backend,
      tuning: create_tuning,
    }) {
      log::error!("RangeIndex AOF 入队失败: {e}");
    }

    Ok(())
  }

  /// 设置字段值 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexSet)
  pub async fn range_index_set(
    &self,
    key: &[u8],
    field: &[u8],
    value: &[u8],
  ) -> StdResult<(), RangeIndexError> {
    let (mut meta, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    validate_ri_kv(&stub, field, value)?;

    let is_new = {
      let tree = self.acquire_tree_read(key, &stub).await?;
      match tree.ri_set(field, value) {
        Ok(is_new) => is_new,
        Err(CollectionError::KeyTooLong) => {
          return Err(invalid_kv_error(&stub, field, value.len()));
        }
        Err(e) => return Err(RangeIndexError::from(e)),
      }
    };

    if is_new {
      meta.inc_size(1);
      self.save_bftree_meta_stub(key, &meta, &stub).await?;
    }
    let (ns, db) = self.virtual_domain();
    if let Err(e) = self.store.emit_event(StoreEvent::RangeIndexWrite {
      ns,
      db,
      key,
      field,
      val: value,
      delete: false,
    }) {
      log::error!("RangeIndex AOF 入队失败: {e}");
    }
    Ok(())
  }

  /// 批量设置字段值，返回写入后真实新增的元素数
  ///
  /// rust 侧工程优化批量入口 (C# RangeIndexOps 无对应批量接口——迁移/复制均走
  /// 文件块流换入，见 libs/server/Resp/RangeIndex/RangeIndexManager.Migration.cs；
  /// 按 .agents/skills/transpile/SKILL.md 批量接口单次折叠机制实现)：
  /// 单次折叠元记录加载与长度预校验、树会话获取 (acquire_tree_read 共享读锁
  /// 一次)、meta.size 单次增量回写 (1 次 RMW 落盘，消除 N 次元数据写放大)；
  /// 树内写入经 ri_set_batch 栈上索引排序集中命中相邻页压降页分裂。
  ///
  /// 预校验先于任何写入：任一条目违反长度契约即整体失败返回零副作用错误；
  /// 同批重复字段取末值，与逐条 [`Self::range_index_set`] 最终态一致。事件
  /// 按输入原序逐条发射 (AOF 复制端逐条重放收敛同态，对标单点语义)。
  pub async fn range_index_set_batch(
    &self,
    key: &[u8],
    entries: &[(&[u8], &[u8])],
  ) -> StdResult<usize, RangeIndexError> {
    let (mut meta, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    for &(field, value) in entries {
      validate_ri_kv(&stub, field, value)?;
    }

    let inserted = {
      let tree = self.acquire_tree_read(key, &stub).await?;
      match tree.ri_set_batch(entries) {
        Ok(inserted) => inserted,
        Err(CollectionError::KeyTooLong) => {
          // 预校验已按存根契约放行，此处仅为树配置与存根偏离的异常态兜底；
          // 部分写入已发生，语义同单点路径的存储层兜底
          return Err(RangeIndexError::Internal(
            "ri_set_batch 树配置与存根长度契约偏离".to_string(),
          ));
        }
        Err(e) => return Err(RangeIndexError::from(e)),
      }
    };

    if inserted > 0 {
      meta.inc_size(inserted as u64);
      self.save_bftree_meta_stub(key, &meta, &stub).await?;
    }
    let (ns, db) = self.virtual_domain();
    for &(field, value) in entries {
      // AOF 入队失败不中断 RI 批量写（复制/重建面可自树状态收敛），告警可见
      if let Err(e) = self.store.emit_event(StoreEvent::RangeIndexWrite {
        ns,
        db,
        key,
        field,
        val: value,
        delete: false,
      }) {
        log::error!("RangeIndex 批量写 AOF 入队失败: {e}");
      }
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
    let (_, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    let tree = self.acquire_tree_read(key, &stub).await?;

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
  /// 锁次序同 `drain_or_save`：树读守卫在上方块尾先行释放，drain 侧排空在途读者
  /// 须取得条带写锁，守卫未放即互锁。
  ///
  /// WATCH 版本栅栏：删空臂仅经 wkv 物理键原语（delete_raw / del_ttl），不经用户
  /// 键写入口收口，故本层按 wnode `apply_rmw_post_operate` 同款显式恰一次推进；
  /// 非删空臂维持原状（本内核写面历史上不推进栅栏，不属本票范围，绝不双计）。
  ///
  /// 复制面 AOF 先后顺序固定：删空臂 drain 的整键墓碑（RangeIndexDrop →
  /// StoreDelete）先行入账，函数尾部 RangeIndexWrite(delete=true) 字段条目后至
  ///（与四族同步 RMW 骨架 `run_sync_rmw` 的「墓碑写回先入账、增量条目后通知」同
  /// 序）。副本据此确定性收敛：整键墓碑经既有全键删臂彻底清场，随后字段条目
  /// 命中 NotFound 由 `handle_range_index_del_replay` 静默跳过，绝不残留；主臂与
  /// 回放臂共用本函数，删空自愈在两侧同源生效。
  pub async fn range_index_del(
    &self,
    key: &[u8],
    field: &[u8],
  ) -> StdResult<bool, RangeIndexError> {
    let (mut meta, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    let removed = {
      let tree = self.acquire_tree_read(key, &stub).await?;
      tree.ri_del(field)?
    };

    if removed {
      meta.dec_size(1);
      if meta.size == 0 {
        // 删空自愈：接既有排空回收内核（keep_ttl=false 随键清 TTL 杜绝孤儿），
        // 不新造第二套清理编排、不加后台周期任务
        self.handle_bftree_drain_and_delete(key, false).await?;
        // drain 仅物理键原语 → 显式恰一次推进 WATCH 版本栅栏
        self.bump_watch_version(key);
      } else {
        self.save_bftree_meta_stub(key, &meta, &stub).await?;
      }
    }
    let (ns, db) = self.virtual_domain();
    if let Err(e) = self.store.emit_event(StoreEvent::RangeIndexWrite {
      ns,
      db,
      key,
      field,
      val: &[],
      delete: true,
    }) {
      log::error!("RangeIndex AOF 入队失败: {e}");
    }
    Ok(true)
  }

  /// 基于数量的流式范围扫描 (内部栈缓冲区零分配回调，透传切片引用，O(1) 空间复杂度)
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
    let (meta, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    if stub.storage_backend == StorageBackendType::Memory.to_u8() {
      return Err(RangeIndexError::MemoryModeNotSupported);
    }

    if count == 0 || meta.size == 0 {
      return Ok(0);
    }

    let tree = self.acquire_tree_read(key, &stub).await?;

    Ok(tree.ri_scan_with_field(start, count, return_field, on_record)?)
  }

  /// 闭区间流式范围扫描 (内部栈缓冲区零分配回调，透传切片引用，O(1) 空间复杂度)
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
    let (meta, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    if stub.storage_backend == StorageBackendType::Memory.to_u8() {
      return Err(RangeIndexError::MemoryModeNotSupported);
    }

    if meta.size == 0 || start > end {
      return Ok(0);
    }

    let tree = self.acquire_tree_read(key, &stub).await?;

    Ok(tree.ri_range_with_field(start, end, return_field, on_record)?)
  }

  /// 获取 RangeIndex 元素计数（O(1) 复杂度计数规约的唯一实现：
  /// `load_range_index_stub` 一次主存读直取 MetaValue.size，不触树、
  /// 不调用 `acquire_tree_read`，严禁扫树兜底）。RESP 面 RI.COUNT 与其
  /// 同命令双名 RI.LEN 均接本函数。
  pub async fn range_index_count(&self, key: &[u8]) -> StdResult<usize, RangeIndexError> {
    let (meta, _) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;
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
    let (_, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;
    Ok(stub)
  }

  /// 获取索引指标与运行状态 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexMetrics)
  pub async fn range_index_metrics(
    &self,
    key: &[u8],
  ) -> StdResult<RangeIndexMetrics, RangeIndexError> {
    let (_, stub) = self
      .load_range_index_stub(key)
      .await?
      .ok_or(RangeIndexError::NotFound)?;

    // is_live 只以注册表为准：stub.tree_handle 仅作标识且可能为陈旧值
    // (崩溃恢复改写注册表前)，据此推断会误报
    let (tree_handle, is_live) = match self.store.range_index.get_tree(key) {
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
/// RI 面单点/批量与 wnode 分层集合写臂预校验共用，禁止各处另写长度判定)。
/// `record_len` 为实际落树记录长度：RI 面即原值长，分层面为 member_ttl 编码后长
#[inline]
pub fn validate_bftree_record(
  stub: &RangeIndexStub,
  key: &[u8],
  record_len: usize,
) -> StdResult<(), RangeIndexError> {
  if key.len() > stub.max_key_len as usize
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
