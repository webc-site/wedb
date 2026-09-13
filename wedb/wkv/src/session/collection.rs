//! 集合元数据操作（对标 C# Garnet StorageSession/MainObjectStore 的元数据读写）
//!
//! 覆盖：元数据记录 load/save（含严格删空与 TTL 守卫）、集合快删路由
//! （delete/contains_key）、BfTree 存根持久化、RangeIndex/打平集合统一生命周期，
//! 以及 Hash 字段级 TTL 的后台收集候选处理（对标 Garnet ObjectCollectTask 的
//! HashCollect）。

use std::sync::{Arc, atomic::Ordering};

use wbftree::RangeIndexStub;
use wdev::Device;
use wval::{CompactHashCodec, GarnetObjectType, META_VALUE_SIZE, MetaValue, StorageEncoding};

use crate::{
  error::Result,
  range_index::{encode_meta_stub_record, range_index_blocking},
  session::StoreSession,
};

/// 元数据 reserved[1] 最高位：Compact 载荷可能含 TTL 字段标志
pub const META_HAS_EXPIRE_MASK: u8 = 0x80;

/// 元数据物理记录（32B 大端布局）中 reserved[1]（has_expire 标志所在字节）的偏移：
/// key_id 8B + collection_type 1B + reserved[0] 1B
const META_RESERVED1_OFFSET: usize = 10;

impl<D: Device> StoreSession<D> {
  /// 删除指定键（支持普通键及集合打平存储 O(1) 秒删自愈与 ZSet Fast Drop）
  pub async fn delete(&self, key: &[u8]) -> Result<bool> {
    // 先清除随键 TTL 记录（DEL 语义一致性），同时保证后续 load_meta 的 TTL 守卫
    // 探测不到记录而不触发二次清除；无 TTL 键仅一次哈希探针零额外写入
    self.del_ttl(key).await?;

    // 快速检查对象元数据防复合对象删除
    // 99.999% 普通键：直接走 delete_raw 极速通道，零复合对象判断与零多余 load_meta 异步查询开销
    if let Some(false) = self.check_object_meta_fast(key)? {
      let str_k = self.session_string_key(key);
      return self.delete_raw(&str_k).await;
    }

    let mut meta_del = false;
    if let Some(mut meta) = self.load_meta(key).await? {
      if meta.collection_type == GarnetObjectType::RangeIndex || meta.encoding().is_flattened() {
        // 统一 RangeIndex 与打平集合（whlog 打平 / BfTree 树算子）生命周期：
        // 原子墓碑 + 版本号栅栏 + 树排空与磁盘释放（whlog 打平键无树文件时
        // delete_index 为幂等 no-op）
        self.handle_bftree_drain_and_delete(key, &meta).await?;
        meta_del = true;
      } else {
        // 非 Flattened（紧凑/分块）及其他集合：原子墓碑 + 版本号栅栏 + 随键 TTL 清理
        self
          .drain_and_delete_collection_meta(key, &mut meta)
          .await?;
        meta_del = true;
      }
    }
    let str_k = self.session_string_key(key);
    let key_del = self.delete_raw(&str_k).await?;
    Ok(meta_del || key_del)
  }

  /// 检查指定键是否存在且未被墓碑删除（支持普通键及集合打平存储）
  ///
  /// 存活判定含 key 级惰性过期：集合走 load_meta 的 TTL 守卫，
  /// 普通键仅在字符串命中后经 has_ttl_tag 单探针门控探测 TTL 记录（快路径零额外 I/O）
  pub async fn contains_key(&self, key: &[u8]) -> Result<bool> {
    if let Some(meta) = self.load_meta(key).await?
      && meta.size > 0
    {
      return Ok(true);
    }
    let str_k = self.session_string_key(key);
    if self.contains_key_raw(&str_k).await? {
      // 仅当数据存在时才惰性检查过期（过期物理清除后视同不存在）；
      // has_ttl_tag 单探针门控：无 TTL 记录时完全跳过异步过期裁决（与 read_with 口径
      // 一致，本调用链内该键的 TTL 裁决仅此一次）
      if self.has_ttl_tag(key)? && self.check_expired(key).await? {
        return Ok(false);
      }
      return Ok(true);
    }
    Ok(false)
  }

  /// 裸数据存活判定（不含任何 TTL 探测/清除）：集合元记录存活（size > 0）或字符串记录存在
  ///
  /// 专供 expire_at/persist 的融合读改写路径：本键 TTL 裁决由调用方单次 `ttl_of`
  /// 读取统一闭环（读路径 TTL 探测收敛不变式：同一同步调用链内同一用户键只做一次
  /// TTL 裁决），此处刻意采用 contains_key 的数据面口径但剥离惰性过期探测，避免
  /// 同一链内对同一 TTL 记录双次遍历。元记录口径与 load_meta 一致：命中即同步
  /// 维护 key_id 判活映射（含幽灵元记录的判死同步）
  pub(crate) async fn contains_key_ignore_ttl(&self, key: &[u8]) -> Result<bool> {
    let meta_k = self.session_meta_key(key);
    if let Some(meta) = self.read_raw_with(&meta_k, MetaValue::from_slice).await? {
      let meta = meta?;
      if meta.size > 0 {
        self
          .store
          .update_key_id_meta(meta.key_id, meta.version, true);
        return Ok(true);
      }
      // 幽灵元记录（打平集合秒删残留）：对齐 load_meta 口径同步判死后再探裸键
      self
        .store
        .update_key_id_meta(meta.key_id, meta.version, false);
    }
    let str_k = self.session_string_key(key);
    self.contains_key_raw(&str_k).await
  }

  /// 读取 Compact 载荷是否可能含 TTL 字段标志（const fn）
  ///
  /// 标志仅由写路径在写入带 TTL 字段时置位（粘性，不清除），读/写路径据此跳过无效的
  /// 全量过期淘汰扫描
  #[inline(always)]
  pub const fn get_meta_has_expire(reserved: &[u8; 7]) -> bool {
    reserved[1] & META_HAS_EXPIRE_MASK != 0
  }

  /// 从元数据物理记录值切片直接判定 has_expire 粘性标志（const fn，零解码）
  ///
  /// 供 GC 扫描过滤链对 Meta 元记录做单字节标志位初筛（log 记录 value =
  /// 32B MetaValue + 紧凑载荷，标志位恒在 [`META_RESERVED1_OFFSET`] 偏移），
  /// 无需整记录 `MetaValue::from_slice` 解析
  #[inline(always)]
  pub(crate) const fn meta_value_has_expire(meta_bytes: &[u8]) -> bool {
    meta_bytes.len() >= META_VALUE_SIZE
      && meta_bytes[META_RESERVED1_OFFSET] & META_HAS_EXPIRE_MASK != 0
  }

  /// 最新态元记录是否仍带 has_expire 标志（纯内存单探针，GC 候选初筛双检专用）
  ///
  /// 标志粘性（只置位不清除），内存最新态无标志仅见于「删除重建同名键后旧日志
  /// 版本仍在扫描窗口」的罕见场景——此时放行候选防其反复占据删除预算饿死存活
  /// 候选（与 key 级 probe_ttl 陈旧版本双检同型）。三态口径：
  /// - 内存命中：以最新记录标志位为准；
  /// - 内存确认不存在（墓碑/无候选）：false，候选消亡；
  /// - 记录落盘（磁盘候选）：保守返回 true，由候选处理阶段的最新态双检兜底
  #[inline]
  pub(crate) fn probe_meta_has_expire(&self, user_key: &[u8]) -> Result<bool> {
    let meta_k = self.session_meta_key(user_key);
    match self.try_read_raw_in_memory(&meta_k, Self::meta_value_has_expire)? {
      // 内存命中：以最新记录标志位为准
      Some(Some(has)) => Ok(has),
      // 内存确认不存在（墓碑/无候选）：候选消亡
      Some(None) => Ok(false),
      // 记录落盘：保守收集，由候选处理阶段的最新态双检兜底
      None => Ok(true),
    }
  }

  /// 在已有纪元保护下快速检查是否存在集合对象元数据（完全绕过 enter() 原子开销）
  #[inline]
  pub fn check_object_meta_fast_unprotected(&self, user_key: &[u8]) -> Result<Option<bool>> {
    let meta_k = self.session_meta_key(user_key);
    // 纯哈希表无锁极速初筛（零内存记录访问）：
    // 若哈希表中连元数据 Tag 都完全不存在，100% 确认无此集合对象，极速返回 false
    let Some(first_addr) = self.store.index.find_tag(&meta_k) else {
      return Ok(Some(false));
    };
    match self.try_read_raw_in_memory_with_addr(&meta_k, Some(first_addr), |bytes| {
      bytes.len() >= META_VALUE_SIZE
        && MetaValue::from_slice(bytes)
          .is_ok_and(|m| m.size > 0 || m.collection_type == GarnetObjectType::RangeIndex)
    })? {
      Some(Some(is_active)) => Ok(Some(is_active)),
      Some(None) => Ok(Some(false)),
      None => Ok(None),
    }
  }

  /// 快速检查是否存在集合对象元数据（纯同步无锁内存探测，对齐 Garnet NetworkSET 快速路径）
  /// - Ok(Some(true)): 明确存在对象元数据（需要报错 WRONGTYPE 或走异步处理）
  /// - Ok(Some(false)): 明确不存在对象元数据（可安全执行纯同步快速写）
  /// - Ok(None): 冷数据可能驻留磁盘，需回退异步深层加载
  #[inline]
  pub fn check_object_meta_fast(&self, user_key: &[u8]) -> Result<Option<bool>> {
    let _guard = self.participant.enter();
    self.check_object_meta_fast_unprotected(user_key)
  }

  /// 读取集合元数据记录
  ///
  /// 闭包直出按值 32 字节 `MetaValue` 小结构（`repr(C, align(8))` + `Copy`，`from_slice` 为
  /// const fn 纯栈解析）：内存命中路径零堆分配，免去 `to_vec` 整记录拷贝；
  /// 磁盘回退路径亦免一次中间 Vec 拷贝，仅保留设备读取固有的单次缓冲分配。
  ///
  /// 含 key 级 TTL 守卫：仅当集合存活（size > 0）时才探测 TTL 记录，已过期则经统一
  /// DEL 路径物理清除并视同不存在——写路径（HSET 等）对已过期集合按不存在重建，
  /// 符合 Redis 语义；本守卫经 purge_expired"先删 TTL 记录"约定保证无递归。
  /// 读路径 TTL 收敛不变式：内部 `read_raw_with` 为无守卫裸读内核，本键 TTL 裁决
  /// 只在此入口做一次，嵌套裸读绝不重复裁决
  pub async fn load_meta(&self, user_key: &[u8]) -> Result<Option<MetaValue>> {
    let meta_k = self.session_meta_key(user_key);
    match self.read_raw_with(&meta_k, MetaValue::from_slice).await? {
      Some(meta) => {
        let meta = meta?;
        let is_alive = meta.size > 0 || meta.collection_type == GarnetObjectType::RangeIndex;
        if is_alive && self.has_ttl_tag(user_key)? && self.check_expired(user_key).await? {
          return Ok(None);
        }
        self
          .store
          .update_key_id_meta(meta.key_id, meta.version, is_alive);
        if !is_alive {
          let _ = self.delete_raw(&meta_k).await;
          let _ = self.del_ttl(user_key).await;
          return Ok(None);
        }
        Ok(Some(meta))
      }
      None => Ok(None),
    }
  }

  /// 集合装载公共前置段：读元记录 + 幽灵/裸键 WRONGTYPE 判定 + key 级惰性过期守卫
  ///
  /// 返回 `Some((存活元数据, 整记录字节))`；以下三种情形返回 `None`：
  /// 无元记录（裸键也不存在）、幽灵元记录（size=0 且裸键不存在）、key 级已过期
  /// （物理清除后视同不存在）；裸键存活时返回 WRONGTYPE（0xFF，具体类型未知）。
  /// 裸键探测是对同名裸键这一不同物理记录的独立裁决（保证过期字符串不可见），
  /// 不属同键同记录的重复探测；此后本链不再二次裁决
  async fn load_live_meta_bytes(&self, key: &[u8]) -> Result<Option<(MetaValue, Vec<u8>)>> {
    let meta_k = self.session_meta_key(key);
    let bytes = match self.read_raw(&meta_k).await? {
      Some(b) => b,
      None => {
        if self.read(key).await?.is_some() {
          return Err(wval::Error::InvalidCollectionType(0xFF).into());
        }
        return Ok(None);
      }
    };
    let meta = MetaValue::from_slice(&bytes)?;
    if meta.size == 0 {
      // 幽灵元记录（打平集合秒删残留）：自愈清理元记录墓碑与随键 TTL
      let _ = self.delete_raw(&meta_k).await;
      let _ = self.del_ttl(key).await;
      self
        .store
        .update_key_id_meta(meta.key_id, meta.version, false);
      // 继续探测同名裸键，杜绝在活字符串之上静默重建同名集合（读路径识别 WRONGTYPE，与写路径口径一致）
      if self.read(key).await?.is_some() {
        return Err(wval::Error::InvalidCollectionType(0xFF).into());
      }
      return Ok(None);
    }
    // 仅当集合存活时才惰性检查过期（快路径零额外 I/O）；
    // has_ttl_tag 单探针门控：无 TTL 记录时完全跳过异步过期裁决（与 read_with 口径一致）
    if self.has_ttl_tag(key)? && self.check_expired(key).await? {
      return Ok(None);
    }
    Ok(Some((meta, bytes)))
  }

  /// 读取集合元数据及紧凑载荷（准备写入）
  ///
  /// 含 key 级 TTL 守卫：已过期集合物理清除后视同不存在，
  /// 写路径（HSET/SADD 等）对过期集合按不存在重建（Redis 语义）。
  /// 幽灵分支的 `read` 是对同名裸键这一不同物理记录的独立裁决（保证过期字符串不可见），
  /// 不属同键同记录的重复探测；此后本链不再二次裁决
  pub(crate) async fn load_collection_raw_write(
    &self,
    key: &[u8],
    expected: GarnetObjectType,
  ) -> Result<Option<(MetaValue, Option<Vec<u8>>)>> {
    let Some((meta, mut bytes)) = self.load_live_meta_bytes(key).await? else {
      return Ok(None);
    };
    if meta.collection_type != expected {
      return Err(wval::Error::InvalidCollectionType(meta.collection_type.as_u8()).into());
    }
    let payload = if meta.encoding() == StorageEncoding::Compact {
      if bytes.len() > META_VALUE_SIZE {
        // 调用方均需 owned 可变载荷做原地变更（purge/delete/append），drain 原地 memmove
        // 免新增堆分配
        bytes.drain(..META_VALUE_SIZE);
        Some(bytes)
      } else {
        Some(Vec::new())
      }
    } else {
      None
    };
    Ok(Some((meta, payload)))
  }

  /// 通用集合删空生命周期与原子墓碑自愈（对标 Garnet `HasRemoveKey` -> `ExpireAndStop` + `IncrementVersion`）
  ///
  /// 在 garnet 中的相对路径:libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:InPlaceUpdaterWorker
  ///
  /// 当集合元素计数减至 0 时触发：
  /// 1. 原子删除元记录（写墓碑）；
  /// 2. 清理随键 TTL（杜绝孤儿 TTL）；
  /// 3. 更新 key_id 判活映射为 false，推进版本号（版本号栅栏保证历史打平子键立即逻辑失效）；
  /// 4. 彻底杜绝幽灵空元记录与孤儿 TTL。
  pub(crate) async fn drain_and_delete_collection_meta(
    &self,
    user_key: &[u8],
    meta: &mut MetaValue,
  ) -> Result<()> {
    let meta_k = self.session_meta_key(user_key);
    self.delete_raw(&meta_k).await?;
    self.del_ttl(user_key).await?;
    meta.version = meta.version.wrapping_add(1);
    self
      .store
      .update_key_id_meta(meta.key_id, meta.version, false);
    Ok(())
  }

  /// 保存集合元数据记录（严格删空语义：若元素数量为 0 则直接删除元数据记录与 TTL，杜绝幽灵空元数据与孤儿 TTL）
  pub async fn save_meta(&self, user_key: &[u8], meta: &MetaValue) -> Result<()> {
    if meta.size == 0 {
      let mut meta_copy = *meta;
      return self
        .drain_and_delete_collection_meta(user_key, &mut meta_copy)
        .await;
    }
    self
      .store
      .update_key_id_meta(meta.key_id, meta.version, true);
    let meta_k = self.session_meta_key(user_key);
    let bytes = meta.to_bytes();
    self.upsert_raw(&meta_k, &bytes).await?;
    Ok(())
  }

  /// 保存紧凑元数据记录（无缝拼接 MetaValue 与紧凑载荷，小载荷优先栈缓冲消除堆分配）
  pub(crate) async fn save_compact_meta(
    &self,
    user_key: &[u8],
    meta: &MetaValue,
    payload: &[u8],
  ) -> Result<()> {
    if meta.size == 0 {
      let mut meta_copy = *meta;
      return self
        .drain_and_delete_collection_meta(user_key, &mut meta_copy)
        .await;
    }
    // 与 save_meta 口径一致：同步维护 key_id 判活映射（幂等快路径零写入），
    // 供 Fast Drop 与 LogCompactor 紧缩正确判定紧凑集合的存活状态
    self
      .store
      .update_key_id_meta(meta.key_id, meta.version, true);
    let meta_k = self.session_meta_key(user_key);
    let total_len = META_VALUE_SIZE + payload.len();
    const STACK_LIMIT: usize = 512;
    if total_len <= STACK_LIMIT {
      let mut buf = [0u8; STACK_LIMIT];
      buf[..META_VALUE_SIZE].copy_from_slice(&meta.to_bytes());
      buf[META_VALUE_SIZE..total_len].copy_from_slice(payload);
      self.upsert_raw(&meta_k, &buf[..total_len]).await?;
    } else {
      let mut buf = Vec::with_capacity(total_len);
      buf.extend_from_slice(&meta.to_bytes());
      buf.extend_from_slice(payload);
      self.upsert_raw(&meta_k, &buf).await?;
    }
    Ok(())
  }

  /// 收集并物理清除指定 Hash 集合内已过期字段（后台字段收集候选处理入口）
  ///
  /// 对标 Garnet ObjectCollectTask → storageSession.HashCollect 的对象内过期成员
  /// 收集：GC 扫描过滤链从日志中识别带 has_expire 标志的元记录产出候选后，经本
  /// 入口「加载最新集合 → purge_expired → 回写压缩」闭环。
  ///
  /// 双检幂等（与 key 级 sweep 最新态双检同型）：持本键独占桶锁后重读最新元记录
  /// 再 purge——扫描与回写间隙内的并发字段写以最新态为准，绝不丢更新；无过期字段
  /// 则零写入直接返回，陈旧候选（集合已删除/重建/已被惰性 purge 清空）零副作用。
  /// 返回物理清除的字段数（`now` 为 i64 .NET Ticks 过期判定基准，与字段级内联过期值同域）
  pub(crate) async fn collect_expired_hash_fields(&self, user_key: &[u8], now: i64) -> Result<u64> {
    let _key_lock = self.store.index.acquire_keys_lock_exclusive(&[user_key])?;
    let Some((mut meta, Some(mut payload))) = self
      .load_collection_raw_write(user_key, GarnetObjectType::Hash)
      .await?
    else {
      return Ok(0);
    };
    // 单探针门控：标志未置位即无字段 TTL（防陈旧候选触发无谓全量扫描）
    if !Self::get_meta_has_expire(&meta.reserved) {
      return Ok(0);
    }
    let purged = CompactHashCodec::purge_expired(&mut payload, now)?;
    if purged > 0 {
      meta.dec_size(purged as u64);
      if meta.size == 0 {
        self
          .drain_and_delete_collection_meta(user_key, &mut meta)
          .await?;
      } else {
        self.save_compact_meta(user_key, &meta, &payload).await?;
      }
    }
    Ok(purged as u64)
  }

  /// 严格删空生命周期：写墓碑、清理 TTL，并排空在途写者、释放树实例及删除磁盘数据文件
  pub(crate) async fn handle_bftree_drain_and_delete(
    &self,
    key: &[u8],
    meta: &MetaValue,
  ) -> Result<()> {
    let mut meta_copy = *meta;
    self
      .drain_and_delete_collection_meta(key, &mut meta_copy)
      .await?;
    let mgr = Arc::clone(&self.store.range_index);
    let del_key = key.to_vec();
    let _ = range_index_blocking(move || mgr.delete_index(&del_key)).await??;
    if !self.store.aof_listeners_paused.load(Ordering::Relaxed)
      && let Some(listener) = self.store.range_drop_listener()
    {
      listener.call(key);
    }
    Ok(())
  }

  /// 栈分配持久化 MetaValue 与 RangeIndexStub (定长 67 字节，零堆分配)
  pub(crate) async fn save_bftree_meta_stub(
    &self,
    key: &[u8],
    meta: &MetaValue,
    stub: &RangeIndexStub,
  ) -> Result<()> {
    let meta_k = self.session_meta_key(key);
    let val = encode_meta_stub_record(meta, stub);
    self.upsert_raw(&meta_k, &val).await?;
    self
      .store
      .update_key_id_meta(meta.key_id, meta.version, true);
    Ok(())
  }
}
