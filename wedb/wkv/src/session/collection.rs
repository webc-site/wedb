//! 集合元数据与紧凑编码操作（对标 C# Garnet StorageSession/MainObjectStore 的
//! 元数据读写与 Compact 内联编码、分块存储）
//!
//! 覆盖：元数据记录 load/save（含严格删空与 TTL 守卫）、元数据 reserved 字段的
//! 分块信息与 TTL 标志位编码、集合快删路由（delete/contains_key）、Flattened
//! ZSet 的 BfTree 索引回收、Hash 元素向分块索引的批量折叠追加，以及
//! hash 字段级 TTL 全链路：写入口 hexpire_at/hpersist（对标 Redis 7.4 HEXPIRE
//! 系与 Garnet HashObject.HashExpire/HashPersist）、读路径惰性 purge（单探针
//! 门控）与后台收集候选处理（对标 Garnet ObjectCollectTask 的 HashCollect）。

use wdev::Device;
use wval::{CollectionType, KeyTag, META_VALUE_SIZE, MetaValue};

use crate::{error::Result, session::StoreSession};

/// 元数据 reserved[1] 最高位：Compact 载荷可能含 TTL 字段标志
pub const META_HAS_EXPIRE_MASK: u8 = 0x80;

/// 元数据物理记录（32B 大端布局）中 reserved[1]（has_expire 标志所在字节）的偏移：
/// key_id 8B + collection_type 1B + reserved[0] 1B
const META_RESERVED1_OFFSET: usize = 10;

impl<D: Device> StoreSession<D> {
  /// 删除指定键（支持普通键及集合打平存储 O(1) 秒删自愈与 ZSet Fast Drop）
  ///
  /// 双域删除：String 域未命中再删 ObjectEnvelope 域——用户键同一时刻至多
  /// 驻留一个物理域，DEL 语义与类型无关（对标 C# 统一存 DELETE 不区分
  /// ValueIsObject）；对象删空自愈与过期清除（purge_expired → delete）同经此处
  pub async fn delete(&self, key: &[u8]) -> Result<bool> {
    // 先清除随键 TTL 与 ETag 旁路记录（DEL 语义一致性：C# 删除记录连同
    // 记录尾可选 ETag/Expiration 字段一并消失），同时保证后续 load_meta 的
    // TTL 守卫探测不到记录而不触发二次清除；无旁路记录键仅哈希探针零额外写入
    self.del_ttl(key).await?;
    self.del_etag(key).await?;

    // 快速检查对象元数据防复合对象删除
    // 99.999% 普通键：直接走 delete_raw 极速通道，零复合对象判断与零多余 load_meta 异步查询开销
    if let Some(false) = self.check_object_meta_fast(key)? {
      let str_k = self.session_string_key(key);
      if self.delete_raw(&str_k).await? {
        return Ok(true);
      }
      let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, key);
      return self.delete_raw(&env_k).await;
    }

    let mut meta_del = false;
    if let Some(mut meta) = self.load_meta(key).await? {
      if meta.collection_type == CollectionType::RangeIndex || meta.encoding().is_flattened() {
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
    let env_del = if key_del {
      false
    } else {
      let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, key);
      self.delete_raw(&env_k).await?
    };
    Ok(meta_del || key_del || env_del)
  }

  /// 检查指定键是否存在且未被墓碑删除（支持普通键及集合打平存储）
  ///
  /// 存活判定含 key 级惰性过期：集合走 load_meta 的 TTL 守卫，
  /// 普通键仅在数据命中后经 has_ttl_tag 单探针门控探测 TTL 记录（快路径零额外 I/O）；
  /// 数据面双域判定：String 域未命中再探 ObjectEnvelope 域（对象键 EXPIRE/TTL 生效前提）
  pub async fn contains_key(&self, key: &[u8]) -> Result<bool> {
    if let Some(meta) = self.load_meta(key).await?
      && meta.size > 0
    {
      return Ok(true);
    }
    let str_k = self.session_string_key(key);
    let hit = if self.contains_key_raw(&str_k).await? {
      true
    } else {
      let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, key);
      self.contains_key_raw(&env_k).await?
    };
    if hit {
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

  /// 裸数据存活判定（不含任何 TTL 探测/清除）：集合元记录存活（size > 0）或数据记录存在
  ///
  /// 专供 expire_at/persist 的融合读改写路径：本键 TTL 裁决由调用方单次 `ttl_of`
  /// 读取统一闭环（读路径 TTL 探测收敛不变式：同一同步调用链内同一用户键只做一次
  /// TTL 裁决），此处刻意采用 contains_key 的数据面口径但剥离惰性过期探测，避免
  /// 同一链内对同一 TTL 记录双次遍历。元记录口径与 load_meta 一致：命中即同步
  /// 维护 key_id 判活映射（含幽灵元记录的判死同步）。数据面双域判定：
  /// String 域未命中再探 ObjectEnvelope 域
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
    if self.contains_key_raw(&str_k).await? {
      return Ok(true);
    }
    let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, key);
    self.contains_key_raw(&env_k).await
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
          .is_ok_and(|m| m.size > 0 || m.collection_type == CollectionType::RangeIndex)
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
        let is_alive = meta.size > 0 || meta.collection_type == CollectionType::RangeIndex;
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

  /// 通用集合删空生命周期与原子墓碑自愈（对标 Garnet `HasRemoveKey` -> `ExpireAndStop` + `IncrementVersion`）
  ///
  /// 在 garnet 中的相对路径:libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:InPlaceUpdaterWorker
  ///
  /// 当集合元素计数减至 0 时触发：
  /// 1. 原子删除元记录（写墓碑）；
  /// 2. 清理随键 TTL（杜绝孤儿 TTL）；
  /// 3. 更新 key_id 判活映射为 false，推进版本号（版本号栅栏保证历史打平子键立即逻辑失效）；
  /// 4. 彻底杜绝幽灵空元记录与孤儿 TTL。
  pub async fn drain_and_delete_collection_meta(
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
}
