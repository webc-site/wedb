//! 集合元数据与紧凑编码操作（对标 C# Garnet StorageSession/MainObjectStore 的
//! 元数据读写与 Compact 内联编码、分块存储）
//!
//! 覆盖：元数据记录 load/save（含严格删空与 TTL 守卫）、元数据 reserved 字段的
//! 分块信息与 TTL 标志位编码、集合快删路由（delete/contains_key）、Flattened
//! ZSet 的 BfTree 索引回收，以及 Hash/Set 元素向分块索引的批量折叠追加。

use std::sync::Arc;

use wdev::Device;
use wval::{CollectionType, KeyTag, META_VALUE_SIZE, MetaValue, StorageEncoding, ZSetSubKeyCodec};

use crate::{error::Result, range_index::range_index_blocking, session::StoreSession};

/// 元数据 reserved[1] 最高位：Compact 载荷可能含 TTL 字段标志（chunk_id 实际仅占用低 31 位）
pub const META_HAS_EXPIRE_MASK: u8 = 0x80;

impl<D: Device> StoreSession<D> {
  /// 默认集合分块容量（128 元素，定长分块，适配 4KB 小页）
  pub const CHUNK_CAPACITY: u16 = 128;

  /// 清理 Flattened ZSet 在 BfTree 中的全部索引项 (score 索引与 member 索引，定长批次安全回收防 OOM)
  pub fn clear_bftree_zset(&self, key_id: u64, version: u64) -> Result<()> {
    const BATCH_SIZE: usize = 256;
    let prefixes = [
      ZSetSubKeyCodec::encode_score_prefix(key_id, version),
      ZSetSubKeyCodec::encode_member_header(key_id, version),
    ];
    let mut batch = Vec::with_capacity(BATCH_SIZE);
    for prefix in prefixes {
      let mut end_key = prefix;
      let mut has_next = false;
      for i in (0..end_key.len()).rev() {
        if end_key[i] < 0xff {
          end_key[i] += 1;
          has_next = true;
          break;
        }
        end_key[i] = 0;
      }
      let end_slice: &[u8] = if has_next { &end_key } else { &[0xff; 18] };
      let mut cursor = prefix.to_vec();
      loop {
        batch.clear();
        let _ = self.store.bftree.scan_with_end_key_callback(
          &cursor,
          end_slice,
          wbftree::ScanReturnField::Key,
          |k, _| {
            if k.starts_with(&prefix) {
              batch.push(k.to_vec());
              batch.len() < BATCH_SIZE
            } else {
              false
            }
          },
        );
        if batch.is_empty() {
          break;
        }
        for k in &batch {
          let _ = self.store.bftree.delete(k);
        }
        let Some(last) = batch.last() else {
          break;
        };
        if last.as_slice() <= cursor.as_slice() {
          break;
        }
        cursor.clear();
        cursor.extend_from_slice(last);
        let mut carried = false;
        for b in cursor.iter_mut().rev() {
          if *b < 0xff {
            *b += 1;
            carried = true;
            break;
          }
          *b = 0;
        }
        if !carried || cursor.as_slice() > end_slice {
          break;
        }
      }
    }
    Ok(())
  }

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
      let meta_k = self.session_meta_key(key);
      if meta.size > 0 {
        meta_del = true;
      }
      if meta.collection_type == CollectionType::ZSet {
        if meta.encoding() == StorageEncoding::Flattened {
          let old_version = meta.version;
          self.clear_bftree_zset(meta.key_id, old_version)?;
          // 快速瞬时清空 (Fast Drop): Flattened 模式下递增 version 并置 size = 0，保存 Meta 记录实现 O(1) 瞬时清空与版本隔离
          meta.bump_version();
          meta.size = 0;
          Self::set_meta_chunk_info(&mut meta.reserved, 0, 0);
          let bytes = meta.to_bytes();
          self.upsert_raw(&meta_k, &bytes).await?;
          self
            .store
            .update_key_id_meta(meta.key_id, meta.version, false);
        } else {
          self
            .store
            .update_key_id_meta(meta.key_id, meta.version, false);
          self.delete_raw(&meta_k).await?;
        }
      } else if meta.collection_type == CollectionType::Hash
        || meta.collection_type == CollectionType::Set
      {
        if meta.encoding() == StorageEncoding::Flattened {
          // 快速瞬时清空 (Fast Drop): Flattened 模式下递增 version 并置 size = 0，保存 Meta 记录实现 O(1) 瞬时清空与版本隔离
          meta.bump_version();
          meta.size = 0;
          Self::set_meta_chunk_info(&mut meta.reserved, 0, 0);
          let bytes = meta.to_bytes();
          self.upsert_raw(&meta_k, &bytes).await?;
          self
            .store
            .update_key_id_meta(meta.key_id, meta.version, false);
        } else {
          self
            .store
            .update_key_id_meta(meta.key_id, meta.version, false);
          self.delete_raw(&meta_k).await?;
        }
      } else if meta.collection_type == CollectionType::RangeIndex {
        // 删除顺序：先写元记录墓碑再释放树并删数据文件 (对标 C# 墓碑与树释放同链
        // 完成的原子语义)。反转旧序 (先删文件后写墓碑) 后，两步间崩溃的最坏结果
        // 从「存根在而数据文件失 → 惰性恢复显式报错」降级为无害孤儿文件 (存根已
        // 墓碑化，键对外不存在，残留工作文件不再被任何存根引用)。
        self.delete_raw(&meta_k).await?;
        self
          .store
          .update_key_id_meta(meta.key_id, meta.version, false);
        // 整树释放 (Drop 遍历基页刷盘 + 删文件) 属重操作，卸载 compio 阻塞线程
        let mgr = Arc::clone(&self.store.range_index);
        let del_key = key.to_vec();
        let _deleted = range_index_blocking(move || mgr.delete_index(&del_key)).await?;
        meta_del = true;
      } else {
        self
          .store
          .update_key_id_meta(meta.key_id, meta.version, false);
        self.delete_raw(&meta_k).await?;
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

  /// 从元数据 reserved 预留字段中读取当前分块 ID 与元素数量（const fn，零 panic）
  ///
  /// reserved[1] 最高位为 Compact 载荷 TTL 标志位，读取 chunk_id 时需掩码剥离（chunk_id 实际仅低 31 位）
  #[inline(always)]
  pub const fn get_meta_chunk_info(reserved: &[u8; 7]) -> (u32, u16) {
    let chunk_id = u32::from_be_bytes([
      reserved[1] & !META_HAS_EXPIRE_MASK,
      reserved[2],
      reserved[3],
      reserved[4],
    ]);
    let chunk_len = u16::from_be_bytes([reserved[5], reserved[6]]);
    (chunk_id, chunk_len)
  }

  /// 将当前分块 ID 与元素数量写入元数据 reserved 预留字段（const fn，零分配，保留 reserved[0] 编码位）
  #[inline(always)]
  pub const fn set_meta_chunk_info(reserved: &mut [u8; 7], chunk_id: u32, chunk_len: u16) {
    let c = chunk_id.to_be_bytes();
    reserved[1] = c[0];
    reserved[2] = c[1];
    reserved[3] = c[2];
    reserved[4] = c[3];
    let l = chunk_len.to_be_bytes();
    reserved[5] = l[0];
    reserved[6] = l[1];
  }

  /// 读取 Compact 载荷是否可能含 TTL 字段标志（const fn）
  ///
  /// 标志仅由写路径在写入带 TTL 字段时置位（粘性，不清除），读/写路径据此跳过无效的
  /// 全量过期淘汰扫描；与分块信息共用 reserved[1..5]，分块信息仅打平编码使用、互不冲突
  #[inline(always)]
  pub const fn get_meta_has_expire(reserved: &[u8; 7]) -> bool {
    reserved[1] & META_HAS_EXPIRE_MASK != 0
  }

  /// 置位 Compact 载荷含 TTL 字段标志（const fn）
  #[inline(always)]
  pub const fn set_meta_has_expire(reserved: &mut [u8; 7]) {
    reserved[1] |= META_HAS_EXPIRE_MASK;
  }

  /// 快速检查是否存在集合对象元数据（纯同步无锁内存探测，严格对标 Garnet NetworkSET）
  /// - Ok(Some(true)): 明确存在对象元数据（需要报错 WRONGTYPE 或走异步处理）
  /// - Ok(Some(false)): 明确不存在对象元数据（可安全执行纯同步快速写）
  /// - Ok(None): 冷数据可能驻留磁盘，需回退异步深层加载
  #[inline]
  pub fn check_object_meta_fast(&self, user_key: &[u8]) -> Result<Option<bool>> {
    let meta_k = self.session_meta_key(user_key);
    let _guard = self.participant.enter();
    // 纯哈希表无锁极速初筛（零内存记录访问）：
    // 若哈希表中连元数据 Tag 都完全不存在，100% 确认无此集合对象，极速返回 false
    let Some(first_addr) = self.store.index.find_tag(&meta_k) else {
      return Ok(Some(false));
    };
    match self.try_read_raw_in_memory_with_addr(&meta_k, Some(first_addr), |bytes| {
      bytes.len() >= META_VALUE_SIZE && matches!(MetaValue::read_size(bytes), Ok(size) if size > 0)
    })? {
      Some(Some(is_active)) => Ok(Some(is_active)),
      Some(None) => Ok(Some(false)),
      None => Ok(None),
    }
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
        if meta.size > 0 && self.has_ttl_tag(user_key)? && self.check_expired(user_key).await? {
          return Ok(None);
        }
        self
          .store
          .update_key_id_meta(meta.key_id, meta.version, meta.size > 0);
        Ok(Some(meta))
      }
      None => Ok(None),
    }
  }

  /// 获取给定集合键的当前分块信息 (chunk_id, chunk_len)
  pub async fn get_collection_chunk_info(&self, user_key: &[u8]) -> Result<Option<(u32, u16)>> {
    if let Some(meta) = self.load_meta(user_key).await? {
      Ok(Some(Self::get_meta_chunk_info(&meta.reserved)))
    } else {
      Ok(None)
    }
  }

  /// 读取集合元数据及紧凑载荷（只读，零拷贝直接切片视图）
  ///
  /// 含 key 级 TTL 守卫：仅当集合存活时探测 TTL 记录，已过期则经统一 DEL 路径
  /// 物理清除并视同不存在（hget/hgetall 等集合读入口的惰性过期语义）。
  /// 幽灵分支的 `read` 是对同名裸键这一不同物理记录的独立裁决（保证过期字符串
  /// 不可见），不属同键同记录的重复探测；此后本链不再二次裁决
  pub async fn load_collection_raw_read(
    &self,
    key: &[u8],
    expected: CollectionType,
  ) -> Result<Option<RawCollectionRead>> {
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
      // 幽灵元记录（打平集合秒删残留）：继续探测同名裸键以识别 WRONGTYPE（与写路径口径一致）
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
    if meta.collection_type != expected {
      return Ok(None);
    }
    let raw = if meta.encoding() == StorageEncoding::Compact {
      Some(bytes)
    } else {
      None
    };
    Ok(Some(RawCollectionRead::new(meta, raw)))
  }

  /// 读取集合元数据及紧凑载荷（准备写入）
  ///
  /// 含 key 级 TTL 守卫：已过期集合物理清除后视同不存在，
  /// 写路径（HSET/SADD 等）对过期集合按不存在重建（Redis 语义）。
  /// 幽灵分支的 `read` 是对同名裸键这一不同物理记录的独立裁决（保证过期字符串
  /// 不可见），不属同键同记录的重复探测；此后本链不再二次裁决
  pub async fn load_collection_raw_write(
    &self,
    key: &[u8],
    expected: CollectionType,
  ) -> Result<Option<(MetaValue, Option<Vec<u8>>)>> {
    let meta_k = self.session_meta_key(key);
    let mut bytes = match self.read_raw(&meta_k).await? {
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
      // 幽灵元记录：继续探测同名裸键，杜绝在活字符串之上静默重建同名集合
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
    if meta.collection_type != expected {
      return Err(wval::Error::InvalidCollectionType(meta.collection_type.as_u8()).into());
    }
    let payload = if meta.encoding() == StorageEncoding::Compact {
      if bytes.len() > META_VALUE_SIZE {
        // 调用方均需 owned 可变载荷做原地变更（purge/delete/append），drain 原地 memmove
        // 免新增堆分配；只读场景请走 load_collection_raw_read 的 compact_payload 切片视图
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

  /// 保存紧凑元数据记录（无缝拼接 MetaValue 与紧凑载荷，小载荷优先栈缓冲消除堆分配）
  pub async fn save_compact_meta(
    &self,
    user_key: &[u8],
    meta: &MetaValue,
    payload: &[u8],
  ) -> Result<()> {
    // 与 save_meta 口径一致：同步维护 key_id 判活映射（幂等快路径零写入），
    // 供 Fast Drop 与 LogCompactor 紧缩正确判定紧凑集合的存活状态
    self
      .store
      .update_key_id_meta(meta.key_id, meta.version, meta.size > 0);
    let meta_k = self.session_meta_key(user_key);
    if meta.size == 0 {
      self.del_ttl(user_key).await?;
      self.delete_raw(&meta_k).await?;
    } else {
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
    }
    Ok(())
  }

  /// 保存集合元数据记录（严格删空语义：若元素数量为 0 则直接删除元数据记录与 TTL，杜绝幽灵空元数据与孤儿 TTL）
  pub async fn save_meta(&self, user_key: &[u8], meta: &MetaValue) -> Result<()> {
    self
      .store
      .update_key_id_meta(meta.key_id, meta.version, meta.size > 0);
    let meta_k = self.session_meta_key(user_key);
    if meta.size == 0 {
      self.del_ttl(user_key).await?;
      self.delete_raw(&meta_k).await?;
    } else {
      let bytes = meta.to_bytes();
      self.upsert_raw(&meta_k, &bytes).await?;
    }
    Ok(())
  }

  /// 追加哈希字段至当前分块索引
  pub async fn append_hash_field(&self, meta: &mut MetaValue, field: &[u8]) -> Result<()> {
    self.append_hash_fields_batch(meta, &[field]).await
  }

  /// 批量追加集合元素至分块索引的核心通用实现（折叠多项更新为最少次数 I/O 写出）
  async fn append_collection_chunks_batch(
    &self,
    tag: KeyTag,
    meta: &mut MetaValue,
    items: &[impl AsRef<[u8]>],
  ) -> Result<()> {
    if items.is_empty() {
      return Ok(());
    }
    let (mut max_chunk_id, mut curr_chunk_len) = Self::get_meta_chunk_info(&meta.reserved);
    let mut chunk_buf: Option<Vec<u8>> = None;

    for (idx, item) in items.iter().enumerate() {
      let it = item.as_ref();
      if curr_chunk_len == 0 || curr_chunk_len >= Self::CHUNK_CAPACITY {
        if let Some(buf) = chunk_buf.take() {
          let chunk_k = self.chunk_key(tag, meta.key_id, meta.version, max_chunk_id);
          self.upsert_raw(&chunk_k, &buf).await?;
        }
        if curr_chunk_len >= Self::CHUNK_CAPACITY {
          max_chunk_id = max_chunk_id.saturating_add(1);
        }
        // 基于本批次剩余条目数与分块剩余容量精确估算缓冲区，消除追加过程中的反复扩容
        let items_left = items.len().saturating_sub(idx);
        let chunk_slots = (Self::CHUNK_CAPACITY as usize).min(items_left);
        let est_cap = (4 + it.len()).saturating_mul(chunk_slots);
        let mut buf = Vec::with_capacity(est_cap);
        wrecord::ChunkCodec::append(it, &mut buf)?;
        chunk_buf = Some(buf);
        curr_chunk_len = 1;
      } else {
        let buf = match chunk_buf {
          Some(ref mut b) => b,
          None => {
            let chunk_k = self.chunk_key(tag, meta.key_id, meta.version, max_chunk_id);
            let mut b = self.read_raw(&chunk_k).await?.unwrap_or_default();
            let items_left = items.len().saturating_sub(idx);
            let chunk_slots =
              (Self::CHUNK_CAPACITY.saturating_sub(curr_chunk_len) as usize).min(items_left);
            let est_add_cap = (4 + it.len()).saturating_mul(chunk_slots);
            b.reserve(est_add_cap);
            chunk_buf.insert(b)
          }
        };
        wrecord::ChunkCodec::append(it, buf)?;
        curr_chunk_len = curr_chunk_len.saturating_add(1);
      }
    }

    if let Some(buf) = chunk_buf {
      let chunk_k = self.chunk_key(tag, meta.key_id, meta.version, max_chunk_id);
      self.upsert_raw(&chunk_k, &buf).await?;
    }

    Self::set_meta_chunk_info(&mut meta.reserved, max_chunk_id, curr_chunk_len);
    Ok(())
  }

  /// 批量追加哈希字段至分块索引（折叠多项更新为最少次数 I/O 写出）
  pub async fn append_hash_fields_batch(
    &self,
    meta: &mut MetaValue,
    fields: &[impl AsRef<[u8]>],
  ) -> Result<()> {
    self
      .append_collection_chunks_batch(KeyTag::HashChunk, meta, fields)
      .await
  }

  /// 追加集合成员至当前分块索引
  pub async fn append_set_member(&self, meta: &mut MetaValue, member: &[u8]) -> Result<()> {
    self.append_set_members_batch(meta, &[member]).await
  }

  /// 批量追加集合成员至分块索引（折叠多项更新为最少次数 I/O 写出）
  pub async fn append_set_members_batch(
    &self,
    meta: &mut MetaValue,
    members: &[impl AsRef<[u8]>],
  ) -> Result<()> {
    self
      .append_collection_chunks_batch(KeyTag::SetChunk, meta, members)
      .await
  }
}

/// 只读集合元数据与紧凑载荷包装（零内存搬移与零额外堆分配）
#[derive(Debug, Clone)]
pub struct RawCollectionRead {
  pub meta: MetaValue,
  raw: Option<Vec<u8>>,
}

impl RawCollectionRead {
  /// 构造只读集合包装
  #[inline(always)]
  pub const fn new(meta: MetaValue, raw: Option<Vec<u8>>) -> Self {
    Self { meta, raw }
  }

  /// 获取紧凑集合载荷切片（零拷贝直接切片偏移，无需对底层缓冲进行 drain 内存搬移）
  #[inline(always)]
  pub fn compact_payload(&self) -> Option<&[u8]> {
    self.raw.as_deref().map(|b| {
      if b.len() > META_VALUE_SIZE {
        &b[META_VALUE_SIZE..]
      } else {
        &[]
      }
    })
  }
}
