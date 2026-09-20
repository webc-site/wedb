//! 物理键写入与删除路径（对标 C# Garnet ClientSession 的 Upsert/Delete 快慢路径）

mod append;
mod copy_to_tail;
mod inplace;
mod rmw;

use std::result::Result as StdResult;

pub(crate) use copy_to_tail::CopyToTailOutcome;
pub use rmw::RmwGrow;
use wdev::Device;
use wval::KeyTag;

use crate::{
  error::{Error, Result},
  session::StoreSession,
};

impl<D: Device> StoreSession<D> {
  /// 纯同步快速路径写入当前会话指定标签物理键（对齐 Garnet InternalUpsert / NetworkSET 执行链路）
  ///
  /// String 域 SET 语义同步清除既有 key 级 TTL 记录：TTL 记录驻留可变区时墓碑
  /// 同步闭环；需异步驱逐（PageNotReady）或冷数据确认时返回 Ok(Err(u64::MAX))，
  /// 交由调用方降级异步 upsert 路径闭环清除（杜绝残留 TTL 使新值被误判过期）。
  /// ObjectEnvelope 域为集合对象 RMW 回写面，保留既有 TTL 不触碰（语义详见
  /// [`Self::try_upsert_tag_sync_unprotected`]）
  #[inline(always)]
  pub fn try_upsert_tag_sync(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    val: &[u8],
  ) -> Result<StdResult<u64, u64>> {
    let _guard = self.enter_gated();
    self.try_upsert_tag_sync_unprotected(user_key, tag, val)
  }

  /// 纯同步快速路径写入当前会话普通字符串键（对齐 Garnet InternalUpsert / NetworkSET 执行链路）
  ///
  /// SET 语义同步清除既有 key 级 TTL 记录：TTL 记录驻留可变区时墓碑同步闭环；
  /// 需异步驱逐（PageNotReady）或冷数据确认时返回 Ok(Err(u64::MAX))，
  /// 交由调用方降级异步 upsert 路径闭环清除（杜绝残留 TTL 使新值被误判过期）
  #[inline(always)]
  pub fn try_upsert_sync(&self, user_key: &[u8], val: &[u8]) -> Result<StdResult<u64, u64>> {
    self.try_upsert_tag_sync(user_key, KeyTag::String, val)
  }

  /// 纯同步快速路径写入内核（调用方须已处于纪元保护下，批处理上下文专用）
  ///
  /// SET 语义（仅 String 域）：同步清除既有 key 级 TTL 记录（Redis 字符串写入
  /// 命令一律移除 TTL，对标 C# UpsertMethods 建新记录即无 Expiration），TTL 清除
  /// 需异步驱逐或冷数据确认时返回 Ok(Err(u64::MAX))，环形缓冲区翻转时返回精确
  /// page_id，均由调用方退出批处理纪元后降级全异步路径闭环（纪元守卫绝不跨越
  /// 任何可能磁盘 I/O 的 await）
  ///
  /// RMW 语义（ObjectEnvelope 域，即集合对象回写 ZADD/HSET/SADD/LPUSH 等）：
  /// 保留既有 key 级 TTL 记录不触碰——对标 C#
  /// UnifiedStore/VarLenInputMethods 的 GetRMWModifiedFieldInfo
  /// （HasExpiration = srcLogRecord.DataHeader.HasExpiration）与
  /// ObjectStore/RMWMethods.cs 经 TrySetValueObjectAndPrepareOptionals 保留
  /// expiration；带 TTL 的 STORE 族目标键写回等 SET 语义场景由调用方显式清 TTL
  ///
  /// KeyTag::String 写入附带对象信封域覆写清退（探测到 ObjectEnvelope 残留
  /// 记录即墓碑之）：对标 C# 统一存 SET 覆写任意类型记录（Redis SET 语义：
  /// 字符串写入使键变为 string），杜绝信封幽灵记录令集合命令读到已覆写键。
  /// 对象命令写信封前已双探裁决类型不符（WRONGTYPE），信封写入不反查 String 域
  ///
  /// RENAME 迁移 claim 判点先于一切清退（String 域块首复合判定，命中借降级臂
  /// Ok(Err(u64::MAX)) 交异步闭环显式拒绝，被拒写零副作用，语义详见
  /// [`Self::upsert_tag`]）
  ///
  /// WATCH 版本推进收口（用户键写入口单点）：内存写入实际完成后推进一次，
  /// 对标 C# UpsertMethods 的 PostInitialWriter / InPlaceUpdater 挂点
  /// （MainStore UpsertMethods.cs:45/:58、UnifiedStore/ObjectStore 同名面）；
  /// 降级臂（Ok(Err)）未落任何写入不推进，由调用方异步闭环路径（本模块
  /// [`Self::upsert_tag`]）补推。附带清退的 TTL/信封旁路墓碑不单独推进——
  /// C# 记录一体（Expiration 随记录写入）同向只计一次。刻意差异：Vector
  /// 域回调（vector_store_callbacks）与内部元数据走 `*_raw_*` 物理键原语，
  /// 不属 RESP 用户键空间，不经本收口；后台 GC 仅清除已过期键，读语义
  /// 等价 NOTFOUND，亦不推进（C# 后台清除经 functions 推版本，待后续对齐）
  #[inline(always)]
  pub fn try_upsert_tag_sync_unprotected(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    val: &[u8],
  ) -> Result<StdResult<u64, u64>> {
    let prefix = self.session_prefix();
    self.try_upsert_tag_sync_unprotected_with_prefix(prefix.as_slice(), user_key, tag, val)
  }

  /// 纯同步快速路径删除指定标签物理键（无锁/无内部 enter，批处理上下文专用）
  #[inline(always)]
  pub fn try_delete_tag_sync_unprotected(
    &self,
    user_key: &[u8],
    tag: KeyTag,
  ) -> Result<StdResult<bool, u64>> {
    let rec_k = self.session_tag_key(tag, user_key);
    self.try_delete_raw_sync_unprotected(&rec_k)
  }

  /// 显式前缀删除内核（循环前缀外提对位，语义与
  /// [`Self::try_delete_tag_sync_unprotected`] 完全一致；rust 工程优化无 c#
  /// 对应）：ACL 等固定落于 db 0 的旁路标签须以目标前缀（而非会话活跃库
  /// 前缀）定位记录，故提供前缀显式变体
  #[inline(always)]
  pub fn try_delete_tag_sync_unprotected_with_prefix(
    &self,
    prefix: &[u8],
    user_key: &[u8],
    tag: KeyTag,
  ) -> Result<StdResult<bool, u64>> {
    let rec_k = Self::session_tag_key_with_prefix(prefix, tag, user_key);
    self.try_delete_raw_sync_unprotected(&rec_k)
  }

  /// 显式前缀写内核（循环前缀外提对位，语义与
  /// [`Self::try_upsert_tag_sync_unprotected`] 完全一致；rust 工程优化无 c#
  /// 对应）：String 域随键 TTL/信封残留探测与目标记录共三次编码复用同一
  /// 外提前缀，消除批量遍历逐键重读 ns/db 原子变量与重算 Varint
  #[inline(always)]
  pub fn try_upsert_tag_sync_unprotected_with_prefix(
    &self,
    prefix: &[u8],
    user_key: &[u8],
    tag: KeyTag,
    val: &[u8],
  ) -> Result<StdResult<u64, u64>> {
    // SET 语义清 TTL 仅 String 域（Redis 字符串写入移除 TTL）；信封域对象回写
    // 走 RMW 语义保留 TTL（对标 C# GetRMWModifiedFieldInfo），STORE 族目标键等
    // SET 语义场景由调用方显式 del_ttl
    if tag == KeyTag::String {
      // RENAME 迁移 claim 判点先于一切清退（判点前置，入口即拒零副作用）：
      // Meta 在场探测 + migration_claimed 复合判定置于 TTL/信封/树清退之前，
      // 杜绝被拒 SET 先行销毁被 claim 键的旁域记录（懒降阶窗内被拒 SET 删掉
      // 降阶臂刚写回的新信封 → drain 落 meta 墓碑后整键消失；RENAME 窗内被拒
      // SET 剥旧键 TTL，破坏「失败即原态」承诺）。判点收口与四装载判点同语义，
      // 命中借既有 Ok(Err(u64::MAX)) 降级臂交异步闭环显式拒绝——异步 upsert_tag
      // 同位判定显式回 MigrationBusy，禁「视同不存在」穿透（穿透即 SET 销毁
      // 迁移在册树，已 ACK 值随段四 meta 换域遮蔽丢失、RangeIndexDrop 入账与
      // 段二流块 AOF 交错致主从发散）。claim 在册键必有存活元记录，普通字符串
      // 键仅多一次 Meta 在场探针（本就每笔 String SET 都要探测，前移零新增开销）
      let meta_k = Self::session_tag_key_with_prefix(prefix, KeyTag::Meta, user_key);
      if self.store.index.load().find_tag(&meta_k).is_some()
        && self.store.range_index.migration_claimed(&meta_k)
      {
        return Ok(Err(u64::MAX));
      }
      let ttl_k = Self::ttl_key_with_prefix(prefix, user_key);
      if self.has_ttl_key_unprotected(&ttl_k)?
        && self.try_delete_raw_sync_unprotected(&ttl_k)?.is_err()
      {
        return Ok(Err(u64::MAX));
      }
      let env_k = Self::session_tag_key_with_prefix(prefix, KeyTag::ObjectEnvelope, user_key);
      if self.store.index.load().find_tag(&env_k).is_some()
        && self.try_delete_raw_sync_unprotected(&env_k)?.is_err()
      {
        return Ok(Err(u64::MAX));
      }
      if self.store.index.load().find_tag(&meta_k).is_some() {
        if self.try_delete_raw_sync_unprotected(&meta_k)?.is_err() {
          return Ok(Err(u64::MAX));
        }
        self.unregister_bftree_key(user_key);
        // 树身份键 = 物理 Meta 键（本分支探测与删除同键），跨库同名键的
        // 树注册与数据文件按物理域隔离
        let _ = self.store.range_index.delete_index(&meta_k);
      }
    }
    let rec_k = Self::session_tag_key_with_prefix(prefix, tag, user_key);
    let written = self.try_upsert_raw_sync_unprotected(&rec_k, val)?;
    if written.is_ok() {
      self.bump_watch_version(user_key);
    }
    Ok(written)
  }

  /// 纯同步快速路径写入当前会话普通字符串键内核（调用方须已处于纪元保护下，批处理上下文专用）
  #[inline(always)]
  pub fn try_upsert_sync_unprotected(
    &self,
    user_key: &[u8],
    val: &[u8],
  ) -> Result<StdResult<u64, u64>> {
    self.try_upsert_tag_sync_unprotected(user_key, KeyTag::String, val)
  }

  /// 写入或更新当前会话指定标签物理键（Upsert，异步闭环）
  ///
  /// String 域 SET 语义：清除既有 key 级 TTL 记录（Redis 字符串写入命令一律
  /// 移除 TTL）；ObjectEnvelope 域 RMW 语义保留 TTL（语义详见
  /// [`Self::try_upsert_tag_sync_unprotected`]）。
  /// 内部元数据/分块写入必须走 upsert_raw，避免误清用户键 TTL。
  /// String 域附带对象信封覆写清退（语义见 [`Self::try_upsert_tag_sync_unprotected`]）
  ///
  /// RENAME 迁移 claim 判点先于一切清退（封堵面收口，wkv 写原语单点）：
  /// Meta 在场探测 + migration_claimed 复合判定置于 del_ttl/信封清退/树排空
  /// 之前，claim 在册即回 [`Error::MigrationBusy`]——SET 族覆写对迁移窗内被
  /// claim 键的旧树/元记录/TTL/信封旁域记录禁行任何清退，被拒写零副作用，
  /// 杜绝已 ACK 值随段四 meta 换域遮蔽丢失与主从 AOF 交错发散（与
  /// load_range_index_stub / load_collection_stub / refresh_tiered_meta /
  /// load_meta 四判点同语义；RESP 层 ri_write_gate 不另设第二判点，见
  /// migration.rs 头注）
  ///
  /// WATCH 版本推进收口：写入完成后推进一次，对标 C# UpsertMethods
  /// PostInitialWriter（快路径降级臂由此统一补推，杜绝两套口径）
  #[inline(always)]
  pub async fn upsert_tag(&self, user_key: &[u8], tag: KeyTag, val: &[u8]) -> Result<u64> {
    if tag == KeyTag::String {
      // RENAME 迁移 claim 判点先于一切清退（判点前置，入口即拒零副作用，与
      // 同步臂同位复合判定）：Meta 在场探测 + migration_claimed 置于 del_ttl/
      // 信封清退/树排空之前，杜绝被拒 SET 先行销毁被 claim 键的旁域记录；
      // 同步快路径命中降级臂由此闭环显式拒绝（判点前移后同步臂入口即拒，
      // 降级臂不再承载「拒绝但已破坏」形态；claim 登记与判定间隙的派发微窗
      // 为 migration.rs 段一既述的既有残余，非本判点射程）
      let meta_k = self.session_meta_key(user_key);
      if self.store.index.load().find_tag(&meta_k).is_some()
        && self.store.range_index.migration_claimed(&meta_k)
      {
        return Err(Error::MigrationBusy);
      }
      self.del_ttl(user_key).await?;
      // SET 覆写清退信封域：纯索引探针初筛，无信封键零额外写入
      let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, user_key);
      if self.store.index.load().find_tag(&env_k).is_some() {
        self.delete_raw(&env_k).await?;
      }
      // SET 覆写清退 Meta 域（升阶树/RangeIndex 存根）：若存在则排空并注销
      //（删键臂 keep_ttl=false，TTL 已在上方清除，此处仅余探针零写）
      if self.store.index.load().find_tag(&meta_k).is_some() {
        self.handle_bftree_drain_and_delete(user_key, false).await?;
      }
    }
    let rec_k = self.session_tag_key(tag, user_key);
    let addr = self.upsert_raw(&rec_k, val).await?;
    self.bump_watch_version(user_key);
    Ok(addr)
  }

  /// 写入或更新当前会话普通字符串键（Upsert）
  ///
  /// SET 语义：同步清除既有 key 级 TTL 记录（Redis 字符串写入命令一律移除 TTL）；
  /// 内部元数据/分块写入必须走 upsert_raw，避免误清用户键 TTL
  #[inline(always)]
  pub async fn upsert(&self, user_key: &[u8], val: &[u8]) -> Result<u64> {
    self.upsert_tag(user_key, KeyTag::String, val).await
  }

  /// 纯同步快速删除键（支持普通键快速路径，对齐 Garnet NetworkDEL 行为）
  ///
  /// - 若为普通键且内存命中：纯同步直接返回 Ok(Ok(deleted))；
  /// - 若遭遇环形页翻转：返回 Ok(Err(page_id))；
  /// - 若属于复合对象元数据：返回 Ok(Err(u64::MAX)) 指示调用方降级走完整异步路由。
  #[inline]
  pub fn try_delete_sync(&self, user_key: &[u8]) -> Result<StdResult<bool, u64>> {
    let _guard = self.enter_gated();
    self.try_delete_sync_unprotected(user_key)
  }

  /// 纯同步快速删除键内核（调用方处于已有纪元保护下，批处理上下文专用，零 enter() 开销）
  ///
  /// 级联清理随键旁路记录（TTL 与 ETag）：对标 C# 删除记录即连同记录尾
  /// 可选 ETag/Expiration 字段一并消失（LogRecord 物理一体）。
  ///
  /// 双域删除（一处定义）：String 域未命中再删 ObjectEnvelope 域——用户键
  /// 同一时刻至多驻留其中一个物理域，DEL 语义须与类型无关（对标 C# 统一存
  /// DELETE 不区分 ValueIsObject）
  ///
  /// RENAME 迁移 claim 判点先于一切清退（与 SET 两臂同纪律，见
  /// [`Self::try_delete_sync_unprotected_with_prefix`] 头注）：命中借降级臂交
  /// 异步 delete() 显式 MigrationBusy，被拒 DEL 零副作用
  ///
  /// WATCH 版本推进收口（用户键删除入口单点）：同步闭环（含未命中无写入的
  /// `Ok(Ok(false))`）即推进，对标 C# DeleteMethods.InitialDeleter 无条件
  /// IncrementVersion（缺席键的墓碑追加同样计入，MainStore DeleteMethods.cs:16）；
  /// 降级臂（Ok(Err)）未落任何写入不推进，由调用方异步闭环路径补推
  #[inline]
  pub fn try_delete_sync_unprotected(&self, user_key: &[u8]) -> Result<StdResult<bool, u64>> {
    let prefix = self.session_prefix();
    self.try_delete_sync_unprotected_with_prefix(prefix.as_slice(), user_key)
  }

  /// 显式前缀删内核（循环前缀外提对位，语义与
  /// [`Self::try_delete_sync_unprotected`] 完全一致；rust 工程优化无 c# 对应）：
  /// TTL/ETag/元数据/记录共五处物理键编码复用同一外提前缀，消除批量遍历
  /// 逐键重读 ns/db 原子变量与重算 Varint。
  /// RENAME 迁移 claim 判点先于一切清退（DEL 链与 SET 两臂同纪律）：命中借
  /// 降级臂 Ok(Err(u64::MAX)) 交异步 delete() 同位判定显式 MigrationBusy，
  /// 杜绝被拒 DEL 先剥被 claim 键的 TTL/ETag/信封旁域记录（懒降阶窗内被拒
  /// DEL 删掉降阶臂刚写回的新信封 → drain 落 meta 墓碑后整键消失；RENAME
  /// 窗内被拒 DEL 剥旧键 TTL，破坏「失败即原态」承诺）
  #[inline]
  pub fn try_delete_sync_unprotected_with_prefix(
    &self,
    prefix: &[u8],
    user_key: &[u8],
  ) -> Result<StdResult<bool, u64>> {
    // 判点前置：Meta 在场探测 + migration_claimed 复合判定置于 TTL/ETag 清退
    // 之前——claim 在册键必有存活元记录，普通键仅多一次 Meta 在场探针。
    // claim 判据取树身份键 = 物理 Meta 键（本函数下方排空与元记录同键），
    // 跨库同名键按物理域隔离
    let meta_k = Self::session_tag_key_with_prefix(prefix, KeyTag::Meta, user_key);
    if self.store.index.load().find_tag(&meta_k).is_some()
      && self.store.range_index.migration_claimed(&meta_k)
    {
      return Ok(Err(u64::MAX));
    }
    let ttl_k = Self::ttl_key_with_prefix(prefix, user_key);
    if self.has_ttl_key_unprotected(&ttl_k)?
      && self.try_delete_raw_sync_unprotected(&ttl_k)?.is_err()
    {
      return Ok(Err(u64::MAX));
    }
    let etag_k = Self::etag_key_with_prefix(prefix, user_key);
    if self.has_etag_key_unprotected(&etag_k)?
      && self.try_delete_raw_sync_unprotected(&etag_k)?.is_err()
    {
      return Ok(Err(u64::MAX));
    }
    let deleted = match self.check_object_meta_fast_unprotected_with_prefix(prefix, user_key)? {
      Some(false) => {
        let str_k = Self::session_tag_key_with_prefix(prefix, KeyTag::String, user_key);
        match self.try_delete_raw_sync_unprotected(&str_k)? {
          Ok(true) => Ok(Ok(true)),
          // 环形页翻转 / 冷数据确认：降级全异步路径（不得再探信封域）
          Err(page_id) => Ok(Err(page_id)),
          Ok(false) => {
            let env_k = Self::session_tag_key_with_prefix(prefix, KeyTag::ObjectEnvelope, user_key);
            match self.try_delete_raw_sync_unprotected(&env_k)? {
              // 双域（String/ObjectEnvelope）皆未命中：宿主值域外登记态缺席
              // 观测（对标 C# GarnetRecordTriggers.cs:OnDispose Deleted →
              // VectorManager.RequestDeletion）。命中登记即视同删除成功，
              // 计数据随存储删除单点收敛，RESP 各臂不再另配第二套清退判据
              Ok(false) => match self.store.delete_miss_hook.get() {
                Some(hook) if hook.call(prefix, user_key) => Ok(Ok(true)),
                _ => Ok(Ok(false)),
              },
              // 信封域命中 / 页翻转降级：原样返回
              other => Ok(other),
            }
          }
        }
      }
      // 复合对象元数据：降级完整异步路由
      _ => Ok(Err(u64::MAX)),
    };
    if let Ok(Ok(_)) = &deleted {
      self.bump_watch_version(user_key);
    }
    deleted
  }
}
