//! 集合元数据操作（对标 C# Garnet StorageSession/MainObjectStore 的元数据读写）
//!
//! 覆盖：元数据记录 load（含 TTL 守卫与幽灵清理）、集合快删路由（delete/contains_key）、
//! 树排空纯粹路径。物理域按 doc/zh/collection.md 存储打标分两态：
//! 「分页分层态」唯一物理域是 KeyTag::Meta（MetaValue + BftreeStub 存根 + 独立
//! 树文件），RangeIndex（RI.CREATE 显式索引）与升阶后的通用对象（Hash/Set/List/
//! ZSet）共用该域——升阶臂见 wkv range_index/stub.rs promote_collection_to_bftree，
//! 先建快照后原子换入：首升阶（replace=false）落元记录、删信封为非原子三步，
//! 窗口内双态残留由排空回收单点 handle_bftree_drain_and_delete 的信封域
//! 幂等墓碑兜底收敛；分层重灌（replace=true）经 publish_tree_from_snapshot_locked
//! 同一内核换树，旧树全程可读、无销毁蒸发窗口；降阶臂反向（先写回信封、再清退树与元记录）；「内存态」的
//! 通用对象只驻信封
//! KeyTag::ObjectEnvelope（含 ObjectStoreRMW 增量条目），该态绝无独立 Meta 记录，
//! 删除收敛至信封墓碑。

use wdev::Device;
use wval::{KeyTag, META_VALUE_SIZE, MetaValue};

use crate::{
  error::{Error, Result},
  session::{StoreResult, StoreSession},
};

impl<D: Device> StoreSession<D> {
  /// 删除指定键（支持普通键及集合 O(1) 秒删自愈与树态键排空：RangeIndex / 升阶集合）
  ///
  /// 双域删除：String 域未命中再删 ObjectEnvelope 域——用户键同一时刻至多
  /// 驻留一个物理域，DEL 语义与类型无关（对标 C# 统一存 DELETE 不区分
  /// ValueIsObject）；对象删空自愈与过期清除（purge_expired → delete）同经此处
  ///
  /// RENAME 迁移 claim 判点先于一切清退（DEL 链与 SET 两臂同纪律，wkv 写
  /// 内核单点）：Meta 在场探测 + migration_claimed 复合判定置于 del_ttl/
  /// del_etag 之前，claim 在册即显式回 [`Error::MigrationBusy`]——禁「视同
  /// 不存在」穿透（load_meta 的缺席穿透仅服务探测面；DEL 是写入口，穿透即
  /// 排空臂放弃后继续销毁信封域：懒降阶窗内被拒 DEL 删掉降阶臂刚写回的新
  /// 信封，drain 落 meta 墓碑后整键消失；RENAME 窗内被拒 DEL 剥旧键 TTL，
  /// 破坏「失败即原态」承诺），被拒 DEL 零副作用
  pub async fn delete(&self, key: &[u8]) -> Result<bool> {
    let meta_k = self.session_meta_key(key);
    if self.store.index.load().find_tag(&meta_k).is_some()
      && self.store.range_index.migration_claimed(&meta_k)
    {
      return Err(Error::MigrationBusy);
    }
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
    if self.load_meta(key).await?.is_some() {
      // KeyTag::Meta 树态域（RangeIndex 与升阶集合共用，单树单文件生命周期）：
      // 原子墓碑 + BfTree 树实例注销排空与底层磁盘文件物理释放；未升阶通用对象
      // 的信封物理删除收敛至下方双域墓碑通道。删键臂随键清 TTL（keep_ttl=false）
      self.handle_bftree_drain_and_delete(key, false).await?;
      meta_del = true;
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

  /// 检查指定键是否存在且未被墓碑删除（支持普通键及集合对象）
  ///
  /// 存活判定含 key 级惰性过期：集合走 load_meta 的 TTL 守卫，
  /// 普通键仅在数据命中后经 has_ttl_tag 单探针门控探测 TTL 记录（快路径零额外 I/O）；
  /// 数据面双域判定：String 域未命中再探 ObjectEnvelope 域（对象键 EXPIRE/TTL 生效前提）。
  /// 元记录存活判据取 [`MetaValue::is_live`] 单点（RangeIndex 恒活 + size > 0），
  /// 与 load_meta / 同步面 meta_collection_type_of 同源一处定义——空 RangeIndex
  /// 仍是存活索引，TTL 族快慢路径不得分叉
  pub async fn contains_key(&self, key: &[u8]) -> Result<bool> {
    if let Some(meta) = self.load_meta(key).await?
      && meta.is_live()
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
      // probe_alive 单点：has_ttl_tag 快门控，无 TTL 记录时完全跳过异步过期裁决
      //（与 read_with 口径一致，本调用链内该键的 TTL 裁决仅此一次）
      if !self.probe_alive(key).await? {
        return Ok(false);
      }
      return Ok(true);
    }
    Ok(false)
  }

  /// 裸数据存活判定（不含任何 TTL 探测/清除）：集合元记录存活（[`MetaValue::is_live`]）或数据记录存在
  ///
  /// 专供 expire_at/persist 的融合读改写路径：本键 TTL 裁决由调用方单次 `ttl_of`
  /// 读取统一闭环（读路径 TTL 探测收敛不变式：同一同步调用链内同一用户键只做一次
  /// TTL 裁决），此处刻意采用 contains_key 的数据面口径但剥离惰性过期探测，避免
  /// 同一链内对同一 TTL 记录双次遍历。元记录口径与 load_meta 事实一致：
  /// [`MetaValue::is_live`] 单点判存活（RangeIndex 恒活 + size > 0），空 RangeIndex
  /// 视同存活——expire_at 对空 RI 设 TTL 成功（:1），AOF/复制重放端 TTL 不再被
  /// 「空索引判缺失」静默丢弃（-2）
  pub(crate) async fn contains_key_ignore_ttl(&self, key: &[u8]) -> Result<bool> {
    let meta_k = self.session_meta_key(key);
    if let Some(meta) = self.read_raw_with(&meta_k, MetaValue::from_slice).await? {
      let meta = meta?;
      if meta.is_live() {
        return Ok(true);
      }
    }
    let str_k = self.session_string_key(key);
    if self.contains_key_raw(&str_k).await? {
      return Ok(true);
    }
    let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, key);
    self.contains_key_raw(&env_k).await
  }

  /// 在已有纪元保护下快速检查是否存在集合对象元数据（完全绕过 enter() 原子开销）
  #[inline]
  pub fn check_object_meta_fast_unprotected(&self, user_key: &[u8]) -> Result<Option<bool>> {
    let prefix = self.session_prefix();
    self.check_object_meta_fast_unprotected_with_prefix(prefix.as_slice(), user_key)
  }

  /// 显式前缀快速检查集合对象元数据（循环前缀外提对位，语义与
  /// [`Self::check_object_meta_fast_unprotected`] 一致；rust 工程优化无 c# 对应）
  #[inline]
  pub fn check_object_meta_fast_unprotected_with_prefix(
    &self,
    prefix: &[u8],
    user_key: &[u8],
  ) -> Result<Option<bool>> {
    let meta_k = Self::session_tag_key_with_prefix(prefix, KeyTag::Meta, user_key);
    let hash = whasher::fast_hash(&meta_k);
    // 纯哈希表无锁极速初筛（零内存记录访问）：
    // 若哈希表中连元数据 Tag 都完全不存在，100% 确认无此集合对象，极速返回 false
    let Some(first_addr) = self.store.index.load().find_tag_by_hash(hash) else {
      return Ok(Some(false));
    };
    match self.try_read_raw_in_memory_with_addr(&meta_k, hash, Some(first_addr), |bytes| {
      bytes.len() >= META_VALUE_SIZE && MetaValue::from_slice(bytes).is_ok_and(|m| m.is_live())
    })? {
      StoreResult::Success(is_active) => Ok(Some(is_active)),
      StoreResult::NotFound => Ok(Some(false)),
      StoreResult::RecordOnDisk => Ok(None),
    }
  }

  /// 快速检查是否存在集合对象元数据（纯同步无锁内存探测，对齐 Garnet NetworkSET 快速路径）
  /// - Ok(Some(true)): 明确存在对象元数据（需要报错 WRONGTYPE 或走异步处理）
  /// - Ok(Some(false)): 明确不存在对象元数据（可安全执行纯同步快速写）
  /// - Ok(None): 冷数据可能驻留磁盘，需回退异步深层加载
  #[inline]
  pub fn check_object_meta_fast(&self, user_key: &[u8]) -> Result<Option<bool>> {
    let _guard = self.enter_gated();
    self.check_object_meta_fast_unprotected(user_key)
  }

  /// 读取树态元数据记录（KeyTag::Meta 域：RangeIndex 与升阶集合共用）
  ///
  /// 闭包直出按值 32 字节 `MetaValue` 小结构（`repr(C, align(8))` + `Copy`，`from_slice` 为
  /// const fn 纯栈解析）：内存命中路径零堆分配，免去 `to_vec` 整记录拷贝；
  /// 磁盘回退路径亦免一次中间 Vec 拷贝，仅保留设备读取固有的单次缓冲分配。
  ///
  /// 含 key 级 TTL 守卫：仅当树态元记录存活时才探测 TTL 记录，已过期则视同
  /// 不存在——写路径对已过期索引按不存在重建，符合 Redis 语义；本守卫经
  /// purge_expired"先删 TTL 记录"约定保证无递归。
  /// 幽灵清除：非 RangeIndex 且 size 为 0 的空元记录（删空自愈残留，RangeIndex
  /// 空树仍为存活索引故不计）物理清除元记录与随键 TTL 后视同不存在，杜绝
  /// 幽灵空元记录引发的状态不一致。
  /// 读路径 TTL 收敛不变式：内部 `read_raw_with` 为无守卫裸读内核，本键 TTL 裁决
  /// 只在此入口做一次，嵌套裸读绝不重复裁决
  pub async fn load_meta(&self, user_key: &[u8]) -> Result<Option<MetaValue>> {
    let meta_k = self.session_meta_key(user_key);
    // RENAME 迁移 claim 判定：迁移中 → 视同不存在（与持久墓碑态等价）——
    // 并发 DEL 排空臂据此放弃（防迁移窗内排空正在快照的旧树）、TTL 惰性清除
    // 与分层路由探测视键缺失（缺席即终态，无穿透写面）；RI.CREATE 不吃本入口
    // 的缺席穿透，由 range_index_create 的 claim 早退按 MigrationBusy 显式
    // 拒绝（禁走重建路径）。claim 判据取树身份键 = 物理 Meta 键（下方装载
    // 同键，零额外分配），跨库同名键按物理域隔离
    if self.store.range_index.migration_claimed(&meta_k) {
      return Ok(None);
    }
    match self.read_raw_with(&meta_k, MetaValue::from_slice).await? {
      Some(meta) => {
        let meta = meta?;
        let is_alive = meta.is_live();
        if is_alive && !self.probe_alive(user_key).await? {
          return Ok(None);
        }
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

  /// 树态元记录原子墓碑自愈（对标 Garnet `HasRemoveKey` -> `ExpireAndStop`）
  ///
  /// 在 garnet 中的相对路径:libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:InPlaceUpdaterWorker
  ///
  /// 仅供树态排空路径（handle_bftree_drain_and_delete，RangeIndex 与升阶集合共用）：
  /// 1. 原子删除元记录（写物理墓碑，记录即删即失效）；
  /// 2. 随键 TTL 分流（keep_ttl 判别，见下）。
  ///
  /// keep_ttl 两态对标 C# 对象记录两形态的过期字段口径：
  /// - false = 删键臂（DEL/过期清除、SET 覆写清退、RMW 删空自愈、STORE 族目标
  ///   清退）：记录随键消亡，HasExpiration 亦随之消失（RMWMethods.cs:111
  ///   CheckExpiry → ExpireAndStop 与 DeleteMethods 删除臂连尾随字段一并清除），
  ///   同步 del_ttl 清旁路记录，杜绝孤儿 TTL；
  /// - true = 降阶迁移臂：键全程存活，仅元记录/信封换域。C# 对象记录重写
  ///   （GetRMWModifiedFieldInfo，VarLenInputMethods.cs:42）把 HasExpiration 从
  ///   源记录原样前移到修改后记录，重写事件不脱落过期、也零发 TTL 事件；本仓
  ///   TTL 为独立旁路记录（KeyTag::Ttl，随用户键而非换域记录），故前移语义 =
  ///   不触碰旁路。此臂绝不清 TTL，否则一次迁移即静默抹掉 EXPIRE 设置的键级
  ///   过期，且清除会经 TtlWrite(expire_at=None) 镜像成 Persist 条目扩散到
  ///   从库与 AOF 回放面。
  ///
  /// WATCH 版本栅栏与 C# watchVersionMap.IncrementVersion 对齐，由 wtxn
  /// watch_version_map 单点承载（写面经 bump_watch_version 推进），元记录
  /// 本身不携带版本号。
  pub(crate) async fn drain_and_delete_collection_meta(
    &self,
    user_key: &[u8],
    keep_ttl: bool,
  ) -> Result<()> {
    let meta_k = self.session_meta_key(user_key);
    self.delete_raw(&meta_k).await?;
    if !keep_ttl {
      self.del_ttl(user_key).await?;
    }
    Ok(())
  }
}
