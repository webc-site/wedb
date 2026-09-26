//! 物理键写入与删除路径（对标 C# Garnet ClientSession 的 Upsert/Delete 快慢路径）

mod append;
mod copy_to_tail;
mod inplace;
mod rmw;

use std::result::Result as StdResult;

pub(crate) use copy_to_tail::CopyToTailOutcome;
pub use rmw::RmwGrow;
use wdev::Device;
use wrecord::ValSrc;
use wval::{I64Codec, KeyTag, TaggedKeyBuf};

use super::DEGRADE_ASYNC;
use crate::{
  error::{Error, Result},
  session::{StoreResult, StoreSession},
};

impl<D: Device> StoreSession<D> {
  /// 纯同步快速路径写入当前会话指定标签物理键（对齐 Garnet InternalUpsert / NetworkSET 执行链路）
  ///
  /// String 域 SET 语义同步清除既有 key 级 TTL 记录：TTL 记录驻留可变区时墓碑
  /// 同步闭环；需异步驱逐（PageNotReady）或冷数据确认时返回 Ok(Err(DEGRADE_ASYNC))，
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
  /// 调用契约：本原语是纯盲写（无地址复验），命令层调用方须先持本键读改写
  /// 窗口（`try_rmw_window`）——盲写落在他者读算写间隙即非可串行化（GETDEL
  /// 答旧删新、INCR 写回顶替），对标 C# InternalUpsert.cs:67 纯写回同取记录
  /// 闩（票 zcode-r32-rmwmatrix 立项一）
  ///
  /// SET 语义同步清除既有 key 级 TTL 记录：TTL 记录驻留可变区时墓碑
  /// 同步闭环；需异步驱逐（PageNotReady）或冷数据确认时返回 Ok(Err(DEGRADE_ASYNC))，
  /// 交由调用方降级异步 upsert 路径闭环清除（杜绝残留 TTL 使新值被误判过期）
  #[inline(always)]
  pub fn try_upsert_sync(&self, user_key: &[u8], val: &[u8]) -> Result<StdResult<u64, u64>> {
    self.try_upsert_tag_sync(user_key, KeyTag::String, val)
  }

  /// 纯同步快速路径写入内核（调用方须已处于纪元保护下，批处理上下文专用）
  ///
  /// SET 语义（仅 String 域）：同步清除既有 key 级 TTL 记录（Redis 字符串写入
  /// 命令一律移除 TTL，对标 C# UpsertMethods 建新记录即无 Expiration），TTL 清除
  /// 需异步驱逐或冷数据确认时返回 Ok(Err(DEGRADE_ASYNC))，环形缓冲区翻转时返回精确
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
  /// 本声明射程（票 zcode-r147c-incrovf 案一订正，免遭断章）：「无反向并存窗」
  /// 不限于 SET 命令面——凡 String 域重建落笔（含 RMW 写回
  /// [`RmwWindow::try_rmw_sync`](crate::session::RmwWindow::try_rmw_sync) 的过期
  /// 重建臂）一律改道本内核与异步 [`Self::upsert_tag`]，旁域清退单源在本模块，
  /// 任何调用臂禁另起第二套裸写/裸清退
  ///
  /// 分层树 / RangeIndex 存根键（Meta 在场）覆写一律降级异步：树清退单点定义于
  /// 异步臂 [`Self::handle_bftree_drain_and_delete`]（元记录墓碑 + 在途写者排空 +
  /// 树文件释放 + 换号旁表注销 + RangeIndexDrop AOF 入账），同步快路径禁做第二套
  /// 裸清退——裸删 Meta 记录经 notify_write_listener 落 StoreEvent::Write，而 AOF
  /// 镜像 Write 臂放行集不含 KeyTag::Meta，同步臂又不发 RangeIndexDrop，两臂各
  /// 自推进即生「一臂清元数据、另一臂留孤儿树文件」分裂态。删除路径「复合对象
  /// 元数据降级完整异步路由」同纪律（见
  /// [`Self::try_delete_sync_unprotected_with_prefix`]）
  ///
  /// RENAME 迁移 claim 判点前移至异步臂 [`Self::upsert_tag`] 单点：同步臂 Meta
  /// 在场即降级，在册键由异步同位判点显式回 MigrationBusy（判点先于异步臂
  /// del_ttl/信封/树清退，被拒写零副作用）
  ///
  /// 写序收口（票 zcode-r34-writekernel 条目一，对标 C# 记录一体写入的「任意
  /// 一步失败键保持完整旧态（含 TTL）」不变式——InitialWriter 先
  /// TrySetValueSpanAndPrepareOptionals 再 TrySetExpiration，索引 CAS 即原子
  /// 切换）：String 域按「Meta 探针降级 → TTL 腿删除（预读补偿）→ 数据落笔 →
  /// 信封清退」排序，数据落笔置于一切破坏性清退生效之后；数据落笔遇页翻转/
  /// 硬错误即回填预读的原过期刻度再降级/上抛，旧值不失 TTL 永生。信封清退
  /// 重排至数据落笔成功之后，杜绝「信封已删而数据未写」的对象销毁形——
  /// 「String+信封并存」暂存窗由读面域探针序（String 优先）与 wnode 落笔前
  /// 域复验（obj_save_recheck）既有机制兜底，不新增第二套裁决
  ///
  /// WATCH 版本推进收口（用户键写入口单点）：数据落笔提交成功即推进一次，
  /// 对标 C# UpsertMethods 的 PostInitialWriter / InPlaceUpdater 挂点
  /// （MainStore UpsertMethods.cs:45/:58、UnifiedStore/ObjectStore 同名面）。
  /// 降级/失败臂口径按重排后事实：Meta/TTL 腿降级与数据落笔失败（页翻转/
  /// 硬错误，TTL 已回填补偿还原旧态）零逻辑写入不推进，由调用方异步闭环路径
  /// （本模块 [`Self::upsert_tag`]）补推；数据已提交后的信封清退降级/硬错误
  /// 已随数据提交推进，闭环臂幂等重做。附带清退的 TTL/信封旁路墓碑不单独
  /// 推进——C# 记录一体（Expiration 随记录写入）同向只计一次。刻意差异：
  /// Vector 域回调（vector_store_callbacks）与内部元数据走 `*_raw_*` 物理键
  /// 原语，不属 RESP 用户键空间，不经本收口；后台 GC 仅清除已过期键，读语义
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
    // SET 语义场景由调用方显式 del_ttl。
    // ttl_restore 携带已删 TTL 记录的键与原过期刻度（条目一补偿依据）；env_present
    // 为数据落笔前协同后的信封在场快照（清退调用在数据落笔成功后）
    let mut ttl_restore: Option<(TaggedKeyBuf, i64)> = None;
    let mut env_k: Option<TaggedKeyBuf> = None;
    if tag == KeyTag::String {
      // 树清退单点归异步臂（handle_bftree_drain_and_delete，见 range_index/drain.rs）：
      // Meta 在场即分层树 / RangeIndex 存根键，同步快路径禁做第二套裸清退，一律借
      // 既有降级臂 Ok(Err(DEGRADE_ASYNC)) 交异步 upsert_tag 闭环。裸删 Meta 记录不经
      // AOF 镜像（service.rs Write 臂放行集不含 KeyTag::Meta），与异步臂各有推进即生
      // 「一臂清元数据、另一臂留孤儿树文件」分裂态；单点归异步后杜绝。与删除路径
      // 「复合对象元数据降级完整异步路由」同纪律。
      // RENAME 迁移 claim 判点一并前移至异步臂同位判定：claim 在册键必有存活元记录，
      // 降级后由异步 upsert_tag 显式回 MigrationBusy（判点先于异步臂 del_ttl/信封/树
      // 清退，被拒写零副作用），同步臂 Meta 在场探针与在册探针折叠为一次前置探测
      let meta_k = Self::session_tag_key_with_prefix(prefix, KeyTag::Meta, user_key);
      // 旁路探针前的分裂协同（对标 InternalUpsert.cs:64-66 入口铁律）：扩容期
      // 先迁移目标分块，杜绝裸探未迁移新桶漏检存活元记录致降级臂失效、同步臂
      // 对分层树/存根键裸写
      self.ensure_split(&meta_k)?;
      if self.store.index.load().find_tag(&meta_k).is_some() {
        return Ok(Err(DEGRADE_ASYNC));
      }
      // TTL 腿（先删后写语义所需，条目一）：删除前预读原过期刻度留存补偿依据，
      // 数据落笔失败时回填还原旧态。磁盘候选（RecordOnDisk）时同步段零写入沿
      // 降级臂交异步闭环；预读缺席（探针后被并发清退成墓碑）视同无 TTL 免删
      let ttl_k = Self::ttl_key_with_prefix(prefix, user_key);
      if self.has_ttl_key_unprotected(&ttl_k)? {
        let expire = match self.try_read_raw_in_memory(&ttl_k, I64Codec::decode)? {
          StoreResult::Success(v) => v,
          StoreResult::NotFound => None,
          StoreResult::RecordOnDisk => return Ok(Err(DEGRADE_ASYNC)),
        };
        if let Some(exp) = expire {
          if self.try_delete_raw_sync_unprotected(&ttl_k)?.is_err() {
            return Ok(Err(DEGRADE_ASYNC));
          }
          ttl_restore = Some((ttl_k, exp));
        }
      }
      // 信封旁路探针同纪律先协同（env_k 与 meta_k 哈希异桶，各自独立协同）；
      // 协同错误发生在任何写入之前（零副作用上抛），清退调用移落数据落笔成功
      // 之后（条目一重排，杜绝信封已删而数据未写的对象销毁形）
      let ek = Self::session_tag_key_with_prefix(prefix, KeyTag::ObjectEnvelope, user_key);
      self.ensure_split(&ek)?;
      env_k = Some(ek);
    }
    let rec_k = Self::session_tag_key_with_prefix(prefix, tag, user_key);
    let written = self.try_upsert_raw_sync_unprotected(&rec_k, val);
    if matches!(written, Err(_) | Ok(Err(_)))
      && let Some((ttl_k, exp)) = &ttl_restore
    {
      // TTL 腿失败补偿（条目一）：数据落笔遇页翻转/硬错误即盲写回填原过期刻度
      // 再降级/上抛——旧值不失 TTL 判死依据而永生复活；回填条目经写监听镜像入
      // AOF（TtlWrite(Some)），副本/恢复重放同向还原。回填自身再失败（极端页
      // 翻转）弃补偿保首发错误，异步闭环臂幂等重做
      let _ = self.try_upsert_raw_sync_unprotected(ttl_k, &I64Codec::encode(*exp));
    }
    let written = written?;
    if written.is_err() {
      return Ok(written);
    }
    self.bump_watch_version(user_key);
    // 信封清退（数据落笔提交成功后，条目一重排）：删除遇页翻转/冷数据确认沿
    // 降级臂交异步闭环（数据已生效并已推进，闭环臂幂等重做 SET + 清退）；
    // 「String+信封并存」暂存窗由读面域探针序（String 优先）与 wnode 落笔前
    // 域复验（obj_save_recheck）既有机制兜底
    if let Some(env_k) = &env_k
      && self.store.index.load().find_tag(env_k).is_some()
      && self.try_delete_raw_sync_unprotected(env_k)?.is_err()
    {
      return Ok(Err(DEGRADE_ASYNC));
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

  /// 对象信封单次成形同步快路径写（记录挂 KeyTag::ObjectEnvelope 物理域）
  ///
  /// 信封值 `[1B 对象标签][payload]` 经 [`EnvelopeSrc`] 分段直写源在记录槽位
  /// 分配点一次成形（原位 / 链内复活 / 复活池 / 尾部追加四臂同源），消除调用方
  /// 先 `Vec` 整值暂存再转拷入记录的中间拷贝与堆分配——对标 C#
  /// GarnetObjectSerializer.Serialize 经 BinaryObjectSerializer 直写记录
  /// value span、无中间堆缓冲再转拷（garnet/libs/server/Objects/Types/
  /// GarnetObjectSerializer.cs:104）。信封字节布局与 wcol `obj_encode_custom_into`
  /// 逐字节一致。
  ///
  /// 语义与 [`Self::try_upsert_tag_sync_unprotected_with_prefix`] 的
  /// ObjectEnvelope 域一致（RMW 语义保留 TTL、不触碰 String/Meta 域）：
  /// - `Ok(Ok(addr))`：纯内存写入成功（WATCH 版本推进已收口），addr 供
  ///   [`Self::with_record_value`] 零拷贝借值发 AOF 镜像；
  /// - `Ok(Err(page_id))`：环形缓冲区翻转，调用方物化整值降级异步闭环；
  /// - `Err`：存储层错误。
  ///
  /// 本口不发任何镜像事件：RMW 增量条目路径（run_sync_rmw 经
  /// `obj_save_sync`）另行显式通知，整值收敛路径（`obj_save_custom_notified` /
  /// 异步档 `obj_save`）由调用方携 addr 借记录值经
  /// [`WedbStore::notify_envelope_upsert`] 单点入账，杜绝双份。
  /// `rec_k` 由调用方单次编码（[`Self::session_tag_key_with_prefix`]）复用至
  /// 镜像入账，热路径物理键一次编码零重复。
  /// 调用契约：本口为 unprotected 内核（调用方须已处于纪元保护下，批处理
  /// 上下文专用——语义同 [`Self::try_upsert_tag_sync_unprotected_with_prefix`]）
  pub fn try_upsert_envelope_sync_fill_with_prefix(
    &self,
    user_key: &[u8],
    rec_k: &TaggedKeyBuf,
    obj_tag: u8,
    payload: &[u8],
  ) -> Result<StdResult<u64, u64>> {
    let src = EnvelopeSrc {
      tag: obj_tag,
      payload,
    };
    match self.try_upsert_raw_sync_unprotected_val(rec_k.as_slice(), &src)? {
      StdResult::Ok(addr) => {
        self.bump_watch_version(user_key);
        Ok(Ok(addr))
      }
      StdResult::Err(page_id) => Ok(Err(page_id)),
    }
  }

  /// [`Self::try_upsert_envelope_sync_fill_with_prefix`] 的裸会话便捷档
  /// （自带纪元保护 enter；物理键一次编码即弃，无镜像复用诉求的纯写调用方
  /// 专用。批处理上下文（BatchStoreSession）请用其零 enter 包装，杜绝逐写
  /// 冗余原子）
  #[inline]
  pub fn try_upsert_envelope_sync_fill(
    &self,
    user_key: &[u8],
    obj_tag: u8,
    payload: &[u8],
  ) -> Result<StdResult<u64, u64>> {
    let _guard = self.enter_gated();
    let rec_k = self.session_tag_key(KeyTag::ObjectEnvelope, user_key);
    self.try_upsert_envelope_sync_fill_with_prefix(user_key, &rec_k, obj_tag, payload)
  }

  /// 从内存驻留记录零拷贝借用 value 切片消费（`*_with` 闭包只读 API 族）
  ///
  /// 服务对象信封写回成功后的 AOF 镜像：镜像值与记录共享同一字节（对齐 C#
  /// WriteLogUpsert 从 srcLogRecord 取值入账，libs/server/Storage/Functions/
  /// ObjectStore/PrivateMethods.cs），杜绝为镜像再物化整值缓冲。`addr` 须为
  /// 同步写入口刚返回的内存地址（epoch 保护下驻留恒成立）；极端驻留缺口回
  /// `None`，调用方按防御口径物化补投
  #[inline]
  pub fn with_record_value<R>(&self, addr: u64, f: impl FnOnce(&[u8]) -> R) -> Option<R> {
    self.store.hlog.with_record_value(addr, f).ok().flatten()
  }

  /// 纯同步快速条件写入物理记录（NX 语义：仅当键不存在时原子写入，存在即原样保留）
  #[inline(always)]
  pub fn try_insert_tag_sync_unprotected_with_prefix(
    &self,
    prefix: &[u8],
    user_key: &[u8],
    tag: KeyTag,
    val: &[u8],
  ) -> Result<StdResult<bool, u64>> {
    if tag == KeyTag::String {
      let meta_k = Self::session_tag_key_with_prefix(prefix, KeyTag::Meta, user_key);
      self.ensure_split(&meta_k)?;
      if self.store.index.load().find_tag(&meta_k).is_some() {
        return Ok(Err(DEGRADE_ASYNC));
      }
      let env_k = Self::session_tag_key_with_prefix(prefix, KeyTag::ObjectEnvelope, user_key);
      self.ensure_split(&env_k)?;
      if self.store.index.load().find_tag(&env_k).is_some() {
        return Ok(Err(DEGRADE_ASYNC));
      }
    }
    let rec_k = Self::session_tag_key_with_prefix(prefix, tag, user_key);
    let written = self.try_insert_raw_sync_unprotected(&rec_k, val)?;
    match written {
      Ok(Some(_)) => {
        self.bump_watch_version(user_key);
        Ok(Ok(true))
      }
      Ok(None) => Ok(Ok(false)),
      Err(page_id) => Ok(Err(page_id)),
    }
  }

  /// 纯同步快速条件写入当前会话普通字符串键（NX 语义）
  #[inline]
  pub fn try_insert_sync(&self, user_key: &[u8], val: &[u8]) -> Result<StdResult<bool, u64>> {
    let _guard = self.enter_gated();
    let prefix = self.session_prefix();
    self.try_insert_tag_sync_unprotected_with_prefix(
      prefix.as_slice(),
      user_key,
      KeyTag::String,
      val,
    )
  }

  /// 纯同步快速条件写入当前会话指定标签物理键（NX 语义）
  #[inline]
  pub fn try_insert_tag_sync(
    &self,
    user_key: &[u8],
    tag: KeyTag,
    val: &[u8],
  ) -> Result<StdResult<bool, u64>> {
    let _guard = self.enter_gated();
    let prefix = self.session_prefix();
    self.try_insert_tag_sync_unprotected_with_prefix(prefix.as_slice(), user_key, tag, val)
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
    // TTL 腿补偿依据（条目一）：已删 TTL 记录的原过期刻度，数据落笔硬失败时
    // 回填还原旧态
    let mut ttl_restore: Option<i64> = None;
    if tag == KeyTag::String {
      // RENAME 迁移 claim 判点先于一切清退（判点前置，入口即拒零副作用，与
      // 同步臂同位复合判定）：Meta 在场探测 + migration_claimed 置于 del_ttl/
      // 信封清退/树排空之前，杜绝被拒 SET 先行销毁被 claim 键的旁域记录；
      // 同步快路径命中降级臂由此闭环显式拒绝（判点前移后同步臂入口即拒，
      // 降级臂不再承载「拒绝但已破坏」形态；claim 登记与判定间隙的派发微窗
      // 为 migration.rs 段一既述的既有残余，非本判点射程）
      let meta_k = self.session_meta_key(user_key);
      // 异步臂旁路探针同纪律先协同（对标 InternalUpsert.cs:64-66：协同严格先于
      // FindTag；await 间隙可能有新一轮扩容启动，各探针位点独立协同）
      self.ensure_split(&meta_k)?;
      if self.store.index.load().find_tag(&meta_k).is_some()
        && self.store.range_index.migration_claimed(&meta_k)
      {
        return Err(Error::MigrationBusy);
      }
      // TTL 腿（先删后写语义所需，条目一）：删除前异步读原过期刻度留存补偿依据
      //（异步读通含磁盘候选，无降级形态）；预读缺席（墓碑/非法长度/并发清退）
      // 视同无 TTL 免删
      let ttl_k = self.ttl_key(user_key);
      if self.has_ttl_key(&ttl_k)?
        && let Some(exp) = self.read_i64_sidecar(&ttl_k).await?
      {
        self.del_ttl(user_key).await?;
        ttl_restore = Some(exp);
      }
      // SET 覆写清退信封域与 Meta 域（升阶树/RangeIndex 存根）的探针协同保留在
      // 数据落笔前（await 间隙扩容防护，对标 InternalUpsert.cs:64-66），清退
      // 调用移落数据落笔成功之后（条目一重排，杜绝信封已删/树已排空而数据
      // 未写的对象销毁形）
      let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, user_key);
      self.ensure_split(&env_k)?;
      self.ensure_split(&meta_k)?;
    }
    let rec_k = self.session_tag_key(tag, user_key);
    let addr = match self.upsert_raw(&rec_k, val).await {
      Ok(addr) => addr,
      Err(e) => {
        // TTL 腿失败补偿（条目一，同同步臂）：经 put_ttl 完整原语（原位优先
        // RCU 降级）回填原过期刻度再上抛，回填条目镜像入 AOF；回填自身再失败
        // 弃补偿保首发错误
        if let Some(exp) = ttl_restore {
          let _ = self.put_ttl(user_key, exp).await;
        }
        return Err(e);
      }
    };
    self.bump_watch_version(user_key);
    if tag == KeyTag::String {
      // SET 覆写清退信封域（数据落笔提交成功后，条目一重排）：纯索引探针初筛，
      // 无信封键零额外写入
      let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, user_key);
      self.ensure_split(&env_k)?;
      if self.store.index.load().find_tag(&env_k).is_some() {
        self.delete_raw(&env_k).await?;
      }
      // SET 覆写清退 Meta 域（升阶树/RangeIndex 存根）：若存在则排空并注销
      //（删键臂 keep_ttl=false，TTL 已在上方清除，此处仅余探针零写）
      let meta_k = self.session_meta_key(user_key);
      self.ensure_split(&meta_k)?;
      if self.store.index.load().find_tag(&meta_k).is_some()
        && let Err(e) = self.handle_bftree_drain_and_delete(user_key, false).await
      {
        if matches!(e, Error::Swapped(_)) {
          self.bump_watch_version(user_key);
        }
        return Err(e);
      }
    }
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
  /// 调用契约：本原语是纯盲删（无地址复验），命令层调用方须先持本键读改写
  /// 窗口（`try_rmw_window`）——盲删落在他者读算写间隙即「DEL :1 而键复活」
  /// 非可串行化（票 zcode-r32-rmwmatrix 立项一），对标 C# InternalDelete.cs:60
  ///
  /// - 若为普通键且内存命中：纯同步直接返回 Ok(Ok(deleted))；
  /// - 若遭遇环形页翻转：返回 Ok(Err(page_id))；
  /// - 若属于复合对象元数据：返回 Ok(Err(DEGRADE_ASYNC)) 指示调用方降级走完整异步路由。
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
  /// 降级臂 Ok(Err(DEGRADE_ASYNC)) 交异步 delete() 同位判定显式 MigrationBusy，
  /// 杜绝被拒 DEL 先剥被 claim 键的 TTL/ETag/信封旁域记录（懒降阶窗内被拒
  /// DEL 删掉降阶臂刚写回的新信封 → drain 落 meta 墓碑后整键消失；RENAME
  /// 窗内被拒 DEL 剥旧键 TTL，破坏「失败即原态」承诺）
  /// DEL/GETDEL 同步内核共序单点：RENAME 迁移 claim 判点前置（Meta 在场探测 +
  /// migration_claimed 复合判定置于 TTL/ETag 清退之前——claim 在册键必有存活
  /// 元记录，普通键仅多一次 Meta 在场探针；claim 判据取树身份键 = 物理 Meta
  /// 键，本函数下方排空与元记录同键，跨库同名键按物理域隔离）→ 随键
  /// TTL/ETag 旁路记录成对清退，任一腿降级即 `Ok(Err(DEGRADE_ASYNC))` 交异步闭环
  fn try_strip_key_sidecars_sync_unprotected_with_prefix(
    &self,
    prefix: &[u8],
    user_key: &[u8],
  ) -> Result<StdResult<(), u64>> {
    let meta_k = Self::session_tag_key_with_prefix(prefix, KeyTag::Meta, user_key);
    // Meta 在场探针前先协同（对标 InternalDelete.cs:57-59），杜绝扩容期漏检
    // 在册 claim 键的元记录致 RENAME 迁移冲突检测失效
    self.ensure_split(&meta_k)?;
    if self.store.index.load().find_tag(&meta_k).is_some()
      && self.store.range_index.migration_claimed(&meta_k)
    {
      return Ok(Err(DEGRADE_ASYNC));
    }
    for tag in [KeyTag::Ttl, KeyTag::Etag] {
      let k = Self::session_tag_key_with_prefix(prefix, tag, user_key);
      if self.has_tag_key_unprotected(&k)? && self.try_delete_raw_sync_unprotected(&k)?.is_err() {
        return Ok(Err(DEGRADE_ASYNC));
      }
    }
    Ok(Ok(()))
  }

  #[inline]
  pub fn try_delete_sync_unprotected_with_prefix(
    &self,
    prefix: &[u8],
    user_key: &[u8],
  ) -> Result<StdResult<bool, u64>> {
    if let Err(page_id) =
      self.try_strip_key_sidecars_sync_unprotected_with_prefix(prefix, user_key)?
    {
      return Ok(Err(page_id));
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
              // VectorManager.RequestDeletion）。观测臂为真异步（宿主登记
              // 摘除 `.await` 闭环，无内联收割），同步内核不可直调——钩子
              // 在场即降级完整异步路由（异步 delete 臂 await 同一钩子收口，
              // 判据同源不分叉）；钩子缺席维持缺席删除原口径
              Ok(false) if self.store.delete_miss_hook.get().is_some() => Ok(Err(DEGRADE_ASYNC)),
              Ok(false) => Ok(Ok(false)),
              // 信封域命中 / 页翻转降级：原样返回
              other => Ok(other),
            }
          }
        }
      }
      // 复合对象元数据：降级完整异步路由
      _ => Ok(Err(DEGRADE_ASYNC)),
    };
    if let Ok(Ok(_)) = &deleted {
      self.bump_watch_version(user_key);
    }
    deleted
  }

  /// 纯同步快速取删字符串域键（GETDEL 读删一体：应答值 = 实际摘除记录的值）
  ///
  /// [`Self::try_delete_sync`] 的取值对位；域判已由调用方带 TTL 裁决的探针收敛
  /// （Hit 即 String 域在场），信封/hook 臂不设
  #[inline]
  pub fn try_take_sync(&self, user_key: &[u8]) -> Result<StdResult<Option<Vec<u8>>, u64>> {
    let _guard = self.enter_gated();
    self.try_take_sync_unprotected(user_key)
  }

  /// 纯同步快速取删键（闭包消费零分配变体）
  #[inline]
  pub fn try_take_sync_with<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<StdResult<Option<R>, u64>> {
    let _guard = self.enter_gated();
    self.try_take_sync_with_unprotected(user_key, f)
  }

  /// 纯同步快速取删键内核（调用方处于已有纪元保护下，批处理上下文专用）
  ///
  /// 级联纪律与 [`Self::try_delete_sync_unprotected_with_prefix`] 完全同构：
  /// RENAME 迁移 claim 判点前置（命中借降级臂交异步 `take_string` 显式
  /// MigrationBusy，被拒 GETDEL 零副作用）→ 随键 TTL/ETag 旁路记录清退 →
  /// String 域记录取删一体摘除（捕获与摘除同一临界区）→ WATCH 版本推进收口
  #[inline]
  pub fn try_take_sync_unprotected(
    &self,
    user_key: &[u8],
  ) -> Result<StdResult<Option<Vec<u8>>, u64>> {
    self.try_take_sync_with_unprotected(user_key, |v| v.to_vec())
  }

  /// 纯同步快速取删键内核（闭包消费零分配变体）
  #[inline]
  pub fn try_take_sync_with_unprotected<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<StdResult<Option<R>, u64>> {
    let prefix = self.session_prefix();
    if let Err(page_id) =
      self.try_strip_key_sidecars_sync_unprotected_with_prefix(prefix.as_slice(), user_key)?
    {
      return Ok(Err(page_id));
    }
    let taken = match self
      .check_object_meta_fast_unprotected_with_prefix(prefix.as_slice(), user_key)?
    {
      Some(false) => {
        let str_k = Self::session_tag_key_with_prefix(prefix.as_slice(), KeyTag::String, user_key);
        self.try_take_raw_sync_with_unprotected(&str_k, f)?
      }
      // 复合对象元数据：降级完整异步路由
      _ => Err(DEGRADE_ASYNC),
    };
    if taken.is_ok() {
      self.bump_watch_version(user_key);
    }
    Ok(taken)
  }
}

/// 对象信封值分段直写源：`[1B 对象标签][payload]`
///
/// 字节布局与 wcol `obj_encode_custom_into`（`buf.push(tag); buf.extend(payload)`）
/// 逐字节一致——信封线格式字节不变；在记录槽位分配点经 [`ValSrc::write_val`]
/// 一次成形，消除中间整值 `Vec` 暂存（对标 C# 序列化器直写记录 value span）
struct EnvelopeSrc<'a> {
  tag: u8,
  payload: &'a [u8],
}

impl ValSrc for EnvelopeSrc<'_> {
  #[inline(always)]
  fn val_len(&self) -> usize {
    self.payload.len() + 1
  }

  #[inline(always)]
  fn write_val(&self, dst: &mut [u8]) {
    dst[0] = self.tag;
    dst[1..].copy_from_slice(self.payload);
  }
}
