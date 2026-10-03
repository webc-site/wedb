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
use wval::{KeyTag, TaggedKeyBuf};

use super::DEGRADE_ASYNC;
use crate::{
  error::{Error, Result},
  session::StoreSession,
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
  /// SET 语义（仅 String 域）：同步清除既有 key 级 TTL 记录与 ETag 旁路记录
  ///（Redis 字符串写入命令一律移除 TTL，且按 C# FieldInfo 规范 HasETag=false
  /// 使覆写后 etag 消亡，票 wkv-set-family-etag-bypass-residue-cas-baseline-
  /// fork 定裁 (a) 对齐清退），TTL/ETag 清除
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
  /// 写序收口（票 zcode-r34-writekernel 条目一 + 票
  /// wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal 崩溃面）：
  /// 对标 C# 记录一体写入（InitialWriter 先 TrySetValueSpanAndPrepareOptionals
  /// 再 TrySetExpiration，索引 CAS 即原子切换）的「键要么完整旧态（含 TTL）
  /// 要么新态」不变式，rust 旁路记录分解形态下以「主记录落笔先行、TTL 腿
  /// 随后」逼近——String 域按「Meta 探针降级 → 数据落笔 → TTL 腿删除 →
  /// 信封清退」排序，数据落笔先于一切破坏性清退生效，崩溃前缀截断残留只有
  /// 两形：数据未落 = 键完整旧态（含 TTL）；数据已落 = 新值 + 残留旧 TTL
  /// （有界、到期正常出账；SET 带 EX 新键「值已落 TTL 未落」同形窗另案勿
  /// 扩面），绝不出现「TTL 已墓碑而值存活」的永生形。旧序「TTL 先剥 +
  /// ttl_restore 预读回填补偿」双机制随序内联消解：数据落笔失败时 TTL 未被
  /// 触碰，补偿链自然消失。信封清退维持数据落笔成功之后（杜绝「信封已删
  /// 而数据未写」的对象销毁形）——「String+信封并存」暂存窗由读面域探针序
  /// （String 优先）与 wnode 落笔前域复验（obj_save_recheck）既有机制兜底，
  /// 不新增第二套裁决
  ///
  /// WATCH 版本推进收口（用户键写入口单点）：数据落笔提交成功即推进一次，
  /// 对标 C# UpsertMethods 的 PostInitialWriter / InPlaceUpdater 挂点
  /// （MainStore UpsertMethods.cs:45/:58、UnifiedStore/ObjectStore 同名面）。
  /// 降级/失败臂口径按重排后事实：Meta 探针降级与数据落笔失败（页翻转/
  /// 硬错误）零副作用（TTL 未触碰）不推进，由调用方异步闭环路径（本模块
  /// [`Self::upsert_tag`]）补推；数据已提交后的 TTL 腿降级/信封清退降级/
  /// 硬错误已随数据提交推进，闭环臂幂等重做。附带清退的 TTL/信封旁路墓碑
  /// 不单独推进——C# 记录一体（Expiration 随记录写入）同向只计一次。刻意差异：
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
    // env_k 为数据落笔前协同后的信封在场快照（清退调用在数据落笔成功后）
    let mut env_k: Option<TaggedKeyBuf> = None;
    if tag == KeyTag::String {
      // 树清退单点归异步臂（handle_bftree_drain_and_delete，见 range_index/drain.rs）：
      // Meta 在场即分层树 / RangeIndex 存根键，同步快路径禁做第二套裸清退，一律借
      // 既有降级臂 Ok(Err(DEGRADE_ASYNC)) 交异步 upsert_tag 闭环。裸删 Meta 记录不经
      // AOF 镜像（service.rs Write 臂放行集不含 KeyTag::Meta），与异步臂各有推进即生
      // 「一臂清元数据、另一臂留孤儿树文件」分裂态；单点归异步后杜绝。与删除路径
      // 「复合对象元数据降级完整异步路由」同纪律。
      // RENAME 迁移 claim 判点一并前移至异步臂同位判定：claim 在册键必有存活元记录，
      // 降级后由异步 upsert_tag 显式回 MigrationBusy（判点先于异步臂 TTL/信封/树
      // 清退，被拒写零副作用），同步臂 Meta 在场探针与在册探针折叠为一次前置探测
      let meta_k = Self::session_tag_key_with_prefix(prefix, KeyTag::Meta, user_key);
      // 旁路探针走协同单点（对标 InternalUpsert.cs:64-66 入口铁律：扩容期
      // 先迁移目标分块，杜绝裸探未迁移新桶漏检存活元记录致降级臂失效、同步臂
      // 对分层树/存根键裸写）
      if self.find_tag_cooperative(&meta_k)?.is_some() {
        return Ok(Err(DEGRADE_ASYNC));
      }
      // 信封旁路探针同纪律先协同（env_k 与 meta_k 哈希异桶，各自独立协同）；
      // 协同错误发生在任何写入之前（零副作用上抛），清退调用移落数据落笔成功
      // 之后（条目一重排，杜绝信封已删而数据未写的对象销毁形）
      let ek = Self::session_tag_key_with_prefix(prefix, KeyTag::ObjectEnvelope, user_key);
      self.ensure_split(&ek)?;
      env_k = Some(ek);
    }
    let rec_k = Self::session_tag_key_with_prefix(prefix, tag, user_key);
    let written = self.try_upsert_raw_sync_unprotected(&rec_k, val)?;
    if written.is_err() {
      return Ok(written);
    }
    self.bump_watch_version(user_key);
    // 随键旁路腿（数据落笔提交成功后，票
    // wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal 后置）：
    // SET 语义成对清退 TTL 与 ETag 旁路——String 域覆写在 TTL 腿删除同点成对
    // 清退 etag 旁路记录，按 C# FieldInfo 规范定裁 (a) 对齐清退（对位
    // libs/server/Storage/Functions/MainStore/VarLenInputMethods.cs:GetUpsertFieldInfo 的 SET/SETEX/APPEND 臂
    // 恒 HasETag=false，票 wkv-set-family-etag-bypass-residue-cas-baseline-
    // fork；原「普通 SET 覆写保留 etag」登记论据系误读 TryCopyOptionals，随裁
    // 收口）。数据未落笔前旁路零触碰（失败即完整旧态，含 TTL/etag），删除遇页
    // 翻转/冷数据沿降级臂交异步闭环——数据已生效并已推进，SET 异步臂 upsert_tag
    // 以同一绝对值幂等重做 SET + 清 TTL + 清 etag，崩溃残留窗收敛为「新值 +
    // 残留旧 TTL/etag」有界形（TTL 到期正常出账；孤儿 etag 待下次覆写或键删除
    // 级联清退，条件写基线经 EtagWrite(None)→Setwithetag(0) 镜像于副本/恢复
    // 同收敛），绝非旧序「旁路已墓碑而值永生」形
    if tag == KeyTag::String
      && !(matches!(
        self.try_strip_ttl_sync_unprotected_with_prefix(prefix, user_key),
        Ok(true)
      ) && matches!(
        self.try_strip_etag_sync_unprotected_with_prefix(prefix, user_key),
        Ok(true)
      ))
    {
      // 页翻转/冷数据/硬失败同沿降级臂（票
      // wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal）：
      // 数据已生效并已推进，异步闭环臂幂等重做 SET + 清 TTL + 清 etag，应答
      // +OK 不失真
      return Ok(Err(DEGRADE_ASYNC));
    }
    // 信封清退（数据落笔提交成功后，条目一重排）：删除遇页翻转/冷数据确认沿
    // 降级臂交异步闭环（数据已生效并已推进，闭环臂幂等重做 SET + 清退）；
    // 「String+信封并存」暂存窗由读面域探针序（String 优先）与 wnode 落笔前
    // 域复验（obj_save_recheck）既有机制兜底
    if let Some(env_k) = &env_k
      && self.find_tag_cooperative(env_k)?.is_some()
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
  /// [`crate::store::WedbStore::notify_envelope_upsert`] 单点入账，杜绝双份。
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
      if self.find_tag_cooperative(&meta_k)?.is_some() {
        return Ok(Err(DEGRADE_ASYNC));
      }
      let env_k = Self::session_tag_key_with_prefix(prefix, KeyTag::ObjectEnvelope, user_key);
      if self.find_tag_cooperative(&env_k)?.is_some() {
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
  /// String 域 SET 语义：成对清除既有 key 级 TTL 与 ETag 旁路记录（Redis 字符串
  /// 写入命令一律移除 TTL；etag 按 C# FieldInfo HasETag=false 随覆写消亡，票
  /// wkv-set-family-etag-bypass-residue-cas-baseline-fork 定裁 (a)）；ObjectEnvelope
  /// 域 RMW 语义保留 TTL（语义详见
  /// [`Self::try_upsert_tag_sync_unprotected`]）。
  /// 内部元数据/分块写入必须走 upsert_raw，避免误清用户键 TTL。
  /// String 域附带对象信封覆写清退（语义见 [`Self::try_upsert_tag_sync_unprotected`]）
  ///
  /// RENAME 迁移 claim 判点先于一切清退（封堵面收口，wkv 写原语单点）：
  /// Meta 在场探测 + migration_claimed 复合判定置于 TTL/信封清退/树排空
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
      // 同步臂同位复合判定）：Meta 在场探测 + migration_claimed 置于 TTL/
      // 信封清退/树排空之前，杜绝被拒 SET 先行销毁被 claim 键的旁域记录；
      // 同步快路径命中降级臂由此闭环显式拒绝（判点前移后同步臂入口即拒，
      // 降级臂不再承载「拒绝但已破坏」形态；claim 登记与判定间隙的派发微窗
      // 为 migration.rs 段一既述的既有残余，非本判点射程）
      let meta_k = self.session_meta_key(user_key);
      // 异步臂旁路探针同纪律先协同（对标 InternalUpsert.cs:64-66：协同严格先于
      // FindTag；await 间隙可能有新一轮扩容启动，各探针位点独立协同）
      if self.migration_claim_busy(user_key)? {
        return Err(Error::MigrationBusy);
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
    // TTL 腿随数据落笔后置（票
    // wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal）：
    // 数据未落笔前 TTL 零触碰，硬失败即完整旧态（含 TTL），ttl_restore 预读
    // 补偿链随序内联消解
    let addr = self.upsert_raw(&rec_k, val).await?;
    self.bump_watch_version(user_key);
    if tag == KeyTag::String {
      // 旁路腿（数据落笔提交成功后）：SET 语义成对清退 TTL 与 ETag（各探针初筛，
      // 无记录键零额外写入）；本腿硬失败上抛——值已提交，调用方重试幂等重做
      // SET + 清 TTL + 清 etag，残留窗收敛为「新值 + 旧 TTL/etag」有界形（TTL
      // 到期正常出账；etag 经 EtagWrite(None)→Setwithetag(0) 镜像于副本/恢复
      // 同收敛）。etag 清退按 C# FieldInfo HasETag=false 定裁 (a)（票
      // wkv-set-family-etag-bypass-residue-cas-baseline-fork），与同步臂
      // try_strip_etag_sync_unprotected_with_prefix 同判据单点收口
      self.del_ttl(user_key).await?;
      self.del_etag(user_key).await?;
      // SET 覆写清退信封域（数据落笔提交成功后，条目一重排）：纯索引探针初筛，
      // 无信封键零额外写入
      let env_k = self.session_tag_key(KeyTag::ObjectEnvelope, user_key);
      if self.find_tag_cooperative(&env_k)?.is_some() {
        self.delete_raw(&env_k).await?;
      }
      // SET 覆写清退 Meta 域（升阶树/RangeIndex 存根）：若存在则排空并注销
      //（删键臂 keep_ttl=false，TTL 已在上方清除，此处仅余探针零写）；探针
      // 与信封臂同走分裂协同单点（await 间隙扩容防护，两探针形制归一）
      let meta_k = self.session_meta_key(user_key);
      if self.find_tag_cooperative(&meta_k)?.is_some()
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
  /// 级联清理随键旁路记录：对标 C# 删除记录即连同记录尾可选 ETag/Expiration
  /// 字段一并消失（LogRecord 物理一体）——rust 旁路分解形态下按「记录墓碑
  /// 先行、TTL 腿随后」序逼近（ETag 先剥维持，票
  /// wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal）。
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
  /// migration_claimed 复合判定置于 ETag 清退之前——claim 在册键必有存活
  /// 元记录，普通键仅多一次 Meta 在场探针；claim 判据取树身份键 = 物理 Meta
  /// 键，本函数下方排空与元记录同键，跨库同名键按物理域隔离）→ 随键 ETag
  /// 旁路记录先剥（孤儿 ETag 危害面真实，其崩溃残留「记录在而 etag 失」良性
  /// 有界，维持先剥序），任一降级即 `Ok(Err(DEGRADE_ASYNC))` 交异步闭环；
  /// 随键 TTL 腿后置到记录墓碑/取删成功之后（票
  /// wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal，见
  /// [`Self::try_strip_ttl_sync_unprotected_with_prefix`])
  fn try_strip_key_sidecars_sync_unprotected_with_prefix(
    &self,
    prefix: &[u8],
    user_key: &[u8],
  ) -> Result<StdResult<(), u64>> {
    // RENAME 迁移 claim 复合判点转调单点 [`Self::migration_claim_busy`]（判据
    // 同位序：分裂协同严格先于 Meta 在场探针，杜绝扩容期漏检在册 claim 键的
    // 元记录致冲突检测失效；两调用方外提前缀恒为会话自前缀，判据键同源）
    if self.migration_claim_busy(user_key)? {
      return Ok(Err(DEGRADE_ASYNC));
    }
    let k = Self::session_tag_key_with_prefix(prefix, KeyTag::Etag, user_key);
    if self.has_tag_key_unprotected(&k)? && self.try_delete_raw_sync_unprotected(&k)?.is_err() {
      return Ok(Err(DEGRADE_ASYNC));
    }
    Ok(Ok(()))
  }

  /// 记录墓碑/落笔成功后的随键 TTL 腿（票
  /// wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal 后置）：
  /// 宿主已定形后再剥 TTL，崩溃残留收敛为「无主 TTL 记录」良性自愈态（紧缩
  /// 孤儿判死丢弃 + GC 过期扫描回收 + SET 覆写清退 + 读面宿主缺席裁决
  /// no-op），绝非旧序「TTL 已墓碑而值存活」的永生形。
  /// 返回 false = 删除遇页翻转/冷数据未剥成，失败政策按消费方裁量：
  /// SET 同步臂沿降级臂交异步幂等重做（值幂等、应答 +OK 不失真）；DEL/GETDEL
  /// 记录已摘除后严禁降级重做——异步闭包对已墓碑键回 false / nil，DEL 计数
  /// 与 GETDEL 应答值失真，调用方弃置即良性残留
  #[inline]
  fn try_strip_ttl_sync_unprotected_with_prefix(
    &self,
    prefix: &[u8],
    user_key: &[u8],
  ) -> Result<bool> {
    self.try_strip_sidecar_sync_unprotected_with_prefix(prefix, user_key, KeyTag::Ttl)
  }

  /// 随键旁路记录（TTL/ETag）同步剥除共核单点：`has` 探针初筛（无记录零额外
  /// 写入）+ 裸删除；返回 false = 删除遇页翻转/冷数据未剥成，失败政策按消费
  /// 方裁量（TTL 后置腿与 ETag 腿契约各见其具名委托）。键构造与探针均收敛到
  /// tag 泛型单点（`ttl_key_with_prefix`/`has_ttl_key_unprotected` 与 etag 侧
  /// 同为 `session_tag_key_with_prefix`/`has_tag_key_unprotected` 转调）。
  #[inline]
  fn try_strip_sidecar_sync_unprotected_with_prefix(
    &self,
    prefix: &[u8],
    user_key: &[u8],
    tag: KeyTag,
  ) -> Result<bool> {
    let k = Self::session_tag_key_with_prefix(prefix, tag, user_key);
    Ok(!(self.has_tag_key_unprotected(&k)? && self.try_delete_raw_sync_unprotected(&k)?.is_err()))
  }

  /// 记录落笔成功后的随键 ETag 腿（票
  /// wkv-set-family-etag-bypass-residue-cas-baseline-fork 定裁 (a) 对齐清退）：
  /// String 域覆写按 C# FieldInfo（HasETag=false）成对清退 etag 旁路记录，与
  /// [`Self::try_strip_ttl_sync_unprotected_with_prefix`] 同判据同点（共核
  /// [`Self::try_strip_sidecar_sync_unprotected_with_prefix`]）。单次哈希
  /// 探针初筛（无 etag 键零额外写入），删除经写监听分发 EtagWrite(None) →
  /// AOF Setwithetag(0) 条目，副本/恢复回放同收敛。返回 false = 删除遇页翻转/
  /// 冷数据未剥成：SET 同步臂沿降级臂交异步 `upsert_tag` 幂等重做（值绝对、
  /// 旁路清退幂等），绝不在值未落笔时前推——本腿严格后置数据提交。
  #[inline]
  fn try_strip_etag_sync_unprotected_with_prefix(
    &self,
    prefix: &[u8],
    user_key: &[u8],
  ) -> Result<bool> {
    self.try_strip_sidecar_sync_unprotected_with_prefix(prefix, user_key, KeyTag::Etag)
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
      // TTL 腿后置（记录墓碑之后，票
      // wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal）：
      // 删除失败弃置不降级——宿主已亡（含双域缺席形），残留为无主 TTL 记录
      // 良性自愈态；异步闭包对已墓碑键回 false，降级重做令 DEL 应答计数失真。
      // 页翻转降级形与硬失败 Err 形同权弃置，严禁 `?` 上抛撕裂已提交的删除
      let _ = self.try_strip_ttl_sync_unprotected_with_prefix(prefix, user_key);
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
  /// MigrationBusy，被拒 GETDEL 零副作用）→ 随键 ETag 旁路记录先剥 →
  /// String 域记录取删一体摘除（捕获与摘除同一临界区）→ 随键 TTL 腿后置
  /// 剥除（失败弃置不降级，票
  /// wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal）→
  /// WATCH 版本推进收口
  #[inline]
  fn try_take_sync_unprotected(&self, user_key: &[u8]) -> Result<StdResult<Option<Vec<u8>>, u64>> {
    self.try_take_sync_with_unprotected(user_key, |v| v.to_vec())
  }

  /// 纯同步快速取删键内核（闭包消费零分配变体）
  #[inline]
  fn try_take_sync_with_unprotected<R>(
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
      // TTL 腿后置（取删成功之后，票
      // wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal）：
      // 删除失败弃置不降级——值已摘除须原样应答（异步 take 对已墓碑键回
      // nil，降级重做令 GETDEL 应答值丢失），残留为无主 TTL 记录良性自愈态。
      // 页翻转降级形与硬失败 Err 形同权弃置，严禁 `?` 上抛丢取删应答值
      let _ = self.try_strip_ttl_sync_unprotected_with_prefix(prefix.as_slice(), user_key);
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
