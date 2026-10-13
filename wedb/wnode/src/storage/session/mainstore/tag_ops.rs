//! 标签物理域读写操作面（对标 C# StorageSession partial 在 libs/server/
//! Storage/Session/MainStore/ 的域分文件组织；rust 工程标签域内核，无单一
//! C# 文件对应）
//!
//! 全部落在本域 [`StorageSession`] 上：标签读族（同步快路径 + 磁盘候选降级
//! wkv 异步闭环）、标签写族（upsert / 对象信封整值写回 / 删除，WATCH 版本
//! 推进收口见各写口注释）。

use std::result::Result as StdResult;
// obj_save 页翻转故障注入（Ordering）仅 debug 装配，release 剔除防 unused imports
#[cfg(debug_assertions)]
use std::sync::atomic::Ordering;

use wcol::object_payload::obj_encode_into;
use wdev::Device;
use wkv::StoreResult;
use wval::{GarnetObjectType, KeyTag};

#[cfg(debug_assertions)]
use super::super::storage_session::OBJ_SAVE_PAGESWAP_INJECT;
use super::super::storage_session::StorageSession;

/// 标签读「同步命中即回值 / 磁盘候选降级 PENDING 异步闭环」骨架单点：`read_tag_quiet`、
/// `read_tag_with_size` 的 ctx/batch 两分支共用同一 match（纯文本展开，`&mut f` 复用 /
/// `&f` 借值与返回口径逐字不变；`$this` 承 `self`，因宏卫生下 `self` 关键字不可由宏体直引）
macro_rules! tag_read_sync_then_pending {
  ($this:expr, $sync:expr, $asyncf:expr) => {
    match $sync? {
      StoreResult::RecordOnDisk => $this.with_pending_metrics($asyncf).await?,
      res => res.value(),
    }
  };
}

impl<'a, D: Device> StorageSession<'a, D> {
  /// 读指定标签物理键值（零拷贝闭包版：快路径内存直读，磁盘候选异步闭环）
  ///
  /// libs/server/Storage/Session/MainStore/MainStoreOps.cs:ReadWithUnsafeContext
  ///
  /// [`Self::read_string_with`] 的带标签内核（一处定义）：对象信封域
  /// （KeyTag::ObjectEnvelope）与字符串域共用本实现；TTL 门控按用户键裁决
  ///
  /// C# 统一存 GET（Read → IsPending 才 CompletePending → Found 入账 found/
  /// notfound）的 rust 单点：libs/server/Storage/Session/UnifiedStore/
  /// UnifiedStoreOps.cs:GET （pending 段收口 [`Self::with_pending_metrics`]，
  /// RENAME/EXISTS 内部读亦经本口）。入账在本入口薄包装单点完成（不入
  /// [`Self::read_tag_quiet`] 内核），多域组合探针经静默内核出口折叠计数
  pub async fn read_tag_with<R>(
    &self,
    key: &[u8],
    tag: wval::KeyTag,
    f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<Option<R>> {
    let opt = self.read_tag_quiet(key, tag, f).await?;
    self.record_read_outcome(opt.is_some());
    Ok(opt)
  }

  /// 单域标签读静默内核（[`Self::read_tag_with`] 的不入账对偶）：供多域
  /// 组合漏斗（[`Self::read_user_with_prefix`]）逐探承接——域探各自入账会使
  /// 缺失键计 3、对象键计 2，与 C# GET 恒单条口径失联（票
  /// zcode-r131c-dumprest 案二）；组合漏斗出口按 `UserReadAsync::record_outcome`
  /// 折叠恰一条。另供零入账漏斗（[`Self::read_user_quiet`]）的域后二级续探
  /// （OBJECT 慢臂信封/Meta 两探，票 zcode-r157c-objenc 案一：该漏斗出口本不
  /// 入账，续探若走簿记入口会使分层驻态键虚报 2 条）。单域簿记消费者一律走
  /// [`Self::read_tag_with`]，勿直取本内核（防静默丢计）
  pub(crate) async fn read_tag_quiet<R>(
    &self,
    key: &[u8],
    tag: wval::KeyTag,
    mut f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<Option<R>> {
    Ok(if let Some(ctx) = self.consistent_read_context() {
      tag_read_sync_then_pending!(
        self,
        ctx.try_read_tag_sync_unprotected(key, tag, &mut f),
        || ctx.read_tag_with(key, tag, &mut f)
      )
    } else {
      tag_read_sync_then_pending!(self, self.batch.try_read_tag_sync(key, tag, &mut f), || {
        self.batch.read_tag_with(key, tag, &mut f)
      })
    })
  }

  /// 读指定标签物理键值并披露记录物理尺寸（MEMORY USAGE 慢路径专用）
  ///
  /// 在 garnet 中的相对路径:libs/server/API/GarnetApiUnifiedCommands.cs:MEMORYUSAGE
  ///
  /// [`Self::read_tag_with`] 的带尺寸对位（尺寸口径见 [`wkv::RecordRead`]，
  /// 对标 C# `srcLogRecord.AllocatedSize`）；附着一致读会话时协议口同步触发
  ///（pre 超时上抛中止，对标 C# 一致读会话切换后 MEMORYUSAGE 经一致读上下文）
  pub async fn read_tag_with_size<R>(
    &self,
    key: &[u8],
    tag: wval::KeyTag,
    f: impl Fn(&[u8], usize) -> R,
  ) -> wkv::Result<Option<R>> {
    let opt = if let Some(ctx) = self.consistent_read_context() {
      tag_read_sync_then_pending!(self, ctx.try_read_tag_sync_with_size(key, tag, &f), || ctx
        .read_tag_with_size(key, tag, &f))
    } else {
      tag_read_sync_then_pending!(
        self,
        self.batch.try_read_tag_sync_with_size(key, tag, &f),
        || self.batch.read_tag_with_size(key, tag, &f)
      )
    };
    Ok(opt)
  }

  /// 读指定标签物理键值的显式前缀变体（循环前缀外提对位，语义与
  /// [`Self::read_tag_with`] 逐臂一致；rust 工程优化无 c# 对应：批量键命令
  /// （EXISTS 族）在循环外单次外提 `session_prefix()` 交本口，消除逐域重读
  /// ns/db 原子变量与重算 Varint）
  ///
  /// 同步快路径走 wkv 前缀内核 `try_read_tag_sync_unprotected_with_prefix`
  /// （TTL 门裁决与数据读取复用同一外提前缀），附着一致读会话的 pre/post 回合
  /// 与触发哈希同取自入参前缀（wkv `with_session_consistent_read_with_prefix`，
  /// 与 [`Self::read_tag_with`] 的 ctx 分支同一协议单点）；磁盘候选降级臂整体交
  /// 既有 [`Self::read_tag_with`] 闭环（wkv 异步内核自带前缀解析，冷路径成本
  /// 与本口接入前一致），入账与 [`Self::read_tag_with`] 同在本入口薄包装单点
  pub async fn read_tag_with_prefix<R>(
    &self,
    prefix: &[u8],
    key: &[u8],
    tag: wval::KeyTag,
    f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<Option<R>> {
    let opt = self.read_tag_quiet_with_prefix(prefix, key, tag, f).await?;
    self.record_read_outcome(opt.is_some());
    Ok(opt)
  }

  /// 带前缀标签读静默内核（[`Self::read_tag_with_prefix`] 的不入账对偶，
  /// 供 [`Self::read_user_with_prefix`] 多域组合漏斗逐探承接，出入账纪律与
  /// [`Self::read_tag_quiet`] 一致）；磁盘候选降级臂整体交
  /// [`Self::read_tag_quiet`]（wkv 异步内核自带前缀解析，冷路径成本与本口
  /// 接入前一致，记账统一在簿记入口薄包装完成，内核不重复记账）
  pub(crate) async fn read_tag_quiet_with_prefix<R>(
    &self,
    prefix: &[u8],
    key: &[u8],
    tag: wval::KeyTag,
    mut f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<Option<R>> {
    let opt =
      match self
        .batch
        .with_session_consistent_read_with_prefix(prefix, key, tag, || {
          self
            .batch
            .try_read_tag_sync_unprotected_with_prefix(prefix, key, tag, &mut f)
        })?? {
        // 磁盘候选：整体降级静默异步读口（记账在簿记入口承接）
        StoreResult::RecordOnDisk => self.read_tag_quiet(key, tag, &mut f).await?,
        res => res.value(),
      };
    Ok(opt)
  }

  /// 写指定标签物理键值（String 域 SET 语义清 TTL；ObjectEnvelope 域 RMW 语义
  /// 保留 TTL，同步快路径优先，环形缓冲翻转异步闭环）
  ///
  /// [`Self::upsert_string`] 的带标签内核（一处定义）；String 域写入附带
  /// 对象信封覆写清退（语义见 wkv `try_upsert_tag_sync_unprotected`）。
  /// 对象信封域写成功后同栈触发信封整值写通知（对标 C# WriteLogUpsert：
  /// libs/server/Storage/Functions/ObjectStore/PrivateMethods.cs）——本方法是
  /// 异步段对象写回的唯一漏斗（自定义对象命令慢路径 / RENAME 经此），
  /// 同步快路径增量条目由 resp 层 ObjectStoreRMW 端口单独承接，不重复入账
  ///
  /// C# 统一存 SET 双重载（upsert 源记录 / RENAME 键覆写重投）的 rust 单轨
  /// 落点：libs/server/Storage/Session/UnifiedStore/UnifiedStoreOps.cs:SET
  /// （键覆写重载由 RENAME 慢路径经本口带新键重投承接）
  pub async fn upsert_tag(&self, key: &[u8], tag: wval::KeyTag, val: &[u8]) -> wkv::Result<()> {
    match self.batch.try_upsert_tag_sync(key, tag, val)? {
      Ok(_) => {}
      // 降级 wkv 异步闭环（BatchStoreSession 官方封装，等价于退出批处理纪元后重写）
      Err(_) => {
        self
          .with_pending_metrics(|| self.batch.upsert_tag(key, tag, val))
          .await
          .map(|_| ())?;
      }
    }
    // WATCH 版本推进由 wkv 用户键写入口统一收口（同步成功臂 /
    // 异步闭环臂各恰好一次，C# PostInitialWriter 对位），本层不再重复推进
    // 信封整值写通知（物理键随栈帧内联编码，零堆分配）
    if tag == KeyTag::ObjectEnvelope {
      let raw_key = self.batch.session_tag_key(tag, key);
      self.batch.notify_envelope_upsert(raw_key.as_slice(), val)?;
    }
    Ok(())
  }

  /// 写入对象键（覆盖既有信封；记录挂 KeyTag::ObjectEnvelope 物理域）
  ///
  /// C# 对象记录创建/更新双臂（对象存 InitialUpdater 建新记录、Simple 对象
  /// 会话函数 InitialUpdater/CopyUpdater/InPlaceUpdater 三写臂）在 rust 信封
  /// 单轨下的折叠落点——内存对象变更后整值写回一处承接：
  /// libs/server/Storage/Functions/ObjectStore/RMWMethods.cs:InitialUpdater
  /// libs/server/Storage/Functions/SimpleGarnetObjectSessionFunctions.cs:InitialUpdater
  /// libs/server/Storage/Functions/SimpleGarnetObjectSessionFunctions.cs:CopyUpdater
  /// libs/server/Storage/Functions/SimpleGarnetObjectSessionFunctions.cs:InPlaceUpdater
  ///
  /// 同步快路径走 wkv 单次成形直写（`try_upsert_envelope_sync_fill`）：信封
  /// `[1B 标签][payload]` 在记录槽位一次落笔、物理键单次编码，AOF 镜像与记录
  /// 共享同一已编码字节（写成功地址零拷贝借值，对齐 C# WriteLogUpsert 从
  /// srcLogRecord 取值入账）——全程无中间整值 `Vec`。
  ///
  /// 返回值契约（降级态可见通道，票 wnode-collect-fallback-blind-write-after-
  /// recheck）：`Ok(true)` = 同步快路径闭环；`Ok(false)` = 环形页翻转降级
  ///（同步臂未落写入）——信封写回是复验判词先于落笔的面，内部降级闭环
  ///（upsert_tag 异步 I/O 窗）跨 await 无再裁决，盲写即复活已删键 / 与新写
  /// String 双域并存，故绝不内嵌盲写：写回复验面（apply_rmw_post_operate，
  /// 经 [`crate::resp::objects::rmw_helpers`] 页翻转原地重试闭环）见信号即
  /// 「复验 → 同步快写」原地重试，重试窗复验不过按存储忙拒绝；重放 / 迁移等
  /// 无复验诉求面改经显式降级入口 [`Self::upsert_tag`] 闭环（对标 C# 无此
  /// 形态——求值与写回同记录锁内，间隙结构性不存在）
  #[inline]
  pub async fn obj_save(
    &self,
    key: &[u8],
    tag: GarnetObjectType,
    payload: &[u8],
  ) -> wkv::Result<bool> {
    // 页翻转故障注入（测试钩子，一次性）：模拟同步快路径页翻转降级，
    // 同步臂未落写入即回降级信号
    #[cfg(debug_assertions)]
    if OBJ_SAVE_PAGESWAP_INJECT.swap(false, Ordering::AcqRel) {
      return Ok(false);
    }
    let rec_k = self.batch.session_tag_key(KeyTag::ObjectEnvelope, key);
    match self
      .batch
      .try_upsert_envelope_sync_fill_with_prefix(key, &rec_k, tag as u8, payload)?
    {
      StdResult::Ok(addr) => {
        // 信封整值写通知：镜像值与记录共享同一字节；极端驻留缺口防御物化补投
        let mirrored = self.batch.with_record_value(addr, |val| {
          self.batch.notify_envelope_upsert(rec_k.as_slice(), val)
        });
        match mirrored {
          Some(r) => r?,
          None => {
            let mut val = Vec::with_capacity(payload.len() + 1);
            obj_encode_into(tag, payload, &mut val);
            self.batch.notify_envelope_upsert(rec_k.as_slice(), &val)?;
          }
        }
        Ok(true)
      }
      // 环形页翻转：驱动官方驱逐推进原语解除翻转（`upsert_raw` 翻转臂同款
      // 循环步，对标 C# InternalUpsert.cs 异步落盘驱逐重试循环）——纯物理
      // 推进零写入零语义，落笔绝不进驱逐 await 窗；降级信号显式回传，落笔由
      // 调用方「复验终判 → 同步快写」零让核临界区重试（写回复验面经
      // obj_save_pageswap_replay 闭环）
      StdResult::Err(page_id) => {
        self.batch.session.evict_pages_for(page_id).await?;
        Ok(false)
      }
    }
  }

  /// 删除指定标签物理键（同步快路径优先，磁盘异步闭环）
  ///
  /// [`Self::delete_string`] 的带标签对位（ACL 旁路标签 AOF 回放等以非
  /// String 域承载的整值记录删除面）
  ///
  /// WATCH 版本推进收口：底层 [`wkv::BatchStoreSession::try_delete_tag_sync`]
  /// 与降级臂 `delete_raw` 均为物理键原语（无用户键版本收口，与
  /// `try_delete_sync` 在 wkv 用户键入口收口不同），故统一在本层 match
  /// 汇合后单点推进 `bump_watch_version(key)`——含同步快路径未命中的
  /// `Ok(false)` 缺席观测，对标 C# MainStore
  /// DeleteMethods.InitialDeleter 无条件 IncrementVersion（缺席键墓碑
  /// 追加同向计入，MainStore DeleteMethods.cs:16）；降级臂物理删除失败
  /// （Err 上抛）未落任何写入不推进
  pub async fn delete_tag(&self, key: &[u8], tag: wval::KeyTag) -> wkv::Result<bool> {
    let deleted = match self.batch.try_delete_tag_sync(key, tag)? {
      // 快路径闭环：内存命中或明确未命中（Ok(false)），版本推进由下方单点承接
      Ok(deleted) => deleted,
      // 降级异步闭环（环形页翻转 / 冷数据确认）：物理键原语，无用户键
      // 版本收口，故本层推进一次
      Err(_) => {
        let rec_k = self.batch.session_tag_key(tag, key);
        self
          .with_pending_metrics(|| self.batch.delete_raw(&rec_k))
          .await?
      }
    };
    self.bump_watch_version(key);
    Ok(deleted)
  }
}
