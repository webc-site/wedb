//! 用户键读会话操作面（双域异步读 + 批量冷读；异步漏斗对标 C#
//! libs/server/Storage/Functions/ObjectStore/ReadMethods.cs:Reader 与
//! libs/server/Storage/Session/MainStore/AdvancedOps.cs 批量预读管线，
//! C# 为 StorageSession partial）
//!
//! [`StorageSession`] 会话面：用户数据双域异步读（簿记 / 静默双口）、对象
//! 判型续探、MGET / GET 慢臂批量漏斗。同步单点与判型内核、折叠出口类型
//! [`UserReadAsync`] 在同域 [`super::user_read`] 一处定义。

use wdev::Device;
use wresp::{cmd_strings::RESP_ERR_WRONG_TYPE, ext::RespVecExt};
use wval::KeyTag;

use super::{
  super::storage_session::StorageSession, ttl_sync::meta_collection_type_of,
  user_read::UserReadAsync,
};

impl<'a, D: Device> StorageSession<'a, D> {
  /// 带 TTL 裁决的用户数据双域异步读（磁盘候选在 [`Self::read_tag_with`] 内
  /// 惰性清除闭环，无 Deferred 态）
  ///
  /// [`Self::read_user_with_prefix`] 的无前缀薄包装：本口取一次
  /// `session_prefix()` 后即交带前缀内核，全仓仅此一条异步读用户键机制
  pub async fn read_user<R>(
    &self,
    key: &[u8],
    f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<UserReadAsync<R>> {
    let prefix = self.batch.session_prefix();
    self.read_user_with_prefix(prefix.as_slice(), key, f).await
  }

  /// 用户数据双域异步读的零入账对偶（RMW 前置读慢臂与元数据读族慢臂探针
  /// 专用）：三域判型与 [`Self::read_user`] 同一静默内核，漏斗尾不折叠命中/
  /// 未命中入账——对位快臂 `read_user_sync(…, None, …)`「句柄 None＝采样关闭
  /// 或 RMW 前置读不入账」纪律（`user_read.rs`）的慢臂出口：GETDEL 族慢臂
  /// 探针走本口（C# GETDEL 全链零入账，MainStoreOps 的 GETDEL 件无
  /// incr_session_*）、OBJECT 族慢臂走本口（C# Read_UnifiedStore
  /// AdvancedOps.cs:12-21 恒零计，票 zcode-r157c-objenc 案一）；
  /// GET/GETEX 等读命令慢臂一律走 [`Self::read_user`] 簿记入口，勿误取本口
  ///
  /// [`Self::read_user_quiet_with_prefix`] 的无前缀薄包装（镜像
  /// [`Self::read_user`] 薄包装先例形，不起第二套判型机制）
  pub async fn read_user_quiet<R>(
    &self,
    key: &[u8],
    f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<UserReadAsync<R>> {
    let prefix = self.batch.session_prefix();
    self
      .read_user_quiet_with_prefix(prefix.as_slice(), key, f)
      .await
  }

  /// 带 TTL 裁决的用户数据双域异步读（簿记入口）：静默三域内核
  /// [`Self::read_user_quiet_with_prefix`] 的入账薄包装，漏斗出口按
  /// [`UserReadAsync::record_outcome`] 折叠恰一条（与同步漏斗
  /// [`crate::storage::session::common::UserRead::record_outcome`] 同一
  /// [`crate::storage::session::common::fold_outcome`] 规则单点）；入账在
  /// 本入口薄包装单点完成（不入静默内核），镜像 [`Self::read_tag_with`]
  /// 形态，读命令慢路径分派臂共用本面
  pub async fn read_user_with_prefix<R>(
    &self,
    prefix: &[u8],
    key: &[u8],
    mut f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<UserReadAsync<R>> {
    let read = self
      .read_user_quiet_with_prefix(prefix, key, &mut f)
      .await?;
    read.record_outcome(self.session_metrics.as_deref());
    Ok(read)
  }

  /// 带 TTL 裁决的用户数据双域异步读的显式前缀静默内核（循环前缀外提对位，
  /// 判型与 [`Self::read_user_with_prefix`] 逐臂一致；rust 工程优化无 c#
  /// 对应：BITOP 等批量键命令在循环外单次外提 `session_prefix()` 交簿记入口，
  /// 消除逐域重读 ns/db 原子变量与重算 Varint）
  ///
  /// 域次序与判型对齐同步单点
  /// [`crate::storage::session::common::read_adjudicated_user_sync`]：String 域
  /// 命中即用户数据；未命中探 ObjectEnvelope 域（对象键 →
  /// [`UserReadAsync::WrongType`]）；再未命中探 Meta 域（升阶 / RI 键同判对象键口径，
  /// C# Reader 单记录统一 ValueIsObject）。三域探针均走
  /// [`Self::read_tag_quiet_with_prefix`] 静默内核（逐域入账会使缺失键计 3、
  /// 对象键计 2，与 C# GET 单条口径失联）；本内核自身零入账（供
  /// [`Self::read_user_quiet`] RMW 前置读臂直取），读命令簿记口径的折叠入账
  /// 由 [`Self::read_user_with_prefix`] 薄包装入口承接
  ///
  /// C# 对象存 ISessionFunctions 读回调（ValueIsObject 门 + CheckExpiry +
  /// 对象输出三段职责）的 rust 异步漏斗：
  /// libs/server/Storage/Functions/ObjectStore/ReadMethods.cs:Reader
  /// ——对象域命中即产出信封载荷（消费闭包内反序列化），过期在
  /// [`Self::read_tag_quiet`] 惰性清除闭环（C# ReadAction.Expire 同判），
  /// 自定义对象命令分派留在命令层单点
  async fn read_user_quiet_with_prefix<R>(
    &self,
    prefix: &[u8],
    key: &[u8],
    mut f: impl FnMut(&[u8]) -> R,
  ) -> wkv::Result<UserReadAsync<R>> {
    if let Some(v) = self
      .read_tag_quiet_with_prefix(prefix, key, KeyTag::String, &mut f)
      .await?
    {
      return Ok(UserReadAsync::Hit(v));
    }
    if self.object_kind_alive_with_prefix(prefix, key).await? {
      return Ok(UserReadAsync::WrongType);
    }
    Ok(UserReadAsync::Missing)
  }

  /// String 域确认缺失后的对象键判型续探（信封 / 升阶 Meta 两域，一处定义）：
  /// [`Self::read_user_quiet_with_prefix`] 三域折叠的后两腿单源抽出，供
  /// GET 慢臂批量漏斗 [`Self::read_user_batch_into`] 复用——批量口（wkv
  /// `session/raw/batch.rs` 恒 KeyTag::String 单域探针，保持不动）确认
  /// String 域缺失后的判型与单键三域折叠走同一通道，零第二套机制。探针走
  /// [`Self::read_tag_quiet_with_prefix`] 静默内核（自带 TTL 门与磁盘候选
  /// 异步冷读闭环），「已过期对象键答 nil」采序与快臂折叠
  /// `read_adjudicated_user_sync_with_prefix` 天然同源（deviations §133，
  /// 过期先于判型，严禁回改判型先行）
  async fn object_kind_alive_with_prefix(&self, prefix: &[u8], key: &[u8]) -> wkv::Result<bool> {
    if self
      .read_tag_quiet_with_prefix(prefix, key, KeyTag::ObjectEnvelope, |_| ())
      .await?
      .is_some()
    {
      return Ok(true);
    }
    Ok(matches!(
      self
        .read_tag_quiet_with_prefix(prefix, key, KeyTag::Meta, meta_collection_type_of)
        .await?,
      Some(Some(_))
    ))
  }

  /// 批量读字符串键值并流式写出 RESP 应答（Scatter-Gather 批量冷读对标）
  ///
  /// 本函数是 C# MGET 异步批处理管线三件套在 rust 的合并承接：
  /// - libs/server/Resp/MGetReadArgBatch.cs:SetStatus（逐键 pending 态登记 +
  ///   ArrayPool 状态数组租用）：rust 无 Tsavorite pending IO 中间态，读经
  ///   单次 await 内联闭环，逐键状态数组不存在的，pending 记账按批一条经
  ///   [`Self::with_pending_metrics`] 承接；
  /// - libs/server/Resp/MGetReadArgBatch.cs:CompletePending（GET_CompletePending
  ///   强制收割 + 顺序补写应答）：rust 批量口单次 await 即收割完成，emit 闭包
  ///   顺序写等价承接；
  /// - libs/server/Resp/BasicCommands.cs:SetResult（输出数组惰性分配与倍增
  ///   累积）：rust 直接流式写 output，无中间累积数组。
  ///
  /// 附着一致读会话时走 [`wkv::ConsistentReadContext::read_batch_with`] 折叠重试口
  ///（pre_batch/post_batch 协议，读后校验不过整批重试，对标 C#
  /// ConsistentReadContext.ReadWithPrefetch）；否则直读底层批量口。
  /// 逐键命中/未命中经 [`Self::record_read_outcome`] 共享句柄入账（对位 C#
  /// 批量 GET 循环内空条件累加 `sessionMetrics?.incr_total_found/notfound`）。
  /// 整批异步闭环复用 [`Self::with_pending_metrics`] 单点漏斗起停 PENDING_LAT
  ///（对位 C# MainStore/AdvancedOps.cs 的 GET_CompletePending 两个重载在
  /// `CompletePendingWithOutputs` 前后成对起停表）：C# 批量收割是一次
  /// CompletePending 调用，rust 批量口同样单次 await，故样本按批一条、pending
  /// 计数按批一条，条目命中计数仍只由 record_read_outcome 单点入账不重复
  ///
  /// C# AdvancedOps 批量预读转发门（batch 一次性交 context 预取读）的对位：
  /// libs/server/Storage/Session/MainStore/AdvancedOps.cs:ReadWithPrefetch
  ///
  /// # 输出缓冲次序契约
  /// 流式直写语义下 `Err` 中止时 output 内残留首个磁盘候选之前已交付的部分帧
  ///（透传底层 wkv `session/raw/batch.rs:read_batch_raw_with` 的整体丢弃契约），
  /// 调用方必须回滚或清空本批 output 后再成帧，严禁在其后追加错误帧
  ///（MGET 会成 `*N` + 部分元素 + 错误帧的畸形数组，SG GET 会缺帧错位）
  pub async fn read_string_batch_into(
    &self,
    keys: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wkv::Result<()> {
    let mut emit = |_idx: usize, val_opt: Option<&[u8]>| {
      self.record_read_outcome(val_opt.is_some());
      match val_opt {
        Some(v) => output.write_resp_bulk_string(v),
        None => output.write_resp_null_ver(self.resp_version),
      }
    };
    match self.consistent_read_context() {
      Some(ctx) => {
        self
          .with_pending_metrics(|| ctx.read_batch_with(keys, &mut emit))
          .await
      }
      None => {
        self
          .with_pending_metrics(|| self.batch.read_batch_with(keys, &mut emit))
          .await
      }
    }
  }

  /// 批量读用户键并流式写出 GET 应答（String 域批量取值 + 缺失键三域判型续探）
  ///
  /// GET 慢臂（`garnet_api/slow/` C::Get，SG 流水线降级整批承接）专用漏斗，
  /// 对位快臂 `read_user_sync` 三域折叠的批量形态：底层批量读口保持 wkv 单域
  /// 探针不动（KeyTag::String 取值），String 域确认缺失的键经
  /// [`Self::object_kind_alive_with_prefix`]（与单键三域折叠
  /// [`Self::read_user_quiet_with_prefix`] 同一判型通道）续探信封 / Meta 域——
  /// 命中即集合对象键，nil 占位帧替换为 WRONGTYPE 错误帧（对标 C#
  /// NetworkGET / NetworkGET_SG pending 收割后同一 Reader 判型，快慢两通道
  /// 无第二形态，BasicCommands.cs:81 / :244）；真缺失键保持 nil 帧。MGET 语义
  /// 刻意不共享本口（Redis MGET 对非字符串键答 nil，双臂一致，仍走
  /// [`Self::read_string_batch_into`]）。
  ///
  /// # 入账
  /// Hit → found、真缺失 → notfound、WrongType 静默（`common::fold_outcome`
  /// 单规则，对位 C# MainStoreOps GET 的 WrongType 双臂均不计数）。批量口
  /// Err 中止时整批本地计数随部分帧一并丢弃零入账（与快臂 network_get_sg
  /// 的 Deferred 出口同形：整批回滚重放由本臂唯一收口，杜绝双计）。
  ///
  /// # 输出缓冲次序契约
  /// 与 [`Self::read_string_batch_into`] 一致：`Err` 中止时 output 残留首个
  /// 磁盘候选之前已交付的部分帧，调用方必须回滚或清空本批 output 后再成帧
  ///（N 键 N 帧序恒键序，nil 占位帧仅在批量读闭环后按原位替换，帧序零漂移）
  pub async fn read_user_batch_into(
    &self,
    keys: &[&[u8]],
    output: &mut Vec<u8>,
  ) -> wkv::Result<()> {
    // String 域缺失键的 nil 占位帧登记（键 idx、帧偏移、帧长；倒序替换时
    // 后续帧偏移不受扰）。Vec::new 零分配：全命中批（绝大多数）无登记
    let mut miss: Vec<(usize, usize, usize)> = Vec::new();
    let (mut found, mut notfound) = (0u64, 0u64);
    {
      let resp_ver = self.resp_version;
      let mut emit = |idx: usize, val_opt: Option<&[u8]>| {
        match val_opt {
          Some(v) => {
            found += 1;
            output.write_resp_bulk_string(v);
          }
          None => {
            // 缺失键入账延迟到判型续探后（WrongType 静默 / 真缺失 notfound）
            let off = output.len();
            output.write_resp_null_ver(resp_ver);
            miss.push((idx, off, output.len() - off));
          }
        }
      };
      match self.consistent_read_context() {
        Some(ctx) => {
          self
            .with_pending_metrics(|| ctx.read_batch_with(keys, &mut emit))
            .await?
        }
        None => {
          self
            .with_pending_metrics(|| self.batch.read_batch_with(keys, &mut emit))
            .await?
        }
      };
    }
    // 判型续探（磁盘候选在 read_tag_quiet 异步内核闭环，冷信封冷读装载）：
    // 倒序遍历保证 WRONGTYPE 替换帧时未处理帧的偏移恒准；全命中批零续探
    if !miss.is_empty() {
      let prefix = self.batch.session_prefix();
      let prefix_slice = prefix.as_slice();
      for &(idx, off, len) in miss.iter().rev() {
        if self
          .object_kind_alive_with_prefix(prefix_slice, keys[idx])
          .await?
        {
          let mut frame = Vec::with_capacity(RESP_ERR_WRONG_TYPE.len() + 3);
          frame.write_resp_error(RESP_ERR_WRONG_TYPE);
          output.splice(off..off + len, frame);
        } else {
          notfound += 1;
        }
      }
    }
    if let Some(metrics) = &self.session_metrics {
      metrics.incr_total_found(found);
      metrics.incr_total_notfound(notfound);
    }
    Ok(())
  }
}
