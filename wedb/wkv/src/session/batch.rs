//! 批处理会话上下文（严格对标 C# Garnet IUnsafeContext 与 UnsafeContext）
//!
//! 在处理网络流水线（Pipeline）批量命令时，外层仅进入并持有一次纪元保护，
//! 批处理期间的所有内存直读完全跳过原子 enter/exit，
//! 将纪元保护开销降至绝对零，极大释放多核高并发吞吐。

use std::{ops::Deref, result::Result as StdResult};

use smallvec::SmallVec;
use wdev::Device;
use wepoch::{EpochGuard, EpochSuspendGuard};
use wval::KeyTag;

use crate::{
  error::Result,
  session::{ConsistentReadContext, ConsistentReadFunctions, StoreResult, StoreSession},
  store::{ObjectRmwNotification, TieredCollectionNotification},
};

/// 批处理会话上下文（严格对标 C# Garnet IUnsafeContext 与 UnsafeContext）
///
/// 在处理网络流水线（Pipeline）批量命令时，外层仅进入并持有一次纪元保护，
/// 批处理期间的所有内存直读完全跳过原子 enter/exit，
/// 将纪元保护开销降至绝对零，极大释放多核高并发吞吐。
pub struct BatchStoreSession<'a, D: Device> {
  pub session: &'a StoreSession<D>,
  pub(super) _guard: EpochGuard<'a>,
}

impl<'a, D: Device> Deref for BatchStoreSession<'a, D> {
  type Target = StoreSession<D>;

  #[inline(always)]
  fn deref(&self) -> &Self::Target {
    self.session
  }
}

impl<'a, D: Device> BatchStoreSession<'a, D> {
  /// 挂起本批会话的纪元保护窗口（返回的守卫 Drop 时按原重入深度自动重入）
  ///
  /// 对标 C# Tsavorite 长 I/O 临界区的 epoch.UnsafeSuspendThread / ResumeThread
  /// 协议（libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs 的 OnPagesClosed），
  /// 与会话内前台驱逐窗口的挂起同一内核 wepoch 的 EpochSuspendGuard，全仓只此一处分发。
  ///
  /// C# 上下文层 UnsafeSuspendThread 入口族在本 rust 单点的折叠映射：
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/BasicContext.cs:UnsafeSuspendThread
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/ClientSession.cs:UnsafeSuspendThread
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/SessionFunctionsWrapper.cs:UnsafeSuspendThread
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalContext.cs:UnsafeSuspendThread
  ///
  /// 用途：批会话存活期内需要驱动「自带纪元排空屏障」的存储级动作（副本重放
  /// 检查点臂即其一）时，必须先解除自钉再动手——屏障谓词按全局最旧保护纪元
  /// 判定，批守卫不解除即把该谓词钉死为永假。调用点约定：只在两条记录之间、
  /// 上一次会话操作已完整落库（记录追加 + 索引插入闭环）处挂起，绝不在单条
  /// 操作中途挂起（那才是「页已刷、索引未插」的丢失更新窗口）。
  #[inline]
  pub fn suspend_epoch(&self) -> EpochSuspendGuard<'_> {
    EpochSuspendGuard::new(self.session.participant())
  }

  /// 批会话纪元让步（瞬时挂起窗：立即挂起并即刻按重入深度重入）
  ///
  /// [`Self::suspend_epoch`] 的零持有形态——守卫构造即弃：按当前重入深度逐层
  /// 退出保护区（会话槽位纪元位清 0，`compute_safe_to_reclaim` 扫描不再被本
  /// 会话钉死），随即按原深度重入（首层经 `enter_with_tid` 现场 CAS 公布
  /// **最新**全局纪元，补上重入臂不刷新公布纪元的缺口）。
  ///
  /// 用途：批守卫必须整轮在场（批内同步快路径依赖保护区前提）而批间存在天然
  /// 记录边界的长周期消费面——AOF 逐记录重放即其一：每条记录处理入口调用一次，
  /// 在两条记录之间（上一条已完整落库）给排空屏障一个确定性让步窗，对标 C#
  /// 重放会话逐记录常规 context 的 enter/exit（Tsavorite 重放不经 UnsafeContext
  /// 持整轮保护）。调用点约定与 [`Self::suspend_epoch`] 相同：只在两条记录
  /// 之间让步，绝不在单条操作中途。
  #[inline]
  pub fn epoch_yield(&self) {
    drop(self.suspend_epoch());
  }

  /// 创建一致读会话上下文（对标 libs/storage/Tsavorite/cs/src/core/ClientSession/ConsistentReadContext.cs）
  #[inline]
  pub fn consistent_read<'b, F: ConsistentReadFunctions + ?Sized>(
    &'b self,
    functions: &'b F,
  ) -> ConsistentReadContext<'b, D, F> {
    ConsistentReadContext::new(self.session, functions)
  }

  /// 纯同步快速路径写入当前会话普通字符串键（严格对标 C# UnsafeContext 的 SET 快路径）
  ///
  /// 语义与 [`StoreSession::try_upsert_sync`] 完全一致且零 enter() 原子开销：
  /// - `Ok(Ok(addr))`：纯内存写入成功（原位更新 / 复活 / 盲追加）；
  /// - `Ok(Err(page_id))`：环形缓冲区翻转（精确 page_id）或 TTL 清除需异步闭环
  ///   （`u64::MAX`），调用方须先 drop 本守卫再降级 `upsert().await`，随后可重回批处理。
  #[inline(always)]
  pub fn try_upsert_sync(&self, key: &[u8], val: &[u8]) -> Result<StdResult<u64, u64>> {
    self.session.try_upsert_sync_unprotected(key, val)
  }

  /// 纯同步快速路径写入当前会话指定标签物理键（零 enter() 原子开销）
  ///
  /// 语义与 [`StoreSession::try_upsert_tag_sync`] 完全一致，语义细节见
  /// [`StoreSession::try_upsert_tag_sync_unprotected`]
  #[inline(always)]
  pub fn try_upsert_tag_sync(
    &self,
    key: &[u8],
    tag: KeyTag,
    val: &[u8],
  ) -> Result<StdResult<u64, u64>> {
    self.session.try_upsert_tag_sync_unprotected(key, tag, val)
  }

  /// 对象信封单次成形快写（零 enter() 原子开销，批处理热路径专用）
  ///
  /// 语义与 [`StoreSession::try_upsert_envelope_sync_fill`] 完全一致（批处理
  /// 纪元已由本守卫持有，直呼 unprotected 内核），返回值语义与其文档一致
  #[inline(always)]
  pub fn try_upsert_envelope_sync_fill(
    &self,
    key: &[u8],
    obj_tag: u8,
    payload: &[u8],
  ) -> Result<StdResult<u64, u64>> {
    let rec_k = self.session.session_tag_key(KeyTag::ObjectEnvelope, key);
    self
      .session
      .try_upsert_envelope_sync_fill_with_prefix(key, &rec_k, obj_tag, payload)
  }

  /// 纯同步快速条件写入当前会话普通字符串键（NX 语义：仅当键不存在时原子写入）
  #[inline(always)]
  pub fn try_insert_sync(&self, key: &[u8], val: &[u8]) -> Result<StdResult<bool, u64>> {
    // 普通字符串即 String 标签特例，收口至同型带标签单源（对位 raw/write 各
    // 同步口的 String→tag 转发形态），杜绝前缀外提样板的第二套实现
    self.try_insert_tag_sync(key, KeyTag::String, val)
  }

  /// 纯同步快速条件写入当前会话指定标签物理键（NX 语义）
  #[inline(always)]
  pub fn try_insert_tag_sync(
    &self,
    key: &[u8],
    tag: KeyTag,
    val: &[u8],
  ) -> Result<StdResult<bool, u64>> {
    let prefix = self.session_prefix();
    self
      .session
      .try_insert_tag_sync_unprotected_with_prefix(prefix.as_slice(), key, tag, val)
  }

  /// 纯同步快速路径删除当前会话指定标签物理键（零 enter() 原子开销）
  #[inline(always)]
  pub fn try_delete_tag_sync(&self, key: &[u8], tag: KeyTag) -> Result<StdResult<bool, u64>> {
    self.session.try_delete_tag_sync_unprotected(key, tag)
  }

  /// 同步读当前会话普通字符串键快路径（TTL 快门控 + 内存直读，零 enter() 原子开销）
  ///
  /// 返回三态见 [`StoreResult`]：
  /// - `Success(r)`：内存命中，闭包零拷贝消费；
  /// - `NotFound`：内存中明确不存在（无候选 / 墓碑 / TTL 已到期）；
  /// - `RecordOnDisk`：须降级全异步 `read_with().await`（数据或 TTL 记录存在磁盘候选，
  ///   `check_expired` 含磁盘路径与物理清除，绝不跨纪元 await）。
  #[inline(always)]
  pub fn try_read_sync<R>(&self, key: &[u8], f: impl FnOnce(&[u8]) -> R) -> Result<StoreResult<R>> {
    self.session.try_read_sync_unprotected(key, f)
  }

  /// 同步读当前会话指定标签物理键快路径并披露记录物理尺寸（TTL 同栈门裁决 + 内存直读）
  ///
  /// MEMORY USAGE 统计内核：[`Self::try_read_tag_sync`] 的带尺寸对位，三态语义一致
  #[inline(always)]
  pub fn try_read_tag_sync_with_size<R>(
    &self,
    key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8], usize) -> R,
  ) -> Result<StoreResult<R>> {
    self.session.try_read_tag_sync_with_size(key, tag, f)
  }

  /// 同步读当前会话指定标签物理键快路径（TTL 同栈门裁决 + 内存直读，零 enter() 原子开销）
  ///
  /// 返回三态与 [`Self::try_read_sync`] 一致；TTL 门控按用户键同栈裁决
  /// （无 TTL / 未到期放行，已到期快路径 NOTFOUND），与数据记录标签无关
  #[inline(always)]
  pub fn try_read_tag_sync<R>(
    &self,
    key: &[u8],
    tag: KeyTag,
    f: impl FnOnce(&[u8]) -> R,
  ) -> Result<StoreResult<R>> {
    self.session.try_read_tag_sync_unprotected(key, tag, f)
  }

  /// 纯同步快速路径物理删除当前会话普通字符串键（零 enter() 原子开销）
  #[inline(always)]
  pub fn try_delete_sync(&self, key: &[u8]) -> Result<StdResult<bool, u64>> {
    self.session.try_delete_sync_unprotected(key)
  }

  /// 写入或更新键值对
  #[inline(always)]
  pub async fn upsert(&self, key: &[u8], val: &[u8]) -> Result<u64> {
    self.session.upsert(key, val).await
  }

  /// 读取键值对
  #[inline(always)]
  pub async fn read(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
    self.session.read(key).await
  }

  /// 删除键值对
  #[inline(always)]
  pub async fn delete(&self, key: &[u8]) -> Result<bool> {
    self.session.delete(key).await
  }

  /// 异步读取用户键的 TTL 并判定在指定 ticks 是否已过期（无 TTL 视同未过期）
  #[inline(always)]
  pub async fn is_expired_at(&self, user_key: &[u8], now: i64) -> Result<bool> {
    self.session.is_expired_at(user_key, now).await
  }

  /// 批量读取当前会话普通字符串记录（12 项流水线预取）
  #[inline(always)]
  pub async fn read_batch_with<K, F>(&self, keys: &[K], on_item: F) -> Result<()>
  where
    K: AsRef<[u8]>,
    F: FnMut(usize, Option<&[u8]>),
  {
    self.session.read_batch_with(keys, on_item).await
  }

  /// 纯内存批量直读当前会话普通字符串记录（零堆分配与零异步开销）
  #[inline(always)]
  pub fn try_read_batch_in_memory<K, F>(&self, keys: &[K], on_item: F) -> Result<()>
  where
    K: AsRef<[u8]>,
    F: FnMut(usize, Option<&[u8]>),
  {
    self.session.try_read_batch_in_memory(keys, on_item)
  }

  /// 触发对象 RMW 增量日志通知（AOF 入队失败沿写路径上抛拒绝该命令）
  #[inline(always)]
  pub fn notify_object_rmw(&self, notif: &ObjectRmwNotification<'_>) -> Result<()> {
    self
      .store
      .notify_object_rmw(self.session.aof_session_id, notif)
  }

  /// 触发分层稳态写命令镜像通知（AOF 入队失败沿写路径上抛拒绝该命令）
  #[inline(always)]
  pub fn notify_tiered_collection_write(
    &self,
    notif: &TieredCollectionNotification<'_>,
  ) -> Result<()> {
    self
      .store
      .notify_tiered_collection_write(self.session.aof_session_id, notif)
  }

  /// 触发对象信封整值写通知（AOF 入队失败沿写路径上抛拒绝该命令）
  #[inline(always)]
  pub fn notify_envelope_upsert(&self, key: &[u8], val: &[u8]) -> Result<()> {
    self
      .store
      .notify_envelope_upsert(self.session.aof_session_id, key, val)
  }

  /// 推进键的 WATCH 版本（批处理上下文的写面收口出口，供存储层
  /// TTL 同步快路径等旁路物理键原语的调用方按用户键显式推进，内部转发 bump_watch_version）
  #[inline(always)]
  pub fn bump_watch_version(&self, user_key: &[u8]) {
    self.session.bump_watch_version(user_key);
  }

  /// 纯同步快速路径物理删除当前会话普通字符串键的显式前缀变体（循环前缀外提
  /// 对位，语义与 [`Self::try_delete_sync`] 完全一致且零 enter() 原子开销；
  /// rust 工程优化无 c# 对应）
  #[inline(always)]
  pub fn try_delete_sync_with_prefix(
    &self,
    prefix: &[u8],
    key: &[u8],
  ) -> Result<StdResult<bool, u64>> {
    self
      .session
      .try_delete_sync_unprotected_with_prefix(prefix, key)
  }

  /// 批量同步快速路径写入当前会话普通字符串键（批量接口单次折叠，transpile
  /// SKILL 工程准则；rust 工程优化无 c# 对应）
  ///
  /// 单次折叠：纪元守卫复用本上下文外层持有（循环零 enter() 原子开销）、
  /// 会话前缀单次外提（循环零 ns/db 原子变量重读与 Varint 重算）、借用对
  /// 携带命令序下标排序（键序优先、同键按下标升序的全序，仅重排引用，零 KV
  /// 拷贝）后顺序写入——同键相邻、去重保末值恒为命令序末值
  /// （MSET 重复键后者胜语义，先例 ri_set_batch），批量集中命中相邻索引桶
  /// 压降探针缓存缺失。逐键 WATCH 推进与降级信号语义与逐键
  /// [`Self::try_upsert_sync`] 循环等价：任一键遇环形页翻转 / TTL 清退
  /// 异步闭环信号（`Err(page_id)`）立即整体返回，已写键保持（调用方降级
  /// 慢路径整命令重放幂等），剩余键不写
  ///
  /// 折叠先例对标 C# MainStoreOps.cs:MSET_Conditional（全键排他锁内批量
  /// SET）；rust 主存储为 compio 每核单线程 + epoch 无锁写，无条带锁可
  /// 分组，折叠收益为纪元/前缀/编码单次化与写入局部性（注释声明与条目
  /// 「按条带锁分组」的差异）
  ///
  /// 调用契约：批量盲写无地址复验，命令层调用方须以键组读改写窗口
  /// （`try_rmw_window_sorted`）覆盖全部键（快路径 network_mset 先例，
  /// 票 zcode-r32-rmwmatrix 立项一）
  #[inline]
  pub fn try_upsert_batch_sync<I, K, V>(&self, pairs: I) -> Result<StdResult<(), u64>>
  where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
  {
    let prefix = self.session.session_prefix();
    self.try_upsert_batch_sync_with_prefix(prefix.as_slice(), pairs)
  }

  /// 批量同步快速路径写入当前会话普通字符串键的显式前缀变体（支持复用外层已计算好的会话前缀）
  pub fn try_upsert_batch_sync_with_prefix<I, K, V>(
    &self,
    prefix_slice: &[u8],
    pairs: I,
  ) -> Result<StdResult<(), u64>>
  where
    I: IntoIterator<Item = (K, V)>,
    K: AsRef<[u8]>,
    V: AsRef<[u8]>,
  {
    // 栈上固定容量缓冲承接小批次排序（≤8 元素零堆分配，超限自动溢出至堆）；
    // enumerate 携带命令序下标，与借用对同为栈上三元组，零拷贝不变
    let mut sorted: SmallVec<[(K, V, usize); 8]> = pairs
      .into_iter()
      .enumerate()
      .map(|(i, (k, v))| (k, v, i))
      .collect();
    // 键序优先、同键按命令序下标升序，构成无相等元素的全序；sort_unstable_by
    // 在全序下结果确定，纯键序比较器不承诺相等键相对顺序的缺陷就此封堵
    sorted.sort_unstable_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()).then_with(|| a.2.cmp(&b.2)));
    let mut iter = sorted.into_iter().peekable();
    while let Some((k, v, _)) = iter.next() {
      // 相邻去重保末值：全序排序后同键末位恒为命令序末值（MSET 后者胜语义，
      // 对标 C# ArrayCommands 的 NetworkMSET 与 MainStoreOps 的 SET 条件写折叠
      // 按命令序逐对 SET）
      if iter
        .peek()
        .is_some_and(|(next_key, ..)| next_key.as_ref() == k.as_ref())
      {
        continue;
      }
      match self.session.try_upsert_tag_sync_unprotected_with_prefix(
        prefix_slice,
        k.as_ref(),
        KeyTag::String,
        v.as_ref(),
      )? {
        Ok(_) => {}
        // 环形页翻转 / TTL 清退异步闭环：立即整体降级（page_id 为首个触发键）
        Err(page_id) => return Ok(Err(page_id)),
      }
    }
    Ok(Ok(()))
  }
}
