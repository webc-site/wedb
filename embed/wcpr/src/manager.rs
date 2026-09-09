use std::{
  fs::{read_dir, remove_dir_all, remove_file},
  future::Future,
  marker::PhantomData,
  path::Path,
  str,
  sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
  time::Duration,
};

use compio::{
  fs::{File, create_dir_all, metadata, read, rename},
  io::AsyncWriteAtExt,
  time::sleep,
};
use log::{info, warn};
use wbase::time::{now_ms, now_nanos};
use wdev::Device;
use wepoch::LightEpoch;
use whlog::{HybridLog, HybridLogConfig};
use windex::HashIndex;

use super::{
  error::{Error, Result},
  index_ckpt::{read_index_checkpoint_truncated, write_index_checkpoint},
  meta::{
    CheckpointMeta, CheckpointType, FORMAT_VERSION, HlogMeta, INDEX_EXT, INTEGRITY_FROM_VERSION,
    IndexMeta, META_EXT, META_PREFIX, StoreMeta, TMP_EXT, index_filename, index_tmp_filename,
    meta_filename, meta_tmp_filename,
  },
};

/// 恢复出的检查点核心组件集合（解耦具体的 WedbStore 组装）
pub struct RecoveredCheckpoint<D: Device> {
  pub meta: CheckpointMeta,
  pub index: Arc<HashIndex>,
  pub hlog: Arc<HybridLog<D>>,
  pub epoch: Arc<LightEpoch>,
}

/// 检查点宿主存储引擎状态读取与快照契约（对标 C# Tsavorite Checkpoint API）
pub trait CprStore {
  /// 底层存储设备类型
  type Device: Device;

  /// 混合日志分配器引用
  fn hlog(&self) -> &HybridLog<Self::Device>;

  /// 无锁哈希索引引用
  fn index(&self) -> &HashIndex;

  /// 全局纪元系统引用
  fn epoch(&self) -> &LightEpoch;

  /// 当前日志尾部逻辑地址
  fn tail_address(&self) -> u64;

  /// 有效起始逻辑地址
  fn begin_address(&self) -> u64;

  /// 头部有效逻辑地址
  fn head_address(&self) -> u64;

  /// 推进只读边界
  fn shift_read_only_address(&self, target: u64);

  /// 全量刷写脏页至设备
  fn flush_all(&self) -> impl Future<Output = Result<()>>;

  /// 估算哈希索引当前条目数
  fn entry_count(&self) -> usize;

  /// 顺链跳过 ReadCache 取得真实主日志逻辑地址
  fn skip_read_cache(&self, addr: u64) -> u64;

  /// 执行 RangeIndex 快照
  fn take_range_index_checkpoints(&self, dir: &Path, token: u128) -> Result<usize>;

  /// 执行共享 BfTree 快照
  fn take_bftree_checkpoint(&self, dir: &Path, token: u128) -> Result<usize>;

  /// 生成当前存储配置元数据
  fn checkpoint_store_meta(&self) -> StoreMeta;
}

/// 检查点崩溃恢复重构契约接口
pub trait CprRecover: CprStore {
  /// 从恢复的核心组件重构完整的宿主存储引擎实例
  fn from_recovered(
    recovered: RecoveredCheckpoint<Self::Device>,
    checkpoint_dir: &Path,
    device: Arc<Self::Device>,
  ) -> impl Future<Output = Result<Self>>
  where
    Self: Sized;
}

impl<S: CprStore> CprStore for Arc<S> {
  type Device = S::Device;

  #[inline]
  fn hlog(&self) -> &HybridLog<Self::Device> {
    (**self).hlog()
  }

  #[inline]
  fn index(&self) -> &HashIndex {
    (**self).index()
  }

  #[inline]
  fn epoch(&self) -> &LightEpoch {
    (**self).epoch()
  }

  #[inline]
  fn tail_address(&self) -> u64 {
    (**self).tail_address()
  }

  #[inline]
  fn begin_address(&self) -> u64 {
    (**self).begin_address()
  }

  #[inline]
  fn head_address(&self) -> u64 {
    (**self).head_address()
  }

  #[inline]
  fn shift_read_only_address(&self, target: u64) {
    (**self).shift_read_only_address(target)
  }

  #[inline]
  fn flush_all(&self) -> impl Future<Output = Result<()>> {
    (**self).flush_all()
  }

  #[inline]
  fn entry_count(&self) -> usize {
    (**self).entry_count()
  }

  #[inline]
  fn skip_read_cache(&self, addr: u64) -> u64 {
    (**self).skip_read_cache(addr)
  }

  #[inline]
  fn take_range_index_checkpoints(&self, dir: &Path, token: u128) -> Result<usize> {
    (**self).take_range_index_checkpoints(dir, token)
  }

  #[inline]
  fn take_bftree_checkpoint(&self, dir: &Path, token: u128) -> Result<usize> {
    (**self).take_bftree_checkpoint(dir, token)
  }

  #[inline]
  fn checkpoint_store_meta(&self) -> StoreMeta {
    (**self).checkpoint_store_meta()
  }
}

impl<S: CprStore> CprStore for &S {
  type Device = S::Device;

  #[inline]
  fn hlog(&self) -> &HybridLog<Self::Device> {
    (**self).hlog()
  }

  #[inline]
  fn index(&self) -> &HashIndex {
    (**self).index()
  }

  #[inline]
  fn epoch(&self) -> &LightEpoch {
    (**self).epoch()
  }

  #[inline]
  fn tail_address(&self) -> u64 {
    (**self).tail_address()
  }

  #[inline]
  fn begin_address(&self) -> u64 {
    (**self).begin_address()
  }

  #[inline]
  fn head_address(&self) -> u64 {
    (**self).head_address()
  }

  #[inline]
  fn shift_read_only_address(&self, target: u64) {
    (**self).shift_read_only_address(target)
  }

  #[inline]
  fn flush_all(&self) -> impl Future<Output = Result<()>> {
    (**self).flush_all()
  }

  #[inline]
  fn entry_count(&self) -> usize {
    (**self).entry_count()
  }

  #[inline]
  fn skip_read_cache(&self, addr: u64) -> u64 {
    (**self).skip_read_cache(addr)
  }

  #[inline]
  fn take_range_index_checkpoints(&self, dir: &Path, token: u128) -> Result<usize> {
    (**self).take_range_index_checkpoints(dir, token)
  }

  #[inline]
  fn take_bftree_checkpoint(&self, dir: &Path, token: u128) -> Result<usize> {
    (**self).take_bftree_checkpoint(dir, token)
  }

  #[inline]
  fn checkpoint_store_meta(&self) -> StoreMeta {
    (**self).checkpoint_store_meta()
  }
}

/// 异步生成并原子落盘 HashIndex 快照（纯 compio 异步 I/O，全程零线程创建）
///
/// io_uring 下真正的磁盘 I/O 由内核完成，reactor 线程仅提交请求与收割完成事件，不会阻塞；
/// 序列化与 CRC32 累积属微秒级纯计算，按 thread-per-core 模型留在 reactor 线程执行。
///
/// `rc_skip` 为 ReadCache 易失指针解析闭包（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:SkipReadCacheBucket，见
/// [`crate::write_index_checkpoint`]）；无 ReadCache 时传恒等闭包 `|addr| addr`。
pub async fn take_index_checkpoint(
  index: &HashIndex,
  entry_count: usize,
  checkpoint_dir: impl AsRef<Path>,
  token: u128,
  rc_skip: impl Fn(u64) -> u64,
) -> Result<IndexMeta> {
  write_index_checkpoint(index, entry_count, checkpoint_dir, token, &rc_skip).await
}

/// 用于生成单调唯一 Token 的原子自增序列号
static TOKEN_SEQ: AtomicU64 = AtomicU64::new(1);

/// 进程内最近一次签发的 Token（墙钟回拨单调守卫）
static LAST_TOKEN: Mutex<Option<u128>> = Mutex::new(None);

/// 由墙钟与序列号合成候选 Token：高 64 位墙钟纳秒，低 64 位进程内自增序列号
#[inline]
fn candidate_token() -> u128 {
  let now = now_nanos() as u128;
  let seq = TOKEN_SEQ.fetch_add(1, Ordering::Relaxed) as u128;
  (now << 64) | seq
}

/// Token 签发闸门：候选值低于进程历史签发最大值（墙钟回拨）时以历史最大值 +1
/// 续发，且强制严格大于调用方给定的下界 `floor`（检查点目录内现存最大 Token，
/// 覆盖跨进程重启后的时钟回拨），签发值全局严格单调递增、永不重复。
fn issue_token_after(candidate: u128, floor: u128) -> u128 {
  // 中毒锁直接取回内部数据：守卫自身绝不向检查点路径传播 panic
  let mut last = LAST_TOKEN.lock().unwrap_or_else(|e| e.into_inner());
  let mut token = match *last {
    Some(issued) if candidate <= issued => issued.checked_add(1).unwrap_or(candidate),
    _ => candidate,
  };
  if token <= floor {
    // floor == u128::MAX 为理论边界（checked_add 饱和回退），实际不可达
    token = floor.checked_add(1).unwrap_or(token);
  }
  *last = Some(token);
  token
}

/// 检查点创建进程级串行闸门：SAVE 手动触发与周期快照可能并发进入，两次检查点在
/// 同一 WedbStore 上叠加变更（只读线封印、全库 flush、CPR 短屏障、复活池清理），
/// 必须串行执行；文件集层面各 Token 互不相交本就免锁，闸门仅裁断存储引擎状态机的
/// 并发叠加。
static CKPT_GATE: AtomicBool = AtomicBool::new(false);

/// 获取检查点闸门：TAS 自旋 + 让渡轮询，等待者在自身 reactor 上让出而非阻塞线程
/// （与纪元排空屏障同一轮询风格）
async fn lock_ckpt_gate() -> CkptGate {
  while CKPT_GATE.swap(true, Ordering::AcqRel) {
    sleep(Duration::from_micros(FENCE_POLL_INTERVAL_US)).await;
  }
  CkptGate
}

/// 闸门 RAII 守卫：Drop 释放，错误与 panic 路径均不漏放
struct CkptGate;

impl Drop for CkptGate {
  fn drop(&mut self) {
    CKPT_GATE.store(false, Ordering::Release);
  }
}

/// 纪元排空屏障的异步轮询间隔（微秒）
const FENCE_POLL_INTERVAL_US: u64 = 200;

/// 构造统一格式的恢复地址校验错误：`name (0xval) cond (0xbound) note`
#[inline]
fn addr_violation(name: &str, val: u64, cond: &str, bound: u64, note: &str) -> Error {
  use core::fmt::Write;
  let mut s = String::with_capacity(96);
  let _ = write!(s, "{name} ({val:#x}) {cond} ({bound:#x}){note}");
  Error::InvalidRecoveryAddress(s)
}

/// 构造索引快照与引擎配置/元数据不一致的统一错误：`desc: actual vs expected`（十进制）
#[inline]
fn index_mismatch(desc: &str, actual: u64, expected: u64) -> Error {
  let mut s = String::with_capacity(desc.len() + 40);
  s.push_str(desc);
  s.push_str(": ");
  let mut buf = itoa::Buffer::new();
  s.push_str(buf.format(actual));
  s.push_str(" vs ");
  s.push_str(buf.format(expected));
  Error::InvalidIndexCkpt(s)
}

/// 生成全局唯一的 128 位快照 Token（对标 Garnet Guid）
///
/// 布局：高 64 位为墙钟纳秒，低 64 位为进程内原子自增序列号。`recover_latest`/
/// `list_checkpoints` 依赖「目录内 Token 的大小序即版本新旧序」，墙钟回拨（NTP
/// 步进/手动校时）由两层守卫兜底，保证签发值严格单调递增：
/// - 进程级：[`issue_token_after`] 以历史签发最大值为下界 +1 续发；
/// - 跨进程重启：[`CheckpointManager::create_checkpoint`] 将目录内现存最大 Token
///   作为签发下界，重启后回拨同样无法颠倒版本序。
pub fn next_token() -> u128 {
  issue_token_after(candidate_token(), 0)
}

/// 文件数据 fsync
///
/// Windows 的 `FlushFileBuffers` 要求写权限句柄（读句柄报拒绝访问），故写模式
/// 打开（不截断不动内容）；POSIX 读句柄即可 fsync 数据
async fn sync_file_data(path: &Path) -> Result<()> {
  #[cfg(windows)]
  let file = compio::fs::OpenOptions::new()
    .write(true)
    .open(path)
    .await?;
  #[cfg(not(windows))]
  let file = File::open(path).await?;
  file.sync_all().await?;
  Ok(())
}

/// 异步刷写检查点目录项，保证 rename 的原子替换在掉电后依然持久
///
/// POSIX 崩溃一致性语义下，数据文件自身 `sync_all` 并不覆盖目录项变更：
/// `rename` 必须辅以父目录 fsync 才能保证掉电后目录视图中出现的是新文件名
/// （而非半截 `.tmp` 残留）。此为「元数据最后落盘」闭环的最后一环。
///
/// 仅 POSIX 提供 `fsync(dirfd)` 原语；Windows 无等价机制（`FlushFileBuffers` 对
/// 目录句柄拒绝访问，NTFS 目录项随卷元数据自动持久），故非 Unix 平台跳过
async fn sync_dir_handle(dir: &Path) -> Result<()> {
  #[cfg(unix)]
  {
    File::open(dir).await?.sync_all().await?;
  }
  #[cfg(not(unix))]
  {
    let _ = dir;
  }
  Ok(())
}

pub(crate) async fn sync_checkpoint_dir(dir: &Path) -> Result<()> {
  sync_dir_handle(dir).await
}

/// 异步自底向上递归刷写目录树，令 `dir` 下全部文件数据与各级目录项掉电持久
///
/// RangeIndex CPR 快照位于 `<token>/rangeindex/` 子目录树：活跃树由 BfTree 引擎
/// 自行 fsync 文件数据，冷树分支与目录创建（`create_dir_all`）均无任何持久化
/// 保证——若仅 fsync 文件而不逐级 fsync 目录项，掉电后 meta 已见而子目录链仍可能
/// 整体消失。作为「元数据最后落盘」提交协议的一环，发布 `checkpoint_<token>.meta`
/// 之前必须先令 token 目录树整体持久，确保 meta 可见即快照全量可见。
///
/// 目录枚举使用 std `read_dir`：compio-fs 0.12.1 未提供异步目录遍历原语，而
/// getdents 属纯元数据 syscall（无数据面 I/O），与 [`CheckpointManager::purge_all`]
/// 等目录维护路径一致保持同步实现；文件数据 fsync 走 compio 异步定位 I/O。
/// 递归经 `Box::pin` 装箱，深度受快照子目录结构约束（个位数层级）。
pub(crate) async fn sync_dir_tree(dir: &Path) -> Result<()> {
  for entry in read_dir(dir)? {
    let entry = entry?;
    if entry.file_type()?.is_dir() {
      Box::pin(sync_dir_tree(&entry.path())).await?;
    } else {
      sync_file_data(&entry.path()).await?;
    }
  }
  sync_dir_handle(dir).await
}

/// 尽力删除单个残留路径：目录形态（如被误建/注入的同名 `.tmp` 目录）整体递归删除，
/// 文件形态直接 unlink，不存在则静默跳过
fn rm_path_best_effort(path: &Path) {
  if path.is_dir() {
    let _ = remove_dir_all(path);
  } else {
    let _ = remove_file(path);
  }
}

/// 异步快照与崩溃恢复管理器（对标 Microsoft Garnet GarnetCheckpointManager）
#[derive(Debug)]
pub struct CheckpointManager<D: Device = wdev::SegmentedDevice> {
  _marker: PhantomData<D>,
}

impl<D: Device> Default for CheckpointManager<D> {
  fn default() -> Self {
    Self {
      _marker: PhantomData,
    }
  }
}

impl<D: Device> CheckpointManager<D> {
  /// 创建 CheckpointManager 实例
  pub const fn new() -> Self {
    Self {
      _marker: PhantomData,
    }
  }

  /// 创建指定设备类型的 CheckpointManager 实例
  pub const fn with_device() -> Self {
    Self::new()
  }

  /// 异步生成并原子落盘 HashIndex 快照（纯 compio 异步 I/O，全程零线程创建）
  ///
  /// `rc_skip` 为 ReadCache 易失指针解析闭包（无 ReadCache 时传恒等闭包 `|addr| addr`）。
  pub async fn take_index_checkpoint(
    &self,
    index: &HashIndex,
    entry_count: usize,
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
    rc_skip: impl Fn(u64) -> u64,
  ) -> Result<IndexMeta> {
    take_index_checkpoint(index, entry_count, checkpoint_dir, token, rc_skip).await
  }

  /// 创建持久化 Checkpoint 快照
  ///
  /// 核心流程（对标 C# FullCheckpointSM 状态机 REST → PREPARE → WAIT_FLUSH →
  /// PERSISTENCE_CALLBACK → REST 的闭环语义）：
  /// 1. 生成全局唯一 128 位快照版本 Token。
  /// 2. PREPARE：捕获一致性截断点 TailAddress。
  /// 3. 封印只读边界：将 `[0, tail)` 冻结为只读（ShiftReadOnlyToTail），后续更新一律 RCU 追加。
  /// 4. Epoch 排空屏障：等待全部前置纪元在途操作完成，索引与日志对截断点趋于静稳。
  /// 5. WAIT_FLUSH：刷写全部脏页并同步设备，随后为所有 RangeIndex 执行 CPR 快照，
  ///    并原子刷写 HashIndex 快照 `index_<token>.ckpt`。
  /// 6. PERSISTENCE_CALLBACK：写入 `checkpoint_<token>.meta` 元数据文件（最后落盘，
  ///    崩溃时未完成检查点绝不会出现在恢复视图中）。
  ///
  /// # 调用方契约
  /// - 必须在纪元保护区外调用（会话操作作用域之外）：调用方自持纪元会使排空屏障
  ///   永远无法完成，本方法以 [`Error::CheckpointWhileEpochProtected`] fail-fast
  ///   拒绝（对照 C#：Garnet CHECKPOINT 命令与周期紧缩任务均在会话作用域外串行
  ///   驱动检查点，契约靠外部调用约定成立；wedb 将其显式化为类型化错误）。
  /// - 单线程测试等场景天然满足：会话操作守卫均为 RAII 作用域，操作返回即退出保护区。
  ///
  /// # 崩溃一致性与并发语义
  /// - Token 签发的目录下界（floor）在进程级闸门内计算：并发持有闸门的
  ///   [`Self::create_checkpoint_with_token`] 可能正以调用方指定的大 Token 发布，
  ///   闸门外枚举目录会读到陈旧视图，签发值可能小于已发布 Token，颠倒
  ///   「目录内 Token 的大小序即版本新旧序」不变式（recover_latest/purge_outdated
  ///   依赖此不变式）；闸门内计算覆盖跨进程重启后的墙钟回拨，签发闸门全局串行，
  ///   并发创建亦不撞号。
  /// - 快照一致性点与前台写入可见性边界：检查点严格限定在 PREPARE 捕获的 tail 时点。
  ///   屏障放行后到 flush_all 结束期间，新起会话仅能对 `[tail, ∞)` 做 RCU 追加（只读区
  ///   已封印，`[0, tail)` 的字节绝无在途改写）；这些超界追加可能随本次 flush_all 一并
  ///   落盘并使 FlushedUntilAddress 超前 tail，恢复阶段按截断点 tail 丢弃全部超界索引
  ///   条目并钳制 FlushedUntilAddress——tail 之后落盘的写入对本检查点不可见。
  /// - 持久化闭环：`index_<token>.ckpt` 与 `checkpoint_<token>.meta` 均经 tmp 写入 +
  ///   fsync + 原子 rename，并随后 fsync 父目录保证 rename 目录项掉电持久；
  ///   RangeIndex 快照子目录树（含冷树 `fs::copy` 文件与中间目录项）在 meta 发布前
  ///   被整体递归 fsync——meta 可见即该 Token 文件集全量可见。
  /// - 并发检查点：进程级闸门（[`lock_ckpt_gate`]）将 SAVE 手动触发与周期快照等并发
  ///   调用串行化，杜绝两次检查点在宿主状态机上的并发叠加；各 Token 的文件集
  ///   互不相交，`recover` 仅读取某一 Token 的不可变文件集（原子 rename 发布），与并发
  ///   的创建/清理操作互不撕裂。Token 由签发闸门全局唯一，对同一 Token 并发/重复调用
  ///   [`Self::create_checkpoint_with_token`] 属调用方违约（失败清场与临时文件路径均按
  ///   Token 独占假设执行），须保证 Token 唯一。
  pub async fn create_checkpoint<S: CprStore<Device = D>>(
    &self,
    store: &S,
    checkpoint_dir: impl AsRef<Path>,
    cp_type: CheckpointType,
  ) -> Result<CheckpointMeta> {
    let dir = checkpoint_dir.as_ref();
    let _gate = lock_ckpt_gate().await;
    // 跨进程墙钟回拨防御：目录内 Token 的大小序即版本新旧序（recover_latest/
    // purge_outdated 依赖此不变式）。目录现存最大 Token 作为签发下界，重启后 NTP
    // 步进回拨同样无法使新版本倒序（避免恢复到陈旧检查点、误清最新检查点）；
    // floor 必须在闸门内计算——并发持有闸门的 create_checkpoint_with_token 可能
    // 正在发布更大 Token 的检查点，闸门外取 floor 会读到陈旧目录视图（论证见
    // 本方法文档「崩溃一致性与并发语义」首条）。
    let floor = Self::list_checkpoints(dir)?.last().copied().unwrap_or(0);
    let token = issue_token_after(candidate_token(), floor);
    Self::create_gated(store, dir, cp_type, token).await
  }

  /// 使用指定 Token 创建持久化 Checkpoint 快照
  ///
  /// 进程级串行化（见 [`lock_ckpt_gate`]）：SAVE 手动触发与周期快照并发调用时依次
  /// 排队执行，杜绝两次检查点在宿主状态机上的并发叠加；等待者以让渡轮询排队，
  /// 不阻塞任何 reactor 线程。
  ///
  /// 失败清场：任一阶段失败（如磁盘满 ENOSPC、注入故障）时 best-effort 回收本
  /// Token 的全部物理文件（含半截 `.tmp` 与孤儿 token 子目录）——元数据最后落盘
  /// 保证失败检查点绝无进入恢复视图的可能，清场仅回收磁盘空间。注意该 Token 若
  /// 恰有历史文件集亦会被一并回收，对同一 Token 的重复/复用调用属调用方违约。
  pub async fn create_checkpoint_with_token<S: CprStore<Device = D>>(
    &self,
    store: &S,
    checkpoint_dir: impl AsRef<Path>,
    cp_type: CheckpointType,
    token: u128,
  ) -> Result<CheckpointMeta> {
    let dir = checkpoint_dir.as_ref();
    let _gate = lock_ckpt_gate().await;
    Self::create_gated(store, dir, cp_type, token).await
  }

  /// 闸门内创建主流程（调用方须已持有进程级闸门）：创建 + 失败清场
  async fn create_gated<S: CprStore<Device = D>>(
    store: &S,
    dir: &Path,
    cp_type: CheckpointType,
    token: u128,
  ) -> Result<CheckpointMeta> {
    let res = Self::create_checkpoint_inner(store, dir, cp_type, token).await;
    if let Err(e) = &res {
      warn!("Checkpoint 创建失败，已回收本 Token 残留文件: token={token:#x}, err={e}");
      let _ = Self::purge_checkpoint(dir, token);
    }
    res
  }

  /// 检查点创建主流程（调用方须已持有闸门，见 [`Self::create_checkpoint_with_token`]）
  async fn create_checkpoint_inner<S: CprStore<Device = D>>(
    store: &S,
    dir: &Path,
    cp_type: CheckpointType,
    token: u128,
  ) -> Result<CheckpointMeta> {
    // 0. 调用方契约前置校验：处于纪元保护区时 fail-fast 拒绝（零副作用——位于建目录、
    //    封印只读边界等一切状态变更之前）。C# Tsavorite/Garnet 的检查点由外部串行驱动
    //    （CHECKPOINT 命令、StoreWrapper.CompactionTaskAsync 均在会话作用域外调用），
    //    「不在保护区内发起」仅靠调用约定成立；wedb 将其升级为类型化错误：若在保护
    //    区内跳过排空屏障继续创建，「数据页已刷盘但索引插入尚未提交」的丢失更新窗口
    //    将静默打开（静稳性缺口），fail-fast 把缺口暴露为可操作的显式错误而非脏快照。
    //    单线程测试等合法场景天然满足：会话操作守卫均为 RAII 作用域，返回即退保护。
    if store.epoch().this_instance_protected() {
      return Err(Error::CheckpointWhileEpochProtected);
    }

    create_dir_all(dir).await?;

    // 注：不持有跨 await 的共享 BfTree 外层屏障（对标差异，刻意为之）。
    // Garnet 的 SetCheckpointBarrier 从 VersionShift 起阻塞树写入直至快照完成，
    // 但本流程在 tail 捕获与 CPR 快照之间存在多个 await（epoch 排空、flush_all、
    // index 快照异步 I/O）；BfTreeService 的写者等待是同步忙自旋、不让出 executor，
    // 若屏障跨 await 持有，同一 compio worker 线程上的树写任务将永久自旋霸占
    // 线程，checkpoint 的 I/O 事件永远无法收割，形成死锁。
    // 快照互斥因此仅由 cpr_snapshot 内部的同步短屏障承担：快照自身绝无撕裂；
    // tail 捕获到快照之间落进的树写入会包含进快照（恢复态可能超前于 hlog tail，
    // 表现为「多存不丢」，符合尽力持久化语义）。未配置持久工作文件时跳过快照，
    // 非磁盘后端则由 take_bftree_checkpoint 显式报错。

    // 1. PREPARE：捕获一致性截断点（对标 C# PREPARE 阶段的 startLogicalAddress 捕获）
    let tail = store.tail_address();

    // 2. 封印只读边界（对标 C# FoldOver WAIT_FLUSH 的 ShiftReadOnlyToTail）：
    //    这是崩溃一致性的核心前置条件——先封印后刷盘，保证刷盘期间没有任何在途
    //    写入可以原位改写 `[0, tail)` 内已被捕获的字节，彻底消除检查点撕裂窗口
    store.shift_read_only_address(tail);

    // 3. Epoch 排空屏障（对标 C# 状态机 TrackLastVersion + WAIT_FLUSH 排空语义）：
    //    此时调用方必不在纪元保护区（步骤 0 契约校验已拒绝保护区内调用），推进全局
    //    纪元并异步等待所有前置纪元的在途操作完成，确保索引对 `[0, tail)`
    //    区间趋于静稳，杜绝「数据页已刷盘但索引插入尚未提交」的丢失更新窗口
    let fence_epoch = store.epoch().current_epoch();
    store.epoch().bump_epoch();
    while store.hlog().safe_read_only_address() < tail
      || !store.epoch().is_safe_to_reclaim(fence_epoch)
    {
      store.epoch().drain();
      sleep(Duration::from_micros(FENCE_POLL_INTERVAL_US)).await;
    }
    // 收割 SafeReadOnlyAddress 推进等全部就绪的延迟动作
    store.epoch().drain();

    // 4. WAIT_FLUSH：刷写 HybridLog 所有未落盘内存脏页至存储介质并同步设备
    store.flush_all().await?;

    // 5. 同步遍历 store 为所有 RangeIndex 执行 CPR 快照
    //    (1:1 对标 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotAllTreesForCheckpoint)
    let _ri_count = store.take_range_index_checkpoints(dir, token)?;

    // 5.0.1 共享 BfTree CPR 快照：Flattened ZSet 成员与 ACL/集群元数据的持久化闭环
    //    (对标 Garnet 共享树快照；未配置 bftree_path 时内部跳过)
    let _bftree_count = store.take_bftree_checkpoint(dir, token)?;

    // 5.1 持久化 token 子目录树：RangeIndex 快照必须先于 meta 达到掉电持久
    //    （纯 compio 异步 fsync，全程零线程创建）
    let mut token_buf = itoa::Buffer::new();
    let token_dir = dir.join(token_buf.format(token));
    if metadata(&token_dir).await.is_ok() {
      sync_dir_tree(&token_dir).await?;
    }

    // 6. 原子刷写 Index Checkpoint（纯 compio 异步定位 I/O：io_uring 下磁盘 I/O 由内核
    //    完成，reactor 仅提交/收割完成事件，大索引刷盘不再需要线程池中转）。
    //    传入 ReadCache 解析闭包：指向易失读缓存的索引条目在快照前必须顺链回写为主日志
    //    真实地址（对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:SkipReadCacheBucket），否则恢复后这些键将永久不可见
    let index_meta =
      take_index_checkpoint(store.index(), store.entry_count(), dir, token, |addr| {
        store.skip_read_cache(addr)
      })
      .await?;

    let hlog_meta = HlogMeta {
      begin_address: store.begin_address(),
      head_address: store.head_address(),
      flushed_until_address: store.hlog().flushed_until_address(),
      tail_address: tail,
    };

    let store_meta = store.checkpoint_store_meta();

    let mut meta = CheckpointMeta {
      token,
      cp_type,
      index_meta,
      hlog_meta,
      store_meta,
      created_at: now_ms(),
      format_version: FORMAT_VERSION,
      integrity_crc32: 0,
    };
    // 发布前封签：封签之外仅封签字段自身例外，恢复时逐字段比对拦截落盘后篡改
    meta.seal();

    let json_bytes = meta.encode_json()?;
    let tmp_meta_path = dir.join(meta_tmp_filename(token));
    let final_meta_path = dir.join(meta_filename(token));

    let mut file = File::create(&tmp_meta_path).await?;
    file.write_all_at(json_bytes, 0).await.0?;
    file.sync_all().await?;

    rename(&tmp_meta_path, &final_meta_path).await?;
    sync_checkpoint_dir(dir).await?;

    info!(
      "成功创建 Checkpoint: token={token:#x}, type={cp_type:?}, entry_count={}, tail={tail:#x}",
      index_meta.entry_count
    );

    Ok(meta)
  }

  /// 从指定 Checkpoint 进行崩溃恢复，重构并实例化底层核心组件集合
  ///
  /// 恢复流程：
  /// 1. 读取并反序列化 `checkpoint_<token>.meta` 元数据文件。
  /// 2. 从 `index_<token>.ckpt` 二进制快照无损重建 64B Cacheline 对齐的 HashIndex。
  /// 3. 重建 LightEpoch 与底层 HybridLog 实例。
  /// 4. 从底层 Device 预热读取尾部活跃页数据至内存页缓冲，恢复地址状态机（Head、Tail、ReadOnly 等）。
  ///
  /// # 数据链信任边界（与 waof 自同步扫描的刻意差异，与 C# 一致）
  ///
  /// 恢复对 `[flushed, tail)` 日志数据链不做逐记录 CRC 重放校验：该区间的完整性完全
  /// 信任创建时 `flush_all` + device sync 的时序闭环（脏页按序刷盘、元数据最后落盘），
  /// 这与 C# Tsavorite 恢复路径一致——C# 亦不重放主日志 CRC，仅校验快照文件自身校验和。
  /// 介质损坏（位翻转、半截写入）不在恢复期拦截，而在读路径暴露：数据记录自带 CRC，
  /// 读取时校验失败即显式报错。waof 则选择恢复期全量自同步扫描前摄暴露损坏，两者是
  /// 面向不同 RTO/数据量权衡的刻意分化，非实现遗漏。
  pub async fn recover_checkpoint_components(
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
    device: Arc<D>,
  ) -> Result<RecoveredCheckpoint<D>> {
    let dir = checkpoint_dir.as_ref();
    let meta_path = dir.join(meta_filename(token));
    if metadata(&meta_path).await.is_err() {
      return Err(Error::MetaNotFound(meta_path));
    }

    // 1. 读取元数据文件（支持 bitcode 与 JSON 自动适配）
    // 对标 libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/DeviceLogCommitCheckpointManager.cs:ThrowIfInvalidMetadataSize
    // 的「损坏元数据具名拒绝」语义：C# 元数据为设备日志上的长度前缀流（读首部
    // int 长度，<= 0 或超 64MB 上限即抛 TsavoriteException，把截断/损坏文件变成
    // 具名错误而非巨型分配）；本实现元数据经 `read` 整文件读入后按自描述
    // JSON/bitcode 整体反序列化，无长度前缀分配路径，截断在 decode 与下方
    // 版本/Token/完整性封签逐项校验中显式报错，等价达成「损坏元数据绝不静默
    // 恢复、绝不引发失控分配」的防护目标。
    let meta_bytes = read(&meta_path).await?;
    let meta = CheckpointMeta::decode_auto(&meta_bytes)?;
    if meta.token != token {
      return Err(Error::TokenMismatch {
        expected: token,
        actual: meta.token,
      });
    }

    if meta.format_version > FORMAT_VERSION {
      return Err(Error::UnsupportedMetaVersion {
        actual: meta.format_version,
        supported: FORMAT_VERSION,
      });
    }

    // 完整性封签校验（v2 起强制）：JSON 文本级篡改/介质位翻转虽可通过反序列化
    // （结构合法），但无法通过逐字段摘要比对——在此拦截「静默错误恢复」类损坏，
    // recover_latest 依此回退至更早的有效检查点。遗留格式（< v2）按旧语义放行。
    if meta.format_version >= INTEGRITY_FROM_VERSION {
      let digest = meta.integrity_digest();
      if digest != meta.integrity_crc32 {
        return Err(Error::MetaChecksumMismatch {
          expected: meta.integrity_crc32,
          actual: digest,
        });
      }
    }

    let epoch = Arc::new(LightEpoch::new(meta.store_meta.max_sessions));
    let hlog_config = HybridLogConfig::new(
      meta.store_meta.page_size,
      meta.store_meta.num_pages,
      meta.store_meta.mutable_fraction,
    )?;

    if meta.index_meta.size != meta.store_meta.index_size {
      return Err(index_mismatch(
        "索引元数据大小与配置大小不匹配",
        meta.index_meta.size as u64,
        meta.store_meta.index_size as u64,
      ));
    }

    let begin = meta.hlog_meta.begin_address;
    let tail = meta.hlog_meta.tail_address;
    let flushed = meta.hlog_meta.flushed_until_address;

    if tail < hlog_config.initial_address {
      return Err(addr_violation(
        "TailAddress",
        tail,
        "小于日志起始基准地址",
        hlog_config.initial_address,
        "",
      ));
    }
    if begin > tail {
      return Err(addr_violation(
        "BeginAddress",
        begin,
        "超出 TailAddress",
        tail,
        "",
      ));
    }
    if meta.hlog_meta.head_address > tail {
      return Err(addr_violation(
        "HeadAddress",
        meta.hlog_meta.head_address,
        "超出 TailAddress",
        tail,
        "",
      ));
    }
    if meta.hlog_meta.head_address < begin {
      return Err(addr_violation(
        "HeadAddress",
        meta.hlog_meta.head_address,
        "低于 BeginAddress",
        begin,
        "",
      ));
    }
    if flushed < begin {
      return Err(addr_violation(
        "FlushedUntilAddress",
        flushed,
        "小于 BeginAddress",
        begin,
        "",
      ));
    }
    // 不变式 head <= flushed：已从内存驱逐的数据必须早已落盘，违反即为元数据损坏
    if flushed < meta.hlog_meta.head_address {
      return Err(addr_violation(
        "FlushedUntilAddress",
        flushed,
        "低于 HeadAddress",
        meta.hlog_meta.head_address,
        "，存在未落盘的已驱逐页",
      ));
    }

    // 4. 重建 HashIndex（纯 compio 异步流式反序列化，熔合 tail 截断与条目净化：
    //    I/O 等待由内核异步完成，反序列化与 CRC 校验为纯计算留在 reactor 线程）
    let index_path = dir.join(index_filename(token));
    let (index, index_meta) =
      read_index_checkpoint_truncated(&index_path, token, Some(tail)).await?;

    if index_meta.size != meta.store_meta.index_size {
      return Err(index_mismatch(
        "索引快照实际大小与引擎配置大小不匹配",
        index_meta.size as u64,
        meta.store_meta.index_size as u64,
      ));
    }
    if index_meta.overflow_count != meta.index_meta.overflow_count {
      return Err(index_mismatch(
        "索引快照溢出桶数量与元数据不一致",
        index_meta.overflow_count,
        meta.index_meta.overflow_count,
      ));
    }
    if index_meta.entry_count != meta.index_meta.entry_count {
      return Err(index_mismatch(
        "索引快照条目总数与元数据不一致",
        index_meta.entry_count as u64,
        meta.index_meta.entry_count as u64,
      ));
    }

    let hlog = Arc::new(HybridLog::new(
      hlog_config.clone(),
      Arc::clone(&device),
      Arc::clone(&epoch),
    )?);

    let curr_page = hlog_config.page_id(tail);
    let offset = hlog_config.page_offset(tail);
    let page_start = hlog_config.page_start_address(curr_page);

    // 若尾页包含历史有效数据，从底层设备预热加载至内存缓冲，以确保后续追加写入不损坏同页已有记录
    if offset > 0 && tail > hlog_config.initial_address {
      let tail_buf = device.read_range(page_start, hlog_config.page_size).await?;
      {
        let mut guard = hlog.buffer.write_page(curr_page);
        guard.copy_from_slice(&tail_buf);
      }
      hlog.buffer.set_page_id(curr_page);
    } else {
      hlog.buffer.clear_page(curr_page);
    }

    // 设置地址边界：
    // - head_address 设定为当前活跃页起始地址，确保该页之前的所有历史页面在读取时正确路由至底层 Device
    // - FoldOver: read_only_address 推进至 tail，确保历史数据封印为只读，后续更新全部走 RCU 追加
    // - Snapshot: read_only_address 基于 mutable_fraction 计算，保留内存可变区原位覆写能力（对齐 C# Tsavorite CalculateReadOnlyAddress）
    let head = page_start.max(meta.hlog_meta.head_address);
    let ro = if meta.cp_type == CheckpointType::FoldOver {
      tail
    } else {
      hlog_config
        .calculate_read_only_address(head, tail)
        .max(head)
    };

    // 钳制 FlushedUntilAddress 至截断点：封印 tail 之后新起的 RCU 追加可能随本次
    // flush_all 一并落盘，使持久化值超前截断点。恢复视图必须以截断点为准——
    // `[begin, tail)` 已由 flush_all + device.sync 保证落盘，钳制至 tail 只会引发
    // 后续可能的冗余重刷，绝不漏刷任何脏页，同时维持 flushed_until <= tail 单调不变式
    let flushed = flushed.min(tail);

    hlog.addresses.begin_address.store(begin, Ordering::Release);
    hlog.addresses.head_address.store(head, Ordering::Release);
    hlog
      .addresses
      .safe_head_address
      .store(head, Ordering::Release);
    hlog
      .addresses
      .read_only_address
      .store(ro, Ordering::Release);
    hlog
      .addresses
      .safe_read_only_address
      .store(ro, Ordering::Release);
    hlog
      .addresses
      .flushed_until_address
      .store(flushed, Ordering::Release);
    hlog.addresses.tail_address.store(tail, Ordering::Release);

    info!(
      "成功完成 Checkpoint 崩溃恢复组件加载: token={token:#x}, entry_count={}, tail={tail:#x}, head={head:#x}, ro={ro:#x}",
      meta.index_meta.entry_count
    );

    Ok(RecoveredCheckpoint {
      meta,
      index: Arc::new(index),
      hlog,
      epoch,
    })
  }

  /// 从指定 Checkpoint 进行崩溃恢复，重构并实例化全新的宿主存储引擎
  pub async fn recover<S: CprRecover<Device = D>>(
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
    device: Arc<D>,
  ) -> Result<S> {
    let dir = checkpoint_dir.as_ref();
    let recovered = Self::recover_checkpoint_components(dir, token, Arc::clone(&device)).await?;
    S::from_recovered(recovered, dir, device).await
  }

  /// 实例恢复方法（便捷转发至关联静态方法）
  pub async fn recover_store<S: CprRecover<Device = D>>(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    token: u128,
    device: Arc<D>,
  ) -> Result<S> {
    Self::recover(checkpoint_dir, token, device).await
  }

  /// 从目录中最新的有效 Checkpoint 执行崩溃恢复
  ///
  /// 自最新 Token 起由新到旧逐一尝试，自动跳过损坏或不完整的检查点
  /// （对标 C# Tsavorite GetClosestHybridLogCheckpointInfo 对无效 Token 的
  /// 容错跳过语义，及 libs/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs:
  /// GetClosestHybridLogCheckpointInfo / GetClosestIndexCheckpointInfo——上游
  /// d20d63993 将该跳过路径的吞异常改为 LogWarning，使「坏检查点被跳过」可区分于
  /// 「检查点集为空」；本实现自始即以 `warn!` 记录被跳过的 Token 与原因，语义一致）。
  /// 目录中不存在任何 Token 时返回 `NoValidCheckpoint`。
  ///
  /// 排序稳定性：候选 Token 集为调用时刻的目录快照（数值升序，u128 全序确定），
  /// 扫描期间新落地的检查点留待下一次调用发现，不扰动本轮尝试序；与并发的
  /// [`Self::purge_outdated`] 相互作用时，被清理 Token 的恢复按「文件缺失」容错
  /// 回退至更早版本，不会产生撕裂视图。
  pub async fn recover_latest<S: CprRecover<Device = D>>(
    checkpoint_dir: impl AsRef<Path>,
    device: Arc<D>,
  ) -> Result<S> {
    let dir = checkpoint_dir.as_ref();
    let tokens = Self::list_checkpoints(dir)?;
    let mut first_err = None;
    for token in tokens.into_iter().rev() {
      match Self::recover::<S>(dir, token, Arc::clone(&device)).await {
        Ok(store) => return Ok(store),
        Err(e) => {
          warn!("跳过无效 Checkpoint（回退至更早版本）: token={token:#x}, err={e}");
          // 保留最新 Token 的错误作为代表性失败原因
          first_err.get_or_insert(e);
        }
      }
    }
    Err(first_err.unwrap_or(Error::NoValidCheckpoint(dir.to_path_buf())))
  }

  /// 实例恢复最新方法（便捷转发至关联静态方法）
  pub async fn recover_latest_store<S: CprRecover<Device = D>>(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    device: Arc<D>,
  ) -> Result<S> {
    Self::recover_latest(checkpoint_dir, device).await
  }

  /// 列出目标目录中所有可用的 Checkpoint Token
  ///
  /// 目录枚举为纯元数据 syscall（compio-fs 0.12.1 未提供 read_dir 异步原语），保持同步实现。
  pub fn list_checkpoints(checkpoint_dir: impl AsRef<Path>) -> Result<Vec<u128>> {
    let dir = checkpoint_dir.as_ref();
    if !dir.exists() {
      return Ok(Vec::new());
    }

    let mut tokens: Vec<u128> = read_dir(dir)?
      .flatten()
      .filter_map(|entry| {
        let name = entry.file_name();
        let name_str = name.to_str()?;
        name_str
          .strip_prefix(META_PREFIX)
          .and_then(|s| s.strip_suffix(META_EXT))
          .and_then(|s| s.parse::<u128>().ok())
      })
      .collect();

    tokens.sort_unstable();
    Ok(tokens)
  }

  /// 获取目标目录中最新的 Checkpoint Token
  pub fn find_latest_checkpoint(checkpoint_dir: impl AsRef<Path>) -> Result<Option<u128>> {
    let tokens = Self::list_checkpoints(checkpoint_dir)?;
    Ok(tokens.last().copied())
  }

  /// 清理指定 Token 的快照物理文件（包含 meta 与 ckpt 文件及临时文件，彻底回收 token 子目录）
  /// 对标 C# Tsavorite CheckpointManager.Purge(Guid)
  ///
  /// unlink/rmdir 为纯元数据 syscall（compio-fs 0.12.1 未提供 remove_dir_all 异步原语），
  /// 保持同步实现，与 wdev/wbftree 的目录维护路径一致。
  pub fn purge_checkpoint(checkpoint_dir: impl AsRef<Path>, token: u128) -> Result<()> {
    let dir = checkpoint_dir.as_ref();
    let mut itoa_buf = itoa::Buffer::new();
    let token_str = itoa_buf.format(token);
    let meta_path = dir.join(meta_filename(token));
    let index_path = dir.join(index_filename(token));
    let meta_tmp = dir.join(meta_tmp_filename(token));
    let index_tmp = dir.join(index_tmp_filename(token));
    let token_dir = dir.join(token_str);

    rm_path_best_effort(&meta_path);
    rm_path_best_effort(&index_path);
    rm_path_best_effort(&meta_tmp);
    rm_path_best_effort(&index_tmp);
    rm_path_best_effort(&token_dir);
    Ok(())
  }

  /// 同步清扫检查点目录的全部残留物：RangeIndex 快照子树、孤儿 `.tmp`/`.ckpt`/`.meta`
  /// 文件与孤儿 token 子目录（meta 已丢失但快照子目录残留）
  ///
  /// 同步实现论证：`read_dir`/`unlink`/`rmdir` 全部为纯元数据 syscall（无数据面 I/O，
  /// 微秒级返回，混入异步管理器不构成阻塞风险）；compio-fs 0.12.1 未提供 `read_dir`
  /// 与递归 `remove_dir_all` 异步原语（仅有单层 `remove_dir`，递归删除仍需同步枚举），
  /// 若为元数据 syscall 引入 blocking 线程池中转，反而引入跨线程调度与线程创建开销，
  /// 违背 thread-per-core 零线程创建约束。与 [`CheckpointManager::list_checkpoints`]、
  /// [`CheckpointManager::purge_checkpoint`] 保持同一同步口径。
  fn sweep_checkpoint_residue(dir: &Path) {
    let ri_dir = dir.join("rangeindex");
    if ri_dir.exists() {
      let _ = remove_dir_all(ri_dir);
    }
    // 彻底清理任何残留的临时文件（.tmp）、孤儿快照文件与失去 meta 的孤儿 token 子目录
    if let Ok(entries) = read_dir(dir) {
      for entry in entries.flatten() {
        let name = entry.file_name();
        if let Some(name_str) = name.to_str() {
          if name_str.ends_with(TMP_EXT)
            || name_str.ends_with(INDEX_EXT)
            || name_str.ends_with(META_EXT)
          {
            let _ = remove_file(entry.path());
          } else if name_str.parse::<u128>().is_ok() && entry.file_type().is_ok_and(|t| t.is_dir())
          {
            // 孤儿 token 目录：meta 已丢失但 RangeIndex 快照子目录残留
            // （对标 C# CheckpointManager RemoveOutdated 的陈旧检查点清理语义）
            let _ = remove_dir_all(entry.path());
          }
        }
      }
    }
  }

  /// 清理目标目录下所有快照物理文件（对标 C# Tsavorite CheckpointManager.PurgeAll）
  pub fn purge_all(checkpoint_dir: impl AsRef<Path>) -> Result<()> {
    let dir = checkpoint_dir.as_ref();
    if !dir.exists() {
      return Ok(());
    }
    let tokens = Self::list_checkpoints(dir)?;
    for token in tokens {
      Self::purge_checkpoint(dir, token)?;
    }
    Self::sweep_checkpoint_residue(dir);
    Ok(())
  }

  /// 保留最新 `keep` 个检查点，清理更早的全部快照物理文件
  ///
  /// 磁盘空间回收入口（对标 C# Tsavorite Recovery 后 "Purge all log/index checkpoints
  /// that were not used for recovery" 的陈旧检查点回收语义）：每次成功恢复或周期性
  /// 快照后调用，即可将检查点磁盘占用约束在 `keep` 个版本之内。
  ///
  /// 返回本轮被清理的 Token 列表（由旧到新）。`keep` 为 0 时清理全部可识别的检查点
  /// 文件集；对不存在或不可识别的文件不做任何触碰。
  ///
  /// 调用方契约：删除为纯 unlink，不感知在途使用者——正在被 `recover_latest` 尝试的
  /// Token 若同被并发清理，该轮恢复按文件缺失容错回退至更早版本（不产生撕裂视图）；
  /// 因此调用方应遵循「恢复成功后再回收、且保留用于恢复的那一版」的时序约定。
  pub fn purge_outdated(checkpoint_dir: impl AsRef<Path>, keep: usize) -> Result<Vec<u128>> {
    let dir = checkpoint_dir.as_ref();
    let tokens = Self::list_checkpoints(dir)?;
    let boundary = tokens.len().saturating_sub(keep);
    for &token in &tokens[..boundary] {
      Self::purge_checkpoint(dir, token)?;
    }
    Ok(tokens[..boundary].to_vec())
  }

  /// 实例清理方法（便捷转发至关联静态方法）
  pub fn purge(&self, checkpoint_dir: impl AsRef<Path>, token: u128) -> Result<()> {
    Self::purge_checkpoint(checkpoint_dir, token)
  }

  /// 实例全量清理方法（便捷转发至关联静态方法）
  pub fn purge_all_checkpoints(&self, checkpoint_dir: impl AsRef<Path>) -> Result<()> {
    Self::purge_all(checkpoint_dir)
  }

  /// 实例保留最新 N 个检查点方法（便捷转发至关联静态方法）
  pub fn purge_outdated_checkpoints(
    &self,
    checkpoint_dir: impl AsRef<Path>,
    keep: usize,
  ) -> Result<Vec<u128>> {
    Self::purge_outdated(checkpoint_dir, keep)
  }
}

#[cfg(test)]
mod tests {
  use std::fs::{create_dir_all, write};

  use compio::runtime::Runtime;
  use tempfile::tempdir;

  use super::{issue_token_after, next_token, sync_dir_tree};

  /// Token 签发闸门：墙钟回拨续发、目录下界钳制与两者叠加均保持签发值严格递增
  ///
  /// 单测进程独占全局闸门状态，断言仅依赖自身签发值的相对序，对并发测试无时序敏感
  #[test]
  fn token_gate_monotonic_under_rollback_and_floor() {
    let a = next_token();
    // 模拟墙钟回拨：候选值低于历史签发最大值 → 以历史最大值 +1 续发
    let b = issue_token_after(a.saturating_sub(1), 0);
    assert!(b > a, "回拨候选必须续发: {a} -> {b}");
    // 目录下界钳制：下界高于历史签发值 → 下界 +1
    let c = issue_token_after(0, b);
    assert!(c > b, "目录下界必须抬升签发值: {b} -> {c}");
    // 回拨与下界叠加（前一轮被下界抬升的签发值未回写进程守卫的场景已由
    // issue_token_after 统一串行签发消除）：候选回拨 + 陈旧下界仍不撞号
    let d = issue_token_after(b, c);
    assert!(d > c, "叠加路径必须续发: {c} -> {d}");
    // 常规路径恢复后仍严格递增（进程守卫已吸收下界抬升，后续签发不回退）
    let e = next_token();
    assert!(e > d, "常规签发必须严格递增: {d} -> {e}");
    // 高位候选越过下界后，守卫继续从高位续发
    let f = issue_token_after(u128::MAX - 5, 0);
    let g = next_token();
    assert!(f > e && g > f, "高位候选后仍须递增: {e} -> {f} -> {g}");
  }

  /// sync_dir_tree 必须容忍任意深度的嵌套目录树、空目录与空文件，且幂等可重入
  #[test]
  fn sync_dir_tree_handles_nested_tree_and_is_idempotent() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let dir = tempdir().unwrap();
      let deep = dir.path().join("token/rangeindex/prefix");
      create_dir_all(&deep).unwrap();
      write(deep.join("data.bftree"), b"payload").unwrap();
      write(deep.join("empty.bftree"), b"").unwrap();
      create_dir_all(dir.path().join("token/empty_dir")).unwrap();

      sync_dir_tree(dir.path()).await.unwrap();
      // 幂等：重复调用不报错（如同一 Token 连续两次检查点）
      sync_dir_tree(dir.path()).await.unwrap();
    });
  }

  /// 目标目录不存在必须报错暴露（由调用方的 exists() 守卫先行过滤）
  #[test]
  fn sync_dir_tree_fails_on_missing_dir() {
    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let dir = tempdir().unwrap();
      assert!(sync_dir_tree(&dir.path().join("missing")).await.is_err());
    });
  }
}
