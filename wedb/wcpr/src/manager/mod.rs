//! 自研依据: 检查点协议管理器（C# 对应 libs/storage/Tsavorite/cs/src/core/Checkpoint 族）
use std::{
  fs::{read, read_dir, remove_dir_all, remove_file},
  future::Future,
  path::Path,
  process::id,
  sync::{
    Arc, OnceLock,
    atomic::{AtomicBool, AtomicU64, Ordering},
  },
};

use compio::fs::File;
use event_listener::Event;
use parking_lot::Mutex;
use wbase::time::now_nanos;
use wdev::Device;
use wepoch::LightEpoch;
use whlog::HybridLog;
use windex::HashIndex;

mod create;
mod recover;

pub use create::*;
pub use recover::*;

use crate::{
  error::{Error, Result},
  meta::{
    CheckpointMeta, INDEX_EXT, INDEX_PREFIX, META_EXT, META_PREFIX, StoreMeta, TMP_EXT,
    index_filename, index_tmp_filename, meta_filename, meta_tmp_filename, parse_token,
    token_to_base32,
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

  /// 当前活跃无锁哈希索引共享句柄
  fn index(&self) -> Arc<HashIndex>;

  /// 宿主哈希索引是否正处于在线扩容期（PrepareGrow 排空/构建窗与
  /// IN_PROGRESS_GROW 迁移窗均算）
  ///
  /// 对标 C# StateMachineDriver 单槽互斥：扩容状态机运行期间检查点发起必须
  /// 即时失败（见 `ensure_not_growing`）；宿主经本端口上报扩容相位
  fn is_growing(&self) -> bool;

  /// 宿主是否遗留「扩容中止索引撕裂待重建」显式标记（票 zcode-r135c-rehash
  /// 案二）：grow_index 分块迁移错误中止后，活跃表停在带未迁残片的旧新表，
  /// 未迁分块键在线不可见；标记存续期内索引检查点必须拒发快照（宁试后重试
  /// 不静默丢数），待在线收口重建成功后消标记。无该状态机的宿主用默认实现
  fn index_rebuild_pending(&self) -> bool {
    false
  }

  /// 进入检查点临界区：宿主独占标记置位，扩容发起自此被拒
  ///
  /// 对标 C# StateMachineDriver.cs:167-190 单槽注册（`Register` 以
  /// `Interlocked.CompareExchange(ref stateMachine, sm, null)` 抢占槽位，被占即
  /// 返回 false）：检查点自索引快照至 flush_all 全异步窗口期内必须对外持有
  /// 排他标记，杜绝并发 grow_index 切表致快照尺寸与 store_meta 撕裂。宿主无
  /// 扩容状态机时用默认空实现（零开销透明）
  fn enter_checkpoint(&self) -> Result<()> {
    Ok(())
  }

  /// 退出检查点临界区：宿主独占标记复位（幂等，失败清场路径兜底）
  fn exit_checkpoint(&self) {}

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

  /// 顺链跳过 ReadCache 把数据槽位解析为真实主日志逻辑地址（带驱逐等待的走查单口，
  /// 对标 C# SkipReadCache 每步判定当前位置 + RestartChain 回链头重读哈希项，
  /// 与紧缩面 `wcompact::CompactStore::skip_read_cache_with_wait` 同一 wkv 内核，
  /// 杜绝两套宿主口径）
  ///
  /// 入参为**待落盘的哈希数据槽位本体**而非地址值：走查触到正被换页驱逐的 ReadCache
  /// 记录（链中段滑出而槽头尚在窗内，为驱逐进行中的常态形态）时，本口以该滑出地址
  /// 就地自旋等待驱逐方完成该页哈希链清洗并发布 ClosedUntilAddress，落定后重读槽位
  /// 重探（清洗方已把本槽 CAS 回主日志地址），循环直至解析落定——检查点线程按
  /// `ensure_epoch_unprotected` 契约不持纪元保护区，故自旋续转的刷新动作为宿主纪元
  /// drain。C# 快照面靠 `epoch.Resume()` 冻结驱逐免等待（IndexCheckpoint.cs:146-157），
  /// 本 port 不冻结驱逐，改由本口等待收口。
  ///
  /// 返回值恒为解析后的低 48 位地址字段，绝无「不可判读」三态：
  /// - `0`：链尽合法形态（ReadCache 专属记录无主日志对应，交调用方整体归零净化）；
  /// - 非 ReadCache 槽位与未启用 ReadCache 的宿主恒等透传（`clean_address(slot)`）。
  ///
  /// 本口绝不得把存活槽位折成 0：快照归零即恢复后永久漏键（无重插、无自愈通道）
  fn skip_read_cache_with_wait(&self, slot: &AtomicU64) -> u64;

  /// 执行 RangeIndex 快照
  fn take_range_index_checkpoints(&self, dir: &Path, token: u128) -> Result<usize>;

  /// 设置 RangeIndex 检查点屏障：阻塞全部 BfTree 树写入直至配对清屏
  ///
  /// 在 garnet 中的相对路径:libs/server/Storage/Functions/GarnetRecordTriggers.cs:OnCheckpoint
  /// （CheckpointTrigger::VersionShift 分支触发 RangeIndexManager.SetCheckpointBarrier，
  /// 自版本切换起阻塞树写入直至 flush 完成后 ClearCheckpointBarrier；宿主经本端口在
  /// 排空与全部快照之前设屏，由本流程 flush 完成后显式清屏，失败路径 RAII 兜底）
  fn set_range_index_checkpoint_barrier(&self);

  /// 清除 RangeIndex 检查点屏障（幂等，失败清场路径兜底）
  fn clear_range_index_checkpoint_barrier(&self);

  /// 生成当前存储配置元数据
  fn checkpoint_store_meta(&self) -> StoreMeta;
}

/// 检查点崩溃恢复重构契约接口
pub trait CprRecover: CprStore {
  /// 从恢复的核心组件重构完整的宿主存储引擎实例。
  ///
  /// 契约：实现方**必须恰好调用一次** [`run_recovery_kernel`]（区间取
  /// `recovered.meta.index_start_logical_address` 与 `recovered.hlog` 的
  /// begin/tail，undo_next_version 默认传 true），同趟完成模糊区回滚
  /// （undoNextVersion）、模糊区重插与宿主恢复回调；组件加载阶段
  /// （`recover_checkpoint_components`）刻意不扫描——部分回调依赖宿主对象
  /// （RangeIndex 桩表、虚拟库表），只能在 from_recovered 组装后执行。
  /// 该差异对标 C# core 内驱动（Recovery.cs:RecoverHybridLogAsync 同趟派发
  /// GarnetRecordTriggers.cs:OnRecoverySnapshotRead），仅驱动方由 core 移
  /// 至宿主，扫描次数与记录语义保持一致。
  ///
  /// [`run_recovery_kernel`]: super::recover::run_recovery_kernel
  fn from_recovered(
    recovered: RecoveredCheckpoint<Self::Device>,
    checkpoint_dir: &Path,
    device: Arc<Self::Device>,
  ) -> impl Future<Output = Result<Self>>
  where
    Self: Sized;

  /// 回退重试清场钩子：摘除失败轮已预置的宿主派生工件（静态，无实例）
  ///
  /// [`recover_latest`] 由新到旧逐 Token 重试；某轮 `from_recovered` 中途
  /// 失败时，宿主可能已把检查点快照预置为磁盘工作文件（RI 快照树文件拷贝；
  /// 本轮注册的 pending 条目随失败实例析构自动摘除，磁盘工件留存）——下一
  /// 轮更早 Token 恢复时快照不含该 key，残留工件不被覆盖，AOF 重放首次访问
  /// 即混出代际混杂视图。实现方按本轮 Token 定位并删除已拷贝工件
  /// （`store_meta` 供宿主定位其工作目录）；meta 解码失败的轮次未进入宿主
  /// 预置段，无残留可清，不回调。默认空实现（无磁盘派生工件的宿主）。
  fn discard_partial_recovery(checkpoint_dir: &Path, token: u128, store_meta: &StoreMeta)
  where
    Self: Sized,
  {
    let _ = (checkpoint_dir, token, store_meta);
  }
}

impl<S: CprStore> CprStore for Arc<S> {
  type Device = S::Device;

  #[inline]
  fn hlog(&self) -> &HybridLog<Self::Device> {
    (**self).hlog()
  }

  #[inline]
  fn index(&self) -> Arc<HashIndex> {
    (**self).index()
  }

  #[inline]
  fn is_growing(&self) -> bool {
    (**self).is_growing()
  }

  #[inline]
  fn index_rebuild_pending(&self) -> bool {
    (**self).index_rebuild_pending()
  }

  #[inline]
  fn enter_checkpoint(&self) -> Result<()> {
    (**self).enter_checkpoint()
  }

  #[inline]
  fn exit_checkpoint(&self) {
    (**self).exit_checkpoint()
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
  fn skip_read_cache_with_wait(&self, slot: &AtomicU64) -> u64 {
    (**self).skip_read_cache_with_wait(slot)
  }

  #[inline]
  fn take_range_index_checkpoints(&self, dir: &Path, token: u128) -> Result<usize> {
    (**self).take_range_index_checkpoints(dir, token)
  }

  #[inline]
  fn set_range_index_checkpoint_barrier(&self) {
    (**self).set_range_index_checkpoint_barrier()
  }

  #[inline]
  fn clear_range_index_checkpoint_barrier(&self) {
    (**self).clear_range_index_checkpoint_barrier()
  }

  #[inline]
  fn checkpoint_store_meta(&self) -> StoreMeta {
    (**self).checkpoint_store_meta()
  }
}

/// 用于生成单调唯一 Token 的原子自增序列号
static TOKEN_SEQ: AtomicU64 = AtomicU64::new(1);

/// 进程内最近一次签发的 Token（墙钟回拨单调守卫）
static LAST_TOKEN: Mutex<Option<u128>> = Mutex::new(None);

/// 进程判别盐（启动期一次性铸型）：高 32 位 pid、低 32 位启动随机熵
static PROCESS_SALT: OnceLock<u64> = OnceLock::new();

/// 铸进程判别盐：pid 高位使同机异进程天然相异（同一检查点目录仅同机文件系统
/// 可达，为唯一真实威胁面）；随机熵低位覆盖 pid 回收复用（重启重叠时新旧进程
/// pid 同值）。纯函数公开：跨进程唯一性形制是 token 全序契约的组成部分，供
/// 集成测试与宿主复核唯一性边界共用
#[inline]
pub fn salt_bits(pid: u32, entropy: u64) -> u64 {
  ((pid as u64) << 32) | (entropy as u32 as u64)
}

/// 进程判别盐取值：惰性初始化即「进程启动期一次性」，进程生命周期内恒定
#[inline]
fn process_salt() -> u64 {
  *PROCESS_SALT.get_or_init(|| salt_bits(id(), fastrand::u64(..)))
}

/// Token 低位铸型：进程判别盐 + 进程内自增序列号，模加单射（固定盐下序列号双射）
///
/// 低位唯一性范围自此扩至跨进程：对标 C# Checkpoint.cs 各 Take*Checkpoint 的
/// token 为 `Guid.NewGuid()`（128 位随机全局唯一，不依赖时钟与进程内序列），rust
/// 以「墙钟 + 序列」替代 GUID 时必须自带进程判别量才不失其唯一性语义——同机
/// 两进程对同一检查点目录各自从序列号 1 起签发（重启重叠/双实例），裸序列低位
/// 必同值撞号：文件集按 token 命名互相覆写，失败清场按 token 回收互毁
#[inline]
pub fn mint_low(salt: u64, seq: u64) -> u64 {
  salt.wrapping_add(seq)
}

/// 由墙钟与「进程盐 + 序列号」合成候选 Token：高 64 位墙钟纳秒，低 64 位跨进程
/// 判别盐加自增序列（见 [`mint_low`]）
///
/// 唯一性不依赖时钟精度：`now_nanos` 为精确实时域读数，同窗口内重复取值
/// 由低位原子序列号消歧（固定盐下任意两次调用的低位恒不相等；跨进程由盐判别）；
/// 候选值仅是初值，最终签发值经 [`issue_token_after`] 以进程历史最大值与目录
/// 下界的**高位**为下界串行钳制续发。对标 C#
/// Checkpoint.cs 各 Take*Checkpoint 的 `Guid.NewGuid()` 随机 token——C# 不依赖
/// token 大小序（按元数据 version 挑选恢复点），wedb 以「token 大小序即版本新旧序」
/// 取代该机制，故签发必须严格单调（见 [`issue_token_after`] 的回拨防御）。
#[inline]
pub(crate) fn candidate_token() -> u128 {
  let now = now_nanos() as u128;
  let low = mint_low(process_salt(), TOKEN_SEQ.fetch_add(1, Ordering::Relaxed));
  (now << 64) | low as u128
}

/// Token 签发闸门：候选值高 64 位不严格超越「进程历史签发最大值与目录下界
/// `floor`（检查点目录内现存最大 Token，覆盖跨进程重启后的时钟回拨）」的高位时，
/// 签发值高 64 位强制抬升至 max_hi + 1、低 64 位承接进程盐加自增序列续发
/// （见 [`mint_low`]），签发值进程内严格单调递增、跨进程互不相异，且高 64 位
/// 逐次严格递增。
///
/// 高位钳制是版本正确性的根：wnode 以 `token >> 64` 投影检查点/AOF 版本号
/// （对标 C# HybridLogRecoveryInfo.version，由 VersionChangeSM.NextState 的
/// `nextState.Version = start.Version + 1` 每代严格 +1 驱动）。若只钳 128 位全序
/// （旧实现 `floor + 1` 仅低位进位），墙钟回拨/跨进程时钟滞后时连续两代 Token
/// 高位相等——版本号跨代停滞，恢复基线与前代一致，AOF
/// `IsOldVersionRecord(header.store_version < CurrentVersion)` 对已固化旧代条目
/// 恒判 false，非幂等命令（INCR/LPUSH/APPEND/HINCRBY）重复重放破坏一致性。
pub(crate) fn issue_token_after(candidate: u128, floor: u128) -> u128 {
  // parking_lot 无中毒语义：守卫绝不向检查点路径传播 panic
  let mut last = LAST_TOKEN.lock();
  let prev = *last;
  // 高 64 位下界：历史签发最大值与目录现存最大 Token 的高位取大（u64 域，无溢出）
  let hi_floor = prev
    .map_or(0, |t| (t >> 64) as u64)
    .max((floor >> 64) as u64);
  let token = match (candidate >> 64) as u64 {
    // 候选高位严格超越下界：原值直发（低位为进程盐加自增序列，跨进程恒不相异）
    hi if hi > hi_floor => candidate,
    // 墙钟回拨/跨进程时钟滞后：高位抬至下界 +1，低位承接进程盐加自增序列，
    // 令版本投影域（token >> 64）跨代严格递增
    _ => match hi_floor.checked_add(1) {
      Some(next_hi) => {
        let low = mint_low(process_salt(), TOKEN_SEQ.fetch_add(1, Ordering::Relaxed));
        ((next_hi as u128) << 64) | low as u128
      }
      // hi_floor == u64::MAX 为理论边界（2^128 空间耗尽，实际不可达）：
      // 退回低位续发（历史最大值 +1），饱和再退候选值
      None => prev.unwrap_or(floor).checked_add(1).unwrap_or(candidate),
    },
  };
  *last = Some(token);
  token
}

/// 检查点串行闸门状态：宿主存储引擎实例所有权字段（对标 C#
/// GarnetDatabase.cs:75 `CheckpointingLock`——per-database 实例锁，非 static），
/// 由宿主（如 `wkv::WedbStore::ckpt_gate`）随引擎实例持有。同一实例上的
/// SAVE 手动触发与周期快照可能并发进入，两次检查点会叠加变更同一存储引擎
/// 状态机（只读线封印、全库 flush、CPR 短屏障、复活池清理），必须串行执行；
/// 文件集层面各 Token 互不相交本就免锁，闸门仅裁断存储引擎状态机的并发叠加。
/// 互斥粒度随引擎实例而非目录路径：嵌入形态下实例生命周期即闸门生命周期，
/// 无进程级注册表的条目泄漏面与路径口径分叉（symlink/相对绝对混用裂为两锁）
#[derive(Default)]
pub struct CkptGateState {
  busy: AtomicBool,
  event: Event,
}

/// 闸门 RAII 守卫：持有实例闸的异步互斥权，Drop 自动释放并通知就绪等待者
pub struct CkptGate<'a> {
  state: &'a CkptGateState,
}

impl Drop for CkptGate<'_> {
  fn drop(&mut self) {
    self.state.busy.store(false, Ordering::Release);
    self.state.event.notify(1);
  }
}

/// 获取实例检查点闸门：基于 event_listener 的零开销异步互斥
/// 快路径原子 CAS 抢占，冲突时挂起注册 event_listener，前任持有者 Drop 释放锁时精准唤醒下一个等待者，
/// 彻底消除定时器轮询与延迟抖动。等待与唤醒全部异步完成，不阻塞任何 reactor 线程。
#[inline]
pub async fn acquire(gate: &CkptGateState) -> CkptGate<'_> {
  loop {
    if !gate.busy.swap(true, Ordering::AcqRel) {
      return CkptGate { state: gate };
    }
    let listener = gate.event.listen();
    if !gate.busy.swap(true, Ordering::AcqRel) {
      return CkptGate { state: gate };
    }
    listener.await;
  }
}

/// 检查点入口契约校验：当前线程处于纪元保护区时 fail-fast 拒绝（零副作用）
///
/// 必须在进入 [`acquire`] 等待队列之前执行——保护区内发起检查点的违约
/// 调用若挂起在闸门队列上，其 RAII 纪元守卫永不退出，会把闸门持有者的 epoch
/// 排空屏障（[`LightEpoch::is_safe_to_reclaim`] 谓词）钉死为永假，形成循环等待。
/// C# Tsavorite/Garnet 的检查点由外部串行驱动（CHECKPOINT 命令、
/// StoreWrapper.CompactionTaskAsync 均在会话作用域外调用），「不在保护区内发起」
/// 仅靠调用约定成立；wedb 将其升级为类型化错误：若在保护区内继续创建，
/// 「数据页已刷盘但索引插入尚未提交」的丢失更新窗口将静默打开（静稳性缺口）。
/// 判定须覆盖 TLS 作用域与 Participant 句柄双轨保护
/// （[`LightEpoch::thread_protected`]）：仅查 TLS 轨会漏放
/// 「Participant::enter 守卫内发起检查点」的调用，该调用将以自钉纪元使排空屏障
/// 谓词永假——fail-fast 退化为无限自旋。单线程测试等合法场景天然满足：会话
/// 操作守卫均为 RAII 作用域，操作返回即退出保护区。
#[inline]
pub(crate) fn ensure_epoch_unprotected<S: CprStore>(store: &S) -> Result<()> {
  if store.epoch().thread_protected() {
    return Err(Error::CheckpointWhileEpochProtected);
  }
  Ok(())
}

/// 检查点入口零副作用 fail-fast 校验：索引在线扩容期（准备窗与迁移窗）拒绝发起
///
/// 对标 C# StateMachineDriver.cs:164-166 `Register` 的单槽
/// `Interlocked.CompareExchange` 互斥——扩容状态机（Tsavorite.cs:850
/// `GrowIndexAsync` 经同一驱动器 `RunAsync` 注册）在跑时槽位已被占用，
/// 检查点状态机注册即时返回 false，即「发起即失败」；Tsavorite.cs:343-350
/// 文档明言 "initiation may fail if we are already taking a checkpoint or
/// performing some other operation such as growing the index"。rust 未把两类
/// 状态机收进同一槽位驱动器，扩容与检查点仅各自内部串行，故在入口显式过
/// `is_growing` 门补回该互斥语义，由调用方（SAVE / 周期快照）按既有重试路径处理。
/// 判定覆盖扩容全生命周期（PrepareGrow 事务屏障排空/新表构建窗 + InProgressGrow
/// 分裂迁移窗）：准备窗与快照窗口重叠同样构成撕裂源。
///
/// 数据丢失机理（漏门的后果）：IN_PROGRESS_GROW 期间哈希条目正被 split_buckets
/// 逐桶搬运，快照捕获点落在搬运中序上——未迁移分块的条目既不进快照、事后模糊区
/// 重放也找不回（其记录地址在扫描起点之下、截断线之外），重启后键永久不可见。
///
/// 本门是闸门前的一次性快筛；闸门内的排他独占由 [`CprStore::enter_checkpoint`]
/// 承担（排除并发检查点后 CAS 失败当且仅当扩容在跑），两道口合起来才完整复刻
/// C# 单槽互斥的双向语义。
///
/// 判定置于纪元校验、Token 签发与实例闸门之前：拒绝路径不创建目录、不签发
/// Token、不触碰设备，零副作用（不留半截 token 目录）。REST 态行为逐字节不变。
///
/// 第二道口同检 [`CprStore::index_rebuild_pending`]（票 zcode-r135c-rehash
/// 案二）：扩容错误中止后相位已回 Rest，但活跃表停在带未迁残片的旧新表——
/// 此时快照会把撕裂空分块原样落盘，重启后模糊区重放窗口之外无人补回（其
/// 记录地址在扫描起点之下、截断线之外），未迁键永久不可见；标记存续期一律
/// 拒发，放行给调用方既有重试轨道，待在线收口重建消标记后再试（宁试后
/// 重试不静默丢数，与索引预算耗尽显式上抛教义一致）。
#[inline]
pub(crate) fn ensure_not_growing<S: CprStore>(store: &S) -> Result<()> {
  if store.is_growing() {
    return Err(Error::Host(
      "哈希索引正在线扩容（PrepareGrow/InProgressGrow），拒绝发起 Checkpoint，待扩容完成后重试"
        .into(),
    ));
  }
  if store.index_rebuild_pending() {
    return Err(Error::Host(
      "扩容中止遗留索引撕裂待重建，拒绝发起 Checkpoint，待在线收口重建完成后重试".into(),
    ));
  }
  Ok(())
}

/// 以给定下界（如检查点目录内现存最大 Token）签发新 Token
///
/// 供 Garnet 检查点管理器层（wkv::CheckpointManager）在需要**预知** Token 的
/// 场景（版本切换回调先于快照执行、回调需携带新版本号，对标 C#
/// GarnetClusterCheckpointManager.checkpointVersionShiftStart 携 newVersion）
/// 使用：本函数与 [`create_checkpoint`] 内部签发共用同一
/// 串行闸门，目录下界 + 进程历史的墙钟回拨双重防御语义完全一致
pub fn next_token_above(floor: u128) -> u128 {
  issue_token_after(candidate_token(), floor)
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

/// 刷写检查点目录项，保证 rename 的原子替换在掉电后依然持久
///
/// POSIX 崩溃一致性语义下，数据文件自身 `sync_all` 并不覆盖目录项变更：
/// `rename` 必须辅以父目录 fsync 才能保证掉电后目录视图中出现的是新文件名
/// （而非半截 `.tmp` 残留）。此为「元数据最后落盘」闭环的最后一环。
///
/// wcpr 侧目录屏障的唯一具名出口：原语与跨平台口径由 [`wdev::sync_dir`] 单点定义
/// （非 Unix 平台空操作），本函数只做失败上抛，不再叠加任何转发层——与 wbftree
/// `manager::lifecycle` 发布路径直调 `wdev::sync_dir(parent)?` 的收口形态一致。
/// C# 无对应层次：目录 fsync 属设备层细节
/// （libs/storage/Tsavorite/cs/src/core/Device/LocalStorageDevice.cs 只有文件
/// Flush/Sync，检查点目录维护由 libs/server/GarnetCheckpointManager.cs 顺序完成）。
pub(crate) fn sync_checkpoint_dir(dir: &Path) -> Result<()> {
  wdev::sync_dir(dir)?;
  Ok(())
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
/// getdents 属纯元数据 syscall（无数据面 I/O），与 [`purge_all`]
/// 等目录维护路径一致保持同步实现；文件数据 fsync 走 compio 异步定位 I/O。
/// 递归经 `Box::pin` 装箱，深度受快照子目录结构约束（个位数层级）。
/// 目录项屏障直调 [`wdev::sync_dir`]，POSIX 语义口径见 [`sync_checkpoint_dir`]。
pub(crate) async fn sync_dir_tree(dir: &Path) -> Result<()> {
  for entry in read_dir(dir)? {
    let entry = entry?;
    if entry.file_type()?.is_dir() {
      Box::pin(sync_dir_tree(&entry.path())).await?;
    } else {
      sync_file_data(&entry.path()).await?;
    }
  }
  wdev::sync_dir(dir)?;
  Ok(())
}

/// 尽力删除单个残留路径：目录形态（如被误建/注入同名 `.tmp` 目录）整体递归删除，
/// 文件形态直接 unlink，不存在则静默跳过
fn rm_path_best_effort(path: &Path) {
  if path.is_dir() {
    let _ = remove_dir_all(path);
  } else {
    let _ = remove_file(path);
  }
}

/// 列出目标目录中所有可用的 Checkpoint Token
///
/// 目录枚举为纯元数据 syscall（compio-fs 0.12.1 未提供 read_dir 异步原语），保持同步实现。
pub fn list_checkpoints(checkpoint_dir: impl AsRef<Path>) -> Result<Vec<u128>> {
  let dir = checkpoint_dir.as_ref();
  if !dir.is_dir() {
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
        .and_then(parse_token)
    })
    .collect();

  tokens.sort_unstable();
  tokens.dedup();
  Ok(tokens)
}

/// 从单个目录项名解析其归属的快照 Token（覆盖四类物理文件与 Base32 快照子目录）
///
/// 命名面（[`crate::meta`] 唯一构造点）：
/// - `checkpoint_{b32}.meta`、`checkpoint_{b32}.meta.tmp`（meta 与其临时件）
/// - `index_{b32}.ckpt`、`index_{b32}.ckpt.tmp`（索引快照与其临时件）
/// - `{b32}` 快照子目录（RangeIndex 树快照落此）
///
/// 解析次序：先剥临时后缀 `.tmp`，再按前缀分派剥离语义扩展名，最终对余下 Base32
/// 串走 [`parse_token`] 精确解码（26 字符 RFC 4648 Base32hex，长度与字符集双门）。
/// Base32 字母表不含 `.` 与 `_`，故前缀/后缀分派与各分支互不误命中：任意非快照文件
/// （含随机同名目录）解码必失败返回 None，杜绝把无关项纳入清理候选。
fn extract_checkpoint_token(name: &str) -> Option<u128> {
  let core = name.strip_suffix(TMP_EXT).unwrap_or(name);
  if let Some(rest) = core.strip_prefix(META_PREFIX) {
    return parse_token(rest.strip_suffix(META_EXT).unwrap_or(rest));
  }
  if let Some(rest) = core.strip_prefix(INDEX_PREFIX) {
    return parse_token(rest.strip_suffix(INDEX_EXT).unwrap_or(rest));
  }
  parse_token(core)
}

/// 单次遍历列出检查点目录内**全部物理实体**的 Token（含未提交/损坏的孤儿快照）
///
/// 与 [`list_checkpoints`] 的语义分工（关键差异，勿混用）：
/// - [`list_checkpoints`] 仅认含完整 `.meta` 的**有效已提交**快照，保护
///   [`find_latest_checkpoint`]/[`latest_checkpoint_meta`] 的恢复选点不被半截快照污染；
/// - 本口枚举**任何**留有物理痕迹的 Token（meta/ckpt/tmp/Base32 子目录任一存在即纳入），
///   供磁盘淘汰路径把传输中断遗留的孤儿索引文件与 RangeIndex 子目录一并纳入回收候选。
///
/// 对标 C# `DeviceLogCommitCheckpointManager.GetLogCheckpointTokens`/
/// `GetIndexCheckpointTokens`（:235/:285）：二者各自 `deviceFactory.ListContents`
/// 直接物理列举快照根目录/索引目录内容并经 `checkpointNamingScheme.Token` 归组，
/// 不以元数据完整性过滤——故任何损坏或失去 meta 关联的孤儿文件均可被枚举、进而被
/// `PurgeAllCheckpointsExceptTokens` 物理删除。wcpr 统一检查点模型单 Token 一套文件，
/// 将 C# 的 log/index 两轨枚举收敛为「一处遍历、按 Token 归组去重」的最小实现，
/// 结果升序去重返回。目录枚举为纯元数据 syscall，与 [`list_checkpoints`] 同持同步口径。
pub fn list_all_checkpoint_tokens(checkpoint_dir: impl AsRef<Path>) -> Result<Vec<u128>> {
  let dir = checkpoint_dir.as_ref();
  if !dir.is_dir() {
    return Ok(Vec::new());
  }

  let mut tokens: Vec<u128> = read_dir(dir)?
    .flatten()
    .filter_map(|entry| extract_checkpoint_token(entry.file_name().to_str()?))
    .collect();

  tokens.sort_unstable();
  tokens.dedup();
  Ok(tokens)
}

/// 获取目标目录中最新的 Checkpoint Token
///
/// 恢复选点入口对标（C# 按版本取「距目标版本最近的 hlog 检查点」，wedb 单版本
/// 单全量快照下「最新即最近」）：
/// libs/storage/Tsavorite/cs/src/core/Index/Recovery/Recovery.cs:GetClosestHybridLogCheckpointInfo
pub fn find_latest_checkpoint(checkpoint_dir: impl AsRef<Path>) -> Result<Option<u128>> {
  let tokens = list_checkpoints(checkpoint_dir)?;
  Ok(tokens.last().copied())
}

/// 扫盘读最新有效快照元数据（C# CheckpointStore.cs:GetLatestCheckpointEntryFromDisk
/// 的 wcpr 磁盘模型对位）；目录无快照或读取/解码失败返回 None
pub fn latest_checkpoint_meta(checkpoint_dir: impl AsRef<Path>) -> Option<(u128, CheckpointMeta)> {
  let dir = checkpoint_dir.as_ref();
  let token = find_latest_checkpoint(dir).ok().flatten()?;
  let bytes = read(dir.join(meta_filename(token))).ok()?;
  let meta = CheckpointMeta::decode(&bytes).ok()?;
  Some((token, meta))
}

/// 清理指定 Token 的快照物理文件（包含 meta 与 ckpt 文件及临时文件，彻底回收 token 子目录）
/// 对标 C# Tsavorite CheckpointManager.Purge(Guid)
///
/// 接口与实现两层同挂此处（rust 折叠单点）：
/// - libs/storage/Tsavorite/cs/src/core/Index/Recovery/ICheckpointManager.cs:Purge
/// - libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/DeviceLogCommitCheckpointManager.cs:Purge
///
/// unlink/rmdir 为纯元数据 syscall（compio-fs 0.12.1 未提供 remove_dir_all 异步原语），
/// 保持同步实现，与 wdev/wbftree 的目录维护路径一致。
pub fn purge_checkpoint(checkpoint_dir: impl AsRef<Path>, token: u128) -> Result<()> {
  let dir = checkpoint_dir.as_ref();
  let b32 = token_to_base32(token);

  // 清理 Base32 命名快照文件与临时文件
  let files = [
    meta_filename(token),
    index_filename(token),
    meta_tmp_filename(token),
    index_tmp_filename(token),
  ];
  for f in &files {
    rm_path_best_effort(&dir.join(f));
  }
  // 清理 Base32 命名的子目录
  rm_path_best_effort(&dir.join(b32.as_str()));
  Ok(())
}

/// 同步清扫检查点目录的全部残留物：RangeIndex 快照子树、孤儿 `.tmp`/`.ckpt`/`.meta`
/// 文件与孤儿 token 子目录（meta 已丢失但快照子目录残留）
///
/// 同步实现论证：`read_dir`/`unlink`/`rmdir` 全部为纯元数据 syscall（无数据面 I/O，
/// 微秒级返回，混入异步管理器不构成阻塞风险）；compio-fs 0.12.1 未提供 `read_dir`
/// 与递归 `remove_dir_all` 异步原语（仅有单层 `remove_dir`，递归删除仍需同步枚举），
/// 若为元数据 syscall 引入 blocking 线程池中转，反而引入跨线程调度与线程创建开销，
/// 违背 thread-per-core 零线程创建约束。与 [`list_checkpoints`]、
/// [`purge_checkpoint`] 保持同一同步口径。
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
        } else if parse_token(name_str).is_some() && entry.file_type().is_ok_and(|t| t.is_dir()) {
          // 孤儿 token 目录（Base32 格式）：meta 已丢失但 RangeIndex 快照子目录残留
          // （对标 C# CheckpointManager RemoveOutdated 的陈旧检查点清理语义）
          let _ = remove_dir_all(entry.path());
        }
      }
    }
  }
}

/// 清理目标目录下所有快照物理文件（对标 C# Tsavorite CheckpointManager.PurgeAll）
///
/// 接口与实现两层同挂此处（rust 折叠单点）：
/// - libs/storage/Tsavorite/cs/src/core/Index/Recovery/ICheckpointManager.cs:PurgeAll
/// - libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/DeviceLogCommitCheckpointManager.cs:PurgeAll
pub fn purge_all(checkpoint_dir: impl AsRef<Path>) -> Result<()> {
  let dir = checkpoint_dir.as_ref();
  if !dir.is_dir() {
    return Ok(());
  }
  let tokens = list_checkpoints(dir)?;
  for token in tokens {
    purge_checkpoint(dir, token)?;
  }
  sweep_checkpoint_residue(dir);
  Ok(())
}

/// 保留最新 `keep` 个检查点，清理更早的全部快照物理文件
///
/// 引擎级自保回收入口（对标 C# 检查点管理器的 removeOutdated 环
/// `DeviceLogCommitCheckpointManager.cs:222` / `:272`：CleanupIndexCheckpoint /
/// CleanupLogCheckpoint 以 `indexTokenCount = 2` 环形 tokenHistory 拍新删旧）。
/// 返回本轮被清理的 Token 列表（由旧到新）。`keep` 为 0 时清理全部可识别的检查点
/// 文件集；对不存在或不可识别的文件不做任何触碰。
///
/// 形态契约（对标 C# `GarnetServer.cs:396` 的 removeOutdated = !EnableCluster
/// 分工）：只供**无复制域接管快照淘汰**的单机形态调用。集群形态的回收单轨归
/// 复制域检查点仓库的读者闸门（`CheckpointStore::delete_outdated_checkpoints`
/// 逐 Token [`purge_checkpoint`]，感知在途读者），按条数纯 unlink 的本口不得与
/// 其并行，否则在传快照会被误删。
///
/// 调用方契约：删除为纯 unlink，不感知在途使用者——正在被 `recover_latest` 尝试的
/// Token 若同被并发清理，该轮恢复按文件缺失容错回退至更早版本（不产生撕裂视图）；
/// 因此调用方应遵循「恢复成功后再回收、且保留用于恢复的那一版」的时序约定。
pub fn purge_outdated(checkpoint_dir: impl AsRef<Path>, keep: usize) -> Result<Vec<u128>> {
  let dir = checkpoint_dir.as_ref();
  let mut tokens = list_checkpoints(dir)?;
  let boundary = tokens.len().saturating_sub(keep);
  for &token in &tokens[..boundary] {
    purge_checkpoint(dir, token)?;
  }
  tokens.truncate(boundary);
  Ok(tokens)
}

#[cfg(test)]
mod tests {
  use std::fs::{create_dir_all, write};

  use compio::runtime::Runtime;
  use tempfile::tempdir;

  use super::{candidate_token, issue_token_after, sync_dir_tree};

  /// Token 签发闸门：墙钟回拨续发、目录下界钳制与两者叠加均保持签发值严格递增，
  /// 且高 64 位（wnode `checkpoint_version` 的版本投影域）逐次严格递增
  ///
  /// 单测进程独占全局闸门状态，断言仅依赖自身签发值的相对序，对并发测试无时序敏感
  #[test]
  fn token_gate_monotonic_under_rollback_and_floor() {
    let a = issue_token_after(candidate_token(), 0);
    let b = issue_token_after(a.saturating_sub(1), 0);
    assert!(b > a, "回拨候选必须续发: {a} -> {b}");
    assert!(
      b >> 64 > a >> 64,
      "回拨续发必须抬升高位版本投影域: {a} -> {b}"
    );
    let c = issue_token_after(0, b);
    assert!(c > b, "目录下界必须抬升签发值: {b} -> {c}");
    assert!(c >> 64 > b >> 64, "目录下界钳制必须抬升高位: {b} -> {c}");
    let d = issue_token_after(b, c);
    assert!(d > c, "叠加路径必须续发: {c} -> {d}");
    assert!(d >> 64 > c >> 64, "叠加路径必须抬升高位: {c} -> {d}");
    let e = issue_token_after(candidate_token(), 0);
    assert!(e > d, "常规签发必须严格递增: {d} -> {e}");
    assert!(e >> 64 > d >> 64, "常规签发高位必须严格递增: {d} -> {e}");
    let f = issue_token_after(u128::MAX - 5, 0);
    let g = issue_token_after(candidate_token(), 0);
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

  /// 快照 Token 规范 Base32 编码解析与目录列举验证
  #[test]
  fn test_token_parsing_and_listing() {
    use super::{list_checkpoints, purge_checkpoint};
    use crate::meta::{meta_filename, parse_token, token_to_base32};

    let token = 0x0123_4567_89ab_cdef_fedc_ba98_7654_3210_u128;
    let b32 = token_to_base32(token);

    // 1. Base32 格式正确解析
    assert_eq!(parse_token(b32.as_str()), Some(token));
    // 非规范格式（十进制、无效长度等）拒绝
    assert_eq!(parse_token(&token.to_string()), None);
    assert_eq!(parse_token("invalid-guid-string"), None);

    // 2. 目录中 Base32 文件名列举与排序
    let dir = tempdir().unwrap();
    let f1 = dir.path().join(meta_filename(token));
    write(f1, b"").unwrap();

    let list = list_checkpoints(dir.path()).unwrap();
    assert_eq!(list.len(), 1);
    assert_eq!(list[0], token);

    // 3. 清理
    purge_checkpoint(dir.path(), token).unwrap();
    let list_after = list_checkpoints(dir.path()).unwrap();
    assert!(list_after.is_empty());
  }

  /// 实例闸粒度契约：闸门随宿主存储引擎实例持有——同实例（同一
  /// `CkptGateState`）串行（挂起等待持闸者释放），不同实例（不同闸门状态）
  /// 互不阻塞即时取得。对标 C# GarnetDatabase.CheckpointingLock per-instance 形态
  #[test]
  fn ckpt_gate_is_per_instance() {
    use std::{
      sync::{
        Arc,
        atomic::{AtomicBool, Ordering as AtomicOrdering},
      },
      time::{Duration, Instant},
    };

    use compio::time::sleep;

    use super::{CkptGateState, acquire};

    /// 持闸最短时长（异闸臂即时性断言窗远小于此，同闸挂起臂等待窗据此锚定）
    const HOLD_MS: u64 = 120;

    let rt = Runtime::new().unwrap();
    rt.block_on(async {
      let gate_a = Arc::new(CkptGateState::default());
      let gate_b = Arc::new(CkptGateState::default());

      let a_acquired = Arc::new(AtomicBool::new(false));
      let b_acquired = Arc::new(AtomicBool::new(false));

      // H 臂：占 gate_a 闸并持 HOLD_MS
      let hold = {
        let gate = Arc::clone(&gate_a);
        let flag = Arc::clone(&a_acquired);
        rt.spawn(async move {
          let _gate = acquire(&gate).await;
          flag.store(true, AtomicOrdering::Release);
          sleep(Duration::from_millis(HOLD_MS)).await;
        })
      };
      while !a_acquired.load(AtomicOrdering::Acquire) {
        sleep(Duration::from_millis(1)).await;
      }

      // 异闸臂：gate_b 独立实例闸，必须即时取得（互不阻塞；跨实例互斥回归时
      // 此处将挂起至 H 臂释放，耗时 ≥ HOLD_MS 即暴露）
      let d_start = Instant::now();
      let waiter_d = {
        let gate = Arc::clone(&gate_b);
        let flag = Arc::clone(&b_acquired);
        rt.spawn(async move {
          let _gate = acquire(&gate).await;
          flag.store(true, AtomicOrdering::Release);
        })
      };
      while !b_acquired.load(AtomicOrdering::Acquire) {
        sleep(Duration::from_millis(1)).await;
      }
      assert!(
        d_start.elapsed() < Duration::from_millis(HOLD_MS / 2),
        "不同实例闸必须互不阻塞，实测等待 {:?}",
        d_start.elapsed()
      );

      // 同闸臂：gate_a 同实例，必须挂起至 H 臂持闸期满释放
      let c_start = Instant::now();
      let waiter_c = {
        let gate = Arc::clone(&gate_a);
        rt.spawn(async move {
          let _gate = acquire(&gate).await;
        })
      };
      waiter_c.await.unwrap();
      let c_waited = c_start.elapsed();
      assert!(
        c_waited >= Duration::from_millis(HOLD_MS / 2),
        "同实例闸门必须串行挂起等待持闸者释放，实测等待 {c_waited:?}"
      );

      waiter_d.await.unwrap();
      hold.await.unwrap();
    });
  }
}
