//! 向量集合键锁与索引读取（对标 libs/server/Resp/Vector/VectorManager.Locking.cs 与 libs/common/Synchronization/ReadOptimizedLock.cs）
//!
//! C# 以 ReadOptimizedLock（键哈希 → 读优化锁）实现共享读 / 独占写的
//! 升降级协议：共享读命中"需重建"的索引时升级独占，重建完成降级共享，
//! 并把持有的锁以 VectorSetLock（IDisposable）交还调用方。
//! Rust 侧条带基座为 [`async_lock::RwLock`] 的 128 字节对齐条带数组（键哈希
//! → 条带，零每键堆分配、消除伪共享；异步锁的守卫可随 async 栈帧跨
//! `.await` 存活——compio thread-per-core 下任务不迁线程，等待方让出线程
//! 而非自旋挂起，持锁任务不会被等锁任务饿死，故独占段内的登记写透
//! `.await` 与慢臂存储异步操作不再打穿锁纪律），本模块承接 C#
//! VectorSetLock 的升降级协议层语义。
//! [`async_lock::RwLockWriteGuard::downgrade`] 承接写→读无缝原子降级
//! （对齐 C# 重建完成后"降级共享"的协议终点，且无释放重取的时序空隙）；
//! 仅缺共享锁的原地无让步升级，升级以"释放共享 → 竞争独占 → 复核"的
//! 让步重试形态对齐 C# TryPromote 失败后的再入路径。
//! 索引记录的存储读取经 [`super::vector_manager::VectorManager`] 的
//! 键值登记表承接（wkv 集成前为域内表）。

use core::fmt;
use std::{
  array::from_fn,
  boxed::Box,
  future::Future,
  pin::Pin,
  sync::{Arc, atomic::Ordering::Relaxed},
};

use async_lock::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use wval::{NamespaceDbCodec, SessionPrefixBuf, TaggedKeyBuf};
use wvector::{
  IndexConfig, VectorDistanceMetricType, VectorQuantType, VectorSetFlags, store::StoreCallbacks,
};

use super::{
  vector_manager::{INDEX_SIZE_BYTES, VectorManager, VectorManagerResult},
  vector_manager_index::Index,
  vector_manager_quantization::{QuantizationChannel, QuantizationState, QuantizationStep},
};

/// 固定条带数（2 的幂，对齐 Garnet 静态分片哈希条带架构）。
pub const STRIPE_COUNT: usize = 256;

/// 条带掩码（编译期位与计算）。
pub const STRIPE_MASK: usize = STRIPE_COUNT - 1;

const _: () = assert!(STRIPE_COUNT.is_power_of_two(), "STRIPE_COUNT 必须为 2 的幂");

/// 登记表条目的物理会话域数值（复合键前缀段的解码形态）。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistryDomain {
  /// 虚拟命名空间号。
  pub vns: u64,
  /// 虚拟库号。
  pub vdb: u64,
}

/// 登记表复合键合成单点：`[NsVarint][DbVarint] + user_key`
///
/// C# 每库一实例 VectorManager（VectorManager.cs:173 `dbId` 首参）天然库隔离；
/// rust 单例共享登记表，以物理会话前缀直拼用户键达成同等隔离（doc/zh/db.md
/// §1.1 前缀刚性隔离；无 KeyTag——登记表自身即专属域）。前缀与数据条目
/// 物理键同源（`StoreSession::session_prefix`），变长段自定界保证唯一性。
/// 全仓唯一拼装入口，调用侧禁止自行拼接。
#[inline]
pub fn registry_key(prefix: &[u8], key: &[u8]) -> TaggedKeyBuf {
  let total_len = prefix.len() + key.len();
  if total_len <= wval::STACK_KEY_CAP {
    let mut buf = [0u8; wval::STACK_KEY_CAP];
    buf[..prefix.len()].copy_from_slice(prefix);
    buf[prefix.len()..total_len].copy_from_slice(key);
    TaggedKeyBuf::from_stack(buf, total_len as u8)
  } else {
    let mut vec = Vec::with_capacity(total_len);
    vec.extend_from_slice(prefix);
    vec.extend_from_slice(key);
    TaggedKeyBuf::from_heap(vec)
  }
}

/// 登记表复合键解码原语（仅本模块的两个公开剥域单点消费）。
///
/// 自定界论证：OPPV varint 首字节查表即定长（`varint_len_from_byte` 零回溯），
/// [NsVarint][DbVarint] 段自闭合，剩余字节即用户键。不作对外可选返回——
/// 调用侧一旦拿到 `Option` 就会各写一套兜底臂，兜底方向还容易反成泄漏面
///（把带域前缀的整键当用户键外发）。
#[inline]
fn decode_registry_key(composite: &[u8]) -> Option<(RegistryDomain, &[u8])> {
  let (vns, ns_len) = NamespaceDbCodec::decode_varint(composite).ok()?;
  let rest = &composite[ns_len..];
  let (vdb, db_len) = NamespaceDbCodec::decode_varint(rest).ok()?;
  Some((RegistryDomain { vns, vdb }, &rest[db_len..]))
}

/// 登记表复合键拆解单点（[`registry_key`] 的对偶读端，域 + 用户键一次取全）。
///
/// 登记表键必由 [`registry_key`] 构造（全仓唯一拼装入口），解码失败即本模块
/// 不变量被破坏的不可达态：显式失败，不静默降级、不返回带域前缀的整键。
#[inline]
pub fn split_registry_key(composite: &[u8]) -> (RegistryDomain, &[u8]) {
  match decode_registry_key(composite) {
    Some(parts) => parts,
    None => unreachable!("登记表复合键必由 registry_key 构造，解码失败即不变量破坏"),
  }
}

/// 登记表复合键 → 用户键剥域单点（[`registry_key`] 的对偶读端的帧面投影）。
///
/// 帧面（diskless 快照、迁移传输）恒发剥域用户键（C# 每库一实例天然无域前缀，
/// rust 单例以复合键隔离，出帧即剥）。不可达态口径同 [`split_registry_key`]。
#[inline]
pub fn registry_user_key(composite: &[u8]) -> &[u8] {
  split_registry_key(composite).1
}

/// 登记条目域 → 会话前缀缓冲（[`split_registry_key`] 消费端重构用）。
///
/// varint 编码确定性保证与写入时前缀字节恒等（wval 权威编码器单点）。
#[inline]
pub fn domain_prefix(domain: RegistryDomain) -> SessionPrefixBuf {
  SessionPrefixBuf::new(domain.vns, domain.vdb)
}

/// 条带槽位（128 字节对齐 [`RwLock`]，消除伪共享；对位
/// `wbase::striped::CacheAlignedLock` 的槽位形态，锁本体改异步基座）。
#[repr(align(128))]
struct CacheAlignedAsyncRwLock(RwLock<()>);

const _: () = assert!(
  size_of::<CacheAlignedAsyncRwLock>() == 128,
  "异步条带槽位必须恰占一个对齐槽（128 字节）"
);

/// 向量集合键的读优化静态条带锁表（对标 Garnet ReadOptimizedLock）。
///
/// 条带基座为 128 字节对齐 [`RwLock`]（异步锁）条带数组，Arc 共享支持
/// VectorManager 克隆；无 ConcurrentMap 并发查找开销，无每键动态堆分配。
/// 异步锁语义：阻塞获取（[`Self::acquire_shared`] /
/// [`Self::acquire_exclusive`]）仅在 async 上下文 await 点让出线程，守卫
/// 可跨 `.await` 存活（compio thread-per-core 下守卫随 async 栈帧钉在
/// 属主任务线程）；非阻塞获取（[`Self::try_acquire_shared`] /
/// [`Self::try_acquire_exclusive`]）供协作让步重试形态（量化 worker 等）。
#[derive(Clone)]
pub struct VectorSetLocks {
  stripes: Arc<[CacheAlignedAsyncRwLock; STRIPE_COUNT]>,
}

const _: () = assert!(
  size_of::<VectorSetLocks>() == 8,
  "VectorSetLocks 内部仅持有一个薄指针 Arc"
);

impl Default for VectorSetLocks {
  #[inline]
  fn default() -> Self {
    Self::new()
  }
}

impl fmt::Debug for VectorSetLocks {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.debug_struct("VectorSetLocks")
      .field("stripe_count", &self.stripes.len())
      .finish()
  }
}

impl VectorSetLocks {
  /// 初始化静态条带锁表（条带基座逐条带堆上就地构造，避免栈溢出）。
  #[inline]
  pub fn new() -> Self {
    Self {
      stripes: Arc::new(from_fn(|_| CacheAlignedAsyncRwLock(RwLock::new(())))),
    }
  }

  /// 计算键所属的条带编号。
  #[inline]
  pub fn stripe_for(&self, key: &[u8]) -> usize {
    stripe_for(key)
  }

  /// 获取共享锁（读路径；guard 存续期间阻止索引被丢弃；async 上下文竞争
  /// 时在 await 点让出线程，绝不自旋挂起）。
  #[inline]
  pub async fn acquire_shared(&self, key: &[u8]) -> RwLockReadGuard<'_, ()> {
    self.stripes[stripe_for(key)].0.read().await
  }

  /// 尝试非阻塞共享锁获取；竞争时返回 None。
  #[inline]
  pub fn try_acquire_shared(&self, key: &[u8]) -> Option<RwLockReadGuard<'_, ()>> {
    self.stripes[stripe_for(key)].0.try_read()
  }

  /// 获取独占锁（VREM / VSETATTR / 重建等写路径；async 上下文竞争时在
  /// await 点让出线程，排空在途读者后继续，锁纪律见模块头）。
  #[inline]
  pub async fn acquire_exclusive(&self, key: &[u8]) -> RwLockWriteGuard<'_, ()> {
    self.write_at_stripe(stripe_for(key)).await
  }

  /// 尝试非阻塞独占锁获取；竞争时返回 None。
  #[inline]
  pub fn try_acquire_exclusive(&self, key: &[u8]) -> Option<RwLockWriteGuard<'_, ()>> {
    self.stripes[stripe_for(key)].0.try_write()
  }

  /// 按条带号获取独占锁（RENAME 双键定序加锁的对偶入口）。
  #[inline]
  pub async fn write_at_stripe(&self, stripe: usize) -> RwLockWriteGuard<'_, ()> {
    self.stripes[stripe].0.write().await
  }
}

/// RENAME 双键条带独占锁守卫（C# txnManager 对旧/新键排他锁的
/// [`VectorSetLocks`] 对偶）：同条带单次获取，异条带按条带序号定序获取，
/// 杜绝 A→B / B→A 交叉重命名互等死锁。
pub struct VectorSetKeyLocks<'a> {
  guards: [Option<RwLockWriteGuard<'a, ()>>; 2],
}

impl<'a> VectorSetKeyLocks<'a> {
  /// 按条带定序获取两键的独占锁（同条带只取一次；async 上下文竞争时在
  /// await 点让出，定序协议保证无交叉互等）。
  pub async fn acquire(locks: &'a VectorSetLocks, old_key: &[u8], new_key: &[u8]) -> Self {
    let old_stripe = locks.stripe_for(old_key);
    let new_stripe = locks.stripe_for(new_key);
    if old_stripe == new_stripe {
      return Self {
        guards: [Some(locks.write_at_stripe(old_stripe).await), None],
      };
    }
    let (first, second) = (old_stripe.min(new_stripe), old_stripe.max(new_stripe));
    Self {
      guards: [
        Some(locks.write_at_stripe(first).await),
        Some(locks.write_at_stripe(second).await),
      ],
    }
  }
}

impl Drop for VectorSetKeyLocks<'_> {
  fn drop(&mut self) {
    // 按加锁逆序释放（后取的高位条带先还），对齐锁序协议的逆序解锁惯例
    self.guards[1] = None;
    self.guards[0] = None;
  }
}

/// `read_vector_index_core` 的三态结果（对齐 C# 的 status + wouldBlock 出参）。
#[derive(Debug)]
pub enum ReadIndexOutcome<'a> {
  /// 命中：索引记录 + 持有共享锁的守卫（存续期间索引不可被丢弃）。
  Hit(Index, RwLockReadGuard<'a, ()>),
  /// 键不存在（C# readRes != OK）。
  NotFound,
  /// 锁竞争：调用方应让出线程后异步重试（仅 non_blocking 形态）。
  WouldBlock,
  /// 自动重建失败（对齐 C# RecreateIndex 异常上抛会话 catch → 命令报错）。
  /// 登记记录维持 ptr=0 原状，后续访问自动重试重建。
  Failed,
}

/// 键哈希条带定位（whasher::fast_hash 单源承担，对标 libs/server/Resp/Vector/VectorManager.Locking.cs:97
/// 经 GetKeyHash → GarnetKeyComparer → Utility.HashBytes 的条带定位职责，位与 STRIPE_MASK，O(1) 零分配）。
#[inline]
pub fn stripe_for(key: &[u8]) -> usize {
  (whasher::fast_hash(key) as usize) & STRIPE_MASK
}

/// 新建索引的几何参数（对齐 C# ReadOrCreateVectorIndex 自 parseState 提取的字段集）。
#[derive(Debug, Clone, Copy)]
pub struct CreateIndexParams {
  /// 键所属 hash slot（创建时关联，供后续迁移）。
  pub hash_slot: u16,
  /// 向量维度。
  pub dims: u32,
  /// 降维后维度（0 = 不降维）。
  pub reduce_dims: u32,
  /// 量化类型。
  pub quant: VectorQuantType,
  /// 构建期探索因子。
  pub build_exploration_factor: u32,
  /// 每层链接数（M）。
  pub num_links: u32,
  /// 距离度量。
  pub distance_metric: VectorDistanceMetricType,
}

impl CreateIndexParams {
  /// 转换为索引几何配置。
  pub fn index_config(&self) -> IndexConfig {
    IndexConfig {
      dims: self.dims,
      reduce_dims: self.reduce_dims,
      quant_type: self.quant,
      distance_metric: self.distance_metric,
      build_exploration_factor: self.build_exploration_factor,
      num_links: self.num_links,
    }
  }
}

/// 重建完成后的量化调度（对齐 C# RecreateIndex 的 out requestQuantization 通道注入）。
///
/// `rk` 为登记表复合键（量化通道载荷全程工作在复合键域）。
fn request_quantization_if_needed<S: StoreCallbacks>(
  channel: &QuantizationChannel,
  manager: &VectorManager<S>,
  rk: &[u8],
  context: u64,
) {
  if manager.service.needs_quantization(context) {
    let _ = channel.push(QuantizationState::new(
      rk.to_vec(),
      QuantizationStep::BuildQuantizationTable,
      0,
    ));
  }
}

impl<S: StoreCallbacks> VectorManager<S> {
  /// libs/server/Resp/Vector/VectorManager.Locking.cs:ReadVectorIndex
  ///
  /// 只读取向量集合索引（不创建）；需重建时自动重建。
  /// 命中时以共享锁守卫返回（guard 存续期间索引不可被删除）。
  pub async fn read_vector_index<'a>(
    &'a self,
    prefix: &[u8],
    key: &[u8],
  ) -> (Option<Index>, Option<RwLockReadGuard<'a, ()>>) {
    match self.read_vector_index_core(prefix, key, false).await {
      ReadIndexOutcome::Hit(index, guard) => (Some(index), Some(guard)),
      // 重建失败映射为缺读：重放层对 None 自有 "Failed to read Vector Set
      // index during AOF replay" 错误出口，错误不被吞（C# 异常上抛同效）
      ReadIndexOutcome::NotFound | ReadIndexOutcome::WouldBlock | ReadIndexOutcome::Failed => {
        (None, None)
      }
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Locking.cs:ReadVectorIndexCore
  ///
  /// 锁升降级读协议：共享读 →（需重建）→ 独占重建 → [`RwLockWriteGuard::downgrade`]
  /// 原子降级共享直接命中返回。`non_blocking == true` 时任一锁竞争都以
  /// [`ReadIndexOutcome::WouldBlock`] 让步返回（调用方让出线程后异步重试，
  /// 对齐 C# 线程池协作语义）。
  /// 锁轴与登记表查询均为复合键域（C# 每库实例隔离的 rust 单例对偶）。
  pub async fn read_vector_index_core<'a>(
    &'a self,
    prefix: &[u8],
    key: &[u8],
    non_blocking: bool,
  ) -> ReadIndexOutcome<'a> {
    let rk = registry_key(prefix, key);
    let key = rk.as_slice();

    // 阶段 1：共享读
    let shared = if non_blocking {
      let Some(guard) = self.vector_set_locks.try_acquire_shared(key) else {
        return ReadIndexOutcome::WouldBlock;
      };
      guard
    } else {
      self.vector_set_locks.acquire_shared(key).await
    };

    let Some(bytes) = self.stored_index_of(key) else {
      // 读取未命中（C# readRes != OK）：无锁返回
      return ReadIndexOutcome::NotFound;
    };

    // 单次解析：若已初始化直接命中返回，避免二次反序列化开销
    if let Some(index) = Index::from_bytes(&bytes)
      && index.index_ptr != 0
    {
      return ReadIndexOutcome::Hit(index, shared);
    }
    drop(shared);

    // 阶段 2：竞争独占（C# 经 TryPromoteSharedLock 原子升级；共享锁无
    // 原地升级原语，以释放后竞争 + 独占下复核对齐）。
    let exclusive = if non_blocking {
      let Some(guard) = self.vector_set_locks.try_acquire_exclusive(key) else {
        return ReadIndexOutcome::WouldBlock;
      };
      guard
    } else {
      self.vector_set_locks.acquire_exclusive(key).await
    };

    let Some(bytes) = self.stored_index_of(key) else {
      return ReadIndexOutcome::NotFound;
    };

    let index = match Index::from_bytes(&bytes) {
      // 他人已完成重建：原子降级直接命中返回
      Some(index) if index.index_ptr != 0 => {
        return ReadIndexOutcome::Hit(index, RwLockWriteGuard::downgrade(exclusive));
      }
      Some(index) => index,
      None => Index::default(),
    };

    // 阶段 3：独占下重建原生索引并写回记录
    // （对齐 C# arg1 = RecreateIndexArg 的 RMW 写回路径；重建失败对齐 C#
    // RecreateIndex 异常上抛：报错出站，登记记录维持 ptr=0 原状。
    // 独占锁为异步锁：守卫随 async 栈帧跨登记写透的 `.await` 存活，
    // 重建写透让出线程期间并发删除/读被排挡在锁外——原 parking_lot
    // 同步锁形态下这正是 inline_write 收割得以存在的死锁根源，async
    // 锁替换后该约束消除，见模块头）
    let Ok(rebuilt) = self.recreate_index_locked(key, &index).await else {
      return ReadIndexOutcome::Failed;
    };

    // 原子降级写→读返回：重建完成至句柄交还调用方零时序空隙，并发
    // DEL/UNLINK/FLUSHDB/RENAME 无法抢入销毁索引、摘除登记记录
    //（C# 因 ReadOptimizedLock 无降级原语，释放共享后重读同效，
    // 但存在穿透窗口）
    ReadIndexOutcome::Hit(rebuilt, RwLockWriteGuard::downgrade(exclusive))
  }

  /// libs/server/Resp/Vector/VectorManager.Locking.cs:ReadOrCreateVectorIndex
  ///
  /// 读取向量集合索引；缺失或需重建时（在 `create` 提供建造参数的前提下）
  /// 创建/重建。`create == None` 对齐 InitialUpdater: NO 的 arg 语义：
  /// 缺失即报错。命中/建成后以共享锁守卫返回（C# 重建后同样降级共享，
  /// 避免持独占锁执行搜索；rust 以 [`RwLockWriteGuard::downgrade`] 原子
  /// 降级承接，独占写回完成至读守卫交还零空隙）。
  pub async fn read_or_create_vector_index<'a>(
    &'a self,
    prefix: &[u8],
    key: &[u8],
    create: Option<&CreateIndexParams>,
  ) -> Result<(Index, RwLockReadGuard<'a, ()>), VectorManagerResult> {
    let rk = registry_key(prefix, key);
    let key = rk.as_slice();
    let mut demand_exclusive = false;

    loop {
      // 上轮共享升级失败：本轮直接以独占进入（对齐 C# takeExclusiveLock）
      if demand_exclusive {
        let exclusive = self.vector_set_locks.acquire_exclusive(key).await;
        let index = self.create_or_recreate_under_exclusive(key, create).await?;
        return Ok((index, RwLockWriteGuard::downgrade(exclusive)));
      }

      // 阶段 1：共享读
      let shared = self.vector_set_locks.acquire_shared(key).await;
      if let Some(bytes) = self.stored_index_of(key)
        && let Some(index) = Index::from_bytes(&bytes)
        && index.index_ptr != 0
      {
        return Ok((index, shared));
      }
      drop(shared);

      // 缺失或需重建 → 尝试非阻塞升级独占；失败则本轮以独占重入
      if let Some(exclusive) = self.vector_set_locks.try_acquire_exclusive(key) {
        let index = self.create_or_recreate_under_exclusive(key, create).await?;
        return Ok((index, RwLockWriteGuard::downgrade(exclusive)));
      }
      demand_exclusive = true;
    }
  }

  /// 独占锁内：命中且无需重建 → 原样返回；需重建 → 重建；缺失 → 按参数创建或报错。
  async fn create_or_recreate_under_exclusive(
    &self,
    key: &[u8],
    create: Option<&CreateIndexParams>,
  ) -> Result<Index, VectorManagerResult> {
    // 缺记录与解析失败同走创建/拒建臂（两臂逐位同形，Option 链折叠单源，语义不变）
    let record = self.stored_index_of(key);
    match record.as_ref().and_then(|bytes| Index::from_bytes(bytes)) {
      Some(index) if index.index_ptr != 0 => Ok(index),
      Some(index) => self.recreate_index_locked(key, &index).await,
      None => match create {
        // 缺失且不允许建桩（InitialUpdater: NO）
        None => Err(VectorManagerResult::Invalid),
        Some(params) => self.create_index_locked(key, params).await,
      },
    }
  }

  /// libs/server/Resp/Vector/VectorManager.Index.cs:RecreateIndex
  ///
  /// 独占锁内执行原生索引重建并写回记录（对齐 RecreateIndexArg 路径）。
  /// `key` 为复合登记键。
  ///
  /// 重建失败转错误帧返回且不落登记桩，对齐 C# RecreateIndex 异常上抛会话
  /// catch（RMW 不执行、登记记录维持 ptr=0 原状，后续访问自动重试重建）。
  /// 降级保留旧行为（吞错照常落 index_ptr=1 桩）不可取：两个重建调用点均以
  /// index_ptr == 0 为前提（原生索引已不在内存），失败后照常落桩即登记表与
  /// 原生索引失配的幽灵桩，后续 VADD/VSIM 打到不存在索引上行为未定义。
  async fn recreate_index_locked(
    &self,
    key: &[u8],
    index: &Index,
  ) -> Result<Index, VectorManagerResult> {
    if let Err(err) = self
      .service
      .create_index(index.context, index.index_config(), self.callbacks.clone())
      .await
    {
      log::error!("recreate_index_locked: 原生索引重建失败，登记记录不落桩: {err:?}");
      return Err(VectorManagerResult::Invalid);
    }
    let mut rebuilt = *index;
    rebuilt.index_ptr = 1;
    if !self.put_stored_index(key, &rebuilt.to_bytes()).await {
      log::error!("recreate_index_locked: 索引登记旁路记录写透失败");
      return Err(VectorManagerResult::Invalid);
    }
    request_quantization_if_needed(&self.quantization_channel, self, key, rebuilt.context);
    Ok(rebuilt)
  }

  /// libs/server/Resp/Vector/VectorManager.Index.cs:CreateIndex
  ///
  /// 独占锁内创建新索引（对齐 CreateIndexArg 路径：分配上下文 + 全几何落记录）。
  /// `key` 为复合登记键。
  async fn create_index_locked(
    &self,
    key: &[u8],
    params: &CreateIndexParams,
  ) -> Result<Index, VectorManagerResult> {
    // 上下文 0 非法（保留），分配失败须报错而非落 0 桩记录
    let context = self
      .next_vector_set_context(params.hash_slot)
      .await
      .ok_or(VectorManagerResult::Invalid)?;
    if let Err(err) = self
      .service
      .create_index(context, params.index_config(), self.callbacks.clone())
      .await
    {
      // 创建失败回收刚占位的上下文（in_use 清零 + 写透，槽位立即可复用），
      // 杜绝存储抖动期每次失败首插各漏一个 8 宽槽位的单调泄漏
      self.release_vector_set_context(context).await;
      log::error!("create_index_locked: 原生索引构建失败，上下文 {context} 已回收: {err:?}");
      return Err(VectorManagerResult::Invalid);
    }
    let index = Index {
      context,
      index_ptr: 1,
      dimensions: params.dims,
      reduce_dims: params.reduce_dims,
      num_links: params.num_links,
      build_exploration_factor: params.build_exploration_factor,
      quant_type: params.quant,
      distance_metric: params.distance_metric,
      flags: VectorSetFlags::NONE,
    };
    if !self.put_stored_index(key, &index.to_bytes()).await {
      self.release_vector_set_context(context).await;
      log::error!("create_index_locked: 索引登记旁路记录写透失败，上下文 {context} 已回收");
      return Err(VectorManagerResult::Invalid);
    }
    request_quantization_if_needed(&self.quantization_channel, self, key, context);
    Ok(index)
  }

  /// 存储层读取承接：(会话前缀, 用户键) → 索引记录字节（域内登记表）。
  pub fn read_stored_index(&self, prefix: &[u8], key: &[u8]) -> Option<[u8; INDEX_SIZE_BYTES]> {
    let rk = registry_key(prefix, key);
    self.stored_index_of(rk.as_slice())
  }

  /// 存储层写入承接：(会话前缀, 用户键) → 索引记录字节。
  pub async fn write_stored_index(
    &self,
    prefix: &[u8],
    key: &[u8],
    bytes: &[u8; INDEX_SIZE_BYTES],
  ) -> bool {
    let rk = registry_key(prefix, key);
    self.put_stored_index(rk.as_slice(), bytes).await
  }

  /// 登记表复合键直读单点（锁协议/回收/迁移等已持复合键的内部轴键面）。
  pub(crate) fn stored_index_of(&self, rk: &[u8]) -> Option<[u8; INDEX_SIZE_BYTES]> {
    self.key_index_registry.pin().get(rk).copied()
  }

  /// 登记表复合键直写单点（内部轴键面；写透 KeyTag::VectorRegistry
  /// 旁路记录——C# 索引记录驻主存随检查点持久的 rust 等价承接）。
  /// 返回装箱 future 而非 async fn：`RegistryPersistence::put` 的会话
  /// 引用在**同步调用点**取定并随 future 跨 await 自持，引用存活期由会话
  /// 属主承担（论证同 `persist_registry_index`：兜底专用会话守卫须持有至
  /// 写透完成，禁止 await 前让位）。
  pub(crate) fn put_stored_index(
    &self,
    rk: &[u8],
    bytes: &[u8; INDEX_SIZE_BYTES],
  ) -> Pin<Box<dyn Future<Output = bool> + Send + '_>> {
    self.key_index_registry.pin().insert(rk.to_vec(), *bytes);
    let write = self.persist_registry_index(rk, bytes);
    let rk = rk.to_vec();
    Box::pin(async move {
      let ok = write.await;
      if !ok {
        self.key_index_registry.pin().remove(&rk);
      }
      ok
    })
  }

  /// 登记表复合键摘除单点（删除/回收共用，内部轴键面；摘除写透旁路记录
  /// 墓碑，重启回建后不再复活。写透真异步与取会话时点论证同
  /// [`Self::put_stored_index`]）。返回摘除是否成功（内存登记表 miss 或
  /// 写透未成皆为 false）；失败模式——登记表 miss、持久化无会话、写透
  /// 失败——不按臂拆分，单点归入 [`Self::vector_registry_remove_failures`]
  /// 结构化计数（INFO bg_task_health 可见），对标 C#
  /// ReplicateVectorSetRemove 失败 throw 的错误必达口径（rust 以 bool +
  /// 计数承接 throw）；log::error 各臂保留。
  pub(crate) fn remove_stored_index<'a>(
    &'a self,
    rk: &'a [u8],
  ) -> Pin<Box<dyn Future<Output = bool> + Send + 'a>> {
    let removed = self.key_index_registry.pin().remove(rk).is_some();
    if !removed {
      log::error!("remove_stored_index 失败: 键不存在于内存登记表: {rk:?}");
    }
    let write = self.evict_registry_index(rk);
    Box::pin(async move {
      let ok = write.await;
      if !removed || !ok {
        // 幽灵复活危害面（内存已摘而盘上墓碑缺）常驻计数可巡检
        self.vector_registry_remove_failures.fetch_add(1, Relaxed);
      }
      removed && ok
    })
  }
}
