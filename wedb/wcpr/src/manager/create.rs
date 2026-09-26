use std::path::Path;

use compio::{
  fs::{File, create_dir_all, metadata, rename},
  io::AsyncWriteAtExt,
  time::sleep,
};
use log::{debug, info, warn};
use wbase::time::now_ms;
use wdev::Device;
use wepoch::LightEpoch;

use super::{
  CkptGateState, CprStore, acquire, candidate_token, ensure_epoch_unprotected, ensure_not_growing,
  issue_token_after, list_checkpoints, purge_checkpoint, sync_checkpoint_dir, sync_dir_tree,
};
use crate::{
  error::Result,
  index_ckpt::write_index_checkpoint,
  meta::{
    CheckpointMeta, CheckpointType, FORMAT_VERSION, HlogMeta, meta_filename, meta_tmp_filename,
    token_to_base32,
  },
};

/// 创建持久化 Checkpoint 快照
///
/// 核心流程（对标 C# FullCheckpointSM 状态机 REST → PREPARE → IN_PROGRESS →
/// WAIT_FLUSH → COMPLETE → REST 的闭环语义）：
/// 1. 生成全局唯一 128 位快照版本 Token。
/// 2. PREPARE：先于一切相位捕获 beginAddress 快照（对标 C#
///    HybridLogCheckpointSMTask.cs:38-39，杜绝并发紧缩推 begin 越过索引快照条目
///    地址致恢复静默悬挂）；VersionShift 外层屏障阻塞全部 BfTree 树写入，Epoch
///    排空屏障等待全部前置纪元在途操作完成（含「树写已落、meta.size 增量在途」
///    的 RI 写）。
/// 3. WAIT_INDEX_CHECKPOINT：原子刷写 HashIndex 快照 `index_<token>.ckpt`（条目
///    地址全部不小于入口捕获的 begin、且全部小于其后捕获的截断点，恢复截断净空
///    零误伤、冷读窗恒可及）。
/// 4. WAIT_FLUSH 入口：同步段为所有 RangeIndex 执行 CPR 快照，随后立即捕获一致性
///    截断点 TailAddress（finalLogicalAddress），快照与 hlog tail 严格一致前缀；
///    封印只读边界并等待传播，刷写全部脏页并同步设备覆盖至截断点。
/// 5. ClearCheckpointBarrier 放行树写入。
/// 6. PERSISTENCE_CALLBACK：写入 `checkpoint_<token>.meta` 元数据文件（最后落盘，
///    崩溃时未完成检查点绝不会出现在恢复视图中）。
///
/// # 调用方契约
/// - 必须在纪元保护区外调用（会话操作作用域之外）：调用方自持纪元会使排空屏障
///   永远无法完成，本方法以 [`crate::error::Error::CheckpointWhileEpochProtected`] fail-fast
///   拒绝（对照 C#：Garnet CHECKPOINT 命令与 StoreWrapper.CompactionTaskAsync 周期紧缩任务均在会话作用域外串行
///   驱动检查点，契约靠外部调用约定成立；wedb 将其显式化为类型化错误）。
/// - 单线程测试等场景天然满足：会话操作守卫均为 RAII 作用域，操作返回即退出保护区。
/// - 索引在线扩容迁移中发起即时失败（返回 [`crate::error::Error::Host`]，零副作用：不签发 Token、
///   不创建目录；对标 C# StateMachineDriver 单槽互斥下 grow 在跑时检查点注册返回
///   false，见 `ensure_not_growing`），调用方按既有重试路径处理。
///
/// # 崩溃一致性与并发语义
/// - Token 签发的目录下界（floor）在实例闸门内计算：并发持有闸门的
///   [`create_checkpoint_with_token`] 可能正以调用方指定的大 Token 发布，
///   闸门外枚举目录会读到陈旧视图，签发值可能小于已发布 Token，颠倒
///   「目录内 Token 的大小序即版本新旧序」不变式（recover_latest/purge_outdated
///   依赖此不变式）；闸内计算覆盖跨进程重启后的墙钟回拨，签发闸门进程内串行，
///   低位另携进程判别盐（见 [`super::mint_low`]），并发创建跨进程亦不撞号。
/// - 快照一致性点与前台写入可见性边界：检查点严格限定在 RI 快照同步段完成后立即
///   捕获的 tail 时点（对标 C# WAIT_FLUSH 入口捕获 finalLogicalAddress 的时序），
///   HashIndex 快照先行且条目地址全部小于该截断点、RangeIndex 树快照经 VersionShift
///   外层屏障与该截断点刚性对齐（严格一致前缀：截断点之前的 meta 记录与树效果两侧
///   齐全，截断点之后的记录与树效果两侧同弃）。屏障放行后到 flush_all 结束期间，新起
///   会话仅能对 `[tail, ∞)` 做 RCU 追加（只读区已封印，`[0, tail)` 的字节绝无在途
///   改写）；这些超界追加可能随本次 flush_all 一并落盘并使 FlushedUntilAddress 超前
///   tail，恢复阶段按截断点 tail 丢弃全部超界索引；
///   文件集层面各 Token 互不相交，`recover` 仅读取某一 Token 的不可变文件集（原子 rename 发布），与并发
///   的创建/清理操作互不撕裂。Token 由签发闸门进程内串行签发、低位携进程判别盐
///   跨进程唯一（见 [`super::mint_low`]），对同一 Token 并发/重复调用
///   [`create_checkpoint_with_token`] 属调用方违约（失败清场与临时文件路径均按
///   Token 独占假设执行），须保证 Token 唯一。
pub async fn create_checkpoint<D: Device, S: CprStore<Device = D>>(
  store: &S,
  gate: &CkptGateState,
  checkpoint_dir: impl AsRef<Path>,
  cp_type: CheckpointType,
) -> Result<CheckpointMeta> {
  ensure_not_growing(store)?;
  ensure_epoch_unprotected(store)?;
  let dir = checkpoint_dir.as_ref();
  let _gate = acquire(gate).await;
  let floor = list_checkpoints(dir)?.last().copied().unwrap_or(0);
  let token = issue_token_after(candidate_token(), floor);
  // 自签发入口无宿主版本推进时刻，模糊区地板退化为闸内即取 tail（快照扫描前
  // 的最近时点，与宿主驱动入口的「版本推进瞬间 tail」同一构造性下界）
  let index_start = store.tail_address();
  create_gated(store, dir, cp_type, token, index_start).await
}

/// 使用指定 Token 创建持久化 Checkpoint 快照
///
/// 同实例串行化（见 [`super::acquire`]）：同一存储引擎实例上 SAVE 手动触发与
/// 周期快照并发调用时依次排队执行，杜绝两次检查点在宿主状态机上的并发叠加；
/// 等待者以异步互斥挂起排队，精确事件唤醒，不阻塞任何 reactor 线程且零空转
/// 开销。闸门粒度随宿主引擎实例（`gate` 由宿主持有，对标 C#
/// `GarnetDatabase.CheckpointingLock` 实例锁），不同实例互不互斥。
///
/// `index_start_logical_address`：本轮快照的模糊区地板，必须精准等于宿主
/// 存储版本推进那一刻的 tail 逻辑地址（对标 Tsavorite PREPARE 阶段
/// `startLogicalAddress = GetTailAddress()` 与版本推进同点捕获）——版本推进
/// 窗口内追加的记录携带 IN_NEW_VERSION_BIT 且地址恒不小于该地板，恢复内核
/// [`super::recover::run_recovery_kernel`] 据同一地板执行 undoNextVersion
/// 回滚；调用方（宿主版本推进入口）经 wkv 版本切换窗口 API 取得并传入。
///
/// 失败清场：任一阶段失败（如磁盘满 ENOSPC、注入故障）时 best-effort 回收本
/// Token 的全部物理文件（含半截 `.tmp` 与孤儿 token 子目录）——元数据最后落盘
/// 保证失败检查点绝无进入恢复视图的可能，清场仅回收磁盘空间。注意该 Token 若
/// 恰有历史文件集亦会被一并回收，对同一 Token 的重复/复用调用属调用方违约。
pub async fn create_checkpoint_with_token<D: Device, S: CprStore<Device = D>>(
  store: &S,
  gate: &CkptGateState,
  checkpoint_dir: impl AsRef<Path>,
  cp_type: CheckpointType,
  token: u128,
  index_start_logical_address: u64,
) -> Result<CheckpointMeta> {
  ensure_not_growing(store)?;
  ensure_epoch_unprotected(store)?;
  let dir = checkpoint_dir.as_ref();
  let _gate = acquire(gate).await;
  create_gated(store, dir, cp_type, token, index_start_logical_address).await
}

/// 检查点临界区 RAII 守卫：任一退出路径（含中途失败）复位宿主独占标记
///
/// 构造晚于 `CkptGate`、作用域短于闸门（闸门在公共入口持），Drop 复位先于闸门
/// 释放执行，无裸窗；成功路径在本流程发布完成后显式清，此处复位幂等兜底。
/// 检查点闸门串行化下无并发检查点，但 `grow_index` 不走检查点闸门：显式退出后、
/// 本 Drop 二次复位前，并发扩容可抢占槽位进入 PrepareGrow，无条件 store 会把它
/// 打回 Rest 破坏事务屏障与切表序——`exit_checkpoint` 为仅清自身所置 Checkpoint
/// 相位的条件 CAS（对标 C# StateMachineDriver.cs:345-362 单次清槽契约），二次
/// 复位对扩容相位 no-op，双调幂等
struct CkptPhaseGuard<'a, S: CprStore>(&'a S);

impl<S: CprStore> Drop for CkptPhaseGuard<'_, S> {
  fn drop(&mut self) {
    self.0.exit_checkpoint();
  }
}

/// 闸门内创建主流程（调用方须已持有实例闸门）：创建 + 失败清场
async fn create_gated<D: Device, S: CprStore<Device = D>>(
  store: &S,
  dir: &Path,
  cp_type: CheckpointType,
  token: u128,
  index_start: u64,
) -> Result<CheckpointMeta> {
  // 检查点临界区独占（对标 C# StateMachineDriver.cs:167-190 单槽注册）：
  // 闸门已排除并发检查点，此处 CAS 失败当且仅当扩容在跑（或检查点槽位被他方
  // 占用）——自索引快照至 flush_all 全异步窗口期对外持有排他标记，并发
  // grow_index 的 Rest→PrepareGrow CAS 失败即拒，快照尺寸与 store_meta 不再撕裂
  store.enter_checkpoint()?;
  let _ckpt_phase = CkptPhaseGuard(store);
  let res = create_checkpoint_inner(store, dir, cp_type, token, index_start).await;
  if let Err(e) = &res {
    warn!("Checkpoint 创建失败，已回收本 Token 残留文件: token={token:#x}, err={e}");
    let _ = purge_checkpoint(dir, token);
  }
  res
}

/// RI 检查点屏障 RAII 守卫：任何退出路径（含提前失败）解除屏障
///
/// 成功路径在本流程 flush 完成后显式清屏，此处清屏幂等兜底
/// （`clear_checkpoint_barrier` 仅复位原子标志）；检查点闸门串行化下无并发检查点，
/// 重复清屏无副作用
struct RiBarrierGuard<'a, S: CprStore>(&'a S);

impl<S: CprStore> Drop for RiBarrierGuard<'_, S> {
  fn drop(&mut self) {
    self.0.clear_range_index_checkpoint_barrier();
  }
}

/// 检查点创建主流程（调用方须已持有闸门并完成 [`ensure_epoch_unprotected`] 契约校验，
/// 见 [`create_checkpoint_with_token`]）
///
/// 全量检查点状态机组装入口对标（索引 + 日志两后端一次到位）：
/// libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/Checkpoint.cs:Full
///
/// 阶段时序 1:1 对标 C# FullCheckpointSM（HybridLogCheckpointSMTask
/// GlobalBeforeEnteringState）：VersionShift 设 RI 外层屏障 → 排空旧版本在途操作 →
/// WAIT_INDEX_CHECKPOINT 先刷 HashIndex 快照 → FlushBegin 同步段快照全部 RangeIndex
/// 树后立即捕获一致性截断点 finalLogicalAddress → ShiftReadOnlyToTail 封印并等待
/// 传播 → flush 覆盖至截断点 → ClearCheckpointBarrier 放行树写入。屏障自设屏起
/// 跨越全部 await 持有至清屏，快照与 hlog tail 形成严格一致前缀。
async fn create_checkpoint_inner<D: Device, S: CprStore<Device = D>>(
  store: &S,
  dir: &Path,
  cp_type: CheckpointType,
  token: u128,
  index_start: u64,
) -> Result<CheckpointMeta> {
  // PREPARE 段最先捕获 beginAddress（对标 C# HybridLogCheckpointSMTask.cs:38-39
  // GlobalBeforeEnteringState(PREPARE)：`info.beginAddress = hlogBase.BeginAddress`
  // 先于 VersionShift 屏障、纪元排空与 WAIT_INDEX_CHECKPOINT 索引快照刷写等一切
  // 相位；PERSISTENCE_CALLBACK 只序列化已捕获快照）：构造性次序「落盘 begin <=
  // 索引快照扫描时点 BeginAddress <= 快照全部条目地址」由 begin 单调推进与「存活
  // 记录地址恒 >= 当期 begin」不变式闭合。紧缩链不经检查点相位互斥（wkv try_compact
  // 无 Checkpoint 相位门、常驻回收轮 200ms 节奏驱动；C# 紧缩同样不经驱动注册可并发
  // 直调 ShiftBeginAddress，防失恰靠本捕获次序而非互斥，rust 同不新增加锁）：若 begin
  // 迟于索引快照固化采样（旧形态第 3 步同点采 cp_begin），窗口内 compact_with_filter
  // 收尾 shift_begin_address 全量推进逻辑 begin 并连带推 head，落盘 meta 即出现
  // 「begin/head 合法（<= tail）却越过快照已固化条目地址 a」——恢复装载窗 [head, tail)
  // 不含 a、冷读判 addr < begin 失效、重放窗 [index_start, tail) 不含老地址、紧缩
  // 搬迁旁路 AOF，四路皆无补救，存活键静默永久悬挂。begin 采样上移至一切相位之前
  // 即根治两子形态（a < begin <= index_start 静默悬挂与 begin > index_start 具名
  // 拒启）：cp_begin <= 入口 tail = index_start 恒成立，恢复校验族恒过。head 维持
  // 第 3 步采样（晚采仅扩大装载窗、无正确性危害，与 C# 分工同型；cp_begin <= cp_head
  // 由 begin <= head 不变式恒成立）。与 wcpr-hlog-meta-late-sampling-race 票互补：
  // 彼修 meta 撕裂拒启（begin/head > tail），本修快照条目悬挂（begin 合法但越过
  // 快照固化地址），两修复缺一不可。物理删段由第 10 步 release_history_until(落盘
  // begin) 钳制，修复后落盘 begin 变小、删段更保守、无副作用；快照内低于 cp_begin
  // 的陈旧同键旧槽无害（链上必有更新版本槽可达）。
  let cp_begin = store.begin_address();

  create_dir_all(dir).await?;

  // VersionShift 外层屏障（对标 C# OnCheckpoint(CheckpointTrigger::VersionShift) →
  // RangeIndexManager.SetCheckpointBarrier）：自一致性截断点捕获之前阻塞全部 BfTree
  // 树写入，直至 flush 完成后统一清屏（本守卫兜底任一提前失败路径，幂等）。写者
  // 等待为异步挂起（wkv 侧零预算探针 + compio sleep 轮询让出 reactor），屏障跨
  // await 持有不阻塞 compio worker。旧实现因写者同步忙自旋不可跨 await，屏障仅覆盖
  // 快照同步窗口：tail 捕获到快照之间落入的树写混进快照，而其 meta.size 增量记录
  // 落 hlog [tail, ∞) 被恢复截断丢弃，MetaValue.size 与树内容永久漂移（RI.COUNT O(1)
  // 直读失真、删空自愈误判/永不触发），非「多存不丢」的尽力持久化语义可豁免。
  store.set_range_index_checkpoint_barrier();
  let _ri_barrier = RiBarrierGuard(store);

  // 1. IN_PROGRESS：Epoch 排空屏障（对标 C# 状态机 TrackLastVersion + IN_PROGRESS 语义）：
  //    屏障已先行阻塞树写入，在途 RI 写（树写已落、meta.size 增量在途）在此全部落定，
  //    确保索引与日志对其后捕获的截断点趋于静稳，杜绝「数据页已刷盘但索引插入尚未
  //    提交」的丢失更新窗口。fence 纪元须在等待排空之前推进取得（见 [`wait_epoch_drain`]）
  debug!("CPR 检查点: 纪元排空屏障 (对标 C# Phase.IN_PROGRESS), token={token:#x}");
  let epoch = store.epoch();
  let fence_epoch = epoch.current_epoch();
  wait_epoch_drain(epoch, || epoch.is_safe_to_reclaim(fence_epoch)).await;

  // 2. WAIT_INDEX_CHECKPOINT：原子刷写 HashIndex 快照（对标 FullCheckpointSM
  //    WAIT_INDEX_CHECKPOINT 先于 WAIT_FLUSH 的阶段序：索引快照捕获的全部条目地址
  //    均小于其后捕获的截断点，恢复阶段截断净空永不误伤现存键）。纯 compio 异步定位
  //    I/O：io_uring 下磁盘 I/O 由内核完成，reactor 仅提交与收割完成事件，大索引刷盘
  //    不再需要线程池中转。传入 ReadCache 解析单口：指向易失读缓存的索引条目在快照前
  //    必须顺链回写为主日志真实地址（对标 libs/storage/Tsavorite/cs/src/core/Index/
  //    Tsavorite/Implementation/ReadCache.cs:SkipReadCacheBucket），否则恢复后这些键将
  //    永久不可见；走查触到换页驱逐过渡态（含链中段滑出）时由该端口以滑出地址就地
  //    等待清洗落定再重读槽位重探——C# 快照面靠 epoch.Resume() 冻结驱逐故无此等待臂，
  //    本 port 不冻结驱逐，改走与紧缩面同一套逐位置锚定等待口径（见
  //    `CprStore::skip_read_cache_with_wait`）
  //
  //    模糊区窗口起点 `index_start` 由调用方以入参给出（对标 IndexCheckpointSMTask.cs:34
  //    在版本推进相位入口取 `startLogicalAddress = GetTailAddress()`，见
  //    [`create_checkpoint_with_token`] 文档），本步不再就地重取 tail。漏网条目恒落于
  //    `[index_start, tail)` 的论证：上一步纪元排空先行静稳全部在途会话（其记录 CAS
  //    恒完成于快照扫描起跑之前，扫描必可见）；其后新起会话先占 tail 后落 CAS，记录
  //    地址恒不小于排空点 tail ≥ index_start。恢复期据同一地板对携带 IN_NEW_VERSION_BIT
  //    的新版记录执行 undoNextVersion 回滚、其余重插，见
  //    [`super::recover::run_recovery_kernel`] 单趟扫描内核。
  //    启用 AOF 的装配另有版本过滤重放覆盖同一区间（窗口写入携带检查点版本号、
  //    重放不跳过），与上述回滚严格互补：快照内新版记录被剔除，重放恰好生效一次。
  debug!("CPR 检查点: 索引快照刷写 (对标 C# Phase.WAIT_INDEX_CHECKPOINT), token={token:#x}");
  let index_meta =
    write_index_checkpoint(&store.index(), store.entry_count(), dir, token, &|slot| {
      store.skip_read_cache_with_wait(slot)
    })
    .await?;

  // 3. WAIT_FLUSH 入口：同步段快照全部 RangeIndex 后，在同一个无 await 区段内一次性
  //    捕获一致性元数据快照——截断点 tail 与 head 同点取值（1:1 对标
  //    libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotAllTreesForCheckpoint 与
  //    HybridLogCheckpointSMTask.cs:GlobalBeforeEnteringState(WAIT_FLUSH) 的相邻同步语句
  //    捕获：finalLogicalAddress = GetTailAddress() 紧接 headAddress = HeadAddress；
  //    beginAddress 更先在 PREPARE 段捕获，rust 已上移至本流程入口同步段，见函数首部）。
  //    屏障持续持有：快照段与截断点捕获之间无任何
  //    await（单核串行化，多核下仅剩清屏后写者的 µs 级窗口与 C# 同型），截断点之前的
  //    RI meta 记录与树效果两侧齐全、之后的记录与效果两侧同弃——严格一致前缀；索引快照
  //    条目地址全部小于截断点，恢复截断净空零误伤。
  //    采样点上移的收口意义：捕获时刻运行时不变式 begin <= head <= flushed <= tail 恒
  //    自洽，落盘元数据因此天然通过恢复侧地址校验族；本检查点生命周期内 head 的
  //    后续推进（前台写入回绕驱逐经 ensure_page_ready 自动推 head）绝不再被第 9 步
  //    现场重读。旧形态第 9 步现采
  //    head/begin，跨越第 5/6/8 步三个 await 的刷盘窗口（大库可达秒级）内 head/begin 可
  //    越过捕获 tail，产出 head>tail / begin>tail 的撕裂元数据：integrity_crc32 只封签
  //    采样结果本身拦不住时序撕裂，崩溃恢复时被 recover.rs 具名校验拒启、recover_latest
  //    逐代回退叠加本代发布后已截断的 AOF，旧检查点之后的写入静默永久丢失。
  //    C# 时序对照：本轮 BeginAddress 的物理推进推迟到 REST 段 CleanupLogCheckpoint
  //    （Checkpoint.cs:54-59 的 Log.ShiftBeginAddress）；rust 逻辑 begin 由紧缩链推进、
  //    窗下历史段物理删段由第 10 步 release_history_until 收口，两者均在采样点之外，
  //    不改写本快照取值（采样单点收口，不新增加锁；begin 越窗危害另由入口 PREPARE 段
  //    先采收口，见函数首部注释）。
  let snapshot_trees = store.take_range_index_checkpoints(dir, token)?;
  let tail = store.tail_address();
  let cp_head = store.head_address();
  debug!(
    "CPR 检查点: RangeIndex 快照与一致性元数据捕获 (对标 C# Phase.WAIT_FLUSH), token={token:#x}, snapshot_trees={snapshot_trees}, tail={tail:#x}, begin={cp_begin:#x}, head={cp_head:#x}"
  );

  // 4. 封印只读边界（对标 C# FoldOver WAIT_FLUSH 的 ShiftReadOnlyToTail）：先封印后
  //    刷盘，保证刷盘期间没有任何在途写入可以原位改写 `[0, tail)` 内已捕获字节
  store.shift_read_only_address(tail);

  // 5. 只读封印传播等待（对标 C# WAIT_FLUSH 入状态前的纪元屏障收割）：
  //    SafeReadOnlyAddress 推进至截断点且前置纪元完全排空后，刷盘方可安全进行
  let hlog = store.hlog();
  let fence_epoch = epoch.current_epoch();
  wait_epoch_drain(epoch, || {
    hlog.safe_read_only_address() >= tail && epoch.is_safe_to_reclaim(fence_epoch)
  })
  .await;

  // 6. WAIT_FLUSH：刷写 HybridLog 所有未落盘内存脏页至存储介质并同步设备
  //    （刷盘边界为活跃 tail ≥ 截断点：索引快照引用的全部记录必然落盘）
  store.flush_all().await?;

  // 7. ClearCheckpointBarrier（对标 FlushBegin 尾段 ClearCheckpointBarrier）：
  //    放行树写入。此后 RI 写的树效果不在快照内、其 meta 记录落 `[tail, ∞)` 被
  //    恢复截断丢弃，两侧同弃保持一致
  store.clear_range_index_checkpoint_barrier();

  // 8. 持久化 token 子目录树：RangeIndex 快照必须先于 meta 达到掉电持久
  //    （纯 compio 异步 fsync，全程零线程创建）
  let b32 = token_to_base32(token);
  let token_dir = dir.join(b32.as_str());
  if metadata(&token_dir).await.is_ok() {
    sync_dir_tree(&token_dir).await?;
  }

  // head 取第 3 步 WAIT_FLUSH 入口同点捕获的快照、begin 取流程入口 PREPARE 段先于
  // 一切相位捕获的快照（对齐 C# GlobalBeforeEnteringState：PREPARE 捕获 beginAddress、
  // WAIT_FLUSH 相邻同步语句捕获 finalLogicalAddress/headAddress），杜绝跨刷盘窗口
  // 现采被并发驱逐/紧缩推过截断点的时序撕裂与快照条目悬挂。
  // flushed_until 维持本步后采样：flushed 单调不减且恢复侧 recover.rs 以 flushed.min(tail)
  // 钳制在位，次序自洽（cp_head <= 第 3 步 flushed <= 本步 flushed；恢复装载 [head, tail)
  // 驻留窗口与 C# WAIT_FLUSH 入口捕获行为一致，无内存正确性影响）。tail 取捕获值。
  let hlog_meta = HlogMeta {
    begin_address: cp_begin,
    head_address: cp_head,
    flushed_until_address: store.hlog().flushed_until_address(),
    tail_address: tail,
  };

  let store_meta = store.checkpoint_store_meta();

  let mut meta = CheckpointMeta {
    token,
    cp_type,
    index_meta,
    index_start_logical_address: index_start,
    hlog_meta,
    store_meta,
    created_at: now_ms(),
    // 创建时未知 AOF 边界（CprStore 泛型不感知 AOF 域），由持有 AOF 的
    // 宿主经 publish_checkpoint_aof_address 在提交后补写
    checkpoint_aof_address: None,
    format_version: FORMAT_VERSION,
    integrity_crc32: 0,
  };
  // 发布前封签：封签之外仅封签字段自身例外，恢复时逐字段比对拦截落盘后篡改
  meta.seal();

  // 9. PERSISTENCE_CALLBACK：写入元数据文件并原子 rename（对标 C# Phase.PERSISTENCE_CALLBACK）
  debug!("CPR 检查点: 元数据落盘 (对标 C# Phase.PERSISTENCE_CALLBACK), token={token:#x}");

  let meta_bytes = meta.encode();
  let tmp_meta_path = dir.join(meta_tmp_filename(token));
  let final_meta_path = dir.join(meta_filename(token));

  let mut file = File::create(&tmp_meta_path).await?;
  file.write_all_at(meta_bytes, 0).await.0?;
  file.sync_all().await?;

  rename(&tmp_meta_path, &final_meta_path).await?;
  sync_checkpoint_dir(dir)?;

  // 10. 「拍检查点才真正删文件」（对标 C# CleanupLogCheckpoint：
  // libs/storage/Tsavorite/cs/src/core/Index/Recovery/Checkpoint.cs:58 的
  // Log.ShiftBeginAddress(beginAddress, truncateLog: true)）：meta 已落盘发布，
  // 本检查点重放窗 [begin, tail) 自此冻结受保，窗下历史段放行物理回收——
  // 抬升删段地板并补收此前被 shift_begin_address 钳制延后的越窗段。
  // 失败仅告警不上抛：发布已完成，补收失败由下一轮移位紧缩按新地板重试，
  // 正确性不受影响（磁盘卫生延迟，非一致性问题）
  if let Err(e) = store
    .hlog()
    .release_history_until(meta.hlog_meta.begin_address)
    .await
  {
    warn!("检查点窗下历史段补收失败（下一轮紧缩按新地板补收）: {e}");
  }

  // 检查点临界区显式退出（对标 RiBarrierGuard 的显式清屏 + Drop 兜底分工）：
  // 元数据已发布、窗下补收已落定，放行并发扩容；失败路径由 CkptPhaseGuard
  // Drop 兜底复位，复位幂等
  store.exit_checkpoint();

  info!(
    "成功创建 Checkpoint: token={token:#x}, type={cp_type:?}, entry_count={}, tail={tail:#x}",
    index_meta.entry_count
  );

  Ok(meta)
}

/// 纪元排空屏障：等待前置纪元的在途操作全部排空（检查点域专用实现）
///
/// 版本边界事务排空协议对标（C# 检查点阶段内 TrackLastVersion 记录版本并等待
/// 活跃事务清零方得推进；rust 检查点无独立版本计数，纪元排空即同一语义）：
/// libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/StateMachineDriver.cs:TrackLastVersion
///
/// 对标 C# 状态机驱动器对全部阶段转换的单点纪元包裹
/// `libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/StateMachineDriver.cs` 的
/// `epoch.BumpCurrentEpoch(() => MakeTransitionWorker(nextState))`：C# 的「推进纪元并
/// 等前置排空」在检查点流程中只有一个表达位置，谓词由阶段自身决定。
///
/// 实现收口为转调 [`wepoch::wait_condition_async`] 单原语，参数化档位如下：
/// - `epoch = None` 且 `allow_protected = false`：检查点调用方入口已由
///   [`ensure_epoch_unprotected`] 强制保证非受保护态，无需临时让渡重入；
/// - `condition = fence_drained`：外层判定前置 fence 纪元已排空或只读已入盘；
/// - `on_step = |backoff| epoch.drain()`：每轮退避步进时收割就绪延迟动作；
/// - `timeout = None`：无超时档，活性依赖入口无保护契约与系统其余线程的有穷退出；
/// - `sleeper = compio::time::sleep`：通过 compio 异步执行器实现自适应退避让渡。
///
/// # 前提一：fence 纪元须在等待前推进取得
/// `fence_drained` 比较的 fence 须等于本阶段要排空的纪元基线，由调用方在调用前先经
/// `epoch.bump_current_epoch()` 推进取得，以确保屏障等待严格覆盖推进点之前的全部在途事务。
///
/// # 前提二：调用方必须不在纪元保护区
/// 本屏障以 `allow_protected = false` 档运行，无超时，活性依赖入口契约：公共入口
/// [`ensure_epoch_unprotected`] 在闸门前以 [`crate::error::Error::CheckpointWhileEpochProtected`]
/// fail-fast 拦截保护区内调用（见 [`create_checkpoint`] 的「调用方契约」）。调用线程
/// 若自钉旧纪元，谓词永假导致死锁。
///
/// # 取消语义
/// 外层 Future 若在 await 挂起点被 Drop（上层取消），底层 [`wepoch::wait_condition_async`]
/// 具备取消安全。未完成的检查点既不落元数据 rename 也不落 Err 清场，由后续
/// `purge_all` 全量残留清扫兜底回收半截文件，取消只浪费临时空间、不破坏一致性。
///
/// # 与仓内其他纪元等待原语的口径分工
/// 全仓纪元等待统一基于 `wepoch` 单原语分档：
/// - `whlog` 侧日志截断与刷盘屏障转调 [`wepoch::wait_condition_async`]，使用
///   `allow_protected = true` 档（由内部 RAII 守卫安全让渡并重入保护区）；
/// - 同步阻塞等待 [`LightEpoch::bump_and_wait`] 转调 [`wepoch::wait_condition_sync`]；
/// - 本函数转调 [`wepoch::wait_condition_async`]，按 `allow_protected = false` 档运行。
async fn wait_epoch_drain(epoch: &LightEpoch, fence_drained: impl Fn() -> bool) {
  epoch.bump_current_epoch();
  wepoch::wait_condition_async(
    None,
    false,
    fence_drained,
    |_backoff| {
      epoch.drain();
    },
    None,
    sleep,
  )
  .await;
  epoch.drain();
}

/// 补写检查点覆盖的 AOF 边界地址至元数据（快照提交后的第二阶段持久化）
///
/// 在 garnet 中的相对路径:libs/server/GarnetCheckpointManager.cs:GetCookie
/// （C# 检查点提交时经 GetCookie 把 CurrentSafeAofAddress 序列化进 cookie 随
/// 元数据落盘——多子日志形态走 `CurrentSafeAofAddress.Serialize(writer)`
/// 全向量；rust 检查点内核 CprStore 泛型不感知 AOF 域，边界由持有共享 AOF 的
/// 宿主在快照发布后补写：读元数据 → 校验 token/版本/封签完整性 → 回填字段 →
/// 重算封签 → 原子 rename。补写窗口内崩溃则字段保持 None，单机恢复不依赖该
/// 字段，正确性不受影响。入参取向量切片：按物理子日志逐位传入覆盖边界，
/// wcpr 底层 crate 不反向依赖 waof 的 AofAddress）
pub async fn publish_checkpoint_aof_address(
  checkpoint_dir: impl AsRef<Path>,
  token: u128,
  addresses: &[u64],
) -> Result<()> {
  use compio::fs::read;

  let dir = checkpoint_dir.as_ref();
  let meta_path = dir.join(meta_filename(token));
  let mut meta = CheckpointMeta::verify_sealed(&read(&meta_path).await?, token)?;
  meta.checkpoint_aof_address = Some(addresses.to_vec());
  meta.seal();

  let meta_bytes = meta.encode();
  let tmp_meta_path = dir.join(meta_tmp_filename(token));
  let mut file = File::create(&tmp_meta_path).await?;
  file.write_all_at(meta_bytes, 0).await.0?;
  file.sync_all().await?;
  rename(&tmp_meta_path, &meta_path).await?;
  sync_checkpoint_dir(dir)
}
