use std::path::Path;

use compio::{
  fs::{File, create_dir_all, metadata, rename},
  io::AsyncWriteAtExt,
};
use crossfire::oneshot::oneshot;
use log::{debug, info, warn};
use wbase::time::now_ms;
use wdev::Device;
use wepoch::LightEpoch;

use super::{
  CprStore, candidate_token, ensure_epoch_unprotected, ensure_not_growing, issue_token_after,
  list_checkpoints, lock_ckpt_gate, purge_checkpoint, sync_checkpoint_dir, sync_dir_tree,
};
use crate::{
  error::{Error, Result},
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
/// 2. PREPARE：VersionShift 外层屏障阻塞全部 BfTree 树写入，Epoch 排空屏障等待
///    全部前置纪元在途操作完成（含「树写已落、meta.size 增量在途」的 RI 写）。
/// 3. WAIT_INDEX_CHECKPOINT：原子刷写 HashIndex 快照 `index_<token>.ckpt`（条目
///    地址全部小于其后捕获的截断点，恢复截断净空零误伤）。
/// 4. WAIT_FLUSH 入口：同步段为所有 RangeIndex 执行 CPR 快照，随后立即捕获一致性
///    截断点 TailAddress（finalLogicalAddress），快照与 hlog tail 严格一致前缀；
///    封印只读边界并等待传播，刷写全部脏页并同步设备覆盖至截断点。
/// 5. ClearCheckpointBarrier 放行树写入。
/// 6. PERSISTENCE_CALLBACK：写入 `checkpoint_<token>.meta` 元数据文件（最后落盘，
///    崩溃时未完成检查点绝不会出现在恢复视图中）。
///
/// # 调用方契约
/// - 必须在纪元保护区外调用（会话操作作用域之外）：调用方自持纪元会使排空屏障
///   永远无法完成，本方法以 [`Error::CheckpointWhileEpochProtected`] fail-fast
///   拒绝（对照 C#：Garnet CHECKPOINT 命令与 StoreWrapper.CompactionTaskAsync 周期紧缩任务均在会话作用域外串行
///   驱动检查点，契约靠外部调用约定成立；wedb 将其显式化为类型化错误）。
/// - 单线程测试等场景天然满足：会话操作守卫均为 RAII 作用域，操作返回即退出保护区。
/// - 索引在线扩容迁移中发起即时失败（返回 [`Error::Host`]，零副作用：不签发 Token、
///   不创建目录；对标 C# StateMachineDriver 单槽互斥下 grow 在跑时检查点注册返回
///   false，见 `ensure_not_growing`），调用方按既有重试路径处理。
///
/// # 崩溃一致性与并发语义
/// - Token 签发的目录下界（floor）在进程级闸门内计算：并发持有闸门的
///   [`create_checkpoint_with_token`] 可能正以调用方指定的大 Token 发布，
///   闸门外枚举目录会读到陈旧视图，签发值可能小于已发布 Token，颠倒
///   「目录内 Token 的大小序即版本新旧序」不变式（recover_latest/purge_outdated
///   依赖此不变式）；闸门内计算覆盖跨进程重启后的墙钟回拨，签发闸门全局串行，
///   并发创建亦不撞号。
/// - 快照一致性点与前台写入可见性边界：检查点严格限定在 RI 快照同步段完成后立即
///   捕获的 tail 时点（对标 C# WAIT_FLUSH 入口捕获 finalLogicalAddress 的时序），
///   HashIndex 快照先行且条目地址全部小于该截断点、RangeIndex 树快照经 VersionShift
///   外层屏障与该截断点刚性对齐（严格一致前缀：截断点之前的 meta 记录与树效果两侧
///   齐全，截断点之后的记录与树效果两侧同弃）。屏障放行后到 flush_all 结束期间，新起
///   会话仅能对 `[tail, ∞)` 做 RCU 追加（只读区已封印，`[0, tail)` 的字节绝无在途
///   改写）；这些超界追加可能随本次 flush_all 一并落盘并使 FlushedUntilAddress 超前
///   tail，恢复阶段按截断点 tail 丢弃全部超界索引；
///   文件集层面各 Token 互不相交，`recover` 仅读取某一 Token 的不可变文件集（原子 rename 发布），与并发
///   的创建/清理操作互不撕裂。Token 由签发闸门全局唯一，对同一 Token 并发/重复调用
///   [`create_checkpoint_with_token`] 属调用方违约（失败清场与临时文件路径均按
///   Token 独占假设执行），须保证 Token 唯一。
pub async fn create_checkpoint<D: Device, S: CprStore<Device = D>>(
  store: &S,
  checkpoint_dir: impl AsRef<Path>,
  cp_type: CheckpointType,
) -> Result<CheckpointMeta> {
  ensure_not_growing(store)?;
  ensure_epoch_unprotected(store)?;
  let dir = checkpoint_dir.as_ref();
  let _gate = lock_ckpt_gate().await;
  let floor = list_checkpoints(dir)?.last().copied().unwrap_or(0);
  let token = issue_token_after(candidate_token(), floor);
  create_gated(store, dir, cp_type, token).await
}

/// 使用指定 Token 创建持久化 Checkpoint 快照
///
/// 进程级串行化（见 `lock_ckpt_gate`）：SAVE 手动触发与周期快照并发调用时依次
/// 排队执行，杜绝两次检查点在宿主状态机上的并发叠加；等待者以异步互斥挂起排队，
/// 精确事件唤醒，不阻塞任何 reactor 线程且零空转开销。
///
/// 失败清场：任一阶段失败（如磁盘满 ENOSPC、注入故障）时 best-effort 回收本
/// Token 的全部物理文件（含半截 `.tmp` 与孤儿 token 子目录）——元数据最后落盘
/// 保证失败检查点绝无进入恢复视图的可能，清场仅回收磁盘空间。注意该 Token 若
/// 恰有历史文件集亦会被一并回收，对同一 Token 的重复/复用调用属调用方违约。
pub async fn create_checkpoint_with_token<D: Device, S: CprStore<Device = D>>(
  store: &S,
  checkpoint_dir: impl AsRef<Path>,
  cp_type: CheckpointType,
  token: u128,
) -> Result<CheckpointMeta> {
  ensure_not_growing(store)?;
  ensure_epoch_unprotected(store)?;
  let dir = checkpoint_dir.as_ref();
  let _gate = lock_ckpt_gate().await;
  create_gated(store, dir, cp_type, token).await
}

/// 闸门内创建主流程（调用方须已持有进程级闸门）：创建 + 失败清场
async fn create_gated<D: Device, S: CprStore<Device = D>>(
  store: &S,
  dir: &Path,
  cp_type: CheckpointType,
  token: u128,
) -> Result<CheckpointMeta> {
  let res = create_checkpoint_inner(store, dir, cp_type, token).await;
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
) -> Result<CheckpointMeta> {
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
  //    提交」的丢失更新窗口。fence 纪元须在注册排空动作之前取（见 [`wait_epoch_drain`]）
  debug!("CPR 检查点: 纪元排空屏障 (对标 C# Phase.IN_PROGRESS), token={token:#x}");
  let epoch = store.epoch();
  let fence_epoch = epoch.current_epoch();
  wait_epoch_drain(epoch, || epoch.is_safe_to_reclaim(fence_epoch)).await;

  // 2. WAIT_INDEX_CHECKPOINT：原子刷写 HashIndex 快照（对标 FullCheckpointSM
  //    WAIT_INDEX_CHECKPOINT 先于 WAIT_FLUSH 的阶段序：索引快照捕获的全部条目地址
  //    均小于其后捕获的截断点，恢复阶段截断净空永不误伤现存键）。纯 compio 异步定位
  //    I/O：io_uring 下磁盘 I/O 由内核完成，reactor 仅提交与收割完成事件，大索引刷盘
  //    不再需要线程池中转。传入 ReadCache 解析闭包：指向易失读缓存的索引条目在快照前
  //    必须顺链回写为主日志真实地址（对标 libs/storage/Tsavorite/cs/src/core/Index/
  //    Tsavorite/Implementation/ReadCache.cs:SkipReadCacheBucket），否则恢复后这些键将永久不可见
  //
  //    模糊区窗口起点：扫描开跑前先取 tail 作为 `index_start_logical_address`
  //    （对标 IndexCheckpointSMTask.cs:34 在 PREPARE 入口取
  //    `startLogicalAddress = GetTailAddress()`）。本步骤的快照是单遍扫描，而检查点
  //    屏障只覆盖 RangeIndex 树写与上一步的纪元排空——扫描期间前台哈希索引写完全不
  //    阻断，故扫描可能漏掉「桶已越过、CAS 后落下」的条目（以及扫描后才分配、
  //    overflow_count 已取样因而整桶缺席的溢出桶）。这些漏网条目的记录地址恒落在
  //    `[index_start, tail)` 内：起点之上不存在「记录已预占而 CAS 在途」（纪元已排空），
  //    终点之上不进入本快照。恢复期据此区间重扫日志并重插索引，见
  //    [`super::recover::run_recovery_kernel`] 单趟扫描内核的模糊区重插步骤。
  //    启用 AOF 的装配另有版本过滤重放覆盖同一区间（窗口写入携带检查点版本号、
  //    重放不跳过），但那是旁路冗余而非本机制的前提：无 AOF 装配的正确性只依赖上述
  //    模糊区重插，两者互不依赖。
  debug!("CPR 检查点: 索引快照刷写 (对标 C# Phase.WAIT_INDEX_CHECKPOINT), token={token:#x}");
  let index_start = store.tail_address();
  let index_meta =
    write_index_checkpoint(&store.index(), store.entry_count(), dir, token, &|addr| {
      store.skip_read_cache(addr)
    })
    .await?;

  // 3. WAIT_FLUSH 入口：同步段快照全部 RangeIndex 后立即捕获一致性截断点（1:1 对标
  //    libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotAllTreesForCheckpoint 与
  //    HybridLogCheckpointSMTask GlobalBeforeEnteringState(WAIT_FLUSH) 的
  //    finalLogicalAddress = GetTailAddress() 捕获时序）。屏障持续持有：快照段与截断点
  //    捕获之间无任何 await（单核串行化，多核下仅剩清屏后写者的 µs 级窗口与 C# 同型），
  //    截断点之前的 RI meta 记录与树效果两侧齐全、之后的记录与效果两侧同弃——严格
  //    一致前缀；索引快照条目地址全部小于截断点，恢复截断净空零误伤
  debug!("CPR 检查点: RangeIndex 快照与截断点捕获 (对标 C# Phase.WAIT_FLUSH), token={token:#x}");
  let _ = store.take_range_index_checkpoints(dir, token)?;
  let tail = store.tail_address();

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

  info!(
    "成功创建 Checkpoint: token={token:#x}, type={cp_type:?}, entry_count={}, tail={tail:#x}",
    index_meta.entry_count
  );

  Ok(meta)
}

/// 纪元排空屏障：推进全局纪元并等待前置纪元的在途操作全部排空（检查点域唯一实现）
///
/// 五步协议（对标 C# 状态机驱动器对全部阶段转换的单点纪元包裹
/// `libs/storage/Tsavorite/cs/src/core/Index/Checkpointing/StateMachineDriver.cs` 的
/// `epoch.BumpCurrentEpoch(() => MakeTransitionWorker(nextState))`：C# 的「推进纪元并
/// 等前置排空」在检查点流程中只有一个表达位置，谓词由阶段自身决定；rust 把阶段摊平
/// 为顺序代码后曾把同一协议抄成两份，本函数即其唯一载体）：
///
/// 1. 创建 `crossfire::oneshot` 通道；
/// 2. [`LightEpoch::bump_current_epoch_action`] 把完成信号注册为前置纪元的延迟动作；
/// 3. 前置 [`LightEpoch::drain`] 收割已就绪动作——快路径下谓词此刻已为真，全程零挂起；
/// 4. 谓词为假时被动挂起等待，由步骤 2 注册的动作精准唤醒（零轮询、零 CPU）；
/// 5. 收尾 [`LightEpoch::drain`] 收割推进 SafeReadOnlyAddress 等全部就绪的延迟动作。
///
/// # 前提一：fence 纪元必须紧贴本函数调用之前取
/// `fence_drained` 比较的 fence 须等于本屏障动作的触发纪元，即 `current_epoch()` 在
/// 本函数内 bump 之前的快照——[`LightEpoch::bump_current_epoch_action`] 把动作挂在
/// `bump() - 1` 上，两者天然同值，步骤 3 的快路径谓词与动作就绪条件因此严格同步。若在
/// 更早处取 fence（小于触发纪元），谓词会先于动作就绪而为真、快路径跳过挂起，返回时
/// 前置纪元并未排空——正是「刷盘/快照提前于屏障满足」的撕裂窗口。
///
/// # 前提二：调用方必须不在纪元保护区
/// 本屏障无超时、动作丢失即永久挂起，活性完全依赖入口契约：公共入口
/// [`ensure_epoch_unprotected`] 在闸门前以 [`Error::CheckpointWhileEpochProtected`]
/// fail-fast 拦截保护区内调用（见 [`create_checkpoint`] 的「调用方契约」）。调用线程
/// 若自钉旧纪元，步骤 2 的动作永不就绪，谓词永假。
///
/// # 取消语义
/// `tx` 已 move 进排空闭包：外层 Future 在步骤 4 的挂起点被 Drop（上层取消）后，闭包
/// 仍留在纪元动作表内，后续任一 `drain` 照常收割它——crossfire oneshot 的 `send` 无
/// 返回值，接收端已释放时值写入即由通道自行回收，不 panic、不占用动作槽位；代价是该
/// 次检查点未走完屏障即中止：`create_gated` 的失败清场只挂在 `Err` 分支上，被 Drop 的
/// Future 既不落 Err 也到不了末段元数据发布（半截文件由 `purge_all` 全量残留清扫兜底
/// 回收，恢复视图始终只认已发布 meta），故取消只浪费空间、不破坏一致性。
///
/// # 与仓内另两套纪元等待写法的口径差异（本轮不下沉到 wepoch）
/// - `whlog` 侧 `wait_epoch_condition`（wedb/whlog/src/hlog/shift.rs:168）：退避轮询 +
///   周期性 bump，并以 `unpin_self` + `TlsResumeGuard`（RAII 重入）主动解除本线程对旧
///   纪元的自钉，可容忍调用线程处于保护区；
/// - [`LightEpoch::bump_and_wait`]（wedb/wepoch/src/epoch.rs:698）：阻塞自旋版，跨
///   await 使用会霸占 compio 单线程执行器，检查点流程不可复用。
///
/// 本函数取三者中最轻的 oneshot 形态，把「调用方不在保护区」这一活性前提外置为入口
/// 契约，不内建 `unpin_self`/`TlsResumeGuard` 的取消安全语义。若要统一为单一实现，须
/// 连带补齐该语义并下沉到 wepoch，属独立改造，勿夹带在本屏障内。
async fn wait_epoch_drain(epoch: &LightEpoch, fence_drained: impl Fn() -> bool) {
  // 事件驱动零轮询：前置纪元排空时，由注册在 LightEpoch 上的动作精准发送完成信号
  let (tx, rx) = oneshot::<()>();
  epoch.bump_current_epoch_action(move || {
    tx.send(());
  });
  // 步骤 3：优先收割已就绪动作
  epoch.drain();
  if !fence_drained() {
    // 步骤 4：慢路径，被动挂起等待前置纪元完全排空并精准触发通知
    let _ = rx.await;
  }
  // 步骤 5：收割 SafeReadOnlyAddress 推进等全部就绪的延迟动作
  epoch.drain();
}

/// 补写检查点覆盖的 AOF 边界地址至元数据（快照提交后的第二阶段持久化）
///
/// 在 garnet 中的相对路径:libs/server/GarnetCheckpointManager.cs:GetCookie
/// （C# 检查点提交时经 GetCookie 把 CurrentSafeAofAddress 序列化进 cookie 随
/// 元数据落盘，恢复侧 RecoveredSafeAofAddress 供复制域消费；rust 检查点内核
/// CprStore 泛型不感知 AOF 域，边界由持有共享 AOF 的宿主在快照发布后补写：
/// 读元数据 → 校验 token/版本/封签完整性 → 回填字段 → 重算封签 → 原子 rename。补写窗口内
/// 崩溃则字段保持 None，单机恢复不依赖该字段，正确性不受影响）
pub async fn publish_checkpoint_aof_address(
  checkpoint_dir: impl AsRef<Path>,
  token: u128,
  address: u64,
) -> Result<()> {
  use compio::fs::read;

  let dir = checkpoint_dir.as_ref();
  let meta_path = dir.join(meta_filename(token));
  let mut meta = CheckpointMeta::decode(&read(&meta_path).await?)?;
  if meta.token != token {
    return Err(Error::TokenMismatch {
      expected: token,
      actual: meta.token,
    });
  }
  if meta.format_version != FORMAT_VERSION {
    return Err(Error::UnsupportedMetaVersion {
      actual: meta.format_version,
      supported: FORMAT_VERSION,
    });
  }
  let digest = meta.integrity_digest();
  if digest != meta.integrity_crc32 {
    return Err(Error::MetaChecksumMismatch {
      expected: meta.integrity_crc32,
      actual: digest,
    });
  }
  meta.checkpoint_aof_address = Some(address);
  meta.seal();

  let meta_bytes = meta.encode();
  let tmp_meta_path = dir.join(meta_tmp_filename(token));
  let mut file = File::create(&tmp_meta_path).await?;
  file.write_all_at(meta_bytes, 0).await.0?;
  file.sync_all().await?;
  rename(&tmp_meta_path, &meta_path).await?;
  sync_checkpoint_dir(dir)
}
