use std::{path::Path, sync::Arc};

use compio::fs::{metadata, read};
use itoa::Buffer;
use log::{info, warn};
use wdev::Device;
use wepoch::LightEpoch;
use whlog::{AddressSnapshot, HybridLog, HybridLogConfig};
use windex::HashIndex;

use super::{CprRecover, RecoveredCheckpoint, list_checkpoints};
use crate::{
  error::{Error, Result},
  index_ckpt::read_index_checkpoint_truncated,
  meta::{CheckpointMeta, index_filename, meta_filename},
};

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
  let mut buf = Buffer::new();
  s.push_str(buf.format(actual));
  s.push_str(" vs ");
  s.push_str(buf.format(expected));
  Error::InvalidIndexCkpt(s)
}

/// 从指定 Checkpoint 进行崩溃恢复，重构并实例化底层核心组件集合
///
/// 检查点元数据按 Token 回读入口对标：
/// libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/RecoveryInfo.cs:Recover
/// （C# Recover(token, checkpointManager) 经 GetLogCheckpointMetadata 取元数据后
/// Initialize 解析；rust 侧 = 读 meta 文件 + CheckpointMeta::decode + 封签校验）
///
/// 恢复流程：
/// 1. 读取并反序列化 `checkpoint_<token>.meta` 元数据文件。
/// 2. 从 `index_<token>.ckpt` 二进制快照无损重建 64B Cacheline 对齐的 HashIndex。
/// 3. 重建 LightEpoch，构造 AddressSnapshot 统一经 [`HybridLog::recover`] 恢复底层
///    HybridLog（[head, tail) 驻留窗口整段装载，[tail, 页尾) 崩溃残留强制清零）。
///
/// # 数据链信任边界（与 waof 自同步扫描的刻意差异，与 C# 一致）
///
/// 主日志记录本身无校验和（wrecord/whlog 全链无 CRC 字段，对标 C# Tsavorite
/// LogSettings/AllocatorBase 无 per-record checksum），恢复因此不做逐记录 CRC
/// 重放：完整性边界 = `flushed_until` 持久化前缀（[`HybridLog::recover`] 仅装载
/// `[0, flushed_until)`，`[flushed_until, tail)` 崩溃残留一律清零）+ 创建时
/// `flush_all` + device sync 的时序闭环（脏页按序刷盘、元数据最后落盘）+ 元数据
/// `integrity_crc32` 封签（对标 C# RecoveryInfo.Checksum）。带校验和的是 index
/// 快照文件（自带 CRC，读回校验）与 waof 记录（8B 头含 crc32，恢复期全量自同步
/// 扫描前摄暴露损坏）——主日志数据区的介质损坏（位翻转、半截写入）无恢复期
/// 拦截，亦无读路径逐记录校验兜底，此为与 waof 面向不同 RTO/数据量权衡的
/// 刻意分化，非实现遗漏。
pub(crate) async fn recover_checkpoint_components<D: Device>(
  checkpoint_dir: impl AsRef<Path>,
  token: u128,
  device: Arc<D>,
) -> Result<RecoveredCheckpoint<D>> {
  let dir = checkpoint_dir.as_ref();
  let meta_path = dir.join(meta_filename(token));
  if metadata(&meta_path).await.is_err() {
    return Err(Error::MetaNotFound(meta_path));
  }

  // 1. 读取元数据文件（bitcode 极速反序列化）
  // 对标 libs/storage/Tsavorite/cs/src/core/Index/CheckpointManagement/DeviceLogCommitCheckpointManager.cs:ThrowIfInvalidMetadataSize
  // 的「损坏元数据具名拒绝」语义：C# 元数据为设备日志上的长度前缀流（读首部
  // int 长度，<= 0 或超 64MB 上限即抛 TsavoriteException，把截断/损坏文件变成
  // 具名错误而非巨型分配）；本实现元数据经 `read` 整文件读入后按二进制
  // bitcode 反序列化，无长度前缀分配路径，截断在 decode 与下方
  // 版本/Token/完整性封签逐项校验中显式报错，等价达成「损坏元数据绝不静默
  // 恢复、绝不引发失控分配」的防护目标。
  let meta = CheckpointMeta::verify_sealed(&read(&meta_path).await?, token)?;

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
  let index_start = meta.index_start_logical_address;

  // 设备物理长度预检（杜绝数据文件缺失或被截断导致静默起伪空库）：装载元数据后、
  // 任何页读取前执行。口径注记：begin/head/flushed/tail 均为逻辑字节偏移，与单文件
  // 设备线性偏移 1:1；分段设备按段大小除法换算段号与段内偏移，全程按字节比较，
  // 不涉及页数换算。
  // 两档分治：
  // - flushed_until 是崩溃一致性承诺的连续已落盘前缀，其内任一字节缺失即数据文件
  //   与检查点不配套，具名拒启；flushed 仍处初始基准（全新库从未刷页）时零长度设备
  //   合法，豁免本档。
  // - [flushed, tail) 按上文档信任边界注记本就允许是崩溃残留（恢复期一律清零），
  //   该段短缺刻意不校验、不升级为拒启，保持原有容灾语义。
  // 与 C# 的刻意差异（本仓更严）：C# AllocatorBase.cs:AsyncReadPagesForRecovery 的
  // 读完成回调对文件末端短读按可得字节放行，缺失区等效读为全零，恢复扫描遇零头即
  // 停止重建——前缀缺失时 C# 静默起库、只截短数据；本实现在装载期显式拒启，杜绝
  // 「残缺状态被后续写入固化」。
  if flushed > hlog_config.initial_address {
    let last_byte_addr = flushed - 1;
    // 单点取设备实际物理覆盖：经 wdev::Device::get_file_size（对标 C#
    // IDevice.GetFileSize），不另立第二套设备大小真源
    let size = device.segment_size();
    let (seg_id, expected_in_seg) = (last_byte_addr / size, last_byte_addr % size + 1);
    let actual_in_seg = device
      .get_file_size(u32::try_from(seg_id).unwrap_or(u32::MAX))
      .unwrap_or(0);
    if actual_in_seg < expected_in_seg {
      let actual_total = seg_id * size + actual_in_seg;
      return Err(Error::DeviceTooShort {
        expected: flushed,
        actual: actual_total,
      });
    }
  }

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

  // 模糊区窗口起点合法界：起点由本轮索引快照开跑前的 tail 捕获，必落在
  // [begin, tail] 之内；越界即元数据损坏（封签之外的逻辑矛盾），具名拒绝。
  // 排在三区地址基础一致性防线之后：tail 本身被篡改等更基础的损坏先由
  // 各自具名校验报出，本判仅拦截其余地址均自洽、唯窗口反向或被截断覆盖的形态
  if index_start > tail {
    return Err(addr_violation(
      "IndexStartLogicalAddress",
      index_start,
      "超出 TailAddress",
      tail,
      "，索引快照模糊区窗口反向",
    ));
  }
  if index_start < begin {
    return Err(addr_violation(
      "IndexStartLogicalAddress",
      index_start,
      "低于 BeginAddress",
      begin,
      "，模糊区窗口起点已被日志截断覆盖",
    ));
  }

  let index_path = dir.join(index_filename(token));
  let (index, index_meta) = read_index_checkpoint_truncated(&index_path, token, Some(tail)).await?;

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

  // 恢复地址边界（在快照构造期一次定稿，交由 [HybridLog::recover] 统一装载）：
  // - head 保留 checkpoint 元数据值，[head, tail) 驻留窗口整段装载预热
  // - read_only_address 两型（FoldOver/Snapshot）一律推进至 tail：快照字节在作为
  //   恢复基线期间全程只读，运行期原位写绝不触碰 [begin, tail) 区间——原位能力
  //   由换页驱动的 [HybridLog::maybe_advance_read_only] 随 head 推进自然重建。
  //   对标 C# SnapshotCheckpointSMTask（PREPARE 设 useSnapshotFile=1、WAIT_FLUSH
  //   刷独立 snapshotFileDevice，主日志运行写与快照字节物理隔离，Recovery.cs:465/:595
  //   经 useSnapshotFile 分支读页）：C# 的 in-place 恢复语义与独立快照文件是配套
  //   设计（快照基线永不被运行写污染，二次崩溃恢复从干净基线 + AOF 重放每操作
  //   恰好一次）；本实现无独立快照文件（主日志即快照字节唯一载体），若按
  //   CalculateReadOnlyAddress 重建 ro < tail，恢复实例的原位写会覆写设备上
  //   [ro, tail) 的快照字节，下一检查点发布前再次崩溃时 recover_latest 装载
  //   污染页后 AOF 重放同一非幂等命令——计数器翻倍式静默双算。ro=tail 单点
  //   收口即等价达成 C# 快照文件隔离性（快照基线只读），无第二套恢复语义
  let head = meta.hlog_meta.head_address;
  let ro = tail;

  // 钳制 FlushedUntilAddress 至截断点：封印 tail 之后新起的 RCU 追加可能随本次
  // flush_all 一并落盘，使持久化值超前截断点。恢复视图必须以截断点为准——
  // [begin, tail) 已由 flush_all + device.sync 保证落盘，钳制至 tail 只会引发
  // 后续可能的冗余重刷，绝不漏刷任何脏页，同时维持 flushed_until <= tail 单调不变式
  let flushed = flushed.min(tail);

  // 统一走 [HybridLog::recover] 完成页装载、地址重建与崩溃残留清洗（对标 C#
  // Recovery/Recovery.cs:RecoverHybridLogAsync 经 AllocatorBase.cs:
  // AsyncReadPagesForRecovery 的唯一装载入口：装载页先 ClearPage 清零、尾页只读
  // 到截断点，[tail, page_end) 残留恒为零），消除 wcpr 手工重建的双恢复路径：
  // checkpoint 续写→崩溃→再恢复场景下，上一进程写入截断点之后的旧记录不会被
  // 拷入内存后被并发扫描误当新记录解析或抛 RecordCorrupted
  let hlog = Arc::new(
    HybridLog::recover(
      hlog_config.clone(),
      Arc::clone(&device),
      Arc::clone(&epoch),
      AddressSnapshot::from_bounds(begin, head, flushed, ro, tail),
    )
    .await?,
  );

  // 在此不做日志扫描：恢复期的模糊区重插与主机恢复回调（wkv 的 RI 桩识别/修复、
  // DbMeta 虚拟库表重建）必须共用同一次有序扫描，且部分回调依赖主机对象（此刻
  // 尚未组装）。故 [begin, tail) 的单趟扫描内核 [`run_recovery_kernel`] 移交
  // [`CprRecover::from_recovered`] 主机恰好调用一次，区间与回调语义见其文档；
  // 本函数保留上文对 index_start 窗口合法性的全部校验。

  info!(
    "完成 Checkpoint 崩溃恢复组件加载: token={token:#x}, entry_count={}, tail={tail:#x}, head={head:#x}, ro={ro:#x}",
    meta.index_meta.entry_count
  );

  Ok(RecoveredCheckpoint {
    meta,
    index: Arc::new(index),
    hlog,
    epoch,
  })
}

/// 恢复期唯一一次有序日志扫描内核。
///
/// core 触发器契约同挂此处（宿主派发与 core 接口在 rust 折叠为同一回调点）：
/// libs/storage/Tsavorite/cs/src/core/Index/StoreFunctions/IRecordTriggers.cs:OnRecoverySnapshotRead
///
/// 沿 `[begin_address, tail_address)` 单调升序扫描一次，同趟完成三件事（对标
/// C# Recovery/Recovery.cs:RecoverHybridLogAsync 的单趟 page 扫描经
/// ClearBitsOnPage 派发 GarnetRecordTriggers.cs:OnRecoverySnapshotRead 逐记录
/// 回调、同趟 RecoverFromPage 索引重插与 undoNextVersion 回滚）：
///
/// 1. **模糊区回滚（undoNextVersion，默认启用）**：对齐
///    Recovery.cs:RecoverFromPage 的
///    `addr < fuzzyRegionStart || !info.IsInNewVersion || !undoNextVersion → CAS 插入；
///    否则 → info.SetInvalid() + 前驱回滚` 的回滚臂——携带 IN_NEW_VERSION_BIT 的
///    记录属版本推进窗口的新一代写入，快照物化与 AOF 版本过滤重放（gate 判据
///    `header.store_version < store_version`）对其天然双重生效，恢复侧必须将其
///    失效并把索引槽位回滚至窗口前的老版本锚点，让恢复出的存储严格停留在老
///    版本快照基线，重放恰好一次（Exactly-Once）。rust 判据严格限定于当前模糊区
///    （`item.addr >= index_start_address && item.rec.header.is_in_new_version()`，
///    对齐 C# Recovery.cs:RecoverFromPage 原型）：仅回滚当前这轮 Checkpoint 窗口内
///    的新纪元记录；历史前代 Checkpoint 遗留在磁盘上的带位记录已进入历史提交基线，
///    绝不得跨轮误回滚（否则历史有效键在内存中被误注销且 AOF 跳过，造成数据静默丢失）。
///    回滚处理对本轮模糊区闭环：
///    - 记录失效复用 whlog 密封原语 [`whlog::HybridLog::try_seal_record`]（本仓
///      RecordInfo 无独立 Valid 位，SEALED 即 C# SetInvalid 的关闭态，对标
///      RecordInfo.cs:Invalid => IsClosed；恢复期单线程、写者冻结，槽位密封仅
///      存在于内存视图，不做持久化承诺）；
///    - 索引回滚 `prev < index_start_address` 才执行（对标 C#
///      `if (info.PreviousAddress < recoverFromAddress) hei.entry.Set(PreviousAddress)`）：
///      前驱已属模糊区间时不动槽位——升序扫描下前驱的插入/回滚处理必先于本记录，
///      槽位已收敛至正确锚点；前驱为 INVALID(0) 时 [`windex::HashEntryInfo::try_cas`]
///      天然拒绝对空槽写入，新键整链回滚即槽位保持空置；
///    - 被回滚记录**跳过主机回调**：其 RI 桩 / DbMeta / TTL 素材效果由 AOF 重放
///      恰一次性承接（wkv 宿主各域镜像事件），回调再收集即为二次生效。
/// 2. **模糊区重插**：对 `addr >= index_start_address` 且不带新纪元位（或回滚关闭）
///    的记录用与前台索引写同一套原语（[`HashIndex::find_or_create_tag_by_hash_with_min_addr`] +
///    [`windex::HashEntryInfo::try_cas`]）把索引快照生成后才落地的 CAS 记录补回
///    内存索引；min_valid_addr 取 `begin_address`，与 C# `FindOrCreateTag(ref
///    hei, hlogBase.BeginAddress)` 同口径。窗口外的记录已被快照收录（或属被
///    GC/截断区间），重插会以旧地址覆写快照中更新的有效地址，故一律跳过。
/// 3. **主机回调**：每条未被回滚的记录（含窗口外）按扫描顺序恰好回调一次
///    [`RecoveryVisitor::on_record`]，供主机收集 RI 桩与 DbMeta 等恢复素材；
///    回调只收集不改写日志，修复动作由主机在扫描结束后执行。
///
/// 记录侧不做其他过滤：`[begin, tail)` 由 HybridLog 恢复截断保证全部为已落盘的
/// 完整记录（tail 之后的崩溃残留已在 [`HybridLog::recover`] 内清零），Pad 与页尾
/// 残片由扫描内核就地跳过；键长为 0 的记录（页头占位与罕见空键）与前台写路径
/// 口径一致地跳过索引操作，避免以空键造出无主槽位。墓碑记录必须一并重插：wedb 的
/// 删除是「原位墓碑」或「盲追加 0 字节墓碑 + 索引 CAS」，跳过墓碑会让盲追加删除
/// 的键在恢复后复活，而升序覆写保证同一键最终停在最大地址（窗口内墓碑携带纪元位
/// 时同样被回滚，其删除效果由 AOF 重放承接）。
///
/// 修复动作可能向 tail 之后 append 新记录；扫描区间终点建立时即固定（且
/// [`whlog::ScanIterator`] 每步还会钳制到当前 tail），追加的记录地址恒大于
/// `tail_address`，绝不会被本趟再次访问。恢复期窗口关闭（版本推进窗随检查点
/// 收口），修复追加不带纪元位，不受后续轮次回滚误伤之外的影响。
///
/// 恢复期单一线程驱动、写者已冻结，故 CAS 恒成功；索引原语的错误通道与扫描
/// 内核不同（windex vs whlog），首错以槽位暂存并在提前终止后统一上抛——溢出桶
/// 分配失败若被吞掉，等价于该键再次静默丢失。
///
/// 逐记录回调按泛型参数 `V` 编译期单态化静态分发（对标 C# Recovery.cs 的
/// `TSyncStoreFunctions`/`TInput,TOutput,TContext` 泛型族：C# 以泛型约束收口
/// 回调类型，无运行时虚表）；本内核每条记录一次回调、全量日志百万至千万条，
/// 虚分派在此即阻断回调体内联。visitor 由调用方在栈上以具体类型构造并借用，
/// 单态化实例数 = 实现者数，不向恢复入口外层扩散任何泛型参数。
///
/// # Errors
///
/// 扫描坏记录上抛 [`Error::Host`]（whlog 扫描错误透传）；索引 CAS 失败首错上抛
/// [`Error::Index`] 并终止本趟（与并入前的独立模糊区重放实现口径一致）。
pub async fn run_recovery_kernel<D: Device, V: RecoveryVisitor>(
  hlog: &HybridLog<D>,
  index: &HashIndex,
  begin_address: u64,
  index_start_address: u64,
  tail_address: u64,
  undo_next_version: bool,
  visitor: &mut V,
) -> Result<RecoveryScanStats> {
  let mut it = hlog.scan_iter(begin_address, tail_address);
  let mut stats = RecoveryScanStats::default();
  // 索引原语的错误通道与扫描内核不同（windex vs whlog），逐条消费时以槽位暂存，
  // 出循环后统一上抛——溢出桶分配失败若被吞掉，等价于该键再次静默丢失
  let mut index_err: Option<windex::Error> = None;
  // 回滚失效的槽位密封延后至扫描收尾统一执行：next_ref 回调全程持有记录所在页的
  // 读锁，而原位密封需取同页写锁，扫描中就地调用即自死锁。恢复期写者冻结、
  // 单趟扫描唯一，延后与 C# RecoverFromPage 扫描内 SetInvalid 语义等价。
  let mut undo_seals: Vec<u64> = Vec::new();
  loop {
    let stepped = it
      .next_ref(|item| {
        stats.visited += 1;
        // undoNextVersion 回滚分支（对齐 C# RecoverFromPage:1313-1329 的
        // else 臂：仅在 addr >= index_start_address 且携带新纪元位时回滚；
        // 历史前代快照遗留在盘上的 IN_NEW_VERSION 记录严禁回滚，否则多轮检查点后
        // 历史有效数据被误删）
        if undo_next_version
          && item.addr >= index_start_address
          && item.rec.header.is_in_new_version()
        {
          stats.undone += 1;
          // info.SetInvalid() 的 rust 对位：SEALED 即关闭/失效态（is_invalid == is_closed），
          // 延后统一密封（见 undo_seals 声明处注释）
          undo_seals.push(item.addr);
          let prev = item.rec.header.address();
          if prev < index_start_address && !item.rec.key().is_empty() {
            let hash = HashIndex::hash_key(item.rec.key());
            match index.find_or_create_tag_by_hash_with_min_addr(hash, begin_address) {
              Ok(mut hei) => {
                // 前驱为 INVALID(0)（窗口内新键）时 try_cas 依契约拒绝对空槽写入，
                // 须经 try_set_invalid（对标 C# HashEntryInfo.TryElide 的 CAS→kInvalidAddress）
                // 把已指向密封窗口记录的槽位摘空——否则密封头滞留槽位，读侧命中即按
                // RETRY_LATER 无限活锁，重放侧新键 CAS 亦无从落笔
                if prev == 0 {
                  hei.try_set_invalid();
                } else {
                  hei.try_cas(prev);
                }
              }
              Err(e) => {
                index_err = Some(e);
                return Ok(false);
              }
            }
          }
          return Ok(true);
        }
        if item.addr >= index_start_address && !item.rec.key().is_empty() {
          let hash = HashIndex::hash_key(item.rec.key());
          match index.find_or_create_tag_by_hash_with_min_addr(hash, begin_address) {
            Ok(mut hei) => {
              // 单调补齐守卫（在线收口重建复用本内核所需，票 zcode-r135c-rehash
              // 案二）：仅当槽位当前地址低于本记录地址才 CAS 推进，杜绝与在线
              // 写者竞态下把更高地址覆写回旧版记录（回退即写丢失）；恢复期
              // 写者冻结、扫描升序且快照条目恒低于窗口地板，本守卫恒不触发、
              // 行为逐字节不变
              if !(hei.is_found() && hei.address() > item.addr) {
                hei.try_cas(item.addr);
                stats.replayed += 1;
              }
            }
            Err(e) => {
              index_err = Some(e);
              return Ok(false);
            }
          }
        }
        visitor.on_record(
          item.addr,
          item.rec.key(),
          item.rec.value(),
          item.rec.is_tombstone(),
        );
        Ok(true)
      })
      .await?;
    match stepped {
      None => break,
      Some(false) => break,
      Some(true) => {}
    }
  }
  if let Some(e) = index_err {
    return Err(e.into());
  }
  // 扫描收尾统一落密封位（页读锁已随迭代器释放）
  for addr in undo_seals {
    hlog.try_seal_record(addr, true);
  }
  Ok(stats)
}

/// [`run_recovery_kernel`] 单趟扫描的计数摘要。
#[derive(Debug, Default, Clone, Copy)]
pub struct RecoveryScanStats {
  /// 扫描到的记录条数（Pad 与页尾残片已由扫描内核跳过，不计入）。
  pub visited: u64,
  /// 其中地址落在模糊窗口内、实际执行过索引 CAS 的记录条数。
  pub replayed: u64,
  /// 其中携带 IN_NEW_VERSION_BIT、被 undoNextVersion 回滚失效的记录条数。
  pub undone: u64,
}

/// [`run_recovery_kernel`] 的逐记录主机回调。实现者只收集，不在回调内改写
/// 日志；修复类动作在扫描结束后按收集结果执行。
///
/// 内核以 `V: RecoveryVisitor` 泛型形参静态分发本回调（对标 C# 的泛型
/// `StoreFunctions` 一族），实现者以栈上具体类型接入，全仓不留 `dyn` 兜底臂。
pub trait RecoveryVisitor: Send {
  /// 按物理扫描顺序对每条记录恰好回调一次。
  fn on_record(&mut self, addr: u64, key: &[u8], value: &[u8], is_tombstone: bool);
}

/// [`run_recovery_kernel`] 的无副作用访问器：纯建表/补表调用面（如 wkv 扩容
/// 中止在线收口重建）不需要收集 RI 桩与 DbMeta 素材，复用同一扫描建表内核、
/// 复用同一扫描建表内核、回调空转
pub struct NoopRecoveryVisitor;

impl RecoveryVisitor for NoopRecoveryVisitor {
  #[inline(always)]
  fn on_record(&mut self, _addr: u64, _key: &[u8], _value: &[u8], _is_tombstone: bool) {}
}

/// 从指定 Checkpoint 进行崩溃恢复，重构并实例化全新的宿主存储引擎
pub async fn recover<D: Device, S: CprRecover<Device = D>>(
  checkpoint_dir: impl AsRef<Path>,
  token: u128,
  device: Arc<D>,
) -> Result<S> {
  let dir = checkpoint_dir.as_ref();
  let recovered = recover_checkpoint_components(dir, token, Arc::clone(&device)).await?;
  S::from_recovered(recovered, dir, device).await
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
/// [`purge_outdated`] 相互作用时，被清理 Token 的恢复按「文件缺失」容错
/// 回退至更早版本，不会产生撕裂视图。
pub async fn recover_latest<D: Device, S: CprRecover<Device = D>>(
  checkpoint_dir: impl AsRef<Path>,
  device: Arc<D>,
) -> Result<S> {
  let dir = checkpoint_dir.as_ref();
  let tokens = list_checkpoints(dir)?;
  let mut first_err = None;
  for token in tokens.into_iter().rev() {
    match recover::<D, S>(dir, token, Arc::clone(&device)).await {
      Ok(store) => return Ok(store),
      Err(e) => {
        warn!("跳过无效 Checkpoint（回退至更早版本）: token={token:#x}, err={e}");
        // 回退清场：摘除失败轮已预置的宿主派生工件，防下一轮更早 Token 恢复
        // 继承半预置状态混出代际混杂视图。meta 缺损轮未进入宿主预置段，
        // 读取/解码失败即无残留可清
        if let Ok(meta_bytes) = read(dir.join(meta_filename(token))).await
          && let Ok(meta) = CheckpointMeta::decode(&meta_bytes)
        {
          S::discard_partial_recovery(dir, token, &meta.store_meta);
        }
        // 保留最新 Token 的错误作为代表性失败原因
        first_err.get_or_insert(e);
      }
    }
  }
  Err(first_err.unwrap_or(Error::NoValidCheckpoint(dir.to_path_buf())))
}
