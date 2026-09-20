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
  meta::{CheckpointMeta, CheckpointType, FORMAT_VERSION, index_filename, meta_filename},
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
  let meta_bytes = read(&meta_path).await?;
  let meta = CheckpointMeta::decode(&meta_bytes)?;
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

  // 完整性封签校验（对标 C# RecoveryInfo.Checksum 强制比对）：介质位翻转虽可通过
  // 反序列化（结构合法），但无法通过逐字段摘要比对——在此拦截「静默错误恢复」类损坏，
  // recover_latest 依此回退至更早的有效检查点。
  let digest = meta.integrity_digest();
  if digest != meta.integrity_crc32 {
    return Err(Error::MetaChecksumMismatch {
      expected: meta.integrity_crc32,
      actual: digest,
    });
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
  let index_start = meta.index_start_logical_address;

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
  // - FoldOver: read_only_address 推进至 tail，确保历史数据封印为只读，后续更新全部走 RCU 追加
  // - Snapshot: read_only_address 基于 mutable_fraction 计算，保留内存可变区原位覆写能力（对齐 C# Tsavorite CalculateReadOnlyAddress）
  let head = meta.hlog_meta.head_address;
  let ro = if meta.cp_type == CheckpointType::FoldOver {
    tail
  } else {
    hlog_config
      .calculate_read_only_address(head, tail)
      .max(head)
  };

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
/// 沿 `[begin_address, tail_address)` 单调升序扫描一次，同趟完成两件事（对标
/// C# Recovery/Recovery.cs:RecoverHybridLogAsync 的单趟 page 扫描经
/// ClearBitsOnPage 派发 GarnetRecordTriggers.cs:OnRecoverySnapshotRead 逐记录
/// 回调、同趟 RecoverFromPage 索引重插）：
///
/// 1. **模糊区重插**：对 `addr >= index_start_address` 的记录用与前台索引写同一
///    套原语（[`HashIndex::find_or_create_tag_by_hash_with_min_addr`] +
///    [`windex::HashEntryInfo::try_cas`]）把索引快照生成后才落地的 CAS 记录补回
///    内存索引；min_valid_addr 取 `begin_address`，与 C# `FindOrCreateTag(ref
///    hei, hlogBase.BeginAddress)` 同口径。窗口外的记录已被快照收录（或属被
///    GC/截断区间），重插会以旧地址覆写快照中更新的有效地址，故一律跳过。
/// 2. **主机回调**：每条记录（含窗口外）按扫描顺序恰好回调一次
///    [`RecoveryVisitor::on_record`]，供主机收集 RI 桩与 DbMeta 等恢复素材；
///    回调只收集不改写日志，修复动作由主机在扫描结束后执行。
///
/// 记录侧不做任何过滤：`[begin, tail)` 由 HybridLog 恢复截断保证全部为已落盘的
/// 完整记录（tail 之后的崩溃残留已在 [`HybridLog::recover`] 内清零），Pad 与页尾
/// 残片由扫描内核就地跳过；键长为 0 的记录（页头占位与罕见空键）与前台写路径
/// 口径一致地跳过重插，避免以空键造出无主槽位。墓碑记录必须一并重插：wedb 的
/// 删除是「原位墓碑」或「盲追加 0 字节墓碑 + 索引 CAS」，跳过墓碑会让盲追加删除
/// 的键在恢复后复活，而升序覆写保证同一键最终停在最大地址。
///
/// 修复动作可能向 tail 之后 append 新记录；扫描区间终点建立时即固定（且
/// [`whlog::ScanIterator`] 每步还会钳制到当前 tail），追加的记录地址恒大于
/// `tail_address`，绝不会被本趟再次访问。
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
  visitor: &mut V,
) -> Result<RecoveryScanStats> {
  let mut it = hlog.scan_iter(begin_address, tail_address);
  let mut stats = RecoveryScanStats::default();
  // 索引原语的错误通道与扫描内核不同（windex vs whlog），逐条消费时以槽位暂存，
  // 出循环后统一上抛——溢出桶分配失败若被吞掉，等价于该键再次静默丢失
  let mut index_err: Option<windex::Error> = None;
  loop {
    let stepped = it
      .next_ref(|item| {
        stats.visited += 1;
        if item.addr >= index_start_address && !item.rec.key().is_empty() {
          let hash = HashIndex::hash_key(item.rec.key());
          match index.find_or_create_tag_by_hash_with_min_addr(hash, begin_address) {
            Ok(mut hei) => {
              hei.try_cas(item.addr);
              stats.replayed += 1;
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
  Ok(stats)
}

/// [`run_recovery_kernel`] 单趟扫描的计数摘要。
#[derive(Debug, Default, Clone, Copy)]
pub struct RecoveryScanStats {
  /// 扫描到的记录条数（Pad 与页尾残片已由扫描内核跳过，不计入）。
  pub visited: u64,
  /// 其中地址落在模糊窗口内、实际执行过索引 CAS 的记录条数。
  pub replayed: u64,
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
