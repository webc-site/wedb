//! 自研依据: 区间索引删空自愈（doc/zh/collection.md 删空生命周期）
use std::result::Result as StdResult;

use wbase::addr::is_read_cache;
use wbftree::{BfTreeService, RANGE_INDEX_STUB_SIZE, RangeIndexStub};
use wdev::Device;
use wval::{KeyTag, META_VALUE_SIZE, MetaValue};

use super::RangeIndexError;
use crate::{
  error::{Error, Result},
  range_index::meta_and_stub_of,
  session::{CopyToTailOutcome, StoreSession},
};

impl<D: Device> StoreSession<D> {
  /// 写臂锁内刷新分层元记录副本
  ///
  /// 分层写臂的 meta 装载发生在条带锁之外（rmw_helpers 路由探测），两臂并发时
  /// 各持装载快照、收尾 [`Self::save_bftree_meta_stub`] 整体覆写会互相丢更新
  /// （size / next_expiry 增量被后写者抹掉）。本函数在独占写锁内重读元记录覆盖
  /// 调用方副本，使互斥窗口完整覆盖「装载 → 树写 → 计数 → 回写」。
  ///
  /// 返回假 = 键已被并发排空回收（无记录 / 非 live 元记录），调用臂放弃写面
  /// 穿透重建（写锁保证此刻起无人能再动该键，非 live 判定即终态）；RENAME
  /// 迁移 claim 在册则按 `Err(MigrationBusy)` 显式拒绝（禁借 Ok(false) 穿透）。
  pub async fn refresh_tiered_meta(
    &self,
    key: &[u8],
    meta: &mut MetaValue,
    stub: Option<&mut RangeIndexStub>,
  ) -> Result<bool> {
    let prefix = self.session_prefix();
    self
      .refresh_tiered_meta_with_prefix(prefix.as_slice(), key, meta, stub)
      .await
  }

  /// [`Self::refresh_tiered_meta`] 的显式前缀形态（链首域钉发布链刷新专用，
  /// 与 [`Self::save_bftree_meta_stub_with_prefix`] 同族分工）：重读落域 =
  /// 调用方钉定前缀，禁链中重解析——RI 稳态写臂多 await 链中换代若被逐点
  /// 重解析吃进，刷新域即与装载记录域、元记录回写域撕裂（refresh 钉定域内
  /// 记录恒在时 Ok(true) 放行，撕裂形下回写即落错代幽灵键）
  pub(crate) async fn refresh_tiered_meta_with_prefix(
    &self,
    prefix: &[u8],
    key: &[u8],
    meta: &mut MetaValue,
    stub: Option<&mut RangeIndexStub>,
  ) -> Result<bool> {
    let meta_k = Self::session_tag_key_with_prefix(prefix, KeyTag::Meta, key);
    // RENAME 迁移 claim 判定（分层写臂纵深防御）：迁移窗内显式拒绝
    // （MigrationBusy 锁忙/重试语义，经 tiered_guard 的 map_err 上浮为存储忙
    // 错误应答）。禁回 Ok(false)——调用臂会按「键已被并发排空」穿透物化降级，
    // 在 dst 信封域重建对象（换一种已 ACK 丢失形）；真·已排空（无记录/非 live）
    // 才走 Ok(false) 穿透臂。路由探测的 load_collection_stub 判定与本锁内复核
    // 双门闭合。claim 判据取树身份键 = 物理 Meta 键（下方重读同键，零额外分配）
    if self.store.range_index.migration_claimed(&meta_k) {
      return Err(Error::MigrationBusy);
    }
    let Some(bytes) = self.read_raw(&meta_k).await? else {
      return Ok(false);
    };
    if bytes.len() < META_VALUE_SIZE {
      return Ok(false);
    }
    let fresh = MetaValue::from_slice(&bytes[..META_VALUE_SIZE])?;
    if !fresh.is_live() {
      return Ok(false);
    }
    *meta = fresh;
    if let Some(stub) = stub
      && let Some(s) = range_index_stub_of(&bytes)
    {
      *stub = s;
    }
    Ok(true)
  }

  /// 可变区原位治愈快路径 (InPlaceUpdater 等价，RIPROMOTE/RIRESTORE 共用)
  ///
  /// find_tag 命中可变区记录时，页写锁内读-验-改：治愈内核 [`patch_stub_record`]
  /// 就地改存根窗口，零复制零分配。`try_modify_record_in_place` 返回 Ok(None)
  /// 仅表示记录不在可变区 (含 RC 链头 / 墓碑 / Tag 碰撞)。返回 Ok(true) 原位闭环
  /// (含命中但已治愈的零写)；Ok(false) 降级候选链慢路径
  pub(super) async fn heal_stub_in_place(
    &self,
    meta_k: &[u8],
    patch: impl FnOnce(&mut RangeIndexStub) -> bool,
  ) -> StdResult<bool, RangeIndexError> {
    let _guard = self.enter_gated();
    let Some(addr) = self.store.index.load().find_tag(meta_k) else {
      return Ok(false);
    };
    if is_read_cache(addr) {
      return Ok(false);
    }
    let closed = self
      .store
      .hlog
      .try_modify_record_in_place(addr, meta_k, |val| Some(patch_stub_record(val, patch)))
      .map_err(Error::from)?;
    Ok(closed.is_some())
  }

  /// 候选链定位当前记录并追加治愈帧 (CopyUpdater 等价，RIPROMOTE/RIRESTORE 共用)
  ///
  /// 骨架、纪元纪律与 CAS 收尾（含败帧回复活池，杜绝旧实现「落败即遗弃」的
  /// 槽位泄漏）全部转调 copy-to-tail 内核 [`StoreSession::copy_record_to_tail`]，
  /// 本函数仅保留 RangeIndex 侧协议适配：位变更器交 wkv 唯一治愈内核
  /// [`patch_stub_record`] 在等长堆副本上就地改 35B 存根窗口后追加至日志尾部
  /// (对标 C# TryCopyToTail 先分配新尾记录再拷值，值体长度无上限)，plan 返回
  /// None = 非存活 RangeIndex 元记录 / 已治愈，零写闭环
  pub(super) async fn tail_patch_record(
    &self,
    meta_k: &[u8],
    patch: impl Fn(&mut RangeIndexStub) -> bool,
  ) -> StdResult<TailPatchOutcome, RangeIndexError> {
    let outcome = self
      .copy_record_to_tail(meta_k, false, false, |record| {
        let Ok(val) = record.value() else {
          return None;
        };
        let mut frame = val.to_vec();
        if !patch_stub_record(&mut frame, |stub| patch(stub)) {
          return None;
        }
        Some(frame)
      })
      .await?;
    Ok(match outcome {
      CopyToTailOutcome::Miss => TailPatchOutcome::Miss,
      CopyToTailOutcome::Closed => TailPatchOutcome::Closed,
      CopyToTailOutcome::Appended { src_addr, cas_ok } => {
        TailPatchOutcome::Appended { src_addr, cas_ok }
      }
    })
  }

  /// 惰性激活后的存根回写 (1:1 对标 libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RestoreRangeIndexStub
  /// ——回写落地的存根字段变更本体见 wbftree::RangeIndexStub::recreate_index)
  ///
  /// C# 在 RestoreTree 恢复并注册树实例后，经 RIRESTORE RMW 把恢复树的新句柄写回
  /// 存根并清除 IsRecovered 位，本方法承接同一语义：
  /// - 快路径为可变区原位读-改-写 (InPlaceUpdater 等价，页写锁内读-验-改，零追加零 CAS)；
  /// - 只读/磁盘区降级候选链定位 + 补丁追加 + CAS 挂载 (CopyUpdater 等价)；
  /// - 墓碑 / 非 RangeIndex 元记录零写跳过 (RIRESTORE.NeedInitialUpdate=false：键已被
  ///   并发删除时 RMW 返回 NOTFOUND，绝不复活存根)；
  /// - 内部维护写旁路写监听 (对标 C# RIRESTORE 不入 AOF——瞬态句柄非用户写效果，
  ///   copy-to-tail 内核以 notify=false 追加，原位直调 hlog 无监听通知)；
  /// - 幂等：句柄已绑定当前树且 Recovered 位已清时零写。
  ///
  /// 清 Recovered 位的意义 (对标 C# RecreateIndex 注释)：存根已脱离「检查点恢复态」，
  /// 后续淘汰周期按激活后的树写刷盘快照 (反映恢复后的写)，不再被当作恢复期未激活
  /// 的桩 (判据见本文件 mark_recovered_patch 与 wbftree::RangeIndexStub::recreate_index)。
  /// 冷读侧只打开预置就位的 data.bftree，恢复源选择不含 recovered 分流。
  ///
  /// 对标差异：C# 存根句柄是热路径直接调用的原生指针，重启后由 OnDiskRead 清零
  /// (InvalidateStub)；本实现路由一律走注册表、句柄仅作标识，故以「句柄 ≠ 当前树
  /// native_ptr」的补丁条件等价覆盖跨重启陈旧句柄的清理。C# 在 RIRESTORE 失败时把
  /// 整个 RestoreTree 视作失败 (客户端见 NOTFOUND)；本实现显式上抛 Internal——激活
  /// 已成功而回写失败属存储 I/O 故障，静默吞掉会掩盖持久化态与运行态的偏离。
  pub(super) async fn restore_range_index_stub(
    &self,
    key: &[u8],
    tree: &BfTreeService,
  ) -> StdResult<(), RangeIndexError> {
    let meta_k = self.session_meta_key(key);
    let native = tree.native_ptr();

    // 1. InPlaceUpdater 等价：可变区页写锁内读-验-改。原位治愈闭环即返回；
    //    记录不在可变区 (含 RC 链头 / 墓碑 / Tag 碰撞) 交由候选链慢路径定位
    if self
      .heal_stub_in_place(&meta_k, |stub| recreate_patch(stub, native))
      .await?
    {
      return Ok(());
    }

    // 2. CopyUpdater 等价：候选链定位 + 补丁追加 + CAS 挂载。CAS 失败 (并发写
    //    移动链头)：败帧由 copy-to-tail 内核回复活池回收；治愈幂等，下次激活重试
    //    (对标 C# CopyUpdater CAS 败者不重试同帧)；RIRESTORE 无 PostCopyUpdater
    //    后处理——句柄是瞬态标识，源记录无需转移语义
    let _ = self
      .tail_patch_record(&meta_k, |stub| recreate_patch(stub, native))
      .await?;
    Ok(())
  }
}

/// RIPROMOTE/RIRESTORE 慢路径 (CopyUpdater 等价) 结果
pub(super) enum TailPatchOutcome {
  /// 候选链无存活目标记录，零写
  Miss,
  /// 命中但零写闭环：墓碑 / 非 RI 元记录 / 已治愈
  Closed,
  /// 已追加治愈帧并尝试 CAS 挂载
  Appended {
    /// 命中的源记录地址 (PostCopyUpdater 所有权转移目标)
    src_addr: u64,
    /// 索引 CAS 是否成功
    cas_ok: bool,
  },
}

pub const STUB_WINDOW_START: usize = META_VALUE_SIZE;
pub const STUB_WINDOW_END: usize = META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE;

/// RangeIndex 复合元记录（`[MetaValue 32B][RangeIndexStub 35B][可选扩展]`）的存根解码单点
///
/// 在 garnet 中的相对路径: libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:ReadIndex
/// （C# 先按 `DataHeader.RecordType == RangeIndexRecordType` 判别记录类型，再
/// `Unsafe.As` 把值体首段 reinterpret 为存根；本仓记录类型事实由
/// [`MetaValue::collection_type`] 的分层域判据 [`GarnetObjectType::is_tiered_collection`]
/// 承载——Meta 域复合记录由 RI.CREATE 与升阶集合共用（wkv session/collection.rs
/// 域文档），升阶形态保留原集合类型，判据收窄为 `== RangeIndex` 会把升阶集合
/// 整体排除出存根治愈链（OnFlush 置位 / RIPROMOTE / RIRESTORE 对升阶树静默
/// 失效，摘树恢复即撞未收口工作文件）；RI 元记录唯一编码口为
/// [`encode_meta_stub_record`](crate::range_index::encode_meta_stub_record)）
///
/// 返回 Some = 目标记录，`[META_VALUE_SIZE, +RANGE_INDEX_STUB_SIZE)` 窗口必在界内；
/// None = 非目标记录（定长不足 / Meta 解码失败 / 非分层域类型），调用方零写跳过
#[doc(hidden)]
#[inline]
pub fn range_index_stub_of(val: &[u8]) -> Option<RangeIndexStub> {
  let (meta, stub) = meta_and_stub_of(val).ok()?;
  if !meta.collection_type.is_tiered_collection() {
    return None;
  }
  Some(stub)
}

/// wkv 唯一的 RangeIndex 存根治愈内核：解码 → 位变更 → 就地回填 35B 存根窗口
///
/// 本内核只承接「解码—就地回填」骨架，锚点仅挂 C# 侧无独立 rust 位变更器的两枚：
/// 在 garnet 中的相对路径: libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:SetFlushedFlag
/// 与 libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:InvalidateStub
/// （清 Flushed / 标记恢复 / 重绑句柄 / 所有权转出四枚的符号锚点 1:1 挂在下方对应
/// 位变更器 [`clear_flushed_patch`] / [`mark_recovered_patch`] / [`recreate_patch`] /
/// [`transfer_out_patch`]，本内核不复挂；
/// C# 的 in-span 单点变更器族：全部经
/// `ref var stub = ref Unsafe.As<byte, RangeIndexStub>(ref valueSpan[0])` 就地改位，
/// 无任何「复制整值体」的第二形态；变更器只被
/// libs/server/Storage/Functions/GarnetRecordTriggers.cs:130/:157/:248 与
/// RMWMethods 的 RIPROMOTE/RIRESTORE 分支转调。wbftree 侧最后一个零调用切片位写入器
/// （`RangeIndexStub::slice_set_flushed`）已随本内核收口删除——
/// rust 位变更一律经本内核 + 下方四个位变更器，杜绝跨 crate 双口径）
///
/// 值体长度口径（超容量策略取堆侧，杜绝旧实现三种互斥口径）：内核只对存根窗口
/// 落笔，Meta 段与其后扩展字节原样保留，**不设任何上限**——C# 侧
/// `Debug.Assert(valueSpan.Length >= RangeIndexStub.Size)`（Index.cs:153）给出的是
/// 下界，压根不存在「值体超容量」概念，旧 rust 的 `[u8; 128]` 栈缓冲与
/// `val.len().min(128)` 是转写自造，并对 >128B 值体分裂出「Vec 回退 / 静默截断
/// 丢扩展 / 判 None 放弃治愈」三口径。确需新帧的调用点（RIPROMOTE/RIRESTORE 慢
/// 路径、紧缩搬迁、恢复原位失败降级）按 `val.to_vec()` 精确等长复制后交本内核
/// 就地改位，对标 C# TryCopyToTail 先分配新尾记录再拷值
/// (libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/
/// TryCopyToTail.cs:24)；原位路径（可变区页写锁、刷盘置位）零复制零分配。
/// 不采「超界拒绝并报错」：C# 无此对位，且会把 >上限的存活索引永久卡在 Flushed
/// 态反复晋升。
///
/// 返回 true = 已回填（值体被改）；false = 零写跳过（非目标记录，或 `patch`
/// 判定已治愈）
#[doc(hidden)]
#[inline]
pub fn patch_stub_record(val: &mut [u8], patch: impl FnOnce(&mut RangeIndexStub) -> bool) -> bool {
  let Some(mut stub) = range_index_stub_of(val) else {
    return false;
  };
  if !patch(&mut stub) {
    return false;
  }
  val[STUB_WINDOW_START..STUB_WINDOW_END].copy_from_slice(&stub.encode());
  true
}

/// 清除 Flushed 位 (在 garnet 中的相对路径:
/// libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:ClearFlushedFlag，
/// RIPROMOTE 尾部新帧与紧缩搬迁共用)
///
/// 已清零时返回 false 零写（幂等）
#[doc(hidden)]
#[inline]
pub fn clear_flushed_patch(stub: &mut RangeIndexStub) -> bool {
  if !stub.is_flushed() {
    return false;
  }
  stub.set_flushed(false);
  true
}

/// 标记「已从检查点快照恢复」并清零句柄 (在 garnet 中的相对路径:
/// libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:MarkRecoveredFromCheckpoint)
///
/// 持久化字节已是该态时返回 false 零写：多轮恢复的重复回写纯属浪费——原位改写
/// 退化为同址重写，失败路径还多出一次追加 + 索引地址更新
#[doc(hidden)]
#[inline]
pub fn mark_recovered_patch(stub: &mut RangeIndexStub) -> bool {
  if stub.tree_handle == 0 && stub.is_recovered() {
    return false;
  }
  stub.mark_recovered_from_checkpoint();
  true
}

/// 重绑在线树句柄并清 Recovered 位 (在 garnet 中的相对路径:
/// libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:RecreateIndex，RIRESTORE 回写)
///
/// 句柄已绑定当前树且恢复位已清时返回 false 零写（幂等）
#[doc(hidden)]
#[inline]
pub fn recreate_patch(stub: &mut RangeIndexStub, new_tree_handle: u64) -> bool {
  if stub.tree_handle == new_tree_handle && !stub.is_recovered() {
    return false;
  }
  stub.recreate_index(new_tree_handle);
  true
}

/// 源存根所有权转出：句柄清零 + 置 Transferred 位
/// (在 garnet 中的相对路径:
/// libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:ClearTreeHandle 与
/// libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:SetTransferredFlag，
/// PostCopyToTail 活跃转移分支)
///
/// 唯一消费方是下方 [`transfer_out_mark`] 判收折叠与集成测试，转出判据
/// （句柄零 + Transferred 已置即幂等零写）单点挂死在此，禁第二处复判
#[doc(hidden)]
#[inline]
pub fn transfer_out_patch(stub: &mut RangeIndexStub) -> bool {
  if stub.tree_handle == 0 && stub.is_transferred() {
    return false;
  }
  stub.tree_handle = 0;
  stub.set_transferred(true);
  true
}

/// 转出标记单次落笔判收三态（[`transfer_out_source_stub`] 两内核降级臂共用）
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TransferOutMark {
  /// 本次改位成功（[`transfer_out_patch`] 已回填存根窗口）
  Written,
  /// 已处转出态（句柄已零且 Transferred 已置），幂等零写——判收与落笔同静默
  Already,
  /// 非目标记录（定长不足 / Meta 解码失败 / 非分层域类型），零写
  NotTarget,
}

/// 转出标记单次落笔 + 三态判收折叠：骨架复用唯一治愈内核 [`patch_stub_record`]，
/// 位变更与幂等判定单点 [`transfer_out_patch`]，本折叠只把布尔结果细化为三态——
/// 「已处转出态」（幂等静默通过）与「非目标记录」（异常留痕）不得混为同一布尔面
#[inline]
pub(super) fn transfer_out_mark(val: &mut [u8]) -> TransferOutMark {
  let mut mark = TransferOutMark::NotTarget;
  patch_stub_record(val, |stub| {
    if transfer_out_patch(stub) {
      mark = TransferOutMark::Written;
      true
    } else {
      mark = TransferOutMark::Already;
      false
    }
  });
  mark
}
