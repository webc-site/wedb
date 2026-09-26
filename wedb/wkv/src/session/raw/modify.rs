//! 原位读-改-写路径（对标 C# Tsavorite InPlaceUpdaterWorker）

use wbase::addr::is_read_cache;
use wdev::Device;

use crate::{error::Result, session::StoreSession};

impl<D: Device> StoreSession<D> {
  /// 沿 Tag 链回溯定位内存可变区中「键匹配、未密封、未墓碑」的记录地址
  /// （原位写族唯一回溯内核，等长改写与原位增长两臂共用，杜绝两套链走查）
  ///
  /// 更新路径的记录定位单点：以 `minAddress = ReadOnlyAddress` 沿前驱回溯并按密封状态
  /// 退回重试或降级；rust 侧读路径回溯单点在
  /// [`super::read::StoreSession::trace_back_for_key_match`]，两者分属
  /// 读/写两臂不共用。本函数专精于可变区 `[read_only_addr, tail)` 的热路径
  /// 原位命中判据，零分配零拷贝；与诊断面 WedbStore::is_latest_hlog_version
  /// `[head, tail)` 全域版本判定各司其职）：首项为 ReadCache 虚拟地址、命中
  /// 密封（对应 RETRY_LATER 降级）/墓碑、滑出可变区或记录不可解一律回
  /// `Ok(None)`，调用方降级 RMW 完整路径。
  fn trace_live_mutable_addr(&self, key: &[u8]) -> Result<Option<u64>> {
    let hash = whasher::fast_hash(key);
    self.ensure_split_by_hash(hash)?;
    let Some(addr) = self.store.index.load().find_tag_by_hash(hash) else {
      return Ok(None);
    };
    if is_read_cache(addr) {
      return Ok(None);
    }
    let read_only_addr = self.store.hlog.read_only_address();
    let mut cur = addr;
    while cur >= read_only_addr {
      match self.store.hlog.with_memory_record(cur, |rec| {
        if rec.matches_key(key) {
          Ok((true, rec.is_closed(), rec.is_tombstone(), 0))
        } else {
          Ok((false, false, false, rec.prev_address()))
        }
      }) {
        // 命中存活匹配记录：交调用方在页写锁内原位改写
        Ok(Some((true, false, false, _))) => return Ok(Some(cur)),
        // Tag 碰撞：沿 prev_address 继续反向回溯
        Ok(Some((false, .., prev))) => cur = prev,
        // 密封在途（C# InPlaceUpdater 对 IsClosed 拒绝原位修改）/ 墓碑命中、
        // 页未就绪或记录不可解：降级完整路径走 CopyUpdater 追加
        _ => return Ok(None),
      }
    }
    Ok(None)
  }

  /// 底层物理在已有纪元保护下尝试在内存可变区原位读-改-写记录（Raw）
  ///
  /// 严格对照 C# libs/server/Storage/Functions/MainStore/RMWMethods.cs:InPlaceUpdater
  /// （MainStore 层 InPlaceUpdater 全仓唯一锚点；等长臂之外的 APPEND/SETRANGE
  /// 增长臂见 [`Self::try_grow_raw_in_place_unprotected`]。底层原位写锁与字节
  /// 改写由 whlog/src/hlog/inplace.rs 承接；回溯定位单点见
  /// [`Self::trace_live_mutable_addr`]，命中未被墓碑标记的匹配记录即持页写锁
  /// 就地读-改-写；闭包返回 None 时返回 Ok(None) 供上层降级走 RMW 完整路径）。
  ///
  /// 返回值契约（票 zcode-r34-writekernel 条目二收口）：
  /// - `Ok(Some(r))`：原位改写已提交，AOF 镜像已同栈发出；
  /// - `Ok(None)`：未命中原位（键缺失/墓碑/密封/只读区/页未就绪/闭包长度门
  ///   拒绝），降级 RCU 追加——闭包未落笔，真零副作用；
  /// - `Err(e)`：**已生效 + AOF 镜像缺失**（写监听端口入队失败，对标全仓
  ///   AofEnqueue 契约「主存写入已生效，调用方须以错误拒绝该命令防主从发散」）
  ///   或回溯/存储层错误。已提交态不撤回：等长臂闭包内新值字节已物理落笔且
  ///   对无锁读者可见，通知失败不再伪装 None（回 None 会跳过 MODIFIED 位落笔
  ///   并诱导调用方 RCU 追加产生第二份效果），调用方不得把本 Err 当零副作用
  ///   处理
  #[inline]
  pub fn try_modify_raw_in_place_unprotected<R>(
    &self,
    key: &[u8],
    f: impl FnOnce(&mut [u8]) -> Option<R>,
  ) -> Result<Option<R>> {
    let Some(addr) = self.trace_live_mutable_addr(key)? else {
      return Ok(None);
    };
    // 写监听通知在持有页写锁的闭包内直接借用更新后切片透传，零堆分配
    // （对标 C# InPlaceUpdater 仅置 NeedAofLog 标志、上层按输入透传 AOF，
    // 原位路径无"读回更新值"的中间拷贝；StoreEvent 为借用型同步分发，
    // 消费端仅向 AOF WAL 内存缓冲无锁入队，持锁内调用无重入死锁风险）。
    // AOF 版本戳在生效点后单读（读点下移，对齐 emit_event 分发时点读——
    // 原位改写不重新编码记录头，纪元位恒为记录落笔时自带的位）；
    // 通知失败保持 Some 承认提交（已提交态不撤回，条目二收口），错误经
    // notify_err 以「已生效+镜像缺失」Err 语义上抛
    let capture = self.store.event_sink.get().is_some();
    let mut notify_err = None;
    let wrapped = |val: &mut [u8]| {
      let r = f(val);
      if capture
        && r.is_some()
        && let Err(e) = self.notify_write_listener(key, val, false)
      {
        notify_err = Some(e);
      }
      r
    };
    let opt = self
      .store
      .hlog
      .try_modify_record_in_place(addr, key, wrapped)?;
    if let Some(e) = notify_err {
      return Err(e);
    }
    Ok(opt)
  }

  /// 底层物理在已有纪元保护下尝试在内存可变区原位增长读-改-写记录（Raw，
  /// 严格对标 C# RMWMethods InPlaceUpdater 的 APPEND :799-834 /
  /// SETRANGE :734-763 增长臂；MainStore 层 InPlaceUpdater 锚由
  /// [`Self::try_modify_raw_in_place_unprotected`] 单点持有，本增长臂不重复挂锚）
  ///
  /// 回溯定位与等长改写共用 [`Self::trace_live_mutable_addr`]，差异只在披露给
  /// 闭包的值切片按**槽位物理容量**而非逻辑值长出参：闭包据此只写新字节并
  /// 回报新逻辑值长，改长由 whlog 原位内核经 RDH 单字原子协议发布——旧数据
  /// 零复制、零尾部追加。写监听通知同等长臂在持页写锁闭包内透传，且**必须**
  /// 披露改长后的新全值切片，绝不通知旧长度切片。
  ///
  /// `Ok(None)` = 未命中原位（键不存在 / 墓碑 / 密封 / 只读区 / 槽位富余不足），
  /// 调用方回落尾部追加；`Ok(Some(new_len))` = 已原位闭环。Err 含义与等长臂
  /// [`Self::try_modify_raw_in_place_unprotected`] 的「已生效+镜像缺失」不同：
  /// 本臂通知失败时新长度未发布（富余字节对读者不可见），值零变化，Err 即
  /// 真零副作用失败（票 zcode-r34-writekernel 条目二确认的对照臂）
  #[inline]
  pub fn try_grow_raw_in_place_unprotected(
    &self,
    key: &[u8],
    f: impl FnOnce(&mut [u8], usize) -> Option<usize>,
  ) -> Result<Option<usize>> {
    let Some(addr) = self.trace_live_mutable_addr(key)? else {
      return Ok(None);
    };
    // AOF 版本戳在生效点后单读（时序论证同等长臂：对齐 emit_event 分发时点读）
    let capture = self.store.event_sink.get().is_some();
    let mut notify_err = None;
    let wrapped = |cap: &mut [u8], old_len| {
      let new_len = f(&mut *cap, old_len)?;
      // 原位增长后通知的必须是新全值：超出新逻辑值长的富余字节尚未发布，
      // 截到 new_len 即与尾部追加臂的通知口径逐字节一致
      if capture && let Err(e) = self.notify_write_listener(key, &cap[..new_len], false) {
        notify_err = Some(e);
        return None;
      }
      Some(new_len)
    };
    let grown = self
      .store
      .hlog
      .try_grow_record_in_place(addr, key, wrapped)?;
    if let Some(e) = notify_err {
      return Err(e);
    }
    Ok(grown)
  }
}
