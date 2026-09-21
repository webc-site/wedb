//! 原位读-改-写路径（对标 C# Tsavorite InternalRMW & InPlaceUpdaterWorker）

use wbase::addr::is_read_cache;
use wdev::Device;

use crate::{error::Result, session::StoreSession};

impl<D: Device> StoreSession<D> {
  /// 沿 Tag 链回溯定位内存可变区中「键匹配、未密封、未墓碑」的记录地址
  /// （原位写族唯一回溯内核，等长改写与原位增长两臂共用，杜绝两套链走查）
  ///
  /// 在 garnet 中的相对路径:libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/FindRecord.cs:TryFindRecordForUpdate
  /// （更新路径的记录定位单点：C# 以 `minAddress = ReadOnlyAddress` 走
  /// TraceBackForKeyMatch 回溯并按 IsClosed 回 RETRY_LATER；rust 侧读路径回溯
  /// 单点在 [`super::read::StoreSession::trace_back_for_key_match`]，两者分属
  /// 读/写两臂不共用。本函数专精于可变区 `[read_only_addr, tail)` 的热路径
  /// 原位命中判据，零分配零拷贝；与诊断面 WedbStore::is_latest_hlog_version
  /// `[head, tail)` 全域版本判定各司其职）：首项为 ReadCache 虚拟地址、命中
  /// 密封（对应 RETRY_LATER 降级）/墓碑、滑出可变区或记录不可解一律回
  /// `Ok(None)`，调用方降级 RMW 完整路径。
  fn trace_live_mutable_addr(&self, key: &[u8]) -> Result<Option<u64>> {
    let hash = whasher::fast_hash(key);
    if self.store.is_growing() {
      self.store.split_buckets(hash)?;
    }
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
  /// （底层原位写锁与字节改写由 whlog/src/hlog/inplace.rs 承接；回溯定位单点见
  /// [`Self::trace_live_mutable_addr`]，命中未被墓碑标记的匹配记录即持页写锁
  /// 就地读-改-写；闭包返回 None 时返回 Ok(None) 供上层降级走 RMW 完整路径）。
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
    // 消费端仅向 AOF WAL 内存缓冲无锁入队，持锁内调用无重入死锁风险）
    let capture = self.store.event_sink.get().is_some();
    let mut notify_err = None;
    let wrapped = |val: &mut [u8]| {
      let r = f(val);
      if capture
        && r.is_some()
        && let Err(e) = self.notify_write_listener(key, val, false)
      {
        notify_err = Some(e);
        return None;
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
  /// 严格对标 C# libs/server/Storage/Functions/MainStore/RMWMethods.cs:InPlaceUpdater 的 APPEND :799-834 /
  /// SETRANGE :734-763 增长臂）
  ///
  /// 回溯定位与等长改写共用 [`Self::trace_live_mutable_addr`]，差异只在披露给
  /// 闭包的值切片按**槽位物理容量**而非逻辑值长出参：闭包据此只写新字节并
  /// 回报新逻辑值长，改长由 whlog 原位内核经 RDH 单字原子协议发布——旧数据
  /// 零复制、零尾部追加。写监听通知同等长臂在持页写锁闭包内透传，且**必须**
  /// 披露改长后的新全值切片，绝不通知旧长度切片。
  ///
  /// `Ok(None)` = 未命中原位（键不存在 / 墓碑 / 密封 / 只读区 / 槽位富余不足），
  /// 调用方回落尾部追加；`Ok(Some(new_len))` = 已原位闭环。
  #[inline]
  pub fn try_grow_raw_in_place_unprotected(
    &self,
    key: &[u8],
    f: impl FnOnce(&mut [u8], usize) -> Option<usize>,
  ) -> Result<Option<usize>> {
    let Some(addr) = self.trace_live_mutable_addr(key)? else {
      return Ok(None);
    };
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

  /// 底层物理读-改-写（RMW Raw，严格对照 C# Tsavorite InternalRMW 状态转移机）
  ///
  /// C# 上下文层 RMW 入口族在本 rust 单点的折叠映射（多态上下文已被统一会话
  /// 消除，一臂承接全部变体）：
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/BasicContext.cs:RMW
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/ITsavoriteContext.cs:RMW
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalContext.cs:RMW
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/TransactionalUnsafeContext.cs:RMW
  /// - libs/storage/Tsavorite/cs/src/core/ClientSession/UnsafeContext.cs:RMW
  ///
  /// 1. 若记录在内存可变区且尺寸匹配，优先就地读改写；
  /// 2. 若不可原位（只读区/磁盘区/未命中/尺寸变动）：
  ///    - 读取旧值切片（不存在或墓碑时传入 None）；
  ///    - 调用 `updater(Option<&[u8]>) -> Option<(R, Vec<u8>)>`；
  ///    - 若 updater 返回 None 则取消操作（对标 C# RMWAction.CancelOperation）；
  ///    - 否则走 upsert_raw 追加新记录（CopyUpdater / InitialUpdater）并 CAS 挂链；
  ///    - 返回 Ok(Some(R))。
  pub async fn rmw_raw<R, F>(&self, key: &[u8], updater: F) -> Result<Option<R>>
  where
    F: FnOnce(Option<&[u8]>) -> Option<(R, Vec<u8>)>,
  {
    let mut updater = Some(updater);
    let updated = self
      .read_raw_with(key, |val| {
        let u = unsafe { updater.take().unwrap_unchecked() };
        u(Some(val))
      })
      .await?;

    let update_res = match updated {
      Some(res) => res,
      None => {
        let u = unsafe { updater.take().unwrap_unchecked() };
        u(None)
      }
    };

    let Some((res, new_val)) = update_res else {
      return Ok(None);
    };
    self.upsert_raw(key, &new_val).await?;
    Ok(Some(res))
  }

  /// 读-改-写当前会话普通字符串键（RMW，对标 Redis 读改写指令如 INCR/APPEND 及 Tsavorite RMW）
  ///
  /// - 包含 TTL 惰性过期判定（已过期键物理清除并视作 None 传入闭包）；
  /// - 保持原有 key 级 TTL 记录不变（对齐 Redis 规范：INCR/APPEND 等读改写不清除 TTL）；
  /// - 闭包返回 None 时取消操作，返回 Some((res, new_val)) 时以 upsert_raw 追加写入新值。
  pub async fn rmw<R, F>(&self, user_key: &[u8], updater: F) -> Result<Option<R>>
  where
    F: FnOnce(Option<&[u8]>) -> Option<(R, Vec<u8>)>,
  {
    // 惰性过期裁决（probe_alive 单点）：已过期键物理清除后，旧值经墓碑路径按
    // None 处理（闭包 updater(None)）
    self.probe_alive(user_key).await?;
    let str_k = self.session_string_key(user_key);
    self.rmw_raw(&str_k, updater).await
  }
}
