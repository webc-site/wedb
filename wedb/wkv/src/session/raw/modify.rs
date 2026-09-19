//! 原位读-改-写路径（对标 C# Tsavorite InternalRMW & InPlaceUpdaterWorker）

use wbase::addr::is_read_cache;
use wdev::Device;

use crate::{error::Result, session::StoreSession};

impl<D: Device> StoreSession<D> {
  /// 底层物理在已有纪元保护下尝试在内存可变区原位读-改-写记录（Raw）
  ///
  /// 严格对照 C# MainStore InPlaceUpdaterWorker（精确锚点见 whlog/src/hlog/inplace.rs）&
  /// FindRecord.cs:TraceBackForKeyMatch：
  /// - 首项若是 ReadCache 虚拟地址，顺链获取主日志真实地址；
  /// - 沿 prev_address 反向回溯可变区 `[read_only_addr, tail)` 消解 15 位 Tag 碰撞；
  /// - 若命中未被墓碑标记的匹配记录，持有页写锁就地读-改-写；
  /// - 遭遇墓碑、滑出可变区或闭包返回 None（尺寸不匹配等）时返回 Ok(None) 供上层降级走 RMW 完整路径。
  #[inline]
  pub fn try_modify_raw_in_place_unprotected<R>(
    &self,
    key: &[u8],
    f: impl FnOnce(&mut [u8]) -> Option<R>,
  ) -> Result<Option<R>> {
    let hash = whasher::fast_hash(key);
    if self.store.is_growing() {
      self.store.split_buckets(hash)?;
    }
    if let Some(addr) = self.store.index.load().find_tag_by_hash(hash) {
      if is_read_cache(addr) {
        return Ok(None);
      }
      let read_only_addr = self.store.hlog.read_only_address();
      let mut cur = addr;

      while cur >= read_only_addr {
        let probe = self.store.hlog.with_memory_record(cur, |rec| {
          if rec.matches_key(key) {
            Ok((true, rec.is_closed(), rec.is_tombstone(), 0))
          } else {
            Ok((false, false, false, rec.prev_address()))
          }
        });

        match probe {
          Ok(Some((true, false, false, _))) => {
            // 写监听通知在持有页写锁的闭包内直接借用更新后切片透传，零堆分配
            // （对标 C# InPlaceUpdater 仅置 NeedAofLog 标志、上层按输入透传 AOF，
            // 原位路径无"读回更新值"的中间拷贝；StoreEvent 为借用型同步分发，
            // 消费端仅向 AOF WAL 内存缓冲无锁入队，持锁内调用无重入死锁风险）
            let capture = self.store.has_event_sink();
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
              .try_modify_record_in_place(cur, key, wrapped)?;
            if let Some(e) = notify_err {
              return Err(e);
            }
            return Ok(opt);
          }
          // 密封在途记录命中（C# InPlaceUpdater 对 IsClosed 记录拒绝原位修改）：
          // 降级完整路径走 CopyUpdater 追加
          Ok(Some((true, true, ..))) => break,
          Ok(Some((false, .., prev))) => {
            cur = prev;
          }
          _ => break,
        }
      }
    }
    Ok(None)
  }

  /// 底层物理读-改-写（RMW Raw，严格对照 C# Tsavorite InternalRMW 状态转移机）
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
