//! 原位读-改-写路径（对标 C# Tsavorite InternalRMW & InPlaceUpdaterWorker）

use wdev::Device;

use crate::{error::Result, read_cache::is_read_cache_addr, session::StoreSession};

impl<D: Device> StoreSession<D> {
  /// 尝试在内存可变区原位读-改-写记录（严格对标 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs:InternalRMW.cs & InPlaceUpdaterWorker）
  ///
  /// 利用单趟探针定位槽位后，若记录处于内存可变区且键匹配非墓碑，直接持有页写锁在物理内存切片上执行闭包原地修改。
  /// 若原地修改成功返回 `Ok(Some(R))`，完全 0 堆分配、0 HLog 追加、0 哈希表 CAS！
  /// 若记录不在可变区、长度不符或闭包返回 `None`，安全返回 `Ok(None)` 供调用方降级到完整路径。
  pub fn try_modify_in_place<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&mut [u8]) -> Option<R>,
  ) -> Result<Option<R>> {
    let _guard = self.participant.enter();
    self.try_modify_in_place_unprotected(user_key, f)
  }

  /// 底层物理在已有纪元保护下尝试在内存可变区原位读-改-写记录（Raw）
  ///
  /// 严格对照 libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs:InPlaceUpdaterWorker &
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
    if let Some(addr) = self.store.index.find_tag(key) {
      let read_only_addr = self.store.hlog.read_only_address();
      let mut cur = if is_read_cache_addr(addr) {
        self.store.read_cache.skip_read_cache(addr)
      } else {
        addr
      };

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
            let capture = self.store.write_listener().is_some();
            let mut captured: Option<Vec<u8>> = None;
            let wrapped = |val: &mut [u8]| {
              let r = f(val);
              if capture && r.is_some() {
                captured = Some(val.to_vec());
              }
              r
            };
            let opt = self
              .store
              .hlog
              .try_modify_record_in_place(cur, key, wrapped)?;
            if opt.is_some()
              && let Some(new_val) = captured
            {
              self.notify_write_listener(key, &new_val, false);
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

  /// 在已有纪元保护下尝试在内存可变区原位读-改-写当前会话普通字符串记录（彻底绕过 enter() 原子开销）
  #[inline]
  pub fn try_modify_in_place_unprotected<R>(
    &self,
    user_key: &[u8],
    f: impl FnOnce(&mut [u8]) -> Option<R>,
  ) -> Result<Option<R>> {
    let str_k = self.session_string_key(user_key);
    self.try_modify_raw_in_place_unprotected(&str_k, f)
  }

  /// 在单次纪元保护下尝试利用动态松弛原位覆写当前会话字符串记录的值（严格对标 Tsavorite TrySetPinnedValueSpan 原位覆写语义）
  #[inline]
  pub fn try_modify_with_slack(&self, user_key: &[u8], new_val: &[u8]) -> Result<bool> {
    let _guard = self.participant.enter();
    self.try_modify_with_slack_unprotected(user_key, new_val)
  }

  /// 底层物理在已有纪元保护下尝试在内存可变区基于动态松弛原位覆写记录的值（Raw）
  #[inline]
  pub fn try_modify_raw_with_slack_unprotected(&self, key: &[u8], new_val: &[u8]) -> Result<bool> {
    if let Some(addr) = self.store.index.find_tag(key) {
      let read_only_addr = self.store.hlog.read_only_address();
      let mut cur = if is_read_cache_addr(addr) {
        self.store.read_cache.skip_read_cache(addr)
      } else {
        addr
      };

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
            let ok = self.store.hlog.try_update_in_place(cur, key, new_val)?;
            if ok {
              self.notify_write_listener(key, new_val, false);
            }
            return Ok(ok);
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
    Ok(false)
  }

  /// 在已有纪元保护下尝试在内存可变区基于动态松弛原位覆写当前会话普通字符串记录的值（严格对标 Tsavorite TrySetPinnedValueSpan 原位覆写语义）
  #[inline]
  pub fn try_modify_with_slack_unprotected(&self, user_key: &[u8], new_val: &[u8]) -> Result<bool> {
    let str_k = self.session_string_key(user_key);
    self.try_modify_raw_with_slack_unprotected(&str_k, new_val)
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
    if self.has_ttl_tag(user_key)? && self.check_expired(user_key).await? {
      // 已惰性清除，旧值按 None 处理
    }
    let str_k = self.session_string_key(user_key);
    self.rmw_raw(&str_k, updater).await
  }
}
