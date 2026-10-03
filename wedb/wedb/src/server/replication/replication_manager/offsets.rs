//! 复制位点读写原语与异步追平等待（对标 C# GetReplicationOffset/WaitForReplicationOffsetAsync）

use super::*;

impl ReplicationManager {
  /// 注入主端角色实时谓词（对标 C# rm 读位点时反查
  /// clusterManager.CurrentConfig.LocalNodeRole == PRIMARY；rust 依赖方向反转，
  /// ClusterProvider::set_aof 装配期一次注入）。日志尾句柄不在此注入，
  /// 复用同一装配点写好的 AofSyncDriverStore.log，全 rm 只留一份本地 AOF 句柄。
  pub fn set_primary_role_source(&self, primary_role: Option<Arc<dyn Fn() -> bool + Send + Sync>>) {
    *self.primary_role.write() = primary_role;
  }

  /// 本地是否主端角色（对标 C# CurrentConfig.LocalNodeRole == PRIMARY；
  /// 未注入角色源时按主处理，对齐 ClusterProvider::is_primary 的
  /// unwrap_or(true)——无集群管理器的独立/未装配形态默认主）
  #[inline]
  fn is_primary_role(&self) -> bool {
    self.primary_role.read().as_ref().is_none_or(|f| f())
  }

  /// 本地 AOF 物理日志句柄（None = AOF 门控未点亮，对标 C# !EnableAOF）。
  /// 句柄由 ClusterProvider::set_aof 注入到 AofSyncDriverStore.log，即 C#
  /// storeWrapper.appendOnlyFile.Log 的唯一反查面——rm 内不再存第二份。
  #[inline]
  fn local_aof_log(&self) -> Option<Arc<GarnetLog>> {
    self.aof_sync_driver_store.log.read().clone()
  }

  /// 本地 AOF 日志尾（对标 C# storeWrapper.appendOnlyFile.Log.TailAddress；
  /// AOF 门控未点亮 = None，句柄同 C# 为一处注入）。私有读原语：只服务本
  /// 类型的角色分支 getter，INFO / gossip / failover 应答等消费方一律走
  /// [`Self::get_current_replication_offset`]，不得绕过角色分支直取日志尾。
  #[inline]
  fn replication_log_tail(&self) -> Option<AofAddress> {
    self.local_aof_log().map(|l| l.tail_address())
  }

  /// 本地指定子日志 AOF 尾（对标 C# appendOnlyFile.Log.GetTailAddress(sublogIdx)）
  /// 私有读原语，约束同上（唯一出口 [`Self::get_replication_offset`]）
  #[inline]
  fn replication_log_tail_at(&self, sublog_idx: usize) -> Option<i64> {
    self.local_aof_log().map(|l| l.get_tail_address(sublog_idx))
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:GetReplicationOffset
  /// libs/cluster/Server/Replication/ReplicationManager.cs:GetSublogReplicationOffset
  ///
  /// 获取指定子日志的复制偏移。主端角色且 AOF 在场时动态读日志尾
  /// （对标 C# PRIMARY 分支 appendOnlyFile.Log.GetTailAddress(sublogIdx)），
  /// 副本或未装配动态源时读 replication_offset 字段（副本重放链权威回推；
  /// C# GetSublogReplicationOffset 别名消费方同归本单点，不再另设转发层）。
  pub fn get_replication_offset(&self, sublog_idx: usize) -> i64 {
    if self.is_primary_role()
      && let Some(tail) = self.replication_log_tail_at(sublog_idx)
    {
      return tail;
    }
    self.replication_offset.read().get(sublog_idx).unwrap_or(0)
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:SetSublogReplicationOffset
  ///
  /// 设置指定子日志的复制偏移（对标 C# 直接赋值）。推进源 = 副本重放链
  /// 应用记录进存储后的权威回推（applied 语义，见 replica_replay_task
  /// consume_chunk；退化形态——重放资产缺席时——由副本会话流式落盘面
  /// 直推 enqueued）。
  pub fn set_sublog_replication_offset(&self, sublog_idx: usize, offset: i64) {
    self.replication_offset.write().set(sublog_idx, offset);
    self.wake_offset_waiters();
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:ReplicationOffset
  ///
  /// 获取当前完整的 AOF 复制地址。主端角色且 AOF 在场时动态读日志尾
  /// （对标 C# PRIMARY 分支 appendOnlyFile.Log.TailAddress），副本或未装配
  /// 动态源时读 replication_offset 字段。INFO / CLUSTER NODES / gossip /
  /// failover 停写应答的位点全部单点走本方法，不再各写一份读法。
  pub fn get_current_replication_offset(&self) -> AofAddress {
    if self.is_primary_role()
      && let Some(tail) = self.replication_log_tail()
    {
      return tail;
    }
    *self.replication_offset.read()
  }

  /// 提交尾复制地址：[`Self::try_update_for_failover`] 的取值源分解——AOF 在场
  /// 动态读 storeWrapper.appendOnlyFile.Log.CommittedUntilAddress，AOF 缺席
  /// 回退 replication_offset 字段。C# 侧 TryUpdateForFailover 映射登记在该方法。
  pub(super) fn get_committed_replication_offset(&self) -> AofAddress {
    if let Some(log) = self.local_aof_log() {
      return log.committed_until_address();
    }
    *self.replication_offset.read()
  }

  /// 设置当前完整的 AOF 复制地址（对标 C# 直接赋值）
  pub fn set_current_replication_offset(&self, offset: AofAddress) {
    *self.replication_offset.write() = offset;
    self.wake_offset_waiters();
  }

  /// 唤醒所有位点已追平的异步等待者（零锁争用，精确唤醒）
  fn wake_offset_waiters(&self) {
    // 快路径：无等待者单指令返回，消除高频复制流的热路径锁争用
    if self.waiters_count.load(Ordering::Acquire) == 0 {
      return;
    }
    let current = self.get_current_replication_offset();
    let mut waiters = self.offset_waiters.lock();
    waiters.retain_mut(|w| {
      if let Some(ref tx) = w.tx
        && tx.is_disconnected()
      {
        self.waiters_count.fetch_sub(1, Ordering::Release);
        return false;
      }
      if !current.any_lesser(&w.target) {
        if let Some(tx) = w.tx.take() {
          tx.send(());
        }
        self.waiters_count.fetch_sub(1, Ordering::Release);
        false
      } else {
        true
      }
    });
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:ReplicationCheckpointStartOffset
  pub fn get_replication_checkpoint_start_offset(&self) -> AofAddress {
    *self.replication_checkpoint_start_offset.read()
  }

  /// 设置检查点开始标记偏移
  pub fn set_replication_checkpoint_start_offset(&self, offset: AofAddress) {
    *self.replication_checkpoint_start_offset.write() = offset;
  }

  /// 设置指定子日志检查点开始标记偏移
  pub fn set_sublog_checkpoint_start_offset(&self, sublog_idx: usize, offset: i64) {
    self
      .replication_checkpoint_start_offset
      .write()
      .set(sublog_idx, offset);
  }

  /// libs/cluster/Server/Replication/ReplicationManager.cs:WaitForReplicationOffsetAsync
  ///
  /// 无内层超时的位点追平等待（对标 C#：调用侧 BlockingWait 不带超时，
  /// 唯一提前退出面是 ctsRepManager 停机取消）。追平返回当下位点
  /// （C# `return ReplicationOffset`），停机按 C# :572 返回 Create(
  /// AofPhysicalSublogCount, -1)。调用侧限时单点由 cluster_timeout 兜底
  /// （PrimaryFailoverSession.cs:22 WaitAsync(clusterTimeout)），本函数
  /// 不得叠加第二层内超时常量
  pub async fn wait_for_replication_offset_async(&self, target_offset: &AofAddress) -> AofAddress {
    // C# while 环首判对位：AnyLesser 假分支先于取消检查，追平即当下位点直返
    if !self
      .get_current_replication_offset()
      .any_lesser(target_offset)
    {
      return self.get_current_replication_offset();
    }
    // 先注册取消 listener（event_listener 5.4 listen() 创建即插入等待链），
    // 再读停机粘滞标志：观测到未置位时 dispose 的置位与 notify 必晚于插入，
    // 通知必达本 listener，无丢唤醒窗口；已置位即刻按 C# :572 返 -1 位点
    let mut cancel = self.cancel_event.listen();
    if self.cancelled.load(Ordering::SeqCst) {
      return self.cancelled_offset();
    }
    if self
      .wait_for_replication_offset_async_with_abort(target_offset, None, &mut cancel)
      .await
    {
      self.get_current_replication_offset()
    } else {
      self.cancelled_offset()
    }
  }

  /// 停机应答位点（对标 C# WaitForReplicationOffsetAsync :572 的
  /// AofAddress.Create(AofPhysicalSublogCount, -1)：各槽 -1 为明确的
  /// 未同步哨兵，调用方比对判定永不将其误认为追平）
  fn cancelled_offset(&self) -> AofAddress {
    AofAddress::create(self.sublog_count as i32, -1)
  }

  /// 等待副本位点追平目标位点（带中断面）
  ///
  /// duration 为 None 即无界等待，只与取消 listener 竞速（对标 C#
  /// WaitForReplicationOffsetAsync 轮询环本体）；Some 即有界等待（对标
  /// 轮询环外叠 WaitAsync(timeout, token) 的副本侧形态）。listener 须由
  /// 调用方预注册传入（listen() 创建即插入等待链），无界臂借此完成
  /// 「注册先于粘滞标志检查」的次序契约，杜绝 dispose 与挂起之间的丢唤醒。
  /// 取消触发或超时即以未追平收口并注销在途等待项。取消面的必要性对位：
  /// C# 轮询环天然观察超时、RPC 等待由 cts.Cancel 打断（FailoverManager.cs:
  /// TryAbortReplicaFailover → Dispose → cts.Cancel），rust 事件驱动 oneshot
  /// 等待必须由已注册 listener 精准唤醒，否则 abort 后在途位点等待挂满
  /// 剩余超时。
  pub async fn wait_for_replication_offset_async_with_abort(
    &self,
    target_offset: &AofAddress,
    duration: Option<Duration>,
    abort: &mut EventListener,
  ) -> bool {
    // 1. 快路径：位点已追平直接就绪，零分配
    if !self
      .get_current_replication_offset()
      .any_lesser(target_offset)
    {
      return true;
    }

    let (tx, rx) = oneshot();
    {
      let mut waiters = self.offset_waiters.lock();
      // 双检：登记锁间隙到来的位点推进
      if !self
        .get_current_replication_offset()
        .any_lesser(target_offset)
      {
        return true;
      }
      waiters.push(OffsetWaiter {
        target: *target_offset,
        tx: Some(tx),
      });
      self.waiters_count.fetch_add(1, Ordering::Release);
    }

    // 2. 挂起等待位点精确唤醒与中断 listener 竞速；结果先落定为本站变量，
    //    竞速 future 随语句终结释放（rx 断连）后清扫注销本等待项防泄漏
    //    ——若以 match 穿查 await，临时 future 活到 match 结束，清扫时
    //    is_disconnected 尚不成立，登记项将泄漏至下一次位点推进
    let caught = match duration {
      Some(d) => matches!(timeout(d, select(rx, abort)).await, Ok(Either::Left(_))),
      // 无界臂（C# 轮询环本体）：只与取消 listener 竞速，Left = 位点推进唤醒
      None => matches!(select(rx, abort).await, Either::Left(_)),
    };
    if !caught {
      let mut waiters = self.offset_waiters.lock();
      waiters.retain_mut(|w| {
        if let Some(ref tx) = w.tx
          && tx.is_disconnected()
        {
          self.waiters_count.fetch_sub(1, Ordering::Release);
          return false;
        }
        true
      });
    }
    caught
  }

  /// 是否存在在途位点等待项（诊断与测试观测面：中断/超时收口后应归零）
  pub fn has_offset_waiters(&self) -> bool {
    self.waiters_count.load(Ordering::Acquire) > 0
  }
}
