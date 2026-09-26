//! 初始化与提交等待面（对标 libs/server/AOF/GarnetLog.cs:RecoverAsync、
//! Reset、SafeInitialize、Initialize、InitializeIf、Commit、CommitAsync、
//! WaitForCommit、WaitForCommitAsync 段）。

use std::hint::spin_loop;

use event_listener::Listener;
use futures_util::future::join_all;
use waof::{AofAddress, NO_COOKIE};
use wbase::backoff::{Backoff, BackoffStage};

use super::GarnetLog;

impl GarnetLog {
  /// libs/server/AOF/GarnetLog.cs:RecoverAsync
  ///
  /// 设备面恢复（C# RecoverAsync 路由：逐物理子日志扫描磁盘段定位位点；
  /// ValueTask 透明上抛。C# 侧续行门控 FailOnRecoveryError 生效默认关；
  /// rust 侧该旗标零代码消费、回放驱动无此门，恢复失败恒拒启
  ///（刻意收紧，见 deviations.md §122））。
  /// 异步成员以 match 分派（两个后端各自的 future 类型不同，不经 route 闭包）。
  pub async fn recover_async(&self) -> waof::Result<()> {
    match &self.single_log {
      Some(single) => single.recover_async().await,
      None => self.sharded().recover_async().await,
    }
  }

  /// libs/server/AOF/GarnetLog.cs:Reset
  ///
  /// 重置日志（C# Reset 路由：逐物理子日志回退位点；FLUSHDB/FLUSHALL 出口。
  /// 设备后端持提交锁原子复位并 sync_data，对标 TsavoriteLog.Reset 持锁语义）。
  pub async fn reset_async(&self) {
    match &self.single_log {
      Some(single) => single.reset_async().await,
      None => self.sharded().reset_async().await,
    }
  }

  /// libs/server/AOF/GarnetLog.cs:SafeInitialize
  ///
  /// 安全初始化指定物理子日志位点（C# :427 `singleLog.log.SafeInitialize` /
  /// :429 `shardedLog.sublog[idx].SafeInitialize`）。
  pub fn safe_initialize(
    &self,
    sublog_idx: usize,
    begin_address: i64,
    committed_until_address: i64,
    last_commit_num: i64,
  ) {
    self.route(
      |single| {
        debug_assert_eq!(sublog_idx, 0);
        single
          .log
          .safe_initialize(begin_address, committed_until_address, last_commit_num);
      },
      |sharded| {
        sharded.sublog[sublog_idx].safe_initialize(
          begin_address,
          committed_until_address,
          last_commit_num,
        );
      },
    );
  }

  /// libs/server/AOF/GarnetLog.cs:Initialize
  pub fn initialize(
    &self,
    begin_address: &AofAddress,
    committed_until_address: &AofAddress,
    last_commit_num: i64,
  ) {
    self.route(
      |single| {
        debug_assert_eq!(begin_address.length(), 1);
        single.log.safe_initialize(
          begin_address[0],
          committed_until_address[0],
          last_commit_num,
        );
      },
      |sharded| {
        debug_assert_eq!(begin_address.length(), sharded.len() as i32);
        sharded.initialize(begin_address, committed_until_address, last_commit_num);
      },
    );
  }

  /// libs/server/AOF/GarnetLog.cs:InitializeIf
  ///
  /// 条件初始化：当子日志尾位点落后于恢复出的安全 AOF 地址时，将位点推进至安全位点。
  /// C# 的单日志分支即 shardedLog 循环在 Size == 1 时的同一形状，单次迭代物理子日志切片。
  pub fn initialize_if(&self, recovered_safe_aof_address: &AofAddress) {
    for (i, sublog) in self.sublogs().iter().enumerate() {
      let tail = sublog.tail_address();
      if let Some(safe_addr) = recovered_safe_aof_address.get(i)
        && tail < safe_addr
      {
        sublog.safe_initialize(tail, safe_addr, 0);
      }
    }
  }

  /// libs/server/AOF/GarnetLog.cs:CommitAsync
  ///
  /// 物理刷盘提交全部物理子日志（分片拓扑单点取全局单调 cookie 序列号后并发扇出，
  /// 耗时为子日志上界而非求和）。
  pub async fn commit_async(&self) {
    match &self.single_log {
      Some(single) => single.log.commit_flush_async(NO_COOKIE).await,
      None => {
        let cookie = self.next_sequence_number();
        // 取 join_all 而非 try_join_all：WhenAll 不取消兄弟、全部完成才聚合，
        // 短路丢弃 future 会令未落盘子日志的 AOF 持久化被取消；刷盘失败已在
        // WaofSublog::commit_flush_async 面显式告警，门面签名与 C# 同为无返回值
        join_all(
          self
            .sharded()
            .sublog
            .iter()
            .map(|sublog| sublog.commit_flush_async(cookie)),
        )
        .await;
      }
    }
  }

  /// 副本角色提交落盘：全部物理子日志纯刷盘（不写本地 commit 元数据帧）。
  ///
  /// 副本 AOF 须为主端流的严格镜像，本地帧会令重启后的增量协商位点漂出主端
  /// 帧边界（机制约束见 [`waof::WalLog::commit_flush_only`]；C# 对偶：副本
  /// CommitTaskAsync 跳过提交、Dispose 链无 CommitAsync）。无序列号分配——
  /// 副本序列号随主端转发帧收敛。sharded 分支取 [`join_all`] 而非
  /// try_join_all：副本落盘不可短路取消兄弟子日志（选型论证同
  /// [`GarnetLog::commit_async`]）
  pub async fn commit_flush_only_async(&self) {
    match &self.single_log {
      Some(single) => single.log.commit_flush_only_async().await,
      None => {
        join_all(
          self
            .sharded()
            .sublog
            .iter()
            .map(|sublog| sublog.commit_flush_only_async()),
        )
        .await;
      }
    }
  }

  /// libs/server/AOF/GarnetLog.cs:Commit
  ///
  /// 提交全部物理子日志；分片拓扑以同一 cookie（序列号）随各子日志提交，
  /// 恢复期经 [`GarnetLog::recover_latest_sequence_number`] 收敛恢复上界。
  pub fn commit(&self) {
    self.route(
      |single| {
        let tail = single.log.tail_address();
        single.log.commit(tail, NO_COOKIE);
      },
      |sharded| {
        let cookie = self.next_sequence_number();
        for sublog in &sharded.sublog {
          let tail = sublog.tail_address();
          sublog.commit(tail, cookie);
        }
      },
    );
  }

  /// 异步等待指定物理子日志提交落盘（0 表示等待当前尾地址；自旋退避 + 无锁事件挂起）。
  /// 提交失败沿等待上浮（C# GarnetLog.cs:WaitForCommitAsync await 即重抛，
  /// TsavoriteLog.cs:1866-1879）
  pub async fn wait_for_commit_async(&self, sublog_idx: usize, address: i64) -> waof::Result<()> {
    let target = if address == 0 {
      self.get_tail_address(sublog_idx)
    } else {
      address
    };
    let sublog = self.get_sub_log(sublog_idx);
    if sublog.committed_until_address() >= target {
      return Ok(());
    }
    // 短暂自旋退避，兼顾多核高频瞬时提交场景（零任务切换开销）
    for _ in 0..16 {
      if sublog.committed_until_address() >= target {
        return Ok(());
      }
      spin_loop();
    }
    // 设备面无锁事件驱动挂起等待
    sublog.wait_for_commit_async(target).await
  }

  /// libs/server/AOF/GarnetLog.cs:WaitForCommitAsync
  ///
  /// 异步等待全部物理子日志提交落盘（0 表示等待当前尾地址；分片拓扑并发等待，
  /// 对标 C#:550-555 建 Task[] + Task.WhenAll）。提交失败上浮（join_all 跑完
  /// 聚合取首个 Err，C# WhenAll 同款：不取消兄弟、聚合后抛第一个异常）
  pub async fn wait_for_commit_all_async(&self, until_address: i64) -> waof::Result<()> {
    if self.using_single_physical_log {
      self.wait_for_commit_async(0, until_address).await
    } else {
      // 并发等待即并发驱动各子日志刷盘（WaofSublog::wait_for_commit_async 走
      // WalLog::wait_for_commit → commit_to，等待者本身是刷盘驱动），
      // 串行 await 会把 N 次刷盘串成求和
      let results = join_all(
        (0..self.physical_sublog_count).map(|i| self.wait_for_commit_async(i, until_address)),
      )
      .await;
      results.into_iter().collect()
    }
  }

  /// libs/server/AOF/GarnetLog.cs:WaitForCommit
  ///
  /// 阶梯退避阻塞直至提交水位达到 `address`（0 表示等待当前尾地址）。
  /// 采用 wbase::backoff::Backoff（spin -> yield -> sleep/listener）阶梯退避，杜绝忙循环空转。
  pub fn wait_for_commit(&self, sublog_idx: usize, address: i64) {
    let target = if address == 0 {
      self.get_tail_address(sublog_idx)
    } else {
      address
    };
    let sublog = self.get_sub_log(sublog_idx);
    let mut backoff = Backoff::new();
    while sublog.committed_until_address() < target {
      match backoff.stage() {
        BackoffStage::Spin | BackoffStage::Yield => {
          backoff.snooze();
        }
        // 挂起等待子日志刷盘推进事件（先注册 listener 再复查水位，无丢唤醒）
        BackoffStage::Sleep => {
          let listener = sublog.flush_event().listen();
          if sublog.committed_until_address() >= target {
            break;
          }
          listener.wait();
        }
      }
    }
  }
}
