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
  /// 设备面恢复（C# RecoverAsync 路由：逐物理子日志扫描磁盘段定位位点）。
  /// 异步成员以 match 分派（两个后端各自的 future 类型不同，不经 route 闭包）。
  pub async fn recover_async(&self) {
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
  /// C# 的单日志分支即 shardedLog 循环在 Size == 1 时的同一形状，故按 size 单点循环。
  pub fn initialize_if(&self, recovered_safe_aof_address: &AofAddress) {
    for i in 0..self.size() {
      let tail = self.get_tail_address(i);
      let Some(safe_addr) = recovered_safe_aof_address.get(i) else {
        continue;
      };
      if tail < safe_addr {
        self.get_sub_log(i).safe_initialize(tail, safe_addr, 0);
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
  pub async fn wait_for_commit_async(&self, sublog_idx: usize, address: i64) {
    let target = if address == 0 {
      self.get_tail_address(sublog_idx)
    } else {
      address
    };
    let sublog = self.get_sub_log(sublog_idx);
    if sublog.committed_until_address() >= target {
      return;
    }
    // 短暂自旋退避，兼顾多核高频瞬时提交场景（零任务切换开销）
    for _ in 0..16 {
      if sublog.committed_until_address() >= target {
        return;
      }
      spin_loop();
    }
    // 设备面无锁事件驱动挂起等待
    sublog.wait_for_commit_async(target).await;
  }

  /// libs/server/AOF/GarnetLog.cs:WaitForCommitAsync
  ///
  /// 异步等待全部物理子日志提交落盘（0 表示等待当前尾地址；分片拓扑并发等待，
  /// 对标 C#:550-555 建 Task[] + Task.WhenAll）。
  pub async fn wait_for_commit_all_async(&self, until_address: i64) {
    if self.using_single_physical_log {
      self.wait_for_commit_async(0, until_address).await;
    } else {
      // 并发等待即并发驱动各子日志刷盘（WaofSublog::wait_for_commit_async 走
      // WalLog::wait_for_commit → commit_to，等待者本身是刷盘驱动），
      // 串行 await 会把 N 次刷盘串成求和
      join_all(
        (0..self.physical_sublog_count).map(|i| self.wait_for_commit_async(i, until_address)),
      )
      .await;
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
