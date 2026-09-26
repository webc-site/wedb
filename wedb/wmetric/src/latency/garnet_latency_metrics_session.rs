use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use parking_lot::Mutex;

use super::{
  garnet_latency_metrics::GarnetLatencyMetrics,
  latency_metrics_entry_session::LatencyMetricsEntrySession,
  latency_metrics_type::LatencyMetricsType,
};

/// 会话侧延迟指标：连接任务独占的双缓冲直方图组。
///（对标 libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:GarnetLatencyMetricsSession）
///
/// 条目数组与会话同生命周期、迭代奇偶双槽互斥，两点均与 C# 同形；差异只在
/// 跨线程一面：C# 监视器经 `ActiveConsumers` 裸读会话数组的上一版本槽
/// （`SingleWriterMultiReaderLock` 只护 `Return` 置空），rust 的所有权模型不容
/// 跨线程裸读，故把「监视器读会话槽」倒转为「属主线程在版本翻转点把退役槽
/// 并入全局表」。本类型因此不含任何锁、不再 `Arc` 共享：记点一律 `&mut self`，
/// 唯一共享态是只读的原子迭代时钟（C# `monitor.monitor_iterations` 同源）。
pub struct GarnetLatencyMetricsSession {
  /// 监视器迭代时钟（绝对迭代计数直读；对标 C# `monitor.monitor_iterations`）。
  monitor_iterations: Arc<AtomicU64>,
  /// 归并出口（对标 C# `monitor.GlobalMetrics.globalLatencyMetrics`）；
  /// 监视器未装配时为空，退役槽就地清零。
  global: Option<Arc<Mutex<GarnetLatencyMetrics>>>,
  /// 本会话上次处理时钟时的绝对迭代计数；其奇偶即当前写入槽。翻转检测以
  /// 绝对迭代差为准——只存折叠奇偶会漏检偶数个周期的跨越（ABA 回绕盲区），
  /// 令退役槽永不结算、新旧样本混写同槽。
  last_iteration: u64,
  /// 双缓冲条目，下标即类别判别值
  ///（对标 C# `public LatencyMetricsEntrySession[] metrics`）。
  pub metrics: Vec<LatencyMetricsEntrySession>,
}

impl GarnetLatencyMetricsSession {
  /// 默认统计的全部延迟类别（对齐 C# defaultLatencyTypes）。
  pub const DEFAULT_LATENCY_TYPES: &[LatencyMetricsType] = &LatencyMetricsType::ALL;

  /// 版本 0 槽（时钟奇偶值即槽位下标）。
  const SLOT_ZERO: usize = 0;
  /// 版本 1 槽。
  const SLOT_ONE: usize = 1;

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:GarnetLatencyMetricsSession（构造）
  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:Init（构造内建直方图表，C# Init 分配语义并入 new）。
  pub fn new(
    monitor_iterations: Arc<AtomicU64>,
    global: Option<Arc<Mutex<GarnetLatencyMetrics>>>,
    latency_types: &'static [LatencyMetricsType],
  ) -> Self {
    Self {
      last_iteration: monitor_iterations.load(Ordering::Relaxed),
      monitor_iterations,
      global,
      metrics: latency_types
        .iter()
        .map(|_| LatencyMetricsEntrySession::new())
        .collect(),
    }
  }

  /// 当前写入版本槽（上次处理的绝对迭代计数之奇偶，C# `Version` 同源）。
  #[inline]
  pub fn version(&self) -> usize {
    (self.last_iteration % 2) as usize
  }

  /// 版本翻转检测：以绝对迭代差按迭代周期结算，并把退役槽并入全局出口。
  ///
  /// `diff == 0` 同周期未跨越；`diff == 1` 恰跨一周期，结算退役槽；
  /// `diff >= 2` 跨多周期（含旧奇偶折叠漏检的偶数盲区），两槽皆属过期
  /// 窗口，一并结算。每次记点仅一次原子读 + 整数比较，归并分支每跨周期
  /// 至多走一次。
  #[inline]
  fn roll_version(&mut self) -> usize {
    let curr_iter = self.monitor_iterations.load(Ordering::Relaxed);
    match curr_iter.saturating_sub(self.last_iteration) {
      0 => {}
      1 => {
        let retired = (self.last_iteration % 2) as usize;
        self.last_iteration = curr_iter;
        self.flush(retired);
      }
      _ => {
        self.last_iteration = curr_iter;
        self.flush(Self::SLOT_ZERO);
        self.flush(Self::SLOT_ONE);
      }
    }
    (curr_iter % 2) as usize
  }

  /// 把指定版本槽按引用并入全局出口后清零
  ///（C# 监视器侧 `GarnetLatencyMetrics.cs:Merge` 与
  /// `GarnetServerMonitor.cs:ResetLatencySessionMetrics` 两步在属主线程合一）。
  fn flush(&mut self, ver: usize) {
    match &self.global {
      // 归并即清零，见 GarnetLatencyMetrics::merge
      Some(global) => Arc::clone(global).lock().merge(&mut self.metrics, ver),
      // 无归并出口（监视器未装配）：退役槽无处可去，就地清零免跨窗累加
      None => self.reset_slot(ver),
    }
  }

  /// 清零指定版本槽（保留堆内存，不触发重分配）。
  fn reset_slot(&mut self, ver: usize) {
    for entry in &mut self.metrics {
      entry.latency[ver].reset();
    }
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:Return
  ///
  /// 会话释放：C# 由监视器在 `AddMetricsHistorySessionDispose` 里先 Merge
  /// 上一版本槽、再 Return 归还条目；rust 归并只发生在属主线程，故两槽一并
  /// 在此结算（上一版本槽归并后即空，双计无从发生），样本不随释放丢失。
  pub fn return_to_pool(&mut self) {
    self.flush(Self::SLOT_ZERO);
    self.flush(Self::SLOT_ONE);
    for entry in &mut self.metrics {
      entry.return_to_pool();
    }
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:Start
  ///
  /// 记录指定类别的起始时间戳。`now_ticks` 为当前 Stopwatch tick（单调时钟）。
  /// 批首顺带做版本翻转检测：C# 的归并由监视器异步发起，rust 须在本线程
  /// 下一次触碰延迟表时完成，批首即命令执行前，全局表据此先获得上一窗样本。
  #[inline]
  pub fn start(&mut self, cmd: LatencyMetricsType, now_ticks: u64) {
    self.roll_version();
    if let Some(entry) = self.metrics.get_mut(cmd.idx()) {
      entry.start(now_ticks);
    }
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:Get
  ///
  /// 读取指定类别的起始时间戳。
  #[inline]
  pub fn get(&self, cmd: LatencyMetricsType) -> u64 {
    self
      .metrics
      .get(cmd.idx())
      .map_or(0, |entry| entry.start_timestamp)
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:StopAndSwitch
  ///
  /// 停止旧类别并将起始时间戳转移给新类别后记录（同一操作跨类别切换）。
  /// C# 三句按「转移 → 置零 → 记新槽」顺序逐字段直写，rust 同序执行：
  /// 同类别输入即读即清，自然退化为清戳不记样本（C# 同形，无须专分支）。
  #[inline]
  pub fn stop_and_switch(
    &mut self,
    old_cmd: LatencyMetricsType,
    new_cmd: LatencyMetricsType,
    now_ticks: u64,
  ) {
    let ver = self.roll_version();
    let (old_idx, new_idx) = (old_cmd.idx(), new_cmd.idx());
    if let Some(timestamp) = self.metrics.get(old_idx).map(|old| old.start_timestamp)
      && let Some(new) = self.metrics.get_mut(new_idx)
    {
      new.start_timestamp = timestamp;
    }
    if let Some(old) = self.metrics.get_mut(old_idx) {
      old.start_timestamp = 0;
    }
    if let Some(new) = self.metrics.get_mut(new_idx) {
      new.record_value(ver, now_ticks);
    }
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:Stop
  ///
  /// 结束指定类别的进行中操作并按当前版本记录。
  #[inline]
  pub fn stop(&mut self, cmd: LatencyMetricsType, now_ticks: u64) {
    let ver = self.roll_version();
    if let Some(entry) = self.metrics.get_mut(cmd.idx()) {
      entry.record_value(ver, now_ticks);
    }
  }

  /// libs/server/Metrics/Latency/GarnetLatencyMetricsSession.cs:RecordValue
  ///
  /// 直接记录一段耗时（按当前版本）。
  #[inline]
  pub fn record_value(&mut self, cmd: LatencyMetricsType, elapsed: i64) {
    let ver = self.roll_version();
    if let Some(entry) = self.metrics.get_mut(cmd.idx()) {
      entry.record_elapsed(ver, elapsed);
    }
  }
}

#[cfg(test)]
mod tests {
  use hdrhistogram::Histogram;

  use super::*;

  /// 监视器时钟 + 全局出口（会话归并目标）。
  struct Env {
    iterations: Arc<AtomicU64>,
    global: Arc<Mutex<GarnetLatencyMetrics>>,
  }

  fn env() -> Env {
    Env {
      iterations: Arc::new(AtomicU64::new(0)),
      global: Arc::new(Mutex::new(GarnetLatencyMetrics::new(
        GarnetLatencyMetrics::DEFAULT_LATENCY_TYPES,
      ))),
    }
  }

  fn session(env: &Env) -> GarnetLatencyMetricsSession {
    GarnetLatencyMetricsSession::new(
      Arc::clone(&env.iterations),
      Some(Arc::clone(&env.global)),
      GarnetLatencyMetricsSession::DEFAULT_LATENCY_TYPES,
    )
  }

  /// 全局出口指定类别的样本数。
  fn global_calls(env: &Env, cmd: LatencyMetricsType) -> u64 {
    env
      .global
      .lock()
      .metrics
      .get(cmd.idx())
      .map_or(0, Histogram::len)
  }

  #[test]
  fn test_session_lifecycle_and_switching() {
    let env = env();
    let mut session = session(&env);

    assert_eq!(session.version(), 0);

    // 1. start + stop
    session.start(LatencyMetricsType::NetRsLat, 100);
    assert_eq!(session.get(LatencyMetricsType::NetRsLat), 100);
    session.stop(LatencyMetricsType::NetRsLat, 200);
    assert_eq!(session.get(LatencyMetricsType::NetRsLat), 0);
    assert_eq!(
      session.metrics[LatencyMetricsType::NetRsLat.idx()].latency[0].len(),
      1
    );

    // 2. stop_and_switch（同类别输入：C# 即读即清，清戳不记样本）
    session.start(LatencyMetricsType::NetRsLat, 300);
    session.stop_and_switch(
      LatencyMetricsType::NetRsLat,
      LatencyMetricsType::NetRsLat,
      500,
    );
    assert_eq!(session.get(LatencyMetricsType::NetRsLat), 0);
    assert_eq!(
      session.metrics[LatencyMetricsType::NetRsLat.idx()].latency[0].len(),
      1
    );

    // 3. stop_and_switch（跨命令切换：戳转移后记入新类别）
    session.start(LatencyMetricsType::NetRsLat, 600);
    session.stop_and_switch(
      LatencyMetricsType::NetRsLat,
      LatencyMetricsType::NetRsLatAdmin,
      800,
    );
    assert_eq!(
      session.metrics[LatencyMetricsType::NetRsLatAdmin.idx()].latency[0].len(),
      1
    );
    assert_eq!(session.get(LatencyMetricsType::NetRsLat), 0);

    // 4. record_value
    session.record_value(LatencyMetricsType::NetRsBytes, 1024);
    assert_eq!(
      session.metrics[LatencyMetricsType::NetRsBytes.idx()].latency[0].len(),
      1
    );

    // 5. 版本翻转：退役槽并入全局后清零（slot0 累计 NET_RS_LAT 样本仅步骤 1
    // 一条：步骤 2 同类别清戳不记账，步骤 3 戳已转移）
    env.iterations.fetch_add(1, Ordering::Relaxed);
    session.start(LatencyMetricsType::NetRsLat, 900);
    assert_eq!(session.version(), 1);
    assert_eq!(global_calls(&env, LatencyMetricsType::NetRsLat), 1);
    assert_eq!(
      session.metrics[LatencyMetricsType::NetRsLat.idx()].latency[0].len(),
      0
    );

    // 6. 会话释放：残余槽同样计入全局且不再重复
    session.stop(LatencyMetricsType::NetRsLat, 1_300);
    session.return_to_pool();
    assert_eq!(global_calls(&env, LatencyMetricsType::NetRsLat), 2);
    assert_eq!(
      session.metrics[LatencyMetricsType::NetRsLat.idx()].latency[1].len(),
      0
    );
  }
}
