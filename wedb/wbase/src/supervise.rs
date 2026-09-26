//! 后台任务监督域：panic 隔离薄包装的唯一单点（全仓 spawn 任务体 / 逐项
//! 处理体的 catch_unwind 一处封装，禁再散写第二机制）。
//!
//! 对标 libs/server/TaskManager/TaskManager.cs 的注册表观测面
//!（IsRunning/IsRegistered）与 C# 任务体 try/catch 异常必落日志的终止语义：
//! .NET 无 panic 穿透形态，rust unwind profile 下任务体 panic 即未来体展开
//! 终止，默认 hook 仅写 stderr（文件日志通道收不到），任务死亡与空闲不可
//! 区分。本域把「捕获 → 落 log::error（带任务名）→ 累计 panic 次数 → 以
//! Err 产出载荷」收成一口：
//! - [`supervise_task`]：任务级——注册存活位（启动即真、终局复位），供
//!   INFO 快照判死；调用方在 Err 臂复位各自幂等启动位，由既有 CONFIG SET
//!   调停 / 角色恢复 / 事件重拉通路自然复活（不新造重派机制）；
//! - [`supervise_item`]：逐项——消费循环内单条处理体的 panic 隔离，弃项
//!   续跑，主循环永续（对标 C# 逐项 catch 后进入下轮），同名归组累计；
//! - [`register_counter`]：进程级后台计数登记（无监督任务的标量面，
//!   如量化吞吐），随快照一并出。
//!
//! 快照经 [`snapshots`] / [`counter_snapshots`] 供 INFO server 段一行暴露，
//! 堵「panic 死亡与空闲不可区分」的观测缺口。

use core::{
  any::Any,
  fmt,
  future::Future,
  pin::Pin,
  task::{Context, Poll},
};
use std::{
  panic::{AssertUnwindSafe, catch_unwind},
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
  },
};

use parking_lot::Mutex;

/// panic 载荷盒（catch_unwind 的 Err 产物；文本化见 [`PanicPayload::text`]）
pub struct PanicPayload(Box<dyn Any + Send>);

impl fmt::Debug for PanicPayload {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    f.write_str(&self.text())
  }
}

impl PanicPayload {
  /// 裸载荷盒包装（供既持 `Box<dyn Any + Send>` 的守卫点转文本）
  pub fn new(payload: Box<dyn Any + Send>) -> Self {
    Self(payload)
  }

  /// 载荷转可读文本（&str / String 直取，其余退回固定描述）
  pub fn text(&self) -> String {
    Self::text_of(&*self.0)
  }

  /// 载荷引用转可读文本（&str / String 直取，其余退回固定描述）
  fn text_of(payload: &(dyn Any + Send)) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
      (*s).into()
    } else if let Some(s) = payload.downcast_ref::<String>() {
      s.clone()
    } else {
      "非文本 panic 载荷".into()
    }
  }
}

/// 监督注册表条目（进程生命周期固定名单：注册幂等、同名归组）
struct Entry {
  alive: AtomicBool,
  panics: AtomicU64,
}

static TASKS: Mutex<Vec<(&'static str, &'static Entry)>> = Mutex::new(Vec::new());

/// 同名条目认领（注册幂等：任务级与逐项监督同名共用 panic 计数）
fn entry(name: &'static str) -> &'static Entry {
  let mut tasks = TASKS.lock();
  if let Some((_, e)) = tasks.iter().find(|(n, _)| *n == name) {
    return e;
  }
  // 名单条目进程生命周期常驻，leak 换 &'static 免锁内生命周期纠缠
  let e = Box::leak(Box::new(Entry {
    alive: AtomicBool::new(false),
    panics: AtomicU64::new(0),
  }));
  tasks.push((name, e));
  e
}

/// 监督薄包装未来体（[`supervise_task`] / [`supervise_item`] 的产出物）
pub struct Supervised<F> {
  task: &'static str,
  track_alive: bool,
  fut: F,
}

impl<F: Future> Future for Supervised<F> {
  type Output = Result<F::Output, PanicPayload>;

  fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
    // SAFETY:结构化 pin 投影——仅在本次 poll 内借 &mut,绝不移动 fut、
    // 绝不交出其所有权,fut 自首 poll 起被外层 Pin 钉住
    let this = unsafe { self.get_unchecked_mut() };
    // SAFETY:同上,fut 为结构 pin 字段,此处仅构造投影引用
    let fut = unsafe { Pin::new_unchecked(&mut this.fut) };
    let e = entry(this.task);
    if this.track_alive {
      e.alive.store(true, Relaxed);
    }
    match catch_unwind(AssertUnwindSafe(|| fut.poll(cx))) {
      Ok(Poll::Ready(out)) => {
        if this.track_alive {
          e.alive.store(false, Relaxed);
        }
        Poll::Ready(Ok(out))
      }
      Ok(Poll::Pending) => Poll::Pending,
      Err(payload) => {
        e.panics.fetch_add(1, Relaxed);
        if this.track_alive {
          e.alive.store(false, Relaxed);
        }
        let text = PanicPayload::text_of(&*payload);
        log::error!("后台任务 {} panic(已监督隔离): {text}", this.task);
        Poll::Ready(Err(PanicPayload(payload)))
      }
    }
  }
}

/// 任务级监督：注册存活位（启动即真、终局或 panic 复位），panic 单点落
/// log::error 并计数，产出 `Err(PanicPayload)` 供调用方复位各自幂等启动位
/// （由既有 CONFIG SET 调停 / 角色恢复 / 事件重拉通路自然复活）
pub fn supervise_task<T, F: Future<Output = T>>(task: &'static str, fut: F) -> Supervised<F> {
  let e = entry(task);
  e.alive.store(true, Relaxed);
  Supervised {
    task,
    track_alive: true,
    fut,
  }
}

/// 逐项监督：消费循环内单条处理体的 panic 隔离——落日志 + 同名计数 + 弃项
/// 续跑（对标 C# 逐项 catch 后进入下轮），不动存活位
pub fn supervise_item<T, F: Future<Output = T>>(task: &'static str, fut: F) -> Supervised<F> {
  Supervised {
    task,
    track_alive: false,
    fut,
  }
}

/// 任务健康快照（alive 为任务级监督的存活位；逐项同名注册不影响）
pub struct BgTaskSnapshot {
  pub name: &'static str,
  pub alive: bool,
  pub panics: u64,
}

/// 进程级后台计数登记条目快照
pub struct BgCounterSnapshot {
  pub name: &'static str,
  pub value: u64,
}

static COUNTERS: Mutex<Vec<(&'static str, Arc<AtomicU64>)>> = Mutex::new(Vec::new());

/// 登记进程级后台计数（无监督任务的标量面；同名幂等，登记点为装配/构造期
/// 一次性，快照行一并出，堵「计数冻结与空闲不可区分」缺口）
pub fn register_counter(name: &'static str, counter: Arc<AtomicU64>) {
  let mut counters = COUNTERS.lock();
  if counters.iter().any(|(n, _)| *n == name) {
    return;
  }
  counters.push((name, counter));
}

/// 全部监督任务快照（按任务名排序，输出稳定）
pub fn snapshots() -> Vec<BgTaskSnapshot> {
  let mut rows: Vec<(&'static str, &Entry)> = TASKS.lock().clone();
  rows.sort_unstable_by_key(|(n, _)| *n);
  rows
    .into_iter()
    .map(|(name, e)| BgTaskSnapshot {
      name,
      alive: e.alive.load(Relaxed),
      panics: e.panics.load(Relaxed),
    })
    .collect()
}

/// 全部登记计数快照（按名排序，输出稳定）
pub fn counter_snapshots() -> Vec<BgCounterSnapshot> {
  let mut rows: Vec<(&'static str, Arc<AtomicU64>)> = COUNTERS.lock().clone();
  rows.sort_unstable_by_key(|(n, _)| *n);
  rows
    .into_iter()
    .map(|(name, c)| BgCounterSnapshot {
      name,
      value: c.load(Relaxed),
    })
    .collect()
}

#[cfg(test)]
mod tests {
  use std::{task::Waker, thread::yield_now};

  use super::*;

  /// 纯 park 驱动器（监督未来体不挂起：单 poll 即终局，noop waker 足够）
  fn drive<F: Future>(fut: F) -> F::Output {
    let mut fut = Box::pin(fut);
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    loop {
      if let Poll::Ready(v) = fut.as_mut().poll(&mut cx) {
        return v;
      }
      yield_now();
    }
  }

  #[test]
  fn supervise_task_ok_passes_through() {
    let out = drive(supervise_task("t_ok", async { 7 }));
    assert_eq!(out.unwrap(), 7);
    let e = snapshots().into_iter().find(|s| s.name == "t_ok").unwrap();
    assert!(!e.alive, "终局后存活位复位为假");
    assert_eq!(e.panics, 0);
  }

  #[test]
  fn supervise_task_panic_counts_and_reports() {
    let out: Result<(), _> = drive(supervise_task("t_panic", async {
      panic!("boom");
    }));
    assert_eq!(out.unwrap_err().text(), "boom");
    let e = snapshots()
      .into_iter()
      .find(|s| s.name == "t_panic")
      .unwrap();
    assert!(!e.alive);
    assert_eq!(e.panics, 1, "panic 次数累计一次");
    // 同名归组：逐项监督复用同一计数，且不触碰任务存活位
    let item: Result<(), _> = drive(supervise_item("t_panic", async {}));
    assert!(item.is_ok());
    let e = snapshots()
      .into_iter()
      .find(|s| s.name == "t_panic")
      .unwrap();
    assert_eq!(e.panics, 1);
    assert!(!e.alive, "逐项监督不得触碰任务存活位");
  }

  #[test]
  fn supervise_item_panics_do_not_touch_alive() {
    let out: Result<(), _> = drive(supervise_item("t_item", async {
      panic!("item boom");
    }));
    assert_eq!(out.unwrap_err().text(), "item boom");
    let e = snapshots()
      .into_iter()
      .find(|s| s.name == "t_item")
      .unwrap();
    assert_eq!(e.panics, 1);
    assert!(!e.alive, "逐项监督无任务级注册，存活位保持假");
  }

  #[test]
  fn counter_register_idempotent_and_snapshot() {
    let c = Arc::new(AtomicU64::new(3));
    register_counter("c_x", Arc::clone(&c));
    register_counter("c_x", Arc::clone(&c));
    c.fetch_add(2, Relaxed);
    let e = counter_snapshots()
      .into_iter()
      .find(|s| s.name == "c_x")
      .unwrap();
    assert_eq!(e.value, 5);
  }

  #[test]
  fn payload_text_covers_str_string_and_other() {
    assert_eq!(PanicPayload::new(Box::new("静态串")).text(), "静态串");
    assert_eq!(
      PanicPayload::new(Box::new("堆串".to_owned())).text(),
      "堆串"
    );
    assert_eq!(
      PanicPayload::new(Box::new(42_u32)).text(),
      "非文本 panic 载荷"
    );
  }
}
