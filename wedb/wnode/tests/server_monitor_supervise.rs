//! 指标监视采样循环监督接线集成测试（工单 bg-periodic-task-supervise-gap-matrix
//! 项 c）：[`start_server_monitor`] 的 spawn 体经 wbase [`supervise_task`] 单点
//! 顶层监督——拉起后监督快照（INFO bg_task_health 真源）含本名且 alive 真；停机
//! 协调器落旗（对标 C# CancellationToken）终局后 alive 复位假；同名 panic 注入
//! 验证 panics 计数递增与 alive 复位假——采样冻结与无负载不再不可区分。Err 臂
//! 无需复位启动位（装配期一次性拉起，死亡留观测即可，不加自动重拉防毒丸风暴）。
//! 对标 C# GarnetServerMonitor.cs:MainMonitorTaskAsync catch LogCritical +
//! finally done.Set() 的死亡留痕契约。
//!
//! 本二进制仅此一个测试：监督注册表为进程级全局名单，panic 注入与真实拉起须
//! 串行执行。
use std::{sync::Arc, time::Duration};

use aok::{OK, Void};
use compio::{runtime::Runtime, time::sleep};
use wbase::supervise::{BgTaskSnapshot, snapshots, supervise_task};
use wmetric::GarnetServerMonitor;
use wnode::{
  ClusterProvider, MessageConsumerFace, SessionProviderFace, ShutdownCoordinator, WireFormat,
  server::start_server_monitor, servers::consumer_registry::ConsumerRegistry,
};

/// 哑会话域：不触存储与协议，仅占住监视循环的两臂句柄位
struct NullConsumer;

impl MessageConsumerFace for NullConsumer {
  fn try_consume_messages_into(&mut self, _resp_buf: &mut Vec<u8>) -> Option<usize> {
    Some(0)
  }
  fn take_recv_scratch(&mut self) -> Vec<u8> {
    Vec::new()
  }
  fn return_recv_scratch(&mut self, _buf: Vec<u8>) {}
  fn dispose(&mut self) {}
}

struct NullProvider;

impl SessionProviderFace for NullProvider {
  type Consumer = NullConsumer;
  fn get_session(&self, _wf: WireFormat, _id: u64) -> Option<NullConsumer> {
    Some(NullConsumer)
  }
}

/// 集群面哑探针（trait 全默认实现，单机对位 NoopClusterProvider 语义；
/// 自带 Clone 满足宿主泛型装配位）
#[derive(Clone, Copy)]
struct ProbeCluster;

impl ClusterProvider for ProbeCluster {}

/// 取指定任务名的监督快照条目
fn snapshot(name: &str) -> Option<BgTaskSnapshot> {
  snapshots().into_iter().find(|s| s.name == name)
}

#[test]
fn test_server_monitor_supervise_registration() -> Void {
  Runtime::new()?.block_on(async {
    let monitor = Arc::new(GarnetServerMonitor::new(1, true, false, false));
    let coordinator = ShutdownCoordinator::new();
    // 生产拉起路径：频率 1s > 0 → spawn 体经 supervise_task 包裹采样循环
    start_server_monitor(
      monitor,
      coordinator.clone(),
      Arc::new(ConsumerRegistry::new()),
      ProbeCluster,
      Arc::new(NullProvider),
      1,
    );
    // 让执行器 poll 到监督包装（注册与 alive 置真发生在 supervise_task 调用即行）
    sleep(Duration::from_millis(50)).await;
    let e = snapshot("server_monitor").expect("指标监视采样循环须注册进监督快照");
    assert!(e.alive, "在跑态存活位须为真");
    assert_eq!(e.panics, 0, "拉起即行无 panic");

    // 停机协调器落旗（对标 C# CancellationToken 取消采样循环）→ 循环在下一
    // await 点正常终局 → alive 复位假
    coordinator.stop();
    let mut exited = false;
    for _ in 0..500 {
      if snapshot("server_monitor").is_none_or(|s| !s.alive) {
        exited = true;
        break;
      }
      sleep(Duration::from_millis(10)).await;
    }
    assert!(exited, "落旗后采样循环退出，存活位复位假");

    // 同名 panic 注入（wbase 单点真实驱动、生产归组名）：panics 计数递增、
    // alive 保持复位假——采样停摆经 bg_task_health 可观测
    let out: Result<(), _> = supervise_task("server_monitor", async {
      panic!("测试注入毒丸探针（指标监视采样循环）");
    })
    .await;
    assert!(out.is_err(), "panic 臂须以 Err 产出");
    assert_eq!(
      out.unwrap_err().text(),
      "测试注入毒丸探针（指标监视采样循环）",
      "panic 载荷文本留痕"
    );
    let e = snapshot("server_monitor").expect("同名条目归组复用");
    assert_eq!(e.panics, 1, "panic 计数监督快照可观测");
    assert!(!e.alive, "panic 终局后存活位复位假");

    OK
  })
}
