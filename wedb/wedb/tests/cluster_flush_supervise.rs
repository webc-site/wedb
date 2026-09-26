//! 集群配置周期刷盘任务监督接线集成测试（工单 bg-periodic-task-supervise-gap-matrix
//! 项 b）：[`ClusterManager::start_flush_task`] 的 spawn 体经 wbase [`supervise_task`]
//! 单点顶层监督——装配期真实拉起（initialize_cluster_config 频刷 > 0 走生产路径）
//! 后监督快照（INFO bg_task_health 真源）含本名且 alive 真；停机臂落旗终局后
//! alive 复位假；同名 panic 注入验证 panics 计数递增与 alive 复位假。Err 臂复位
//! flush_running 的幂等门纪律沿 supervise.rs 模块头自陈（upgrade 失败即宿主已亡
//! 无需复位），与同族 reclaim.rs 监督快照测试同形闭环。对标 C#
//! ClusterManager.cs 构造尾 Task.Run(FlushTaskAsync) + try-finally numActiveTasks
//! 的终止留痕契约。
//!
//! 本二进制仅此一个测试：监督注册表为进程级全局名单，panic 注入与真实拉起须
//! 串行执行。
use std::time::Duration;

use aok::{OK, Void};
use compio::{runtime::Runtime, time::sleep};
use wbase::supervise::{BgTaskSnapshot, snapshots, supervise_task};
use wedb::server::cluster_provider::ClusterProvider;

/// 取指定任务名的监督快照条目
fn snapshot(name: &str) -> Option<BgTaskSnapshot> {
  snapshots().into_iter().find(|s| s.name == name)
}

#[test]
fn test_cluster_flush_task_supervise_registration() -> Void {
  Runtime::new()?.block_on(async {
    let dir = tempfile::tempdir()?.keep();
    let cp = ClusterProvider::new();
    // 真实装配路径：刷盘频率 50ms > 0 → initialize_cluster_config 尾段
    // start_flush_task 拉起周期刷盘任务（幂等门 swap(true) 闭合）
    cp.initialize_cluster_config("127.0.0.1", 7301, &dir.join("nodes.conf"), 50, true, "")?;
    let cm = cp.cluster_manager().expect("集群管理器已装配");
    // 让执行器 poll 到监督包装（注册与 alive 置真发生在 supervise_task 调用即行）
    sleep(Duration::from_millis(100)).await;
    let e = snapshot("cluster_flush").expect("周期刷盘任务须注册进监督快照（bg_task_health）");
    assert!(e.alive, "在跑态存活位须为真");
    assert_eq!(e.panics, 0, "拉起即行无 panic");

    // 停机臂落旗（dispose_background_tasks 复位 flush_running）→ 循环 break
    // 正常终局 → alive 复位假
    cm.dispose_background_tasks();
    let mut exited = false;
    for _ in 0..300 {
      if snapshot("cluster_flush").is_none_or(|s| !s.alive) {
        exited = true;
        break;
      }
      sleep(Duration::from_millis(10)).await;
    }
    assert!(exited, "落旗后循环在一个周期内退出，存活位复位假");

    // 同名 panic 注入（wbase 单点真实驱动、生产归组名）：panics 计数递增、
    // alive 保持复位假——刷盘停摆经 bg_task_health 可观测，不再静默冻结
    let out: Result<(), _> = supervise_task("cluster_flush", async {
      panic!("测试注入毒丸探针（集群配置周期刷盘）");
    })
    .await;
    assert!(out.is_err(), "panic 臂须以 Err 产出");
    assert_eq!(
      out.unwrap_err().text(),
      "测试注入毒丸探针（集群配置周期刷盘）",
      "panic 载荷文本留痕"
    );
    let e = snapshot("cluster_flush").expect("同名条目归组复用");
    assert_eq!(e.panics, 1, "panic 计数监督快照可观测");
    assert!(!e.alive, "panic 终局后存活位复位假");

    drop(cm);
    OK
  })
}
