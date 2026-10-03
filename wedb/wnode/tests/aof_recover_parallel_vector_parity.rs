//! 向量族条目并行恢复对账回归（task/ing/zcode-r43-parrecover 发现二）
//!
//! 对标 C# AofReplayCoordinator.cs:InitializeReplayContext 每虚拟子日志专用
//! RespServerSession（AofProcessor.cs:161-162 ObtainServerSession，StoreRMW
//! 重放臂显式携带 replayContext.respServerSession 下传）——C# 并行回放上下文
//! 自持会话，向量族重放无「worker 无会话」形态。
//!
//! 回归点：rust 向量应用臂依赖 thread_local 会话槽（ACTIVE_VECTOR_SESSION），
//! 绑定唯一发生在重放装配点 leader 线程（replay_database_aof 的
//! ActiveVectorSessionGuard::bind）；并行臂 worker 系全新 OS 线程，修复前线程
//! 槽恒空——worker 应用 VADD/VREM/VSETATTR 时索引登记/元素写臂取槽 None 按
//! 失败口径报错 → 并行恢复恒败（存量键则登记写透静默跳过，恢复完成态缺向量
//! 数据无信号），同输入单任务臂健康，单/多路径行为不同构。修复后 worker 私有
//! 会话同窗绑入本线程槽，向量族并行恢复与单任务臂逐键对账一致：VSIM 检索、
//! 已删元素不复活、重复 VADD 判 Duplicate（登记回建 + context 原位复用）。
//!
//! 键定向：VADD 键经 GarnetLog::hash + get_replay_task_idx 定向路由至非 0 号
//! 回放任务，确保向量族条目在并行恢复中确凿落于 worker（非 leader 自有域）。

use std::{borrow::Cow, sync::Arc, time::Duration};

use compio::{net::TcpStream, runtime::Runtime, time::timeout};
use tempfile::TempDir;
use wconf::RuntimeServerOptions;
use wnode::{aof::garnet_log::GarnetLog, service::StorageSessionProvider};
use wnode_test::{read_line_reply, read_reply, send_cmd, session_factory, start_server};
use wtest_base::test_store_config;

/// 有界时限：恢复在 provider 开库段完成（先于 accept），修复前 worker 向量臂
/// 报错即恢复失败（拒启可观测）；本时限兜底任何挂起形态
const BOUNDED_TIMEOUT: Duration = Duration::from_secs(60);

/// FP32 向量字节（行主小端）
fn fp32(vals: &[f32]) -> Vec<u8> {
  vals.iter().flat_map(|v| v.to_le_bytes()).collect()
}

fn lossy(bytes: &[u8]) -> Cow<'_, str> {
  String::from_utf8_lossy(bytes)
}

/// 定向路由键：枚举键名，取并行恢复（count=4，单物理日志）归属哈希恰落到
/// 指定回放任务的键。路由纯函数式（GarnetLog::get_replay_task_idx 同式：
/// `hash as u64 / 物理日志数 % 回放任务数`，单物理日志即 `hash % 4`），
/// 不依赖任一代 provider 的拓扑实值——写入代 count=1 与恢复代 count=4
/// 取到同一键名，键在恢复代确凿落于非 0 号 worker
fn key_for_task(task: usize) -> Vec<u8> {
  assert!(
    task > 0,
    "定向目标必须为非 0 号 worker（任务 0 即 leader 自有域）"
  );
  for i in 0..1024u32 {
    let key = format!("vpts-{task}-{i}").into_bytes();
    let hash = GarnetLog::hash(&key);
    if (hash as u64) % 4 == task as u64 {
      return key;
    }
  }
  unreachable!("1024 个候选键必覆盖全部回放任务桶");
}

/// 恢复探针应答（VSIM 主键 / VSIM 已删键 / 重复 VADD 判定）
type Probes = (Vec<u8>, Vec<u8>, Vec<u8>);

/// 单代 lifecycle：写入（count=1，向量族命令经生产 RESP 面）→ 提交停服 →
/// 恢复（`recover_count` 决定并行/单任务拓扑）→ 检索探针。写入代零检查点
///（无 SAVE），向量族条目的唯一来源即 AOF 重放——恢复拓扑面被直接点亮。
async fn lifecycle(recover_count: i32) -> aok::Result<Probes> {
  let dir = TempDir::new()?;
  let data_path = dir.path().join("node").join("vector-para.db");

  // ---- 写入代（单任务拓扑）：三个定向键分别路由至回放任务 1/2/3，
  //      VADD 建集 + VSETATTR 改属性 + VREM 删元素（向量族三形态全覆盖）
  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      test_store_config(),
      &data_path,
      None,
      RuntimeServerOptions {
        aof_physical_sublog_count: 1,
        aof_replay_task_count: 1,
        ..RuntimeServerOptions::default()
      },
      session_factory,
    )
    .expect("open with aof")
    .with_vector_set_preview(true),
  );
  let (server, addr) = start_server(Arc::clone(&provider));
  {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    for task in 1..=3usize {
      let key = key_for_task(task);
      let elem: &[u8] = if task == 1 { b"e-removed" } else { b"e-live" };
      send_cmd(
        &mut stream,
        &[b"VADD", &key, b"FP32", &fp32(&[task as f32, 1.0]), elem],
      )
      .await
      .expect("vadd");
      let reply = read_line_reply(&mut stream).await;
      assert_eq!(reply, b":1\r\n", "写入代 VADD 建集必须成功（task {task}）");
      send_cmd(&mut stream, &[b"VSETATTR", &key, elem, b"{\"n\":1}"])
        .await
        .expect("vsetattr");
      let attr_reply = read_line_reply(&mut stream).await;
      assert!(
        !attr_reply.starts_with(b"-"),
        "写入代 VSETATTR 不得报错: {:?}",
        lossy(&attr_reply)
      );
      if task == 1 {
        send_cmd(&mut stream, &[b"VREM", &key, elem])
          .await
          .expect("vrem");
        let rem_reply = read_line_reply(&mut stream).await;
        assert_eq!(rem_reply, b":1\r\n", "写入代 VREM 删元素必须成功");
      }
    }
    // 零检查点提交落盘：向量族条目唯一持久载体即 AOF（恢复重放直接点亮）
    provider
      .aof()
      .expect("aof enabled")
      .log()
      .commit_async()
      .await;
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
  }
  server.stop();
  drop(server);
  drop(provider);

  // ---- 恢复代（`recover_count` 拓扑）：恢复先于 accept，向量族条目经
  //      并行 worker 重放（键定向路由至非 0 号任务）
  let provider2 = Arc::new(
    timeout(
      BOUNDED_TIMEOUT,
      StorageSessionProvider::open_recovered_with_config_and_aof(
        test_store_config(),
        &data_path,
        None,
        RuntimeServerOptions {
          aof_physical_sublog_count: 1,
          aof_replay_task_count: recover_count,
          ..RuntimeServerOptions::default()
        },
        true,
        session_factory,
      ),
    )
    .await
    .expect("恢复不得冻结（有界时限外仍无返回）")
    .expect("恢复必须成功（worker 向量臂须有会话在场）"),
  );
  let (server2, addr2) = start_server(Arc::clone(&provider2));
  let probes: Probes = {
    let mut stream = TcpStream::connect(addr2).await.expect("reconnect");
    let live_key = key_for_task(2);
    let removed_key = key_for_task(1);

    // VSIM：并行重放后的元素必须可检索（键判活随之自证）
    send_cmd(
      &mut stream,
      &[
        b"VSIM",
        &live_key,
        b"FP32",
        &fp32(&[2.0, 1.0]),
        b"COUNT",
        b"10",
      ],
    )
    .await
    .expect("vsim");
    let vsim = read_reply(&mut stream).await;
    assert!(
      lossy(&vsim).contains("e-live"),
      "恢复后向量元素必须可检索，实际应答: {:?}",
      lossy(&vsim)
    );

    // VREM 过的元素不得复活（删除臂重放对账）
    send_cmd(
      &mut stream,
      &[
        b"VSIM",
        &removed_key,
        b"FP32",
        &fp32(&[1.0, 1.0]),
        b"COUNT",
        b"10",
      ],
    )
    .await
    .expect("vsim removed");
    let vsim_removed = read_reply(&mut stream).await;
    assert!(
      !lossy(&vsim_removed).contains("e-removed"),
      "已 VREM 元素不得在恢复后复活，实际应答: {:?}",
      lossy(&vsim_removed)
    );

    // 重复 VADD 判 Duplicate：登记回建 + context 原位复用（登记表对账面）
    send_cmd(
      &mut stream,
      &[b"VADD", &live_key, b"FP32", &fp32(&[2.0, 1.0]), b"e-live"],
    )
    .await
    .expect("vadd dup");
    let dup = read_line_reply(&mut stream).await;
    assert_eq!(
      dup, b":0\r\n",
      "已存在元素 VADD 必须判 Duplicate（登记在场）"
    );
    send_cmd(&mut stream, &[b"QUIT"]).await.expect("quit");
    (vsim, vsim_removed, dup)
  };
  server2.stop();
  drop(server2);
  drop(provider2);
  Ok(probes)
}

/// 单任务恢复对照臂（count=1）：向量族三形态全量健康基线
#[test]
fn vector_family_single_task_recover_baseline() -> aok::Result<()> {
  Runtime::new()?.block_on(lifecycle(1))?;
  Ok(())
}

/// 并行恢复对账臂（count=4）：向量族条目确凿落于 worker 重放，恢复后
/// VSIM/删除臂/登记 Duplicate 与单任务基线逐项全等
#[test]
fn vector_family_parallel_recover_matches_single_task() -> aok::Result<()> {
  let baseline = Runtime::new()?.block_on(lifecycle(1))?;
  let parallel = Runtime::new()?.block_on(lifecycle(4))?;
  assert_eq!(baseline.0, parallel.0, "VSIM 检索应答必须逐字节一致");
  assert_eq!(baseline.1, parallel.1, "已删元素不可复活面必须一致");
  assert_eq!(baseline.2, parallel.2, "登记 Duplicate 判定必须一致");
  Ok(())
}
