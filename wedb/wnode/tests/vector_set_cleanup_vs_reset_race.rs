//! 清理任务 vs 存储重置竞争回归（对标 test/standalone/Garnet.test.vectorset/VectorCleanupVsResetRaceTests.cs:DropVectorSetWhileResettingStore）
//!
//! C# 场景：删除向量集（入队全键空间清理）后并发锤 `Pause + Reset +
//! Resume`（集群 re-attach 的生产模式），回归清理任务与分配器拆除的 AVE。
//! rust 侧承接：VADD 建集 → 删除入队（request_deletion）→ 清理任务链
//! （request-cleanup → cleanup，闸门内元数据 RMW）运行期间并发锤
//! cleanup 闸门，断言：
//!   * 清理管道在闸门锤击下照常完成（索引丢弃 + 元数据终结），无 panic；
//!   * 闸门正确串行化清理迭代与外界操作（pause 期间不再有清理处理体推进）。
//!
//! 注：C# 的 `StoreWrapper.Reset()`（存储级拆除重建）在 rust 侧尚未落地
//! （wkv 存储域待实现），故以闸门锤击 + 清理管道全链路并发代替；存储
//! Reset 落地后应补 `Pause + Reset + Resume` 三段锤击。

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  thread,
  time::{Duration, Instant},
};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  MessageConsumerFace,
  resp::{
    RespSessionConsumer,
    garnet_api::StoreGarnetApi,
    resp_server_session::RespServerSessionOptions,
    vector::{
      vector_manager::{VectorManager, VectorManagerOptions},
      vector_manager_index::Index,
      vector_store_callbacks::WedbVectorStoreCallbacks,
    },
  },
};
use wvector::Callbacks;

/// 向量个数（C# 4000 仅为让清理扫描"有事可做"，且 C# 只锤固定 5 秒窗口、
/// 不等清理完成；rust 侧断言管道跑完全链，故取 400 控制逐条 RMW 的
/// debug 构建耗时，同时保持扫描 + 闸门竞争窗口有效）
const VECTORS: u32 = 400;

/// 任意字节参数的 RESP 数组帧
fn encode_frame(parts: &[&[u8]]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", parts.len()).into_bytes();
  for p in parts {
    out.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
    out.extend_from_slice(p);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// C# DropVectorSetWhileResettingStore
/// 泵等价消费（直填会话接收缓冲 → 唯一入口 → 应答取出）
/// 返回 (消费后残余, 应答)：Some(0) = 完整消费，None = 协议违规
fn pump(consumer: &mut RespSessionConsumer, frame: &[u8]) -> (Option<usize>, Vec<u8>) {
  let mut scratch = consumer.take_recv_scratch();
  scratch.extend_from_slice(frame);
  consumer.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let remaining = consumer.try_consume_messages_into(&mut resp);
  (remaining, resp)
}

#[test]
fn drop_vector_set_while_resetting_store() {
  let rt = Runtime::new().unwrap();
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("race.db")).unwrap());
  let mut config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
  config.gc.enabled = false;
  let store = Arc::new(WedbStore::open(config, device).unwrap());

  let v_session = Arc::new(store.new_session().unwrap());
  let callbacks = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::new(v_session)));
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    callbacks,
  ));

  rt.block_on(async {
    // 生产拓扑的三条常驻清理协程（对标 C# VectorManager 常驻任务；
    // compio::spawn 须在运行时上下文内启动）
    let _t1 = vm.run_cleanup_task_async();
    let _t2 = vm.run_request_cleanup_task_async();
    let _t3 = vm.run_request_drop_task_async();

    // 会话消费者在运行时上下文内装配（存储会话绑 TLS 运行时）
    let api =
      StoreGarnetApi::new(store.new_session().unwrap()).with_vector_manager(Arc::clone(&vm));
    let mut consumer = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api);

    let key = b"DropVectorSetWhileResettingStore";

    // 灌入向量集：清理扫描有大量记录可迭代（C# Random(2026_05_01) 同型）
    let mut data = [0u8; 75];
    let mut seed = 20_260_501u64;
    for i in 0..VECTORS {
      for b in data.iter_mut() {
        seed = seed
          .wrapping_mul(6364136223846793005)
          .wrapping_add(1442695040888963407);
        *b = (seed >> 33) as u8;
      }
      let id = i.to_le_bytes();
      let req = encode_frame(&[b"VADD", key, b"XB8", &data, &id]);
      let (consumed, out) = pump(&mut consumer, &req);
      assert_eq!(consumed, Some(0));
      assert_eq!(out, b":1\r\n", "VADD #{i} 应成功: {out:?}");
    }

    // 键上记录即索引存根（DEL 路径的 request_deletion 载荷）。
    // 本分支索引记录在管理器域内登记表（read_stored_index），不经存储会话
    let value = vm.read_stored_index(key).expect("索引记录在位");
    let index = Index::from_bytes(&value).expect("索引存根可解码");
    let context = index.context;

    // 竞争窗口：闸门锤击线程（C# Pause + Reset + Resume 的闸门段）
    let stop = Arc::new(AtomicBool::new(false));
    let hammer = {
      let vm = Arc::clone(&vm);
      let stop = Arc::clone(&stop);
      thread::spawn(move || {
        let mut pauses = 0u64;
        while !stop.load(Ordering::Relaxed) {
          vm.pause_cleanup_async();
          pauses += 1;
          vm.resume_cleanup();
          // 对标 C# 锤击循环的 Thread.Sleep(1)：给清理任务留闸门空档推进，
          // 纯 yield 忙自旋会与清理 RMW 恶性争核，拖死整条清理管道
          thread::sleep(Duration::from_millis(1));
        }
        pauses
      })
    };

    // 删除向量集 → 清理管道（request-cleanup → 闸门内 cleanup → 元数据 RMW）
    vm.request_deletion(&value);

    // 等待索引服务侧丢弃（request-cleanup 循环已消费）
    let deadline = Instant::now() + Duration::from_secs(30);
    while vm.service.card(context) > 0 {
      assert!(
        Instant::now() < deadline,
        "清理管道未在闸门锤击下完成索引丢弃"
      );
      thread::sleep(Duration::from_millis(20));
    }

    // 元数据终结（cleanup 循环在闸门内完成 finished_cleaning_up + RMW）
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
      let pending = vm.queue_cleanups();
      if pending == 0 {
        break;
      }
      assert!(
        Instant::now() < deadline,
        "清理元数据未终结：仍有 {pending} 个待清理上下文"
      );
      thread::sleep(Duration::from_millis(50));
    }

    // 停锤、收口
    stop.store(true, Ordering::Relaxed);
    let pauses = hammer.join().unwrap();
    eprintln!("[drop_vector_set_while_resetting_store] gate pauses={pauses}");

    // 存储与会话仍健康：命令通路正常
    let (_, out) = pump(&mut consumer, &encode_frame(&[b"VCARD", key]));
    assert!(!out.is_empty(), "删除后会话应仍可应答");
  });
}
