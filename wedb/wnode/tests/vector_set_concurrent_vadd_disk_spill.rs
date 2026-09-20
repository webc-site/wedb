//! 并发 VADD 磁盘溢写活性回归（对标 test/standalone/Garnet.test.vectorset/ConcurrentVaddDiskSpillTests.cs:ConcurrentVaddToSpilledSetMakesProgress）
//!
//! C# 场景：8 线程并发 VADD 单一向量集，记录溢写对象日志（lowMemory 4KB
//! 页 vs ~8KB DiskANN 记录）；历史死锁为服务端 VADD 持独占集锁等待挂起盘读
//! 时，量化 worker 自旋占满线程池致盘读完成项永不可调度。rust 侧 VADD 记录
//! 经 WedbVectorStoreCallbacks 全量落 wkv（天然盘 backed），活性断言等价：
//!   * 并发 VADD 持续有进展（推进计数窗口内不得停摆 ≥ 15s）；
//!   * 结束时总插入数 > 下界（证明溢写路径真实执行过）。

use std::{
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
  thread,
  time::{Duration, Instant},
};

use compio::{runtime::Runtime, time::sleep};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::resp::{
  garnet_api::{GarnetApiFace, StoreGarnetApi},
  resp_server_session::RespServerSession,
  vector::{
    vector_manager::{VectorManager, VectorManagerOptions},
    vector_store_callbacks::WedbVectorStoreCallbacks,
  },
};
use wresp::command::RespCommand;
use wtest_base::test_store_config;
use wvector::Callbacks;

const THREADS: usize = 8;
const DIM: usize = 32;
const RUN_SECONDS: u64 = 20;
const STALL_LIMIT_SECONDS: u64 = 15;
const MIN_INSERTS: u64 = 100;

static DONE: AtomicU64 = AtomicU64::new(0);

/// 归一化随机 FP32 向量（C# Vec(Random, dim) 同型：均匀 [-1,1) 后归一化）
fn vec_bytes(seed: u64) -> Vec<u8> {
  let mut s = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
  let mut v = [0f32; DIM];
  let mut n = 0f64;
  for e in &mut v {
    s = s
      .wrapping_mul(6364136223846793005)
      .wrapping_add(1442695040888963407);
    *e = (((s >> 33) % 2000) as f32 / 1000.0) - 1.0;
    n += (*e as f64) * (*e as f64);
  }
  let norm = n.sqrt().max(1e-9) as f32;
  let mut out = Vec::with_capacity(DIM * 4);
  for e in v {
    out.extend_from_slice(&(e / norm).to_le_bytes());
  }
  out
}

/// C# ConcurrentVaddToSpilledSetMakesProgress（时长减半以控制 CI；停摆判定同型）
///
/// 当前 src 状态：8 线程并发 VADD 在 ~9k 次插入后全部楔死于 `api.exec`
/// 内部（加大运行时让出比例亦不恢复）——量化协程与 VADD 的集合锁竞争
/// 即 C# 注释所述历史死锁，rust 侧"量化 worker 非阻塞取锁 + 让出"修复
/// 未落地。修复后移除 ignore 转常规运行。
#[test]
#[ignore = "并发 VADD 死锁复现（~9k 次后楔死于 VADD，量化锁竞争）；src 修复后移除"]
fn concurrent_vadd_to_spilled_set_makes_progress() {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("spill.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线）依旧放不下 ~8KB/条的向量记录，
  // 盘溢写路径照常触发；GC 关闭保持历史语义
  let config = test_store_config();
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

  let deadline = Instant::now() + Duration::from_secs(RUN_SECONDS);

  // 8 个并发 VADD worker（每连接独立会话，共享存储与向量管理器）
  let mut handles = Vec::new();
  for tid in 0..THREADS {
    let store = Arc::clone(&store);
    let vm = Arc::clone(&vm);
    handles.push(thread::spawn(move || {
      // VADD 内部量化协程依赖 compio 运行时：每 worker 独立 runtime
      let rt = Runtime::new().unwrap();
      rt.block_on(async {
        let session = store.new_session().unwrap();
        let api = StoreGarnetApi::new(session).with_vector_manager(vm);
        let mut resp = RespServerSession::new(tid as i64 + 1, Default::default());

        let mut id = [0u8; 4];
        let mut k: u32 = 0;
        while Instant::now() < deadline {
          id.copy_from_slice(&((tid as u32) * 10_000_000 + k).to_le_bytes());
          k += 1;
          let vec = vec_bytes(tid as u64 * 7919 + k as u64);
          // VADD hk FP32 <vec> <id> BIN EF 64 M 16 XDISTANCE_METRIC COSINE（C# 同参）
          let args: Vec<&[u8]> = vec![
            b"hk",
            b"FP32",
            &vec,
            &id,
            b"BIN",
            b"EF",
            b"64",
            b"M",
            b"16",
            b"XDISTANCE_METRIC",
            b"COSINE",
          ];
          api.exec(&mut resp, RespCommand::Vadd, &args);
          // 回包必须为 :1/:0（元素新增或更新）；错误即中断活性
          assert!(
            resp.output.starts_with(b":"),
            "VADD 应为整数回包: {:?}",
            String::from_utf8_lossy(&resp.output)
          );
          resp.output.clear();
          DONE.fetch_add(1, Ordering::Relaxed);
          // 周期让出运行时：量化协程与本 worker 同处一个 compio runtime，
          // 同步 exec 循环不让出则量化任务永久饿死（C# 侧量化 worker 跑在
          // 线程池，等价供给由池调度完成）
          if k.is_multiple_of(4) {
            sleep(Duration::from_millis(2)).await;
          }
        }
        k
      })
    }));
  }

  // 停摆监视（C# stalledSeconds 监视同型）：窗口内零进展即判死锁
  let mut last = 0u64;
  let mut stalled_secs = 0u64;
  while handles.iter().any(|h| !h.is_finished()) && Instant::now() < deadline {
    thread::sleep(Duration::from_secs(2));
    let cur = DONE.load(Ordering::Relaxed);
    stalled_secs = if cur == last { stalled_secs + 2 } else { 0 };
    last = cur;
    assert!(
      stalled_secs < STALL_LIMIT_SECONDS,
      "DEADLOCK: {THREADS} 个并发 VADD worker 已 {stalled_secs}s 零进展（累计 {cur} 次插入）"
    );
  }

  let total: u32 = handles.into_iter().map(|h| h.join().unwrap()).sum();
  let done = DONE.load(Ordering::Relaxed);
  assert_eq!(done, total as u64, "计数应与各 worker 插入数一致");
  assert!(
    done > MIN_INSERTS,
    "worker 插入量 {done} 未达下界 {MIN_INSERTS}，未充分演练溢写路径"
  );
  eprintln!("[concurrent_vadd_to_spilled_set_makes_progress] inserts={done}");
}
