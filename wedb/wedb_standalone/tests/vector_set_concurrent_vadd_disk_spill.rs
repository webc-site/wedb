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

use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApiFace, StoreGarnetApi},
  resp_server_session::RespServerSession,
  vector::{
    vector_manager::{VectorManager, VectorManagerOptions},
    vector_store_callbacks::WedbVectorStoreCallbacks,
  },
};
use wresp::RespCommand;
use wvector::Callbacks;

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

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
#[test]
fn concurrent_vadd_to_spilled_set_makes_progress() {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("spill.db")).unwrap());
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

  let deadline = Instant::now() + Duration::from_secs(RUN_SECONDS);

  // 8 个并发 VADD worker（每连接独立会话，共享存储与向量管理器）
  let mut handles = Vec::new();
  for tid in 0..THREADS {
    let store = Arc::clone(&store);
    let vm = Arc::clone(&vm);
    handles.push(thread::spawn(move || {
      // 每线程独立 compio runtime：VADD 慢路径 spawn 须在 runtime 上下文内
      //（等价 C# 每连接独立会话跑在服务器执行域）
      let rt = compio::runtime::Runtime::new().unwrap();
      rt.block_on(async {
        // 每个 worker runtime 自带量化消费者协程（C# 量化 worker 跑共享线程池，
        // compio 单线程执行域必须本地拉起，否则量化通道无人排空）
        vm.start_quantization_tasks(1);
        let session = store.new_session().unwrap();
        let api = StoreGarnetApi::new(session).with_vector_manager(vm);
        let mut resp = RespServerSession::new(tid as i64 + 1, Default::default());

        let mut id = [0u8; 4];
        let mut k: u32 = 0;
        while Instant::now() < deadline {
          id.copy_from_slice(&((tid as u32) * 10_000_000 + k).to_le_bytes());
          k += 1;
          let vec = vec_bytes(tid as u64 * 7919 + k as u64);
          api.exec(
            &mut resp,
            RespCommand::Vadd,
            &[
              b"hk".as_slice(),
              b"FP32".as_slice(),
              vec.as_slice(),
              id.as_slice(),
              b"BIN".as_slice(),
              b"EF".as_slice(),
              b"64".as_slice(),
              b"M".as_slice(),
              b"16".as_slice(),
              b"XDISTANCE_METRIC".as_slice(),
              b"COSINE".as_slice(),
            ],
          );
          // 回包必须为 :1/:0（元素新增或更新）；错误即中断活性
          assert!(
            resp.output.starts_with(b":"),
            "VADD 应为整数回包: {:?}",
            String::from_utf8_lossy(&resp.output)
          );
          resp.output.clear();
          DONE.fetch_add(1, Ordering::Relaxed);
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
