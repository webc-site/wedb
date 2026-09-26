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
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::RespServerSession,
  vector::{
    vector_manager::{VectorManager, VectorManagerOptions},
    vector_store_callbacks::{OwnedActiveVectorSession, WedbVectorStoreCallbacks},
  },
};
use wnode_test::auto_exec;
use wresp::command::RespCommand;
use wtest_base::test_store_config;
use wvector::Callbacks;

const THREADS: usize = 8;
const DIM: usize = 32;
const RUN_SECONDS: u64 = 20;
const STALL_LIMIT_SECONDS: u64 = 15;
const MIN_INSERTS: u64 = 100;
/// 全用例有界时限（handshake_timeout.rs TEST_DEADLINE 先例）：RUN + 停摆窗 + 余量，
/// 停摆楔死形态下 worker 收割 join 永不返回——到点确定性失败而非死等 nextest 兜杀
const TEST_DEADLINE: Duration = Duration::from_secs(RUN_SECONDS + STALL_LIMIT_SECONDS + 5);

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
/// ignore 注记定因改述（2026-09 单点复跑，票 wnode-vadd-wedge-regression-ignored-no-ticket）：
///   * 原注记「量化 worker 非阻塞取锁 + 让出修复未落地致 ~9k 次后楔死」归因失真——
///     该形态已在 vector_manager_quantization.rs:run_quantization_task_loop 1:1 落地；
///     且本装配不拉量化协程（start_quantization_tasks 生产唯一拉起点 service.rs
///     get_session 惰性路径，本测试不经网络会话），所述楔死机制在装配内不可能发生。
///   * 复跑未现停摆（8 worker × 20s 全程有推进，零 15s 停摆）；实测失败形态为间歇
///     （5/8 轮）VADD 回 `-ERR Error indicating response from vector service`，因果链
///     实锤：graph_insert → 向量桥 rmw/read 冷读臂 → wkv session/raw/read.rs 盘读
///     复检预算尽上抛可重试 Index(LockTimeout)（其「交调用方重投收敛」契约）。调用方
///     重投档已实验并撤除：同桶持续写负载下重投不收敛（32 轮预算尽），且重投滞留
///     推高失败率（5/8→12/12）——根因属 wkv 冷读收敛面业务缺陷，需专票承接（桥层
///     外层重试非解，收敛机制须含链头单调收窄或等价让位序）。
///   * 本票转该业务缺陷的回归载体：装配已订正为 wnode_test::auto_exec 快慢两臂
///     收口形（旧同步 api.exec 裸驱形过时——向量命令全臂挂起化后落「执行域未挂载」
///     错误臂即秒败，从未到达生产锁竞争面）并加 TEST_DEADLINE 有界收割；业务票
///     修复落地后移除 #[ignore] 即转常规活性回归。
///   * 收口落地（票 wkv-cold-read-locktimeout-caller-no-convergence，2026-09-26）：
///     wkv read_from_disk 冷读复检重投已内聚 C# ContinuePendingRead 同形收窄——
///     每轮以已见最高链头为 minAddress 下界滤旧、下轮触发链头须严格越过上界，
///     重投窗单调收窄使复检预算内收敛；链头稳定即数轮落真实走尽/命中出口，不再
///     假性推满预算上抛 LockTimeout，VADD 冷读臂间歇 -ERR 根因消除；#[ignore]
///     按收口约定移除，本用例转常规活性回归。
#[test]
fn concurrent_vadd_to_spilled_set_makes_progress() {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("spill.db")).unwrap());
  // 小预算测试配置（对标 C# 16MB 基线）依旧放不下 ~8KB/条的向量记录，
  // 盘溢写路径照常触发；GC 关闭保持历史语义
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());

  // 回调无状态：向量会话按执行域绑定（测试为单任务同步段，专用会话持至用例结束）
  let _vector_domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
  let callbacks = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::<SegmentedDevice>::new()));
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    callbacks,
  ));

  let started = Instant::now();
  let deadline = started + Duration::from_secs(RUN_SECONDS);

  // 8 个并发 VADD worker（每连接独立会话，共享存储与向量管理器）
  let mut handles = Vec::new();
  for tid in 0..THREADS {
    let store = Arc::clone(&store);
    let vm = Arc::clone(&vm);
    handles.push(thread::spawn(move || {
      // VADD 慢路径（挂起闭环）依赖 compio 运行时收割：每 worker 独立 runtime
      let rt = Runtime::new().unwrap();
      rt.block_on(async {
        let session = store.new_session().unwrap();
        let api: GarnetApi = Arc::new(StoreGarnetApi::new(session).with_vector_manager(vm));
        let mut resp = RespServerSession::new(tid as i64 + 1, Default::default());
        // 慢路径调度须经会话挂载的 GarnetApi 停车（exec 侧 garnet_api 未挂载
        // 即写明错误拒命令），网络泵收割角色由本线程 auto_exec 承接
        resp.set_garnet_api(Arc::clone(&api));

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
          // 快慢两臂闭环（wnode_test::auto_exec 同仓收口形态）：同步段应答
          // 直取，挂起 SlowWait 经本线程 compio runtime 阻塞续跑
          let out = auto_exec(&api, &rt, &mut resp, RespCommand::Vadd, &args);
          // 回包必须为 :1/:0（元素新增或更新）；错误即中断活性
          assert!(
            out.starts_with(b":"),
            "VADD 应为整数回包: {:?}",
            String::from_utf8_lossy(&out)
          );
          DONE.fetch_add(1, Ordering::Relaxed);
          // 周期让步松弛共享 WedbStore 溢写面上的 worker 节奏（本装配无量化
          // 协程参与，历史 ignore 注记的「量化协程同 runtime 饿死」归因不成立，
          // 见用例文档注记）
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

  // 有界收割（TEST_DEADLINE，handshake_timeout.rs 先例）：停摆楔死形态下
  // join 永不返回——到点判死确定性失败，不依赖外置兜杀
  let case_deadline = started + TEST_DEADLINE;
  let mut total = 0u32;
  for h in handles {
    while !h.is_finished() {
      assert!(
        Instant::now() < case_deadline,
        "DEADLOCK: worker 收割超出全用例有界时限 {}s",
        TEST_DEADLINE.as_secs()
      );
      thread::sleep(Duration::from_millis(50));
    }
    total += h.join().unwrap();
  }
  let done = DONE.load(Ordering::Relaxed);
  assert_eq!(done, total as u64, "计数应与各 worker 插入数一致");
  assert!(
    done > MIN_INSERTS,
    "worker 插入量 {done} 未达下界 {MIN_INSERTS}，未充分演练溢写路径"
  );
}
