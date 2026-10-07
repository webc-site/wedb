#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 对象 RMW 落笔前复验探针 IO 失败单帧收口回归（票
//! wnode-rmw-recheck-err-exit-partial-reply-no-reset）
//!
//! 缺陷形：run_async_rmw 把 operate 负载直写会话 output 尾段后，落笔前终态
//! 复验 [`obj_save_recheck_async`] 的探针 Err 臂以 `map_err(log)?` 直返，未回退
//! 挂载点——output 尾已有 HINCRBY 等写命令的成功负载帧（`:N`）时，慢臂漏斗
//! slow_arm 随后追加存储错误帧，单命令两帧：伪成功整数先于错误帧抵达，客户端
//! 可能已按成功入账且后续流水线应答整体串位。同函数三兄弟臂（unchanged=false
//! 弃写臂 / apply_rmw_post_operate 失败臂 / 物化臂）均先 obj_out.reset() 再落
//! 错，唯探针臂漏复位。
//! 对标 C# 锚：garnet/libs/server/Resp/Objects/HashCommands.cs 等 Network* 方法
//! 在 storageSession.RMW 完成返回后才一次性出应答；RMWMethods.cs 记录锁内不出
//! 应答字节，结构上不存在「半截负载帧 + 错误帧」拼接形态。
//! 修法：探针 Err 臂补 obj_out.reset() 单点复位（零新机制）。
//!
//! 故障注入口径（真设备层读 IO 失败，禁假 mock，形制同
//! vector_error_swallow_regression.rs 的 FailingDevice）：测试配置 ReadCache 与
//! copy_reads_to_tail 均默认关，冷读不回填驻留，单条冷键 HINCRBY 内「装载
//! （第 1 次盘读）→ operate 写帧 → 复验探针异步读通（第 2 次盘读）」两次盘读
//! 真实可分——计数放行设备放行首读、注错次读，定向命中「output 尾已有负载 +
//! 探针失败」交叠面。
//!
//! 判据：
//! 1. 注入轮应答恰为单条 `-ERR slow path storage error`，无前导 `:15` 残帧；
//! 2. 失败轮写回未落（复验弃写先于 apply），解除注入后 HINCRBY 从旧值 10 起算
//!    得 `:15`（重放双施即 `:20` 转红），闭环反空跑。

use std::sync::{
  Arc,
  atomic::{AtomicU64, Ordering},
};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::pool::{AlignedBuf, BufferPool};
use wdev::{Device, Error, Result as DevResult, SegmentedDevice};
use wkv::WedbStore;
use wnode_test::{consumer_on, err_frame, roundtrip};
use wresp::cmd_strings::RESP_ERR_SLOW_PATH_STORAGE;
use wtest_base::test_store_config;

type TestStore = WedbStore<FailAfterDevice>;

/// 慢臂存储错误帧期望字节（单点常量派生，同 obj_rmw_aof_enqueue_fail 形制）
fn slow_storage_frame() -> Vec<u8> {
  err_frame(RESP_ERR_SLOW_PATH_STORAGE)
}

/// 计数放行读故障注入设备（包装真实单文件设备）：前 `fail_after` 次读 IO 放行，
/// 其后一律报短读错（`fail_after` 置 u64::MAX 即解除）。整臂无差别失败的
/// FailingDevice 形不能满足「装载读通放行、复验探针读失败」的命令内时序，
/// 故取计数对位
struct FailAfterDevice {
  inner: Arc<SegmentedDevice>,
  reads: AtomicU64,
  fail_after: AtomicU64,
}

impl FailAfterDevice {
  fn read_blocked(buf: &AlignedBuf) -> DevResult<usize> {
    Err(Error::UnexpectedEof {
      expected: buf.len(),
      actual: 0,
    })
  }
}

impl Device for FailAfterDevice {
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }

  fn segment_size(&self) -> u64 {
    self.inner.segment_size()
  }

  fn pool(&self) -> &Arc<BufferPool> {
    self.inner.pool()
  }

  async fn write_aligned(&self, offset: u64, buf: AlignedBuf) -> (DevResult<usize>, AlignedBuf) {
    self.inner.write_aligned(offset, buf).await
  }

  async fn read_aligned(&self, offset: u64, buf: AlignedBuf) -> (DevResult<usize>, AlignedBuf) {
    let seq = self.reads.fetch_add(1, Ordering::AcqRel);
    if seq >= self.fail_after.load(Ordering::Acquire) {
      return (Self::read_blocked(&buf), buf);
    }
    self.inner.read_aligned(offset, buf).await
  }

  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (DevResult<usize>, AlignedBuf) {
    let seq = self.reads.fetch_add(1, Ordering::AcqRel);
    if seq >= self.fail_after.load(Ordering::Acquire) {
      return (Self::read_blocked(&buf), buf);
    }
    self.inner.read_raw(offset, buf).await
  }

  async fn sync(&self) -> DevResult<()> {
    self.inner.sync().await
  }

  async fn truncate_until_segment(&self, segment_id: u32) -> DevResult<()> {
    self.inner.truncate_until_segment(segment_id).await
  }
}

/// 冷键 HINCRBY 复验探针读 IO 失败：应答回退挂载点后恰单条存储错误帧，
/// 禁 `:15` 伪成功残帧与错误帧拼帧；失败轮零持久化施加，解除注入后续算闭环
#[test]
fn cold_rmw_recheck_probe_io_err_single_error_frame() {
  let dir = tempdir().unwrap();
  let device = Arc::new(FailAfterDevice {
    inner: Arc::new(SegmentedDevice::single_file(dir.path().join("rmwrechk.db")).unwrap()),
    reads: AtomicU64::new(0),
    fail_after: AtomicU64::new(u64::MAX),
  });
  let store: Arc<TestStore> =
    Arc::new(WedbStore::open(test_store_config(), device.clone()).unwrap());
  let rt = Runtime::new().unwrap();

  // 暖态种子后整库冷化：信封落盘、内存索引指示磁盘候选（无 TTL / 无 String
  // 历史 / 未升阶，装载与复验的盘读均只有信封域一处）
  let mut seed = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut seed, &[b"HSET", b"h:c", b"n", b"10"]),
    b":1\r\n"
  );
  drop(seed);
  rt.block_on(store.flush_and_evict_all())
    .expect("冷化刷盘不得报错");

  // 定向注入：第 1 次盘读（装载）放行，第 2 次盘读（复验探针异步读通）失败
  device.fail_after.store(1, Ordering::Release);

  let mut c = consumer_on(&store);
  let reply = roundtrip(&rt, &mut c, &[b"HINCRBY", b"h:c", b"n", b"5"]);
  assert_eq!(
    reply,
    slow_storage_frame(),
    "复验探针 IO 失败必须先回退 output 挂载点再落错：单命令恰一条错误帧，\
     禁 :15 伪成功残帧 + 错误帧拼帧破协议流"
  );
  drop(c);

  // 失败轮复验弃写先于 apply（零持久化施加）：解除注入后 HINCRBY 从旧值 10
  // 起算得 :15——若缺陷轮已施加或借重放双施即 :20 转红
  device.fail_after.store(u64::MAX, Ordering::Release);
  let mut w = consumer_on(&store);
  assert_eq!(
    roundtrip(&rt, &mut w, &[b"HINCRBY", b"h:c", b"n", b"5"]),
    b":15\r\n",
    "解除注入后续算自旧值闭环（失败轮零施加、无双施）"
  );
  assert_eq!(
    roundtrip(&rt, &mut w, &[b"HGET", b"h:c", b"n"]),
    b"$2\r\n15\r\n"
  );
}
