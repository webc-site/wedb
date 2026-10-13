#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! fsm next_id 飞行计数取消窗泄漏回归（票：wvector-fsm-next-id-inflight-count-cancel-leak-quantization-hang）
//!
//! 缺陷：next_id 登记臂 fetch_add 后横跨 reuse_or_mint 全 await 方达守卫
//! 构造，future 在该窗被取消丢弃（KILL/注销）时注销臂与守卫均不存在，
//! `pre_switch_inflight` 永久 +1——enable_quantization 排空环只 load 自旋、
//! 计数无归零路径：量化建表永挂、enable_reuse 不可达、fsm 块无界膨胀。
//!
//! 注入面：一次性 rmw 闸（仅命中 `_fsm` 块键——仓内唯一 8 字节宽 id 键，
//! 武装后首个命中必为 next_id 登记窗内的铸造 rmw）+ 无 waker 忙驱。首个
//! 插入正常落位（起点 id 0、元素 id 1）后张闸，下一条插入恰在铸造 rmw
//! 挂起点被整体丢弃——精确复现取消窗，全程单线程确定性，无竞态。
//!
//! 契约（修复后由计数守卫 Drop 构造性保证，旧码此测试必挂）：
//! 1. 取消丢弃后计数归零——建表排空环在定额 poll 预算内定点完成（泄漏即
//!    排空环永挂，预算耗尔回归失败而非测试挂死）；
//! 2. 回填收尾翻 `_qnt` 完成标志——enable_reuse 可达；
//! 3. remove 释放的 id 被新插入复用——复用通道重新打开；
//! 4. 被取消插入零残留——id 映射不存在。

use std::{
  future::{Future, pending},
  pin::{Pin, pin},
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
  },
  task::{Context as TaskContext, Poll, Waker},
};

use wvector::{
  Callbacks, DiskANNService, DiskAnnInsertResult, StoreCallbacks, VectorQuantType, store::Term,
};
use wvector_test::{MemStore, f32_bytes, test_config};

const CTX: u64 = 8;

/// 训练样本门槛（Spherical1Bit::required_vectors 恒 1000）。
const TRAIN_ROWS: usize = 1000;

/// 排空环 poll 预算：正确路径排空即刻通过（首检即零）；泄漏时排空环自旋
/// 永挂，预算耗尽以断言失败收场，杜绝测试整体挂死。
const DRAIN_BUDGET: usize = 10_000_000;

/// 量化状态键（`_qnt`，与 dynamic_quant 的 QUANT_STATE_KEY 同值）。
const QUANT_STATE_KEY: u32 = u32::from_be_bytes(*b"_qnt");

/// fsm 块键前缀（`_fsm`，与 fsm.rs 的 FSM_KEY_PREFIX 同值）。
const FSM_KEY_PREFIX: u32 = u32::from_be_bytes(*b"_fsm");

/// 一次性 rmw 闸内存桥：武装后首个 fsm 块键 rmw（next_id 登记窗内的铸造
/// 置位）即永久挂起，由驱动环丢弃承载 future 完成取消注入；其余调用直通
/// 内存桥。
struct GateStore {
  inner: MemStore,
  /// 一次性武装位（置位后首个 fsm rmw 命中即清）。
  arm: AtomicBool,
  /// 挂起标志（驱动环观察到即丢弃承载 future）。
  parked: AtomicBool,
}

impl GateStore {
  /// 是否为 fsm 块键（8 字节宽 id 键、低 4 字节为 `_fsm` 前缀小端）。
  fn is_fsm_block_key(key: &[u8]) -> bool {
    key.len() == 8 && key[..4] == FSM_KEY_PREFIX.to_le_bytes()
  }
}

impl StoreCallbacks for GateStore {
  async fn read_multi<F>(&self, context: u64, keys: &[u8], length_hint: usize, f: F) -> bool
  where
    F: FnMut(u32, &[u8]) + Send,
  {
    self.inner.read_multi(context, keys, length_hint, f).await
  }

  async fn read<F>(&self, context: u64, key: &[u8], f: F) -> bool
  where
    F: FnMut(&[u8]) + Send,
  {
    self.inner.read(context, key, f).await
  }

  async fn write(&self, context: u64, key: &[u8], value: &[u8]) -> bool {
    self.inner.write(context, key, value).await
  }

  async fn delete(&self, context: u64, key: &[u8]) -> bool {
    self.inner.delete(context, key).await
  }

  async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
    if Self::is_fsm_block_key(key) && self.arm.swap(false, Ordering::AcqRel) {
      self.parked.store(true, Ordering::Release);
      // 闸门永不开：承载 future 在 next_id 登记窗内的挂起点被丢弃（取消注入）
      pending::<()>().await;
    }
    self.inner.rmw(context, key, write_len, f).await
  }

  async fn filter(&self, context: u64, internal_id: u32) -> bool {
    self.inner.filter(context, internal_id).await
  }

  async fn purge_context(&self, context: u64) -> bool {
    self.inner.purge_context(context).await
  }

  fn log(&self, context: u64, msg: &str) {
    self.inner.log(context, msg);
  }
}

/// 无 waker 忙驱：把 future 推进到闸门挂起点，随即由调用方丢弃（取消注入）。
fn drive_to_park<F: Future>(mut fut: Pin<&mut F>, parked: &AtomicBool) {
  let waker = Waker::noop();
  let mut cx = TaskContext::from_waker(waker);
  while !parked.load(Ordering::Acquire) {
    assert!(
      matches!(fut.as_mut().poll(&mut cx), Poll::Pending),
      "取消窗注入落空：插入在闸门命中前已完成"
    );
  }
}

/// 定额预算驱动建表至定点完成：泄漏时排空环自旋永挂，预算耗尽回归失败。
fn drive_build_within_budget(mut build: Pin<&mut impl Future<Output = ()>>) {
  let waker = Waker::noop();
  let mut cx = TaskContext::from_waker(waker);
  for _ in 0..DRAIN_BUDGET {
    if matches!(build.as_mut().poll(&mut cx), Poll::Ready(())) {
      return;
    }
  }
  panic!("enable_quantization 排空环预算耗尽仍未定点：飞行计数取消窗泄漏未闭环");
}

/// 取消丢弃后飞行计数守卫兜底闭环：排空定点、回填收尾、复用重开。
#[compio::test]
async fn cancel_drop_releases_inflight_count() {
  let store = Arc::new(GateStore {
    inner: MemStore::new(),
    arm: AtomicBool::new(false),
    parked: AtomicBool::new(false),
  });
  let service = Arc::new(DiskANNService::default());
  assert_eq!(
    service
      .create_index(
        CTX,
        test_config(VectorQuantType::Bin),
        Callbacks::new(Arc::clone(&store))
      )
      .await,
    Ok(false)
  );

  // 首插正常落位：起点 id 0、元素 id 1（闸未武装，铸造链无挂起）
  assert!(
    matches!(
      service
        .insert(CTX, b"e1", &f32_bytes(&[0.0, 0.0]), b"")
        .await,
      DiskAnnInsertResult::True
    ),
    "首插应直接成功"
  );

  // 张闸：下一条插入在 next_id 登记窗内的铸造 rmw 挂起，随即丢弃 future
  //（KILL/注销取消语义）——旧码此处 pre_switch_inflight 永久 +1
  store.arm.store(true, Ordering::Release);
  {
    let aborted = pin!(async {
      service
        .insert(CTX, b"e_abort", &f32_bytes(&[1.0, 1.0]), b"")
        .await
    });
    drive_to_park(aborted, &store.parked);
    // 出作用域即丢弃：取消窗主体，守卫 Drop 应兜底注销计数
  }

  // 契约四：取消零残留，id 映射不存在
  assert_eq!(
    service.internal_id_of(CTX, b"e_abort").await.unwrap(),
    None,
    "被取消插入不应留下 id 映射"
  );

  // 补足训练样本（元素 id 2 已被取消消费且位图未置位，铸造顺延 id 3 起）
  for i in 3..=(TRAIN_ROWS as u32 + 2) {
    let id = format!("e{i}").into_bytes();
    let x = (i % 32) as f32 * 0.5;
    let y = (i / 32) as f32 * 0.5;
    let res = service.insert(CTX, &id, &f32_bytes(&[x, y]), b"").await;
    assert!(
      matches!(
        res,
        DiskAnnInsertResult::True | DiskAnnInsertResult::QuantizationRequested
      ),
      "e{i} 插入失败: {res:?}"
    );
  }

  // 契约一：计数归零——建表排空环预算内定点完成（泄漏必自旋永挂）
  drive_build_within_budget(pin!(async {
    while !service.build_quantization_table(CTX).await {
      // 单线程无争用，重试臂仅为建表语义对齐保留（取样瞬态兜底）
    }
  }));

  // 契约二：回填收尾翻 `_qnt` 完成标志——enable_reuse 可达
  service.backfill_quantized_vectors(CTX, 0, 1).await;
  let state = store
    .inner
    .peek(CTX, Term::Metadata, &QUANT_STATE_KEY.to_le_bytes())
    .expect("_qnt 状态记录缺失");
  assert_eq!(
    state[0], 1,
    "回填收尾应置全量化完成标志（enable_reuse 可达）"
  );

  // 契约三：remove 释放的 id 被新插入复用——复用通道重新打开
  let victim = service
    .internal_id_of(CTX, b"e1")
    .await
    .unwrap()
    .expect("e1 映射缺失");
  assert!(service.remove(CTX, b"e1").await.unwrap(), "remove 应命中");
  assert!(
    matches!(
      service
        .insert(CTX, b"e_new", &f32_bytes(&[2.0, 2.0]), b"")
        .await,
      DiskAnnInsertResult::True | DiskAnnInsertResult::QuantizationRequested
    ),
    "复用通道验证插入失败"
  );
  assert_eq!(
    service.internal_id_of(CTX, b"e_new").await.unwrap(),
    Some(victim),
    "新插入应复用被释放的内部 id（enable_reuse 后复用通道重开）"
  );
}
