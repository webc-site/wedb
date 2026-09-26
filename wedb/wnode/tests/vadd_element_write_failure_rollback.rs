//! VADD 元素项写失败回滚回归（票：wvector-set-element-partial-term-write-residue-on-failure）
//!
//! 缺陷形态（provider 四步写 (d) 档）：`set_element` 的 IntMap 写失败时
//! Vector/ExtMap 已落盘，失败出口仅 `mark_free` 不清残留 ⇒
//! `random_members`（VRANDMEMBER 后端）对 `[0, max_internal_id]` 均匀采样
//! 不过滤 `is_free`，把残留 ExtMap 投影成"幽灵外部 id"直接回给客户端。
//! 修复契约：provider 失败出口按已写档位逆序单次回滚（对标 C#
//! `garnet/libs/server/Resp/Vector/DiskANNService.cs:Insert` 单次原生调用
//! 失败即 managed 无残留）。
//!
//! 注入面：生产回调 `WedbVectorStoreCallbacks`（不持会话；真 wkv 会话由测试体
//! 经 `OwnedActiveVectorSession` 绑到本执行域，见 `vector_store_callbacks`
//! 模块头「会话绑定的执行域私有化」）全包透传，仅按
//! `context & TERM_BITMASK` 对目标项类型的写返回 false
//! （`WedbVectorStoreCallbacks::write` IO 失败同形，形态迁移自
//! `vadd_attribute_write_failure.rs` 的属性档注入，两票射程正交）。

use std::{
  fs::remove_dir_all,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

use wbase::hash_slot::slot_of;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::resp::vector::{
  resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
  vector_manager::{VectorManager, VectorManagerOptions},
  vector_manager_index::Index,
  vector_store_callbacks::{OwnedActiveVectorSession, WedbVectorStoreCallbacks},
};
use wtest_base::test_store_config;
use wval::SessionPrefixBuf;
use wvector::{
  Callbacks, Context, DiskANNService, DiskAnnInsertResult, StoreCallbacks,
  store::{TERM_BITMASK, Term},
};

/// 默认会话库槽（测试键不参与定槽，统一取 (0,0) 库槽位）
const SLOT0: u16 = slot_of(0, 0);

/// 失败元素内部 id（起点 0、keeper 1 之后序贯新铸第三枚）。
const IID_GHOST: u32 = 2;
/// 解除注入哨兵。
const DISARMED: u64 = u64::MAX;

/// 项类型写落盘口故障：其余读写删/RMW 全透传生产回调（真 wkv 会话 + 真设备）。
struct ElementWriteFault {
  inner: WedbVectorStoreCallbacks<SegmentedDevice>,
  fail_term: AtomicU64,
}

impl ElementWriteFault {
  #[inline]
  fn write_failed(&self, context: u64) -> bool {
    let fail = self.fail_term.load(Ordering::Acquire);
    fail != DISARMED && context & TERM_BITMASK == fail
  }

  #[inline]
  fn arm(&self, kind: Term) {
    self.fail_term.store(kind as u64, Ordering::Release);
  }

  #[inline]
  fn disarm(&self) {
    self.fail_term.store(DISARMED, Ordering::Release);
  }
}

impl StoreCallbacks for ElementWriteFault {
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
    if self.write_failed(context) {
      return false;
    }
    self.inner.write(context, key, value).await
  }

  async fn delete(&self, context: u64, key: &[u8]) -> bool {
    self.inner.delete(context, key).await
  }

  async fn rmw<F>(&self, context: u64, key: &[u8], write_len: usize, f: F) -> bool
  where
    F: FnMut(&mut [u8]) + Send,
  {
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

/// FP32 2 维向量参数
fn fp32(vals: [f32; 2]) -> Vec<u8> {
  vals.iter().flat_map(|f| f.to_le_bytes()).collect()
}

/// RESP2 整数帧推导公式：`:` + 十进制 + CRLF
fn int_frame(v: i64) -> Vec<u8> {
  [b":", v.to_string().as_bytes(), b"\r\n"].concat().to_vec()
}

/// RESP2 bulk 帧推导公式：`$` + 长度 + CRLF + 载荷 + CRLF
fn bulk_frame(s: &[u8]) -> Vec<u8> {
  [format!("${}\r\n", s.len()).as_bytes(), s, b"\r\n"]
    .concat()
    .to_vec()
}

/// RESP2 数组帧推导公式：`*` + 条数 + CRLF + 逐项 bulk
fn array_frame(items: &[&[u8]]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", items.len()).into_bytes();
  for i in items {
    out.extend_from_slice(&bulk_frame(i));
  }
  out
}

/// 编码应答为 RESP2 帧字节
fn frame2(reply: &VectorReply) -> Vec<u8> {
  let mut out = Vec::new();
  reply.encode_resp2(&mut out);
  out
}

/// 取出应答中的 bulk 字节集合（Array of Bulk）。
fn bulk_items(reply: &VectorReply) -> Vec<Vec<u8>> {
  match reply {
    VectorReply::Array(items) => items
      .iter()
      .map(|i| match i {
        VectorReply::Bulk(Some(b)) => b.to_vec(),
        other => panic!("数组项应为 bulk，实际 {other:?}"),
      })
      .collect(),
    other => panic!("应答应为数组，实际 {other:?}"),
  }
}

fn arg_refs(v: &[Vec<u8>]) -> Vec<&[u8]> {
  v.iter().map(Vec::as_slice).collect()
}

/// VADD 命令参（无属性）
fn vadd_args(element: &[u8], vec: [f32; 2]) -> Vec<Vec<u8>> {
  vec![
    b"vk".to_vec(),
    b"FP32".to_vec(),
    fp32(vec),
    element.to_vec(),
    b"NOQUANT".to_vec(),
  ]
}

/// 装配：真盘 wkv + 项写故障回调 + 完整会话链路（目录路径交还调用方供重开）。
///
/// 回调不持会话（执行域私有化）：本函数只交还 `store`，真会话由测试体经
/// [`OwnedActiveVectorSession`] 绑定到当前执行域，透传臂据此取用。
fn harness() -> (
  tempfile::TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  Arc<ElementWriteFault>,
  RespServerSessionVectors<ElementWriteFault>,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("v.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let fault = Arc::new(ElementWriteFault {
    inner: WedbVectorStoreCallbacks::new(),
    fail_term: AtomicU64::new(DISARMED),
  });
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::clone(&fault)),
  ));
  (dir, store, fault, RespServerSessionVectors::new(vm))
}

/// (d) 档主靶协议级回归：IntMap 写失败 ⇒ 失败应答后 VRANDMEMBER 循环 20 次
/// 均不得命中幽灵 eid；VISMEMBER/VCARD 计数据实归位；恢复后重试 LIFO 复用同槽，
/// 采样面只见存活元素。
#[compio::test]
async fn int_map_write_failure_vrandmember_no_ghost_eid() {
  let (dir, store, fault, sess) = harness();
  // 本执行域绑定专用会话（回调透传臂取用面），持有至测试结束
  let _domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
  let root = SessionPrefixBuf::ROOT.as_slice();

  // keeper 正常插入（占起点 id 0 + 元素 id 1）
  let vadd_k1 = vadd_args(b"k1", [1.0, 0.0]);
  assert_eq!(
    frame2(
      &sess
        .network_vadd(root, &arg_refs(&vadd_k1), SLOT0, false)
        .await
    ),
    int_frame(1)
  );

  // 注入 IntMap 档写失败 → ghost 铸 id 2，四步写撕在末步
  fault.arm(Term::IntMap);
  let vadd_g1 = vadd_args(b"g1", [0.0, 1.0]);
  // provider StoreError::Write ⇒ diskann insert Err ⇒ service.rs:897 折叠
  // DiskAnnInsertResult::False ⇒ try_add Duplicate ⇒ `:0`
  assert_eq!(
    frame2(
      &sess
        .network_vadd(root, &arg_refs(&vadd_g1), SLOT0, false)
        .await
    ),
    int_frame(0)
  );
  fault.disarm();

  // 失败面回收：ghost 两面不可见、计数归位
  assert_eq!(
    frame2(&sess.network_vismember(root, &[b"vk", b"g1"], false).await),
    int_frame(0),
    "失败元素不得存活"
  );
  assert_eq!(
    frame2(&sess.network_vcard(root, &[b"vk"]).await),
    int_frame(1),
    "计数不得含失败元素"
  );

  // 协议级幽灵断言：VRANDMEMBER vk 2 恰回存活单元素 k1
  //（修复前：残留 ExtMap 被 random_members 投影，应答为 [k1, g1] 双元素）
  assert_eq!(
    frame2(&sess.network_vrandmember(root, &[b"vk", b"2"]).await),
    array_frame(&[b"k1"]),
    "VRANDMEMBER 不得回投幽灵 eid"
  );
  // 处方档位：循环 20 次均匀采样均不得命中 ghost
  for _ in 0..20 {
    let items = bulk_items(&sess.network_vrandmember(root, &[b"vk", b"3"]).await);
    assert!(
      items.iter().all(|i| i != b"g1"),
      "VRANDMEMBER 命中幽灵 eid: {items:?}"
    );
  }

  // 恢复重试：LIFO 复用失败槽位覆写，旧 eid 全面 miss
  let vadd_g2 = vadd_args(b"g2", [1.0, 1.0]);
  assert_eq!(
    frame2(
      &sess
        .network_vadd(root, &arg_refs(&vadd_g2), SLOT0, false)
        .await
    ),
    int_frame(1),
    "恢复后重试必须成功"
  );
  let mut items = bulk_items(&sess.network_vrandmember(root, &[b"vk", b"3"]).await);
  items.sort();
  assert_eq!(
    items,
    vec![b"g2".to_vec(), b"k1".to_vec()],
    "复用后采样面收敛存活集"
  );
  assert_eq!(
    frame2(&sess.network_vismember(root, &[b"vk", b"g1"], false).await),
    int_frame(0),
    "旧 eid 映射不得复活"
  );

  let _ = remove_dir_all(dir.path());
}

/// 跨重启回灌（真盘回调形态，参照 vector_set_interrupt_delete_recovery.rs:60
/// 的 WedbVectorStoreCallbacks + 真盘装配）：IntMap 档写失败后销毁会话与
/// provider、以新会话重建（生产重建入口 create_index → 内部 fsm `load_state`
/// 只从位图重建、不清 ExtMap），失败当场的逆序回滚必须已在存储上清道——
/// 残留不得复活；复用槽位覆写后两面读一致。
#[compio::test]
async fn int_map_write_failure_residue_absent_after_provider_rebuild() {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("v.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());

  // ---- 第一代：keeper 成功 + ghost IntMap 档失败
  let (ctx, cfg) = {
    // 本执行域绑定第一代会话（守卫先于其余句柄声明、故最后析构）
    let _domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
    let fault = Arc::new(ElementWriteFault {
      inner: WedbVectorStoreCallbacks::new(),
      fail_term: AtomicU64::new(DISARMED),
    });
    let vm = Arc::new(VectorManager::new(
      VectorManagerOptions {
        is_enabled: true,
        ..Default::default()
      },
      Callbacks::new(Arc::clone(&fault)),
    ));
    let sess = RespServerSessionVectors::new(vm.clone());
    let root = SessionPrefixBuf::ROOT.as_slice();

    let vadd_k1 = vadd_args(b"k1", [1.0, 0.0]);
    assert_eq!(
      frame2(
        &sess
          .network_vadd(root, &arg_refs(&vadd_k1), SLOT0, false)
          .await
      ),
      int_frame(1)
    );
    fault.arm(Term::IntMap);
    let vadd_g1 = vadd_args(b"g1", [0.0, 1.0]);
    assert_eq!(
      frame2(
        &sess
          .network_vadd(root, &arg_refs(&vadd_g1), SLOT0, false)
          .await
      ),
      int_frame(0)
    );
    let index_value = sess
      .manager
      .read_stored_index(root, b"vk")
      .expect("登记记录在位");
    let index = Index::from_bytes(&index_value).expect("登记记录解析");
    (index.context, index.index_config())
    // 此处执行域守卫（含其自持会话）/vm/fault/provider 全部离开作用域
    // = 重启后内存态销毁 + 本执行域解绑
  };

  // ---- 第二代：全新会话（重新绑定本执行域）+ 全新 provider（WedbProvider::new
  // → fsm load_state）
  let _domain2 = OwnedActiveVectorSession::new(store.new_session().unwrap());
  let cbs2 = Callbacks::new(Arc::new(WedbVectorStoreCallbacks::<SegmentedDevice>::new()));
  let service = DiskANNService::default();
  assert_eq!(
    service.create_index(ctx, cfg, cbs2.clone()).await,
    Ok(false),
    "重建应成功"
  );

  // 盘上残留判据（修复前：ExtMap[id 2] 仍存 Some(g1) 字节 ⇒ 回灌复活）：
  // 按 internal_id 直读 ExtMap 必须 miss
  let ghost_ext = cbs2
    .read_varsize_iid::<u8>(&Context::new(ctx).term(Term::ExtMap), IID_GHOST)
    .await;
  assert!(ghost_ext.is_none(), "重启后 ExtMap 残留不得回灌");
  assert!(
    !cbs2
      .read_single_iid(
        &Context::new(ctx).term(Term::Vector),
        IID_GHOST,
        &mut [0u8; 8]
      )
      .await,
    "重启后向量残留不得回灌"
  );
  // fsm 位图重建后计数据实归位（起点 + keeper）
  assert!(!service.check_internal_id_valid(ctx, IID_GHOST).await);
  assert_eq!(service.card(ctx), 1);
  assert_eq!(service.internal_id_of(ctx, b"g1").await, None);
  let mut sample = service.sample(ctx, 5).await;
  sample.sort();
  assert_eq!(sample, vec![b"k1".to_vec()], "重启后采样面不得出现幽灵 eid");

  // 重启后复用槽位覆写（load_state 后 next_id 新铸到同一 id 2）
  assert_eq!(
    service.insert(ctx, b"g2", &fp32([1.0, 1.0]), b"").await,
    DiskAnnInsertResult::True
  );
  assert_eq!(service.internal_id_of(ctx, b"g2").await, Some(IID_GHOST));
  assert!(
    !service.check_external_id_valid(ctx, b"g1").await,
    "旧 eid 不得复活"
  );
  let mut sample = service.sample(ctx, 5).await;
  sample.sort();
  assert_eq!(sample, vec![b"g2".to_vec(), b"k1".to_vec()]);

  let _ = remove_dir_all(dir.path());
}
