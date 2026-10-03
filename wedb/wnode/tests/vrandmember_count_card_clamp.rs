//! VRANDMEMBER count 无界预分配进程中止面回归（票：wvector-vrandmember-count-unbounded-alloc-abort）
//!
//! 钳制契约：`network_vrandmember` 以 `service.card`（直读标量零成本）钳制
//! count——正数取 min(count, card)，负数归零回空数组；钳制位收敛会话层单点，
//! `service.sample` 不设第二道门。
//!
//! C# 锚：`VectorStoreOps.cs:VectorSetRandomMembers` 为零分配 TODO 桩
//! （`// TODO: Implement!` + `idResults.Length = 0`，count 从未消费），
//! 接口注释钉 "It is OK to fetch fewer than the requested number of elements"
//! （IGarnetApi.cs:2154，少取合法）——card 内全量返回即契约上界。不钳则
//! sample 按 count 两笔 vec 预分配（12B/ID，`VRANDMEMBER key 2147483647`
//! ≈ 25.7GB + 8.6GB），分配失败默认 abort 整进程（拒绝服务面）；同族
//! VSIM COUNT 有 MAX_RETRIEVE_COUNT 门，本命令以 card 承担对称防御。

use std::{fs::remove_dir_all, sync::Arc};

use wbase::hash_slot::slot_of;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::resp::vector::{
  resp_server_session_vectors::{RespServerSessionVectors, VectorReply},
  vector_manager::{VectorManager, VectorManagerOptions},
  vector_store_callbacks::{OwnedActiveVectorSession, WedbVectorStoreCallbacks},
};
use wtest_base::test_store_config;
use wval::SessionPrefixBuf;
use wvector::Callbacks;

/// 默认会话库槽（测试键不参与定槽，统一取 (0,0) 库槽位）
const SLOT0: u16 = slot_of(0, 0);

/// 装配：真盘 wkv + 生产回调 + 直调会话（无故障注入，每测试独立临时目录）。
fn harness() -> (
  tempfile::TempDir,
  Arc<WedbStore<SegmentedDevice>>,
  RespServerSessionVectors<WedbVectorStoreCallbacks<SegmentedDevice>>,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("v.db")).unwrap());
  let store = Arc::new(WedbStore::open(test_store_config(), device).unwrap());
  let vm = Arc::new(VectorManager::new(
    VectorManagerOptions {
      is_enabled: true,
      ..Default::default()
    },
    Callbacks::new(Arc::new(WedbVectorStoreCallbacks::<SegmentedDevice>::new())),
  ));
  (dir, store, RespServerSessionVectors::new(vm))
}

/// RESP2 整数帧推导公式：`:` + 十进制 + CRLF
fn int_frame(v: i64) -> Vec<u8> {
  [b":", v.to_string().as_bytes(), b"\r\n"].concat().to_vec()
}

/// RESP2 数组帧推导公式：`*` + 条数 + CRLF + 逐项 bulk
fn array_frame(items: &[&[u8]]) -> Vec<u8> {
  let mut out = format!("*{}\r\n", items.len()).into_bytes();
  for i in items {
    out.extend_from_slice(format!("${}\r\n", i.len()).as_bytes());
    out.extend_from_slice(i);
    out.extend_from_slice(b"\r\n");
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
    vec.iter().flat_map(|f| f.to_le_bytes()).collect(),
    element.to_vec(),
    b"NOQUANT".to_vec(),
  ]
}

/// 主靶：i32::MAX count 常规应答不中止——钳到 card=2 恰回全集两元素。
#[compio::test]
async fn i32_max_count_clamped_to_card_no_abort() {
  let (dir, store, sess) = harness();
  let _domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
  let root = SessionPrefixBuf::ROOT.as_slice();

  for (i, eid) in [&b"k1"[..], &b"k2"[..]].into_iter().enumerate() {
    let args = vadd_args(eid, [i as f32, 1.0]);
    assert_eq!(
      frame2(
        &sess
          .network_vadd(root, &arg_refs(&args), SLOT0, false)
          .await
      ),
      int_frame(1),
      "VADD {eid:?} 应成功"
    );
  }

  let mut items = bulk_items(
    &sess
      .network_vrandmember(root, &[b"vk", b"2147483647"])
      .await,
  );
  items.sort();
  assert_eq!(
    items,
    vec![b"k1".to_vec(), b"k2".to_vec()],
    "i32::MAX count 应钳制到 card=2 恰回全集（少取合法契约上界）"
  );

  let _ = remove_dir_all(dir.path());
}

/// 单元素集超界 count 恰回单元素（min(count, card)=1）。
#[compio::test]
async fn oversized_count_single_element_set_returns_single() {
  let (dir, store, sess) = harness();
  let _domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
  let root = SessionPrefixBuf::ROOT.as_slice();

  let args = vadd_args(b"only", [1.0, 2.0]);
  assert_eq!(
    frame2(
      &sess
        .network_vadd(root, &arg_refs(&args), SLOT0, false)
        .await
    ),
    int_frame(1)
  );

  let items = bulk_items(
    &sess
      .network_vrandmember(root, &[b"vk", b"2147483646"])
      .await,
  );
  assert_eq!(
    items,
    vec![b"only".to_vec()],
    "超界 count 应钳到 card=1 恰回单元素"
  );

  let _ = remove_dir_all(dir.path());
}

/// 负 count 归零回空数组（现状维持，含 i32::MIN 下界）。
#[compio::test]
async fn negative_count_returns_empty_array() {
  let (dir, store, sess) = harness();
  let _domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
  let root = SessionPrefixBuf::ROOT.as_slice();

  let args = vadd_args(b"k1", [1.0, 2.0]);
  assert_eq!(
    frame2(
      &sess
        .network_vadd(root, &arg_refs(&args), SLOT0, false)
        .await
    ),
    int_frame(1)
  );

  for n in ["-1", "-5", "-2147483648"] {
    assert_eq!(
      frame2(&sess.network_vrandmember(root, &[b"vk", n.as_bytes()]).await),
      array_frame(&[]),
      "负 count {n} 应维持回空数组现状"
    );
  }

  let _ = remove_dir_all(dir.path());
}

/// 缺键大 count 回空数组（NOTFOUND 臂不受钳制影响，防倒退）。
#[compio::test]
async fn missing_key_large_count_returns_empty_array() {
  let (dir, store, sess) = harness();
  let _domain = OwnedActiveVectorSession::new(store.new_session().unwrap());
  let root = SessionPrefixBuf::ROOT.as_slice();

  assert_eq!(
    frame2(
      &sess
        .network_vrandmember(root, &[b"ghost", b"2147483647"])
        .await
    ),
    array_frame(&[]),
    "缺键 + 大 count 应回空数组"
  );

  let _ = remove_dir_all(dir.path());
}
