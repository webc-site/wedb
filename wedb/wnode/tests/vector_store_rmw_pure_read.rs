//! Vector 生产 RMW 纯读短路回归（`write_len == 0` ⇒ 不建不写）
//!
//! 对标 C# VectorSessionFunctions 谓词（同判据 `input.WriteDesiredSize != 0`）：
//!   * NeedInitialUpdate（:250）判假 → 缺失键不建空值幽灵记录（NOTFOUND）；
//!   * NeedCopyUpdate（:319）判假 → 已存在键不进 CopyUpdater、无冗余回写（SUCCESS）。
//!
//! 两种应答 IsCompletedSuccessfully 皆为真 ⇒ 短路返回恒 true 且不回调 `f`。
//! 断言口径与 wkv 既有测试一致：rmw 前后 tail_address 不变 ⇒ 零物理写。

use std::sync::Arc;

use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::vector::vector_store_callbacks::{
  ActiveVectorSessionGuard, WedbVectorStoreCallbacks,
};
use wvector::store::{StoreCallbacks, term};

#[compio::test]
async fn rmw_zero_write_len_never_writes() {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("vec.db")).unwrap());
  let config = StoreConfig::new(1024, 4096, 16, 0.5).unwrap();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();
  let callbacks = WedbVectorStoreCallbacks::<SegmentedDevice>::new();
  // 本执行域绑定会话（回调臂无状态经线程槽取会话；本测试为单任务同步段，
  // 全程无他任务交错，守卫持至体尾即栈纪律许可形态）
  let _domain = ActiveVectorSessionGuard::bind(&session);
  let ctx = term::METADATA;
  let key = b"rmw-pure-read";

  // 缺失键：纯读短路——不建空值幽灵记录，f 不被回调
  let tail = store.tail_address();
  let mut f_called = false;
  assert!(callbacks.rmw(ctx, key, 0, |_| f_called = true).await);
  assert!(!f_called, "判假路径不进 updater，f 不应被回调");
  assert_eq!(store.tail_address(), tail, "缺失键不得写入幽灵记录");
  assert!(!callbacks.read(ctx, key, |_| {}).await, "键应仍不存在");

  // 已存在键：纯读短路——无冗余回写，值原样保留
  assert!(callbacks.write(ctx, key, &[1u8; 8]).await);
  let tail = store.tail_address();
  assert!(callbacks.rmw(ctx, key, 0, |_| f_called = true).await);
  assert_eq!(store.tail_address(), tail, "已存在键不得冗余回写");
  let mut val = [0u8; 8];
  assert!(
    callbacks
      .read(ctx, key, |curr| val.copy_from_slice(curr))
      .await
  );
  assert_eq!(val, [1u8; 8], "纯读短路不得变更已存在值");

  // 对照组：write_len > 0 的缺失键走 InitialUpdater 追加新记录（证明断言口径有效）
  let tail = store.tail_address();
  assert!(
    callbacks
      .rmw(ctx, b"rmw-normal", 4, |data| data[0] = 7)
      .await
  );
  assert!(store.tail_address() > tail, "正常 RMW 应产生物理写");
  let mut val = [0u8; 4];
  assert!(
    callbacks
      .read(ctx, b"rmw-normal", |curr| val.copy_from_slice(curr))
      .await
  );
  assert_eq!(val, [7, 0, 0, 0], "RMW 缺失键应以零初始化值落盘");
}
