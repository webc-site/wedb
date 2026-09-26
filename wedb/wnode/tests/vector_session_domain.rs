//! 向量会话执行域私有化集成测试（对标 C# `[ThreadStatic] ActiveThreadSession`）
//!
//! C# 侧 VectorManager 不持会话，回调经线程静态字段取「当前线程当前命令」的
//! 会话（libs/server/Resp/Vector/VectorManager.Callbacks.cs:254），后台清理
//! 自备一次性专用会话（libs/server/Resp/Vector/VectorManager.Cleanup.cs:149）。
//! rust 等价机制为 `vector_store_callbacks` 的线程槽 + RAII 守卫，本文件真实
//! 驱动其四条规范：
//!   1. 缺绑执行域拒绝一切存储回调落盘（Debug 显式 panic，Release 返回失败），
//!      拒绝即绝无副作用写；
//!   2. 守卫严格嵌套：绑定即归属、离开即还原（LIFO 栈纪律），槽位身份可判别；
//!   3. 跨线程隔离：属主线程的绑定不泄漏到他人执行域，他线程自备专用会话与
//!      属主会话互不串扰；
//!   4. 无状态回调与管理器天然 `Send + Sync`（守卫本体的 `!Send` 禁令由
//!      `vector_session_domain_ui.rs` 的 `assert_not_impl_any!` 编译期断言承接）。

use std::{
  panic::{AssertUnwindSafe, catch_unwind},
  path::Path,
  ptr::from_ref,
  sync::Arc,
  thread::scope,
};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, StoreResult, StoreSession, WedbStore};
use wnode::resp::vector::{
  vector_manager::VectorManager,
  vector_store_callbacks::{
    ActiveVectorSessionGuard, OwnedActiveVectorSession, WedbVectorStoreCallbacks,
    active_vector_session_bound, with_active_vector_session,
  },
};
use wvector::store::{StoreCallbacks, term};

type Cbs = WedbVectorStoreCallbacks<SegmentedDevice>;

/// 小预算测试存储（GC 关闭，语义同其余向量集成测试）
fn open_store(dir: &Path) -> Arc<WedbStore<SegmentedDevice>> {
  let device = Arc::new(SegmentedDevice::single_file(dir.join("domain.db")).unwrap());
  let config = StoreConfig::new(1024, 4096, 16, 0.5).unwrap();
  Arc::new(WedbStore::open(config, device).unwrap())
}

/// 本执行域绑定会话的指认（指针身份判别「回调看到的是谁的会话」）
fn bound_addr<D: wdev::Device>() -> Option<usize> {
  with_active_vector_session::<D, _>(|s: &StoreSession<D>| from_ref(s) as usize)
}

/// 缺绑回调写必须被拒（Debug：debug_assert 显式暴露；Release：返回失败）。
/// write async 化后必须真实驱动 future，缺绑断言臂才会触发
fn write_rejected(cbs: &Cbs, ctx: u64, key: &[u8], val: &[u8]) -> bool {
  #[cfg(debug_assertions)]
  {
    catch_unwind(AssertUnwindSafe(|| {
      Runtime::new().unwrap().block_on(cbs.write(ctx, key, val))
    }))
    .is_err()
  }
  #[cfg(not(debug_assertions))]
  {
    !Runtime::new().unwrap().block_on(cbs.write(ctx, key, val))
  }
}

/// 规范 1 + 2 的存储侧半：缺绑拒绝无副作用，绑定后同键可落盘可读回
#[test]
fn unbound_domain_rejects_write_and_bound_domain_lands() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    let store = open_store(dir.path());
    let session = store.new_session().unwrap();
    let callbacks = Cbs::new();
    let ctx = term::METADATA;
    let key = b"domain-bind-key";
    let val = b"domain-bind-val";

    assert!(!active_vector_session_bound(), "新执行域初始必未绑定");
    assert!(
      write_rejected(&callbacks, ctx, key, val),
      "缺绑执行域的回调写必须被拒"
    );

    let _guard = ActiveVectorSessionGuard::bind(&session);
    assert!(
      active_vector_session_bound(),
      "守卫存续期间本执行域必已绑定"
    );
    // 缺绑期的被拒写绝无落盘：绑定后物理读不命中
    let phys = session.vector_key(ctx, key);
    assert!(
      matches!(
        session.try_read_raw_in_memory(&phys, |_| {}),
        Ok(StoreResult::NotFound)
      ),
      "缺绑被拒的写不得留下任何副作用"
    );
    // 绑定后同口写入成功且可命中
    assert!(
      callbacks.write(ctx, key, val).await,
      "绑定执行域后回调写应成功"
    );
    let mut seen: Option<Vec<u8>> = None;
    match session.try_read_raw_in_memory(&phys, |v| seen = Some(v.to_vec())) {
      Ok(StoreResult::Success(_)) => {}
      other => panic!("写后物理直读应命中，实际 {other:?}"),
    }
    assert_eq!(seen.as_deref(), Some(val.as_slice()), "负载须逐字节还原");

    aok::OK
  })
  .unwrap();
}

/// 规范 2 的槽位侧半：嵌套守卫 LIFO 还原，回调恒见最内层属主会话
#[test]
fn guards_nest_lifo_and_restore_owner_slot() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    let store = open_store(dir.path());
    let session_a = store.new_session().unwrap();
    let session_b = store.new_session().unwrap();
    let addr_a = from_ref(&session_a) as usize;
    let addr_b = from_ref(&session_b) as usize;

    assert_eq!(bound_addr::<SegmentedDevice>(), None, "未绑定时槽位为空");
    let _ga = ActiveVectorSessionGuard::bind(&session_a);
    assert_eq!(
      bound_addr::<SegmentedDevice>(),
      Some(addr_a),
      "绑定后回调须见本会话"
    );
    {
      let _gb = ActiveVectorSessionGuard::bind(&session_b);
      assert_eq!(
        bound_addr::<SegmentedDevice>(),
        Some(addr_b),
        "嵌套段回调须见最内层会话"
      );
    }
    assert_eq!(
      bound_addr::<SegmentedDevice>(),
      Some(addr_a),
      "内层守卫离开须精确还原外层绑定"
    );
    drop(_ga);
    assert_eq!(
      bound_addr::<SegmentedDevice>(),
      None,
      "最外层守卫离开即完全解绑"
    );

    aok::OK
  })
  .unwrap();
}

/// 规范 3：属主线程绑定不泄漏他线程；他线程自持专用会话与属主互不串扰
#[test]
fn binding_is_private_to_owner_thread() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempdir().unwrap();
    let store = open_store(dir.path());
    let session = store.new_session().unwrap();
    let callbacks = Cbs::new();
    let ctx = term::METADATA;

    let _owner_guard = ActiveVectorSessionGuard::bind(&session);
    let addr_owner = from_ref(&session) as usize;

    // 回调无状态可跨线程共享引用（Send + Sync），但线程槽不随引用迁移
    let cbs_ref = &callbacks;
    let store2 = Arc::clone(&store);
    scope(|s| {
      let handle = s.spawn(move || {
        let rt = Runtime::new().unwrap();
        rt.block_on(async {
          assert!(
            !active_vector_session_bound(),
            "属主线程的绑定绝不泄漏到新建执行域"
          );
          // 他线程缺绑：回调写同样被拒（绝不错用属主会话）
          assert!(
            write_rejected(cbs_ref, ctx, b"cross-thread-key", b"x"),
            "他线程缺绑的回调写必须被拒"
          );
          // 他线程自持专用会话：绑定的是自己槽位里的会话，与属主无涉
          let own = store2.new_session().unwrap();
          let _owned = OwnedActiveVectorSession::new(own);
          // 槽位指向堆上稳定副本，身份以 `session()` 取回比对
          let addr_own = from_ref(_owned.session()) as usize;
          assert_eq!(
            bound_addr::<SegmentedDevice>(),
            Some(addr_own),
            "专用会话须绑定为本执行域当前会话"
          );
          assert_ne!(addr_own, addr_owner, "两线程会话必为两份实体");
        });
      });
      handle.join().expect("子线程执行域规范断言");
    });

    // 子线程全程未扰动属主槽位
    assert_eq!(
      bound_addr::<SegmentedDevice>(),
      Some(addr_owner),
      "子线程活动后属主槽位仍归本域会话"
    );
    // 缺绑期被拒的跨线程写无副作用
    let phys = session.vector_key(ctx, b"cross-thread-key");
    assert!(
      matches!(
        session.try_read_raw_in_memory(&phys, |_| {}),
        Ok(StoreResult::NotFound)
      ),
      "他线程被拒的写不得落入属主域"
    );

    aok::OK
  })
  .unwrap();
}

/// 规范 4：私有化后回调与 VectorManager 天然 `Send + Sync`（旧持
/// `Arc<StoreSession>` 形态即本断言的反面）
#[test]
fn privatized_callbacks_are_send_sync() {
  fn assert_send_sync<T: Send + Sync>() {}
  assert_send_sync::<Cbs>();
  assert_send_sync::<Arc<Cbs>>();
  assert_send_sync::<VectorManager>();
}
