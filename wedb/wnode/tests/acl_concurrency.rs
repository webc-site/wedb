#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! ACL 并发臂集成测试（对标 C# test/standalone/Garnet.test.acl/Resp/ACL/
//! ParallelTests.cs 四法）
//!
//! 四法对标（并发度按 CI 收敛，C# 128×2048 → 8×32；竞争面同构）：
//! 1. ParallelAuthTest：多连接并发 AUTH（正确口令恒 +OK、错误口令恒
//!    WRONGPASS），验证认证并发不污染会话态、结果一致；
//! 2. ParallelPasswordHashTest：并发口令哈希（`AclPassword::from_string`，
//!    C# ACLPassword.ACLPasswordFromString），跨线程结果须逐字节一致；
//! 3. ParallelAclSetUserTest：多连接并发交替 SETUSER on/+get 与 off/-get，
//!    每轮 GETUSER 复核组合态，off+get 非法组合（线程交错污染）即失败；
//! 4. ParallelAclSetUserAvoidsMapContentionTest：不预建用户，多连接并发
//!    SETUSER 同一用户竞争首插，全部 +OK、终态一致、无竞争崩溃。
//!
//! 并发先例沿 wnode/tests/rmw_key_concurrency.rs：std thread + 每线程独立
//! compio Runtime + 每线程独立会话消费者（共享存储实例与 ACL 列表）。
//! rust ACL 存储为唯一真源（KeyTag::Acl 记录点查，无 C# 全局用户大字典锁），
//! 「无 map 竞争崩溃」由并发点查/写穿面承接。mpsc 看门封死死锁挂起面。

use std::{
  panic::{AssertUnwindSafe, catch_unwind},
  sync::{Arc, mpsc},
  thread,
  time::Duration,
};

use compio::runtime::Runtime;
use wacl::{AccessControlList, AclPassword, GarnetAclAuthenticator};
use wnode::RespSessionConsumer;
use wnode_test::{consumer_on, drive_pending_parks_consumer, err_frame, feed};
use wresp::cmd_strings::RESP_WRONGPASS_INVALID_USERNAME_PASSWORD;
use wtest_base::open_test_store;

/// 并发连接数（C# degreeOfParallelism 收敛档）
const THREADS: usize = 8;

/// 每连接操作轮数（C# iterationsPerSession 收敛档）
const ITERS: usize = 32;

/// 口令哈希压力轮数（C# ParallelPasswordHashTest 2048 同值）
const HASH_ITERS: usize = 2048;

/// 看门时限：任一线程未在时限内完成即判死锁失败
const LOCK_DEADLINE: Duration = Duration::from_secs(120);

/// 主用账号与口令（C# TestUserA / DummyPassword / DummyPasswordB 对位）
const USER_A: &[u8] = b"parallel_user";
const GOOD_PW: &[u8] = b"good_password";
/// SETUSER 加口令规则单 token（C# `>{DummyPassword}` 同形，禁拆两参）
const GOOD_PW_TOKEN: &[u8] = b">good_password";
const BAD_PW: &[u8] = b"bad_password";
const DUMMY_PW: &str = "dummy_password";
const DUMMY_PW_B: &str = "dummy_password_b";

/// 命令入参组帧投喂 + 停车臂闭环（AUTH/ACL 族产应答型），返回全量应答
async fn park_call(c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut out = feed(c, args);
  drive_pending_parks_consumer(c, &mut out).await;
  out
}

/// 多线程并发驱动：每线程独立 compio Runtime + 独立会话消费者（共享存储与
/// ACL 列表），mpsc 看门封死死锁挂起面——任一线程超时未完成即断言失败；
/// body 以 catch_unwind 包裹：worker panic 即时断 send（通道断开令看门立刻
/// 旱涝收针），真实 panic 文案经 join 上抛，不被「疑似死锁」误诊掩盖
fn run_parallel<T: Send + 'static>(
  threads: usize,
  body: impl Fn(usize) -> T + Send + Sync + Clone + 'static,
) -> Vec<T> {
  let (tx, rx) = mpsc::channel::<()>();
  let handles: Vec<_> = (0..threads)
    .map(|t| {
      let tx = tx.clone();
      let body = body.clone();
      thread::spawn(move || {
        let out = catch_unwind(AssertUnwindSafe(|| body(t)));
        let _ = tx.send(());
        out
      })
    })
    .collect();
  drop(tx);
  for _ in 0..threads {
    rx.recv_timeout(LOCK_DEADLINE)
      .expect("并发 ACL 操作疑似死锁（看门超时）");
  }
  handles
    .into_iter()
    .map(|h| {
      h.join()
        .expect("并发线程自身崩账（join 错误）")
        .expect("并发线程不得 panic")
    })
    .collect()
}

/// 装配挂载 ACL 认证器的会话消费者（共享存储实例 + 共享 ACL 列表）；
/// 认证器逐连接新建（C# GarnetAclAuthenticator 会话档同构），ACL 列表单例共享
fn acl_consumer(
  store: &Arc<wkv::WedbStore<wdev::SegmentedDevice>>,
  acl: &Arc<AccessControlList>,
) -> RespSessionConsumer {
  let mut c = consumer_on(store);
  c.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(Arc::clone(acl)))));
  c
}

/// 法一（C# ParallelAuthTest）：多连接并发 AUTH，正确口令恒 +OK、错误口令恒
/// WRONGPASS——认证并发不污染会话态、结果逐轮一致
#[test]
fn parallel_auth_results_are_consistent() {
  let (_dir, store) = open_test_store("acl-concurrent-auth.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());

  // 预建测试用户（主线程同步段：SETUSER 落存储记录）
  Runtime::new().unwrap().block_on(async {
    let mut c = acl_consumer(&store, &acl);
    assert_eq!(
      park_call(&mut c, &[b"ACL", b"SETUSER", USER_A, b"on", GOOD_PW_TOKEN]).await,
      b"+OK\r\n",
      "预建并发认证用户须 +OK"
    );
  });

  run_parallel(THREADS, move |t| {
    let mut c = acl_consumer(&store, &acl);
    Runtime::new().unwrap().block_on(async {
      for i in 0..ITERS {
        let good = park_call(&mut c, &[b"AUTH", USER_A, GOOD_PW]).await;
        assert_eq!(good, b"+OK\r\n", "线程 {t} 第 {i} 轮正确口令 AUTH 须 +OK");
        let bad = park_call(&mut c, &[b"AUTH", USER_A, BAD_PW]).await;
        assert_eq!(
          bad,
          err_frame(RESP_WRONGPASS_INVALID_USERNAME_PASSWORD),
          "线程 {t} 第 {i} 轮错误口令 AUTH 须 WRONGPASS"
        );
      }
    });
  });
}

/// 法二（C# ParallelPasswordHashTest）：并发口令哈希，跨线程结果逐字节一致
#[test]
fn parallel_password_hash_is_deterministic() {
  let reference_a = AclPassword::from_string(DUMMY_PW);
  let reference_b = AclPassword::from_string(DUMMY_PW_B);
  let hashes = run_parallel(THREADS, move |_| {
    let mut local_a = Vec::with_capacity(HASH_ITERS);
    for _ in 0..HASH_ITERS {
      local_a.push(AclPassword::from_string(DUMMY_PW).password_hash);
      // 口令 B 每轮即算即校验（C# 双口令交替压力同构）
      assert_eq!(
        AclPassword::from_string(DUMMY_PW_B).password_hash,
        reference_b.password_hash,
        "并发口令 B 哈希漂移"
      );
    }
    local_a
  });
  assert_eq!(hashes.len(), THREADS);
  for (t, local) in hashes.iter().enumerate() {
    assert_eq!(local.len(), HASH_ITERS, "线程 {t} 哈希轮数不足");
    for (i, hash) in local.iter().enumerate() {
      assert_eq!(
        hash, &reference_a.password_hash,
        "线程 {t} 第 {i} 轮口令 A 哈希须与单线程基准逐字节一致"
      );
    }
  }
}

/// 法三（C# ParallelAclSetUserTest）：多连接并发交替 on/+get 与 off/-get，
/// 每轮 GETUSER 复核——off 与 +get 并存即线程交错污染（C# inactiveUserWithGet）
#[test]
fn parallel_acl_setuser_never_corrupts_user_rules() {
  let (_dir, store) = open_test_store("acl-concurrent-setuser.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());

  // 预置活跃用户（C# activeUserWithGetCommand 同参）
  Runtime::new().unwrap().block_on(async {
    let mut c = acl_consumer(&store, &acl);
    assert_eq!(
      park_call(
        &mut c,
        &[b"ACL", b"SETUSER", USER_A, b"on", GOOD_PW_TOKEN, b"+get"]
      )
      .await,
      b"+OK\r\n",
      "预置活跃用户须 +OK"
    );
  });

  run_parallel(THREADS, move |t| {
    let mut c = acl_consumer(&store, &acl);
    Runtime::new().unwrap().block_on(async {
      for i in 0..ITERS {
        // C# activeUserWithGetCommand / inactiveUserWithoutGetCommand 逐参同构
        let active = park_call(
          &mut c,
          &[b"ACL", b"SETUSER", USER_A, b"on", GOOD_PW_TOKEN, b"+get"],
        )
        .await;
        assert_eq!(active, b"+OK\r\n", "线程 {t} 第 {i} 轮 on/+get 须 +OK");

        let inactive = park_call(
          &mut c,
          &[b"ACL", b"SETUSER", USER_A, b"off", GOOD_PW_TOKEN, b"-get"],
        )
        .await;
        assert_eq!(inactive, b"+OK\r\n", "线程 {t} 第 {i} 轮 off/-get 须 +OK");

        // C# ACL LIST 脏态复核的 GETUSER 等价：off 与 +get 不得并存
        let frame = park_call(&mut c, &[b"ACL", b"GETUSER", USER_A]).await;
        let text = String::from_utf8_lossy(&frame);
        let off = text.contains("$3\r\noff\r\n");
        let get = text.contains("$4\r\n+get\r\n");
        assert!(
          !(off && get),
          "线程 {t} 第 {i} 轮出现非法组合态 off+get（SETUSER 竞争污染）: {text}"
        );
      }
    });
  });
}

/// 法四（C# ParallelAclSetUserAvoidsMapContentionTest）：不预建用户，多连接
/// 并发 SETUSER 同一用户竞争首插——全部 +OK、终态一致、无竞争崩溃
#[test]
fn parallel_acl_setuser_first_insert_race_survives() {
  let (_dir, store) = open_test_store("acl-concurrent-insert-race.db").unwrap();
  let acl = Arc::new(AccessControlList::new("").unwrap());

  // 竞争体与终态复核各持一份 Arc（并发体 move 捕获后主线程仍需复核）
  let race_store = Arc::clone(&store);
  let race_acl = Arc::clone(&acl);
  run_parallel(THREADS, move |t| {
    let mut c = acl_consumer(&race_store, &race_acl);
    Runtime::new().unwrap().block_on(async {
      for i in 0..ITERS {
        // C# setUserCommand 同参（用户不预建，竞争首插在首轮即触发）
        let resp = park_call(&mut c, &[b"ACL", b"SETUSER", USER_A, b"on", GOOD_PW_TOKEN]).await;
        assert_eq!(resp, b"+OK\r\n", "线程 {t} 第 {i} 轮竞争首插须 +OK");
      }
    });
  });

  // 终态一致：用户在册、启用、口令可过认证（主线程同步段）
  Runtime::new().unwrap().block_on(async {
    let mut c = acl_consumer(&store, &acl);
    let frame = park_call(&mut c, &[b"ACL", b"GETUSER", USER_A]).await;
    let text = String::from_utf8_lossy(&frame);
    assert!(
      text.contains("$2\r\non\r\n"),
      "竞争首插终态用户须为启用: {text}"
    );
    let auth = park_call(&mut c, &[b"AUTH", USER_A, GOOD_PW]).await;
    assert_eq!(auth, b"+OK\r\n", "竞争首插终态口令须可过认证");
  });
}
