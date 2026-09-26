use std::{
  panic::{AssertUnwindSafe, catch_unwind},
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
  time::Duration,
};

use compio::runtime::Runtime;
use wtest_base::{
  DEFAULT_WAIT_STEP, DEFAULT_WAIT_TIMEOUT, IntoDuration, parse_duration_token,
  try_parse_duration_token, wait_for, wait_for_step, wait_until,
};

#[test]
fn duration_parsing_units() {
  assert_eq!(DEFAULT_WAIT_TIMEOUT, Duration::from_secs(5));
  assert_eq!(DEFAULT_WAIT_STEP, Duration::from_millis(10));

  assert_eq!(parse_duration_token("5s"), Duration::from_secs(5));
  assert_eq!(parse_duration_token(" 10 sec "), Duration::from_secs(10));
  assert_eq!(parse_duration_token("500ms"), Duration::from_millis(500));
  assert_eq!(
    parse_duration_token("200 micros"),
    Duration::from_micros(200)
  );
  assert_eq!(parse_duration_token("50µs"), Duration::from_micros(50));
  assert_eq!(parse_duration_token("100ns"), Duration::from_nanos(100));
  assert_eq!(parse_duration_token("2m"), Duration::from_secs(120));
  assert_eq!(parse_duration_token("1min"), Duration::from_secs(60));

  // try_parse_duration_token 安全解析与非法用例
  assert_eq!(try_parse_duration_token("5s"), Some(Duration::from_secs(5)));
  assert_eq!(try_parse_duration_token("invalid"), None);
  assert_eq!(try_parse_duration_token(""), None);
  assert_eq!(try_parse_duration_token("500unknown"), None);

  let d = Duration::from_secs(3);
  assert_eq!(d.into_duration(), Duration::from_secs(3));
  assert_eq!("250ms".into_duration(), Duration::from_millis(250));
}

#[test]
fn wait_for_step_immediate_and_delayed() {
  let rt = Runtime::new().expect("创建 runtime");
  rt.block_on(async {
    // 立即成功
    assert!(wait_for(|| true, Duration::from_secs(1)).await);
    assert!(wait_for_step(|| true, Duration::from_secs(1), Duration::from_millis(1)).await);

    // 延迟成功
    let counter = Arc::new(AtomicUsize::new(0));
    let c = Arc::clone(&counter);
    let ok = wait_for_step(
      move || {
        let prev = c.fetch_add(1, Ordering::Relaxed);
        prev >= 3
      },
      Duration::from_secs(1),
      Duration::from_millis(5),
    )
    .await;
    assert!(ok);
    assert!(counter.load(Ordering::Relaxed) >= 4);

    // 超时返回 false
    let failed = wait_for_step(
      || false,
      Duration::from_millis(30),
      Duration::from_millis(10),
    )
    .await;
    assert!(!failed);
  });
}

#[test]
fn wait_until_macro_syntax_permutations() {
  let rt = Runtime::new().expect("创建 runtime");
  rt.block_on(async {
    let dur_timeout = Duration::from_secs(2);
    let dur_step = Duration::from_millis(5);

    // 1. 基本语法与全参数
    wait_until!(1 + 1 == 2, timeout: 5s, step: 5ms, "失败说明: {}", "加法失败");
    wait_until!(1 + 1 == 2, timeout: 5s, step: 5ms);

    // 2. 变量语法
    wait_until!(true, timeout: dur_timeout, step: dur_step, "变量时长失败");
    wait_until!(true, timeout: dur_timeout, step: dur_step);

    // 3. 顺序互换 (step 前 timeout 后)
    wait_until!(true, step: 5ms, timeout: 1s, "步长在前");
    wait_until!(true, step: 5ms, timeout: 1s);
    wait_until!(true, step: dur_step, timeout: dur_timeout);

    // 4. 仅 timeout
    wait_until!(true, timeout: 500ms, "仅超时参数");
    wait_until!(true, timeout: 500ms);
    wait_until!(true, timeout: dur_timeout);

    // 5. 仅 step
    wait_until!(true, step: 2ms, "仅步长参数");
    wait_until!(true, step: 2ms);
    wait_until!(true, step: dur_step);

    // 6. 仅条件或条件加说明（默认 5s 超时 / 10ms 步长）
    wait_until!(true, "仅说明消息");
    wait_until!(true);

    // 7. 闭包式条件推进
    let step_count = Arc::new(AtomicUsize::new(0));
    let sc = Arc::clone(&step_count);
    wait_until!(
      {
        let val = sc.fetch_add(1, Ordering::Relaxed);
        val >= 2
      },
      timeout: 1s,
      step: 5ms,
      "步进累计未能达标"
    );
  });
}

#[test]
fn wait_until_timeout_panics() {
  let rt = Runtime::new().expect("创建 runtime");
  let res = catch_unwind(AssertUnwindSafe(|| {
    rt.block_on(async {
      wait_until!(false, timeout: 20ms, step: 5ms, "预期超时失败");
    });
  }));
  assert!(res.is_err(), "超时必须触发 panic 断言");
}
