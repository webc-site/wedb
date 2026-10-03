//! AOF 装配期尺寸体检与物理尺寸投影单点集成测试
//! （对应 libs/server/Servers/GarnetServerOptions.cs:GetAofSettings）

use wconf::{
  RuntimeServerOptions,
  size::{MIN_PAGE_SIZE_BYTES, pretty_size},
};
use wnode::aof::aof_settings::{AofSettings, MEMORY_OPT, PAGE_OPT, SEGMENT_OPT};

/// 校验一：常驻窗口容不下两页即拒启，文案点名 aof-memory 与 aof-page-size
#[test]
fn rejects_memory_not_twice_page() {
  let options = RuntimeServerOptions {
    aof_memory_size: Some("32m".into()),
    aof_page_size: Some("32m".into()),
    ..RuntimeServerOptions::default()
  };
  let msg = AofSettings::from_options(&options)
    .expect_err("窗口与页等值（不足两倍）必须拒启")
    .to_string();
  assert!(
    msg.contains(MEMORY_OPT) && msg.contains(PAGE_OPT),
    "文案须点名两侧配置项: {msg}"
  );
}

/// 校验二：页大于段即拒启，文案点名 aof-page-size 与 aof-segment-size
#[test]
fn rejects_page_over_segment() {
  let options = RuntimeServerOptions {
    aof_memory_size: Some("256m".into()),
    aof_page_size: Some("128m".into()),
    aof_segment_size: Some("32m".into()),
    ..RuntimeServerOptions::default()
  };
  let msg = AofSettings::from_options(&options)
    .expect_err("页大于段必须拒启")
    .to_string();
  assert!(
    msg.contains(PAGE_OPT) && msg.contains(SEGMENT_OPT),
    "文案须点名两侧配置项: {msg}"
  );
}

/// 校验三：页小于实配主存页两倍即拒启，文案点名 aof-page-size、给出按
/// 实配值折算的可下调界（吃 `hlog_page_size` 实配投影，非缺省 16m 常量）
#[test]
fn rejects_page_below_main_page_floor() {
  const MAIN_PAGE: usize = 4 * 1024 * 1024;
  let options = RuntimeServerOptions {
    aof_memory_size: Some("64m".into()),
    aof_page_size: Some("4m".into()),
    hlog_page_size: MAIN_PAGE,
    ..RuntimeServerOptions::default()
  };
  let msg = AofSettings::from_options(&options)
    .expect_err("页低于实配主存页两倍下界必须拒启")
    .to_string();
  assert!(msg.contains(PAGE_OPT), "文案须点名 aof-page-size: {msg}");
  assert!(
    msg.contains(&pretty_size((MAIN_PAGE * 2) as i64)),
    "文案须按实配主存页给出可调下界: {msg}"
  );
}

/// 页尺寸下限由 wconf 校验核在场：256 字节的 aof-page-size 在旋钮读取处即拒，
/// 文案点名本配置项与 MIN_PAGE_SIZE_BYTES（证伪「小页静默接受」）
#[test]
fn rejects_page_below_min_page_size_floor() {
  let options = RuntimeServerOptions {
    aof_page_size: Some("256".into()),
    ..RuntimeServerOptions::default()
  };
  let msg = AofSettings::from_options(&options)
    .expect_err("低于页容量下限的页尺寸必须拒启")
    .to_string();
  assert!(
    msg.contains(PAGE_OPT) && msg.contains(&MIN_PAGE_SIZE_BYTES.to_string()) && msg.contains("256"),
    "文案须点名 {PAGE_OPT} 与页容量下限字节: {msg}"
  );
}

/// 正常参数解析：合法尺寸三元组投影
#[test]
fn accepts_valid_sizes() {
  let options = RuntimeServerOptions {
    aof_memory_size: Some("128m".into()),
    aof_page_size: Some("32m".into()),
    aof_segment_size: Some("1g".into()),
    ..RuntimeServerOptions::default()
  };
  let settings = AofSettings::from_options(&options).expect("合法参数必须通过");
  assert_eq!(settings.memory_size_bytes, 128 * 1024 * 1024);
  assert_eq!(settings.page_size_bytes, 32 * 1024 * 1024);
  assert_eq!(settings.segment_size_bytes, 1024 * 1024 * 1024);
}
