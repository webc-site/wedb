#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! ETag 递增环绕契约回归（C# unchecked 递增对位）：客户端 etag 可为 i64::MAX，
//! SETIFMATCH 初写 / SETWITHETAG 递增以 wrapping_add 环绕承接——debug 构建
//! integer overflow panic 即客户端单命令可达面（SKILL：禁止 panic，除非绝对
//! 不会触发）。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::{TempDir, tempdir};
use wkv::StoreConfig;
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::RespServerSession,
};
use wnode_test::{pump_session_park, session_on};
use wtest_base::resp_frame_str;

fn env(tag: &str) -> (TempDir, RespServerSession) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 16 * 1024, 4, 0.5).unwrap();
  let store = wnode_test::store_open(&dir, tag, config);
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap())).into();
  let s = session_on(&api);
  (dir, s)
}

#[test]
fn etag_increment_wraps_at_i64_max_instead_of_panicking() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    // SETIFMATCH 键缺失初写臂：next_etag(i64::MAX) 须环绕为 i64::MIN 落库应答
    let (_dir, mut s) = env("etag-wrap-ifmatch");
    let reply = pump_session_park(
      &mut s,
      &resp_frame_str(&["SETIFMATCH", "k", "v", "9223372036854775807"]),
    )
    .await
    .0;
    assert!(
      reply.windows(21).any(|w| w == b":-9223372036854775808"),
      "初写臂应答环绕 etag i64::MIN（etag+值数组帧）: {reply:?}"
    );

    // SETWITHETAG 命中已有 i64::MAX etag 键再递增：existing+1 须环绕不 panic
    //（SETIFGREATER 直取 given 为新 etag，锚定 existing=i64::MAX）
    let (_dir2, mut s2) = env("etag-wrap-setwithetag");
    // SETIFGREATER 仅为锚定 existing=i64::MAX 的副作用，应答不判（裸 `.0`
    // 语句触发 clippy::unnecessary_operation，显式 let _ 丢弃）
    let _ = pump_session_park(
      &mut s2,
      &resp_frame_str(&["SETIFGREATER", "k", "v", "9223372036854775807"]),
    )
    .await
    .0;
    let reply = pump_session_park(&mut s2, &resp_frame_str(&["SETWITHETAG", "k", "v2"]))
      .await
      .0;
    assert!(
      reply.windows(21).any(|w| w == b":-9223372036854775808"),
      "命中递增应答环绕 etag i64::MIN: {reply:?}"
    );
  });
}
