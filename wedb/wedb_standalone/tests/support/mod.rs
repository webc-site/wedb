mod env;

use compio::runtime::Runtime;
use env::test_env;
use wdev::SegmentedDevice;
use wnode::resp::resp_server_session::RespServerSession;

pub type Batch<'a> = wkv::BatchStoreSession<'a, SegmentedDevice>;

/// 批量写会话环境：统一 setup（[`test_env`]）后进入 batch，闭包内分派 RESP 命令
pub fn with_batch(f: impl FnOnce(&mut RespServerSession, &Batch)) {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_dir, session, resp) = test_env(false);
    let batch = session.enter_batch();
    let mut s = resp;
    f(&mut s, &batch);
  });
}
