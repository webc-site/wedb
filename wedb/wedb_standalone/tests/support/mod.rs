use std::sync::Arc;

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::resp_server_session::RespServerSession;

pub type Batch<'a> = wkv::BatchStoreSession<'a, SegmentedDevice>;

pub fn with_batch(f: impl FnOnce(&mut RespServerSession, &Batch)) {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let dir = tempfile::tempdir().unwrap();
    let device = Arc::new(SegmentedDevice::single_file(dir.path().join("resp.db")).unwrap());
    let mut config = StoreConfig::new(16384, 65536, 64, 0.5).unwrap();
    config.gc.enabled = false;
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let session = store.new_session().unwrap();
    let batch = session.enter_batch();
    let mut s = RespServerSession::default();
    f(&mut s, &batch);
  });
}
