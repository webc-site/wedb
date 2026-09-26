//! 扫描推流热路径堆分配次数回归测试：以计数分配器断言每记录堆分配次数降至 1

use std::{
  alloc::{GlobalAlloc, Layout, System},
  sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
  },
};

use waof::{WalConfig, WalLog};
use wdev::SegmentedDevice;

static ALLOC_COUNT: AtomicUsize = AtomicUsize::new(0);

struct CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
  unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
    ALLOC_COUNT.fetch_add(1, Ordering::SeqCst);
    unsafe { System.alloc(layout) }
  }

  unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
    unsafe { System.dealloc(ptr, layout) }
  }
}

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

#[compio::test]
async fn scan_next_frame_allocates_exactly_once_per_record() {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(
    SegmentedDevice::single_file(dir.path().join("alloc_test.wal")).expect("create wal device"),
  );
  let wal = Arc::new(WalLog::new(device, WalConfig::default()).expect("create wal"));

  const RECORD_COUNT: usize = 10;
  for i in 0..RECORD_COUNT {
    wal
      .enqueue(format!("payload-data-entry-{i}").as_bytes())
      .unwrap();
  }
  let tail = wal.safe_tail_address();

  // 预热并构造迭代器
  let mut iter = wal.scan(0, tail);

  let mut allocs_per_record = Vec::with_capacity(RECORD_COUNT);
  let mut frames = Vec::with_capacity(RECORD_COUNT);
  loop {
    let b = ALLOC_COUNT.load(Ordering::SeqCst);
    let opt = iter.next_frame().await.expect("next_frame ok");
    let a = ALLOC_COUNT.load(Ordering::SeqCst);
    if let Some(frame) = opt {
      allocs_per_record.push(a - b);
      frames.push(frame);
    } else {
      break;
    }
  }
  assert_eq!(frames.len(), RECORD_COUNT);
  for (idx, allocs) in allocs_per_record.iter().enumerate() {
    assert_eq!(*allocs, 1, "第 {idx} 条记录分配次数必须严格为 1");
  }

  // 校验每帧数据完整性与头部正确性
  for (i, frame) in frames.iter().enumerate() {
    let expected_payload = format!("payload-data-entry-{i}").into_bytes();
    assert_eq!(frame.payload(), &expected_payload[..]);
    assert_eq!(frame.header().payload_len(), expected_payload.len());
  }
}
