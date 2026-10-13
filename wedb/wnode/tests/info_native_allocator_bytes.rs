#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! INFO memory 段 native_allocator_bytes 嵌入链验收（工单
//! wmetric-native-allocator-bytes-unwired 嵌入链臂）
//!
//! 真实起库建主索引：`WedbStore::open` → `HashIndex::new` 桶数组分配经
//! `DirectVirtualMemory::allocate` 全程喂账 `windex::ram::NativeMemoryTracker`，
//! 会话 INFO memory 段经 `SessionInfoSource` 单点透传披露非零记账。下界断言
//! ＞0 即可——tracker 系进程级全局总账且记页对齐 reserve 后跨度，恒 ≥ 主索引
//! 桶数组净字节，禁与 store_index_size 等值断言。夹具形态对标本仓
//! resp_info_per_db.rs。

use std::str::from_utf8;

use compio::runtime::Runtime;
use wnode::RespSessionConsumer;
use wnode_test::{database_consumer_env, pump};

/// 装配带真存储执行域 + 常驻单库管理器的会话消费者
fn consumer() -> (Runtime, RespSessionConsumer) {
  database_consumer_env("allocbytes.db")
}

/// 同步单命令往返（INFO memory 为同步段，不走慢路径）
fn sync_roundtrip(c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, out) = pump(c, frame);
  assert_eq!(consumed, Some(0));
  out
}

/// 起库建主索引后 INFO memory 段 native_allocator_bytes 严格大于 0
///（下界断言；禁与 store_index_size 等值——恢复期换表瞬态另有新旧交叠）
#[test]
fn info_memory_discloses_nonzero_native_allocator_after_store_open() {
  let (_rt, mut c) = consumer();
  let info = sync_roundtrip(&mut c, b"*2\r\n$4\r\nINFO\r\n$6\r\nmemory\r\n");
  let text = from_utf8(&info).unwrap();
  assert!(text.contains("# Memory\r\n"), "应有 Memory 段头: {text}");

  let native: i64 = text
    .split("\r\n")
    .find_map(|l| l.strip_prefix("native_allocator_bytes:"))
    .unwrap_or_else(|| panic!("缺 native_allocator_bytes 行: {text}"))
    .parse()
    .unwrap_or_else(|_| panic!("native_allocator_bytes 非整数: {text}"));
  assert!(native > 0, "建库建主索引后原生记账必须在场: {text}");
}
