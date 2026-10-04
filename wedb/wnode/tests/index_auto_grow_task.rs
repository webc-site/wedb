//! 索引周期自动扩容任务端到端回归
//!
//! 对标 C# garnet/test/standalone/Garnet.test.extensions/IndexGrowthTests.cs:
//! IndexGrowthTest（libs/server/StoreWrapper.cs:IndexAutoGrowTaskAsync，
//! libs/server/TaskManager/TaskType.cs:IndexAutoGrowTask 执行体；配置面
//! index-max-size + index-resize-frequency + index-resize-threshold，
//! 默认关闭）语义：小索引 + 大量键 → 开启周期任务 → 索引自动翻倍至
//! 配置上限并停扩（allIndexesMaxedOut 收敛退出），数据全程完整。

use std::{
  sync::{Arc, atomic::AtomicBool},
  time::Duration,
};

use compio::{runtime::Runtime, time::sleep};
use tempfile::tempdir;
use wconf::{RuntimeServerConfig, RuntimeServerOptions};
use wnode::{
  resp::config_commands::{ConfigSetHost, ServerConfig},
  service::{StorageSessionProvider, spawn_index_auto_grow_task},
};
use wtest_base::test_store_config;

/// 初始小索引桶数（64 桶 × 64B = 4KB，写入后大量溢出桶）
const INITIAL_INDEX_SIZE: usize = 64;
/// 自动扩容上限桶数（初始的 2 倍：恰好容纳一次自动翻倍）
const MAX_INDEX_SIZE: usize = 128;
/// 触发阈值百分比（对标 C# 默认 50）
const THRESHOLD: i64 = 50;
/// 周期任务检查间隔秒（注入最小周期，加快触发）
const FREQUENCY_SECS: u64 = 1;
/// 写入键数（64 桶 × 7 槽 = 448 容量，1000 键稳超产生大量溢出桶）
const WRITE_COUNT: usize = 1000;
/// 触发轮询超时（秒）
const WAIT_SECS: u64 = 15;

fn key(i: usize) -> String {
  format!("grow:key:{i}")
}

fn value(i: usize) -> Vec<u8> {
  format!("grow:value:{i}").into_bytes()
}

/// 小索引 + 大量键 → 周期任务自动翻倍至上限 → 数据完整
#[test]
fn index_auto_grow_task_doubles_index_and_keeps_data_intact() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("index_grow.db");

  // 注入小索引（test_store_config 基线上收窄 index_size；64 = 2 的幂合法）
  let mut config = test_store_config();
  config.index_size = INITIAL_INDEX_SIZE;

  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      config,
      &data_path,
      None,
      RuntimeServerOptions::default(),
      wnode_test::session_factory,
    )
    .expect("open"),
  );

  rt.block_on(async {
    // 触发前索引为初始容量
    assert_eq!(
      provider.store().active_index().size,
      INITIAL_INDEX_SIZE,
      "初始索引须为 {INITIAL_INDEX_SIZE} 桶"
    );
    assert_eq!(
      provider
        .store()
        .active_index()
        .overflow_pool
        .allocated_count(),
      0,
      "初始索引无溢出桶"
    );

    // 拉起周期执行体（生产路径经 with_index_auto_grow + 首会话惰性装配，
    // 此处直接驱动同一 spawn 入口验证执行体语义）
    spawn_index_auto_grow_task(
      Arc::clone(&provider.database_manager),
      MAX_INDEX_SIZE,
      THRESHOLD,
      FREQUENCY_SECS,
      Arc::new(AtomicBool::new(false)),
    );

    // 大量键写入制造溢出桶（写监听端口自动镜像 AOF，顺带覆盖 AOF 域数据面）
    {
      let session = provider.store().new_session().expect("new session");
      for i in 0..WRITE_COUNT {
        session
          .upsert(key(i).as_bytes(), &value(i))
          .await
          .expect("upsert");
      }
    }

    // 轮询等待周期任务自动翻倍：64 → 128（达到上限即停）
    let mut grown = false;
    for _ in 0..WAIT_SECS * 10 {
      sleep(Duration::from_millis(100)).await;
      if provider.store().active_index().size >= MAX_INDEX_SIZE {
        grown = true;
        break;
      }
    }
    assert!(
      grown,
      "周期任务须在 {WAIT_SECS}s 内将索引自 {INITIAL_INDEX_SIZE} 扩至 {MAX_INDEX_SIZE}"
    );

    // 达上限后停扩（allIndexesMaxedOut 语义）：再等两个周期容量不变
    sleep(Duration::from_secs(FREQUENCY_SECS * 2)).await;
    assert_eq!(
      provider.store().active_index().size,
      MAX_INDEX_SIZE,
      "达上限后索引不得继续扩容"
    );

    // 扩容后全部数据完整（对标 C# 断言 entry created before resizing is still accessible）
    let session = provider.store().new_session().expect("new session");
    for i in 0..WRITE_COUNT {
      assert_eq!(
        session.read(key(i).as_bytes()).await.expect("read"),
        Some(value(i)),
        "扩容后键 {} 数据不一致",
        key(i)
      );
    }
  });
}

/// CONFIG SET index 等值/缩小三态差分（对位 C# ServerConfig.cs:270
/// HandleIndexSizeChangeAsync：2 的幂校验后等值目标桶数直接 return → +OK、
/// 缩小回 GenericErrIndexSizeSmallerThanCurrent 专用文案、仅扩容走 GrowIndex）。
/// 旧实现把 `target<=curr` 一律映射为 GROW_FAILED，等值输入被误拒、缩小态文案漂移。
#[test]
fn config_set_index_equal_ok_smaller_dedicated_error() {
  let rt = Runtime::new().expect("compio runtime");
  let dir = tempdir().expect("tempdir");
  let data_path = dir.path().join("node").join("index_cfg.db");

  // 小索引基线：桶数取 2 的幂，等值/缩小目标据此推导
  let mut config = test_store_config();
  config.index_size = INITIAL_INDEX_SIZE;
  let provider = Arc::new(
    StorageSessionProvider::open_with_config_and_aof(
      config,
      &data_path,
      None,
      RuntimeServerOptions::default(),
      wnode_test::session_factory,
    )
    .expect("open"),
  );

  rt.block_on(async {
    let store = provider.store();
    let curr_buckets = store.active_index().size;
    assert!(curr_buckets.is_power_of_two(), "基线索引桶数须为 2 的幂");

    let rc = RuntimeServerConfig::new(RuntimeServerOptions::default());
    let host = ConfigSetHost {
      runtime_config: &rc,
      primary_tasks: None,
      aof: None,
      cluster: None,
      // 本测专测等值/缩小两臂，门关（auto-grow 拒绝臂由案二门单测覆盖）
      index_auto_grow_active: false,
      #[cfg(feature = "tls")]
      tls_config: None,
    };
    let mut s = ServerConfig;

    // 等值：目标桶数 == 当前（字节值 = curr_buckets * 64）→ 无错续行 → +OK
    let equal = (curr_buckets as i64 * 64).to_string();
    let mut out = Vec::new();
    s.network_config_set(&[b"index", equal.as_bytes()], Some(&store), &host, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n", "等值 index 须成功回 +OK");

    // 缩小：目标桶数 < 当前 → 专用 smaller-than-current 文案，绝非 GROW_FAILED
    let smaller = ((curr_buckets / 2) as i64 * 64).to_string();
    let mut out = Vec::new();
    s.network_config_set(
      &[b"index", smaller.as_bytes()],
      Some(&store),
      &host,
      &mut out,
    )
    .unwrap();
    assert!(
      out.starts_with(b"-ERR Cannot set dynamic index size smaller than current index size")
        && out.ends_with(b"(option: 'index')\r\n"),
      "缩小 index 须回专用文案，实得 {out:?}"
    );
    assert!(
      !String::from_utf8_lossy(&out).contains("grow index size beyond"),
      "缩小态不得再误报 GROW_FAILED，实得 {out:?}"
    );
  });
}
