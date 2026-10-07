#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 转出标记未落笔留痕臂集成测试（自 src/range_index/promote.rs 内联测迁入：
//! 真 WedbStore + SegmentedDevice 落盘形态，断言与覆盖原样保留）
//!
//! 墓碑/键不匹配两形需直调编排口注入（转移源经公共流程抵达转出点时恒驻只读区，
//! 原位判死与同址异键均无确定性注入缝），经 #[doc(hidden)] 测试专用口
//! `transfer_out_source_stub` 直调（见该定义处注）。
//!
//! 同进程共享全局日志捕获注册表（wtest_base ctor 装配），用例间天然串行
//! （cargo test 同一二进制默认串行跑同线程），无需另加互斥。

use std::sync::Arc;

use compio::runtime::Runtime;
use log::Level;
use tempfile::TempDir;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wtest_base::{log_capture_mark, log_capture_records_since};
use wval::{GarnetObjectType, KeyTag, NamespaceDbCodec, TaggedKeyBuf};

/// 树身份键 = 物理 Meta 键（默认会话域 (0, 0)）
fn id_key(user_key: &[u8]) -> TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::Meta, user_key)
}

/// 升阶分层键并定位源存根地址（可变区、未封印——两形注入的前提）
async fn setup(
  dir: &TempDir,
  name: &str,
  key: &[u8],
) -> wkv::Result<(Arc<WedbStore<SegmentedDevice>>, TaggedKeyBuf, u64)> {
  let mut config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5)?
    .with_range_index_dir(dir.path().join("range_indexes"));
  config.gc.enabled = false;
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(name))?);
  let store = Arc::new(WedbStore::open(config, device)?);
  let session = store.new_session()?;
  session
    .promote_collection_to_bftree(
      key,
      GarnetObjectType::Hash,
      vec![(b"f1".to_vec(), b"v1".to_vec())],
      i64::MAX,
      false,
    )
    .await?;
  let tree_key = id_key(key);
  let src_addr = store
    .index
    .load()
    .find_tag(&tree_key)
    .expect("升阶 Meta 存根必须挂载索引");
  Ok((store, tree_key, src_addr))
}

/// 墓碑形（并发 DEL 交叠窗等价形）：源记录原位判死后转出标记必留痕
#[test]
fn tombstoned_source_leaves_unlanded_trace() -> wkv::Result<()> {
  Runtime::new()?.block_on(async {
    let dir = TempDir::new()?;
    let (store, tree_key, src_addr) = setup(&dir, "face_tomb.db", b"h:tomb").await?;

    // 可变区源记录原位判死（页写锁内原子，对标 DEL 原位墓碑内核）
    store
      .hlog
      .try_set_tombstone_in_place(src_addr, &tree_key)
      .expect("可变区源记录原位判死必须成功");

    let mark = log_capture_mark();
    store
      .new_session()?
      .transfer_out_source_stub(&tree_key, src_addr);

    let logs = log_capture_records_since(mark);
    assert!(
      logs.iter().any(|(lvl, msg)| {
        *lvl == Level::Error && msg.contains("转出标记未落笔") && msg.contains("墓碑")
      }),
      "墓碑形未落笔必须留痕可见: {logs:?}"
    );
    Ok(())
  })
}

/// 键不匹配形（SWAPDB 换号/槽位复用交叠窗等价形）：误键编排必留痕且零副作用
#[test]
fn key_mismatched_source_leaves_unlanded_trace() -> wkv::Result<()> {
  Runtime::new()?.block_on(async {
    let dir = TempDir::new()?;
    let (store, _, src_addr) = setup(&dir, "face_mismatch.db", b"h:mismatch").await?;
    let src_val_before = {
      let rec = store.new_session()?.read_record(src_addr).await?;
      rec.value()?.to_vec()
    };

    let wrong_key = id_key(b"h:other");
    let mark = log_capture_mark();
    store
      .new_session()?
      .transfer_out_source_stub(&wrong_key, src_addr);

    let logs = log_capture_records_since(mark);
    assert!(
      logs.iter().any(|(lvl, msg)| {
        *lvl == Level::Error && msg.contains("转出标记未落笔") && msg.contains("键不匹配")
      }),
      "键不匹配形未落笔必须留痕可见: {logs:?}"
    );

    // 零副作用：误键预置臂不落 pending、源记录原位分毫未动
    let rec = store.new_session()?.read_record(src_addr).await?;
    assert_eq!(rec.value()?.to_vec(), src_val_before, "误键编排零副作用");
    Ok(())
  })
}
