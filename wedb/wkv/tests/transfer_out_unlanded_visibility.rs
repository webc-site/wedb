//! 票 wkv-range-index-transfer-out-patch-double-discard 回归：transfer-out 弃返
//! 改留痕 + 待治愈登记
//!
//! 转移源恒驻只读区，begin/head 推进令页滑出环形窗是常态竞速；未落笔面弃返即
//! 转出标记静默丢失（过期源存根句柄非零且未 Transferred 的幻影面）。修复后
//! 未落笔一律 log::error 留痕登记待治愈，恢复期存根治愈链按「被索引引用」判活
//! 单点结算（过期源帧结构性不收录、不重写），正常落笔臂零回归。
//!
//! 键不匹配/墓碑两形需直调编排口注入（转移源经公共流程抵达转出点时恒驻只读区，
//! 原位判死不可达，无确定性注入缝），见 transfer_out_unlanded_face.rs（自 src
//! 单测 promote.rs#tests 迁入）。
//!
//! 本文件用例同进程共享全局转出标记窗留钩，串行锁防互扰。

#[path = "store_open.rs"]
mod store_open;

use std::sync::{Arc, Mutex, MutexGuard, OnceLock, PoisonError};

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::Level;
use store_open::{open_store_in, range_index_config};
use tempfile::TempDir;
use wbftree::{RANGE_INDEX_STUB_SIZE, RangeIndexStub};
#[cfg(debug_assertions)]
use wcpr::CheckpointType;
// 注入用例（transfer_out_unlanded_on_disk_slide_leaves_error_trace）专属：
// TEST_TRANSFER_OUT_HOOK 系 debug 门控符号，随用例 release 剔除
use wdev::SegmentedDevice;
#[cfg(debug_assertions)]
use wkv::TEST_TRANSFER_OUT_HOOK;
use wkv::WedbStore;
use wval::{GarnetObjectType, KeyTag, META_VALUE_SIZE, NamespaceDbCodec, TaggedKeyBuf};

const PAGE: usize = 1024 * 1024;

/// 用例串行闸（同进程全局留钩互斥；毒锁解包续用，单例失败不连坐后续用例）
fn serial() -> MutexGuard<'static, ()> {
  static SERIAL: OnceLock<Mutex<()>> = OnceLock::new();
  SERIAL
    .get_or_init(|| Mutex::new(()))
    .lock()
    .unwrap_or_else(PoisonError::into_inner)
}

/// 树身份键 = 物理 Meta 键（默认会话域 (0, 0)）
fn id_key(user_key: &[u8]) -> TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::Meta, user_key)
}

/// 读指定地址记录值体中的存根（界内性由 Meta 复合记录编码保证）
async fn stub_at(
  store: &Arc<WedbStore<SegmentedDevice>>,
  addr: u64,
) -> aok::Result<RangeIndexStub> {
  let session = store.new_session()?;
  let rec = session.read_record(addr).await?;
  let val = rec.value()?;
  Ok(RangeIndexStub::decode(
    &val[META_VALUE_SIZE..META_VALUE_SIZE + RANGE_INDEX_STUB_SIZE],
  )?)
}

/// 页滑出形未落笔必留痕：转出标记窗内 head 推过源地址（磁盘冷区常态），转出
/// 标记丢失必须 log::error 可见，源存根保持原位位形登记待治愈；活视图与恢复
/// 期判活结算不受波及（过期源帧不收录、不重写）
// 留钩注入用例：随 TEST_TRANSFER_OUT_HOOK 的 debug 门控剔除（release 走
// transfer_out_marks_source_on_normal_resident_path 纯行为口径）
#[cfg(debug_assertions)]
#[test]
fn transfer_out_unlanded_on_disk_slide_leaves_error_trace() -> Void {
  let _serial = serial();
  Runtime::new()?.block_on(async {
    let dir = TempDir::new()?;
    let mut config = range_index_config(&dir, 2048, PAGE)?;
    config.gc.enabled = false;
    let store = open_store_in(&dir, "transfer_slide.db", config)?;
    let session = store.new_session()?;

    let key = b"h:slide";
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

    // 组提交刷盘：源存根置 Flushed（RIPROMOTE 前置）并滑入只读区
    store.flush_all().await?;
    store.shift_read_only_address(store.tail_address());
    assert!(
      stub_at(&store, src_addr).await?.is_flushed(),
      "前置：源存根已刷盘置位"
    );

    // 转出标记窗注入：预置臂之后、落笔判定之前把 head 推过源地址
    let hook_store = Arc::clone(&store);
    *TEST_TRANSFER_OUT_HOOK.lock() = Some(Box::new(move || {
      hook_store.shift_head_address(src_addr + 1);
    }));

    let log_mark = wtest_base::log_capture_mark();
    session.promote_range_index_to_tail(key).await?;

    // 留痕断言：转出失败必可见（错误留痕 + 待治愈登记）
    let logs = wtest_base::log_capture_records_since(log_mark);
    assert!(
      logs.iter().any(|(lvl, msg)| {
        *lvl == Level::Error && msg.contains("转出标记未落笔") && msg.contains("滑出内存环形窗")
      }),
      "页滑出形未落笔必须留痕可见: {logs:?}"
    );

    // 源存根原位位形不变：句柄非零 + 未转出（待治愈登记形态，留待恢复期判活复核）
    let src_stub = stub_at(&store, src_addr).await?;
    assert!(src_stub.is_flushed(), "非目标位不得被顺带改写");
    assert!(!src_stub.is_transferred(), "未落笔即未转出");
    assert_ne!(src_stub.tree_handle, 0, "未落笔即句柄未清零");

    // 活视图不受波及：索引已迁尾部新帧，树在册
    let dst_addr = store
      .index
      .load()
      .find_tag(&tree_key)
      .expect("RIPROMOTE 后索引必须挂载尾部新帧");
    assert_ne!(dst_addr, src_addr);
    assert!(store.range_index().get_tree(&tree_key).is_some());

    // 消费面安全：检查点快照收集与恢复期结算不收录过期源存根成活视图——
    // 被索引引用的活帧结算 mark_recovered，过期源帧不收录、不重写
    let ckpt_dir = dir.path().join("checkpoints");
    let meta = store
      .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
      .await?;
    let recovered =
      Arc::new(WedbStore::recover(&ckpt_dir, meta.token, Arc::clone(&store.device)).await?);
    let r_session = recovered.new_session()?;
    let loaded = r_session
      .load_meta(key)
      .await?
      .expect("恢复后元记录必须存活");
    assert!(loaded.collection_type.is_tiered_collection(), "单视图存活");

    let live_addr = recovered
      .index
      .load()
      .find_tag(&tree_key)
      .expect("恢复索引必须挂载活帧");
    let live_stub = stub_at(&recovered, live_addr).await?;
    assert_eq!(live_stub.tree_handle, 0, "被索引引用帧结算清零句柄");
    assert!(live_stub.is_recovered(), "被索引引用帧结算恢复位");

    let stale_stub = stub_at(&recovered, src_addr).await?;
    assert!(
      !stale_stub.is_transferred() && stale_stub.tree_handle != 0,
      "过期源帧按判活单点跳过：不收录、不重写"
    );
    OK
  })
}

/// 正常只读区臂零回归：无注入时 RIPROMOTE 源存根经驻留内核落笔转出
/// （句柄清零 + Transferred 置位），尾部新帧清 Flushed，全程零未落笔留痕
#[test]
fn transfer_out_marks_source_on_normal_resident_path() -> Void {
  let _serial = serial();
  Runtime::new()?.block_on(async {
    let dir = TempDir::new()?;
    let mut config = range_index_config(&dir, 2048, PAGE)?;
    config.gc.enabled = false;
    let store = open_store_in(&dir, "transfer_normal.db", config)?;
    let session = store.new_session()?;

    let key = b"h:normal";
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

    store.flush_all().await?;
    store.shift_read_only_address(store.tail_address());

    let log_mark = wtest_base::log_capture_mark();
    session.promote_range_index_to_tail(key).await?;

    let logs = wtest_base::log_capture_records_since(log_mark);
    assert!(
      !logs
        .iter()
        .any(|(lvl, msg)| *lvl == Level::Error && msg.contains("转出标记未落笔")),
      "正常落笔臂不得产生未落笔留痕: {logs:?}"
    );

    // 源存根转出闭环（C# ClearTreeHandle + SetTransferredFlag 等价落定）
    let src_stub = stub_at(&store, src_addr).await?;
    assert!(src_stub.is_transferred(), "只读区源必经驻留内核落笔转出");
    assert_eq!(src_stub.tree_handle, 0, "转出即句柄清零");
    assert!(src_stub.is_flushed(), "非目标位不得被顺带改写");

    // 尾部新帧承接活视图：Flushed 已清、句柄在位、未转出
    let dst_addr = store
      .index
      .load()
      .find_tag(&tree_key)
      .expect("RIPROMOTE 后索引必须挂载尾部新帧");
    assert_ne!(dst_addr, src_addr);
    let dst_stub = stub_at(&store, dst_addr).await?;
    assert!(!dst_stub.is_flushed(), "RIPROMOTE 尾部新帧清 Flushed");
    assert!(!dst_stub.is_transferred());
    assert_ne!(dst_stub.tree_handle, 0);
    assert!(store.range_index().get_tree(&tree_key).is_some());
    OK
  })
}
