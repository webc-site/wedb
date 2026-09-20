//! 排序批量装载内核与逐条 insert 的等价性 (批量折叠准则的语义回归门)
//!
//! 等价口径取「逻辑终态」而非引擎数据文件字节：实测同一批条目经逐条路径与批量
//! 内核各建一树，页内记录偏移可不一致 (物理页布局是引擎内部细节，非内核对宿主的
//! 契约，见 task/ing/wbftree-sorted-bulk-load.md 的等价口径登记)。
//!
//! - 有序输入：内核与逐条 insert 的全区间有序扫描记录序列、逐键点读 (含长值页
//!   外链) 逐字节一致，去重条数等于条目数；
//! - 乱序 + 同批重复键 + 长值输入：同上，且重复键取输入序末值；
//! - 升阶规模 (65536 条目) 单独走一轮，覆盖批量装载在真实升阶容量下的终态。

use std::time::Instant;

use aok::{OK, Result};
use wbftree::{
  BfTreeInsertResult, BfTreeReadResult, BfTreeService, ScanReturnField, StorageBackendType,
  TreeTuning,
};

#[path = "interop/common.rs"]
mod common;
#[path = "guard/mod.rs"]
mod guard;

use common::managed_tree;

/// 装载面测试调优：16KB 叶页 + 1KB 记录上限，使批次跨越多个叶页并留出长值余量
const TUNE: TreeTuning = TreeTuning {
  cache_size: 8 << 20,
  min_record_size: 8,
  max_record_size: 1024,
  max_key_len: 128,
  leaf_page_size: 16384,
};

/// 等长定宽键 (字节序即编号序)，每 32 条一条 576B 长值触页外链
fn entry(i: usize) -> (Vec<u8>, Vec<u8>) {
  let val = if i.is_multiple_of(32) {
    format!("big{i:06}").repeat(64)
  } else {
    format!("val{i:06}")
  };
  (format!("key{i:06}").into_bytes(), val.into_bytes())
}

/// 确定性伪随机置换 (黄金比例乘法散列，无随机种子依赖，跨运行可复现)
fn scrambled(n: usize) -> Vec<usize> {
  let mut order: Vec<usize> = (0..n).collect();
  order.sort_unstable_by_key(|&i| (i as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15).swap_bytes());
  order
}

/// 全区间有序扫描导出 (逐条拷出以脱离引擎复用缓冲)
fn dump(tree: &BfTreeService) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
  let mut out = Vec::new();
  tree.scan_with_count_callback(&[0], usize::MAX, ScanReturnField::KeyAndValue, |k, v| {
    out.push((k.to_vec(), v.to_vec()));
    true
  })?;
  Ok(out)
}

/// 同键点读结果 (值本体，含长值的页外链)
fn read(tree: &BfTreeService, key: &[u8]) -> Option<Vec<u8>> {
  let (res, val) = tree.read(key);
  (res == BfTreeReadResult::Found).then_some(val.unwrap_or_default())
}

/// 有序输入下批量装载与逐条 insert 的逻辑终态等价
///
/// 口径为「有序扫描记录序列 + 逐键点读 (含长值页外链)」，不做引擎数据文件逐字节
/// 比对：实测同一条逐条路径与批量内核建树，页内记录偏移可不同 (物理页布局属引擎
/// 内部细节，非内核对宿主的契约)，见 task/ing/wbftree-sorted-bulk-load.md 的等价口径登记。
#[test]
fn bulk_load_matches_sequential_insert_terminal_state() -> Result<()> {
  const N: usize = 4096;
  let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..N).map(entry).collect();

  let (_dir_seq, _mgr_seq, seq) = managed_tree("bftree_bulk_seq", StorageBackendType::Disk, TUNE)?;
  for (k, v) in &entries {
    assert_eq!(seq.insert(k, v), BfTreeInsertResult::Success);
  }
  let (_dir_bulk, _mgr_bulk, bulk) =
    managed_tree("bftree_bulk_load", StorageBackendType::Disk, TUNE)?;
  assert_eq!(
    bulk.bulk_load(&entries).expect("批量装载内核不应失败"),
    N as u64
  );

  let (dump_seq, dump_bulk) = (dump(&seq)?, dump(&bulk)?);
  assert_eq!(
    dump_bulk, dump_seq,
    "全区间有序扫描记录序列须与逐条 insert 一致"
  );
  assert!(
    dump_bulk.windows(2).all(|w| w[0].0 < w[1].0),
    "扫描输出须严格按键升序"
  );
  for (k, v) in &entries {
    assert_eq!(
      read(&bulk, k),
      read(&seq, k),
      "逐键点读 (含长值页外链) 须同字节"
    );
    assert_eq!(read(&seq, k).as_deref(), Some(v.as_slice()), "点读须回原值");
  }
  OK
}

/// 升阶容量 (65536 条目) 下批量装载与逐条 insert 的终态等价 + 一次性计时登记
#[test]
fn bulk_load_matches_sequential_insert_at_promotion_capacity() -> Result<()> {
  const N: usize = 65536;
  let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..N).map(entry).collect();

  let (_dir_seq, _mgr_seq, seq) =
    managed_tree("bftree_bulk_cap_seq", StorageBackendType::Disk, TUNE)?;
  let t0 = Instant::now();
  for (k, v) in &entries {
    assert_eq!(seq.insert(k, v), BfTreeInsertResult::Success);
  }
  let per_entry = t0.elapsed();
  let (_dir_bulk, _mgr_bulk, bulk) =
    managed_tree("bftree_bulk_cap_load", StorageBackendType::Disk, TUNE)?;
  let t1 = Instant::now();
  assert_eq!(
    bulk.bulk_load(&entries).expect("升阶容量批量装载不应失败"),
    N as u64
  );
  let bulk_load = t1.elapsed();

  assert_eq!(dump(&seq)?, dump(&bulk)?);
  eprintln!("bulk_load_promotion_capacity entries={N} per_entry={per_entry:?} bulk={bulk_load:?}");
  OK
}

/// 乱序 + 重复键 + 长值输入下，批量装载与逐条 insert 终态等价
#[test]
fn bulk_load_matches_sequential_insert_on_scrambled_input() -> Result<()> {
  const N: usize = 2048;
  let mut entries: Vec<(Vec<u8>, Vec<u8>)> = scrambled(N).into_iter().map(entry).collect();
  // 同批重复键：后出现者胜 (与逐条 insert 的覆盖序一致)
  let dup_key = format!("key{:06}", N / 3).into_bytes();
  entries.push(entry(N / 3));
  entries.push((dup_key.clone(), b"last-write-wins".to_vec()));

  let (_dir_seq, _mgr_seq, seq) =
    managed_tree("bftree_bulk_rand_seq", StorageBackendType::Disk, TUNE)?;
  for (k, v) in &entries {
    assert_eq!(seq.insert(k, v), BfTreeInsertResult::Success);
  }
  let (_dir_bulk, _mgr_bulk, bulk) =
    managed_tree("bftree_bulk_rand", StorageBackendType::Disk, TUNE)?;
  // 去重后落刷条数 = 唯一键数 (旧逐条路径把重复键重复计入 meta.size)
  assert_eq!(
    bulk.bulk_load(&entries).expect("批量装载内核不应失败"),
    N as u64
  );

  assert_eq!(
    dump(&bulk)?,
    dump(&seq)?,
    "全区间有序扫描字节流须与逐条 insert 一致"
  );
  for (k, _) in &entries {
    assert_eq!(
      read(&bulk, k),
      read(&seq, k),
      "逐键点读 (含长值链) 须同字节"
    );
  }
  assert_eq!(read(&bulk, &dup_key), Some(b"last-write-wins".to_vec()));
  OK
}

/// upsert 语义：真实新增计数 + 同批重复键取输入序末值 + 覆盖不增长
#[test]
fn upsert_counts_only_new_keys() -> Result<()> {
  let (_dir, _mgr, tree) = managed_tree("bftree_bulk_upsert", StorageBackendType::Disk, TUNE)?;
  let batch = [
    (b"k_b".as_slice(), b"v_b_11".as_slice()),
    (b"k_a".as_slice(), b"v_a_11".as_slice()),
    (b"k_b".as_slice(), b"v_b_22".as_slice()),
    (b"k_c".as_slice(), b"v_c_11".as_slice()),
  ];
  assert_eq!(
    tree.upsert(&batch).expect("upsert 内核不应失败"),
    3,
    "同批重复键只计一次新增"
  );
  assert_eq!(dump(&tree)?.len(), 3);
  assert_eq!(
    read(&tree, b"k_b"),
    Some(b"v_b_22".to_vec()),
    "同批重复键取末值"
  );

  assert_eq!(
    tree
      .upsert(&[(b"k_a".as_slice(), b"v_a_22".as_slice())])
      .expect("覆盖写不应失败"),
    0
  );
  assert_eq!(read(&tree, b"k_a"), Some(b"v_a_22".to_vec()));

  // 既有树上的批量覆盖：新增计数恒 0，值全部换成新字节
  common::insert_test_data(&tree, 256);
  let seeded: Vec<(Vec<u8>, Vec<u8>)> = (0..256)
    .map(|i| {
      (
        format!("key:{i:04}").into_bytes(),
        format!("newv{i:04}").into_bytes(),
      )
    })
    .collect();
  assert_eq!(tree.upsert(&seeded).expect("覆盖不应失败"), 0);
  assert_eq!(dump(&tree)?.len(), 259, "256 既有键 + 3 新键，覆盖不增长");
  assert_eq!(read(&tree, b"key:0007"), Some(b"newv0007".to_vec()));
  OK
}

/// 前置校验零副作用：空值条目整批拒绝，批次内任何条目都不落树
#[test]
fn bulk_load_rejects_empty_value_without_side_effects() -> Result<()> {
  let (_dir, _mgr, tree) = managed_tree("bftree_bulk_guard", StorageBackendType::Disk, TUNE)?;
  assert_eq!(
    tree.bulk_load::<&[u8], &[u8]>(&[]).expect("空批次不应失败"),
    0,
    "空批次零借用零写入"
  );

  let batch = [
    (b"aa_1".as_slice(), b"v1_x".as_slice()),
    (b"bb_1".as_slice(), b"".as_slice()),
    (b"cc_1".as_slice(), b"v3_x".as_slice()),
  ];
  assert_eq!(tree.bulk_load(&batch), Err(BfTreeInsertResult::InvalidKV));
  assert!(dump(&tree)?.is_empty(), "拒绝须先于任何下刷");
  assert_eq!(
    tree.upsert(&batch),
    Err(BfTreeInsertResult::InvalidKV),
    "upsert 同口径前置拒绝"
  );
  assert!(dump(&tree)?.is_empty());
  OK
}

/// 已释放实例上的批量写入一律结构化失败，绝不触引擎
#[test]
fn bulk_load_on_disposed_service_reports_invalid_arguments() -> Result<()> {
  let (_dir, _mgr, tree) = managed_tree("bftree_bulk_disposed", StorageBackendType::Disk, TUNE)?;
  tree.dispose();
  assert_eq!(
    tree.bulk_load(&[(b"kk_1".as_slice(), b"vv_1".as_slice())]),
    Err(BfTreeInsertResult::InvalidArguments)
  );
  assert_eq!(
    tree.upsert(&[(b"kk_1".as_slice(), b"vv_1".as_slice())]),
    Err(BfTreeInsertResult::InvalidArguments)
  );
  OK
}
