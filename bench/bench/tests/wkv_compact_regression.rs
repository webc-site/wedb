//! wkv 紧缩回归测试（票 bench-wkv-compact-flush-fake）
//!
//! 契约锚：bench/bench/src/traits.rs:15-18 compact 须触发物理数据整理；
//! 调用形态对标 wkv/tests/compact/basic.rs:57-99（在线紧缩 + FoldOver 检查点物理删段）。
//! 断言：compact() 返回 true、整理后目录字节回落、段文件数减少、存活键读回全等。

use std::{fs, path::Path};

use aok::{OK, Void};
use bench::{
  engines::wkv_engine::WkvEngine,
  harness::database_size,
  traits::{BenchDatabase, BenchDatabaseConnection, BenchReadTransaction, BenchWriteTransaction},
};

/// 数据规模：1.2 万键 × 8KB ≈ 99MB，跨 2 个 64MiB 段；75% 删除令尾段复制量
/// ≈ 64MiB × 25% ≈ 16MB，不越第 3 段边界（紧缩后收敛回单段）
const ELEMENTS: usize = 12_000;
const REMOVED: usize = 9_000;
const KEY_SIZE: usize = 24;
const VALUE_SIZE: usize = 8 * 1024;

fn write_key(i: usize, buf: &mut [u8]) {
  let k = format!("key_{i:08}");
  buf[..k.len()].copy_from_slice(k.as_bytes());
}

/// 值体按索引填充确定性非零模式，读回逐字节全等校验零映射开销
fn fill_value(i: usize, val: &mut [u8]) {
  val.fill((i % 251 + 1) as u8);
}

/// 统计数据库目录内段文件（命名形态 bench.wkv.<13 字符 base32>）
fn segment_files(dir: &Path) -> Vec<String> {
  const PREFIX: &str = "bench.wkv.";
  let mut segs: Vec<String> = fs::read_dir(dir)
    .unwrap()
    .filter_map(|e| e.ok())
    .map(|e| e.file_name().to_string_lossy().into_owned())
    .filter(|n| {
      n.starts_with(PREFIX)
        && n.len() == PREFIX.len() + 13
        && n[PREFIX.len()..]
          .bytes()
          .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
    })
    .collect();
  segs.sort();
  segs
}

#[test]
fn wkv_compact_reclaims_dead_space_and_keeps_live_keys() -> Void {
  let dir = tempfile::tempdir()?;
  let mut engine = WkvEngine::open(&dir.path().join("bench.wkv"), 32 * 1024 * 1024, ELEMENTS)?;
  let mut conn = engine.connect();
  // nosync 批量写（与 harness nosync 段同口径），紧缩链首步 flush_all 兜底落盘
  assert!(conn.set_sync(false));

  let mut kb = [0u8; KEY_SIZE];
  let mut vb = vec![0u8; VALUE_SIZE];
  {
    let mut txn = conn.write_transaction();
    for i in 0..ELEMENTS {
      write_key(i, &mut kb);
      fill_value(i, &mut vb);
      txn.insert(&kb, &vb)?;
    }
    txn.commit()?;
  }
  {
    let mut txn = conn.write_transaction();
    for i in 0..REMOVED {
      write_key(i, &mut kb);
      txn.remove(&kb)?;
    }
    txn.commit()?;
  }

  // 整理前测量（harness 第 10 段同口径：drop 连接 → flush → 量目录）
  drop(conn);
  engine.flush();
  let uncompacted = database_size(dir.path());
  let segs_before = segment_files(dir.path());
  assert!(
    segs_before.len() >= 2,
    "写入须跨多段，实际段 {segs_before:?}"
  );

  // 真紧缩链（flush → 在线紧缩 → 检查点物理删段）必须全链成功
  assert!(engine.compact(), "compact() 须返回 true");

  let compacted = database_size(dir.path());
  let segs_after = segment_files(dir.path());
  assert!(
    compacted < uncompacted,
    "整理后目录字节须回落: {compacted} !< {uncompacted}"
  );
  assert!(
    segs_after.len() < segs_before.len(),
    "段文件数须减少: {segs_before:?} -> {segs_after:?}"
  );

  // 存活键读回全等，被删键读空
  let conn = engine.connect();
  let mut rd = conn.read_transaction();
  for i in 0..ELEMENTS {
    write_key(i, &mut kb);
    if i < REMOVED {
      assert_eq!(rd.get(&kb), None, "键 {i} 已删须读空");
    } else {
      fill_value(i, &mut vb);
      assert_eq!(rd.get(&kb).as_deref(), Some(&vb[..]), "键 {i} 读回须全等");
    }
  }

  OK
}

/// 空库零写入紧缩链极端边界：全链照常成功，杜绝 Err 误报 false
#[test]
fn wkv_compact_on_empty_store_succeeds() -> Void {
  let dir = tempfile::tempdir()?;
  let mut engine = WkvEngine::open(&dir.path().join("bench.wkv"), 8 * 1024 * 1024, 16)?;
  assert!(engine.compact(), "空库紧缩链须成功");
  OK
}
