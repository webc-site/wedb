//! 批量写折叠与循环前缀外提内核测试（transpile SKILL 工程准则；rust 工程优化无 c# 对应）
//!
//! 覆盖：try_upsert_batch_sync 重复键后者胜与乱序不变性、with_prefix 读/删
//! 内核跨库前缀隔离、空批次零副作用。
//!
//! 自研依据: 批量接口单次折叠——循环前缀外提（transpile 契约 Batching API，消除逐字段重算 Varint）

use aok::Void;
use wkv::StoreResult;
use wval::{KeyTag, SessionPrefixBuf};

use crate::support::{config, open_store};

/// 批量写折叠：乱序输入写入后可全部回读，重复键保末值（MSET 后者胜语义）
#[compio::test]
async fn batch_upsert_dup_last_wins_and_readback() -> Void {
  let env = open_store("batch_prefix.db", config(2048, 64 * 1024, 16)?)?;
  let store = env.store;
  let session = store.new_session()?;
  let batch = session.enter_batch();

  // 乱序输入 + 重复键（排序后相邻去重仅末值生效）
  let pairs = [
    (b"kb" as &[u8], b"vb1" as &[u8]),
    (b"ka", b"va1"),
    (b"kc", b"vc1"),
    (b"ka", b"va2"),
  ];
  let res = batch.try_upsert_batch_sync(pairs)?;
  assert_eq!(res, Ok(()));

  let read = |batch: &wkv::BatchStoreSession<_>, k: &[u8]| {
    batch
      .try_read_sync(k, |v| v.to_vec())
      .unwrap()
      .value()
      .unwrap()
  };
  assert_eq!(read(&batch, b"ka"), b"va2".to_vec(), "重复键应保末值");
  assert_eq!(read(&batch, b"kb"), b"vb1".to_vec());
  assert_eq!(read(&batch, b"kc"), b"vc1".to_vec());
  Ok(())
}

/// 空批次零副作用成功
#[compio::test]
async fn batch_upsert_empty_ok() -> Void {
  let env = open_store("batch_prefix_empty.db", config(2048, 64 * 1024, 16)?)?;
  let session = env.store.new_session()?;
  let batch = session.enter_batch();
  let pairs: [(&[u8], &[u8]); 0] = [];
  assert_eq!(batch.try_upsert_batch_sync(pairs)?, Ok(()));
  Ok(())
}

/// 循环前缀外提内核：db0/db1 各写同名键，显式前缀读/删严格按前缀隔离
#[compio::test]
async fn with_prefix_kernels_isolate_active_db() -> Void {
  let env = open_store("batch_prefix_db.db", config(2048, 64 * 1024, 16)?)?;
  let store = env.store;
  let session = store.new_session()?;

  // db0 写 k=v0
  session.set_context(0, 0);
  let p0 = session.session_prefix();
  let batch0 = session.enter_batch();
  assert_eq!(
    batch0.try_upsert_batch_sync([(b"k" as &[u8], b"v0" as &[u8])])?,
    Ok(())
  );
  drop(batch0);

  // db1 批量写同名键 k=v1
  session.set_context(0, 1);
  let p1 = session.session_prefix();
  let batch1 = session.enter_batch();
  assert_eq!(
    batch1.try_upsert_batch_sync([(b"k" as &[u8], b"v1" as &[u8])])?,
    Ok(())
  );

  // 显式前缀读内核：两库前缀各自取回各自值（前缀错误即缺失，杜绝串库）
  let read_with = |prefix: &SessionPrefixBuf, k: &[u8]| {
    batch1.try_read_tag_sync_unprotected_with_prefix(prefix.as_slice(), k, KeyTag::String, |v| {
      v.to_vec()
    })
  };
  assert_eq!(
    read_with(&p0, b"k")?,
    StoreResult::Success(b"v0".to_vec()),
    "db0 前缀应读回 db0 值"
  );
  assert_eq!(
    read_with(&p1, b"k")?,
    StoreResult::Success(b"v1".to_vec()),
    "db1 前缀应读回 db1 值"
  );

  // 显式前缀删内核：仅删除 db1 前缀数据，db0 前缀数据不受影响
  assert_eq!(
    batch1.try_delete_sync_with_prefix(p1.as_slice(), b"k")?,
    Ok(true)
  );
  assert_eq!(
    read_with(&p1, b"k")?,
    StoreResult::NotFound,
    "db1 前缀删除后应缺失"
  );
  assert_eq!(
    read_with(&p0, b"k")?,
    StoreResult::Success(b"v0".to_vec()),
    "db0 前缀数据不受影响"
  );
  Ok(())
}

/// 显式前缀批量写：复用外提前缀切片，乱序去重与写入语义与默认 try_upsert_batch_sync 一致
#[compio::test]
async fn batch_upsert_with_prefix_reuse() -> Void {
  let env = open_store("batch_prefix_reuse.db", config(2048, 64 * 1024, 16)?)?;
  let store = env.store;
  let session = store.new_session()?;

  session.set_context(0, 2);
  let prefix = session.session_prefix();
  let batch = session.enter_batch();

  let pairs = [
    (b"k1" as &[u8], b"v1" as &[u8]),
    (b"k2", b"v2_old"),
    (b"k2", b"v2_new"),
  ];
  let res = batch.try_upsert_batch_sync_with_prefix(prefix.as_slice(), pairs)?;
  assert_eq!(res, Ok(()));

  let read_with = |k: &[u8]| {
    batch.try_read_tag_sync_unprotected_with_prefix(prefix.as_slice(), k, KeyTag::String, |v| {
      v.to_vec()
    })
  };
  assert_eq!(read_with(b"k1")?, StoreResult::Success(b"v1".to_vec()));
  assert_eq!(read_with(b"k2")?, StoreResult::Success(b"v2_new".to_vec()));
  Ok(())
}

/// 大批次随机重复键后者胜确定性回归（对标 C# ArrayCommands.cs:NetworkMSET 与
/// MainStoreOps.cs:MSET_Conditional 按命令序逐对 SET、重复键末值确定性生效）：
/// 折叠内核 sort_unstable_by 纯键序比较器不承诺相等键的相对顺序，批量 ≥ 24 对
/// 时同键幸存值实测被旧值覆盖（工单独立复刻 100% 破坏区间）。本用例固定种子
/// LCG 构造 160 对乱序批次、64 键每键至少一次重复，逐键断言幸存值恒为命令序末值
#[compio::test]
async fn batch_upsert_large_random_dup_last_wins() -> Void {
  let env = open_store("batch_prefix_large.db", config(2048, 64 * 1024, 16)?)?;
  let store = env.store;
  let session = store.new_session()?;
  let batch = session.enter_batch();

  const KEYS: usize = 64;
  const EXTRA: usize = 96;

  // 固定种子 xorshift 伪随机，保证乱序次序可复现（无依据的伪竞态测试不写）
  let mut state: u64 = 0x9E37_79B9_7F4A_7C15;
  let mut next = || {
    state ^= state << 13;
    state ^= state >> 7;
    state ^= state << 17;
    state
  };

  // 命令序列：64 个键全覆盖后，再散落 96 次重复键出现，整批 Fisher-Yates 乱序
  let mut seq: Vec<usize> = (0..KEYS).collect();
  for _ in 0..EXTRA {
    seq.push((next() % KEYS as u64) as usize);
  }
  for i in (1..seq.len()).rev() {
    seq.swap(i, (next() % (i as u64 + 1)) as usize);
  }

  // 每次出现携带唯一值，期望末值 = 该键在命令序中最后一次出现的值
  let keys: Vec<Vec<u8>> = (0..KEYS).map(|k| format!("k{k:02}").into_bytes()).collect();
  let vals: Vec<Vec<u8>> = (0..seq.len())
    .map(|i| format!("v{i:03}").into_bytes())
    .collect();
  let mut expected: Vec<&[u8]> = vec![b"".as_slice(); KEYS];
  let mut pairs: Vec<(&[u8], &[u8])> = Vec::with_capacity(seq.len());
  for (i, &k) in seq.iter().enumerate() {
    expected[k] = &vals[i];
    pairs.push((&keys[k], &vals[i]));
  }

  assert_eq!(batch.try_upsert_batch_sync(pairs)?, Ok(()));

  for k in 0..KEYS {
    let got = batch.try_read_sync(&keys[k], |v| v.to_vec())?;
    assert_eq!(
      got.value().as_deref(),
      Some(expected[k]),
      "键 k{k:02} 幸存值应为命令序末值（重复键后者胜，MSET 契约）"
    );
  }
  Ok(())
}
