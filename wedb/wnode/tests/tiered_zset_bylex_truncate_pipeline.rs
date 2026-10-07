#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 分层态 ZRANGE byLex 块解析失败撤帧语义集成测试（自
//! src/resp/objects/tiered_collection_ops/zset.rs 内联测迁入，断言与覆盖
//! 原样保留；暴露面经 [`doc(hidden)`] 测试专用口 `exec_tiered_zset`）

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wcol::{
  types::member_ttl::encode_member,
  zset::sorted_set_object::{SortedSetOperation, SortedSetRangeOpts},
};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::objects::tiered_collection_ops::{
  TieredCollectionArgs, TieredCtx, exec_tiered_zset,
};
use wval::GarnetObjectType;

#[test]
fn test_tiered_zset_bylex_syntax_error_truncate() {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("zset_truncate.db")).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let rt = Runtime::new().unwrap();
  let sess = store.new_session().unwrap();
  let key = b"tiered_zset_key";

  // 灌入条目并升阶为分层态
  let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..10)
    .map(|i| {
      let member = format!("m{i:02}").into_bytes();
      let val = encode_member(&((i as f64).to_be_bytes()), None);
      (member, val)
    })
    .collect();

  rt.block_on(sess.promote_collection_to_bftree(
    key,
    GarnetObjectType::SortedSet,
    entries,
    i64::MAX,
    false,
  ))
  .unwrap();

  let batch = sess.enter_batch();
  let (mut meta, mut stub) = rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .unwrap();

  // 1. 单命令场景：output 为空，语法错误写出错误帧
  {
    let mut ctx = TieredCtx::new(&mut meta, &mut stub);
    let mut output = Vec::new();
    let args: &[&[u8]] = &[b"invalid_lex", b"[m05"];
    let range_opts = SortedSetRangeOpts::BY_LEX;
    let call = TieredCollectionArgs::new(
      SortedSetOperation::Zrange,
      (0, range_opts.bits() as i32),
      args,
      2,
    );
    let res = rt.block_on(exec_tiered_zset(&batch, key, &mut ctx, call, &mut output));
    assert_eq!(res, Ok(true));
    assert_eq!(output, b"-ERR min or max not valid string range item\r\n");
  }

  // 2. 流水线（Pipeline）场景：output 包含先前命令已写应答，严禁被 clear 冲刷
  {
    let mut ctx = TieredCtx::new(&mut meta, &mut stub);
    let mut output = Vec::new();
    let prefix = b"+PONG\r\n:1\r\n";
    output.extend_from_slice(prefix);

    let args: &[&[u8]] = &[b"invalid_lex", b"[m05"];
    let range_opts = SortedSetRangeOpts::BY_LEX;
    let call = TieredCollectionArgs::new(
      SortedSetOperation::Zrange,
      (0, range_opts.bits() as i32),
      args,
      2,
    );
    let res = rt.block_on(exec_tiered_zset(&batch, key, &mut ctx, call, &mut output));
    assert_eq!(res, Ok(true));
    assert_eq!(
      &output[..prefix.len()],
      prefix,
      "前置流水线响应必须被完整保留，严禁调用 output.clear()"
    );
    assert_eq!(
      output,
      b"+PONG\r\n:1\r\n-ERR min or max not valid string range item\r\n"
    );
  }

  // 3. BYSCORE 与 BYLEX 并置场景：BYSCORE 写出后 BYLEX 语法失败，truncate 仅撤回本命令输出
  {
    let mut ctx = TieredCtx::new(&mut meta, &mut stub);
    let mut output = Vec::new();
    let prefix = b"+OK\r\n";
    output.extend_from_slice(prefix);

    let args: &[&[u8]] = &[b"0", b"5", b"BYSCORE", b"BYLEX"];
    let range_opts = SortedSetRangeOpts::BY_SCORE | SortedSetRangeOpts::BY_LEX;
    let call = TieredCollectionArgs::new(
      SortedSetOperation::Zrange,
      (0, range_opts.bits() as i32),
      args,
      2,
    );
    let res = rt.block_on(exec_tiered_zset(&batch, key, &mut ctx, call, &mut output));
    assert_eq!(res, Ok(true));
    assert_eq!(
      output, b"+OK\r\n-ERR min or max not valid string range item\r\n",
      "BYLEX 失败后应回滚 BYSCORE 输出，仅留存前置应答与错误帧"
    );
  }
}
