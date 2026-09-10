use aok::{OK, Void};
use log::info;
use wrecord::{Error, RecordMut, RecordRef, encode_to_slice, record_size, try_encode_to_vec};

/// 前驱地址单向链表结构与多版本历史回溯测试
/// 对标 C# Tsavorite RecordLifecycleTests.cs / HybridLog RCU (Read-Copy-Update) 版本链模型：
/// - 同一 Key 随着多次写操作在日志 Tail 追加新记录，其 PreviousAddress 指向前驱旧版本的逻辑地址
/// - 创世第一版本（Genesis）其 PreviousAddress 为 0 (Constants.kInvalidEntry)
/// - 从最新版本顺着 PreviousAddress 单向链表依次反向回溯至最初版本
#[test]
fn test_predecessor_address_chain_backtracking() -> Void {
  info!("开始测试: 前驱地址单向链表结构与多版本历史回溯");

  let mut log_pool = Vec::<u8>::new();
  let key = b"user:order:1001";
  let versions: [&[u8]; 5] = [
    b"{\"status\":\"created\",\"amount\":100}",
    b"{\"status\":\"paid\",\"amount\":100}",
    b"{\"status\":\"shipped\",\"amount\":100}",
    b"{\"status\":\"delivered\",\"amount\":100}",
    b"{\"status\":\"completed\",\"amount\":100}",
  ];

  let mut current_hash_bucket_address: u64 = 0;
  let mut record_offsets = Vec::with_capacity(versions.len());

  // 依次追加 5 个版本形成反向版本链（原位追加零临时堆分配）
  for (idx, &val) in versions.iter().enumerate() {
    let offset = log_pool.len() as u64;
    record_offsets.push(offset);

    let prev_address = current_hash_bucket_address;
    let rec_len = record_size(key.len(), val.len());
    let start = log_pool.len();
    log_pool.resize(start + rec_len, 0);
    encode_to_slice(&mut log_pool[start..], prev_address, key, val, false)?;

    current_hash_bucket_address = offset;

    info!(
      "追加版本 V{}: offset={:#x}, prev_address={:#x}, val={}",
      idx + 1,
      offset,
      prev_address,
      String::from_utf8_lossy(val)
    );
  }

  assert_eq!(record_offsets.len(), 5);

  // 从最新版本顺着 prev_address 逐级反向回溯
  let mut trace_address = current_hash_bucket_address;
  let mut traced_values = Vec::new();
  let mut traced_steps = 0;

  while trace_address != 0 {
    let slice = &log_pool[trace_address as usize..];
    let rec_ref = RecordRef::from_slice(slice)?;

    assert_eq!(rec_ref.key(), key);
    traced_values.push(rec_ref.value());
    traced_steps += 1;

    let next_trace = rec_ref.prev_address();
    trace_address = next_trace;
  }

  // 检查首个创世版本 (offset 0)
  let genesis_slice = &log_pool[0..];
  let genesis_ref = RecordRef::from_slice(genesis_slice)?;
  assert_eq!(genesis_ref.key(), key);
  assert_eq!(genesis_ref.value(), versions[0]);
  assert_eq!(genesis_ref.prev_address(), 0);
  traced_values.push(genesis_ref.value());
  traced_steps += 1;

  assert_eq!(traced_steps, 5);
  assert_eq!(traced_values.len(), 5);

  // 验证回溯序列严格为最新到最旧：[V5, V4, V3, V2, V1]
  let expected_descending_versions: Vec<&[u8]> = versions.iter().rev().copied().collect();
  assert_eq!(traced_values, expected_descending_versions);

  info!("前驱地址单向链表结构与多版本历史回溯测试通过");
  OK
}

/// 原位内存覆写值内容且保持前驱指针恒定测试
/// 对标 C# Tsavorite RecordLifecycleTests.cs / 可变区原位更新模型：
/// - Key 在记录生命周期中不可变
/// - 前驱指针 PreviousAddress 必须保持恒定，覆盖更新不可破坏链表拓扑
/// - 等长更新直接原位覆盖；长度不匹配时实施防御拦截，保持原始数据完整
#[test]
fn test_in_place_value_update_preserves_predecessor() -> Void {
  info!("开始测试: 原位内存覆写与前驱指针恒定性");

  let key = b"sensor:temperature:node_42";
  let initial_val = b"status=OK;temp=21.50;battery=98%";
  let prev_address = 0x0000_3344_5566_7788_u64;

  let mut buf = try_encode_to_vec(prev_address, key, initial_val, false)?;
  let total_record_size = buf.len();

  let mut rec_mut = RecordMut::from_slice_mut(&mut buf)?;
  assert_eq!(rec_mut.key(), key);
  assert_eq!(rec_mut.value(), initial_val);
  assert_eq!(rec_mut.prev_address(), prev_address);
  assert!(!rec_mut.is_tombstone());

  // 第一次等长原位覆盖更新
  let updated_val_1 = b"status=OK;temp=22.80;battery=97%";
  assert_eq!(updated_val_1.len(), initial_val.len());
  rec_mut.update_value_in_place(updated_val_1)?;
  assert_eq!(rec_mut.value(), updated_val_1);
  assert_eq!(rec_mut.prev_address(), prev_address);
  assert_eq!(rec_mut.key(), key);

  // 第二次等长原位覆盖更新
  let updated_val_2 = b"status=WN;temp=35.10;battery=96%";
  rec_mut.update_value_in_place(updated_val_2)?;
  assert_eq!(rec_mut.value(), updated_val_2);
  assert_eq!(rec_mut.prev_address(), prev_address);

  // 通过 value_mut() 原位修改局部字节
  rec_mut.value_mut()[0..9].copy_from_slice(b"status=AL");
  assert_eq!(&rec_mut.value()[0..9], b"status=AL");
  assert_eq!(rec_mut.prev_address(), prev_address);

  const CURRENT_VAL: &[u8] = b"status=AL;temp=35.10;battery=96%";
  assert_eq!(rec_mut.value(), CURRENT_VAL);

  // 长度较短时的防御拦截
  let short_val = b"status=ERR";
  let err_short = rec_mut.update_value_in_place(short_val);
  assert_eq!(
    err_short,
    Err(Error::ValueLengthMismatch {
      expected: initial_val.len(),
      actual: short_val.len(),
    })
  );
  assert_eq!(rec_mut.value(), CURRENT_VAL);
  assert_eq!(rec_mut.prev_address(), prev_address);

  // 长度较长时的防御拦截
  let long_val = b"status=OK;temp=21.50;battery=98%;extended_diagnostics=all_passed";
  let err_long = rec_mut.update_value_in_place(long_val);
  assert_eq!(
    err_long,
    Err(Error::ValueLengthMismatch {
      expected: initial_val.len(),
      actual: long_val.len(),
    })
  );
  assert_eq!(rec_mut.value(), CURRENT_VAL);
  assert_eq!(rec_mut.prev_address(), prev_address);

  // 转换为 RecordRef 验证一致性
  let rec_ref = rec_mut.into_ref();
  assert_eq!(rec_ref.total_size(), total_record_size);
  assert_eq!(rec_ref.prev_address(), prev_address);
  assert_eq!(rec_ref.key(), key);
  assert_eq!(rec_ref.value(), CURRENT_VAL);

  info!("原位内存覆写与前驱指针恒定性测试通过");
  OK
}

/// 快速连续墓碑翻转与深层版本链回溯测试
/// 对标 C# Tsavorite RecordLifecycleTests.cs 高频事务下记录频繁删除、原位复活与深层版本追踪：
/// - 快速连续 100 次原位翻转墓碑标记，确认位操作绝对不破坏低 48 位前驱逻辑地址
/// - 深度为 50 的版本链回溯（交替包含活跃记录与墓碑删除），从最新 Tail 完整反向回溯至 Genesis
#[test]
fn test_rapid_tombstone_flip_and_deep_chain_traversal() -> Void {
  info!("开始测试: 快速连续墓碑翻转与深层版本链回溯");

  let key = b"tombstone_stress_key";
  let val = b"stress_payload_v1";
  let target_addr = 0x0000_dead_beef_cafe_u64;

  let mut buf = try_encode_to_vec(target_addr, key, val, false)?;

  // 1. 快速连续 100 次墓碑翻转
  {
    let mut rec_mut = RecordMut::from_slice_mut(&mut buf)?;
    for i in 0..100 {
      let should_tombstone = i % 2 == 0;
      rec_mut.set_tombstone(should_tombstone);
      assert_eq!(rec_mut.is_tombstone(), should_tombstone);
      assert_eq!(rec_mut.prev_address(), target_addr);

      let view = rec_mut.as_ref();
      assert_eq!(view.is_tombstone(), should_tombstone);
      assert_eq!(view.prev_address(), target_addr);
    }
  }

  // 2. 深度为 50 的版本链回溯（交替包含有效更新与墓碑删除）
  let mut log_pool = Vec::<u8>::new();
  let mut head_addr: u64 = 0;
  const CHAIN_DEPTH: usize = 50;
  let mut expected_history = Vec::new();

  for ver in 0..CHAIN_DEPTH {
    let offset = log_pool.len() as u64;
    let is_deleted = ver % 5 == 3;
    let v_str = format!("version_content_{ver:04}");
    let v_bytes = if is_deleted {
      b"".as_slice()
    } else {
      v_str.as_bytes()
    };

    let encoded = try_encode_to_vec(head_addr, key, v_bytes, is_deleted)?;
    log_pool.extend_from_slice(&encoded);

    expected_history.push((offset, is_deleted, v_bytes.to_vec()));
    head_addr = offset;
  }

  // 从链尾逐级反向回溯至创世块
  let mut curr = head_addr;
  let mut backward_steps = 0;

  while backward_steps < CHAIN_DEPTH {
    let slice = &log_pool[curr as usize..];
    let r = RecordRef::from_slice(slice)?;

    let expected_idx = CHAIN_DEPTH - 1 - backward_steps;
    let (exp_offset, exp_tomb, ref exp_val) = expected_history[expected_idx];

    assert_eq!(curr, exp_offset);
    assert_eq!(r.is_tombstone(), exp_tomb);
    assert_eq!(r.value(), exp_val.as_slice());
    assert_eq!(r.key(), key);

    curr = r.prev_address();
    backward_steps += 1;
  }

  assert_eq!(backward_steps, CHAIN_DEPTH);
  assert_eq!(curr, 0);

  info!("快速连续墓碑翻转与深层版本链回溯测试通过");
  OK
}

/// 尾部复制 (Copy-to-tail) 与槽位复活 (Revivification) 定型测试
/// 对标 C# Tsavorite:
/// - RecordTriggersExtTests.cs: PostCopyToTail(srcAddr, dstAddr) 在 Tail 分配新记录，其 prev_address 严格链接至旧只读地址
/// - LogRecordTests.cs: RevivificationPreparationPreservesLiveFraming 验证槽位复活原位定型不破坏物理边界与前驱指针
#[test]
fn test_post_copy_to_tail_and_slot_revivification() -> Void {
  info!("开始测试: 尾部复制与槽位复活定型");

  let key = b"session:order_id:999";
  let v1_val = b"order_state=PENDING_PAYMENT";
  let src_logical_address: u64 = 0x0000_0000_0001_0000;

  // 1. 只读区源记录
  let src_encoded = try_encode_to_vec(0, key, v1_val, false)?;
  let src_ref = RecordRef::from_slice(&src_encoded)?;
  assert_eq!(src_ref.key_len(), key.len() as u32);
  assert_eq!(src_ref.val_len(), v1_val.len() as u32);

  // 2. 模拟 PostCopyToTail: 在 Tail 地址追加新记录，链接至源地址
  let v2_val = b"order_state=PAYMENT_SETTLED";
  let mut tail_buffer = vec![0u8; 256];
  let written = encode_to_slice(&mut tail_buffer, src_logical_address, key, v2_val, false)?;
  assert_eq!(written, record_size(key.len(), v2_val.len()));

  let tail_rec = RecordRef::from_slice(&tail_buffer[..written])?;
  assert_eq!(tail_rec.prev_address(), src_logical_address);
  assert_eq!(tail_rec.key(), key);
  assert_eq!(tail_rec.value(), v2_val);
  assert_eq!(tail_rec.key_len(), key.len() as u32);
  assert_eq!(tail_rec.val_len(), v2_val.len() as u32);

  // 3. 模拟槽位逻辑删除后复活 (Revivification)
  let mut slot_buf = tail_buffer[..written].to_vec();
  {
    let mut rec_mut = RecordMut::from_slice_mut(&mut slot_buf)?;
    rec_mut.set_tombstone(true);
    assert!(rec_mut.is_tombstone());
    assert_eq!(rec_mut.prev_address(), src_logical_address);

    assert_eq!(rec_mut.as_slice().len(), written);
    assert_eq!(
      &rec_mut.as_slice()[0..8],
      &rec_mut.header().prev_address.to_le_bytes()
    );

    // 复活槽位：清除墓碑并原位更新为新值（前驱地址保持编码时取值）
    rec_mut.set_tombstone(false);
    let v3_val = b"order_state=DELIVERY_PACKED";
    rec_mut.update_value_in_place(v3_val)?;

    assert_eq!(rec_mut.key_len(), key.len() as u32);
    assert_eq!(rec_mut.val_len(), v3_val.len() as u32);
    assert_eq!(rec_mut.value(), v3_val);
    assert_eq!(rec_mut.prev_address(), src_logical_address);
    assert!(!rec_mut.is_tombstone());
  }

  let revivified_ref = RecordRef::from_slice(&slot_buf)?;
  assert_eq!(revivified_ref.prev_address(), src_logical_address);
  assert_eq!(revivified_ref.key(), key);
  assert_eq!(revivified_ref.value(), b"order_state=DELIVERY_PACKED");
  assert!(!revivified_ref.is_tombstone());
  assert_eq!(revivified_ref.total_size(), written);

  info!("尾部复制与槽位复活定型测试通过");
  OK
}
