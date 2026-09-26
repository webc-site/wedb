//! 自研依据: 记录生命周期与版本链（C# 对应 test.recordops/RecordLifecycleTests.cs + LogRecordTests.cs）
use aok::{OK, Void};
use log::info;
use wrecord::{
  Error, HEADER_SIZE, RDH_WORD_OFFSET, RecordHeader, RecordMut, RecordRef, encode_to_slice,
  record_size, try_encode_to_vec,
};

/// 前驱地址单向链表结构与多版本历史回溯测试
/// 验证 RCU (Read-Copy-Update) 版本链模型：
/// - 同一 Key 随着多次写操作在日志 Tail 追加新记录，其 PreviousAddress 指向前驱旧版本的逻辑地址
/// - 创世第一版本（Genesis）其 PreviousAddress 为 0 (Constants.kInvalidEntry)
/// - 从最新版本顺着 PreviousAddress 单向链表依次反向回溯至最初版本
#[test]
fn test_predecessor_address_chain_backtracking() -> Void {
  info!("开始测试: 前驱地址单向链表结构与多版本历史回溯");

  const BASE_OFFSET: usize = 64;
  let mut log_pool = vec![0u8; BASE_OFFSET];
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

  // 依次追加 5 个版本形成反向版本链（从非零基础地址开始，0 保持为创世空前驱哨兵）
  for (idx, &val) in versions.iter().enumerate() {
    let offset = log_pool.len() as u64;
    record_offsets.push(offset);

    let prev_address = current_hash_bucket_address;
    let rec_len = record_size(key.len(), val.len());
    let start = log_pool.len();
    log_pool.resize(start + rec_len, 0);
    encode_to_slice(&mut log_pool[start..], prev_address, key, val, false, false)?;

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

  // 从最新版本顺着 prev_address 单向回溯完整历史链直至创世哨兵 0
  let mut trace_address = current_hash_bucket_address;
  let mut traced_values = Vec::new();
  let mut traced_steps = 0;

  while trace_address != 0 {
    let slice = &log_pool[trace_address as usize..];
    let rec_ref = RecordRef::from_slice(slice)?;

    assert_eq!(rec_ref.key(), key);
    traced_values.push(rec_ref.value());
    traced_steps += 1;

    trace_address = rec_ref.prev_address();
  }

  assert_eq!(traced_steps, 5);
  assert_eq!(traced_values.len(), 5);

  // 验证回溯序列严格为最新到最旧：[V5, V4, V3, V2, V1]
  let expected_descending_versions: Vec<&[u8]> = versions.iter().rev().copied().collect();
  assert_eq!(traced_values, expected_descending_versions);

  info!("前驱地址单向链表结构与多版本历史回溯测试通过");
  OK
}

/// 原位内存覆写值内容且保持前驱指针恒定测试
/// 验证可变区原位更新模型：
/// - Key 在记录生命周期中不可变
/// - 前驱指针 PreviousAddress 必须保持恒定，覆盖更新不可破坏链表拓扑
/// - 容量内更新原位覆写且不漂移物理占用；超出槽位容量的扩展实施防御拦截，保持原始数据完整
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

  // 第一次等长原位覆盖更新（等长为动态松弛路径零富余特例）
  let updated_val_1 = b"status=OK;temp=22.80;battery=97%";
  assert_eq!(updated_val_1.len(), initial_val.len());
  rec_mut.update_value_with_slack(updated_val_1)?;
  assert_eq!(rec_mut.value(), updated_val_1);
  assert_eq!(rec_mut.prev_address(), prev_address);
  assert_eq!(rec_mut.key(), key);
  assert!(rec_mut.is_modified());

  // 第二次等长原位覆盖更新
  let updated_val_2 = b"status=WN;temp=35.10;battery=96%";
  rec_mut.update_value_with_slack(updated_val_2)?;
  assert_eq!(rec_mut.value(), updated_val_2);
  assert_eq!(rec_mut.prev_address(), prev_address);

  // 通过 value_mut() 原位修改局部字节
  rec_mut.value_mut()[0..9].copy_from_slice(b"status=AL");
  assert_eq!(&rec_mut.value()[0..9], b"status=AL");
  assert_eq!(rec_mut.prev_address(), prev_address);

  const CURRENT_VAL: &[u8] = b"status=AL;temp=35.10;battery=96%";
  assert_eq!(rec_mut.value(), CURRENT_VAL);

  // 长度不等的定长原位语义由 can_update_in_place 查询单点承接（严格等长判定）
  let short_val = b"status=ERR";
  let long_val = b"status=OK;temp=21.50;battery=98%;extended_diagnostics=all_passed";
  assert!(!rec_mut.can_update_in_place(short_val.len()));
  assert!(!rec_mut.can_update_in_place(long_val.len()));

  // 超出物理槽位容量的扩展被 with_slack 拦截，原始数据保持完整
  assert_eq!(
    rec_mut.update_value_with_slack(long_val),
    Err(Error::ValueLengthMismatch {
      expected: total_record_size,
      actual: long_val.len(),
    })
  );
  assert_eq!(rec_mut.value(), CURRENT_VAL);
  assert_eq!(rec_mut.prev_address(), prev_address);

  // 视图归还底层缓冲后以 RecordRef::from_slice 重读验证一致性
  let rec_ref = RecordRef::from_slice(&buf)?;
  assert_eq!(rec_ref.total_size(), total_record_size);
  assert_eq!(rec_ref.prev_address(), prev_address);
  assert_eq!(rec_ref.key(), key);
  assert_eq!(rec_ref.value(), CURRENT_VAL);

  info!("原位内存覆写与前驱指针恒定性测试通过");
  OK
}

/// 快速连续墓碑翻转与深层版本链回溯测试
/// 验证高频事务下记录频繁删除、原位复活与深层版本追踪：
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
  const BASE_OFFSET: usize = 64;
  let mut log_pool = vec![0u8; BASE_OFFSET];
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
/// 验证以下行为:
/// - 尾部追加记录，其 prev_address 严格链接至旧只读地址
/// - 验证槽位复活原位定型不破坏物理边界与前驱指针
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
  let written = encode_to_slice(
    &mut tail_buffer,
    src_logical_address,
    key,
    v2_val,
    false,
    false,
  )?;
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
      &rec_mut.prev_address.to_le_bytes()
    );

    // 复活槽位：清除墓碑并原位更新为新值（前驱地址保持编码时取值）
    rec_mut.set_tombstone(false);
    let v3_val = b"order_state=DELIVERY_PACKED";
    rec_mut.update_value_with_slack(v3_val)?;

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

/// 墓碑记录普通原位更新防御测试（复活必须走显式复活路径）
#[test]
fn test_tombstone_update_rejected() -> Void {
  info!("开始测试: 墓碑记录普通原位更新拦截");

  // 1. 等长覆写在墓碑记录上被拦截
  let mut buf = try_encode_to_vec(0x66, b"tomb_key", b"val_10___", true)?;
  let mut rec_mut = RecordMut::from_slice_mut(&mut buf)?;
  assert!(rec_mut.is_tombstone());
  assert_eq!(
    rec_mut.update_value_with_slack(b"newval_10"),
    Err(Error::TombstoneUpdate)
  );
  // 被拦截的更新不得触碰底层值区
  assert_eq!(rec_mut.value(), b"val_10___");

  // 2. 缩短覆写同样拦截（与 can_update_with_slack 查询语义一致）
  assert!(!rec_mut.can_update_with_slack(4));
  assert_eq!(
    rec_mut.update_value_with_slack(b"val"),
    Err(Error::TombstoneUpdate)
  );

  // 3. 复活路径放行并单次覆写清墓碑
  rec_mut.revivify_with_slack(b"revived")?;
  assert!(!rec_mut.is_tombstone());
  assert_eq!(rec_mut.value(), b"revived");
  assert_eq!(rec_mut.val_len(), 7);
  // 复活后普通原位更新恢复可用（等长 7 字节）
  rec_mut.update_value_with_slack(b"back__7")?;
  assert_eq!(rec_mut.value(), b"back__7");

  info!("墓碑记录普通原位更新拦截测试通过");
  OK
}

/// 基于 FillerWords 与动态松弛的原位更新测试（LogRecord.TrySetPinnedValueSpan）
#[test]
fn test_record_mut_dynamic_slack_and_filler_words() -> Void {
  info!("开始测试: FillerWords 动态松弛全生命周期原位覆写与容量自洽");

  let key = b"session:user:1001";
  let initial_val = b"status=active;score=987654;role=admin;meta=verified_2026"; // 56 字节 (8 * 7)
  let prev_addr = 0x0000_1234_5678_0000_u64;

  let mut buf = try_encode_to_vec(prev_addr, key, initial_val, false)?;
  // 记录 8 字节对齐：16 + 17 + 56 = 89 → 对齐逻辑尺寸 96（隐式填充 7 字节）
  let initial_physical_size = buf.len();
  assert_eq!(initial_physical_size, 96);

  {
    let mut rec_mut = RecordMut::from_slice_mut(&mut buf)?;
    assert_eq!(rec_mut.val_len(), 56);
    assert_eq!(rec_mut.filler_words(), 0);
    // 容量含可复用隐式对齐填充：96 - 16 - 17 = 63
    assert_eq!(rec_mut.val_capacity(), 63);
    assert_eq!(rec_mut.physical_size(), initial_physical_size);

    // 1. 动态缩短：从 56 字节缩短至 24 字节（对齐(16+17+24)=64，腾出 32 字节 = 4 words filler）
    let short_val = b"status=idle;score=100000"; // 24 字节
    assert!(rec_mut.can_update_with_slack(short_val.len()));
    rec_mut.update_value_with_slack(short_val)?;

    assert_eq!(rec_mut.val_len(), 24);
    assert_eq!(rec_mut.filler_words(), 4); // 32 / 8 = 4
    assert_eq!(rec_mut.filler_bytes(), 32);
    assert_eq!(rec_mut.val_capacity(), 63); // 96 - 16 - 17 = 63 保持不变
    assert_eq!(rec_mut.physical_size(), initial_physical_size); // 物理占用大小绝对恒定
    assert_eq!(rec_mut.value(), short_val);

    // 用 as_ref() 零拷贝视图回读验证
    let rec_ref = rec_mut.as_ref();
    assert_eq!(rec_ref.value(), short_val);
    assert_eq!(rec_ref.val_len(), 24);
    assert_eq!(rec_ref.filler_words(), 4);
    assert_eq!(rec_ref.physical_size(), initial_physical_size);

    // 1.1 非词整数倍差值测试：更新为 25 字节（对齐(16+17+25)=64，
    // 隐式填充吸纳 6 字节差值，显式松弛 32 字节 = 4 words）
    let non_align_val = b"status=idle;score=100000_"; // 25 字节
    assert!(rec_mut.can_update_with_slack(non_align_val.len()));
    rec_mut.update_value_with_slack(non_align_val)?;
    assert_eq!(rec_mut.val_len(), 25);
    assert_eq!(rec_mut.filler_words(), 4); // 32 / 8 = 4
    assert_eq!(rec_mut.filler_bytes(), 32);
    assert_eq!(rec_mut.val_capacity(), 63);
    assert_eq!(rec_mut.physical_size(), initial_physical_size); // 物理占用绝对无任何漂移！
    assert_eq!(rec_mut.value(), non_align_val);

    // 2. 动态扩充：在松弛空间内扩充至 40 字节（对齐(16+17+40)=80，剩余 16 字节 = 2 words filler）
    let medium_val = b"status=active;score=20000;role=moderator"; // 40 字节 (8 * 5)
    assert!(rec_mut.can_update_with_slack(medium_val.len()));
    rec_mut.update_value_with_slack(medium_val)?;

    assert_eq!(rec_mut.val_len(), 40);
    assert_eq!(rec_mut.filler_words(), 2); // (96 - 80) / 8 = 2
    assert_eq!(rec_mut.filler_bytes(), 16);
    assert_eq!(rec_mut.val_capacity(), 63);
    assert_eq!(rec_mut.physical_size(), initial_physical_size);
    assert_eq!(rec_mut.value(), medium_val);

    // 2.1 墓碑化与单次覆写原子链内原地复活测试（revivify_with_slack）
    rec_mut.set_tombstone(true);
    assert!(rec_mut.is_tombstone());
    assert!(!rec_mut.can_update_with_slack(24)); // 墓碑状态下普通更新应被拦截
    rec_mut.revivify_with_slack(short_val)?;
    assert!(!rec_mut.is_tombstone());
    assert_eq!(rec_mut.val_len(), 24);
    assert_eq!(rec_mut.filler_bytes(), 32);
    assert_eq!(rec_mut.val_capacity(), 63);
    assert_eq!(rec_mut.physical_size(), initial_physical_size);
    assert_eq!(rec_mut.value(), short_val);

    // 3. 动态填满：恢复到 56 字节（对齐(16+17+56)=96，消耗完所有 filler 与隐式填充余量）
    rec_mut.update_value_with_slack(initial_val)?;
    assert_eq!(rec_mut.val_len(), 56);
    assert_eq!(rec_mut.filler_words(), 0);
    assert_eq!(rec_mut.val_capacity(), 63);
    assert_eq!(rec_mut.value(), initial_val);

    // 4. 超出容量（67 字节 > 63）必须被拦截
    let overflow_val = b"status=active;score=987654;role=admin;meta=verified_2026_exceed_cap"; // 67 字节
    assert!(!rec_mut.can_update_with_slack(overflow_val.len()));
    let err = rec_mut.update_value_with_slack(overflow_val);
    assert_eq!(
      err,
      Err(Error::ValueLengthMismatch {
        expected: 96,
        actual: 67,
      })
    );
  }

  // 最终从底层原始字节完全回读验证
  let final_ref = RecordRef::from_slice(&buf)?;
  assert_eq!(final_ref.value(), initial_val);
  assert_eq!(final_ref.val_len(), 56);
  assert_eq!(final_ref.filler_words(), 0);
  assert_eq!(final_ref.physical_size(), initial_physical_size);

  info!("FillerWords 动态松弛全生命周期原位覆写与容量自洽测试通过");
  OK
}

/// 零复制原位改长与全量原位覆写的帧字节对拍
///
/// 验证原位改长时的帧字节一致性：只改长度、旧值字节一律不动，新字节另落；
/// 本处的 `value_capacity_mut` 配合 `resize_val_with_slack` 实现了这一操作。
///
/// 证伪判据：同一槽位、同一逻辑终值，「原位改长臂」与「全量覆写臂
/// （[RecordMut::update_value_with_slack]）」的整帧字节必须逐字节相等（含 RDH
/// 原子字的 filler 与 val_len、RecordInfo 字的 MODIFIED 位），且槽位外的金丝雀
/// 字节与改长前的旧值区段绝不被侵蚀——改长若暗含整值搬迁或多写一处头，本对拍即红。
#[test]
fn test_resize_val_with_slack_frame_parity_with_full_overwrite() -> Void {
  info!("开始测试: 零复制原位改长与全量覆写的帧字节对拍");

  const CANARY_TAIL: usize = 64;
  let key = b"append:target";
  let long_val = vec![b'L'; 40];
  let base_val = b"1234567890"; // 缩写后腾出槽位松弛富余
  let extra = b"ABCDE";
  let prev_addr = 0x0000_0000_1122_3344_u64;
  let grown_val = [base_val.as_slice(), extra.as_slice()].concat();

  // 同一槽位的两个完全相同的起点：长值 40 落槽后缩写为 10
  let seed = || -> aok::Result<Vec<u8>> {
    let mut buf = try_encode_to_vec(prev_addr, key, &long_val, false)?;
    // 16 + 13 + 40 = 69 → 对齐 72；尾接金丝雀区探测任何越出槽位的写面
    let slot_size = buf.len();
    assert_eq!(slot_size, 72);
    buf.resize(slot_size + CANARY_TAIL, super::support::CANARY_BYTE);
    let mut rec = RecordMut::from_slice_mut(&mut buf[..slot_size])?;
    rec.update_value_with_slack(base_val)?;
    Ok(buf)
  };

  // 甲臂：原位改长（值字节由容量视图就地落笔，只发布新长度）
  let mut grow_buf = seed()?;
  {
    let slot = grow_buf.len() - CANARY_TAIL;
    let frame_before = grow_buf[..slot].to_vec();
    let mut rec = RecordMut::from_slice_mut(&mut grow_buf[..slot])?;
    assert_eq!(rec.val_len() as usize, 10);
    let cap = rec.value_capacity_mut();
    // 容量视图 = 槽位 72 - 头 16 - 键 13，且前 10 字节即逻辑旧值
    assert_eq!(cap.len(), 43);
    assert_eq!(&cap[..10], base_val, "旧数据必须原封不动可见");
    cap[10..grown_val.len()].copy_from_slice(extra);
    rec.resize_val_with_slack(grown_val.len())?;
    // RecordInfo 字（含 MODIFIED 位与前驱地址）一字未动；键区与旧值区未被搬迁
    assert_eq!(&frame_before[..8], &grow_buf[..8], "RecordInfo 字不得改动");
    assert_eq!(&frame_before[16..29], &grow_buf[16..29], "键区不得被改写");
    assert_eq!(
      &frame_before[29..39],
      &grow_buf[29..39],
      "旧值区段不得被搬迁"
    );
  }
  super::support::assert_canary_intact(&grow_buf, 72);

  // 乙臂：全量原位覆写同一逻辑终值
  let mut full_buf = seed()?;
  {
    let slot = full_buf.len() - CANARY_TAIL;
    let mut rec = RecordMut::from_slice_mut(&mut full_buf[..slot])?;
    rec.update_value_with_slack(&grown_val)?;
  }

  assert_eq!(
    grow_buf, full_buf,
    "两臂同终值须落逐字节相同的帧（长度发布只此一处）"
  );

  // 终态头字段对拍：物理占用恒定、松弛重排、前驱与键不动
  let grown = RecordRef::from_slice(&grow_buf)?;
  assert_eq!(grown.value(), grown_val);
  assert_eq!(grown.val_len() as usize, 15);
  assert_eq!(grown.physical_size(), 72);
  assert_eq!(grown.filler_bytes(), 24, "对齐(16+13+15)=48 → 富余 24 字节");
  assert_eq!(grown.prev_address(), prev_addr);
  assert_eq!(grown.key(), key);
  assert!(grown.is_modified());

  // 边界：等长改长为幂等发布（0 长度 APPEND 的引擎侧特例），帧字节一字不动
  let frame_after = grow_buf[..72].to_vec();
  {
    let mut rec = RecordMut::from_slice_mut(&mut grow_buf[..72])?;
    rec.resize_val_with_slack(grown_val.len())?;
    assert_eq!(rec.value(), grown_val);
  }
  assert_eq!(
    grow_buf[..72],
    frame_after[..],
    "等长发布不得改动任何帧字节"
  );
  super::support::assert_canary_intact(&grow_buf, 72);

  // 边界：超容量与墓碑一律拒绝且不落半成品
  {
    let mut rec = RecordMut::from_slice_mut(&mut grow_buf[..72])?;
    let err = rec.resize_val_with_slack(44);
    assert_eq!(
      err,
      Err(Error::ValueLengthMismatch {
        expected: 72,
        actual: 44,
      })
    );
    assert_eq!(rec.value(), grown_val, "被拒的改长不得改写逻辑值");
  }
  assert_eq!(
    &grow_buf[..72],
    &frame_after[..],
    "被拒的改长不得改动任何帧字节"
  );
  {
    let mut rec = RecordMut::from_slice_mut(&mut grow_buf[..72])?;
    rec.set_tombstone(true);
    assert_eq!(
      rec.resize_val_with_slack(20),
      Err(Error::TombstoneUpdate),
      "墓碑记录改长须走显式复活口"
    );
  }
  // 墓碑位只落 RecordInfo 字，长度与载荷区一字未动
  assert_eq!(
    &grow_buf[8..72],
    &frame_after[8..],
    "墓碑拒绝路径不得改动 RDH 字与载荷"
  );
  super::support::assert_canary_intact(&grow_buf, 72);

  info!("零复制原位改长与全量覆写的帧字节对拍测试通过");
  OK
}

/// 原位增长前后的帧字节差分与「整帧重发到尾部」对照臂（最小写证据）
///
/// 验证原位增长改长时的字节差分证据（原位改长 + 只拷新字节、旧数据
/// 一律不动）并与整帧另落一条新记录的行为进行对照。两条臂的物理写面
/// 差数倍，本用例把「改长前后同一槽位逐字节差分」钉成断言：
///
/// - 甲臂（原位）被改动的字节**只可能**落在两处窗口——RDH 原子字
///   `[RDH_WORD_OFFSET, HEADER_SIZE)`（filler 与 val_len 同字单次发布）与新增段
///   `[key_end + old_len, key_end + new_len)`；RecordInfo 字（前驱地址与 MODIFIED 等
///   标志位）、键区、旧值区、槽位尾部未发布的松弛富余四段必须逐字节不动；
/// - 乙臂（对照＝本票之前的旧实现）按整帧重发装配：原槽位改动 0 字节、同一地址读回
///   仍是改长前的旧值，新槽位被写满 48 字节整帧；
/// - 丙臂（反证＝判据 3 禁止的第二处长度发布口）只改 val_len 不重排 filler，其帧与
///   甲臂逐字节不等价且头部自洽性当场崩塌（解码越出槽位）。
///
/// 三臂合起来的证伪力：原位臂若悄悄退化成整帧重写，甲臂的「同址读回新值」必红；
/// 若改长暗含整值搬迁或多写一处头，甲臂的差分窗口断言必红；若长度旁生第二发布口，
/// 丙臂与甲臂的对拍必红。
#[test]
fn test_in_place_grow_frame_byte_diff_vs_whole_record_rewrite() -> Void {
  info!("开始测试: 原位增长帧字节差分与整帧重发/第二发布口对照");

  const KEY: &[u8] = b"append:target";
  const KEY_LEN: usize = KEY.len(); // 13 字节键
  let key = KEY;
  let long_val = vec![b'L'; 40];
  let base_val = b"1234567890"; // 原位缩写后的 10 字节旧值（腾空转松弛填充）
  let extra = b"ABCDE"; // APPEND 的 5 字节新增段
  let prev_addr = 0x0000_0000_77aa_55cc_u64;
  let grown_val = [base_val.as_slice(), extra.as_slice()].concat();

  const SLOT: usize = 72; // 16 + 13 + 40 = 69 → 对齐 72
  const KEY_END: usize = HEADER_SIZE + KEY_LEN; // 键区右界 = 值区起点
  let new_lo = KEY_END + base_val.len(); // 新增段左界 39
  let new_hi = KEY_END + grown_val.len(); // 新增段右界 44

  // 两臂共用的同一槽位起点（长值落槽后原位缩写，MODIFIED 已在起点置位）
  let seed = || -> aok::Result<Vec<u8>> {
    let mut buf = try_encode_to_vec(prev_addr, key, &long_val, false)?;
    assert_eq!(buf.len(), SLOT);
    let mut rec = RecordMut::from_slice_mut(&mut buf)?;
    rec.update_value_with_slack(base_val)?;
    Ok(buf)
  };

  // ---------- 甲臂：原位增长（只落新字节 + 单次 RDH 发布）----------
  let mut buf = seed()?;
  let frame0 = buf.clone();
  // 「旧数据零复制、零中间 Vec」的机器证据：整段原位增长的堆分配增量为 0
  let allocs_before = super::support::alloc_probe::allocs();
  {
    let mut rec = RecordMut::from_slice_mut(&mut buf)?;
    let cap = rec.value_capacity_mut();
    assert_eq!(&cap[..base_val.len()], base_val, "旧数据必须原封不动可见");
    cap[base_val.len()..grown_val.len()].copy_from_slice(extra);
    rec.resize_val_with_slack(grown_val.len())?;
  }
  let grow_allocs = super::support::alloc_probe::allocs() - allocs_before;
  assert_eq!(
    grow_allocs, 0,
    "原位增长臂须零堆分配——旧实现的整值物化至少要付一次"
  );

  let changed: Vec<usize> = (0..SLOT).filter(|&i| frame0[i] != buf[i]).collect();
  // 最小写面的精确个数：RDH 字内 filler 词数与 val_len 各 1 字节 + 新增段 5 字节
  assert_eq!(
    changed.len(),
    2 + extra.len(),
    "原位增长的全帧改动面须恰为 {changed:?}"
  );
  // 判据 1：改动字节全部落在「RDH 原子字 ∪ 新增段」两段窗口内，无一逸出
  for &i in &changed {
    assert!(
      (RDH_WORD_OFFSET..HEADER_SIZE).contains(&i) || (new_lo..new_hi).contains(&i),
      "原位增长改动了不该动的字节 {i}，全部改动集 {changed:?}"
    );
  }
  // 判据 2：RecordInfo 字 / 键区 / 旧值区 / 未发布富余四段逐字节不动
  assert_eq!(
    &frame0[..RDH_WORD_OFFSET],
    &buf[..RDH_WORD_OFFSET],
    "RecordInfo 字（前驱地址与标志位）不得改动"
  );
  assert_eq!(
    &frame0[HEADER_SIZE..KEY_END],
    &buf[HEADER_SIZE..KEY_END],
    "键区不得被改写"
  );
  assert_eq!(
    &frame0[KEY_END..new_lo],
    &buf[KEY_END..new_lo],
    "旧值区段不得被搬迁——原位臂的全部意义"
  );
  assert_eq!(
    &frame0[new_hi..SLOT],
    &buf[new_hi..SLOT],
    "新长度之外尚未发布的松弛富余不得被触碰"
  );
  // 判据 3：新增段落笔完整，且长度与松弛经同一 RDH 字自洽发布
  assert_eq!(&buf[new_lo..new_hi], extra, "新增段 5 字节须一次落齐");
  let grown = RecordRef::from_slice(&buf)?;
  assert_eq!(grown.value(), grown_val);
  assert_eq!(grown.physical_size(), SLOT, "原位增长不得改动槽位物理尺寸");
  assert_eq!(
    grown.filler_bytes(),
    SLOT - record_size(key.len(), grown_val.len()),
    "RDH 字内 filler 须与新 val_len 同步重排"
  );

  // ---------- 乙臂：整帧重发到尾部（本票之前的旧实现形态）----------
  let mut legacy = seed()?;
  let legacy_before = legacy[..SLOT].to_vec();
  legacy.resize(SLOT * 2, super::support::CANARY_BYTE);
  // 同一探针窗口量旧实现形态的分配增量，作为零分配断言的非空转对照
  let allocs_before = super::support::alloc_probe::allocs();
  let reemit = {
    let old = RecordRef::from_slice(&legacy[..SLOT])?;
    let whole = [old.value(), extra.as_slice()].concat();
    let frame = try_encode_to_vec(prev_addr, key, &whole, false)?;
    legacy[SLOT..SLOT + frame.len()].copy_from_slice(&frame);
    frame
  };
  let reemit_allocs = super::support::alloc_probe::allocs() - allocs_before;
  assert_eq!(reemit.len(), 48, "16 + 13 + 15 = 44 → 对齐 48 的独立整帧");
  assert!(
    grow_allocs < reemit_allocs,
    "原位臂零分配、旧实现形态至少 {reemit_allocs} 次——探针非空转"
  );

  assert_eq!(
    &legacy[..SLOT],
    &legacy_before[..],
    "整帧重发一字节也不碰原槽位（与甲臂的写面互斥）"
  );
  assert_eq!(
    RecordRef::from_slice(&legacy[..SLOT])?.value(),
    base_val,
    "整帧重发下原地址读不到新值：甲臂的「同址读回新全值」断言对旧实现必红"
  );
  let reemit_changed = (0..reemit.len())
    .filter(|&i| legacy[SLOT + i] != super::support::CANARY_BYTE)
    .count();
  assert_eq!(
    reemit_changed,
    reemit.len(),
    "整帧重发把 16 头 + 13 键 + 15 值 + 4 填充共 48 字节全部重写"
  );
  assert!(
    changed.len() * 6 <= reemit_changed,
    "原位写面须显著小于整帧重发：原位 {changed:?}（{} 字节）vs 重发 {reemit_changed} 字节",
    changed.len()
  );

  // ---------- 丙臂：第二处长度发布口（只改 val_len、不重排 filler）----------
  // 判据 3 明令禁止的旁生写口，产物与甲臂逐字节不等价且布局自相矛盾
  let mut rogue = frame0.clone();
  {
    let mut hdr = RecordHeader::from_slice(&rogue)?;
    hdr.set_val_len(grown_val.len() as u32);
    rogue[..HEADER_SIZE].copy_from_slice(&hdr.to_bytes());
  }
  assert_ne!(
    &rogue[..SLOT],
    &buf[..SLOT],
    "裸改长度与原位臂的帧必须不等价"
  );
  // 陈旧 filler（改长前的 4 词 = 32 字节）配新 val_len：物理尺寸虚涨为 48 + 32 = 80，
  // 越出 72 字节槽位 ⇒ 页扫描与后继记录解码当场错位
  let rogue_rec = RecordRef::from_slice(&rogue[..SLOT])?;
  assert_eq!(
    rogue_rec.physical_size(),
    record_size(key.len(), grown_val.len()) + (SLOT - record_size(key.len(), base_val.len())),
    "旁生第二处长度发布口必留 filler 与 val_len 不同步的破损布局"
  );
  assert!(
    rogue_rec.physical_size() > SLOT,
    "破损布局虚报物理尺寸、越出槽位；原位臂经单点发布内核不可能如此"
  );
  super::support::assert_canary_intact(&legacy, SLOT + reemit.len());

  info!(
    "原位增长帧字节差分对照测试通过: 原位改动 {} 字节 {changed:?} / 整帧重发改动 {reemit_changed} 字节、原槽位改动 0 字节",
    changed.len()
  );
  OK
}
