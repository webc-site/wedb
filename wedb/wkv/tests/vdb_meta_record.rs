//! DbMeta 记录编解码与布局集成测试
//!
//! 覆盖：DbMeta 布局固化、六变体 encode→decode 往返一致、点查键与写侧键同字节、
//! 长度差一字节拒绝、未知子类型拒绝、墓碑注销提取与前缀错位防误读。

use wkv::vdb::DbMetaRecord;

/// DbMeta 布局固化：六变体 encode→decode 往返一致、点查键与写侧键同字节、
/// 长度差一字节必失败、未知子类型必失败、墓碑注销口径（载荷末 8 字节）
#[test]
fn test_dbmeta_record_roundtrip() {
  let records = [
    DbMetaRecord::NsMap {
      logic_ns: 7,
      vns: 3,
    },
    DbMetaRecord::DbMap {
      vns: 5,
      logic_db: 2,
      vdb: 9,
    },
    DbMetaRecord::GcDeadNs {
      expired_at: -123_456_789,
      old_vns: 11,
      tail_address: u64::MAX,
    },
    DbMetaRecord::GcDeadDb {
      expired_at: 1_000_000_000,
      vns: 12,
      old_vdb: 13,
      tail_address: 42,
    },
    DbMetaRecord::NextId {
      next_virtual_id: 1_099_511_627_775,
    },
    DbMetaRecord::DbSwap {
      vns: 5,
      logic_db1: 2,
      logic_db2: 8,
      swapped_db1: 14,
      swapped_db2: 9,
    },
  ];
  let key_lens = [9usize, 17, 17, 25, 16, 25];
  for (rec, klen) in records.iter().zip(key_lens) {
    let key = rec.key();
    assert_eq!(key.as_slice().len(), klen, "键载荷长度固化于单点");
    let back = DbMetaRecord::decode(key.as_slice(), rec.value().as_slice())
      .expect("encode→decode 往返必复原（含负 expired_at 与 u64::MAX）");
    assert_eq!(rec, &back);
    if rec.value().as_slice().len() == 8 {
      assert_eq!(
        DbMetaRecord::decode_value(rec.value().as_slice()),
        Some(u64::from_be_bytes(
          <[u8; 8]>::try_from(rec.value().as_slice()).unwrap()
        ))
      );
    }
  }
  // 点查探测键与写侧键同字节：NS_MAP/DB_MAP 键与 vns/vdb 值侧字段无关；
  // 0x06 值侧两条新指向不进键
  assert_eq!(
    DbMetaRecord::key_ns_map(7).as_slice(),
    records[0].key().as_slice()
  );
  assert_eq!(
    DbMetaRecord::key_db_map(5, 2).as_slice(),
    records[1].key().as_slice()
  );
  // 长度差一字节必失败（截短与多尾各验一轮）
  for rec in &records {
    let key = rec.key();
    let k = key.as_slice();
    assert!(DbMetaRecord::decode(&k[..k.len() - 1], rec.value().as_slice()).is_none());
    let mut longer = k.to_vec();
    longer.push(0);
    assert!(DbMetaRecord::decode(&longer, rec.value().as_slice()).is_none());
    // 记录值非定长（8B 族截为 7B / 0x06 截为 15B）必失败
    assert!(
      DbMetaRecord::decode(
        k,
        &rec.value().as_slice()[..rec.value().as_slice().len() - 1]
      )
      .is_none()
    );
  }
  // 未知子类型必失败
  let bogus = [0x09u8; 9];
  assert!(DbMetaRecord::decode(&bogus, &[0u8; 8]).is_none());
  assert_eq!(DbMetaRecord::decode_value(&bogus), None);
  // 0x05 固定标记不符必失败（任意 16 字节载荷不误读为水位）
  let mut fake_marker = [0u8; 16];
  fake_marker[0] = 0x05;
  assert!(DbMetaRecord::decode(&fake_marker, &[0u8; 8]).is_none());
  // 0x06 键不得混读为 GcDeadDb（同为 25B 但子类型分派）
  assert!(
    DbMetaRecord::decode(
      records[5].key().as_slice(),
      &records[3].value().as_slice()[..8]
    )
    .is_none()
  );
  // 墓碑注销：GC 变体载荷末 8 字节即死亡 vid，映射变体不判死
  assert_eq!(
    DbMetaRecord::dead_vid_of(records[2].key().as_slice()),
    Some(11)
  );
  assert_eq!(
    DbMetaRecord::dead_vid_of(records[3].key().as_slice()),
    Some(13)
  );
  assert!(DbMetaRecord::dead_vid_of(records[0].key().as_slice()).is_none());
  assert!(DbMetaRecord::dead_vid_of(records[1].key().as_slice()).is_none());
  assert!(DbMetaRecord::dead_vid_of(&[]).is_none());
  // 前缀错位不误读：真 GC_DEAD_NS 键整体右移一字节仍为 17B，子类型签名
  // 已不在 offset 0，末 8 字节却是位移后的实况字节——只按长度放行的实现
  // 必从此处读出假死亡号并把活库判死
  let shifted = {
    let dead_key = records[2].key();
    let mut b = [0u8; DbMetaRecord::KEY_GC_DEAD_NS_LEN];
    b[1..].copy_from_slice(&dead_key.as_slice()[..DbMetaRecord::KEY_GC_DEAD_NS_LEN - 1]);
    b
  };
  assert!(DbMetaRecord::dead_vid_of(&shifted).is_none());
}
