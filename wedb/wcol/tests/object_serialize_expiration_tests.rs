//! 集合对象序列化的成员级过期快照测试（纯集合层，不牵引擎）
//!
//! 从原 wkv/tests/garnet_object_tests.rs 的混合用例拆出：对象自身序列化只消费
//! wcol/wresp/wbase，不应挂引擎测试装配（对标 C# 顶层集合测试工程
//! test/standalone/Garnet.test.collections 的序列化族，rust 按被测层归位到 wcol）。
//!
//! 刻意差异：C# 的 BuildWithExpiredMember（GarnetObjectTests.cs:229）先以未来刻度
//! 经读取构造函数造出存活成员、再轮询等 ticks 过去（慢机则窗口翻倍重试）——不能走
//! SetExpiration 设过去刻度，两侧 SetExpiration 对过去刻度一律 Remove + KeyAlreadyExpired
//! （libs/server/Objects/Hash/HashObject.cs:561-565）。rust 侧改用过期结构装载单点
//! insert_expiration（无过期判定，SetExpiration 的删除分支不触发）直挂过去刻度，
//! 与 C# 等待后的对象状态等效，免真实等待导致的慢机 flaky。

use std::io::Cursor;

use wbase::time::now_ticks;
use wcol::{HashObject, SortedSetObject};
use wresp::options::ExpireOption;

/// 存活与已过期成员各一的字段级过期刻度（存活取远未来，过期取过去）
const LIVE_SPAN: i64 = 1_000_000_000;
const EXPIRED_SPAN: i64 = 1_000_000;

/// test/standalone/Garnet.test.collections/GarnetObjectTests.cs:SerializeDoesNotMutateHashWithExpiredFields
#[test]
fn serialize_does_not_mutate_hash_with_expired_fields() {
  let now = now_ticks();
  let mut hash = HashObject::new();
  hash.hash.insert(b"field_live".to_vec(), b"val1".to_vec());
  hash
    .hash
    .insert(b"field_expired".to_vec(), b"val2".to_vec());
  hash.set_expiration(b"field_live", now + LIVE_SPAN, ExpireOption::NONE);
  // 过期成员经装载单点直挂（C# SetExpiration 对过去刻度直接删除条目，
  // HashObject.cs:561-565，故不能走 set_expiration 造过期态）
  hash.insert_expiration(b"field_expired".to_vec(), now - EXPIRED_SPAN);

  // 序列化在 &self 上进行，不得就地剔除过期字段（C# 同注释：落盘期读者并发访问同一实例，
  // 序列化必须是纯读，否则与读者竞态并损坏集合）
  let mut buf = Vec::new();
  hash.serialize(&mut buf).unwrap();
  assert_eq!(hash.hash.len(), 2, "序列化不得删除过期字段");
  assert!(hash.has_expirable_items(), "序列化不得拆除过期结构");
  assert!(
    hash
      .expiration_times
      .as_ref()
      .is_some_and(|t| t.contains_key(b"field_expired".as_slice())),
    "序列化不得拆除过期字段的登记"
  );

  // 过期字段在反序列化读取时剔除，存活字段与其 TTL 保真
  let deserialized = HashObject::deserialize(&mut Cursor::new(&buf)).unwrap();
  assert!(deserialized.hash.contains_key(b"field_live".as_slice()));
  assert!(!deserialized.hash.contains_key(b"field_expired".as_slice()));
  assert_eq!(
    deserialized
      .expiration_times
      .as_ref()
      .and_then(|t| t.get(b"field_live".as_slice())),
    Some(&(now + LIVE_SPAN))
  );
}

/// test/standalone/Garnet.test.collections/GarnetObjectTests.cs:SerializeDoesNotMutateSortedSetWithExpiredMembers
#[test]
fn serialize_does_not_mutate_sorted_set_with_expired_members() {
  let now = now_ticks();
  let mut zset = SortedSetObject::new();
  zset.add(b"m_live", 1.0);
  zset.add(b"m_expired", 2.0);
  zset.set_expiration(b"m_live", now + LIVE_SPAN, ExpireOption::NONE);
  // 同 hash 用例：装载单点直挂过去刻度，避开 set_expiration 的
  // KeyAlreadyExpired 删除分支（SortedSetObject.cs:717-723 同语义）
  zset.insert_expiration(b"m_expired".to_vec(), now - EXPIRED_SPAN);

  let mut buf = Vec::new();
  zset.serialize(&mut buf).unwrap();
  assert_eq!(zset.sorted_set_dict.len(), 2, "序列化不得删除过期成员");
  assert!(zset.has_expirable_items(), "序列化不得拆除过期结构");
  assert!(
    zset
      .expiration_times
      .as_ref()
      .is_some_and(|t| t.contains_key(b"m_expired".as_slice())),
    "序列化不得拆除过期成员的登记"
  );

  // 已过期成员在反序列化读取时剔除，存活成员与其 TTL 保真（C# roundTripped.Count() == 1）
  let mut deserialized = SortedSetObject::deserialize(&mut Cursor::new(&buf)).unwrap();
  assert_eq!(deserialized.purge_expired_len(), 1);
  assert!(
    deserialized
      .sorted_set_dict
      .contains_key(b"m_live".as_slice())
  );
  assert!(
    !deserialized
      .sorted_set_dict
      .contains_key(b"m_expired".as_slice())
  );
  assert_eq!(
    deserialized
      .expiration_times
      .as_ref()
      .and_then(|t| t.get(b"m_live".as_slice())),
    Some(&(now + LIVE_SPAN))
  );
}

/// O(1) 计数规约（doc/zh/collection.md §6 自研规约，无 C# 对标用例：
/// C# Count() 为 O(K) 只读逐键过滤，见 HashObject::count 刻意差异声明）：
/// 存活/过期混合态的精度与物理剔除闭环断言
#[test]
fn count_purges_expired_and_reads_len() {
  let now = now_ticks();
  let mut hash = HashObject::new();
  for field in ["f_live1", "f_live2", "f_live3"] {
    hash.hash.insert(field.as_bytes().to_vec(), b"v".to_vec());
    hash.set_expiration(field.as_bytes(), now + LIVE_SPAN, ExpireOption::NONE);
  }
  for field in ["f_dead1", "f_dead2"] {
    hash.hash.insert(field.as_bytes().to_vec(), b"v".to_vec());
    hash.insert_expiration(field.as_bytes().to_vec(), now - EXPIRED_SPAN);
  }

  // 精度与 C# Count() 逐值一致（已过期不计入），且堆序剔除物理落地
  assert_eq!(hash.purge_expired_len(), 3);
  assert_eq!(hash.hash.len(), 3, "count 堆序剔除须物理移除已过期字段");
  assert!(hash.mutated_by_ttl(), "剔除须经写回升格标志闭环");
}

/// 全字段未到期时 count 堆顶 peek 短路（无 O(K) 扫描；以结构无损断言
/// 代替时间测量，免慢机 flaky；规模取升阶门限 65536）
#[test]
fn count_peek_shortcircuits_on_future_ttl() {
  let now = now_ticks();
  let mut hash = HashObject::new();
  for i in 0..65536_u32 {
    let field = format!("f{i}").into_bytes();
    hash.hash.insert(field.clone(), b"v".to_vec());
    hash.set_expiration(&field, now + LIVE_SPAN, ExpireOption::NONE);
  }

  assert_eq!(hash.purge_expired_len(), 65536);
  assert_eq!(hash.hash.len(), 65536, "未到期不得物理剔除");
  assert!(!hash.mutated_by_ttl(), "短路不得置写回升格标志");
}
