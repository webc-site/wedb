//! upsert 链首脱钩门归池复核回归（对标 C# Helpers.cs:CanElide 单点判据、
//! InternalUpsert.cs:CreateNewRecordUpsert 一次判定两处消费）：
//! elide_src 候选在链回溯段置位（两处均带 `cur == addr` 链首守卫），脱钩门
//! `src == addr && src_prev < begin_addr` 必须折成单一 elided 判定值，前驱接管
//! 与 CAS 成功后的 seal+归池两处严格消费同一判定值。
//!
//! 两支审核判读的实测裁决：
//! - 判读 A（`src_prev >= begin_addr` 一支可达）：`test_upsert_no_elide_while_src_still_on_chain`
//!   构造仍挂在链上（前驱 ≥ 截断边界）的旧链首加长更新，修复前该记录被密封并
//!   送入复活池、随后续复活覆写导致碰撞前驱键丢键——确红；
//! - 判读 B（链更深处命中 src 的「非连续链前缀」形态不可达）：回溯段两处
//!   elide_src 置位均被 `if cur == addr` 守卫挡在链首，深层命中永不产生候选，
//!   `test_upsert_deeper_src_never_pooled` 修复前后恒绿，仅作不可达性锁定。

use std::sync::atomic::Ordering;

use aok::{OK, Void};
use itoa::Buffer;
use wbase::align::DEFAULT_SECTOR_SIZE;
use wdev::Device;
use windex::{HashBucketEntry, HashIndex};
use wkv::StoreSession;
use wval::KeyTag;

use crate::support::{config, open_store, slot_in_pool};

/// 搜索物理键哈希对 `mask` 命中 `wanted` 的键（与 collision_chain.rs 同一确定性
/// 构造：index_size=64 → 桶位 6 位 + tag 15 位，其余 43 位哈希位不参与索引判定，
/// 期望 2^21 步内命中），排除与给定键集物理键全等的候选
fn find_hash_matching<D: Device>(
  session: &StoreSession<D>,
  wanted: u64,
  mask: u64,
  prefix_str: &str,
  excluded: &[&[u8]],
) -> String {
  let prefix = session.session_prefix();
  let mut buf = Vec::with_capacity(64);
  buf.extend_from_slice(prefix.as_slice());
  buf.push(KeyTag::String as u8);
  let user_key_offset = buf.len();
  buf.extend_from_slice(prefix_str.as_bytes());
  buf.push(b'-');
  let base_len = buf.len();

  let excluded_phys: Vec<wval::TaggedKeyBuf> = excluded
    .iter()
    .map(|e| session.session_string_key(e))
    .collect();

  let mut itoa_buf = Buffer::new();
  for i in 0u64.. {
    buf.truncate(base_len);
    buf.extend_from_slice(itoa_buf.format(i).as_bytes());
    if HashIndex::hash_key(&buf) & mask == wanted
      && !excluded_phys.iter().any(|e| e.as_slice() == buf.as_slice())
    {
      return String::from_utf8(buf[user_key_offset..].to_vec()).expect("utf8 key");
    }
  }
  unreachable!("哈希键搜索不可能耗尽")
}

/// 判读 A 确红用例：链首旧记录前驱仍 ≥ 截断边界（碰撞键挂在链上、新记录前驱
/// 仍指向旧链首）时，加长更新触发尾部追加——修复前归池分支只判 elide_src
/// 候选存在即 seal(true) + put，仍被新记录引用的旧链首被送入复活池；后续一次
/// 普通新键写入即从池中取走该槽位覆写为异键记录，victim 键沿链回溯断链丢键。
/// 修复后前驱接管与归池严格消费同一 elided 判定值：门不成立 ⇒ 候选作废，
/// 旧链首不密封、不入池、链上可达性完好。
#[compio::test]
async fn test_upsert_no_elide_while_src_still_on_chain() -> Void {
  let env = open_store(
    "elide_gate_chain.db",
    config(64, DEFAULT_SECTOR_SIZE, 16)?.with_revivification(true),
  )?;
  let store = env.store;
  let session = store.new_session()?;

  let mask = (store.index.load().mask as u64) | (0x7fffu64 << HashBucketEntry::HASH_TAG_SHIFT);
  let victim = b"elide_gate_victim".to_vec();
  // twin 与 victim 同桶同 tag：先写 victim，后写 twin 使 twin 记录成为槽位链首、
  // victim 记录被链入 twin 的 prev——twin 链首的前驱（victim 地址）≥ begin_addr
  let twin_hash = HashIndex::hash_key(session.session_string_key(&victim).as_slice()) & mask;
  let twin = find_hash_matching(&session, twin_hash, mask, "elide-gate-twin", &[&victim]);
  let victim_val = vec![b'V'; 96];
  let head_val = vec![b'H'; 96];

  let addr_victim = session.upsert(&victim, &victim_val).await?;
  let addr_head = session.upsert(twin.as_bytes(), &head_val).await?;
  assert!(
    addr_victim >= store.begin_address(),
    "前置：victim 记录须位于截断边界之上（旧链首前驱 ≥ begin_addr，脱钩门不成立）"
  );
  assert_eq!(session.read(&victim).await?, Some(victim_val.clone()));
  assert_eq!(session.read(twin.as_bytes()).await?, Some(head_val.clone()));

  let put_before = store.reviv_pool.put_count.load(Ordering::Relaxed);

  // 加长更新击中原位容量门 → 尾部追加；旧链首未脱钩（新记录前驱仍指向它）
  let long_val = vec![b'L'; 400];
  let addr_new = session.upsert(twin.as_bytes(), &long_val).await?;
  assert_ne!(addr_new, addr_head, "加长更新必须落尾追加分支，地址换新");

  // 契约核心断言（修复前确红：归池分支未复核脱钩前提）：
  // 1. 未脱钩记录绝不触达复活池入池口
  assert_eq!(
    store.reviv_pool.put_count.load(Ordering::Relaxed),
    put_before,
    "旧链首仍挂在链上（前驱 ≥ begin_addr）时绝不得触达复活池 put"
  );
  // 2. 旧链首不得被密封（密封使无锁读者对其 RETRY_LATER 且语义上已判死）
  assert_eq!(
    store
      .hlog
      .with_memory_record(addr_head, |rec| Ok(rec.is_closed()))?,
    Some(false),
    "未脱钩的链上旧记录不得被密封"
  );
  // 3. 旧链首不得作为空闲槽位存在于分桶
  assert!(
    !slot_in_pool(&store, addr_head),
    "未脱钩的链上旧记录绝不得进入复活池分桶"
  );
  // 4. 双键读取闭环完好
  assert_eq!(session.read(twin.as_bytes()).await?, Some(long_val.clone()));
  assert_eq!(session.read(&victim).await?, Some(victim_val.clone()));

  // 危害实锤（修复前确红、修复后恒绿）：solo 键与 twin 桶位刻意取反（不同桶
  // 不同 tag，绝不参与该碰撞链），其帧足印搜索不超过误归池槽位 → 修复前一次
  // 普通新键写入即从池中取走 addr_head 槽位原地覆写为 solo 记录（prev=0），
  // victim 沿链回溯经 addr_head 读到异键且断链，丢键坐实；修复后池中无槽位，
  // solo 正常尾追加，链完好
  let solo = find_hash_matching(
    &session,
    twin_hash ^ 0b10,
    mask,
    "elide-gate-solo",
    &[&victim, twin.as_bytes()],
  );
  let twin_phys_len = session.session_string_key(twin.as_bytes()).len();
  let solo_phys_len = session.session_string_key(solo.as_bytes()).len();
  let head_frame = wrecord::record_size(twin_phys_len, head_val.len()) as u32;
  let solo_val_len = (0..=400usize)
    .rev()
    .find(|&n| wrecord::record_size(solo_phys_len, n) as u32 <= head_frame)
    .expect("存在使帧足印不超过误归池槽位的值长");
  let solo_val = vec![b'S'; solo_val_len];
  session.upsert(solo.as_bytes(), &solo_val).await?;
  assert_eq!(
    session.read(&victim).await?,
    Some(victim_val),
    "未脱钩槽位若被误归池并复活覆写，victim 链断裂丢键（判读 A 危害确证）"
  );
  assert_eq!(
    session.read(twin.as_bytes()).await?,
    Some(long_val),
    "twin 链首新版本读取不得受影响"
  );
  assert_eq!(session.read(solo.as_bytes()).await?, Some(solo_val));
  OK
}

/// 判读 B 不可达锁定用例（非连续链前缀形态）：目标键命中于链更深处（槽位头
/// 为碰撞键、原位更新失败）时，回溯段两处 elide_src 置位均被 `cur == addr`
/// 链首守卫挡住，候选恒为 None——修复前后本用例均绿，证明该形态在现网代码下
/// 永不产生归池输入，判读 B 的真险形态不成立；修复后 elided 单点消费亦不改变
/// 此行为（深层旧记录不密封、不入池，链完整）。
#[compio::test]
async fn test_upsert_deeper_src_never_pooled() -> Void {
  let env = open_store(
    "elide_gate_deeper.db",
    config(64, DEFAULT_SECTOR_SIZE, 16)?.with_revivification(true),
  )?;
  let store = env.store;
  let session = store.new_session()?;

  let mask = (store.index.load().mask as u64) | (0x7fffu64 << HashBucketEntry::HASH_TAG_SHIFT);
  let deep = b"elide_gate_deep".to_vec();
  // 先写 deep，后写 head 碰撞键：槽位头为 head 记录，deep 记录被掩埋在链深处
  let deep_hash = HashIndex::hash_key(session.session_string_key(&deep).as_slice()) & mask;
  let head = find_hash_matching(&session, deep_hash, mask, "elide-gate-head", &[&deep]);
  let deep_val = vec![b'D'; 96];
  let head_val = vec![b'H'; 96];

  let addr_deep = session.upsert(&deep, &deep_val).await?;
  let addr_head = session.upsert(head.as_bytes(), &head_val).await?;

  let put_before = store.reviv_pool.put_count.load(Ordering::Relaxed);

  // 对链深处 deep 键加长更新：可变区回溯在槽位头 Miss、深层 Active 且原位
  // 失败——`cur == addr` 守卫使 elide 候选永不置位
  let long_val = vec![b'L'; 400];
  let addr_new = session.upsert(&deep, &long_val).await?;
  assert_ne!(addr_new, addr_deep, "加长更新必须落尾追加分支");

  assert_eq!(
    store.reviv_pool.put_count.load(Ordering::Relaxed),
    put_before,
    "链更深处命中的旧记录绝不产生脱钩候选，更不得触达复活池"
  );
  assert_eq!(
    store
      .hlog
      .with_memory_record(addr_deep, |rec| Ok(rec.is_closed()))?,
    Some(false),
    "深层旧记录不得被密封"
  );
  assert_eq!(
    store
      .hlog
      .with_memory_record(addr_head, |rec| Ok(rec.is_closed()))?,
    Some(false),
    "槽位头碰撞键记录不得受牵连密封"
  );
  assert!(!slot_in_pool(&store, addr_deep), "深层旧记录不得入池");
  assert!(!slot_in_pool(&store, addr_head), "槽位头记录不得入池");

  // 链完整：新 deep 记录接管槽位，head 键经新记录 prev 仍可达
  assert_eq!(session.read(&deep).await?, Some(long_val));
  assert_eq!(session.read(head.as_bytes()).await?, Some(head_val));
  OK
}
