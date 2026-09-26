//! 在 garnet 中的相对路径: C# watchVersionMap.IncrementVersion（libs/server/Storage/Functions/MainStore/DeleteMethods.cs）+ BasicLockTests.cs WatchKey 面
use std::sync::Arc;

use wtxn::{TxnKeyEntryComparison, TxnWatchedKeysContainer, WatchVersionMap};
use wval::SessionPrefixBuf;

/// 与写面推进同源的根域前缀（ns 0, db 0）：本文件纯容器用例的缺省归属域
fn root() -> SessionPrefixBuf {
  SessionPrefixBuf::ROOT
}

fn container() -> (TxnWatchedKeysContainer, Arc<WatchVersionMap>) {
  let map = Arc::new(WatchVersionMap::new(64));
  (TxnWatchedKeysContainer::new(Arc::clone(&map)), map)
}

#[test]
fn add_watch_then_untouched_key_validates() {
  let (mut c, _) = container();
  c.add_watch(root().as_slice(), b"user:1");
  assert!(c.validate_watch_version());
}

#[test]
fn modified_watched_key_fails_validation() {
  let (mut c, map) = container();
  let root = root();
  c.add_watch(root.as_slice(), b"user:1");
  map.increment_version(TxnKeyEntryComparison::scoped_key_hash(root.as_slice(), b"user:1") as u64);
  assert!(!c.validate_watch_version());
}

#[test]
fn reset_clears_all_watches() {
  let (mut c, map) = container();
  let root = root();
  c.add_watch(root.as_slice(), b"a");
  c.add_watch(root.as_slice(), b"b");
  map.increment_version(TxnKeyEntryComparison::scoped_key_hash(root.as_slice(), b"a") as u64);
  c.reset();
  assert!(c.validate_watch_version());
}

/// 跨归属域同名键互不串扰（本票核心隔离性回归）
///
/// 对标 C# 每库独持 WatchVersionMap 的物理隔离（libs/server/GarnetDatabase.cs:156）：
/// rust 共享单表形态下，版本表槽位以 (会话物理前缀, 用户键) 复合身份构造
/// （`TxnKeyEntryComparison::scoped_key_hash` 单点）。域 A 登记 WATCH 后，
/// 域 B 对同字面键的写推进不得使域 A 的校验失效；唯同域写入方触发失配。
#[test]
fn scoped_watch_isolates_same_key_across_domains() {
  let (mut c, map) = container();
  // 域 A = 根 (ns0,db0)；域 B = (ns1,db0)；两域观察同字面键 k
  let domain_a = SessionPrefixBuf::new(0, 0);
  let domain_b = SessionPrefixBuf::new(1, 0);
  let key = b"k";

  c.add_watch(domain_a.as_slice(), key);
  assert!(c.validate_watch_version());

  // 异域（租户 B）写同字面键：不得误伤域 A 的监视
  map.increment_version(TxnKeyEntryComparison::scoped_key_hash(domain_b.as_slice(), key) as u64);
  assert!(
    c.validate_watch_version(),
    "跨租户同名键写入必须不影响本域 WATCH"
  );

  // 同域（租户 A）写：必须判失配
  map.increment_version(TxnKeyEntryComparison::scoped_key_hash(domain_a.as_slice(), key) as u64);
  assert!(!c.validate_watch_version(), "同域写入必须使 WATCH 校验失配");
}

/// 重复 WATCH 同键幂等去重（本票核心：对标 Redis src/multi.c:watchForKey）
///
/// 同一连接对同 (prefix, key) 连续多次 add_watch，容器内至多一条切片：
/// save_lock_hashes 与 save_keys_to_key_list 均仅产出 1 条，杜绝重复条目
/// 挤占内联容量逃逸堆分配、放大 TxnKeyEntries 锁集。
#[test]
fn duplicate_watch_same_key_yields_single_entry() {
  let (mut c, _) = container();
  let root = root();
  for _ in 0..5 {
    c.add_watch(root.as_slice(), b"dup:key");
  }
  assert_eq!(
    c.save_lock_hashes(root.as_slice()).count(),
    1,
    "重复 WATCH 不得在锁集登记面产出重复条目"
  );
  assert_eq!(
    c.save_keys_to_key_list().count(),
    1,
    "重复 WATCH 不得在键列表登记面产出重复条目"
  );
  assert!(c.validate_watch_version());
}

/// 重复 WATCH 严禁刷新监视基线（本票核心：乐观锁隔离正确性回归）
///
/// 首次 WATCH 后版本被写推进，再次 WATCH 同键的早退路径不得读取并覆盖
/// 新 version：EXEC 校验仍须以首次监视基线判失配，抹除监视期间的写推进
/// 即脏写漏洞。
#[test]
fn duplicate_watch_does_not_refresh_version_baseline() {
  let (mut c, map) = container();
  let root = root();
  c.add_watch(root.as_slice(), b"k");
  map.increment_version(TxnKeyEntryComparison::scoped_key_hash(root.as_slice(), b"k") as u64);
  // 期间已发生写推进，重复 WATCH 命中查重早退，基线保持首次监视时刻
  c.add_watch(root.as_slice(), b"k");
  assert!(
    !c.validate_watch_version(),
    "重复 WATCH 不得刷新版本基线，首次监视后的写推进必须使校验失败"
  );
}
