use compio_buf::ReserveError;
use wbase::primed::PrimedVec;

/// 记忆键在同一分配上稳定：清零一次后重复取用不换键
#[test]
fn prime_key_stable_across_len_changes() {
  use compio_buf::SetLen;
  use wbase::primed::PrimedRecv;

  let mut pv = PrimedVec::new(Vec::with_capacity(64));
  assert_eq!(pv.prime_key(), None);
  let key1 = {
    let spare = pv.primed_spare();
    assert_eq!(spare.len(), 64);
    assert!(spare.iter().all(|&b| b == 0));
    pv.prime_key()
  };
  // 推进长度后同段再取：键不变（不重清零），视图随长度收缩
  unsafe { pv.advance_to(8) };
  assert_eq!(pv.len(), 8);
  assert_eq!(pv.prime_key(), key1);
  assert_eq!(pv.primed_spare().len(), 56);
  // 归零长度：记忆保持，视图恢复全容量
  pv.clear();
  assert_eq!(pv.prime_key(), key1);
  assert_eq!(pv.primed_spare().len(), 64);
}

/// 扩容（realloc）与取出（换缓冲）均失忆，新段重新清零
#[test]
fn memo_invalidated_by_grow_and_take() -> Result<(), ReserveError> {
  use compio_buf::IoBufMut;
  use wbase::primed::PrimedRecv;

  let mut pv = PrimedVec::new(Vec::with_capacity(16));
  let _ = pv.primed_spare();
  assert!(pv.prime_key().is_some());
  pv.reserve(4096)?;
  assert_ne!(pv.capacity(), 16);
  assert_eq!(pv.prime_key(), None, "扩容后代际必须失效");
  let _ = pv.primed_spare();
  let raw = pv.take();
  assert_eq!(pv.prime_key(), None, "取出后必须失忆");
  // 异体缓冲以外部键装配不非法，但新段须重新清零后才可作读视图
  let mut reborn = PrimedVec::from_primed(raw, Some((0, 0)));
  let spare = reborn.primed_spare();
  assert!(spare.iter().all(|&b| b == 0));
  Ok(())
}
