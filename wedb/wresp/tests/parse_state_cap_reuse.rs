//! `SessionParseState::initialize` 容量复用回归（对位 C# `Initialize(int)` 早退）
//!
//! 证伪目标：容量满足时不清零槽位后，读面严格以 `count` 为界——
//! 越出 `count` 的陈旧槽位（脏偏移 + 脏长度）虽仍驻留底层缓冲，但对参数
//! 视图不可达，绝不泄漏到命令处理侧。

use wresp::{argslice::ArgSlice, session_parse_state::SessionParseState};

/// 以宿主缓冲连续排布一批参数，返回 (槽位列, 宿主缓冲)
fn build(args: &[&[u8]]) -> (Vec<ArgSlice>, Vec<u8>) {
  let mut buf = Vec::new();
  let mut slices = Vec::with_capacity(args.len());
  for arg in args {
    slices.push(ArgSlice::new(buf.len(), arg.len()));
    buf.extend_from_slice(arg);
  }
  (slices, buf)
}

/// 长载荷 → 短载荷复用：脏槽位驻留但读面不可达，且底层 len 不回缩
#[test]
fn initialize_reuses_capacity_without_zeroing_slots() {
  // 第 1 轮：8 个长载荷槽位，逐槽给可区分的旧值，避免全等掩盖残留
  let long_owned: Vec<Vec<u8>> = (0..8usize).map(|i| vec![b'a' + i as u8; 32]).collect();
  let long_refs: Vec<&[u8]> = long_owned.iter().map(|v| &v[..]).collect();
  let (old_slices, old_buf) = build(&long_refs);

  let mut state = SessionParseState::new();
  state.initialize(8);
  assert_eq!(
    state.root_buffer.len(),
    8,
    "首轮扩容至 cap=max(8,MIN_PARAMS)=8"
  );
  state.root_buffer[..8].copy_from_slice(&old_slices);
  assert_eq!(state.arg_in(&old_buf, 5), &long_owned[5][..]);

  // 记录 index 2 的旧槽位值（下一轮 count=2 不可达，须原样残留）
  let stale_at_2 = state.root_buffer[2];

  // 第 2 轮：initialize(2)，容量足够（len 8 >= cap max(2,5)=5）→ 不覆写任何槽位
  state.initialize(2);
  assert_eq!(state.count, 2);
  assert!(
    state.root_buffer.len() >= 8,
    "len 不得回缩：证明未走 clear()，脏槽位仍驻留"
  );
  assert_eq!(
    state.root_buffer[2], stale_at_2,
    "第 3 槽（越出 count）仍为上轮旧值：证明 initialize 未清零"
  );

  // 仅覆写本轮 count 之内的 2 个短载荷槽位（模拟真实写循环 0..count）
  let short_owned: Vec<Vec<u8>> = vec![b"GET".to_vec(), b"k".to_vec()];
  let short_refs: Vec<&[u8]> = short_owned.iter().map(|v| &v[..]).collect();
  let (new_slices, new_buf) = build(&short_refs);
  state.root_buffer[..2].copy_from_slice(&new_slices);

  // 读面只看到 len(=count) 之内的新值
  assert_eq!(state.parameters().len(), 2, "参数视图收口于 count");
  assert_eq!(state.len(), 2);
  assert_eq!(state.arg_in(&new_buf, 0), b"GET");
  assert_eq!(state.arg_in(&new_buf, 1), b"k");

  // 脏槽位不泄漏：以 count 为界的序列化预算只计入 2 个新参数
  assert_eq!(
    state.get_serialized_length(),
    size_of::<i32>() + 3 + 4 + 1 + 4,
    "序列化仅覆盖前 2 槽，第 3 槽脏值不参与"
  );
}

/// 短 → 长复用：容量不足才扩容，扩容后新读到的槽位由写循环覆值
#[test]
fn initialize_grows_only_when_capacity_short() {
  let mut state = SessionParseState::new();
  // 首轮 2 参：cap=max(2,MIN_PARAMS)，MIN_PARAMS=5
  state.initialize(2);
  let after_small = state.root_buffer.len();
  assert!(after_small >= 2);

  // 次轮 8 参：容量不足须扩到 8
  state.initialize(8);
  assert_eq!(state.root_buffer.len(), 8);

  let args: Vec<&[u8]> = (0..8u8).map(|_| &b"v"[..]).collect();
  let (slices, buf) = build(&args);
  state.root_buffer[..8].copy_from_slice(&slices);
  assert_eq!(state.parameters().len(), 8);
  assert_eq!(state.arg_in(&buf, 7), b"v");
}
