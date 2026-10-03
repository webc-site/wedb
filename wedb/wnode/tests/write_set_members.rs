use std::str::from_utf8;

use wbase::map::HashSet;
use wnode::resp::objects::set_commands::write_set_members;
use wresp::{cmd_strings as cs, ext::RespVecExt};

/// 分配探针（本测试二进制内的手工计数包装分配器，原样转调 System，
/// 仅原子自增；先例形制同 wcol list_object_impl probe 与
/// tests/tiered_output_frame_head.rs backfill_shape_drops_body_scale_scratch——
/// 计数窗口重复多次比最小值，结论不受同进程并行分配噪声影响）
mod probe {
  use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
  };

  static ALLOCS: AtomicUsize = AtomicUsize::new(0);

  struct Counting;

  // SAFETY: alloc/dealloc 原样转调 System，计数仅为无副作用的原子自增；
  // realloc/alloc_zeroed 走 trait 默认实现（即本 alloc/dealloc 组合）
  unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
      ALLOCS.fetch_add(1, Ordering::Relaxed);
      unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
      unsafe { System.dealloc(ptr, layout) }
    }
  }

  #[global_allocator]
  static COUNTING: Counting = Counting;

  /// 至今累计的分配次数
  pub(super) fn allocs() -> usize {
    ALLOCS.load(Ordering::Relaxed)
  }
}

/// 应答头行（首个 CRLF 之前，不含 CRLF）
fn head(out: &[u8]) -> &str {
  let end = out.windows(2).position(|w| w == b"\r\n").unwrap();
  from_utf8(&out[..end]).unwrap()
}

/// 抽出帧头行之外的全部 bulk 实体（成员序非契约，按集合等价比对，
/// 口径见 doc/zh/deviations.md「集合族双态应答成员序非契约」条目）
fn bulks(out: &[u8]) -> Vec<Vec<u8>> {
  let mut items = Vec::new();
  let mut pos = out.windows(2).position(|w| w == b"\r\n").unwrap() + 2;
  while pos < out.len() {
    assert_eq!(out[pos], b'$', "实体不是 bulk: {:?}", &out[pos..]);
    let line_end = pos + out[pos..].windows(2).position(|w| w == b"\r\n").unwrap();
    let len: usize = String::from_utf8_lossy(&out[pos + 1..line_end])
      .parse()
      .unwrap();
    let body = line_end + 2;
    items.push(out[body..body + len].to_vec());
    pos = body + len + 2;
  }
  items
}

/// 多成员集：SINTER/SUNION/SDIFF 结果集合头版本分派（RESP2 *N / RESP3 ~N），
/// 成员按集合等价断言（帧头与成员集合为契约、序非契约）
#[test]
fn set_head_resp2_array_resp3_set() {
  let members: HashSet<Vec<u8>> = [b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()]
    .into_iter()
    .collect();

  let mut out2 = Vec::new();
  write_set_members(&members, &mut out2, 2);
  assert_eq!(head(&out2), "*3");
  let mut got = bulks(&out2);
  got.sort();
  assert_eq!(got, vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()]);

  let mut out3 = Vec::new();
  write_set_members(&members, &mut out3, 3);
  assert_eq!(head(&out3), "~3");
  let mut got = bulks(&out3);
  got.sort();
  assert_eq!(got, vec![b"a".to_vec(), b"bb".to_vec(), b"ccc".to_vec()]);
}

/// 空集位点（SMEMBERS 缺键 / SPOP count==0 / SPOP 缺键带 count）
/// 对位 C# RespServerSessionOutput.cs:100 WriteEmptySet
#[test]
fn empty_set_resp2_star_resp3_tilde() {
  let members = HashSet::default();

  let mut out2 = Vec::new();
  write_set_members(&members, &mut out2, 2);
  assert_eq!(out2, b"*0\r\n");

  let mut out3 = Vec::new();
  write_set_members(&members, &mut out3, 3);
  assert_eq!(out3, b"~0\r\n");
}

/// 分配证据（票 zcode-r137c-setstore2 案二·读臂出帧零中转）：改前形态
/// （to_members 逐成员 clone 成 `Vec<Vec<u8>>` 中转数组后出帧，已随臂清退，
/// 此处按原实现同形复刻）vs 现生产借用迭代直写形态，中转数组本体
/// `n` 次成员克隆 + 1 次数组体分配在改后臂恒不发生
#[test]
fn borrowed_frame_drops_member_scratch() {
  const N: usize = 2000;
  let members: HashSet<Vec<u8>> = (0..N)
    .map(|i| format!("member-{i:05}").into_bytes())
    .collect();
  // 出帧缓冲预留足量容量，隔离应答缓冲自身扩容，使计数只反映中转形态差异
  let frame_budget = members.iter().map(|m| m.len() + 16).sum::<usize>() + 64;

  let (mut min_old, mut min_new) = (usize::MAX, usize::MAX);
  for _ in 0..20 {
    let mut out = Vec::with_capacity(frame_budget);
    let before = probe::allocs();
    {
      // 改前形态同形复刻：全量 clone 中转数组后逐条出帧
      let scratch: Vec<Vec<u8>> = members.iter().cloned().collect();
      cs::write_set_len(&mut out, scratch.len(), 2);
      for member in &scratch {
        out.write_resp_bulk_string(member);
      }
    }
    min_old = min_old.min(probe::allocs() - before);

    let mut out = Vec::with_capacity(frame_budget);
    let before = probe::allocs();
    write_set_members(&members, &mut out, 2);
    min_new = min_new.min(probe::allocs() - before);
  }

  // 改前窗口每窗至少实付 N 次成员克隆 + 1 次数组体（自形保证，噪声只增不减）
  assert!(min_old > N, "改前形态窗口未实付中转克隆: {min_old}");
  assert!(
    min_old > min_new,
    "借出帧形态分配未降: 改前 {min_old} 次 vs 改后 {min_new} 次"
  );
}
