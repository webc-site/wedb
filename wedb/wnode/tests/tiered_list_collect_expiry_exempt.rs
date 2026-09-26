//! 分层 List 到期收集臂豁免（task/ing/wnode-tiered-llen-collect-sweeps-list-values-as-expiry-frames）
//!
//! 缺陷口径：LLEN 分层慢路径 Degrade 分支进 `exec_tiered_collect`，其出账内核
//! `expire_sweep_or_rebuild` → `sweep_expired_members` 逐记录 `decode_member`
//! 解「到期刻度」，对 List 无类型豁免（SCAN 臂 scan.rs:324 有 `!= List` 豁免，
//! 收集臂没有）——List 树记录载荷是元素本身，不是 member+expiry 帧，树内任何
//! `0x01` 首字节且总长 ≥ 9B 的记录，其元素载荷前 8B 被宽松解码读成远古假刻度
//! `ticks < now`，即被判「已到期」**物理出账**（整值重灌剔除 + meta.size 扣减
//! + 键可被删空回收），客户端零写入而丢数据。
//!
//! 修复判据（方向 A，与 SCAN 臂豁免同源单点）：`exec_tiered_collect` 入口按
//! tag 对 List 分流到水位纯修复臂——不出账不扫树，仅把越线水位归 `i64::MAX`
//! 回写元记录，不置脏不推进 WATCH 栅栏。本载体构造：真实自动升阶（体积维跨
//! `wcol::TIERED_PROMOTE_BYTES`）→ 手工把树内首条记录改写为
//! `0x01 + 8B ticks=1 + 原载荷` 形态并把 `meta.next_expiry` 落 0（完整缺陷
//! 前置）→ 断言 LLEN 应答不减、`meta.size` 守恒、成员字节逐枚等值、水位自愈
//! 归 MAX。
//!
//! 反向注入（撤掉 scan.rs 的 `if tag == GarnetObjectType::List` 分流）转红
//! 路径：`exec_tiered_collect` 径直进出账内核 → 伪刻度 `ticks=1 < now` 判
//! 到期 `expired=1` → 整值重灌物理剔除 shaped 元素并 `dec_size`（键非空不回
//! 收）——红报文恰落在下方「LLEN 不得减少成员数」「meta.size 必须守恒」
//! 「成员字节逐枚等值」三处数据断言上（非编译错）。

use std::{mem::take, str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbftree::{BfTreeInsertResult, ScanReturnField};
use wcol::types::member_ttl::{decode_member, encode_member};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::resp::{
  garnet_api::{GarnetApi, StoreGarnetApi},
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wresp::command::RespCommand;

const KEY: &[u8] = b"collectlist";

/// 填充元素 1000B（树内记录 = 16B 序号键 + 1B 旗标 + 1000B 载荷，仍在集合
/// 升阶树 1024B 记录上限内）
const FILLER_LEN: usize = 1000;

/// `wbase::heap` 每条目记账口径：`round_up_ptr(len) + SLOT * 2`
const PER_ELEMENT_BYTES: usize = FILLER_LEN + 32;

/// 跨 4MB 体积门限所需的填充元素数（含余量）
fn filler_count() -> usize {
  wcol::TIERED_PROMOTE_BYTES / PER_ELEMENT_BYTES + 250
}

/// 首字节 `0x01`、长度 ≥9 的元素：入树时经已修复的导出臂编码为 plain 形态，
/// 随后由 [`forge_expiry_shaped_record`] 把其树内记录改写为带远古假刻度形态
fn shaped_element() -> Vec<u8> {
  let mut v = vec![0x01u8, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF];
  v.extend_from_slice(b"one-tail-marker");
  while v.len() < 64 {
    v.push(b'y');
  }
  v
}

fn ascii_element() -> Vec<u8> {
  b"plain-ascii-element".to_vec()
}

struct Env {
  rt: Runtime,
  store: Arc<WedbStore<SegmentedDevice>>,
  api: GarnetApi,
  _dir: tempfile::TempDir,
}

fn env(tag: &str) -> Env {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  Env {
    rt: Runtime::new().unwrap(),
    store: store.clone(),
    api: Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    _dir: dir,
  }
}

fn session_with(env: &Env) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(env.api.clone());
  s
}

fn exec(env: &Env, s: &mut RespServerSession, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
  s.output.clear();
  env.api.exec(s, cmd, args);
  if !s.output.is_empty() {
    return take(&mut s.output);
  }
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
  let out = env.rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// 元记录权威读（键是否分层态 + 水位/计数对账）
fn tiered_meta(env: &Env, key: &[u8]) -> Option<(wval::MetaValue, wbftree::RangeIndexStub)> {
  let sess = env.store.new_session().unwrap();
  env.rt.block_on(sess.load_collection_stub(key)).unwrap()
}

fn llen(env: &Env, s: &mut RespServerSession, key: &[u8]) -> i64 {
  let frame = exec(env, s, RespCommand::Llen, &[key]);
  assert_eq!(&frame[0..1], b":", "LLEN 应答非整数帧：{frame:?}");
  from_utf8(&frame[1..frame.len() - 2])
    .unwrap()
    .parse()
    .unwrap()
}

/// 严格解析 RESP2 批量字符串数组应答（帧长与实体逐字节对账）
fn parse_bulk_array(frame: &[u8]) -> Vec<Vec<u8>> {
  let mut items = Vec::new();
  let mut i = 0usize;
  let line_end = |buf: &[u8], from: usize| -> usize {
    buf[from..]
      .windows(2)
      .position(|w| w == b"\r\n")
      .map(|p| from + p)
      .expect("RESP 行终止符缺失")
  };
  assert_eq!(&frame[0..1], b"*", "应答应为数组帧，实得 {frame:?}");
  let mut n: usize = 0;
  let mut first = true;
  while i < frame.len() {
    let e = line_end(frame, i);
    let head = &frame[i..e];
    i = e + 2;
    if first {
      assert_eq!(&head[0..1], b"*");
      n = from_utf8(&head[1..]).unwrap().parse().unwrap();
      first = false;
      continue;
    }
    assert_eq!(&head[0..1], b"$", "数组第 {} 项非批量字符串帧", items.len());
    let len: usize = from_utf8(&head[1..]).unwrap().parse().unwrap();
    assert!(
      i + len + 2 <= frame.len(),
      "帧承诺 {len} 字节而实体只剩 {} 字节",
      frame.len() - i - 2
    );
    assert_eq!(&frame[i + len..i + len + 2], b"\r\n");
    items.push(frame[i..i + len].to_vec());
    i += len + 2;
  }
  assert_eq!(items.len(), n, "帧头承诺 {n} 项而实得 {} 项", items.len());
  items
}

fn lrange_all(env: &Env, s: &mut RespServerSession, key: &[u8]) -> Vec<Vec<u8>> {
  let frame = exec(env, s, RespCommand::Lrange, &[key, b"0", b"-1"]);
  parse_bulk_array(&frame)
}

/// 缺陷前置注入（写锁窗口内直写树，与写臂同一互斥面）：把树内首条记录
/// （shaped 元素，RPUSH 序 0）改写为 `0x01 + 8B ticks=1 + 原载荷` 形态，并把
/// `meta.next_expiry` 落 0 越线——单一机制下 LLEN 即落 Degrade 进收集臂
fn forge_expiry_shaped_record(env: &Env, key: &[u8]) {
  env.rt.block_on(async {
    let sess = env.store.new_session().unwrap();
    let (mut meta, mut stub) = sess
      .load_collection_stub(key)
      .await
      .unwrap()
      .expect("伪造前置：键应处于分层态");
    let guard = sess
      .acquire_tree_write(key, &mut stub)
      .await
      .expect("树写锁获取失败");
    let mut first: Option<(Vec<u8>, Vec<u8>)> = None;
    guard
      .tree()
      .scan_with_count_callback(&[0u8], 1, ScanReturnField::KeyAndValue, |k, v| {
        first = Some((k.to_vec(), v.to_vec()));
        true
      })
      .expect("扫描树首条记录失败");
    let (k, v) = first.expect("树内应已有记录");
    let (expiry, payload) = decode_member(&v);
    assert!(
      expiry.is_none(),
      "伪造前置：升阶导出臂产物应为 plain 形态（无旗标刻度）"
    );
    let forged = encode_member(payload, Some(1));
    assert_eq!(
      guard.tree().insert(&k, &forged),
      BfTreeInsertResult::Success,
      "伪造记录写树被拒"
    );
    drop(guard);
    meta.next_expiry = 0;
    sess
      .save_bftree_meta_stub(key, &meta, &stub)
      .await
      .expect("伪造水位落盘失败");
  });
}

#[test]
fn tiered_llen_collect_exempt_keeps_list_members() {
  let env = env("tiered-list-collect-exempt.db");
  let mut s = session_with(&env);

  let shaped = shaped_element();
  let ascii = ascii_element();
  assert_eq!(shaped[0], 0x01);
  assert!(shaped.len() >= 9);
  exec(&env, &mut s, RespCommand::Rpush, &[KEY, &shaped, &ascii]);

  // 真实自动升阶：RPUSH 收尾 apply_rmw_post_operate → should_promote →
  // export_entries（修复后恒 plain 前缀）→ promote_collection_to_bftree
  let total = filler_count();
  let filler = vec![b'v'; FILLER_LEN];
  let mut pushed = 0usize;
  while pushed < total {
    let chunk = (total - pushed).min(500);
    let mut args: Vec<&[u8]> = Vec::with_capacity(chunk + 1);
    args.push(KEY);
    for _ in 0..chunk {
      args.push(&filler);
    }
    exec(&env, &mut s, RespCommand::Rpush, &args);
    pushed += chunk;
  }
  let (meta, _) = tiered_meta(&env, KEY).expect("跨体积门限应已自动升阶为分层树");
  assert_eq!(meta.size, (total + 2) as u64, "升阶后计数应承接全量元素");
  assert_eq!(
    meta.next_expiry,
    i64::MAX,
    "List 恒无成员级 TTL：升阶后水位必须为无刻度（i64::MAX）"
  );

  // 缺陷前置注入：树内首条记录带远古假刻度 + meta.next_expiry 落 0（越线）
  forge_expiry_shaped_record(&env, KEY);
  let (meta, _) = tiered_meta(&env, KEY).expect("注入后键应仍处分层态");
  assert_eq!(
    meta.next_expiry, 0,
    "伪造水位应已落盘（LLEN 越线 Degrade 前置）"
  );
  assert_eq!(meta.size, (total + 2) as u64, "注入只改形态不动计数");

  // 修复判据 1：LLEN 应答不得减少成员数——缺陷形下出账内核把伪刻度记录判
  // 到期物理出账，应答退化为 total+1
  assert_eq!(
    llen(&env, &mut s, KEY),
    (total + 2) as i64,
    "LLEN 不得因伪刻度把真实成员到期出账（成员被物理剔除）"
  );

  // 修复判据 2：meta.size 守恒——size 扣减即「成员被物理出账」的记账痕迹
  let (meta, _) = tiered_meta(&env, KEY).expect("键不得被删空回收或退出分层态");
  assert_eq!(
    meta.size,
    (total + 2) as u64,
    "meta.size 必须守恒：扣减即到期出账已物理剔除真实成员"
  );

  // 修复判据 3：豁免臂水位自愈——越线水位被归 MAX（LLEN 后唯一写入者只能是
  // 豁免臂），键仍处分层态未被动过
  assert_eq!(
    meta.next_expiry,
    i64::MAX,
    "豁免臂应把被伪造的越线水位校正归 i64::MAX（自愈出口）"
  );

  // 修复判据 4：成员字节逐枚等值（含被伪造记录的 shaped 元素，读侧剥 9B 后
  // 应还原原载荷；缺陷形下该元素已被整值重灌剔除）
  let mut expect: Vec<Vec<u8>> = Vec::with_capacity(total + 2);
  expect.push(shaped);
  expect.push(ascii);
  expect.extend((0..total).map(|_| filler.clone()));
  let got = lrange_all(&env, &mut s, KEY);
  assert_eq!(got.len(), expect.len(), "LRANGE 条数背离（成员丢失？）");
  for (i, (g, w)) in got.iter().zip(expect.iter()).enumerate() {
    assert_eq!(g, w, "LRANGE 第 {i} 个元素字节背离");
  }

  // 幂等收敛：水位已自愈归 MAX，第二次 LLEN 走 O(1) 快路径，应答不变
  assert_eq!(llen(&env, &mut s, KEY), (total + 2) as i64);
}
