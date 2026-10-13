#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 升阶 list/set 命令面语义对齐集成测试（自 tiered_cmds_align.rs 按主题拆分，
//! 对标 C# Garnet.test.collections 命令语义）
//!
//! 覆盖：
//! 1. List LPOP count 数组形态、RPOP 尾端弹出（fDelAtHead 双分支）、
//!    LPUSH 序号不覆盖、LINDEX；分层态序号窗口两端伸缩与内存态逐条对齐；
//! 2. LPOS 双态逐字节全等矩阵（独立参照模型）与缺省形首命中早停；
//! 3. SPOP 负 count 拦截、SRANDMEMBER 负 count、空分层集合键应答语义（D15）；
//! 4. 未支持操作物化降级走对象层（SMOVE/LTRIM）。

use std::{collections::VecDeque, time::Instant};

use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use wcol::types::{garnet_object::LIST_SEQ_BASE, member_ttl::encode_member};
use wnode::resp::{garnet_api::GarnetApi, resp_server_session::RespServerSession};
use wnode_test::{auto_exec, open_env, session_on as session_with};
use wresp::command::RespCommand;
use wval::GarnetObjectType;

/// 前缀字节 + 序号文本（成员命名 f1/v1/m1 同型）
fn prefixed(prefix: u8, buf: &mut ItoaBuffer, i: usize) -> Vec<u8> {
  let s = buf.format(i).as_bytes();
  let mut v = Vec::with_capacity(s.len() + 1);
  v.push(prefix);
  v.extend_from_slice(s);
  v
}

/// 升阶灌水批量写入：按 16384 一片组装 args（首元素为键）驱动 auto_exec。
/// `elem` 产出第 i 个成员的追加字节段——单段（list/set 成员）或双段
/// （hash field+value、zset score+member，段序与 RESP 参数序一致）
fn bulk_fill(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  cmd: RespCommand,
  key: &[u8],
  total: usize,
  mut elem: impl FnMut(usize, &mut ItoaBuffer) -> Vec<Vec<u8>>,
) {
  let mut buf = ItoaBuffer::new();
  for chunk_start in (1..=total).step_by(16384) {
    let chunk_end = (chunk_start + 16383).min(total);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(key.to_vec());
    for i in chunk_start..=chunk_end {
      args.extend(elem(i, &mut buf));
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(api, rt, s, cmd, &arg_slices);
  }
}
/// 升阶键 List/Set 命令面与未支持操作物化降级
#[test]
fn test_tiered_list_set_and_demote() {
  let (rt, api, _store, _dir) = open_env("tiered-list-set.db");
  let mut s = session_with(&api);

  // ---- List 升阶
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let mut buf = ItoaBuffer::new();
  bulk_fill(
    &api,
    &rt,
    &mut s,
    RespCommand::Rpush,
    b"l",
    total,
    |i, buf| vec![prefixed(b'v', buf, i)],
  );

  // LPOP count 数组形态 / 单形态 / 零形态
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lpop, &[b"l", b"3"]);
  assert_eq!(out, b"*3\r\n$2\r\nv1\r\n$2\r\nv2\r\n$2\r\nv3\r\n");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lpop, &[b"l", b"1"]);
  assert_eq!(out, b"$2\r\nv4\r\n");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lpop, &[b"l", b"0"]);
  assert_eq!(out, b"*0\r\n");

  // LPUSH 序号不覆盖：推入后 LINDEX 0 应为最新推入元素（头端不丢）
  auto_exec(&api, &rt, &mut s, RespCommand::Lpush, &[b"l", b"head"]);
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"0"]);
  assert_eq!(out, b"$4\r\nhead\r\n");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"-1"]);
  assert_eq!(out, b"$6\r\nv65546\r\n");

  // ---- RPOP 尾端弹出（C# ListPop 的 fDelAtHead=false 分支：list.Last + RemoveLast，
  // 与 LPOP 的 fDelAtHead=true 两支互反。旧分层臂把两支塌缩成同一次头端正序扫描，
  // split_off(0) 恒等 ⇒ RPOP 弹的是最旧元素且与 LPOP 弹同一批）
  // 尾端最大序号先出、其余按序号降序跟进（与 LINDEX -1/-2/-3 的独立臂口径一致）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"l", b"3"]);
  assert_eq!(
    out, b"*3\r\n$6\r\nv65546\r\n$6\r\nv65545\r\n$6\r\nv65544\r\n",
    "RPOP count 须自尾端弹出且尾在前"
  );
  // RPOP 无 count → 单 bulk 形态，弹当前尾端 v65543
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"l"]);
  assert_eq!(out, b"$6\r\nv65543\r\n");
  // RPOP 0 → 空数组，长度不变
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"l", b"0"]);
  assert_eq!(out, b"*0\r\n");
  // 尾弹只动尾端：头端仍是 LPUSH 的 head、次头端 v5 未被侵蚀
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lpop, &[b"l", b"1"]);
  assert_eq!(out, b"$4\r\nhead\r\n", "RPOP 不得侵蚀头端元素");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lpop, &[b"l", b"2"]);
  assert_eq!(out, b"*2\r\n$2\r\nv5\r\n$2\r\nv6\r\n");
  // 剩余尾端（LINDEX 独立臂交叉核对，须与 RPOP 已弹端相邻不重不漏）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"-1"]);
  assert_eq!(out, b"$6\r\nv65542\r\n");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Llen, &[b"l"]);
  assert_eq!(out, b":65536\r\n", "size 递减须与净弹出条数一致");

  // ---- 内存态（未升阶）同序列对照：同一份数据两态逐条对齐
  auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Rpush,
    &[b"ml", b"v1", b"v2", b"v3", b"v4", b"v5"],
  );
  auto_exec(&api, &rt, &mut s, RespCommand::Lpush, &[b"ml", b"head"]);
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"ml", b"3"]);
  assert_eq!(out, b"*3\r\n$2\r\nv5\r\n$2\r\nv4\r\n$2\r\nv3\r\n");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"ml"]);
  assert_eq!(out, b"$2\r\nv2\r\n");
  // count 大于剩余长度 → 截断为剩余条数（含头端 head，非 null）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"ml", b"9"]);
  assert_eq!(out, b"*2\r\n$2\r\nv1\r\n$4\r\nhead\r\n");
  // 弹空自愈：键已删 → null
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"ml"]);
  assert_eq!(out, b"$-1\r\n");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Exists, &[b"ml"]);
  assert_eq!(out, b":0\r\n", "弹空后键须自愈删除");

  // ---- Set 升阶
  bulk_fill(
    &api,
    &rt,
    &mut s,
    RespCommand::Sadd,
    b"k",
    total,
    |i, buf| vec![prefixed(b'm', buf, i)],
  );

  // SPOP 负 count → 拦截（C# SetCommands.cs:SetPop）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Spop, &[b"k", b"-1"]);
  assert_eq!(out, b"-ERR value is not an integer or out of range.\r\n");
  // SRANDMEMBER 负 count → |count| 个数组
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Srandmember, &[b"k", b"-3"]);
  // 数组头 + 3 成员（不删除，成员数不变）
  let text = String::from_utf8(out).unwrap();
  assert!(
    text.starts_with("*3\r\n$"),
    "SRANDMEMBER -3 应回 3 成员: {text}"
  );
  // SRANDMEMBER 0 → 空数组
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Srandmember, &[b"k", b"0"]);
  assert_eq!(out, b"*0\r\n");
  // SPOP count=2 → 数组形态且计数递减
  let before = auto_exec(&api, &rt, &mut s, RespCommand::Scard, &[b"k"]);
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Spop, &[b"k", b"2"]);
  assert_eq!(&out[..4], b"*2\r\n");
  let after = auto_exec(&api, &rt, &mut s, RespCommand::Scard, &[b"k"]);
  assert_ne!(before, after, "SPOP 后基数应递减");

  // ---- 未支持操作物化降级（对象层单源闭环）
  // HRANDFIELD 信封键兜底臂（分层树态已另立树内只读抽样臂矩阵用例
  // test_tiered_hash_random_field_tree_arm_matrix）——新建 hash（信封域直接对象层）
  for chunk_start in (1..=2048).step_by(2048) {
    let chunk_end = (chunk_start + 2047).min(2048);
    let mut args: Vec<Vec<u8>> = Vec::with_capacity((chunk_end - chunk_start + 1) * 2 + 1);
    args.push(b"hh".to_vec());
    for i in chunk_start..=chunk_end {
      let s = buf.format(i).as_bytes();
      let mut f = Vec::with_capacity(s.len() + 1);
      f.push(b'f');
      f.extend_from_slice(s);
      args.push(f);
      args.push(s.to_vec());
    }
    let arg_slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&api, &rt, &mut s, RespCommand::Hset, &arg_slices);
  }
  // 未升阶 hash 的 HRANDFIELD 走 slow 段信封兜底臂（对象层单源）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"hh"]);
  assert!(out.starts_with(b"$"), "HRANDFIELD 应回成员 bulk: {out:?}");

  // SMOVE（升阶源键 → 物化装载 + tiered 感知写回）
  auto_exec(&api, &rt, &mut s, RespCommand::Sadd, &[b"dst", b"hold"]);
  let out = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Smove,
    &[b"k", b"dst", b"m100"],
  );
  assert_eq!(out, b":1\r\n");
  // 源键 SMOVE 后信封接管（size 跌回降阶阈值下 → 懒降阶），数据不丢
  let out = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Sismember,
    &[b"dst", b"m100"],
  );
  assert_eq!(out, b":1\r\n");

  // LTRIM（升阶 list → 物化 + tiered 感知写回）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Ltrim, &[b"l", b"0", b"9"]);
  assert_eq!(out, b"+OK\r\n");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Llen, &[b"l"]);
  assert_eq!(out, b":10\r\n");
}

/// 分层态列表序号窗口：两端推入/弹出只动两端，终态与内存态语义逐条对齐
///
/// 旧臂每次 RPUSH/LPUSH 前全树扫描求 min/max（升阶门槛 65536 ⇒ 单条推入即付
/// 整树页级 IO），RPOP 支另付一次正序全扫滚动窗口。现臂一次定位树最左键
/// （`tiered_collection_ops/list.rs:list_head_seq`，scan_cnt=1）取头端，尾端按
/// 「序号区间恒连续」由头 + meta.size - 1 派生，三支恒 O(1)/O(count)。
/// 参照模型即 C# ListPush/ListPop 的一参一次 AddFirst/AddLast/RemoveFirst/
/// RemoveLast 循环（ListObjectImpl.cs:229-298），错端、序号覆盖、丢元素都会在
/// 本用例的混合伸缩与终态全量比对中露相。
#[test]
fn test_tiered_list_push_seq_window() {
  let (rt, api, store, _dir) = open_env("tiered-list-seq.db");
  let mut s = session_with(&api);

  // 预灌至升阶（RPUSH 成批 1000 条）
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let mut model: VecDeque<Vec<u8>> = VecDeque::new();
  let mut buf = ItoaBuffer::new();
  bulk_fill(
    &api,
    &rt,
    &mut s,
    RespCommand::Rpush,
    b"l",
    total,
    |i, buf| {
      let v = prefixed(b'v', buf, i);
      model.push_back(v.clone());
      vec![v]
    },
  );
  let sess = store.new_session().unwrap();
  assert!(
    rt.block_on(sess.load_collection_stub(b"l"))
      .unwrap()
      .is_some(),
    "应已升阶为分层态"
  );

  // RPUSH 多参：自尾 +1 向上连续分配
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpush, &[b"l", b"t1", b"t2"]);
  assert_eq!(out, format!(":{}\r\n", model.len() + 2).into_bytes());
  for v in [b"t1".as_slice(), b"t2"] {
    model.push_back(v.to_vec());
  }

  // LPUSH 多参：一参一次 AddFirst ⇒ LPUSH l h1 h2 h3 落 [h3, h2, h1, v1…]
  // （旧臂按 args 正序自 base 递增分配，落 [h1, h2, h3, v1…]，与内存态错序）
  auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Lpush,
    &[b"l", b"h1", b"h2", b"h3"],
  );
  for v in [b"h1".as_slice(), b"h2", b"h3"] {
    model.push_front(v.to_vec());
  }
  // 两端直读交叉核对窗口未错端（LINDEX 走独立树内臂）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"0"]),
    b"$2\r\nh3\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"3"]),
    b"$2\r\nv1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"-1"]),
    b"$2\r\nt2\r\n"
  );

  // RPOP / LPOP 各摘一端：RPOP 起点 = 头 + size - n
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Rpop, &[b"l", b"2"]);
  assert_eq!(out, b"*2\r\n$2\r\nt2\r\n$2\r\nt1\r\n");
  model.pop_back();
  model.pop_back();
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lpop, &[b"l", b"3"]);
  assert_eq!(out, b"*3\r\n$2\r\nh3\r\n$2\r\nh2\r\n$2\r\nh1\r\n");
  for _ in 0..3 {
    model.pop_front();
  }

  // 弹后再自尾推入：新元素落在剩余尾端 +1（窗口随 size 收缩同步回缩）
  auto_exec(&api, &rt, &mut s, RespCommand::Rpush, &[b"l", b"t3"]);
  model.push_back(b"t3".to_vec());
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"-1"]),
    b"$2\r\nt3\r\n"
  );

  // 批量 LPUSH：一次自减 args.len()，整块下移不覆盖既有元素
  let mut batch: Vec<Vec<u8>> = Vec::with_capacity(500);
  for i in 0..500_usize {
    let s = buf.format(i).as_bytes();
    let mut b = Vec::with_capacity(s.len() + 1);
    b.push(b'b');
    b.extend_from_slice(s);
    batch.push(b);
  }
  let mut args: Vec<&[u8]> = vec![b"l"];
  args.extend(batch.iter().map(|v| v.as_slice()));
  auto_exec(&api, &rt, &mut s, RespCommand::Lpush, &args);
  for v in &batch {
    model.push_front(v.clone());
  }
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Lindex, &[b"l", b"0"]),
    b"$4\r\nb499\r\n",
    "LPUSH 末参须为新头端"
  );

  // 终态全量比对：无丢元素、无重复、序不错（序号区间连续性固证）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Llen, &[b"l"]);
  let mut exp_len = Vec::with_capacity(16);
  exp_len.push(b':');
  exp_len.extend_from_slice(buf.format(model.len()).as_bytes());
  exp_len.extend_from_slice(b"\r\n");
  assert_eq!(out, exp_len);
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Lrange, &[b"l", b"0", b"-1"]);
  let text = String::from_utf8(out).unwrap();
  let mut parts = text.split("\r\n");
  let len_str = buf.format(model.len()).to_string();
  let len_hdr = format!("*{len_str}");
  assert_eq!(parts.next(), Some(len_hdr.as_str()));
  for expect in &model {
    let hdr = parts.next().unwrap();
    assert!(hdr.starts_with('$'));
    assert_eq!(&hdr[1..], buf.format(expect.len()));
    let got = parts.next().unwrap();
    assert_eq!(
      got.as_bytes(),
      expect.as_slice(),
      "分层态与内存态须逐条同序"
    );
  }
  assert_eq!(parts.next(), Some(""));
}

/// 空分层集合键 SRANDMEMBER 应答语义（D15）：手工升阶空条目（size=0 元
/// 记录）被 MetaValue::is_live 门（size > 0）判死，分层臂结构性不可达，
/// 一律按缺失口径应答——无 count / 负 count → RESP null，count>0 → 空集
/// 头，count==0 → 键态无关空数组。对标 C# SetObjectImpl.cs SetRandomMember
/// 空集三分支（WriteSetLength(0) / WriteNull / WriteNull）与 SetCommands.cs
/// SetRandomMember 的 count==0 拦截；SPOP 为分层穿透臂（物化降级），
/// 无 SPOP 分层位点
#[test]
fn test_tiered_empty_set_srandmember_null_semantics() {
  let (rt, api, store, _dir) = open_env("tiered-empty-set.db");
  let mut s = session_with(&api);

  // 手工升阶空集（count=0 元记录 + 空树）：元记录在册但 is_live 判死
  rt.block_on(async {
    store
      .new_session()
      .unwrap()
      .promote_collection_to_bftree(b"es", GarnetObjectType::Set, vec![], i64::MAX, false)
      .await
      .unwrap();
  });
  assert!(
    !rt.block_on(async {
      store
        .new_session()
        .unwrap()
        .load_collection_stub(b"es")
        .await
        .unwrap()
        .is_some()
    }),
    "size=0 集合元记录应被 is_live 门判死（空分层键不得以活键形态泄出）"
  );

  // 无 count → RESP null
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Srandmember, &[b"es"]),
    b"$-1\r\n"
  );
  // 带 count（含负）→ NOTFOUND 口径空数组（C# SetCommands.cs SetRandomMember
  // 的 NOTFOUND 分支只区分有无 count，不分公司负；对象层空集负 count 的
  // WriteNull 是另一层口径，与本缺失形态无关）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Srandmember, &[b"es", b"3"]),
    b"*0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Srandmember, &[b"es", b"0"]),
    b"*0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Srandmember, &[b"es", b"-2"]),
    b"*0\r\n"
  );
}

/// LPOS 独立参照模型（不经被测码，对位 C# ListObjectImpl.cs:ListPosition :383-444
/// 的扫描算法：正/负 rank 窗口折 maxlen、rank 跳过、noOfFoundItem==count break、
/// 命中序正/降序直出），输出 RESP 帧字节。tiered_cmds_align LRANGE 窗口用例
/// 同款「测试侧独立复写参照」口径
fn lpos_reference(
  list: &[Vec<u8>],
  element: &[u8],
  rank: i64,
  count: Option<i64>,
  maxlen: Option<i64>,
  resp3: bool,
) -> Vec<u8> {
  let len = list.len() as i64;
  let is_default = count.is_none();
  let parsed = count.unwrap_or(1);
  let cap = if parsed == 0 { len } else { parsed };
  let ml = maxlen.unwrap_or(0);
  let mut hits: Vec<i64> = Vec::new();
  if rank > 0 {
    let bound = if ml == 0 { len } else { len.min(ml) };
    let mut r = rank;
    for (i, item) in list.iter().enumerate().take(bound.max(0) as usize) {
      if item.as_slice() == element {
        if r == 1 {
          hits.push(i as i64);
          if is_default || hits.len() as i64 == cap {
            break;
          }
        } else {
          r -= 1;
        }
      }
    }
  } else {
    let mut r = rank.unsigned_abs() as i64;
    let low = if ml == 0 { 0 } else { (len - ml).max(0) };
    let mut i = len - 1;
    while i >= low && i >= 0 {
      if list[i as usize].as_slice() == element {
        if r == 1 {
          hits.push(i);
          if is_default || hits.len() as i64 == cap {
            break;
          }
        } else {
          r -= 1;
        }
      }
      i -= 1;
    }
  }
  if is_default {
    match hits.first() {
      Some(idx) => format!(":{idx}\r\n").into_bytes(),
      None => {
        if resp3 {
          b"_\r\n".to_vec()
        } else {
          b"$-1\r\n".to_vec()
        }
      }
    }
  } else if hits.is_empty() {
    b"*0\r\n".to_vec()
  } else {
    let mut out = format!("*{}\r\n", hits.len()).into_bytes();
    for idx in &hits {
      out.extend_from_slice(format!(":{idx}\r\n").as_bytes());
    }
    out
  }
}

/// LPOS 单命令装配往返（RANK 恒显式给出；COUNT/MAXLEN 依缺省形省略）
fn lpos_exec(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  key: &[u8],
  elem: &[u8],
  (rank, count, maxlen): (i64, Option<i64>, Option<i64>),
) -> Vec<u8> {
  let mut owned: Vec<Vec<u8>> = vec![
    key.to_vec(),
    elem.to_vec(),
    b"RANK".to_vec(),
    rank.to_string().into_bytes(),
  ];
  if let Some(c) = count {
    owned.push(b"COUNT".to_vec());
    owned.push(c.to_string().into_bytes());
  }
  if let Some(m) = maxlen {
    owned.push(b"MAXLEN".to_vec());
    owned.push(m.to_string().into_bytes());
  }
  let arg_slices: Vec<&[u8]> = owned.iter().map(|v| v.as_slice()).collect();
  auto_exec(api, rt, s, RespCommand::Lpos, &arg_slices)
}

/// 分层/信封双态 LPOS 树内臂逐字节全等矩阵（票 zcode-r149c-lposrank 案二）：
/// RANK{1,2,3,5,6,-1,-2,-5,-6} × COUNT{缺省,1,0,2} × MAXLEN{缺省,0,阈中,=len,>len}
/// × 命中{头,尾,多,零} × RESP{2,3}，全组合三方交叉——独立参照模型、手工升阶
/// 分层键（seed_list 同款原语）、RPUSH 信封键（对象层单源）逐字节全等；
/// 三门拒绝/语法错误帧双态同帧（wcol read_list_position_params 单源验证）
#[test]
fn test_tiered_list_lpos_dualstate_parity_matrix() {
  let (rt, api, store, _dir) = open_env("tiered-list-lpos-matrix.db");
  let mut s = session_with(&api);
  // 16 元素：c 出现于 {0,2,5,10,14}（多重命中、头命中、尾侧命中），
  // z15 唯一尾命中、m3 唯一中段命中、nope 零命中
  let content: Vec<Vec<u8>> = (0..16usize)
    .map(|i| match i {
      0 | 2 | 5 | 10 | 14 => b"c".to_vec(),
      15 => b"z15".to_vec(),
      3 => b"m3".to_vec(),
      _ => format!("e{i}").into_bytes(),
    })
    .collect();
  {
    let sess = store.new_session().unwrap();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = content
      .iter()
      .enumerate()
      .map(|(i, e)| {
        (
          (LIST_SEQ_BASE + i as u128).to_be_bytes().to_vec(),
          encode_member(e, None),
        )
      })
      .collect();
    rt.block_on(sess.promote_collection_to_bftree(
      b"tl",
      GarnetObjectType::List,
      entries,
      i64::MAX,
      false,
    ))
    .unwrap();
    assert!(
      rt.block_on(sess.load_collection_stub(b"tl"))
        .unwrap()
        .is_some(),
      "前置判据：tl 须处于分层树态"
    );
  }
  // 信封对照键 tm：同一份数据一次 RPUSH（16 元素远低于升阶门限）
  let mut args: Vec<&[u8]> = vec![b"tm"];
  args.extend(content.iter().map(|v| v.as_slice()));
  auto_exec(&api, &rt, &mut s, RespCommand::Rpush, &args);

  for version in [2u8, 3] {
    s.resp_protocol_version = version;
    let resp3 = version == 3;
    for elem in [b"c".as_slice(), b"z15", b"m3", b"nope"] {
      for rank in [1i64, 2, 3, 5, 6, -1, -2, -5, -6] {
        for count in [None, Some(1i64), Some(0), Some(2)] {
          for maxlen in [None, Some(0i64), Some(3), Some(16), Some(20)] {
            let expect = lpos_reference(&content, elem, rank, count, maxlen, resp3);
            let got_tiered = lpos_exec(&api, &rt, &mut s, b"tl", elem, (rank, count, maxlen));
            assert_eq!(
              got_tiered,
              expect,
              "分层态 v{version} elem={:?} rank={rank} count={count:?} maxlen={maxlen:?}",
              String::from_utf8_lossy(elem)
            );
            let got_envelope = lpos_exec(&api, &rt, &mut s, b"tm", elem, (rank, count, maxlen));
            assert_eq!(
              got_envelope,
              expect,
              "信封态 v{version} elem={:?} rank={rank} count={count:?} maxlen={maxlen:?}",
              String::from_utf8_lossy(elem)
            );
          }
        }
      }
    }
  }

  // 三门拒绝/语法错误帧双态同帧（词元与三门与信封态同一份码）
  s.resp_protocol_version = 2;
  for bad in [
    vec![b"c".to_vec(), b"RANK".to_vec(), b"0".to_vec()],
    vec![b"c".to_vec(), b"COUNT".to_vec(), b"-1".to_vec()],
    vec![b"c".to_vec(), b"MAXLEN".to_vec(), b"-5".to_vec()],
    vec![b"c".to_vec(), b"RANK".to_vec(), b"abc".to_vec()],
    vec![b"c".to_vec(), b"FOO".to_vec(), b"1".to_vec()],
    vec![b"c".to_vec(), b"COUNT".to_vec()],
  ] {
    let mut t_args: Vec<&[u8]> = vec![b"tl"];
    t_args.extend(bad.iter().map(|v| v.as_slice()));
    let mut m_args: Vec<&[u8]> = vec![b"tm"];
    m_args.extend(bad.iter().map(|v| v.as_slice()));
    let t = auto_exec(&api, &rt, &mut s, RespCommand::Lpos, &t_args);
    let m = auto_exec(&api, &rt, &mut s, RespCommand::Lpos, &m_args);
    assert_eq!(t, m, "错误帧双态同帧: bad={bad:?}");
  }
  // 字面量抽查：三门 not-an-integer 帧（既有 Spop 界测同款文本）
  let t = lpos_exec(&api, &rt, &mut s, b"tl", b"c", (0, None, None));
  assert_eq!(t, b"-ERR value is not an integer or out of range.\r\n");
}

/// 缺省形首命中早停（票 zcode-r149c-lposrank 案二测试点）：2×阈 键 canary 居
/// 头，LPOS 缺省形命中即 return false 截断（O(1) 定位 + 一条记录读），与全树
/// 131072 条逐条解码比较的未命中形计时对照——旧形物化通道两形皆付全树三拷贝，
/// 无早停面。量级差判据（8 倍裕度），先各跑一轮预热使冷页 IO 不参与对照
#[test]
fn test_tiered_list_lpos_default_first_hit_early_stop() {
  let (rt, api, _store, _dir) = open_env("tiered-list-lpos-stop.db");
  let mut s = session_with(&api);
  let total = 2 * wcol::TIERED_PROMOTE_THRESHOLD;
  bulk_fill(
    &api,
    &rt,
    &mut s,
    RespCommand::Rpush,
    b"l2",
    total,
    |i, buf| {
      if i == 1 {
        vec![b"head_canary".to_vec()]
      } else {
        vec![prefixed(b'v', buf, i)]
      }
    },
  );

  // 预热两形（页缓存收敛后对比）
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Lpos,
      &[b"l2", b"head_canary"]
    ),
    b":0\r\n",
    "缺省形头命中位次 0"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Lpos,
      &[b"l2", b"absent_elem"]
    ),
    b"$-1\r\n",
    "未命中形回 null（严禁折存储错误帧）"
  );

  let mark = Instant::now();
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Lpos,
      &[b"l2", b"head_canary"]
    ),
    b":0\r\n"
  );
  let d_head = mark.elapsed();
  let mark = Instant::now();
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Lpos,
      &[b"l2", b"absent_elem"]
    ),
    b"$-1\r\n"
  );
  let d_full = mark.elapsed();
  assert!(
    d_head * 8 < d_full,
    "缺省形首命中须早停截断（头命中 O(1) vs 全树 {total} 条扫）: head={d_head:?} full={d_full:?}"
  );
}
