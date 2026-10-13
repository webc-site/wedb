#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! 升阶键命令面语义对齐集成测试——hash 主题（对标 C# Garnet.test.collections
//! 命令语义；zset/list/expire 主题已按测试二进制拆分至
//! tiered_cmds_align_zset.rs / tiered_cmds_align_list.rs /
//! tiered_cmds_align_expire.rs）
//!
//! 升阶（wcol should_promote → KeyTag::Meta 元记录 + wbftree 树）后：
//! 1. 键存活探针五命令 EXISTS/TTL/EXPIRE/PERSIST/TYPE 与统计面
//!    MEMORY USAGE / OBJECT ENCODING / IDLETIME / REFCOUNT 对升阶键回值；
//! 2. SCAN 族树内游标全量遍历（HSCAN/SSCAN/ZSCAN；COSCAN 域收口仅服务
//!    自定义对象，升阶键不再在其扫描域）；
//! 3. 分层树 HRANDFIELD 只读抽样臂双态矩阵（§8.5 SRANDMEMBER 对偶，
//!    票 zcode-r151c-smembers），含成员级 TTL 抽样域收敛与全到期树形态。

use std::str::from_utf8;

use compio::runtime::Runtime;
use itoa::Buffer as ItoaBuffer;
use wbase::{
  convert::TICKS_PER_SECOND,
  map::{HashSet, HashSetExt},
  time::now_ticks,
};
use wcol::types::member_ttl::{decode_member, encode_member};
use wnode::resp::{garnet_api::GarnetApi, resp_server_session::RespServerSession};
use wnode_test::{auto_exec, open_env, session_on as session_with};
use wresp::command::RespCommand;
use wval::GarnetObjectType;

/// 序号文本（纯数字成员/分值用）
fn num(buf: &mut ItoaBuffer, i: usize) -> Vec<u8> {
  buf.format(i).as_bytes().to_vec()
}

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
/// 升阶键探针五命令与统计面（EXISTS/TTL/EXPIRE/PERSIST/TYPE +
/// MEMORY USAGE / OBJECT ENCODING / IDLETIME / REFCOUNT）
#[test]
fn test_tiered_key_probe_and_stats() {
  let (rt, api, store, _dir) = open_env("tiered-probe.db");
  let mut s = session_with(&api);

  // 升阶：hash 写入 65546 字段
  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  bulk_fill(
    &api,
    &rt,
    &mut s,
    RespCommand::Hset,
    b"h",
    total,
    |i, buf| vec![prefixed(b'f', buf, i), num(buf, i)],
  );

  // 升阶确认
  let sess = store.new_session().unwrap();
  assert!(
    rt.block_on(sess.load_collection_stub(b"h"))
      .unwrap()
      .is_some(),
    "应已升阶"
  );

  // 探针面：键对客户端可见
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Exists, &[b"h"]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Type, &[b"h"]),
    b"+hash\r\n"
  );
  // 无 TTL → -1（C# ExpiryRead::NoExpiry）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Ttl, &[b"h"]),
    b":-1\r\n"
  );
  // EXPIRE 生效 → :1，TTL 落在 (0,100]
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Expire, &[b"h", b"100"]),
    b":1\r\n"
  );
  let ttl = auto_exec(&api, &rt, &mut s, RespCommand::Ttl, &[b"h"]);
  let ttl_val: i64 = from_utf8(&ttl[1..ttl.len() - 2]).unwrap().parse().unwrap();
  assert!((0..=100).contains(&ttl_val), "TTL 应在 (0,100]: {ttl:?}");
  // PERSIST → :1，TTL 恢复 -1
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Persist, &[b"h"]),
    b":1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Ttl, &[b"h"]),
    b":-1\r\n"
  );

  // 统计面
  let mem = auto_exec(&api, &rt, &mut s, RespCommand::MemoryUsage, &[b"h"]);
  let mem_val: i64 = from_utf8(&mem[1..mem.len() - 2]).unwrap().parse().unwrap();
  assert!(mem_val > 0, "MEMORY USAGE 应回正整数: {mem:?}");
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::ObjectEncoding, &[b"h"]),
    b"$9\r\nhashtable\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::ObjectIdletime, &[b"h"]),
    b":0\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::ObjectRefcount, &[b"h"]),
    b":1\r\n"
  );
}

/// 升阶键 SCAN 族树内游标全量遍历
#[test]
fn test_tiered_scan_full_iteration() {
  let (rt, api, _store, _dir) = open_env("tiered-scan.db");
  let mut s = session_with(&api);

  let total = wcol::TIERED_PROMOTE_THRESHOLD + 10;
  let mut buf = ItoaBuffer::new();
  bulk_fill(
    &api,
    &rt,
    &mut s,
    RespCommand::Sadd,
    b"s",
    total,
    |i, buf| vec![prefixed(b'm', buf, i)],
  );

  // SSCAN 全量遍历：COUNT 50000（钳 OBJECT_SCAN_COUNT_LIMIT=1000），
  // 迭代至 cursor 0，成员去重计数 == total
  let mut cursor = b"0".to_vec();
  let mut seen = HashSet::new();
  let mut rounds = 0;
  loop {
    let count_arg = buf.format(50000).as_bytes().to_vec();
    let out = auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Sscan,
      &[b"s", &cursor, b"COUNT", &count_arg],
    );
    let text = String::from_utf8(out).unwrap();
    // 帧形态：*2\r\n$<len>\r\n<cursor>\r\n*<n>\r\n($<len>\r\n<member>\r\n)*
    let mut parts = text.split("\r\n");
    assert_eq!(parts.next(), Some("*2"));
    let _ = parts.next().unwrap();
    let next_cursor = parts.next().unwrap().to_string();
    let arr_len: usize = parts
      .next()
      .unwrap()
      .trim_start_matches('*')
      .parse()
      .unwrap();
    // 每成员占两行（bulk 头 + 值）
    for _ in 0..arr_len {
      let hdr = parts.next().unwrap();
      assert!(hdr.starts_with('$'), "成员应为 bulk 头: {hdr}");
      let m = parts.next().unwrap();
      assert!(seen.insert(m.to_string()), "成员不应重复返回: {m}");
    }
    rounds += 1;
    if next_cursor == "0" {
      break;
    }
    cursor = next_cursor.into_bytes();
    assert!(rounds < 200, "游标未收敛: {rounds} 轮");
  }
  assert_eq!(seen.len(), total, "全量遍历应覆盖全部成员");

  // HSCAN 升阶键可遍历（hash 域）
  let mut buf = ItoaBuffer::new();
  let h_total = 2048; // 不再升阶，仅验证树内 hash 扫描通道（复用 s 键会 WRONGTYPE）
  let mut h_args: Vec<Vec<u8>> = Vec::with_capacity(h_total * 2 + 1);
  h_args.push(b"h".to_vec());
  for i in 1..=h_total {
    let s = buf.format(i).as_bytes();
    let mut f = Vec::with_capacity(s.len() + 1);
    f.push(b'f');
    f.extend_from_slice(s);
    h_args.push(f);
    h_args.push(s.to_vec());
  }
  let h_slices: Vec<&[u8]> = h_args.iter().map(|v| v.as_slice()).collect();
  auto_exec(&api, &rt, &mut s, RespCommand::Hset, &h_slices);
  let mut seen_fields = 0usize;
  let mut cursor = b"0".to_vec();
  loop {
    let out = auto_exec(&api, &rt, &mut s, RespCommand::Hscan, &[b"h", &cursor]);
    let text = String::from_utf8(out).unwrap();
    let mut parts = text.split("\r\n");
    assert_eq!(parts.next(), Some("*2"));
    let _ = parts.next().unwrap();
    let next_cursor = parts.next().unwrap().to_string();
    let arr_len: usize = parts
      .next()
      .unwrap()
      .trim_start_matches('*')
      .parse()
      .unwrap();
    seen_fields += arr_len;
    let _ = parts.take(arr_len * 2).count();
    if next_cursor == "0" {
      break;
    }
    cursor = next_cursor.into_bytes();
  }
  assert_eq!(seen_fields, h_total * 2, "HSCAN 应回全量 field/value 对");
}

/// 分层树 HRANDFIELD 只读抽样臂矩阵（票 zcode-r151c-smembers 案一，
/// collection.md §8.5 SRANDMEMBER 树内臂对偶）：真正 bftree 大 hash
/// （2×阈值灌水自动升阶）× 无 count/正 count/负 count/WITHVALUES ×
/// RESP2/RESP3 与信封态小 hash 应答集合等价，确定性帧（count 0、错误帧、
/// 缺键 null、互异全量域）逐字节锁头。两态随机源独立（§12 不承诺同 seed
/// 同序），本用例锁帧形/条数/值域/配对契约面，非具体抽样位点。
///
/// 成员级 TTL 段沿用 zte 预烘到期形（水位随灌入批落真实最早值）：抽样域
/// 收敛至存活集、声明头恒等实发，剔除不固化——出账仍归计数校正臂（HLEN）。
///
/// 测试侧独立递归帧解析器（与被测写侧代码异源），解析后另验全帧消费：
/// 声明头 > 实发即解析越界 panic，声明头 < 实发即残留 assert 红。
#[test]
fn test_tiered_hash_random_field_tree_arm_matrix() {
  #[derive(Debug)]
  enum Rd {
    Bulk(Vec<u8>),
    Null,
    Arr(Vec<Rd>),
  }
  fn px(b: &[u8], i: &mut usize) -> Rd {
    assert!(*i < b.len(), "帧在偏移 {i} 提前耗尽: {b:?}");
    let t = b[*i];
    *i += 1;
    let nl = b[*i..]
      .iter()
      .position(|&c| c == b'\n')
      .unwrap_or_else(|| panic!("缺行终止: {b:?}"))
      + *i;
    let line = &b[*i..nl - 1];
    *i = nl + 1;
    match t {
      b'$' => {
        let len: i64 = from_utf8(line).unwrap().parse().unwrap();
        if len < 0 {
          Rd::Null
        } else {
          let l = len as usize;
          assert!(
            *i + l + 2 <= b.len() && &b[*i + l..*i + l + 2] == b"\r\n",
            "bulk 体越界: {b:?}"
          );
          let v = b[*i..*i + l].to_vec();
          *i += l + 2;
          Rd::Bulk(v)
        }
      }
      b'*' => {
        let n: i64 = from_utf8(line).unwrap().parse().unwrap();
        if n < 0 {
          Rd::Null
        } else {
          let mut v = Vec::with_capacity(n as usize);
          for _ in 0..n {
            v.push(px(b, i));
          }
          Rd::Arr(v)
        }
      }
      b'_' => Rd::Null,
      b'+' | b'-' | b':' => Rd::Bulk(line.to_vec()),
      other => panic!("未知帧首 {} @ {i}", other as char),
    }
  }
  fn parse1(out: &[u8], ctxmsg: &str) -> Rd {
    let mut pos = 0usize;
    let f = px(out, &mut pos);
    assert_eq!(
      pos,
      out.len(),
      "{ctxmsg}: 声明头与实发不符（越界/残留）raw={out:?}"
    );
    f
  }
  fn as_arr<'a>(f: &'a Rd, ctxmsg: &str) -> &'a [Rd] {
    match f {
      Rd::Arr(v) => &v[..],
      other => panic!("{ctxmsg} 非数组帧: {other:?}"),
    }
  }
  fn as_bulk<'a>(f: &'a Rd, ctxmsg: &str) -> &'a [u8] {
    match f {
      Rd::Bulk(v) => &v[..],
      other => panic!("{ctxmsg} 非 bulk 帧: {other:?}"),
    }
  }

  let (rt, api, store, _dir) = open_env("hrandfield-tree-arm.db");
  let mut s = session_with(&api);
  let mut buf = ItoaBuffer::new();

  // ---- 分层大 hash：2×阈值 灌水自动升阶（§8.3 信封上界）
  let total = 2 * wcol::TIERED_PROMOTE_THRESHOLD;
  bulk_fill(
    &api,
    &rt,
    &mut s,
    RespCommand::Hset,
    b"th",
    total,
    |i, buf| vec![prefixed(b'f', buf, i), prefixed(b'v', buf, i)],
  );
  let (meta, _stub) = rt
    .block_on(store.new_session().unwrap().load_collection_stub(b"th"))
    .unwrap()
    .expect("th 须经灌水自动升阶为分层态");
  assert_eq!(meta.size, total as u64, "升阶后 meta.size 须为灌入总数");

  // ---- 信封对照 hash：32 字段（阈下永不升阶）
  let mh_total = 32usize;
  let mut args: Vec<Vec<u8>> = vec![b"mh".to_vec()];
  for i in 1..=mh_total {
    args.push(prefixed(b'f', &mut buf, i));
    args.push(prefixed(b'v', &mut buf, i));
  }
  let argv: Vec<&[u8]> = args.iter().map(|v| &v[..]).collect();
  auto_exec(&api, &rt, &mut s, RespCommand::Hset, &argv);
  assert!(
    rt.block_on(store.new_session().unwrap().load_collection_stub(b"mh"))
      .unwrap()
      .is_none(),
    "mh 须保持信封态"
  );

  // f<i>→v<i> 确定性配对（WITHVALUES 保真判据）
  let expect_val = |f: &[u8]| -> Vec<u8> {
    assert_eq!(f[0], b'f', "抽样字段须落在 f<i>/v<i> 值域: {f:?}");
    let mut v = Vec::with_capacity(f.len());
    v.push(b'v');
    v.extend_from_slice(&f[1..]);
    v
  };
  // 存活探针：HGET 命中 bulk ⟺ 字段域内且未到期（两态 Hget 臂同做成员级
  // 剔除，异臂交叉验证，不依赖被测抽样臂自身）
  let assert_alive =
    |api: &GarnetApi, rt: &Runtime, s: &mut RespServerSession, key: &[u8], f: &[u8]| {
      let o = auto_exec(api, rt, s, RespCommand::Hget, &[key, f]);
      assert!(
        o.starts_with(b"$") && !o.starts_with(b"$-1"),
        "抽样字段 {f:?} ∈ {key:?} 须可 HGET 命中存活: {o:?}",
      );
    };

  let mut space: HashSet<Vec<u8>> = HashSet::new();
  for i in 1..=total {
    let mut b2 = ItoaBuffer::new();
    space.insert(prefixed(b'f', &mut b2, i));
  }
  let mut space32: HashSet<Vec<u8>> = HashSet::new();
  for i in 1..=mh_total {
    let mut b2 = ItoaBuffer::new();
    space32.insert(prefixed(b'f', &mut b2, i));
  }

  for ver in [2u8, 3u8] {
    s.resp_protocol_version = ver;
    for key in [b"th".as_slice(), b"mh".as_slice()] {
      let tag = if key == b"th".as_slice() {
        "分层"
      } else {
        "信封"
      };

      // ---- 无 count 单成员形：bulk 且域内存活（位点各自随机，§12 独立源）
      let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[key]);
      assert!(out.starts_with(b"$"), "{tag} 无 count 形须 bulk: {out:?}");
      let frame = parse1(&out, &format!("{tag} 无count ver={ver}"));
      let f = as_bulk(&frame, "无count 字段").to_vec();
      assert_alive(&api, &rt, &mut s, key, &f);

      // ---- 正 count 3：帧头 *3 逐字节锁，互异不重样、域内存活
      let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[key, b"3"]);
      assert_eq!(
        &out[..4],
        b"*3\r\n",
        "{tag} 正 count 帧头逐字节锁 ver={ver}"
      );
      let frame = parse1(&out, &format!("{tag} count3 ver={ver}"));
      let items = as_arr(&frame, "count3");
      assert_eq!(items.len(), 3);
      let mut uniq: HashSet<Vec<u8>> = HashSet::new();
      for it in items {
        let f = as_bulk(it, "字段项").to_vec();
        assert!(uniq.insert(f.clone()), "互异 count 形不得重样: {f:?}");
        assert_alive(&api, &rt, &mut s, key, &f);
      }

      // ---- WITHVALUES：RESP2 平铺 2n 头、RESP3 每项 *2 对帧，配对保真
      let out = auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Hrandfield,
        &[key, b"3", b"WITHVALUES"],
      );
      let frame = parse1(&out, &format!("{tag} HWV ver={ver}"));
      if ver == 2 {
        assert_eq!(&out[..4], b"*6\r\n", "{tag} RESP2 WITHVALUES 平铺 2n 头锁");
        let items = as_arr(&frame, "RESP2 HWV");
        assert_eq!(items.len(), 6);
        for j in 0..3 {
          let f = as_bulk(&items[j * 2], "HWV 字段项").to_vec();
          let v = as_bulk(&items[j * 2 + 1], "HWV 值项");
          assert_eq!(v, expect_val(&f), "WITHVALUES 须 f<i>→v<i> 配对: {f:?}");
          assert_alive(&api, &rt, &mut s, key, &f);
        }
      } else {
        assert_eq!(&out[..4], b"*3\r\n", "{tag} RESP3 WITHVALUES 外层头锁");
        let items = as_arr(&frame, "RESP3 HWV");
        assert_eq!(items.len(), 3);
        for it in items {
          let pair = as_arr(it, "RESP3 每项对帧");
          assert_eq!(pair.len(), 2, "RESP3 WITHVALUES 每项 *2 对帧");
          let f = as_bulk(&pair[0], "HWV 字段项").to_vec();
          let v = as_bulk(&pair[1], "HWV 值项");
          assert_eq!(v, expect_val(&f), "WITHVALUES 须 f<i>→v<i> 配对: {f:?}");
          assert_alive(&api, &rt, &mut s, key, &f);
        }
      }

      // ---- 负 count -4：可重复形帧头恒 *4（RESP2 HWV *8 / RESP3 *4 对帧）
      let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[key, b"-4"]);
      assert_eq!(
        &out[..4],
        b"*4\r\n",
        "{tag} 负 count 帧头逐字节锁 ver={ver}"
      );
      let frame = parse1(&out, &format!("{tag} neg ver={ver}"));
      for it in as_arr(&frame, "neg") {
        let f = as_bulk(it, "neg 字段项").to_vec();
        assert!(
          space.contains(&f) || space32.contains(&f),
          "负 count 字段越域: {f:?}"
        );
        assert_alive(&api, &rt, &mut s, key, &f);
      }
      let out = auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Hrandfield,
        &[key, b"-4", b"WITHVALUES"],
      );
      let head = if ver == 2 { "*8\r\n" } else { "*4\r\n" };
      assert_eq!(
        &out[..head.len()],
        head.as_bytes(),
        "{tag} 负 count HWV 头锁 ver={ver}"
      );
      let frame = parse1(&out, &format!("{tag} negHWV ver={ver}"));
      let items = as_arr(&frame, "negHWV");
      if ver == 2 {
        assert_eq!(items.len(), 8);
        for j in 0..4 {
          let f = as_bulk(&items[j * 2], "negHWV 字段项").to_vec();
          assert_eq!(as_bulk(&items[j * 2 + 1], "negHWV 值项"), expect_val(&f));
        }
      } else {
        assert_eq!(items.len(), 4);
        for it in items {
          let pair = as_arr(it, "negHWV 对帧");
          assert_eq!(pair.len(), 2);
          let f = as_bulk(&pair[0], "negHWV 字段项").to_vec();
          assert_eq!(as_bulk(&pair[1], "negHWV 值项"), expect_val(&f));
        }
      }

      // ---- count 0：不触后端，双态双版本 *0 逐字节锁（含 WITHVALUES 尾缀）
      for extra in [None, Some(&b"WITHVALUES"[..])] {
        let mut a: Vec<&[u8]> = vec![key, b"0"];
        if let Some(e) = extra {
          a.push(e);
        }
        let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &a);
        assert_eq!(out, b"*0\r\n", "{tag} count 0 短路帧锁 args={a:?}");
      }

      // ---- 互异钳制域：count ≥ size → 恰全集，帧头字节锁 + 集合等价
      if key == b"th".as_slice() {
        let narg = buf.format(total).as_bytes().to_vec();
        let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[key, &narg]);
        let hdr = format!("*{total}\r\n");
        assert_eq!(
          &out[..hdr.len()],
          hdr.as_bytes(),
          "分层全量域钳制帧头逐字节锁"
        );
        let frame = parse1(&out, "th clamp");
        let items = as_arr(&frame, "th clamp");
        assert_eq!(items.len(), total);
        let mut got: HashSet<Vec<u8>> = HashSet::new();
        for it in items {
          let f = as_bulk(it, "clamp 字段项").to_vec();
          assert!(got.insert(f.clone()), "互异全量域不得重样: {f:?}");
          assert!(space.contains(&f), "clamp 字段越域: {f:?}");
        }
        assert_eq!(got, space, "HRANDFIELD 全量域须与灌入字段域集合等价");
      } else {
        let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[key, b"40"]);
        assert_eq!(
          &out[..5],
          b"*32\r\n",
          "信封态 count 越界须钳至 size（帧头锁）"
        );
        let frame = parse1(&out, "mh clamp");
        let items = as_arr(&frame, "mh clamp");
        let mut got: HashSet<Vec<u8>> = HashSet::new();
        for it in items {
          got.insert(as_bulk(it, "clamp32 字段项").to_vec());
        }
        assert_eq!(got, space32, "信封态全量域集合等价");
      }
    }

    // ---- 错误帧双态逐字节全等（单源上游解析器，同源即同帧）
    let err_int = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"th", b"xx"]);
    assert_eq!(
      err_int,
      b"-ERR value is not an integer or out of range.\r\n"
    );
    assert_eq!(
      err_int,
      auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"mh", b"xx"]),
      "非整数错误帧分层/信封须同字节"
    );
    assert_eq!(
      err_int,
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Hrandfield,
        &[b"th", b"99999999999999"]
      ),
      "int32 越界同 VALUE_IS_NOT_INTEGER 帧（C# TryGetInt 口径）"
    );
    let err_syn = auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hrandfield,
      &[b"th", b"3", b"WITHSCORES"],
    );
    assert_eq!(err_syn, b"-ERR syntax error\r\n");
    assert_eq!(
      err_syn,
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Hrandfield,
        &[b"mh", b"3", b"WITHSCORES"]
      ),
      "第三词元语法门错误帧双态同字节"
    );
    let err_ar = auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Hrandfield,
      &[b"th", b"1", b"2", b"3"],
    );
    assert!(
      err_ar.starts_with(b"-ERR "),
      "arity 越界须错误帧: {err_ar:?}"
    );
    assert_eq!(
      err_ar,
      auto_exec(
        &api,
        &rt,
        &mut s,
        RespCommand::Hrandfield,
        &[b"mh", b"1", b"2", b"3"]
      ),
      "arity 错误帧双态同字节"
    );

    // ---- 缺键：版本感知 null 与 *0 逐字节锁（双态共用短路出口）
    let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"nope"]);
    assert_eq!(
      &out[..],
      if ver == 2 {
        &b"$-1\r\n"[..]
      } else {
        &b"_\r\n"[..]
      },
      "缺键无 count 形版本感知 null 锁"
    );
    let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"nope", b"5"]);
    assert_eq!(out, b"*0\r\n", "缺键带 count 形空数组锁");
  }

  // ---- 抽样电池为纯读：不触元记录、不触发降阶（§8.5 读侧口径）
  let (meta, _stub) = rt
    .block_on(store.new_session().unwrap().load_collection_stub(b"th"))
    .unwrap()
    .expect("抽样电池后 th 仍分层态");
  assert_eq!(
    meta.size, total as u64,
    "HRANDFIELD 树内臂不得改动 meta.size（不置脏）"
  );

  // ---- 随机源独立面（§12）：131072 字段域连抽 32 次无 count 形，
  // 全同概率 ≈ 131072^-31 ≈ 0，须见 ≥2 相异（锁随机起始键定位，
  // 非退化为固定树头命中）
  s.resp_protocol_version = 2;
  let mut seen: HashSet<Vec<u8>> = HashSet::new();
  for _ in 0..32 {
    let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"th"]);
    let frame = parse1(&out, "随机性抽样");
    seen.insert(as_bulk(&frame, "随机性字段").to_vec());
  }
  assert!(
    seen.len() >= 2,
    "同一树连抽 32 次位点须呈随机散布（随机起始键）: {seen:?}"
  );

  // ---- 成员级 TTL：抽样域收敛至存活集，剔除不固化（§53 读侧口径）。
  // fixture 沿用 zte 预烘形：1..=64 中 4 的倍数共 16 条即时到期，水位随
  // 灌入批落真实最早值（到期出账归后续 HLEN 计数校正臂）
  let past = now_ticks() - 60 * TICKS_PER_SECOND;
  let mut ents: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(64);
  for i in 1..=64usize {
    let f = prefixed(b'f', &mut buf, i);
    let v = prefixed(b'v', &mut buf, i);
    let expiry = if i % 4 == 0 { Some(past) } else { None };
    ents.push((f, encode_member(&v, expiry)));
  }
  let next_expiry = ents
    .iter()
    .filter_map(|(_, record)| decode_member(record).0)
    .min()
    .unwrap_or(i64::MAX);
  rt.block_on(store.new_session().unwrap().promote_collection_to_bftree(
    b"thx",
    GarnetObjectType::Hash,
    ents,
    next_expiry,
    false,
  ))
  .unwrap();
  // 正 count 超存活域：诚实短供，声明头恒等实发（TTL 二次遍历去重形）
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"thx", b"64"]);
  assert_eq!(&out[..5], b"*48\r\n", "TTL 短供：互异形帧头收敛至存活数");
  let frame = parse1(&out, "thx 64");
  let items = as_arr(&frame, "thx 64");
  assert_eq!(items.len(), 48);
  let mut uniq: HashSet<Vec<u8>> = HashSet::new();
  for it in items {
    let f = as_bulk(it, "存活字段").to_vec();
    assert!(
      uniq.insert(f.clone()),
      "TTL 短供回绕亦不得重样（分段互斥）: {f:?}"
    );
    assert_alive(&api, &rt, &mut s, b"thx", &f);
  }
  // 越 meta.size 钳制后同域收敛（min(200,64) 再 TTL 滤）
  let out = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Hrandfield,
    &[b"thx", b"200"],
  );
  assert_eq!(&out[..5], b"*48\r\n", "越 meta.size 钳制后 TTL 收敛帧头锁");
  let frame = parse1(&out, "thx 200");
  assert_eq!(as_arr(&frame, "thx 200").len(), 48);
  // RESP2 WITHVALUES：平铺头 *96 且配对全存活（到期成员不入帧）
  let out = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Hrandfield,
    &[b"thx", b"64", b"WITHVALUES"],
  );
  assert_eq!(&out[..5], b"*96\r\n", "TTL 短供 RESP2 WITHVALUES 2n 头锁");
  let frame = parse1(&out, "thx HWV 短供");
  let items = as_arr(&frame, "thx HWV");
  assert_eq!(items.len(), 96);
  for j in 0..48 {
    let f = as_bulk(&items[j * 2], "HWV 存活字段").to_vec();
    assert_eq!(as_bulk(&items[j * 2 + 1], "HWV 存活值项"), expect_val(&f));
    assert_alive(&api, &rt, &mut s, b"thx", &f);
  }
  // 负 count（可重复）亦只在存活域收敛
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"thx", b"-5"]);
  assert_eq!(&out[..4], b"*5\r\n", "TTL 负 count 帧头锁");
  let frame = parse1(&out, "thx -5");
  for it in as_arr(&frame, "thx -5") {
    assert_alive(&api, &rt, &mut s, b"thx", as_bulk(it, "neg 存活字段"));
  }
  // 无 count 形 16 连抽全存活（HGET 异臂探针）
  for _ in 0..16 {
    let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"thx"]);
    assert!(
      out.starts_with(b"$"),
      "TTL 树无 count 形须命中存活 bulk: {out:?}"
    );
    let frame = parse1(&out, "thx 单抽");
    assert_alive(&api, &rt, &mut s, b"thx", as_bulk(&frame, "单抽字段"));
  }
  // 剔除不固化：抽样电池后元记录 size 仍为灌入物理值（本臂零置脏零出账）
  let (meta, _stub) = rt
    .block_on(store.new_session().unwrap().load_collection_stub(b"thx"))
    .unwrap()
    .expect("thx 抽样电池后仍分层态");
  assert_eq!(
    meta.size, 64,
    "HRANDFIELD 剔除不固化：出账归计数臂，本臂零置脏"
  );
  // 对照口径：水位越过的出账校正仍由 HLEN 计数臂完成（与本臂分工相承）
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Hlen, &[b"thx"]),
    b":48\r\n",
    "计数校正臂照常出账（HRANDFIELD 未替其固化）"
  );

  // ---- 全到期树：count 形诚实短帧 *0、无 count 形版本感知 null
  //（对标对象层 purge 归零形）
  let mut ents: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(16);
  let all_past = past - 10 * TICKS_PER_SECOND;
  for i in 1..=16usize {
    let f = prefixed(b'f', &mut buf, i);
    let v = prefixed(b'v', &mut buf, i);
    ents.push((f, encode_member(&v, Some(all_past))));
  }
  rt.block_on(store.new_session().unwrap().promote_collection_to_bftree(
    b"thz",
    GarnetObjectType::Hash,
    ents,
    all_past,
    false,
  ))
  .unwrap();
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"thz", b"3"]);
  assert_eq!(out, b"*0\r\n", "全到期树 count 形诚实空数组帧");
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"thz"]);
  assert_eq!(out, b"$-1\r\n", "全到期树无 count 形 RESP2 null 锁");
  s.resp_protocol_version = 3;
  let out = auto_exec(&api, &rt, &mut s, RespCommand::Hrandfield, &[b"thz"]);
  assert_eq!(out, b"_\r\n", "全到期树无 count 形 RESP3 null 锁");
  let out = auto_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Hrandfield,
    &[b"thz", b"3", b"WITHVALUES"],
  );
  assert_eq!(out, b"*0\r\n", "全到期树 WITHVALUES 形诚实空数组帧");
}
