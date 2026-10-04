//! 向量集十二命令 RESP2/RESP3 回复契约全量对标
//!
//! 在 garnet 中的相对路径: test/standalone/Garnet.test.vectorset/VectorSetProtocolTests.cs
//!
//! C# 以 RedisProtocol.Resp2/Resp3 双 fixture 覆盖十二命令的应答类型契约
//! （7398c0625 #2184）；rust 以真消费链 + `HELLO 3` 升级驱动双协议，逐字节
//! 锁帧型：VREM/VISMEMBER/VADD/VSETATTR 布尔族（RESP3 `#t/#f`、RESP2
//! `:1/:0`）、VSIM 检索族（RESP3 map / RESP2 扁平化、空属性 NULL）、VINFO
//! 7 键值 map（RESP3 `%7`、RESP2 `*14` 键值交错、数值纯整数）、VLINKS 邻居
//! 分组嵌套（无分数 `[id]`、WITHSCORES RESP2 `[id, score]` / RESP3
//! `{id: score}`）。
//!
//! 装配同 vector_key_domain_ops.rs（真存储 + bound_vector_manager + 真命令
//! 臂消费驱动）；每用例在 RESP2/RESP3 双协议下各跑一轮（C# fixture 参数化
//! 的 rust 形态）。

use std::{mem::forget, str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace,
  database::{SingleDatabaseManager, garnet_database::GarnetDatabase},
  resp::{
    RespSessionConsumer, garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions,
    vector::vector_manager::VectorManager,
  },
};
use wnode_test::{bound_vector_manager, pump, send_consumer_args};
use wtest_base::{resp_frame as encode_frame, test_store_config};

/// 会话消费者 + 向量登记表 + 存储句柄（vector_key_domain_ops.rs 同款装配）
fn consumer() -> (
  RespSessionConsumer,
  Arc<VectorManager>,
  Arc<WedbStore<SegmentedDevice>>,
) {
  let dir = tempfile::tempdir().unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("vec_protocol.db")).unwrap());
  let config = test_store_config();
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let session = store.new_session().unwrap();

  let (_vector_domain, vm) = bound_vector_manager(&store);

  let cp_dir = dir.path().join("cp");
  let db = Arc::new(GarnetDatabase::new(
    0,
    Arc::clone(&store),
    Arc::clone(&store.device),
    cp_dir.clone(),
    None,
  ));
  let mgr = Arc::new(SingleDatabaseManager::new(cp_dir, db));
  mgr.attach_vector_manager(Arc::clone(&vm));

  let api = StoreGarnetApi::new(session)
    .with_vector_manager(Arc::clone(&vm))
    .with_database_manager(mgr);
  let consumer = RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::new(api));
  forget(dir);
  (consumer, vm, store)
}

/// 单帧驱动（同步段 + 挂起慢路径闭环）
fn drive(rt: &Runtime, c: &mut RespSessionConsumer, frame: &[u8]) -> Vec<u8> {
  let (consumed, mut out) = pump(c, frame);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {frame:?}");
  if let Some(slow) = c.take_slow_wait() {
    let resolved = rt.block_on(slow.resolve());
    out.extend_from_slice(&resolved);
  }
  out
}

/// 断言应答文本
fn s(out: &[u8]) -> &str {
  from_utf8(out).unwrap()
}

/// 从应答缓冲切出首帧与其后剩余（RESP2/RESP3 全型递归长度解析）。
///
/// 单帧判型：`+ - : , ( _ #` 行帧读至 CRLF；`$<n>` 读 n+2 字节载荷
/// （`$-1` 无载荷）；`*<n>` / `~<n>` 读 n 个嵌套帧；`%<n>` 读 2n 个
/// 嵌套帧（map 键值对交错）。
fn split_frame(buf: &[u8]) -> (&[u8], &[u8]) {
  assert!(!buf.is_empty(), "空应答缓冲");
  let ty = buf[0];
  let (head_len, body_len, items) = match ty {
    b'+' | b'-' | b':' | b',' | b'(' | b'_' | b'#' => {
      let end = buf.windows(2).position(|w| w == b"\r\n").unwrap() + 2;
      (end, 0, 0)
    }
    b'$' => {
      let eol = buf.windows(2).position(|w| w == b"\r\n").unwrap();
      let n: i64 = from_utf8(&buf[1..eol]).unwrap().parse().unwrap();
      if n < 0 {
        (eol + 2, 0, 0)
      } else {
        (eol + 2, n as usize + 2, 0)
      }
    }
    b'*' | b'~' => {
      let eol = buf.windows(2).position(|w| w == b"\r\n").unwrap();
      let n: i64 = from_utf8(&buf[1..eol]).unwrap().parse().unwrap();
      (eol + 2, 0, n.max(0) as usize)
    }
    b'%' => {
      let eol = buf.windows(2).position(|w| w == b"\r\n").unwrap();
      let n: i64 = from_utf8(&buf[1..eol]).unwrap().parse().unwrap();
      (eol + 2, 0, 2 * n.max(0) as usize)
    }
    other => panic!("未支持的应答帧型 {other:#x}: {buf:?}"),
  };
  let mut rest = &buf[head_len..];
  for _ in 0..items {
    let (_, tail) = split_frame(rest);
    rest = tail;
  }
  if body_len > 0 {
    rest = &rest[body_len..];
  }
  (&buf[..buf.len() - rest.len()], rest)
}

/// 顶层帧切分（`*<n>` 数组 / `%<n>` map 的逐项视图）
fn top_items(reply: &[u8]) -> Vec<&[u8]> {
  let (first, rest) = split_frame(reply);
  assert!(rest.is_empty(), "应答应恰为一帧: {reply:?}");
  let ty = first[0];
  assert!(ty == b'*' || ty == b'%', "应答应为数组/map: {first:?}");
  let eol = first.windows(2).position(|w| w == b"\r\n").unwrap();
  let n: i64 = from_utf8(&first[1..eol]).unwrap().parse().unwrap();
  // `*<n>`/`~<n>` 数组 n 项；`%<n>` map 键值交错 2n 项
  let total = if ty == b'%' {
    2 * n.max(0) as usize
  } else {
    n.max(0) as usize
  };
  let mut items = Vec::with_capacity(total);
  let mut cursor = &first[eol + 2..];
  for _ in 0..total {
    let (item, tail) = split_frame(cursor);
    items.push(item);
    cursor = tail;
  }
  items
}

/// VADD 建集助手（对标 C# Add：VALUES 3 1 0 0 NOQUANT [SETATTR attr]）
fn add(rt: &Runtime, c: &mut RespSessionConsumer, key: &[u8], elem: &[u8], attr: Option<&[u8]>) {
  let mut args = vec![
    b"VADD".as_slice(),
    key,
    b"VALUES",
    b"3",
    b"1",
    b"0",
    b"0",
    elem,
    b"NOQUANT",
  ];
  if let Some(a) = attr {
    args.push(b"SETATTR");
    args.push(a);
  }
  let out = drive(rt, c, &encode_frame(&args));
  let expected: &[u8] = if out.first() == Some(&b'#') {
    b"#t\r\n"
  } else {
    b":1\r\n"
  };
  assert_eq!(out, expected, "VADD {key:?}/{elem:?} 应回真");
}

/// 十二命令对字符串键的 WRONGTYPE 对拍（C# WrongType；rust 全族统一无句点
/// 版文案，doc/zh/deviations.md §164）
#[test]
fn wrong_type_all_twelve_commands() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    for resp3 in [false, true] {
      let (mut c, _vm, _store) = consumer();
      if resp3 {
        // HELLO 3 经 ACL 停车臂域内联闭环（泵侧 parking 单点），应答为
        // RESP3 map，此处不比对
        send_consumer_args(&mut c, &[b"HELLO", b"3"]).await;
      }
      assert_eq!(
        drive(&rt, &mut c, &encode_frame(&[b"SET", b"string", b"v"])),
        b"+OK\r\n"
      );

      let commands: Vec<Vec<&[u8]>> = vec![
        vec![
          b"VADD", b"string", b"VALUES", b"3", b"1", b"0", b"0", b"first", b"NOQUANT",
        ],
        vec![b"VCARD", b"string"],
        vec![b"VDIM", b"string"],
        vec![b"VEMB", b"string", b"first"],
        vec![b"VGETATTR", b"string", b"first"],
        vec![b"VINFO", b"string"],
        vec![b"VISMEMBER", b"string", b"first"],
        vec![b"VLINKS", b"string", b"first"],
        vec![b"VRANDMEMBER", b"string"],
        vec![b"VREM", b"string", b"first"],
        vec![b"VSETATTR", b"string", b"first", b"{}"],
        vec![
          b"VSIM", b"string", b"VALUES", b"3", b"1", b"0", b"0", b"COUNT", b"2",
        ],
      ];
      for args in &commands {
        let out = drive(&rt, &mut c, &encode_frame(args));
        assert!(
          s(&out).starts_with("-WRONGTYPE"),
          "{args:?} 应回 WRONGTYPE: {out:?}"
        );
        // 连接存活（C# AssertConnectionAlive）
        assert_eq!(drive(&rt, &mut c, &encode_frame(&[b"PING"])), b"+PONG\r\n");
      }
    }
  })
}

/// 在 garnet 中的相对路径:VectorSetProtocolTests:VADD + VCARD + VDIM
#[test]
fn vadd_vcard_vdim_contract() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    for resp3 in [false, true] {
      let (mut c, _vm, _store) = consumer();
      if resp3 {
        // HELLO 3 经 ACL 停车臂域内联闭环（泵侧 parking 单点），应答为
        // RESP3 map，此处不比对
        send_consumer_args(&mut c, &[b"HELLO", b"3"]).await;
      }

      // VADD：RESP2 整数 1 / RESP3 布尔真
      let out = drive(
        &rt,
        &mut c,
        &encode_frame(&[
          b"VADD", b"vectors", b"VALUES", b"3", b"1", b"0", b"0", b"first", b"NOQUANT",
        ]),
      );
      assert_eq!(out, if resp3 { b"#t\r\n" } else { b":1\r\n" });
      assert_eq!(
        drive(&rt, &mut c, &encode_frame(&[b"VCARD", b"vectors"])),
        b":1\r\n"
      );

      // VCARD 缺失键 → :0
      assert_eq!(
        drive(&rt, &mut c, &encode_frame(&[b"VCARD", b"missing"])),
        b":0\r\n"
      );

      // VDIM 命中 → :3；缺失键 → 错误帧
      assert_eq!(
        drive(&rt, &mut c, &encode_frame(&[b"VDIM", b"vectors"])),
        b":3\r\n"
      );
      let err = drive(&rt, &mut c, &encode_frame(&[b"VDIM", b"missing"]));
      assert_eq!(err, b"-ERR Key not found\r\n");

      assert_eq!(drive(&rt, &mut c, &encode_frame(&[b"PING"])), b"+PONG\r\n");
    }
  })
}

/// 在 garnet 中的相对路径:VectorSetProtocolTests:VEMB / VEMBRawQ8
#[test]
fn vemb_and_raw_contract() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    for resp3 in [false, true] {
      let (mut c, _vm, _store) = consumer();
      if resp3 {
        // HELLO 3 经 ACL 停车臂域内联闭环（泵侧 parking 单点），应答为
        // RESP3 map，此处不比对
        send_consumer_args(&mut c, &[b"HELLO", b"3"]).await;
      }
      add(&rt, &mut c, b"vectors", b"first", None);

      // 普通形态：3 坐标数组，RESP2 bulk 串 / RESP3 双精度
      let out = drive(&rt, &mut c, &encode_frame(&[b"VEMB", b"vectors", b"first"]));
      let items = top_items(&out);
      assert_eq!(items.len(), 3, "VEMB 3 坐标: {out:?}");
      for item in &items {
        assert_eq!(
          item[0],
          if resp3 { b',' } else { b'$' },
          "坐标帧型: {item:?}"
        );
      }

      // RAW（NOQUANT → fp32）：[量名, 原始字节, 范数] 三项
      let out = drive(
        &rt,
        &mut c,
        &encode_frame(&[b"VEMB", b"vectors", b"first", b"RAW"]),
      );
      let items = top_items(&out);
      assert_eq!(items.len(), 3, "RAW fp32 三项: {out:?}");
      assert_eq!(items[0], b"+fp32\r\n", "量名简单字符串: {items:?}");
      assert_eq!(items[1][0], b'$', "原始字节 bulk 串: {items:?}");
      assert_eq!(items[2][0], if resp3 { b',' } else { b'$' }, "范数帧型");

      // 缺失元素/缺失键 → 空数组（双协议同型）
      let cases: [Vec<&[u8]>; 2] = [
        vec![b"VEMB", b"vectors", b"missing"],
        vec![b"VEMB", b"missing", b"first", b"RAW"],
      ];
      for args in cases {
        let out = drive(&rt, &mut c, &encode_frame(&args));
        assert_eq!(out, b"*0\r\n", "{args:?} 缺失应空数组");
      }

      // RAW（默认 Q8）：[q8, 原始字节, 范数, 量化范围] 四项
      let out = drive(
        &rt,
        &mut c,
        &encode_frame(&[
          b"VADD", b"q8set", b"VALUES", b"3", b"1", b"0", b"0", b"first", b"Q8",
        ]),
      );
      assert!(out == b"#t\r\n" || out == b":1\r\n", "VADD Q8: {out:?}");
      let out = drive(
        &rt,
        &mut c,
        &encode_frame(&[b"VEMB", b"q8set", b"first", b"RAW"]),
      );
      let items = top_items(&out);
      assert_eq!(items.len(), 4, "RAW q8 四项: {out:?}");
      assert_eq!(items[0], b"+q8\r\n", "量名: {items:?}");

      assert_eq!(drive(&rt, &mut c, &encode_frame(&[b"PING"])), b"+PONG\r\n");
    }
  })
}

/// 在 garnet 中的相对路径:VectorSetProtocolTests:VGETATTR
#[test]
fn vgetattr_null_contract() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    for resp3 in [false, true] {
      let (mut c, _vm, _store) = consumer();
      if resp3 {
        // HELLO 3 经 ACL 停车臂域内联闭环（泵侧 parking 单点），应答为
        // RESP3 map，此处不比对
        send_consumer_args(&mut c, &[b"HELLO", b"3"]).await;
      }
      add(&rt, &mut c, b"vectors", b"first", Some(b"{\"id\":1}"));
      add(&rt, &mut c, b"vectors", b"without-attributes", None);

      // 命中属性 → bulk 串
      assert_eq!(
        drive(
          &rt,
          &mut c,
          &encode_frame(&[b"VGETATTR", b"vectors", b"first"])
        ),
        b"$8\r\n{\"id\":1}\r\n"
      );
      // 缺失元素 / 缺失键 / 无属性元素 → null（RESP2 `$-1`、RESP3 `_`）
      let null: &[u8] = if resp3 { b"_\r\n" } else { b"$-1\r\n" };
      let cases: [Vec<&[u8]>; 3] = [
        vec![b"VGETATTR", b"vectors", b"missing"],
        vec![b"VGETATTR", b"missing", b"first"],
        vec![b"VGETATTR", b"vectors", b"without-attributes"],
      ];
      for args in cases {
        assert_eq!(
          drive(&rt, &mut c, &encode_frame(&args)),
          null,
          "{args:?} 应回 null"
        );
      }

      assert_eq!(drive(&rt, &mut c, &encode_frame(&[b"PING"])), b"+PONG\r\n");
    }
  })
}

/// 在 garnet 中的相对路径:VectorSetProtocolTests:VINFO
#[test]
fn vinfo_map_contract() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    for resp3 in [false, true] {
      let (mut c, _vm, _store) = consumer();
      if resp3 {
        // HELLO 3 经 ACL 停车臂域内联闭环（泵侧 parking 单点），应答为
        // RESP3 map，此处不比对
        send_consumer_args(&mut c, &[b"HELLO", b"3"]).await;
      }
      // REDUCE 2 / EF 41 / M 7：数值字段逐一锁定
      let out = drive(
        &rt,
        &mut c,
        &encode_frame(&[
          b"VADD", b"vectors", b"REDUCE", b"2", b"VALUES", b"3", b"1", b"0", b"0", b"first",
          b"NOQUANT", b"EF", b"41", b"M", b"7",
        ]),
      );
      assert!(out == b"#t\r\n" || out == b":1\r\n", "VADD: {out:?}");

      let info = drive(&rt, &mut c, &encode_frame(&[b"VINFO", b"vectors"]));
      // RESP3 `%7` map / RESP2 `*14` 键值交错扁平数组
      let head: &[u8] = if resp3 { b"%7" } else { b"*14" };
      assert_eq!(&info[..head.len()], head, "VINFO 头: {info:?}");
      let text = s(&info);
      // 数值字段为纯整数帧（RESP2/RESP3 同为 `:n`）
      assert!(
        text.contains("+input-vector-dimensions\r\n:3\r\n"),
        "{text}"
      );
      assert!(text.contains("+reduced-dimensions\r\n:2\r\n"), "{text}");
      assert!(
        text.contains("+build-exploration-factor\r\n:41\r\n"),
        "{text}"
      );
      assert!(text.contains("+num-links\r\n:7\r\n"), "{text}");
      assert!(text.contains("+size\r\n:1\r\n"), "{text}");
      assert!(text.contains("+quant-type\r\n+f32\r\n"), "{text}");
      assert!(text.contains("+distance-metric\r\n+l2\r\n"), "{text}");

      // 缺失键 → null 数组（RESP2 `*-1`、RESP3 `_`）
      let missing: &[u8] = if resp3 { b"_\r\n" } else { b"*-1\r\n" };
      assert_eq!(
        drive(&rt, &mut c, &encode_frame(&[b"VINFO", b"missing"])),
        missing
      );

      assert_eq!(drive(&rt, &mut c, &encode_frame(&[b"PING"])), b"+PONG\r\n");
    }
  })
}

/// 在 garnet 中的相对路径:VectorSetProtocolTests:VISMEMBER + VREM + VSETATTR
#[test]
fn boolean_family_contract() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    for resp3 in [false, true] {
      let (mut c, _vm, _store) = consumer();
      if resp3 {
        // HELLO 3 经 ACL 停车臂域内联闭环（泵侧 parking 单点），应答为
        // RESP3 map，此处不比对
        send_consumer_args(&mut c, &[b"HELLO", b"3"]).await;
      }
      add(&rt, &mut c, b"vectors", b"first", None);

      let (t, f): (&[u8], &[u8]) = if resp3 {
        (b"#t\r\n", b"#f\r\n")
      } else {
        (b":1\r\n", b":0\r\n")
      };

      // VISMEMBER
      assert_eq!(
        drive(
          &rt,
          &mut c,
          &encode_frame(&[b"VISMEMBER", b"vectors", b"first"])
        ),
        t
      );
      assert_eq!(
        drive(
          &rt,
          &mut c,
          &encode_frame(&[b"VISMEMBER", b"vectors", b"missing"])
        ),
        f
      );
      assert_eq!(
        drive(
          &rt,
          &mut c,
          &encode_frame(&[b"VISMEMBER", b"missing", b"first"])
        ),
        f
      );

      // VREM：命中真、复删假、缺键假
      assert_eq!(
        drive(&rt, &mut c, &encode_frame(&[b"VREM", b"vectors", b"first"])),
        t
      );
      assert_eq!(
        drive(&rt, &mut c, &encode_frame(&[b"VREM", b"vectors", b"first"])),
        f
      );
      assert_eq!(
        drive(&rt, &mut c, &encode_frame(&[b"VREM", b"missing", b"first"])),
        f
      );

      // VSETATTR：命中真、缺元素假、缺键假
      add(&rt, &mut c, b"vectors", b"back", None);
      assert_eq!(
        drive(
          &rt,
          &mut c,
          &encode_frame(&[b"VSETATTR", b"vectors", b"back", b"{\"id\":1}"])
        ),
        t
      );
      assert_eq!(
        drive(
          &rt,
          &mut c,
          &encode_frame(&[b"VSETATTR", b"vectors", b"missing", b"{\"id\":1}"])
        ),
        f
      );
      assert_eq!(
        drive(
          &rt,
          &mut c,
          &encode_frame(&[b"VSETATTR", b"missing", b"first", b"{\"id\":1}"])
        ),
        f
      );
      assert_eq!(
        drive(
          &rt,
          &mut c,
          &encode_frame(&[b"VGETATTR", b"vectors", b"back"])
        ),
        b"$8\r\n{\"id\":1}\r\n"
      );

      assert_eq!(drive(&rt, &mut c, &encode_frame(&[b"PING"])), b"+PONG\r\n");
    }
  })
}

/// 在 garnet 中的相对路径:VectorSetProtocolTests:VLINKS
#[test]
fn vlinks_nested_contract() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    for resp3 in [false, true] {
      let (mut c, _vm, _store) = consumer();
      if resp3 {
        // HELLO 3 经 ACL 停车臂域内联闭环（泵侧 parking 单点），应答为
        // RESP3 map，此处不比对
        send_consumer_args(&mut c, &[b"HELLO", b"3"]).await;
      }
      add(&rt, &mut c, b"vectors", b"element-0", None);

      // 单元素（无邻居）：VLINKS 空数组、WITHSCORES 同
      assert_eq!(
        drive(
          &rt,
          &mut c,
          &encode_frame(&[b"VLINKS", b"vectors", b"element-0"])
        ),
        b"*0\r\n"
      );
      assert_eq!(
        drive(
          &rt,
          &mut c,
          &encode_frame(&[b"VLINKS", b"vectors", b"element-0", b"WITHSCORES"])
        ),
        b"*0\r\n"
      );

      // 补邻居（16 个，保证 element-0 层 0 出边）
      for i in 1..16 {
        let elem = format!("element-{i}");
        add(&rt, &mut c, b"vectors", elem.as_bytes(), None);
      }

      // 缺失键 → null
      let missing: &[u8] = if resp3 { b"_\r\n" } else { b"$-1\r\n" };
      assert_eq!(
        drive(
          &rt,
          &mut c,
          &encode_frame(&[b"VLINKS", b"missing", b"element-0"])
        ),
        missing
      );

      // 无分数：每邻居单元素数组 [id]
      let links = drive(
        &rt,
        &mut c,
        &encode_frame(&[b"VLINKS", b"vectors", b"element-0"]),
      );
      let items = top_items(&links);
      assert!(!items.is_empty(), "邻居数应大于 0: {links:?}");
      for item in &items {
        assert_eq!(item[0], b'*', "无分数每邻居为数组: {item:?}");
        let nested = top_items(item);
        assert_eq!(nested.len(), 1, "单元素 [id]: {item:?}");
        assert_eq!(nested[0][0], b'$', "邻居 id 为 bulk 串: {item:?}");
      }

      // WITHSCORES：RESP2 每邻居 [id, score] 数组 / RESP3 {id: score} map
      let scored = drive(
        &rt,
        &mut c,
        &encode_frame(&[b"VLINKS", b"vectors", b"element-0", b"WITHSCORES"]),
      );
      let items = top_items(&scored);
      assert_eq!(items.len(), top_items(&links).len(), "双形态邻居数一致");
      for item in &items {
        assert_eq!(
          item[0],
          if resp3 { b'%' } else { b'*' },
          "WITHSCORES 帧型: {item:?}"
        );
        let pair = top_items(item);
        assert_eq!(pair.len(), 2, "id/score 成对: {item:?}");
        assert_eq!(pair[0][0], b'$', "id 为 bulk 串: {item:?}");
        assert_eq!(
          pair[1][0],
          if resp3 { b',' } else { b'$' },
          "score 帧型: {item:?}"
        );
      }

      assert_eq!(drive(&rt, &mut c, &encode_frame(&[b"PING"])), b"+PONG\r\n");
    }
  })
}

/// 在 garnet 中的相对路径:VectorSetProtocolTests:VRANDMEMBER
#[test]
fn vrandmember_null_and_array_contract() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    for resp3 in [false, true] {
      let (mut c, _vm, _store) = consumer();
      if resp3 {
        // HELLO 3 经 ACL 停车臂域内联闭环（泵侧 parking 单点），应答为
        // RESP3 map，此处不比对
        send_consumer_args(&mut c, &[b"HELLO", b"3"]).await;
      }
      let null: &[u8] = if resp3 { b"_\r\n" } else { b"$-1\r\n" };

      // 缺失键：无 count → null；有 count → 空数组
      assert_eq!(
        drive(&rt, &mut c, &encode_frame(&[b"VRANDMEMBER", b"missing"])),
        null
      );
      assert_eq!(
        drive(
          &rt,
          &mut c,
          &encode_frame(&[b"VRANDMEMBER", b"missing", b"2"])
        ),
        b"*0\r\n"
      );

      add(&rt, &mut c, b"vectors", b"first", None);
      add(&rt, &mut c, b"vectors", b"second", None);

      // 无 count → 单 bulk 成员
      let member = drive(&rt, &mut c, &encode_frame(&[b"VRANDMEMBER", b"vectors"]));
      assert_eq!(member[0], b'$', "单成员 bulk 串: {member:?}");
      let text = s(&member);
      assert!(text.contains("first") || text.contains("second"), "{text}");

      // 有 count → 数组 of bulk
      let members = drive(
        &rt,
        &mut c,
        &encode_frame(&[b"VRANDMEMBER", b"vectors", b"2"]),
      );
      let items = top_items(&members);
      assert_eq!(items.len(), 2, "count 2 取 2: {members:?}");
      for item in &items {
        assert_eq!(item[0], b'$', "成员 bulk 串: {item:?}");
      }

      assert_eq!(drive(&rt, &mut c, &encode_frame(&[b"PING"])), b"+PONG\r\n");
    }
  })
}

/// 在 garnet 中的相对路径:VectorSetProtocolTests:VSIM + VSIMFilteredComplexReplies +
/// VSIMMissingAttribute
#[test]
fn vsim_reply_contract() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    for resp3 in [false, true] {
      let (mut c, _vm, _store) = consumer();
      if resp3 {
        // HELLO 3 经 ACL 停车臂域内联闭环（泵侧 parking 单点），应答为
        // RESP3 map，此处不比对
        send_consumer_args(&mut c, &[b"HELLO", b"3"]).await;
      }
      add(&rt, &mut c, b"vectors", b"first", Some(b"{\"id\":1}"));
      add(&rt, &mut c, b"vectors", b"second", Some(b"{\"id\":2}"));

      let search = |extra: &[&[u8]]| {
        let mut args = vec![
          b"VSIM".as_slice(),
          b"vectors",
          b"VALUES",
          b"3",
          b"1",
          b"0",
          b"0",
          b"COUNT",
          b"2",
          b"EF",
          b"40",
        ];
        args.extend_from_slice(extra);
        encode_frame(&args)
      };

      // 仅 id：双协议均为扁平数组（C# SearchFields.Ids RESP3 亦为 Array；
      // 各元素向量同值次序不定，按成员断言）
      let ids = drive(&rt, &mut c, &search(&[]));
      let items = top_items(&ids);
      assert_eq!(items.len(), 2, "仅 id 两成员: {ids:?}");
      let text = s(&ids);
      assert!(
        text.contains("$5\r\nfirst\r\n") && text.contains("$6\r\nsecond\r\n"),
        "{text}"
      );
      for item in &items {
        assert_eq!(item[0], b'$', "id 为 bulk 串: {item:?}");
      }

      // WITHSCORES：RESP2 扁平 id/score 交错（score bulk 串）；RESP3 map id → double
      let scored = drive(&rt, &mut c, &search(&[b"WITHSCORES"]));
      if resp3 {
        assert_eq!(&scored[..2], b"%2", "RESP3 map 头: {scored:?}");
        let items = top_items(&scored);
        for pair in items.chunks(2) {
          assert_eq!(pair[0][0], b'$', "map 键为 id: {pair:?}");
          assert_eq!(pair[1][0], b',', "map 值为 double: {pair:?}");
        }
      } else {
        assert_eq!(&scored[..4], b"*4\r\n", "RESP2 扁平头: {scored:?}");
        for item in top_items(&scored).chunks(2) {
          assert_eq!(item[0][0], b'$', "id: {item:?}");
          assert_eq!(item[1][0], b'$', "score bulk 串: {item:?}");
        }
      }

      // WITHATTRIBS + WITHSCORES：RESP3 map 值为 [score, attr] 数组；RESP2 三元扁平
      let both = drive(&rt, &mut c, &search(&[b"WITHATTRIBS", b"WITHSCORES"]));
      if resp3 {
        let items = top_items(&both);
        for pair in items.chunks(2) {
          assert_eq!(pair[1][0], b'*', "map 值为 [score, attr] 数组: {pair:?}");
          let nested = top_items(pair[1]);
          assert_eq!(nested.len(), 2);
          assert_eq!(nested[1][0], b'$', "attr bulk 串: {pair:?}");
        }
      } else {
        assert_eq!(&both[..4], b"*6\r\n", "RESP2 三元扁平头: {both:?}");
        assert!(s(&both).contains("$8\r\n{\"id\":1}\r\n"), "{both:?}");
      }

      // 缺失键：全部选项组合空数组
      let flag_sets: [Vec<&[u8]>; 4] = [
        vec![],
        vec![b"WITHSCORES"],
        vec![b"WITHATTRIBS"],
        vec![b"WITHSCORES", b"WITHATTRIBS"],
      ];
      for flags in flag_sets {
        let mut args = vec![
          b"VSIM".as_slice(),
          b"missing",
          b"VALUES",
          b"3",
          b"1",
          b"0",
          b"0",
          b"COUNT",
          b"2",
        ];
        args.extend_from_slice(&flags);
        assert_eq!(
          drive(&rt, &mut c, &encode_frame(&args)),
          b"*0\r\n",
          "{args:?}"
        );
      }

      // 过滤检索：FILTER .id == 2 → 仅 second（四种旗标组合逐项对拍）
      add(&rt, &mut c, b"vectors", b"third", Some(b"{\"id\":3}"));
      let filtered = |extra: &[&[u8]]| {
        let mut args = vec![
          b"VSIM".as_slice(),
          b"vectors",
          b"ELE",
          b"first",
          b"COUNT",
          b"3",
          b"EF",
          b"40",
          b"FILTER",
          b".id == 2",
        ];
        args.extend_from_slice(extra);
        encode_frame(&args)
      };
      let flag_sets: [Vec<&[u8]>; 4] = [
        vec![],
        vec![b"WITHSCORES"],
        vec![b"WITHATTRIBS"],
        vec![b"WITHSCORES", b"WITHATTRIBS"],
      ];
      for flags in flag_sets {
        let out = drive(&rt, &mut c, &filtered(&flags));
        let text = s(&out);
        assert!(
          text.contains("$6\r\nsecond\r\n"),
          "过滤应命中 second: {text}"
        );
        assert!(
          !text.contains("$5\r\nfirst\r\n") && !text.contains("$5\r\nthird\r\n"),
          "{text}"
        );
        if flags.iter().any(|f| f.eq_ignore_ascii_case(b"WITHATTRIBS")) {
          assert!(text.contains("$8\r\n{\"id\":2}\r\n"), "属性在位: {text}");
        }
        if resp3 && !flags.is_empty() {
          assert_eq!(&out[..2], b"%1", "RESP3 带旗标为单键值 map: {out:?}");
        }
      }

      // 缺失属性元素（second 无属性）：WITHATTRIBS + WITHSCORES 下属性位为 null
      add(&rt, &mut c, b"attrmix", b"first", Some(b"{\"id\":1}"));
      add(&rt, &mut c, b"attrmix", b"second", None);
      let out = drive(
        &rt,
        &mut c,
        &encode_frame(&[
          b"VSIM",
          b"attrmix",
          b"ELE",
          b"first",
          b"COUNT",
          b"2",
          b"WITHATTRIBS",
          b"WITHSCORES",
        ]),
      );
      if resp3 {
        assert_eq!(&out[..2], b"%2", "RESP3 map: {out:?}");
        // 缺失属性在 map 值数组 [score, attr] 内为 `_`（全应答唯一 null 帧）
        assert!(s(&out).contains("_\r\n"), "缺失属性应 null: {out:?}");
      } else {
        assert_eq!(&out[..4], b"*6\r\n", "RESP2 扁平: {out:?}");
        assert!(s(&out).contains("$-1\r\n"), "缺失属性应 $-1: {out:?}");
      }
      assert!(
        s(&out).contains("$8\r\n{\"id\":1}\r\n"),
        "命中属性在位: {out:?}"
      );

      // 过滤无命中：RESP2 空数组 / RESP3 带旗标为空 map（C# AssertSearchReply：
      // fields != Ids 时 RESP3 应答恒为 ResultType.Map，含空集）
      let out = drive(
        &rt,
        &mut c,
        &encode_frame(&[
          b"VSIM",
          b"vectors",
          b"ELE",
          b"first",
          b"COUNT",
          b"3",
          b"FILTER",
          b".id == 99",
          b"WITHSCORES",
          b"WITHATTRIBS",
        ]),
      );
      let empty: &[u8] = if resp3 { b"%0\r\n" } else { b"*0\r\n" };
      assert_eq!(out, empty, "过滤无命中应空集: {out:?}");

      assert_eq!(drive(&rt, &mut c, &encode_frame(&[b"PING"])), b"+PONG\r\n");
    }
  })
}
