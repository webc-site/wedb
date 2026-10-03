//! PUBSUB CHANNELS RESP 数组成帧完整性测试
//! （对标 garnet/libs/server/PubSub/SubscribeBroker.cs:GetChannels +
//! garnet/libs/server/Resp/PubSubCommands.cs:NetworkPUBSUB_CHANNELS）
//!
//! C# 契约：GetChannels 先物化一份 `List<ByteArrayWrapper>` 快照，
//! NetworkPUBSUB_CHANNELS 再以同一份 list 的 `Count` 落 RESP 数组长度头、
//! `foreach` 写出各元素——长度与实际元素严格取同一真源，天然一致。
//!
//! 本测试锚定该「数组长度前缀与实际 bulk string 个数绝对一致」的成帧不变式：
//! 以最小 RESP 客户端解析模型消费 `write_channels` 的输出缓冲，任何
//! 「声明 N 个元素却写出 N±1 个」都会令解析失败（后续帧被误吞 / 缓冲余量），
//! 即协议成帧损坏。并发压测在订阅表被密集增删（内层集合 is_empty 翻转、外层表
//! 键进出）的同时高频快照，验证修复后单趟收集在任何交错下都不破坏该不变式。

use std::{
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
  },
  thread,
};

use wpubsub::{subscribe_broker::SubscribeBroker, subscriber::PubSubMailbox};

/// 解析 `write_channels` 输出：`*N\r\n` 后必须恰好 N 个 `$len\r\n<data>\r\n`
/// 且整个缓冲被精确消费完毕；任何偏差（元素少于/多于声明、截断、余量、
/// 帧头非 `*`/`$`）都判定为成帧损坏，返回 None（模拟真实客户端解析失败/串包）。
fn parse_channels(buf: &[u8]) -> Option<Vec<Vec<u8>>> {
  /// 从 `from` 起定位 `\r\n`，返回 `\r` 下标
  fn crlf(buf: &[u8], from: usize) -> Option<usize> {
    let mut j = from;
    while j + 1 < buf.len() {
      if buf[j] == b'\r' && buf[j + 1] == b'\n' {
        return Some(j);
      }
      j += 1;
    }
    None
  }
  /// 解析一行十进制计数（数组头或 bulk 长度头，纯数字无内嵌 CRLF）
  fn read_len(buf: &[u8], from: usize) -> Option<(usize, usize)> {
    let nl = crlf(buf, from)?;
    let n: usize = from_utf8(&buf[from..nl]).ok()?.parse().ok()?;
    Some((n, nl + 2))
  }

  let mut i = 0;
  if *buf.get(i)? != b'*' {
    return None;
  }
  i += 1;
  let (count, next) = read_len(buf, i)?;
  i = next;

  let mut items = Vec::with_capacity(count);
  for _ in 0..count {
    if *buf.get(i)? != b'$' {
      return None;
    }
    i += 1;
    let (len, next) = read_len(buf, i)?;
    i = next;
    // 数据段 + 结尾 \r\n 必须完整存在
    let end = i.checked_add(len)?;
    if buf.len() < end + 2 {
      return None;
    }
    items.push(buf[i..end].to_vec());
    if buf.get(end).copied()? != b'\r' || buf.get(end + 1).copied()? != b'\n' {
      return None;
    }
    i = end + 2;
  }
  // 声明计数与实际写出必须一致：解析完后缓冲应被精确耗尽
  if i != buf.len() {
    return None;
  }
  Some(items)
}

/// 第 `i` 个测试通道名（纯 ASCII，无 \r\n，便于解析）
fn channel(i: usize) -> Vec<u8> {
  format!("ch{i}").into_bytes()
}

/// 第 `j` 个 churn（振荡）通道名
fn churn_channel(j: usize) -> Vec<u8> {
  format!("cx{j}").into_bytes()
}

/// 常驻通道数（始终有订阅者，撑起帧体规模）
const STABLE: usize = 24;
/// 变更线程数
const MUTATORS: usize = 6;
/// 振荡通道数：须为 MUTATORS 整数倍，令每个 churn 通道只被唯一属主增删，
/// 从而其内层订阅集合能在「非空↔空」间真正翻转（触发旧双趟跳过的关键）
const CHURN: usize = MUTATORS * 4;
/// 写线程数与每线程迭代次数
const WRITERS: usize = 4;
const WRITER_ITERS: usize = 60_000;

/// 单线程基准：无并发变更时，成帧与元素集必须精确匹配订阅表
#[test]
fn channels_array_frames_exactly_under_no_contention() {
  let broker = SubscribeBroker::<Arc<PubSubMailbox>>::new();
  let sink = Arc::new(PubSubMailbox::new(8));
  for i in 0..STABLE {
    assert!(broker.subscribe(&channel(i), 1, sink.clone()));
  }
  // 制造一个已无订阅者的通道：unsubscribe 仅摘内层订阅者集合元素，外层键仍
  // 驻留不删；ch900 不出现在 CHANNELS 快照由读侧 is_empty 过滤承接
  broker.subscribe(&channel(900), 2, sink.clone());
  broker.unsubscribe(&channel(900), 2);

  let mut out = Vec::new();
  broker.write_channels(&mut out, b"", None);
  let items = parse_channels(&out).expect("成帧必须完整");
  let mut got: Vec<Vec<u8>> = items;
  got.sort();
  let mut want: Vec<Vec<u8>> = (0..STABLE).map(channel).collect();
  want.sort();
  assert_eq!(got, want, "数组元素集合须与有订阅者的通道精确一致");

  // glob 过滤同样一致：ch1* 命中 ch1, ch10..ch19（本测试仅 0..STABLE 存在 ch1,ch10-ch19）
  let mut out_pat = Vec::new();
  broker.write_channels(&mut out_pat, b"", Some(b"ch1*"));
  let filtered = parse_channels(&out_pat).expect("过滤后成帧仍须完整");
  assert!(
    filtered.iter().all(|c| c.starts_with(b"ch1")),
    "过滤结果均须匹配 ch1*"
  );
  assert!(!filtered.is_empty());
}

/// 空表与纯空数组：`*0\r\n` 必须解析为 0 元素且被精确消费
#[test]
fn empty_table_writes_zero_length_frame() {
  let broker = SubscribeBroker::<Arc<PubSubMailbox>>::new();
  let mut out = Vec::new();
  broker.write_channels(&mut out, b"", None);
  assert_eq!(out, b"*0\r\n");
  assert_eq!(parse_channels(&out), Some(Vec::new()));
}

/// 解析模型自检：长度头与元素数不符必须被判定为损坏（回归护栏本身可信）
#[test]
fn parser_detects_length_element_mismatch() {
  // 声明 2 个元素、实际只有 1 个 bulk string → 解析须失败
  assert_eq!(parse_channels(b"*2\r\n$3\r\nabc\r\n"), None);
  // 声明 1 个元素、实际写出 2 个 → 缓冲有余量 → 解析须失败
  assert_eq!(parse_channels(b"*1\r\n$3\r\nabc\r\n$3\r\ndef\r\n"), None);
  // 声明 1 个元素、bulk 数据被截断 → 解析须失败
  assert_eq!(parse_channels(b"*1\r\n$5\r\nab\r\n"), None);
  // 正确帧 2 元素 → 解析成功且长度一致
  assert_eq!(
    parse_channels(b"*2\r\n$3\r\nabc\r\n$3\r\ndef\r\n"),
    Some(vec![b"abc".to_vec(), b"def".to_vec()])
  );
}

/// 并发压测：常驻通道撑帧 + 每 churn 通道单主在「非空↔空」高频振荡
/// （内层集合 is_empty 翻转、外层键进出），写线程同时高频快照并以客户端
/// 解析模型校验成帧。对标被修复的双趟 TOCTOU：旧实现在计数趟与写出趟之间
/// 若某通道最后一个订阅者退订，写出趟跳过该通道 → 声明长度 > 实际元素，
/// 解析判定损坏；反之新增则 < → 缓冲余量。修复为单趟快照收集后此不变式恒成立。
#[test]
fn concurrent_churn_preserves_framing() {
  let broker = Arc::new(SubscribeBroker::<Arc<PubSubMailbox>>::new());
  let sink = Arc::new(PubSubMailbox::new(8));
  // 常驻通道：id 0 永不退订，保证每次快照帧体非空、规模可观
  for i in 0..STABLE {
    broker.subscribe(&channel(i), 0, sink.clone());
  }

  let stop = Arc::new(AtomicBool::new(false));
  let violations = Arc::new(AtomicUsize::new(0));
  let writers_done = Arc::new(AtomicUsize::new(0));
  let mut handles = Vec::new();

  // 变更线程：m 只触碰 j % MUTATORS == m 的 churn 通道，独占该通道唯一订阅者，
  // 交替 subscribe / unsubscribe 令其在空↔非单间翻转；周期性 remove_subscription
  // 追加一次表级收缩，进一步放大两趟之间的抖动
  for m in 0..MUTATORS {
    let broker = broker.clone();
    let sink = sink.clone();
    let stop = stop.clone();
    handles.push(thread::spawn(move || {
      let id = (100 + m) as u64;
      let mut round = 0usize;
      loop {
        if stop.load(Ordering::Relaxed) {
          break;
        }
        let j = m + (round % (CHURN / MUTATORS)) * MUTATORS;
        let ch = churn_channel(j);
        if round & 1 == 0 {
          broker.subscribe(&ch, id, sink.clone());
        } else {
          broker.unsubscribe(&ch, id);
        }
        if round % 128 == 127 {
          broker.remove_subscription(id);
        }
        round = round.wrapping_add(1);
      }
    }));
  }

  // 写线程：高频快照，交替全量与 glob 视图，逐帧校验成帧自洽
  for w in 0..WRITERS {
    let broker = broker.clone();
    let violations = violations.clone();
    let writers_done = writers_done.clone();
    handles.push(thread::spawn(move || {
      // 全量视图与覆盖全部通道（c*）的 glob 视图两条过滤分支都纳入
      let pattern: Option<&[u8]> = if w & 1 == 0 { None } else { Some(b"c*") };
      let mut out: Vec<u8> = Vec::with_capacity(1024);
      for _ in 0..WRITER_ITERS {
        out.clear();
        broker.write_channels(&mut out, b"", pattern);
        if parse_channels(&out).is_none() {
          violations.fetch_add(1, Ordering::Relaxed);
        }
      }
      writers_done.fetch_add(1, Ordering::Release);
    }));
  }

  // 等全部写线程完成快照后置位停止变更线程，再 join 收尾
  while writers_done.load(Ordering::Acquire) < WRITERS {
    thread::yield_now();
  }
  stop.store(true, Ordering::Relaxed);
  for handle in handles {
    handle.join().expect("线程不得 panic");
  }
  assert_eq!(
    violations.load(Ordering::Relaxed),
    0,
    "并发退订/新增下不得出现任何成帧脱节（长度与实际元素数不符）"
  );
}
