//! TTL 族读径 vs 并发删除真实交错回归（票 zcode-r127c-genexpire1）
//!
//! 缺陷背景：快径读内核 `ttl_read_sync` 旧形把本可一源的两半拆成两次独立
//! 旁路读（存活探针内第一次 TTL 读 + 存活后再经 `ttl_of_sync` 第二次读），
//! 窗内并发删除（DEL 级联与 EXPIRE 过去戳臂皆先剔 TTL 后删数据）使第二读
//! 折叠 `NoExpiry`，键正在消亡/已消亡瞬态误报 -1「存在且永不过期」第三态，
//! 与 C# 单记录快照（UnifiedStore/ReadMethods.cs:162-188 HandleTtl/
//! HandleExpireTime 同快照折叠，键亡经上层 status != OK 折 -2，
//! KeyAdminCommands.cs:495-559）应答集不同构。修法：单读正序一处收口
//! （TTL 先、数据走查后，折叠终判权归数据走查），wkv 慢臂 pttl_ms/
//! expiretime_ms 同构三读同点收敛。
//!
//! 本文件两案（全真实装配，零 mock）：
//! - [`ttl_family_vs_concurrent_delete_interleave`]：双线程真并发——写者
//!   栅栏前静默（预检窗逐拍恒正，确定性），放行后真速 DEL 同键，读者锤击
//!   TTL/PTTL/EXPIRETIME/PEXPIRETIME 四命令，断言应答集恒落
//!   {正数旧值, 窗内真实无 TTL -1, 终态 -2} 三翼且逐代单调不回退，join 后
//!   终检恒 -2（「键全亡仍出 -1」的第三态在真实交错下封死）；
//! - [`ttl_family_fast_slow_arm_parity`]：快慢两径同数据集应答回归——
//!   热键（快径）与 flush_and_evict 冷化孪生键（磁盘候选降级 → wkv 慢臂）
//!   哨兵 -1/-2 与绝对域 EXPIRETIME/PEXPIRETIME 帧逐字节全等，TTL/PTTL
//!   差值域仅容忍两读间时钟推进。

use std::{
  str::from_utf8,
  sync::{Arc, Barrier},
  thread,
};

use compio::runtime::Runtime;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  storage::session::common::ttl_sync::put_ttl_sync,
};
use wnode_test::roundtrip;
use wtest_base::open_test_store;

/// 预检窗逐拍数（写者静默窗内，应答恒正为确定性断言）
const PREFLIGHT: usize = 8;
/// 竞态窗锤击拍数（写者真速 DEL 与读者交叠窗口）
const OPS: usize = 60;
/// TTL 族四命令（错误文案与派发名同口径）
const TTL_CMDS: [&[u8]; 4] = [b"TTL", b"PTTL", b"EXPIRETIME", b"PEXPIRETIME"];

type TestStore = Arc<WedbStore<SegmentedDevice>>;

/// 在既有 store 上开一条独立连接（独立 StoreSession = 独立纪元参与者）
fn consumer_on(store: &TestStore) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// `:N\r\n` 整数回执解析
fn reply_int(resp: &[u8]) -> Option<i64> {
  let body = resp.strip_prefix(b":")?.strip_suffix(b"\r\n")?;
  from_utf8(body).ok()?.parse().ok()
}

/// 双线程真并发：读者锤击 TTL 族四命令，写者栅栏放行后真速 DEL 同键；
/// 逐拍断言应答集与逐代单调性，join 后终检恒 -2
#[test]
fn ttl_family_vs_concurrent_delete_interleave() {
  let (_dir, store) = open_test_store("ttl-race.db").unwrap();
  for round in 0..40 {
    let key = format!("rc:k{round}");
    {
      // 串行预置（先于两线程 happens-before）：SET + 远未来 TTL 40000s
      let rt = Runtime::new().unwrap();
      let mut seed = consumer_on(&store);
      assert_eq!(
        roundtrip(&rt, &mut seed, &[b"SET", key.as_bytes(), b"v"]),
        b"+OK\r\n".as_slice()
      );
      assert_eq!(
        reply_int(&roundtrip(
          &rt,
          &mut seed,
          &[b"EXPIRE", key.as_bytes(), b"40000"]
        )),
        Some(1)
      );
    }
    let gate = Arc::new(Barrier::new(2));
    // 写者：放行即真速 DEL（级联内部先剔 TTL 后删数据，与 EXPIRE 过去戳臂同序）
    let writer = {
      let store = Arc::clone(&store);
      let gate = Arc::clone(&gate);
      let key = key.clone();
      thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let mut c = consumer_on(&store);
        gate.wait();
        reply_int(&roundtrip(&rt, &mut c, &[b"DEL", key.as_bytes()]))
      })
    };
    // 读者：预检窗（写者栅栏静默，恒正为确定性断言）→ 放行 → OPS 拍
    // cycle 锤击四命令；窗内逐拍断言应答集与逐代单调性（键单代生命周期，
    // 无重建无 PERSIST，TTL 记录剔后不回）：应答集 {正数} → {-1}?（仅窗内
    // 真实无 TTL 可达）→ {-2}，永不回退；翼外应答与回退即折叠撕裂证据。
    // 收尾于写者 join（键全亡 happens-before 于终检拍）后四命令逐字节 -2，
    // 「全亡出 -1」第三态在真实交错下封死
    let reader = {
      let store = Arc::clone(&store);
      let gate = Arc::clone(&gate);
      let key = key.clone();
      thread::spawn(move || {
        let rt = Runtime::new().unwrap();
        let mut c = consumer_on(&store);
        let mut seen = Vec::with_capacity(PREFLIGHT + OPS);
        for i in 0..PREFLIGHT {
          let v = reply_int(&roundtrip(&rt, &mut c, &[b"TTL", key.as_bytes()])).unwrap();
          assert!(
            v > 0,
            "预检窗（写者未启动）第 {i} 拍必须恒正，实际 {v}（撕裂自证）"
          );
          seen.push(v);
        }
        gate.wait();
        for &cmd in TTL_CMDS.iter().cycle().take(OPS) {
          let v = reply_int(&roundtrip(&rt, &mut c, &[cmd, key.as_bytes()])).unwrap();
          seen.push(v);
        }
        assert_eq!(
          writer.join().unwrap(),
          Some(1),
          "DEL 须回执 :1（键在场删除，第 {round} 轮）"
        );
        // 逐代单调断言（全窗收集完毕后统一核验，不干扰交叠时序）
        let mut phase = 0u8; // 0=正数域 1=-1 域 2=-2 终态
        for &v in &seen {
          assert!(v >= -2, "应答集越翼：{v}（整轮 {seen:?}）");
          let p = if v > 0 {
            0
          } else if v == -1 {
            1
          } else {
            2
          };
          assert!(p >= phase, "应答集回退撕裂：{seen:?}");
          phase = p;
        }
        for cmd in TTL_CMDS {
          assert_eq!(
            reply_int(&roundtrip(&rt, &mut c, &[cmd, key.as_bytes()])),
            Some(-2),
            "键全亡（DEL 已回执并 join）后第 {round} 轮 {cmd:?} 仍非 -2"
          );
        }
      })
    };
    reader.join().unwrap();
  }
}

/// 一组成员应答帧与哨兵帧逐字节全等断言
fn assert_frames_all(frames: &[Vec<u8>], want: &[u8], msg: &str) {
  for frame in frames {
    assert_eq!(frame.as_slice(), want, "{msg}");
  }
}

/// 快慢两径同数据集应答回归：热键走快径、flush_and_evict 冷化孪生键走
/// wkv 慢臂（磁盘候选降级重放），哨兵 -1/-2 与绝对域帧逐字节全等，
/// TTL/PTTL 差值仅容忍两读间时钟推进
#[test]
fn ttl_family_fast_slow_arm_parity() {
  let (_dir, store) = open_test_store("ttl-parity.db").unwrap();
  let rt = Runtime::new().unwrap();
  let far = now_ticks() + 40_000 * TICKS_PER_SECOND;
  let dead = now_ticks() - TICKS_PER_SECOND;
  {
    // 孪生装配：pt:hot-* 常驻内存走快径，pt:cold-* 冷化后走慢臂；
    // 无 TTL 键经命令面、远 TTL 与惰性过期键经旁路内核同刻预置
    let mut c = consumer_on(&store);
    for tag in [b"hot-nottl".as_slice(), b"cold-nottl".as_slice()] {
      let mut full = b"pt:".to_vec();
      full.extend_from_slice(tag);
      assert_eq!(
        roundtrip(&rt, &mut c, &[b"SET", &full, b"v"]),
        b"+OK\r\n".as_slice()
      );
    }
    drop(c);
    let sess = store.new_session().unwrap();
    let batch = sess.enter_batch();
    for (pair, exp) in [
      b"hot-far".as_slice(),
      b"cold-far".as_slice(),
      b"hot-exp".as_slice(),
      b"cold-exp".as_slice(),
    ]
    .into_iter()
    .zip([far, far, dead, dead])
    {
      let mut full = b"pt:".to_vec();
      full.extend_from_slice(pair);
      batch.try_upsert_sync(&full, b"v").unwrap().unwrap();
      assert!(
        put_ttl_sync(&batch, &full, exp).unwrap(),
        "旁路 TTL 写须闭环"
      );
    }
    drop(batch);
    drop(sess);
  }

  let quad = |c: &mut RespSessionConsumer, key: &[u8]| -> Vec<Vec<u8>> {
    TTL_CMDS
      .into_iter()
      .map(|cmd| roundtrip(&rt, c, &[cmd, key]))
      .collect()
  };
  // 快臂（热径直读，内存驻留）
  let mut c = consumer_on(&store);
  let hot_missing = quad(&mut c, b"pt:missing");
  let hot_nottl = quad(&mut c, b"pt:hot-nottl");
  let hot_far = quad(&mut c, b"pt:hot-far");
  let hot_exp = quad(&mut c, b"pt:hot-exp");
  drop(c);
  // 冷化（磁盘驻留强制降级慢臂）
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let mut c = consumer_on(&store);
  let cold_missing = quad(&mut c, b"pt:cold-missing");
  let cold_nottl = quad(&mut c, b"pt:cold-nottl");
  let cold_far = quad(&mut c, b"pt:cold-far");
  let cold_exp = quad(&mut c, b"pt:cold-exp");

  // 哨兵臂逐字节全等：缺席 -2、无 TTL -1、已过期 -2（快慢两径瞬态互斥收口）
  assert_frames_all(&hot_missing, b":-2\r\n", "缺席键恒 -2");
  assert_frames_all(&cold_missing, b":-2\r\n", "冷径缺席键恒 -2");
  assert_frames_all(&hot_nottl, b":-1\r\n", "无 TTL 在场键恒 -1");
  assert_frames_all(&cold_nottl, b":-1\r\n", "无 TTL 在场键冷径恒 -1");
  assert_frames_all(&hot_exp, b":-2\r\n", "惰性过期键快径折 -2");
  assert_frames_all(&cold_exp, b":-2\r\n", "已过期键慢臂 purge 后仍 -2");
  // 绝对域逐字节全等（孪生同刻装配同一 ticks，无 now 时钟参与）
  assert_eq!(hot_far[2], cold_far[2], "EXPIRETIME 快慢臂逐字节全等");
  assert_eq!(hot_far[3], cold_far[3], "PEXPIRETIME 快慢臂逐字节全等");
  // 相对域同翼正数，差值仅容忍两读间时钟推进
  let (h_ttl, c_ttl) = (
    reply_int(&hot_far[0]).unwrap(),
    reply_int(&cold_far[0]).unwrap(),
  );
  let (h_pttl, c_pttl) = (
    reply_int(&hot_far[1]).unwrap(),
    reply_int(&cold_far[1]).unwrap(),
  );
  assert!(
    (39_000..=40_000).contains(&h_ttl) && (39_000..=40_000).contains(&c_ttl),
    "远 TTL 键两径秒域恒正：{h_ttl} / {c_ttl}"
  );
  assert!(
    (h_ttl - c_ttl).abs() <= 5,
    "TTL 快慢臂秒域漂移：{h_ttl} vs {c_ttl}"
  );
  assert!(
    (39_000_000..=40_000_000).contains(&h_pttl) && (39_000_000..=40_000_000).contains(&c_pttl),
    "远 TTL 键两径毫秒域恒正：{h_pttl} / {c_pttl}"
  );
  assert!(
    (h_pttl - c_pttl).abs() <= 6_000,
    "PTTL 快慢臂毫秒域漂移：{h_pttl} vs {c_pttl}"
  );
}
