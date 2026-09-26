//! SET 覆写族对存活向量登记键的窗内第四态折叠回归
//! （票 zcode-r163c-setguard 案四，P2）
//!
//! 缺陷形态：轮 161 丙收敛 SETNX/MSETNX/RESTORE 后，SET 覆写族留口三缝——
//! 其一，快臂锁窗内 probe_alive_domain 只扫三域无登记表第四态，派发层
//! set_vector_guard 窗外放行后至取窗间的并发 VADD 令 SET NX 双成功，已答
//! +OK 的字符串被值域门永久 -WRONGTYPE 遮蔽；其二，慢臂 string_slow 选项
//! 形态在窗外无条件 clear_vector_registry 预清退后 slow_set_conditional 按
//! 缺失键裁决——SET NX 命中向量键误判缺写 +OK 且登记被毁、SET XX 回 nil
//! 却已静默摧毁存活向量索引（NX/XX 语义反转 + 数据丢失）；其三，盲写慢臂
//! blind_write_gate 清退漏在 rmw_window 之前，与 MSET 慢臂窗内清退标准割裂。
//!
//! 修复形态（本组锁钉）：案一快臂 SET 族全窗内接 registry_alive 第四态复验
//! （NX 窗内直出 nil 零副作用、覆写形态诚实降级 Ok(false)、GET 形态
//! -WRONGTYPE 保留登记）；案二慢臂窗外预清退删除，slow_set_conditional 持
//! 窗后一次折叠终裁（NX 判在出 nil 登记保留；XX/无条件/KEEPTTL 窗内清退后
//! 覆写，对位 C# 锁内 DELETE+SET_Conditional 重投终态
//! BasicCommands.cs:788-796）；案三 blind_write_gate 取窗先行、RI 门与清退
//! 皆落持窗临界区内，严格对齐 array_commands.rs MSET 慢臂窗内清退单源标准。
//! 对标 C#：向量记录与 String 同驻 MainStore 同槽
//! （VarLenInputMethods.cs:194 RecordType），NX 存在性与旧记录清退恒在统一
//! 记录闩临界区内原子执行，绝无窗外裸清退。
//!
//! 测试全真存储真协议帧，无 mock。降级宽窗用占窗会话夹逼 try_rmw_window
//! 失闩构造（先例 rmw_key_concurrency.rs 互斥判据），SlowWait 前后夹注入
//! VADD 的确定性序无 sleep（先例 msetnx_vector_registry_fold.rs）。

use std::{str::from_utf8, sync::Arc};

use compio::runtime::Runtime;
use itoa::Buffer;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer, resp::vector::vector_manager::VectorManager,
};
use wnode_test::{roundtrip, vector_env as env};
use wval::{KeyTag, SessionPrefixBuf};

const REPLY_WRONGTYPE: &[u8] =
  b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";
const REPLY_OK: &[u8] = b"+OK\r\n";
const REPLY_NIL: &[u8] = b"$-1\r\n";

type TestStore = WedbStore<SegmentedDevice>;

/// RESP2 请求帧编码（任意字节参数）
fn frame(args: &[&[u8]]) -> Vec<u8> {
  let mut ibuf = Buffer::new();
  let mut out = Vec::new();
  out.push(b'*');
  out.extend_from_slice(ibuf.format(args.len()).as_bytes());
  out.extend_from_slice(b"\r\n");
  for arg in args {
    out.push(b'$');
    out.extend_from_slice(ibuf.format(arg.len()).as_bytes());
    out.extend_from_slice(b"\r\n");
    out.extend_from_slice(arg);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// 喂帧取同步段输出（不闭环慢路径，供交叠注入窗口使用）
fn feed(c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&frame(args));
  c.return_recv_scratch(scratch);
  let mut out = Vec::new();
  let consumed = c.try_consume_messages_into(&mut out);
  assert_eq!(consumed, Some(0), "帧应被完整消费: {args:?}");
  out
}

/// 建存活向量登记键（VADD 三参形态，先例 resp_vector_set_wrong_type.rs）
fn vadd(rt: &Runtime, c: &mut RespSessionConsumer, key: &[u8], member: &[u8]) {
  let vbytes: Vec<u8> = [1.0f32, 2.0, 3.0]
    .iter()
    .flat_map(|v| v.to_le_bytes())
    .collect();
  let out = roundtrip(rt, c, &[b"VADD", key, b"FP32", &vbytes, member]);
  assert_eq!(out, b":1\r\n", "VADD 建登记应回 :1: {out:?}");
}

/// 登记表存活判据（第四态源单点读取口）
fn registry_present(vm: &VectorManager, key: &[u8]) -> bool {
  vm.read_stored_index(SessionPrefixBuf::ROOT.as_slice(), key)
    .is_some()
}

/// 物理域记录在场取证（String 域零写判据）
fn string_record_present(rt: &Runtime, store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().expect("取证会话");
  let rec_k = sess.session_tag_key(KeyTag::String, key);
  rt.block_on(sess.read_raw(&rec_k))
    .expect("物理记录读取不得报存储错误")
    .is_some()
}

/// 案二收口形态主锁：存活向量登记键上 SET 覆写族全形态由持窗折叠终裁——
/// SET NX 回 nil 且登记零副作用保留、SET k v GET / GETSET 回 -WRONGTYPE
/// 登记保留、SET XX 覆写成功且登记窗内清退、盲写族（SET/SETEX/SET k v EX）
/// 与 KEEPTTL 覆写成功且登记清退，杜绝 NX/XX 语义反转与存活索引静默损毁
/// （修复前慢臂窗外预清退形：NX 误回 +OK 毁登记、XX 回 nil 毁登记，红灯取证）
#[test]
fn set_family_fourth_state_terminal_adjudication() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_api, mut c, _inj, vm, store) = env("setguard_terminal.db");

    // NX 命中存活登记键：nil 出、登记保留、String 域零写
    // （与 SETNX/MSETNX 窗内折叠同一裁决源，票 zcode-r161c-msetnx 同形）
    vadd(&rt, &mut c, b"vnx", b"m1");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", b"vnx", b"x", b"NX"]),
      REPLY_NIL
    );
    assert!(registry_present(&vm, b"vnx"), "NX 违例须零副作用保留登记");
    assert!(
      !string_record_present(&rt, &store, b"vnx"),
      "NX 违例须零落笔"
    );
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"vnx"]), REPLY_WRONGTYPE);

    // GET 形态与 GETSET 命中存活登记键：-WRONGTYPE 登记保留
    // （C# getValue 臂 BasicCommands.cs:832-835 无 DELETE 重试）
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", b"vnx", b"x", b"GET"]),
      REPLY_WRONGTYPE
    );
    assert!(registry_present(&vm, b"vnx"));
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"GETSET", b"vnx", b"x"]),
      REPLY_WRONGTYPE
    );
    assert!(registry_present(&vm, b"vnx"));
    assert!(!string_record_present(&rt, &store, b"vnx"));

    // XX 命中存活登记键：键在条件成立 → 持窗清退后覆写成功 +OK
    // （修复前预清退致判缺回 nil 且向量已毁——双红灯取证）
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", b"vnx", b"newx", b"XX"]),
      REPLY_OK
    );
    assert!(!registry_present(&vm, b"vnx"), "XX 覆写后登记须窗内清退");
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"vnx"]), b"$4\r\nnewx\r\n");

    // 盲写族：SET 裸形 / SETEX / SET k v EX / KEEPTTL 命中存活登记键
    // 覆写成功且登记清退无双域残骸（C# DELETE+SET 重投同终态）
    vadd(&rt, &mut c, b"vbl", b"m1");
    assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"vbl", b"b1"]), REPLY_OK);
    assert!(!registry_present(&vm, b"vbl"));
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"vbl"]), b"$2\r\nb1\r\n");

    vadd(&rt, &mut c, b"vsx", b"m1");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SETEX", b"vsx", b"100", b"s1"]),
      REPLY_OK
    );
    assert!(!registry_present(&vm, b"vsx"));
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"vsx"]), b"$2\r\ns1\r\n");

    vadd(&rt, &mut c, b"vex", b"m1");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", b"vex", b"e1", b"EX", b"100"]),
      REPLY_OK
    );
    assert!(!registry_present(&vm, b"vex"));
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"vex"]), b"$2\r\ne1\r\n");

    vadd(&rt, &mut c, b"vkt", b"m1");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", b"vkt", b"k1", b"KEEPTTL"]),
      REPLY_OK
    );
    assert!(!registry_present(&vm, b"vkt"));
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"vkt"]), b"$2\r\nk1\r\n");
  })
}

/// 案一/案二宽窗红钉（执行方案 4 第三项的确定性对偶）：victim `SET k v NX`
/// 快臂取窗遭占窗会话失闩整体降级挂 SlowWait，降级快照与慢臂持窗之间，
/// 他会话 VADD 提交登记（VADD 零触键闩，可达序如实构造）；慢臂折叠须判
/// 「键在」回 nil 零写、登记保留。修复前慢臂窗外 reg_hit 先清退登记再按
/// 缺失键裁决，必回 +OK 且存活索引遭毁（NX 契约背离红灯取证）
#[test]
fn set_nx_wide_window_counts_vadd_committed_midway() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_api, mut c, mut inj, vm, store) = env("setguard_widewin.db");

    // 占窗装配：独立 store 会话对本键桶持排他闩（rmw_key_concurrency 同法），
    // victim 快臂 try_rmw_window 必失闩降级
    let holder = store.new_session().expect("占窗会话");
    let batch_h = holder.enter_batch();
    let window = batch_h.try_rmw_window(b"wk").expect("占窗会话取本键排他闩");

    // victim：SET wk v NX 挂 SlowWait 零应答
    let out = feed(&mut c, &[b"SET", b"wk", b"v", b"NX"]);
    assert!(out.is_empty(), "失闩应整体降级零应答: {out:?}");
    let slow = c.take_slow_wait().expect("失闩降级应挂起慢路径");

    // 宽窗内注入：他会话 VADD 提交登记（此时派发层守卫早已放行）
    vadd(&rt, &mut inj, b"wk", b"m1");
    assert!(registry_present(&vm, b"wk"));

    drop(window);
    let verdict = rt.block_on(slow.resolve());
    assert_eq!(verdict, REPLY_NIL, "SET NX 慢臂窗内折叠须计登记表第四态");
    assert!(registry_present(&vm, b"wk"), "NX 违例须零副作用保留登记");
    assert!(
      !string_record_present(&rt, &store, b"wk"),
      "NX 违例须零落笔"
    );
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"wk"]), REPLY_WRONGTYPE);
  })
}

/// 案三宽窗锁：盲写 `SET k v` 快臂失闩降级，宽窗内并发 VADD 提交登记，
/// 慢臂 blind_write_gate 取窗先行、清退落持窗临界区内——终态无双域并存：
/// 覆写 +OK、登记摘除、值可读不被值域门遮蔽（对齐 MSET 窗内清退单源标准，
/// 清退与取窗之间再无并发 VADD 可钻的锁外天窗；锁序收拢的观测面钉）
#[test]
fn blind_set_wide_window_interleave_leaves_no_dual_domain() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_api, mut c, mut inj, vm, store) = env("setguard_blind.db");

    let holder = store.new_session().expect("占窗会话");
    let batch_h = holder.enter_batch();
    let window = batch_h.try_rmw_window(b"bk").expect("占窗会话取本键排他闩");

    let out = feed(&mut c, &[b"SET", b"bk", b"v1"]);
    assert!(out.is_empty(), "盲写失闩应整体降级零应答: {out:?}");
    let slow = c.take_slow_wait().expect("盲写失闩应挂起慢路径");

    vadd(&rt, &mut inj, b"bk", b"m1");
    assert!(registry_present(&vm, b"bk"));

    drop(window);
    let verdict = rt.block_on(slow.resolve());
    assert_eq!(verdict, REPLY_OK, "盲写覆写形窗内清退后应 +OK");
    assert!(
      !registry_present(&vm, b"bk"),
      "盲写落毕登记须摘除，杜绝双域并存"
    );
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"bk"]), b"$2\r\nv1\r\n");
  })
}

/// 反向对拍钉：普通字符串键上 SET 覆写族各形态逐字节零漂移（第四态折叠
/// 不误伤缺席态与三域命中态，NX/XX/GET/KEEPTTL 契约与修前同形）
#[test]
fn set_family_zero_drift_for_plain_keys() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_api, mut c, _inj, _vm, store) = env("setguard_drift.db");

    // 缺失键：NX 写入 +OK、XX 回 nil、盲写覆写
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", b"pk", b"a", b"NX"]),
      REPLY_OK
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", b"pk", b"b", b"NX"]),
      REPLY_NIL
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", b"pk", b"c", b"XX"]),
      REPLY_OK
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", b"nope", b"d", b"XX"]),
      REPLY_NIL
    );
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"pk"]), b"$1\r\nc\r\n");

    // GET 形态回旧值（命中旧值 / 缺失 nil）、GETSET 无条件覆写回旧值
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", b"pk", b"e", b"GET"]),
      b"$1\r\nc\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"GETSET", b"gk", b"g1"]),
      REPLY_NIL
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"GETSET", b"gk", b"g2"]),
      b"$2\r\ng1\r\n"
    );

    // KEEPTTL：EX 落 TTL 后 KEEPTTL 覆写保 TTL 不失、无 EX 覆写清 TTL
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", b"tk", b"t1", b"EX", b"100"]),
      REPLY_OK
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"SET", b"tk", b"t2", b"KEEPTTL"]),
      REPLY_OK
    );
    let ttl = roundtrip(&rt, &mut c, &[b"TTL", b"tk"]);
    let secs: i64 = from_utf8(&ttl[1..ttl.len() - 2])
      .expect("TTL 整数帧")
      .parse()
      .expect("TTL 须为整数");
    assert!((50..=100).contains(&secs), "KEEPTTL 应保既有 TTL: {ttl:?}");
    assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"tk", b"t3"]), REPLY_OK);
    assert_eq!(roundtrip(&rt, &mut c, &[b"TTL", b"tk"]), b":-1\r\n");
    assert!(!string_record_present(&rt, &store, b"untouched_key"));
  })
}
