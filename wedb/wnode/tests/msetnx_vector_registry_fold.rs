//! 写面 NX 族（SETNX/MSETNX/RESTORE）对存活向量登记键的第四态折叠回归
//! （票 zcode-r161c-msetnx 案一，P2）
//!
//! 缺陷形态：写面 NX 存在性折叠探针只叠 String/ObjectEnvelope/Meta 三域，
//! 结构性不见 VectorManager 进程内登记表；向量键的「键在否」唯派发层
//! `garnet_api::raw` 的 set_vector_guard / vector_registry_gate 在取窗之前
//! 窗外单裁，与 VADD（零触键闩）交叉可双双成功——已应答 :1 的覆写值被
//! 登记表值域门永久 -WRONGTYPE 遮蔽，RESTORE 同形为 BUSYKEY 契约绕过。
//! MSETNX 判定段磁盘候选降级后另含 await 宽窗，快慢双臂全程零第四态最急。
//!
//! 修复形态（本组锁钉）：NX 存在性唯各写臂闩窗内折叠探针单源
//! （probe_alive_with_registry / _async，第四态判据 registry_alive 一处），
//! 派发层三处窗外终态位删除。对标 C#：MSET_Conditional 全键排他锁内
//! EXISTS→SET（MainStoreOps.cs:349-402）、NetworkSETNX 单次 SET_Conditional
//! （BasicCommands.cs:581-600），VADD 落主存与 String 同记录槽
//! （VectorStoreOps.cs:190），统一 Reader 叠 CheckExpiry 对存活向量记录
//! 恒判「键在」——NX 违例 :0 零写、RESTORE BUSYKEY，双域并存不可构造。
//!
//! 测试全真存储真协议帧，无 mock。磁盘候选经 DEBUG FLUSHANDEVICT 真实
//! 刷盘驱逐构造（先例 msetnx_atomic.rs）；降级宽窗交叠注入用
//! take_slow_wait 前后夹注入的确定性序，无 sleep（先例
//! set_store_cold_window_ttl_selfheal.rs / restore_expired_residual_busykey.rs）。

use std::sync::Arc;

use compio::runtime::Runtime;
use itoa::Buffer;
use wbase::crc64::hash as crc64_hash;
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer, resp::vector::vector_manager::VectorManager,
};
use wnode_test::{roundtrip, vector_env as env};
use wresp::{command::RespCommand, length::try_write_length};
use wval::{KeyTag, SessionPrefixBuf};

const REPLY_WRONGTYPE: &[u8] =
  b"-WRONGTYPE Operation against a key holding the wrong kind of value.\r\n";
const REPLY_BUSYKEY: &[u8] = b"-BUSYKEY Target key name already exists.\r\n";
const REPLY_OK: &[u8] = b"+OK\r\n";

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

/// 建存活向量登记键（VADD 三参形态，先例 resp_vector_set_wrong_type.rs:CreateKeyWithTypeAsync）
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

/// 物理域记录在场取证（String 域零写判据，
/// 先例 resp_vector_set_wrong_type.rs::domain_record_present 通道）
fn string_record_present(rt: &Runtime, store: &Arc<TestStore>, key: &[u8]) -> bool {
  let sess = store.new_session().expect("取证会话");
  let rec_k = sess.session_tag_key(KeyTag::String, key);
  rt.block_on(sess.read_raw(&rec_k))
    .expect("物理记录读取不得报存储错误")
    .is_some()
}

/// 合法 RESTORE 载荷（类型 0x00 + 长度前缀 + 值 + rdb 版本 11 + crc64；
/// crc 含类型字节的 rust 口径，deviations §21，先例
/// restore_expired_residual_busykey.rs::restore_payload）
fn restore_payload(val: &[u8]) -> Vec<u8> {
  let mut encoded_len = [0u8; 5];
  let written = try_write_length(val.len() as u32, &mut encoded_len).unwrap();
  let mut payload = Vec::with_capacity(1 + written + val.len() + 2 + 8);
  payload.push(0x00);
  payload.extend_from_slice(&encoded_len[..written]);
  payload.extend_from_slice(val);
  payload.extend_from_slice(&11u16.to_le_bytes());
  payload.extend_from_slice(&crc64_hash(&payload));
  payload
}

/// 案一收口形态主锁（执行方案 5(1)）：存活向量登记键上 SETNX/MSETNX/
/// RESTORE 恒判「键在」——NX 违例 :0 零写、RESTORE BUSYKEY，全部由写臂
/// 闩窗内折叠探针出终态（派发层窗外位已删除），登记表存续、值域命令
/// 终态不变、String 物理域零写
#[test]
fn nx_family_folds_registry_for_live_vector_key() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_api, mut c, _inj, vm, store) = env("msetnx_fold.db");
    vadd(&rt, &mut c, b"vk", b"m1");
    assert!(registry_present(&vm, b"vk"));

    // 单键/批内含向量键：恒 :0 零写入（C# MSET_Conditional 锁内 EXISTS
    // 对存活向量记录判在）；修复前后应答同形，本钉锁收口不回摆
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"MSETNX", b"vk", b"v1"]),
      b":0\r\n"
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"MSETNX", b"fresh", b"fv", b"vk", b"v2"]),
      b":0\r\n"
    );
    // 整批零落库：批内普通键亦未写
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"fresh"]), b"$-1\r\n");
    assert!(
      !string_record_present(&rt, &store, b"vk"),
      "向量键 String 域须零写"
    );

    // SETNX 同判：窗内折叠判存在回 :0（对偶 C# SETEXNX 臂锁内 NeedCopyUpdate）
    assert_eq!(roundtrip(&rt, &mut c, &[b"SETNX", b"vk", b"v3"]), b":0\r\n");

    // RESTORE 同判：存活登记键 BUSYKEY（窗外第二判据位删除后窗内单裁）
    let payload = restore_payload(b"blob");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"RESTORE", b"vk", b"0", &payload]),
      REPLY_BUSYKEY
    );

    // 终态不变：键仍为登记向量键，值域命令照旧 -WRONGTYPE，登记存续
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"vk"]), REPLY_WRONGTYPE);
    assert!(registry_present(&vm, b"vk"));
    assert!(!string_record_present(&rt, &store, b"vk"));
  })
}

/// 案一 MSETNX 慢臂宽窗锁（执行方案 5(2)，本案最急红钉）：快臂判定段遇
/// 磁盘候选整体降级（零落库）后，降级快照与慢臂落笔之间为 await 宽窗；
/// 窗内注入他会话 VADD 新提交登记，慢臂折叠须判存在回 :0 零写。
/// 修复前慢臂判定段全程零第四态（probe_alive_with_prefix / 三域异步闭环），
/// 本窗注入形必回 :1 并落 String——已确认覆写值被值域门永久遮蔽（红灯取证）
#[test]
fn msetnx_slow_degrade_window_counts_vadd_committed_midway() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (api, mut c, mut inj, vm, store) = env("msetnx_widewin.db");

    // 构造磁盘候选：SET 后 DEBUG FLUSHANDEVICT 真实刷盘驱逐（msetnx_atomic
    // 同款通道），判定段探 dk 即 Ok(None) 整体降级
    assert_eq!(roundtrip(&rt, &mut c, &[b"SET", b"dk", b"old"]), REPLY_OK);
    let dbg = rt.block_on(Arc::clone(&api).exec_slow(
      RespCommand::Debug,
      vec![b"FLUSHANDEVICT".to_vec()],
      wconf::DEFAULT_RESP_VERSION,
    ));
    assert!(
      dbg.starts_with(b"+OK head="),
      "FLUSHANDEVICT 应闭环: {dbg:?}"
    );

    // victim：vk 未登记未落库、dk 磁盘候选 → 判定段降级零应答挂 SlowWait
    let out = feed(&mut c, &[b"MSETNX", b"vk", b"v1", b"dk", b"v2"]);
    assert!(out.is_empty(), "磁盘候选应整体降级慢路径零应答: {out:?}");
    let slow = c.take_slow_wait().expect("判定段降级应挂起慢路径");

    // 宽窗内注入：他会话 VADD 提交登记（VADD 零触键闩，可达序如实构造）
    vadd(&rt, &mut inj, b"vk", b"m1");
    assert!(registry_present(&vm, b"vk"));

    // 慢臂折叠须判存在：:0 零写入（修复前此窗回 :1 并落 String）
    let verdict = rt.block_on(slow.resolve());
    assert_eq!(verdict, b":0\r\n", "MSETNX 慢臂折叠须计登记表第四态");
    assert!(
      !string_record_present(&rt, &store, b"vk"),
      "慢臂判存在须零落笔"
    );

    // 其余键亦零写：dk 旧值经冷读原样可读；vk 仍为登记向量键
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"dk"]), b"$3\r\nold\r\n");
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"vk"]), REPLY_WRONGTYPE);
    assert!(registry_present(&vm, b"vk"));
  })
}

/// 执行方案 5(3) 反向对拍钉：登记表未命中之普通键与 DEL 清退登记后的同键，
/// MSETNX/SETNX 恒 :1、RESTORE 恒 +OK，逐字节零漂移（折叠第四态不误伤缺席态）
#[test]
fn nx_family_zero_drift_for_plain_and_deregistered_keys() {
  let rt = Runtime::new().unwrap();
  rt.block_on(async {
    let (_api, mut c, _inj, vm, store) = env("msetnx_drift.db");

    // 普通缺失键：折叠第四态判缺照常写入
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"MSETNX", b"pa", b"pb"]),
      b":1\r\n"
    );
    assert_eq!(roundtrip(&rt, &mut c, &[b"SETNX", b"sc", b"sd"]), b":1\r\n");
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"pa"]), b"$2\r\npb\r\n");
    let payload = restore_payload(b"restored");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"RESTORE", b"re", b"0", &payload]),
      REPLY_OK
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"GET", b"re"]),
      b"$8\r\nrestored\r\n"
    );

    // 存活向量键 DEL 清退登记后同键放行（登记摘除位 storage_session DEL 钩子）
    vadd(&rt, &mut c, b"vk3", b"m1");
    assert_eq!(roundtrip(&rt, &mut c, &[b"DEL", b"vk3"]), b":1\r\n");
    assert!(!registry_present(&vm, b"vk3"), "DEL 后登记应摘除");
    assert_eq!(roundtrip(&rt, &mut c, &[b"SETNX", b"vk3", b"x"]), b":1\r\n");
    assert_eq!(roundtrip(&rt, &mut c, &[b"GET", b"vk3"]), b"$1\r\nx\r\n");
    vadd(&rt, &mut c, b"vk4", b"m1");
    assert_eq!(roundtrip(&rt, &mut c, &[b"DEL", b"vk4"]), b":1\r\n");
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"RESTORE", b"vk4", b"0", &payload]),
      REPLY_OK
    );
    assert_eq!(
      roundtrip(&rt, &mut c, &[b"GET", b"vk4"]),
      b"$8\r\nrestored\r\n"
    );
    assert!(!string_record_present(&rt, &store, b"never_touched"));
  })
}
