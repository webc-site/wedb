//! 脚本窗内 ACL 改权停车收口回归锁（工单 wnode-lua-script-window-acl-park-starvation）
//!
//! 票面命题：EVAL 入口后窗口内到达的并发改权（跨连接 SETUSER / AOF 回放
//! bump）令门链 Parked 臂对后续每条 redis.call 连锁挡回——旧形态三消费面
//! 各出异象：fallback 空应答折 nil（脚本以 nil 续跑出错值）、GET/SET 快路
//! 空应答落假「protocol error」协议违规文案（§97 Protocol 折叠面收缩至真
//! 协议损伤）、acl_check_cmd 在陈旧挂载上恒回陈旧布尔。收口后三形态统一
//! 为确定性窗内改权错误（deviations §159），游标回退保存储零执行，EVAL
//! 收尾泵即时刷新挂载、下一笔外层命令按新规则 fresh 裁决。
//!
//! 驱动形态（真实路径，无假 mock）：RespServerSession + StoreGarnetApi +
//! 共享集合经纪（泵 harness 经 wnode_test::drive_pending_parks，与
//! acl_tests.rs:98 刷新臂循环同构），脚本内以 redis.call('BLPOP') 挂起
//! 协程打开注入窗口——挂起期经 AclStore 真源写口 bump 代数（与 SETUSER
//! 同一写出口），续跑后首条 redis.call 即命中窗内停车收口。

use std::{str::from_utf8, sync::Arc};

use wacl::{AccessControlList, AclParser, GarnetAclAuthenticator};
use wcol::itembroker::{
  collection_item_broker::CollectionItemBroker,
  item_broker_face::{ItemBrokerFinisher, SharedItemBroker},
};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wlua::ScriptApiError;
use wnode::resp::{
  acl_store::AclStore,
  garnet_api::StoreGarnetApi,
  objects::collection_item_source::CollectionItemSource,
  resp_server_session::{RespServerSession, RespServerSessionOptions},
};
use wnode_test::{drain_output, err_frame};
use wresp::cmd_strings::RESP_ERR_NOPERM;
use wtest_base::{open_test_store, resp_frame};

type TestStore = Arc<WedbStore<SegmentedDevice>>;
type SharedBroker = Arc<SharedItemBroker<CollectionItemSource<SegmentedDevice>>>;

/// 本票收口错误文案字节形（单源真值取 wlua 常量，断言侧零复刻文案）
const ACL_CHANGED_BYTES: &[u8] = ScriptApiError::ACL_CHANGED_TEXT.as_bytes();

/// ACL 真源写口（与 SETUSER 同一出口：落记录 + bump 引擎代数）
async fn acl_write(store: &TestStore, user: &str, rule: &str) {
  let session = store.new_session().unwrap();
  let parsed = AclParser::parse_acl_rule(rule).unwrap();
  AclStore::new(&session)
    .write(0, user.as_bytes(), &parsed.to_bytes())
    .await
    .unwrap();
}

/// 全装配脚本会话：enable_lua + 共享经纪（BLPOP 挂起承接）+ 可选 ACL 档
fn script_session(
  id: i64,
  store: &TestStore,
  broker: &SharedBroker,
  acl: Option<&Arc<AccessControlList>>,
) -> RespServerSession {
  let notify_broker = Arc::clone(broker);
  let wait_broker = Arc::clone(broker) as Arc<dyn ItemBrokerFinisher>;
  let api = Arc::new(
    StoreGarnetApi::new(store.new_session().unwrap())
      .with_collection_notify(Some(Arc::new(move |domain: (u64, u64), key: &[u8]| {
        notify_broker.handle_collection_update(domain, key)
      })))
      .with_item_broker_wait(Some(wait_broker)),
  );
  let mut s = RespServerSession::new(
    id,
    RespServerSessionOptions {
      enable_lua: true,
      ..RespServerSessionOptions::default()
    },
  );
  if let Some(acl) = acl {
    s.attach_acl(Some(Arc::new(GarnetAclAuthenticator::new(Arc::clone(acl)))));
  }
  s.set_garnet_api(api);
  s.set_item_broker(Arc::clone(broker));
  s
}

/// 泵 harness：喂一帧 → 消费 → 停车/挂起臂驱动闭环 → 出应答字节
async fn roundtrip(s: &mut RespServerSession, frame: &[u8]) -> Vec<u8> {
  s.recv_buffer.extend_from_slice(frame);
  let mut resp_buf = Vec::new();
  assert!(s.try_consume_messages().is_some(), "帧应被完整消费");
  s.take_output_into(&mut resp_buf);
  wnode_test::drive_pending_parks(s, &mut resp_buf, true).await;
  s.output.extend_from_slice(&resp_buf);
  drain_output(s)
}

struct Parked {
  _dir: tempfile::TempDir,
  store: TestStore,
  victim: RespServerSession,
  pusher: RespServerSession,
}

/// 场景装配：ACL 档命名用户 park（初始 +@all）已认证 + 无 ACL 推手连接
async fn parked_scenario(tag: &str) -> Parked {
  let (_dir, store) = open_test_store(tag).unwrap();
  let broker = Arc::new(SharedItemBroker::new(Arc::new(CollectionItemBroker::new(
    CollectionItemSource::new(store.new_session().unwrap()),
  ))));
  let acl = Arc::new(AccessControlList::new("").unwrap());
  acl_write(&store, "park", "user park on >pw +@all").await;
  let mut victim = script_session(1, &store, &broker, Some(&acl));
  assert_eq!(
    roundtrip(&mut victim, &resp_frame(&[b"AUTH", b"park", b"pw"])).await,
    b"+OK\r\n"
  );
  let pusher = script_session(2, &store, &broker, None);
  Parked {
    _dir,
    store,
    victim,
    pusher,
  }
}

/// 窗内 bump 驱动单脚本：先消费至脚本挂起（BLPOP 让渡窗），推手补元素
/// 唤醒、真源写口 bump 代数，再交泵续跑至脚本收口，返回 EVAL 应答字节。
/// 挂起断言即「改权落在脚本执行中段」的真实在场证明。
async fn eval_with_midwindow_bump(p: &mut Parked, script: &str, keys: &[&str]) -> Vec<u8> {
  let numkeys = keys.len().to_string();
  let mut parts: Vec<&[u8]> = vec![b"EVAL", script.as_bytes(), numkeys.as_bytes()];
  parts.extend(keys.iter().map(|k| k.as_bytes()));
  p.victim.recv_buffer.extend_from_slice(&resp_frame(&parts));
  let mut resp_buf = Vec::new();
  assert!(p.victim.try_consume_messages().is_some(), "EVAL 应被消费");
  p.victim.take_output_into(&mut resp_buf);
  assert!(
    p.victim.has_script_suspend(),
    "脚本应在 BLPOP 处挂起（改权注入窗在场）"
  );

  // 挂起期：对端推入唤醒元素 + ACL 真源 bump（改权命中在飞脚本）
  p.pusher
    .recv_buffer
    .extend_from_slice(&resp_frame(&[b"LPUSH", b"park:blk", b"v1"]));
  assert!(p.pusher.try_consume_messages().is_some());
  p.pusher.take_output_into(&mut Vec::new());
  acl_write(&p.store, "park", "user park on >pw +@all -get").await;

  // 泵续跑：BLPOP 应答回填 → 下一条 redis.call 命中窗内停车收口 → 脚本
  // 中断 → 收尾刷新臂以真源记录重挂挂载
  wnode_test::drive_pending_parks(&mut p.victim, &mut resp_buf, true).await;
  p.victim.output.extend_from_slice(&resp_buf);
  drain_output(&mut p.victim)
}

/// 收口形态公共断言：确定性窗内改权错误文案在场，假协议违规文案灭失
fn assert_deterministic_error(out: &[u8]) {
  let text = from_utf8(out).unwrap();
  assert!(
    out
      .windows(ACL_CHANGED_BYTES.len())
      .any(|w| w == ACL_CHANGED_BYTES),
    "应答须含窗内改权收口文案，got: {text}"
  );
  assert!(
    !out
      .windows(b"protocol error".len())
      .any(|w| w == b"protocol error"),
    "改权并发不得再伪装为协议违规文案: {text}"
  );
}

/// 其一（fallback 形）：bump 后第二条 fallback redis.call 断确定性错误帧、
/// 脚本中断、存储零执行（键未落）、EVAL 收尾泵刷新后下一笔外层命令 fresh
#[compio::test]
async fn fallback_form_parks_to_deterministic_error() {
  let p = &mut parked_scenario("lua-park-fb.db").await;
  let out = eval_with_midwindow_bump(
    p,
    "local a = redis.call('INCR', KEYS[1]) redis.call('BLPOP', KEYS[2], 5) \
     local b = redis.call('INCR', KEYS[3]) return b",
    &["park:fb:pre", "park:blk", "park:fb:post"],
  )
  .await;
  assert_deterministic_error(&out);

  // bump 前命令照常落库；停车命令存储零执行（键未落）
  let pre = roundtrip(&mut p.victim, &resp_frame(&[b"EXISTS", b"park:fb:pre"])).await;
  let post = roundtrip(&mut p.victim, &resp_frame(&[b"EXISTS", b"park:fb:post"])).await;
  assert_eq!(pre, b":1\r\n", "bump 前 fallback 命令须照常执行");
  assert_eq!(post, b":0\r\n", "停车命令须存储零执行（键未落）");

  // EVAL 收尾泵刷新完成：下一笔外层 GET 按新规则（-get）fresh 裁决
  let denied = roundtrip(&mut p.victim, &resp_frame(&[b"GET", b"park:fb:pre"])).await;
  assert_eq!(
    denied,
    err_frame(RESP_ERR_NOPERM),
    "收尾泵刷新后须按新规则裁决"
  );
}

/// 其二（SET 快路形）：bump 后 redis.call('SET') 收口为同文案 Lua 错误，
/// 不再落假 PROTOCOL_TEXT 违规，且脚本中断、键未落
#[compio::test]
async fn fast_path_set_form_parks_to_deterministic_error() {
  let p = &mut parked_scenario("lua-park-fs.db").await;
  let out = eval_with_midwindow_bump(
    p,
    "redis.call('SET', KEYS[1], 'pre') redis.call('BLPOP', KEYS[2], 5) \
     redis.call('SET', KEYS[3], 'post') return 'done'",
    &["park:fs:k1", "park:blk", "park:fs:k2"],
  )
  .await;
  assert_deterministic_error(&out);
  assert!(
    !out.windows(4).any(|w| w == b"done"),
    "脚本须已中断（不得续跑到 return）: {}",
    from_utf8(&out).unwrap()
  );
  let k2 = roundtrip(&mut p.victim, &resp_frame(&[b"EXISTS", b"park:fs:k2"])).await;
  assert_eq!(k2, b":0\r\n", "快路停车命令须存储零执行（键未落）");
}

/// 其二·GET 形：bump 后 redis.call('GET') 同判据收口（旧形折 false 续跑
/// 或假协议违规，两态均灭失）
#[compio::test]
async fn fast_path_get_form_parks_to_deterministic_error() {
  let p = &mut parked_scenario("lua-park-fg.db").await;
  let out = eval_with_midwindow_bump(
    p,
    "redis.call('BLPOP', KEYS[1], 5) redis.call('GET', KEYS[2]) return 'done'",
    &["park:blk", "park:fg:k"],
  )
  .await;
  assert_deterministic_error(&out);
}

/// 其三（acl_check_cmd 形）：bump 后 redis.acl_check_cmd 不复报陈旧 true
/// （C# 现读新句柄形为按新规则裁决；本仓窗口面收口为中止语义，§159）
#[compio::test]
async fn acl_check_cmd_no_stale_true_after_bump() {
  let p = &mut parked_scenario("lua-park-ac.db").await;
  let out = eval_with_midwindow_bump(
    p,
    "redis.call('BLPOP', KEYS[1], 5) \
     if redis.acl_check_cmd('GET') then return 'stale-true' end \
     return 'fresh-false'",
    &["park:blk"],
  )
  .await;
  assert_deterministic_error(&out);
  assert!(
    !out.windows(11).any(|w| w == b"stale-true"),
    "不得复报陈旧 true: {}",
    from_utf8(&out).unwrap()
  );
}
