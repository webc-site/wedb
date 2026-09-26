//! RMW 重建臂旁域清退回归（票 zcode-r147c-incrovf 案一）
//!
//! 缺陷形态（修复前）：wkv `RmwWindow::try_rmw_sync` 的 `Due`（已过期未清退）臂
//! 只删共享键级 TTL 记录再裸写 String 域记录，全程不碰信封/Meta 旁域——TTL 记录
//! 被删后，幽灵信封/幽灵 Meta 失去唯一过期凭据（无 TTL 记录时门恒 Pass、GC 由
//! TTL 记录驱动），永世不清：已过期对象键一经 INCR 族重建即产永久幽灵，集合面
//! 读到旧成员而 GET 回新值（跨域脑裂），升阶键的 bftree 树文件成磁盘孤儿，
//! 仅手工 DEL 可解。
//!
//! 修复形态：Due 臂重建写回改道 SET 同步内核
//! `StoreSession::try_upsert_tag_sync_unprotected_with_prefix`（KeyTag::String），
//! 与 SET 覆写共用同一套旁域卫生单源（Meta 在簿即降级异步树清退、数据落笔提交
//! 成功后清退信封残留）；异步闭包臂 `upsert_rmw` 的无存活 TTL 记录臂同改道 SET
//! 异步内核 `upsert_tag`，杜绝「同步/异步两臂终态发散」。C# 侧无此问题系其
//! 单记录一体架构（值 + 刻度同 DataHeader，整记录同体消亡），非第二套清退机制。
//!
//! C# 对照锚（本仓有意分叉，登记见 doc/zh/deviations.md §133 写侧并册）：
//! MainStore/ReadMethods.cs:31 ValueIsObject 判型先行、:37 CheckExpiry 后至，
//! BasicCommands.cs:877-880 与 MainStore/RMWMethods.cs:388-393 对「已过期未清退
//! 对象键 × INCR 族」回 -WRONGTYPE，至主动扫描清退后方按缺失重建；rust 采真
//! Redis「过期即缺席」形按缺失重建回新值（承 ttl_rmw_semantics.rs:784 锁面），
//! 严禁按 C# 判型先行回改 Due 臂。
//!
//! 夹具全真存储真协议帧（无 mock）：快臂经会话消费者生产口驱动，慢臂经
//! `SlowWait::for_command` 直驱（§133 锁测同形先例），逐臂应答与终态逐字节对拍；
//! 旁域取证走物理域记录点查 + 树注册表/数据文件/页缓存预算三账（wkv
//! tests/wkv_compact_tiered_expired_drain.rs 同形）。

use std::sync::Arc;

use compio::runtime::Runtime;
use itoa::Buffer;
use tempfile::TempDir;
use wbase::{convert::TICKS_PER_SECOND, time::now_ticks};
use wdev::SegmentedDevice;
use wkv::WedbStore;
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  database::{GarnetDatabase, SingleDatabaseManager},
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::RespServerSessionOptions,
    slow_path::SlowWait,
  },
};
use wresp::{cmd_strings::RESP_ERR_WRONG_TYPE, command::RespCommand};
use wtest_base::{resp_frame as frame, test_store_config};
use wval::{GarnetObjectType, KeyTag, NamespaceDbCodec, TaggedKeyBuf};

/// 慢臂直驱的 RESP 协议版本入参
const RESP_V2: u8 = 2;

type TestStore = WedbStore<SegmentedDevice>;

/// 被测臂别（快臂 = 会话消费者生产口，慢臂 = SlowWait 直驱）
#[derive(Clone, Copy)]
enum Arm {
  Fast,
  Slow,
}

impl Arm {
  fn label(self) -> &'static str {
    match self {
      Self::Fast => "fast",
      Self::Slow => "slow",
    }
  }
}

/// 命令面执行域：存储 + api（慢臂会话）+ 消费者（快臂会话）+ 临时目录托管
struct Env {
  rt: Runtime,
  store: Arc<TestStore>,
  api: GarnetApi,
  c: RespSessionConsumer,
  _dir: TempDir,
}

/// 带分层树目录的执行域（夹具 b 需真实 bftree 数据文件）
fn env(tag: &str) -> Env {
  let dir = tempfile::tempdir().unwrap();
  let mut config = test_store_config();
  config.gc.enabled = false;
  config.range_index_dir = Some(dir.path().join("ri"));
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let c = RespSessionConsumer::new(1, RespServerSessionOptions::default(), Arc::clone(&api));
  Env {
    rt: Runtime::new().unwrap(),
    store,
    api,
    c,
    _dir: dir,
  }
}

/// RESP2 bulk string 应答帧编码（终态值对拍用，二进制安全）
fn bulk(val: &[u8]) -> Vec<u8> {
  let mut itoa_buf = Buffer::new();
  let mut out = Vec::new();
  out.push(b'$');
  out.extend_from_slice(itoa_buf.format(val.len()).as_bytes());
  out.extend_from_slice(b"\r\n");
  out.extend_from_slice(val);
  out.extend_from_slice(b"\r\n");
  out
}

impl Env {
  /// 快臂单命令往返（同步段降级即由 SlowWait 承接闭环）
  fn fast(&mut self, args: &[&[u8]]) -> Vec<u8> {
    fast(&self.rt, &mut self.c, args)
  }

  /// 慢臂直驱（与降级快照投递同径，不经会话快路径）
  fn slow(&self, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
    let snapshot: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
    let api = &self.api;
    self.rt.block_on(async {
      SlowWait::for_command(api, cmd, snapshot, RESP_V2)
        .resolve()
        .await
    })
  }

  /// 按臂别驱动被测命令
  fn exec(&mut self, arm: Arm, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
    match arm {
      Arm::Fast => {
        // 快臂走线帧与产线同形：命令名首 token 必在（args 仅承载键与参数尾），
        // 缺首 token 则键被解析器当命令名回 unknown command 哨兵
        let mut framed: Vec<&[u8]> = Vec::with_capacity(args.len() + 1);
        framed.push(cmd.to_cs_name().as_bytes());
        framed.extend_from_slice(args);
        self.fast(&framed)
      }
      Arm::Slow => self.slow(cmd, args),
    }
  }

  /// 写入已过期 TTL 记录（过去刻度直写，不经 EXPIRE 的「过去即删」语义，
  /// 复现「已过期未清退」窗）
  fn seed_expired_ttl(&self, key: &[u8]) {
    let session = Arc::clone(&self.store);
    let key = key.to_vec();
    self.rt.block_on(async {
      let s = session.new_session().unwrap();
      s.put_ttl(&key, now_ticks() - TICKS_PER_SECOND)
        .await
        .unwrap();
    });
  }

  /// 手工升阶 hash 至 bftree（与 wkv promote 测试同款 entries 形）
  fn promote_hash(&self, key: &[u8], fields: &[(&[u8], &[u8])]) {
    let ents: Vec<(Vec<u8>, Vec<u8>)> = fields
      .iter()
      .map(|(f, v)| (f.to_vec(), v.to_vec()))
      .collect();
    let session = Arc::clone(&self.store);
    let key = key.to_vec();
    self.rt.block_on(async {
      let s = session.new_session().unwrap();
      s.promote_collection_to_bftree(&key, GarnetObjectType::Hash, ents, i64::MAX, false)
        .await
        .unwrap();
    });
  }

  /// 物理域取证：指定 tag 的物理记录是否在场（墓碑按缺席计，幽灵残留判据）
  fn record_present(&self, tag: KeyTag, key: &[u8]) -> bool {
    let store = Arc::clone(&self.store);
    let rec_k = NamespaceDbCodec::encode_tagged_key(0, 0, tag, key);
    self.rt.block_on(async move {
      store
        .new_session()
        .unwrap()
        .read_raw(&rec_k)
        .await
        .unwrap()
        .is_some()
    })
  }
}

/// 分层树身份键（= 物理 Meta 键，默认会话域；与 wkv 紧缩清退锁测 id_key 同形）
fn tree_id_key(user_key: &[u8]) -> TaggedKeyBuf {
  NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::Meta, user_key)
}

/// 夹具 a：已过期未清退集合对象键 × INCR 重建——信封幽灵须物理消亡，
/// 快慢双臂应答与终态逐字节全等
#[test]
fn expired_object_key_incr_rebuild_retires_envelope() {
  let mut e = env("rmw-rebuild-env.db");
  for arm in [Arm::Fast, Arm::Slow] {
    let key = format!("rb:cnt:{}", arm.label());
    let kb = key.as_bytes();
    assert_eq!(
      e.fast(&[b"SADD", kb, b"ghost"]),
      b":1\r\n",
      "{} 铺对象键",
      arm.label()
    );
    e.seed_expired_ttl(kb);
    assert!(
      e.record_present(KeyTag::ObjectEnvelope, kb),
      "{} 前置：信封记录须在簿（幽灵源）",
      arm.label()
    );

    assert_eq!(
      e.exec(arm, RespCommand::Incr, &[kb]),
      b":1\r\n",
      "{} INCR 过期对象键按缺失重建",
      arm.label()
    );
    assert_eq!(
      e.fast(&[b"GET", kb]),
      bulk(b"1"),
      "{} 重建值可见",
      arm.label()
    );
    assert_eq!(
      e.fast(&[b"TYPE", kb]),
      b"+string\r\n",
      "{} 重建后类型须 string（修复前回 set 幽灵）",
      arm.label()
    );
    // 重建后键为活 string 值：集合面按跨型裁决回 WRONGTYPE（C#
    // Objects/SetCommands.cs:SetMembers 对 NOTFOUND 才回空集，string 记录走
    // WRONGTYPE 臂；真 Redis 同形）。空集 *0 形只在键判缺失（未重建或重建前
    // 探针窗）时出现——本臂锁「旧成员不可再读」终态：信封已物理清退、值域
    // 判跨型拒，幽灵成员无从复活（与 object_cross_type_regression.rs 跨型
    // 同口径；修复前形为 *1 旧成员，系跨域脑裂）
    assert_eq!(
      e.fast(&[b"SMEMBERS", kb]),
      format!("-{RESP_ERR_WRONG_TYPE}\r\n").into_bytes(),
      "{} 重建后集合面须跨型拒（修复前回旧成员）",
      arm.label()
    );
    assert_eq!(e.fast(&[b"EXISTS", kb]), b":1\r\n");
    assert_eq!(
      e.fast(&[b"TTL", kb]),
      b":-1\r\n",
      "{} 重建无 TTL（过期凭据已清退）",
      arm.label()
    );

    assert!(
      !e.record_present(KeyTag::ObjectEnvelope, kb),
      "{} 重建后信封幽灵残留",
      arm.label()
    );
    assert!(
      e.record_present(KeyTag::String, kb),
      "{} 重建值须落 String 域",
      arm.label()
    );
  }
  assert_eq!(
    e.fast(&[b"GET", b"rb:cnt:fast"]),
    e.fast(&[b"GET", b"rb:cnt:slow"]),
    "快慢臂终态逐字节全等"
  );
}

/// 夹具 b：过期分层键（Meta 存根 + bftree 数据文件）× INCRBY 重建——同步臂
/// Meta 在场必降级，异步级联须注销树实例并物理释放树文件，杜绝磁盘孤儿
#[test]
fn expired_tiered_key_incrby_rebuild_retires_tree() {
  let mut e = env("rmw-rebuild-tiered.db");
  for arm in [Arm::Fast, Arm::Slow] {
    let key = format!("rb:tiered:{}", arm.label());
    let kb = key.as_bytes();
    let mgr = Arc::clone(e.store.range_index());
    let base_reserved = mgr.cache_reserved();
    e.promote_hash(kb, &[(b"f1", b"v1"), (b"f2", b"v2")]);
    let tree_key = tree_id_key(kb);
    let data_path = mgr.data_file_path_for_key(&tree_key);
    assert!(
      mgr.get_tree(&tree_key).is_some(),
      "{} 前置：树实例须在注册表在册",
      arm.label()
    );
    assert!(
      e.record_present(KeyTag::Meta, kb),
      "{} 前置：分层存根须在簿",
      arm.label()
    );
    assert!(
      mgr.cache_reserved() > base_reserved,
      "{} 前置：树页缓存预算须在账",
      arm.label()
    );
    assert!(data_path.exists(), "{} 前置：树数据文件须在盘", arm.label());
    e.seed_expired_ttl(kb);

    assert_eq!(
      e.exec(arm, RespCommand::Incrby, &[kb, b"5"]),
      b":5\r\n",
      "{} INCRBY 过期分层键按缺失重建",
      arm.label()
    );
    assert_eq!(
      e.fast(&[b"GET", kb]),
      bulk(b"5"),
      "{} 重建值可见",
      arm.label()
    );
    assert_eq!(
      e.fast(&[b"TYPE", kb]),
      b"+string\r\n",
      "{} 重建后类型须 string（修复前 Meta 幽灵回 hash）",
      arm.label()
    );
    assert!(
      !e.record_present(KeyTag::Meta, kb),
      "{} 重建后分层存根残留",
      arm.label()
    );
    assert!(
      mgr.get_tree(&tree_key).is_none(),
      "{} 重建后树实例须注销，杜绝注册表孤儿",
      arm.label()
    );
    assert!(
      !data_path.exists(),
      "{} 重建后树数据文件须物理释放（磁盘孤儿）",
      arm.label()
    );
    assert_eq!(
      mgr.cache_reserved(),
      base_reserved,
      "{} 重建后页缓存预算须归还",
      arm.label()
    );
  }
}

/// 夹具 c：INCR/DECR/DECRBY/INCRBYFLOAT/SETRANGE/APPEND 六臂同挂 RMW 重建口——
/// 同夹具快慢双臂应答与终态逐字节全等，逐臂信封幽灵齐清
#[test]
fn string_rmw_family_rebuild_arms_identical_and_retire_envelope() {
  let mut e = env("rmw-rebuild-family.db");
  type RmwCase = (
    RespCommand,
    &'static [&'static [u8]],
    &'static [u8],
    &'static [u8],
  );
  let cases: &[RmwCase] = &[
    (RespCommand::Incr, &[], b":1\r\n", b"1"),
    (RespCommand::Decr, &[], b":-1\r\n", b"-1"),
    (RespCommand::Decrby, &[b"3"], b":-3\r\n", b"-3"),
    (
      RespCommand::Incrbyfloat,
      &[b"0.5"],
      b"$3\r\n0.5\r\n",
      b"0.5",
    ),
    (RespCommand::Setrange, &[b"1", b"XY"], b":3\r\n", b"\x00XY"),
    (RespCommand::Append, &[b"cd"], b":2\r\n", b"cd"),
  ];
  for arm in [Arm::Fast, Arm::Slow] {
    for (idx, (cmd, tail, expect_reply, expect_val)) in cases.iter().enumerate() {
      let key = format!("rb:fam:{}:{}", arm.label(), idx);
      let kb = key.as_bytes();
      assert_eq!(e.fast(&[b"SADD", kb, b"ghost"]), b":1\r\n");
      e.seed_expired_ttl(kb);

      let mut args: Vec<&[u8]> = vec![kb];
      args.extend_from_slice(tail);
      assert_eq!(
        e.exec(arm, *cmd, &args),
        *expect_reply,
        "{} 命令 {idx} 应答帧",
        arm.label()
      );
      assert_eq!(
        e.fast(&[b"GET", kb]),
        bulk(expect_val),
        "{} 命令 {idx} 重建终态值",
        arm.label()
      );
      assert!(
        !e.record_present(KeyTag::ObjectEnvelope, kb),
        "{} 命令 {idx} 重建后信封幽灵残留",
        arm.label()
      );
    }
  }
  for idx in 0..cases.len() {
    let fast = e.fast(&[b"GET", format!("rb:fam:fast:{idx}").as_bytes()]);
    let slow = e.fast(&[b"GET", format!("rb:fam:slow:{idx}").as_bytes()]);
    assert_eq!(fast, slow, "第 {idx} 形快慢臂终态逐字节全等");
  }
}

/// 夹具 a 尾段：重建后 SAVE（生产快照发布口）→ recover 重载逐字段全等，
/// 断旁域清退落物理墓碑而非仅内存遮蔽
#[test]
fn expired_object_key_rebuild_survives_save_and_recover() {
  let rt = Runtime::new().unwrap();
  let dir = tempfile::tempdir().unwrap().keep();
  let cp_dir = dir.join("checkpoints");
  let db_file = dir.join("rebuild_recover.db");
  let token = {
    let mut config = test_store_config();
    config.gc.enabled = false;
    let device = Arc::new(SegmentedDevice::single_file(&db_file).unwrap());
    let store = Arc::new(WedbStore::open(config, device).unwrap());
    let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
    let mut c = RespSessionConsumer::new(1, RespServerSessionOptions::default(), api.clone());

    assert_eq!(
      fast(&rt, &mut c, &[b"SADD", b"rec:k", b"ghost"]),
      b":1\r\n",
      "铺对象键"
    );
    // 已过期 TTL 记录直写，复现「已过期未清退」窗
    let store2 = Arc::clone(&store);
    rt.block_on(async {
      store2
        .new_session()
        .unwrap()
        .put_ttl(b"rec:k", now_ticks() - TICKS_PER_SECOND)
        .await
        .unwrap();
    });
    assert_eq!(fast(&rt, &mut c, &[b"INCR", b"rec:k"]), b":1\r\n");
    assert_eq!(
      fast(&rt, &mut c, &[b"TYPE", b"rec:k"]),
      b"+string\r\n",
      "重建后命令面类型须 string"
    );

    drop(c);
    drop(api);
    let db = Arc::new(GarnetDatabase::new(
      0,
      Arc::clone(&store),
      Arc::clone(&store.device),
      cp_dir.clone(),
      None,
    ));
    let mgr = SingleDatabaseManager::new(cp_dir.clone(), Arc::clone(&db));
    assert!(
      rt.block_on(mgr.take_checkpoint(true)).unwrap(),
      "快照须发布"
    );
    let token = wcpr::find_latest_checkpoint(&cp_dir)
      .unwrap()
      .expect("SAVE 后必须存在快照");
    drop(mgr);
    drop(db);
    drop(store);
    token
  };

  // 重启恢复（全新设备句柄 + 检查点恢复口，database_manager.rs 同形）：
  // 重建终态 1:1 重现，信封幽灵在恢复面同样缺席
  rt.block_on(async move {
    let device = Arc::new(SegmentedDevice::single_file(&db_file).unwrap());
    let recovered = Arc::new(WedbStore::recover(&cp_dir, token, device).await.unwrap());
    let session = recovered.new_session().unwrap();
    assert_eq!(
      session.read(b"rec:k").await.unwrap().as_deref(),
      Some(b"1".as_slice()),
      "重载后重建值全等"
    );
    let env_k = NamespaceDbCodec::encode_tagged_key(0, 0, KeyTag::ObjectEnvelope, b"rec:k");
    assert!(
      session.read_raw(&env_k).await.unwrap().is_none(),
      "重载后信封幽灵残留（清退未落物理墓碑）"
    );
  });
}

/// 快臂往返单实现（`Env::fast` 与会话装配各异的重载夹具共用的借用形态）
fn fast(rt: &Runtime, c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut scratch = c.take_recv_scratch();
  scratch.extend_from_slice(&frame(args));
  c.return_recv_scratch(scratch);
  let mut resp = Vec::new();
  let _ = c.try_consume_messages_into(&mut resp);
  if let Some(slow) = c.take_slow_wait() {
    resp.extend_from_slice(&rt.block_on(slow.resolve()));
  }
  resp
}
