#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! MGET/MSET/MSETNX/DEL/LCS 批量命令前缀外提与批量写折叠回归测试
//!
//! 对标 garnet/libs/server/Resp/ArrayCommands.cs:NetworkMGET/NetworkMSET/
//! NetworkMSETNX/NetworkDEL/NetworkLCS；折叠与前缀外提为 transpile SKILL
//! 工程准则（rust 工程优化无 c# 对应），本套件锁定语义等价性：MSET 重复键
//! 后者胜、对象键覆盖、信封键 MGET 答 nil、DEL 计数、切库后批量命令使用
//! 新前缀、LCS LEN+IDX 冲突错误文案、MGET/LCS 快臂会话级 found/notfound
//! 入账口径与降级不双计。

use std::sync::Arc;

use compio::runtime::Runtime;
use tempfile::tempdir;
use wkv::StoreConfig;
use wmetric::SessionMetricsHandle;
use wnode::{
  MessageConsumerFace,
  resp::garnet_api::{GarnetApi, StoreGarnetApi},
};
use wnode_test::{
  auto_exec, counts, err_frame, feed, metrics_env, roundtrip, session_on as session_with,
  with_batch,
};
use wresp::{cmd_strings::RESP_ERR_WRONG_TYPE, command::RespCommand};

/// MSET 批量写 → MGET 回读 → DEL 计数（含未命中键）流水线
#[test]
fn mset_mget_del_roundtrip() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    assert!(
      s.network_mset(&[b"k1", b"v1", b"k2", b"v2", b"k3", b"v3"], batch, &mut out)
        .unwrap()
    );
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    assert!(
      s.network_mget(&[b"k1", b"k2", b"missing"], batch, &mut out)
        .unwrap()
    );
    assert_eq!(out, b"*3\r\n$2\r\nv1\r\n$2\r\nv2\r\n$-1\r\n");

    out.clear();
    assert!(
      s.network_del(&[b"k1", b"missing", b"k2"], batch, &mut out)
        .unwrap()
    );
    assert_eq!(out, b":2\r\n");

    // DEL 后 MGET 全部缺失
    out.clear();
    assert!(s.network_mget(&[b"k1", b"k2"], batch, &mut out).unwrap());
    assert_eq!(out, b"*2\r\n$-1\r\n$-1\r\n");
  });
}

/// MSET 重复键后者胜（批量折叠相邻去重保末值语义等价）
#[test]
fn mset_dup_key_last_wins() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    assert!(
      s.network_mset(
        &[b"dk", b"first", b"dk", b"second", b"other", b"ov"],
        batch,
        &mut out
      )
      .unwrap()
    );
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    assert!(s.network_mget(&[b"dk", b"other"], batch, &mut out).unwrap());
    assert_eq!(out, b"*2\r\n$6\r\nsecond\r\n$2\r\nov\r\n");
  });
}

/// 集合对象键（信封域）MGET 答 nil 不报错（Redis MGET 非字符串键同答 nil）
#[test]
fn mget_envelope_key_answers_nil() {
  with_batch(|s, batch| {
    // 先经 HSET 建立信封域对象键
    let mut out = Vec::new();
    s.hash_set(&[b"hkey", b"f1", b"v1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // MGET 混合 String 与信封键：信封键对位 nil
    out.clear();
    assert!(s.network_mset(&[b"skey", b"sv"], batch, &mut out).unwrap());
    out.clear();
    assert!(
      s.network_mget(&[b"skey", b"hkey"], batch, &mut out)
        .unwrap()
    );
    assert_eq!(out, b"*2\r\n$2\r\nsv\r\n$-1\r\n");

    // DEL 信封键：信封域无对象元记录时快路径闭环删除应答 :1，删除后 MGET 缺失
    out.clear();
    assert!(s.network_del(&[b"hkey"], batch, &mut out).unwrap());
    assert_eq!(out, b":1\r\n");
    out.clear();
    assert!(s.network_mget(&[b"hkey"], batch, &mut out).unwrap());
    assert_eq!(out, b"*1\r\n$-1\r\n");
  });
}

/// MSETNX 批量折叠回归：全不存在整批写入答 :1、任一存在整体拒绝答 :0、
/// 重复键后者胜（判定循环与写循环共用外提前缀，语义与逐键循环等价）
#[test]
fn msetnx_batch_all_or_nothing() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    // 全不存在：整批写入成功
    assert!(
      s.network_msetnx(
        &[b"nx1", b"v1", b"nx2", b"v2", b"nx1", b"v1b"],
        batch,
        None,
        &mut out
      )
      .unwrap()
    );
    assert_eq!(out, b":1\r\n", "全不存在应答 :1");

    // 重复键后者胜，先值被覆盖
    out.clear();
    assert!(s.network_mget(&[b"nx1", b"nx2"], batch, &mut out).unwrap());
    assert_eq!(out, b"*2\r\n$3\r\nv1b\r\n$2\r\nv2\r\n");

    // 任一键已存在：整体拒绝且不写任何新键（全有或全无）
    out.clear();
    assert!(
      s.network_msetnx(&[b"nx3", b"v3", b"nx1", b"again"], batch, None, &mut out)
        .unwrap()
    );
    assert_eq!(out, b":0\r\n", "任一存在应答 :0");
    out.clear();
    assert!(s.network_mget(&[b"nx3"], batch, &mut out).unwrap());
    assert_eq!(out, b"*1\r\n$-1\r\n", "拒绝批次不得写入任何新键");
  });
}

/// 批量命令前缀外提正确性：SELECT 切库后 MSET/MGET/DEL 使用新库前缀，跨库不可见
#[test]
fn array_commands_honor_active_db_switch() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    // db0 写 db0key
    assert!(
      s.network_mset(&[b"db0key", b"in0"], batch, &mut out)
        .unwrap()
    );

    // 切 db1：SELECT 依赖 allowMultiDb 配置，此处直接经会话原子变量切库
    // （network_select 在集群/单库形态下拒绝非 0 库，测试环境直改会话上下文
    // 等价覆盖前缀外提重算路径）
    batch.session.set_context(0, 1);

    // db1 写同名键不同值，回读互不串库
    out.clear();
    assert!(
      s.network_mset(&[b"db0key", b"in1"], batch, &mut out)
        .unwrap()
    );
    out.clear();
    assert!(s.network_mget(&[b"db0key"], batch, &mut out).unwrap());
    assert_eq!(out, b"*1\r\n$3\r\nin1\r\n");

    // 删 db1 前缀版本，db0 值不受影响
    out.clear();
    assert!(s.network_del(&[b"db0key"], batch, &mut out).unwrap());
    assert_eq!(out, b":1\r\n");
    batch.session.set_context(0, 0);
    out.clear();
    assert!(s.network_mget(&[b"db0key"], batch, &mut out).unwrap());
    assert_eq!(out, b"*1\r\n$3\r\nin0\r\n");
  });
}

/// MSET 遇集合对象键覆盖为字符串（C# NetworkMSET WRONGTYPE 分支 promote 事务
/// DELETE + SET 的等价终态：rust 由 String 域写原语内建级联清退信封/Meta 域承接）
#[test]
fn mset_overwrites_object_key() {
  with_batch(|s, batch| {
    // 先经 HSET 建立信封域对象键
    let mut out = Vec::new();
    s.hash_set(&[b"hkey", b"f1", b"v1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // MSET 覆盖对象键：+OK 且字符串域生效
    out.clear();
    assert!(
      s.network_mset(&[b"hkey", b"strval"], batch, &mut out)
        .unwrap()
    );
    assert_eq!(out, b"+OK\r\n");
    out.clear();
    assert!(s.network_get(&[b"hkey"], batch, &mut out).unwrap());
    assert_eq!(out, b"$6\r\nstrval\r\n");

    // 对象域已级联清退：TYPE 转 string，HGET 报 WRONGTYPE
    out.clear();
    assert!(s.network_type(&[b"hkey"], batch, None, &mut out).unwrap());
    assert_eq!(out, b"+string\r\n");
    out.clear();
    s.hash_get(&[b"hkey", b"f1"], batch, &mut out).unwrap();
    assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE));
  });
}

/// LCS 同时携带 LEN 与 IDX：C# RESP_ERR_LENGTH_AND_INDEXES 常量无 ERR 前缀
///（RespWriteUtils.TryWriteError 直加 `-` 成帧），1:1 保留 garnet quirk
#[test]
fn lcs_len_idx_conflict_error() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    assert!(
      s.network_set(&[b"k1", b"abc"], batch, None, &mut out)
        .unwrap()
    );
    out.clear();
    assert!(
      s.network_set(&[b"k2", b"bcd"], batch, None, &mut out)
        .unwrap()
    );

    out.clear();
    assert!(
      s.network_lcs(&[b"k1", b"k2", b"LEN", b"IDX"], batch, &mut out)
        .unwrap()
    );
    assert_eq!(
      out,
      b"-If you want both the length and indexes, please just use IDX.\r\n"
    );

    // 仅 IDX 不冲突，正常出 matches（map 头 RESP2 退化 *4：matches 一段 + len=2）
    out.clear();
    assert!(
      s.network_lcs(&[b"k1", b"k2", b"IDX"], batch, &mut out)
        .unwrap()
    );
    assert!(out.starts_with(b"*4\r\n$7\r\nmatches\r\n*1\r\n"));
  });
}

/// MGET 快路径对象键计入 notfound 指标回归（zcode-r25-batch 发现二）
///
/// 对齐 C# MGetReadArgBatch.SetStatus（非 Found 即计入 notfound）：
/// String 域命中计 found，信封域命中（WrongType）与缺失（Missing）均计入 notfound
#[test]
fn mget_metrics_accumulates_notfound_for_wrongtype_and_missing() {
  with_batch(|s, batch| {
    let metrics = Arc::new(SessionMetricsHandle::default());
    s.attach_session_metrics(Some(Arc::clone(&metrics)));

    // skey 为普通字符串键，hkey 为哈希信封键
    let mut out = Vec::new();
    assert!(
      s.network_set(&[b"skey", b"hello"], batch, None, &mut out)
        .unwrap()
    );
    out.clear();
    s.hash_set(&[b"hkey", b"f1", b"v1"], batch, &mut out)
      .unwrap();
    out.clear();

    // MGET [skey, hkey, missing]：skey 命中，hkey 错型回 nil，missing 缺失回 nil
    assert!(
      s.network_mget(&[b"skey", b"hkey", b"missing"], batch, &mut out)
        .unwrap()
    );
    assert_eq!(out, b"*3\r\n$5\r\nhello\r\n$-1\r\n$-1\r\n");

    let snap = metrics.snapshot();
    assert_eq!(snap.total_found, 1, "skey 应计入 found");
    assert_eq!(
      snap.total_notfound, 2,
      "hkey (WrongType) 与 missing 必须均计入 notfound"
    );
  });
}

fn open_env(tag: &str) -> (Runtime, GarnetApi, tempfile::TempDir) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5).unwrap();
  let store = wnode_test::store_open(&dir, tag, config);
  (
    Runtime::new().unwrap(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())).into(),
    dir,
  )
}

/// DEL/UNLINK 快路径降级续传计数回归（zcode-r25-batch 发现一）
///
/// SET 普通串键 a，创建复合/RI 键 ri。快路径先删除 a（已删计数 1），
/// 遇 ri 的 Meta 在场触发降级 Ok(false)，SlowWait 快照传递已删计数 1。
/// 慢路径继承起始计数 1，重放删除 ri 成功，总删除计数必须为 2（修复前少报为 1）
#[test]
fn del_and_unlink_degrade_preserves_deleted_count() {
  let (rt, api, _dir) = open_env("del-degrade.db");
  let mut s = session_with(&api);

  // 1. DEL 验证
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Set, &[b"a", b"1"]),
    b"+OK\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Ricreate,
      &[b"ri_del", b"DISK"]
    ),
    b"+OK\r\n"
  );
  // a 在快路径被删，ri_del 遇 Meta 降级慢路径，整命令应答 :2
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Del, &[b"a", b"ri_del"]),
    b":2\r\n"
  );
  // 两键均已被删除
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Get, &[b"a"]),
    b"$-1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Type, &[b"ri_del"]),
    b"+none\r\n"
  );

  // 2. UNLINK 验证同臂
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Set, &[b"b", b"2"]),
    b"+OK\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Ricreate,
      &[b"ri_unlink", b"DISK"]
    ),
    b"+OK\r\n"
  );
  assert_eq!(
    auto_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Unlink,
      &[b"b", b"ri_unlink"]
    ),
    b":2\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Get, &[b"b"]),
    b"$-1\r\n"
  );
  assert_eq!(
    auto_exec(&api, &rt, &mut s, RespCommand::Type, &[b"ri_unlink"]),
    b"+none\r\n"
  );
}

/// LCS 快臂会话级指标入账回归（票 wnode-lcs-fast-path-session-metrics-missing）
///
/// 对标 C# MainStoreOps.cs LCSInternal 两次 GET 经 :29/:38 incr_session_found/
/// notfound 逐键按实恒计（WRONGTYPE 臂静默 return 不计数）：命中计 found、
/// 缺失计 notfound、入账与应答形态（默认/LEN）解耦、WRONGTYPE 错误键本身
/// 不入账且首键即错时次键不读不计、选项解析失败面零触达零入账。修复前快臂
/// read_user_sync 传 None 且收尾零补偿，纯内存命中恒计 0（本测即红灯）
#[test]
fn lcs_fast_arm_session_metrics_counts_every_read() {
  let (rt, mut c, _api, handle, _dir, _store) = metrics_env("lcs-account-fast.db");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"k1", b"abcdef"]),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"k2", b"abcxyz"]),
    b"+OK\r\n"
  );
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"HSET", b"hk", b"f", b"v"]),
    b":1\r\n"
  );

  // 双命中 → found 恰 2（修复前 0/0 红灯）
  let (f0, n0) = counts(&handle);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LCS", b"k1", b"k2"]),
    b"$3\r\nabc\r\n"
  );
  assert_eq!(
    counts(&handle),
    (f0 + 2, n0),
    "双命中 LCS 恒计 2 found（C# 两次 GET 各一条）"
  );

  // 命中 1 缺失 1 → found+1 / notfound+1
  let (f1, n1) = counts(&handle);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LCS", b"k1", b"miss"]),
    b"$0\r\n\r\n"
  );
  assert_eq!(counts(&handle), (f1 + 1, n1 + 1), "半缺失 LCS 各按实计");

  // 双缺失 → notfound 恰 2
  let (f2, n2) = counts(&handle);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LCS", b"m1", b"m2"]),
    b"$0\r\n\r\n"
  );
  assert_eq!(counts(&handle), (f2, n2 + 2), "双缺失 LCS 恒计 2 notfound");

  // 次键对象键 → WRONGTYPE 应答；首键命中已入账恰 1 found，错误键不入账
  // （C# 首 GET 计数先于次 GET WRONGTYPE return；严禁照抄 MGET 对象键计
  // notfound 的批次专属口径）
  let (f3, n3) = counts(&handle);
  let wt = roundtrip(&rt, &mut c, &[b"LCS", b"k1", b"hk"]);
  assert!(
    wt.starts_with(b"-WRONGTYPE"),
    "对象键次位 LCS 应答 WRONGTYPE: {wt:?}"
  );
  assert_eq!(
    counts(&handle),
    (f3 + 1, n3),
    "WRONGTYPE 提前回帧只入前键 found，错误键不计"
  );

  // 首键对象键 → 次键不读，整命令零入账
  let (f4, n4) = counts(&handle);
  let wt0 = roundtrip(&rt, &mut c, &[b"LCS", b"hk", b"k1"]);
  assert!(
    wt0.starts_with(b"-WRONGTYPE"),
    "对象键首位 LCS 应答 WRONGTYPE: {wt0:?}"
  );
  assert_eq!(counts(&handle), (f4, n4), "首键 WRONGTYPE 整命令零入账");

  // LEN 形态入账同恒计（计数与应答形态解耦，C# lenOnly 分支在两 GET 之后）
  let (f5, n5) = counts(&handle);
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"LCS", b"k1", b"k2", b"LEN"]),
    b":3\r\n"
  );
  assert_eq!(counts(&handle), (f5 + 2, n5), "LEN 形态恒计 2 found");

  // 选项解析失败面不触达存储（LEN+IDX 互斥）→ 零入账
  let (f6, n6) = counts(&handle);
  let bad = roundtrip(&rt, &mut c, &[b"LCS", b"k1", b"k2", b"LEN", b"IDX"]);
  assert!(bad.starts_with(b"-"), "LEN+IDX 冲突须为错误帧: {bad:?}");
  assert_eq!(counts(&handle), (f6, n6), "解析失败零触达零入账");
}

/// LCS 混合驻留降级不双计锁（同票执行方案验证点二）
///
/// 首键内存命中、次键磁盘候选形态：快臂若逐键即时入账（read_user_sync 直接
/// 传会话句柄的缺陷形），首键 Hit 帧已落账后次键 Deferred 整命令转慢重放，
/// 慢臂簿记入口 read_user 再逐键恰一条——双键命令净计 3 即双计红灯；正确
/// 形态快臂 deferred 静默回滚零入账，净计恰 2（慢臂唯一收口）。纯冷键形
/// 首键即 Deferred 无此不对称面，故选热-冷混驻布局取证
#[test]
fn lcs_mixed_residency_degrade_counts_once() {
  let (rt, mut c, _api, handle, _dir, store) = metrics_env("lcs-account-degrade.db");
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"k2", b"abcxyz"]),
    b"+OK\r\n"
  );
  // k2 冷化为磁盘候选后再写 k1，钉住「首键热命中、次键冷降级」不对称驻留
  rt.block_on(store.flush_and_evict_all()).unwrap();
  assert_eq!(
    roundtrip(&rt, &mut c, &[b"SET", b"k1", b"abcdef"]),
    b"+OK\r\n"
  );

  let (f0, n0) = counts(&handle);
  let mut out = feed(&mut c, &[b"LCS", b"k1", b"k2"]);
  let slow = c
    .take_slow_wait()
    .expect("测试前提：次键磁盘候选须使快路径降级挂起 SlowWait");
  out.extend_from_slice(&rt.block_on(slow.resolve()));
  assert_eq!(out, b"$3\r\nabc\r\n", "降级重放应答须与热路径逐字节一致");
  assert_eq!(
    counts(&handle),
    (f0 + 2, n0),
    "混合驻留降级经慢臂簿记入口恰计 2 found（快臂即时入账净计 3，双计红灯）"
  );

  // 纯冷键全降级形：首键即 Deferred，快臂须整体静默、慢臂唯一收口恰 2
  rt.block_on(store.flush_and_evict_all()).unwrap();
  let (f1, n1) = counts(&handle);
  let mut out = feed(&mut c, &[b"LCS", b"k1", b"k2"]);
  let slow = c
    .take_slow_wait()
    .expect("测试前提：双冷键 LCS 快路径应降级挂起 SlowWait");
  out.extend_from_slice(&rt.block_on(slow.resolve()));
  assert_eq!(
    out, b"$3\r\nabc\r\n",
    "冷键慢路径应答须与热键快路径逐字节一致"
  );
  assert_eq!(
    counts(&handle),
    (f1 + 2, n1),
    "全降级 LCS 经慢臂簿记入口恒计 2 found"
  );
}
