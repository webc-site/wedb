//! MGET/MSET/MSETNX/DEL/LCS 批量命令前缀外提与批量写折叠回归测试
//!
//! 对标 garnet/libs/server/Resp/ArrayCommands.cs:NetworkMGET/NetworkMSET/
//! NetworkMSETNX/NetworkDEL/NetworkLCS；折叠与前缀外提为 transpile SKILL
//! 工程准则（rust 工程优化无 c# 对应），本套件锁定语义等价性：MSET 重复键
//! 后者胜、对象键覆盖、信封键 MGET 答 nil、DEL 计数、切库后批量命令使用
//! 新前缀、LCS LEN+IDX 冲突错误文案。

use wnode_test::{err_frame, with_batch};
use wresp::cmd_strings::RESP_ERR_WRONG_TYPE;

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
      s.network_msetnx(&[b"nx3", b"v3", b"nx1", b"again"], batch, &mut out)
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
    assert!(s.network_set(&[b"k1", b"abc"], batch, &mut out).unwrap());
    out.clear();
    assert!(s.network_set(&[b"k2", b"bcd"], batch, &mut out).unwrap());

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
