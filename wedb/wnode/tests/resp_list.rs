use std::{mem::take, slice::from_ref, sync::Arc, time::Duration};

use compio::{
  runtime::Runtime,
  time::{sleep, timeout},
};
use tempfile::tempdir;
use wbase::ns_prefix::NsPrefix;
use wcol::{
  itembroker::{
    collection_item_broker::CollectionItemBroker,
    item_broker_face::{ItemBrokerFinisher, SharedItemBroker},
  },
  types::{garnet_object::LIST_SEQ_BASE, member_ttl::encode_member},
};
use wconf::RuntimeServerConfig;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    objects::collection_item_source::CollectionItemSource,
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
};
use wnode_test::{err_frame, with_batch};
use wresp::{
  cmd_strings::{RESP_ERR_GENERIC_SYNTAX_ERROR, RESP_ERR_SLOW_PATH_STORAGE, RESP_ERR_WRONG_TYPE},
  command::RespCommand,
};
use wval::GarnetObjectType;

/// test/standalone/Garnet.test.collections/RespListTests.cs:BasicLPUSHAndLPOP
#[test]
fn basic_lpush_and_lpop() {
  with_batch(|s, batch| {
    let key = b"List_Test";
    let val = b"Value-0";

    let mut out = Vec::new();
    s.list_push(&[key, val], batch, &mut out, true).unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.list_pop(&[key], batch, &mut out, true).unwrap();
    assert_eq!(out, b"$7\r\nValue-0\r\n");

    out.clear();
    s.network_exists(&[key], batch, None, &mut out).unwrap();
    assert_eq!(out, b":0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:BasicRPUSHAndRPOP
#[test]
fn basic_rpush_and_rpop() {
  with_batch(|s, batch| {
    let key = b"List_Test";
    let mut out = Vec::new();
    s.list_push(&[key, b"Value-0"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.list_pop(&[key], batch, &mut out, false).unwrap();
    assert_eq!(out, b"$7\r\nValue-0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:BasicLPUSHAndLRANGE
#[test]
fn basic_lpush_and_lrange() {
  with_batch(|s, batch| {
    let key = b"List_Test";
    let mut out = Vec::new();
    s.list_push(&[key, b"val_0", b"val_1", b"val_2"], batch, &mut out, true)
      .unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.list_length(&[key], batch, &mut out).unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.list_range(&[key, b"0", b"0"], batch, &mut out).unwrap();
    assert_eq!(out, b"*1\r\n$5\r\nval_2\r\n");

    out.clear();
    s.list_range(&[key, b"-3", b"2"], batch, &mut out).unwrap();
    assert_eq!(out, b"*3\r\n$5\r\nval_2\r\n$5\r\nval_1\r\n$5\r\nval_0\r\n");

    out.clear();
    s.list_range(&[key, b"-100", b"100"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*3\r\n$5\r\nval_2\r\n$5\r\nval_1\r\n$5\r\nval_0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:BasicRPUSHAndLINDEX
#[test]
fn basic_rpush_and_lindex() {
  with_batch(|s, batch| {
    let key = b"List_Test";
    let mut out = Vec::new();
    s.list_push(&[key, b"val_0", b"val_1", b"val_2"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.list_index(&[key, b"0"], batch, &mut out).unwrap();
    assert_eq!(out, b"$5\r\nval_0\r\n");

    out.clear();
    s.list_index(&[key, b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"$5\r\nval_2\r\n");

    out.clear();
    s.list_index(&[key, b"99"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:BasicRPUSHAndLINSERT
#[test]
fn basic_rpush_and_linsert() {
  with_batch(|s, batch| {
    let key = b"List_Test";
    let mut out = Vec::new();
    s.list_push(&[key, b"val_0", b"val_1"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    out.clear();
    s.list_insert(&[key, b"BEFORE", b"val_1", b"val_mid"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.list_range(&[key, b"0", b"-1"], batch, &mut out).unwrap();
    assert_eq!(
      out,
      b"*3\r\n$5\r\nval_0\r\n$7\r\nval_mid\r\n$5\r\nval_1\r\n"
    );
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:BasicRPUSHAndLREM
#[test]
fn basic_rpush_and_lrem() {
  with_batch(|s, batch| {
    let key = b"List_Test";
    let mut out = Vec::new();
    s.list_push(&[key, b"val_0", b"val_1", b"val_0"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.list_remove(&[key, b"1", b"val_0"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.list_range(&[key, b"0", b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"*2\r\n$5\r\nval_1\r\n$5\r\nval_0\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:BasicLPUSHAndLTRIM
#[test]
fn basic_lpush_and_ltrim() {
  with_batch(|s, batch| {
    let key = b"List_Test";
    let mut out = Vec::new();
    s.list_push(&[key, b"val_0", b"val_1", b"val_2"], batch, &mut out, false)
      .unwrap();

    out.clear();
    s.list_trim(&[key, b"1", b"2"], batch, &mut out).unwrap();
    assert_eq!(out, b"+OK\r\n");

    out.clear();
    s.list_range(&[key, b"0", b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"*2\r\n$5\r\nval_1\r\n$5\r\nval_2\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:CanDoRPopLPush
#[test]
fn can_do_rpop_lpush() {
  with_batch(|s, batch| {
    let key = b"mylist";
    let mut out = Vec::new();
    s.list_push(
      &[key, b"Value-one", b"Value-two", b"Value-three"],
      batch,
      &mut out,
      false,
    )
    .unwrap();

    out.clear();
    s.list_right_pop_left_push(&[b"mylist", b"myotherlist"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$11\r\nValue-three\r\n");

    out.clear();
    s.list_range(&[b"myotherlist", b"0", b"-1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*1\r\n$11\r\nValue-three\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:CanUseLMoveGC 同键 LMOVE 旋转臂（mylist→mylist）
#[test]
fn lmove_same_key_singleton_returns_correct_value() {
  with_batch(|s, batch| {
    let key = b"singleton_list";
    let mut out = Vec::new();
    s.list_push(&[key, b"only_elem"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    // LMOVE 同键单元素旋转（例如 LEFT RIGHT 或 RIGHT LEFT）
    out.clear();
    s.list_move(&[key, key, b"LEFT", b"RIGHT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$9\r\nonly_elem\r\n");

    // 验证元素仍完整存在
    out.clear();
    s.list_range(&[key, b"0", b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"*1\r\n$9\r\nonly_elem\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:ListMoveWrongTypeDestinationDoesNotLoseElement
#[test]
fn lmove_destination_wrong_type_does_not_corrupt_source() {
  with_batch(|s, batch| {
    let src = b"lmove_src";
    let dst = b"lmove_dst_str";
    let mut out = Vec::new();

    // 目标键存字符串
    s.network_set(&[dst, b"str_val"], batch, None, &mut out)
      .unwrap();
    assert_eq!(out, b"+OK\r\n");

    // 源键推入元素
    out.clear();
    s.list_push(&[src, b"elem1", b"elem2"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":2\r\n");

    // LMOVE 目标键类型错误，应返回 WRONGTYPE 且不损坏源键
    out.clear();
    s.list_move(&[src, dst, b"LEFT", b"RIGHT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, err_frame(RESP_ERR_WRONG_TYPE));

    // 源键元素完整保留
    out.clear();
    s.list_range(&[src, b"0", b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"*2\r\n$5\r\nelem1\r\n$5\r\nelem2\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:LPOSWithoutOptions
#[test]
fn lpos_without_options() {
  with_batch(|s, batch| {
    let key = b"KeyA";
    let mut out = Vec::new();
    s.list_push(&[key, b"a", b"c", b"b", b"c", b"d"], batch, &mut out, false)
      .unwrap();
    assert_eq!(out, b":5\r\n");

    // 命中：首现下标
    out.clear();
    s.list_position(&[key, b"c"], batch, &mut out).unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.list_position(&[key, b"d"], batch, &mut out).unwrap();
    assert_eq!(out, b":4\r\n");

    // 未命中（缺省 COUNT）：RESP2 null
    out.clear();
    s.list_position(&[key, b"nx"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:LPOSWithInvalidKey
#[test]
fn lpos_with_invalid_key() {
  with_batch(|s, batch| {
    let key = b"KeyA";
    let mut out = Vec::new();

    // 键缺失：无 COUNT → null；含 COUNT → 空数组（命令层 NOTFOUND 分支）
    s.list_position(&[key, b"nx"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");

    out.clear();
    s.list_position(&[key, b"nx", b"COUNT", b"3"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");

    // 键存在但元素不匹配：同样 null / 空数组
    out.clear();
    s.list_push(&[key, b"e"], batch, &mut out, true).unwrap();
    out.clear();
    s.list_position(&[key, b"nx"], batch, &mut out).unwrap();
    assert_eq!(out, b"$-1\r\n");

    out.clear();
    s.list_position(&[key, b"nx", b"COUNT", b"3"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");

    out.clear();
    s.list_position(&[key, b"nx", b"cOuNt", b"3"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*0\r\n");

    // RESP3：缺省形态无匹配回 `_`（C# RespMemoryWriter.WriteNull 按版本）
    s.resp_protocol_version = 3;
    out.clear();
    s.list_position(&[key, b"nx"], batch, &mut out).unwrap();
    assert_eq!(out, b"_\r\n");
  });
}

/// test/standalone/Garnet.test.collections/RespListTests.cs:LPOSWithOptions
///（rank/count/maxlen 组合；列表 [a,c,b,c,d]）
#[test]
fn lpos_with_options() {
  with_batch(|s, batch| {
    let key = b"KeyA";
    let mut out = Vec::new();
    s.list_push(&[key, b"a", b"c", b"b", b"c", b"d"], batch, &mut out, false)
      .unwrap();

    // 词元双形态：全大写 / 全小写（C# SequenceEqual RANK|rank）
    for rank_token in [b"RANK".as_slice(), b"rank"] {
      out.clear();
      s.list_position(&[key, b"c", rank_token, b"2"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":3\r\n");

      out.clear();
      s.list_position(&[key, b"c", rank_token, b"-2"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b":1\r\n");

      out.clear();
      s.list_position(&[key, b"c", rank_token, b"3"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"$-1\r\n");
    }

    // COUNT 形态：数组回复；count 0 = 全量
    for count_token in [b"COUNT".as_slice(), b"count"] {
      out.clear();
      s.list_position(&[key, b"c", count_token, b"2"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*2\r\n:1\r\n:3\r\n");

      out.clear();
      s.list_position(&[key, b"c", count_token, b"0"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*2\r\n:1\r\n:3\r\n");

      out.clear();
      s.list_position(&[key, b"c", count_token, b"9"], batch, &mut out)
        .unwrap();
      assert_eq!(out, b"*2\r\n:1\r\n:3\r\n");
    }

    // MAXLEN 截断扫描
    out.clear();
    s.list_position(&[key, b"c", b"MAXLEN", b"2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.list_position(&[key, b"c", b"maxlen", b"1"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$-1\r\n");

    // rank -1 + COUNT 0：自尾全量（顺序 3,1）
    out.clear();
    s.list_position(
      &[key, b"c", b"rank", b"-1", b"count", b"0"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"*2\r\n:3\r\n:1\r\n");

    // 混合大小写词元匹配（eq_ignore_ascii_case）
    out.clear();
    s.list_position(&[key, b"c", b"Rank", b"2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":3\r\n");

    out.clear();
    s.list_position(&[key, b"c", b"rAnK", b"-2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.list_position(&[key, b"c", b"cOuNt", b"2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"*2\r\n:1\r\n:3\r\n");

    out.clear();
    s.list_position(&[key, b"c", b"MaxLen", b"2"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b":1\r\n");

    out.clear();
    s.list_position(
      &[key, b"c", b"Rank", b"-1", b"cOuNt", b"0", b"MaxLen", b"2"],
      batch,
      &mut out,
    )
    .unwrap();
    assert_eq!(out, b"*1\r\n:3\r\n");

    // 非法选项仍报语法错误
    for opts in [
      [b"UNKNOWN".as_slice(), b"1".as_slice()],
      [b"Rankk", b"1"],
      [b"cOuNtX", b"2"],
      [b"Max_Len", b"3"],
    ] {
      out.clear();
      s.list_position(&[key, b"c", opts[0], opts[1]], batch, &mut out)
        .unwrap();
      assert_eq!(out, err_frame(RESP_ERR_GENERIC_SYNTAX_ERROR));
    }
  });
}

/// LMOVE 异键四形与同键旋转/窥视回归锁（票 wnode-lmove-writeback-order-replay-idempotency
/// 执行方案第 3 步 d）：钉死快臂写回序前移（先目标后源）后，无门零降级路径的
/// 应答与 src/dst 终态零漂移；同键旋转臂/同向窥视臂（严禁先 pop 再 push 防
/// TTL 丢失）不动，同形对拍
#[test]
fn lmove_cross_key_four_shapes_and_same_key_rotation() {
  with_batch(|s, batch| {
    let mut out = Vec::new();
    // 异键四形：src [a,b,c]，弹出端/推入端 LEFT|RIGHT 全组合
    for (si, (sd, dd)) in [
      ("LEFT", "LEFT"),
      ("LEFT", "RIGHT"),
      ("RIGHT", "LEFT"),
      ("RIGHT", "RIGHT"),
    ]
    .iter()
    .enumerate()
    {
      let src_key = format!("mv4_src_{si}");
      let dst_key = format!("mv4_dst_{si}");
      let (src, dst): (&[u8], &[u8]) = (src_key.as_bytes(), dst_key.as_bytes());
      out.clear();
      s.list_push(&[src, b"a", b"b", b"c"], batch, &mut out, false)
        .unwrap();
      assert_eq!(out, b":3\r\n");

      // 应答 = 弹出元素 bulk：LEFT 弹 a、RIGHT 弹 c
      let popped: &[u8] = if *sd == "LEFT" { b"a" } else { b"c" };
      let dirs: [&[u8]; 2] = [sd.as_bytes(), dd.as_bytes()];
      out.clear();
      s.list_move(&[src, dst, dirs[0], dirs[1]], batch, &mut out)
        .unwrap();
      assert_eq!(
        out,
        format!(
          "${}\r\n{}\r\n",
          popped.len(),
          String::from_utf8_lossy(popped)
        )
        .as_bytes()
      );

      // 推入端定 dst 仅含该元素；src 余两元素
      out.clear();
      s.list_range(&[dst, b"0", b"-1"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        format!(
          "*1\r\n${}\r\n{}\r\n",
          popped.len(),
          String::from_utf8_lossy(popped)
        )
        .as_bytes()
      );
      out.clear();
      s.list_range(&[src, b"0", b"-1"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        if *sd == "LEFT" {
          b"*2\r\n$1\r\nb\r\n$1\r\nc\r\n".to_vec()
        } else {
          b"*2\r\n$1\r\na\r\n$1\r\nb\r\n".to_vec()
        }
      );
      // dst 推入端形态：LEFT 入头、RIGHT 入尾（单元素列表下两者同帧，LINDEX -1 复核）
      out.clear();
      s.list_index(&[dst, b"-1"], batch, &mut out).unwrap();
      assert_eq!(
        out,
        format!(
          "${}\r\n{}\r\n",
          popped.len(),
          String::from_utf8_lossy(popped)
        )
        .as_bytes()
      );
    }

    // 同键旋转臂：LMOVE k k LEFT RIGHT 于 [a,b] → 弹出首元素 a 追加至尾、应答 a、终态 [b,a]
    let rot: &[u8] = b"mv4_rot";
    out.clear();
    s.list_push(&[rot, b"a", b"b"], batch, &mut out, false)
      .unwrap();
    out.clear();
    s.list_move(&[rot, rot, b"LEFT", b"RIGHT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$1\r\na\r\n");
    out.clear();
    s.list_range(&[rot, b"0", b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nb\r\n$1\r\na\r\n");

    // 同键同向窥视臂：不弹不推、零落库，列表原态
    out.clear();
    s.list_move(&[rot, rot, b"LEFT", b"LEFT"], batch, &mut out)
      .unwrap();
    assert_eq!(out, b"$1\r\nb\r\n");
    out.clear();
    s.list_range(&[rot, b"0", b"-1"], batch, &mut out).unwrap();
    assert_eq!(out, b"*2\r\n$1\r\nb\r\n$1\r\na\r\n");
  });
}

// ———— LMOVE/RPOPLPUSH 双键写回序（先目标后源）门重放幂等界测 ————
//（票 wnode-lmove-writeback-order-replay-idempotency 执行方案第 3 步 a/b/c）
//
// 危害旧序：快臂 src save 先于 dst，dst 门 Ok(false) 整命令重放非幂等——src
// 恰弹空则键回收、重放 Missing 分支回 null 元素蒸发；尚余则弹出下一个元素、
// 首个永久丢失且应答错位。修复后 dst 复验+save 前移至 src save 之前，首跑
// 零持久变异，重放自完整初态经慢臂升阶闭环收敛。三测分别钉：升阶 count 阈界、
// 小页配置 envelope_overflow 门同形界、迁移窗忙错夹具（stub 探测 Err 形）。

type GateStore = WedbStore<SegmentedDevice>;

/// 门界用例环境：页容量按参数收缩（64KB 形暴露 envelope_overflow 死区，
/// 1MB 形令 count 阈界载荷仍信封存活）
fn gate_env(page_size: usize) -> (Runtime, GarnetApi, Arc<GateStore>, tempfile::TempDir) {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, page_size, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join("lmove-gate.db")).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  (
    Runtime::new().unwrap(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
    store,
    dir,
  )
}

fn gate_session(api: &GarnetApi) -> RespServerSession {
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  s
}

/// 快臂门降级 → SlowWait → 慢臂整命令重放的同步泵（tiered_cmds_align 同款）
fn gate_exec(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  s.output.clear();
  api.exec(s, cmd, args);
  if !s.output.is_empty() {
    return take(&mut s.output);
  }
  let slow = s
    .take_slow_wait()
    .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
  let out = rt.block_on(slow.resolve());
  s.output.clear();
  out
}

/// 分块 RPUSH（每批回帧须为整数计数，写入侧拒绝当场红）
fn gate_push_all(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  key: &[u8],
  values: &[Vec<u8>],
) {
  for chunk in values.chunks(1000) {
    let mut args: Vec<Vec<u8>> = vec![key.to_vec()];
    args.extend(chunk.iter().cloned());
    let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    let rep = gate_exec(api, rt, s, RespCommand::Rpush, &slices);
    assert!(rep.starts_with(b":"), "RPUSH 批回帧须为整数计数: {rep:?}");
  }
}

/// 分层存根在场判据（键已落 wbftree 树态）
fn stub_present(rt: &Runtime, store: &Arc<GateStore>, key: &[u8]) -> bool {
  let sess = store.new_session().unwrap();
  rt.block_on(sess.load_collection_stub(key))
    .unwrap()
    .is_some()
}

/// 异值 600B 元素（3 字节区分头 + 'm' 填充，剥离 bitcode 同值折叠侥幸通道，
/// tiered_hset_mixed_large_value 同款口径）
fn mixed_elem(i: usize) -> Vec<u8> {
  let mut v = vec![
    b'A' + (i % 26) as u8,
    b'a' + (i / 26 % 26) as u8,
    b'0' + (i % 10) as u8,
  ];
  v.resize(600, b'm');
  v
}

/// 唯一 canary 元素（600B 异值，与全部 mixed_elem 内容相异且等长——信封载荷
/// 字节量等价，越页判据不因内容漂移）
fn canary_elem(tag: u8) -> Vec<u8> {
  let mut v = vec![b'*', b'*', tag];
  v.resize(600, b'z');
  v
}

/// bulk 帧构造helper
fn bulk(elem: &[u8]) -> Vec<u8> {
  format!("${}\r\n{}\r\n", elem.len(), String::from_utf8_lossy(elem)).into_bytes()
}

/// 界测 a：dst 推至升阶 count 阈界下 1 个（TIERED_PROMOTE_THRESHOLD-1，信封
/// 态），LMOVE src→dst 恰推入第阈界个元素——首跑 dst save 命中
/// should_promote 门 Ok(false) 整命令重放（修复前旧序此处 src 已持久弹出：
/// src 恰单元素弹空即键回收，重放 Missing 分支客户端收 null、元素蒸发）。
/// 断言：应答恒为弹出元素 bulk（禁 null/错位）、src 删空整键回收、dst 终态
/// 含且仅含该元素一处（COUNT 0 全量命中恰一处，双份残留即 *2 红）
#[test]
fn lmove_dst_promote_count_gate_replay_moves_element_exactly_once() {
  let (rt, api, _store, _dir) = gate_env(1024 * 1024);
  let mut s = gate_session(&api);
  let (src, dst): (&[u8], &[u8]) = (b"gate_cnt_src", b"gate_cnt_dst");

  let dst_seed: Vec<Vec<u8>> = (0..wcol::TIERED_PROMOTE_THRESHOLD - 1)
    .map(|i| format!("d{i}").into_bytes())
    .collect();
  gate_push_all(&api, &rt, &mut s, dst, &dst_seed);
  assert_eq!(
    gate_exec(&api, &rt, &mut s, RespCommand::Llen, &[dst]),
    format!(":{}\r\n", wcol::TIERED_PROMOTE_THRESHOLD - 1).as_bytes(),
    "前置判据：dst 停于升阶 count 阈界下 1 个（信封态）"
  );

  let moved: &[u8] = b"moved_canary_element";
  gate_push_all(&api, &rt, &mut s, src, &[moved.to_vec()]);

  // LMOVE src→dst 推入第 TIERED_PROMOTE_THRESHOLD 个：dst 门降级 → SlowWait
  // → 慢臂按完整初态重放，dst save 经升阶闭环落树、src 删空回收
  let rep = gate_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Lmove,
    &[src, dst, b"LEFT", b"RIGHT"],
  );
  assert_eq!(
    rep,
    bulk(moved),
    "应答恒为弹出元素 bulk（修复前旧序此处为 $-1 null：src 已弹空回收、重放 Missing 蒸发）"
  );
  assert_eq!(
    gate_exec(&api, &rt, &mut s, RespCommand::Exists, &[src]),
    b":0\r\n",
    "src 删空须整键回收"
  );
  assert_eq!(
    gate_exec(&api, &rt, &mut s, RespCommand::Llen, &[dst]),
    format!(":{}\r\n", wcol::TIERED_PROMOTE_THRESHOLD).as_bytes(),
    "dst 终态计数 = 阈界值（不虚增双份、不丢失）"
  );
  let hits = gate_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Lpos,
    &[dst, moved, b"COUNT", b"0"],
  );
  assert_eq!(
    hits,
    format!("*1\r\n:{}\r\n", wcol::TIERED_PROMOTE_THRESHOLD - 1).as_bytes(),
    "dst 含且仅含该元素一处、位于 RIGHT 推入尾位"
  );
}

/// 界测 b：小页容量（64KB）配置下 envelope_overflow 门同形界——逐元素猎页界
/// （越界那次 RPUSH 自身经闭环落树），DEL 复原界下 1 个的信封态后 LMOVE 推入
/// 等长异值 canary 恰超页。断言面同界测 a
#[test]
fn lmove_dst_envelope_overflow_gate_replay_moves_element_exactly_once() {
  let (rt, api, store, _dir) = gate_env(64 * 1024);
  let mut s = gate_session(&api);
  let (src, dst): (&[u8], &[u8]) = (b"gate_ovf_src", b"gate_ovf_dst");

  // 逐元素猎界：载荷越页的那一次 RPUSH 走门降级 → 慢臂超页升阶闭环落树
  let mut elems: Vec<Vec<u8>> = Vec::new();
  for i in 0..10_000 {
    elems.push(mixed_elem(i));
    let rep = gate_exec(
      &api,
      &rt,
      &mut s,
      RespCommand::Rpush,
      &[dst, elems.last().unwrap()],
    );
    assert!(rep.starts_with(b":"), "猎界 RPUSH 回帧须整数: {rep:?}");
    if stub_present(&rt, &store, dst) {
      break;
    }
  }
  let over = elems.len();
  assert!(
    (2..wcol::TIERED_PROMOTE_THRESHOLD).contains(&over),
    "猎界失败或误命中 count 门（应走体积超页门）: over={over}"
  );
  assert_eq!(
    gate_exec(&api, &rt, &mut s, RespCommand::Llen, &[dst]),
    format!(":{over}\r\n").as_bytes(),
    "越界推入经闭环落树，计数无损"
  );

  // 复原信封态于界下 1 个：DEL 后重灌前 over-1 个（猎界途中该载荷信封存活已实证）
  assert_eq!(
    gate_exec(&api, &rt, &mut s, RespCommand::Del, &[dst]),
    b":1\r\n"
  );
  elems.remove(over - 1);
  gate_push_all(&api, &rt, &mut s, dst, &elems);
  assert!(
    !stub_present(&rt, &store, dst),
    "前置判据：dst 须停于信封态界下 1 个（超页门测试对象）"
  );

  let moved = canary_elem(b'1');
  gate_push_all(&api, &rt, &mut s, src, from_ref(&moved));

  // LMOVE：dst 推入 canary 后信封载荷恰超页 → dst save 门 Ok(false) 整命令
  // 重放（修复前旧序 src 已持久弹出，重放弹空回 null）
  let rep = gate_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Lmove,
    &[src, dst, b"LEFT", b"RIGHT"],
  );
  assert_eq!(rep, bulk(&moved), "应答恒为弹出元素 bulk（禁 null/错位）");
  assert_eq!(
    gate_exec(&api, &rt, &mut s, RespCommand::Exists, &[src]),
    b":0\r\n",
    "src 删空整键回收"
  );
  assert_eq!(
    gate_exec(&api, &rt, &mut s, RespCommand::Llen, &[dst]),
    format!(":{over}\r\n").as_bytes(),
    "dst 终态计数 = 界下基线 + 1（超页闭环收敛，双份/丢失即红）"
  );
  let hits = gate_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Lpos,
    &[dst, &moved, b"COUNT", b"0"],
  );
  assert_eq!(
    hits,
    format!("*1\r\n:{}\r\n", over - 1).as_bytes(),
    "dst 含且仅含该元素一处、居尾"
  );
}

/// 界测 c：迁移窗忙错夹具（try_swap_in_window claim 在册 → 慢臂物化封窗探测
/// 门 Err 形）——dst 树态键封窗内 LMOVE 回存储忙错误帧；修复后 dst 侧任何
/// 失败均先于 src 落笔（旧序慢臂 src save 后 dst Err 上抛伴随弹出已持久、
/// 元素已丢），断言 src 终态零变异；释窗重试经物化闭环收敛，元素含且仅含一次
#[test]
fn lmove_dst_migration_busy_leaves_src_untouched_and_retry_converges() {
  let (rt, api, store, _dir) = gate_env(64 * 1024);
  let mut s = gate_session(&api);
  let (src, dst): (&[u8], &[u8]) = (b"gate_busy_src", b"gate_busy_dst");

  // dst 猎界升为分层树态（存根在场即停）
  let mut count = 0usize;
  for i in 0..10_000 {
    let v = mixed_elem(i);
    count += 1;
    gate_exec(&api, &rt, &mut s, RespCommand::Rpush, &[dst, &v]);
    if stub_present(&rt, &store, dst) {
      break;
    }
  }
  assert!(
    stub_present(&rt, &store, dst),
    "前置判据：dst 须已为分层树态"
  );

  let moved = canary_elem(b'2');
  gate_push_all(&api, &rt, &mut s, src, from_ref(&moved));

  // 封窗夹具：独立会话登记同键迁移 claim（守卫 Drop 即释窗）；快臂对树态 dst
  // 恒 Degrade → 慢臂物化封窗探测被在册 claim 拒绝 → 存储忙统一应答
  let fixture = store.new_session().unwrap();
  let guard = fixture
    .try_swap_in_window(dst)
    .expect("封窗夹具 claim 须可登记");
  let rep = gate_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Lmove,
    &[src, dst, b"LEFT", b"RIGHT"],
  );
  assert_eq!(
    rep,
    err_frame(RESP_ERR_SLOW_PATH_STORAGE),
    "窗内 LMOVE 须按存储忙拒写交重试"
  );
  // src 终态零变异：dst 侧失败先于 src 落笔（本臂零持久写入）
  assert_eq!(
    gate_exec(&api, &rt, &mut s, RespCommand::Llen, &[src]),
    b":1\r\n",
    "src 不得被部分应用"
  );
  assert_eq!(
    gate_exec(&api, &rt, &mut s, RespCommand::Lindex, &[src, b"0"]),
    bulk(&moved)
  );

  drop(guard);
  // 释窗重试收敛：慢臂物化封窗臂整体执行（dst 先落、src 删空回收）
  let rep = gate_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Lmove,
    &[src, dst, b"LEFT", b"RIGHT"],
  );
  assert_eq!(rep, bulk(&moved), "重试应答弹出元素 bulk");
  assert_eq!(
    gate_exec(&api, &rt, &mut s, RespCommand::Exists, &[src]),
    b":0\r\n"
  );
  assert_eq!(
    gate_exec(&api, &rt, &mut s, RespCommand::Llen, &[dst]),
    format!(":{}\r\n", count + 1).as_bytes(),
    "dst 终态计数 = 树态基线 + 1（丢失/双份即红）"
  );
  let hits = gate_exec(
    &api,
    &rt,
    &mut s,
    RespCommand::Lpos,
    &[dst, &moved, b"COUNT", b"0"],
  );
  assert_eq!(
    hits,
    format!("*1\r\n:{}\r\n", count).as_bytes(),
    "dst 含且仅含该元素一处、居尾"
  );
}

// ──────────────────────────────────────────────────────────────────────────
// r145c 案一锁面（票 zcode-r145c-lblpop2）：LMOVE/RPOPLPUSH 冷臂
// move_core_cold 异键尾段 dst 已提交而 src 写回 fail-closed Err 时唤醒丢失。
// 害形：notify(dst_key) 旧置 src save `?` 之后，src 写回命中升阶 fail-closed
// （三 Err 形之一，见票）直贯统一错误帧，notify 整体跳过——dst 元素已持久
// 入队而零唤醒，timeout=0 观察者永悬至该键下一写事件；与快臂
// write.rs list_move_core「Ok(true) 即贴发」（提交先于通知）不同构，违板块
// 4.2 多路径行为同构。C# ListOps.cs:293-299 finally 无条件 Commit 后仅 dst
// 唤醒，结构上无「dst 已持久而整命令报错且不发通知」中间态。
// 夹具为 §100 界测 b 小页升阶门机制反置 src 侧（dst 侧零触碰）：src 先经
// 超页闭环升为分层树态并续灌至「弹出后残余载荷仍超页」；filler 第二棵树
// 占满双环总闸 → LMOVE 时 src 写回重升阶建树被总闸拒（BudgetExhausted→
// Err，fail-closed 旧态分毫未动），dst 新键单笔信封写回先行成功。空 dst
// 挂 BLPOP timeout=0 观察者，三点断言：①命令回存储错误帧；②dst 观察者收
// [dst, 元素] 指派帧（修前此处永悬判红）；③观察表摘队归零。另以快臂同形
// 成功路径出件帧对照双臂同构（除键名外逐字节一致）。
// ──────────────────────────────────────────────────────────────────────────

/// 单环预算（DEFAULT_RI_COLLECTION 页环口径，
/// collection_cache_budget_fallback.rs SINGLE_RING_BUDGET 同款）
const LCP_RING_BUDGET: usize = 16 * 1024 * 1024;

/// RESP 数组命令帧（二进制安全，不经 str 转换）
fn lcp_frame(parts: &[&[u8]]) -> Vec<u8> {
  let mut f = format!("*{}\r\n", parts.len()).into_bytes();
  for p in parts {
    f.extend_from_slice(format!("${}\r\n", p.len()).as_bytes());
    f.extend_from_slice(p);
    f.extend_from_slice(b"\r\n");
  }
  f
}

/// 经纪装配客户端（resp_blocking_commands.rs Harness::client 同形）
struct LcpClient {
  consumer: RespSessionConsumer,
}

impl LcpClient {
  /// 泵等价序直填消费（不含挂起续驱）
  fn feed(&mut self, frame: &[u8]) -> Vec<u8> {
    let mut scratch = self.consumer.take_recv_scratch();
    scratch.extend_from_slice(frame);
    self.consumer.return_recv_scratch(scratch);
    let mut resp = Vec::new();
    let remaining = self.consumer.try_consume_messages_into(&mut resp);
    assert!(remaining.is_some(), "命令帧须被完整消费: {frame:?}");
    resp
  }

  /// 驱动到完成（含慢路径挂起续驱；阻塞命令挂起体留给调用方处置）
  async fn roundtrip(&mut self, frame: &[u8]) -> Vec<u8> {
    let mut resp = self.feed(frame);
    if let Some(slow) = self.consumer.take_slow_wait() {
      resp.extend_from_slice(&slow.resolve().await);
    }
    resp
  }

  /// 有界解除阻塞挂起体（timeout=0 永悬臂的测试侧保险丝：修前红灯于此
  /// 取证判红，修后指派帧即时抵达）
  async fn resolve_blocked_bounded(&mut self, dur: Duration) -> Result<Vec<u8>, ()> {
    let mut blocked = self
      .consumer
      .take_blocked_wait()
      .expect("BLPOP 挂起体应在会话");
    let Ok((cmd, result)) = timeout(dur, blocked.resolve()).await else {
      return Err(());
    };
    let mut reply = Vec::new();
    self
      .consumer
      .resolve_blocked_wait_into(cmd, result, &mut reply);
    Ok(reply)
  }
}

/// 分层树态异步探针（stub_present 同判据，async 测试内免 block_on）
async fn lcp_stub_present(store: &Arc<WedbStore<SegmentedDevice>>, key: &[u8]) -> bool {
  let sess = store.new_session().unwrap();
  sess.load_collection_stub(key).await.unwrap().is_some()
}

/// 观察表在册棘轮等待（broker 测试同款挂队判据）
async fn lcp_await_parked(
  inner: &Arc<CollectionItemBroker<CollectionItemSource<SegmentedDevice>>>,
  folded: &[u8],
) -> bool {
  for _ in 0..5_000 {
    if inner.waiting_observer_count(folded) == Some(1) {
      return true;
    }
    sleep(Duration::from_millis(1)).await;
  }
  false
}

/// 票 zcode-r145c-lblpop2 案一：冷臂异键 src 写回 fail-closed Err 后
/// dst 已持久必须唤醒观察者——修前 notify 永悬判红，修后指派闭环判绿
#[compio::test]
async fn lmove_cold_src_failcloses_still_wakes_dst_observer() {
  let dir = tempdir().unwrap();
  // 64KB 小页（界测 b 同款门形）；总闸 = 双环：src + filler 各占一环，
  // LMOVE 时 src 重升阶 scratch 环预留超限 → 显式拒
  let config = StoreConfig::new(2048, 64 * 1024, 16, 0.5)
    .unwrap()
    .with_tree_cache_budget(2 * LCP_RING_BUDGET);
  let device =
    Arc::new(SegmentedDevice::single_file(dir.path().join("lmove-coldwake.db")).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  let inner = Arc::new(CollectionItemBroker::new(CollectionItemSource::new(
    store.new_session().unwrap(),
  )));
  let broker = Arc::new(SharedItemBroker::new(Arc::clone(&inner)));
  let runtime_config = RuntimeServerConfig::shared_default();

  let client = |id: u64| -> LcpClient {
    let notify_broker = Arc::clone(&broker);
    let wait_broker = Arc::clone(&broker) as Arc<dyn ItemBrokerFinisher>;
    let api = Arc::new(
      StoreGarnetApi::new(store.new_session().unwrap())
        .with_collection_notify(Some(Arc::new(move |domain: (u64, u64), key: &[u8]| {
          notify_broker.handle_collection_update(domain, key)
        })))
        .with_item_broker_wait(Some(wait_broker)),
    );
    let mut consumer = RespSessionConsumer::new(id, RespServerSessionOptions::default(), api);
    consumer.set_item_broker(broker.clone());
    consumer.set_runtime_config(runtime_config.clone());
    LcpClient { consumer }
  };

  let (src, filler, dst, src2, dst2) = (
    b"lcp_src",
    b"lcp_fill",
    b"lcp_dst1",
    b"lcp_src2",
    b"lcp_dst2",
  );
  let mut b = client(2);

  // 1. 猎超页界升 src 为分层树态（越界那笔 RPUSH 走门降级 → 慢臂升阶闭环
  // 落树，界测 b 同款），再续灌 8 个 600B 大值：弹出 1 个后残余载荷仍超页
  // （票面「src 预置弹出后仍超页」形）
  let mut elems: Vec<Vec<u8>> = Vec::new();
  for i in 0..10_000 {
    elems.push(mixed_elem(i));
    let rep = b
      .roundtrip(&lcp_frame(&[b"RPUSH", src, elems.last().unwrap()]))
      .await;
    assert!(rep.starts_with(b":"), "猎界 RPUSH 回帧须整数: {rep:?}");
    if lcp_stub_present(&store, src).await {
      break;
    }
  }
  assert!(
    lcp_stub_present(&store, src).await,
    "前置判据：src 须经超页闭环升为分层树态"
  );
  for i in 0..8 {
    elems.push(mixed_elem(100 + i));
    b.roundtrip(&lcp_frame(&[b"RPUSH", src, elems.last().unwrap()]))
      .await;
  }
  let total = elems.len();

  // 2. filler 逐元素猎界升为第二棵树：占满双环总闸（此后任何重升阶必被拒）
  for i in 0..10_000 {
    let v = mixed_elem(500 + i);
    let rep = b.roundtrip(&lcp_frame(&[b"RPUSH", filler, &v])).await;
    assert!(
      rep.starts_with(b":"),
      "filler 猎界 RPUSH 回帧须整数: {rep:?}"
    );
    if lcp_stub_present(&store, filler).await {
      break;
    }
  }
  assert_eq!(
    store.range_index().cache_reserved(),
    2 * LCP_RING_BUDGET,
    "校准：双树环在册占满总闸（src 写回重升阶的 scratch 环预留必超限）"
  );

  // 3. 空 dst 挂 BLPOP timeout=0 观察者（首试未果入窗挂队）
  let mut a = client(1);
  let resp = a.feed(&lcp_frame(&[b"BLPOP", dst, b"0"]));
  assert!(resp.is_empty(), "timeout=0 观察者应挂起无即时应答");
  let folded = NsPrefix::new(0).join(0).isolate(dst);
  assert!(
    lcp_await_parked(&inner, folded.as_slice()).await,
    "观察者须真实挂队（Some(1)）"
  );

  // 4. LMOVE 异键：dst 写回先成（新键信封），src 写回重升阶被总闸拒 →
  // fail-closed 统一存储错误帧
  let moved = elems[0].clone();
  let rep = b
    .roundtrip(&lcp_frame(&[b"LMOVE", src, dst, b"LEFT", b"RIGHT"]))
    .await;
  assert_eq!(
    rep,
    err_frame(RESP_ERR_SLOW_PATH_STORAGE),
    "src 写回 fail-closed 应回存储错误帧（升阶未成且信封超页拒写保旧态）"
  );

  // 5. 核心断言：dst 已持久，观察者须收 [dst, 元素] 指派帧。修前
  // notify 位于 src save `?` 之后被整体跳过——timeout=0 无穷臂永悬，此处判红
  let reply = match a.resolve_blocked_bounded(Duration::from_secs(5)).await {
    Ok(reply) => reply,
    Err(_) => {
      assert_eq!(
        inner.waiting_observer_count(folded.as_slice()),
        Some(1),
        "红灯取证形：唤醒丢失时观察者应仍挂队不指派不归零"
      );
      panic!(
        "LMOVE 冷臂 src fail-closed 后 dst 观察者未获指派——r145c 案一复现（notify 发射位漂回 src save 之后）"
      );
    }
  };
  let mut expect = b"*2\r\n".to_vec();
  expect.extend_from_slice(&bulk(dst));
  expect.extend_from_slice(&bulk(&moved));
  assert_eq!(
    reply, expect,
    "观察者收帧须为 [dst, 弹出元素] 指派帧，与快臂同构"
  );

  // 6. 挂队-清退闭环：指派后队列摘除、键映射回收
  assert_eq!(
    inner.waiting_observer_count(folded.as_slice()),
    None,
    "指派后观察者须出队摘键"
  );

  // 7. fail-closed 旧态零变异：src 整树分毫未动（§100 元素双份容忍形——
  // dst 那笔已随观察者取走出清，src 保留完整初态，重试可收敛）；
  // 树账簿不变（旧环仍在册，拒写零滞留）
  assert_eq!(
    b.roundtrip(&lcp_frame(&[b"LLEN", src])).await,
    format!(":{total}\r\n").as_bytes(),
    "src 写回 fail-closed 不得部分应用"
  );
  assert_eq!(
    b.roundtrip(&lcp_frame(&[b"LLEN", dst])).await,
    b":0\r\n",
    "dst 元素已被观察者指派取走"
  );
  assert_eq!(
    store.range_index().cache_reserved(),
    2 * LCP_RING_BUDGET,
    "升阶被拒不得改变树账簿（scratch 环预留出口必归还）"
  );

  // 8. 双臂同形对照：快臂同夹具成功路径（信封 src2→dst2，notify 贴发位
  // 现形即目标形），dst 观察者收帧除键名外与冷臂逐字节一致
  assert_eq!(
    b.roundtrip(&lcp_frame(&[b"RPUSH", src2, &moved])).await,
    b":1\r\n"
  );
  let mut c = client(3);
  let resp = c.feed(&lcp_frame(&[b"BLPOP", dst2, b"0"]));
  assert!(resp.is_empty(), "对照观察者应挂起");
  let folded2 = NsPrefix::new(0).join(0).isolate(dst2);
  assert!(lcp_await_parked(&inner, folded2.as_slice()).await);
  let rep = b
    .roundtrip(&lcp_frame(&[b"LMOVE", src2, dst2, b"LEFT", b"RIGHT"]))
    .await;
  assert_eq!(rep, bulk(&moved), "快臂成功应答恒为弹出元素 bulk");
  let reply2 = c
    .resolve_blocked_bounded(Duration::from_secs(5))
    .await
    .expect("快臂观察者指派即时抵达");
  assert_eq!(
    String::from_utf8_lossy(&reply).replace("lcp_dst1", "lcp_dst2"),
    String::from_utf8_lossy(&reply2),
    "冷臂与快臂 dst 观察者收帧须逐字节同构（键名归一后）"
  );
}

/// LPOS 单命令装配往返（probe 为元素 + 选项词元序列）
fn lpos_probe(
  api: &GarnetApi,
  rt: &Runtime,
  s: &mut RespServerSession,
  key: &[u8],
  probe: &[Vec<u8>],
) -> Vec<u8> {
  let mut args: Vec<&[u8]> = vec![key];
  args.extend(probe.iter().map(|v| v.as_slice()));
  gate_exec(api, rt, s, RespCommand::Lpos, &args)
}

/// 案一界测（票 zcode-r149c-lposrank）：分层键 claim 在册期间 LPOS 三形零
/// 存储错误帧，与无窗基线及信封态逐字节全等——路由经
/// load_collection_stub_for_read 回退快照、树内臂直读，全程零
/// SwapInWindowGuard 封窗；旧形经 load_typed_sealed 封窗物化核，claim 在册
/// 即被贯至 RESP_ERR_SLOW_PATH_STORAGE 忙错帧（同窗 LINDEX/LRANGE 照常出读的
/// 族内唯一读面分叉）。claim 夹具与界测 c（lmove_dst_migration_busy_*）同款，
/// 手工升阶原语与 tiered_read_stale_meta 同款（非 mock）
#[test]
fn lpos_tiered_under_claim_zero_storage_error_and_envelope_parity() {
  let (rt, api, store, _dir) = gate_env(1024 * 1024);
  let mut s = gate_session(&api);
  let (tk, mk): (&[u8], &[u8]) = (b"lpos_claim_tiered", b"lpos_claim_envelope");

  // 手工升阶小列表：24 元素，c 出现于 {0,5,11,23}（序号自 LIST_SEQ_BASE 连续
  // 排布，记录编码与 IGarnetObject::export_entries 同基准）
  let elems: Vec<Vec<u8>> = (0..24usize)
    .map(|i| {
      if matches!(i, 0 | 5 | 11 | 23) {
        b"c".to_vec()
      } else {
        format!("e{i}").into_bytes()
      }
    })
    .collect();
  {
    let sess = store.new_session().unwrap();
    let entries: Vec<(Vec<u8>, Vec<u8>)> = elems
      .iter()
      .enumerate()
      .map(|(i, e)| {
        (
          (LIST_SEQ_BASE + i as u128).to_be_bytes().to_vec(),
          encode_member(e, None),
        )
      })
      .collect();
    rt.block_on(sess.promote_collection_to_bftree(
      tk,
      GarnetObjectType::List,
      entries,
      i64::MAX,
      false,
    ))
    .unwrap();
    assert!(stub_present(&rt, &store, tk), "前置判据：tk 须处于分层树态");
  }
  // 信封态对照键：同一份数据 RPUSH（24 元素远低于升阶门限）
  gate_push_all(&api, &rt, &mut s, mk, &elems);

  // 三形探针（缺省形 / COUNT 全命中形 / MAXLEN 形 / 负 rank 形）
  let probes: Vec<Vec<Vec<u8>>> = vec![
    vec![b"c".to_vec()],
    vec![b"c".to_vec(), b"COUNT".to_vec(), b"0".to_vec()],
    vec![b"c".to_vec(), b"MAXLEN".to_vec(), b"6".to_vec()],
    vec![b"c".to_vec(), b"RANK".to_vec(), b"-2".to_vec()],
  ];
  let baseline: Vec<Vec<u8>> = probes
    .iter()
    .map(|p| lpos_probe(&api, &rt, &mut s, tk, p))
    .collect();
  assert_eq!(baseline[0], b":0\r\n", "缺省形首命中位");
  assert_eq!(
    baseline[1], b"*4\r\n:0\r\n:5\r\n:11\r\n:23\r\n",
    "COUNT 0 全命中形"
  );
  assert_eq!(baseline[2], b":0\r\n", "MAXLEN 6 窗内首命中");
  assert_eq!(baseline[3], b":11\r\n", "RANK -2 尾侧第二次出现");
  // 信封态参照（对象层单源）：分层态基线与之逐字节全等
  for (probe, expect) in probes.iter().zip(&baseline) {
    assert_eq!(
      lpos_probe(&api, &rt, &mut s, mk, probe),
      *expect,
      "信封态与分层态基线逐字节全等: {probe:?}"
    );
  }

  // claim 夹具：独立会话登记同键迁移窗（守卫 Drop 即释窗）
  let fixture = store.new_session().unwrap();
  let guard = fixture.try_swap_in_window(tk).expect("claim 夹具须可登记");
  assert!(
    store
      .new_session()
      .unwrap()
      .try_swap_in_window(tk)
      .is_none(),
    "夹具 claim 须在册（四探测门活跃性判据）"
  );
  for (probe, expect) in probes.iter().zip(&baseline) {
    let rep = lpos_probe(&api, &rt, &mut s, tk, probe);
    assert_ne!(
      rep,
      err_frame(RESP_ERR_SLOW_PATH_STORAGE),
      "claim 在册期间 LPOS 不得被忙拒成存储错误帧: probe={probe:?}"
    );
    assert_eq!(
      rep, *expect,
      "claim 在册期间应答与无窗基线逐字节全等（回退快照树内直读）"
    );
  }
  drop(guard);
  // 读臂零封窗收口：释窗后同键 claim 可再登记（LPOS 全程零 try_claim_migration
  // 命中、零残留），且同键写恢复即时成功（危害链 B 判据：读不封写）
  assert!(
    store
      .new_session()
      .unwrap()
      .try_swap_in_window(tk)
      .is_some(),
    "LPOS 读臂不得留存 claim"
  );
  assert_eq!(
    gate_exec(&api, &rt, &mut s, RespCommand::Rpush, &[tk, b"e24"]),
    b":25\r\n",
    "同键写面不因读臂封窗被忙拒"
  );
  let rep = lpos_probe(
    &api,
    &rt,
    &mut s,
    tk,
    &[b"c".to_vec(), b"RANK".to_vec(), b"-1".to_vec()],
  );
  assert_eq!(rep, b":23\r\n", "写后负 rank 形随树内容收敛");
}
