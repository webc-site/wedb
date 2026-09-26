//! 分层集合输出臂帧头「预留 + 回填」回归（task/done/fix-tiered-output-frame-scratch.md）
//!
//! 四条臂（SMEMBERS 与 HGETALL / HKEYS / HVALS）原先把整应答实体写进
//! `scratch: Vec<u8>`，再 `output.extend_from_slice(&scratch)` 整块拷进会话输出
//! ——大集合应答堆峰值翻倍 + 一次全量 memcpy。现改为 wresp::ext 单点
//! （[`reserve_resp_frame_head`] / [`backfill_resp_frame_head`]）：帧头位先按存活
//! 上界估宽预留，实体逐条**直写**最终 output，扫完以实际出帧计数回填，位宽不等
//! 以 `copy_within` 移动实体消除间隙。
//!
//! 参照模型即 C# 对象层「先落头、流式直写」形态（libs/server/Objects/Set/
//! SetObjectImpl.cs:38-52 `SetMembers` 的 `WriteSetLength(Set.Count)` + foreach
//! `WriteBulkString`；libs/server/Objects/Hash/HashObjectImpl.cs:61-75 `HashGetAll`
//! 与 :248-287 HKEYS/HVALS 共用出帧体），在测试侧以格式化字面量独立复写一份，
//! 全程不调用生产成帧函数——回填机制若产出与直写路径分叉的帧字节，这里逐条露相。
//!
//! 两条不变量各自的证伪面：
//! - 帧头与实体同源（严禁以 `meta.size` 落头）：`swept_expiry_*` 构造「水位越过 +
//!   出账后条数收窄」，帧头必按实际出帧条数回填，位宽自 `*100\r\n`（6 字节）收到
//!   `*4\r\n`（4 字节）；预留位以 0 填充，回填若有遗漏即残留 NUL，
//!   [`assert_no_reserved_residue`] 就是抓这个；
//! - 错误路径应答未落帧：`watermark_crossed_scan_err_*` 在出账扫描点注入失败，
//!   断言应答恰为错误帧，不含预留头与部分实体字节。

use std::sync::atomic::Ordering;

use wbase::time::now_ticks;
use wbftree::SCAN_FAIL_INJECT;
use wcol::{SET_MEMBER_DUMMY_VALUE, types::member_ttl::encode_member};
use wnode::resp::resp_server_session::RespServerSession;
use wnode_test::{TestEnv, session_with_ver, tiered_env};
use wresp::{
  cmd_strings,
  command::RespCommand,
  ext::{RespVecExt, backfill_resp_frame_head, reserve_resp_frame_head, resp_frame_head_len},
};
use wval::GarnetObjectType;

/// 慢路径命令同步求值并回帧字节（与 tiered_scan_err_propagate 同款泵）
fn auto_exec(
  env: &TestEnv,
  s: &mut RespServerSession,
  cmd: RespCommand,
  args: &[&[u8]],
) -> Vec<u8> {
  wnode_test::auto_exec(&env.api, &env.rt, s, cmd, args)
}

/// 手工升阶（`next_expiry` = 灌入批最早到期水位，`i64::MAX` = 无成员挂 TTL）
fn promote(
  env: &TestEnv,
  key: &[u8],
  obj_type: GarnetObjectType,
  entries: Vec<(Vec<u8>, Vec<u8>)>,
  next_expiry: i64,
) {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.promote_collection_to_bftree(key, obj_type, entries, next_expiry, false))
    .unwrap();
  assert!(is_tiered(env, key), "键应处于 wbftree 分层态");
}

fn is_tiered(env: &TestEnv, key: &[u8]) -> bool {
  let sess = env.store.new_session().unwrap();
  env
    .rt
    .block_on(sess.load_collection_stub(key))
    .unwrap()
    .is_some()
}

/// 定宽成员名（`m00000`）：树内字节序 = 生成序，故分层态出帧序可先验
fn name(prefix: &str, i: usize) -> Vec<u8> {
  format!("{prefix}{i:05}").into_bytes()
}

/// 分层集合键：`count` 个成员（集合成员恒无成员级 TTL），回出帧序成员清单
fn promote_set(env: &TestEnv, key: &[u8], count: usize) -> Vec<Vec<u8>> {
  let members: Vec<Vec<u8>> = (0..count).map(|i| name("m", i)).collect();
  promote(
    env,
    key,
    GarnetObjectType::Set,
    members
      .iter()
      .map(|m| (m.clone(), SET_MEMBER_DUMMY_VALUE.to_vec()))
      .collect(),
    i64::MAX,
  );
  members
}

/// 分层哈希键：`count` 对字段，字段升序、值按**降序**取材——出帧序若被误按值
/// 排序即刻露相（HVALS 的序基准是字段序，不是值序）
fn promote_hash(env: &TestEnv, key: &[u8], count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
  let plain: Vec<(Vec<u8>, Vec<u8>)> = (0..count)
    .map(|i| (name("f", i), name("v", count - 1 - i)))
    .collect();
  let entries: Vec<(Vec<u8>, Vec<u8>)> = plain
    .iter()
    .map(|(field, value)| (field.clone(), encode_member(value, None)))
    .collect();
  promote(env, key, GarnetObjectType::Hash, entries, i64::MAX);
  plain
}

/// 参照成帧（测试侧独立实现，零生产成帧函数）：`head` 为头行字面量，实体逐条
/// `$<len>\r\n<bytes>\r\n`，形态即 C# 的「WriteXxxLength 后流式直写」
fn reference_frame(head: &str, items: &[&[u8]]) -> Vec<u8> {
  let mut out = head.as_bytes().to_vec();
  for item in items {
    out.extend_from_slice(format!("${}\r\n", item.len()).as_bytes());
    out.extend_from_slice(item);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// 协议感知头行（与 C# WriteSetLength / WriteMapLength / WriteArrayLength 同式：
/// RESP3 set `%`→`~`、map `%`，RESP2 一律退化为 `*`，map 计数取 `2×对数`）
fn head_of(kind: &str, count: usize, ver: u8) -> String {
  match (kind, ver >= 3) {
    ("set", true) => format!("~{count}\r\n"),
    ("map", true) => format!("%{count}\r\n"),
    ("map", false) => format!("*{}\r\n", count * 2),
    _ => format!("*{count}\r\n"),
  }
}

/// 应答字节里的预留位残留探针：预留位以 0 填充，回填若遗漏（未回填 / 收窄未截尾 /
/// 扩宽未前移）必残留 NUL，故「无 NUL」即「预留位被真实帧头完全接管」
fn assert_no_reserved_residue(out: &[u8], label: &str) {
  assert!(
    !out.contains(&0),
    "{label} 应答含预留位残留 NUL，帧头回填不完备: {:?}",
    String::from_utf8_lossy(out)
  );
}

/// 应答头行（首个 CRLF 之前，不含 CRLF）
fn head_line(out: &[u8]) -> String {
  let end = out
    .windows(2)
    .position(|w| w == b"\r\n")
    .unwrap_or_else(|| panic!("应答无头行: {:?}", String::from_utf8_lossy(out)));
  String::from_utf8_lossy(&out[..end]).into_owned()
}

/// 按序抽出应答里的全部 bulk string 实体（跨态对照用）
fn bulks(out: &[u8]) -> Vec<Vec<u8>> {
  let mut items = Vec::new();
  let mut pos = out.windows(2).position(|w| w == b"\r\n").unwrap() + 2;
  while pos < out.len() {
    assert_eq!(out[pos], b'$', "实体不是 bulk: {:?}", &out[pos..]);
    let line_end = pos + out[pos..].windows(2).position(|w| w == b"\r\n").unwrap();
    let len: usize = String::from_utf8_lossy(&out[pos + 1..line_end])
      .parse()
      .unwrap();
    let body = line_end + 2;
    items.push(out[body..body + len].to_vec());
    pos = body + len + 2;
  }
  items
}

/// 位宽边界矩阵：条目数跨 9→10、99→100、999→1000，覆盖「预留位宽与实际同宽
/// （零移动）」与「跨宽（移动）」两支
const COUNTS: [usize; 7] = [1, 9, 10, 99, 100, 101, 1000];
const VERSIONS: [u8; 2] = [2, 3];

/// SMEMBERS 臂：分层态应答与 C# 参照模型逐字节全等（RESP2/RESP3 × 位宽边界）
#[test]
fn smembers_tiered_reply_is_byte_identical_to_reference() {
  let env = tiered_env("frame-head-smembers.db");
  for ver in VERSIONS {
    let mut s = session_with_ver(&env, ver);
    for count in COUNTS {
      let key = format!("s{ver}-{count}");
      let members = promote_set(&env, key.as_bytes(), count);
      let refs: Vec<&[u8]> = members.iter().map(Vec::as_slice).collect();
      let out = auto_exec(&env, &mut s, RespCommand::Smembers, &[key.as_bytes()]);
      assert_eq!(
        out,
        reference_frame(&head_of("set", count, ver), &refs),
        "SMEMBERS {key}（{ver} 版、{count} 成员）与 C# 参照模型分叉"
      );
      assert_no_reserved_residue(&out, "SMEMBERS");
    }
  }
}

/// HGETALL / HKEYS / HVALS 三臂：同一分层键上逐字节全等（RESP2/RESP3 × 位宽边界）
#[test]
fn hash_three_arms_replies_are_byte_identical_to_reference() {
  let env = tiered_env("frame-head-hash.db");
  for ver in VERSIONS {
    let mut s = session_with_ver(&env, ver);
    for count in COUNTS {
      let key = format!("h{ver}-{count}");
      let pairs = promote_hash(&env, key.as_bytes(), count);
      let fields: Vec<&[u8]> = pairs.iter().map(|(f, _)| f.as_slice()).collect();
      let values: Vec<&[u8]> = pairs.iter().map(|(_, v)| v.as_slice()).collect();
      let interleaved: Vec<&[u8]> = pairs
        .iter()
        .flat_map(|(f, v)| [f.as_slice(), v.as_slice()])
        .collect();

      let out = auto_exec(&env, &mut s, RespCommand::Hgetall, &[key.as_bytes()]);
      assert_eq!(
        out,
        reference_frame(&head_of("map", count, ver), &interleaved),
        "HGETALL {key}（{ver} 版、{count} 对）与 C# 参照模型分叉"
      );
      assert_no_reserved_residue(&out, "HGETALL");

      let out = auto_exec(&env, &mut s, RespCommand::Hkeys, &[key.as_bytes()]);
      assert_eq!(
        out,
        reference_frame(&head_of("array", count, ver), &fields),
        "HKEYS {key}（{ver} 版、{count} 对）与 C# 参照模型分叉"
      );
      assert_no_reserved_residue(&out, "HKEYS");

      let out = auto_exec(&env, &mut s, RespCommand::Hvals, &[key.as_bytes()]);
      assert_eq!(
        out,
        reference_frame(&head_of("array", count, ver), &values),
        "HVALS {key}（{ver} 版、{count} 对）与 C# 参照模型分叉（HVALS 序基准=字段序）"
      );
      assert_no_reserved_residue(&out, "HVALS");
    }
  }
}

/// 出账后条数收窄：帧头必按**实际出帧条数**回填，不得按 `meta.size` 先验落头
///
/// 100 字段灌入（`meta.size` = 100，估宽预留 `*100\r\n` = 6 字节）→ 前 98 条挂
/// 过去刻度、水位随灌入批越过 → 首枪走 `expire_sweep_or_rebuild` 的 Swept 支，
/// 存活全集经 `drain_live` 单次遍历交出（零二次扫树），应答头按实际 2 对回填为
/// `*4\r\n`（RESP2 4 字节）/ `%2\r\n`（RESP3 3 字节），走 `copy_within` 左移收窄支。
/// 条数若取自 size 会错报 200 元素 / `%100`。
#[test]
fn swept_expiry_backfills_actual_live_count() {
  let env = tiered_env("frame-head-swept.db");
  let stale = now_ticks() - 1;
  let total = 100usize;
  let live = 2usize;
  for ver in VERSIONS {
    let key = format!("h{ver}");
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..total)
      .map(|i| {
        (
          name("f", i),
          encode_member(
            &name("v", total - 1 - i),
            if i < total - live { Some(stale) } else { None },
          ),
        )
      })
      .collect();
    promote(&env, key.as_bytes(), GarnetObjectType::Hash, entries, stale);
    let mut s = session_with_ver(&env, ver);

    let want: Vec<(Vec<u8>, Vec<u8>)> = (total - live..total)
      .map(|i| (name("f", i), name("v", total - 1 - i)))
      .collect();
    let items: Vec<&[u8]> = want
      .iter()
      .flat_map(|(f, v)| [f.as_slice(), v.as_slice()])
      .collect();
    let expect = reference_frame(&head_of("map", live, ver), &items);

    let out = auto_exec(&env, &mut s, RespCommand::Hgetall, &[key.as_bytes()]);
    assert_eq!(
      out,
      expect,
      "出账后 HGETALL（{ver} 版）帧头须按实际存活对数回填，实际头 {:?}",
      head_line(&out)
    );
    assert_no_reserved_residue(&out, "Swept HGETALL");
    // 计数与应答同源：出账后 HLEN 与帧头条数同基准
    assert_eq!(
      auto_exec(&env, &mut s, RespCommand::Hlen, &[key.as_bytes()]),
      format!(":{live}\r\n").into_bytes(),
      "出账后 size 校正须与应答条数同源"
    );
    // 出账后键仍分层态、水位已前移：二次 HGETALL 走 Below 支，应答逐字节复现
    assert!(is_tiered(&env, key.as_bytes()), "整值重灌后键应保持分层态");
    let again = auto_exec(&env, &mut s, RespCommand::Hgetall, &[key.as_bytes()]);
    assert_eq!(again, expect, "出账后二次 HGETALL（Below 支）应答漂移");
  }
}

/// 全到期：出账后应答恰为空帧头（`*0\r\n` / `%0\r\n`），预留位宽自 6 字节收到
/// 4/3 字节；删空自愈随键回收元记录
#[test]
fn all_expired_sweep_frames_empty_head() {
  let env = tiered_env("frame-head-empty.db");
  let stale = now_ticks() - 1;
  let total = 100usize;
  for ver in VERSIONS {
    let key = format!("h{ver}");
    let entries: Vec<(Vec<u8>, Vec<u8>)> = (0..total)
      .map(|i| (name("f", i), encode_member(&name("v", i), Some(stale))))
      .collect();
    promote(&env, key.as_bytes(), GarnetObjectType::Hash, entries, stale);
    let mut s = session_with_ver(&env, ver);
    let out = auto_exec(&env, &mut s, RespCommand::Hgetall, &[key.as_bytes()]);
    assert_eq!(
      out,
      reference_frame(&head_of("map", 0, ver), &[]),
      "全出账后 HGETALL（{ver} 版）应恰回头 0 的空帧，实际: {:?}",
      String::from_utf8_lossy(&out)
    );
    assert_no_reserved_residue(&out, "空集 HGETALL");
    assert!(
      !is_tiered(&env, key.as_bytes()),
      "全出账后键应随删空自愈消失（不留幽灵空元记录）"
    );
  }
}

/// 水位越过 + 出账扫描失败：应答未落帧（预留头与部分实体一并撤除），错误帧闭环
///
/// 与 `tiered_scan_err_propagate`（Below 支流式扫描失败）互补：本用例把键做成
/// 「水位已越过」态，令一次性注入命中 `expire_sweep_or_rebuild` 的出账扫描，即
/// 臂内 `Err(()) => { output.truncate(base); return Err(()) }` 那条撤帧支；三臂
/// 各自独立注入，逐臂证伪。
#[test]
fn watermark_crossed_scan_err_leaves_no_partial_frame() {
  let env = tiered_env("frame-head-scan-err.db");
  let stale = now_ticks() - 1;
  let arms = [RespCommand::Hgetall, RespCommand::Hkeys, RespCommand::Hvals];
  for cmd in arms {
    let key = format!("{cmd:?}").into_bytes();
    promote(
      &env,
      &key,
      GarnetObjectType::Hash,
      vec![
        (b"f1".to_vec(), encode_member(b"v1", Some(stale))),
        (b"f2".to_vec(), encode_member(b"v2", None)),
      ],
      stale,
    );
    let mut s = session_with_ver(&env, 2);

    SCAN_FAIL_INJECT.store(true, Ordering::SeqCst);
    let out = auto_exec(&env, &mut s, cmd, &[&key]);
    assert!(
      out.starts_with(b"-ERR "),
      "{cmd} 出账扫描失败须回存储错误帧且不留半成品帧，实际: {:?}",
      String::from_utf8_lossy(&out)
    );
    assert_no_reserved_residue(&out, "{cmd} 错误帧");

    // 失败不改树态：复位后同命令走同一 Swept 支成功出帧，与参照模型逐字节全等
    let out = auto_exec(&env, &mut s, cmd, &[&key]);
    let items: Vec<&[u8]> = match cmd {
      RespCommand::Hgetall => vec![b"f2".as_slice(), b"v2".as_slice()],
      RespCommand::Hkeys => vec![b"f2".as_slice()],
      _ => vec![b"v2".as_slice()],
    };
    assert_eq!(
      out,
      reference_frame(&head_of("array", items.len(), 2), &items),
      "{cmd} 撤帧后复位应答漂移（撤帧只回退应答缓冲，不得触树）"
    );
  }
}

/// 内存态与分层态回包一致（登记口径见 doc/zh/deviations.md §136：
/// collection.md 第 5 节「透明」= 帧头与成员集合等价，成员序非契约）
///
/// 两态成员迭代序本就不同（内存态走 gxhash 布局序、分层态走树扫描序，
/// 且 wbase 进程级随机种子跨重启漂移），故断言面为
/// 帧头（条数同源）与实体**集合**逐字节全等，实体序不参与断言，避免把无序契约
/// 写成假断言。
#[test]
fn memory_and_tiered_replies_agree_on_head_and_member_set() {
  let env = tiered_env("frame-head-cross-state.db");
  let count = 120usize;
  let mut s = session_with_ver(&env, 3);

  // 内存态对照键：逐批 SADD / HSET（远未跨升阶门槛）
  let members: Vec<Vec<u8>> = (0..count).map(|i| name("m", i)).collect();
  for chunk in members.chunks(64) {
    let mut args: Vec<Vec<u8>> = vec![b"ms".to_vec()];
    args.extend(chunk.iter().cloned());
    let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&env, &mut s, RespCommand::Sadd, &slices);
  }
  let mut flat: Vec<Vec<u8>> = Vec::new();
  for i in 0..count {
    flat.push(name("f", i));
    flat.push(name("v", i));
  }
  for chunk in flat.chunks(64) {
    let mut args: Vec<Vec<u8>> = vec![b"mh".to_vec()];
    args.extend(chunk.iter().cloned());
    let slices: Vec<&[u8]> = args.iter().map(|v| v.as_slice()).collect();
    auto_exec(&env, &mut s, RespCommand::Hset, &slices);
  }
  assert!(
    !is_tiered(&env, b"ms") && !is_tiered(&env, b"mh"),
    "对照键须保持内存态"
  );

  // 分层态键：同一成员集手工升阶
  promote_set(&env, b"ts", count);
  let tiered: Vec<(Vec<u8>, Vec<u8>)> = (0..count)
    .map(|i| (name("f", i), encode_member(&name("v", i), None)))
    .collect();
  promote(&env, b"th", GarnetObjectType::Hash, tiered, i64::MAX);

  let cases = [
    (
      RespCommand::Smembers,
      "set",
      b"ms".as_slice(),
      b"ts".as_slice(),
    ),
    (
      RespCommand::Hkeys,
      "array",
      b"mh".as_slice(),
      b"th".as_slice(),
    ),
    (
      RespCommand::Hvals,
      "array",
      b"mh".as_slice(),
      b"th".as_slice(),
    ),
    (
      RespCommand::Hgetall,
      "map",
      b"mh".as_slice(),
      b"th".as_slice(),
    ),
  ];
  for (cmd, kind, mem_key, tiered_key) in cases {
    let mem = auto_exec(&env, &mut s, cmd, &[mem_key]);
    let tiered = auto_exec(&env, &mut s, cmd, &[tiered_key]);
    let want = head_of(kind, count, 3);
    assert_eq!(
      head_line(&tiered),
      want.trim_end(),
      "{cmd} 分层态头行形态漂移"
    );
    assert_eq!(
      head_line(&mem),
      head_line(&tiered),
      "{cmd} 两态帧头分叉（条数未同源）"
    );
    let mut mem_items = bulks(&mem);
    let mut tiered_items = bulks(&tiered);
    mem_items.sort();
    tiered_items.sort();
    assert_eq!(mem_items, tiered_items, "{cmd} 两态实体集合分叉");
    assert_no_reserved_residue(&tiered, "分层态应答");
  }
}

/// 分配计数证据：回填形态不再分配与应答同量级的中转缓冲
///
/// 同一成员集、同一出帧语义，只换形态：
/// - [`old_scratch_shape`]：改前形态（实体先写 `scratch`，再落协议头、整块
///   `extend_from_slice` 进 output）；
/// - [`new_backfill_shape`]：改后形态，逐语句复用生产单点（[`reserve_resp_frame_head`]
///   + [`backfill_resp_frame_head`] + [`RespVecExt::write_resp_bulk_string`]），
///     与四条臂现刻代码同形。
///
/// 探针是本测试二进制内的手工计数包装分配器（原样转调 `System`，仅原子自增）：
/// wcol 那枚 crate 级 `#[global_allocator]` 活在 wcol 的单元测试二进制内，与本
/// 二进制不同链接单元，故此处不构成第二份 crate 级探针。计数窗口对每种形态重复
/// 多次并比最小值，使结论不受同进程并行分配噪声影响。
#[test]
fn backfill_shape_drops_body_scale_scratch() {
  const ITEMS: usize = 1000;
  let items: Vec<Vec<u8>> = (0..ITEMS).map(|i| name("m", i)).collect();
  // 实体字节总量（含各条 bulk 头尾）即改前 scratch 的规模
  let body_len: usize = items
    .iter()
    .map(|m| 1 + m.len().to_string().len() + 2 + m.len() + 2)
    .sum();
  let need = body_len + 16;

  // 改前后应答逐字节全等（本票判据 5 的直接证明）
  let mut old_out = Vec::with_capacity(need);
  let mut new_out = Vec::with_capacity(need);
  old_scratch_shape(&mut old_out, &items);
  new_backfill_shape(&mut new_out, &items);
  assert_eq!(old_out, new_out, "改前后应答字节分叉");
  assert_no_reserved_residue(&new_out, "回填形态应答");

  let (mut min_old_bytes, mut min_new_bytes) = (usize::MAX, usize::MAX);
  let (mut min_old_allocs, mut min_new_allocs) = (usize::MAX, usize::MAX);
  for _ in 0..64 {
    let mut out = Vec::with_capacity(need);
    let (allocs, bytes) = measure(old_scratch_shape, &items, &mut out);
    min_old_bytes = min_old_bytes.min(bytes);
    min_old_allocs = min_old_allocs.min(allocs);
    let mut out = Vec::with_capacity(need);
    let (allocs, bytes) = measure(new_backfill_shape, &items, &mut out);
    min_new_bytes = min_new_bytes.min(bytes);
    min_new_allocs = min_new_allocs.min(allocs);
  }
  // 改前：中转 scratch 从 `Vec::new()` 起步，几何扩容的分配量必越过实体总量一半
  assert!(
    min_old_bytes >= body_len / 2,
    "改前形态应付出与应答同量级的中转分配: 实测 {min_old_bytes} < {}",
    body_len / 2
  );
  // 改后：只剩估宽与回填各一枚 ≤24 字节的帧头临时量，且与条目数无关
  assert!(
    min_new_bytes <= 64,
    "改后形态仅允许帧头临时量（两枚 ≤24B）: 实测 {min_new_bytes}"
  );
  assert!(
    min_old_allocs > min_new_allocs,
    "分配次数未降: 改前 {min_old_allocs} 次 vs 改后 {min_new_allocs} 次"
  );
  println!(
    "帧头回填分配证据: 实体 {body_len}B｜改前 {min_old_allocs} 次 {min_old_bytes}B → \
     改后 {min_new_allocs} 次 {min_new_bytes}B"
  );
}

/// 改前形态（生产码已删，此处仅复写作对照）：实体进 scratch，头进 output，整块拷贝
fn old_scratch_shape(output: &mut Vec<u8>, items: &[Vec<u8>]) {
  let mut scratch: Vec<u8> = Vec::new();
  for item in items {
    scratch.write_resp_bulk_string(item);
  }
  cmd_strings::write_set_len(output, items.len(), 2);
  output.extend_from_slice(&scratch);
}

/// 改后形态：与四条臂现刻代码同形（估宽预留 → 实体直写 → 实际计数回填）
fn new_backfill_shape(output: &mut Vec<u8>, items: &[Vec<u8>]) {
  let write_head = |buf: &mut Vec<u8>, n| cmd_strings::write_set_len(buf, n, 2);
  let reserved = resp_frame_head_len(items.len(), write_head);
  let base = reserve_resp_frame_head(output, reserved);
  for item in items {
    output.write_resp_bulk_string(item);
  }
  backfill_resp_frame_head(output, base, reserved, items.len(), write_head);
}

/// 计数窗口：`shape` 在已预留容量的 `output` 上出帧，回（次数, 字节）增量
fn measure(
  shape: impl Fn(&mut Vec<u8>, &[Vec<u8>]),
  items: &[Vec<u8>],
  output: &mut Vec<u8>,
) -> (usize, usize) {
  let (allocs0, bytes0) = counting::snapshot();
  shape(output, items);
  let (allocs1, bytes1) = counting::snapshot();
  (allocs1 - allocs0, bytes1 - bytes0)
}

/// 本测试二进制内的手工计数包装分配器（仅计数，其余原样转调 `System`）
mod counting {
  use std::{
    alloc::{GlobalAlloc, Layout, System},
    sync::atomic::{AtomicUsize, Ordering},
  };

  static ALLOCS: AtomicUsize = AtomicUsize::new(0);
  static BYTES: AtomicUsize = AtomicUsize::new(0);

  struct Counting;

  // SAFETY: alloc/dealloc 原样转调 System，计数仅为无副作用的原子自增；
  // realloc/alloc_zeroed 走 trait 默认实现（即本 alloc/dealloc 组合）
  unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
      ALLOCS.fetch_add(1, Ordering::Relaxed);
      BYTES.fetch_add(layout.size(), Ordering::Relaxed);
      unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
      unsafe { System.dealloc(ptr, layout) }
    }
  }

  #[global_allocator]
  static COUNTING: Counting = Counting;

  /// 至今累计的（分配次数, 分配字节数）
  pub fn snapshot() -> (usize, usize) {
    (
      ALLOCS.load(Ordering::Relaxed),
      BYTES.load(Ordering::Relaxed),
    )
  }
}
