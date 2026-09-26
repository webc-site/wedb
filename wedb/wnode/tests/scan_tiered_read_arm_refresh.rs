//! 分层 SCAN 族（SSCAN/HSCAN/ZSCAN）锁窗内元记录刷新回归（票 zcode-r127c-setscan1）
//!
//! 缺陷形：`exec_tiered_scan` 在锁外装载 `(meta, stub)`，以陈旧 `meta.size` 作
//! total 驱动游标收敛与帧头预留上界，且路由装载门对迁移 claim 直接 `map_err`
//! 上抛——键死亡 / 迁移窗与同键并发增长窗一律折 `-ERR` 存储错误帧或按陈旧
//! total 提前归零收敛（持续存活成员跨页漏扫），与 [`tiered_guard`] 读臂
//! （取锁后锁窗内 `refresh_tiered_meta` 单点）的裁决互相矛盾。
//!
//! 修法判据（本文件锁定，复用既有机制单点、禁第二套）：
//! - 读臂装载路由收口 [`load_collection_stub_for_read`] 单点（与
//!   `try_tiered_arm` 同一函数）：claim 在册回退未门禁装载快照，读面忙拒面
//!   不扩大（claim 窗逐字节帧回归见 scan_family_dualstate_frames.rs 追加用例）；
//! - 取锁后锁窗内 `refresh_tiered_meta` 三态与 `tiered_guard` 读臂逐臂同形：
//!   `Ok(true)` 用新值 / `Ok(false)` size 记 0 走应答缺失语义出 `[0, 空]` /
//!   `Err(MigrationBusy)` 回退装载快照照常扫 / 真实 IO 失败未落帧前上抛；
//! - total 以锁窗内新值为准 ⇒ 并发增长窗内「持续存活成员跨页至少一次覆盖」
//!   不变量成立（本文件压测核心）。
//!
//! 并发形态沿用 tiered_read_stale_meta.rs 先例：真实多线程 × 独立 compio
//! Runtime × [`std::sync::Barrier`] 起跑对齐，逐应答断言不变量，无 sleep、
//! 无 fake mock；静默态用例全部走 RESP 真实命令面逐字节断言。

use std::{
  mem::take,
  str::from_utf8,
  sync::{
    Arc, Barrier,
    atomic::{AtomicUsize, Ordering},
  },
  thread,
};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::map::HashSet;
use wcol::{SET_MEMBER_DUMMY_VALUE, types::member_ttl::encode_member};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};
use wnode::{
  resp::{
    garnet_api::{GarnetApi, StoreGarnetApi},
    resp_server_session::{RespServerSession, RespServerSessionOptions},
  },
  storage::session::storage_session::version_map_watch_hook,
};
use wresp::command::RespCommand;
use wtxn::WatchVersionMap;
use wval::GarnetObjectType;

type TestStore = WedbStore<SegmentedDevice>;

struct Env {
  rt: Runtime,
  store: Arc<TestStore>,
  _dir: tempfile::TempDir,
}

fn env(tag: &str) -> Env {
  let dir = tempdir().unwrap();
  let config = StoreConfig::new(2048, 1024 * 1024, 16, 0.5).unwrap();
  let device = Arc::new(SegmentedDevice::single_file(dir.path().join(tag)).unwrap());
  let store = Arc::new(WedbStore::open(config, device).unwrap());
  assert!(
    store.set_watch_hook(version_map_watch_hook(Arc::new(WatchVersionMap::new(
      1 << 10
    )))),
    "引擎级写面钩子应首次挂载"
  );
  Env {
    rt: Runtime::new().unwrap(),
    store,
    _dir: dir,
  }
}

struct Conn {
  api: GarnetApi,
  s: RespServerSession,
}

/// 独立连接装配（生产 thread-per-core 形态：并发各方各持一份会话）
fn conn_on_store(store: &Arc<TestStore>) -> Conn {
  let api: GarnetApi = Arc::new(StoreGarnetApi::new(store.new_session().unwrap()));
  let mut s = RespServerSession::new(1, RespServerSessionOptions::default());
  s.set_garnet_api(api.clone());
  Conn { api, s }
}

fn conn_on(env: &Env) -> Conn {
  conn_on_store(&env.store)
}

impl Conn {
  /// 单命令往返：同步段无输出且挂起慢路径时，以 await 承担网络泵角色闭环
  async fn exec(&mut self, cmd: RespCommand, args: &[&[u8]]) -> Vec<u8> {
    self.s.output.clear();
    self.api.exec(&mut self.s, cmd, args);
    if !self.s.output.is_empty() {
      return take(&mut self.s.output);
    }
    let slow = self
      .s
      .take_slow_wait()
      .unwrap_or_else(|| panic!("命令 {cmd} 无输出且未挂起慢路径"));
    slow.resolve().await
  }
}

/// 手工升阶（与 tiered_read_stale_meta.rs 同款原语，非 mock）
fn promote(
  env: &Env,
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
  assert!(
    env
      .rt
      .block_on(sess.load_collection_stub(key))
      .unwrap()
      .is_some(),
    "键应处于 wbftree 分层态"
  );
}

/// `:N\r\n` 整数应答解析
fn reply_int(resp: &[u8]) -> Option<i64> {
  let body = resp.strip_prefix(b":")?.strip_suffix(b"\r\n")?;
  from_utf8(body).ok()?.parse().ok()
}

fn bulk(v: &[u8]) -> Vec<u8> {
  let mut out = format!("${}\r\n", v.len()).into_bytes();
  out.extend_from_slice(v);
  out.extend_from_slice(b"\r\n");
  out
}

/// SCAN 族应答帧组装：`*2\r\n` + 游标 bulk + 条目数组头 + 逐条目帧
fn scan_frame(cursor: i64, items: &[Vec<u8>]) -> Vec<u8> {
  let mut out = b"*2\r\n".to_vec();
  out.extend_from_slice(&bulk(cursor.to_string().as_bytes()));
  out.extend_from_slice(&format!("*{}\r\n", items.len()).into_bytes());
  for item in items {
    out.extend_from_slice(item);
  }
  out
}

/// 解析 SCAN 族应答帧 → (游标, 条目字节列表)，非法帧直接 panic（帧合法性门）
fn parse_scan(frame: &[u8]) -> (i64, Vec<Vec<u8>>) {
  let text = String::from_utf8_lossy(frame);
  let mut parts = text.split("\r\n");
  assert_eq!(parts.next(), Some("*2"), "外层应为 *2: {text}");
  let cursor_hdr = parts.next().unwrap();
  assert!(cursor_hdr.starts_with('$'), "游标 bulk 头: {cursor_hdr}");
  let cursor: i64 = parts.next().unwrap().parse().unwrap();
  let arr_hdr = parts.next().unwrap();
  assert!(arr_hdr.starts_with('*'), "条目数组头: {arr_hdr}");
  let n: usize = arr_hdr[1..].parse().unwrap();
  let mut items = Vec::with_capacity(n);
  for _ in 0..n {
    let hdr = parts.next().unwrap();
    if hdr == "$-1" {
      items.push(Vec::new());
    } else {
      let len: usize = hdr[1..].parse().unwrap();
      let val = parts.next().unwrap().as_bytes().to_vec();
      assert_eq!(val.len(), len);
      items.push(val);
    }
  }
  (cursor, items)
}

/// 应答帧合法性门：不得为存储错误帧（本票核心不变量，先例同 assert_not_err）
fn assert_not_err(resp: &[u8], ctx: &str) {
  assert!(
    !resp.starts_with(b"-ERR "),
    "{ctx} 收到存储错误帧（有效读被折叠）: {}",
    String::from_utf8_lossy(resp)
  );
}

/// 存储忙错误帧识别（票方案二维护的装载即缺失折叠漏斗，仅此形态允许出错）
fn is_storage_err(resp: &[u8]) -> bool {
  resp.starts_with(b"-ERR ")
}

// ---------------- 测试点 a：并发增长窗持续存活成员跨页漏扫回归锁 ----------------

const OLD_SET_N: usize = 48;
const OLD_HASH_N: usize = 32;
const NEW_N: usize = 200;
const PAGES_ROUNDS: usize = 60;

fn old_set_members() -> Vec<Vec<u8>> {
  (0..OLD_SET_N)
    .map(|i| format!("o{i:02}").into_bytes())
    .collect()
}
fn new_set_members() -> Vec<Vec<u8>> {
  (0..NEW_N)
    .map(|i| format!("n{i:04}").into_bytes())
    .collect()
}
fn old_hash_fields() -> Vec<Vec<u8>> {
  (0..OLD_HASH_N)
    .map(|i| format!("z{i:02}").into_bytes())
    .collect()
}
fn new_hash_fields() -> Vec<Vec<u8>> {
  (0..NEW_N)
    .map(|i| format!("y{i:04}").into_bytes())
    .collect()
}

/// 分层集合 / 哈希固定数据集升阶（新成员词形 n* < o*、y* < z* 字典序在前，
/// 专门对位「装载→锁窗间隙新增成员插入游标之前」的最恶劣交错形态）
fn seed_tiered_growth(env: &Env) {
  promote(
    env,
    b"ts",
    GarnetObjectType::Set,
    old_set_members()
      .into_iter()
      .map(|m| (m, SET_MEMBER_DUMMY_VALUE.to_vec()))
      .collect(),
    i64::MAX,
  );
  promote(
    env,
    b"th",
    GarnetObjectType::Hash,
    old_hash_fields()
      .into_iter()
      .map(|f| {
        (
          f.clone(),
          encode_member(&[f.as_slice(), b"v"].concat()[..], None),
        )
      })
      .collect(),
    i64::MAX,
  );
}

/// 测试点 a（核心回归锁，票面「并发写交错下持续存活成员跨页漏扫」）：
/// 写线程逐个 SADD/HSET 增长（新成员字典序在全部旧成员之前）× 读线程
/// SSCAN/HSCAN 多页轮次遍历——修复后任一页 total 取锁窗内刷新值：
/// - 任一应答不得折存储错误帧（装载→锁窗间隙增长不再使陈旧 total 提前
///   归零、claim 自迁移窗读臂回退快照照常出帧）；
/// - 每轮全游标遍历的页并集必覆盖全部持续存活旧成员（漏扫即红）；
/// - 每页帧必须可解析且条目落在已知字母表（帧头预留-回填加宽/收窄合法形）；
/// - join 后静默态全游标遍历与终态全集精确相等收口。
#[test]
fn concurrent_growth_scan_pages_cover_persistent_members() {
  let env = env("scan-lockwin-growth.db");
  seed_tiered_growth(&env);
  let written = Arc::new(AtomicUsize::new(0));
  let barrier = Arc::new(Barrier::new(2));

  let writer = {
    let store = Arc::clone(&env.store);
    let barrier = Arc::clone(&barrier);
    let written = Arc::clone(&written);
    thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      let mut c = conn_on_store(&store);
      rt.block_on(async {
        barrier.wait();
        for i in 0..NEW_N {
          let m = format!("n{i:04}").into_bytes();
          let f = format!("y{i:04}").into_bytes();
          let v = format!("y{i:04}v").into_bytes();
          assert_not_err(&c.exec(RespCommand::Sadd, &[b"ts", &m]).await, "并发 SADD");
          assert_not_err(
            &c.exec(RespCommand::Hset, &[b"th", &f, v.as_slice()]).await,
            "并发 HSET",
          );
          written.store(i + 1, Ordering::Release);
        }
      });
    })
  };

  let reader = {
    let store = Arc::clone(&env.store);
    let barrier = Arc::clone(&barrier);
    let written = Arc::clone(&written);
    thread::spawn(move || {
      let rt = Runtime::new().unwrap();
      let mut c = conn_on_store(&store);
      let old_set: HashSet<Vec<u8>> = old_set_members().into_iter().collect();
      let old_hash: HashSet<Vec<u8>> = old_hash_fields().into_iter().collect();
      let new_set: HashSet<Vec<u8>> = new_set_members().into_iter().collect();
      let new_hash: HashSet<Vec<u8>> = new_hash_fields().into_iter().collect();
      rt.block_on(async {
        barrier.wait();
        for round in 0..PAGES_ROUNDS {
          // 轮初快照：已提交且持续存活（写方从不删除）的新成员前缀，与旧成员
          // 同属「轮开始即在树」集合——全游标遍历必至少一次覆盖
          let c0 = written.load(Ordering::Acquire);
          // SSCAN 全游标遍历：每页 total 已锁窗内刷新，轮始存活成员必覆盖
          let mut cursor = 0_i64;
          let mut seen: HashSet<Vec<u8>> = HashSet::default();
          let mut pages = 0usize;
          loop {
            let cs = cursor.to_string();
            let out = c
              .exec(RespCommand::Sscan, &[b"ts", cs.as_bytes(), b"COUNT", b"6"])
              .await;
            assert_not_err(&out, &format!("第 {round} 轮 SSCAN 第 {pages} 页"));
            let (next, items) = parse_scan(&out);
            for item in &items {
              assert!(
                old_set.contains(item) || new_set.contains(item),
                "SSCAN 出帧越界成员 {item:?}"
              );
              seen.insert(item.clone());
            }
            cursor = next;
            pages += 1;
            assert!(pages < 400, "SSCAN 游标未收敛（死循环防护）");
            if cursor == 0 {
              break;
            }
          }
          for m in old_set_members()
            .into_iter()
            .chain(new_set_members().into_iter().take(c0))
          {
            assert!(
              seen.contains(&m),
              "第 {round} 轮 SSCAN 全游标遍历漏扫轮始持续存活成员 {:?}（陈旧 total 提前收敛形）",
              String::from_utf8_lossy(&m)
            );
          }

          // HSCAN 全游标遍历（成对形态，帧头 ×2 上界同臂）
          let mut cursor = 0_i64;
          let mut fields: HashSet<Vec<u8>> = HashSet::default();
          let mut pages = 0usize;
          loop {
            let cs = cursor.to_string();
            let out = c
              .exec(RespCommand::Hscan, &[b"th", cs.as_bytes(), b"COUNT", b"6"])
              .await;
            assert_not_err(&out, &format!("第 {round} 轮 HSCAN 第 {pages} 页"));
            let (next, items) = parse_scan(&out);
            assert_eq!(items.len() % 2, 0, "HSCAN 成对条目数须为偶数");
            for pair in items.chunks(2) {
              assert!(
                old_hash.contains(&pair[0]) || new_hash.contains(&pair[0]),
                "HSCAN 出帧越界字段 {:?}",
                String::from_utf8_lossy(&pair[0])
              );
              assert_eq!(
                pair[1],
                match pair[0].starts_with(b"z") {
                  true => [pair[0].as_slice(), b"v"].concat(),
                  false => format!("{}v", String::from_utf8_lossy(&pair[0])).into_bytes(),
                },
                "HSCAN 字段-值配对偏离"
              );
              fields.insert(pair[0].clone());
            }
            cursor = next;
            pages += 1;
            assert!(pages < 400, "HSCAN 游标未收敛（死循环防护）");
            if cursor == 0 {
              break;
            }
          }
          for f in old_hash_fields()
            .into_iter()
            .chain(new_hash_fields().into_iter().take(c0))
          {
            assert!(
              fields.contains(&f),
              "第 {round} 轮 HSCAN 全游标遍历漏扫轮始持续存活字段 {:?}",
              String::from_utf8_lossy(&f)
            );
          }
        }
      });
    })
  };

  writer.join().unwrap();
  reader.join().unwrap();

  // 静默态收口：全游标遍历 == 终态全集（旧 ∪ 全部新），逐条目精确
  let mut c = conn_on(&env);
  let rt = &env.rt;
  let mut cursor = 0_i64;
  let mut seen: HashSet<Vec<u8>> = HashSet::default();
  loop {
    let cs = cursor.to_string();
    let out = rt.block_on(c.exec(RespCommand::Sscan, &[b"ts", cs.as_bytes()]));
    assert_not_err(&out, "静默态 SSCAN");
    let (next, items) = parse_scan(&out);
    seen.extend(items);
    cursor = next;
    if cursor == 0 {
      break;
    }
  }
  let expect: HashSet<Vec<u8>> = old_set_members()
    .into_iter()
    .chain(new_set_members())
    .collect();
  assert_eq!(seen.len(), expect.len(), "静默态 SSCAN 并集出现重复？");
  assert_eq!(seen, expect, "静默态 SSCAN 全游标并集 != 终态全集");
  assert_eq!(
    reply_int(&rt.block_on(c.exec(RespCommand::Scard, &[b"ts"]))),
    Some((OLD_SET_N + NEW_N) as i64)
  );
}

// ---------------- 测试点 b：键死亡应答缺失语义（非错误帧） ----------------

/// 测试点 b（静态判据形）：分层键消亡后 SCAN 族应答与信封缺失键逐字节同形
/// `[0, 空]`（tiered_guard 读臂 `Ok(false)` size 记 0 同一「应答缺失」语义，
/// 不走错误帧）；三域齐断
#[test]
fn dead_tiered_key_scan_answers_zero_empty_not_error() {
  let env = env("scan-lockwin-dead.db");
  promote(
    &env,
    b"ds",
    GarnetObjectType::Set,
    (0..6)
      .map(|i| {
        (
          format!("m{i}").into_bytes(),
          SET_MEMBER_DUMMY_VALUE.to_vec(),
        )
      })
      .collect(),
    i64::MAX,
  );
  promote(
    &env,
    b"dh",
    GarnetObjectType::Hash,
    (0..6)
      .map(|i| (format!("f{i}").into_bytes(), encode_member(b"v", None)))
      .collect(),
    i64::MAX,
  );
  promote(
    &env,
    b"dz",
    GarnetObjectType::SortedSet,
    (0..6)
      .map(|i| {
        (
          format!("e{i}").into_bytes(),
          encode_member(&1.0f64.to_be_bytes(), None),
        )
      })
      .collect(),
    i64::MAX,
  );

  let mut c = conn_on(&env);
  let rt = &env.rt;
  assert_eq!(
    rt.block_on(c.exec(RespCommand::Del, &[b"ds", b"dh", b"dz"])),
    b":3\r\n"
  );
  // 消亡后三域 SCAN 一律 [0, 空]，与从未存在的信封缺失键逐字节同形
  let missing = rt.block_on(c.exec(RespCommand::Sscan, &[b"never", b"0"]));
  assert_eq!(missing, scan_frame(0, &[]));
  for (cmd, key) in [
    (RespCommand::Sscan, &b"ds"[..]),
    (RespCommand::Hscan, &b"dh"[..]),
    (RespCommand::Zscan, &b"dz"[..]),
  ] {
    let out = rt.block_on(c.exec(cmd, &[key, b"0"]));
    assert!(
      !is_storage_err(&out),
      "{cmd} 消亡键应答折错误帧: {}",
      String::from_utf8_lossy(&out)
    );
    assert_eq!(out, missing, "{cmd} 消亡键应答应与缺失键逐字节同形");
    // 起始游标非零的消亡键同样收敛为 [0, 空]（scan_converge_cursor 单点）
    let out2 = rt.block_on(c.exec(cmd, &[key, b"7"]));
    assert_eq!(out2, scan_frame(0, &[]), "{cmd} 消亡键 start=7 应归零收敛");
  }
}

// ---------------- 测试点 d：静默态双态全游标集合相等 ----------------

/// 测试点 d：同一 30 成员数据集，信封态与分层态各自全游标分页（COUNT 7）
/// 遍历——两态页序列各自收敛至游标 0，排序并集逐字节相等（双态游标语义
/// 单源静默一致；迭代序域差异不构成集合差异）
#[test]
fn dual_state_quiescent_full_cursor_sets_equal() {
  let env = env("scan-lockwin-dualstate.db");
  let mut c = conn_on(&env);
  let rt = &env.rt;

  let members: Vec<Vec<u8>> = (0..30).map(|i| format!("x{i:02}").into_bytes()).collect();
  let mut add_args: Vec<&[u8]> = vec![b"es"];
  for m in &members {
    add_args.push(m.as_slice());
  }
  assert_eq!(
    rt.block_on(c.exec(RespCommand::Sadd, &add_args)),
    b":30\r\n"
  );
  promote(
    &env,
    b"et",
    GarnetObjectType::Set,
    members
      .iter()
      .cloned()
      .map(|m| (m, SET_MEMBER_DUMMY_VALUE.to_vec()))
      .collect(),
    i64::MAX,
  );

  let paginate = |c: &mut Conn, key: &[u8]| -> Vec<Vec<u8>> {
    let mut cursor = 0_i64;
    let mut all: Vec<Vec<u8>> = Vec::new();
    rt.block_on(async {
      loop {
        let cs = cursor.to_string();
        let out = c
          .exec(RespCommand::Sscan, &[key, cs.as_bytes(), b"COUNT", b"7"])
          .await;
        assert_not_err(&out, "静默态双态分页");
        let (next, items) = parse_scan(&out);
        all.extend(items);
        cursor = next;
        if cursor == 0 {
          break;
        }
      }
    });
    all
  };
  let mut env_seen = paginate(&mut c, b"es");
  let mut tiered_seen = paginate(&mut c, b"et");
  env_seen.sort();
  tiered_seen.sort();
  let expect: Vec<Vec<u8>> = {
    let mut v = members.clone();
    v.sort();
    v
  };
  assert_eq!(env_seen, expect, "信封态全游标并集偏离全集（重/漏）");
  assert_eq!(tiered_seen, expect, "分层态全游标并集偏离全集（重/漏）");

  // 单页全量形双态逐字节（分层字典序 == 排序后信封收集？否——信封迭代序
  // 进程随机，此处仅锁分层 COUNT 越界起始游标的收敛形与缺失形，序无关面
  // 由 scan_family_dualstate_frames.rs 既有逐字节用例覆盖）
  assert_eq!(
    rt.block_on(c.exec(RespCommand::Sscan, &[b"et", b"99"])),
    scan_frame(0, &[]),
    "分层态越界起始游标应空页归零"
  );
}
