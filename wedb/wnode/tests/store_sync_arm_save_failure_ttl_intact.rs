//! STORE 族同步臂「写故障窗失败即原态」回归（票 zcode-r122c-setstore1）
//!
//! 缺陷形态（旧序「清退先于写回」）：五同步写点在目标键 rmw 窗内先 `del_ttl_sync`
//! 真实清退（即落盘并 bump WATCH 版本），后 `*_save_or_gc` 写回遇存储故障以错误帧
//! 终结命令——TTL 已亡、值未写回、命令未成功：dst 旧值被失败命令「复活」永不过期，
//! 用户 TTL 账本被白扣一度，观察者伪 abort。违反冷臂 `obj_save_clear_ttl`
//! （storage_session.rs「先信封后清」）单机制；C# 面上该中间态不可观测
//!（SetOps.cs:422 SET 整写随载 expiration 归零、错误臂先于 SortedSetGeoOps.cs:178
//! Delete 返回，失败命令对 dst 零副作用）。
//!
//! 修复态：写回先行、清退随后（`store_writeback_clear_ttl` 单点）——save 失败/
//! 降级臂零清退即回错误帧/重放，失败命令对 dst 的 TTL/值/版本零触碰。
//!
//! 注入形态（真 IO 故障，非假 mock；hyperloglog.rs pfmerge 写故障注入门先例同款：
//! 小环形日志饱和 + InjectFailDevice 写开关定点恒败）：启用恒败门后连发越页大笔
//! 填充，环回绕复用「门后封页」脏 victim 的刷盘必败且失败笔定住 tail，再以页内
//! pad 把当页剩余收敛到 (墓碑尺寸, 信封尺寸) 区间——失败 STORE 的写回信封
//! （≈6KB）必越页触达卡死槽位：同步臂 PageNotReady 降级、慢路径补写强制刷盘
//! 真败、错误帧终结命令——旧序下清退墓碑小页内先落盘、写回后失败，本组判据即红；
//! 新序下写回先失败、清退零执行，键原态（值 + 剩余 TTL + WATCH 零推进）可断言。
//! 同步段写回与清退同处持窗单线程临界区零 await 点（store_ttl_clear_critical_section
//! 判据 1 执行期豁免申报见 `store_writeback_clear_ttl` 头注），本组以终态判据锁序。

use std::{
  io,
  str::from_utf8,
  sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
  },
};

use compio::runtime::Runtime;
use tempfile::tempdir;
use wbase::pool::{AlignedBuf, BufferPool};
use wdev::{Device, Error as WdevError, SegmentedDevice};
use wkv::{StoreConfig, WedbStore};
use wnode::{
  MessageConsumerFace, RespSessionConsumer,
  resp::{garnet_api::StoreGarnetApi, resp_server_session::RespServerSessionOptions},
  storage::session::storage_session::version_map_watch_hook,
};
use wtest_base::resp_frame as frame;
use wtxn::{TxnLockTable, WatchVersionMap};

type FailStore = WedbStore<InjectFailDevice>;

/// 页尺寸（与 StoreConfig 16KB × 4 页环形日志配置一致，degrade_env / pfmerge 同款小环）
const PAGE: u64 = 16 * 1024;

/// 设备故障注入开关共享态（rename_semantics / hyperloglog 同款计数定点恒败口径：
/// 0 = 关闭不计数，N = 自第 N 次对应 I/O 起恒败粘性）
#[derive(Default)]
struct InjectSwitches {
  fail_write_from: AtomicU64,
  writes: AtomicU64,
}

/// 真设备故障注入包装：全方法委托 SegmentedDevice，写按开关定点恒败——
/// 生产写失败同形的真 IO 错误上抛（非假 mock 虚设应答）
struct InjectFailDevice {
  inner: SegmentedDevice,
  switches: Arc<InjectSwitches>,
}

impl InjectFailDevice {
  fn tripped(counter: &AtomicU64, from: &AtomicU64) -> bool {
    match from.load(Ordering::Relaxed) {
      0 => false,
      n => counter.fetch_add(1, Ordering::Relaxed) + 1 >= n,
    }
  }
}

impl Device for InjectFailDevice {
  fn sector_size(&self) -> usize {
    self.inner.sector_size()
  }
  fn segment_size(&self) -> u64 {
    self.inner.segment_size()
  }
  fn direct_io(&self) -> bool {
    self.inner.direct_io()
  }
  fn start_segment(&self) -> u32 {
    self.inner.start_segment()
  }
  fn end_segment(&self) -> Option<u32> {
    self.inner.end_segment()
  }
  fn capacity(&self) -> Option<u64> {
    self.inner.capacity()
  }
  fn pool(&self) -> &Arc<BufferPool> {
    self.inner.pool()
  }
  async fn write_aligned(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    if Self::tripped(&self.switches.writes, &self.switches.fail_write_from) {
      return (
        Err(WdevError::Io(io::Error::other(
          "injected device write failure",
        ))),
        buf,
      );
    }
    self.inner.write_aligned(offset, buf).await
  }
  async fn read_aligned(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    self.inner.read_aligned(offset, buf).await
  }
  async fn read_raw(&self, offset: u64, buf: AlignedBuf) -> (wdev::Result<usize>, AlignedBuf) {
    self.inner.read_raw(offset, buf).await
  }
  async fn sync(&self) -> wdev::Result<()> {
    self.inner.sync().await
  }
  fn get_file_size(&self, segment_id: u32) -> wdev::Result<u64> {
    self.inner.get_file_size(segment_id)
  }
  async fn remove_segment(&self, segment_id: u32) -> wdev::Result<()> {
    self.inner.remove_segment(segment_id).await
  }
  async fn truncate_until_segment(&self, segment_id: u32) -> wdev::Result<()> {
    self.inner.truncate_until_segment(segment_id).await
  }
  fn reset(&self) {
    self.inner.reset();
  }
  fn recover(&self) -> wdev::Result<()> {
    self.inner.recover()
  }
}

/// 独立连接装配（每判点一份会话，生产 thread-per-core 形态）
fn consumer_on(store: &Arc<FailStore>) -> RespSessionConsumer {
  RespSessionConsumer::new(
    1,
    RespServerSessionOptions::default(),
    Arc::new(StoreGarnetApi::new(store.new_session().unwrap())),
  )
}

/// 单命令往返（慢路径挂起时以 block_on 承担网络泵角色闭环）
fn roundtrip(rt: &Runtime, c: &mut RespSessionConsumer, args: &[&[u8]]) -> Vec<u8> {
  let mut out = {
    let mut scratch = c.take_recv_scratch();
    scratch.extend_from_slice(&frame(args));
    c.return_recv_scratch(scratch);
    let mut out = Vec::new();
    let _ = c.try_consume_messages_into(&mut out);
    out
  };
  if let Some(slow) = c.take_slow_wait() {
    rt.block_on(async { out.extend_from_slice(&slow.resolve().await) });
  }
  out
}

/// PTTL 应答帧 → 剩余毫秒（-2 键缺失 / -1 无 TTL）
fn pttl_ms(reply: &[u8]) -> i64 {
  String::from_utf8_lossy(reply)
    .trim_start_matches(':')
    .trim_end_matches("\r\n")
    .parse()
    .unwrap_or(-2)
}

/// RESP2 数组帧成员排序归一：SMEMBERS 集合迭代序随哈希拼装非契约面，
/// 判据锁成员集合本身（帧形逐字节仍严格比）；非数组帧（:1 等）原样透传
fn norm_array(reply: &[u8]) -> Vec<u8> {
  if !reply.starts_with(b"*") {
    return reply.to_vec();
  }
  let mut els = Vec::new();
  let nl = reply.iter().position(|&b| b == b'\n').unwrap() + 1;
  let mut rest = &reply[nl..];
  while let Some(pos) = rest.iter().position(|&b| b == b'\n') {
    // 行尾含 \r：剥除后才是 `$len`
    let line = rest[..pos].strip_suffix(b"\r").unwrap_or(&rest[..pos]);
    let len: usize = line
      .strip_prefix(b"$")
      .and_then(|v| from_utf8(v).ok())
      .and_then(|v| v.parse().ok())
      .unwrap_or_else(|| panic!("非 bulk 成员帧形：{:?}", String::from_utf8_lossy(reply)));
    els.push(rest[pos + 1..pos + 1 + len].to_vec());
    rest = &rest[pos + 1 + len + 2..];
  }
  els.sort();
  let mut out = format!("*{}\r\n", els.len()).into_bytes();
  for e in els {
    out.extend_from_slice(format!("${}\r\n", e.len()).as_bytes());
    out.extend_from_slice(&e);
    out.extend_from_slice(b"\r\n");
  }
  out
}

/// 60B 定长成员名（结果信封 ≈ 6KB 载荷，恒大于页界剩余、小于单页容量）
fn big_members(n: usize) -> Vec<Vec<u8>> {
  (0..n)
    .map(|i| {
      let base = format!("mem-{i:04}-");
      let mut m = base.into_bytes();
      m.resize(60, b'x');
      m
    })
    .collect()
}

/// 环形饱和（写开关未启用，驱逐刷盘正常闭环；降级笔经慢路径补完保证推进）
fn saturate(rt: &Runtime, store: &Arc<FailStore>) {
  let mut c = consumer_on(store);
  let filler = vec![b'v'; 2048];
  for i in 0..200 {
    let key = format!("sat{i:03}");
    let out = roundtrip(rt, &mut c, &[b"SET", key.as_bytes(), &filler]);
    assert_eq!(out, b"+OK\r\n", "饱和期 SET 应恒成功闭环");
  }
}

/// 故障窗确立：恒败门启用后连发越页大笔 SET 填充——环形回绕一旦要求复用
/// 「启用门后封页」的脏 victim，其驱逐刷盘必败、慢路径补写即回错误帧，且失败
/// 命令绝不推进 tail：tail 恒滞留于卡死脏槽位页界之前、当页剩余恒小于信封尺寸
/// 而大于 TTL 墓碑尺寸（pfmerge_slowpath_partial_commit_write_failure 同款
/// 「饱和+恒败门」确立面，判据以本命令必回错误帧自证窗已生效）
fn force_fail_window(rt: &Runtime, store: &Arc<FailStore>) {
  let mut c = consumer_on(store);
  let big = vec![b'q'; 14 * 1024];
  for i in 0..24usize {
    let key = format!("prime:pf{i:02}");
    let out = roundtrip(rt, &mut c, &[b"SET", key.as_bytes(), &big]);
    if out.starts_with(b"-") {
      return;
    }
  }
  panic!("故障窗未确立：启用恒败门后 24 笔越页填充无一触发卡死刷盘失败");
}

/// 停笔收敛：故障窗确立后失败笔不推进 tail，当页剩余或为大段留白——页内补
/// 小笔 pad 把剩余压到 < PAD_CEIL（pad 笔恒页内闭环不越页、不触达卡死槽位），
/// 令后续 STORE 结果信封（≈6KB ≫ PAD_CEIL）必越页触达卡死脏 victim；而 TTL
/// 墓碑小笔（百B 级 < PAD_CEIL）旧序下仍可页内先落——新旧序判据在此几何上分叉
const PAD_CEIL: u64 = 2048;
fn shrink_current_page(rt: &Runtime, store: &Arc<FailStore>) {
  let mut c = consumer_on(store);
  // 最坏情形 tail 恰在新页页首：rem=16KB，400B pad ≈ 每笔 415B 足印，<40 笔收敛
  for i in 0..96usize {
    let rem = PAGE - store.tail_address() % PAGE;
    if rem < PAD_CEIL {
      return;
    }
    let key = format!("shrink:sc{i:02}");
    let out = roundtrip(rt, &mut c, &[b"SET", key.as_bytes(), &vec![b'z'; 400]]);
    assert!(
      !out.starts_with(b"-"),
      "页内 pad 笔不应触达故障窗：rem={rem}，实际 {:?}",
      String::from_utf8_lossy(&out)
    );
  }
  panic!(
    "页内停笔收敛迭代未落位：rem={}",
    PAGE - store.tail_address() % PAGE
  );
}

/// 通用故障臂用例：饱和 → seed 素材 → 恒败门启用并确立故障窗 →
/// 目标键 WATCH → 失败 STORE（错误帧）→ 键原态三连判（值 / 剩余 TTL / WATCH 零推进）
fn run_fail_arm(
  tag: &str,
  dst: &[u8],
  seed: &[Vec<Vec<u8>>],
  store_cmd: &[&[u8]],
  verify_cmd: &[&[u8]],
  expect_value: &[u8],
) {
  let dir = tempdir().unwrap();
  let switches = Arc::new(InjectSwitches::default());
  let inner = SegmentedDevice::single_file(dir.path().join(format!("st-{tag}.db"))).unwrap();
  let device = Arc::new(InjectFailDevice {
    inner,
    switches: Arc::clone(&switches),
  });
  let store =
    Arc::new(WedbStore::open(StoreConfig::new(1024, 16 * 1024, 4, 0.5).unwrap(), device).unwrap());
  let rt = Runtime::new().unwrap();
  // WATCH/MULTI/EXEC 事务组件装配（tiered_watch_fence.rs 同款生产装配同径）：
  // 引擎级写面钩子把真实写 bumps 打进共享版本表，观察者经同一表校验伪推进
  let map = Arc::new(WatchVersionMap::new(1 << 10));
  assert!(
    store.set_watch_hook(version_map_watch_hook(Arc::clone(&map))),
    "引擎级写面钩子应首次挂载"
  );

  saturate(&rt, &store);
  {
    let mut c = consumer_on(&store);
    for cmd in seed {
      let slices: Vec<&[u8]> = cmd.iter().map(Vec::as_slice).collect();
      let out = roundtrip(&rt, &mut c, &slices);
      assert!(
        !out.starts_with(b"-"),
        "seed 命令 {}/ {:?} 不应报错：{:?}",
        tag,
        String::from_utf8_lossy(slices[0]),
        String::from_utf8_lossy(&out)
      );
    }
  }
  // 恒败门启用 + 确立故障窗：此后一切 victim 驱逐刷盘必败，回绕复用卡死脏槽位
  // 的填充笔先失败定住 tail——STORE 写回笔越页必触达该窗（force_fail_window）
  switches.fail_write_from.store(1, Ordering::Relaxed);
  force_fail_window(&rt, &store);
  // 停笔收敛：把当页剩余压到信封之下、墓碑之上（shrink_current_page）
  shrink_current_page(&rt, &store);

  let mut watcher = consumer_on(&store);
  watcher.attach_transaction_components(Arc::clone(&map), TxnLockTable::new());
  assert_eq!(
    roundtrip(&rt, &mut watcher, &[b"WATCH", dst]),
    b"+OK\r\n",
    "WATCH 前置失败（用例失效）"
  );

  let mut c = consumer_on(&store);
  let reply = roundtrip(&rt, &mut c, store_cmd);
  assert!(
    reply.starts_with(b"-"),
    "{tag}：写故障窗 STORE 必以错误帧终结，实际 {:?}",
    String::from_utf8_lossy(&reply)
  );
  // 失败即原态判据一：旧值原样（旧序仅值面同判据成立，靠 TTL 判据分叉）；
  // 集合族迭代序非契约，数组帧成员排序后逐字节锁
  let value = roundtrip(&rt, &mut c, verify_cmd);
  assert_eq!(
    norm_array(&value),
    norm_array(expect_value),
    "{tag}：失败 STORE 后 dst 值应原样（旧值未被清退亦未被覆写）"
  );
  // 失败即原态判据二：剩余 TTL 原样存活——旧序「清退先于写回」在此抹 TTL 判红
  let ttl = pttl_ms(&roundtrip(&rt, &mut c, &[b"PTTL", dst]));
  assert!(
    ttl > 0,
    "{tag}：失败 STORE 不得先清 TTL 后写失败（键被失败命令复活永不过期），实际 PTTL={ttl}"
  );
  // 失败即原态判据三：WATCH 零推进（C# 失败命令版本零推进；旧序真实清退即 bump
  // 判红伪 abort）
  assert_eq!(roundtrip(&rt, &mut watcher, &[b"MULTI"]), b"+OK\r\n");
  let queued = roundtrip(&rt, &mut watcher, verify_cmd);
  assert!(
    queued.starts_with(b"+QUEUED"),
    "MULTI 排队失败：{:?}",
    String::from_utf8_lossy(&queued)
  );
  let exec = roundtrip(&rt, &mut watcher, &[b"EXEC"]);
  assert!(
    !exec.starts_with(b"*-1"),
    "{tag}：失败 STORE 令 WATCH 版本伪推进致观察者 abort（C# 失败命令零推进），EXEC={:?}",
    String::from_utf8_lossy(&exec)
  );
}

/// SINTERSTORE 同步臂（set write.rs combine_store）：三源内存态、dst 带 TTL，
/// 写故障窗失败后 dst 值/TTL/版本三判据原样
#[test]
fn sinterstore_sync_save_failure_keeps_dest_ttl_intact() {
  let ms = big_members(100);
  let seed = vec![
    vec![
      b"SADD".to_vec(),
      b"s:dst".to_vec(),
      b"old1".to_vec(),
      b"old2".to_vec(),
    ],
    {
      let mut v = vec![b"SADD".to_vec(), b"s:s1".to_vec()];
      v.extend(ms.iter().cloned());
      v
    },
    {
      let mut v = vec![b"SADD".to_vec(), b"s:s2".to_vec()];
      v.extend(ms.iter().cloned());
      v
    },
    vec![b"EXPIRE".to_vec(), b"s:dst".to_vec(), b"600".to_vec()],
  ];
  run_fail_arm(
    "sinter",
    b"s:dst",
    &seed,
    &[b"SINTERSTORE", b"s:dst", b"s:s1", b"s:s2"],
    &[b"SMEMBERS", b"s:dst"],
    b"*2\r\n$4\r\nold1\r\n$4\r\nold2\r\n",
  );
}

/// SUNIONSTORE 同漏斗回归探针（combine_store 三命令共用单漏斗，锁一形即锁全形；
/// 独立存储实例防用例间串扰）
#[test]
fn sunionstore_sync_save_failure_keeps_dest_ttl_intact() {
  let ms = big_members(100);
  let seed = vec![
    vec![b"SADD".to_vec(), b"u:dst".to_vec(), b"old1".to_vec()],
    {
      let mut v = vec![b"SADD".to_vec(), b"u:s1".to_vec()];
      v.extend(ms.iter().cloned());
      v
    },
    vec![b"EXPIRE".to_vec(), b"u:dst".to_vec(), b"600".to_vec()],
  ];
  run_fail_arm(
    "sunion",
    b"u:dst",
    &seed,
    &[b"SUNIONSTORE", b"u:dst", b"1", b"u:s1"],
    &[b"SMEMBERS", b"u:dst"],
    b"*1\r\n$4\r\nold1\r\n",
  );
}

/// ZRANGESTORE 同步臂（zset write.rs :256 裸点形）
#[test]
fn zrangestore_sync_save_failure_keeps_dest_ttl_intact() {
  let ms = big_members(100);
  let mut zadd = vec![b"ZADD".to_vec(), b"z:src".to_vec()];
  for (i, m) in ms.iter().enumerate() {
    zadd.push((i + 1).to_string().into_bytes());
    zadd.push(m.clone());
  }
  let seed = vec![
    vec![
      b"ZADD".to_vec(),
      b"z:dst".to_vec(),
      b"1".to_vec(),
      b"old".to_vec(),
    ],
    zadd,
    vec![b"EXPIRE".to_vec(), b"z:dst".to_vec(), b"600".to_vec()],
  ];
  run_fail_arm(
    "zrange",
    b"z:dst",
    &seed,
    &[b"ZRANGESTORE", b"z:dst", b"z:src", b"0", b"-1"],
    &[b"ZCARD", b"z:dst"],
    b":1\r\n",
  );
}

/// ZDIFFSTORE 同步臂（zset write.rs :341 裸点形）
#[test]
fn zdiffstore_sync_save_failure_keeps_dest_ttl_intact() {
  let ms = big_members(100);
  let mut za = vec![b"ZADD".to_vec(), b"d:a".to_vec()];
  let mut zb = vec![b"ZADD".to_vec(), b"d:b".to_vec()];
  for (i, m) in ms.iter().enumerate() {
    za.push((i + 1).to_string().into_bytes());
    za.push(m.clone());
    let other = format!("x-{i:04}");
    zb.push((i + 1).to_string().into_bytes());
    zb.push(other.into_bytes());
  }
  let seed = vec![
    vec![
      b"ZADD".to_vec(),
      b"d:dst".to_vec(),
      b"1".to_vec(),
      b"old".to_vec(),
    ],
    za,
    zb,
    vec![b"EXPIRE".to_vec(), b"d:dst".to_vec(), b"600".to_vec()],
  ];
  run_fail_arm(
    "zdiff",
    b"d:dst",
    &seed,
    &[b"ZDIFFSTORE", b"d:dst", b"2", b"d:a", b"d:b"],
    &[b"ZCARD", b"d:dst"],
    b":1\r\n",
  );
}

/// ZUNIONSTORE 同步臂（zset write.rs :699 sorted_set_combine_store 裸点形，
/// ZINTERSTORE 共漏斗）
#[test]
fn zunionstore_sync_save_failure_keeps_dest_ttl_intact() {
  let ms = big_members(100);
  let mut za = vec![b"ZADD".to_vec(), b"u:a".to_vec()];
  let mut zb = vec![b"ZADD".to_vec(), b"u:b".to_vec()];
  for (i, m) in ms.iter().enumerate() {
    if i % 2 == 0 {
      za.push((i + 1).to_string().into_bytes());
      za.push(m.clone());
    } else {
      zb.push((i + 1).to_string().into_bytes());
      zb.push(m.clone());
    }
  }
  let seed = vec![
    vec![
      b"ZADD".to_vec(),
      b"u:dst".to_vec(),
      b"1".to_vec(),
      b"old".to_vec(),
    ],
    za,
    zb,
    vec![b"EXPIRE".to_vec(), b"u:dst".to_vec(), b"600".to_vec()],
  ];
  run_fail_arm(
    "zunion",
    b"u:dst",
    &seed,
    &[b"ZUNIONSTORE", b"u:dst", b"2", b"u:a", b"u:b"],
    &[b"ZCARD", b"u:dst"],
    b":1\r\n",
  );
}

/// GEOSEARCHSTORE 同步臂（sorted_set_geo_commands.rs :704 裸点形）：对标 C#
/// GeoSearchStore dest 排他锁跨 Delete→ZADD、错误臂先于 Delete 零副作用
#[test]
fn geosearchstore_sync_save_failure_keeps_dest_ttl_intact() {
  let mut geoadd = vec![b"GEOADD".to_vec(), b"g:src".to_vec()];
  for i in 0..100 {
    let lon = 13.36 + (i as f64) * 0.0005;
    let lat = 38.11 + (i as f64) * 0.0003;
    let name = format!("mem-{i:04}-{}", "y".repeat(52));
    geoadd.push(lon.to_string().into_bytes());
    geoadd.push(lat.to_string().into_bytes());
    geoadd.push(name.into_bytes());
  }
  let seed = vec![
    vec![
      b"GEOADD".to_vec(),
      b"g:dst".to_vec(),
      b"1.0".to_vec(),
      b"1.0".to_vec(),
      b"old".to_vec(),
    ],
    geoadd,
    vec![b"EXPIRE".to_vec(), b"g:dst".to_vec(), b"600".to_vec()],
  ];
  run_fail_arm(
    "geo",
    b"g:dst",
    &seed,
    &[
      b"GEOSEARCHSTORE",
      b"g:dst",
      b"g:src",
      b"FROMLONLAT",
      b"13.36",
      b"38.11",
      b"BYRADIUS",
      b"200",
      b"km",
      b"WITHDIST",
    ],
    &[b"ZCARD", b"g:dst"],
    b":1\r\n",
  );
}
