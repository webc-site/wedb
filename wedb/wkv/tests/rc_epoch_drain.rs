//! ReadCache 回绕换页纪元延迟关闭屏障验收测试（本票新增）
//!
//! 对标 C# 原型：
//! - libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:ShiftHeadAddress
//!   的 `epoch.BumpCurrentEpoch(() => OnPagesClosed(newHeadAddress))`
//! - AllocatorBase.cs:OnPagesClosed → OnPagesClosedWorker → OnPagesClosedWorkerCore
//!   （SafeHeadAddress 先于 ClosedUntilAddress 单调推进，清页/换装恒在纪元排空后）
//! - docs/readcache.md「Memory reclamation and epoch protection」
//!
//! 测试一为确定性屏障判据：读线程持 Participant 纪元守卫停在 `PageView::Fast`
//! 裸切片借出的 disclose 闭包内，换页方武装 + 泵入注册后，旧页关闭序列必须
//! 分毫不发（closed/safe_head 不推进、tail 不换页、借出字节逐字节恒稳）；读线程
//! 退出守卫后方收割执行。测试二为有界高并发压测：多写多读跨多轮回绕，全部读链
//! 走查在纪元保护下进行，期望键/值由编码几何直接推导，断言零撕裂、零假
//! NOTFOUND（静默点走链 Gone 违例）、零 panic，且全程 10 秒硬时限（零活锁/死锁）。
//!
//! 自研依据: RCU 纪元排空（C# 纪元保护方案对标 libs/storage/Tsavorite/cs/test/EpochProtectedVersionScheme.cs）

use std::{
  mem::take,
  sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering::Relaxed},
    mpsc,
  },
  thread::{scope, yield_now},
  time::{Duration, Instant},
};

use wbase::addr::{is_read_cache, with_read_cache};
use wepoch::LightEpoch;
use windex::{HashBucketEntry, HashIndex};
use wkv::{RcVisit, ReadCache};
use wrecord::record_size;

/// 测试几何：512B 页 × 2 页环；记录恒 24B（16B 头 + 5B 键 + 1B 值，8B 对齐），
/// 每页恰 21 条（512 = 21*24 + 8，页尾余量 8 < 24 恒触发换页分支）。
/// REC/PER_PAGE 运行时经 `check_geometry` 校验与编码算式一致
const PAGE: usize = 512;
const REC: usize = 24;
const PER_PAGE: usize = PAGE / REC;

/// 主日志哨兵地址（cleanse 脱钩后索引恢复至此）
const MAIN_ADDR: u64 = 0x1_0000;

/// 编码几何单点校验：期望记录尺寸由编码格式推导，公式失配即测试几何失效
fn check_geometry() {
  assert_eq!(
    record_size(5, 1),
    REC,
    "16B 头 + 5B 键 + 1B 值按 8B 对齐恒 24B"
  );
  assert_eq!(PAGE % REC, 8, "页尾余量必为 8");
  const {
    assert!(
      PAGE % REC < REC,
      "页尾余量必须小于单记录，每页灌入条数恒定几何公式才成立",
    );
  }
}

/// 定宽 5 字节键
fn key(prefix: u8, i: u64) -> Vec<u8> {
  let mut k = vec![prefix];
  k.extend_from_slice(format!("{i:04}").as_bytes());
  k
}

/// 值编码推导：1B 定值（序号唯一决定的字节模式）
fn val1(i: u64) -> Vec<u8> {
  vec![b'v' ^ (i % 7) as u8]
}

/// 测试灌数挂载：按唯一免查重追加写入口挂主日志地址（与 src 内测试同口径，
/// append 内部以链首插入 CAS 协议抢占该槽位，cleanse 后恢复至此）
fn mount_main(index: &HashIndex, k: &[u8], main_addr: u64) {
  let hash = HashIndex::hash_key(k);
  let tag = HashBucketEntry::tag_from_hash(hash);
  index
    .insert_to_bucket(index.bucket_index_for_hash(hash), tag, main_addr)
    .expect("测试灌数挂载必成功");
}

/// 武装/泵入一体式追加：回绕换页武装拍返回 None（旧页关闭已挂入纪元延迟队列），
/// 调用线程此时恒处于出借期安全点，泵入注册后重试同条
fn append_pump_retry(rc: &Arc<ReadCache>, index: &Arc<HashIndex>, k: &[u8], v: &[u8]) -> u64 {
  loop {
    if let Some(a) = rc.append(k, v, index, 0) {
      return a;
    }
    rc.pump_close_barrier(index, None);
  }
}

/// 确定性屏障判据：纪元守卫内的在途无锁直读借出切片，跨「武装 + 泵入注册 +
/// 换页方持续尝试追加」全程逐字节恒稳；读线程退出守卫前 closed/safe_head/tail
/// 一律不得推进，退出后方完成关闭
#[test]
fn epoch_barrier_defers_page_clear_until_protected_reader_exits() {
  check_geometry();
  let epoch = Arc::new(LightEpoch::new(8));
  let rc = Arc::new(ReadCache::new(PAGE, 2, true, Arc::clone(&epoch)).expect("构造读缓存"));
  let index = Arc::new(HashIndex::new(64).expect("构造索引"));

  // 灌满页 0 与页 1（跳向页 1 为正向首轮内联换装，无驱逐）
  for i in 0..(2 * PER_PAGE) as u64 {
    let k = key(b'A', i);
    mount_main(&index, &k, MAIN_ADDR + i);
    append_pump_retry(&rc, &index, &k, &val1(i));
  }
  assert_eq!(rc.head_address(), 0, "两页正向首轮不应推进 head");
  assert_eq!(rc.closed_until_address(), 0, "两页正向首轮不应发生驱逐");

  // 页 0 首条记录：绝对地址 0，键 A0000、值 val1(0)——快路径裸切片直读的受害者
  let victim = key(b'A', 0);
  let victim_val = val1(0);

  let (parked_tx, parked_rx) = mpsc::channel::<()>();
  let (wake_tx, wake_rx) = mpsc::channel::<()>();

  let (k_after, v_after) = scope(|s| {
    let epoch2 = Arc::clone(&epoch);
    let rc2 = Arc::clone(&rc);
    let victim2 = victim.clone();
    let victim_val2 = victim_val.clone();
    let reader = s.spawn(move || {
      let participant = epoch2.register().expect("注册纪元参与者");
      let out;
      {
        let _guard = participant.enter();
        let visit = rc2.with_record(with_read_cache(0), |k, v| {
          // 借出窗口入口判据：快路径裸切片此刻完整
          assert_eq!(k, victim2.as_slice(), "借出窗口入口键必须完整");
          assert_eq!(v, victim_val2.as_slice(), "借出窗口入口值必须完整");
          parked_tx.send(()).expect("停车信号送达");
          wake_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("唤醒信号 10 秒内送达");
          // 唤醒时点仍在守卫借出窗内取快照：屏障成立则字节逐字节恒稳
          //（epoch 排空先于 clear_page 的 fill(0) 与新代 encode_to_slice 覆写）
          Some((k.to_vec(), v.to_vec()))
        });
        out = match visit {
          RcVisit::Found(pair) => pair,
          other => panic!("纪元守卫内的借出记录绝不允许 Gone/链终止，实际 {other:?}"),
        };
      }
      out
    });

    // 主线程：确认读线程已停在借出闭包内（持旧纪元守卫）
    parked_rx
      .recv_timeout(Duration::from_secs(10))
      .expect("读线程已进入借出停车窗");

    // 触发首轮回绕驱逐页 0：换页武装拍（head 推进 + 关闭水位登记）返回 None，
    // 安全点泵入注册纪元延迟动作
    let b = key(b'B', 0);
    mount_main(&index, &b, MAIN_ADDR + 100);
    let armed = rc.append(&b, &val1(100), &index, 0);
    assert!(armed.is_none(), "回绕换页武装拍必须作废本条晋升");
    assert_eq!(
      rc.head_address(),
      PAGE as u64,
      "武装即推进 head 至被驱逐页 0 页末"
    );
    rc.pump_close_barrier(&index, None);

    // 屏障判据：读线程仍受保护 ⇒ 关闭序列分毫不得发生
    assert_eq!(
      rc.closed_until_address(),
      0,
      "纪元排空完成前 ClosedUntilAddress 绝不发布（旧缺陷：clear_page 抢先于排空）"
    );
    assert_eq!(
      rc.safe_head_address(),
      0,
      "纪元排空完成前 SafeHeadAddress 绝不标定（对标 OnPagesClosedWorkerCore 序）"
    );
    assert!(
      rc.tail_address() < 2 * PAGE as u64,
      "关闭未执行 ⇒ tail 不得换页发布"
    );

    // 借出期持续追加尝试：正确屏障下页不换装、全部作废返回 None
    let mut overwrite = 0usize;
    for i in 1..8u64 {
      let b = key(b'B', i);
      mount_main(&index, &b, MAIN_ADDR + 200 + i);
      let a = rc.append(&b, &val1(200 + i), &index, 0);
      rc.pump_close_barrier(&index, None);
      if a.is_some() {
        overwrite += 1;
      }
    }
    assert_eq!(
      overwrite, 0,
      "在途受保护读者未退出前不得有任何新代记录落入被驱逐槽位（借出切片覆写违例）"
    );

    // 放行读线程（守卫退出后延迟关闭动作方可被收割执行）
    wake_tx.send(()).expect("唤醒信号送达");
    reader.join().expect("读线程零 panic")
  });

  // 唤醒时点读线程仍持守卫 ⇒ 借出切片恒稳：逐字节精确判据（期望值由编码几何推导）
  assert_eq!(
    k_after, victim,
    "纪元屏障成立时借出键在停车全程逐字节恒稳（撕裂/清零/覆写均在此现形）"
  );
  assert_eq!(
    v_after, victim_val,
    "纪元屏障成立时借出值在停车全程逐字节恒稳（撕裂/清零/覆写均在此现形）"
  );

  // 守卫退出后：延迟动作经 exit 尾随收割 + drain 兜底执行，水位补齐、旧链恢复主日志
  epoch.drain();
  assert_eq!(
    rc.safe_head_address(),
    PAGE as u64,
    "关闭完成后 SafeHead 标定被驱逐页页末"
  );
  assert_eq!(
    rc.closed_until_address(),
    PAGE as u64,
    "关闭完成后 ClosedUntil 精确推进至被驱逐页页末"
  );
  assert_eq!(rc.head_address(), rc.closed_until_address());
  let slot = index.find_tag(&victim).expect("已挂载键必须可寻址");
  assert!(
    !is_read_cache(slot) && slot == MAIN_ADDR,
    "cleanse 必须把被驱逐页记录的索引恢复至主日志地址，实际 {slot:#x}"
  );
}

/// 有界高并发压测：3 写线程跨 ≈114 个换页事件持续回绕驱逐，2 读线程全程持
/// 纪元守卫走链——每个命中键的值必须与编码推导的期望字节逐位相等（零撕裂），
/// 静默点（写尽 + 全部延迟关闭收割后）走链不得出现 Gone（零假 NOTFOUND），
/// 参与者刷新有界推进关闭收割（10 秒硬时限，零活锁/死锁）。
#[test]
fn bounded_stress_guarded_reads_never_tear_under_wrapped_eviction() {
  check_geometry();
  const WRITERS: usize = 3;
  const READERS: usize = 2;
  const EACH: u64 = 800; // 2400 条 ≈ 114 次换页事件，其中回绕驱逐 113 次

  let epoch = Arc::new(LightEpoch::new(8));
  let rc = Arc::new(ReadCache::new(PAGE, 2, true, Arc::clone(&epoch)).expect("构造读缓存"));
  let index = Arc::new(HashIndex::new(1 << 14).expect("构造索引"));
  // 读线程抽样队列：写线程周期性投放 (键, 期望值)，读线程消费；消费掉的
  // 样本转入 done 队列供静默点终检取证
  let samples = Arc::new(Mutex::new(Vec::<(Vec<u8>, Vec<u8>)>::new()));
  let done = Arc::new(Mutex::new(Vec::<(Vec<u8>, Vec<u8>)>::new()));
  let stop = Arc::new(AtomicBool::new(false));
  let tears = Arc::new(AtomicU64::new(0));
  let false_gone = Arc::new(AtomicU64::new(0));
  let guard_hits = Arc::new(AtomicU64::new(0));
  let wrap_events = Arc::new(AtomicU64::new(0));

  let deadline = Instant::now() + Duration::from_secs(10);

  scope(|s| {
    let mut wh = Vec::with_capacity(WRITERS);
    for w in 0..WRITERS {
      let (rc, index, samples, wrap_events) = (
        Arc::clone(&rc),
        Arc::clone(&index),
        Arc::clone(&samples),
        Arc::clone(&wrap_events),
      );
      wh.push(s.spawn(move || {
        for i in 0..EACH {
          let k = key(b'p' + w as u8, i);
          let v = val1(i);
          mount_main(&index, &k, MAIN_ADDR + w as u64 * 100_000 + i);
          let head_before = rc.head_address();
          append_pump_retry(&rc, &index, &k, &v);
          if rc.head_address() > head_before {
            wrap_events.fetch_add(1, Relaxed);
          }
          if i % 32 == 31 {
            samples.lock().unwrap().push((k.clone(), v.clone()));
          }
          assert!(
            Instant::now() < deadline,
            "写线程 10 秒硬时限内必须完成配额"
          );
        }
      }));
    }
    let mut rh = Vec::with_capacity(READERS);
    for _r in 0..READERS {
      let (rc, index, epoch, samples, stop, tears, guard_hits, done) = (
        Arc::clone(&rc),
        Arc::clone(&index),
        Arc::clone(&epoch),
        Arc::clone(&samples),
        Arc::clone(&stop),
        Arc::clone(&tears),
        Arc::clone(&guard_hits),
        Arc::clone(&done),
      );
      rh.push(s.spawn(move || {
        let participant = epoch.register().expect("注册纪元参与者");
        loop {
          let sample = {
            let mut q = samples.lock().unwrap();
            if q.is_empty() {
              None
            } else {
              Some(q.remove(0))
            }
          };
          let Some((want_k, want_v)) = sample else {
            assert!(Instant::now() < deadline, "读线程等待样本不得越 10 秒时限");
            if stop.load(Relaxed) {
              break;
            }
            yield_now();
            continue;
          };
          // 纪元守卫全程覆盖单次链走查：对标 session/raw/read.rs
          // find_in_read_cache 的 Participant 保护口径
          {
            let _guard = participant.enter();
            let mut curr = index.find_tag(&want_k).expect("样本键必须可寻址");
            let mut restart = 0u32;
            loop {
              assert!(
                Instant::now() < deadline,
                "单次走链 10 秒硬时限（死锁/活锁征兆）"
              );
              if !is_read_cache(curr) {
                break; // 链落主日志段（已清洗恢复），合法终态
              }
              // 驱逐等待协议（对标 ReadCacheNeedToWaitForEviction）：解除
              // 判据闭包携本参与者刷新（ProtectAndDrain 语义）
              if rc.need_to_wait_for_eviction(curr, || participant.refresh()) {
                curr = index.find_tag(&want_k).expect("样本键必须可寻址");
                continue;
              }
              match rc.with_record(curr, |k, v| {
                (k == want_k.as_slice()).then(|| {
                  if v != want_v.as_slice() {
                    tears.fetch_add(1, Relaxed);
                  }
                })
              }) {
                RcVisit::Found(_) => {
                  guard_hits.fetch_add(1, Relaxed);
                  break;
                }
                RcVisit::Next(prev) => {
                  if prev == 0 {
                    break; // 链尽：RC 前缀已被热写脱钩/清洗，主日志域合法
                  }
                  curr = prev;
                }
                RcVisit::Gone => {
                  // 换装竞态：回链头重探（RestartChain），绝不当不存在；
                  // 重探有界防活锁
                  restart += 1;
                  assert!(restart <= 64, "链头重探 64 次仍未落定即活锁征兆");
                  curr = index.find_tag(&want_k).expect("样本键必须可寻址");
                }
              }
            }
          }
          // 出守卫即归还纪元钉；样本转入 done 队列供静默点终检取证
          done.lock().unwrap().push((want_k, want_v));
          assert!(Instant::now() < deadline, "读线程主循环 10 秒硬时限");
        }
      }));
    }

    for h in wh {
      h.join().expect("写线程零 panic");
    }
    stop.store(true, Relaxed);
    for h in rh {
      h.join().expect("读线程零 panic（10 秒硬时限内收敛）");
    }

    // 静默点：泵尽一切待关闭并收割延迟动作（此后环上不再有任何在途读者）
    for _ in 0..256 {
      rc.pump_close_barrier(&index, None);
      epoch.drain();
      if !epoch.has_pending_drain() {
        break;
      }
    }

    // 静默点终检：全部样本键（读线程已消费 + 残留）自槽位走链，命中即值必须
    // 逐位相等，Gone 即假 NOTFOUND 违例
    let mut samples_final = take(&mut *done.lock().unwrap());
    samples_final.append(&mut *samples.lock().unwrap());
    assert!(!samples_final.is_empty(), "压测必须真实投放过抽样键");
    for (k, v) in &samples_final {
      let Some(mut curr) = index.find_tag(k) else {
        continue; // 槽位不在域（理论不可达），主日志域合法
      };
      loop {
        if !is_read_cache(curr) {
          break; // 链落主日志段（已清洗），合法终态
        }
        match rc.with_record(curr, |rk, rv| {
          if rk == k.as_slice() {
            assert_eq!(
              rv,
              v.as_slice(),
              "静默点撕裂现形：键命中但值字节与编码推导失配"
            );
            Some(true)
          } else {
            Some(false)
          }
        }) {
          RcVisit::Found(true) => break,
          RcVisit::Found(false) => {
            let prev = rc.prev_address_of(curr).expect("窗口内记录必可取前驱");
            curr = prev;
          }
          RcVisit::Next(prev) => {
            if prev == 0 {
              break;
            }
            curr = prev;
          }
          RcVisit::Gone => {
            false_gone.fetch_add(1, Relaxed);
            break;
          }
        }
      }
    }
  });

  assert_eq!(
    tears.load(Relaxed),
    0,
    "纪元屏障下受保护直读零撕裂：任何命中键值与编码推导期望失配均在此计数"
  );
  assert_eq!(
    false_gone.load(Relaxed),
    0,
    "静默点零假 NOTFOUND（走链 Gone 违例）"
  );
  assert!(
    guard_hits.load(Relaxed) > 0,
    "压测必须真实发生守卫内纪元保护直读命中"
  );
  assert!(
    wrap_events.load(Relaxed) > 50,
    "压测必须真实发生多轮回绕驱逐（观测 {wrap_events:?} 次 head 推进）"
  );
  assert!(
    rc.closed_until_address() > 0,
    "静默点必须已有回绕驱逐完成关闭"
  );
  assert!(
    rc.closed_until_address() <= rc.head_address() && rc.head_address() <= rc.tail_address(),
    "静默点水位序 closed <= head <= tail 必须成立"
  );
  assert!(!epoch.has_pending_drain(), "静默点延迟关闭队列必须收割干净");
}
