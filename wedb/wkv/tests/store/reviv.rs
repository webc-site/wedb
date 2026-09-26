//! 空间复活与读缓存测试：revivification 槽位复用、copy_reads_to_tail 晋升、ReadCache 挂载与脱钩。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/test.recordops/RevivificationTests.cs

use std::{sync::atomic::Ordering, thread::spawn};

use aok::{OK, Void};
use compio::runtime::{Runtime, spawn as spawn_task};
use log::info;
use wbase::{addr::is_read_cache, align::DEFAULT_SECTOR_SIZE};
use windex::{HashBucketEntry, HashIndex};
use wkv::StoreResult;
use wrecord::{HEADER_SIZE, record_size};

use crate::support::{HashIndexTestOps, config, open_store, slot_in_pool};

/// 对标 Garnet Tsavorite TryCopyToTail / CopyReadsToTail ——
/// 落盘冷数据读取后自动晋升至 Tail 活跃区
///
/// 验证 store 级 `copy_reads_to_tail`（对标 C# `kvSettings.ReadCopyOptions =
/// new(AllImmutable, MainLog)`，GarnetServerOptions.cs:899-900）开启后，首次读取从磁盘
/// 回填并自动晋升至 Tail，后续对该键的再次读取直接命中内存直读（`try_read_sync` 成功），
/// 避免二次磁盘 I/O；关闭时读取结果一致但 Tail 零推进（两条方向都断言，防回归）。
#[compio::test]
async fn test_copy_reads_to_tail_from_disk() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let cold_k = b"cold_key_for_copy_to_tail";
  let cold_v = b"cold_payload_initial_disk_data";

  for (enable, name) in [(true, "crtt_disk_on.db"), (false, "crtt_disk_off.db")] {
    let env = open_store(
      name,
      config(1024, page_size, 16)?.with_copy_reads_to_tail(enable),
    )?;
    let store = env.store;
    let session = store.new_session()?;

    let addr_init = session.upsert(cold_k, cold_v).await?;
    assert!(addr_init < page_size as u64);

    // 填充跨越第 0 页
    let pad = vec![b'P'; 3000];
    session.upsert(b"pad_k1", &pad).await?;
    let addr_pad = session.upsert(b"pad_k2", &pad).await?;
    assert!(addr_pad >= page_size as u64);

    // 刷盘并驱逐第 0 页至磁盘（shift_head 内部先把只读线推到同值，故只调一次）
    store.flush_all().await?;
    store.shift_head_address(page_size as u64);

    assert!(store.hlog.is_on_disk(addr_init));
    assert!(!store.hlog.is_in_memory(addr_init));

    let tail_before = store.hlog.tail_address();
    let val = session.read(cold_k).await?;
    assert_eq!(val, Some(cold_v.to_vec()), "两态下冷读结果必须一致");

    if enable {
      // 关键断言：此时该记录已被原子晋升到 Tail 内存中！
      assert!(
        store.hlog.tail_address() > tail_before,
        "开启时磁盘冷读必须追加晋升回 Tail"
      );
      // 再次读取时，纯同步内存探针直接精准命中，返回 StoreResult::Success(val)，完全绕过磁盘！
      let mem_hit = session.try_read_sync(cold_k, |v| v.to_vec())?;
      assert_eq!(
        mem_hit,
        StoreResult::Success(cold_v.to_vec()),
        "晋升后必须 100% 命中 DRAM 同步内存直读"
      );
    } else {
      assert_eq!(
        store.hlog.tail_address(),
        tail_before,
        "关闭时冷读零写放大，Tail 绝不得推进"
      );
      // 依然无法命中内存（必须回退到磁盘）
      assert_eq!(
        session.try_read_sync(cold_k, |_| ())?,
        StoreResult::RecordOnDisk,
        "关闭时二次读仍须回退磁盘候选"
      );
    }
  }

  info!("对照 C# Garnet TryCopyToTail 冷读自动晋升至 Tail 活跃区验证通过");
  OK
}

/// copy_reads_to_tail 的「内存不可变区命中」臂（验证 CopyFromImmutable
/// 在 `CopyTo == MainLog` 下走 ConditionalCopyToTail(wantIO:false) 的语义）
///
/// 修复前该臂全缺：`--copy-reads-to-tail` 开、read-cache 关（Garnet 主用法）时，
/// 命中内存不可变区 [head, safe_read_only) 的记录不回 Tail，只有磁盘冷读那一段对齐。
/// 本用例断言：开时不可变区命中即同步晋升 Tail（Tail 推进 + 索引改指新地址 +
/// 二次读命中可变区且不再重复晋升）；关时 Tail 零推进。
#[compio::test]
async fn test_copy_reads_to_tail_from_immutable_region() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let cold_k = b"immutable_key_for_copy_to_tail";
  let cold_v = b"immutable_payload_disk_data";

  for (enable, name) in [
    (true, "crtt_immutable_on.db"),
    (false, "crtt_immutable_off.db"),
  ] {
    let env = open_store(
      name,
      config(1024, page_size, 16)?.with_copy_reads_to_tail(enable),
    )?;
    let store = env.store;
    let session = store.new_session()?;

    let addr_init = session.upsert(cold_k, cold_v).await?;
    // 填充跨越第 0 页，令 addr_init 落入 Tail 之前的冷内存段
    let pad = vec![b'P'; 3000];
    session.upsert(b"pad_i1", &pad).await?;
    let addr_pad = session.upsert(b"pad_i2", &pad).await?;
    assert!(addr_pad >= page_size as u64);

    // 只刷盘、不驱逐：刷盘内核「先封后刷」把 read_only / safe_read_only 一并
    // 封印至刷盘上界，head 不动，故 addr_init 恰好落在内存不可变区内
    store.flush_all().await?;
    assert!(
      store.hlog.is_in_memory(addr_init),
      "前置条件：记录必须仍驻留内存（未驱逐）"
    );
    assert!(
      addr_init >= store.hlog.head_address() && addr_init < store.hlog.safe_read_only_address(),
      "前置条件：记录必须落在不可变区 [head, safe_read_only)"
    );

    let tail_before = store.hlog.tail_address();
    assert_eq!(session.read(cold_k).await?, Some(cold_v.to_vec()));

    if enable {
      let tail_after = store.hlog.tail_address();
      assert!(
        tail_after > tail_before,
        "不可变区命中必须同步晋升回 Tail（对标 wantIO:false 最佳努力）"
      );
      // 索引必须改指新地址（新帧位于可变区 [tail_before, tail_after) 内）
      let phys = session.session_string_key(cold_k);
      let mounted = store.index.load().lookup_vec(&phys);
      assert_eq!(mounted.len(), 1, "晋升挂载后该键在索引中唯指新地址");
      let new_addr = mounted[0];
      assert!(
        new_addr >= tail_before && new_addr < tail_after && new_addr != addr_init,
        "索引新地址 {new_addr:#x} 必须落在本次晋升推进出的 Tail 区间内，而非原不可变槽位"
      );
      // 二次读命中可变区：不得再触发晋升（目的地只服务不可变区）
      assert_eq!(
        session.try_read_sync(cold_k, |v| v.to_vec())?,
        StoreResult::Success(cold_v.to_vec()),
        "晋升后必须 100% 命中 DRAM 同步内存直读"
      );
      assert_eq!(
        store.hlog.tail_address(),
        tail_after,
        "可变区命中不得重复晋升（写放大必须收敛）"
      );
    } else {
      assert_eq!(
        store.hlog.tail_address(),
        tail_before,
        "关闭时不可变区命中零写放大，Tail 绝不得推进"
      );
    }
  }

  info!("对照 C# CopyFromImmutable 不可变区命中同步晋升 Tail 验证通过");
  OK
}

/// 对标 Garnet Tsavorite 空间复活机制 —— In-Chain Revivification 与 FreeRecordPool 槽位回收复用
#[compio::test]
async fn test_revivification() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let env = open_store(
    "reviv.db",
    config(1024, page_size, 16)?.with_revivification(true),
  )?;
  let store = env.store;
  let session = store.new_session()?;

  let k1 = b"reviv_key1";
  let v1 = b"original_value_01";
  let addr1 = session.upsert(k1, v1).await?;

  // 1. 测试空间脱钩与 FreeRecordPool 槽位回收：
  // 删除 k1，由于 k1 在内存可变区且无前驱版本，触发 HandleRecordElision 直接从索引脱钩并放入 reviv_pool
  let deleted = session.delete(k1).await?;
  assert!(deleted, "单记录删除成功");
  assert_eq!(session.read(k1).await?, None);

  // 验证复活池成功回收了槽位
  assert!(
    store.reviv_pool.put_count.load(Ordering::Relaxed) >= 1,
    "删除的槽位必须成功归还入 FreeRecordPool"
  );

  // 2. 测试分配拦截与槽位复用 (BlockAllocate.cs TryTakeFreeRecord):
  // 写入一个新 key2（大小相仿），优先从 FreeRecordPool 借出刚刚回收的 addr1 槽位！
  let curr_tail = store.hlog.tail_address();
  let k2 = b"reviv_key2";
  let v2 = b"reused_value__02";
  let addr2 = session.upsert(k2, v2).await?;

  assert_eq!(
    addr2, addr1,
    "新记录必须精准复用被删除槽位 addr1，零多余内存开销"
  );
  assert_eq!(
    store.hlog.tail_address(),
    curr_tail,
    "从复活池分配记录时 TailAddress 绝对不推进"
  );

  // 验证读取新 key2 内容准确无误
  assert_eq!(session.read(k2).await?, Some(v2.to_vec()));

  // 3. 测试链内原地复活 (In-Chain Revivification):
  // 强制写入一条带有墓碑的前驱记录（物理层直调统一携带会话前缀物理键）
  let k3 = b"reviv_key3";
  let v3_old = b"chain_val_001";
  let phys_k3 = session.session_string_key(k3);
  let addr3 = session.append_record(&phys_k3, v3_old, 0, true).await?;
  store.index.load().insert(&phys_k3, addr3)?;

  let tail_before_reviv = store.hlog.tail_address();
  let v3_new = b"chain_val_002";
  let addr3_reviv = session.upsert(k3, v3_new).await?;

  assert_eq!(
    addr3_reviv, addr3,
    "墓碑记录在长度匹配时必须在链内原地复活，地址保持不变"
  );
  assert_eq!(
    store.hlog.tail_address(),
    tail_before_reviv,
    "链内原地复活不推进 TailAddress"
  );
  assert_eq!(session.read(k3).await?, Some(v3_new.to_vec()));

  info!("对照 C# Garnet 空间复活与 FreeRecordPool 回收复用验证通过");
  OK
}

/// Record Elision 归池尺寸回归（r3 验证轮）：收缩更新 → elide → 复用 → 扫描闭环
///
/// 对标 C# FreeRecordPool 以 AllocatedSize（含 FillerWords 松弛填充）归还槽位的口径：
/// 1. 原位收缩更新令记录头产生 filler 松弛，物理帧大于逻辑记录尺寸且帧边界恒定；
/// 2. Record Elision 删除必须以 physical_size（含 filler）整帧归还复活池；
/// 3. 复用槽位整帧覆写（富余折算 pad / filler）后，日志扫描按物理帧边界精确步进；
/// 4. 若以逻辑尺寸归池，复用仅覆盖逻辑长度，帧尾遗留孤儿 filler 字节（旧值残片），
///    扫描器在帧中段解析出乱步头部——解析报错或整页跳过，漏扫其后全部记录。
#[compio::test]
async fn test_elide_physical_size_reuse_scan() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let env = open_store(
    "elide_phys.db",
    config(1024, page_size, 16)?.with_revivification(true),
  )?;
  let store = env.store;
  let session = store.new_session()?;

  // 1. 写入后原位收缩：富余（含隐式对齐填充）折算为 filler，物理帧恒定为对齐逻辑尺寸
  let k = b"elide_shrink_key";
  let v1 = vec![b'A'; 200];
  let addr_a = session.upsert(k, &v1).await?;
  let v2 = vec![b'B'; 40];
  let addr_shrunk = session.upsert(k, &v2).await?;
  assert_eq!(
    addr_shrunk, addr_a,
    "收缩更新必须原位完成，逻辑地址保持不变"
  );

  let phys_k = session.session_string_key(k);
  let frame = record_size(phys_k.len(), v1.len());

  // 2. 哨兵记录紧贴帧尾追加：证明物理帧含 filler 恒定，未被收缩压缩
  let sent_k = b"elide_sentinel_key";
  let sent_v = b"sentinel_after_frame";
  let addr_sent = session.upsert(sent_k, sent_v).await?;
  assert_eq!(
    addr_sent,
    addr_a + frame as u64,
    "收缩后物理帧必须恒定（filler 吸纳富余），后续记录严格贴合帧尾追加"
  );

  // 3. Record Elision：无前驱 + 可变区 + 双开关显式开启 → 脱钩并整帧归池
  assert!(session.delete(k).await?);
  assert_eq!(session.read(k).await?, None);
  assert!(
    store.reviv_pool.put_count.load(Ordering::Relaxed) >= 1,
    "elide 删除必须归还槽位入复活池"
  );

  // 归池注册尺寸必须等于物理足印（含 filler）：修复前此处注册逻辑长度
  // （HEADER + key + 40），中等尺寸（介于逻辑长度与物理足印之间）的 take 将错失槽位
  let registered = store
    .reviv_pool
    .bins
    .iter()
    .flat_map(|bin| bin.slots.iter())
    .find(|slot| !slot.is_empty() && slot.address() == addr_a)
    .map(|slot| slot.size());
  assert_eq!(
    registered,
    Some(frame as u32),
    "elide 归池必须以 physical_size（含 filler）整帧注册，而非逻辑长度"
  );

  // 4. 复用闭环：新键分配精准命中回收槽位，Tail 零推进。
  // v3 刻意取中等尺寸（HEADER+key2+100 > 逻辑长度 HEADER+key+40）：只有按物理足印
  // 注册，take 才可能命中该槽位；按逻辑长度注册时本次 take 必然落空退化为尾部追加
  let tail_before_reuse = store.hlog.tail_address();
  let k2 = b"elide_reuse_key";
  let v3 = vec![b'C'; 100];
  let addr_reuse = session.upsert(k2, &v3).await?;
  assert_eq!(addr_reuse, addr_a, "新记录必须复用 elide 归还的槽位");
  assert_eq!(
    store.hlog.tail_address(),
    tail_before_reuse,
    "复用槽位时 TailAddress 绝对不推进"
  );
  assert_eq!(session.read(k2).await?, Some(v3.clone()));

  // 5. 帧尾再追加一条记录，扫描必须按物理帧边界精确步进，一个不漏
  let k3 = b"elide_tail_key";
  let v4 = b"tail_after_reuse";
  let addr_tail = session.upsert(k3, v4).await?;

  let begin = store.hlog.begin_address();
  let tail = store.hlog.tail_address();
  let mut scan = store.hlog.scan_iter(begin, tail);
  let mut seen = Vec::new();
  while let Some((addr, rec)) = scan.next().await? {
    seen.push((addr, rec.key()?.to_vec(), rec.value()?.to_vec()));
  }

  let phys_k2 = session.session_string_key(k2).as_slice().to_vec();
  let phys_k3 = session.session_string_key(k3).as_slice().to_vec();
  let phys_sent = session.session_string_key(sent_k).as_slice().to_vec();
  // 松弛吸纳语义（对标 C# SetFiller / InitializeForRevivification）：k2 复用槽位
  // 的富余（frame − 逻辑 136 ≤ MAX_FILLER_BYTES）全数吸纳为记录内填充——帧内不切
  // Pad 空洞，k3 必然落回尾部追加贴哨兵帧尾（若悄悄退回切洞形态，两断言必红）
  let k2_rec = record_size(phys_k2.len(), v3.len());
  let k2_frame = store
    .hlog
    .read_record(addr_a)
    .await?
    .header()?
    .physical_size();
  assert_eq!(k2_frame, frame, "复用记录物理帧必须整帧覆盖槽位");
  assert_eq!(
    store
      .hlog
      .read_record(addr_a)
      .await?
      .header()?
      .filler_bytes(),
    frame - k2_rec,
    "富余必须全数转为记录内松弛填充"
  );
  assert_eq!(
    addr_tail,
    addr_sent + record_size(phys_sent.len(), sent_v.len()) as u64,
    "k3 必须尾部追加恰贴哨兵帧尾（无空洞可复活），addr_tail={addr_tail:#x}"
  );
  assert_eq!(
    seen,
    vec![
      (addr_a, phys_k2, v3.clone()),
      (addr_sent, phys_sent, sent_v.to_vec()),
      (addr_tail, phys_k3, v4.to_vec()),
    ],
    "扫描必须按物理帧边界精确步进：复用槽整帧覆写（含吸纳填充）+ 哨兵 + 尾部记录一个不漏"
  );

  info!("Record Elision 归池尺寸（physical_size 含 filler）复用闭环回归通过");
  OK
}

/// 复活暂停门控与复活区间比例门控（对标 C# RevivificationManager 暂停生命周期
/// 与 GetMinRevivifiableAddress 比例窗口）
#[compio::test]
async fn test_revivification_pause_and_fraction_gate() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;

  // 1. 暂停门控：pause 期间 take 不外借槽位，分配回退尾部追加；resume 后恢复复用
  {
    let env = open_store(
      "reviv_pause.db",
      config(1024, page_size, 16)?.with_revivification(true),
    )?;
    let store = env.store;
    let session = store.new_session()?;

    let k1 = b"pause_gate_key1";
    let v1 = b"pause_gate_val01";
    let addr1 = session.upsert(k1, v1).await?;
    assert!(session.delete(k1).await?);

    store.reviv_pool.pause();
    assert!(
      !store.reviv_pool.is_enabled(),
      "暂停后 is_enabled 必须为 false"
    );

    let tail_before = store.hlog.tail_address();
    let k2 = b"pause_gate_key2";
    let addr2 = session.upsert(k2, v1).await?;
    assert!(
      addr2 != addr1 && addr2 >= tail_before,
      "暂停期间复活分配必须回退为尾部追加"
    );

    store.reviv_pool.resume();
    assert!(
      store.reviv_pool.is_enabled(),
      "恢复后 is_enabled 必须为 true"
    );

    // 恢复后删除 k2 归池，同尺寸新键必须重新命中池中回收槽位（addr1/k1 与
    // addr2/k2 均在池中，任一命中皆证明复活分配已恢复，Tail 零推进）
    assert!(session.delete(k2).await?);
    let tail_before = store.hlog.tail_address();
    let k3 = b"pause_gate_key3";
    let addr3 = session.upsert(k3, v1).await?;
    assert!(
      (addr3 == addr1 || addr3 == addr2) && addr3 < tail_before,
      "恢复后必须重新复用回收槽位而非尾部追加"
    );
  }

  // 2. 比例门控：fraction 收窄后，远离尾部的回收槽位低于复活下限，不再被复用
  {
    let env = open_store(
      "reviv_fraction.db",
      config(1024, page_size, 16)?
        .with_revivification(true)
        .with_revivifiable_fraction(0.5)?,
    )?;
    let store = env.store;
    let session = store.new_session()?;

    let k1 = b"fraction_gate_key1";
    let v1 = b"fraction_gate_val01";
    let addr1 = session.upsert(k1, v1).await?;
    assert!(session.delete(k1).await?);

    // 撑开可变区窗口：未推进只读线时 read_only = 0，window = tail，
    // 复活下限 = tail × (1 - 0.5) = tail / 2，低位槽位 addr1 被排除在窗口外
    let pad = vec![b'P'; 128];
    for i in 0..8 {
      session
        .upsert(format!("fraction_pad_{i}").as_bytes(), &pad)
        .await?;
    }
    assert!(
      addr1 * 2 < store.hlog.tail_address(),
      "前置条件：回收槽位必须落在复活窗口之外"
    );

    let tail_before = store.hlog.tail_address();
    let k2 = b"fraction_gate_key2";
    let addr2 = session.upsert(k2, v1).await?;
    assert!(
      addr2 != addr1 && addr2 >= tail_before,
      "窗口外的回收槽位必须被比例门控过滤，分配回退尾部追加"
    );
  }

  info!("对照 C# RevivificationManager 暂停生命周期与比例门控验证通过");
  OK
}

/// 链内原地复活的双门（对标 C# InternalUpsert.cs:125 / InternalRMW.cs:126
/// `RevivificationManager.IsEnabled && LogicalAddress >= GetMinRevivifiableAddress()`）
///
/// 修复前链内复活臂只判 `config.enable_revivification` 单门，后果两条：
/// a) `--reviv-fraction` 对链内复活零效力（只有池取受限，C# 同一谓词管两路）；
/// b) 迁移暂停窗口（RevivPauseGuard）内新到的写仍可在链中部原地复活墓碑。
/// 本用例对两门各写正反双断言：负断言要求地址推进 Tail（落回尾部追加），
/// 正断言要求地址原地不变且 Tail 零推进——任一谓词掉线即红。
#[compio::test]
async fn test_revivification_in_chain_dual_gate() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  // 等长值：墓碑帧容量与松弛填充前置恒成立，断言只反映双门本身
  let v = b"chaingate_val1";

  // 1. 暂停臂：pause 挡死链内复活，resume 后同臂恢复
  {
    let env = open_store(
      "reviv_chain_pause.db",
      config(1024, page_size, 16)?.with_revivification(true),
    )?;
    let store = env.store;
    let session = store.new_session()?;

    // 悬置「索引指向可变区墓碑」的链首（与 test_revivification 步骤 3 同法）
    let k = b"chain_pause_key";
    let phys = session.session_string_key(k);
    let tomb = session.append_record(&phys, v, 0, true).await?;
    store.index.load().insert(&phys, tomb)?;

    store.reviv_pool.pause();
    // 前置自查：墓碑必须已通过比例门（cur >= 下限），本次落回尾部才只可能由
    // 暂停门产生，负断言不因另一道门同向挡路而变得空洞
    assert!(
      tomb >= store.min_revivifiable_address(),
      "前置条件：墓碑须落在复活窗口内，令暂停臂成为唯一变量"
    );
    let tail_before = store.hlog.tail_address();
    let after = session.upsert(k, v).await?;
    assert_ne!(
      after, tomb,
      "暂停期间链内复活臂必须关闭，不得原地复用墓碑槽位"
    );
    assert!(
      after >= tail_before,
      "暂停期间的 upsert 须落回尾部追加（对标 C# goto CreateNewRecord）"
    );
    assert_eq!(session.read(k).await?, Some(v.to_vec()), "落链后值可读");

    store.reviv_pool.resume();
    assert!(store.reviv_pool.is_enabled(), "resume 后启用谓词复原");
    // 正断言另起一键：键 k 的链首槽位已随暂停臂落回尾部改写为 Active 记录，
    // 对 Active 头等长命中会先被「原位更新」臂服务（同样零 Tail 推进、同样地址
    // 不变），根本走不到复活臂——沿用 k 断言会测出一个与复活门无关的假绿。
    // 换用独立桶位的干净键，链首恒为墓碑，本步红/绿只可能来自复活双门
    let kr = b"chain_resume_key";
    let phys_r = session.session_string_key(kr);
    let tomb_r = session.append_record(&phys_r, v, 0, true).await?;
    store.index.load().insert(&phys_r, tomb_r)?;
    assert!(
      tomb_r >= store.min_revivifiable_address(),
      "前置条件：恢复用墓碑须落在复活窗口内，令启用门成为唯一变量"
    );
    let tail_before2 = store.hlog.tail_address();
    let after2 = session.upsert(kr, v).await?;
    assert_eq!(
      after2, tomb_r,
      "恢复后链内复活臂必须复用墓碑槽位，地址原地不变"
    );
    assert_eq!(
      store.hlog.tail_address(),
      tail_before2,
      "链内原地复活不推进 TailAddress"
    );
    assert_eq!(session.read(kr).await?, Some(v.to_vec()));
  }

  // 2. 比例臂：fraction 收窄落在复活窗口外的墓碑不得原地复活
  {
    let env = open_store(
      "reviv_chain_fraction.db",
      config(1024, page_size, 16)?
        .with_revivification(true)
        .with_revivifiable_fraction(0.5)?,
    )?;
    let store = env.store;
    let session = store.new_session()?;

    let k = b"chain_frac_key";
    let phys = session.session_string_key(k);
    let tomb = session.append_record(&phys, v, 0, true).await?;
    store.index.load().insert(&phys, tomb)?;

    // 未推进只读线时 read_only = 0，窗口 = tail，下限 = tail/2：低位墓碑被排除
    let pad = vec![b'P'; 256];
    for i in 0..8 {
      session
        .upsert(format!("chain_frac_pad_{i}").as_bytes(), &pad)
        .await?;
    }
    assert!(
      tomb * 2 < store.hlog.tail_address(),
      "前置条件：墓碑必须落在复活窗口之外"
    );

    let tail_before = store.hlog.tail_address();
    let after = session.upsert(k, v).await?;
    assert_ne!(
      after, tomb,
      "--reviv-fraction 必须同样约束链内复活，不得只挡池取"
    );
    assert!(after >= tail_before, "窗口外墓碑的 upsert 须落回尾部追加");
  }

  // 3. 比例臂对照正断言：默认 fraction = 1.0（下限退化为 read_only）时，
  //    同样的低位墓碑仍须原地复活——证明上一步的红确由比例门产生
  {
    let env = open_store(
      "reviv_chain_fraction_full.db",
      config(1024, page_size, 16)?.with_revivification(true),
    )?;
    let store = env.store;
    let session = store.new_session()?;

    let k = b"chain_full_key";
    let phys = session.session_string_key(k);
    let tomb = session.append_record(&phys, v, 0, true).await?;
    store.index.load().insert(&phys, tomb)?;

    let pad = vec![b'P'; 256];
    for i in 0..8 {
      session
        .upsert(format!("chain_full_pad_{i}").as_bytes(), &pad)
        .await?;
    }
    assert!(
      tomb * 2 < store.hlog.tail_address(),
      "前置条件：与比例臂同位形（墓碑远离 Tail）"
    );

    let after = session.upsert(k, v).await?;
    assert_eq!(
      after, tomb,
      "fraction 默认 1.0 时全可变区皆可复活，地址必须原地不变"
    );
    assert_eq!(session.read(k).await?, Some(v.to_vec()));
  }

  info!("对照 C# 链内原地复活双门（IsEnabled + GetMinRevivifiableAddress）验证通过");
  OK
}

/// 复活槽位富余处置闭环（对标 C# RecordDataHeader.SetFiller 松弛留存 +
/// ComputeFillerWordsOrSplit → SplitOverflowingFiller 超限分裂后经
/// TryTransferToFreeList 归还空闲列表，RecordDataHeader.cs:462/:509、Helpers.cs:124）
///
/// 修复前 revivify_record_at 一律就地切 Pad 而切出块无人回收，复活槽位已从哈希链
/// 脱钩，Pad 成为物理页内孤儿死内存。本用例确定性断言新语义的完整链路：
/// 1. 64B 整帧归池 → 40B 需求（检索窗 = 64B/128B 两档）复用该槽位；
/// 2. 富余 24B ≤ MAX_FILLER_BYTES（2040）→ 全数吸纳为记录内松弛填充，帧内无空洞
///    （复活槽整段保留给本记录，供后续原位增长吸纳）；
/// 3. 4096B 整帧复用 2048B 需求：富余 2048B 超上限 → 分裂，记录保留 64 词 = 512B
///    填充，切出 1536B 剩余块以 (槽位地址+2560, 1536) 坐标准确回池；
/// 4. 回池切出块可再被匹配尺寸取出（复用链路打通，非死内存）；
/// 5. 跨桶上限回归：当期+相邻档皆空时，微小记录不得掠夺 512B 桶大帧，回退尾部追加。
#[compio::test]
async fn test_revivify_pad_cut_transferred_to_pool() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let env = open_store(
    "reviv_pad.db",
    config(1024, page_size, 16)?.with_revivification(true),
  )?;
  let store = env.store;
  let session = store.new_session()?;

  // 1. 构造恰为 64B 整帧的 k1（值长由目标帧反推：帧 = HEADER + 键 + 值，天然整词）
  let k1 = b"padk1";
  let phys1 = session.session_string_key(k1);
  let v1_len = 64 - HEADER_SIZE - phys1.len();
  let addr1 = session.upsert(k1, &vec![b'A'; v1_len]).await?;
  assert_eq!(record_size(phys1.len(), v1_len), 64);

  // 2. elide 删除整帧归池（池中唯一槽位 = (addr1, 64)，64B 桶）
  assert!(session.delete(k1).await?);
  let slot_of = |addr: u64| -> Option<u32> {
    store
      .reviv_pool
      .bins
      .iter()
      .flat_map(|bin| bin.slots.iter())
      .find(|s| !s.is_empty() && s.address() == addr)
      .map(|s| s.size())
  };
  assert_eq!(slot_of(addr1), Some(64), "前置条件：64B 整帧已归池");

  // 3. 甲臂：k2 需求恰为 40B（窗 = 64B 当期档 + 128B 相邻档），复用 addr1 槽位；
  //    富余 64 − 40 = 24B ≤ 2040 → 全数吸纳为记录内松弛填充，不切空洞
  let k2 = b"padk2";
  let phys2 = session.session_string_key(k2);
  let v2_len = 40 - HEADER_SIZE - phys2.len();
  let v2 = vec![b'B'; v2_len];
  let addr2 = session.upsert(k2, &v2).await?;
  assert_eq!(addr2, addr1, "40B 需求必须命中窗内 64B 槽位");
  assert_eq!(
    slot_of(addr1 + 40),
    None,
    "富余在表示上限内严禁切出 Pad 块（对标 C# MaxFillerWords 吸纳语义，旧切分形态已废）"
  );
  let out2 = store.hlog.read_record(addr2).await?;
  assert_eq!(out2.value()?, v2, "复活记录可读");
  assert_eq!(
    out2.header()?.filler_bytes(),
    24,
    "富余 24B 全数转为松弛填充"
  );
  assert_eq!(out2.header()?.physical_size(), 64, "物理帧精确覆盖整个槽位");

  // 4. 乙臂：4096B 整帧（恰一物理页，追加必落页首）→ 归池 → 2048B 需求复用
  //    （窗 = 2048B 当期档 + 4096B 相邻档）；富余 2048B > 2040 → 分裂：
  //    记录内保留 512B 填充（对位 RecordSplitRetainFillerWords = 64 词），
  //    切出 1536B 剩余块以 (addr_big + 2048 + 512, 1536) 坐标回池
  let kb = b"padbig";
  let physb = session.session_string_key(kb);
  let vb_len = 4096 - HEADER_SIZE - physb.len();
  let addr_big = session.upsert(kb, &vec![b'G'; vb_len]).await?;
  assert_eq!(
    addr_big % page_size as u64,
    0,
    "前置条件：整页记录必落页首，槽位不跨页"
  );
  assert!(session.delete(kb).await?);
  assert_eq!(slot_of(addr_big), Some(4096), "前置条件：4096B 整帧已归池");

  let km = b"padmid";
  let physm = session.session_string_key(km);
  let vm_len = 2048 - HEADER_SIZE - physm.len();
  let vm = vec![b'M'; vm_len];
  let addr_mid = session.upsert(km, &vm).await?;
  assert_eq!(addr_mid, addr_big, "2048B 需求必须命中窗内 4096B 槽位");
  assert_eq!(
    slot_of(addr_big + 2560),
    Some(1536),
    "富余超上限须分裂并以 (addr+记录+保留段, 切出尺寸) 归入复活池（修复前此处必红：孤儿死内存）"
  );
  // 归池密封不变量：Pad 切出块入池前必须置 SEALED（对标 C# TryTransferToFreeList
  // 的 IsClosed 前置断言，Helpers.cs:128；修复前 Pad 头 info 字恒 0，此处必红）
  assert_eq!(
    store
      .hlog
      .peek_memory_header(addr_big + 2560)?
      .map(|h| h.is_closed()),
    Some(true),
    "复活分裂切出的 Pad 剩余块归池时必须处于 SEALED（is_closed）态"
  );
  let outm = store.hlog.read_record(addr_mid).await?;
  assert_eq!(outm.value()?, vm);
  assert_eq!(
    outm.header()?.filler_bytes(),
    512,
    "分裂后记录内保留 64 词填充"
  );
  assert_eq!(
    outm.header()?.physical_size(),
    2560,
    "记录物理 = 逻辑 2048 + 保留 512"
  );

  // 5. 回池切出块可再被取出：1200B 需求（窗 = 2048B/4096B 档，切出块按 1536B
  //    归入 2048B 档）命中复用链路，切出块成功脱池
  assert_eq!(
    store.reviv_pool.take(1200, 0, 0),
    Some((addr_big + 2560, 1536)),
    "切出块回池后必须可再被取出复用"
  );
  assert_eq!(slot_of(addr_big + 2560), None, "取出后切出块槽位已清空");

  // 6. 跨桶上限回归：池中仅剩 512B 桶大帧，微小需求（当期+相邻档均 < 512B 档）
  //    绝不可掠夺它，必须回退尾部追加（Tail 推进）
  let k3 = b"padk3";
  let phys3 = session.session_string_key(k3);
  let v3_len = 504 - HEADER_SIZE - phys3.len();
  let addr3 = session.upsert(k3, &vec![b'C'; v3_len]).await?;
  assert!(session.delete(k3).await?, "前置条件：504B 帧归池");
  assert_eq!(slot_of(addr3), Some(504));
  let tail_before = store.hlog.tail_address();
  let k4 = b"padk4";
  let phys4 = session.session_string_key(k4);
  let v4 = b"x";
  assert!(
    record_size(phys4.len(), v4.len()) <= 64,
    "前置条件：k4 检索窗上界须低于 512B 档"
  );
  let addr4 = session.upsert(k4, v4).await?;
  assert_ne!(addr4, addr3, "微小记录严禁跨多档掠夺 512B 桶大帧");
  assert!(
    addr4 >= tail_before,
    "窗外有帧时微小分配必须落回尾部追加（对标 C# TryTake 失败即走分配器）"
  );

  info!("复活富余吸纳/分裂切出块归池闭环与跨桶上限回归验证通过");
  OK
}

/// 对标 Garnet Tsavorite ReadCache 独立只读非脏页内存日志 —— 冷读回填晋升与原子脱钩
///
/// 验证点：
/// 1. 启用 `with_read_cache(true)`；
/// 2. 写入冷数据落盘并驱逐出 DRAM（HLog is_on_disk）；
/// 3. 从磁盘读取冷数据后，数据自动挂载入纯内存 ReadCache（零持久化写放大，HLog 尾部不推进）；
/// 4. 后续读请求 100% 命中 DRAM 同步内存直读快路径；
/// 5. 后续对该 Key 写入新值（Upsert），单次 CAS 原子脱钩旧 ReadCache 链，新记录写入主日志 Tail；
/// 6. 删除 Key 时，原子脱钩 ReadCache 链并追加墓碑记录。
#[compio::test]
async fn test_read_cache_promotion_and_detach() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let num_pages = 8;
  let env = open_store(
    "read_cache.db",
    config(64, page_size, num_pages)?.with_read_cache(true),
  )?;
  let store = env.store;
  let session = store.new_session()?;

  assert!(store.read_cache.is_enabled);

  let key = b"read_cache_key_01";
  let val = b"read_cache_val_01";

  let init_addr = session.upsert(key, val).await?;
  assert!(init_addr < page_size as u64);

  // 填充数据使得 tail 自然跨越第 0 页进入第 1 页
  let pad = vec![b'P'; 3000];
  session.upsert(b"pad_k1", &pad).await?;
  let addr_pad = session.upsert(b"pad_k2", &pad).await?;
  assert!(addr_pad >= page_size as u64);

  // 刷盘并驱逐第 0 页到磁盘
  store.flush_all().await?;
  store.shift_read_only_address(page_size as u64);
  store.shift_head_address(page_size as u64);

  assert!(store.hlog.is_on_disk(init_addr));
  assert!(!store.hlog.is_in_memory(init_addr));

  let tail_before_read = store.hlog.tail_address();

  // 首次冷读：从磁盘读取
  let res = session.read(key).await?;
  assert_eq!(res, Some(val.to_vec()));

  // 验证 HLog Tail 并没有因为冷读而追加（区别于 copy_reads_to_tail 的写放大）
  assert_eq!(
    store.hlog.tail_address(),
    tail_before_read,
    "ReadCache 冷读回填纯在 DRAM 分配器中完成，HLog 尾部零写放大"
  );

  // 验证后续内存探针 100% 命中 ReadCache！
  let mem_hit = session.try_read_sync(key, |v| v.to_vec())?;
  assert_eq!(
    mem_hit,
    StoreResult::Success(val.to_vec()),
    "冷读完成后必须 100% 命中 ReadCache 纯内存直读"
  );

  // 测试写操作（Upsert）：原子脱钩 ReadCache
  let new_val = b"read_cache_val_updated";
  let new_addr = session.upsert(key, new_val).await?;
  assert!(
    new_addr >= store.hlog.head_address(),
    "新记录必须写入 HLog 内存活跃区"
  );

  // 读取验证新值
  let updated_hit = session.try_read_sync(key, |v| v.to_vec())?;
  assert_eq!(updated_hit, StoreResult::Success(new_val.to_vec()));

  // 测试删除操作（Delete）：delete_raw 直调物理键，须携带会话前缀
  let phys_key = session.session_string_key(key);
  let deleted = session.delete_raw(&phys_key).await?;
  assert!(deleted, "删除应返回 true");
  let after_del = session.try_read_sync(key, |v| v.to_vec())?;
  assert_eq!(
    after_del,
    StoreResult::NotFound,
    "删除后内存探针确认不存在或为墓碑"
  );

  info!("对照 C# Garnet ReadCache 独立只读非脏页内存日志验证通过");
  Ok(())
}

/// 验证 ReadCache::cleanse_page 在遇到空键记录时能够正确清洗，且不会由于 key_len == 0 中断同页后续记录的清洗
#[test]
fn test_read_cache_cleanse_page_empty_key_continuity() -> Void {
  use std::sync::Arc;

  use wepoch::LightEpoch;
  use windex::HashIndex;
  use wkv::ReadCache;

  let page_size = 1024;
  let num_pages = 2;
  let rc = Arc::new(ReadCache::new(
    page_size,
    num_pages,
    true,
    Arc::new(LightEpoch::new(8)),
  )?);
  let index = Arc::new(HashIndex::new(64)?);

  // 1. 在第 0 页写入空键记录与同页后续记录
  // （先挂主日志地址条目，append 内部 CAS 挂载 RC 地址，对标 hei.TryCAS）
  index.insert(b"", 0x100)?;
  let rc_addr1 = rc
    .append(b"", b"val_empty_key", &index, 0)
    .expect("追加空键缓存记录成功");
  assert!(is_read_cache(rc_addr1));

  index.insert(b"k_after", 0x200)?;
  let rc_addr2 = rc
    .append(b"k_after", b"val_after", &index, 0)
    .expect("追加同页后续缓存记录成功");
  assert!(is_read_cache(rc_addr2));

  // 2. 写入足够大数据跨入第 1 页
  let pad_val = vec![b'P'; 800];
  index.insert(b"fill_p0", 0x300)?;
  rc.append(b"fill_p0", &pad_val, &index, 0);

  // 3. 跨入第 2 页，触发环形回绕驱逐第 0 页并调用 cleanse_page(0)
  index.insert(b"fill_p1", 0x400)?;
  rc.append(b"fill_p1", &pad_val, &index, 0);
  index.insert(b"turn_p2", 0x500)?;
  rc.append(b"turn_p2", &pad_val, &index, 0);
  // 回绕换页武装拍已把旧页关闭序列挂入纪元延迟队列：本线程处于出借期安全点，
  // 泵入注册即同步收割执行（cleanse → ClosedUntil 发布 → 清页换装）
  rc.pump_close_barrier(&index, None);

  // 4. 断言：空键与同页后续记录的索引指针均必须被恢复为主日志地址（0x100 与 0x200），证明没有被提前截断
  let l1 = index.lookup_vec(b"");
  assert_eq!(l1, vec![0x100], "空键索引槽位必须恢复指向主日志地址");

  let l2 = index.lookup_vec(b"k_after");
  assert_eq!(
    l2,
    vec![0x200],
    "同页后续记录索引槽位必须同样恢复指向主日志地址"
  );

  OK
}

#[compio::test]
async fn test_elide_unconditionally_removes_chain_head() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let env = open_store(
    "test_elide_unconditionally.db",
    config(1024, page_size, 16)?.with_revivification(true),
  )?;
  let store = &env.store;
  let session = env.store.new_session()?;

  let k = b"elide_del_key";
  let v1 = b"value_1";

  // 写入一个键
  let _ = session.upsert(k, v1).await?;

  // 删除该键（触发 elide）
  let _ = session.delete(k).await?;

  // 断言槽位已空，或者放入了 reviv_pool
  // elide 删除必须归还槽位入复活池
  assert!(
    store.reviv_pool.put_count.load(Ordering::Relaxed) >= 1,
    "elide unconditionally removes chain head and puts into reviv_pool"
  );
  Ok(())
}

/// upsert 链首脱钩密封回归（对标 C# InternalUpsert.cs:369 CreateNewRecordUpsert
/// CAS 成功后的落笔顺序：`srcLogRecord.InfoRef.SealAndInvalidate()` →
/// `TryTransferToFreeList`，后者前置断言 IsClosed）
///
/// 修复前 elide 分支直接归还复活池不落密封：在途无锁读者回溯时无法感知
/// Closed 态，槽位一经复活方出池原位覆写即读出新旧混合的撕裂内容。本用例
/// 走确定性单线程路径（加长值击穿上原位更新容量门 → 尾部追加 + CAS 脱钩
/// 唯一前驱为 0 的链首）：断言旧槽位已注册进复活池且记录头处于 SEALED 态。
#[compio::test]
async fn test_upsert_elide_seals_pooled_slot() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let env = open_store(
    "upsert_elide_seal.db",
    config(1024, page_size, 16)?.with_revivification(true),
  )?;
  let store = env.store;
  let session = store.new_session()?;

  let k = b"upsert_elide_seal_key";
  let v1 = vec![b'A'; 64];
  let v2 = vec![b'B'; 300];
  let addr_old = session.upsert(k, &v1).await?;
  // 加长更新原位失败 → 尾部追加并 CAS 脱钩旧链首（旧链首 prev=0 可安全脱钩）
  let addr_new = session.upsert(k, &v2).await?;
  assert_ne!(addr_new, addr_old, "加长更新必须落尾追加脱钩路径，地址换新");
  assert_eq!(session.read(k).await?, Some(v2));

  // 关键断言：脱钩入池的旧槽位必须先被原子密封（SEALED+INVALID）
  assert_eq!(
    store
      .hlog
      .with_memory_record(addr_old, |rec| Ok(rec.is_closed()))?,
    Some(true),
    "elide 脱钩槽位进入复活池时记录头必须处于 SEALED 态"
  );

  // 并且已整帧注册进复活池等待安全复用
  let registered = store
    .reviv_pool
    .bins
    .iter()
    .flat_map(|bin| bin.slots.iter())
    .any(|slot| !slot.is_empty() && slot.address() == addr_old);
  assert!(registered, "elide 脱钩槽位必须注册进复活池");

  info!("对照 C# SealAndInvalidate → TryTransferToFreeList 脱钩密封顺序验证通过");
  Ok(())
}

/// copy-to-tail CAS 落败帧归池密封判别（对标 C# TryCopyToTail.cs:67
/// `stackCtx.SetNewRecordInvalid` 先行 → `OnDispose` 回收 → TryTransferToFreeList
/// 的 IsClosed 前置断言，Helpers.cs:128）
///
/// 同一运行时两任务并发冷读同一磁盘记录（copy_reads_to_tail 开、ReadCache 关，
/// 磁盘回填晋升臂经 `allocate_record` 单点 + `cas_mount_copied_frame` 挂载）：
/// 单线程 FIFO 轮询下，两任务的索引探测都发生在任何一方磁盘 I/O 恢复挂载之前，
/// 同持旧候选走磁盘回填。第一读完成「追加 + CAS 挂载」晋升；第二读 I/O 返回后
/// 经内存复检（对标 C# ContinuePendingRead（ContinuePending.cs）:174 "If we
/// already found the record in memory, we're done." 与 :175 `!memoryRecord.IsSet`
/// 前置——内存已有该键记录则免 copy to tail）直接消费晋升新帧返回，故真实并发下不再天然产生败帧。
/// CAS 落败场景改由判别 2 以并发写者视角手工构造（持旧 old_addr 执行
/// `cas_mount_copied_frame` 必落败）。修复前败帧未密封即入池（is_closed 恒
/// false），槽位一经出池原位覆写，在途读者即读出撕裂内容。判别断言：
/// 1. Tail 恰推进一帧：第一读真实追加晋升（非真空测试），第二读复检免追加；
/// 2. 索引只挂载一帧，手工败帧确在复活池且记录头 SEALED（is_closed == true）；
/// 3. 胜帧为存活记录，不得被误密封。
#[compio::test]
async fn test_cas_lost_copy_frame_sealed_in_pool() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let env = open_store(
    "cas_lost_seal.db",
    config(1024, page_size, 16)?
      .with_revivification(true)
      .with_copy_reads_to_tail(true),
  )?;
  let store = env.store;
  let session = store.new_session()?;

  let k = b"cas_lost_key";
  let v = b"cas_lost_val_payload";
  let addr_disk = session.upsert(k, v).await?;
  // 填充跨第 0 页 → 刷盘驱逐：目标记录落磁盘区，读必然走磁盘回填晋升臂
  let pad = vec![b'P'; 3000];
  session.upsert(b"cas_pad1", &pad).await?;
  session.upsert(b"cas_pad2", &pad).await?;
  store.flush_all().await?;
  store.shift_head_address(page_size as u64);
  assert!(
    store.hlog.is_on_disk(addr_disk),
    "前置条件：目标记录必须已驱逐落盘"
  );
  let phys_k = session.session_string_key(k).as_slice().to_vec();
  drop(session);

  let frame = record_size(phys_k.len(), v.len()) as u64;
  let tail_before = store.hlog.tail_address();
  // 同一运行时并发两冷读：两任务先后越过索引探测、同持旧候选挂起于磁盘 I/O
  let store1 = store.clone();
  let store2 = store.clone();
  let h1 = spawn_task(async move { store1.new_session()?.read(k).await });
  let h2 = spawn_task(async move { store2.new_session()?.read(k).await });
  let r1 = h1.await.expect("冷读任务一不得 panic/取消")?;
  let r2 = h2.await.expect("冷读任务二不得 panic/取消")?;
  assert_eq!(r1, Some(v.to_vec()), "两读结果必须一致");
  assert_eq!(r2, Some(v.to_vec()));

  // 判别 1：第一读完成尾部追加晋升（Tail 推进一帧）；第二读 I/O 返回后内存复检
  // 命中可变区新帧免重复追加（严格对标 C# ContinuePendingRead（ContinuePending.cs）
  // :174 "If we already found the record in memory, we're done."、:175
  // `!memoryRecord.IsSet` 才走 ConditionalCopyToTail，及 :146-154 可变区命中分支
  // "we don't need to copy to tail or readcache, so return success"）
  assert_eq!(
    store.hlog.tail_address(),
    tail_before + frame,
    "第一读完成晋升后，第二读复检命中内存免重复追加（对标 C# ContinuePendingRead）"
  );
  let mounted = store.index.load().lookup_vec(&phys_k);
  assert_eq!(mounted.len(), 1, "晋升挂载后该键在索引中唯指一帧");
  let winner = mounted[0];
  assert_eq!(
    winner,
    tail_before,
    "胜帧必须落在首次晋升追加区间，winner={winner:#x} tail={:#x}",
    store.hlog.tail_address()
  );

  // 判别 2：构造 CAS 挂载竞争——模拟并发写者以旧 old_addr 执行 cas_mount_copied_frame，
  // CAS 必落败，且败帧确在复活池、归池时记录头已密封（严格对标 C# TryCopyToTail.cs:67
  // stackCtx.SetNewRecordInvalid → OnDispose(InitialWriterCASFailed) → TryTransferToFreeList）
  let (loser, _) = store.hlog.append(&phys_k, v, addr_disk, false)?;
  assert_eq!(store.hlog.tail_address(), tail_before + 2 * frame);
  let cas_session = store.new_session()?;
  let cas_ok = cas_session.cas_mount_copied_frame(&phys_k, addr_disk, loser, frame as u32);
  assert!(
    !cas_ok,
    "old_addr (addr_disk) 已被胜帧取代，本次 CAS 挂载必落败"
  );

  // 败帧确在复活池，且归池时记录头已密封
  assert!(
    slot_in_pool(&store, loser),
    "CAS 落败帧必须归还复活池，loser={loser:#x}"
  );
  assert_eq!(
    store
      .hlog
      .with_memory_record(loser, |rec| Ok(rec.is_closed()))?,
    Some(true),
    "CAS 落败帧归池前必须置 SEALED（is_closed），防出池覆写后读者撕裂读"
  );

  // 判别 3：胜帧为链上存活记录，不得被误密封
  assert_eq!(
    store
      .hlog
      .with_memory_record(winner, |rec| Ok(rec.is_closed()))?,
    Some(false),
    "挂载成功的晋升帧必须保持存活未密封态"
  );

  info!("对照 C# TryCopyToTail CAS 败帧归池密封（SetNewRecordInvalid → IsClosed）验证通过");
  Ok(())
}

/// 复活池密封不变式压测（elide 与 RetryAlloc::discard 并发撕裂回归）
///
/// 热键被多线程以三种自校验图案（长度 + 填充字节互异）交替覆写：CAS 败方
/// 暂存分配经复用判定失败落入 RetryAlloc::discard，胜方走脱钩入池——两条
/// 归池路径同场加压。撕裂类断言无确定性单线程复现窗，按不变式 + 多轮压测
/// 的确定性形态落笔：
/// 1. 每轮读结果必须是 None 或合法图案之一（任一新旧混合字节即红）；
/// 2. 压停后复活池中仍在内存可判读的注册槽位，记录头必须全部 SEALED
///    （对标 C# TryTransferToFreeList 的 IsClosed 前置断言）；
/// 3. put_count 非零，确认归池回收路径确实被加压触达。
#[test]
fn test_reviv_pool_seal_invariant_under_contention() -> Void {
  const FILLERS: [u8; 3] = *b"ABC";
  const SIZES: [usize; 3] = [64, 192, 300];
  const THREADS: usize = 4;
  const ROUNDS: usize = 200;

  // 合法图案：长度与填充字节必属写入集之一（混合残片判否）
  fn valid_pattern(v: &[u8]) -> bool {
    SIZES
      .iter()
      .zip(FILLERS)
      .any(|(&len, f)| v.len() == len && v.iter().all(|&b| b == f))
  }

  let rt = Runtime::new()?;
  rt.block_on(async {
    let page_size = DEFAULT_SECTOR_SIZE;
    let env = open_store(
      "reviv_seal_stress.db",
      config(1024, page_size, 128)?.with_revivification(true),
    )?;
    let store = env.store;

    let hot = b"reviv_seal_hot_key";
    // 首发链首 prev=0；其后所有成功写入均脱钩唯一前驱为 0 的旧链首
    // （或等/缩位原位改写），链条恒不积累内存内前驱，压测只聚焦归池密封
    let session = store.new_session()?;
    session.upsert(hot, &vec![FILLERS[0]; SIZES[0]]).await?;
    drop(session);

    let mut writers = Vec::with_capacity(THREADS);
    for t in 0..THREADS {
      let store_w = store.clone();
      writers.push(spawn(move || -> aok::Result<()> {
        let rt = Runtime::new()?;
        rt.block_on(async {
          let s = store_w.new_session()?;
          for i in 0..ROUNDS {
            // 线程间图案序列错位，保证同键并发异尺寸覆写（击穿上原位门 → 追加 + CAS 竞争）
            let idx = (i + t * 2) % 3;
            let v = vec![FILLERS[idx]; SIZES[idx]];
            s.upsert(hot, &v).await?;
            if let Some(got) = s.read(hot).await? {
              assert!(
                valid_pattern(&got),
                "读到撕裂/越界内容：{:?}",
                &got[..got.len().min(8)]
              );
            }
          }
          OK
        })
      }));
    }
    for w in writers {
      w.join().expect("写线程不得 panic")?;
    }

    // 不变式 2：池中仍在内存可判读的注册槽位必须全部处于 SEALED 态。
    // 判读走纯头窥视口（不做键值尺寸校验）：归池形态既含真实记录帧（elide /
    // discard / CAS 败帧），也含复活分裂切出的 Pad 块，密封位只关 RecordInfo
    // 单字，两类槽位同受「归池必密封」不变量约束
    let mut resident_checked = 0usize;
    for bin in store.reviv_pool.bins.iter() {
      for slot in bin.slots.iter() {
        if slot.is_empty() {
          continue;
        }
        if let Some(header) = store.hlog.peek_memory_header(slot.address())? {
          assert!(
            header.is_closed(),
            "复活池槽位 {:#x} 未密封即入池，在途读者可撕裂读",
            slot.address()
          );
          resident_checked += 1;
        }
      }
    }

    // 不变式 3：归池回收确被触达
    assert!(
      store.reviv_pool.put_count.load(Ordering::Relaxed) >= 1,
      "压测必须实际触达 elide/discard 归池路径"
    );
    info!(
      "复活池密封不变式压测通过：{} 个在内存池槽位恒 SEALED",
      resident_checked
    );
    OK
  })
}

/// 复活分配的链首地址下界（对标 C# BlockAllocate.cs:59-62 minRevivAddress 链首抬升 +
/// FreeRecordPool.cs:TryTake 低于 minAddress 仅跳过不清退），杜绝哈希碰撞链逆向成环
///
/// 修复前 `try_allocate_or_append_record_sync` 仅向池传全局复活水位：池中低于目标
/// 哈希链首但仍在可变区的空闲槽位会被取出复活覆写，prev_address 指向高地址链首
/// （链逆向），高地址记录再脱钩复活即成环，读路径 trace_back_for_key_match 无限
/// 自旋。本用例确定性单线程真驱动（全走真实 upsert/delete 分配路径）：
/// 1. 低址 64B 整帧先脱钩归池（64B 档，与后续申请同窗）；
/// 2. 手工构造 Tag 碰撞链 victim ← twin（链首 = twin 高地址）；
/// 3. twin 带前驱更新：池绝不下借低于「链首 + 1」的槽位（落尾部追加），
///    且仅跳过不清退（槽位原样留池、drop/hit 计数零变动）——修复前此处必红（窃槽逆链）；
/// 4. 链上 prev_address 严格单调递减、地址严格递增、双键读取闭环；
/// 5. elided 反向臂：整链清空（链首前驱低于截断线）时下界回退全局水位，
///    低址槽位照常复活——证明下界只链条、绝不断池。
#[compio::test]
async fn test_reviv_take_respects_hash_chain_head_floor() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let env = open_store(
    "reviv_chain_floor.db",
    config(64, page_size, 16)?.with_revivification(true),
  )?;
  let store = env.store;
  let session = store.new_session()?;

  // 与 victim 同桶同 tag 的确定性碰撞键（物理键哈希口径，对标 collision_chain.rs）
  let victim = b"flrvictim";
  let phys_v = session.session_string_key(victim);
  let mask = (store.index.load().mask as u64) | (0x7fffu64 << HashBucketEntry::HASH_TAG_SHIFT);
  let target = HashIndex::hash_key(phys_v.as_slice()) & mask;
  let twin = {
    let mut i = 0u64;
    loop {
      let key = format!("flrtwin{i}");
      let phys = session.session_string_key(key.as_bytes());
      if HashIndex::hash_key(phys.as_slice()) & mask == target {
        break key;
      }
      i += 1;
    }
  };
  let phys_t = session.session_string_key(twin.as_bytes());

  // 1. 低址供体：64B 整帧归池（先建先删，地址恒低于后续一切链首）
  let donor = b"flrdonor";
  let phys_d = session.session_string_key(donor);
  let vd = vec![b'D'; 64 - HEADER_SIZE - phys_d.len()];
  assert_eq!(record_size(phys_d.len(), vd.len()), 64);
  let addr_d = session.upsert(donor, &vd).await?;
  assert!(session.delete(donor).await?);
  assert!(
    slot_in_pool(&store, addr_d),
    "前置条件：低址 64B 整帧已归池"
  );

  // 2. 手工构造碰撞链：victim 挂槽位 → twin upsert 链住 victim 并抢占链首
  //    （twin 申请下界 = 链首 victim + 1，供体低于下界必须被跳过——修复前此步即窃槽逆链）
  let vv = b"flrvictim-value!";
  let addr_v = session.append_record(&phys_v, vv, 0, false).await?;
  store.index.load().insert(&phys_v, addr_v)?;
  let vt1 = b"tt";
  let addr_t = session.upsert(twin.as_bytes(), vt1).await?;
  assert!(
    addr_t > addr_v && slot_in_pool(&store, addr_d),
    "twin 挂链分配绝不得下借低于链首的供体槽位"
  );

  // 3. twin 加长更新（链首存活带前驱）：申请下界 = 链首 + 1，池取必落空
  let vt2 = vec![b'T'; 64 - HEADER_SIZE - phys_t.len()];
  assert_eq!(
    record_size(phys_t.len(), vt2.len()),
    64,
    "新帧恰 64B 与供体同档"
  );
  assert!(
    record_size(phys_t.len(), vt1.len()) < 64,
    "前置条件：旧帧低于新帧，原位更新容量门必败，必落追加臂"
  );
  let stats_before = store.reviv_pool.stats();
  let tail_before = store.hlog.tail_address();
  let addr_t2 = session.upsert(twin.as_bytes(), &vt2).await?;

  assert_ne!(addr_t2, addr_d, "链首存活时绝不复活低于链首的池槽位");
  assert!(
    addr_t2 >= tail_before,
    "下界过滤后池取必落空，分配回退尾部追加（对标 C# TryTake 失败即走分配器）"
  );
  assert!(
    slot_in_pool(&store, addr_d),
    "低于本次下界但高于水位的槽位仅跳过让位，严禁就地清零淘汰"
  );
  let stats_after = store.reviv_pool.stats();
  assert_eq!(
    stats_after.drop_count, stats_before.drop_count,
    "跳过档零淘汰虚增"
  );
  assert_eq!(
    stats_after.hit_count, stats_before.hit_count,
    "本次池取必须落空"
  );

  // 4. 链完整性：prev_address 严格单调递减（高→低），地址严格递增，不成环不失联
  assert_eq!(
    store.hlog.read_record(addr_t2).await?.prev_address()?,
    addr_t
  );
  assert_eq!(
    store.hlog.read_record(addr_t).await?.prev_address()?,
    addr_v
  );
  assert_eq!(store.hlog.read_record(addr_v).await?.prev_address()?, 0);
  assert!(
    addr_v < addr_t && addr_t < addr_t2,
    "碰撞链地址必须严格单调，杜绝逆向指针"
  );
  assert_eq!(
    session.read(victim).await?,
    Some(vv.to_vec()),
    "被碰撞掩埋键读取闭环"
  );
  assert_eq!(
    session.read(twin.as_bytes()).await?,
    Some(vt2.clone()),
    "链首更新读取闭环"
  );

  // 5. elided 反向臂：整链清空（链首前驱 0 低于截断线）时下界回退全局水位，
  //    低址供体照常复活——证明链首下界只「链条」不「断池」
  let chain_k = b"flrchain";
  let phys_k = session.session_string_key(chain_k);
  let vk1 = b"kk";
  let vk2 = vec![b'K'; 64 - HEADER_SIZE - phys_k.len()];
  assert_eq!(
    record_size(phys_k.len(), vk2.len()),
    64,
    "新帧恰 64B 与供体同档"
  );
  assert!(
    record_size(phys_k.len(), vk1.len()) < 64,
    "前置条件：脱钩臂更新同样击穿上原位容量门，必落追加臂"
  );
  let addr_k1 = session.append_record(&phys_k, vk1, 0, false).await?;
  store.index.load().insert(&phys_k, addr_k1)?;
  assert!(
    addr_k1 > addr_t2 && slot_in_pool(&store, addr_d),
    "前置条件：供体仍留池"
  );
  let addr_k2 = session.upsert(chain_k, &vk2).await?;
  assert_eq!(
    addr_k2, addr_d,
    "整链清空（elided）复活不受链首门控，低址槽位照常脱池复用（对标 C# elideSourceRecord 臂不抬升）"
  );
  assert!(!slot_in_pool(&store, addr_d), "供体槽位已出池");
  assert!(slot_in_pool(&store, addr_k1), "被清空的旧链首整帧脱钩归池");
  assert_eq!(
    store.hlog.read_record(addr_d).await?.prev_address()?,
    0,
    "清空链复活记录前驱为旧链首的死前驱（0），无逆向指针"
  );
  assert_eq!(session.read(chain_k).await?, Some(vk2));
  assert_eq!(
    session.read(twin.as_bytes()).await?,
    Some(vt2),
    "碰撞链不受他链脱钩复活分配影响"
  );

  info!("对照 C# BlockAllocate 链首地址下界（minRevivAddress 抬升 / TryTake 仅跳过）成环回归通过");
  Ok(())
}

/// copy-to-tail 慢路径的复活池取收编（对标 C# TryCopyToTail.cs:33 以
/// `AllocateOptions{recycle=true}` 统一经 TryAllocateRecord 的 TryTakeFreeRecord 臂，
/// BlockAllocate.cs:57-82；C# UseFreeRecordPoolForCopyToTail 上游死字段、实际行为
/// 恒可池取）：冷数据删除走 `delete_raw_disk_slow` → `copy_record_to_tail` 内核时，
/// 墓碑帧必须优先落于预置池槽位——tail 不推进、供体脱池、索引 CAS 改指槽位、
/// prev 链挂住冷记录、复活槽地址恒高于候选链首（本例候选链首 = 冷记录低址）。
/// 修复前该臂直落纯尾部追加，池消费面收窄为快路径单臂。
#[compio::test]
async fn test_reviv_ctt_disk_delete_reuses_pool_slot() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let env = open_store(
    "reviv_ctt_disk_del.db",
    config(64, page_size, 16)?.with_revivification(true),
  )?;
  let store = env.store;
  let session = store.new_session()?;

  // 冷键（物理键与供体等长，帧尺寸口径一致）
  let cold = b"cttcoldk";
  let phys_c = session.session_string_key(cold);
  let cold_v = b"coldvalue";
  let cold_frame = record_size(phys_c.len(), cold_v.len());
  let addr_c = session.upsert(cold, cold_v).await?;

  // 跨入页 1 后冷却第 0 页：冷记录入磁盘；只读线停在 head，复活窗口 [ro, tail) 非空
  let pad = vec![b'P'; 3000];
  session.upsert(b"cttpadk1", &pad).await?;
  session.upsert(b"cttpadk2", &pad).await?;
  store.flush_all().await?;
  store.shift_head_address(page_size as u64);
  assert!(store.hlog.is_on_disk(addr_c));

  // 供体：冷却后写尾部（可复活窗口内），帧尺寸与冷记录同档，upsert+delete 整帧归池
  let donor = b"cttdonok";
  let phys_d = session.session_string_key(donor);
  let vd = vec![b'D'; cold_frame - HEADER_SIZE - phys_d.len()];
  assert_eq!(record_size(phys_d.len(), vd.len()), cold_frame);
  let addr_d = session.upsert(donor, &vd).await?;
  assert!(session.delete(donor).await?);
  assert!(slot_in_pool(&store, addr_d), "前置条件：供体整帧已归池");
  assert!(
    addr_d > addr_c,
    "前置条件：供体高于冷记录（链首下界自然满足）"
  );

  // 冷删：内存区探不到 → 降级 copy_record_to_tail 内核 → 池取复活墓碑帧
  let stats_before = store.reviv_pool.stats();
  let tail_before = store.hlog.tail_address();
  assert!(session.delete(cold).await?, "冷删确认生效");

  assert_eq!(
    store.hlog.tail_address(),
    tail_before,
    "池取复活墓碑帧绝不推进 tail（对标 C# CTT 槽位复用的日志空洞收敛收益）"
  );
  assert!(!slot_in_pool(&store, addr_d), "供体槽位必须被取出复活");
  assert!(
    addr_d > addr_c,
    "复活槽地址恒高于候选链首（链首下界 = 冷记录 + 1）"
  );
  let stats_after = store.reviv_pool.stats();
  assert_eq!(
    stats_after.hit_count,
    stats_before.hit_count + 1,
    "池取必须命中"
  );

  // 墓碑帧落于槽位：键匹配、墓碑标记、prev 链挂住冷记录，索引 CAS 已改指
  let rec = store.hlog.read_record(addr_d).await?;
  let rec = rec.as_record_ref()?;
  assert!(
    rec.matches_key(phys_c.as_slice()),
    "墓碑帧必须复活在供体槽位"
  );
  assert!(rec.is_tombstone());
  assert_eq!(rec.prev_address(), addr_c, "墓碑前驱必须挂住被删冷记录");
  assert_eq!(
    store.index.load().find_tag(phys_c.as_slice()),
    Some(addr_d),
    "索引必须 CAS 挂载到复活槽位"
  );
  assert_eq!(session.read(cold).await?, None, "删除语义闭环");

  info!("对照 C# TryCopyToTail 池取臂（冷删慢路径）验证通过");
  Ok(())
}

/// copy-to-tail 慢路径的链首下界拒借（对标 C# BlockAllocate.cs:57-62 minRevivAddress
/// 链首抬升在 CTT 臂的同一口径）：碰撞链 victim（磁盘冷）← twin（可变区链首），
/// 冷删 victim 时候选链首 = twin 槽位，低于「链首 + 1」的池内供体必须仅跳过让位
/// 不清退（hit/drop 计数零变动），墓碑回落尾部追加——杜绝复活槽低于旧链首致
/// prev 逆向成环。供体与链首同驻页 1 可变区，链首以 append_record 直插绕开池取，
/// 保证供体在 twin 挂链前不被快路径抢走。
#[compio::test]
async fn test_reviv_ctt_respects_chain_head_floor() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let env = open_store(
    "reviv_ctt_floor.db",
    config(64, page_size, 16)?.with_revivification(true),
  )?;
  let store = env.store;
  let session = store.new_session()?;

  // victim 冷键先落页 0
  let victim = b"cflvictim";
  let phys_v = session.session_string_key(victim);
  let vv = b"victimvalue00";
  let tomb_frame = record_size(phys_v.len(), 0);
  let addr_v = session.upsert(victim, vv).await?;

  // 跨入页 1 后冷却第 0 页：victim 入磁盘；只读线停在 head，复活窗口 [ro, tail) 非空
  let pad = vec![b'P'; 3000];
  session.upsert(b"cflpadk1", &pad).await?;
  session.upsert(b"cflpadk2", &pad).await?;
  store.flush_all().await?;
  store.shift_head_address(page_size as u64);
  assert!(store.hlog.is_on_disk(addr_v));

  // 供体：冷却后写尾部（可复活窗口内），帧恰高一档以内（16B 值），归池留用
  let donor = b"cfldonor1";
  let phys_d = session.session_string_key(donor);
  let vd = vec![b'D'; 16];
  assert!(record_size(phys_d.len(), vd.len()) > tomb_frame);
  let addr_d = session.upsert(donor, &vd).await?;
  assert!(session.delete(donor).await?);
  assert!(slot_in_pool(&store, addr_d), "前置条件：供体已归池");

  // 确定性碰撞键（同桶同 tag，对标 collision_chain.rs 搜索法）
  let mask = (store.index.load().mask as u64) | (0x7fffu64 << HashBucketEntry::HASH_TAG_SHIFT);
  let target = HashIndex::hash_key(phys_v.as_slice()) & mask;
  let twin = {
    let mut i = 0u64;
    loop {
      let key = format!("cfltwin{i}");
      let phys = session.session_string_key(key.as_bytes());
      if HashIndex::hash_key(phys.as_slice()) & mask == target {
        break key;
      }
      i += 1;
    }
  };
  let phys_t = session.session_string_key(twin.as_bytes());

  // twin 直插挂链（绕开池取，防供体被快路径抢走）：prev 指向 victim，索引链首改指 twin
  let tv = b"twinvalue0000";
  let addr_t = session
    .append_record(phys_t.as_slice(), tv, addr_v, false)
    .await?;
  assert!(
    store
      .index
      .load()
      .update_address(phys_v.as_slice(), addr_v, addr_t),
    "链首必须改指 twin"
  );
  assert!(addr_t > addr_d, "前置条件：供体低于链首（twin）");
  assert_eq!(
    session.read(victim).await?,
    Some(vv.to_vec()),
    "直插不得覆盖 victim 原值"
  );

  // 冷删 victim：候选链首 = twin → 下界 addr_t + 1 → 供体被拒借、仅跳过让位
  let stats_before = store.reviv_pool.stats();
  let tail_before = store.hlog.tail_address();
  assert!(session.delete(victim).await?, "冷删确认生效");

  assert_eq!(
    store.hlog.tail_address(),
    tail_before + tomb_frame as u64,
    "拒借后墓碑必须恰好落在尾部追加（tail_before 之上唯一一帧）"
  );
  assert!(
    slot_in_pool(&store, addr_d),
    "低于下界但高于水位的供体仅跳过让位，严禁就地清退"
  );
  let stats_after = store.reviv_pool.stats();
  assert_eq!(
    stats_after.hit_count, stats_before.hit_count,
    "拒借时池取必须落空"
  );
  assert_eq!(
    stats_after.drop_count, stats_before.drop_count,
    "跳过档零淘汰虚增"
  );

  // 墓碑帧 = tail_before 处唯一追加帧，prev 挂住冷记录；victim 确认删除、twin 存活
  let tomb_addr = tail_before;
  let rec = store.hlog.read_record(tomb_addr).await?;
  let rec = rec.as_record_ref()?;
  assert!(rec.matches_key(phys_v.as_slice()) && rec.is_tombstone());
  assert_eq!(
    rec.prev_address(),
    addr_t,
    "墓碑前驱 = 候选链首（main_head 单点语义），恒低于墓碑地址不成环"
  );
  assert_eq!(
    store.index.load().find_tag(phys_v.as_slice()),
    Some(tomb_addr),
    "索引链首 CAS 改指尾部墓碑"
  );
  assert_eq!(session.read(victim).await?, None);
  assert_eq!(session.read(twin.as_bytes()).await?, Some(tv.to_vec()));

  info!("对照 C# BlockAllocate 链首下界在 CTT 臂的拒借语义验证通过");
  Ok(())
}

/// 磁盘冷读回填臂的复活池取收编（对标 C# ContinuePendingConditionalCopyToTail
/// 磁盘回填臂统一经 TryCopyToTail 的 `AllocateOptions{recycle=true}`；即
/// CopyToTailTests.cs 的 ReadCopyTo.MainLog 场景加池预置）：`--copy-reads-to-tail`
/// 开启时冷读命中后晋升帧必须优先落于预置池槽位——tail 不推进、二次读内存直读
/// 命中、索引改指槽位；旁路写监听（notify=false）不产生 AOF 镜像。
#[compio::test]
async fn test_reviv_ctt_disk_read_backfill_reuses_pool_slot() -> Void {
  let page_size = DEFAULT_SECTOR_SIZE;
  let env = open_store(
    "reviv_ctt_backfill.db",
    config(64, page_size, 16)?
      .with_revivification(true)
      .with_copy_reads_to_tail(true),
  )?;
  let store = env.store;
  let session = store.new_session()?;

  let cold = b"cbfcoldk";
  let phys_c = session.session_string_key(cold);
  let cold_v = b"coldvalue";
  let cold_frame = record_size(phys_c.len(), cold_v.len());
  let addr_c = session.upsert(cold, cold_v).await?;

  let pad = vec![b'P'; 3000];
  session.upsert(b"cbfpadk1", &pad).await?;
  session.upsert(b"cbfpadk2", &pad).await?;
  store.flush_all().await?;
  store.shift_head_address(page_size as u64);
  assert!(store.hlog.is_on_disk(addr_c));

  // 供体：冷却后写尾部（可复活窗口内），帧尺寸与冷记录同档，upsert+delete 整帧归池
  let donor = b"cbfdonok";
  let phys_d = session.session_string_key(donor);
  let vd = vec![b'D'; cold_frame - HEADER_SIZE - phys_d.len()];
  assert_eq!(record_size(phys_d.len(), vd.len()), cold_frame);
  let addr_d = session.upsert(donor, &vd).await?;
  assert!(session.delete(donor).await?);
  assert!(slot_in_pool(&store, addr_d), "前置条件：供体整帧已归池");

  // 冷读回填：晋升帧复活在供体槽位
  let stats_before = store.reviv_pool.stats();
  let tail_before = store.hlog.tail_address();
  assert_eq!(
    session.read(cold).await?,
    Some(cold_v.to_vec()),
    "冷读必须返回正确值"
  );

  assert_eq!(
    store.hlog.tail_address(),
    tail_before,
    "池取复活晋升帧绝不推进 tail"
  );
  assert!(!slot_in_pool(&store, addr_d), "供体槽位必须被取出复活");
  let stats_after = store.reviv_pool.stats();
  assert_eq!(
    stats_after.hit_count,
    stats_before.hit_count + 1,
    "池取必须命中"
  );

  let rec = store.hlog.read_record(addr_d).await?;
  let rec = rec.as_record_ref()?;
  assert!(
    rec.matches_key(phys_c.as_slice()),
    "晋升帧必须复活在供体槽位"
  );
  assert!(!rec.is_tombstone());
  assert_eq!(rec.value(), cold_v, "晋升帧值必须与源一致");
  assert_eq!(
    store.index.load().find_tag(phys_c.as_slice()),
    Some(addr_d),
    "索引必须 CAS 挂载到复活槽位"
  );

  // 二次读：纯同步内存直读命中，不再触磁盘
  assert_eq!(
    session.try_read_sync(cold, |v| v.to_vec())?,
    StoreResult::Success(cold_v.to_vec()),
    "晋升后必须命中 DRAM 同步内存直读"
  );

  info!("对照 C# ContinuePendingConditionalCopyToTail 池取臂（磁盘回填）验证通过");
  Ok(())
}
