//! 空间复活与读缓存测试：revivification 槽位复用、copy_reads_to_tail 晋升、ReadCache 挂载与脱钩。

use std::sync::atomic::Ordering;

use aok::{OK, Void};
use compio::runtime::Runtime;
use log::info;
use wbase::{addr::is_read_cache, align::DEFAULT_SECTOR_SIZE};
use wkv::StoreResult;
use wrecord::record_size;

use crate::support::{HashIndexTestOps, config, open_store};

/// 对标 Garnet Tsavorite TryCopyToTail / CopyReadsToTail ——
/// 落盘冷数据读取后自动晋升至 Tail 活跃区
///
/// 验证 store 级 `copy_reads_to_tail`（对标 C# `kvSettings.ReadCopyOptions =
/// new(AllImmutable, MainLog)`，GarnetServerOptions.cs:899-900）开启后，首次读取从磁盘
/// 回填并自动晋升至 Tail，后续对该键的再次读取直接命中内存直读（`try_read_sync` 成功），
/// 避免二次磁盘 I/O；关闭时读取结果一致但 Tail 零推进（两条方向都断言，防回归）。
#[test]
fn test_copy_reads_to_tail_from_disk() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// copy_reads_to_tail 的「内存不可变区命中」臂（对标 C# InternalRead.cs:CopyFromImmutable
/// 在 `CopyTo == MainLog` 下走 ConditionalCopyToTail(wantIO:false)）
///
/// 修复前该臂全缺：`--copy-reads-to-tail` 开、read-cache 关（Garnet 主用法）时，
/// 命中内存不可变区 [head, safe_read_only) 的记录不回 Tail，只有磁盘冷读那一段对齐。
/// 本用例断言：开时不可变区命中即同步晋升 Tail（Tail 推进 + 索引改指新地址 +
/// 二次读命中可变区且不再重复晋升）；关时 Tail 零推进。
#[test]
fn test_copy_reads_to_tail_from_immutable_region() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 对标 Garnet Tsavorite 空间复活机制 —— In-Chain Revivification 与 FreeRecordPool 槽位回收复用
#[test]
fn test_revivification() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let page_size = DEFAULT_SECTOR_SIZE;
    let env = open_store(
      "reviv.db",
      config(1024, page_size, 16)?.with_revivification(true),
    )?;
    let store = env.store;
    let session = store.new_session()?;
    session.set_record_elision(true);

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
    aok::Result::<()>::Ok(())
  })?;

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
#[test]
fn test_elide_physical_size_reuse_scan() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let page_size = DEFAULT_SECTOR_SIZE;
    let env = open_store(
      "elide_phys.db",
      config(1024, page_size, 16)?.with_revivification(true),
    )?;
    let store = env.store;
    let session = store.new_session()?;
    session.set_record_elision(true);

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
    assert_eq!(
      seen,
      vec![
        (addr_a, phys_k2, v3.clone()),
        (addr_sent, phys_sent, sent_v.to_vec()),
        (addr_tail, phys_k3, v4.to_vec()),
      ],
      "扫描必须按物理帧边界精确步进：复用槽整帧覆写 + 哨兵 + 尾部记录一个不漏"
    );

    info!("Record Elision 归池尺寸（physical_size 含 filler）复用闭环回归通过");
    aok::Result::<()>::Ok(())
  })?;

  OK
}

/// 复活暂停门控与复活区间比例门控（对标 C# RevivificationManager 暂停生命周期
/// 与 GetMinRevivifiableAddress 比例窗口）
#[test]
fn test_revivification_pause_and_fraction_gate() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let page_size = DEFAULT_SECTOR_SIZE;

    // 1. 暂停门控：pause 期间 take 不外借槽位，分配回退尾部追加；resume 后恢复复用
    {
      let env = open_store(
        "reviv_pause.db",
        config(1024, page_size, 16)?.with_revivification(true),
      )?;
      let store = env.store;
      let session = store.new_session()?;
      session.set_record_elision(true);

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
      session.set_record_elision(true);

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
    aok::Result::<()>::Ok(())
  })?;

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
#[test]
fn test_revivification_in_chain_dual_gate() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
    aok::Result::<()>::Ok(())
  })?;

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
#[test]
fn test_read_cache_promotion_and_detach() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
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
    OK
  })
}

/// 验证 ReadCache::cleanse_page 在遇到空键记录时能够正确清洗，且不会由于 key_len == 0 中断同页后续记录的清洗
#[test]
fn test_read_cache_cleanse_page_empty_key_continuity() -> Void {
  use std::sync::Arc;

  use windex::HashIndex;
  use wkv::ReadCache;

  let page_size = 1024;
  let num_pages = 2;
  let rc = ReadCache::new(page_size, num_pages, true)?;
  let index = Arc::new(HashIndex::new(64)?);

  // 1. 在第 0 页写入空键记录与同页后续记录
  // （先挂主日志地址条目，append 内部 CAS 挂载 RC 地址，对标 hei.TryCAS）
  index.insert(b"", 0x100)?;
  let rc_addr1 = rc
    .append(b"", b"val_empty_key", 0x100, &index)
    .expect("追加空键缓存记录成功");
  assert!(is_read_cache(rc_addr1));

  index.insert(b"k_after", 0x200)?;
  let rc_addr2 = rc
    .append(b"k_after", b"val_after", 0x200, &index)
    .expect("追加同页后续缓存记录成功");
  assert!(is_read_cache(rc_addr2));

  // 2. 写入足够大数据跨入第 1 页
  let pad_val = vec![b'P'; 800];
  index.insert(b"fill_p0", 0x300)?;
  rc.append(b"fill_p0", &pad_val, 0x300, &index);

  // 3. 跨入第 2 页，触发环形回绕驱逐第 0 页并调用 cleanse_page(0)
  index.insert(b"fill_p1", 0x400)?;
  rc.append(b"fill_p1", &pad_val, 0x400, &index);
  index.insert(b"turn_p2", 0x500)?;
  rc.append(b"turn_p2", &pad_val, 0x500, &index);

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
