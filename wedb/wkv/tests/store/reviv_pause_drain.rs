//! 复活强同步暂停纪元排空屏障测试
//!
//! 对标 C# Tsavorite.cs:PauseRevivification 的「挂起计数 → BumpCurrentEpoch →
//! 事件等待在途写者退出」协议（rust 门面 wkv/src/store/reviv_host.rs，wreviv
//! pause 契约见 wreviv/src/pool.rs:227-236「强同步暂停需配合上层 epoch 排空」）。
//! 断言口径全部由协议/纪元语义推导，逐条注明依据。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/test.recordops/RevivificationTests.cs（自研暂停排空改良）

use std::{
  sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
  },
  thread::spawn,
  time::Duration,
};

use aok::{OK, Void};
use compio::{
  runtime::{Runtime, spawn as spawn_task},
  time::sleep,
};
use log::info;
use wbase::align::DEFAULT_SECTOR_SIZE;

use crate::support::{HashIndexTestOps, config, open_store};

/// 强同步暂停必须等待旧纪元在途复活写者排空：写者持入场纪元完成真实链内原位复活
/// 覆写后仍滞留临界区（对标 C#「已进入临界区、可能已拿到复活槽位正在执行原位覆写」
/// 的在途工作线程形态），`pause_revivification` 在此期间不得返回；写者退出纪元后
/// 屏障达成，守卫 Drop 恢复，恢复后复活分配重新可用，嵌套暂停/恢复计数口径不变。
#[test]
fn test_pause_revivification_epoch_drain_barrier() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let env = open_store(
      "reviv_pause_drain.db",
      config(1024, DEFAULT_SECTOR_SIZE, 16)?.with_revivification(true),
    )?;
    let store = env.store;
    // 等长值：墓碑帧容量与松弛填充前置恒成立，命中即链内原地复活（reviv.rs 双门用例同法）
    let v: &[u8] = b"pausedrain_val1";
    let k: &[u8] = b"pausedrain_key";

    // 1. 悬置「索引指向可变区墓碑」的链首，供写者在旧纪元内做真实原位复活覆写
    let session = store.new_session()?;
    let phys = session.session_string_key(k);
    let tomb = session.append_record(&phys, v, 0, true).await?;
    store.index.load().insert(&phys, tomb)?;
    drop(session);

    // 2. 在途写者：独立 OS 线程（跨线程保护不被屏障入口的同线程自钉解除触及，
    //    正是 wepoch refresh_thread_protected_entries 语义边界所排除、C# Wait
    //    必须等待的「其他已进入临界区的工作线程」形态）
    let (tx_ready, rx_ready) = mpsc::channel::<(u64, u64)>();
    let (tx_release, rx_release) = mpsc::channel::<()>();
    let store_w = Arc::clone(&store);
    let writer = spawn(move || -> aok::Result<(u64, u64)> {
      let rt = Runtime::new()?;
      rt.block_on(async move {
        let s = store_w.new_session()?;
        // 会话写入口同一纪元屏障（WedbStore::barrier_enter，与 upsert 内部
        // enter_gated 重入复合）：外层守卫存续期内该参与者恒钉住入场纪元
        let g = store_w.barrier_enter(&s.participant);
        let held = g.protected_epoch();
        let addr = s.upsert(k, v).await?;
        tx_ready.send((held, addr)).expect("写者上报通道不得断开");
        // 在途滞留：收到释放信号前仍处于临界区（对标 C# 在途覆写未结束的工作线程）
        rx_release.recv().expect("写者应收到释放信号");
        drop(g);
        Ok((held, addr))
      })
    });
    let (e_w, addr_reviv) = rx_ready.recv().expect("写者应进入在途态");
    assert_eq!(
      addr_reviv, tomb,
      "前置条件：写者已在纪元 {e_w} 内实际完成链内原位复活覆写（地址不变）"
    );
    assert!(
      !store.epoch.is_safe_to_reclaim(e_w),
      "前置条件：写者钉住纪元 {e_w}（safe_to_reclaim 未越过入场纪元）"
    );

    // 3. 屏障等待：done 在 pause_revivification 返回时置位
    let done = Arc::new(AtomicBool::new(false));
    let hold = Arc::new(AtomicBool::new(false));
    let resumed = Arc::new(AtomicBool::new(false));
    let store_p = Arc::clone(&store);
    let (done_p, hold_p, resumed_p) = (Arc::clone(&done), Arc::clone(&hold), Arc::clone(&resumed));
    let pause = spawn_task(async move {
      let guard = store_p
        .pause_revivification(Some(Duration::from_secs(10)))
        .await;
      done_p.store(true, Ordering::SeqCst);
      // 持守卫至主侧指令：验证「返回后、恢复前」守卫持有窗
      while !hold_p.load(Ordering::SeqCst) {
        sleep(Duration::from_millis(2)).await;
      }
      drop(guard);
      resumed_p.store(true, Ordering::SeqCst);
    });
    for _ in 0..20 {
      sleep(Duration::from_millis(10)).await;
      assert!(
        !done.load(Ordering::SeqCst),
        "纪元排空屏障失效：写者仍钉住旧纪元 {e_w}，pause_revivification 不得返回\
         （C# Tsavorite.cs:139 pauseRevivEvent.Wait 语义）"
      );
    }
    assert!(
      !store.reviv_pool.is_enabled(),
      "排空等待期间挂起计数须已生效（新复活分配冻结先于排空完成，对标 C# pause 先于 bump）"
    );
    assert!(
      !store.epoch.is_safe_to_reclaim(e_w),
      "写者仍钉住纪元 {e_w}，屏障未达成"
    );

    // 4. 释放写者 → 屏障达成
    tx_release.send(()).expect("释放信号送达写者");
    for _ in 0..500 {
      if done.load(Ordering::SeqCst) {
        break;
      }
      sleep(Duration::from_millis(10)).await;
    }
    assert!(
      done.load(Ordering::SeqCst),
      "写者退出纪元后 is_safe_to_reclaim 重扫必为真，屏障须达成（对标 C# 事件 Set 后 Wait 返回）"
    );
    assert!(
      store.epoch.is_safe_to_reclaim(e_w),
      "屏障返回后旧纪元 {e_w} 应无在途写者（等待谓词即 is_safe_to_reclaim，单调不回退）"
    );
    assert!(
      !store.reviv_pool.is_enabled(),
      "守卫未 Drop ⇒ 挂起计数保持暂停"
    );

    // 5. 守卫 Drop 恢复 + 恢复后复活分配重新可用
    hold.store(true, Ordering::SeqCst);
    for _ in 0..500 {
      if resumed.load(Ordering::SeqCst) {
        break;
      }
      sleep(Duration::from_millis(10)).await;
    }
    assert!(resumed.load(Ordering::SeqCst), "守卫 Drop 即恢复计数");
    assert!(
      store.reviv_pool.is_enabled(),
      "恢复后 is_enabled 复原（对标 C# ResumeRevivification，MigrationDriver.cs:224 finally）"
    );
    pause.await.expect("暂停任务不得 panic/取消");
    writer.join().expect("写者线程不得 panic")?;

    // 恢复后链内复活臂重新可用：独立干净键的墓碑原位复用、Tail 零推进（reviv.rs
    // 双门用例正断言同法——沿用被写者改写过的键会落入原位更新臂，测不到复活门）
    let kr: &[u8] = b"pausedrain_key2";
    let session2 = store.new_session()?;
    let phys2 = session2.session_string_key(kr);
    let tomb2 = session2.append_record(&phys2, v, 0, true).await?;
    store.index.load().insert(&phys2, tomb2)?;
    assert!(
      tomb2 >= store.min_revivifiable_address(),
      "前置条件：恢复用墓碑须落在复活窗口内"
    );
    let tail_before = store.hlog.tail_address();
    let addr2 = session2.upsert(kr, v).await?;
    assert_eq!(addr2, tomb2, "恢复后链内复活臂复用墓碑槽位，地址原地不变");
    assert_eq!(
      store.hlog.tail_address(),
      tail_before,
      "链内原地复活不推进 TailAddress"
    );
    assert_eq!(session2.read(kr).await?, Some(v.to_vec()));
    drop(session2);

    // 6. 嵌套 pause/resume 计数口径不变：两次暂停需两次等量恢复
    //    （wreviv/src/pool.rs:pause「可重入（嵌套暂停需等量 resume 才恢复）」）
    let g1 = store.pause_revivification(None).await;
    assert!(!store.reviv_pool.is_enabled(), "首次暂停置位挂起计数");
    let g2 = store.pause_revivification(None).await;
    drop(g1);
    assert!(
      !store.reviv_pool.is_enabled(),
      "嵌套暂停未等量恢复前不得提前解冻"
    );
    drop(g2);
    assert!(store.reviv_pool.is_enabled(), "等量恢复后解冻");
    assert!(
      store.epoch.current_epoch() > e_w,
      "每次强同步暂停至少推进一次纪元（对标 C# BumpCurrentEpoch 前置）"
    );

    info!(
      "复活强同步暂停纪元排空屏障（对标 C# PauseRevivification BumpCurrentEpoch+Wait 协议）验证通过"
    );
    OK
  })
}
