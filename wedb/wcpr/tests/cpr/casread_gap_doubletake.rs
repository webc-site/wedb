//! CAS 胜点与读点之间窗关交错：双承素材效果幂等回归
//! （票 whlog-verwindow-casread-gap-doubletake）
//!
//! 交错形态：追加者 tail CAS 胜点（窗口 W 开启中，addr >= floor_W）与
//! version_shift_word 读点之间（相邻语句纳秒域）窗关交错——end_version_shift
//! 先序落定，读点取关闭态，记录不带位落笔于本轮扫描区间、AOF 戳等于关窗
//! 保留版本 V_W。恢复自 W 时 undo 谓词（位 + addr 双门）因无位不剔，重插臂
//! 承接（索引 CAS + on_record 主机回调）与 AOF 等值重放（v_W < v_W 为假不跳旧）
//! 两侧皆承接（双承）。
//!
//! r95 定性修正：双承结构为 rust/C# 共有常态（检查点发布后已落盘的常态
//! 无位写恢复同样双承），本交错仅新增纳秒级触发落点，非数据丢失类。本票
//! 第一步排查结论：on_record 消费域（DbMeta 映射 insert / RI TryAdd /
//! bftree HashSet / 桩原位 patch）与 AOF 重放镜像 emit 面逐域核对**全部幂等**
//! （TTL 不在 on_record 收集面，承接独走 AOF，天然单承），故不触发单承化
//! 收口，本用例断言面为「双承素材效果幂等：两次承接终态等价一次」。
//!
//! 交错以 whlog 测试注入面 [whlog::VersionReadStall]（tail CAS 胜点后、
//! version_shift_word 读点前挂起）+ 窗口手动开关确定性构造；既有
//! [whlog::EncodeStall] 挂点在 encode_at 之后不达本交错。AOF 侧经测试内
//! 镜像仿真 wnode 版本闸（record_gate.rs:is_old_version_record：
//! `stamp < 恢复基线` 即跳旧），与 stale_window_sampling.rs 同款。

use std::{
  sync::{Arc, atomic::Ordering, mpsc},
  thread,
  time::{Duration, Instant},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcpr::{CheckpointType, CkptGateState, next_token_above};
use wdev::SegmentedDevice;
use whlog::VersionReadStall;

use super::support::{HashIndexTestOps, MiniStore};

const K_BASE: &[u8] = b"casgap:base";
const K_X: &[u8] = b"casgap:x";
const V_X: &[u8] = b"doubletake-write";
/// 轮 W 版本（交错写者读点取到的关窗保留版本域）
const V_W: i64 = 1;

/// 阻塞至写者确实挂起在读点前（超时判负，绝不死等拖垮门禁）
fn wait_parked(stall: &VersionReadStall) {
  let started = Instant::now();
  while !stall.is_parked() {
    assert!(
      started.elapsed() < Duration::from_secs(10),
      "写者未挂起在读点前：注入门失效"
    );
    thread::sleep(Duration::from_millis(1));
  }
}

/// CAS 胜点后、读点前窗关的无位等值戳写：恢复期重插臂（on_record 素材收集
/// 恰一）与 AOF 等值重放（恰一）双承，两次承接终态等价一次（素材面全幂等）
#[test]
fn cas_read_gap_window_close_doubletake_idempotent() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let db_path = dir.path().join("casgap_doubletake.db");
    let store = MiniStore::open(&db_path)?;
    let gate = CkptGateState::default();
    let p = store.session()?;

    // 基线键：轮 W 之前的稳定面，恢复读回归哨兵
    store.put(&p, K_BASE, b"keep").await?;

    // 轮 W 开窗：地板 = 开窗瞬间 tail，交错写者须落此域
    let floor_w = store.hlog.begin_version_shift(V_W as u64);

    // 交错构造：写者 CAS 胜点后挂起在读点前（不进纪元——检查点快照段
    // flush 的纪元排空屏障不得被挂起写者卡住），主线程于此间隙关窗
    let hlog_writer = Arc::clone(&store.hlog);
    let (tx, rx) = mpsc::channel::<()>();
    let writer = thread::spawn(move || {
      hlog_writer.version_read_stall.arm();
      let r = hlog_writer.append(K_X, V_X, 0, false);
      // append 返回（读点已取、编码已落、挂起标记已清）后发完成信号
      let _ = tx.send(());
      r
    });
    wait_parked(&store.hlog.version_read_stall);

    // 交错落定：窗关于读点前关闭（end_version_shift 清哨兵、版本域保留 V_W）
    store.hlog.end_version_shift();
    // 放行写者：读点取关闭态 → 无位 + ver = V_W（等值戳）
    store.hlog.version_read_stall.release();
    rx.recv()
      .map_err(|_| aok::anyhow!("交错写者通道意外关闭"))?;
    let (addr_x, ver_x) = writer
      .join()
      .map_err(|_| aok::anyhow!("交错写者线程 panic"))?
      .map_err(|e| aok::anyhow!("交错写者 append 失败: {e}"))?;

    assert!(
      addr_x >= floor_w,
      "结构前提: 交错写必须落在轮 W 模糊区: addr={addr_x:#x} floor={floor_w:#x}"
    );
    assert_eq!(
      ver_x, V_W,
      "读点于关窗后取值: AOF 戳必须为关窗保留版本（等于恢复基线）"
    );

    // 前台写路径后半段：索引发布（交错写者的正常收尾）
    {
      let _guard = p.enter();
      store.index.insert(K_X, addr_x)?;
    }

    // 轮 W 完整检查点：快照收录交错写（flush 覆盖其地址）后发布，恢复基线 V_W
    let token_w = next_token_above(0);
    wcpr::create_checkpoint_with_token(
      &store,
      &gate,
      &ckpt_dir,
      CheckpointType::FoldOver,
      token_w,
      floor_w,
    )
    .await?;

    // 崩溃恢复：自轮 W（最新 token）
    drop(p);
    drop(store);
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let restored = wcpr::recover_latest::<_, MiniStore>(&ckpt_dir, device).await?;
    let p2 = restored.session()?;

    // 第一次承接（重插臂）：无位不被 undo 剔除、落模糊区被重插、素材收集恰一
    assert_eq!(
      restored.recovery_undone.load(Ordering::Acquire),
      0,
      "无位记录不落 undo 谓词（位 + addr 双门），快照收录面不剔"
    );
    assert_eq!(
      restored.recovery_replayed.load(Ordering::Acquire),
      1,
      "模糊区重插恰承接一次（基线键在窗口外不计）"
    );
    assert_eq!(
      restored.recovery_on_record.load(Ordering::Acquire),
      2,
      "on_record 主机回调每条恰一次（基线 + 交错写）：素材收集无双收"
    );
    // 重插承接后的值面已就位（第一次承接终态）
    assert_eq!(
      restored.get(&p2, K_X).await?.as_deref(),
      Some(V_X),
      "第一次承接（重插臂）后键值已就位"
    );

    // 第二次承接（AOF 等值重放）：v_W < v_W 为假不跳旧，再承一次
    let mut applied = 0usize;
    if !(ver_x < V_W) {
      restored.put(&p2, K_X, V_X).await?;
      applied += 1;
    }
    assert_eq!(applied, 1, "等值戳不被版本闸跳旧，AOF 重放恰一次");

    // 双承幂等终态：两次承接后键值等价一次承接
    assert_eq!(
      restored.get(&p2, K_X).await?.as_deref(),
      Some(V_X),
      "双承素材效果幂等: 两次承接终态等价一次"
    );
    assert_eq!(
      restored.get(&p2, K_BASE).await?.as_deref(),
      Some("keep".as_bytes()),
      "基线键不受交错面影响"
    );
    OK
  })
}
