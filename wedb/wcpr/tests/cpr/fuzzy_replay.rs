//! 模糊区重插（对标 Recovery/Recovery.cs:RecoverFromPage 的索引重插遍）：
//! 人为构造「记录已落日志、索引快照漏插」的窗口条目，恢复必须经
//! [`wcpr::run_recovery_kernel`] 单趟扫描内核（宿主 from_recovered 恰好
//! 调用一次）在 `[index_start_logical_address, tail)` 窗口内补插索引；
//! 窗口内墓碑同样重插，盲追加删除的键不得复活

use std::{fs, sync::Arc};

use aok::Void;
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcpr::{CheckpointMeta, CheckpointType, meta_filename};
use wdev::SegmentedDevice;
use whlog::Error as HlogError;

use super::support::MiniStore;

#[test]
fn fuzzy_region_replay_restores_snapshot_missed_keys() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let db_path = dir.path().join("fuzzy.db");
    let store = MiniStore::open(&db_path)?;
    let p = store.session()?;

    // 快照收录键：正常索引条目，随快照文件落盘，恢复不经重放即可见
    store.put(&p, b"k:in", b"kept").await?;

    // 漏插键①（新增型）：记录直写日志、索引从不插入——对标「扫描已越过目标桶之后
    // CAS 才落下」的快照漏收形态
    let addr_miss = {
      let _guard = p.enter();
      match store.hlog.append(b"k:miss", b"rescued", 0, false) {
        Ok(addr) => addr,
        Err(HlogError::PageNotReady(_)) => {
          aok::bail!("fixture 数据集超出环形可变区，无法以直写形态构造漏插条目")
        }
        Err(e) => return Err(e.into()),
      }
    };

    // 复活威胁键：值记录正常入快照，随后仅追加墓碑记录而不 CAS 索引——
    // 对标「墓碑落地于扫描越过桶之后」。若无重放，恢复后索引仍指旧值，键复活
    store.put(&p, b"k:dead", b"zombie").await?;
    let prev_dead = {
      let _guard = p.enter();
      store
        .index
        .find_tag(b"k:dead")
        .map(|slot| store.resolve_main(slot))
        .unwrap_or(0)
    };
    {
      let _guard = p.enter();
      match store.hlog.append(b"k:dead", &[], prev_dead, true) {
        Ok(_) => {}
        Err(HlogError::PageNotReady(_)) => {
          aok::bail!("fixture 数据集超出环形可变区，无法以直写形态构造漏插墓碑")
        }
        Err(e) => return Err(e.into()),
      }
    }

    // 检查点：单线程场景下扫描无并发，index_start == tail，窗口为空
    let meta = wcpr::create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver).await?;
    assert_eq!(
      meta.index_start_logical_address, meta.hlog_meta.tail_address,
      "无并发窗口时起点即终点"
    );

    // 人为构造窗口：把 index_start 下推到漏插记录地址并重封签——对标竞态场景下
    // 「记录地址落在 [index_start, tail) 内但快照未收录」的持久形态
    let meta_path = ckpt_dir.join(meta_filename(meta.token));
    let mut crafted = CheckpointMeta::decode(&fs::read(&meta_path)?)?;
    assert!(
      addr_miss >= crafted.hlog_meta.begin_address
        && addr_miss < crafted.index_start_logical_address,
      "构造窗口须落在截断边界之上、原起点之下: addr={addr_miss:#x}"
    );
    crafted.index_start_logical_address = addr_miss;
    crafted.seal();
    fs::write(&meta_path, crafted.encode())?;

    // 丢弃活实例，仅由检查点恢复（无 AOF 装配形态）
    drop(p);
    drop(store);
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let restored = wcpr::recover_latest::<_, MiniStore>(&ckpt_dir, device).await?;
    let p2 = restored.session()?;

    assert_eq!(
      restored.get(&p2, b"k:in").await?.as_deref(),
      Some("kept".as_bytes()),
      "快照收录键不受重放影响"
    );
    assert_eq!(
      restored.get(&p2, b"k:miss").await?.as_deref(),
      Some("rescued".as_bytes()),
      "窗口内漏插键必须由模糊区重放补插可见"
    );
    assert_eq!(
      restored.get(&p2, b"k:dead").await?.as_deref(),
      None,
      "窗口内墓碑必须一并重插，盲追加删除不得复活"
    );
    Ok(())
  })
}

#[test]
fn fuzzy_window_out_of_range_is_rejected() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let ckpt_dir = dir.path().join("checkpoints");
    let db_path = dir.path().join("fuzzy_oob.db");
    let store = MiniStore::open(&db_path)?;
    let p = store.session()?;
    store.put(&p, b"k:a", b"1").await?;
    let meta = wcpr::create_checkpoint(&store, &ckpt_dir, CheckpointType::FoldOver).await?;

    // 窗口起点高于 tail：区间反向，属封签之外的逻辑矛盾，恢复须具名拒绝
    let meta_path = ckpt_dir.join(meta_filename(meta.token));
    let mut crafted = CheckpointMeta::decode(&fs::read(&meta_path)?)?;
    crafted.index_start_logical_address = crafted.hlog_meta.tail_address + 1;
    crafted.seal();
    fs::write(&meta_path, crafted.encode())?;
    drop(p);
    drop(store);
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let err = wcpr::recover_latest::<_, MiniStore>(&ckpt_dir, device)
      .await
      .err()
      .expect("反向窗口必须拒绝恢复");
    let msg = err.to_string();
    assert!(
      msg.contains("IndexStartLogicalAddress"),
      "拒绝须具名指向窗口起点字段: {msg}"
    );
    Ok(())
  })
}
