//! DbMeta 换号元数据「持久化 → 重启重建 → 冷租户点查装载」同键闭环回归
//! （DbMetaRecord 单点编解码的端到端固化）。
//!
//! 冷装载解析面（resolve_context）落盘的 NS_MAP / DB_MAP 记录须与点查读面、
//! 启动重建扫描面同键同布局：历史上 persist 侧误传完整物理键致双重加缀，
//! 冷租户映射重启即失联（重建静默丢记录、点查永读不到，二访换号旧域判死）。
//! 本测试走 FLUSHDB/SWAPDB 前置解析同款入口，断言修复后闭环恒成立。
//!
//! 重启面走引擎唯一的内容恢复入口：`create_checkpoint` 提交 + `WedbStore::recover`
//! 恢复（恢复段单趟扫描内同趟完成 DbMeta 重建，见 store::cpr_host::run_recovery_pass），
//! 对位 C# libs/host/GarnetServer.cs 启动即 StoreWrapper.cs:RecoverCheckpointAsync；
//! 仅 `flush_all` 刷净日志页不构成可重开的持久化承诺（日志地址窗口只能由检查点
//! 快照还原，C# 侧同样无「裸开设备即续读日志」形态）。

use std::{fs::create_dir_all, sync::Arc};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wcpr::CheckpointType;
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};

const LOGIC_NS: u64 = 9;
const LOGIC_DB: u64 = 1;

fn test_config() -> aok::Result<StoreConfig> {
  Ok(StoreConfig::new(2048, 64 * 1024, 16, 0.5)?)
}

#[test]
fn cold_resolve_persist_survives_rebuild_and_probe() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let cpr_dir = dir.path().join("checkpoints");
    create_dir_all(&cpr_dir)?;
    let device = Arc::new(SegmentedDevice::single_file(
      dir.path().join("dbmeta_layout.db"),
    )?);
    let (vns, vdb);
    let cpr_token: u128;
    {
      let store = Arc::new(WedbStore::open(test_config()?, Arc::clone(&device))?);
      // 冷解析入口（全新租户，probe 未命中 → 分配并经 persist_dbmeta 落盘）
      let (r_ns, r_db) = store.resolve_context(LOGIC_NS, LOGIC_DB).await?;
      vns = r_ns;
      vdb = r_db;
      assert!(vns > 0 && vdb > 0, "新租户解析应分配非零虚拟号");
      // 数据落该域，重启后按原逻辑库可读
      let session = store.new_session()?;
      assert!(session.set_context(LOGIC_NS, LOGIC_DB));
      session.upsert(b"layout_k", b"layout_v").await?;
      drop(session);
      // 检查点提交：重建扫描面须覆盖到 DbMeta 记录
      cpr_token = store
        .create_checkpoint(&cpr_dir, CheckpointType::Snapshot)
        .await?
        .token;
      drop(store);
    }
    // 重启：恢复段（wcpr 恢复 → 同趟 DbMeta 重建 → 交出句柄）
    let store2 = Arc::new(WedbStore::recover(&cpr_dir, cpr_token, Arc::clone(&device)).await?);
    // ns 标量基线经重建装载（DB_MAP 记录同时抬升分配水位）
    assert_eq!(
      store2.vdb.ns_map.pin().get(&LOGIC_NS).copied(),
      Some(vns),
      "重建须还原磁盘 NS_MAP（persist 与 rebuild 同键）"
    );
    // 冷租户库路由表零常驻，首访点查磁盘回建（probe 与 persist 同键）
    let (r_ns2, r_db2) = store2.resolve_context(LOGIC_NS, LOGIC_DB).await?;
    assert_eq!(
      (r_ns2, r_db2),
      (vns, vdb),
      "点查装载须命中磁盘既有映射，绝不另起新号"
    );
    let session = store2.new_session()?;
    assert!(
      session.set_context(LOGIC_NS, LOGIC_DB),
      "装载后上下文可物化"
    );
    assert_eq!(
      session.read(b"layout_k").await?,
      Some(b"layout_v".to_vec()),
      "旧物理前缀数据按原逻辑库可读"
    );
    drop(session);
    drop(store2);
    OK
  })
}
