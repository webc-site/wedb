//! 恢复期单趟有序扫描内核回归：一次 `[begin, tail)` 扫描同趟产出全部恢复态
//!
//! 对标 C# Recovery/Recovery.cs:RecoverHybridLogAsync 单趟扫描经
//! GarnetRecordTriggers.cs:OnRecoverySnapshotRead 逐记录回调的融合形态。
//! 旧 Rust 实现把恢复链拆成三次独立全扫（索引逐条 `read_record` 随机读、
//! DbMeta 独立重建扫描、模糊区独立重放），本测试锁死合一后的等价性：
//! 1. RI 存根：恢复后被标记「已从检查点恢复」且句柄清零，首次访问惰性开树回读；
//! 2. vdb 映射：DbMeta 记录随同趟扫描重建逻辑映射与分配水位（只抬不回退）；
//! 3. 模糊区：索引快照漏收的窗口记录（直写日志、从不 CAS）由扫描内核重插，
//!    恢复后按键可读。
//!
//! 三项判据全部由 [`WedbStore`] 恢复链一次交出句柄的动作满足，无第二次扫描。

use std::{
  fs::{self, create_dir_all},
  sync::{Arc, atomic::Ordering::Relaxed},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use tempfile::tempdir;
use wbftree::{StorageBackendType, TreeTuning};
use wcpr::{CheckpointMeta, CheckpointType, meta_filename};
use wdev::SegmentedDevice;
use wkv::{StoreConfig, WedbStore};

/// 与根域 (0, 0) 错开的逻辑命名空间/库编号
const LOGIC_NS: u64 = 5;
const LOGIC_DB: u64 = 2;

/// 逻辑命名空间当前映射的虚拟命名空间 ID（未映射为 None）
fn mapped_vns(store: &WedbStore<SegmentedDevice>, logic_ns: u64) -> Option<u64> {
  store.vdb.ns_map.pin().get(&logic_ns).copied()
}

/// 虚拟命名空间下逻辑库当前映射的虚拟库 ID（未映射为 None）
fn mapped_vdb(store: &WedbStore<SegmentedDevice>, vns: u64, logic_db: u64) -> Option<u64> {
  store
    .vdb
    .db_routing
    .pin()
    .get(&vns)
    .and_then(|routing| routing.table.get(logic_db))
}

/// 单趟恢复：RI 存根自愈 + vdb 映射重建 + 模糊区重插一次扫描全部到位
#[test]
fn single_pass_recovery_rebuilds_stub_vdb_and_fuzzy_window() -> Void {
  let rt = Runtime::new()?;
  rt.block_on(async {
    let dir = tempdir()?;
    let db_path = dir.path().join("single_pass.db");
    let ckpt_dir = dir.path().join("checkpoints");
    let ri_dir = dir.path().join("ri_data");
    create_dir_all(&ri_dir)?;
    create_dir_all(&ckpt_dir)?;

    const TUNE: TreeTuning = TreeTuning {
      cache_size: 65536,
      min_record_size: 8,
      max_record_size: 1024,
      max_key_len: 128,
      leaf_page_size: 0,
    };

    let config = StoreConfig::new(1024, 64 * 1024, 16, 0.5)?.with_range_index_dir(&ri_dir);
    let token;
    let old_vns;
    let old_vdb;
    let allocated_before;

    // 首进程：三类素材各写一份，再做检查点
    {
      let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
      let store = Arc::new(WedbStore::open(config.clone(), Arc::clone(&device))?);
      let session = store.new_session()?;

      // 素材①：根域普通 KV + RangeIndex（Meta 桩记录入日志，句柄非零）
      session.upsert(b"sys:status", b"online").await?;
      session
        .range_index_create(b"orders", StorageBackendType::Memory, TUNE)
        .await?;
      session
        .range_index_set(b"orders", b"user:1", b"balance_100")
        .await?;
      let live = session
        .load_range_index_stub(b"orders")
        .await?
        .expect("创建后存根必在");
      assert!(
        live.1.tree_handle != 0 && !live.1.is_recovered(),
        "检查点前存根须为活树形态: handle={}",
        live.1.tree_handle
      );

      // 素材②：逻辑 (5, 2) 建库写数据（set_context 同批落 DbMeta 映射记录）
      let tenant = store.new_session()?;
      tenant.set_context(LOGIC_NS, LOGIC_DB);
      tenant.upsert(b"tenant_k", b"tenant_v").await?;
      old_vns = mapped_vns(&store, LOGIC_NS).expect("set_context 须建逻辑命名空间映射");
      old_vdb = mapped_vdb(&store, old_vns, LOGIC_DB).expect("set_context 须建逻辑库映射");
      allocated_before = store.vdb.next_virtual_id.load(Relaxed);

      // 素材③：直写日志、从不 CAS 的漏插记录——对标「快照扫描越过其桶之后
      // 才落地的索引 CAS」的持久形态（与 wcpr fuzzy_replay 同一构造法）
      let phys_miss = session.session_string_key(b"k:fuzzy");
      let addr_miss = {
        let _guard = session.participant().enter();
        store.hlog.append(&phys_miss, b"rescued", 0, false)?
      };

      // 检查点：单线程无并发，快照 index_start == tail，窗口天然为空
      let meta = store
        .create_checkpoint(&ckpt_dir, CheckpointType::FoldOver)
        .await?;
      token = meta.token;

      // 人为构造窗口：把 index_start 下推到漏插记录地址并重封签
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

      drop(tenant);
      drop(session);
      drop(store);
      drop(device);
    } // 模拟断电宕机

    // 重启：恢复链只此一趟扫描，交出句柄即全恢复态
    let device = Arc::new(SegmentedDevice::single_file(&db_path)?);
    let recovered = Arc::new(WedbStore::recover(&ckpt_dir, token, device).await?);
    let session = recovered.new_session()?;

    // 判据①（RI 桩）：存根已被单趟扫描标记恢复且句柄清零（尚未惰性开树）
    let healed = session
      .load_range_index_stub(b"orders")
      .await?
      .expect("恢复后存根必在");
    assert!(
      healed.1.is_recovered() && healed.1.tree_handle == 0,
      "存根须为恢复自愈形态（handle=0 + recovered）: handle={}, recovered={}",
      healed.1.tree_handle,
      healed.1.is_recovered()
    );
    // 惰性开树回读历史字段，验证 pending 注册随同趟完成
    assert_eq!(
      session.range_index_get(b"orders", b"user:1").await?,
      Some(b"balance_100".to_vec()),
      "恢复后首访须惰性开树回读"
    );

    // 判据②（vdb）：DbMeta 随同趟重建——映射还原、水位只抬不回退
    assert_eq!(
      mapped_vns(&recovered, LOGIC_NS),
      Some(old_vns),
      "单趟扫描须还原磁盘逻辑命名空间映射"
    );
    assert_eq!(
      mapped_vdb(&recovered, old_vns, LOGIC_DB),
      None,
      "冷租户条款：非根域库级路由表零常驻"
    );
    let allocated_after = recovered.vdb.next_virtual_id.load(Relaxed);
    assert!(
      allocated_after > old_vns.max(old_vdb) && allocated_after >= allocated_before,
      "分配水位须抬到磁盘水位之上且不回退: before={allocated_before}, after={allocated_after}"
    );
    let tenant = recovered.new_session()?;
    tenant.set_strict_context(true);
    assert!(
      !tenant.set_context(LOGIC_NS, LOGIC_DB),
      "映射只还原 ns 标量，冷库严格会话拒绝盲分配（水位重建生效旁证）"
    );
    let (vns, vdb) = recovered.resolve_context(LOGIC_NS, LOGIC_DB).await?;
    assert_eq!(
      (vns, vdb),
      (old_vns, old_vdb),
      "点查装载须命中磁盘既有映射，绝不另起新号"
    );
    assert!(tenant.set_context(LOGIC_NS, LOGIC_DB), "装载后重放物化");
    assert_eq!(
      tenant.read(b"tenant_k").await?,
      Some(b"tenant_v".to_vec()),
      "重建后旧物理前缀数据须按原逻辑库可读"
    );

    // 判据③（模糊区）：快照漏收的窗口记录由扫描内核重插，按键可读
    assert_eq!(
      session.read(b"k:fuzzy").await?,
      Some(b"rescued".to_vec()),
      "窗口内漏插键必须由同一趟扫描的重插步骤补插可见"
    );
    // 快照收录键不受影响
    assert_eq!(
      session.read(b"sys:status").await?,
      Some(b"online".to_vec()),
      "快照收录键须正常可读"
    );

    drop(tenant);
    drop(session);
    drop(recovered);
    OK
  })?;

  OK
}
