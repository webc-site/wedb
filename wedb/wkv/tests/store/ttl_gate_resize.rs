use std::sync::{
  Arc,
  atomic::{AtomicI64, Ordering},
};

use aok::{OK, Void};
use compio::runtime::Runtime;
use wdev::SegmentedDevice;
use windex::{HashIndex, SPLIT_COMPLETED, SPLIT_UNSTARTED, chunk_count, chunk_offset_for_hash};
use wkv::{TtlGate, WedbStore, store::ResizePhase};
use wtest_base::open_test_store;

fn stage_resize(store: &WedbStore<SegmentedDevice>, publish: bool, phase: ResizePhase) {
  let old_index = store.active_index();
  let count = chunk_count(old_index.size);
  store.resize.split_status.store(Arc::new(
    (0..count)
      .map(|_| AtomicI64::new(SPLIT_UNSTARTED))
      .collect(),
  ));
  store
    .resize
    .num_pending_chunks
    .store(count, Ordering::Release);
  store.resize.old_index.store(Some(Arc::clone(&old_index)));
  if publish {
    store
      .index
      .store(Arc::new(HashIndex::new(old_index.size * 2).unwrap()));
  }
  store.resize.phase.store(phase as u8, Ordering::Release);
}

/// 扩容窗口 TTL 门初筛裁决矩阵（确定性装配，不依赖时序竞争）
///
/// 屏障协议：PrepareGrow 相位内一切受保护会话操作（含 TTL 门读路径）在入口
/// 挂起（挂起语义由 resize 屏障测试锁定），探针断言仅在相位发布后的可达窗口
/// 执行——统一推进 InProgressGrow 后校验门语义与分块协同状态。
/// `publish` 决定装配是否先行切表：false 装配过渡窗（相位发布而表未切，
/// 同表防护按旧表快照裁决不迁移）；true 装配已切表（异表判定触发协同迁移）。
fn check_resize_gate(publish: bool, stage_phase: ResizePhase) -> Void {
  Runtime::new()?.block_on(async {
    for probe_tag in [false, true] {
      let (_dir, store) = open_test_store("ttl_resize")?;
      let session = store.new_session()?;
      let key = b"expired";
      let expiry = 16;
      session.upsert(key, b"value").await?;
      session.put_ttl(key, expiry).await?;
      let ttl_key = session.ttl_key(key);
      let old_index = store.active_index();
      let chunk = chunk_offset_for_hash(HashIndex::hash_key(&ttl_key), old_index.mask);
      assert!(old_index.find_tag(&ttl_key).is_some());
      stage_resize(&store, publish, stage_phase);
      if publish {
        assert!(store.active_index().find_tag(&ttl_key).is_none());
      }
      // 相位发布：进入会话操作可达窗口（过渡窗同表防护 / 迁移期异表协同）
      store
        .resize
        .phase
        .store(ResizePhase::InProgressGrow as u8, Ordering::Release);
      if probe_tag {
        assert!(session.has_ttl_tag(key)?);
      }
      assert!(matches!(
        session.ttl_gate_mem_at(key, expiry + 1)?,
        TtlGate::Due
      ));
      assert!(matches!(
        session.ttl_gate_mem_at(key, expiry)?,
        TtlGate::Pass
      ));
      assert!(matches!(
        session.ttl_gate_mem_at(b"absent", expiry + 1)?,
        TtlGate::Pass
      ));
      assert_eq!(
        store.resize.split_status.load()[chunk].load(Ordering::Acquire),
        if publish {
          SPLIT_COMPLETED
        } else {
          SPLIT_UNSTARTED
        }
      );
      assert_eq!(session.ttl_of(key).await?, Some(expiry));
    }
    OK
  })
}

/// 过渡窗（相位已发布而活跃表未切）：TTL 门经同表防护按旧表快照裁决，
/// 不迁移不假推进，杜绝误判无 TTL 放行
#[test]
fn ttl_gate_before_index_publication() -> Void {
  check_resize_gate(false, ResizePhase::PrepareGrow)
}

/// 防御性装配「切表先行、相位滞留 PrepareGrow」：屏障协议下该矛盾态对会话
/// 不可达；相位发布后切表窗口转为迁移期，TTL 门异表判定触发协同迁移
#[test]
fn ttl_gate_after_index_before_phase_publication() -> Void {
  check_resize_gate(true, ResizePhase::PrepareGrow)
}

/// 迁移期（相位与新表均已发布）：TTL 门异表判定先迁移目标分块再探针，
/// 未迁移分块内 TTL 记录可见，杜绝误判无 TTL 放行过期数据
#[test]
fn ttl_gate_during_migration() -> Void {
  check_resize_gate(true, ResizePhase::InProgressGrow)
}
