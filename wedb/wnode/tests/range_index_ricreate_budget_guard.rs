//! RI.CREATE CACHESIZE 容量守卫集成测试（task/ing/wnode-ricreate-cachesize-unwind-budget-leak.md
//! 登记，task/ing/wbftree-ricreate-panicpremise-falsify-realign.md 返工对齐引擎真实契约）
//!
//! 验收点：
//! 1. CACHESIZE 4096 / 8192（缺省 MAXRECORD 1024 派生叶页 4096，均低于恰 4x
//!    边界 16384）与 PAGESIZE 超限组合回协议错误帧——非引擎串（引擎
//!    InvalidConfig 穿透帧在 4x 严守卫下不可达，"阻塞任务异常退出" panic 收敛
//!    文案更不得出现），守卫在 wnode 入口单点拦截；
//! 2. 重复失败创建 N 次后 `cache_reserved()` 零增长——预算磨穿面（脚本化小
//!    CACHESIZE 失败创建滞留 CACHESIZE 记账、耗尽 256MiB 总闸令升阶静默回落
//!    信封态）闭合；
//! 3. 引擎合法调参建树回归——守卫不误伤。MEMORY（cache_only）的引擎权威下限
//!    为 4× 叶页（bf-tree `Config::validate`），守卫已对齐同线，故合法组取
//!    恰好 4× 边界（CACHESIZE 16384 + 派生叶页 4096）、显式叶页足额环
//!    （PAGESIZE 4096 + CACHESIZE 32768）与深内 defaults（16MiB）；DISK 面
//!    守卫线（4x）严于引擎线（2x）的区间为 deviations 83 登记的保守偏离。
//!
//! 对标 test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs 的
//! RICreate 系列（C# 无容量守卫对应用例：引擎 validate 拒回 NULL → 泛化
//! InvalidOperationException，见 doc/zh/deviations.md 第 83 条，本文件为本仓
//! 守卫面的锁）。

use wdev::SegmentedDevice;
use wkv::StoreSession;
use wnode::resp::resp_server_session::RespServerSession;
use wnode_test::{err_frame, test_env};

/// 守卫拒绝文案（`RiCreateOptions::validate` 单点，对齐引擎 cache-only
/// 4 × 叶页比例判定的严规则）
const ERR_CACHESIZE_GUARD: &str = "ERR CACHESIZE must be at least 4 times the leaf page size";

/// 重复失败创建次数：每次若滞留 4096B 记账即 128KiB 可观测漂移
const FAIL_ROUNDS: usize = 32;

type Env = (
  tempfile::TempDir,
  StoreSession<SegmentedDevice>,
  RespServerSession,
);

fn setup_env() -> Env {
  test_env(true)
}

/// 验收点 1a：CACHESIZE 4096 / 8192、缺省 MAXRECORD 1024 → 派生叶页 4096，
/// 均低于 4 × 4096，回协议错误帧且记账零增长
#[compio::test]
async fn ricreate_cachesize_below_derived_leaf_rejects_with_error_frame() {
  let (_dir, session, mut resp) = setup_env();
  for i in 0..FAIL_ROUNDS {
    for cache in ["4096", "8192"] {
      let mut out = Vec::new();
      resp
        .network_ricreate(
          &[b"idx", b"MEMORY", b"CACHESIZE", cache.as_bytes()],
          &session,
          &mut out,
        )
        .await
        .unwrap();
      assert_eq!(
        out,
        err_frame(ERR_CACHESIZE_GUARD),
        "第 {i} 轮 CACHESIZE {cache} 应回守卫错误帧"
      );
      assert!(
        !String::from_utf8_lossy(&out).contains("异常退出"),
        "错误帧不得为 panic 收敛文案"
      );
    }
  }
  // 验收点 2：重复失败创建后页缓存记账零增长（wbftree cache_reserved 口径）
  assert_eq!(
    session.store.range_index().cache_reserved(),
    0,
    "失败创建不得滞留页缓存预算记账"
  );
}

/// 验收点 1b：PAGESIZE 显式超限组合（8192 > 4096、4096 == 4096）均低于
/// 恰 4x 边界，同拒
#[compio::test]
async fn ricreate_pagesize_exceeding_cachesize_rejects_with_error_frame() {
  let (_dir, session, mut resp) = setup_env();
  for (cache, page) in [("4096", "8192"), ("4096", "4096")] {
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[
          b"idx",
          b"MEMORY",
          b"CACHESIZE",
          cache.as_bytes(),
          b"PAGESIZE",
          page.as_bytes(),
        ],
        &session,
        &mut out,
      )
      .await
      .unwrap();
    assert_eq!(
      out,
      err_frame(ERR_CACHESIZE_GUARD),
      "CACHESIZE {cache} / PAGESIZE {page} 应回守卫错误帧"
    );
  }
  assert_eq!(session.store.range_index().cache_reserved(), 0);
}

/// 验收点 3：合法调参回归——恰好 4× 叶页边界（派生 4096 ↔ CACHESIZE 16384）、
/// 显式叶页足额环（PAGESIZE 4096 + CACHESIZE 32768）与全缺省默认调参（16MiB）
/// 均正常建树
#[compio::test]
async fn ricreate_legal_tunings_still_create() {
  let (_dir, session, mut resp) = setup_env();
  for (i, args) in [
    vec![b"idx_a".as_slice(), b"MEMORY", b"CACHESIZE", b"16384"],
    vec![
      b"idx_b".as_slice(),
      b"MEMORY",
      b"CACHESIZE",
      b"32768",
      b"PAGESIZE",
      b"4096",
    ],
    vec![b"idx_c".as_slice(), b"MEMORY"],
  ]
  .into_iter()
  .enumerate()
  {
    let mut out = Vec::new();
    resp
      .network_ricreate(&args, &session, &mut out)
      .await
      .unwrap();
    assert_eq!(out, b"+OK\r\n", "第 {i} 组合法调参应建树成功");
  }
  // 记账与在线树一致：三棵 MEMORY 树各占其 CACHESIZE（idx_c 走缺省 16MiB）
  let expected = 16384 + 32768 + 16 * 1024 * 1024;
  assert_eq!(
    session.store.range_index().cache_reserved(),
    expected,
    "记账须等于在线树环容量和"
  );
}
