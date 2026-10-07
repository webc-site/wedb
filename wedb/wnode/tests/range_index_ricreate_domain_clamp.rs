#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
//! RI.CREATE PAGESIZE/MAXRECORD 入口域钳制集成测试（task/ing/wnode-ri-create-
//! pagesize-guard-multiply-overflow-bypass.md 登记）
//!
//! 验收点：
//! 1. PAGESIZE 巨值（2^61 / 2^62 / i64::MAX / 上界+1）配 CACHESIZE i64::MAX——
//!    审核钉「仅 saturating_mul 不封死」的组合（4 × 2^62 饱和至 i64::MAX，
//!    cache 同值时容量守卫放行）——回 PAGESIZE 域钳制定向错误帧，不 panic；
//! 2. MAXRECORD 巨值（2^62 / i64::MAX / 上界+1，含 MINRECORD=MAXRECORD=2^62
//!    的关系校验放行形）同回 MAXRECORD 域钳制帧——旧裸乘法放行下传会击穿
//!    wbftree `cb_max_record_size(max_record_size + 1)` 折算与引擎侧
//!    `4 * leaf_page_size` usize 乘法；
//! 3. 钳制上界恰取 compute_leaf_page_size 派生域封顶 32KiB（wbftree
//!    `RangeIndexManager::MAX_LEAF_PAGE_SIZE` 单点）：PAGESIZE 32768 放行建树、
//!    32769 拒；MAXRECORD 32768 过钳制（后续由引擎按记录/叶页比例裁决）、
//!    32769 拒。
//!
//! 对标 test/standalone/Garnet.test.rangeindex/RespRangeIndexTests.cs 的
//! RICreate 系列（C# 无域钳制对应用例：巨 long 经 (uint) 截断静默回绕——
//! 见 doc/zh/deviations.md 第 83 条 rust 入口守卫偏离，本文件为本仓钳制面
//! 的锁）。

use wdev::SegmentedDevice;
use wkv::StoreSession;
use wnode::resp::resp_server_session::RespServerSession;
use wnode_test::{err_frame, test_env};

/// 域钳制拒绝文案（`RiCreateOptions::validate` 单点，上界与
/// `RangeIndexManager::MAX_LEAF_PAGE_SIZE` 对齐自洽）
const ERR_PAGESIZE_CLAMP: &str = "ERR PAGESIZE must not exceed 32768";
const ERR_MAXRECORD_CLAMP: &str = "ERR MAXRECORD must not exceed 32768";

/// 2^62：旧裸乘法 4 × 2^62 = 2^64 恰溢出 i64 的最小攻击档
const POW_2_62: &str = "4611686018427387904";
/// 2^61：票面点名的次档
const POW_2_61: &str = "2305843009213693952";
/// i64 全域上界
const I64_MAX: &str = "9223372036854775807";
/// 派生域封顶 + 1（钳制边界外侧最近档）
const OVER_BOUND: &str = "32769";

type Env = (
  tempfile::TempDir,
  StoreSession<SegmentedDevice>,
  RespServerSession,
);

fn setup_env() -> Env {
  test_env(true)
}

/// 验收点 1：PAGESIZE 巨值 × CACHESIZE i64::MAX（仅饱和乘仍放行的组合）
/// 一律回域钳制帧，debug/release 均不 panic，预算记账零滞留
#[compio::test]
async fn ricreate_pagesize_extremes_reject_with_domain_frame() {
  let (_dir, session, mut resp) = setup_env();
  for page in [OVER_BOUND, POW_2_61, POW_2_62, I64_MAX] {
    let mut out = Vec::new();
    resp
      .network_ricreate(
        &[
          b"idx",
          b"MEMORY",
          b"CACHESIZE",
          I64_MAX.as_bytes(),
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
      err_frame(ERR_PAGESIZE_CLAMP),
      "PAGESIZE {page} 应回域钳制错误帧"
    );
    assert!(
      !String::from_utf8_lossy(&out).contains("异常退出"),
      "错误帧不得为 panic 收敛文案"
    );
  }
  assert_eq!(
    session.store.range_index().cache_reserved(),
    0,
    "钳制拒绝不得滞留页缓存预算记账"
  );
}

/// 验收点 2：MAXRECORD 巨值（含 MINRECORD=MAXRECORD 关系校验放行形）同回
/// 域钳制帧——杜绝下传后 `max_record_size + 1` 折算与引擎 usize 乘法回绕
#[compio::test]
async fn ricreate_maxrecord_extremes_reject_with_domain_frame() {
  let (_dir, session, mut resp) = setup_env();
  for (i, args) in [
    vec![
      b"idx".as_slice(),
      b"MEMORY",
      b"CACHESIZE",
      I64_MAX.as_bytes(),
      b"MAXRECORD",
      OVER_BOUND.as_bytes(),
    ],
    vec![
      b"idx",
      b"MEMORY",
      b"CACHESIZE",
      I64_MAX.as_bytes(),
      b"MAXRECORD",
      POW_2_62.as_bytes(),
    ],
    vec![
      b"idx",
      b"MEMORY",
      b"CACHESIZE",
      I64_MAX.as_bytes(),
      b"MAXRECORD",
      I64_MAX.as_bytes(),
    ],
    // MINRECORD ≤ MAXRECORD 同巨值：关系校验放行，钳制在折算前拦截
    vec![
      b"idx",
      b"MEMORY",
      b"MINRECORD",
      POW_2_62.as_bytes(),
      b"MAXRECORD",
      POW_2_62.as_bytes(),
      b"CACHESIZE",
      I64_MAX.as_bytes(),
    ],
  ]
  .into_iter()
  .enumerate()
  {
    let mut out = Vec::new();
    resp
      .network_ricreate(&args, &session, &mut out)
      .await
      .unwrap();
    assert_eq!(
      out,
      err_frame(ERR_MAXRECORD_CLAMP),
      "第 {i} 组 MAXRECORD 巨值应回域钳制错误帧"
    );
    assert!(
      !String::from_utf8_lossy(&out).contains("异常退出"),
      "错误帧不得为 panic 收敛文案"
    );
  }
  assert_eq!(
    session.store.range_index().cache_reserved(),
    0,
    "钳制拒绝不得滞留页缓存预算记账"
  );
}

/// 验收点 3：钳制上界恰取派生域封顶 32KiB——PAGESIZE 32768 建树成功（配
/// 恰 4x 容量 131072）；32769 拒。MAXRECORD 32768 不被钳制拦（32769 拒），
/// 深入引擎后由引擎按记录/叶页比例拒绝——帧为引擎错误而非钳制帧
#[compio::test]
async fn ricreate_domain_boundary_at_derived_cap() {
  let (_dir, session, mut resp) = setup_env();
  // PAGESIZE 恰上界：放行，容量恰 4x 边界建树成功
  let mut out = Vec::new();
  resp
    .network_ricreate(
      &[
        b"idx_page",
        b"MEMORY",
        b"CACHESIZE",
        b"131072",
        b"PAGESIZE",
        b"32768",
      ],
      &session,
      &mut out,
    )
    .await
    .unwrap();
  assert_eq!(out, b"+OK\r\n", "PAGESIZE 32768 恰上界应建树成功");
  assert_eq!(session.store.range_index().cache_reserved(), 131072);
  // PAGESIZE 上界 + 1：钳制拒
  let mut out = Vec::new();
  resp
    .network_ricreate(
      &[
        b"idx_page2",
        b"MEMORY",
        b"CACHESIZE",
        b"131072",
        b"PAGESIZE",
        OVER_BOUND.as_bytes(),
      ],
      &session,
      &mut out,
    )
    .await
    .unwrap();
  assert_eq!(
    out,
    err_frame(ERR_PAGESIZE_CLAMP),
    "PAGESIZE 32769 应回钳制帧"
  );
  // MAXRECORD 恰上界：过钳制、由引擎按记录/叶页比例拒（非钳制帧、错误帧）
  let mut out = Vec::new();
  resp
    .network_ricreate(
      &[
        b"idx_rec",
        b"MEMORY",
        b"CACHESIZE",
        b"262144",
        b"PAGESIZE",
        b"32768",
        b"MAXRECORD",
        b"32768",
      ],
      &session,
      &mut out,
    )
    .await
    .unwrap();
  assert_eq!(
    out.first(),
    Some(&b'-'),
    "MAXRECORD 32768 过钳制后应回引擎错误帧"
  );
  assert_ne!(
    out,
    err_frame(ERR_MAXRECORD_CLAMP),
    "MAXRECORD 32768 恰上界不得被钳制拦截"
  );
  assert!(
    !String::from_utf8_lossy(&out).contains("异常退出"),
    "错误帧不得为 panic 收敛文案"
  );
  // MAXRECORD 上界 + 1：钳制拒
  let mut out = Vec::new();
  resp
    .network_ricreate(
      &[
        b"idx_rec2",
        b"MEMORY",
        b"CACHESIZE",
        b"262144",
        b"PAGESIZE",
        b"32768",
        b"MAXRECORD",
        OVER_BOUND.as_bytes(),
      ],
      &session,
      &mut out,
    )
    .await
    .unwrap();
  assert_eq!(
    out,
    err_frame(ERR_MAXRECORD_CLAMP),
    "MAXRECORD 32769 应回钳制帧"
  );
  // 仅第一棵恰上界树在线：262144 组合被引擎拒、不滞留记账
  assert_eq!(session.store.range_index().cache_reserved(), 131072);
}
