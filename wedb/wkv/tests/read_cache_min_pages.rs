//! 读缓存页数 ≥2 页硬下界回归（收敛口单点真源）
//!
//! 对标 C#：
//! - libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:650-651
//!   （读缓存预算 `MemorySize < (1L << PageSizeBits) * 2` 即拒，页数同形推导恒 ≥2）
//! - LogSettings.cs:28 `kMinPageCount = 2`
//!
//! 期望值 2 由页界式推导：第 e 次换页后 closed == (e - n + 1) × page_size，
//! n=1 时水位恒等于新 tail（head == closed == tail），head/tail 之间无余页承接
//! 第二机会与在途滞留，环形缓存退化；n=2 起被驱逐页与当前页方分立。
//!
//! `with_read_cache_pages` 为页数唯一收敛口，检查点恢复回灌入口
//! （`wkv::store::cpr_host` 按落盘 StoreMeta.read_cache_num_pages 调本 setter，
//! 历史 1 页检查点收敛为 2 而非硬失败）与本测试共用同一口径。
//!
//! 在 garnet 中的相对路径: libs/storage/Tsavorite/cs/test/test.session/NativeReadCacheTests.cs（最小页数驻留）

use wkv::StoreConfig;

/// 低于下界的入参经收敛口钳至 2 页且校验放行
#[test]
fn read_cache_pages_below_floor_converges_to_min() {
  let config = StoreConfig::default()
    .with_read_cache(true)
    .with_read_cache_pages(1)
    .expect("1 页入参应收敛至下界而非报错");
  assert_eq!(config.read_cache_num_pages, 2, "1 页必须收敛至 ≥2 页硬下界");
  config.validate().expect("收敛后配置须通过校验");
}
