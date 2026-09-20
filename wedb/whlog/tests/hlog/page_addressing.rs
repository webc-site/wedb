//! 页换算单点等价性测试：`CircularPageBuffer` 访问器 vs 历史手写算式
//!
//! 覆盖：
//! - slot_for_address_equals_page_idx_of_page_of_address（环形槽位两步与一步合成同值）
//! - page_start_address_plus_offset_roundtrip（页首地址 + 页内偏移还原原地址）
//! - accessors_match_legacy_bit_math（与读缓存曾自算的 page_shift/page_mask 算式逐项相等）
//!
//! 本组用例是 wkv 读缓存删掉本地 `page_shift`/`page_mask` 派生、改调缓冲页访问器
//! （C# 侧 ReadCache 一律委托 allocator 访问器，不自持页参数）的等价性钉子。

use aok::{OK, Void};
use log::info;
use whlog::{CircularPageBuffer, HybridLogConfig};

/// 环形页槽位数（2 的幂），用于覆盖跨环回绕的页号
const NUM_PAGES: usize = 4;

/// 参与比对的 2 的幂页尺寸（页尺寸下限 4096，由 `HybridLogConfig` 构造校验；
/// 上限 1MB 使整环驻留内存控制在 4MB 内）
const PAGE_SIZES: [usize; 4] = [4_096, 16_384, 65_536, 1_048_576];

/// 构造指定页尺寸的环形页缓冲（读缓存与主日志共用同一换算单点）
fn buffer_for(page_size: usize) -> CircularPageBuffer {
  let config =
    HybridLogConfig::new(page_size, NUM_PAGES, 1.0).expect("页尺寸须为 4096 的 2 的幂整数倍");
  CircularPageBuffer::new(&config).expect("环形页缓冲构造失败")
}

/// 待比对地址样本：页首、页内 1 字节、页中、页尾，跨环回绕的多轮页号
fn address_samples(page_size: usize) -> Vec<u64> {
  let ps = page_size as u64;
  let mut addrs = Vec::new();
  for page_id in 0..(NUM_PAGES as u64 * 3) {
    addrs.extend_from_slice(&[
      page_id * ps,
      page_id * ps + 1,
      page_id * ps + ps / 2,
      page_id * ps + ps - 1,
    ]);
  }
  addrs
}

/// 恒等式一：`slot_for_address(addr) == page_idx(page_of_address(addr))`
///
/// 钉住「一步合成槽位」与「先取页号再取槽位」两条路径同值，读缓存的预注册在途
/// 计数与换页清槽据此指向同一物理槽。
#[test]
fn slot_for_address_equals_page_idx_of_page_of_address() -> Void {
  info!("> slot_for_address_equals_page_idx_of_page_of_address");

  for page_size in PAGE_SIZES {
    let buffer = buffer_for(page_size);
    for addr in address_samples(page_size) {
      assert_eq!(
        buffer.slot_for_address(addr),
        buffer.page_idx(buffer.page_of_address(addr)),
        "页尺寸 {page_size}、地址 {addr}：槽位换算两步与一步不等价"
      );
    }
  }

  OK
}

/// 恒等式二：`page_start_address(page_of_address(addr)) + offset_in_page(addr) == addr`
///
/// 反向换算与页内偏移互为补数，还原原地址无损。
#[test]
fn page_start_address_plus_offset_roundtrip() -> Void {
  info!("> page_start_address_plus_offset_roundtrip");

  for page_size in PAGE_SIZES {
    let buffer = buffer_for(page_size);
    for addr in address_samples(page_size) {
      let page_id = buffer.page_of_address(addr);
      assert_eq!(
        buffer.page_start_address(page_id) + buffer.offset_in_page(addr) as u64,
        addr,
        "页尺寸 {page_size}、地址 {addr}：未能由页首地址 + 页内偏移还原"
      );
    }
  }

  OK
}

/// 访问器与读缓存曾手写算式的逐项等价
///
/// 左侧为历史算式（`page_shift = page_size.trailing_zeros()`、`page_mask = page_size - 1`），
/// 右侧为访问器；两条路径对同一批地址完全同值，据此证明删本地派生为零行为改动。
#[test]
fn accessors_match_legacy_bit_math() -> Void {
  info!("> accessors_match_legacy_bit_math [新旧算式逐项等价]");

  for page_size in PAGE_SIZES {
    let buffer = buffer_for(page_size);

    // 历史算式：由页尺寸自派生 shift 与 mask
    let page_shift = page_size.trailing_zeros();
    let page_mask = (page_size - 1) as u64;

    for addr in address_samples(page_size) {
      let legacy_page_id = addr >> page_shift;
      assert_eq!(
        buffer.page_of_address(addr),
        legacy_page_id,
        "页尺寸 {page_size}、地址 {addr}：页号换算不等价"
      );
      assert_eq!(
        buffer.offset_in_page(addr),
        (addr & page_mask) as usize,
        "页尺寸 {page_size}、地址 {addr}：页内偏移不等价"
      );
      assert_eq!(
        buffer.slot_for_address(addr),
        (legacy_page_id as usize) & (NUM_PAGES - 1),
        "页尺寸 {page_size}、地址 {addr}：环形槽位不等价"
      );
      assert_eq!(
        buffer.page_start_address(legacy_page_id),
        legacy_page_id << page_shift,
        "页尺寸 {page_size}、页号 {legacy_page_id}：页首地址反向换算不等价"
      );
    }
  }

  OK
}
