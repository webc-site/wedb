//! wepoch/whlog 微基准夹具：对标 C# 侧 wepoch ↔ Tsavorite `LightEpoch`（playground/
//! LightEpochLitmus）、whlog ↔ `AllocatorBase` 环形页缓冲（Device.benchmark 页流水）。
//! 现仓零基准守护，本件补建纪元保护与混合日志页缓冲两条关键路径的微基准与回读校验。

use std::sync::Arc;

use wepoch::LightEpoch;
use whlog::{CircularPageBuffer, HybridLogConfig};

/// wepoch 纪元保护微基准夹具：持一份 LightEpoch 实例，测 protected_scope 进出与推进纪元
pub struct EpochHarness {
  pub epoch: Arc<LightEpoch>,
}

impl EpochHarness {
  /// 建 4 槽纪元表（基准单线程，留裕量）
  pub fn bench() -> Self {
    Self {
      epoch: Arc::new(LightEpoch::new(4)),
    }
  }

  /// 受保护域进出一次，返回进出之间 `thread_protected` 真值（供求和 black_box，防被优化掉）
  #[inline]
  pub fn protect_once(&self) -> u64 {
    let _scope = self.epoch.protected_scope();
    self.epoch.thread_protected() as u64
  }

  /// 推进一次当前纪元（对标回收水位推进热路径）
  #[inline]
  pub fn bump_once(&self) -> u64 {
    self.epoch.bump_current_epoch()
  }

  /// 工况预校验：作用域内须受保护、出域后须解除，推进纪元须使 current_epoch 单调增
  pub fn validate(&self) {
    assert!(!self.epoch.thread_protected(), "wepoch 基线态应未受保护");
    {
      let _scope = self.epoch.protected_scope();
      assert!(
        self.epoch.thread_protected(),
        "wepoch protected_scope 内应受保护"
      );
    }
    assert!(
      !self.epoch.thread_protected(),
      "wepoch protected_scope 出域应解除保护"
    );
    let before = self.epoch.current_epoch();
    let bumped = self.bump_once();
    assert_eq!(bumped, before + 1, "wepoch 推进纪元须单调增");
  }
}

/// whlog 环形页缓冲微基准夹具：定长对齐页环，测页装载/直读命中回读
pub struct PageHarness {
  pub buffer: CircularPageBuffer,
  pub page_size: usize,
  pub num_pages: usize,
}

impl PageHarness {
  /// 以 whlog 缺省页配置（64KB × 16 页）建环
  pub fn bench() -> aok::Result<Self> {
    let config = HybridLogConfig::default();
    let buffer = CircularPageBuffer::new(&config)?;
    Ok(Self {
      page_size: config.page_size,
      num_pages: config.num_pages,
      buffer,
    })
  }

  /// 装载一页（将 data 拷入 page_id 对应环形槽）
  #[inline]
  pub fn load(&self, page_id: u64, data: &[u8]) {
    self.buffer.load_page(page_id, data);
  }

  /// 直读命中：取页首字节（供 black_box，杜绝空读被优化）
  #[inline]
  pub fn read_head(&self, page_id: u64) -> u8 {
    self.buffer.read_page(page_id)[0]
  }

  /// 工况预校验：自包含装载-回读闭环——写入一页已知图案后回读，逐字节命中方通过，
  /// 杜绝基准测到脏/空页（对标 C# Device.benchmark 写入后读回校验口径）
  pub fn validate_roundtrip(&self, page_id: u64) {
    let data: Vec<u8> = (0..self.page_size)
      .map(|i| (i as u8).wrapping_mul(7).wrapping_add(1))
      .collect();
    self.buffer.load_page(page_id, &data);
    let guard = self.buffer.read_page(page_id);
    assert_eq!(
      &guard[..self.page_size],
      &data[..],
      "whlog 页回读失真: 装载未落页"
    );
  }
}
