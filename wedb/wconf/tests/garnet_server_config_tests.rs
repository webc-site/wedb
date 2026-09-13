//! 服务器配置集成测试（对标 test/standalone/Garnet.test/GarnetServerConfigTests.cs）
//!
//! 覆盖 rust 侧已落地的 ServerOptions / GarnetServerOptions 解析与校验面：
//! 内联键值尺寸、最小页大小、AOF 段尺寸流、缓冲池预算、初始 IO 记录尺寸、
//! AOF 大小上限与默认值覆盖。C# 中依赖宿主静态（NativeStorageDevice /
//! SectorAlignedBufferPool）、Azure 导入导出、LoadModuleCS、Lua 选项、
//! 设备 io-contexts / uring / 证书扩展等宿主设施的用例在 rust 侧无对应
//! 配置项，随实现落地补齐（见报告遗留清单）。

use wconf::{
  DEFAULT_MAX_INLINE_KEY_SIZE, GarnetServerOptions, OptionsError, ServerOptions,
  USE_DEFAULT_INITIAL_IO_RECORD_SIZE,
};

#[ctor::ctor(unsafe)]
fn _log_init() {
  log_init::init();
}

fn page_size_of(o: &GarnetServerOptions) -> i64 {
  1 << o.page_size_bits().unwrap()
}

// ======================== 默认值覆盖（DefaultConfigurationOptionsCoverage） ========================

/// defaults.conf 全量默认值在 rust 侧的对应字段落位
/// （C# DefaultConfigurationOptionsCoverage：defaults.conf 须覆盖全部选项）
#[test]
fn default_configuration_options_coverage() {
  let o = GarnetServerOptions::default();
  assert_eq!(o.log_memory_size, "16g");
  assert_eq!(o.page_size, "32m");
  assert_eq!(o.index_memory_size, "128m");
  assert_eq!(o.mutable_percent, 90);
  assert_eq!(o.read_cache_page_size, "32m");
  assert_eq!(o.segment_size, "1g");
  assert_eq!(o.aof_memory_size, "128m");
  assert_eq!(o.aof_page_size, "32m");
  assert_eq!(o.aof_segment_size, "1g");
  assert_eq!(o.pub_sub_page_size, "4k");
  // 可空配置项默认 None（C# defaults.conf 中为 null）
  assert!(o.max_inline_key_size.is_none());
  assert!(o.max_inline_value_size.is_none());
  assert!(o.initial_io_record_size.is_none());
  assert!(o.aof_size_limit.is_empty());
  assert!(!o.enable_cluster);
  assert!(!o.use_aof_null_device);
  assert_eq!(o.aof_physical_sublog_count, 1);

  // 旧版换算面（ServerOptions）默认值
  let s = ServerOptions::default();
  assert_eq!(s.log_memory_size, "16g");
  assert_eq!(s.page_size, "16m");
  assert_eq!(s.pub_sub_page_size, "4k");
  assert_eq!(s.segment_size, "1g");
  assert_eq!(s.object_log_segment_size, "1g");
  assert_eq!(s.index_memory_size, "128m");
}

/// C# OptionsDefaultAttributeUsage 的域内等价：换算面默认值不允许与
/// 代码内 fallback 双写（默认即 fallback 本体）
#[test]
fn options_default_single_source() {
  let s = ServerOptions::default();
  // MemorySizeBits 默认 16g → 34 位
  assert_eq!(s.memory_size_bits(), 34);
  // PageSizeBits 默认 16m → 24 位
  assert_eq!(s.page_size_bits().unwrap(), 24);
  // PubSubPageSizeBytes 默认 4k
  assert_eq!(s.pub_sub_page_size_bytes(), 4096);
  // SegmentSizeBits 默认 1g → 30 位（主存 / 对象两路）
  assert_eq!(s.segment_size_bits(false), 30);
  assert_eq!(s.segment_size_bits(true), 30);
  // IndexSizeCachelines 默认 128m
  assert_eq!(
    s.index_size_cachelines("IndexMemorySize", "128m").unwrap(),
    (128 << 20) / 64
  );
}

// ======================== 缓冲池预算（BufferPoolMemoryBudgetOptionParsing） ========================

/// C# BufferPoolMemoryBudgetOptionParsing：默认 / 覆盖 / 显式零
#[test]
fn buffer_pool_memory_budget_parsing() {
  // 默认（C# defaults.conf 为 1g；rust 默认空串 = 0 字节预算）
  let o = GarnetServerOptions::default();
  assert_eq!(o.get_buffer_pool_memory_budget_bytes(), 0);

  // 覆盖解析为字节
  let o = GarnetServerOptions {
    buffer_pool_memory_budget: "8g".into(),
    ..Default::default()
  };
  assert_eq!(o.get_buffer_pool_memory_budget_bytes(), 8i64 << 30);

  // "0" 通过尺寸校验，为显式关闭缓冲缓存
  let o = GarnetServerOptions {
    buffer_pool_memory_budget: "0".into(),
    ..Default::default()
  };
  assert_eq!(o.get_buffer_pool_memory_budget_bytes(), 0);
}

// ======================== AOF 尺寸族（AofSizeLimitWithoutAofEnabled / AofSegmentSizeFlowsToTsavoriteLogSettings） ========================

/// C# AofSizeLimitWithoutAofEnabled：AOF 大小上限独立解析；未设置 = 不限制
#[test]
fn aof_size_limit_without_aof_enabled() {
  let o = GarnetServerOptions::default();
  assert_eq!(o.aof_size_limit_size_bits().unwrap(), 0);

  let o = GarnetServerOptions {
    aof_size_limit: "64m".into(),
    ..Default::default()
  };
  assert_eq!(o.aof_size_limit_size_bits().unwrap(), 26);

  let o = GarnetServerOptions {
    aof_size_limit: "notasize".into(),
    ..Default::default()
  };
  assert!(matches!(
    o.aof_size_limit_size_bits(),
    Err(OptionsError::BadSize(_))
  ));
}

/// C# AofSegmentSizeFlowsToTsavoriteLogSettings：AOF 段尺寸流入日志设置
#[test]
fn aof_segment_size_flows_to_log_settings() {
  let mut o = GarnetServerOptions {
    aof_memory_size: "128m".into(),
    aof_page_size: "32m".into(),
    aof_segment_size: "1g".into(),
    ..Default::default()
  };

  let settings = o.get_aof_settings(0).unwrap();
  assert_eq!(settings.len(), 1);
  let s = &settings[0];
  assert_eq!(s.directory, "AOF");
  assert_eq!(s.log_file, "aof.log");
  assert_eq!(s.memory_size_bits, 27);
  assert_eq!(s.page_size_bits, 25);
  assert_eq!(s.segment_size_bits, 30);

  // 多物理子日志：逐日志命名流段设置
  o.aof_physical_sublog_count = 3;
  let settings = o.get_aof_settings(0).unwrap();
  assert_eq!(settings.len(), 3);
  assert_eq!(settings[0].log_file, "aof.0.log");
  assert_eq!(settings[2].log_file, "aof.2.log");
  for s in &settings {
    assert_eq!(s.segment_size_bits, 30);
  }
}

// ======================== 内联值尺寸（MaxInlineValueSize* 四用例） ========================

/// C# MaxInlineValueSizeParsing：默认按页推算；合法尺寸串逐项解析
#[test]
fn max_inline_value_size_parsing() {
  // 默认 null：16m 页 → min(1m, page/2) = 1m
  let o = GarnetServerOptions::default();
  assert!(o.max_inline_value_size.is_none());
  let page = page_size_of(&o);
  assert_eq!(o.max_inline_value_size_bytes(page).unwrap(), 1048576);

  // 合法尺寸串（1g 页放宽页适配上限）
  let mut o = GarnetServerOptions {
    page_size: "1g".into(),
    ..Default::default()
  };
  let page = page_size_of(&o);
  for (input, expected) in [
    ("64", 64),
    ("1k", 1024),
    ("4k", 4096),
    ("1m", 1048576),
    ("15m", 15 * 1048576),
  ] {
    o.max_inline_value_size = Some(input.into());
    assert_eq!(
      o.max_inline_value_size_bytes(page).unwrap(),
      expected,
      "'{input}' 应为 {expected} 字节"
    );
  }
}

/// C# MaxInlineValueSizeValidation：非法格式 / 越界 / 边界值
#[test]
fn max_inline_value_size_validation() {
  let mut o = GarnetServerOptions {
    page_size: "1g".into(),
    ..Default::default()
  };
  let page = page_size_of(&o);

  // 非法格式（含 C# 正则回归 '4|'）
  for bad in ["abc", "4|", "xyz"] {
    o.max_inline_value_size = Some(bad.into());
    assert!(
      matches!(
        o.max_inline_value_size_bytes(page),
        Err(OptionsError::BadSize(_))
      ),
      "'{bad}' 应解析失败"
    );
  }

  // 低于下限（-1 在消费期拒绝）
  o.max_inline_value_size = Some("-1".into());
  assert!(o.max_inline_value_size_bytes(page).is_err());

  // 高于绝对上限（17m > 16777214）
  o.max_inline_value_size = Some("17m".into());
  assert!(matches!(
    o.max_inline_value_size_bytes(page),
    Err(OptionsError::OutOfRange(_, _))
  ));

  // 恰好下限 0
  o.max_inline_value_size = Some("0".into());
  assert_eq!(o.max_inline_value_size_bytes(page).unwrap(), 0);

  // 恰好上限 16777214（0xFFFFFE）
  o.max_inline_value_size = Some("16777214".into());
  assert_eq!(o.max_inline_value_size_bytes(page).unwrap(), 16777214);
}

/// C# MaxInlineValueSizeMustFitOnPage：值尺寸须 ≤ 页/2
#[test]
fn max_inline_value_size_must_fit_on_page() {
  // 等于页大小 → 拒绝
  let o = GarnetServerOptions {
    page_size: "4k".into(),
    max_inline_value_size: Some("4k".into()),
    ..Default::default()
  };
  let page = page_size_of(&o);
  assert!(matches!(
    o.max_inline_value_size_bytes(page),
    Err(OptionsError::ExceedsHalfPage(_, _, _))
  ));

  // 大于页大小 → 拒绝
  let o = GarnetServerOptions {
    page_size: "1k".into(),
    max_inline_value_size: Some("4k".into()),
    ..Default::default()
  };
  let page = page_size_of(&o);
  assert!(o.max_inline_value_size_bytes(page).is_err());

  // 恰为页/2 → 接受
  let o = GarnetServerOptions {
    page_size: "8k".into(),
    max_inline_value_size: Some("4k".into()),
    ..Default::default()
  };
  let page = page_size_of(&o);
  assert_eq!(o.max_inline_value_size_bytes(page).unwrap(), 4096);

  // 严格小于页/2 → 原样返回（5k = 5120，引擎可内部下取整）
  let o = GarnetServerOptions {
    page_size: "16k".into(),
    max_inline_value_size: Some("5k".into()),
    ..Default::default()
  };
  let page = page_size_of(&o);
  assert_eq!(o.max_inline_value_size_bytes(page).unwrap(), 5120);
}

/// C# MaxInlineValueSizeDefaultBehavior：未设置时 min(页/2, 1m)
#[test]
fn max_inline_value_size_default_behavior() {
  let mut o = GarnetServerOptions {
    max_inline_value_size: None,
    ..Default::default()
  };

  // 页 ≥ 2m → 默认 1m
  for page_str in ["4m", "2m", "16m"] {
    o.page_size = page_str.into();
    let page = page_size_of(&o);
    assert_eq!(
      o.max_inline_value_size_bytes(page).unwrap(),
      1048576,
      "页 {page_str} 下默认应为 1m"
    );
  }

  // 页 < 2m → 默认页/2
  o.page_size = "1m".into();
  let page = page_size_of(&o);
  assert_eq!(o.max_inline_value_size_bytes(page).unwrap(), 524288);

  o.page_size = "4k".into();
  let page = page_size_of(&o);
  assert_eq!(o.max_inline_value_size_bytes(page).unwrap(), 2048);

  // 显式设置 ≤ 页/2 → 原样
  o.page_size = "4m".into();
  o.max_inline_value_size = Some("1m".into());
  let page = page_size_of(&o);
  assert_eq!(o.max_inline_value_size_bytes(page).unwrap(), 1048576);

  // 显式设置超页/2 → 拒绝
  o.page_size = "1m".into();
  let page = page_size_of(&o);
  assert!(matches!(
    o.max_inline_value_size_bytes(page),
    Err(OptionsError::ExceedsHalfPage(_, _, _))
  ));
}

// ======================== 内联键尺寸（MaxInlineKeySize* 两用例） ========================

/// C# MaxInlineKeySizeParsing：默认 1022；合法尺寸串逐项解析
#[test]
fn max_inline_key_size_parsing() {
  let o = GarnetServerOptions::default();
  assert!(o.max_inline_key_size.is_none());
  assert_eq!(o.max_inline_key_size_bytes().unwrap(), 1022);

  let mut o = GarnetServerOptions::default();
  for (input, expected) in [
    ("0", 0),
    ("64", 64),
    ("128", 128),
    ("512", 512),
    ("1022", 1022),
  ] {
    o.max_inline_key_size = Some(input.into());
    assert_eq!(
      o.max_inline_key_size_bytes().unwrap(),
      expected,
      "'{input}' 应为 {expected} 字节"
    );
  }
}

/// C# MaxInlineKeySizeValidation：非法格式 / 越界 / 边界值
#[test]
fn max_inline_key_size_validation() {
  let mut o = GarnetServerOptions::default();

  // 非法格式
  for bad in ["abc", "xyz"] {
    o.max_inline_key_size = Some(bad.into());
    assert!(matches!(
      o.max_inline_key_size_bytes(),
      Err(OptionsError::BadSize(_))
    ));
  }

  // 低于下限
  o.max_inline_key_size = Some("-1".into());
  assert!(o.max_inline_key_size_bytes().is_err());

  // 高于上限（1023 / 1k）
  for over in ["1023", "1k"] {
    o.max_inline_key_size = Some(over.into());
    assert!(
      matches!(
        o.max_inline_key_size_bytes(),
        Err(OptionsError::OutOfRange(_, _))
      ),
      "'{over}' 应越界拒绝"
    );
  }

  // 边界：0 与 1022
  o.max_inline_key_size = Some("0".into());
  assert_eq!(o.max_inline_key_size_bytes().unwrap(), 0);
  o.max_inline_key_size = Some("1022".into());
  assert_eq!(o.max_inline_key_size_bytes().unwrap(), 1022);

  // 默认常量与上限一致
  assert_eq!(DEFAULT_MAX_INLINE_KEY_SIZE, 1022);
}

// ======================== 最小页大小（MinimumPageSize / MinimumReadCachePageSize） ========================

/// C# MinimumPageSize：页尺寸下限 512B（384B 下取 256B 亦拒绝）
#[test]
fn minimum_page_size() {
  let mut o = GarnetServerOptions::default();

  for too_small in ["256", "384"] {
    o.page_size = too_small.into();
    let err = o.page_size_bits().unwrap_err();
    assert!(
      matches!(err, OptionsError::PageSizeTooSmall(_, _, 512)),
      "页 {too_small} 应低于 512B 下限拒绝: {err:?}"
    );
  }

  o.page_size = "512".into();
  assert_eq!(o.page_size_bits().unwrap(), 9);

  o.page_size = "1k".into();
  assert_eq!(o.page_size_bits().unwrap(), 10);
}

/// C# MinimumReadCachePageSize：读缓存页尺寸同一下限
#[test]
fn minimum_read_cache_page_size() {
  let mut o = GarnetServerOptions::default();

  for too_small in ["256", "384"] {
    o.read_cache_page_size = too_small.into();
    assert!(
      matches!(
        o.read_cache_page_size_bits(),
        Err(OptionsError::PageSizeTooSmall(_, _, 512))
      ),
      "读缓存页 {too_small} 应低于 512B 下限拒绝"
    );
  }

  o.read_cache_page_size = "512".into();
  assert_eq!(o.read_cache_page_size_bits().unwrap(), 9);

  o.read_cache_page_size = "1k".into();
  assert_eq!(o.read_cache_page_size_bits().unwrap(), 10);
}

// ======================== 初始 IO 记录尺寸（InitialIORecordSizeParsing） ========================

/// C# InitialIORecordSizeParsing：未设置哨兵 0；合法尺寸串逐项解析
#[test]
fn initial_io_record_size_parsing() {
  let o = GarnetServerOptions::default();
  assert!(o.initial_io_record_size.is_none());
  let page = page_size_of(&o);
  assert_eq!(
    o.get_initial_io_record_size_bytes(page).unwrap(),
    USE_DEFAULT_INITIAL_IO_RECORD_SIZE as i32
  );

  let mut o = GarnetServerOptions::default();
  for (input, expected) in [("24", 24), ("4k", 4096), ("8k", 8192)] {
    o.initial_io_record_size = Some(input.into());
    assert_eq!(
      o.get_initial_io_record_size_bytes(page).unwrap(),
      expected,
      "'{input}' 应为 {expected} 字节"
    );
  }

  // 非法格式 / 非正数 / 超页
  o.initial_io_record_size = Some("abc".into());
  assert!(o.get_initial_io_record_size_bytes(page).is_err());
  o.initial_io_record_size = Some("0".into());
  assert!(matches!(
    o.get_initial_io_record_size_bytes(page),
    Err(OptionsError::NotPositive(_, _))
  ));
  o.initial_io_record_size = Some("64m".into());
  assert!(matches!(
    o.get_initial_io_record_size_bytes(page),
    Err(OptionsError::ExceedsPageSize(_, _, _))
  ));
}
