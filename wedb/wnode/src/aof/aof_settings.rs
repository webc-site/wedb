//! AOF 装配期尺寸体检与物理尺寸投影单点。
//!
//! C# 侧 `GarnetServerOptions` 的 GetAofSettings（:1048）是三尺寸旋钮
//! （AofMemorySize / AofPageSize / AofSegmentSize）的唯一读出口：先做一次组合
//! 体检，非法组合直接抛出点名配置项的异常拒启（不留到运行期），随后把
//! memory/page/segment 一次性投影进每个物理子日志的设置（:1110-1117）。
//!
//! rust 同形：[`AofSettings::from_options`] 为全仓唯一读点与唯一体检口，
//! [`AofSettings::wal_config`] 为物理日志设置的唯一装载口，
//! [`AofSettings::device_segment`] 为物理段设备分段口径的唯一注入口。后续启动期组合校验
//! （如提交频率与 wait-for-commit 的组合）在本函数体内接续，共用本入口与同一
//! 错误类型，禁第二套参数体检函数。页尺寸旋钮自身不在此定规：其「取幂 +
//! `wconf::size::MIN_PAGE_SIZE_BYTES` 下限 + 位宽」与主存日志 / read cache 共用
//! wconf 的同一校验核（C# ServerOptions.cs:ValidatedPageSizeBits 复用形态），
//! 本文件只组合互校验、不重写单页下限判定。

use std::fmt;

use waof::WalConfig;
use wbase::align::DEFAULT_SECTOR_SIZE;
use wconf::{
  RuntimeServerOptions,
  size::{log2_exact, pretty_size, previous_power_of_2, try_parse_size, validated_page_size_bits},
};

use crate::Error;

/// 配置项名（CONFIG 面同名，错误文案点名用；禁散落裸字符串）
const MEMORY_OPT: &str = "aof-memory";
const PAGE_OPT: &str = "aof-page-size";
const SEGMENT_OPT: &str = "aof-segment-size";

/// AOF 物理子日志尺寸三元组（C# 子日志设置的 MemorySize / PageSize /
/// SegmentSize 在 rust 的字节形态；位宽就近下取 2 的幂后唯一定值）
#[derive(Debug, Clone, Copy)]
pub struct AofSettings {
  /// 常驻内存环形窗口上限（aof-memory 投影，物理侧即 `WalConfig::buffer_size`）
  pub memory_size_bytes: usize,
  /// 日志页容量（aof-page-size 投影，AOF 分块写入的片上界来源）
  pub page_size_bytes: usize,
  /// 物理段容量（aof-segment-size 投影，设备分段口径）
  pub segment_size_bytes: u64,
}

impl AofSettings {
  /// libs/server/Servers/GarnetServerOptions.cs:GetAofSettings
  ///
  /// 一处读取三旋钮 → 三条组合互校验（C# 同位同序）→ 一次性投影。
  /// 任一非法组合返回点名配置项的参数错误，装配链据此拒启。校验三的主存页
  /// 位宽吃 `options.hlog_page_size` 实配投影（C# 读同对象 PageSizeBits()
  /// 同源；boot 装配口从 `StoreConfig::page_size` 单点注入）。
  pub fn from_options(options: &RuntimeServerOptions) -> crate::Result<Self> {
    let memory_bits = knob_bits(MEMORY_OPT, options.aof_memory_size.as_deref())?;
    let page_bits = page_knob_bits(PAGE_OPT, options.aof_page_size.as_deref())?;
    let segment_bits = knob_bits(SEGMENT_OPT, options.aof_segment_size.as_deref())?;

    // 校验一：常驻窗口须容得下至少两页（C# 同位——先于设备分配挡下，
    // 否则只有物理层不带 AOF 字样的泛消息可看）
    if memory_bits <= page_bits {
      return Err(param_error(format_args!(
        "{MEMORY_OPT}（生效 {memory}，尺寸已下取 2 的幂）必须至少为 {PAGE_OPT}（生效 {page}）的两倍，\
         请调大 {MEMORY_OPT} 至至少 {min_memory}，或调小 {PAGE_OPT}",
        memory = pretty_size(1 << memory_bits),
        page = pretty_size(1 << page_bits),
        min_memory = pretty_size(1 << (page_bits + 1)),
      )));
    }

    // 校验二：页不得大于段（段是页的物理容器）
    if page_bits > segment_bits {
      return Err(param_error(format_args!(
        "{PAGE_OPT}（生效 {page}，尺寸已下取 2 的幂）不得大于 {SEGMENT_OPT}（生效 {segment}），\
         请调大 {SEGMENT_OPT} 至至少 {page}，或调小 {PAGE_OPT}",
        page = pretty_size(1 << page_bits),
        segment = pretty_size(1 << segment_bits),
      )));
    }

    // 校验三：页须容得下最大的单条非分块主存记录镜像（实配主存页的两倍；
    // C# mainPageSizeBits = PageSizeBits() 同源——实配主存页位随
    // RuntimeServerOptions::hlog_page_size 进本口，用户改 --hlog-page-size
    // 后同 C# 拒启，不再按编译期 16m 常量放行）
    let main_page_bits = log2_exact(previous_power_of_2(options.hlog_page_size as i64)) as u32;
    if page_bits < main_page_bits + 1 {
      return Err(param_error(format_args!(
        "{PAGE_OPT}（生效 {page}，尺寸已下取 2 的幂）必须至少为主存日志页容量（生效 {main_page}）\
         的两倍，请调大 {PAGE_OPT} 至至少 {min_page}（并把 {MEMORY_OPT} 调至至少 \
         {min_memory}），或调小 --hlog-page-size",
        page = pretty_size(1 << page_bits),
        main_page = pretty_size(1 << main_page_bits),
        min_page = pretty_size(1 << (main_page_bits + 1)),
        min_memory = pretty_size(1 << (main_page_bits + 2)),
      )));
    }

    Ok(Self {
      memory_size_bytes: window_bytes(MEMORY_OPT, memory_bits)?,
      page_size_bytes: window_bytes(PAGE_OPT, page_bits)?,
      segment_size_bytes: 1 << segment_bits,
    })
  }

  /// 投影为物理日志设置（C# 逐子日志装载设置的尺寸段；扇区对齐与同步策略
  /// 仍由设备与缺省承担，本口不越权定值）
  #[must_use]
  pub fn wal_config(&self) -> WalConfig {
    WalConfig {
      buffer_size: self.memory_size_bytes,
      page_size: self.page_size_bytes,
      ..WalConfig::default()
    }
  }

  /// 物理段设备构造口径（段字节尺寸, 扇区对齐）
  ///
  /// C# GarnetServerOptions.GetAofSettings 中 SegmentSizeBits 投影
  /// TsavoriteLogSettings、AllocatorBase.Initialize
  /// `LogDevice.Initialize(1L << SegmentSizeBits, ...)` 的 rust 字节形态：
  /// [`wdev::SegmentedDevice`] 分段装配的段容量与扇区两参唯一真源，禁在
  /// 装配处散落二次定值
  #[must_use]
  pub fn device_segment(&self) -> (u64, usize) {
    (self.segment_size_bytes, DEFAULT_SECTOR_SIZE)
  }
}

/// libs/server/Servers/GarnetServerOptions.cs:AofMemorySizeBits
/// libs/server/Servers/GarnetServerOptions.cs:AofSegmentSizeBits
///
/// 单个尺寸旋钮 → 位宽（解析失败即点名配置项拒启；尺寸就近下取 2 的幂，
/// 与 C# PreviousPowerOf2 + Log2 同口径，缺省/空值按 0 位参与互校验）。
/// C# 三份近亲方法（仅字段名之差）在 rust 收敛为本函数 + 配置项名入参；
/// 页旋钮另走 [`page_knob_bits`]（多一道下限校验）。
fn knob_bits(opt: &'static str, raw: Option<&str>) -> crate::Result<u32> {
  let size = parse_knob_size(opt, raw)?;
  // previous_power_of_2 对非正尺寸归 0、对正 i64 上界 2^62，log2 结果恒落 0..=62
  Ok(log2_exact(previous_power_of_2(size)) as u32)
}

/// libs/server/Servers/GarnetServerOptions.cs:AofPageSizeBits
///
/// AOF 页尺寸旋钮 → 位宽：解析后交 wconf 页尺寸校验核
/// （`wconf::size::validated_page_size_bits`，对标 C# ServerOptions.cs
/// ValidatedPageSizeBits），取幂 + `MIN_PAGE_SIZE_BYTES` 下限 + 位宽与主存日志 /
/// read cache 页容量同一入口，本口不另写判定。
fn page_knob_bits(opt: &'static str, raw: Option<&str>) -> crate::Result<u32> {
  let size = parse_knob_size(opt, raw)?;
  validated_page_size_bits(size, opt).map_err(|e| param_error(format_args!("{e}")))
}

/// 尺寸旋钮原文 → 字节（无法整体解析即点名配置项拒启）
fn parse_knob_size(opt: &'static str, raw: Option<&str>) -> crate::Result<i64> {
  let text = raw.unwrap_or_default();
  try_parse_size(text).ok_or_else(|| {
    param_error(format_args!(
      "{opt} 配置值 '{text}' 无法解析为内存尺寸（形如 128m、1g）"
    ))
  })
}

/// 位宽 → 字节（62 位内恒可承载，平台地址空间不足时点名配置项拒启）
fn window_bytes(opt: &str, bits: u32) -> crate::Result<usize> {
  usize::try_from(1u64 << bits).map_err(|_| {
    param_error(format_args!(
      "{opt}（生效 {}）超出本平台地址空间",
      pretty_size(1 << bits)
    ))
  })
}

/// 装配期参数非法错误（与 hlog 内存/页组合校验同一错误类型，禁第二套）
fn param_error(msg: fmt::Arguments) -> Error {
  Error::InvalidArgument(msg.to_string())
}

#[cfg(test)]
mod tests {
  use std::sync::Arc;

  use waof::WalLog;
  use wconf::size::MIN_PAGE_SIZE_BYTES;
  use wdev::SegmentedDevice;

  use super::{AofSettings, MEMORY_OPT, PAGE_OPT, RuntimeServerOptions, SEGMENT_OPT, pretty_size};
  use crate::aof::waof_sublog::WaofSublog;

  /// 非缺省合法组合：64m 窗口 / 32m 页 / 1g 段（页恰为主存 16m 页的两倍下界）
  fn legal_options() -> RuntimeServerOptions {
    RuntimeServerOptions {
      aof_memory_size: Some("64m".into()),
      ..RuntimeServerOptions::default()
    }
  }

  /// 校验一：常驻窗口容不下两页即拒启，文案点名 aof-memory 与 aof-page-size
  #[test]
  fn rejects_memory_not_twice_page() {
    let options = RuntimeServerOptions {
      aof_memory_size: Some("32m".into()),
      aof_page_size: Some("32m".into()),
      ..RuntimeServerOptions::default()
    };
    let msg = AofSettings::from_options(&options)
      .expect_err("窗口与页等值（不足两倍）必须拒启")
      .to_string();
    assert!(
      msg.contains(MEMORY_OPT) && msg.contains(PAGE_OPT),
      "文案须点名两侧配置项: {msg}"
    );
  }

  /// 校验二：页大于段即拒启，文案点名 aof-page-size 与 aof-segment-size
  #[test]
  fn rejects_page_over_segment() {
    let options = RuntimeServerOptions {
      aof_memory_size: Some("256m".into()),
      aof_page_size: Some("128m".into()),
      aof_segment_size: Some("32m".into()),
      ..RuntimeServerOptions::default()
    };
    let msg = AofSettings::from_options(&options)
      .expect_err("页大于段必须拒启")
      .to_string();
    assert!(
      msg.contains(PAGE_OPT) && msg.contains(SEGMENT_OPT),
      "文案须点名两侧配置项: {msg}"
    );
  }

  /// 校验三：页小于实配主存页两倍即拒启，文案点名 aof-page-size、给出按
  /// 实配值折算的可下调界（吃 `hlog_page_size` 实配投影，非缺省 16m 常量）
  #[test]
  fn rejects_page_below_main_page_floor() {
    const MAIN_PAGE: usize = 4 * 1024 * 1024;
    let options = RuntimeServerOptions {
      aof_memory_size: Some("64m".into()),
      aof_page_size: Some("4m".into()),
      hlog_page_size: MAIN_PAGE,
      ..RuntimeServerOptions::default()
    };
    let msg = AofSettings::from_options(&options)
      .expect_err("页低于实配主存页两倍下界必须拒启")
      .to_string();
    assert!(msg.contains(PAGE_OPT), "文案须点名 aof-page-size: {msg}");
    assert!(
      msg.contains(&pretty_size((MAIN_PAGE * 2) as i64)),
      "文案须按实配主存页给出可调下界: {msg}"
    );
  }

  /// 实配投影接线（票 wnode-aof-main-page-bits-unwired）：`--hlog-page-size 4m`
  /// 后 AOF 页 8m 合法放行（编译期 16m 常量口径误拒），4m 页按实配下界拒启
  /// 并引导 --hlog-page-size——C# GetAofSettings 改 PageSize 后拒启的同位语义
  #[test]
  fn main_page_floor_follows_configured_hlog_page_size() {
    const MAIN_PAGE: usize = 4 * 1024 * 1024;
    let base = RuntimeServerOptions {
      aof_memory_size: Some("64m".into()),
      hlog_page_size: MAIN_PAGE,
      ..RuntimeServerOptions::default()
    };

    let ok = RuntimeServerOptions {
      aof_page_size: Some("8m".into()),
      ..base.clone()
    };
    let settings = AofSettings::from_options(&ok).expect("页 = 实配主存页两倍须放行");
    assert_eq!(settings.page_size_bytes, 8 * 1024 * 1024);

    let floor = RuntimeServerOptions {
      aof_page_size: Some("4m".into()),
      ..base
    };
    let msg = AofSettings::from_options(&floor)
      .expect_err("页等于实配主存页（不足两倍）必须拒启")
      .to_string();
    assert!(
      msg.contains(&pretty_size((MAIN_PAGE * 2) as i64)) && msg.contains("--hlog-page-size"),
      "文案须按实配主存页给出下界并引导 --hlog-page-size: {msg}"
    );
  }

  /// 页尺寸下限由 wconf 校验核在场：256 字节的 aof-page-size 在旋钮读取处即拒，
  /// 文案点名本配置项与 MIN_PAGE_SIZE_BYTES（证伪「小页静默接受」）
  #[test]
  fn rejects_page_below_min_page_size_floor() {
    let options = RuntimeServerOptions {
      aof_page_size: Some("256".into()),
      ..RuntimeServerOptions::default()
    };
    let msg = AofSettings::from_options(&options)
      .expect_err("低于页容量下限的页尺寸必须拒启")
      .to_string();
    assert!(
      msg.contains(PAGE_OPT)
        && msg.contains(&MIN_PAGE_SIZE_BYTES.to_string())
        && msg.contains("256"),
      "文案须点名 {PAGE_OPT} 与页容量下限字节: {msg}"
    );
  }

  /// 缺省组合合法且尺寸非 2 的幂时就近下取（C# PreviousPowerOf2 同口径）
  #[test]
  fn accepts_defaults_and_rounds_down() {
    let defaults = AofSettings::from_options(&RuntimeServerOptions::default())
      .expect("缺省组合（128m/32m/1g）须通过");
    assert_eq!(defaults.memory_size_bytes, 128 * 1024 * 1024);
    assert_eq!(defaults.page_size_bytes, 32 * 1024 * 1024);
    assert_eq!(defaults.segment_size_bytes, 1024 * 1024 * 1024);

    let rounded = RuntimeServerOptions {
      aof_memory_size: Some("2500m".into()),
      aof_page_size: Some("33m".into()),
      aof_segment_size: Some("1500m".into()),
      ..RuntimeServerOptions::default()
    };
    let settings = AofSettings::from_options(&rounded).expect("合法组合须通过");
    assert_eq!(settings.memory_size_bytes, 2 * 1024 * 1024 * 1024);
    assert_eq!(settings.page_size_bytes, 32 * 1024 * 1024);
    assert_eq!(settings.segment_size_bytes, 1024 * 1024 * 1024);
  }

  /// 合法组合实测：物理日志页位与常驻窗口上限等于配置生效值（投影真落地，
  /// 非 CONFIG GET 回显）；设备按生产装配同一口径构造，段容量取
  /// [`AofSettings::device_segment`] 唯一注入口
  #[test]
  fn projected_sizes_are_physically_honored() {
    let options = legal_options();
    let settings = AofSettings::from_options(&options).expect("合法组合须通过");
    let dir = tempfile::tempdir().expect("tempdir");
    let (segment_size, sector_size) = settings.device_segment();
    let device = Arc::new(
      SegmentedDevice::new(dir.path().join("projected.wal"), segment_size, sector_size)
        .expect("SegmentedDevice"),
    );
    assert_eq!(
      device.segment_size(),
      segment_size,
      "设备段容量须等于 aof-segment-size 生效值"
    );
    let wal = WalLog::new(device, settings.wal_config()).expect("WalLog 装配");
    let sublog = WaofSublog::new(Arc::new(wal));
    // 64m 窗口 → 32m 页：页位 25、窗口上限 64MB，两者皆来自配置而非设备缺省
    assert_eq!(
      sublog.log_page_size_bits(),
      25,
      "页位须等于 aof-page-size 生效值"
    );
    assert_eq!(
      sublog.max_memory_size_bytes(),
      64 * 1024 * 1024,
      "常驻窗口上限须等于 aof-memory 生效值"
    );
  }
}
