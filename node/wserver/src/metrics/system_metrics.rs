use std::fs;

/// 系统 / 进程内存指标（对标 libs/server/Metrics/SystemMetrics.cs:SystemMetrics）。
///
/// C# 在 Windows 走 psapi P/Invoke（GetPerformanceInfo），其余平台读取
/// `System.Diagnostics.Process` 与 GC 内存信息。Rust 侧以 /proc（Linux）承接
/// 等价读取：VmSize/VmPeak/VmRSS/VmHWM/VmData/VmSwap、MemTotal；
/// 非 Linux 或字段缺失时返回 C# 的失败哨兵 -1。
pub struct SystemMetrics;

/// /proc 记量单位：kB（/proc/meminfo 与 /proc/self/status 的 Vm* 字段均以 kB 表达）。
const PROC_KB: i64 = 1024;

impl SystemMetrics {
  /// libs/server/Metrics/SystemMetrics.cs:GetPerformanceInfo
  ///
  /// Windows psapi `GetPerformanceInfo` P/Invoke 探测；非 Windows 平台调用
  /// 必然失败。Rust 侧无 psapi 面，恒返回 false（对齐 C# 调用失败路径），
  /// 结构体 `PerformanceInformation` 不跨平台建模（见 check/ignore 说明）。
  pub fn get_performance_info() -> bool {
    false
  }

  /// libs/server/Metrics/SystemMetrics.cs:GetTotalMemory
  ///
  /// 物理内存总量 / `units`。C# 非 Windows 路径读 GC TotalAvailableMemoryBytes
  /// （≈ 物理内存总量），此处以 /proc/meminfo 的 MemTotal 承接。
  pub fn get_total_memory(units: i64) -> i64 {
    let units = units.max(1);
    meminfo_kb("MemTotal:").map_or(-1, |kb| kb * PROC_KB / units)
  }

  /// libs/server/Metrics/SystemMetrics.cs:GetPhysicalAvailableMemory
  ///
  /// C# 非 Windows 路径为 `return -1`（已知缺口），Windows 走 psapi；
  /// 保持 1:1：非 Windows 平台返回 -1。
  pub fn get_physical_available_memory(_units: i64) -> i64 {
    -1
  }

  /// libs/server/Metrics/SystemMetrics.cs:GetPagedMemorySize
  ///
  /// 分页内存（.NET Linux 语义 ≈ 私有脏页 VmData）/ `units`。
  pub fn get_paged_memory_size(units: i64) -> i64 {
    let units = units.max(1);
    status_kb(&["VmData:"]).map_or(-1, |kb| kb * PROC_KB / units)
  }

  /// libs/server/Metrics/SystemMetrics.cs:GetPagedSystemMemorySize
  ///
  /// .NET 仅在 Windows 解析内核分页系统内存；Linux 无对应 /proc 字段，
  /// 与 C# 的失败路径一致返回 -1。
  pub fn get_paged_system_memory_size(_units: i64) -> i64 {
    -1
  }

  /// libs/server/Metrics/SystemMetrics.cs:GetPeakPagedMemorySize
  ///
  /// 分页内存峰值（VmPeak 的数据段近似不可得，取 VmData；
  /// 峰值语义缺失时与 C# 的 0 值路径一致）。
  pub fn get_peak_paged_memory_size(units: i64) -> i64 {
    let units = units.max(1);
    status_kb(&["VmData:"]).map_or(-1, |kb| kb * PROC_KB / units)
  }

  /// libs/server/Metrics/SystemMetrics.cs:GetVirtualMemorySize64
  ///
  /// 进程虚拟内存（VmSize）/ `units`。
  pub fn get_virtual_memory_size64(units: i64) -> i64 {
    let units = units.max(1);
    status_kb(&["VmSize:"]).map_or(-1, |kb| kb * PROC_KB / units)
  }

  /// libs/server/Metrics/SystemMetrics.cs:GetPrivateMemorySize64
  ///
  /// 私有提交内存（.NET Linux 语义 ≈ VmData + VmSwap）/ `units`。
  pub fn get_private_memory_size64(units: i64) -> i64 {
    let units = units.max(1);
    status_kb(&["VmData:", "VmSwap:"]).map_or(-1, |kb| kb * PROC_KB / units)
  }

  /// libs/server/Metrics/SystemMetrics.cs:GetPeakVirtualMemorySize64
  ///
  /// 虚拟内存峰值（VmPeak）/ `units`。
  pub fn get_peak_virtual_memory_size64(units: i64) -> i64 {
    let units = units.max(1);
    status_kb(&["VmPeak:"]).map_or(-1, |kb| kb * PROC_KB / units)
  }

  /// libs/server/Metrics/SystemMetrics.cs:GetPhysicalMemoryUsage
  ///
  /// 物理内存占用（工作集 VmRSS）/ `units`。
  pub fn get_physical_memory_usage(units: i64) -> i64 {
    let units = units.max(1);
    status_kb(&["VmRSS:"]).map_or(-1, |kb| kb * PROC_KB / units)
  }

  /// libs/server/Metrics/SystemMetrics.cs:GetPeakPhysicalMemoryUsage
  ///
  /// 物理内存占用峰值（VmHWM）/ `units`。
  pub fn get_peak_physical_memory_usage(units: i64) -> i64 {
    let units = units.max(1);
    status_kb(&["VmHWM:"]).map_or(-1, |kb| kb * PROC_KB / units)
  }
}

/// 从 /proc/meminfo 提取指定行（如 "MemTotal:"）的 kB 数值。
fn meminfo_kb(field: &str) -> Option<i64> {
  let content = fs::read_to_string("/proc/meminfo").ok()?;
  parse_proc_kb(&content, field)
}

/// 从 /proc/self/status 累加多个字段的 kB 数值。
fn status_kb(fields: &[&str]) -> Option<i64> {
  let content = fs::read_to_string("/proc/self/status").ok()?;
  fields
    .iter()
    .try_fold(0i64, |acc, f| Some(acc + parse_proc_kb(&content, f)?))
}

/// 在 /proc 文本中定位 `field` 行并解析其 kB 数值（无正则、单趟扫描）。
fn parse_proc_kb(content: &str, field: &str) -> Option<i64> {
  content.lines().find_map(|line| {
    let rest = line.strip_prefix(field)?;
    // 行形如 "  16384256 kB"，取首个数值 token。
    rest
      .split_whitespace()
      .next()
      .and_then(|token| token.parse::<i64>().ok())
  })
}

#[cfg(test)]
mod tests {
  use super::{SystemMetrics, parse_proc_kb};

  #[test]
  fn parse_proc_fields() {
    let status =
      "VmPeak:\t 13408844 kB\nVmSize:\t 13408840 kB\nVmData:\t 4031804 kB\nVmSwap:\t       0 kB\n";
    assert_eq!(parse_proc_kb(status, "VmData:"), Some(4_031_804));
    assert_eq!(parse_proc_kb(status, "VmSwap:"), Some(0));
    assert_eq!(parse_proc_kb(status, "VmRss:"), None);

    let meminfo = "MemTotal:       16384256 kB\nMemFree:         8422216 kB\n";
    assert_eq!(parse_proc_kb(meminfo, "MemTotal:"), Some(16_384_256));
  }

  #[test]
  fn metrics_contract() {
    // 非负或失败哨兵 -1；不 panic。
    let total = SystemMetrics::get_total_memory(1);
    assert!(total > 0 || total == -1);
    // C# 非 Windows 路径恒 -1。
    assert_eq!(SystemMetrics::get_physical_available_memory(1), -1);
    assert_eq!(SystemMetrics::get_paged_system_memory_size(1), -1);
    assert!(!SystemMetrics::get_performance_info());

    // units 为 0/负值时按 1 处理（除法安全）。
    assert!(SystemMetrics::get_physical_memory_usage(0) >= -1);
  }
}
