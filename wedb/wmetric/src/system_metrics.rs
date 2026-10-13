use std::fs;

/// 系统 / 进程内存指标（对标 libs/server/Metrics/SystemMetrics.cs:SystemMetrics）。
///
/// C# 在 Windows 走 psapi P/Invoke（GetPerformanceInfo），其余平台读取
/// `System.Diagnostics.Process` 与 GC 内存信息。Rust 侧以 /proc（Linux）承接
/// 等价读取：VmSize/VmPeak/VmRSS/VmHWM/VmData/VmSwap、MemTotal；
/// 非 Linux 或字段缺失时返回 C# 的失败哨兵 -1。
/// psapi 属微软平台绑定面，rust 侧无宿主，该分支不转写。
/// /proc 探针族系 Linux 生产宿主限定：非 Linux 宿主（darwin 开发机）
/// 八个名值行恒 -1 哨兵而 C# 非 Windows 路径在 macOS 出真值，
/// 登记锚 deviations.md §165c，对拍轮跳行。
pub struct SystemMetrics;

/// /proc 记量单位：kB（/proc/meminfo 与 /proc/self/status 的 Vm* 字段均以 kB 表达）。
const PROC_KB: i64 = 1024;

impl SystemMetrics {
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
    self_status_kb("VmData:").map_or(-1, |kb| kb * PROC_KB / units)
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
  /// 分页内存峰值：/proc 无数据段峰值计数器，与 get_paged_memory_size
  /// 同读 VmData（峰值行同源复制、峰值语义缺失；禁 fetch_max 真峰值化
  /// 在冷路径新增跨次状态，禁改接 VmPeak 虚拟峰值近似形），
  /// 登记锚 deviations.md §165b。
  pub fn get_peak_paged_memory_size(units: i64) -> i64 {
    let units = units.max(1);
    self_status_kb("VmData:").map_or(-1, |kb| kb * PROC_KB / units)
  }

  /// libs/server/Metrics/SystemMetrics.cs:GetVirtualMemorySize64
  ///
  /// 进程虚拟内存（VmSize）/ `units`。
  pub fn get_virtual_memory_size64(units: i64) -> i64 {
    let units = units.max(1);
    self_status_kb("VmSize:").map_or(-1, |kb| kb * PROC_KB / units)
  }

  /// libs/server/Metrics/SystemMetrics.cs:GetPrivateMemorySize64
  ///
  /// 私有提交内存（.NET Linux 语义 ≈ VmData + VmSwap）/ `units`（唯一多字段累加口）。
  pub fn get_private_memory_size64(units: i64) -> i64 {
    let units = units.max(1);
    status_kb(&["VmData:", "VmSwap:"]).map_or(-1, |kb| kb * PROC_KB / units)
  }

  /// libs/server/Metrics/SystemMetrics.cs:GetPeakVirtualMemorySize64
  ///
  /// 虚拟内存峰值（VmPeak）/ `units`。
  pub fn get_peak_virtual_memory_size64(units: i64) -> i64 {
    let units = units.max(1);
    self_status_kb("VmPeak:").map_or(-1, |kb| kb * PROC_KB / units)
  }

  /// libs/server/Metrics/SystemMetrics.cs:GetPhysicalMemoryUsage
  ///
  /// 物理内存占用（工作集 VmRSS）/ `units`。
  pub fn get_physical_memory_usage(units: i64) -> i64 {
    let units = units.max(1);
    self_status_kb("VmRSS:").map_or(-1, |kb| kb * PROC_KB / units)
  }

  /// libs/server/Metrics/SystemMetrics.cs:GetPeakPhysicalMemoryUsage
  ///
  /// 物理内存占用峰值（VmHWM）/ `units`。
  pub fn get_peak_physical_memory_usage(units: i64) -> i64 {
    let units = units.max(1);
    self_status_kb("VmHWM:").map_or(-1, |kb| kb * PROC_KB / units)
  }
}

/// 从 /proc/meminfo 提取指定行（如 "MemTotal:"）的 kB 数值。
fn meminfo_kb(field: &str) -> Option<i64> {
  let content = fs::read_to_string("/proc/meminfo").ok()?;
  parse_proc_kb(&content, field)
}

/// 从 /proc/self/status 提取单字段（如 "VmRSS:"）的 kB 数值（单字段口径单点）。
fn self_status_kb(field: &str) -> Option<i64> {
  let content = fs::read_to_string("/proc/self/status").ok()?;
  parse_proc_kb(&content, field)
}

/// 从 /proc/self/status 累加多个字段的 kB 数值（多字段累加语义，单趟扫描、匹配完成早退；
/// 单字段口一律走 [self_status_kb]，不走本函数）。
fn status_kb(fields: &[&str]) -> Option<i64> {
  let content = fs::read_to_string("/proc/self/status").ok()?;
  let mut total = 0i64;
  let mut found = 0usize;
  for line in content.lines() {
    for field in fields {
      if let Some(rest) = line.strip_prefix(field) {
        let val = rest.split_whitespace().next()?.parse::<i64>().ok()?;
        total += val;
        found += 1;
        if found == fields.len() {
          return Some(total);
        }
        break;
      }
    }
  }
  (found == fields.len()).then_some(total)
}

/// 在 /proc 文本中定位 `field` 行并解析其 kB 数值（无正则、单趟扫描）。
#[doc(hidden)]
pub fn parse_proc_kb(content: &str, field: &str) -> Option<i64> {
  content.lines().find_map(|line| {
    let rest = line.strip_prefix(field)?;
    // 行形如 "  16384256 kB"，取首个数值 token。
    rest
      .split_whitespace()
      .next()
      .and_then(|token| token.parse::<i64>().ok())
  })
}
