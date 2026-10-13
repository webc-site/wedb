#![recursion_limit = "512"] // 泛型设备实例化下 async 状态机嵌套深（R27 CI 实证）
use wmetric::{SystemMetrics, parse_proc_kb};

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
  // 域分叉锁定：Linux 宿主 /proc 必在（MemTotal > 0、VmRSS 经 units=0 门仍 > 0）；
  // 非 Linux 宿主八个 /proc 探针口全 -1 哨兵（deviations.md §165c 对拍轮跳行）。
  if cfg!(target_os = "linux") {
    assert!(SystemMetrics::get_total_memory(1) > 0);
    // units 为 0/负值时按 1 处理（除法安全、不 panic）
    assert!(SystemMetrics::get_physical_memory_usage(0) > 0);
  } else {
    assert_eq!(SystemMetrics::get_total_memory(1), -1);
    assert_eq!(SystemMetrics::get_paged_memory_size(1), -1);
    assert_eq!(SystemMetrics::get_peak_paged_memory_size(1), -1);
    assert_eq!(SystemMetrics::get_virtual_memory_size64(1), -1);
    assert_eq!(SystemMetrics::get_private_memory_size64(1), -1);
    assert_eq!(SystemMetrics::get_peak_virtual_memory_size64(1), -1);
    assert_eq!(SystemMetrics::get_physical_memory_usage(1), -1);
    assert_eq!(SystemMetrics::get_peak_physical_memory_usage(1), -1);
  }

  // C# 非 Windows 路径恒 -1（平台无关）。
  assert_eq!(SystemMetrics::get_physical_available_memory(1), -1);
  assert_eq!(SystemMetrics::get_paged_system_memory_size(1), -1);
}
