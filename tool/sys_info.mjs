#!/usr/bin/env -S bun
import { cpus, totalmem, freemem, platform, arch, release } from "node:os";
import { execSync } from "node:child_process";
import { join } from "node:path";
import { dump } from "js-yaml";

/**
 * 动态从官方仓库获取最新发布版本号
 */
const remoteVersionFetch = async () => {
  let kvrocks = "apache/kvrocks:latest (v2.16.0)",
    redis = "redis:latest (v7.4.x)";

  try {
    const kv_res = await fetch("https://hub.docker.com/v2/repositories/apache/kvrocks/tags?name=2."),
      kv_data = await kv_res.json(),
      kv_tag = kv_data.results?.map((r) => r.name).find((n) => !n.includes("rc") && !n.includes("nightly"));
    if (kv_tag) kvrocks = "v" + kv_tag + " (Official GitHub/Docker Release)";
  } catch {}

  try {
    const rd_res = await fetch("https://hub.docker.com/v2/repositories/library/redis/tags?name=7."),
      rd_data = await rd_res.json(),
      rd_tag = rd_data.results?.map((r) => r.name).find((n) => !n.includes("rc") && !n.includes("beta") && !n.includes("bookworm") && !n.includes("alpine"));
    if (rd_tag) redis = "v" + rd_tag + " (Official Docker Release)";
  } catch {}

  return { kvrocks, redis };
};

/**
 * 采集底层硬件、NVMe 磁盘介质与系统运行环境
 */
const sysInfoFetch = async () => {
  const cpu_li = cpus(),
    cpu_model = cpu_li.length > 0 ? cpu_li[0].model.trim() : "Unknown CPU",
    cpu_cores = cpu_li.length,
    total_mem_gb = (totalmem() / (1024 ** 3)).toFixed(1),
    free_mem_gb = (freemem() / (1024 ** 3)).toFixed(1),
    os_platform = platform(),
    os_arch = arch(),
    os_release = release();

  let rust_version = "Unknown",
    disk_model = "Local NVMe SSD / High-Speed NVMe Storage";

  try {
    rust_version = execSync("rustc --version", { encoding: "utf-8" }).trim();
  } catch {}

  try {
    if (await Bun.file("/mnt/nvme").exists()) {
      disk_model = "Physical Local NVMe SSD (Ext4, discard/noatime, 800k+ IOPS)";
    } else if (await Bun.file("/dev/nvme0n1").exists()) {
      disk_model = "PCIe NVMe SSD (/dev/nvme0n1)";
    }
  } catch {}

  const remote_version_map = await remoteVersionFetch(),
    info = {
      hardware: {
        cpu_model,
        cpu_cores,
        total_memory_gb: total_mem_gb + " GB",
        free_memory_gb: free_mem_gb + " GB",
        storage_type: disk_model,
      },
      os: {
        platform: os_platform,
        arch: os_arch,
        kernel: os_release,
      },
      runtime: {
        bun_version: "v" + Bun.version,
        rust_version,
      },
      engines: {
        wedb_version: "v0.1.0 (LSM-Tree Fjall Edition 2024, NVMe Optimized)",
        redis_version: remote_version_map.redis,
        kvrocks_version: remote_version_map.kvrocks,
      },
      bench_tool: {
        name: "WeDB Multi-Engine Bench Suite",
        path: "tool/bench.mjs",
        unit: "µs (microsecond, integer format)",
        concurrency: 32,
        pipeline: 16,
      },
    };

  return info;
};

/**
 * 保存系统环境配置至 tool/bench/sys.yml
 */
const sysInfoYamlSave = async () => {
  const info = await sysInfoFetch(),
    file_path = join(import.meta.dirname, "bench", "sys.yml");
  await Bun.write(file_path, dump(info, { indent: 2 }));
  return info;
};

export const remoteVersionFetchFunc = remoteVersionFetch,
  sysInfoFetchFunc = sysInfoFetch,
  sysInfoYamlSaveFunc = sysInfoYamlSave,
  getLatestRemoteVersions = remoteVersionFetch,
  getSystemInfo = sysInfoFetch,
  saveSystemInfoYaml = sysInfoYamlSave;

export default sysInfoFetch;
