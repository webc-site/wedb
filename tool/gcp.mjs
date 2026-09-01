#!/usr/bin/env -S bun
import { $ } from "zx";
import { dump } from "js-yaml";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { execSync } from "node:child_process";
import { gzipSync, gunzipSync } from "node:zlib";

if (!process.env.CLOUDSDK_PYTHON) {
  try {
    const p = execSync("which python3", { encoding: "utf8" }).trim();
    if (p) process.env.CLOUDSDK_PYTHON = p;
  } catch {}
}
$.verbose = 1;

export const DEFAULT_ZONE = "us-central1-a",
  DEFAULT_REGION = "us-central1",
  DEFAULT_MACHINE_TYPE = "t2a-standard-4", // 4 ARM vCPU (Ampere Altra), 16 GB 内存, Spot 抢占实例
  DEFAULT_IMAGE_FAMILY = "ubuntu-2404-lts-arm64", // 最新 Ubuntu 24.04 LTS (Noble Numbat)
  DEFAULT_IMAGE_PROJECT = "ubuntu-os-cloud",
  DEFAULT_BOOT_DISK_SIZE = 50,           // 50 GB 高速 SSD 系统盘
  DEFAULT_BOOT_DISK_TYPE = "pd-ssd";     // 高性能持久化 SSD (NVMe 协议)

/**
 * 统一在远端安全执行 SSH 命名，自动注入完整 PATH 环境变量
 */
export const sshCommandRun = async (name, raw_command, zone = DEFAULT_ZONE, quiet = true) => {
  const wrapped_cmd = `export PATH="/opt/rust/cargo/bin:/opt/bun/bin:/usr/local/bin:/usr/sbin:/usr/bin:/bin:$PATH"; export RUSTUP_HOME=/opt/rust/rustup; export CARGO_HOME=/opt/rust/cargo; ${raw_command}`;
  const quiet_flag = quiet ? ["--quiet"] : [];
  return await $`gcloud compute ssh ${name} --zone=${zone} --tunnel-through-iap --command=${wrapped_cmd} ${quiet_flag}`;
};

/**
 * 轮询等待远端实例本地 127.0.0.1 端口就绪 (通过 SSH 内部验证，杜绝网络公网拦截与假死)
 */
const tcpPortWait = async (name, port, max_wait_sec = 90, zone = DEFAULT_ZONE) => {
  process.stdout.write("⏳ 等待实例 " + name + " 内部 127.0.0.1:" + port + " 就绪... ");
  const start = Date.now(),
    check_cmd = '( ss -tln 2>/dev/null || netstat -tln 2>/dev/null || true ) | grep -E ":' + port + '([[:space:]]|$)" || timeout 1 bash -c "</dev/tcp/127.0.0.1/' + port + '" 2>/dev/null';
  while (Date.now() - start < max_wait_sec * 1000) {
    try {
      await sshCommandRun(name, check_cmd, zone, true);
      console.log("✅ 就绪！");
      return;
    } catch {
      await Bun.sleep(1500);
    }
  }
  throw new Error("超时：等待 " + name + " 内部端口 " + port + " 就绪超过 " + max_wait_sec + " 秒");
};

/**
 * 确保开放压测所需防火墙端口
 */
const firewallRulesEnsure = async (prefix = "wedb-bench-") => {
  const fw_name = "allow-wedb-bench";
  try {
    await $`gcloud compute firewall-rules describe ${fw_name} --format="value(name)" --quiet`;
  } catch {
    console.log("🛡️ 创建压测防火墙规则: " + fw_name + "...");
    await $`gcloud compute firewall-rules create ${fw_name} --allow=tcp:22,tcp:4909,tcp:4910,tcp:6379,tcp:6666,tcp:16379,tcp:16666 --source-ranges=0.0.0.0/0,35.235.240.0/20 --description="Temporary firewall for WeDB benchmark" --quiet`;
  }
};

/**
 * 获取或创建 GCP 实例 (带启动脚本与物理 NVMe 智能适配)
 */
const spotInstanceCreate = async (
  name,
  startup_script,
  zone = DEFAULT_ZONE,
  machine_type = DEFAULT_MACHINE_TYPE,
  image_family = DEFAULT_IMAGE_FAMILY,
  image_project = DEFAULT_IMAGE_PROJECT
) => {
  await firewallRulesEnsure();

  try {
    const check = await $`gcloud compute instances describe ${name} --zone=${zone} --format=json --quiet`,
      info = JSON.parse(check.stdout),
      status = info.status;

    if (status === "RUNNING") {
      const ext_ip = info.networkInterfaces?.[0]?.accessConfigs?.[0]?.natIP || "",
        int_ip = info.networkInterfaces?.[0]?.networkIP || "";
      console.log("⚡ [GCP] 成功复用已存在实例 " + name + " (可用区: " + zone + ", 外网 IP: " + ext_ip + ", 内网 IP: " + int_ip + ")");
      return {
        name,
        zone,
        externalIp: ext_ip,
        internalIp: int_ip,
      };
    } else {
      console.log("⏳ [GCP] 实例 " + name + " 存在但状态为 " + status + "，正在清理并重建...");
      try {
        await $`gcloud compute instances delete ${name} --zone=${zone} --quiet`;
      } catch {}
    }
  } catch {}

  const startup_path = join(tmpdir(), "startup-" + name + "-" + Date.now() + ".sh");
  await Bun.write(startup_path, startup_script);

  console.log("🚀 [GCP] 创建 Spot 实例: " + name + " (机型: " + machine_type + ", 可用区: " + zone + ")...");
  let created = false;

  // 1. 尝试挂载高 IOPS 物理 Local NVMe SSD
  try {
    const res = await $`gcloud compute instances create ${name} --zone=${zone} --machine-type=${machine_type} --provisioning-model=SPOT --image-family=${image_family} --image-project=${image_project} --boot-disk-type=${DEFAULT_BOOT_DISK_TYPE} --boot-disk-size=${DEFAULT_BOOT_DISK_SIZE}GB --local-ssd=interface=NVME --metadata-from-file=startup-script=${startup_path} --format=json --quiet`;
    created = true;
  } catch (err) {
    console.log("⚠️ 挂载 Local NVMe 失败或资源池紧张: " + err.message.slice(0, 120) + "... 降级至 NVMe 高速 pd-ssd 重试...");
  }

  // 2. 降级为高性能 NVMe pd-ssd 重试
  if (!created) {
    await $`gcloud compute instances create ${name} --zone=${zone} --machine-type=${machine_type} --provisioning-model=SPOT --image-family=${image_family} --image-project=${image_project} --boot-disk-type=${DEFAULT_BOOT_DISK_TYPE} --boot-disk-size=${DEFAULT_BOOT_DISK_SIZE}GB --metadata-from-file=startup-script=${startup_path} --format=json --quiet`;
  }

  try { await $`rm -f ${startup_path}`; } catch {}

  // 等待实例状态变为 RUNNING
  let instance_info = null;
  for (let i = 0; i < 30; ++i) {
    try {
      const desc = await $`gcloud compute instances describe ${name} --zone=${zone} --format=json --quiet`,
        parsed = JSON.parse(desc.stdout);
      if (parsed.status === "RUNNING") {
        instance_info = parsed;
        break;
      }
      console.log("⏳ [GCP] 等待实例 " + name + " 状态转为 RUNNING (当前: " + parsed.status + ")...");
    } catch {}
    await Bun.sleep(2000);
  }

  if (!instance_info) throw new Error("获取实例 " + name + " 详细配置信息失败");

  const ext_ip = instance_info.networkInterfaces?.[0]?.accessConfigs?.[0]?.natIP || "",
    int_ip = instance_info.networkInterfaces?.[0]?.networkIP || "";

  console.log("🎉 [GCP] 实例 " + name + " 启动成功！外网 IP: " + ext_ip + " | 内网 IP: " + int_ip);
  return {
    name,
    zone,
    externalIp: ext_ip,
    internalIp: int_ip,
  };
};

/**
 * 等待实例初始化脚本 (startup-script) 安装完成
 */
const startupDoneWait = async (name, max_wait_sec = 600, zone = DEFAULT_ZONE) => {
  console.log("⏳ 等待实例 " + name + " 初始化脚本 (startup-script) 安装环境完成...");
  const start = Date.now();
  while (Date.now() - start < max_wait_sec * 1000) {
    try {
      const res = await sshCommandRun(name, "test -f /var/log/startup_done && echo 'STARTUP_DONE' || echo 'WAITING'", zone, true);
      if ((res.stdout ?? "").includes("STARTUP_DONE")) {
        console.log("✅ 实例 " + name + " 初始化环境 (Rust / Bun / Podman / redis-tools) 已全部就绪！");
        return;
      }
    } catch {}
    await Bun.sleep(5000);
  }
  throw new Error("实例 " + name + " 初始化脚本执行超时（超过 " + max_wait_sec + " 秒）");
};

/**
 * 安全上传文件至远端实例 (利用 gcloud compute scp + IAP 隧道)
 */
const fileUploadWithRetry = async (name, local_path, remote_path, zone = DEFAULT_ZONE, max_retries = 5) => {
  for (let attempt = 1; attempt <= max_retries; ++attempt) {
    try {
      await $`gcloud compute scp ${local_path} ${name}:${remote_path} --zone=${zone} --tunnel-through-iap --quiet`;
      return;
    } catch (err) {
      console.log("⚠️ 上传 " + local_path + " (尝试 " + attempt + "/" + max_retries + "): " + err.message + "，2秒后重试...");
      await Bun.sleep(2000);
    }
  }
  throw new Error("上传 " + local_path + " 到 " + name + ":" + remote_path + " 失败");
};

/**
 * 安全从远端实例下载文件到本地 (利用 gcloud compute scp + IAP 隧道)
 */
const fileDownloadWithRetry = async (name, remote_path, local_path, zone = DEFAULT_ZONE, max_retries = 5) => {
  for (let attempt = 1; attempt <= max_retries; ++attempt) {
    try {
      await $`gcloud compute scp ${name}:${remote_path} ${local_path} --zone=${zone} --tunnel-through-iap --quiet`;
      return;
    } catch (err) {
      console.log("⚠️ 下载 " + remote_path + " (尝试 " + attempt + "/" + max_retries + "): " + err.message + "，2秒后重试...");
      await Bun.sleep(2000);
    }
  }
  throw new Error("下载 " + name + ":" + remote_path + " 到 " + local_path + " 失败");
};

/**
 * 等待实例 SSH 端口与 IAP 通道就绪
 */
const sshReadyWait = async (name, zone = DEFAULT_ZONE, max_retries = 30) => {
  for (let i = 0; i < max_retries; ++i) {
    try {
      await sshCommandRun(name, "true", zone, true);
      return;
    } catch {
      await Bun.sleep(2000);
    }
  }
  throw new Error("实例 " + name + " SSH 启动超时！");
};

/**
 * 销毁单个实例
 */
const instanceDestroy = async (name, zone = DEFAULT_ZONE) => {
  try {
    console.log("🗑️ [GCP] 正在销毁实例: " + name + " (zone: " + zone + ")...");
    await $`gcloud compute instances delete ${name} --zone=${zone} --quiet`;
    console.log("✅ [GCP] 实例 " + name + " 已成功销毁。");
  } catch (err) {
    console.log("⚠️ 销毁实例 " + name + " 时出错或已不存在: " + err.message);
  }
};

/**
 * 彻底清理并销毁所有以指定前缀开头的云端实例及防火墙
 */
const allByPrefixDestroy = async (prefix = "wedb-bench-", delete_firewall = false) => {
  console.log("\n🧹 [GCP Lifecycle] 扫描并清理所有带有前缀 '" + prefix + "' 的云资源...");
  try {
    const list_res = await $`gcloud compute instances list --filter=name:${prefix}* --format="value(name,zone)"`,
      lines = list_res.stdout.trim().split("\n").filter(Boolean);

    if (lines.length > 0) {
      for (const line of lines) {
        const [name, zone] = line.split(/\s+/);
        if (name && zone) {
          await instanceDestroy(name, zone);
        }
      }
    } else {
      console.log("✅ 未发现任何残留的 '" + prefix + "*' 实例。");
    }

    if (delete_firewall) {
      try {
        await $`gcloud compute firewall-rules delete allow-wedb-bench --quiet`;
        console.log("✅ 临时防火墙规则已清理。");
      } catch {}
    }
  } catch (err) {
    console.log("⚠️ 清理云资源时捕获异常: " + err.message);
  }
};

/**
 * 在远端运行测试任务并实时拉取标准输出流
 */
const remoteBenchmarkRunAndStreamLog = async (name, cmd, log_file, done_file, zone = DEFAULT_ZONE, on_log_chunk = null) => {
  try {
    await sshCommandRun(name, "killall -9 redis-benchmark 2>/dev/null || true; ( test -f " + log_file + ".pid && kill -9 $(cat " + log_file + ".pid 2>/dev/null) 2>/dev/null || true ); rm -f " + log_file + " " + log_file + ".pid " + done_file, zone, true);
  } catch {}

  const runner_script = "#!/bin/bash\n" + cmd + "\n",
    local_script_path = join(tmpdir(), "task_" + Date.now() + ".sh");
  await Bun.write(local_script_path, runner_script);
  await fileUploadWithRetry(name, local_script_path, "/tmp/run_remote_task.sh", zone);
  try { await $`rm -f ${local_script_path}`; } catch {}

  const start_cmd = "( chmod +x /tmp/run_remote_task.sh && nohup /tmp/run_remote_task.sh > " + log_file + " 2>&1 & echo $! > " + log_file + ".pid )";
  await sshCommandRun(name, start_cmd, zone, true);

  let last_offset = 1,
    last_activity_time = Date.now(),
    stopped_count = 0,
    is_done = false;
  const start_time = Date.now(),
    max_timeout_ms = 1800 * 1000,
    silent_timeout_ms = 120 * 1000;

  while (!is_done && (Date.now() - start_time < max_timeout_ms)) {
    await Bun.sleep(2500);
    try {
      const check_res = await sshCommandRun(name, "test -f " + done_file + " && echo 'BENCH_COMPLETED' || ( pgrep -F " + log_file + ".pid >/dev/null 2>&1 && echo 'BENCH_RUNNING' || echo 'BENCH_STOPPED' )", zone, true).catch(() => ({ stdout: "BENCH_RUNNING" })),
        status = (check_res.stdout ?? "").trim(),
        log_res = await sshCommandRun(name, "tail -n +" + last_offset + " " + log_file + " 2>/dev/null", zone, true).catch(() => ({ stdout: "" })),
        text = log_res.stdout ?? "";

      if (text && text.length > 0) {
        process.stdout.write(text);
        const line_count = text.split("\n").length - 1;
        last_offset += line_count;
        last_activity_time = Date.now();
        stopped_count = 0;
        if (typeof on_log_chunk === "function") {
          try {
            await on_log_chunk(text);
          } catch {}
        }
      } else {
        const silent_duration_ms = Date.now() - last_activity_time;
        if (silent_duration_ms > silent_timeout_ms) {
          console.log("\n⚠️ [监控告警] 实例 " + name + " 已超过 " + Math.round(silent_duration_ms / 1000) + " 秒无任何日志输出！自动执行系统故障诊断...");
          try {
            const diag_res = await sshCommandRun(name, "echo '=== [DIAG] 进程树 ==='; ps aux | grep -E 'bench_runner|wedb|redis|kvrocks|podman|redis-benchmark' | grep -v grep || true; echo '=== [DIAG] 端口监听 ==='; ( ss -tlpn 2>/dev/null || netstat -tlpn 2>/dev/null || true ); echo '=== [DIAG] 末尾日志 ==='; tail -n 20 " + log_file + " 2>/dev/null || true", zone, true);
            console.log(diag_res.stdout);
          } catch {}
          last_activity_time = Date.now();
        }
      }

      if (status.includes("BENCH_COMPLETED")) {
        is_done = true;
        break;
      } else if (status.includes("BENCH_STOPPED") && !status.includes("BENCH_RUNNING")) {
        if (++stopped_count >= 3 && Date.now() - start_time > 10000) {
          await Bun.sleep(1000);
          try {
            const final_log = await sshCommandRun(name, "cat " + log_file + " 2>/dev/null", zone, true);
            if (final_log.stdout && final_log.stdout.length > 0) {
              process.stdout.write(final_log.stdout.slice(-2000));
            }
          } catch {}
          break;
        }
      }
    } catch {}
  }
};

/**
 * 启动后台常驻服务
 */
const remoteDaemonStart = async (name, command_str, zone = DEFAULT_ZONE) => {
  const launcher_script = "#!/bin/bash\n" + command_str + "\n",
    launcher_path = join(tmpdir(), "daemon_" + Date.now() + ".sh");
  await Bun.write(launcher_path, launcher_script);
  await fileUploadWithRetry(name, launcher_path, "/tmp/run_daemon.sh", zone);
  try { await $`rm -f ${launcher_path}`; } catch {}
  await sshCommandRun(name, "chmod +x /tmp/run_daemon.sh && ( nohup /tmp/run_daemon.sh >/dev/null 2>&1 </dev/null & )", zone, true);
};

/**
 * 执行一次性远端命令任务
 */
const remoteTaskExecuteWithLog = async (name, cmd, log_file = "/tmp/remote_task.log", zone = DEFAULT_ZONE) => {
  await remoteBenchmarkRunAndStreamLog(name, cmd + " && touch /tmp/task_done", log_file, "/tmp/task_done", zone);
};

/**
 * 流式全量无损源码同步
 */
const sourceSyncWithRsync = async (name, local_root_dir, remote_dest_dir = "/opt/wedb", zone = DEFAULT_ZONE) => {
  await sshReadyWait(name, zone);
  console.log("🔄 [sync] 正在极速流式同步纯净源码到 " + name + ":" + remote_dest_dir + " (自动遵循 .gitignore 并排除 .git)...");
  const prep_cmd = "( sudo mkdir -p " + remote_dest_dir + " && sudo chown -R $(whoami):$(whoami) " + remote_dest_dir + " ) 2>/dev/null || ( mkdir -p " + remote_dest_dir + " && chown -R $(whoami):$(whoami) " + remote_dest_dir + " ) 2>/dev/null || true";
  await sshCommandRun(name, prep_cmd, zone, true);

  const local_tar_gz = join(tmpdir(), "src_sync_" + Date.now() + ".tar.gz");
  await $`tar --exclude-vcs --exclude="target" --exclude="node_modules" --exclude=".git" -czf ${local_tar_gz} -C ${local_root_dir} .`;
  await fileUploadWithRetry(name, local_tar_gz, "/tmp/src_sync.tar.gz", zone);
  try { await $`rm -f ${local_tar_gz}`; } catch {}

  const unpack_cmd = "( /bin/tar -xzf /tmp/src_sync.tar.gz -C " + remote_dest_dir + " 2>/dev/null || tar -xzf /tmp/src_sync.tar.gz -C " + remote_dest_dir + " ) && rm -f /tmp/src_sync.tar.gz";
  await sshCommandRun(name, unpack_cmd, zone, true);

  console.log("📦 [sync] 在远端实例执行 bun install 安装纯净依赖...");
  try {
    await sshCommandRun(name, "cd " + remote_dest_dir + " && ( bun install 2>/dev/null || true )", zone, true);
  } catch {}

  console.log("✅ [sync] 源码与依赖同步完成: " + remote_dest_dir);
};

export {
  tcpPortWait,
  firewallRulesEnsure,
  spotInstanceCreate,
  startupDoneWait,
  fileUploadWithRetry,
  sshReadyWait,
  instanceDestroy,
  allByPrefixDestroy,
  remoteBenchmarkRunAndStreamLog,
  remoteDaemonStart,
  remoteTaskExecuteWithLog,
  sourceSyncWithRsync,
  fileUploadWithRetry as uploadFileWithRetry,
  fileDownloadWithRetry as downloadFile,
  spotInstanceCreate as createSpotInstance,
  startupDoneWait as waitForStartupDone,
  tcpPortWait as waitForTcpPort,
  sourceSyncWithRsync as syncSourceWithRsync,
  remoteBenchmarkRunAndStreamLog as runRemoteBenchmarkAndStreamLog,
  remoteTaskExecuteWithLog as executeRemoteTaskWithLog,
  firewallRulesEnsure as ensureFirewallRules,
  allByPrefixDestroy as destroyAllByPrefix,
};
