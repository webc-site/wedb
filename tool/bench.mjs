#!/usr/bin/env -S bun
import { $ } from "zx";
import { join } from "node:path";
import { load } from "js-yaml";
import {
  DEFAULT_ZONE,
  ensureFirewallRules,
  createSpotInstance,
  uploadFileWithRetry,
  downloadFile,
  syncSourceWithRsync,
  destroyAllByPrefix,
  waitForStartupDone,
  waitForTcpPort,
  runRemoteBenchmarkAndStreamLog,
  remoteDaemonStart,
  sshCommandRun,
} from "./gcp.mjs";
import { reportGenerateFromYaml } from "./bench_report.mjs";

if (!process.env.CLOUDSDK_PYTHON) {
  try {
    const p = (await $`which python3`.text()).trim();
    if (p) process.env.CLOUDSDK_PYTHON = p;
  } catch {}
}
$.verbose = 1;

const bench_dir = join(import.meta.dirname, "bench"),
  root_dir = join(import.meta.dirname, ".."),
  resource_prefix = "wedb-bench-";

const main = async () => {
  console.log("==================================================");
  console.log("🚀 WeDB 云端 Spot 实例 (ARM 芯片 / Ubuntu 24.04 / Podman / NVMe 极速物理存储) 本机真实超高速压测套件");
  console.log("==================================================");

  // 读取集群与服务器 YAML 配置
  const conf_path = join(bench_dir, "cluster_conf.yml"),
    conf_data = load(await Bun.file(conf_path).text()),
    machine_type = conf_data.machine_type ?? "t2a-standard-4",
    arch = conf_data.arch ?? "arm64",
    image_family = conf_data.os_image?.family ?? "ubuntu-2404-lts-arm64",
    image_project = conf_data.os_image?.project ?? "ubuntu-os-cloud",
    target_zone = conf_data.zone ?? DEFAULT_ZONE,
    target_triple = arch === "arm64" ? "aarch64-unknown-linux-musl" : "x86_64-unknown-linux-musl",
    runner_bundle = "/tmp/bench_runner_bundle.mjs";

  console.log("⚙️  当前服务器配置 (from cluster_conf.yml):");
  console.log("   - 机型架构: " + machine_type + " (" + arch + ")");
  console.log("   - 系统镜像: " + image_family + " (Ubuntu 24.04 LTS)");
  console.log("   - 容器引擎: " + (conf_data.container_engine ?? "podman"));
  console.log("   - 存储介质: " + conf_data.storage);

  let success = false;

  try {
    // 1. 初始化：确保防火墙规则就绪
    console.log("\n[0/4] 初始化环境：确保防火墙规则就绪并检查已有实例...");
    await ensureFirewallRules(resource_prefix);

    // =====================================================================
    // 2. 云端单机实测 (优先复用已有 Node 1 实例，后续直接复用为集群 Leader)
    // =====================================================================
    console.log("\n[1/4] 获取或启动 GCP 抢占式实例 Node 1 (" + machine_type + " + Ubuntu 24.04 + NVMe SSD)...");

    const node1_startup_script = `#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y build-essential pkg-config libssl-dev protobuf-compiler podman curl unzip git tar rsync redis-tools
mkdir -p /opt/wedb /mnt/nvme /opt/rust /opt/bun
chmod 777 /opt/wedb /mnt/nvme /opt/rust /opt/bun

# 1. 全局安装官方最新 Rust 稳定版 (Edition 2024 支持)
export RUSTUP_HOME=/opt/rust/rustup
export CARGO_HOME=/opt/rust/cargo
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable --no-modify-path
/opt/rust/cargo/bin/rustup default stable
chmod -R 777 /opt/rust
ln -sf /opt/rust/cargo/bin/* /usr/local/bin/ || true

# 2. 全局安装 Bun
export BUN_INSTALL=/opt/bun
curl -fsSL https://bun.sh/install | bash || true
chmod -R 777 /opt/bun
ln -sf /opt/bun/bin/bun /usr/local/bin/bun || true

echo 'export RUSTUP_HOME=/opt/rust/rustup' >> /etc/environment
echo 'export CARGO_HOME=/opt/rust/cargo' >> /etc/environment
echo 'export PATH="/opt/rust/cargo/bin:/opt/bun/bin:/usr/local/bin:$PATH"' >> /etc/environment

# 准备 Kvrocks 配置与数据目录 (挂载在 NVMe 存储卷)
mkdir -p /mnt/nvme/kvrocks_data
chmod 777 /mnt/nvme/kvrocks_data
cat << 'EOF' > /tmp/kvrocks.conf
port 6666
bind 0.0.0.0
dir /mnt/nvme/kvrocks_data
EOF

touch /var/log/startup_done
`;

    const node1 = await createSpotInstance(
      resource_prefix + "cluster-1",
      node1_startup_script,
      target_zone,
      machine_type,
      image_family,
      image_project
    );

    // 等待 Node 1 基础开发环境安装就绪 (最新 Rust 稳定版 + Bun + Podman)
    await waitForStartupDone(node1.name, 600, node1.zone);

    // 清理可能残留的后台进程与容器
    try {
      await sshCommandRun(node1.name, "pkill -9 -f wedb_standalone 2>/dev/null || true; pkill -9 -f bench_runner 2>/dev/null || true; podman rm -f redis kvrocks 2>/dev/null || true", node1.zone);
    } catch {}

    // 同步项目最新纯净源码至 Node 1
    console.log("\n📦 [2/4] 使用 rsync 增量同步最新纯净源码至 Node 1 (" + node1.name + ":/opt/wedb)...");
    await syncSourceWithRsync(node1.name, root_dir, "/opt/wedb", node1.zone);

    // 在 Node 1 本机执行原生 target-cpu=native 极致优化编译 (如果已存在则智能复用)
    const bin_check = await sshCommandRun(node1.name, "test -f /opt/wedb/target/release/wedb_standalone && test -f /opt/wedb/target/release/wedb_cluster && echo 'BIN_EXISTS' || echo 'NEED_BUILD'", node1.zone).catch(() => ({ stdout: "NEED_BUILD" }));
    if (!bin_check.stdout.includes("BIN_EXISTS")) {
      console.log("\n🔨 [3/4] 在 Node 1 本机通过 mise 执行 Rust 原生极致优化编译 (RUSTFLAGS=\"-C target-cpu=native -C opt-level=3 -C codegen-units=1\" cargo build --release)...");
      const node1_compile_cmd = 'export RUSTUP_HOME=/opt/rust/rustup CARGO_HOME=/opt/rust/cargo PATH="/opt/rust/cargo/bin:/opt/bun/bin:/usr/local/bin:$PATH"; export PROTOC=/usr/bin/protoc; cd /opt/wedb && RUSTFLAGS="-C target-cpu=native -C opt-level=3 -C codegen-units=1" cargo build --release --bin wedb_standalone --bin wedb_cluster && touch /tmp/build_done';
      await runRemoteBenchmarkAndStreamLog(
        node1.name,
        node1_compile_cmd,
        "/tmp/cargo_build.log",
        "/tmp/build_done",
        node1.zone
      );
      console.log("✅ Node 1 原生 Release+Native 最优化编译完成！");
    } else {
      console.log("⚡ [极速缓存] Node 1 已存在已编译好的 Release+Native 二进制，跳过重复编译直接进入压测！");
    }

    // 打包并上传压测客户端
    console.log("📦 打包并上传独立压测客户端至 " + node1.name + ":/tmp/bench_runner.mjs...");
    await $`bun build ${join(import.meta.dirname, "remote_runner.mjs")} --target=bun --outfile=${runner_bundle}`;
    await uploadFileWithRetry(node1.name, runner_bundle, "/tmp/bench_runner.mjs", node1.zone);

    const result_dir = join(bench_dir, "result");
    await Bun.spawn(["mkdir", "-p", result_dir]).exited;

    const syncRemoteResultFile = async (node_name, remote_filename, zone) => {
      try {
        const cmd = "cat /tmp/bench_out/" + remote_filename + " 2>/dev/null || true",
          res = (await sshCommandRun(node_name, cmd, zone)).stdout.trim();
        if (res && res.length > 5) {
          const local_path = join(result_dir, remote_filename);
          await Bun.write(local_path, res);
          console.log("📥 [Result-Sync] 成功同步最新测试结果: " + remote_filename + " -> tool/bench/result/" + remote_filename);
          await reportGenerateFromYaml(result_dir);
          return true;
        }
      } catch (e) {
        console.log("⚠️ 同步 " + remote_filename + " 暂不可用: " + e.message);
      }
      return false;
    };

    const skip_standalone = process.env.SKIP_STANDALONE === "1" &&
      (await Bun.file(join(result_dir, "wedb.standalone.yml")).exists()) &&
      (await Bun.file(join(result_dir, "redis.standalone.yml")).exists()) &&
      (await Bun.file(join(result_dir, "kvrocks.standalone.yml")).exists()) &&
      (await Bun.file(join(result_dir, "meta.yml")).exists());

    if (!skip_standalone) {
      // 启动 WeDB Standalone (端口 4909, 数据存放在 /mnt/nvme/wedb_standalone_data)
      console.log("🚀 启动 WeDB Standalone 服务 (Local NVMe 存储)...");
      await remoteDaemonStart(
        node1.name,
        "mkdir -p /mnt/nvme/wedb_standalone_data && nohup /opt/wedb/target/release/wedb_standalone --addr 0.0.0.0:4909 --data_dir /mnt/nvme/wedb_standalone_data >/tmp/wedb_standalone.log 2>&1 &",
        node1.zone
      );
      await waitForTcpPort(node1.name, 4909, 120, node1.zone);

      // 在 Node 1 本机运行单机超高速无损压测 (每完成一项即时同步 yml 并刷新报告)
      console.log("\n⚡ [SSH-Stream] 在 Node 1 本机 (Local NVMe) 执行单机性能基准测试 (WeDB, Redis, Kvrocks 独占隔离)...");
      const run_stand_cmd = 'export RUSTUP_HOME=/opt/rust/rustup CARGO_HOME=/opt/rust/cargo PATH="/opt/rust/cargo/bin:/opt/bun/bin:/usr/local/bin:$PATH"; mkdir -p /tmp/bench_out && bun /tmp/bench_runner.mjs --mode=standalone --outDir=/tmp/bench_out';
      await runRemoteBenchmarkAndStreamLog(
        node1.name,
        run_stand_cmd,
        "/tmp/bench_standalone.log",
        "/tmp/bench_out/meta.yml",
        node1.zone,
        async (chunk) => {
          if (chunk.includes("[WeDB]")) await syncRemoteResultFile(node1.name, "wedb.standalone.yml", node1.zone);
          if (chunk.includes("[Redis]")) await syncRemoteResultFile(node1.name, "redis.standalone.yml", node1.zone);
          if (chunk.includes("[Kvrocks]")) await syncRemoteResultFile(node1.name, "kvrocks.standalone.yml", node1.zone);
          if (chunk.includes("[Meta]")) await syncRemoteResultFile(node1.name, "meta.yml", node1.zone);
        }
      );

      // 兜底下载全部单机独立测试指标
      await syncRemoteResultFile(node1.name, "wedb.standalone.yml", node1.zone);
      await syncRemoteResultFile(node1.name, "redis.standalone.yml", node1.zone);
      await syncRemoteResultFile(node1.name, "kvrocks.standalone.yml", node1.zone);
      await syncRemoteResultFile(node1.name, "meta.yml", node1.zone);
      console.log("✅ 单机各引擎独立测试数据已完整保存至 tool/bench/result/");
    } else {
      console.log("✅ 检测到本地已存在单机测试基准数据 (WeDB / Redis / Kvrocks)，直接复用并进入分布式集群压测！");
    }

    // =====================================================================
    // 3. 组建 3 节点分布式 Raft 集群 (直接复用 Node 1，分发二进制至 Node 2 / Node 3)
    // =====================================================================
    console.log("\n[4/4] 组建 3 节点 Multi-Raft 分布式集群 (复用 Node 1，分发 release+native 二进制至 Node 2 和 Node 3)...");

    // 清理 Node 1 单机残留容器并启动 Node 1 Raft 实例
    console.log("🧹 清理 Node 1 单机进程，启动 Node 1 Multi-Raft Leader (4909 / 4910)...");
    try {
      await sshCommandRun(node1.name, "pkill -9 -f wedb_standalone 2>/dev/null || true; podman rm -f redis kvrocks 2>/dev/null || true", node1.zone);
    } catch {}
    await remoteDaemonStart(
      node1.name,
      "mkdir -p /mnt/nvme/wedb_cluster_data && nohup /opt/wedb/target/release/wedb_cluster --node_id 1 --addr 0.0.0.0:4909 --raft " + node1.internalIp + ":4910 --data_dir /mnt/nvme/wedb_cluster_data >/tmp/wedb_cluster.log 2>&1 &",
      node1.zone
    );
    await waitForTcpPort(node1.name, 4909, 90, node1.zone);

    // 从 Node 1 下载编译好的 release+native 二进制至本地中转
    const local_clust_bin = "/tmp/wedb_cluster_native";
    console.log("📥 从 Node 1 获取已编译好的 release+native 二进制用于快速分发...");
    await downloadFile(node1.name, "/opt/wedb/target/release/wedb_cluster", local_clust_bin, node1.zone);

    const light_startup_script = `#!/bin/bash
set -e
export DEBIAN_FRONTEND=noninteractive
apt-get update -y
apt-get install -y podman curl unzip git tar rsync redis-tools
mkdir -p /opt/wedb /mnt/nvme
chmod 777 /opt/wedb /mnt/nvme
touch /var/log/startup_done
`;

    // 启动 Node 2 (轻量级容器与分布式执行环境)
    const node2 = await createSpotInstance(
      resource_prefix + "cluster-2",
      light_startup_script,
      node1.zone,
      machine_type,
      image_family,
      image_project
    );
    await waitForStartupDone(node2.name, 180, node2.zone);
    console.log("📦 分发 release+native 二进制至 Node 2 (" + node2.name + ")...");
    await uploadFileWithRetry(node2.name, local_clust_bin, "/tmp/wedb_cluster", node2.zone);
    await remoteDaemonStart(
      node2.name,
      "chmod 755 /tmp/wedb_cluster && mkdir -p /mnt/nvme/wedb_cluster_data && nohup /tmp/wedb_cluster --node_id 2 --addr 0.0.0.0:4909 --raft " + node2.internalIp + ":4910 --join " + node1.internalIp + ":4910 --data_dir /mnt/nvme/wedb_cluster_data >/tmp/wedb_cluster.log 2>&1 &",
      node2.zone
    );
    await waitForTcpPort(node2.name, 4909, 90, node2.zone);

    // 启动 Node 3 (轻量级容器与分布式执行环境)
    const node3 = await createSpotInstance(
      resource_prefix + "cluster-3",
      light_startup_script,
      node1.zone,
      machine_type,
      image_family,
      image_project
    );
    await waitForStartupDone(node3.name, 180, node3.zone);
    console.log("📦 分发 release+native 二进制至 Node 3 (" + node3.name + ")...");
    await uploadFileWithRetry(node3.name, local_clust_bin, "/tmp/wedb_cluster", node3.zone);
    await remoteDaemonStart(
      node3.name,
      "chmod 755 /tmp/wedb_cluster && mkdir -p /mnt/nvme/wedb_cluster_data && nohup /tmp/wedb_cluster --node_id 3 --addr 0.0.0.0:4909 --raft " + node3.internalIp + ":4910 --join " + node1.internalIp + ":4910 --data_dir /mnt/nvme/wedb_cluster_data >/tmp/wedb_cluster.log 2>&1 &",
      node3.zone
    );
    await waitForTcpPort(node3.name, 4909, 90, node3.zone);

    // 4.1 在 Node 1 上运行 WeDB 3 节点分布式 Raft 基准测试
    console.log("\n⚡ [SSH-Stream] [1/3] 在 Node 1 上执行 WeDB 分布式 Multi-Raft 集群基准测试...");
    const run_wedb_clust_cmd = 'export RUSTUP_HOME=/opt/rust/rustup CARGO_HOME=/opt/rust/cargo PATH="/opt/rust/cargo/bin:/opt/bun/bin:/usr/local/bin:$PATH"; mkdir -p /tmp/bench_out && bun /tmp/bench_runner.mjs --mode=cluster --node1=127.0.0.1:4909 --node2=' + node2.internalIp + ":4909 --node3=" + node3.internalIp + ":4909 --outDir=/tmp/bench_out";
    await runRemoteBenchmarkAndStreamLog(
      node1.name,
      run_wedb_clust_cmd,
      "/tmp/bench_cluster.log",
      "/tmp/bench_out/wedb.cluster.yml",
      node1.zone,
      async (chunk) => {
        if (chunk.includes("[Cluster]")) {
          await syncRemoteResultFile(node1.name, "wedb.cluster.yml", node1.zone);
          await syncRemoteResultFile(node1.name, "cluster_conf.yml", node1.zone);
        }
      }
    );
    await syncRemoteResultFile(node1.name, "wedb.cluster.yml", node1.zone);
    await syncRemoteResultFile(node1.name, "cluster_conf.yml", node1.zone);

    const safeSshRun = async (name, command, zone, max_retries = 8) => {
      for (let i = 1; i <= max_retries; ++i) {
        try {
          return await sshCommandRun(name, command, zone);
        } catch (e) {
          if (i === max_retries) throw e;
          console.log("⚠️ SSH 执行临时抖动 (" + i + "/" + max_retries + ")，3秒后重试...");
          await Bun.sleep(3000);
        }
      }
    };

    // 4.2 部署并测试 Redis 官方 3 节点分片集群
    console.log("\n⚡ [SSH-Stream] [2/3] 部署并实测 Redis 3 节点官方分片集群 (Redis Cluster)...");
    for (const n of [node1, node2, node3]) {
      const start_redis_cmd = "pkill -9 -f wedb_cluster 2>/dev/null || true; podman rm -f redis_cluster kvrocks_cluster 2>/dev/null || true; podman run -d --name redis_cluster --replace --restart always --network=host docker.io/library/redis:latest redis-server --bind 0.0.0.0 --port 6379 --cluster-enabled yes --cluster-config-file /tmp/nodes.conf --cluster-node-timeout 5000 --appendonly no --protected-mode no";
      await remoteDaemonStart(n.name, start_redis_cmd, n.zone);
      await waitForTcpPort(n.name, 6379, 120, n.zone);
    }
    // 在 Node 1 上执行 Redis 集群创建
    await Bun.sleep(2000);
    try {
      await safeSshRun(
        node1.name,
        `podman run --rm --network=host docker.io/library/redis:latest redis-cli --cluster create ${node1.internalIp}:6379 ${node2.internalIp}:6379 ${node3.internalIp}:6379 --cluster-replicas 0 --cluster-yes 2>/dev/null || true`,
        node1.zone
      );
    } catch {}

    const run_redis_clust_cmd = 'export RUSTUP_HOME=/opt/rust/rustup CARGO_HOME=/opt/rust/cargo PATH="/opt/rust/cargo/bin:/opt/bun/bin:/usr/local/bin:$PATH"; mkdir -p /tmp/bench_out && bun /tmp/bench_runner.mjs --mode=redis-cluster --node1=127.0.0.1:6379 --node2=' + node2.internalIp + ":6379 --node3=" + node3.internalIp + ":6379 --outDir=/tmp/bench_out";
    await runRemoteBenchmarkAndStreamLog(
      node1.name,
      run_redis_clust_cmd,
      "/tmp/bench_redis_cluster.log",
      "/tmp/bench_out/redis.cluster.yml",
      node1.zone,
      async () => {
        await syncRemoteResultFile(node1.name, "redis.cluster.yml", node1.zone);
      }
    );
    await syncRemoteResultFile(node1.name, "redis.cluster.yml", node1.zone);

    // 4.3 部署并测试 Apache Kvrocks 3 节点分片集群 (NVMe 物理存储)
    console.log("\n⚡ [SSH-Stream] [3/3] 部署并实测 Apache Kvrocks 3 节点分片集群 (RocksDB on Local NVMe SSD)...");
    for (const n of [node1, node2, node3]) {
      const start_kvrocks_cmd = "pkill -9 -f wedb_cluster 2>/dev/null || true; podman rm -f redis_cluster kvrocks_cluster 2>/dev/null || true; mkdir -p /mnt/nvme/kvrocks_cluster_data && sudo chmod 777 /mnt/nvme/kvrocks_cluster_data && podman run -d --name kvrocks_cluster --replace --restart always --network=host -v /mnt/nvme/kvrocks_cluster_data:/var/lib/kvrocks/data docker.io/apache/kvrocks:latest --bind 0.0.0.0 --port 6666 --dir /var/lib/kvrocks/data --log-dir stdout --cluster-enabled yes";
      await remoteDaemonStart(n.name, start_kvrocks_cmd, n.zone);
      await waitForTcpPort(n.name, 6666, 120, n.zone);
    }
    await Bun.sleep(2000);

    // 在 Node 1 上执行 Kvrocks 分片拓扑初始化
    const kvrocks_nodes_cfg = `
NODES="07c37dfeb61520913d54c73e414a631fa0d96c70 127.0.0.1 6666 master - 0-5460
67ed2db8d21a4fba79afda1097c76f0f0f1d56ea ${node2.internalIp} 6666 master - 5461-10922
0f54519902c70d9bed937b279d0df314342f04bf ${node3.internalIp} 6666 master - 10923-16383"

redis-cli -h 127.0.0.1 -p 6666 CLUSTERX SETNODEID 07c37dfeb61520913d54c73e414a631fa0d96c70 2>/dev/null || true
redis-cli -h ${node2.internalIp} -p 6666 CLUSTERX SETNODEID 67ed2db8d21a4fba79afda1097c76f0f0f1d56ea 2>/dev/null || true
redis-cli -h ${node3.internalIp} -p 6666 CLUSTERX SETNODEID 0f54519902c70d9bed937b279d0df314342f04bf 2>/dev/null || true

redis-cli -h 127.0.0.1 -p 6666 CLUSTERX SETNODES "$NODES" 1 2>/dev/null || true
redis-cli -h ${node2.internalIp} -p 6666 CLUSTERX SETNODES "$NODES" 1 2>/dev/null || true
redis-cli -h ${node3.internalIp} -p 6666 CLUSTERX SETNODES "$NODES" 1 2>/dev/null || true
`;
    try {
      await safeSshRun(node1.name, kvrocks_nodes_cfg, node1.zone);
    } catch {}

    const run_kvrocks_clust_cmd = 'export RUSTUP_HOME=/opt/rust/rustup CARGO_HOME=/opt/rust/cargo PATH="/opt/rust/cargo/bin:/opt/bun/bin:/usr/local/bin:$PATH"; mkdir -p /tmp/bench_out && bun /tmp/bench_runner.mjs --mode=kvrocks-cluster --node1=127.0.0.1:6666 --node2=' + node2.internalIp + ":6666 --node3=" + node3.internalIp + ":6666 --outDir=/tmp/bench_out";
    await runRemoteBenchmarkAndStreamLog(
      node1.name,
      run_kvrocks_clust_cmd,
      "/tmp/bench_kvrocks_cluster.log",
      "/tmp/bench_out/kvrocks.cluster.yml",
      node1.zone,
      async () => {
        await syncRemoteResultFile(node1.name, "kvrocks.cluster.yml", node1.zone);
      }
    );
    await syncRemoteResultFile(node1.name, "kvrocks.cluster.yml", node1.zone);

    console.log("✅ 全套单机与集群（WeDB、Redis、Kvrocks）实测数据已全部同步至 tool/bench/result/");

    // 销毁所有云端实例
    await destroyAllByPrefix(resource_prefix + "cluster-");

    // 调用独立脚本生成最终报表
    console.log("\n🎉 生成全量终态对比报表...");
    await reportGenerateFromYaml(result_dir);

    success = true;
    console.log("\n🎉 全部云端基准实测与全景对比报告生成完毕！");
  } catch (err) {
    console.error("\n❌ 基准测试流程失败: " + err.message);
  } finally {
    console.log("\n🧹 终态资源回收...");
    try {
      await destroyAllByPrefix(resource_prefix, true);
    } catch {}
  }

  if (!success) process.exit(1);
};

export const mainFunc = main;
export default main;

if (import.meta.main) {
  main().catch(console.error);
}
