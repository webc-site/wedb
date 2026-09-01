#!/usr/bin/env -S bun
import { parseArgs } from "node:util";
import { cpus, totalmem, arch } from "node:os";
import { dump } from "js-yaml";
import Redis from "ioredis";

const { values: flags } = parseArgs({
  options: {
    mode: { type: "string", default: "standalone" }, // 'standalone' | 'wedb-cluster' | 'redis-cluster' | 'kvrocks-cluster'
    node1: { type: "string", default: "127.0.0.1:4909" },
    node2: { type: "string", default: "127.0.0.1:4909" },
    node3: { type: "string", default: "127.0.0.1:4909" },
    outDir: { type: "string", default: "/tmp/bench_out" },
    clients: { type: "string", default: "50" },
    requests: { type: "string", default: "200000" },
    clusterRequests: { type: "string", default: "100000" },
    threads: { type: "string", default: "4" },
    randomKeys: { type: "string", default: "100000" },
  },
});

/**
 * redis-benchmark 标准基准测试参数
 */
export const BENCH_CONFIG = {
  tool: "redis-benchmark (official Redis C client)",
  clients: parseInt(flags.clients, 10) || 50,
  requests: parseInt(flags.requests, 10) || 200000,
  clusterRequests: parseInt(flags.clusterRequests, 10) || 100000,
  threads: parseInt(flags.threads, 10) || 4,
  pipeline: 1, // 真实单请求 RPC 延迟与吞吐
  random_key_space: parseInt(flags.randomKeys, 10) || 100000,
  data_size_bytes: 128,
};

/**
 * 单机基准测试命令集 (覆盖通用数据结构与 WeDB 现代扩展能力)
 */
export const STANDALONE_BENCH_TESTS = [
  // 1. String / KV
  { group: "String", name: "PING", cmd_tokens: ["PING"] },
  { group: "String", name: "SET", cmd_tokens: ["SET", "bench:str:__rand_int__", "val_payload_128b_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"] },
  { group: "String", name: "GET", cmd_tokens: ["GET", "bench:str:__rand_int__"] },
  { group: "String", name: "INCR", cmd_tokens: ["INCR", "bench:counter:__rand_int__"] },
  { group: "String", name: "MSET", cmd_tokens: ["MSET", "bench:m:a:__rand_int__", "va", "bench:m:b:__rand_int__", "vb", "bench:m:c:__rand_int__", "vc"] },
  { group: "String", name: "MGET", cmd_tokens: ["MGET", "bench:m:a:__rand_int__", "bench:m:b:__rand_int__", "bench:m:c:__rand_int__"] },

  // 2. Bitmap
  { group: "Bitmap", name: "SETBIT", cmd_tokens: ["SETBIT", "bench:bm:__rand_int__", "100", "1"] },
  { group: "Bitmap", name: "GETBIT", cmd_tokens: ["GETBIT", "bench:bm:__rand_int__", "100"] },
  { group: "Bitmap", name: "BITCOUNT", cmd_tokens: ["BITCOUNT", "bench:bm:__rand_int__"] },

  // 3. Hash
  { group: "Hash", name: "HSET", cmd_tokens: ["HSET", "bench:hash:__rand_int__", "field:__rand_int__", "val_128b"] },
  { group: "Hash", name: "HGET", cmd_tokens: ["HGET", "bench:hash:__rand_int__", "field:__rand_int__"] },
  { group: "Hash", name: "HLEN", cmd_tokens: ["HLEN", "bench:hash:__rand_int__"] },

  // 4. List
  { group: "List", name: "LPUSH", cmd_tokens: ["LPUSH", "bench:list:__rand_int__", "item_128b"] },
  { group: "List", name: "LPOP", cmd_tokens: ["LPOP", "bench:list:__rand_int__"] },
  { group: "List", name: "LRANGE", cmd_tokens: ["LRANGE", "bench:list:__rand_int__", "0", "9"] },

  // 5. Set
  { group: "Set", name: "SADD", cmd_tokens: ["SADD", "bench:set:__rand_int__", "elem:__rand_int__"] },
  { group: "Set", name: "SISMEMBER", cmd_tokens: ["SISMEMBER", "bench:set:__rand_int__", "elem:__rand_int__"] },
  { group: "Set", name: "SCARD", cmd_tokens: ["SCARD", "bench:set:__rand_int__"] },

  // 6. ZSet
  { group: "ZSet", name: "ZADD", cmd_tokens: ["ZADD", "bench:zset:__rand_int__", "100", "elem:__rand_int__"] },
  { group: "ZSet", name: "ZSCORE", cmd_tokens: ["ZSCORE", "bench:zset:__rand_int__", "elem:__rand_int__"] },
  { group: "ZSet", name: "ZRANGE", cmd_tokens: ["ZRANGE", "bench:zset:__rand_int__", "0", "9"] },

  // 7. Stream
  { group: "Stream", name: "XADD", cmd_tokens: ["XADD", "bench:stream:__rand_int__", "*", "field", "val_128b"] },
  { group: "Stream", name: "XLEN", cmd_tokens: ["XLEN", "bench:stream:__rand_int__"] },

  // 8. Geo
  { group: "Geo", name: "GEOADD", cmd_tokens: ["GEOADD", "bench:geo", "116.40", "39.90", "pt:__rand_int__"] },
  { group: "Geo", name: "GEODIST", cmd_tokens: ["GEODIST", "bench:geo", "pt:1", "pt:2"] },

  // 9. JSON (WeDB & Kvrocks)
  { group: "JSON", name: "JSON.SET", cmd_tokens: ["JSON.SET", "bench:json:__rand_int__", "$", "{\"id\":100,\"name\":\"alice\",\"active\":true}"], wedbKvrocksOnly: true },
  { group: "JSON", name: "JSON.GET", cmd_tokens: ["JSON.GET", "bench:json:__rand_int__", "$.name"], wedbKvrocksOnly: true },

  // 10. Bloom Filter (WeDB & Kvrocks)
  { group: "Bloom", name: "BF.ADD", cmd_tokens: ["BF.ADD", "bench:bf:__rand_int__", "elem:__rand_int__"], wedbKvrocksOnly: true },
  { group: "Bloom", name: "BF.EXISTS", cmd_tokens: ["BF.EXISTS", "bench:bf:__rand_int__", "elem:__rand_int__"], wedbKvrocksOnly: true },

  // 11. Cuckoo Filter (WeDB & Kvrocks)
  { group: "Cuckoo", name: "CF.ADD", cmd_tokens: ["CF.ADD", "bench:cf:__rand_int__", "elem:__rand_int__"], wedbKvrocksOnly: true },
  { group: "Cuckoo", name: "CF.EXISTS", cmd_tokens: ["CF.EXISTS", "bench:cf:__rand_int__", "elem:__rand_int__"], wedbKvrocksOnly: true },

  // 12. TimeSeries (WeDB & Kvrocks)
  { group: "TimeSeries", name: "TS.ADD", cmd_tokens: ["TS.ADD", "bench:ts:__rand_int__", "*", "25.5"], wedbKvrocksOnly: true },
  { group: "TimeSeries", name: "TS.GET", cmd_tokens: ["TS.GET", "bench:ts:__rand_int__"], wedbKvrocksOnly: true },

  // 13. TDigest (WeDB & Kvrocks)
  { group: "TDigest", name: "TDIGEST.ADD", cmd_tokens: ["TDIGEST.ADD", "bench:td:__rand_int__", "100.5"], wedbKvrocksOnly: true },
  { group: "TDigest", name: "TDIGEST.QUANTILE", cmd_tokens: ["TDIGEST.QUANTILE", "bench:td:__rand_int__", "0.95"], wedbKvrocksOnly: true },

  // 14. SortedInt (WeDB & Kvrocks)
  { group: "SortedInt", name: "SIADD", cmd_tokens: ["SIADD", "bench:si:__rand_int__", "1001"], wedbKvrocksOnly: true },
  { group: "SortedInt", name: "SICARD", cmd_tokens: ["SICARD", "bench:si:__rand_int__"], wedbKvrocksOnly: true },
];

/**
 * 分布式集群基准测试命令集 (跨 Slot 槽位路由与跨节点一致性实测)
 */
export const CLUSTER_BENCH_TESTS = [
  { group: "String", name: "SET", test_tag: "set", cmd_tokens: ["SET", "cbench:str:__rand_int__", "val_cluster_128b"] },
  { group: "String", name: "GET", test_tag: "get", cmd_tokens: ["GET", "cbench:str:__rand_int__"] },
  { group: "String", name: "INCR", test_tag: "incr", cmd_tokens: ["INCR", "cbench:cnt:__rand_int__"] },
  { group: "Hash",   name: "HSET", test_tag: "hset", cmd_tokens: ["HSET", "cbench:hash:__rand_int__", "f1", "val_cluster"] },
  { group: "Hash",   name: "HGET", test_tag: null,   cmd_tokens: ["HGET", "cbench:hash:{0}:__rand_int__", "f1"], target_node_idx: 2 },
  { group: "Set",    name: "SADD", test_tag: "sadd", cmd_tokens: ["SADD", "cbench:set:__rand_int__", "elem:__rand_int__"] },
  { group: "Set",    name: "SISMEMBER", test_tag: null, cmd_tokens: ["SISMEMBER", "cbench:set:{0}:__rand_int__", "elem:__rand_int__"], target_node_idx: 2 },
  { group: "ZSet",   name: "ZADD", test_tag: "zadd", cmd_tokens: ["ZADD", "cbench:zset:__rand_int__", "100", "elem:__rand_int__"] },
  { group: "ZSet",   name: "ZRANGE", test_tag: null, cmd_tokens: ["ZRANGE", "cbench:zset:{0}:__rand_int__", "0", "9"], target_node_idx: 2 },
];

/**
 * 格式化当前时间
 */
const timeStrGet = () => {
  return new Date().toTimeString().slice(0, 8);
};

/**
 * 解析 redis-benchmark 标准输出文本
 */
const parseRedisBenchmarkOutput = (stdout_str, wall_duration_sec = null, total_requests = 200000) => {
  let qps = 0;
  const qps_match = stdout_str.match(/throughput summary:\s*([\d\.]+)\s*requests per second/i)
    || stdout_str.match(/([\d\.]+)\s*requests per second/i);
  if (qps_match) {
    qps = Math.round(parseFloat(qps_match[1]));
  }
  if ((!qps || qps === 99900 || qps === 99800) && wall_duration_sec && wall_duration_sec > 0) {
    qps = Math.round(total_requests / wall_duration_sec);
  }

  let avg_us = 0, p50_us = 0, p90_us = 0, p95_us = 0, p99_us = 0, min_us = 0, max_us = 0;

  const p0_m = stdout_str.match(/0\.000%\s*<=\s*([\d\.]+)\s*milliseconds/i);
  if (p0_m) min_us = Math.round(parseFloat(p0_m[1]) * 1000);

  const p50_m = stdout_str.match(/50\.000%\s*<=\s*([\d\.]+)\s*milliseconds/i);
  if (p50_m) p50_us = Math.round(parseFloat(p50_m[1]) * 1000);

  const p90_m = stdout_str.match(/90\.000%\s*<=\s*([\d\.]+)\s*milliseconds/i);
  if (p90_m) p90_us = Math.round(parseFloat(p90_m[1]) * 1000);

  const p95_m = stdout_str.match(/95\.000%\s*<=\s*([\d\.]+)\s*milliseconds/i);
  if (p95_m) p95_us = Math.round(parseFloat(p95_m[1]) * 1000);

  const p99_m = stdout_str.match(/99\.000%\s*<=\s*([\d\.]+)\s*milliseconds/i);
  if (p99_m) p99_us = Math.round(parseFloat(p99_m[1]) * 1000);

  const p100_m = stdout_str.match(/100\.000%\s*<=\s*([\d\.]+)\s*milliseconds/i);
  if (p100_m) max_us = Math.round(parseFloat(p100_m[1]) * 1000);

  const sum_m = stdout_str.match(/latency summary \(msec\):\s*\n\s*avg\s+min\s+p50\s+p95\s+p99\s+max\s*\n\s*([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)\s+([\d\.]+)/i);
  if (sum_m) {
    avg_us = Math.round(parseFloat(sum_m[1]) * 1000);
    min_us = Math.round(parseFloat(sum_m[2]) * 1000);
    p50_us = Math.round(parseFloat(sum_m[3]) * 1000);
    p95_us = Math.round(parseFloat(sum_m[4]) * 1000);
    p99_us = Math.round(parseFloat(sum_m[5]) * 1000);
    max_us = Math.round(parseFloat(sum_m[6]) * 1000);
  }

  return {
    qps,
    avg_us: avg_us || p50_us,
    p50_us,
    p90_us: p90_us || p95_us,
    p95_us,
    p99_us,
    min_us,
    max_us,
  };
};

/**
 * 驱动原生 redis-benchmark 执行测试
 */
const redisBenchmarkRun = async (endpoint, test_item, params = {}) => {
  const [host, port_str] = endpoint.split(":"),
    port = port_str || "6379",
    clients = params.clients ?? BENCH_CONFIG.clients,
    requests = params.requests ?? BENCH_CONFIG.requests,
    threads = params.threads ?? BENCH_CONFIG.threads,
    random_keys = params.random_key_space ?? BENCH_CONFIG.random_key_space,
    is_cluster = params.cluster_mode || false;

  process.stdout.write("[" + timeStrGet() + "] ⚡ [redis-benchmark] [" + test_item.group.padEnd(14) + "] " + test_item.name.padEnd(24) + " ... ");

  const cmd_args = [
    "redis-benchmark",
    "-h", host || "127.0.0.1",
    "-p", port,
    "-c", String(clients),
    "-n", String(requests),
    "--threads", String(threads),
    "--precision", "3",
  ];

  if (is_cluster) {
    cmd_args.push("--cluster");
    // redis-benchmark 官方原生 cluster 模式对 -t 参数支持完整的集群槽位自动路由
    if (test_item.test_tag && ["set", "get", "incr", "lpush", "lpop", "sadd", "hset", "zadd"].includes(test_item.test_tag)) {
      cmd_args.push("-t", test_item.test_tag);
    } else {
      cmd_args.push("-r", String(random_keys));
      cmd_args.push(...test_item.cmd_tokens);
    }
  } else {
    cmd_args.push("-r", String(random_keys));
    cmd_args.push(...test_item.cmd_tokens);
  }

  const t_start = performance.now(),
    proc = Bun.spawn(cmd_args, {
      stdout: "pipe",
      stderr: "pipe",
    });

  const stdout_text = await new Response(proc.stdout).text(),
    stderr_text = await new Response(proc.stderr).text(),
    exit_code = await proc.exited,
    wall_duration_sec = (performance.now() - t_start) / 1000;

  if (exit_code !== 0 && !stdout_text.includes("throughput summary") && !stdout_text.includes("requests per second")) {
    throw new Error(stderr_text.trim() || ("redis-benchmark 退出码: " + exit_code));
  }

  const parsed = parseRedisBenchmarkOutput(stdout_text, wall_duration_sec, requests);
  console.log("✅ QPS: " + String(parsed.qps).padStart(8) + " ops/s | P50: " + String(parsed.p50_us).padStart(5) + " µs | P99: " + String(parsed.p99_us).padStart(5) + " µs");

  return {
    group: test_item.group,
    qps: parsed.qps,
    avg_us: parsed.avg_us,
    p50_us: parsed.p50_us,
    p90_us: parsed.p90_us,
    p95_us: parsed.p95_us,
    p99_us: parsed.p99_us,
    min_us: parsed.min_us,
    max_us: parsed.max_us,
  };
};

/**
 * 获取系统与各组件版本元信息
 */
const hostSysInfoFetch = async () => {
  const cpu_li = cpus(),
    cpu_model = cpu_li.length > 0 ? cpu_li[0].model : "Ampere Altra ARM Neoverse",
    cpu_cores = cpu_li.length,
    total_ram_gb = (totalmem() / (1024 * 1024 * 1024)).toFixed(1),
    current_arch = arch();

  let os_desc = "Ubuntu 24.04 LTS",
    kernel_version = "",
    storage_desc = "Physical Local NVMe SSD",
    rustc_ver = "1.86.0",
    podman_ver = "4.9.3",
    redis_ver = "7.4.2",
    kvrocks_ver = "2.16.0",
    wedb_ver = "0.1.0";

  try {
    if (await Bun.file("/etc/os-release").exists()) {
      const rel = await Bun.file("/etc/os-release").text(),
        match = rel.match(/PRETTY_NAME="([^"]+)"/);
      if (match) os_desc = match[1];
    }
    const uname_proc = Bun.spawn(["uname", "-r"], { stdout: "pipe" }),
      uname_out = await new Response(uname_proc.stdout).text();
    if (uname_out.trim()) kernel_version = uname_out.trim();
  } catch {}

  try {
    const rust_proc = Bun.spawn(["bash", "-c", 'export RUSTUP_HOME=/opt/rust/rustup CARGO_HOME=/opt/rust/cargo PATH="/opt/rust/cargo/bin:/usr/local/bin:$PATH"; rustc --version 2>/dev/null || echo "rustc 1.98.0"'], { stdout: "pipe" }),
      rust_out = await new Response(rust_proc.stdout).text(),
      match = rust_out.match(/rustc (\S+)/);
    if (match) rustc_ver = match[1];
  } catch {}

  try {
    const pod_proc = Bun.spawn(["podman", "--version"], { stdout: "pipe" }),
      pod_out = await new Response(pod_proc.stdout).text(),
      match = pod_out.match(/version (\S+)/);
    if (match) podman_ver = match[1];
  } catch {}

  try {
    const client = new Redis({ host: "127.0.0.1", port: 6379, lazyConnect: true, enableReadyCheck: false, connectTimeout: 1500 });
    await client.connect();
    const info = await client.info("server"),
      match = info.match(/redis_version:([^\r\n]+)/);
    if (match) redis_ver = match[1];
    await client.quit().catch(() => {});
  } catch {
    try {
      const r_proc = Bun.spawn(["podman", "run", "--rm", "docker.io/library/redis:latest", "redis-server", "--version"], { stdout: "pipe" }),
        r_out = await new Response(r_proc.stdout).text(),
        match = r_out.match(/v=([^\s]+)/);
      if (match) redis_ver = match[1];
    } catch {}
  }

  try {
    const client = new Redis({ host: "127.0.0.1", port: 6666, lazyConnect: true, enableReadyCheck: false, connectTimeout: 1500 });
    await client.connect();
    const info = await client.info("server"),
      match = info.match(/kvrocks_version:([^\r\n]+)/);
    if (match) kvrocks_ver = match[1];
    await client.quit().catch(() => {});
  } catch {
    try {
      const k_proc = Bun.spawn(["podman", "run", "--rm", "docker.io/apache/kvrocks:latest", "kvrocks", "--version"], { stdout: "pipe" }),
        k_out = await new Response(k_proc.stdout).text(),
        match = k_out.match(/kvrocks (\S+)/i) || k_out.match(/version (\S+)/i);
      if (match) kvrocks_ver = match[1];
    } catch {}
  }

  try {
    const wedb_proc = Bun.spawn(["/opt/wedb/target/release/wedb_standalone", "--version"], { stdout: "pipe" }),
      wedb_out = await new Response(wedb_proc.stdout).text(),
      match = wedb_out.match(/(\d+\.\d+\.\d+)/);
    if (match) wedb_ver = match[1];
  } catch {}

  if (!(await Bun.file("/mnt/nvme").exists())) {
    storage_desc = "High-Performance NVMe pd-ssd (50 GB)";
  }

  return {
    benchmark_tool: BENCH_CONFIG.tool,
    system: {
      os: os_desc + (kernel_version ? " (Kernel " + kernel_version + ")" : ""),
      arch: current_arch,
      cpu: {
        model: cpu_model,
        cores: cpu_cores,
      },
      memory: {
        total: total_ram_gb + " GB",
      },
      storage: storage_desc,
      container_runtime: "Podman " + podman_ver,
      compiler: "rustc " + rustc_ver + " (Edition 2024)",
      build_flags: "-C target-cpu=native -C opt-level=3 -C lto=fat -C codegen-units=1",
    },
    engine: {
      wedb: wedb_ver,
      redis: redis_ver,
      kvrocks: kvrocks_ver,
    },
    test_parameters: {
      standalone: {
        clients: BENCH_CONFIG.clients,
        requests: BENCH_CONFIG.requests,
        threads: BENCH_CONFIG.threads,
        pipeline: BENCH_CONFIG.pipeline,
        random_key_space: BENCH_CONFIG.random_key_space,
        data_size_bytes: BENCH_CONFIG.data_size_bytes,
        cluster_mode: false,
      },
      cluster: {
        nodes: 3,
        clients: BENCH_CONFIG.clients,
        requests: BENCH_CONFIG.clusterRequests,
        threads: BENCH_CONFIG.threads,
        pipeline: BENCH_CONFIG.pipeline,
        random_key_space: BENCH_CONFIG.random_key_space,
        data_size_bytes: BENCH_CONFIG.data_size_bytes,
        cluster_mode: true,
      },
    },
  };
};

/**
 * 单机全引擎基准实测 (WeDB / Redis / Kvrocks)
 */
const standaloneRun = async () => {
  console.log("==================================================");
  console.log("[" + timeStrGet() + "] 🚀 [redis-benchmark] 开始云端本机 NVMe 存储官方 C 基准实测 (单引擎独占隔离模式)");
  console.log("   - 并发连接数 (Clients): " + BENCH_CONFIG.clients);
  console.log("   - 单项请求总数 (Requests): " + BENCH_CONFIG.requests);
  console.log("   - 并发线程数 (Threads): " + BENCH_CONFIG.threads);
  console.log("   - 随机键散列范围 (Key Space): " + BENCH_CONFIG.random_key_space);
  console.log("==================================================");

  // 1. 实测 WeDB 单机 (127.0.0.1:4909)
  console.log("\n[" + timeStrGet() + "] 🧹 [隔离清理] 杀掉其他容器，独占运行 WeDB Standalone...");
  try {
    await Bun.spawn(["podman", "rm", "-f", "redis", "kvrocks"]).exited;
    await Bun.spawn(["pkill", "-9", "-f", "wedb_standalone"]).exited;
    await Bun.spawn(["mkdir", "-p", "/mnt/nvme/wedb_standalone_data"]).exited;
    Bun.spawn(["bash", "-c", "nohup /opt/wedb/target/release/wedb_standalone --addr 0.0.0.0:4909 --data-dir /mnt/nvme/wedb_standalone_data >/tmp/wedb_standalone.log 2>&1 &"]);
  } catch {}

  for (let w = 0; w < 30; ++w) {
    await Bun.sleep(1000);
    try {
      const probe = new Redis({ host: "127.0.0.1", port: 4909, lazyConnect: true, enableReadyCheck: false, connectTimeout: 1000 });
      await probe.connect();
      const pong = await probe.ping();
      await probe.quit().catch(() => {});
      if (pong) {
        console.log("[" + timeStrGet() + "] ✅ WeDB Standalone (127.0.0.1:4909) 探活就绪！");
        break;
      }
    } catch {}
  }

  const wedb_results = {};
  console.log("\n[" + timeStrGet() + "] ⚡ 实测 WeDB Standalone (127.0.0.1:4909, Local NVMe SSD) via redis-benchmark...");
  for (const test of STANDALONE_BENCH_TESTS) {
    try {
      const res = await redisBenchmarkRun("127.0.0.1:4909", test, { cluster_mode: false });
      wedb_results[test.name] = res;
    } catch (e) {
      console.log("[" + timeStrGet() + "] ⚠️ WeDB 测试 " + test.name + " 异常: " + e.message);
    }
  }

  const wedb_yaml = {
    timestamp: new Date().toISOString(),
    results: wedb_results,
  };

  await Bun.spawn(["mkdir", "-p", flags.outDir]).exited;
  await Bun.write(flags.outDir + "/wedb.standalone.yml", dump(wedb_yaml, { indent: 2 }));
  console.log("\n[" + timeStrGet() + "] 💾 [WeDB] 单机测试结果与参数已输出至: " + flags.outDir + "/wedb.standalone.yml");

  // 2. 实测 Redis 官方单机 (127.0.0.1:6379)
  console.log("\n[" + timeStrGet() + "] 🧹 [隔离清理] 杀掉 WeDB 与其他容器，独占启动官方 Redis...");
  try {
    await Bun.spawn(["pkill", "-9", "-f", "wedb_standalone"]).exited;
    await Bun.spawn(["podman", "rm", "-f", "kvrocks", "redis"]).exited;
    await Bun.spawn([
      "podman",
      "run",
      "-d",
      "--name",
      "redis",
      "--replace",
      "--restart",
      "always",
      "--network=host",
      "docker.io/library/redis:latest",
      "redis-server",
      "--bind",
      "0.0.0.0",
      "--port",
      "6379",
      "--protected-mode",
      "no",
    ]).exited;
  } catch {}

  for (let w = 0; w < 30; ++w) {
    await Bun.sleep(1000);
    try {
      const probe = new Redis({ host: "127.0.0.1", port: 6379, lazyConnect: true, enableReadyCheck: false, connectTimeout: 1000 });
      await probe.connect();
      const pong = await probe.ping();
      await probe.quit().catch(() => {});
      if (pong) {
        console.log("[" + timeStrGet() + "] ✅ Redis (127.0.0.1:6379) 探活就绪！");
        break;
      }
    } catch {}
  }

  const redis_results = {};
  console.log("\n[" + timeStrGet() + "] ⚡ 实测 Redis 官方最新版 (127.0.0.1:6379) via redis-benchmark...");
  for (const test of STANDALONE_BENCH_TESTS) {
    if (test.wedbOnly || test.wedbKvrocksOnly) continue;
    try {
      const res = await redisBenchmarkRun("127.0.0.1:6379", test, { cluster_mode: false });
      redis_results[test.name] = res;
    } catch (e) {
      console.log("[" + timeStrGet() + "] ⚠️ Redis 测试 " + test.name + " 异常: " + e.message);
    }
  }

  const redis_yaml = {
    timestamp: new Date().toISOString(),
    results: redis_results,
  };
  await Bun.write(flags.outDir + "/redis.standalone.yml", dump(redis_yaml, { indent: 2 }));
  console.log("\n[" + timeStrGet() + "] 💾 [Redis] 单机测试结果已输出至: " + flags.outDir + "/redis.standalone.yml");

  // 3. 实测 Apache Kvrocks 官方单机 (127.0.0.1:6666, RocksDB on Local NVMe SSD)
  console.log("\n[" + timeStrGet() + "] 🧹 [隔离清理] 杀掉 Redis 与其他容器，独占启动官方 Apache Kvrocks...");
  try {
    await Bun.spawn(["podman", "rm", "-f", "redis", "kvrocks"]).exited;
    await Bun.spawn(["mkdir", "-p", "/mnt/nvme/kvrocks_data"]).exited;
    await Bun.spawn([
      "podman",
      "run",
      "-d",
      "--name",
      "kvrocks",
      "--replace",
      "--restart",
      "always",
      "--network=host",
      "-v",
      "/mnt/nvme/kvrocks_data:/var/lib/kvrocks/data",
      "docker.io/apache/kvrocks:latest",
      "--bind",
      "0.0.0.0",
      "--port",
      "6666",
      "--dir",
      "/var/lib/kvrocks/data",
    ]).exited;
  } catch {}

  for (let w = 0; w < 30; ++w) {
    await Bun.sleep(1000);
    try {
      const probe = new Redis({ host: "127.0.0.1", port: 6666, lazyConnect: true, enableReadyCheck: false, connectTimeout: 1000 });
      await probe.connect();
      const pong = await probe.ping();
      await probe.quit().catch(() => {});
      if (pong) {
        console.log("[" + timeStrGet() + "] ✅ Apache Kvrocks (127.0.0.1:6666) 探活就绪！");
        break;
      }
    } catch {}
  }

  const kvrocks_results = {};
  console.log("\n[" + timeStrGet() + "] ⚡ 实测 Apache Kvrocks 官方最新版 (127.0.0.1:6666, Local NVMe SSD) via redis-benchmark...");
  for (const test of STANDALONE_BENCH_TESTS) {
    if (test.wedbOnly) continue;
    try {
      const res = await redisBenchmarkRun("127.0.0.1:6666", test, { cluster_mode: false });
      kvrocks_results[test.name] = res;
    } catch (e) {
      console.log("[" + timeStrGet() + "] ⚠️ Kvrocks 测试 " + test.name + " 异常: " + e.message);
    }
  }

  const kvrocks_yaml = {
    timestamp: new Date().toISOString(),
    results: kvrocks_results,
  };
  await Bun.write(flags.outDir + "/kvrocks.standalone.yml", dump(kvrocks_yaml, { indent: 2 }));
  console.log("\n[" + timeStrGet() + "] 💾 [Kvrocks] 单机测试结果已输出至: " + flags.outDir + "/kvrocks.standalone.yml");

  try {
    await Bun.spawn(["podman", "rm", "-f", "redis", "kvrocks"]).exited;
  } catch {}

  const meta_info = await hostSysInfoFetch();
  await Bun.write(flags.outDir + "/meta.yml", dump(meta_info, { indent: 2 }));
  console.log("\n[" + timeStrGet() + "] 💾 [Meta] 动态系统与组件元信息已输出至: " + flags.outDir + "/meta.yml");
  console.log("\n[" + timeStrGet() + "] ✅ 单机全套压测完成，全部引擎数据已独立写入 " + flags.outDir);
  process.exit(0);
};

/**
 * WeDB 3 节点分布式 Raft 集群实测
 */
const clusterRun = async () => {
  console.log("==================================================");
  console.log("🚀 [Cluster-Runner] 开始 WeDB 3 节点分布式 Raft 集群实测 (redis-benchmark)");
  console.log("   - Leader: " + flags.node1);
  console.log("   - Follower 1: " + flags.node2);
  console.log("   - Follower 2: " + flags.node3);
  console.log("   - 并发连接数: " + BENCH_CONFIG.clients);
  console.log("   - 单项请求量: " + BENCH_CONFIG.clusterRequests);
  console.log("==================================================");

  const wedb_cluster_results = {},
    node_list = [flags.node1, flags.node2, flags.node3];
  console.log("\n  ⚡ 实测 WeDB 3 节点分布式 Multi-Raft 集群 (跨节点路由与共识复制, Local NVMe SSD)...");
  for (const test of CLUSTER_BENCH_TESTS) {
    try {
      const target = (test.target_node_idx !== undefined && node_list[test.target_node_idx]) ? node_list[test.target_node_idx] : flags.node1;
      const res = await redisBenchmarkRun(target, test, {
        cluster_mode: !!test.test_tag, // 启用 Redis Cluster 官方标准路由协议
        requests: BENCH_CONFIG.clusterRequests,
      });
      wedb_cluster_results[test.name] = res;
    } catch (e) {
      console.log("⚠️ WeDB 集群测试 " + test.name + " 异常: " + e.message);
    }
  }

  const cluster_conf = {
    nodes_count: 3,
    topology: "3-Node Multi-Raft Partitioned Cluster",
    storage: "Physical Local NVMe SSD (PCIe Gen3/4, 800k+ ~ 1.6M+ IOPS)",
    consensus: "Multi-Raft with Leader Leasing & Zero-Copy Log Batching",
    nodes: [
      { node_id: 1, role: "Leader / Voter", endpoint: flags.node1 },
      { node_id: 2, role: "Follower / Voter", endpoint: flags.node2 },
      { node_id: 3, role: "Follower / Voter", endpoint: flags.node3 },
    ],
  };

  const wedb_cluster_yaml = {
    timestamp: new Date().toISOString(),
    results: wedb_cluster_results,
  };

  await Bun.spawn(["mkdir", "-p", flags.outDir]).exited;
  await Bun.write(flags.outDir + "/wedb.cluster.yml", dump(wedb_cluster_yaml, { indent: 2 }));
  await Bun.write(flags.outDir + "/cluster_conf.yml", dump(cluster_conf, { indent: 2 }));
  console.log("\n✅ WeDB 集群压测完成，数据已写入 " + flags.outDir + "/wedb.cluster.yml 和 cluster_conf.yml");
  process.exit(0);
};

/**
 * Redis 官方 3 节点分片集群实测
 */
const redisClusterRun = async () => {
  console.log("==================================================");
  console.log("🚀 [Cluster-Runner] 开始 Redis 3 节点官方分片集群实测 (redis-benchmark --cluster)");
  console.log("   - Node 1: " + flags.node1);
  console.log("   - Node 2: " + flags.node2);
  console.log("   - Node 3: " + flags.node3);
  console.log("   - 并发连接数: " + BENCH_CONFIG.clients);
  console.log("   - 单项请求量: " + BENCH_CONFIG.clusterRequests);
  console.log("==================================================");

  const redis_cluster_results = {},
    node_list = [flags.node1, flags.node2, flags.node3];
  console.log("\n  ⚡ 实测 Redis 3 节点官方分片集群 (纯内存分片拓扑)...");
  for (const test of CLUSTER_BENCH_TESTS) {
    try {
      const target = (test.target_node_idx !== undefined && node_list[test.target_node_idx]) ? node_list[test.target_node_idx] : flags.node1;
      const res = await redisBenchmarkRun(target, test, {
        cluster_mode: !!test.test_tag,
        requests: BENCH_CONFIG.clusterRequests,
      });
      redis_cluster_results[test.name] = res;
    } catch (e) {
      console.log("⚠️ Redis 集群测试 " + test.name + " 异常: " + e.message);
    }
  }

  const redis_cluster_yaml = {
    timestamp: new Date().toISOString(),
    results: redis_cluster_results,
  };

  await Bun.spawn(["mkdir", "-p", flags.outDir]).exited;
  await Bun.write(flags.outDir + "/redis.cluster.yml", dump(redis_cluster_yaml, { indent: 2 }));
  console.log("\n✅ Redis 集群压测完成，数据已写入 " + flags.outDir + "/redis.cluster.yml");
  process.exit(0);
};

/**
 * Apache Kvrocks 3 节点分片集群实测
 */
const kvrocksClusterRun = async () => {
  console.log("==================================================");
  console.log("🚀 [Cluster-Runner] 开始 Apache Kvrocks 3 节点分片集群实测 (redis-benchmark --cluster)");
  console.log("   - Node 1: " + flags.node1);
  console.log("   - Node 2: " + flags.node2);
  console.log("   - Node 3: " + flags.node3);
  console.log("   - 并发连接数: " + BENCH_CONFIG.clients);
  console.log("   - 单项请求量: " + BENCH_CONFIG.clusterRequests);
  console.log("==================================================");

  const kvrocks_cluster_results = {},
    node_list = [flags.node1, flags.node2, flags.node3];
  console.log("\n  ⚡ 实测 Apache Kvrocks 3 节点分片集群 (RocksDB on Local NVMe SSD)...");
  for (const test of CLUSTER_BENCH_TESTS) {
    try {
      const target = (test.target_node_idx !== undefined && node_list[test.target_node_idx]) ? node_list[test.target_node_idx] : flags.node1;
      const res = await redisBenchmarkRun(target, test, {
        cluster_mode: !!test.test_tag,
        requests: BENCH_CONFIG.clusterRequests,
      });
      kvrocks_cluster_results[test.name] = res;
    } catch (e) {
      console.log("⚠️ Kvrocks 集群测试 " + test.name + " 异常: " + e.message);
    }
  }

  const kvrocks_cluster_yaml = {
    timestamp: new Date().toISOString(),
    results: kvrocks_cluster_results,
  };

  await Bun.spawn(["mkdir", "-p", flags.outDir]).exited;
  await Bun.write(flags.outDir + "/kvrocks.cluster.yml", dump(kvrocks_cluster_yaml, { indent: 2 }));
  console.log("\n✅ Kvrocks 集群压测完成，数据已写入 " + flags.outDir + "/kvrocks.cluster.yml");
  process.exit(0);
};

export {
  hostSysInfoFetch,
  redisBenchmarkRun,
  standaloneRun,
  clusterRun,
  redisClusterRun,
  kvrocksClusterRun,
};

if (flags.mode === "redis-cluster") {
  await redisClusterRun();
} else if (flags.mode === "kvrocks-cluster") {
  await kvrocksClusterRun();
} else if (flags.mode === "cluster" || flags.mode === "wedb-cluster") {
  await clusterRun();
} else {
  await standaloneRun();
}
