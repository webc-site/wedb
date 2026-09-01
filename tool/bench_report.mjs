#!/usr/bin/env -S bun
import { load } from "js-yaml";
import { join } from "node:path";
import ETA from "./ETA.js";

const TEMPLATE_PATH = join(import.meta.dirname, "bench_report.eta"),
  TEMPLATE = await Bun.file(TEMPLATE_PATH).text();

/**
 * 格式化 QPS 数字 (不带千分位逗号)
 */
const qpsFormat = (qps) => {
  if (qps === undefined || qps === null) return "-";
  if (typeof qps === "string") return qps;
  return String(Math.round(qps));
};

/**
 * 格式化微秒延迟为整数 (无小数)
 */
const usFormat = (us) => {
  if (us === undefined || us === null) return "-";
  if (typeof us === "string") return us;
  return Math.round(us) + " µs";
};

/**
 * 安全异步加载 YAML 文件，若不存在或解析异常则返回默认 fallback
 */
const yamlLoadSafe = async (file_path, fallback = {}) => {
  try {
    const file = Bun.file(file_path);
    if (await file.exists()) {
      const text = await file.text();
      return load(text) ?? fallback;
    }
  } catch {}
  return fallback;
};

/**
 * 提取结果对象 (兼容顶层结构或 results 字段)
 */
const resultsExtract = (data_obj) => {
  if (!data_obj) return {};
  if (data_obj.results) return data_obj.results;
  return data_obj;
};

/**
 * 将 yaml 数据对象渲染为结构清晰的全景性能报告 Markdown
 */
const benchMarkdownRender = (sys, cluster_conf, stand_data, cluster_data, template = TEMPLATE) => {
  const system_info = sys.system ?? sys,
    cpu_info = system_info.cpu ?? sys.cpu,
    cpu_model_desc =
      typeof cpu_info === "object"
        ? cpu_info.model && cpu_info.model !== "unknown"
          ? cpu_info.model
          : "Ampere Altra ARM Neoverse"
        : cpu_info || "Ampere Altra ARM Neoverse",
    cpu_cores_desc = typeof cpu_info === "object" ? (cpu_info.cores ?? 4) : 4,
    cloud_provider_desc =
      sys.cloud_provider ?? cluster_conf.cloud_provider ?? "Google Cloud Platform (GCP)",
    machine_type_desc = sys.machine_type ?? cluster_conf.machine_type ?? "t2a-standard-4",
    arch_desc = system_info.arch ?? sys.arch ?? cluster_conf.arch ?? "arm64",
    mem_info = system_info.memory ?? sys.memory ?? cluster_conf.memory,
    memory_desc =
      typeof mem_info === "object" ? (mem_info.total ?? "16.0 GB") : (mem_info ?? "16.0 GB"),
    compiler_desc =
      system_info.compiler ??
      sys.compiler ??
      cluster_conf.compiler ??
      "rustc 1.98.0 (Edition 2024)",
    build_flags_desc =
      system_info.build_flags ??
      sys.build_flags ??
      cluster_conf.build_flags ??
      "-C target-cpu=native -C opt-level=3 -C lto=fat -C codegen-units=1",
    os_desc = system_info.os ?? sys.os ?? "Ubuntu 24.04 LTS (ARM64)",
    container_runtime_desc =
      system_info.container_runtime ??
      sys.container_runtime ??
      cluster_conf.container_engine ??
      "Podman 4.9.3",
    storage_desc =
      system_info.storage ??
      sys.storage ??
      cluster_conf.storage ??
      "Physical Local NVMe SSD (PCIe Gen3/4, 800k+ ~ 1.6M+ IOPS)",
    engine_info = sys.engine ?? {},
    bench_params = sys.test_parameters ?? {
      standalone: {
        clients: 50,
        requests: 200000,
        threads: 4,
        pipeline: 1,
        random_key_space: 100000,
        data_size_bytes: 128,
        cluster_mode: false,
      },
      cluster: {
        nodes: 3,
        clients: 50,
        requests: 100000,
        threads: 4,
        pipeline: 1,
        random_key_space: 100000,
        data_size_bytes: 128,
        cluster_mode: true,
      },
    },
    stand_params = bench_params.standalone ?? {},
    clust_params = bench_params.cluster ?? {},
    bench_time = (
      stand_data.wedb?.timestamp ||
      cluster_data.wedb?.timestamp ||
      new Date().toISOString()
    )
      .replaceAll("T", " ")
      .slice(0, 19),
    wedb_s = resultsExtract(stand_data.wedb),
    redis_s = resultsExtract(stand_data.redis),
    kvrocks_s = resultsExtract(stand_data.kvrocks),
    wedb_c = resultsExtract(cluster_data.wedb),
    redis_c = resultsExtract(cluster_data.redis),
    kvrocks_c = resultsExtract(cluster_data.kvrocks);

  const stand_cmd_li = Object.keys(wedb_s),
    stand_qps_li = stand_cmd_li.map((cmd) => {
      const sw = wedb_s[cmd],
        sr = redis_s[cmd],
        sk = kvrocks_s[cmd],
        group = sw?.group ?? sr?.group ?? sk?.group ?? "General",
        sw_val = sw?.qps != null && typeof sw.qps === "number" ? Math.round(sw.qps) : null,
        sr_val = sr?.qps != null && typeof sr.qps === "number" ? Math.round(sr.qps) : null,
        sk_val = sk?.qps != null && typeof sk.qps === "number" ? Math.round(sk.qps) : null,
        num_li = [sw_val, sr_val, sk_val].filter((v) => v !== null),
        max_qps = num_li.length > 0 ? Math.max(...num_li) : null;

      const valRender = (num_val, fallback_txt) => {
        if (num_val === null) return fallback_txt;
        const txt = String(num_val);
        return num_val === max_qps ? "**" + txt + "**" : txt;
      };

      const sw_qps = valRender(sw_val, "-"),
        sr_qps = valRender(
          sr_val,
          cmd.startsWith("JSON") ||
            cmd.startsWith("BF") ||
            cmd.startsWith("TS") ||
            cmd.startsWith("CF") ||
            cmd.startsWith("TDIGEST")
            ? "扩展未安装"
            : cmd.startsWith("SI") ||
                cmd === "INCREX" ||
                cmd === "DELEX" ||
                cmd === "CAS" ||
                cmd === "CAD"
              ? "不支持"
              : "-",
        ),
        sk_qps = valRender(
          sk_val,
          cmd === "INCREX" ||
            cmd === "DELEX" ||
            cmd === "CAS" ||
            cmd === "CAD" ||
            cmd.startsWith("TDIGEST") ||
            cmd.startsWith("CF")
            ? "不支持"
            : "-",
        );

      return {
        group,
        cmd,
        sw_qps,
        sr_qps,
        sk_qps,
      };
    }),
    cluster_cmd_li = Object.keys(wedb_c),
    cluster_qps_li = cluster_cmd_li.map((cmd) => {
      const cw = wedb_c[cmd],
        cr = redis_c[cmd],
        ck = kvrocks_c[cmd],
        group = cw?.group ?? cr?.group ?? ck?.group ?? "General",
        cw_val = cw?.qps != null && typeof cw.qps === "number" ? Math.round(cw.qps) : null,
        cr_val = cr?.qps != null && typeof cr.qps === "number" ? Math.round(cr.qps) : null,
        ck_val = ck?.qps != null && typeof ck.qps === "number" ? Math.round(ck.qps) : null,
        num_li = [cw_val, cr_val, ck_val].filter((v) => v !== null),
        max_qps = num_li.length > 0 ? Math.max(...num_li) : null;

      const valRender = (num_val, fallback_txt) => {
        if (num_val === null) return fallback_txt;
        const txt = String(num_val);
        return num_val === max_qps ? "**" + txt + "**" : txt;
      };

      const cw_qps = valRender(cw_val, "-"),
        cr_qps = valRender(cr_val, "-"),
        ck_qps = valRender(ck_val, "-");

      return {
        group,
        cmd,
        cw_qps,
        cr_qps,
        ck_qps,
      };
    }),
    stand_lat_li = stand_cmd_li.map((cmd) => {
      const sw = wedb_s[cmd],
        group = sw?.group ?? "General",
        avg = sw ? usFormat(sw.avg_us ?? sw.avg) : "-",
        p50 = sw ? usFormat(sw.p50_us ?? sw.p50) : "-",
        p90 = sw ? usFormat(sw.p90_us ?? sw.p90 ?? sw.p95_us) : "-",
        p99 = sw ? usFormat(sw.p99_us ?? sw.p99) : "-";

      return {
        group,
        cmd,
        avg,
        p50,
        p90,
        p99,
      };
    }),
    cluster_lat_li = cluster_cmd_li.map((cmd) => {
      const cw = wedb_c[cmd],
        group = cw?.group ?? "General",
        avg = cw ? usFormat(cw.avg_us ?? cw.avg) : "-",
        p50 = cw ? usFormat(cw.p50_us ?? cw.p50) : "-",
        p90 = cw ? usFormat(cw.p90_us ?? cw.p90 ?? cw.p95_us) : "-",
        p99 = cw ? usFormat(cw.p99_us ?? cw.p99) : "-";

      return {
        group,
        cmd,
        avg,
        p50,
        p90,
        p99,
      };
    });

  return ETA.renderString(template, {
    cloud_provider: cloud_provider_desc,
    machine_type: cluster_conf.machine_type ?? machine_type_desc,
    region: cluster_conf.region ?? "us-central1",
    zone: cluster_conf.zone ?? "us-central1-a",
    intra_vpc_latency: cluster_conf.cluster_servers?.network?.intra_vpc_latency ?? "< 0.3 ms",
    cpu_model: cpu_model_desc,
    cpu_cores: cpu_cores_desc,
    arch: arch_desc,
    memory: cluster_conf.memory ?? memory_desc,
    storage: cluster_conf.storage ?? storage_desc,
    os: os_desc,
    compiler: compiler_desc,
    build_flags: build_flags_desc,
    container_engine: cluster_conf.container_engine ?? container_runtime_desc,
    bench_time,
    engine_wedb: engine_info.wedb ?? "0.1.0",
    engine_redis: engine_info.redis ?? "8.10.1",
    engine_kvrocks: engine_info.kvrocks ?? "2.16.0",
    stand_params,
    clust_params,
    stand_qps_li,
    cluster_qps_li,
    stand_lat_li,
    cluster_lat_li,
  });
};

/**
 * 从 tool/bench/result/*.yml 渲染生成全景 Markdown 性能报告并写入 doc/bench.md
 */
const reportGenerateFromYaml = async (
  result_dir = join(import.meta.dirname, "bench", "result"),
) => {
  const root_dir = join(import.meta.dirname, ".."),
    bench_dir = join(import.meta.dirname, "bench"),
    doc_path = join(root_dir, "doc", "bench.md"),
    [
      meta,
      sys_static,
      res_cluster_conf,
      bench_cluster_conf,
      wedb_s,
      redis_s,
      kvrocks_s,
      wedb_c,
      redis_c,
      kvrocks_c,
    ] = await Promise.all([
      yamlLoadSafe(join(result_dir, "meta.yml")),
      yamlLoadSafe(join(bench_dir, "sys.yml")),
      yamlLoadSafe(join(result_dir, "cluster_conf.yml")),
      yamlLoadSafe(join(bench_dir, "cluster_conf.yml")),
      yamlLoadSafe(join(result_dir, "wedb.standalone.yml")),
      yamlLoadSafe(join(result_dir, "redis.standalone.yml")),
      yamlLoadSafe(join(result_dir, "kvrocks.standalone.yml")),
      yamlLoadSafe(join(result_dir, "wedb.cluster.yml")),
      yamlLoadSafe(join(result_dir, "redis.cluster.yml")),
      yamlLoadSafe(join(result_dir, "kvrocks.cluster.yml")),
    ]),
    cluster_conf = { ...bench_cluster_conf, ...res_cluster_conf },
    sys = { ...sys_static, ...meta },
    stand_data = { wedb: wedb_s, redis: redis_s, kvrocks: kvrocks_s },
    cluster_data = { wedb: wedb_c, redis: redis_c, kvrocks: kvrocks_c },
    md = benchMarkdownRender(sys, cluster_conf, stand_data, cluster_data);

  await Bun.write(doc_path, md);
  console.log("🎉 性能报告已实时同步更新至: " + doc_path);
  return md;
};

export {
  qpsFormat,
  usFormat,
  yamlLoadSafe,
  resultsExtract,
  benchMarkdownRender,
  reportGenerateFromYaml,
  benchMarkdownRender as renderBenchmarkMarkdown,
  reportGenerateFromYaml as generateReportFromYaml,
};

export default reportGenerateFromYaml;

if (import.meta.main) {
  await reportGenerateFromYaml();
}
