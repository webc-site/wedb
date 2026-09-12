import { resolve } from "node:path";

const LIB_DIR = import.meta.dirname,
  BENCH_DIR = resolve(LIB_DIR, "../.."),
  DEFAULT_JSON_PATH = resolve(BENCH_DIR, "data/latest.json"),
  TYPE_TP = "throughput",
  TYPE_LAT = "latency",
  TYPE_SIZE = "size",
  TYPE_NA = "na";

export const benchDataLoad = async (json_path = DEFAULT_JSON_PATH) =>
    await Bun.file(json_path).json(),
  metricByKey = (engine, key) => {
    const { metrics } = engine;
    return (
      metrics.find((m) => m.key === key) ??
      (key === "memory"
        ? metrics.find((m) => m.key === "peak_memory")
        : key === "peak_memory"
        ? metrics.find((m) => m.key === "memory")
        : undefined)
    );
  },
  multiplierCalc = (metric, min_rate, max_duration) => {
    if (!metric || metric.type === TYPE_NA) return "N/A";
    if (metric.type === TYPE_TP) {
      if (min_rate > 0 && metric.rate > 0) {
        return (metric.rate / min_rate).toFixed(2) + "X";
      }
      return "1.00X";
    }
    if (metric.type === TYPE_LAT) {
      if (max_duration > 0) {
        const dur = Math.max(0.001, metric.duration_ms ?? 0);
        return (max_duration / dur).toFixed(2) + "X";
      }
      return "1.00X";
    }
    return metric.formatted;
  },
  metricSummary = (engine_li, key) => {
    const val_li = engine_li.map((e) => metricByKey(e, key)).filter(Boolean),
      tp_li = val_li
        .filter((m) => m.type === TYPE_TP && m.rate > 0)
        .map((m) => m.rate),
      lat_li = val_li
        .filter((m) => m.type === TYPE_LAT && m.duration_ms > 0)
        .map((m) => m.duration_ms),
      size_li = val_li
        .filter((m) => m.type === TYPE_SIZE && m.bytes > 0)
        .map((m) => m.bytes),
      min_rate = tp_li.length > 0 ? Math.min(...tp_li) : 0,
      max_rate = tp_li.length > 0 ? Math.max(...tp_li) : 0,
      min_lat = lat_li.length > 0 ? Math.min(...lat_li) : 0,
      max_lat = lat_li.length > 0 ? Math.max(...lat_li) : 0,
      min_bytes = size_li.length > 0 ? Math.min(...size_li) : 0,
      max_bytes = size_li.length > 0 ? Math.max(...size_li) : 0;

    let best_idx = -1,
      best_val = null;

    engine_li.forEach((eng, idx) => {
      const m = metricByKey(eng, key);
      if (!m || m.type === TYPE_NA) return;
      if (m.type === TYPE_TP) {
        if (best_val === null || m.rate > best_val) {
          best_val = m.rate;
          best_idx = idx;
        }
      } else if (m.type === TYPE_LAT) {
        if (best_val === null || m.duration_ms < best_val) {
          best_val = m.duration_ms;
          best_idx = idx;
        }
      } else if (m.type === TYPE_SIZE) {
        if (m.bytes > 0 && (best_val === null || m.bytes < best_val)) {
          best_val = m.bytes;
          best_idx = idx;
        }
      }
    });

    return {
      key,
      min_rate,
      max_rate,
      min_lat,
      max_lat,
      min_bytes,
      max_bytes,
      best_idx,
    };
  },
  loadBenchData = benchDataLoad,
  calcMultiplier = multiplierCalc;

export default benchDataLoad;
