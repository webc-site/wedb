#!/usr/bin/env -S bun
import { connect } from "node:net";

const respEncode = (arg_li) => {
  let res = "*" + arg_li.length + "\r\n";
  for (const arg of arg_li) {
    const s = typeof arg === "string" ? arg : String(arg),
      len = Buffer.byteLength(s);
    res += "$" + len + "\r\n" + s + "\r\n";
  }
  return Buffer.from(res);
};

const respReaderNew = () => {
  let buf = Buffer.alloc(0);

  const append = (chunk) => {
    buf = Buffer.concat([buf, chunk]);
  };

  const popResponse = () => {
    if (buf.length === 0) return null;
    const type = buf[0],
      crlf = buf.indexOf(Buffer.from("\r\n"));
    if (crlf === -1) return null;

    if (type === 43 || type === 45 || type === 58) { // +, -, :
      const val = buf.subarray(1, crlf).toString();
      buf = buf.subarray(crlf + 2);
      return val;
    }

    if (type === 36) { // $
      const len_str = buf.subarray(1, crlf).toString(),
        len = parseInt(len_str, 10);
      if (len === -1) {
        buf = buf.subarray(crlf + 2);
        return Symbol.for("nil");
      }
      const data_end = crlf + 2 + len;
      if (buf.length < data_end + 2) return null;
      const val = buf.subarray(crlf + 2, data_end);
      buf = buf.subarray(data_end + 2);
      return val;
    }

    if (type === 42) { // *
      const count_str = buf.subarray(1, crlf).toString(),
        count = parseInt(count_str, 10);
      if (count === -1) {
        buf = buf.subarray(crlf + 2);
        return Symbol.for("nil");
      }
      if (count <= 0) {
        buf = buf.subarray(crlf + 2);
        return [];
      }
      buf = buf.subarray(crlf + 2);
      const item_li = [];
      for (let i = 0; i < count; ++i) {
        const item = popResponse();
        if (item === null && buf.length === 0) return null;
        item_li.push(item);
      }
      return item_li;
    }

    buf = buf.subarray(crlf + 2);
    return true;
  };

  return { append, popResponse };
};

/**
 * 运行单项基准测试（采用流控流水线 Window-based Pipelining，防丢包与超时熔断）
 */
const benchSingleRun = async (
  host,
  port,
  name,
  group,
  cmd_gen,
  total_reqs = 5000,
  concurrency = 16,
  pipeline_depth = 16,
  timeout_ms = 15000
) => {
  process.stdout.write("  ⚡ 测试 [" + group.padEnd(10) + "] " + name.padEnd(20) + " ... ");
  const start_time = performance.now(),
    latency_us_li = [],
    socket_li = [],
    client_promise_li = [],
    target_per_client = Math.ceil(total_reqs / concurrency);
  let total_completed = 0;

  const clientRun = (client_id) => {
    return new Promise((resolve, reject) => {
      let client_completed = 0,
        in_flight = 0,
        sent_count = 0,
        batch_seq = 0;
      const send_batch_time_map = new Map();

      const socket = connect({ port, host, timeout: 5000 }, () => {
        socket_li.push(socket);
        const reader = respReaderNew();

        socket.on("data", (chunk) => {
          reader.append(chunk);

          while (reader.popResponse() !== null) {
            ++client_completed;
            ++total_completed;
            --in_flight;

            sendNextWindow();

            if (client_completed >= target_per_client) {
              socket.destroy();
              resolve();
              return;
            }
          }
        });

        socket.on("error", (err) => {
          socket.destroy();
          reject(err);
        });

        socket.on("timeout", () => {
          socket.destroy();
          reject(new Error("Socket timeout"));
        });

        socket.on("close", () => {
          if (client_completed < target_per_client) {
            reject(new Error("Connection closed before completion (" + client_completed + "/" + target_per_client + ")"));
          }
        });

        sendNextWindow();
      });

      const sendNextWindow = () => {
        while (in_flight < pipeline_depth && sent_count < target_per_client) {
          const current_batch_size = Math.min(pipeline_depth - in_flight, target_per_client - sent_count);
          if (current_batch_size <= 0) break;

          const buf_li = [],
            current_seq = ++batch_seq,
            t0 = performance.now();
          send_batch_time_map.set(current_seq, t0);

          for (let p = 0; p < current_batch_size; ++p) {
            const req_idx = client_id * target_per_client + sent_count;
            ++sent_count;
            ++in_flight;
            buf_li.push(respEncode(cmd_gen(req_idx)));
          }

          socket.write(Buffer.concat(buf_li), () => {
            const elapsed_batch = (performance.now() - t0) * 1000;
            latency_us_li.push(elapsed_batch / current_batch_size);
          });
        }
      };
    });
  };

  for (let c = 0; c < concurrency; ++c) {
    client_promise_li.push(clientRun(c));
  }

  const timeout_promise = new Promise((_, reject) => {
    const timer = setTimeout(() => {
      socket_li.forEach((s) => {
        try { s.destroy(); } catch {}
      });
      reject(new Error("Benchmark timeout after " + timeout_ms + "ms"));
    }, timeout_ms);
    timer.unref?.();
  });

  try {
    await Promise.race([Promise.all(client_promise_li), timeout_promise]);
  } catch (err) {
    socket_li.forEach((s) => {
      try { s.destroy(); } catch {}
    });
    if (total_completed === 0) {
      throw err;
    }
  }

  const total_duration_sec = (performance.now() - start_time) / 1000,
    qps = Math.round(total_completed / total_duration_sec);

  latency_us_li.sort((a, b) => a - b);
  const avg = latency_us_li.length > 0 ? Math.round(latency_us_li.reduce((a, b) => a + b, 0) / latency_us_li.length) : 0,
    p50 = Math.round(latency_us_li[Math.floor(latency_us_li.length * 0.5)] ?? 0),
    p95 = Math.round(latency_us_li[Math.floor(latency_us_li.length * 0.95)] ?? 0),
    p99 = Math.round(latency_us_li[Math.floor(latency_us_li.length * 0.99)] ?? 0);

  console.log("✅ QPS: " + qps.toLocaleString().padStart(8) + " ops/s | P50: " + p50 + " µs | P99: " + p99 + " µs");

  return {
    group,
    name,
    qps,
    avg,
    p50,
    p95,
    p99,
    requests: total_completed,
  };
};

export const respEncodeFunc = respEncode,
  respReaderNewFunc = respReaderNew,
  benchSingleRunFunc = benchSingleRun,
  encodeResp = respEncode,
  RespReader = respReaderNew,
  runSingleBenchmark = benchSingleRun;

export default benchSingleRun;
