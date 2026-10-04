import { defineConfig } from "vite";
import { svelte } from "@sveltejs/vite-plugin-svelte";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";

// 站点数据不进主仓：CI 把 gh-pages 上的历史取回 website/data/history.json，
// 构建前由这里生成 src/lib/benchData.js。空历史也要能出站（首次发布、本地 dev）。
const benchDataPlugin = () => ({
  name: "wedb-bench-data",
  config() {
    const root = import.meta.dirname;
    const history_path = join(root, "data", "history.json");
    const empty = { schema: 1, generated_at_unix: 0, reports: [] };
    let history = empty;
    if (existsSync(history_path)) {
      try {
        const parsed = JSON.parse(readFileSync(history_path, "utf8"));
        if (Array.isArray(parsed?.reports)) history = parsed;
      } catch (error) {
        console.warn(`data/history.json 解析失败，按空历史出站：${error.message}`);
      }
    }
    const module =
      "// 由 vite 配置里的 benchDataPlugin 从 data/history.json 生成，请勿手动编辑\n" +
      `export const HISTORY = ${JSON.stringify(history)};\n`;
    writeFileSync(join(root, "src", "lib", "benchData.js"), module);
    // 原始机读报表也随站点发布（走 public/ 拷贝，避免 dist 被 emptyOutDir 清掉）：
    // 下一轮 CI 从 gh-pages 的 data/history.json 取回它续历史
    mkdirSync(join(root, "public", "data"), { recursive: true });
    writeFileSync(join(root, "public", "data", "history.json"), JSON.stringify(history));
  },
});

export default defineConfig({
  base: "./",
  plugins: [benchDataPlugin(), svelte()],
  build: {
    outDir: "dist",
    emptyOutDir: true,
    target: "esnext",
    chunkSizeWarningLimit: 1500,
  },
});
