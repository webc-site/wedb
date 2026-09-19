#!/usr/bin/env -S bun
import { mkdir, rm } from "node:fs/promises";
import { resolve } from "node:path";
import { parse as yamlParse } from "yaml";
import { benchDataLoad } from "./lib/data.js";
import { svgRender } from "./lib/renderSvg.js";
import { svgOptimize, svgUpload } from "./lib/upload.js";
import { mdRender } from "./lib/renderMd.js";

const JS_DIR = import.meta.dirname,
  BENCH_DIR = resolve(JS_DIR, ".."),
  ROOT_DIR = resolve(BENCH_DIR, ".."),
  I18N_DIR = resolve(JS_DIR, "i18n"),
  IMG_DIR = resolve(JS_DIR, "img"),
  README_DIR = resolve(ROOT_DIR, "readme"),
  LANG_LI = ["zh", "en"],
  ENCODER = new TextEncoder();

export const i18nLoad = async (lang = "zh") => {
    const yml_path = resolve(I18N_DIR, lang + ".yml"),
      content = await Bun.file(yml_path).text();
    return yamlParse(content);
  },
  svgJpgGen = async (svg_path, jpg_path) => {
    const cmd_li = [
      ["magick", "-density", "192", svg_path, "-quality", "92", "-background", "white", "-flatten", jpg_path],
      ["convert", "-density", "192", svg_path, "-quality", "92", "-background", "white", "-flatten", jpg_path],
    ];

    for (const cmd of cmd_li) {
      try {
        const proc = Bun.spawn(cmd);
        if ((await proc.exited) === 0) return true;
      } catch {}
    }

    try {
      const png_path = jpg_path.replace(/\.jpg$/, ".tmp.png"),
        rsvg = Bun.spawn(["rsvg-convert", "-f", "png", "-z", "2", "-b", "white", "-o", png_path, svg_path]);
      if ((await rsvg.exited) === 0) {
        const sips = Bun.spawn(["sips", "-s", "format", "jpeg", "-s", "formatOptions", "92", png_path, "--out", jpg_path]),
          res = (await sips.exited) === 0;
        await rm(png_path, { force: true });
        if (res) return true;
      }
    } catch {}

    return false;
  },
  svgToJpg = svgJpgGen,
  benchGen = async () => {
    console.log("1. 加载评测原始数据集...");
    const bench_data = await benchDataLoad();

    for (const lang of LANG_LI) {
      console.log("\n2. 正在生成 [" + lang + "] SVG 矢量柱状图与 Markdown...");
      const i18n = await i18nLoad(lang),
        raw_svg = svgRender(bench_data, i18n, lang),
        opt_svg = svgOptimize(raw_svg),
        raw_len = ENCODER.encode(raw_svg).length,
        opt_len = ENCODER.encode(opt_svg).length,
        saved_pct = (100 - (opt_len / raw_len) * 100).toFixed(1);

      console.log("  -> SVG 压缩优化: " + raw_len + " B -> " + opt_len + " B (体积缩减 " + saved_pct + "%)");

      const out_dir = resolve(IMG_DIR, lang),
        local_svg_path = resolve(out_dir, "bench.svg");
      await mkdir(out_dir, { recursive: true });
      await Bun.write(local_svg_path, opt_svg);
      console.log("  -> 本地 SVG 已保存: " + local_svg_path);

      const local_jpg_path = resolve(out_dir, "bench.jpg"),
        jpg_ok = await svgJpgGen(local_svg_path, local_jpg_path);
      if (jpg_ok) {
        console.log("  -> 本地 JPG 已保存: " + local_jpg_path);
      } else {
        console.warn("  -> 本地 JPG 生成跳过 (未检测到 magick / convert / rsvg-convert)");
      }

      let cdn_url = "";
      try {
        cdn_url = await svgUpload(opt_svg);
        if (cdn_url) {
          console.log("  -> CDN 上传成功 [" + lang + "]: " + cdn_url);
        }
      } catch (err) {
        console.warn("  -> CDN 上传跳过或失败: " + (err?.message ?? err));
      }

      if (!cdn_url) {
        cdn_url = "https://raw.githubusercontent.com/webc-site/wedb/main/bench/js/img/" + lang + "/bench.svg";
      }

      console.log("3. 正在渲染 readme/" + lang + "/bench.md...");
      const md_content = mdRender(bench_data, i18n, cdn_url),
        md_dir = resolve(README_DIR, lang),
        md_path = resolve(md_dir, "bench.md");
      await mkdir(md_dir, { recursive: true });
      await Bun.write(md_path, md_content);
      console.log("  -> 已更新文档: " + md_path);
    }

    console.log("\n全部 SVG 图表与多语言 Markdown 文档生成完毕！");
  };

if (import.meta.main) {
  await benchGen();
}

export default benchGen;
