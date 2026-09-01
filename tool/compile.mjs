#!/usr/bin/env -S bun
import { $ } from "zx";

$.verbose = 1;

/**
 * 交叉编译适用于 Linux x86_64 的静态无依赖 Release 二进制
 */
const releaseBinCompile = async () => {
  console.log("\n📦 [1/4] 开始跨平台交叉编译 (target: x86_64-unknown-linux-musl, release 模式)...");

  await $`cargo zigbuild --release --target x86_64-unknown-linux-musl --bin wedb_standalone --bin wedb_cluster`;

  const stand_bin = "/private/tmp/rust/x86_64-unknown-linux-musl/release/wedb_standalone",
    clust_bin = "/private/tmp/rust/x86_64-unknown-linux-musl/release/wedb_cluster";

  if (!(await Bun.file(stand_bin).exists()) || !(await Bun.file(clust_bin).exists())) {
    throw new Error("交叉编译产物未找到！请检查 target 输出目录。");
  }

  console.log("✅ 静态二进制构建完成:\n   - Standalone: " + stand_bin + "\n   - Cluster:    " + clust_bin);

  return [stand_bin, clust_bin];
};

export const binCompile = releaseBinCompile;
export default releaseBinCompile;
