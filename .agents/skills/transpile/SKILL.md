---
name: transpile
description: garnet 转写 rust
---

./garnet c# 代码转写为 rust

采用微模块结构，

hash 一律用 gxhash

技术选型如下
消息队列用 crossfire
lua 用 cargo add luau


- 只能使用 cargo add 添加依赖，严禁私自修改 Cargo.toml。
- ./clippy.sh 和 模块 ./test.sh，确保没有警告、从未
- 并发效率最大化，分析拓扑，并发子代理；开发与审查流水线重叠（一边审查上一层 crate，一边开发下一层 crate）。