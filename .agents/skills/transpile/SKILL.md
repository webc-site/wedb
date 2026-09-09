---
name: transpile
description: garnet 转写 rust
---

把 ./garnet 的 c# 代码转写为 rust

采用微模块结构，目录结构如下：

./embed 嵌入式数据库部分
./node  单机版服务器
./cluster 集群版服务器

技术选型参考 ./.agents/skills/rust_review/SKILL.md

运行时用 compio （一个线程一个 cpu）
hash 一律用 gxhash
消息队列用 crossfire
lua 用 luau
并发字典、set 用 papaya + gxhash （在 embed/wbase/map.rs 中定义，map 或者 set 特性启用）

只能使用 cargo add 添加依赖，严禁私自修改 Cargo.toml。
- ./clippy.sh 和 模块 ./test.sh，确保没有警告、从未
- 并发效率最大化，分析拓扑，并发子代理；开发与审查流水线重叠（一边审查上一层 crate，一边开发下一层 crate）。