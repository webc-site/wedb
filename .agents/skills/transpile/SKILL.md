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

尽量 1:1 对标 c#的代码实现，不要实现自己的优化（如果有，也撤销，尽量完全对标 c#，避免出现错误），除了以下几点

- 前缀用 enum u8，而不是字符串，也别加冒号
- 并发字典、set 用 papaya + gxhash （在 embed/wbase/map.rs 中定义，用 map 或 set 特性启用）
- 锁用 parking_lot
- hash 一律用 gxhash

运行时用 compio （一个线程一个 cpu）
消息队列用 crossfire
lua 用 luau

只能使用 cargo add 添加依赖，禁改 Cargo.toml

让子代理开 worktree 到/tmp/fork/下面，优化，写完、测试之后合并到当前目录，清理 worktree。

运行 `./js/check.js` 可以看到缺失实现或者文档注释的 c# 函数

可以查看 `check/miss` 下面的文件，明确还缺少哪些函数和测试，并在 rust 相关的包中实现（或在 rust 相关的函数添加文档注释）

在 rust 函数文档注释中写清楚和 c# 的映射关系，格式是如下：

/// 在 garnet 中的相对路径:函数名

如果缺少相关的包，也可以用各个模块的 `./sh/new.sh` 创建新的 crate，合理规划模块，低耦合，高内聚

如某函数无需在 rust 中实现，在 `js/check/ignore/garnet下面相对路径.yml` 中配置，这样 `check.js` 忽略

让子代理每次都先对照 garnet c# 代码审查 rust 的代码架构、模块依赖，思考如何让其结构更加合理，可以拆分、修订，让其拓扑和 c#更加吻合

如果遇到主分支修改，请提交，然后合并（注意更新 worktree，避免落后）。

写完之后 ./clippy.sh 和 ./test.sh，确保没有警告

子代理开发，要效率最大化，分析拓扑，并发启动

开发与审查流水线重叠（一边审查上一层 crate，一边开发下一层 crate）

不断循环，开新子代理 code review ，运行 check.js，直到 check.js 没有缺失的输出，直到连续三次子代理认为完备完整的实现了 garnet 的代码，并且实现达到了生产级别

全程自主完成决策，禁止请求人工确认