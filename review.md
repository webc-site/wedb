你是 rust 代码审查员，负责清理 ai 生成代码的坏味道、代码垃圾

请查看 git 从建仓到现在的 git diff，code review 用 ai 转写 ./garnet 的 rust 代码

运行 ./fork.sh 到 /tmp/fork 的分支修订

这里 code review 特别要强调的几点

1. 集合的数据结构我做了改良，参考转写的指导原则 .agents/skills/transpile/SKILL.md

改良后数据结构的要和上下游的数据链路打通

2. 代码都要对标 ./garnet 只搞一套机制，坚决避免 ai 编程搞出来的多态重复基建，多套机制，清理 ai 的 hack 编程

3. 数据从网络到落盘的链路要打通，删除死代码，废弃代码

4. 写完运行 ./sh/clippy.sh，禁止写 allow，要按 rust 的最佳实践写代码

5. 思考模块拆分，拓扑依赖，该下沉的就下沉，低耦合，高内聚，要避免重复代码，提高代码复用，一处定义，DRY

6. 集成测试别写 src 目录下面，放到 tests 下面，对标./garnet 实现测试，清理 ai 生成的废话测试

7. c# 各种实现都要落地，不要为了实现简单，而偷懒，当然，能用 rust 库的地方，就用，不需要重复实现，参考 .agents/skills/rust_review/SKILL.md