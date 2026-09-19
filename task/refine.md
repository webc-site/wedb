
思考如何更好地把 ./garnet 改写为 rust

运行./js/check.js，对照上面的需求文档，该忽略的忽略，该补全 rust 代码对应 c#路径的文档注释的，补充文档注释，该实现的实现

让子代理阅读

./.agents/skills/transpile/SKILL.md
./task/refine.md

从下面的角度，并发开子代理审查 rust 代码（告知子代理:只审查，不修改，不运行 test.sh 和 clippy.sh）

1. 审查网络协议、共识、同步、迁移实现， 中你觉得需要补全、优化、修复、优化、拆分、去重、清理的点，到 next/agy.net.md
2. 对照./garnet 的支持的数据类型，redis 命令，redis 命令支持的参数，ttl 的设计，code review，rust 还有哪些遗漏、缺失，到 next/agy.data.md
3. 对照./garnet 的底层引擎，aof，bftree，存储，等等底层设计，思考还有哪些需要拆分，优化，清理，去重，合并，有哪些冗余，需要整理的代码（避免 ai 生成的重复设计），到 next/agy.db.md
4. 对照./garnet，审查数据链条，是否有没用到需要清理的死代码，是否有多套重复的机制（ai 东一榔头西一棒的坏文档，没有一处定义，没有代码复用），是否有需要整合的常量，工具函数，需要拆分复用的模块，模块依赖、拓扑设计是否正确、优雅、高效？把修改意见输出到 next/agy.design.md
5. 对照 ./.agents/skills/transpile/SKILL.md，审查我们相对于 c#的自定义优化，是否上下游打通，实现是否正确，高效，优雅。把修改意见输出到 next/agy.my.md

审查代码后，梳理待办到 next/ 下面的 md，要格式简洁(不用加粗、表格、间隔线），写清具体问题，rust 文件和函数、 对应的 c# 文件和函数(用相对路径）