# 任务需求清单

## 1. wedb_standalone 代码下沉与复用
wedb/wedb_standalone/src 代码继续下沉至公共底座（如 wnode/wconn/wbase 等），让 wedb 复用。
运用设计模式、统一流程、泛型抽象。
wedb 与 wedb_standalone 相互独立，严禁双向依赖。

## 2. wedb 核心打通与重复实现清理
wedb/wedb/src 与下沉模块全面打通。
彻底清理重复实现、重复定义，消除多套机制。

## 3. 文档注释与 Garnet 对标
运行 ./js/check.js，严格比对 ./garnet/ 源代码。
清理重复定义。
补齐缺失的 Garnet C# 文档注释（对照 .agents/skills/transpile/SKILL.md），确保 0 缺失、0 重复。

## 4. 消除多套策略与 AI 坏味道
对标 ./garnet/ 官方实现，清理重复逻辑。
避免多套策略共存。
坚决消除 AI 坏味道（如重复定义、同一逻辑多处分叉、缺乏封装复用）。

## 5. 集成测试规范与废话测试清理
集成测试一律拆分到 tests/ 目录，禁止在 src/ 内部编写端到端集成测试。
对照 ./garnet/ 测试套件，严格保留 C# 官方对应测试。
清理 AI 自动生成的无意义废话测试。

## 6. 严禁跨 crate 二次导出
严禁跨 crate 使用 pub use 转发外部类型。
各模块需要的依赖一律在各自 Cargo.toml 中显式添加导入。

## 7. 子代理闭环审查与迭代优化
开子代理持续对上述问题进行思考与审查。
若子代理有修改，则继续开子代理循环迭代。
以 clippy 0 警告、./test.sh 全量测试通过且子代理确认达到生产级别为最终目标。
