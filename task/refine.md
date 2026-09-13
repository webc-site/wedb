对照 ./garnet/ 清理优化 wedb 代码
不断开子代理审查代码，直到连续十次子代理返回代码完美实现了 c# (参考 .agents/skills/transpile/SKILL.md，我们对数据布局等有优化)
代码是 ai 对标 c#写的 rust，子代理要检查 ai 是否在实现上有偷工减料，偷懒
redis 指令、集群拓扑、日志、数据库、迁移恢复等等都要对标 C#
要避免多重实现，清理冗余代码、死代码，集成测试拆分到 tests 别放到 src
对照 ./garnet/ 清理测试，garnet 有的测试要在 wedb 用 rust 实现，没有的就删除
优化模块的暴露接口，低耦合，高内聚