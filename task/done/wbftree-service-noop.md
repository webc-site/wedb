目标：删除 wbftree 服务面的零消费虚设实现 noop，并登记对应 ignore。

问题
wedb/wbftree/src/service/ops.rs 的 BfTreeService::noop 返回字面量 0，全仓（生产、测试、bench）零消费者。
其文档注释自陈「空操作测量纯调用开销」，属 transpile SKILL 严禁的占位/虚设实现。

C# 对位
garnet/libs/native/bftree-garnet/BfTreeService.cs 的 Noop（约 :225-231），注释明写
"No-op P/Invoke for measuring pure FFI transition overhead"，唯一消费点是
garnet/benchmark/BDN.benchmark/BfTree/BfTreeOperations.cs 的 FFI_Noop 基准臂。
Rust 无 P/Invoke 跨语言边界，该基准面也未移植（js/check/ignore/benchmark.yml 已登记 FFI_Noop，
native.yml 已登记 bftree_noop），noop 属无对位价值的死面，径删不补 bench 臂。

改动
1. 删除 wedb/wbftree/src/service/ops.rs 中 noop 函数及其文档注释（约 :199-204），不留 cfg(test) 兜底。
2. 在 js/check/ignore/native.yml 的 BfTreeService.cs 登记块旁补登 Noop，理由对齐既有 FFI 口径。

收敛机制
FFI 开销测量面统一裁定为「无需实现」：胶水层 bftree_noop、包装层 Noop、基准臂 FFI_Noop 三处
ignore 同口径，一处机制零虚设代码。
