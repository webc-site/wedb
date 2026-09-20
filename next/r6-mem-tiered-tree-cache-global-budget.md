# 升阶树页缓存无全局总闸与 MEMORY USAGE 升阶臂口径失实

来源：next/zcode-r6-mem.md 问题 2 与问题 3。

## 问题

问题 2：每棵活跃升阶树开树即整块预留 16MiB 页环。
wbftree/src/types.rs 的 TreeTuning 默认 cache_size 16MiB（约 69/86/95 行），
经 wbftree/src/manager/lifecycle.rs 的 cb_size_byte（约 78-80）配入引擎；
bf-tree 0.5.6 的 CircularBuffer::new 一次性分配整容量。
RangeIndexManager 的 live_indexes（约 186）只是注册表，无树数或总缓存字节上限，
聚合常驻等于树数乘 16MiB；计数型小条目集合约有 5 到 8 倍的数据放大。
wkv/src/range_index/promote.rs 的 promote_collection_to_bftree（约 71）
在灌入快照前整块建 scratch 环，并发升阶无全局并发上限，峰值无闸。

C# 侧同为 16MiB 调参（RespServerSessionRangeIndex.cs 约 44-50 的 Defaults），
但树只在 RI.CREATE 显式创建、数量由用户可控，C# 无自动升阶面，
所以 C# 不需要这个总闸；rust 引入自动升阶后必须补上，否则是新增的内存放大面。

问题 3：wnode/src/resp/basic_commands/mod.rs 的 network_memory_usage
升阶键 Meta 臂（约 627-631）注释称「树页冷热换入换出，非常驻，不误报」，
与问题 2 的事实相反（页环在树存活期整块常驻），
单键回 16MiB 对 Meta 元记录 32B 是约 50 万倍的低报口径，现值标「下限」名不符实。

## 方案

1. 建立全局树缓存总预算：RangeIndexManager 增加以字节计的常驻页缓存账，
   升阶建树前预留、树清退时归还。超限时拒绝升阶并让键维持信封态
   （信封态是正确语义，只是性能形态不同，与 C# 无升阶基线一致）。
   预算额来源于配置，不硬编码。
2. 并发升阶加全局信号量上限，约束 scratch 环同时分配的峰值。
3. 修正 network_memory_usage 升阶臂注释的错误依据，
   改为按每树 cache_size 与活跃态披露的真实口径；
   若本轮实现 wkv 侧树驻留估算 API 则一并并项，
   否则明确登记为「升阶键返回信封估算 + 已知低报」的声明差异。

## 验收

1. cargo check -p wbftree -p wkv -p wnode --tests 零 error 零 warning。
2. 测试：并发升阶超过总预算时，超出的键保持信封态且命令语义正常；
   树清退后预算归还、可继续升阶。
