优先级：中（拓扑对标：C# 三个独立件在 rust 挤成单文件三态机）
来源：next/agy.db.md 条 7。核销 2026-09-19，取证基线 = 主仓 dev 当下 HEAD。

结论一句话
wbftree/src/chunk.rs 把序列化的写出、读入、跨节点迁移流读取三套互不依赖的状态机塞进一个 673
行文件，而 C# 侧是三个独立文件各承载一类；按 C# 文件名一一对位拆开即可，纯搬移零语义改动。

现状（主仓 HEAD 实测）
1. 三型同档：wbftree/src/chunk.rs:87 pub struct RangeIndexChunkedSerializer（impl :99 起，
   体到 :274）、:275 pub struct RangeIndexChunkedDeserializer（impl :288 起，Drop :561）、
   :570 pub struct RangeIndexMigrationReader<R: Read>（impl :578 起，Drop :669）。文件总 673 行。
2. 注册点：wbftree/src/lib.rs:58 mod chunk;、:65 pub use chunk::{…} 统一对外导出。
3. 该 crate 其余部分已是分域粒度（wbftree/src/{chunk,error,lib,stub,types}.rs +
   manager/ 目录模块 + service/ 目录模块），chunk.rs 是唯一的多态机混聚件。

C# 参考
1. libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs
2. libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs
3. libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs
三者均为独立类型文件，无继承、无共享内部状态（迁移读取器组合使用反序列化器，是消费关系）。

修法
1. 目录化：wbftree/src/chunk.rs → wbftree/src/chunk/{mod.rs, serializer.rs, deserializer.rs,
   migration_reader.rs}，mod.rs 只做子模块声明与对外 re-export，保持 lib.rs 现有
   pub use chunk::{…} 成员集合逐字不变。
2. 搬移前先核三型是否共用文件内私有常量或辅助 fn（分块上限、magic、变长整数编码 helper）：
   若有，按归属下沉到实际使用它的那个子模块，或上提到 chunk/mod.rs 作 pub(crate) 单点，
   禁复制成两份（本票的立论就是「一处定义」）。
3. 每文件的模块头文档注释随迁，保持 /// 在 garnet 中的相对路径:函数名 锚点原样
   （check.js 靠 File.cs:Fn 注释登记映射，锚点不得改口径也不得因拆分而丢挂）。
4. 不借机改写任何逻辑：本票 diff 应只呈现移动与 use 调整。

边界
与 next/tiered-collection-ops-file-split.md（wcol/wbftree 集合操作面拆文件）不同域：那条管
service/ 侧的集合命令操作，本票只管三个分块/迁移状态机件。

验收判据
1. wbftree/src/chunk/ 下三文件各自承载一个 pub 类型，符号锚点为
   RangeIndexChunkedSerializer::、RangeIndexChunkedDeserializer::、RangeIndexMigrationReader::，
   三者定义处各一处（grep 类型定义计数 = 1/1/1）。
2. wbftree/src/lib.rs 的 pub use chunk::{…} 成员集合与拆前逐字相同，wkv/wnode 消费方零改动
   （grep wkv/src wnode/src 内 use wbftree::…Chunked… 的行不变）。
3. 单文件行数 ≤300；无新增 pub(crate) 泄漏（除 mod.rs 汇聚点必要项）。
4. cargo check 通过（禁在共享 target 跑 test.sh / clippy.sh，由主代理合并后统一跑）。
