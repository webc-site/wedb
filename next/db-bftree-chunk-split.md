优先级：中
来源：next/agy.db.md 条 7 立项；next/muse.db.md 条 10 同面增量（manager/service 边界
纪律）并入本票。取证基线：主仓 dev 当下代码。

问题
wbftree chunk.rs 673 行混三个独立序列化状态机：分块序列化器、反序列化器、跨节点
迁移流读取器。C# 侧是三个独立文件，rust 合聚单文件与 C# 拓扑不吻合。

取证
- wedb/wbftree/src/chunk.rs:87 pub struct RangeIndexChunkedSerializer、:275
  RangeIndexChunkedDeserializer（含 :561 Drop）、:570 RangeIndexMigrationReader<R: Read>。
- C# 对标（三文件天然分界）：
  garnet/libs/server/Resp/RangeIndex/RangeIndexChunkedSerializer.cs、
  garnet/libs/server/Resp/RangeIndex/RangeIndexChunkedDeserializer.cs、
  garnet/libs/server/Resp/RangeIndex/RangeIndexMigrationReader.cs。
- muse.db 条 10 增量（muse 判「不拆不合」与本票冲突，采本票方向，C# 三文件是硬依据）：
  其可保留增量为边界纪律——wbftree/src/service/mod.rs:41 fs::File::open、
  service/snapshot.rs:54 create_dir_all、:122/:140 测试期 remove_dir_all，service 侧
  文件 IO 应对标 C# native service 形态收敛到 chunk 与 manager 落笔。

修法建议
拆 chunk/serializer.rs、chunk/deserializer.rs、chunk/migration_reader.rs 三文件，
chunk/mod.rs 统一导出，结构体与行为零改动（1:1 对标 C# 文件拓扑）；service 侧现存
fs 触点在本票落地时一并评估移入 chunk/manager（snapshot.rs 若为树快照落盘应归
manager 域），移动后 service 不再 use std::fs。
