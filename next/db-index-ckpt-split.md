优先级：中
来源：next/agy.db.md 条 13 立项（原档引证 struct PageAlignedBatch 已不存在，按当下
符号修正为 AlignedBatch / BatchWriter / BatchReader）。取证基线：主仓 dev 当下代码。

问题
wcpr index_ckpt.rs 747 行集中：64 字节头编解码、对齐批缓冲分配与 DirectIO 批量写盘、
截断恢复读取三套机制，读写两侧状态机与对齐分配细节同文件耦合。

取证
- wedb/wcpr/src/index_ckpt.rs:174 struct AlignedBatch([u8; BATCH_BYTES])（裸对齐批
  缓冲）、:279 struct BatchWriter<'a, F>（批量写出状态机）、:398 struct BatchReader<'a>
  （恢复读取）、:504 pub async fn write_index_checkpoint、:520 _inner（批量写主体）、
  :599 pub async fn read_index_checkpoint_truncated；文件共 747 行。
- C# 对标：garnet/libs/storage/Tsavorite/cs/src/core/Index/Recovery/IndexCheckpoint.cs
  （原档写 Checkpointing/IndexCheckpoint.cs 路径有漂移，实际在 Recovery/ 目录；
  C# 头持久化 / 桶批量写出 / 恢复读取分段清晰）与同目录 Checkpoint.cs。

修法建议
拆 index_ckpt/header.rs（64B 头编解码）、index_ckpt/write.rs（AlignedBatch + BatchWriter
+ write_index_checkpoint）、index_ckpt/read.rs（BatchReader + 恢复入口），
index_ckpt/mod.rs 统一导出；对齐批缓冲与写出状态机解耦后各自内聚。纯搬运，
DirectIO 对齐与截断语义零改动。
