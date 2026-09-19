快照段流 trait 返回 Vec 强制逐块 to_vec 与零初始化新分配，改回池化缓冲借用直发

来源：next/glm.design.md 第 6 轮条 3。取证基线：主仓绝对根 /Users/z/git/db/wedb，分支 dev，行号
按当下代码重取。

结论
主端快照下发的段流抽象把「一块字节」定成拥有权形态 Result<Vec<u8>, String>，两个实现各自为此
付出一次逐块堆开销，而消费面只需要一个借用；C# 对位整链是池化缓冲借切片直发、块尾归还，零逐块
分配。判定成立且待做。

现状
- trait 签名 /Users/z/git/db/wedb/wedb/wedb/src/server/replication/snapshot_transmission.rs:279
  `fn read_next_chunk(&mut self, max_len: usize) -> impl Future<Output = Result<Vec<u8>, String>>`。
- HLOG 段源实现同文件 :304-312：先经 device.read_range 拿到池化的 AlignedBuf（
  /Users/z/git/db/wedb/wedb/wdev/src/device.rs:181 返回 AlignedBuf，:194-230 的实现里缓冲由
  BufferPool 按策略取出并 set_len），紧接着 `buf[..].to_vec()`，把刚省下的池化收益整块拷回堆
  ——每块一次 alloc 加一次 copy。块大小 SNAPSHOT_CHUNK_SIZE = 1 << 17 即 128KB（同文件 :58，
  注释自认对标 C# FileDataSource.cs 的 DefaultBatchSize）。
- 检查点文件段源实现同文件 :360-366：`read_exact_at(vec![0u8; want], offset)`，每块新分配并
  零初始化，而读侧整体覆写这些字节，memset 纯浪费。
- 消费面同文件 :201-202（send_file_chunks 循环体）仅以 `&chunk` 借用传给 send_snapshot_data 后
  即丢弃，无所有权需求：Vec 形态纯为 trait 签名所迫。

C# 参考
- /Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/
  FileTransmitSource.cs:32 TransmitAsync：:38 逐块 ReadNextChunkAsync 取 SectorAlignedMemory，
  :45 `result.Buffer.GetSlice(result.BytesRead)` 借用切片直发，:53 finally
  `result.Buffer.Return()` 归还池。
- /Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/PrimaryOps/DiskbasedReplication/
  FileDataSource.cs:108 ReadIntoAsync：:126 `bufferPool.Get(...)` 池取缓冲；且 hlog 段与检查点
  文件段两类源共用同一泛化于 IDevice 的 FileDataSource，rust 侧却拆成两个 Source 实现且都不做
  池化直发。

修法
1. trait 返回类型改 AlignedBuf（/Users/z/git/db/wedb/wedb/wbase/src/pool/aligned_buf.rs:21 定义，
   :415/:446 已实现 compio 的 IoBuf / IoBufMut，:300-301 Deref 为 `[u8]`），发送侧按
   `&buf[..n]` 借用喂 send_snapshot_data，块循环尾 drop 归还，对齐 C# GetSlice + Return 闭环。
2. HLOG 段源删 :311 的 to_vec，直接返回设备层给的 AlignedBuf。
3. 检查点文件源改从同一 BufferPool 取 AlignedBuf 喂 read_exact_at（compio 以 IoBufMut 承接，
   免 vec![0u8; want] 的零初始化），池句柄沿 SnapshotTransmitSources 装配面下发（该结构体
   同文件 :61-66 已持有主端引擎设备，设备侧 `pool()` 即缓冲池出口）。
4. 两块源共用同一池化读源后，若段流形态仍分叉，按 C# 「一类源 + IDevice 泛化」口径合并实现，
   避免留下两个 Source 各写一遍块循环。

优先级
功能缺口偏性能（SKILL「读路径借用零拷贝、消除点查 Vec<u8> 堆内存分配」准则的直读下发面未达
标；单帧 128KB 逐块 alloc + copy 在主端持续下发期放大）。

边界
task/ing/resp-null-protocol-single-source.md 与各帧拼装单点面（本单不动帧编码，只换载荷缓冲形
态）；快照接收侧落盘面与 AOF 装配面另见台账在册件 aof- 与 diskless-full-sync-flush-all.md。
