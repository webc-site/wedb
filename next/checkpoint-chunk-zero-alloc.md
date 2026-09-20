任务名称: checkpoint-chunk-zero-alloc

问题描述
wedb/wedb/src/server/replication/receive_checkpoint_handler.rs:write_chunk 接收检查点数据块时，先分配 vec![0u8; padded_len]，再调 AlignedBuf::from_slice 发生二次对齐分配与内存拷贝。
需消除二次堆分配与冗余拷贝，改用就地对齐内存写入。

实现规划
1. 使用 AlignedBuf 单次分配对齐缓冲区，直接接收网络载荷。
2. 消除中间临时 Vec 分配。
3. 运行 cargo check 确保编译通过。
4. 审查优化代码。
