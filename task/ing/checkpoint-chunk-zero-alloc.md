# 检查点数据块零多余分配优化方案

## 背景与问题核实
receive_checkpoint_handler.rs 中 FileDataSink::write_chunk 在处理 StoreHlog 段流时存在性能缺陷：
1. 先分配了 vec![0u8; padded_len] 堆内存。
2. 将入参 data 拷贝至临时 vec 中。
3. 调用 AlignedBuf::from_slice(&padded, sector)，触发第二次堆内存分配并进行第二次数据拷贝。
4. 临时 vec 随后被释放，且写入完成后 AlignedBuf 也随 drop 释放，未能复用底层 Device 的 BufferPool。

对照 C# Garnet 实现：
Garnet 在 FileDataSink.cs 中直接通过 bufferPool.Get((int)numBytesToWrite) 获取扇区对齐内存，通过 Buffer.MemoryCopy 将数据拷贝至对齐缓冲，下发设备写入后将缓冲 Return 归还入池。

## 细化改进方案
1. 消除临时 Vec 分配与二次拷贝：直接通过 device.pool().get(padded_len) 从设备关联的 BufferPool 租借对齐缓冲。
2. 边界保护与对齐检查：提前校验 start_address 满足扇区对齐；计算 padded_len 为 sector_size 向上取整。
3. 数据拷贝与尾部置零：获取对齐缓冲的可变切片，将 data 拷贝到前部，尾部未对齐部分执行 fill(0)。
4. 零堆分配生命周期：AlignedBuf 写入完成后 drop 时自动归还入 pool，在连续接收数据块场景下实现池化复用与零额外堆分配。
5. 负数偏移防御：对 start_address 严格校验，防止非法负值转换溢出。
6. 验证与审查：运行 cargo check 确保编译通过，按 rust_review 规范完成代码审查与优化。
