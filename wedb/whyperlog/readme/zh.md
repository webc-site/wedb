# whyperlog : HyperLogLog 超对数草图

- [项目介绍](#项目介绍)
- [与 whlog 的关系](#与-whlog-的关系)
- [核心 API](#核心-api)
- [设计要点](#设计要点)
- [测试覆盖](#测试覆盖)

## 项目介绍

whyperlog 提供 Garnet 的 HyperLogLog 概率基数估计草图，是 PFADD / PFCOUNT / PFMERGE 命令族的底层结构，1:1 转写自 `libs/server/Resp/HyperLogLog/HyperLogLog.cs`。

草图操作调用方持有的字节载荷，含两种编码：

- 稠密：16 字节头（HYLL 魔数 + 编码 + 卡数缓存）+ 16384 个 6 位打包寄存器（共 12304 字节）
- 稀疏：同样 16 字节头 + 2 字节 RLE 长度 + RLE 操作码流（零段 `1xxx xxxx` / 非零 `0vvv vvvv`），超过 4KB 上限稠密化

基数估计采用 ["New cardinality estimation algorithms for HyperLogLog sketches"](https://arxiv.org/abs/1702.01284) 的新算法（寄存器直方图 + τ/σ 修正），元素哈希用 MurmurHash2x64A。

## 与 whlog 的关系

whyperlog 与 whlog 仅名字形似，两域正交、不合并：

- whyperlog = HyperLogLog 数据结构：定长布局的草图，作为对象载荷存储；无 IO、无地址空间、无并发。C# 对标：`HyperLogLog.cs`
- whlog = HybridLog 日志分配器：设备之上的 64 位逻辑地址空间，带页缓冲、刷盘与三区滑动。C# 对标：Tsavorite 的 `TsavoriteLog`

一个是 Redis 对象编码，一个是存储引擎基础设施。

## 核心 API

- `HyperLogLog`：new（pbit = 14）/ with_pbit、init / init_sparse / init_dense、update（原位或需申请新空间）、copy_update、update_grow / merge_grow / can_grow_in_place（增长规划）、count（卡数缓存 + 失效）、merge / try_merge、sparse_to_dense、sparse_to_sparse_copy / sparse_to_dense_copy / copy_update_merge、is_valid_hyll / is_valid_hll_length / is_valid_sparse_stream、compare_sparse_to_dense、dump_raw_bytes / dump_regs（DEBUG 导出，返回 String）
- `HllDtype`（Sparse / Dense）、`murmur_hash_2_x64_a`
- 布局常量：`SPARSE_SIZE_MAX_CAP`（4096）、`SPARSE_MEMORY_SECTOR_SIZE`（128）

## 设计要点

- Rust 以 `&mut [u8]` 切片替代 C# 裸指针就地改写；切片与存储页的等价性由调用方保证（草图本体不持存储）
- 6 位寄存器自 LSB 起跨字节打包；末寄存器越界一字节以 0 短路，替代 C# 的无害越界读
- 卡数缓存以 i64 存于头部（负数 = 已失效），任何寄存器变更即失效
- 稀疏零段拆分最多写 3 个操作码并后缀右移一字节，对齐 C# `UpdateSparseReg`
- DEBUG 导出方法（C# Console.WriteLine）改为返回 `String`

## 测试覆盖

tests 覆盖：布局常量与 C# 逐项对齐、稀疏/稠密初始化布局、PFADD 语义与重复插入无变更、计数误差区间（稀疏 500 / 稠密 1 万）、稀疏→稠密升级与寄存器级一致、零段首/中/尾边界拆分、reg_idx/clz 边界、稀疏合并与稀疏并入稠密、卡数缓存失效、增长规划含空源下溢防御、拷贝路径、MurmurHash 不变量、非法载荷检测、DEBUG 导出、自定义 pbit。
