# HybridLog 与 ReadCache 环形页分配状态机去重

来源：next/zcode.db.md 问题 7

## 问题

wkv/src/read_cache/append.rs 与 whlog/src/hlog/append.rs 存在大量同构代码：
页号与偏移计算、容量剩余检测、换页互斥、tail_address CAS 循环、Padding 填充等逻辑重复。

## 涉及路径

- wedb/wkv/src/read_cache/append.rs
- wedb/whlog/src/hlog/append.rs
- wedb/whlog/src/buffer.rs

## 解决建议

1. 将环形缓冲区的无锁 CAS 空间分配与换页逻辑抽象下沉至通用的页分配器内核。
2. ReadCache 与 HybridLog 共同复用该内核，消除重复代码。
