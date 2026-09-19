优先级：低
来源：next/agy.db.md 条 5 与 next/muse.db.md 条 19 两轮同题合并。取证基线：主仓 dev 当下代码。

问题
wkv session/raw 写路径过碎：append.rs 全文件 40 行、rmw.rs 105 行、modify.rs 135 行，
多为单函数文件；跨文件跳转成本高于内容本身的维护成本。

取证
- wedb/wkv/src/session/raw/write/append.rs（40 行）、
  wedb/wkv/src/session/raw/write/rmw.rs（105 行）、
  wedb/wkv/src/session/raw/modify.rs（135 行）、
  wedb/wkv/src/session/raw/write/copy_to_tail.rs（157 行）、
  wedb/wkv/src/session/raw/write/mod.rs（291 行）、write/inplace.rs（589 行）。
- C# 对标：garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalUpsert.cs、
  InternalRMW.cs、BlockAllocate.cs——C# 侧 upsert/rmw/追加同在 Tsavorite 实现目录平铺，
  无「单函数一文件」形态；组织粒度按机制不按函数。

修法建议
append、rmw、copy_to_tail 并入 write/mod.rs，modify 并入 raw/mod.rs 或 write/mod.rs
（modify 是 rmw 的同族写变体），read.rs 与 batch.rs 保持独立；并入后 pub use 路径
保持不变（外部引用零改动）。纯搬运。与 next/db-raw-read-variant-collapse.md 同目录
认领时协调布局，避免两次搬同一文件。
