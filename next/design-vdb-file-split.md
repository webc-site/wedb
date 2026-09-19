优先级：中
来源：next/agy.design.md 条 20 与 next/muse.design.md 条 18 的 vdb.rs 部分。
取证基线：主仓 dev 当下代码，行号为当下实测。

问题
wkv vdb.rs 1495 行单文件混杂三域：并发路由表、DbMeta 记录编解码、后台 GC 队列与安全纪元。
三域各自内聚清晰但互不隶属，并 file 堆叠导致路由、持久化格式、回收三方改动同文件冲突。

取证
- wedb/wkv/src/vdb.rs 全 1495 行，三域地标（实测）：
  路由表：:37 pub struct DbRoutingTable（papaya + ArcSwap 的 logic_db → virtual_db_id 映射，
  :41-:115 impl）
  DbMeta 编解码：:118 pub enum DbMetaRecord、:161 DbMetaKeyBuf、:180 DbMetaValueBuf、
  :193-:445 impl（key/value 编码与 decode）
  GC 与纪元：:447 pub struct GcDeadEntry、:460 TenantRouting、:494 GcDeadLog（:510-:611
  含 pop_reclaimable 水位判定）、:613 VirtualDbManager 及其后全部（至 :1495）
- C# 对标：无直接对位（虚拟库映射与偏序 GC 是 transpile SKILL.md:36 自定义设计）；
  组织参考 garnet/libs/server/Databases/（IDatabaseManager 等按职责分文件）与
  Tsavorite core 的 GC/Checkpoint 分文件先例；键值编解码对标 Tsavorite 独立 serializer 文件形态

修法建议
拆为 vdb/ 目录模块：routing.rs（DbRoutingTable）、meta.rs（DbMetaRecord 三结构与编解码）、
gc.rs（GcDeadEntry/GcDeadLog/水位判定）、manager.rs（TenantRouting/VirtualDbManager 装配）；
mod.rs 留声明与 pub use 重导出，对外路径不变。纯搬运禁夹带语义改动；
自定义设计无 C# 锚点的部分保留现有文档注释原样搬位。
