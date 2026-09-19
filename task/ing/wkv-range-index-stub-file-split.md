优先级：中低（大文件多职责：升阶/存根/自愈/排空混聚 886 行）
来源：next/agy.db.md 条 6。核销 2026-09-19，取证基线 = 主仓 dev 当下 HEAD。

结论一句话
wkv/src/range_index/stub.rs 单文件以同一个 impl StoreSession<D> 承载存根读写、集合升阶、
删空排空注销、元数据自愈、树锁守卫五域 886 行；同目录已是分域模块组，按域拆即可，
其中树锁两函数已有在途票，须避让。

现状（主仓 HEAD 实测）
1. 单 impl 块混五域：wkv/src/range_index/stub.rs:32 impl<D: Device> StoreSession<D> 起，
   存根落盘 :33 save_bftree_meta_stub、删空排空注销 :67 handle_bftree_drain_and_delete、
   装载 :92 load_collection_stub 与 :238 load_range_index_stub、升阶 :126
   promote_collection_to_bftree 与 :423 promote_range_index_to_tail、
   树锁 :284 acquire_tree_read 与 :346 acquire_tree_write、自愈 :398 refresh_tiered_meta。
   文件总 886 行，内联测试块起 :741。
2. 同目录已是分域形态：wkv/src/range_index/{mod.rs, ops.rs, migration.rs} + 本 stub.rs，
   即拆分方向与该 crate 既有 grain 一致。

C# 参考
1. 存根类型本体：libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:47 internal struct
   RangeIndexStub（C# 用分部类文件分域：同族 RangeIndexManager.Locking.cs 持锁面、
   .Migration.cs 持迁移、.Replication.cs 持复制）。
2. 升阶与刷盘触发：libs/server/Storage/Functions/GarnetRecordTriggers.cs（PostCopyToTail 等
   记录钩子侧）；删空与判死同文件 IsDeleted。
3. 票内 cite 的 libs/server/Resp/RangeIndex/RangeIndexManager.cs:SnapshotTreeForFlush（:681）
   属 wbftree 侧刷盘件（wkv 消费面在 wkv/src/store/flush.rs:84），不是本文件的对位锚点，
   搬移时勿把该锚点错挂到 wkv 存根件上。

修法
1. 按域拆为 wkv/src/range_index/ 下四件：stub.rs 只留存根本体与装载（:33 save_bftree_meta_stub、
   :92 load_collection_stub、:238 load_range_index_stub）、promote.rs（:126、:423 两升阶臂）、
   drain.rs（:67 删空排空注销）、heal.rs（:398 元数据自愈）。全部保持 impl<D: Device>
   StoreSession<D> 分部实现（本仓先例：wkv/src/store/{gc,reclaim,flush,keyspace}.rs 对同一
   WedbStore 分域 impl），mod.rs 增补子模块声明。
2. 树锁域避让：:284 acquire_tree_read / :346 acquire_tree_write 的收口已由
   task/ing/range-index-locks-acquire-api.md（并发在途）承接，本票不动其实现与归属；
   若那票先把二者搬去独立 guard 件，本票的拆分基线以搬后位置为准（落地前先 ls 该目录重取位置）。
3. 内联测试处置：stub.rs:741 起的 cfg(test) 属全库惯例（201 个 src 文件带内联单测），
   spec 只要求集成测试进 tests/；仅当用例需以外部消费者形态构造整个 store 时，
   才迁 wkv/tests/store/range_index_stub.rs（该目录已有 batch_prefix.rs 等先例），否则随代码留内联。
4. 禁借拆分之机改升阶阈值/迟滞逻辑（doc/zh/collection.md 的双门限死区口径不动）。

验收判据
1. wkv/src/range_index/stub.rs 行数 ≤300，且四域符号各自只在一处定义（判据按限定符号锚定：
   StoreSession::promote_collection_to_bftree、StoreSession::handle_bftree_drain_and_delete、
   StoreSession::refresh_tiered_meta、StoreSession::load_collection_stub 各 grep 定义点 1 处）。
2. wkv 对外方法面（StoreSession 的 pub 成员名集合）逐符号不变，wcol/wbftree/wnode 调用点零改动。
3. 树锁两函数仍在唯一载体内且只有一份实现（与 range-index-locks-acquire-api 票的收口结果一致）。
4. cargo check 通过（禁在共享 target 跑 test.sh / clippy.sh）。

双花登记
并发代理就条 6 另立同题薄票 next/db-range-index-stub-split.md（同改 wkv/src/range_index/stub.rs，
五域划分与本票一致），两票同改一文件只取一棒：本票为正文载体，派发时以本票为准并删除该薄票，禁双花。
排棒次序：本票依赖 task/ing/range-index-locks-acquire-api.md（同为 stub.rs 树锁两函数的收口）先落，
否则拆件会把该锁口复制进两个新文件。
