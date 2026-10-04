终态注记：合入 5a18772，RI.CREATE 落盘前（紧随换代复核后）插三域钉定前缀复查（read_raw/contains_key_raw 消费钉定域），任一域新增存活记录按既有 WrongType 臂显式失败并走既有 unregister+delete_index 回滚单点；「复查后至落盘前」残余窗仍存，由后续 SET 族覆写清退内嵌 Meta 域收敛单态（非全窗闭合）；测试 tests/ri_create_cross_type_window.rs 三场景（SET 穿窗/信封穿窗/静默正路径）按收敛终态断言，变异验证无复查即红

甄别结论：通过（2026-09-29 主控甄别，定级 P3——三域预检 ops.rs:82-141 先于建树长 await，:149 register 与 :174-175 save 间零互斥零复查，SET 穿窗可达 String 与 RI 元记录双域并存；C# RangeIndexOps.cs 经 RMW 落存储层原子无此窗。修复：save 落盘前插三域复查，复用既有 WrongType 臂与 unregister+delete_index 回滚）

审核结论：通过（2026-09-29 甲轮35-A，P3 级）。三域预检与建树长 await 间零跨型复查、双穿物理可达（三域分键寻址）、C# "RMW is atomic at the store level" 结构串化成立。执行席遵照：复查点钉 save_bftree_meta_stub 落盘前最近处（紧随 :161-165 换代复核后），复查原语用钉定前缀 read_raw/contains_key_raw 消费钉定域勿用现解域；票面注明「复查后至落盘前」残余窗仍存（后续 SET 写收敛），测试按收敛终态断言勿宣称全窗闭合。

原票面：
RI.CREATE 三域存在性预检与建树长 await 之间无互斥，跨型并发写穿过预检致 String/信封与 RI 元记录双态共存，C# 记录锁序化契约分叉

问题分析：
1. Garnet 契约对齐（C# 原型行为与协议约定）：C# RI.CREATE 经 RMW 落存储层（RangeIndexOps.cs:RangeIndexCreate），与同键字符串写/集合写经 TsavoriteKV 记录 X 键全程序化——预检（ValueIsObject 与记录类型判别）与提交在同一记录锁窗内，结构上任意跨型并发只有一方胜出（后至方见前至方记录回 WrongType/覆写），双态共存不可达。
2. 工程现状确证：range_index_create（wedb/wkv/src/range_index/ops.rs:51-195）三域预检（:82 String 域 read、:87 load_meta、:97 信封域 contains_key_raw）全部先于 create_bftree 的 spawn_blocking 长 await（:126-142，含冷树回收重试重 I/O 窗），预检与提交（:148 register、:167 save_bftree_meta_stub）之间零互斥零复查：并发 SET k v（字符串写路径不拒无 meta 键）或 HSET k f v（信封物化）在窗内提交即双穿预检，终态为同一逻辑键 String 记录与 RI 元记录（或信封与 RI 元记录）跨物理域并存——GET 回字符串值而 TYPE/RI 族路由 Meta 域报 RangeIndex，双态应答自相矛盾；后续 SET 族覆写清退内嵌 Meta 域清退，RI.CREATE 已 ACK 的索引被后续字符串写静默销毁。RENAME claim 门（:75）只封迁移窗不封跨型窗。
3. 逻辑危害确证：线性化破缺（C# 必拒的创建被 ACK）+ 双态并存期查询应答矛盾 + ACK 后索引静默消失；自限性（下一次字符串写收敛）但违背「内部多态存储对同一数据集返回一致应答」契约面，窗宽为建树实际耗时非窄窗。

涉及代码：
rust 文件与函数：
wedb/wkv/src/range_index/ops.rs:range_index_create（预检 ：82-100、长 await :126-142、提交 ：148/:167）
wedb/wkv/src/session/mod.rs:session_meta_key / session_string_key（三物理域寻址）
对应 c# 文件与函数（无直位对，参照维度：竞态与契约对标）：
garnet/libs/server/Storage/Session/MainStore/RangeIndexOps.cs:RangeIndexCreate（RMW 记录锁窗内预检即提交）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/TsavoriteKV.cs:RMW 记录 X 锁全程序化对位

精炼执行方案：
1. 预检后置复查单点：save_bftree_meta_stub 落盘前（或建树 await 返回后）重跑三域存在性预检，任一域新增存活记录即按既有 WrongType 臂显式失败并走既有 unregister + delete_index 回滚（:162-165 同款机制，不新造清理路径）
2. 不引入每键创建锁（跨条带锁序风险），复查窗收敛即可对齐 C# 串化终态
3. 测试验证点：建树 await 窗内注入并发 SET k v，断言 RI.CREATE 显式失败回 WrongType、终态单态（仅字符串）、无孤儿树与旁表残留；正路径零回归
