终态：已合入 dev（2026-09-27）。9780a2e §170(撞号让位重编) new_readonly 名实归一:36 写位点改 new,剩余消费点全纯读;collect 执行体含 rmw 窗一并收

甄别结论：通过 | 定级 P3 | 2026-09-27 zcode fixloop 甄别席（双侧锚现码亲验，零撞票）
执行提示：方案1：写面改 new 令名实归一

审核结论：通过（P3 登记级。六项判定：一、真实性亲验坐实——new_readonly 函数体逐字即 Self::new(batch)（storage_session.rs:115-117），文档宣称「写入路径绝不可用」（:111-114）无任何机制支撑；slow.rs:313 统一会话承接 Set/Setex/Incr 族、位图写族、EXPIRE/PERSIST/GETDEL/RENAME 族、ETag 写臂与 MSETNX 条件回滚（delete_string），写族确经该会话落笔，非幻觉；C# StorageSession.cs 仅单一 public 构造（:86）无只读变体，N.A. 对位成立。二、消费面全量核扩展——tiered_demote.rs:132/150 为第二处写面违约（降阶写回经同会话落笔）；diskless_sync_ttl.rs:137-150 等测试面亦经 new_readonly 落笔（upsert_string/expire_at_ticks/upsert_tag）；slot_mgmt.rs:94 for_each_db_in_slot 以 readonly 分支选 new_readonly/new 双态，证明仓内契约本意即「写面走 new」，slow.rs 反成主违约方。三、危害实证非假想——假契约已误导注释：migrate.rs:105-107 以「new_readonly 独立表使 bump_watch_version 推进失联」作选型依据，机械不成立（wkv 钩子为引擎级 store.watch_hook OnceLock，wkv/src/store/mod.rs:116，bump 经 wkv/src/session/mod.rs:792-793 直达共享表，new 与 new_readonly 全同）；slow.rs:310-311「只读扫描会话/不推进 WATCH/独立版本表」同误。四、查重成立——task/ 全册无本票重复，近亲 wnode-batch-epoch-guard-across-await-degrade-slow-arms 票仅关联面引用 :111-117，主题不同。五、方向裁决采票内方案 1（写面改 new），拒方案 2（订正文档），理由见文末裁定。六、格式合规：纯文本无违规元素，rust 路径带行锚，C# 侧 N.A. 附构造族出处可接受；定级 P3 恰当——零行为差异纯文档性，实害为选型误导已在仓扩散，够格立票不入 reject）

StorageSession::new_readonly 契约名实不符：文档称「写入路径绝不可用」而 slow.rs 写族经同一构造落笔（机械上即 Self::new 无行为差异），误导后续审查与调用选型（rust 自造口命名漂移，纯文档性）

问题分析：
1 Garnet 契约对齐：无对位——C# 慢路径写与读共用 storageSession 无只读变体（garnet/libs/server/Storage/Session/StorageSession.cs 构造族），本口属 rust 自造命名。
2 工程现状确证：wedb/wnode/src/storage/session/storage_session.rs:111-117 new_readonly 文档「构造只读扫描会话……写入路径绝不可用」；slow.rs:313 StorageSession::new_readonly(batch) 后 string_slow 写族经同一 storage.upsert_string/storage.batch.put_ttl/rmw_window 落笔；new_readonly 机械上即 Self::new，无任何机制差异。
3 逻辑危害确证：纯契约文档失配，无内存/持久化危害；误导后续审查与调用选型（以为该会话写不出盘）。

涉及代码：
rust 文件与函数：
wedb/wnode/src/storage/session/storage_session.rs:new_readonly（:111-117）
wedb/wnode/src/resp/garnet_api/slow.rs:消费点（:313）

对应 c# 文件与函数：
N.A.（rust 自造口；C# StorageSession.cs 读写共用构造）

精炼执行方案：
1 二择一：slow.rs 写族改 StorageSession::new，或订正 new_readonly 文档为实态（「不登记 WATCH 版本表，非机械只读，写入可用」）
2 验证：文档与实现一致性巡检（grep 消费点全量核）

审核裁定执行方案：
1 采票内方案 1「写面改用 new」，拒方案 2「订正文档」：new_readonly 之名为契约载体，全仓约 90 消费点以其作「仅扫描」语义标记，slot_mgmt.rs:94 已示范 readonly/new 分支正例；订正文档令名字永久失真且须连改三处无辜注释，改用 new 后剩余消费点全部真只读、文档成真、名实归一
2 改动点一：slow.rs:313 StorageSession::new_readonly(batch) 改 StorageSession::new(batch)；同步订正 :310-311 注释——去「只读扫描会话/不登记推进 WATCH/独立版本表」表述，改述为「慢路径执行域独立批处理纪元 + 全功能存储会话；WATCH 推进经 wkv 引擎级钩子共享表收口，写族落笔与快路径同一单点」
3 改动点二：tiered_demote.rs:132/150 同改 StorageSession::new（降阶写回在场，非只读扫描；:120-122 注释已如实，构造名归正即可）
4 改动点三：migrate.rs:105-107 注释订正——删「new_readonly 的独立表会使 bump_watch_version 推进失联」不实依据（wkv 钩子引擎级共享，两构造同体），改述为「导入写入走全功能会话语义标记，与 resp 写命令同形」
5 测试面归正：diskless_sync_ttl.rs 等经 new_readonly 落笔的测试位点机械改 new（同体零行为，随 grep 全量核逐点清）；纯只读测试位点不动
6 验证：grep 全量核 new_readonly 剩余消费点均无写族落笔（SCAN/KEYS/DBSIZE、HasKeysInSlots、快照迭代、探针、slot_mgmt readonly 分支）；行为零变更（两构造同体），./test.sh 全绿即闭环
7 deviations.md 顺延登记一条（登记级，纯注释与构造名归正零行为，沿文档分叉订正先例）
