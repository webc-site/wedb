归档注记：合入 2db529bf，SealOutcome 三态化+NotSealable 弃归池(C# Elided 同款)，七调用点收口，池恒 Closed 不变式闭合

甄别结论：通过（甄别席 J6，2026-09-27，定级 P1——复活池转移 seal 失败弃返仍归池，未密封记录入池破不变式）。亲验：transfer_to_reviv_pool let _ = 弃返后无条件归池（addr.rs:137-142）；try_seal_record 失败面真实且 bool 两 false 态不可分辨（inplace.rs:195-220，「已密封幂等回 false」自陈 :191-193）；调用点实为七处（含票面漏的 inplace.rs:100，审核席增补正确）；「归池槽位恒 Closed」不变式在 Pad 特化臂头注自陈。C# SealAndInvalidate 无失败面（InternalUpsert.cs:366-372）+IsClosed 前置断言（Helpers.cs:128）双侧成立。三态化+不可密封弃归池走 C# Elided 同款，单套机制。派沙箱席 c01l。

审核结论：通过，增强（锚点亲验：try_seal 为单次无重试 CAS；调用点实为七处，本票原漏 inplace.rs:100，一并收口）

transfer_to_reviv_pool 吞密封失败仍无条件归池，破「归池槽位恒处 Closed 态」全库单点不变式

问题分析：
1 C# 原型行为（libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalUpsert.cs:366-372 与 Helpers.cs:TryTransferToFreeList :124-140）：CAS 换链成功后旧链首先 SealAndInvalidate（纯内存单原子字操作，结构上无失败面），且仅当地址达复活下界才转 TryTransferToFreeList；后者以 IsClosed 为前置断言（Helpers.cs:128），未闭合槽位绝不入池——池中槽位恒处闭合密封态是该机制的成立前提。
2 工程现状确证（rust）：wedb/wkv/src/store/addr.rs:WedbStore::transfer_to_reviv_pool（:137-143）系全仓自陈「槽位移交 FreeRecordPool 唯一单点」，却对 self.hlog.try_seal_record(addr, true) 以 let _ = 弃返后无条件 reviv_pool.put。而 wedb/whlog/src/hlog/inplace.rs:HybridLog::try_seal_record（:195-220）的失败面真实存在且自带契约注释「页面未就绪、偏移越界或头部解码失败一律回 false，由调用方降级走尾部追加」；更兼其 bool 返回值令「已密封幂等 false」（可入池）与「未能密封 false」（禁入池）两态不可分辨。出池侧 wreviv::FreeRecordPool::take 与 whlog revivify_record_at 均无密封态复核臂，未密封槽位一经复活方原位覆写即生效。
3 逻辑危害确证：调用点自身注释（wkv/src/session/raw/write/inplace.rs:446-449）明言「缺此密封则槽位一经复活方原位覆写即致其后继悬挂、读者读出撕裂内容」——本单点恰把该防线降为概率保障：环形页滑窗竞态下（is_page_loaded 双检败、页关闭清洗窗）seal 静默失败仍入池，沿旧前驱指针回溯的在途无锁读者凭 SEALED 位触发 RETRY_LATER 让步的机制落空，读出新旧混合撕裂帧或经脱链旧记录回溯悬挂。六生产调用点（wkv/src/compact.rs:233、session/raw/mod.rs:222/:236、session/raw/write/inplace.rs:451/:850、copy_to_tail.rs:67）全部依赖此单点承诺。C# 无失败面可弃，rust 引入页驻留失败面后调用侧未接住，属转写自闭责任非既定改良。

涉及代码：
rust 文件与函数：
wedb/wkv/src/store/addr.rs:WedbStore::transfer_to_reviv_pool（let _ = try_seal_record 弃返位）
wedb/whlog/src/hlog/inplace.rs:HybridLog::try_seal_record（失败面与契约注释位）
调用面：wedb/wkv/src/compact.rs、wkv/src/session/raw/mod.rs、wkv/src/session/raw/write/inplace.rs、wkv/src/session/raw/write/copy_to_tail.rs

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalUpsert.cs:elide 转移臂（SealAndInvalidate 先行 + GetMinRevivifiableAddress 门）
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Helpers.cs:TryTransferToFreeList（IsClosed 前置断言）

精炼执行方案：
1 try_seal_record 返回值改三态（本次密封成功 / 已处密封 / 不可密封），或增设密封后槽位头复核口，令两 false 态可分辨。
2 transfer_to_reviv_pool 仅在后两态之一确认槽位恒 Closed 时方 put；不可密封态弃归池走 C# Elided 遗弃同款（可观测留痕，宁漏回收不冒撕裂险）。
3 测试验证点：注入页未就绪与解码失败形，锁「池中无未闭合槽位」不变式（遍历池内槽位头复核 SEAL）；锁既有幂等已密封形仍正常归池零回归；并发读者在途回溯经复活覆写窗读出撕裂内容的断言不复发。
