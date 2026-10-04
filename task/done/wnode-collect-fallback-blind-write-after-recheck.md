归档注记：合入 097677e7，正身收集臂与 custom 慢臂 fail-closed 弃写；run_async_rmw 收尾信封臂改驱逐内联+复验重试闭环(C# CompletePending 同循环重试语义)，盲写通道删除

甄别结论：通过（甄别席 J5，2026-09-27，定级 P2——复活/双域/误删三向正确性洞，但限页翻转窗×await 间隙窄时序，非系统性）。亲验：objects.rs collect_save_fallback :137-150 empty 即 delete_string await 否则 obj_save await 内部零再裁决；collect_hash_key :220 复验先于 :235 Ok(false) fallback await、collect_sorted_set_key :295/:303 同形；storage_session.rs:756-761 obj_save 页翻转降级臂直落 upsert_tag 无域归属探针；custom_object_rmw_async :513-532 复验后 Ok(false) 即 upsert_tag 盲写；同文件 :218-222 复验不过臂弃写通道现成而 fallback 另走盲写，违模块头单套裁决纪律。C# 对照亲验：HashOps.cs HashCollect（:589 起，:601 RMWObjectStoreOperation）求值写回同记录锁内，间隙结构性不存在。审核裁定第 3 条修正（同型三收 obj_save 内部降级臂，弃前置复核）亲验认可——:533 复验与同步尝试间确无让核点。查重：ing 池 latch-starvation 票系锁饥饿异轴，r6 已修面无同形。派沙箱席 c01f。

审核结论：通过（P2）

定级理由：三臂形态逐一亲验坐实，属真实正确性洞（复活/双域/误删三向全成立），但触发面限定在页翻转/TTL 磁盘候选降级窗与 await 间隙交叠的窄时序，非热路径非系统性，P2 恰当。反证排查未翻案：obj_save 降级臂（storage_session.rs:756-761）内部无任何隐性复验，Err 即物化整值走 upsert_tag 异步臂盲写（wkv 异步臂内探针仅 migration claim 与 TTL 腿补偿，非域归属再裁决）；delete_string 降级臂同为盲删；collect 臂持 _window 跨 await 对 DEL/SET 零约束（rmw_helpers.rs:488-490 与 rmw_window.rs 模块头两基说明自证：窗口取 user_key 桶，DEL/SET 取物理记录键桶，两个不同基）——r6 修法本就是「窗口挡 RMW 方＋落笔前复验挡 DEL/SET 方」两半，fallback 臂绕开的正是复验这半边。r6 已修面对照确证：run_sync_rmw Ok(false)|Err 一律 Degrade 整体重放（rmw_helpers.rs:1215-1218，重放按当前态重装载），run_async_rmw/custom_object_rmw_async 复验不过一律 Err 存储忙，已修面无「同步败转异步盲写」形，本票确系其 fallback 臂复活形非重复提报（r6 票档案已归档不在 task/，票号存于 object_store_utils.rs:1038 头注）。先例语义确证：collection_item_source.rs 取件臂复验后写失败即弃原值返回不转异步盲写。查重无冲突：task/issue、todo、ing 现有票无同面（wnode-collect-arm-rmw-writer-latch-starvation-locktimeout 是锁饥饿面）。C# 对应点亲验：HashOps.cs:589 HashCollect 内 :601 RMWObjectStoreOperation，求值与写回同记录锁内，间隙结构性不存在，契约对齐成立。

收集族写回复验在「同步失败转异步 fallback 落笔」路径漏封：复验先于 await 发生，fallback 盲写信封跨 await 无再裁决，复活已删键/信封字符串双域并存/陈旧墓碑误删新值（r6-del-rmw-writeback-revalidate 已修缺陷类的 fallback 臂复活形，正身加同型两臂）

问题分析：
1 Garnet 契约对齐：C# 收集命令逐键经 RMWObjectStoreOperation 进 Tsavorite 记录锁管线（garnet/libs/server/Storage/Session/ObjectStore/HashOps.cs:589 HashCollect 内 :601；SortedSetOps.cs:1735 SortedSetCollect 同形；入口 AdminCommands.cs:651 NetworkHCOLLECT），对象求值与写回同锁内完成，装载到写回间隙结构性不存在（RMWMethods.cs InitialUpdater/CopyUpdater 同锚，本仓 object_store_utils.rs:1050-1054 头注自证一致）。
2 工程现状确证：本仓写回复验单机制——同步档 obj_save_recheck_sync（object_store_utils.rs:1063）/异步档 obj_save_recheck_async（:1089），手写臂统一收口 obj_writeback_recheck（rmw_helpers.rs:1268/1280），run_sync_rmw（:1165）/run_async_rmw（:533）/收集臂/取件臂/STORE 窗俱在。但三条臂在「复验通过 → 首次同步写 Ok(false)（页翻转/TTL 记录磁盘候选降级）→ 改走异步 fallback 盲写」路径上复验先于 await、fallback 落笔无再裁决：正身 collect_save_fallback（wedb/wnode/src/resp/garnet_api/objects.rs:137-150）empty 即 delete_string await 否则 obj_save await；调用点 collect_hash_key（:220 复验 → :224 obj_save_or_gc → :235 Ok(false) 即 fallback await）与 collect_sorted_set_key（:295/:303）同形——复验与其后「同步写失败+异步 obj_save 内部再试再败+upsert_tag await」三个事件位之间终判早已失效，storage.obj_save 页翻转降级臂（storage_session.rs:756-761）调 upsert_tag（:692）全程无存活域探针是裸物理键写。同型二 custom_object_rmw_async（custom_object_commands.rs:513 复验 obj_save_recheck_async → :523 Ok(false) → :525-532 upsert_tag 盲写）。同型三 run_async_rmw（rmw_helpers.rs:533 复验 → apply_rmw_post_operate :712 obj_save await，页翻转时内部再跨 await 盲写）。可达链：收集落在页翻转窗（环形满圈翻转常规事件）→ 复验通过 → 同步写 Ok(false) → await 间隙对面落 DEL（已 ACK 墓碑）→ upsert_tag 落笔陈旧信封复活已删键；对面落 SET → 信封与 String 双域并存探测树类型发散；空集 GC 臂方向 delete_string await 对面落 SET 被陈旧墓碑误删刚 ACK 的新值。collect 臂虽持 _window 跨 await 但对该面零约束（rmw_helpers.rs:488-490：DEL/SET 取物理记录键桶闩不取 RMW 窗）。
3 逻辑危害确证：正是 r6-del-rmw-writeback-revalidate 已修缺陷类在 fallback 臂的复活形；对照同文件 :218-222 复验不过臂自身已裁「弃写留待下一轮周期收集无正确性损失」，弃写通道现成而 fallback 另走盲写，违 rmw_helpers.rs:1228-1232 模块头注「骨架外手写臂复用与骨架同源的窗口与复验判定核，禁任何臂另立第二套裁决」。

涉及代码：
rust 文件与函数：
wedb/wnode/src/resp/garnet_api/objects.rs:collect_save_fallback（:137-150 正身）、collect_hash_key（:220-235）、collect_sorted_set_key（:295/:303）
wedb/wnode/src/storage/session/storage_session.rs:obj_save 降级臂（:756-761）、upsert_tag（:692）
wedb/wnode/src/resp/objects/custom_object_commands.rs:custom_object_rmw_async（:513-532 同型二）
wedb/wnode/src/resp/objects/rmw_helpers.rs:run_async_rmw→apply_rmw_post_operate（:533/:712 同型三）

对应 c# 文件与函数：
garnet/libs/server/Storage/Session/ObjectStore/HashOps.cs:HashCollect（:601 RMWObjectStoreOperation）
garnet/libs/server/Storage/Session/ObjectStore/SortedSetOps.cs:SortedSetCollect
garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs（记录锁内写回无此形态）

精炼执行方案：
1 collect_save_fallback 弃盲写改 fail-closed：Ok(false) 与复验不过臂同款跳过固化留待下一轮周期收集（或按既有 RESP_ERR_SLOW_PATH_STORAGE 存储忙通道交客户端重试），与 collection_item_source.rs:410-421 既有失败语义同构，不新增机制
2 custom_object_rmw_async 的 Ok(false) 臂同改存储忙 Err(())；run_async_rmw 页翻转降级臂经 obj_save 内部盲写的面由复验判定核前置复核收口（复用 obj_save_recheck_async 单点，严禁第二套 fallback 专用再裁决判据）
3 锁测：页翻转桩使首写 Ok(false) + await 间隙并发 DEL/SET 桩，断言不复活不双域不误删；既有收集族回归全绿

审核裁定执行方案：

1 正身（collect_save_fallback）按票面步骤 1 执行：Ok(false) 臂弃盲写改 fail-closed，与同函数复验不过臂（objects.rs:218-222）同款弃写留待下一轮周期收集，或按 RESP_ERR_SLOW_PATH_STORAGE 既有存储忙约定（rmw_helpers.rs:366）上抛；与 collection_item_source.rs 取件臂既有失败语义同构，零新增机制
2 同型二（custom_object_rmw_async）按票面步骤 2 执行：Ok(false) 臂改 Err(()) 存储忙，出口与同函数复验不过臂（custom_object_commands.rs:517 output.truncate(base) + Err）完全同款，含应答帧回退卫生
3 同型三（run_async_rmw→apply_rmw_post_operate）按下列修正执行，票面「前置复核」表述不采纳：亲验确认 :533 复验终判与 obj_save 同步尝试之间零让核点，前置复核无增益；真实窗口在 obj_save 内部降级臂（storage_session.rs:756-761）的异步重写 I/O 窗，复核判词先于落笔仍跨 await。收口正解同正身 fail-closed：run_async_rmw 收尾面的页翻转降级改弃写上抛存储忙（给 wnode 层 obj_save 降级态一条可见通道，如降级标记返回或调用方改经显式降级入口），严禁把 resp 层复验核下沉 wkv、严禁第二套 fallback 专用再裁决判据
4 锁测按票面步骤 3 执行：页翻转桩参照 storage_session.rs:851-856 DELETE_FAIL_INJECT 既有 debug_assertions 故障注入先例同形新造；断言三向（不复活/不双域/不误删）+ 既有收集族回归全绿
