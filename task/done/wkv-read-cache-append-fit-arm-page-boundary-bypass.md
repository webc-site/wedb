# wkv-read-cache-append-fit-arm-page-boundary-bypass（P1，源自 task/issue/wkv-read-cache-append-fit-arm-page-boundary-bypass.md）

## 终态注记（合入哈希：8498969 / 收口形态：页界装载闸+冻结几何）
- 合入哈希：8498969
- 收口形态：
  1. fit 臂补页界装载闸：offset==0 时判目标槽 is_page_loaded，未装载回绕旧代页转投换页协议，对位 whlog is_page_loaded 单闸，绝不直通绕过装页协议。
  2. 换页协议 target_start 区分穿越形态（curr_tail 本页起点）与跳页形态（curr_tail + remaining 本页末），锁内复验 is_page_loaded 幂等短路。
  3. close_pending_page 冻结几何显式化：以 pending_close_until 恒等反推换页目标（next_page_start = want + (num_pages - 1) * page_size），消除对活 tail 的依赖，杜绝几何漂移。
  4. 完备锁测集成：tests/read_cache_page_boundary_turn.rs 包含恰满穿越水位、跳页武装交错、并发等长恰满穿越三组高压用例全绿通过。

## 甄别结论：通过（2026-09-28 独立审核现码复验，P1 维持）

击穿面（a）页界直通完全确证：均匀恰满负载（记录尺寸整除页容量）下每页界必直通，else 臂整套装页协议在该负载下成死代码，首轮回绕即中毒，非小概率对齐巧合。C# 锚逐行核实：AllocatorBase.cs:1460 TryAllocate 恰满后下一条分配 post-Add Offset>PageSize 必入 :1489 HandlePageOverflow（:1352），:1336 NeedToWaitForClose 验证 + IssueShiftAddress 后方入新页；ReadCache 不变式注释实位 garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:74-75（票面原路径有误，此处已订正）。对照正形 whlog/src/hlog/append.rs:125 is_page_loaded 单闸在先、页内 fit 分支在闸后，读缓存 fit 臂无闸分叉确凿。

击穿面（b）武装后余量推进为级联确证，两处口径修正：
1. 纯页内漂移不破 close 几何恒等（:304-306 向上取整重算的不变性），错位一页的可达前提是（a）先发生——余量恰被小记录铺满、tail 落页界、下一条经（a）直通写入未关闭旧代页，此后 close 才执行：从页界内漂移 tail 重算 next_page_id 越一页，cleanse_page 清错页、真正被覆写页无人清洗、head/closed_until 越过未清洗页、悬垂项 abs<closed_until 即刻放行回链头重探死环（读者活锁链确证可达）。
2. 票面「:354 tail.store 回退重叠写」方向笔误，现订正：:306 next_page_start ≥ curr_tail 恒成立，publish 只会前跳，实际危害是 tail 前跳整页地址洞（页尾已分配段永不回收），无重叠写。测试断言勿按回退方向写。

审核优化并入执行方案：
- close_pending_page 冻结几何无需新字段：pending_close_until 与换页目标存在定比（next_page_id = page_of_address(pending_close_until) + num_pages − 1，即 next_page_start = pending_close_until + (num_pages−1)·page_size），执行时据此冻结边界重算；:316 防御护栏「理论不可达」注释语义随冻结重算同步更新。
- 测试增补断言：页界直通修复后，均匀恰满负载下 else 臂必须被触达（可断言 closed_until 单调发布非零），防修复退化为「页界改从 fit 臂取巧清零」。

误报排查零重叠：deviations.md 全文 grep 两阶段关闭/武装/close_armed/pending_close/纪元延迟/closed_until 零命中；§179/§180 属哈希桶闩族不同域；done 在册 wkv-rc-close-pending-page-double-register-toctou（锁内复核）与本案几何失准不同缺陷。

原票面：

wkv 读缓存 append fit 臂缺页界闸：tail 恰落页界直通未换装页槽 + 武装后余量推进击穿 close 几何恒等前提

定级：P1（启用 read_cache 配置下数据损坏与读者活锁；默认 enable_read_cache=false 不暴露）

问题分析：
1. Garnet 契约对齐
C# 主日志分配器把「tail 越入新页」收敛为单点协议：AllocatorBase.cs:1460 TryAllocate 在 increment 后 offset 越过 PageSize（含恰触界后的下一条分配）必入 :1352 HandlePageOverflow，经 :1336 NeedToWaitForClose 验证目标页已关闭并 IssueShiftAddress 后 tail 方可入新页，字节绝不先于关页协议落入下一页槽；ReadCache.cs:73-81 不变式注释「eviction runs as a deferred, epoch-gated drain-list action and HeadAddress is published before it is queued」即页槽复用必须先经驱逐协议（head 推进越过旧代页、纪元屏障、清洗恢复索引、换装）再放新写入。本仓主日志内核同构已对位：whlog/src/hlog/append.rs:125 以 is_page_loaded 页闸强制页界穿越走 ensure_page_ready + 换页协议。
2. 工程现状确证
读缓存追加 wedb/wkv/src/read_cache/append.rs:83-87 fit 判据仅按 remaining = page_size - offset_in_page(curr_tail) 与 rec_size 比较，无任何页界/装载闸。两个击穿面：
（a）tail 恰落页界（上一条记录恰好铺满整页，offset==0）时 remaining 恒等于整页，rec_size ≤ page_size（:69 已钳）恒走 fit 臂，page_inflight 注册检查（:91，fetch_add & INFLIGHT_CLOSED）因目标槽旧代页从未武装（武装只发生在 else 臂 :161-180 回绕分支）而成功，新记录直接写入未经两阶段关闭/清洗/换装的旧代页槽，整套装页协议被绕过。
（b）回绕武装拍（:177-180 fetch_max head/pending_close_until + close_armed 置位后返回 None）只挡回「remaining 不足」的分配者；武装页余量内更小记录仍走 fit 臂 CAS 推进 tail，甚至恰好铺满后越过页界进入下一未武装页。close_pending_page :286-287 注释自陈「武装后、关闭前页内 CAS 预留恒被页满分支挡回，故执行时重算的换页几何与武装时恒等」——该冻结前提被 fit 臂击穿后，:304-313 从已漂移 tail 重算 next_page_start/next_page_id/evicted 页号整体错位一页：cleanse_page 清错页（真正待恢复索引的旧代页无人清洗）、:315/:327/:350 head/safe_head/closed_until 越界发布越过未清洗页、:354 tail.store 发布错位（见甄别结论订正：tail 前跳地址洞，非回退重叠写）。
3. 逻辑危害确证
触发条件为环回绕稳态下某条记录恰好铺满整页（rec_size 与页内剩余相等，8 字节对齐下常规可达概率；均匀恰满负载下确定性触发），一次即中毒。危害链：（一）未清洗覆写——旧代页中仍挂哈希索引的记录字节被新记录覆写，读侧链入该地址读到错键内容，键失配沿链尾返回 None 即活键假 NOTFOUND，或 header 尺寸域解析错乱致链遍历踩进记录中部；（二）读者活锁——错位 close 后 closed_until_address 越过未清洗页，该页索引项地址恒小于 head 而 spin_wait_until_record_is_closed 以 closed_until 为解除判据即刻放行，回链头重探死地址形成读者永久循环；（三）在途读者数据竞争——未武装页槽无纪元屏障保护，window.rs PageView::Fast 裸切片直读读者与新写入并发，撕裂读。默认配置 enable_read_cache=false 不可达，启用配置下为数据正确性缺陷。

涉及代码：
rust 文件与函数：
wedb/wkv/src/read_cache/append.rs:ReadCache::append（fit 臂 :83-87、注册检查 :91、武装拍 :161-180、正向内联换装 :189-200）
wedb/wkv/src/read_cache/append.rs:ReadCache::close_pending_page（几何重算 :304-313、水位发布 :314-321/:327/:350、tail 回写 :354）
wedb/wkv/src/read_cache/window.rs:ReadCache::spin_wait_until_record_is_closed（解除判据消费面）
对照正形：wedb/whlog/src/hlog/append.rs:125（is_page_loaded 页闸强制页界走协议）

对应 c# 文件与函数：
garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:TryAllocate（:1460）/ HandlePageOverflow（:1352）/ NeedToWaitForClose（:1336）/ OnPagesClosed 纪元延迟链
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/ReadCache.cs:74-75（驱逐 deferred epoch-gated 不变式注释，路径经审核订正）

精炼执行方案：
1. fit 臂补页界闸：offset_in_page(curr_tail)==0（页界穿越）时不得直通，先判目标槽 is_page_loaded——已换装（正向/已协议页）零开销直通；未换装（回绕旧代页）转投换页协议路径（区分「页界穿越需协议」与「页内放不下需跳页」两种语义，防 else 臂 next_page_start = curr_tail + remaining 把 tail 跳页造成地址空洞），对位 whlog is_page_loaded 单闸形态，不引入第二机制
2. close_pending_page 冻结几何显式化：按甄别结论优化口径，执行时以 pending_close_until 定比冻结边界重算（next_page_start = pending_close_until + (num_pages−1)·page_size），确保 cleanse 页号/水位边界/tail 发布三值与武装拍恒等；:316 防御护栏注释语义同步更新
3. 测试验证点：wkv 新集成用例构造「恰好铺满记录序列 + 回绕稳态 + 混合小记录」：断言页槽换装协议完整（closed_until 单调不过未清洗页且均匀恰满负载下非零发布、被驱逐页索引全部恢复主日志地址、无读者活锁）、武装后小记录推进场景 close 几何与武装值恒等；既有 read_cache 全量用例保持绿（cargo nextest run -p wkv read_cache 过滤族）
