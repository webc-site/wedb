检查点刷盘 IO 节流旋钮 checkpoint-throttle-delay 五级链缺失：既未转写也未作不移植裁定

来源：next/glm.db.md 条 3 立项（该文件本波剪空删除）。取证基线：主仓 /Users/z/git/db/wedb
分支 dev，行号按当下 HEAD 的符号重取。判定：成立且待做（本单要求先出裁定，再按裁定二择一落地，
不接受继续留在「既没实现也没登记」的第三态）。

裁定收口（分支 cp-flush-throttle，路径乙「登记不移植」）
取证确认本单现状描述全部属实（C# Options.cs:417-418 旋钮、GarnetServerOptions.cs:359 默认 0、
GarnetServer.cs:477 注入、AllocatorBase.cs:2297-2398 逐页 WriteAsync + WaitOneFlush +
Thread.Sleep 节流臂、IndexCheckpoint.cs:25 同参透传；rust wconf/wcpr/whlog/wkv 全域 grep
throttle 零命中）。裁定取路径乙，理由：C# 节流臂的插入点是「逐页一次 WriteAsync」循环本体，
rust 快照刷盘内核（whlog/src/hlog/io.rs flush_sealed_page_range）为 PendingFlushList 贪心合并
相邻区间后的单次连续 flush_range_aligned 写，且按 transpile 既定决策采 compio 一线程一核
await 形态，不存在逐页节流点可插；若强行分段下发睡眠，等于把已裁定合并承接的
FlushCompletionTracker（storage.yml 已登记）的节流闸语义复活成第二套刷盘节奏面，与本仓
「一处机制」准则相悖。故本单不转写五级链，收口动作为 ignore 登记（本 commit）：
hosting.yml Options.cs 冷门项清单点名 checkpoint-throttle-delay（含 GarnetServerOptions
载体字段与 KVSettings/CheckpointSettings/SnapshotCheckpointSMTask/AllocatorBase 注入执行全链
理由），storage.yml 在 FlushCompletionTracker.cs、SnapshotCheckpointSMTask.cs、
IndexCheckpoint.cs 三处理由块补写节流臂/同参透传随旋钮不转写的凭据。
`bun js/check.js` 语料解析零失效、判定输出与本改动前一致（无新增缺失/淘汰）。若未来刷盘改
分段下发（如 flush-safe-read-only-bound 后续批），须连节奏旋钮一次同改，禁另立第二套节流面。

结论一句话
C# 为控制检查点大刷盘对前台 p99 的干扰专门做了旋钮：配置面 checkpoint-throttle-delay，
默认值 0 本身就是「每页等完成再发下一页」的串行节流形态，-1 才全速连发，>0 再叠加页间睡眠；
同一参数还传给索引快照刷盘。rust 侧这条五级链整体不存在（配置槽位、投影、注入、刷盘循环节流
臂、索引侧同参），检查点刷盘走 flush_all → PendingFlushList 贪心合并为单次巨型 write_aligned，
设备队列被一次大写长时独占，节奏完全不可调。按 transpile 规范，未转写项必须在
js/check/ignore 登记理由，本旋钮当前两处都没有。

现状（主仓 HEAD 实测）
1. 配置面无槽位：/Users/z/git/db/wedb/wedb/wconf/src 全域 grep throttle 零命中
   （/Users/z/git/db/wedb/wedb/wbase/src/throttle.rs:19-97 的 NetworkSenderThrottle 是网络发送
   在途配额节流，与刷盘无关，不可当承接点）。
2. 刷盘链无节流面：/Users/z/git/db/wedb/wedb/wcpr/src/manager/create.rs:227 store.flush_all()
   （检查点刷盘唯一入口，无参数）→ /Users/z/git/db/wedb/wedb/wkv/src/store/flush.rs:114
   flush_all → :105-110 flush_pages_range 直接转发 →
   /Users/z/git/db/wedb/wedb/whlog/src/hlog/io.rs:230-289 flush_pages_range（:238-246 贪心合并、
   :262-289 页读锁逐页拷贝进单缓冲后一次 device.flush_range_aligned 巨型写，无页/页批之间的
   等待或睡眠位）。
3. 裸奔的三条检查点生产链：/Users/z/git/db/wedb/wedb/wnode/src/database/database_manager_base.rs:186-250
   take_database_checkpoint_async；SAVE/BGSAVE
   /Users/z/git/db/wedb/wedb/wnode/src/resp/garnet_api/slow.rs:1015-1031；AOF 体积超限周期任务
   /Users/z/git/db/wedb/wedb/wnode/src/service.rs:518-537。
4. ignore 语料的现状只覆盖了一半：/Users/z/git/db/wedb/js/check/ignore/storage.yml:2011-2014
   已整文件登记 FlushCompletionTracker.cs（理由含「节流等待」字样，即节流计数件被记为不转写），
   但旋钮本体（Options.cs 的 checkpoint-throttle-delay 项、GarnetServerOptions
   CheckpointThrottleFlushDelayMs 字段、AllocatorBase 的 throttleCheckpointFlushDelayMs 臂、
   IndexCheckpoint 的同参传递）在 js/check/ignore 全域零命中（grep
   CheckpointThrottleFlushDelayMs / AsyncFlushPagesForSnapshot 均无条目），
   /Users/z/git/db/wedb/js/check/ignore/hosting.yml:58-68 的「Options.cs 冷门项不转写清单」也
   未点名本旋钮；同文件 :940-945、:1050-1056 登记的是设备基类 Throttle/TryComplete 面，
   属另一件事，不能当本旋钮的裁定。也就是说 check.js 当前沉默只是因为 C# 侧这些是属性与可选
   参数而非独立方法，不代表已裁定。

C# 参考
/Users/z/git/db/wedb/garnet/libs/host/Configuration/Options.cs:417-418
（[Option("checkpoint-throttle-delay")]，HelpText "-1 - disable throttling; >= 0 - run
checkpoint flush in separate task, sleep for specified time after each WriteAsync"）；
/Users/z/git/db/wedb/garnet/libs/server/Servers/GarnetServerOptions.cs:359（默认 0）；
/Users/z/git/db/wedb/garnet/libs/host/GarnetServer.cs:476-477（注释 "Run checkpoint on its own
thread to control p99" 后注入 kvSettings.ThrottleCheckpointFlushDelayMs；
/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Common/KVSettings.cs:129
默认 -1，CheckpointSettings.cs:52 同）；执行体
/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:2297-2316
（>= 0 时把刷盘转 Task.Run 独立任务）与 :2327-2398（逐页 WriteAsync 后
:2393-2397 WaitOneFlush + Thread.Sleep(throttleCheckpointFlushDelayMs)）；索引快照同参
/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Recovery/IndexCheckpoint.cs:25。

修法（二择一，须显式选择并留下凭据）
路径甲（转写）：wconf 补 checkpoint-throttle-delay 槽位（-1 关闭 / >=0 启用，语义与 C# 一致），
经 RuntimeServerOptions 投影到 wcpr 的检查点参数，whlog flush_pages_range 在
device.flush_range_aligned 的填充循环外增加「按页批分段下发 + 段间让出/睡眠」形态：compio 线程
每核模型下禁止 Thread.Sleep 阻塞 reactor，段间让出用 await 型睡眠（compio::time::sleep，
whlog/src/hlog/shift.rs:9 已在用同件），默认值取 C# 的串行形态（0 = 发一批等一批完成）。注意与
task/ing/flush-safe-read-only-bound.md 的改动同处一条刷盘链，分段逻辑必须落在同一次改造里，
避免两单各改一遍 io.rs。
路径乙（裁定不移植）：在 js/check/ignore 对应 yml（server/hosting 侧登记 Options.cs 冷门项清单
的同一块，storage 侧登记 AllocatorBase/IndexCheckpoint 的节流臂）写明具体理由——rust 快照刷盘
为 PendingFlushList 合并后的单次连续写（whlog/src/hlog/io.rs:230-289），无「每页一次 WriteAsync」
的节流点可插，且 compio 线程每核下合并大写本身就是对前台更友好的形态。理由务必用 `理由: >-`
折叠块且明文标量内不得出现「 : 」（历史上写坏一份 yml 会让整份语料静默失效、凭空多出一批
缺失）。
无论走哪条，都不许把「忽略」当成默认结果继续悬空。

边界
与 task/ing/on-demand-checkpoint-flag-unwired.md 不同面（那条管 on-demand-checkpoint 开关恒
true），与 task/ing/flush-safe-read-only-bound.md（同波新立）分别是刷盘节奏与刷盘边界，
与 task/ing/checkpoint-dir-retention-wiring.md（同波新立）分别是刷盘节奏与快照保留。

优先级
打磨（可观测调控面缺失），低档，但含一项零成本的裁定动作（ignore 登记），可随任一 wcpr/whlog
改造批顺手收口。
