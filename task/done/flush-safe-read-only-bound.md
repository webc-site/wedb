常规刷盘三驱动上界越过 SafeReadOnlyAddress：先分配后编码窗口内的零/半截记录可被落盘并推进 flushed_until

来源：next/glm.db.md 条 4 立项，本票由 glm.db 分拣波立票，取证基线主仓 dev。
判定：成立，已落地。合入 dev sha ee0f3eb0（分支 flush-bound，父 dev efe1aef3）。

结论一句话
刷盘内核自身改为「先封再刷」：任何设备写入之前先把 ReadOnlyAddress 推到本轮读侧上界
并等纪元排空，持久化前缀记账恒等于该已封印上界。三类常规驱动（前台驱逐、组提交
FlushStep、begin 补刷）因此同享一份上界口径，原先外放给调用方的崩溃一致性契约收归
内核自保，第四处调用方不可能再裸奔。

甄别
1. 协议窗口坐实：whlog/src/hlog/append.rs:98-121 页内快路径 CAS 成功后 :112-114
   unsafe encode_at；:131-183 换页臂同款「CAS 发布 tail → 落笔编码」；编码内核
   hlog/mod.rs encode_at 走 buffer.raw_page_ptr_mut，不取页锁（Safety 只保证切片互不
   相交）。故 tail 越过页界只等于物理空间分配完成，不等于页内记录定稿。
2. 刷盘方护不住：hlog/io.rs 的 fill 闭包只有 per-page read_page 读锁 + is_page_loaded
   复验，页读锁与 encode_at 的裸指针写不互斥。改前 :215-219 把「待刷区间无在途编码」
   写成调用方义务，且 :306-308 的记账口径 done_until = min(merged.until, tail) 允许
   flushed_until 停在安全只读线之上。
3. 驱动一：wkv/src/session/raw/mod.rs evict_pages_for 取 target_evict_page 至
   curr_tail_page-1，刷前无 safe_ro 门槛，:268 的 shift_read_only_address 在刷完之后
   才调用（先刷后封）。触发面同文件 :96-98 与 append.rs:16/:35 PageNotReady 重试。
4. 驱动二：wkv/src/store/flush.rs FlushStep::step 直接刷 [page_id(flushed_until),
   page_id(tail-1)]，自身无门槛；宿主 wcpr/src/manager/create.rs:214-227 自带封印
   （正确形态），宿主 flush_and_evict_all 先刷后封，用户面入口
   wnode/src/resp/garnet_api/slow.rs FLUSHLOG 一族。
5. 驱动三：hlog/shift.rs shift_begin_address 先 shift_read_only_address(new_begin)
   （只登记 bump_current_epoch_action 延迟动作）随即 flush_addr_range 开刷，不等
   safe_ro 达标。
6. C# 实读复核（不采信二手摘要）：AllocatorBase.cs:1644-1652 ShiftReadOnlyAddress 用
   epoch.BumpCurrentEpoch 包裹 OnPagesMarkedReadOnly；:1744-1758 OnPagesMarkedReadOnly
   先 MonotonicUpdate(SafeReadOnlyAddress) 成功后才 AsyncFlushPagesForReadOnly，注释
   "Seal: make sure there are no longer any threads writing to the page"；:2116-2123
   AsyncFlushPagesForReadOnly 文档 "Called when all threads have agreed that a page
   range is sealed"；:2166-2175 GetFlushPageRange 只做页圆整，地址口径不外溢；
   :1180-1204 ShiftReadOnlyAddressWithWait 与 :1207-1242 ShiftAddressesWithWait 是
   「封 → 等 FlushedUntil → 推 head」的编排；:1286-1318 IssueShiftAddress（环形回绕
   前台驱逐的真身）先把 ReadOnlyAddress 封到 CalculateReadOnlyAddress 的结果（显式
   钳到 tail，见 :1621 Debug.Assert(readOnlyAddress <= tailAddress)）再 ShiftHeadAddress
   （:1888-1906 新 head 一律钳到 FlushedUntilAddress）；LogAccessor.cs:155/:271
   FlushAndEvict(wait) = ShiftHeadAddress(tail, wait) = ShiftAddresses(tail, tail, wait)
   = 先 ShiftReadOnlyAddressWithWait 再推 head。要点：C# 的刷盘区间恒为已封印区间的
   子集，且刷盘由排空动作发起，FlushedUntilAddress 结构上不可能越过 SafeReadOnlyAddress。
7. 同族重复票已在 task/reject/flush-drivers-safe-read-only-bound-dup.md 归并到本票。

落地
上界单点收在刷盘内核，不新增第二套等待循环、不改任何调用方签名：
1. hlog/shift.rs:78-102 新增 seal_read_only_and_drain(bound)：shift_read_only_address
   登记封印动作后，复用既有 wait_epoch_condition 纪元屏障内核等
   safe_read_only >= bound。文档写明它对应 C# 把刷盘塞进 BumpCurrentEpoch 排空闭包的
   等价改写（rust 刷盘是 compio 异步 I/O，塞不进 Send + 'static 同步闭包），并与截断
   专用的 wait_safe_read_only_drained（同时封口 SafeHead，谓词锚在 safe_head）划清分工。
2. hlog/io.rs:232-352 刷盘内核 flush_sealed_page_range：coalesce 与下界钳制之后、拷贝
   任何字节之前，取 sealed_until = seal_bound.min(tail) 先封再排空；完成记账
   （:348-350）上界由改前的 min(merged.until, tail) 改为 sealed_until，于是
   `flushed_until <= safe_read_only <= tail` 成为不变式。:215-227 的外放契约段改写为
   「内核自保，调用方无需自备门槛」。
3. 封印上界一律取调用方的逻辑终点而非页圆整/合并膨胀终点：地址入口
   io.rs:205-219 flush_addr_range 传 until_addr（flush_all 至 tail、begin 补刷至
   new_begin），页入口 io.rs:222-230 flush_pages_range 传页范围终点。此点是实测调出来的：
   最初按 merged.until 封印，wkv/tests/compact 两用例即红（total_freed 11200 vs 4800），
   原因是 pending_flush 的陈旧相邻区间被 coalesce 向上吸收，据其封印把调用方未曾意图
   落盘的区间封成只读，把紧缩的滚动 cut 凭空推到 tail。越界字节仍随整页写入设备，
   只是不计入承诺，后续请求自该页起点整体重写（clamp_flush_range 的下界页粒度钳制
   保证覆盖，幂等）。
4. 驱动一 wkv/src/session/raw/mod.rs:243-297 evict_pages_for：min_evicted_addr 的算法
   前移，封印改到刷盘之前（:272），删除改后的补封；仍走 store 级
   shift_read_only_address 以承接复活池 purge_below 联动。内核随后完成排空与落笔。
5. 驱动二宿主 wkv/src/store/flush.rs:146-161 flush_and_evict_all 改为
   封 → flush_all → 推 head（C# LogAccessor.FlushAndEvict 次序）；FlushStep（:171-180）
   不自备第二套钳制，文档标明读侧上界口径在刷盘内核单源。
6. 驱动三 hlog/shift.rs shift_begin_address 的补刷段（:157-167）与
   shift_read_only_address_with_wait（:311-318）不改代码，文档标明排空由内核承担；
   入口处的无条件 ro-shift 保留（C# :1667 同形，区间已预刷时仍需冻结原位更新）。
7. 未动项（射程纪律）：不碰 commit 链与 RMW 锁面，未新增桶闩/互斥；未复原
   task/done/whlog-safe-tail-identity-alias.md 删除的恒等别名与测试后门；未触碰
   wacl-deadchain-b2、boot-assembly、sess-metrics、gate-anchor、reviv-pause-epoch-drain
   及 f11/f12/f17 在飞文件。create.rs:214-227 的 wait_epoch_drain 是 wcpr 私有两项谓词
   （safe_ro 达标 && is_safe_to_reclaim(fence_epoch)）的检查点屏障，与刷盘上界不同
   契约，未合并，留作后续可选收口。
8. js/check 映射：新函数挂载
   garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs:OnPagesMarkedReadOnly
   （该 C# 函数此前无锚点，非复写），不改名不删公开面，无需新增 ignore 登记。

上界前后行为差异说明
改前：三类驱动可把 [safe_ro, tail) 内「已 CAS 发布、尚未或正在 encode_at」的槽位拷成
全零或半截记录写入设备，done_until = min(merged.until, tail) 使 flushed_until 越过该
区间。此后该页不再重刷（下界钳制认定其已承诺），驱逐时 ensure_page_ready 见
flushed_until 达标即清槽回收，记录在设备上永久缺失，而写者编码完成后索引照常发布该
地址：前台读走磁盘得 PadRecord/RecordCorrupted，重启按 flushed_until 前缀扫描亦跳过
零头，属静默丢数据。窗口只在环形回绕驱逐与组提交/补刷并发写时打开，单线程用例永不
触发，故长期未被发现。
改后：写入之前 sealed_until 已被纪元排空认可为定稿区，safe_ro >= sealed_until == 记账
上界，持久化前缀恒不越过安全只读线；sealed_until 以上字节即使随整页落盘也不承诺，
后续刷盘自该页起点整体重写。
数值零漂移面：无并发在途写者时（单线程、恢复、检查点、紧缩），sealed_until 与改前
done_until = min(merged.until, tail) 同值，flushed_until、页覆盖区间、错误路径回填
与陈旧区间吸收口径全部逐位相同（whlog 42 用例、wkv compact 35 用例逐条通过可证），
因此本票不改变既有持久化进度断言。
新增成本面：仅当上界确有在途写者时多等一次纪元排空，且该等待原本就存在——前台驱逐
改前也要在同一排空点上等 safe_head（wait_safe_head_drained），本票只是把等待点从
写之后挪到写之前，等待总次数不变；C# 把刷盘放进排空动作内，成本同型。
语义副作用（与 C# 一致）：刷到 tail 即意味着把当前已发布状态变为不可变，等价 C#
ShiftReadOnlyToTail / Log.FlushAsync，故 flush_all 现会把 ReadOnlyAddress 封到 tail。
生产侧两个宿主本就先封到 tail（create.rs:214-227、FLUSHLOG 链），无额外越界封印；
随之复活窗口 tail - read_only 短暂归零，由后续追加自然张回。实测中真正需要收紧的是
封印上界取逻辑终点（见落地第 3 条），否则紧缩与复活的窗口口径会被页粒度/陈旧区间
凭空推走。

验收
1. 门禁：cargo check --workspace --all-targets 在改前后同点唯一失败面为 dev 自带的
   wext_json/tests/json_commands_test.rs（GarnetJsonObject::try_get 少传一个 u8 实参，
   25 个 E0061），与本票文件无交集；本票载荷按
   cargo check -p whlog -p wkv -p wcpr -p wcompact --all-targets exit=0 收口
   （844c6f6 基线上工作区级 check 曾一次 exit=0）。bun js/check.js 在 844c6f6 基线
   对照跑输出逐字节一致、无新增重复定义/缺失、语料零改写；合入 dev 现点后 check.js
   会自行改写 js/check/ignore/common.yml（自动摘除已被实现的 TryParseAddressList +
   YAML 重排）并新增 slot-gate/vector-preview 两条 dup，均为 dev 存量，非本票引入，
   本票 worktree 已 checkout 还原不入库。按规程未跑 ./test.sh 与 ./sh/clippy.sh。
2. 红测（本票新增，whlog/tests/hlog/concurrent_shift.rs:367-490
   test_flush_sealed_behind_writer_epoch_guard）：写者线程持 protected_scope 追加 16 条
   跨页记录后强持守卫不放手，刷盘线程并发发起 flush_all + sync，Mutex+Condvar 事件日志
   断言 [Dropped, Flushed] 次序，并钉死 flushed_until <= safe_read_only 与
   flushed_until == tail；配 spawn_shift_watchdog 转储快照防自死锁。
   本机实测：加封印 pass（0.21s）；人为摘除内核 seal_read_only_and_drain 调用后
   fail（concurrent_shift.rs:468「越界刷：刷盘先于写者守卫退出完成」，0.22s），
   即该用例确为本票红测而非摆设。
   未做：票文建议的多任务持续 append + 环形回绕驱逐 soak 形态——真窗口只有 ns 级，
   无测试后门（whlog 刚删掉 buffer.rs set_page_id 那类后门）无法确定性命中，写出来
   会是概率断言，故以本确定性交错屏障用例替代。
3. 回归读数（本机）：cargo test -p whlog 42 passed 0 failed；-p wkv --test compact
   35 passed 0 failed；-p wcpr -p wcompact 40 passed 0 failed；-p wkv --test main
   76 passed 7 failed 与 dev 基线失败集逐条相同（dbmeta_layout、flush_database×2、
   read_cache×3、swap_database）；-p wkv --lib 25 passed 1 failed 为
   wkv/src/vdb.rs:1359 测试自身 b[1..10].copy_from_slice(&7u64.to_be_bytes()) 的
   9/8 字节长度 panic（dev 存量，非本票）。
4. 待主代理对账：wext_json json_commands_test 的 6 实参签名缺参与
   wkv vdb.rs:1359 测试自身长度错，均为 dev 现点存量红灯；wkv read_cache 三条与
   dbmeta/flush_database/swap_database 四条为 dev 基线既有红灯，本票改动未增减。
   建议集中回归时一并处置。
