合规门复绿：js/check 实现缺失登记缺口逐族甄别 + 一处虚构锚点改锚

严重度：MED。取证基线：主仓 dev HEAD，实测 bun js/check.js 退出码 1。

## 收尸归档（fixloop；前手 dev 代理耗尽 150 轮未合）

来源：本票 task/ing。前手分支 gate-anchor-drift-reclean（3 commits，13 文件 123+/49-）已由并发合并
b054f1ee 挂上 dev，本轮按「绝不重 merge」口径不重复合入，只复核取证并补正。取证基线 dev HEAD
f48bc9a2 → 纠偏提交 2101377a。期间 1d0acb66（qw13 盘点归档）整树提交时把这 4 个文件的取证订正整体
回滚，本轮以 patch 重落地为 49608199（同一提交保留 7827ba18 对 storage.yml 的无关 prefetch 理由行）。

处置与结论

1. 虚构锚点 1 处已改锚：tiered_collection_ops.rs:91 的 HashObjectImpl.cs:Set 在 C# 不存在，现按实读
   改为「HashObjectImpl.cs 内 HashSet 一族」的散文论证，1:1 符号锚挂
   wcol::hash::hash_object_impl::HashObject::hash_set（现 :89-90），check.js「# 虚构锚点」段清空。
2. 19 个实现缺失家族逐族核实：全部为登记缺口（rust 已有对位活件，或本仓无该面），无真引擎缺件，
   故未另立功能缺口单。逐族承接点与一句话理由登记在 js/check/ignore/{client,common,playground,
   server,storage}.yml 与 js/check/ignore/garnet/libs/cluster/Server/Replication/PrimaryOps/
   {AofOperations/AofSyncDriver,DisklessReplication/ReplicaSyncSession,PrimarySync}.yml。
   AllocatorBase 五件套由 whlog/wdev 自持形态承担、AofAddress/LogAddress 由 waof/whlog 地址类型承担、
   DoubleTurnstileBarrier 由 wcpr/wnode  bespoke 栅栏承担、ReplicaSyncSession/AofSyncDriver 在
   wedb/src/server/replication/diskless_replication/ 有同名活件——均已实测核对。
   载荷另删 4 份嵌套 wedb/js/check/ignore/ 重复挂载：check.js 的 IGNORE_DIR 固定为 js/check/ignore，
   嵌套路径从不被读（实测 dev 上已为 0 文件），属纯去重。
3. 唯一翻案（票面判为登记缺口、实为已活件）：libs/server/AOF/AofAddress.cs:IsOutOfRange 不登记，
   rust 已按 C# 对位实现为 waof/src/aof/address.rs:256 is_out_of_range，由 replication_manager.rs:887
   diskless_resync_strategy 经位点越界判据消费（any_lesser 表达不出区间包含）。
4. 本轮纠出前手 3 处虚假取证 + 1 处不精确表述并改正（2101377a，4 文件 25+/12-）：
   a) common.yml 称 DoubleTurnstileBarrier 两枚阻塞变体「C# 生产侧零消费」为假——真实生产消费者是
      libs/server/AOF/Recover/RecoverLogDriver.cs:153,159（恢复 leader 会合）与
      libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:134,142,273,274
      （页内并行回放会合）。理由改写为「两条阻塞链在 rust 均无承载面（rust 无页内并行回放对位，
      恢复链一律走 compio 异步变体），thread-per-core 下阻塞 Thread.Wait 形态无挂载位；形参超时面
      同 ProcessTimeSpan 口径」。同族顺手核实：MinExchange 零消费为真、AnyGreater 3 消费者为真。
   b) storage.yml 称 ProtectAndDrain 本体在 wepoch/src/epoch.rs 挂有本文件（Tsavorite LightEpoch）
      路径锚点为假——该面只按 C# 两份 LightEpoch 同语义挂 libs/client/LightEpoch.cs 一路
      （epoch.rs protect_and_drain）。
   c) server.yml 的 waof/src/aof/address.rs:253 已漂移，按当下 HEAD 重取为 :256。
   d) wedb/whlog/src/config.rs page_id 注释把 IAllocator / SpanByte / Object / TsavoriteLog 一律称作
      LogAddress.GetPageOfAddress「的一行委托」不精确（分别是接口声明与同式内联右移），改为在该
      函数文档并挂六处 C# 声明位（含 test.hlog/ScanIteratorEpochFailureTests.cs），纯注释改动。
5. 失效锚复核（不照抄票面）：wkv 确无 checkpoint.rs / read_cache.rs，真身为 wkv/src/store/cpr_host.rs
   与 wkv/src/read_cache/append.rs；但 wkv/src/session/raw/read.rs 真实存在，不是失效锚。本载荷只引用
   有效模块目录 wkv/src/read_cache，未踩失效锚。载荷无「以 std 替换自研」主张，故未跑 rustc 探针
   （本机 nightly 的 slice 没有 dedup / dedup_by，公网 1.60+ 常识在本机相反，涉 std 能力仍须先探针）。
   越界遗留：js/check/ignore/storage.yml:1748 仍指 wkv/src/read_cache.rs（init 提交遗留的陈旧指针，
   B 层提示、不参与门判定），不属本票射程。
6. 运行须知（后续动 ignore 表者必读）：bun js/check.js 每次都会以 yaml.stringify 重写
   js/check/ignore/server.yml，抹掉 # 取证书并裁掉「已被文档化」的条目；提交前须
   git checkout -- js/check/ignore/server.yml 复原，切勿把运行时变更当成果挂上。

验收（worktree 实测）

- bun js/check.js 退出码 0：输出仅「# 重复定义」与「# 实现缺失」两段，虚构锚点段与语料失效段均空；
  实现缺失段本票 19 族全清，只剩 libs/server/Lua/NativeMethods（由并发合并 429790c6
  zero-consumer-dead-surfaces-batch-five 引入，非本票家族，另计）。
- cargo check --workspace --all-targets 退出码 0、告警 0。
- 未新增 .rs 行为改动（config.rs 仅文档注释）；ignore 登记逐条附理由。
- 判据基准沿用本票立规：门只由 corpus_invalid 与 symbol_fail 决定，# 重复定义 收口归
  task/ing/cs-anchor-dup-single-mount.md，本票不碰。

---

以下为原票面（立项取证，行号已随 dev 漂移）

## 现状（两类门失败，逐条实测）

一、虚构锚点 1 处（A 层符号存在性断言失败，参与门判定）：
wedb/wnode/src/resp/objects/tiered_collection_ops.rs:91 挂 libs/server/Objects/Hash/HashObjectImpl.cs:Set，
该符号在 HashObjectImpl.cs 不存在，check 只给到 benchmark 族命中（ClusterMigrate.cs /
ClusterOperations.cs / RawStringOperations.cs）。该行属分层写臂「插成功才计数」判据的散文论证，
论点成立、锚点错挂。注意：tiered 命令臂覆盖面有分支在途（worktree tiered-command-arm-coverage、
tiered-list-lrange-full-scan），开工前须按当下 HEAD 重取该行号，命中他人改动即让位。

二、实现缺失 19 个家族（corpus 有 C# 类名、rust 侧无对位实现或无登记）：
libs/client/LightEpoch；libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver、
同目录 DisklessReplication/ReplicaSyncSession、PrimarySync；
libs/common/Synchronization/DoubleTurnstileBarrier；libs/server/AOF/AofAddress；
libs/storage/Tsavorite/cs/src/core/Allocator/{AllocatorBase,IAllocator,ObjectAllocator,SpanByteAllocator,TsavoriteLogAllocator}；
同 core/Epochs/LightEpoch；Index/Common/LogAddress；
Index/Tsavorite/Implementation/Locking/OverflowBucketLockTable；
test/test.epoch/ProtectionTests；test/test.hlog/ScanIteratorEpochFailureTests；
playground/LightEpochLitmus/helpers/BuggyLightEpoch。
这一族是历轮删死 API（task/done/zero-consumer-pub-surface-census.md 等）后锚点让位造成的登记缺口，
台账当时判为「非引擎缺件、待逐族甄别后再登记」，一直未做。AllocatorBase 五件套由 rust
whlog/wdev 自持形态承担（模块头已写明对位），AofAddress/LogAddress 由 waof/whlog 地址类型承担，
DoubleTurnstileBarrier 由 wcpr 屏障承担，ReplicaSyncSession/AofSyncDriver 在
wedb/wedb/src/server/replication/diskless_replication/ 有同名活件——须逐族核实是登记缺口还是真缺件，
再分流处置。

判据基准（先立规矩）：门只由 corpus_invalid 与 symbol_fail 决定（js/check.js:479-485），
# 重复定义 走信息节不参与判定，其收口手法归 task/ing/cs-anchor-dup-single-mount.md，本单不碰；
ignore 表对重复定义无效（dupDefFind 只扫函数文档注释里的路径.cs:符号对，js/check.js:306-338），
禁写 C# 不存在的符号名来拆组（js/check.js:466-477 硬失败）。

C# 参考

garnet/libs/storage/Tsavorite/cs/src/core/Allocator/AllocatorBase.cs（2839 行单体，rust 无 1:1 件）
garnet/libs/storage/Tsavorite/cs/src/core/Epochs/LightEpoch.cs
garnet/libs/cluster/Server/Replication/PrimaryOps/AofOperations/AofSyncDriver.cs
garnet/libs/cluster/Server/Replication/PrimaryOps/DisklessReplication/ReplicaSyncSession.cs
libs/server/Auth/Settings/AuthenticationSettings.cs 同族 ignore 登记先例见
js/check/ignore/storage.yml:2（AllocatorBase 块）

修法

1. 改锚：tiered_collection_ops.rs:91 的符号位换成 C# 真实落点（该断言讲的是「内存对象插入无失败」，
   对位应为 HashObject 侧写入方法，以实读 garnet/libs/server/Objects/Hash/ 为准），或按仓内先例改散文表述
   不再挂符号（先例 wedb/wbase/src/align.rs:115-117）。
2. 逐族甄别 19 个实现缺失家族：真缺件的立功能缺口单（本单不代做实现）；属形态差异或本仓无该面的，
   在 js/check/ignore/ 对应 yml 登记理由一句话，禁改 .rs 绕过。
3. 全族收口后重跑门，确认两类段均空。

优先级：污染扩散（门红使所有回归判定失去基线，失真锚点会把后续转写会话引向不存在的符号）。

验收

bun js/check.js 退出码 0，实现缺失段与虚构锚点段均空；重复定义段变化不属本单射程；
不新增 .rs 行为改动（第 1 步仅注释），js/check/ignore 登记逐条附理由。

---

## 二棒收口（fixloop 收尸二棒，取证基线 dev 7560a76e）

一棒死在「在工作树里执行合并」这一步。其分支 gate-anchor-drift-reclean 实测 ahead=0
（`git rev-list --left-right --count dev...gate-anchor-drift-reclean` = 99/0），死树
/tmp/fork/salvage-gate 里 4 个文件停着与 49608199 逐字节相同的暂存件（19/4/4/10，25+/12-），
即那批纠偏早已导出并入 dev，死树无未导出成果。二棒以 dev HEAD 另起 worktree salvage-gate2，
不沿用任何陈旧树。

### 一、三态清单（逐条自验，不照抄票面）

已落地（dev 实测取证）

1. 虚构锚点改锚：wnode/src/resp/objects/tiered_collection_ops.rs:89-90 现为「HashObjectImpl.cs 内
   HashSet 一族」散文论证，1:1 符号锚 HashObjectImpl.cs:HashSet 挂在
   wcol/src/hash/hash_object_impl.rs:247（fn hash_set 在 :248；tiered_collection_ops.rs:629 的同行
   号是行内注释，dupDefFind 只扫函数文档注释，故不触发重复定义），check.js
   「# 虚构锚点」段空、报告内该符号单挂载。
2. 19 个实现缺失家族：登记分散在 js/check/ignore/{client,common,playground,server,storage,test}.yml
   与 js/check/ignore/garnet/libs/cluster/Server/Replication/PrimaryOps/…（实测 grep 逐族命中）；
   其中 LogAddress.cs 与 PrimarySync.cs 两族不走 ignore，而由 rust 文档注释登记为「已文档化」——
   实测 windex/src/entry.rs:96、whlog/src/config.rs:161,:186（LogAddress.cs）与
   cluster_session/replication.rs:531、replication/replica_diskless_sync.rs:217（PrimarySync.cs）。
   本票 19 族在 check.js「# 实现缺失」段全清，该段只剩 libs/server/Lua/NativeMethods（非本票家族）。
3. 49608199 的四处纠偏与票归档 735466ae 均已在 dev（`git merge-base --is-ancestor` 三条皆真）。
4. 一棒记为「越界遗留」的一处：js/check/ignore/storage.yml ReadCache.cs:SpinWaitUntilRecordIsClosed
   条目理由指 wkv/src/read_cache.rs —— 二棒判为本票射程内的登记语料失效锚，已落地（见下）。

未落地且成立 → 二棒落（同一条提交 7560a76e）

5. storage.yml 上述死路径：实测 wkv/src 下无 checkpoint.rs 亦无 read_cache.rs
   （`ls wedb/wkv/src/` + `find wedb/wkv -name 'read_cache*'`），真身为目录
   wkv/src/read_cache/{append,cleanse,mod,window}.rs；条目所登记的自旋关闭面具体对位在
   read_cache/window.rs:135 spin_wait_until_record_is_closed（append.rs:39 append 对位的
   是 TryCopyToReadCache，非本条目），开关通道 StoreConfig::enable_read_cache 在 wkv/src/config.rs:228。
6. 49608199 的三处纠偏被并发整树提交反复抹回，二棒按 merge-sha 逐件重放：
   a) common.yml「DoubleTurnstileBarrier 两枚阻塞变体在 C# 生产侧零消费」为假，由 21a4bcc9 抹回。
      实测反证：garnet grep SignalWork(Ready|Completed)Wait 非定义处命中
      libs/server/AOF/Recover/RecoverLogDriver.cs:153,159 与
      libs/cluster/Server/Replication/ReplicaOps/AOFReplay/ReplicaReplayDriver.cs:134,142,273,274；
      异步变体消费点 RecoverReplayTask.cs:22,69、ReplicaReplayTask.cs:44,135；
      测试件 test/standalone/Garnet.test/DoubleTurnstileBarrierTests.cs 在 test.yml:1974 整件登记；
      rust 侧 recover_log_driver.rs 只调异步变体（:155,:179,:184,:218,:221），
      两枚异步变体在 double_turnstile_barrier.rs:88,:97 挂锚。
   b) server.yml 的 AofAddress 比较/交换面取证块整块被 21a4bcc9 抹除（连 :256 的重取行号一并丢）。
      实测：MinExchange 全仓仅定义行 AofAddress.cs:300；AnyGreater 三消费点
      DatabaseManagerBase.cs:514 / ReplicationPrimaryAofSync.cs:58 / ReplicaFailoverSession.cs:94；
      waof/src/aof/address.rs 的 is_out_of_range 在 :256（文档注释 :253），消费点
      replication_manager.rs:887 diskless_resync_strategy（调用在 :900）。
   c) storage.yml 的 LightEpoch 块被抹回「ProtectAndDrain 本体亦挂本文件路径锚点」。
      实测 wepoch/src/epoch.rs:413 protect_and_drain 只挂 libs/client/LightEpoch.cs 一路，
      双挂的是 :282 Resume / :331 Suspend / :360 TrySuspend。
   附带：whlog/src/config.rs page_id 的六处 GetPageOfAddress 声明位文档未被回滚（实测仍在）。

判为不成立 / 不属本票

7. 票面「raw/read.rs 是失效锚」不成立：wedb/wkv/src/session/raw/read.rs 实测存在，不动。
8. 登记语料内另有 11 处 rust 路径级失效锚（js/check/ignore 全量扫描脚本实测）：
   client.yml:173 wconn/src/metrics.rs、common.yml:251 wresp/src/output.rs、
   garnet/libs/server/Config/ConfigNameComparer.yml:5 与 Device/{DeviceOptions,DeviceType}.yml
   的 wconf/{config_name_comparer,device_config}.rs、…/LogCommitPolicy.yml:12 wal/flush.rs、
   storage.yml:2097 wreviv/src/stack.rs、storage.yml:2677 wdev/src/null.rs、
   test.yml:1865 wnode/tests/task_manager_tests.rs —— 除下列外均为「已整件删除」的过去式证词，
   指向不存在的文件恰是其论点本身，不算漂移。真正需要跟进的是
   storage.yml:2490 与 test.yml:1267 的 wkv/tests/garnet_object_tests.rs：该路径从未在 wkv/tests
   下存在过（init 时代在 wnode/tests 与 wedb_standalone/tests，9e5bb6df 收敛 wobject→wcol 后
   对象信封覆盖已在 wcol/tests/object_serialize_expiration_tests.rs），是活覆盖声明却指向死文件，
   属 wobject 收敛批次的登记残留，另计不并单。

### 二、门禁与负向验证（全部在 worktree salvage-gate2 内跑，主仓未跑 check.js）

- cargo check --workspace --all-targets（CARGO_TARGET_DIR=<worktree>/target，禁共享）：
  基线（合入前 dev HEAD）exit 0 / 编译告警 0；落地后复跑 exit 0 / 告警 0 / 错误 0。
- bun js/check.js：exit 0，「# 虚构锚点」与语料失效段均空，「# 实现缺失」段仅
  libs/server/Lua/NativeMethods；改动前后 stdout+stderr 逐字节相同（diff 无输出）。
  注：门退出码只由 corpus_invalid 与 symbol_fail 决定（本票立的判据基准），
  「实现缺失」段本身不改 exit 码，故负向验证看段内容不看 exit 码。
- 负向验证（撤登记必须转红才算登记真生效）：临时摘掉 common.yml 的 DoubleTurnstileBarrier
  条目块（连取证注释 17 行）重跑 → 「# 实现缺失」段立即多出
  libs/common/Synchronization/DoubleTurnstileBarrier，且 js/check/miss/ 与 ROOT check/miss/
  两处落盘 DoubleTurnstileBarrier.yml；把登记装回再跑 → 报告回到基线逐字节相同。
  判为登记确实在生效。
- yml 形状：改动只动 理由/注释文本，条目数不变（common 17 / server 84 / storage 234 / hosting 3），
  yaml.parse 全通过（防「一份非法 YAML → 整份语料静默失效」）。
- 运行须知复测：check.js 在陈旧基线上确实会以 prune 为由重写 server.yml（实测抹掉 AofAddress
  取证块并裁 TryReadInt），二棒每轮跑完即 `git checkout -- js/check/ignore/` 复原，
  未把运行时变更当成果挂上。

### 三、事故与自纠（须引以为戒）

- 二棒一次提交（03540706）把 dev@35f82050 的陈旧整树以 d5c01301 为父挂上 dev，
  该提交随后被并发合并继承，抹回他人 12 个路径（hosting.yml 的 checkpoint-throttle-delay
  五级链登记、storage.yml 三处快照节流证词、wcol/wnode 六件 zset 与对象共享面、
  三份票的 task/done 归档位）。实测 dev tip 上 12 路径当时仍等值于 03540706 且无人在其上
  另作改动，遂 CAS 撤销该提交回 d5c01301，再以 d5c01301 为源逐路径还原，与残差重落地并入
  7560a76e（`git diff d5c01301 dev -- <12 路径>` 现为空）。教训：commit-tree 前必须
  重读 dev tip 并让 write-tree 的基与 -p 的父同刻取，否则树会落后于父。
- 主仓 index 里他人未提交的暂存件（js/check/ignore/{common,server,storage}.yml、
  wedb/whlog/src/config.rs、replication_sync_manager.rs、diskless_sync_anchor_window.rs、
  slot_mgmt.rs、cluster_resp_session.rs、cluster_cmd_strings.rs 等）实测内容等值于
  49608199 之前 / 批五删死面之前的旧文，一旦整树提交就会再抹掉本票纠偏与快照锚定改动。
  二棒只读不提交，一条未 add、一条未 commit；本票文档外未提交任何他人路径。
