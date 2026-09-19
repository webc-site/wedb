# C# 锚点重复挂载单点化（承接 next/qcode8.gate.md 条 3 与丙类撤销段，该文件已核销删除）

## 判据基准（先立规矩再改锚）

check.js 的门只由 corpus_invalid 与 symbol_fail 决定（js/check.js:479-485），`# 重复定义` 走信息节
（js/check.js:439-447），不参与失败判定。清理目的是消噪与防锚点失真扩散，不是压组数：

1. ignore 表对重复定义无效——dupDefFind 只扫函数文档注释里的 `路径.cs:符号` 对
   （js/check.js:306-338），完全不读 ignore 集（js/check.js:445），塞 ignore 消不掉重复组。
2. 禁写 C# 不存在的符号名（如 KeyDeleteAsyncMulti）来拆组：A 层符号存在性断言为硬失败
   （js/check.js:466-477）。
3. 合法收口形态是仓内既有先例：一处挂载 + 其余位点散文书写并标「此处不复挂」，见
   wedb/wbase/src/align.rs:115-117 与 wedb/wdev/src/device.rs:311-313。

## 现状（主仓 HEAD 实测 18 组，原报 13 组中五组已落地）

已落地、不再列入本票：object_store_utils.rs 的 parse_elements_header 双挂（现为
wedb/wnode/src/resp/objects/object_store_utils.rs:115-121 散文表述）、waof/src/wal/log.rs 的
has_nonzero_after 挂 AofRecover.cs:Recover（现为 wedb/waof/src/wal/log.rs:353-355 散文）、
NetworkSET_Conditional 与 RESP_ERR_WRONG_TYPE 的测试侧共挂（唯一挂载在
wedb/wnode/src/resp/basic_commands/set.rs:435 与 wedb/wresp/src/cmd_strings.rs:67）、
AllocatorBase.cs:ShiftBeginAddress 与 WriteInlinePageAsync 跨层共挂（各留单点：
wedb/wkv/src/store/addr.rs:153、wedb/wdev/src/device.rs:98）。

待处置分四类：

甲 客户端真重载共挂（4 组，登记粒度问题）：KeyDeleteAsync（wedb/wconn/src/api.rs:51 与 :58）、
StringGetAsync（:30 与 :35）、StringIncrement（:66 与 :72）、StringDecrement（:82 与 :88）。
C# 侧确为同名重载族：garnet/libs/client/GarnetClientAPI/GarnetClientBasicRespCommands.cs:62、
:70、:156、:165（StringGetAsync 四重载）与 :252、:261、:269、:278、:308、:318、:329、:337
（KeyDeleteAsync 八重载）。

丙 门面误挂（1 组）：wedb/wedb/src/client.rs:15 的结构体 doc 挂
libs/client/GarnetClient.cs:GarnetClient，与真正转写主体 wedb/wconn/src/client.rs:46 共挂。
wedb 侧实为集群控制面 facade：wedb/wedb/src/client.rs:6 `use wconn::client::GarnetClient as
ConnClient`、:27 `inner: RwLock<Option<Arc<ConnClient>>>`，全部协议调用委托底层会话。

丁 RangeIndex 治愈内核与位变更器共挂（6 组，同文件内自我重复）：内核
wedb/wkv/src/range_index/stub.rs:542-547 的 doc 一次挂 SetFlushedFlag、ClearFlushedFlag、
InvalidateStub、MarkRecoveredFromCheckpoint 四符号，与位变更器 :586（clear_flushed_patch）、
:600（mark_recovered_patch）、:614（recreate_patch）、:628-629（transfer_out_patch）、
:353-354（post_promote_tail_patch）以及 wbftree 侧 wedb/wbftree/src/stub.rs:151、:158 撞组。

戊 原报未列的新增组（7 组，按下面两条判据逐组分类，不预设结论）：
- 疑真平行实现（判「下沉单点」，属重复/多套架构，本票内优先级最高）：
  RespMemoryWriter.cs:WriteDoubleNumeric（wedb/wcol/src/resp/output.rs:93 与
  wedb/wresp/src/cmd_strings.rs:498）、RespServerSessionOutput.cs:WriteNull
  （wedb/wresp/src/cmd_strings.rs:487、wedb/wnode/src/resp/objects/list_commands/blocking.rs:447、
  wedb/wnode/src/resp/basic_etag_commands.rs:55，与 null 帧单源同域，收口口径随该票一并落地，
  本票不另起第二套 null 常量）、ExpirationWithOption.cs:ExpirationWithOption
  （wedb/wbase/src/convert.rs:187 与 wedb/wresp/src/options.rs:134）。
- 疑分层错位（判「按职责分开锚点」）：ClusterProvider.cs:GetCheckpointInfo
  （wedb/wnode/src/resp/info_provider.rs:186 与 wedb/wedb/src/server/cluster_provider.rs:1558）、
  GetGossipStats（:157 与 :1579）、GarnetInfoMetrics.cs:PopulateReplicationInfo
  （wedb/wnode/src/resp/info_provider.rs:149 与 wedb/wmetric/src/info/garnet_info_metrics.rs:491）、
  ReplicationSyncManager.cs:AddReplicaSyncSession
  （wedb/wedb/src/server/replication/diskless_replication/replica_sync_session.rs:79 与
  wedb/wedb/src/server/replication/diskless_replication/replication_sync_manager.rs:76）。

## 方案

1. 甲：单键臂保留挂载（api.rs:30/:51/:66/:82），多键臂改散文对位并注明 C# 是同名重载、
   登记粒度按符号名，句末标「此处不复挂」，形态照抄 wedb/wbase/src/align.rs:115-117。
2. 丙：wedb/wedb/src/client.rs:15 的 doc 改指其真实 C# 对位（libs/cluster/Server/Gossip/
   GarnetClientExtensions.cs 族的调用方形态，取该文件中确实存在的符号名），或按同散文形态
   声明「C# GarnetClient 主体挂载在 wconn::client::GarnetClient，此处为 facade，不复挂」。
   主体挂载不动。
3. 丁：内核 doc 只保留确无他处挂载的符号（SetFlushedFlag、InvalidateStub 若仍唯一），
   ClearFlushedFlag/MarkRecoveredFromCheckpoint/RecreateIndex/ClearTreeHandle/SetTransferredFlag
   五枚一律改「in-span 单点变更器族」散文表述；1:1 挂载留在其转写主体——wkv 位变更器
   （:586/:600/:614/:628）与 wbftree 侧（stub.rs:151/:158）二者取一，按谁承载 C# 语义主体裁决
   （倾向留 wbftree 的 RangeIndexStub 方法，wkv 位变更器改散文，因其只是传给内核的闭包）。
4. 戊：逐组按上面两条判据分类后落 1-3 的同一手法；判为真平行实现者，先下沉单点再收锚，
   禁止只改注释。
5. 全程不得删合法共挂来凑数，不得新增 ignore 条目来掩盖。

## 验收

1. 复扫重复组数下降，且 `# 虚构锚点`（A 层）与 `# 实现缺失` 两类保持为 0。现 HEAD 该两类非空：
   唯一一条虚构锚点是 wedb/wnode/src/resp/objects/tiered_collection_ops.rs:91 挂的
   libs/server/Objects/Hash/HashObjectImpl.cs:Set（该 C# 文件的插入实现名为 HashSet，:185，无 Set
   符号），其改锚归 task/ing/gate-anchor-drift-reclean.md 条 一，本票不重复处置，只在其落地后复核
   保持为 0；同票亦管「实现缺失」逐族甄别。
2. 每个 `路径.cs:符号` 在函数 doc 面至多一处挂载；散文位点显式写明主体挂载所在。
3. 若第 4 步出现真实代码收敛（WriteNull/WriteDoubleNumeric/ExpirationWithOption 任一下沉单点），
   对应消费点一并转调，不留第二套实现。

优先级：污染扩散（锚点登记面失真扩散，并直接影响 check.js 甄别信噪），改动以注释与少量
下沉为主，量级小。

## 落地结论（合入 dev 6a556e5 后回填）

甄别成立。认领基线（worktree HEAD a7402c4）实测 **23 组**，非本票据所记 18 组：差额 5 组为
取证时点后 dev 上同形新撞共挂，一并按戊段「逐组甄别、不预设结论」口径处置，未照抄分类。
手法一律为仓内既有先例（一处挂载 + 其余位点散文明写主体所在，形态照
/Users/z/git/db/wedb/wedb/wbase/src/align.rs:117 与 /Users/z/git/db/wedb/wedb/wdev/src/device.rs:313）。

### 本票两处倾向被代码事实推翻（按实况反向处置）

1. 丁段「倾向留 wbftree 的 RangeIndexStub 方法」不成立，锚点留在 wkv 位变更器。C# 五枚变更器
   是 garnet/libs/server/Resp/RangeIndex/RangeIndexManager.Index.cs:47
   `internal struct RangeIndexStub` 之上的 in-span 静态方法（:184/:201/:218/:347/:360/:374/:388，
   一律 `ref var stub = ref Unsafe.As<byte, RangeIndexStub>(ref valueSpan[0])` 就地改位），
   语义主体是「持有值域的位变更」，rust 承载者即
   /Users/z/git/db/wedb/wedb/wkv/src/range_index/stub.rs:581 clear_flushed_patch、
   :595 mark_recovered_patch、:608 recreate_patch、:622 transfer_out_patch；
   wbftree 侧 /Users/z/git/db/wedb/wedb/wbftree/src/stub.rs:154/:164 只是字段位 setter，
   改散文指回不挂锚点。治愈内核 doc 亦只保留 C# 侧无独立 rust 位变更器的两枚
   （SetFlushedFlag / InvalidateStub，/Users/z/git/db/wedb/wedb/wkv/src/range_index/stub.rs:532-533），
   编排点 /Users/z/git/db/wedb/wedb/wkv/src/range_index/stub.rs:344 去挂。
2. 戊段「ExpirationWithOption 疑真平行实现」不成立，故不下沉代码：C# 粗化只一处口径
   （garnet/libs/server/ExpirationWithOption.cs 构造器 `(ticks >> 4) << 4`，键级 EXPIRE 亦经该
   构造器），rust 侧 /Users/z/git/db/wedb/wedb/wbase/src/convert.rs:170 的 coarse_expire_ticks
   是键级不落 option 位时的粗化单点，与
   /Users/z/git/db/wedb/wedb/wresp/src/options.rs:132 的带 option 打包臂不构成两套实现，
   只去重挂载、不新增跨 crate 依赖。

### 真代码收敛（全票唯一一处，被替代旧实现直删、无兼容层）

- /Users/z/git/db/wedb/wedb/wcol/src/resp/output.rs:97：本 crate 内第二份 RESP2/RESP3 二选一
  分派体删除，转调 /Users/z/git/db/wedb/wedb/wresp/src/cmd_strings.rs:492 write_double_numeric
  既有单点；`RespMemoryWriter.cs:WriteDoubleNumeric` 锚点现全仓仅此一处（cmd_strings.rs:489）。
  适配口保留是因为同文件 write_null/write_map_length/write_set_length 同形（ObjectOutput
  负载到 sink 的形态转换），非二次导出。

### 散文收口位点（12 文件 18 处去挂）

- 甲 重载族 4：/Users/z/git/db/wedb/wedb/wconn/src/api.rs:36、:61、:76、:93（主体单键臂
  :29/:52/:69/:86 挂载不动）
- 丙 facade 2：/Users/z/git/db/wedb/wedb/wedb/src/client.rs:19、:51（主体在
  /Users/z/git/db/wedb/wedb/wconn/src/client.rs:39）
- 戊 分层错位 6：/Users/z/git/db/wedb/wedb/wedb/wnode/src/resp/info_provider.rs:149、:159、:189
  （主体分别在 /Users/z/git/db/wedb/wedb/wmetric/src/info/garnet_info_metrics.rs:489、
  /Users/z/git/db/wedb/wedb/wedb/src/server/cluster_provider.rs:1607、:1586）、
  /Users/z/git/db/wedb/wedb/wedb/src/server/replication/diskless_replication/replica_sync_session.rs:80
  （主体 replication_sync_manager.rs:75）、
  /Users/z/git/db/wedb/wedb/wmetric/src/garnet_session_metrics.rs:221（主体 :74 值语义 reset）、
  /Users/z/git/db/wedb/wedb/wkv/src/store/cpr_host.rs:133（主体扫描内核
  /Users/z/git/db/wedb/wedb/wcpr/src/manager/recover.rs:287 run_recovery_kernel）
- 其余 6：/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/tiered_collection_ops.rs:93
  （主体 /Users/z/git/db/wedb/wedb/wcol/src/hash/hash_object_impl.rs:247）、
  /Users/z/git/db/wedb/wedb/wnode/src/resp/resp_session_consumer.rs:267（主体为 trait 契约面
  /Users/z/git/db/wedb/wedb/wnode/src/traits.rs:136）、
  /Users/z/git/db/wedb/wedb/wedb/tests/cluster_migration.rs:3056（主体
  /Users/z/git/db/wedb/wedb/wedb/src/server/migration/migrate_driver/slots.rs:64）、
  以及上述 /Users/z/git/db/wedb/wedb/wbase/src/convert.rs:170、
  /Users/z/git/db/wedb/wedb/wkv/src/range_index/stub.rs:344

### 验收（本轮实测）

- 重复组数：23 → 2。两组均非本票基线可动项：
  1. `RespServerSessionOutput.cs:WriteNull`（/Users/z/git/db/wedb/wedb/wresp/src/cmd_strings.rs:478、
     /Users/z/git/db/wedb/wedb/wbftree 无关：/Users/z/git/db/wedb/wedb/wnode/src/resp/basic_etag_commands.rs:51、
     /Users/z/git/db/wedb/wedb/wbftree 无关：/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/list_commands/blocking.rs:444）
     —— 票据正文已声明「收口口径随 null 单源票一并落地，本票不另起第二套 null 常量」，
     故未触碰，归 /Users/z/git/db/wedb/task/ing/resp-null-protocol-single-source.md。
  2. `libs/server/Lua/LuaRunner.Functions.cs:ProcessCommandFromScripting`
     （/Users/z/git/db/wedb/wedb/wlua/src/functions/redis.rs:326、:403、:503）—— 认领基线
     a7402c4 该文件此锚点仅 1 处，第 2、3 处由本票在途期间落地 dev 的 5d35541
     「fix: redis.call SET/GET 快路径补 number 形参臂」新增，不在本票基线；且 wlua 有在途票
     /Users/z/git/db/wedb/task/ing/lua-call-pending-suspend-handoff.md，按「不碰他人在途文件」
     移交下一波复扫（手法即本票散文收口：两枚快路径臂是主体分派的内联片段，锚点留 :503）。
- A 层 `# 虚构锚点`：0（票据点名的 HashObjectImpl.cs:Set 已由
  /Users/z/git/db/wedb/next/gate-anchor-drift-reclean.md 一条在 dev 上处置，本票复核保持为 0；
  本票新增散文一律用真实符号名 HashSet，未造名）。
- 锚点登记面零丢失：逐枚 grep 复核被去挂的 `路径.cs:符号` 在其唯一挂载点仍以带冒号形态登记
  （StringGetAsync/KeyDeleteAsync/StringIncrement/StringDecrement 见 api.rs:29/:52/:69/:86，
  ClearFlushedFlag/MarkRecoveredFromCheckpoint/RecreateIndex/ClearTreeHandle/SetTransferredFlag
  见 wkv/src/range_index/stub.rs:576/:590/:604/:618-619，余枚同上），
  故 `# 实现缺失` 集合不因本票新增条目。
- 门禁：`bun js/check.js` exit 0（worktree 内跑，跑后 `git status --porcelain` 为空，语料零改写）；
  `cargo check --workspace --all-targets` exit 0、零 warning（worktree 私有 target
  /tmp/fork/cs-anchor-dup-single-mount/target）。未跑 test.sh / clippy.sh（本票约束）。
- 量级：13 文件 +86/-50，其中唯一代码改动为 wcol/src/resp/output.rs 的分派体删除与转调。
