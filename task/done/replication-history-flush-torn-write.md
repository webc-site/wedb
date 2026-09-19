ReplicationHistory 落盘无互斥：两个并发 flush 同写固定 .tmp 路径可交错成混合体，C# FlushConfig 的 lock(this) 在 rust 缺位

来源：next/glm.net.md 条 6（该文件已分拣清空删除）。逐句按主仓当下代码与 C# 复核后判定成立待做。
取证基线：主仓 /Users/z/git/db/wedb，行号按符号定位。
载体唯一性：并发拆条在 next/ 下另留了一份本条原文照抄的壳（basename
replication-history-flush-mutex.md，只加「优先级：高」头、无取证订正），
以本文件为唯一载体，派单前先剪壳勿双花。

结论

C# 的 FlushConfig 用 `lock (this)` 把「序列化 + 写设备」整段互斥，任一时刻设备上都是完整版本。
rust 的三个更新入口都是「取写锁改内存 → 显式 drop 写锁 → flush_config」，而 flush_config 内部
只取读锁落盘；parking_lot 读锁不互斥，于是两个并发更新可以同时进入 flush_to_file，
而 tmp 文件名固定（`path.with_extension("tmp")`），两个 `File::create` 互相截断、各自从自己的
offset 追加，后 rename 上去的可能是两个版本的字节混合体。下次重启解析失败即走「损坏重建」
换新 replid，全部副本 replid 失配退全量重同步。C# 的损坏分支只兜意外崩溃，rust 现在把
「并发落盘」也变成了可触发入口。

现状

- 落盘面持读锁：/Users/z/git/db/wedb/wedb/wedb/src/server/replication/replication_manager.rs:220-227
  `flush_config` 在 `if let Some(ref path) = self.config_path` 内 `let config =
  self.current_replication_config.read();` 后 :223 直接 `config.flush_to_file(path)`，
  读锁覆盖整个 IO。字段声明 :71 `pub current_replication_config: RwLock<ReplicationHistory>`，
  :16 导入的是 `parking_lot::{Mutex, RwLock}` —— 共享读、不互斥。
- 三个入口都在落盘前放开写锁：:201-206 `initialize_replication_history`（:204 `drop(config)` 后
  :205 flush）、:230-235 `try_update_my_primary_repl_id`（:233 drop、:234 flush）、
  :572-581 `try_update_for_failover`（:578 drop、:579 flush）。三处 flush_config 之间没有任何
  落盘互斥。
- 固定 tmp 路径：/Users/z/git/db/wedb/wedb/wedb/src/server/replication/replication_history.rs:100-113
  `flush_to_file` 依次 `let tmp_path = path.with_extension("tmp")`（:105）、
  `File::create(&tmp_path)`（:107，截断语义）、`write_all` + `sync_all`、`fs::rename`（:111）。
  两个并发调用写同一个 tmp inode，rename 竞态同样落在同一路径上。
- 损坏后果面：同文件 :116-127 `recover_or_init` 读文件 → `from_byte_array` 失败即
  `Self::new(..)` 重建并回写，replid 随 `create_hex_id()`（:94 `failover_update` 同源）换新，
  副本侧 replid 全部失配退全量。
- 并发窗口真实存在（不同连接任务可同时触达）：
  `try_update_my_primary_repl_id` 由副本恢复命令面调用
  （/Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_diskbased_sync.rs:171、
  /Users/z/git/db/wedb/wedb/wedb/src/server/replication/replica_diskless_sync.rs:203）；
  `try_update_for_failover` 由 /Users/z/git/db/wedb/wedb/wedb/src/server/cluster_session/replica_of.rs:42
  （REPLICAOF NO ONE）与 /Users/z/git/db/wedb/wedb/wedb/src/server/failover/replica_failover_session.rs:198
  （failover 收敛段）调用。集群震荡时「failover 正在推进」与「另一节点的副本 attach 恢复」
  确实可以交叠。
- 澄清不是问题的面：内容倒退竞态不存在——flush_config 现读现写，读到的必是当时最新版；
  本单只有交错损坏一个面。

C# 参考

- /Users/z/git/db/wedb/garnet/libs/cluster/Server/Replication/ReplicationHistoryManager.cs:162-170
  `FlushConfig()`：`lock (this)` 内 LogTrace + `ClusterUtils.WriteInto(..., currentReplicationConfig.ToByteArray(), ...)`
  + 收尾 LogTrace，序列化与写设备整段互斥。
- :133-143 `TryUpdateMyPrimaryReplId`、:148-160 `TryUpdateForFailover`：CAS 循环换 `currentReplicationConfig`
  后调 FlushConfig（互斥由 FlushConfig 自身承担，与 rust 的「写锁先释再落盘」结构同形，
  差别就在 C# 落盘段有锁）。
- :113-118 `InitializeReplicationHistory`、:119-131 `RecoverReplicationHistory`
  （catch InvalidDataException/EndOfStreamException/IOException 即重建，对应 rust 的损坏分支）。

修法

1. 首选补齐 C# 语义：`ReplicationManager` 增一枚落盘锁字段（`parking_lot::Mutex<()>` 已在
   :16 的导入面内，无需新依赖），`flush_config`（replication_manager.rs:220-227）在
   `config_path` 判定之后先取该锁、再在读锁内取字节、最后在持锁期间 `flush_to_file`。
   要点是把 `to_byte_array()` 的序列化也留在锁内（对齐 C# 整段互斥），避免「先算字节后抢锁」
   导致两个版本以任意序落盘；读锁只用于取当前值拷贝，落盘 IO 的串行性由这把专用锁保证，
   不再靠读锁承担互斥语义。函数文档注释保持挂
   `libs/cluster/Server/Replication/ReplicationHistoryManager.cs:FlushConfig`。
2. 三个入口的 `drop(config); self.flush_config();`（:204-205、:233-234、:578-579）保持现结构，
   不改回「持写锁落盘」：那会让 replication.conf 的 IO 阻塞所有读侧 replid 判定，
   而 C# 也没有这么干。
3. 第二道防线（可与第 1 步同时做，也可作为不引新字段的替代）：
   /Users/z/git/db/wedb/wedb/wedb/src/server/replication/replication_history.rs:100-113 的
   tmp 名改成进程与调用唯一（pid + 原子计数后缀，落 `path.with_extension` 之后拼），
   rename 目标恒为 `path`。这样即便将来出现新的并发落盘面也不会互踩；
   但只有第 2 步不可替代的语义是「落盘顺序与内存版本顺序一致」，故第 1 步是主修，
   唯一 tmp 名是补强，二者不冲突。
4. 不做的形态：不加「重试三次」式的损坏自愈、不加向下兼容读旧格式；
   `recover_or_init` 的损坏重建分支保持 C# 的兜崩溃语义即可，本单的目标正是让它重新只兜崩溃。
5. 测试：单测层面在
   /Users/z/git/db/wedb/wedb/wedb/src/server/replication/replication_history.rs:130 起的
   tests 模块补一条「两个不同版本并发 flush_to_file 同一目录，最终文件可被 from_byte_array
   解出且等于其中某一个完整版本」的用例；集成面在 /Users/z/git/db/wedb/wedb/wedb/tests
   现有复制用例（replication_manager.rs 用例集 /Users/z/git/db/wedb/wedb/wedb/tests/replication_manager.rs）
   补 failover 与 replid 更新并发的两条入口。别写只能自证的 sleep 竞态用例，
   判据是「落盘字节恒为某单一完整版本」。

优先级

功能缺口（并发正确性缺陷，触发后果是全量重同步，代价高），类别上排在死代码、重复架构、
污染扩散之后，但本仓并发类缺口的实际处置顺序应靠前，因为它无法靠读代码在后续会话里被发现。

交叉引用

- 同一 attach 链上的恢复锁窗口问题是另一单：task/ing/replica-attach-recovery-lock-window.md。
  两单同域文件（replication_manager.rs / replica_diskbased_sync.rs / assembly.rs）若同批开工，
  共用一次改动窗口，判定各自独立：那单改状态机时序，本单只加落盘互斥与 tmp 命名，不重叠。
- 副本位点语义与 resync 策略相关判定见 task/ing/resync-strategy-store-version.md，
  本单不改位点判定逻辑。

落地记录（分支 fix-replhist，基线 dev 366015a）

- 甄别复验（行号按当下符号定位，「现状」段全部成立，无人在先修）：
  flush_config 持读锁跨 IO :220-227、字段 :71 `RwLock<ReplicationHistory>`、
  :16 导入 `parking_lot::{Mutex, RwLock}`；三入口 drop→flush :204-205 /
  :233-234 / :578-579；固定 tmp 名 replication_history.rs:105 + :107 `File::create`
  截断 + :111 rename；损坏后果 replication_history.rs:116-127 换 replid
  （:94 `create_hex_id` 同源）；四个并发入口逐一核过，各属独立任务/线程：
  replica_diskbased_sync.rs:171 与 replica_diskless_sync.rs:203（副本 attach 恢复）、
  cluster_session/replica_of.rs:42（REPLICAOF NO ONE 会话）、
  failover/replica_failover_session.rs:198（failover 收敛段）。
  C# 侧复核：ReplicationHistoryManager.cs:162-170 `FlushConfig` 确为 `lock (this)`
  包住「ToByteArray 序列化 + WriteInto」整段，:133-160 两更新入口 CAS 后调
  FlushConfig（互斥由落盘口自承），与本单修法同形。
- 落地（修法第 1、2 条）：
  - replication_manager.rs:72-74 新增私有字段 `history_flush_lock: Mutex<()>`
    （parking_lot::Mutex 已在 :16 导入面内，无新依赖），:160 构造期装配，
    与同仓既有先例 ClusterManager::flush_disk_lock（cluster_manager.rs:120）同构。
  - replication_manager.rs:223-238 flush_config 改为「config_path 判定 → 占落盘锁
    → 锁内 `.read().copy()` 取当前历史（读锁不跨 IO）→ 持锁 flush_to_file」，
    即锁覆盖 check→序列化→写 tmp→rename 全程；后到者必待前者 rename 完成后才取值
    落盘，故设备上恒为某单一完整版本、落盘序与内存版本序一致。文档注释仍挂
    libs/cluster/Server/Replication/ReplicationHistoryManager.cs:FlushConfig。
  - 三入口的 `drop(config); self.flush_config();` 结构原样保留（未回到持写锁落盘，
    那会让 replication.conf 的 fsync 阻塞 replid 读侧判定，C# 亦无此形）。
- 未做（修法第 3 条唯一 tmp 名）：本进程运行期落盘面只有 flush_config 一处，
  `flush_to_file` 的另一调用者 `recover_or_init`（replication_history.rs:123）仅在
  构造期 recover_replication_history 持写锁时触发、与 flush_config 无交集，
  单一互斥口已覆盖全部并发面；pid + 原子计数后缀属第二套机制且 C# 无对位形态
  （C# 是 lock 内原地 WriteInto），按「复杂度对标 C#、不造额外抽象」不引入。
- 测试（修法第 5 条）：tests/replication_manager.rs:299-337 新增
  test_concurrent_history_flush_keeps_single_complete_version —— 两入口
  （try_update_my_primary_repl_id / try_update_for_failover）4 线程 × 50 轮交叠落盘，
  判据为终态不变量：join 后 replication.conf 必可 from_byte_array 解出（交错字节
  解不出即红），且等于终态内存历史（版本倒退即不等），非 sleep 时序自证。
  replication_history.rs 层的「并发 flush_to_file」单元用例按事实不补：该函数本身
  不持互斥（互斥在 manager 落盘口），在其上断言并发安全等于断言代码不提供的性质，
  修与不修都可能飘，会成长期 flaky 用例。
- 门禁：只跑 `cargo check -p wedb --tests`（target 独占
  /tmp/fork/fix-replhist/target、/tmp/replhist-target），基线 + 三次 merge dev +
  重贴后主树各一轮，共五次 exit=0、零 warning；按纪律未跑 clippy.sh / test.sh /
  check.js，新增并发用例的实跑回归留主门禁。探针复核：临时注入类型错误能使该 check
  报错，证 --tests 真检到用例。反证记录：在 dev 7a83b8c 的独立树跑同一 check 报
  wnode E0560 `SessionDependencies has no field named acl_settings`
  （wnode/src/service.rs:1473，他域 rm-wacl-auth-settings 在途改动，与本票无关，
  本票 payload 只动 wedb 两文件）。
- 合入：终落 dev a3f6cef5（`git grep -c history_flush_lock HEAD` = 4、
  用例计数 = 1、payload 路径 git status 干净为准）。线型曲折：代码提交 bc8c98f3
  首落 6d27b2c → 被并发过期工作树提交反吞；重落 3a92cb9（--no-ff merge）→ 被
  26de2434（merge resync-strategy-split，整文件覆写）夹带反吞；本票按纪律不 stash
  不 restore，按当下 dev 内容把同一补丁重贴主树（与 e270752 的重同步策略判据拆分
  无冲突），落盘时该补丁已被他票 checkpoint 提交 a3f6cef5 经 fixrs 的 `git add -u`
  搭车带进 dev，本票自己的 pathspec 提交因此空转。归档 mv 见 055867a；
  tests/replication_manager.rs 的 `thread` 文件头导入归一随 0a42c90 落地。
