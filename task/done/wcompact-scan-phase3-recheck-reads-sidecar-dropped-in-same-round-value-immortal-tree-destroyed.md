锁定注记（2026-10-01 r10 波主控，基线 `c2557a1`；wcompact 甄别席候选 + 主控并派复核席逐环反证 + 主控现码两侧亲验，锚以本注记为准，台账禁钉行号）
- 病灶：`wedb/wcompact/src/compactor/run.rs::compact_scan` 阶段 3 为**单环边处置边复查**——
  `let now = now_ticks()` 于阶段 3 入口重取（晚于阶段 2 定稿区 I/O），随后
  `for (key, cand) in candidates`：`cand.is_dead` 者走快速清理通道 `drop_dead` 后 `continue`；
  阶段 1 **存活**候选以该新鲜 `now` 调 `judge_dead` 重估、判活即 `conditional_copy_to_tail` 回拷。
  宿主数据键与其 TTL 旁路键（`KeyTag::Ttl` 独立物理键）在同一 `candidates` 表里是**两个独立候选**，
  表用 `wbase::map.rs` 的 gxhash 随机种子哈希序，二者的处理次序**逐进程任意**。
- 复查读的是将被同轮清退改写的伴生现态：`wedb/wkv/src/compact.rs::WedbCompactionFunctions::is_deleted`
  的 user-visible 宿主臂以 `read_i64_sidecar(&ttl_k).is_ok_and(|exp| is_expired(exp, now))` **现读**旁路；
  `wedb/wkv/src/ttl.rs::read_i64_sidecar` 对「不存在/墓碑/非法长度」一律回 `None`，
  `::is_expired(None, ..)` 回 `false` ⇒ **旁路缺席即判宿主存活**。于是次序「旁路先、宿主后」必然翻转宿主裁决。
- 完整危害链（复核席逐环给码证、主控采纳）：
  1. 同轮同一 `now` 下宿主与旁路**本应同判死**（旁路自身到期直接判死，宿主因旁路过期判死）；
  2. 旁路先被处置：`drop_dead` → `on_dropped`（安全垫 `ttl_of` 仍 `Some(已过期)` 放行）→
     以**同一 `user_key`** 组 `tree_key` → `get_tree` 命中宿主**活树** → `emit_event(RangeIndexDrop)`
     先入账 AOF/副本 → `delete_index` 注销树 → `unregister_bftree_key`；随后 `index().delete(旁路键)` 摘旁路槽；
  3. 宿主后被复查：sidecar 已 miss ⇒ 判活 ⇒ `conditional_copy_to_tail` 把**过期值回拷尾部**、槽位 CAS 迁新帧；
  4. 截断后终态：**值永生**（新帧在截断点上方）、**TTL 永失**（旁路已摘槽）、
     **对象树主从双端已毁而数据帧健在**（分层集合 `get_tree` 落空、副本收伪清理事件）；
  5. **不自愈**：后续每轮 sidecar 恒缺席 ⇒ 恒判活 ⇒ 恒回拷，无任何触点可收敛。
  反序（宿主先清退、旁路后落「树已亡」跳过臂）两形均正确——病灶严格限于「阶段 1 活、阶段 3 死」的**复查互扰**，
  阶段 1 已双判死的候选走快速通道且其安全垫「在场即保守保留」承接，无害。
- 可达性：`wconf/src/node_options.rs` 的 `compaction_type`（None/Shift/Lookup/Scan）为配置源，
  `wedb/wkv/src/gc/compact.rs::GcManager::try_compact` 以 `LogCompactionType::Scan => CompactionType::Scan`
  接线至 `WedbStore::compact`；`wedb/wnode/src/config_owner.rs` 的 `ConfigReconcile::CompactionType`
  令运行时 `CONFIG SET` 即生效（`wedb/wnode/tests/config_owner_bridge` 实证），无需重启即生产可达。
  触发窗为「阶段 1 判活、阶段 3 判死」的到期时刻落环——长区间紧缩窗内到期的全体 TTL 键，每键约二分之一顺序运气。
- Lookup 档对照（措辞按复核席修正收窄）：`run.rs::compact_lookup` 以 run-start 单一 `now` 快照裁决且无复查；
  写序经前案 `task/done/wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal.md` 收口后
  恒「数据先、TTL 腿后置」，旁路最新记录地址总在宿主之上、升序扫描下宿主先判且 sidecar 在场可读，
  **常规写序下**无此形态。残余角部：`wedb/wkv/src/session/collection.rs` 的 `if !keep_ttl` 才清 TTL 通道
  允许新宿主记录落在旧 sidecar 之上——本票**不复判** keep_ttl 写序面（另属写面缝），只在锁测申报中登记该角部不受本票保护。
- C# 对位（本缝属 wedb 自研 sidecar + 自加复查，无 C# 直位）：`garnet/libs/server/Storage/Functions/GarnetRecordTriggers.cs::IsDeleted`
  恒 false；`garnet/libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs::CompactScan`
  只在阶段 1 调 `IsDeleted`，阶段 3（iter3）仅 `ContainsKeyInMemory` + `CompactionCopyToTail`，
  **不存在存活候选复查**，且裁决读自 iter1 定稿的 tempKv 快照、不回读主库现态；C# TTL 内嵌记录物理头，
  值与 Expiration 单记录一体随迁（`UpsertMethods.cs`），结构上不可能出现「值拷回而 TTL 已失」。
  故 rust 侧的对齐目标是**快照化裁决**这一形态本身，而非新增第二真值源；本票判据同时引本仓文档承诺
  （`run.rs` 阶段 3 复查注语自陈「长时间紧缩期间 TTL 可能刚到期，CAS 迁移前必须以最新状态重估」——
  该承诺被同轮伴生清退反噬，恰证复查需冻结时点）。
- 前案边界（查重已核）：`task/done/wcompact-dead-candidate-fast-lane-on-dropped-none-blindspot.md`
  裁的是**死候选补复查**方向（误杀活键），与本票**活候选复查被伴生干扰**方向相反、正交；
  该案的 `on_dropped` 修复恰保证本票修复下「同死」分支清退安全。
  `task/done/wkv-ttl-sidecar-strip-before-record-crash-window-value-immortal.md` 收崩溃面与写面次序，未涉紧缩复查面。
  `task/done/wkv-gc-circuit-breaker-shift-bypasses-liveness-normalization.md` 裁位点归一化，异面。
  另登记同批并案：`task/ing/wkv-compact-on-dropped-host-probe-err-folded-as-orphan-destroys-live-host.md`
  与本票同族（均为紧缩面「不确定/伴生态」被折成确定死亡），**两票必须分席**：本票只动 `wcompact`，
  那张只动 `wkv/src/compact.rs`，交叠处只在测试夹具。
- 禁触域（同侪在途）：`wedb/wedb/src/server/replication/**`、`wedb/wedb/src/server/failover/**`、
  `wedb/wkv/src/store/keyspace.rs`、`wedb/wnode/src/aof/**`、`wedb/wkv/src/session/collection.rs`（keep_ttl 只作证据）；
  本票只动 `wedb/wcompact/src/compactor/run.rs`（阶段 3 结构）与 `wedb/wcompact/tests/**`、
  `wedb/wkv/tests/**` 夹具；**禁**改 `wkv/src/compact.rs` 的 `is_deleted`/`on_dropped` 判据本体。

审核结论：通过（2026-10-01 主控亲验立案；P1。稳态紧缩路径即达、无需崩溃；运行时 CONFIG 可使 Scan 档生效；
后果为三重不可逆——过期值永生 + 分层对象树主从双端被毁 + 伪 `RangeIndexDrop` 事件入副本账，
且终态永不自愈。复核席定级 P1 与同面快速通道票危害同级，主控采纳。
唯一降档因素是需宿主/旁路处理顺序恰好「旁路先」（约半数键），但那是概率而非前提门槛）

Scan 档紧缩阶段 3 复查无同轮快照：TTL 旁路候选先被清退即翻转宿主判活裁决，过期值回拷尾部永生而分层树主从双端已毁

问题分析：
1. 复查语义与伴生清退互斥没被设计进来：阶段 3 的复查目的是「用最新状态重估，别让长窗内刚过期的记录被搬回尾部」，
   但它读的最新状态里包含**同轮同环内其它候选的处置后果**。宿主与旁路是一对必须同命运的伴生记录
   （wedb 把 TTL 拆成独立物理键，C# 是内嵌一体），把它们放进同一个可变环、又让裁决依赖对方的盘上现态，
   等于把「谁先被处理」变成裁决输入——哈希序任意即裁决任意。这不是并发竞态而是**单线程内的时序自污染**，
   加锁、重试、CAS 都挡不住。
2. 破坏性一侧已被前案锁死为不可回退：`on_dropped` 依 WAL 纪律先入 `RangeIndexDrop` 账再注销树，
   旁路候选清退时对宿主树做的是**整键退册**（`tree_key` 由同一 `user_key` 组出），
   一旦宿主随后被判活回拷，就得到「数据帧存活但宿主对象树已毁 + 副本已收伪清理」的终态；
   回拷本身又使值越过截断点上界，永生于日志尾部。
3. 修复方向唯一且零新机制：与 C# tempKv 快照同构——把阶段 3 的复查**与处置解耦成两子段**，
   先以同一新鲜 `now` 对全体非死候选预扫描复查并冻结判死位图，再入环按冻结裁决分派清退/迁移。
   冻结后宿主与旁路的裁决取自同一时点、彼此不可见对方处置后果，同轮同命运即恢复。
   不动业务谓词、不动 `on_dropped`/`is_deleted`、不动线协议与紧缩位点算术。

涉及代码：
rust 文件与函数：
wedb/wcompact/src/compactor/run.rs::compact_scan（病灶：阶段 3 单环边处置边复查）
wedb/wcompact/src/compactor/run.rs::judge_dead、::drop_dead、::conditional_copy_to_tail、::CompactRunTally（裁决与处置单点，复用不改判据）
wedb/wkv/src/compact.rs::WedbCompactionFunctions::is_deleted（user-visible 臂现读 sidecar 的依赖面，只读证据）
wedb/wkv/src/ttl.rs::read_i64_sidecar、::is_expired（缺席即判活的语义源，只读证据）
wedb/wkv/src/gc/compact.rs::GcManager::try_compact、wconf/src/node_options.rs（Scan 档可达链）、
wedb/wnode/src/config_owner.rs::ConfigReconcile::CompactionType（运行时可达链）
对应 c# 文件与函数：
libs/storage/Tsavorite/cs/src/core/Compaction/TsavoriteCompaction.cs::CompactScan（阶段 1 唯一裁决 + tempKv 快照 + 阶段 3 无复查）
libs/server/Storage/Functions/GarnetRecordTriggers.cs::IsDeleted（恒 false）

精炼执行方案：
1. **阶段 3 拆两子段（快照化复查）**：先对 `candidates` 中**全部非死候选**以同一新鲜 `now` 预跑复查
   （`judge_dead` 口径不变：墓碑 + 业务谓词），把结果冻结进与 `candidates` 同键的判死位图
   （`HashSet`/位向量择一，禁引入第二真值源，禁缓存值体）；再入处置环，处置环**只读冻结位图**分派
   `drop_dead`（冻结为死）或 `find_latest_address` + `conditional_copy_to_tail`（冻结为活）。
   预扫描段与处置段各自内部的错误传播形态保持现码（`?` 早退者仍早退，`warn! + retain_record` 者仍保守）。
2. **保留阶段 3 复查的本意**：冻结时点仍取阶段 3 当下的 `now`（而非阶段 1 的 `now`），
   故「长窗内刚过期记录被搬回尾部」的原防治依然成立；须在阶段 3 注语中写明「复查对全体非死候选一次成账，
   处置阶段不再读伴生现态」，禁删除原注语中关于时敏复查的说明。
3. 禁做项：禁改 `is_deleted`/`on_dropped` 的业务判据（含 user-visible 臂的 sidecar 缺席即判活语义——
   那是 TTL 旁路缺失时的正确保守）；禁改 Lookup 档裁决；禁改 `CompactRunTally` 的截断地板算术
   （前案已裁 `floor=min start` 安全）；禁给 `candidates` 表换确定性哈希序来「让次序固定」（治标，
   且违背 gxhash 随机种子防碰撞 DoS 的在册口径）；禁新增配置项/超时淘汰；禁 `#[allow]`/`#[expect]`；禁占位实现；
   禁把本票与同族 `on_dropped` 错误折损票合并成一张（交叠只在夹具，合并即两半互等）。
4. 锁测（`wedb/wcompact/tests/**` 或 `wedb/wkv/tests/**` 紧缩册，择既有 Scan 族册内追加）：
   (a) **伴生次序锁测**——批量 `SETEX` 使宿主数据帧 + TTL 旁路帧同落紧缩区，令到期时刻落在阶段 1 之后、
       阶段 3 复查之前（用 `wkv/src/compact.rs` 既有 `ON_DROPPED_PAUSE_INJECT` 一类 debug 钩放大窗，
       多键批量使「旁路先行」近乎必然）；跑 Scan 档紧缩后断言：**每个过期键 `GET` 落空**、
       尾部无过期值新帧、分层键的 `get_tree` 与记录裁决同命运（不得「记录活而树毁」）、
       AOF 事件面的 `RangeIndexDrop` 条数与本轮判死的分层宿主数**逐键相等**；
   (b) 反序夹具回归——同构造下强制宿主先行（既有 scan_mode/truncation 册形态）保持绿；
   (c) 既有 `wcompact/tests/compact/*`（concurrency/truncation/fuzzy_bound/lookup_mode/scan_mode/grow_window）
       与 `wkv_compact_tiered_expired_drain` 族全绿——复查语义保留、仅时点前移，不得靠改断言迁就。
   revert-proof：把处置环改回「逐候选现调 `judge_dead`」（撤第 1 步）后 (a) 的「`GET` 落空」与
   「事件条数相等」两断言必红（值永生 + 宿主树伪清退复现）；若退化为「冻结位图仍读伴生现态但先处置后冻结」
   （假收口形态），(a) 同红——反向钉死「先全量冻结、后处置」的顺序本身。
5. 登记：本票判据（伴生对偶记录在同轮紧缩内必须同一时点成账，处置阶段禁读对方现态）在
   `doc/zh/deviations.md` 册尾顺编新节登记（先入库者得号、撞号让位不写死；取号前先 grep 册尾），
   锚用 `路径::符号` 形态。
6. 验证面：`cargo check -q -p wcompact -p wkv -p wnode --all-targets` 与
   `cargo nextest run -p wcompact`（全套）与 `cargo nextest run -p wkv --test main`
   （紧缩族在 `tests/main.rs` leaf 内册，禁照抄 leaf 名当 `--test` 靶）；
   禁在主树或沙箱跑 `./test.sh`/`./sh/clippy.sh`（波次门禁由主控统一跑）。

---
### 收口与终态记录（2026-10-01 主控闭环归档）
- **修复方案**：`wcompact/src/compactor/run.rs::compact_scan` 阶段 3 拆分为两子段：预扫描子段以新鲜 `now` 对全体非死候选预跑复查并一次成账冻结为判死位图（`recheck_dead: HashSet<Box<[u8]>>`）；处置子段只读该冻结位图分派清退或条件迁移，处置环内绝不再读存储判活，彻底消除伴生侧车被清退后反噬宿主复查判活的漏洞。
- **锁测验证**：`wcompact/tests/compact/scan_sidecar_snapshot.rs` 增设三组伴生互扰锁测（暂停握手互扰窗、单对反序等价、快速清理与快照复查混合），断言易逝键 GET 全部落空、回迁数恰为对照对、退册事件与判死宿主逐键相等；单测全部通过；revert-proof 验证完成。
- **台账登记**：在 `doc/zh/deviations.md` 顺编登记 `[§196]`。
- **工单归档**：主分支已合入，工单移动至 `task/done/`。
