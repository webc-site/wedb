甄别结论：通过（甄别席 zc-fix-r16-gatereg，2026-09-26）定级 P1
C# 锚亲验：GarnetDatabase.cs:75 CheckpointingLock 确为宿主实例字段（SingleWriterMultiReaderLock，非 static）；DatabaseManagerBase.cs:214/:221 取放锁均经 db 实例路由；SingleDatabaseManager.cs 逐入口调用属实；garnet/libs/server 全树 grep 无任何 static/ConcurrentDictionary 检查点互斥全局表。成立。
rust 锚亲验：wcpr/src/manager/mod.rs:394 CKPT_GATES 为 static LazyLock<Mutex<HashMap<PathBuf, Arc<CkptGateState>>>>，:414 lock_ckpt_gate 按目录查插（entry().or_default()），全文件无表项删除路径（remove 命中均为文件系统操作），条目只增不减成立；create.rs:80/:117 两处消费、cpr_host.rs 两宿主方法均 &self 实例方法、wnode/database_manager_base.rs:384 生产唯一上游、wkv/store/mod.rs:77 WedbStore、mod.rs:893 起三臂闸门测试（911/927/945）逐点吻合。成立。
非重复：deviations.md §27/§29/§81 系恢复/守护/旋钮，§107 为连接数进程级注册表（异轴）；task 各池仅 wcpr-checkpoint-exit-double-reset-grow-cas 同域但系退出双复位轴，无并案。成立。
架构合规：方案为删全局表、状态下沉宿主 WedbStore 实例字段、wcpr 增 pub CkptGateState + acquire(&gate)，单向分层（wkv 已依赖 wcpr），锁等待逻辑原样搬移零新机制，无过度设计无假桩。合规。
勘误补记（不翻案）：执行时 wcpr/tests/cpr 直调面实际另含 casread_gap_doubletake.rs、fuzzy_replay.rs、rc_eviction_ckpt.rs、stale_window_sampling.rs 四文件（票列九件漏计），wkv 侧 reviv_window_floor.rs 经宿主方法调用无需改；wnode 注释 single_database_manager.rs:402/:444 与 database_manager_base.rs:297 亦引用 lock_ckpt_gate/进程闸，随本票一并订正表述；CkptGate 守卫改为借用 &'a CkptGateState 需加生命周期参数。
审核结论：通过（审核席 zcode-r15-review-ckptgates，2026-09-26）

双侧亲验记录：
C# 侧 GarnetDatabase.cs:75 CheckpointingLock 确为实例字段（非 static，宿主 GarnetDatabase 持有），DatabaseManagerBase.cs:214/:221 取锁与释放均经 db 实例路由，SingleDatabaseManager.cs 逐入口调用属实；全仓 grep 无任何 static 检查点互斥全局表与 ConcurrentDictionary 门表。
Rust 侧 mod.rs:394 CKPT_GATES 全局字典与 lock_ckpt_gate 以 PathBuf 为键查插属实，全仓无 remove 删除路径（条目只增不减，嵌入测试逐用例 tempdir 建实例下无界累积属实）；消费点 create.rs:80/:117、宿主通道 cpr_host.rs:82/:103、生产上游 database_manager_base.rs:384 均亲验吻合。
关键裁断依据：mod.rs:380-383 注释自陈闸门保护对象是「同一存储引擎状态机不被两次检查点并发叠加」，文件集层面 Token 互不相交本就免锁——互斥的正确锚点是存储引擎实例而非目录字符串；按目录键属粒度错位，symlink/相对绝对混用下同实例裂为两锁的互斥失效面真实存在。查 doc/zh/deviations.md 在册条目（§27/§29/§81/§95 等）均系恢复行为、守护续跑、旋钮接线，与本案零全局可变状态条款无重叠，非重复立项。
勘误两处（不翻案）：票面引「db.md 实例独占与安全边界」条款，实际出处为 task/review.md 板块 5.2，db.md 无该标题；票面方案 1 写「提升为 pub(crate)」与方案 2 跨 crate 消费 wcpr::CkptGateState 矛盾，须 pub 导出。

整理优化执行方案（覆盖原方案，供 task/fix.md 直接消费）：
1 wcpr 侧（wedb/wcpr/src/manager/mod.rs）删除 static CKPT_GATES 与 lock_ckpt_gate；CkptGateState 提升为 pub（derive Default 保持），busy/event 字段保持私有（加锁逻辑全在 wcpr 内）；新增 pub 异步函数 acquire(gate: &CkptGateState) -> CkptGate 承接原 lock_ckpt_gate 函数体的 CAS 快路径与 event_listener 精准唤醒逻辑（原样搬移，逻辑零改动）；CkptGate 守卫 pub 导出不变。
2 wcpr 侧（wedb/wcpr/src/manager/create.rs）create_checkpoint 与 create_checkpoint_with_token 签名各增参 gate: &CkptGateState（置于 store 之后），函数体内 lock_ckpt_gate(dir).await 改为 acquire(gate).await；ensure_not_growing 与 ensure_epoch_unprotected 前置校验次序不变。
3 wkv 侧（wedb/wkv/src/store/mod.rs）WedbStore 新增实例字段 pub ckpt_gate: wcpr::CkptGateState（构造处 Default 初始化，字段直取风格）；cpr_host.rs 两个宿主方法传 &self.ckpt_gate 进 wcpr。互斥粒度由目录路径回归存储引擎实例，与 C# GarnetDatabase 逐实例锁同形；同步更新 cpr_host.rs:80 注释中「wcpr 进程级闸门」表述为实例闸门。
4 测试适配（完整面，原方案漏计 wcpr/tests）：wkv/tests/checkpoint/reviv_window_floor.rs 与 wcpr/tests/cpr/ 下全部直调文件（concurrent_ckpt.rs、multi_instance_gate.rs、token_layout.rs、meta_tamper.rs、freeze_cross_round.rs、checkpoint_slot.rs、rc_tag.rs、roundtrip.rs、growing_gate.rs）改为构造本地 CkptGateState 传入（单线程用例零互斥开销）；mod.rs:893 起三臂闸门并发测试改为双任务共享同一 CkptGateState 直测（同 gate 串行 + 不同 gate 互不阻塞两臂）；multi_instance_gate.rs 原验证的目录级互斥语义随机制消亡，改写为实例级语义或径直删除。
5 验证点：全仓 grep CKPT_GATES 与 lock_ckpt_gate 零命中；./test.sh 全绿（含 wcpr cpr 套件与 wkv checkpoint 套件）；嵌入形态反复建销引擎实例后进程内无路径键残留面（全局表已物理删除，泄漏面消除为结构性事实）。

检查点串行闸门采用进程级全局字典（按目录路径键），应下沉为存储引擎宿主所有权（对齐 C# GarnetDatabase.CheckpointingLock 实例锁形态）

问题分析：
1 Garnet 契约对齐。C# 检查点互斥锁是数据库实例所有权字段：garnet/libs/server/GarnetDatabase.cs:75 `public SingleWriterMultiReaderLock CheckpointingLock;`（宿主 GarnetDatabase 持有），取锁与释放统一走 garnet/libs/server/Databases/DatabaseManagerBase.cs:214 `db.CheckpointingLock.TryWriteLock()` 与 :221 `db.CheckpointingLock.WriteUnlock()`（实现在 SingleDatabaseManager.cs:100/:104/:122/:156/:188 逐入口调用）。C# 全仓无任何进程级检查点互斥全局表；互斥粒度严格随数据库实例（宿主所有权容器）。
2 工程现状确证。rust 侧 wedb/wcpr/src/manager/mod.rs:394 `static CKPT_GATES: LazyLock<Mutex<HashMap<PathBuf, Arc<CkptGateState>>>>` 为进程级全局可变字典，`lock_ckpt_gate`（mod.rs:414）以检查点目录路径为键查插取闸；消费点两处均在 wedb/wcpr/src/manager/create.rs:80（`create_checkpoint` 自签发入口）与 :117（`create_checkpoint_with_token` 宿主 token 入口）。而 wkv 侧已有宿主方法通道：wedb/wkv/src/store/cpr_host.rs:82 `WedbStore::create_checkpoint` 与 :103 `create_checkpoint_with_token` 均为 `&self` 实例方法并直转 wcpr（生产唯一上游为 wedb/wnode/src/database/database_manager_base.rs:384），下沉通道现成。注释自陈设计动机为「同进程多实例嵌入互不阻塞、共享同目录调用方进程内串行」，但按目录互斥保护的「同目录多实例」场景本被 db.md「实例独占与安全边界：数据存储目录建立独占文件锁守护」判为违约形态，系为违约场景增设的全局承重件。
3 逻辑危害确证。其一，违反板块1「零全局可变状态与清晰所有权：彻底杜绝全局可变字典与单例容器，状态一律下沉为连接私有上下文或明确的宿主所有权容器」明文条款，锁状态脱离宿主生命周期。其二，表条目永不回收：每插入一个新目录路径即常驻一条 `PathBuf + Arc<CkptGateState>` 且无删除路径，嵌入测试形态（wedb_test / wkv tests 逐用例独立 tempdir 反复建引擎实例）下字典条目随实例创建数无界累积，进程生命周期慢性泄漏。其三，互斥正确性依赖路径字符串键：同 store 实例经不同路径字符串形态（符号链接、相对/绝对混用）指向同一目录时，全局字典退化为两把锁，互斥面静默失效；宿主字段形态天然无此口径分叉。

涉及代码：
rust 文件与函数：
wedb/wcpr/src/manager/mod.rs:CKPT_GATES 与 lock_ckpt_gate
wedb/wcpr/src/manager/create.rs:create_checkpoint 与 create_checkpoint_with_token
wedb/wkv/src/store/cpr_host.rs:WedbStore::create_checkpoint 与 WedbStore::create_checkpoint_with_token

对应 c# 文件与函数：
garnet/libs/server/GarnetDatabase.cs:CheckpointingLock 字段
garnet/libs/server/Databases/DatabaseManagerBase.cs:TryPauseCheckpoints 与 ResumeCheckpoints

精炼执行方案：
1 wcpr 侧删除全局 `CKPT_GATES` 字典，`CkptGateState`（busy AtomicBool + Event）提升为 pub(crate) 可构造类型并导出 `CkptGate` 守卫；`create_checkpoint` 与 `create_checkpoint_with_token` 签名增加闸门参数（如 `gate: &CkptGateState`），函数体内以参数闸门取代 `lock_ckpt_gate(dir)` 查插，锁等待逻辑原样保留（异步互斥、精确唤醒不变）。
2 wkv 侧 `WedbStore`（wedb/wkv/src/store/mod.rs:77）新增实例字段 `ckpt_gate: wcpr::CkptGateState`（构造处 Default 初始化），cpr_host.rs 两个宿主方法传 `&self.ckpt_gate` 进 wcpr；互斥粒度由「目录路径」回归「存储引擎实例」，与 C# GarnetDatabase 逐实例锁同形。
3 测试适配：wkv/tests/checkpoint/ 下直调 `wcpr::create_checkpoint*` 的用例改为先构造本地 `CkptGateState` 传入（单线程用例零互斥开销）；wcpr/src/manager/mod.rs:893 起的闸门并发测试改为双任务共享同一 `CkptGateState` 实例直测。
4 验证点：全仓 grep `CKPT_GATES` 零命中；wcpr 闸门并发测试与 wkv checkpoint 既有测试全绿；嵌入形态反复建销引擎实例后进程内无路径键残留（原全局表泄漏面消除）。
合入哈希：ecf7fbf 收口形态：CKPT_GATES 进程级目录键全局字典物理删除，闸门状态下沉 WedbStore 实例字段 ckpt_gate（对标 C# CheckpointingLock 逐实例锁），wcpr 增 pub CkptGateState/acquire/CkptGate<'a>，签名增 gate 参，直调测试面随改，全仓零残留命中。
