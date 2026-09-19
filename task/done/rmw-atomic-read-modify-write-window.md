主存 String RMW 族与信封对象 RMW 内核的读-算-写窗口无同键互斥：并发同键丢失更新

来源：next/glm.my.md 第 7 轮条。本条是 next/glm.db.md「String 域 RMW 命令读-算-写窗口」条的超集
（该条只列 String 域，本条另含信封域整值覆盖），两单同根同修法，本档为收口单（并发双花见文末登记）：
glm.db.md 那条并入本单，不得两处各改一次内核。取证基线：主仓 /Users/z/git/db/wedb 分支 dev，
行号按符号在当下代码复核（初检 HEAD a7402c4，位点在后续 HEAD 复测未漂移）。
原报引用的 wkv/src/session/mod.rs:779「无条带锁可分组」注释
行号已漂移，注释本体在位（现 session/mod.rs:809，try_upsert_batch_sync 的文档注释内），
且其射程只声明 MSET 批量折叠一处，不构成 RMW 免锁依据，本单结论不变。

结论

C# 的 INCR/APPEND/SETBIT/BITFIELD/PFADD 与全部集合族 RMW 只把增量封进 input 交引擎，
旧值读取、计算、写回全程在 Tsavorite 的 hash 桶 ephemeral 独占锁与纪元内完成
（InternalRMW.cs:70 取锁、:244 释放，锁内 TraceBackForKeyMatch 读旧值后走
MainStore/RMWMethods.cs:437 InPlaceUpdaterWorker 或 CopyUpdater/InitialUpdater），
同键并发天然串行。rust 把同一条状态机拆成命令层两步：read_user_sync 裸读旧值 → 命令层算新值 →
try_rmw_sync 盲写绝对值。try_rmw_sync 的实现就是带 TTL 清退门的纯 upsert
（wkv/src/session/raw/write/rmw.rs:34 → :75 try_upsert_raw_sync_unprotected →
wkv/src/session/raw/write/inplace.rs:132），其中的 ephemeral 桶锁只覆盖单次 upsert 的
回溯与挂链，不覆盖命令层那次读。运行时是 thread-per-core 加 SO_REUSEPORT
（/Users/z/git/db/wedb/wedb/wnode/src/server.rs:417 起、:446 起多核 worker），
不同连接落在不同线程共享同一 store，故跨连接同键并发是实况而非理论：两核并发 INCR 同键
各读 5 各写 6（丢一次自增）、并发 APPEND 丢一段、并发 SETRANGE 以零填充互覆、
PFADD 丢成员致 PFCOUNT 虚低；集合域经 run_sync_rmw 反序列化整对象改后整值写回，
并发 HSET 同键不同字段时后写者把前写者的字段整体抹掉。

现状

1. String 域两步形态（同一模式五处）：
   /Users/z/git/db/wedb/wedb/wnode/src/resp/basic_commands/incr.rs:65 network_increment
   （:96 read_user_sync → :116 checked_add → :122 store.try_rmw_sync 写绝对值）、
   :155 network_increment_by_float（:159 读 → :192 写）；
   /Users/z/git/db/wedb/wedb/wnode/src/resp/basic_commands/set.rs:268/:279 network_set_range、
   :607 network_append（:624/:633 写回）；
   /Users/z/git/db/wedb/wedb/wnode/src/resp/bitmap/bitmap_commands.rs:108 SETBIT、
   :715 BITFIELD 写臂；/Users/z/git/db/wedb/wedb/wnode/src/resp/hyperloglog/hyper_log_log_commands.rs:88
   PFADD/PFMERGE 回写。慢路径同型：
   /Users/z/git/db/wedb/wedb/wnode/src/storage/session/storage_session.rs:348 try_rmw_sync
   与 :354 upsert_rmw（两者都是纯写回，无 expected 前值）。
2. 信封域整值覆盖：/Users/z/git/db/wedb/wedb/wnode/src/resp/objects/object_store_utils.rs:1218
   run_sync_rmw（:1246 起装载 → :1271 run_op → obj_save_or_gc_raw（:645）整值写回），
   HSET/ZADD/SADD/LPUSH 等全部集合 RMW 共用该骨架；收尾状态机
   :1006 apply_rmw_post_operate 同样是「整对象序列化后落盘」。
3. 引擎侧无原子 RMW 出口：/Users/z/git/db/wedb/wedb/wkv/src/session/raw/modify.rs:93 rmw_raw
   是 read_raw_with → updater → upsert_raw 两段 await 分离（注释自称对照 C# InternalRMW
   状态机，实际丢了锁语义），且全仓无 wnode 生产调用者；真正原子的
   modify.rs:18 try_modify_raw_in_place_unprotected（页写锁内原位读改写，对标 InPlaceUpdaterWorker）
   只服务 etag/ttl/stub 内部域，且只覆盖内存可变区同尺寸情形，无 CopyUpdater 对等的锁内重试。
4. 同键互斥的先例已在仓内：/Users/z/git/db/wedb/wedb/wkv/src/ttl.rs:454 与 :502
   EXPIRE/PERSIST 持 index.acquire_keys_lock_exclusive(&[user_key]) 串行化整个读改写窗口
   （注释明言无锁时 ttl_of → del_ttl 间隙会产生用户可见异常），锁原语在
   /Users/z/git/db/wedb/wedb/windex/src/table.rs:692。即「本键桶独占锁覆盖读改写窗口」
   是既成范式，RMW 族是唯一没跟上的一族。
5. 测试面无兜底：/Users/z/git/db/wedb/wedb/wnode/tests 无并发同键读改写用例。

C# 参考

garnet/libs/server/Resp/BasicCommands.cs:852 NetworkIncrement、:902 NetworkIncrementByFloat、
:959 NetworkAppend、:441 NetworkSetRange（命令层一律不读旧值，只封 StringInput 交 storageApi）；
引擎锁内读改写
garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs:70
FindOrCreateTagAndTryEphemeralXLock 与 :244 EphemeralXUnlock、
Implementation/Helpers.cs:199（注释「Ephemeral must lock the bucket before traceback」）、
garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs:420/:437 InPlaceUpdaterWorker
与 :544-554 TryInPlaceUpdateNumber；对象域
garnet/libs/server/Storage/Functions/ObjectStore/RMWMethods.cs（记录锁内对 IGarnetObject 执行 op）。
规范源：SKILL.md:10「尽量 1:1 对标 C# 的代码实现」、SKILL.md:21-22「迭代序稳定由连接任务
线程钉定承接」（线程钉定只保证单会话内串行，跨连接同键无承接面，不能作为 RMW 原子性的替身）。

修法

一处收口，禁两套判据：

一、wkv 提供命令级原子 RMW 原语并让命令层只交「增量算子」而非「算好的绝对值」。
形态取最小改动：在既有本键桶锁范式上开一个
rmw_user_key_atomic(user_key, updater: FnOnce(Option<&[u8]>) -> RmWOutcome) 入口，
锁内完成「回溯读旧值（含 TTL 清退门与域归属判定）→ 调 updater → 原位写或追加写 → 推 WATCH 版本」，
覆盖 InPlaceUpdater/CopyUpdater/InitialUpdater 三态（原位失败的翻页降级走同锁内追加写），
使 try_modify_raw_in_place_unprotected 与 upsert 追加臂合并为一条锁内路径。
二、五处 String 臂与 run_sync_rmw 全部改走该原语：INCR/DECR/INCRBY/INCRBYFLOAT 交 delta、
APPEND/SETRANGE/SETBIT/BITFIELD 交片段算子、PFADD/PFMERGE 交 sketch 合并算子、
集合族交 ObjectOperation 算子（信封域锁内反序列化-改-序列化，等价 C# 记录锁内 op）。
命令层的「读旧值再算新值」代码随之删除，错误分支（not-integer / WRONGTYPE）由算子返回值承载，
保持现有 RESP 文案逐字不变。
三、try_rmw_sync 与 upsert_rmw 在迁移完成后删除或降级为内部原语（仅供新原子入口复用），
不留「盲写绝对值」的第二条公开路径——否则该缺陷会以新调用点复发。
四、锁粒度与开销按 C# 同口径：本键桶锁 ephemeral 独占，不引入条带分组；
批量路径（MSET 折叠、prefix hoisting 族）不受影响，其原子性诉求由批锁承接。

优先级

功能缺口，但性质是静默丢数据，列在本文件各票的实用性首位（仅次于
aof-replay-virtual-domain-context 的跨租户串数据）。它是「去条带锁」这一未列入 SKILL 例外的
自定义优化把 C# 语义一并丢掉的结果，属 SKILL.md:10 要撤销的那类优化偏差，应尽早收口。

边界

next/tiered-write-arm-concurrency.md 只管分层树内多步写臂（tree_del 结果丢弃、
meta.size 覆写丢更新），域不同、锁面不同（wbftree 树与 Meta 记录 vs hlog 记录），修完那条
本单的 hlog 与信封两步窗口仍在；两单共用「同键多步序列须整体持锁」的判据但不共用代码。
next/incr-oldvalue-strict-i64.md 管旧值解析口径、next/ttl-purge-watch-version-bump.md 管
WATCH 版本推进，均不覆盖本单。task/ing/msetnx-slow-path-meta-domain-probe.md 管 MSETNX
漏探 Meta 域写出双域键，是判定缺域不是并发缺锁，不同面。
分层升降阶换域窗口另见 task/ing/tiered-drain-envelope-tombstone.md 与
task/ing/tiered-reflush-atomic-swap.md（同轮别条已立项两票，勿并）。

并发双花登记

同一题面在编排期内被三份文档并行承接，除本节所列差异外三者裁决、修法、验收等价，
编排方择一实施、余下两份在合并时删除即可，勿三份各改一次写内核：
本单 task/ing/rmw-atomic-read-modify-write-window.md、
task/ing/rmw-same-key-atomicity.md（同题，来源标注为 glm.my 第 6 轮条，额外给出
C# ISessionLocker.cs:31-44 锁表与 rmw.rs:88-104 慢路径行号）、
task/ing/string-rmw-key-bucket-lock.md（来源 glm.db 条 6，自述以 String 域为实施主体、
并要求 glm.my 侧只补交叉引用不另立单）。本单独占的增量为：五处 String 臂的逐处行号
（incr.rs:65/:96/:116/:122/:155/:192、set.rs:268/:279/:607/:624/:633、
bitmap_commands.rs:108/:715、hyper_log_log_commands.rs:88）与慢路径
storage_session.rs:348/:354 的无 expected 前值取证，以及文首更正的 session/mod.rs:809
注释射程判定。择单时若取他二份，本单取证段可整体转录。

验收

1. 新增并发回归用例（wnode/tests 集成层）：多线程/多会话并发 INCR N 次终值为 N、
   并发 APPEND 不丢段、并发 HSET 同键不同字段全部字段存活、并发 PFADD 后 PFCOUNT 与
   串行参照一致。用例须能在改动前失败（可用最小复现断言）。
2. 全仓 grep 无「命令层 read_user_sync 后紧跟 try_rmw_sync」的两步形态残留。
3. 单键 INCR 快路径新增开销仅一次桶锁获取，与 C# 同量级；不新增跨线程全局锁。
4. cargo check --workspace --all-targets 零告警，禁写 allow。

盘点补记（qw13.invB rmw-atomic-read-modify-write-window）：dev e75716e 复核原样：全仓 rmw_user_key_atomic 零命中，acquire_keys_lock_exclusive 生产消费仍只 wkv/src/ttl.rs:456/:504（EXPIRE/PERSIST），wnode/src/resp/basic_commands/incr.rs:97 read_user_sync → :123 try_rmw_sync 两步形态原样（:156/:193 浮点同型），wnode/tests 无并发同键读改写用例。收口单定位不变，与 string-rmw-key-bucket-lock、wtxn-lock-stripe-count-parity 的并棒建议不变（注意 wtxn 条带票已按「store 注入真实索引联动」落地，并棒时勿再按旧票面改 wtxn）。

---
主代理补录（windex 2pl 收口棒 bfbd1e0 落地后，15:20）：本票若引用 `HashIndex::acquire_keys_lock_exclusive` / `lock_key_exclusive`（含 1024 轮 spin_loop+LockTimeout 中间态）一律失效——该族已连根删除，唯一锁源现为 `HashIndex::try_lock_key_exclusive`（windex/src/table.rs:601，同一把桶闩、无自旋、无超时，KeyLatch 见 lib.rs:17）。在途代码勿再造第二入口；wkv/src/ttl.rs 两处已改持新口。

---

## 判词（三棒收口，分支 fix-rmw-atomic-window，基线 dev 0bcf574）

### 一、棒次覆盖度

一棒（未提交现场，由 d8b5c3d 保全）：定下收口形态——`RmwWindow` 承载本键桶闩的持有证明、
会话锁器模式位 `SessionLocking`/`SessionLockingGuard`、慢路径起手。其价值在架构选型，
未及门禁，未接完臂。

二棒（9b199e2，9 files +514/−61；merge 8402d50/b2f873a）：wkv 侧收口完成——
`wkv/src/session/rmw_window.rs` 窗口取闩/让闩/Drop 放闩三态定型；`session/mod.rs`、
`wkv/src/lib.rs` 导出；**盲写公开口按修法三删除**（会话侧 `try_rmw_sync`/`upsert_rmw` 不再是
`StoreSession`/`BatchStoreSession` 的方法）；写回内核迁到窗口上
（`session/raw/write/rmw.rs:19 impl<'a,'k,D> RmwWindow<'a,'k,D>` → `:43 try_rmw_sync`、`:81 upsert_rmw`）；
五处 String 臂与 HLL/事务过程视图/garnet_api 接线；`wnode/tests/rmw_key_concurrency.rs` 起四例 warm 用例。
停在验收1 的冷路径用例与门禁前（150 回合上限）。

三棒（2121497 +42/−18、968aab1 +475/−7；merge c95a82e/e522614/0b868f2 推基至 0bcf574）：
慢路径六臂接窗（`wnode/src/resp/basic_commands/slow.rs:337/:379/:419/:454/:706/:856` 取窗，
写回一律 `storage.rmw_string(&window, ..)` 于 `:364/:401/:443/:486/:732/:904`），
BITFIELD 无窗即显式 `RESP_ERR_GENERIC` 不作无窗盲写；补八例冷化扇出与窗口层确定性用例；
锁源注释随 `try_lock_key_exclusive` 单点订正（见第四节）；门禁全复跑。

### 二、验收逐条判

1. **满足**。`wedb/wnode/tests/rmw_key_concurrency.rs` 十二例：warm 四例
   （`:124 concurrent_incr_loses_no_update`、`:155 concurrent_append_loses_no_segment`、
   `:206 concurrent_hset_loses_no_field`、`:242 concurrent_pfadd_matches_serial_reference`）+
   窗口层三例（`:369 same_key_window_is_exclusive_and_neighbour_key_unaffected`、
   `:399 transactional_window_yields_latch_and_guard_restores`、
   `:432 two_session_windows_never_cross_read_or_lose_update`，零容量通道握手定序）+
   冷路径五例（`:498` SETRANGE、`:552` APPEND、`:604` SETBIT、`:652` BITFIELD、
   `:700` INCR：断已回执自增值两两互异且终值 = 回执数）。
   **「改动前必失败」以摘闩反证取证**：将 `rmw_window.rs:218` 的取闩分支强制为空手窗口后
   `cargo nextest run -p wnode --test rmw_key_concurrency` → **12 run / 0 passed / 12 failed**，
   断言原文含「同键并发 INCR 丢更新：终值 506 ≠ 已回执自增数 1000」「并发 HSET 同键不同字段被整值写回抹除：
   HLEN 376 ≠ 已回执字段数 1000」「并发 PFADD 丢成员：1000 个已回执元素的基数 430 ≠ 串行参照 1009」
   「已回执自增值出现重复（[1, 1, 1, 1]）：同键冷读改写窗口被串读」。反证补丁已回退，未入库。
2. **满足（判据实质为「读不在锁内」，非字面 grep 两行相邻）**。命令层已无「无锁读 + 无窗写」两步形态：
   `basic_commands/incr.rs:129` 取窗 → `:134 read_user_sync` → `:160 window.try_rmw_sync`；
   `set.rs:228→:231→:240/:251`（SETRANGE）、`set.rs:581→:584→:591/:600`（APPEND）、
   `bitmap_commands.rs:77→:80→:109`（SETBIT）、`:459→:465→:522`（BITFIELD 写臂）、
   `hyper_log_log_commands.rs:379/:499` 取窗 → `:86/:220/:230/:346` 带窗写回、
   信封域 `objects/rmw_helpers.rs:288`（异步）/`:600`（同步）持窗覆盖整值写回、
   事务过程视图 `storage/session/txn_proc_view.rs:120→:121→:138`。
   类型层已无第二条路：`try_rmw_sync`/`upsert_rmw` 只在 `impl RmwWindow` 上存在
   （`wkv/src/session/raw/write/rmw.rs:43/:81`），会话侧同名盲写口 grep 零命中。
   仍存 `read_user_sync` 的臂为只读命令（GET/BITCOUNT/GETBIT，`get.rs`、`bitmap_commands.rs:135/:177/:233/:325`）
   与 SET 条件族盲写（`set.rs:511 network_set_conditional`），按修法四属纯写回族，不在本单射程。
3. **满足**。单键 INCR 快路径新增开销 = 一次本键桶闩取放（`rmw_window.rs:216 try_rmw_window`：
   `index.load_full()` + `bucket_index_for_key` + `HashBucket::try_lock_exclusive`，取不到即
   `Ok(false)` 走既有降级通道转异步，绝不自旋等闩）；全仓载荷 grep `Mutex<|RwLock<|LazyLock|OnceLock|static 锁表|striped`
   **零命中**，无新锁表、无跨线程全局锁、无条带折算；载荷新增原子量仅会话位
   `SessionLockingState(AtomicBool)`（`rmw_window.rs:155`，对标 C# 编译期 api 视图选型，非跨线程锁）。
4. **满足**。`cargo check --workspace --all-targets` exit 0、warning 计数 0；载荷四文件
   `#![allow`/`#[allow` grep 零命中。

### 三、修法四条对照与一处偏离（明记）

- **修法一（字面 `rmw_user_key_atomic(user_key, updater)` 闭包入口）未采，取等价更强的类型强制**。
  理由三条：①闭包入口要求 wkv 承接七族的 RESP 语义（not-integer/WRONGTYPE 文案、HLL sketch 合并、
  BITFIELD 溢出规则），依赖方向倒置；②wedb 同键的字符串/TTL/信封记录各自成键成桶，
  「锁内回溯读旧值」的 C# 单表前提在 rust 侧本就不成立，把读塞进 wkv 反而再造一套双域判定；
  ③票根判据是「读写之间不得交错」+「不留盲写第二条路」——现形形态把二者都做成了**编译期**约束：
  写回入口挂在窗口上，无窗即不可写；窗口即闩的持有证明（`held: Option<(Arc<HashIndex>, usize)>`，
  `rmw_window.rs:101`，Drop 于 `:123` 放闩）。命令层仍只交「算好的绝对值」，这是本仓记录格式
  （hlog 绝对值记录 + TTL 清退门）的既有形态，非本票缺陷面。
- **修法二 已全接**：五处 String 臂 + 慢路径六臂 + 信封 `run_sync_rmw` 两臂（见第二节 1/2 位点）。
- **修法三 已做**（盲写公开口删除，写回内核降级为 `impl RmwWindow` 内部面）。
- **修法四 已守**：本键桶 ephemeral 独占闩、无条带分组；MSET 折叠与 prefix hoisting 批路径未改动
  （载荷未触 `try_upsert_sync`/`upsert_string` 族，见第二节 2 末）。

### 四、锁源单点核（主代理 15:20 补录必改项）

旧族 `acquire_keys_lock_exclusive` / `lock_key_exclusive` 全仓 grep **零命中**（含注释）。
唯一锁源 `HashIndex::try_lock_key_exclusive`（`windex/src/table.rs:601`，实现即
`bucket_for_key(..).lock_exclusive_guard()`）+ `KeyLatch`（`windex/src/bucket.rs:568` = `BucketExclusiveGuard` 别名）。
三棒前遗留两处**散文旧口引用**，已在 968aab1 订正：`wkv/src/session/rmw_window.rs:14-23`（模块头改列
新口，并说明本窗口取闩即该入口的两步组合 `bucket_index_for_key` + `HashBucket::try_lock_exclusive`，
因闩的持有证明须跨 `&self` 借用期交回命令层，故不自建守卫、不另立锁语义，形态与 `wtxn::TxnKeyEntries::acquire_plan` 同款）、
`:55-60`（`RMW_LATCH_SPIN_ATTEMPTS` 注释改按「索引层一次尝试、无自旋、无超时，重试预算由调用方承接」口径）。
新口现消费面：`wkv/src/ttl.rs`、`wtxn/src/txn_lock_table.rs`、本窗口、`windex` 自测——同址同闩，无第四把锁。

### 五、门禁数字（树内私有 target `/tmp/ct-rmw3`，最终树 = 0b868f2）

- `cargo check --workspace --all-targets`：exit 0，warning 0，error 0（曾带一枚 inherited
  `wacl` 未用依赖 `arc-swap` manifest 告警，dev 已在他支清掉，本树复跑为零）。
- `cargo nextest run -p wkv -p wnode --no-fail-fast`：**1332 run / 1332 passed（1 leaky）/ 1 skipped / 0 failed**，exit 0。
- 本票用例单跑：12/12 passed；摘闩反证：12/12 failed。
- `rustfmt --check`（`wedb/rustfmt.toml`，仅施本票 14 枚载荷文件）：全 OK，无待归整。
- `bun js/check.js` 前后逐字节对跑（基线 = HEAD^2 = 0bcf574）：exit 0 双绿，各 4701 字节，
  差异仅两枚**行号漂移**（`storage_session.rs:600→608 vector_registry_delete_hook`、
  `garnet_api/mod.rs:564→577 StoreGarnetApi::store_snapshots`，因载荷在这两文件插行），
  映射零新增、零丢失、零重复定义；`js/check/ignore/` 回写零（跑后 `git status --porcelain js/` 空）。
- 未跑主仓 `./test.sh` / `./sh/clippy.sh`（按票面禁令）。

### 六、红归属与移交登记

- 本支曾在旧基带四红（`range_index_wrongtype_gate::ri_key_rename_not_wrongtyped`、
  `resp_commandstats_session::commandstats_calls_failed_rejected_end_to_end`、
  `resp_pubsub::pub_sub_mode_resp2_whitelist_commands`、
  `tiered_field_ttl::tiered_hash_expire_sets_and_reads_back`）：checkout 当前 dev 复跑同四枚 **23/23 绿**，
  归因为其时基线未含 `fix-obj-arg-reparse`（96cd9a2）收口，非本支载荷所致；
  0b868f2 合入该收口（其票已归档 `task/done/r5-red-attribution-four-failures.md`）后清零。
- 另两枚（`aof_stored_proc_replay::flush_db_entry_replays_targeted_database`、
  `service::ttl_purge_single_deterministic_entry`）判红判在基 a4761f1，同样随推基消失。
- **移交他票**：`user_key` 桶与记录（hlog 物理键）桶两基并存期，窗口持闩期内层 ephemeral 取闩必失败
  并回 `RETRY_LATER` 的退避面（`rmw_window.rs:31-38` 已明记），与纯写回族（SET/DEL/MSET 折叠）
  对本窗口的交错面另票承接；分层树内写臂仍归 `task/ing/tiered-write-arm-concurrency.md`。
- 结论：**本票收口成立，验收四条全绿，判 done**。

载荷与门禁 sha：d8b5c3d（一棒现场保全）、9b199e2（wkv 窗口收口）、8402d50/b2f873a（推基 a2231a5）、
2121497（慢路径六臂接窗）、c95a82e（推基 a4761f1）、968aab1（十二例回归 + 锁源注释订正）、
e522614/0b868f2（推基 c5e3f8f / 0bcf574）。

---

## 主代理委派收口判词（收口子代理现刻复核，dev 尖 862c256，全部证据取 `git show HEAD:` / `git grep HEAD`，未读工作区）

### 一、落地 sha 链与簿记实况

9b199e2（09-19 23:22 wkv 窗口收口）→ 2121497（23:48 慢路径六臂接窗）→ 968aab1（00:12 回归 +
锁源注释订正）→ 0b868f2（推基）→ **2495601**（00:48 `Merge branch 'dev' into fix-rmw-atomic-window`，
再回合 dev 至 18b0789）。2495601 相对其 dev 父的净贡献实测 **14 files +1226/−105**
（`rmw_window.rs` 新件 260 行、`rmw_key_concurrency.rs` 新件 740 行），逐件在本现刻 HEAD 核到（第二节）。

- **勘正委派前提一**：2495601 **不在 dev 第一亲链上**（其 commit message 自称「ff 合入主仓」未成立），
  沿 `b8e9f38`（acl-setuser-propagation 回合）→ `97b097f`（01:14 tiered-zset-range 回合 dev）
  侧链汇入主链，载荷随他支推基扩散。判定不受影响：载荷在 HEAD 完整在位、无第二形态残留。
- **勘正委派前提二**：两票已由 **7999624**（09-20 00:38 三棒归档棒）`git mv` 至 `task/done/`，
  本代理现刻 `task/ing/` 零副本，故本棒**无 mv 可做**，唯一改动是追加本段与姊妹票同段。

### 二、票面机制逐条现刻核（HEAD 862c256）

1. **慢路径六臂接本键原子窗口——在位，行号与判词逐字吻合**。
   `wnode/src/resp/basic_commands/slow.rs` 取窗 `:337`（SETRANGE）`:379`（APPEND）`:419`（INCR）
   `:454`（INCRBYFLOAT）`:706`（SETBIT）`:855-856`（BITFIELD，仅 `cmd == C::Bitfield` 分支取窗），
   写回 `:364/:401/:443/:486/:732/:904` 一律 `storage.rmw_string(&window, ..)`；
   BITFIELD 无窗即 `:896`/`:907` `output.write_resp_error(RESP_ERR_GENERIC)`，**不作无窗盲写**。
2. **写回统一带窗 `rmw_string`——在位且为唯一入口**。`storage/session/storage_session.rs:478`
   `pub async fn rmw_string<'k,'w>(&self, window: &RmwWindow<'w,'k,D>, val:&[u8])`，
   体内 `:483 window.try_rmw_sync` → 降级 `:489 window.upsert_rmw`。
   全仓 `fn try_rmw_sync|fn upsert_rmw` **仅两枚定义**，皆在 `wkv/src/session/raw/write/rmw.rs:19`
   的 `impl<'a,'k,D> RmwWindow` 上（`:43`/`:81`），会话侧同名盲写口 grep 零命中——**修法三成立**。
   16 枚调用点（含测试 `rmw_key_concurrency.rs:316`）接收者清一色 `window.`。
3. **快路径五臂 + 信封 + 事务视图全接**：`incr.rs:129→134→160`、`incr.rs:182→186→223`、
   `set.rs:228→231→240/251`、`set.rs:581→584→591/600`、`bitmap_commands.rs:77→80→109`、
   `:459→465→522`、`hyper_log_log_commands.rs:86/212/321/379/499`、
   `objects/rmw_helpers.rs:571 run_sync_rmw → :600 try_rmw_window`（取不到即 `ObjLoad::Degrade`）、
   异步臂 `:288 rmw_window`、`storage/session/txn_proc_view.rs:120→121→138`。
   仍在 `read_user_sync` 而无窗者只余只读臂（`bitmap_commands.rs:135/177/233/325`）与
   SET 条件族盲写（`set.rs:511`，`apply_set_with_expiry` 纯写回族），按修法四不在射程——与判词一致。
4. **同键 ephemeral 桶闩——在位**：`wkv/src/session/rmw_window.rs:96 struct RmwWindow` /
   `:102 held: Option<(Arc<HashIndex>, usize)>`（钉索引版本 + 纯桶下标，跨 split 不串锁）/
   `:120-128 Drop` 按同版本 `unlock_exclusive` / `:217 try_rmw_window` 非阻塞臂 /
   `:249 rmw_window` 让核等待臂（预算耗尽回 `LockTimeout`，无无界等待）。
5. **会话锁器——在位**：`:72 enum SessionLocking`（`:82 is_transactional`）、
   `:132 SessionLockingGuard` + `:146-148 Drop` RAII 还原、`:156 SessionLockingState(AtomicBool)`、
   `:187 session_locking` / `:194 set_session_locking` / `:200 push_session_locking`；
   `:219` 事务态即回让闩窗口（不自旋等自己）；`:31 lib.rs` 与 `:38 session/mod.rs` 导出三型。
6. **并发回归八例——在位（合计十二例）**：`wnode/tests/rmw_key_concurrency.rs` HEAD 计 741 行、
   `#[test]` 12 枚 = 票面八例（窗口层 `:370` 同键第二窗 `is_none` + 异主桶键 `is_some` + 放闩恢复、
   `:400` 事务态让闩与守卫还原、`:433` 持窗未写回绝不被交叠；冷化扇出 `:499` SETRANGE、
   `:553` APPEND、`:605` SETBIT、`:653` BITFIELD、`:701` INCR 回执自增值互异）+ 原 warm 四例
   （`:125/:156/:207/:243`，即票面验收1 的 INCR/APPEND/HSET/PFADD 四问）。
7. **锁源单点（主代理 15:20 补录必改项）——已核**：`acquire_keys_lock_exclusive` 与裸
   `lock_key_exclusive` 全仓（含 README 与注释）**0 命中**；唯一入口 `windex/src/table.rs:601
   try_lock_key_exclusive` 在位；三面共闩现刻核对——本窗 `rmw_window.rs:223-225`
   （`bucket_index_for_key` table.rs:540 + `HashBucket::try_lock_exclusive` bucket.rs:109 两步组合）、
   `wkv/src/ttl.rs:459/:511`、`wtxn/src/txn_lock_table.rs:116-117`（转发同一 `HashBucket`）；
   `KeyLatch` 仍为 `bucket.rs:563` 的 `BucketExclusiveGuard` 别名，无第二份锁实现。
8. **禁令与告警面守住**：载荷四件（`rmw_window.rs`/`raw/write/rmw.rs`/`slow.rs`/回归测试）
   grep `Mutex<|RwLock<|LazyLock|OnceLock|striped` **零命中**，无第四把锁、无条带折算；
   `#![allow`/`#[allow` 于 `rmw_window.rs`、`rmw.rs` 零命中（验收4 的 `allow` 禁令在位）。

### 三、遗留项（本代理不补码，如实登记）

- **判词行文不精确（非机制缺口）**：三棒判词第二节 3 称 `try_rmw_window` 「取不到即 `Ok(false)`…
  绝不自旋等闩」，HEAD 实况为 `rmw_window.rs:226-233` 在 `RMW_LATCH_SPIN_ATTEMPTS = 1024`（`:61`）
  预算内做**单次尝试的有界自旋**后方回 `None` 降级。模块头 `:55-60` 的口径（索引层一次尝试、
  无自旋驱动、无超时，重试预算由调用方承接）与代码自洽，故判为判词措辞偏差，随本段订正，不改码。
- **锚点漂移**：后续 dev 棒使 `windex/src/bucket.rs` 五枚锚点各下移 5 行
  （113→109、241→236、254→249、486→481、568→563）、`SessionLockingState` 155→156；
  `table.rs:601` 与 slow.rs/命令臂全部锚点未漂移。判词未订版，以本段现刻数字为准。
- **门禁未在 dev 现尖复跑**：`1332/1332`、`cargo check` 零告警记于树 0b868f2；
  其后 dev 有 `8fb4123`、`862c256` 两棒锁 API 重构，经核本票消费面签名
  （`bucket`/`bucket_index_for_key`/`try_lock_exclusive`/`unlock_exclusive`/`KeyLatch`）
  在位未改，判无回归风险，但 workspace check + nextest 归主代理门禁复跑，本代理未代跑。
- **移交他票不变**：`user_key` 桶与记录（hlog 物理键）桶两基并存期的 `RETRY_LATER` 退避面、
  纯写回族（SET/DEL/MSET 折叠）与本窗口的交错面、分层树内写臂（`tiered-write-arm-concurrency`）。

结论：**落地完备，票面机制六项全数在位、零缺口**（唯二偏差为判词措辞与行号漂移，均已上文登记）；
本票维持 **done**。
