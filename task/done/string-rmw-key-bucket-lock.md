String 域 RMW 命令族的读-算-写窗口无同键互斥：并发丢更新，收口点应收敛到 windex 桶闩

来源：next/glm.db.md 条 6 立项（该文件本波剪空删除）。取证基线：主仓 /Users/z/git/db/wedb
分支 dev，行号按当下 HEAD 的符号重取。判定：成立且待做。

同主题登记（本波编排期实况）
本面在 glm.my 侧有一条同根超集主张，已落地为 task/ing/rmw-atomic-read-modify-write-window.md，
该单文首自述为收口单，并登记了同期三份异名薄壳（含本单与已剪除的 rmw-same-key-atomicity.md）。
本单不再自称唯一载体：两单裁决、锁源、修法一致，派发时择一实施即可，勿两处各改一次 RMW 内核。
本单独占、不可随择单丢失的增量为：其一，读侧取证
（user_read.rs:162-169 与 wkv/src/session/mod.rs:205 的普通命令一致性读不加桶锁）；其二，桶闩
对位证据 windex/src/bucket.rs:68-116、:150-280 与 C# HashBucket 的 TryAcquire/Release/Promote
一一映射，这是收口单未列的锁实现层依据；其三，文首「修法」段的禁令——收口不得新建私有锁表、
也不得与 wtxn 条带表并联，须与 task/ing/wtxn-lock-stripe-count-parity.md 共用同一把 windex 桶闩
锁源，收口单无此跨单约束。若编排方取收口单实施，请把上述三点转录过去再剪本单。

结论一句话
C# 这批命令走一条 RMW 状态机，入口即对键所在哈希桶加 ephemeral 独占锁，锁内完成
TraceBackForKeyMatch 读旧值 → 原位改或追加新值 → 解锁，同键并发被桶锁串行。rust 把它拆成命令层
的两次独立调用：无锁读旧值 → 本地算新值 → 纯 upsert 写回，读写之间任意交错即丢更新。wkv 侧
其实已有两个正确件（页写锁内原位读改写的 try_modify_raw_in_place_unprotected、windex 的批量键
桶锁 acquire_keys_lock_exclusive），但都没接到这批命令上。

现状（主仓 HEAD 实测）
1. 两步形态命令层：/Users/z/git/db/wedb/wedb/wnode/src/resp/basic_commands/incr.rs:65-130
   network_increment（:96-114 read_user_sync 取旧值并解析 → :116-119 checked_add →
   :122-127 store.try_rmw_sync 盲写），:133 起 network_increment_by_float 同型；
   /Users/z/git/db/wedb/wedb/wnode/src/resp/basic_commands/set.rs:233 network_set_range、
   :607 network_append；/Users/z/git/db/wedb/wedb/wnode/src/resp/bitmap/bitmap_commands.rs:57
   network_string_set_bit 与 BITFIELD 写臂回写；
   /Users/z/git/db/wedb/wedb/wnode/src/resp/hyperloglog/hyper_log_log_commands.rs:82 store_hll
   （PFADD/PFMERGE）。
2. 读侧无锁：/Users/z/git/db/wedb/wedb/wnode/src/storage/session/common/user_read.rs:162-169
   read_user_sync 只经 with_session_consistent_read 包一层（
   /Users/z/git/db/wedb/wedb/wkv/src/session/mod.rs:205 起），普通命令一致性读臂对键不加任何
   桶锁。
3. 写侧非 RMW：/Users/z/git/db/wedb/wedb/wkv/src/session/raw/write/rmw.rs:34-40 try_rmw_sync、
   :55-80 try_rmw_sync_unprotected_with_prefix —— 主体是 TTL 过期门裁决 +
   :75 try_upsert_raw_sync_unprotected 纯写回 + :77 bump_watch_version，无前值校验、无 expected
   address CAS。慢路径 /Users/z/git/db/wedb/wedb/wnode/src/storage/session/storage_session.rs:347
   rmw_string → upsert_rmw 同为纯写回。WATCH 版本推进只服务事务乐观校验，非事务裸命令无保护。
4. 已有但未接线的原子面：/Users/z/git/db/wedb/wedb/wkv/src/session/raw/modify.rs:18
   try_modify_raw_in_place_unprotected（沿 prev 链回溯 + 页写锁内就地读改写，对标
   InPlaceUpdaterWorker），生产消费只有内部域：
   /Users/z/git/db/wedb/wedb/wkv/src/etag.rs:60、/Users/z/git/db/wedb/wedb/wkv/src/ttl.rs:291、
   /Users/z/git/db/wedb/wedb/wnode/src/storage/session/common/ttl_sync.rs:86、
   etag_sync.rs:51；闭包式 rmw_raw（modify.rs:93，read_raw_with → updater → upsert_raw，本身
   两段 await 分离亦非原子）全仓无 wnode 生产调用者，仅同文件 :133 自用。
5. 桶锁先例（同一把锁的正确用法已在位）：/Users/z/git/db/wedb/wedb/windex/src/table.rs:692
   acquire_keys_lock_exclusive，生产消费点 /Users/z/git/db/wedb/wedb/wkv/src/ttl.rs:454、:502
   （EXPIRE/PERSIST 读改写窗口持本键独占桶锁），桶闩本体
   /Users/z/git/db/wedb/wedb/windex/src/bucket.rs:68-116、:150-280（对位 HashBucket
   TryAcquire/Release/Promote Latch）。

C# 参考
/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/InternalRMW.cs:70
FindOrCreateTagAndTryEphemeralXLock（全程持锁）与 :244 EphemeralXUnlock；
/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Interfaces/ISessionLocker.cs:31-44
BasicSessionLocker.TryLockEphemeralExclusive；
/Users/z/git/db/wedb/garnet/libs/storage/Tsavorite/cs/src/core/Index/Tsavorite/Implementation/Helpers.cs:199
（"Ephemeral must lock the bucket before traceback"）；命令面
/Users/z/git/db/wedb/garnet/libs/server/Resp/BasicCommands.cs:852 NetworkIncrement、
:902 NetworkIncrementByFloat、:441 NetworkSetRange、:959 NetworkAppend；
/Users/z/git/db/wedb/garnet/libs/server/Resp/Bitmap/BitmapCommands.cs:131 NetworkStringSetBit
（单次 storageApi.StringSetBit RMW）；
/Users/z/git/db/wedb/garnet/libs/server/Resp/HyperLogLog/HyperLogLogCommands.cs:26-36、:109；
引擎锁内算值 /Users/z/git/db/wedb/garnet/libs/server/Storage/Functions/MainStore/RMWMethods.cs
（InitialUpdater/InPlaceUpdaterWorker/CopyUpdater 全在记录锁内跑）。

修法
在 wkv 提供一个命令级原子 RMW 入口，读旧值→算新值→写回在同一次本键独占桶锁（或对等的条带写锁）
内闭环，直接复用 windex 的 acquire_keys_lock_exclusive 作为唯一锁源（与 EXPIRE 先例同锁，杜绝
第四把锁），闭包形态沿用 modify.rs 的 with_memory_record 回溯；原位不可行（尺寸变化、已滑出
可变区、命中墓碑）时在锁内降级走 copy-update 追加（对标 C# CopyUpdater，扩展现有 modify.rs
骨架覆盖该分支），失败即锁内重试而非解锁重来。命令层七族（INCR/DECR/INCRBY/DECRBY/INCRBYFLOAT/
APPEND/SETRANGE/SETBIT/BITFIELD 写臂/PFADD/PFMERGE）统一改走该入口，删除各自的读+写两步形态。
同一入口的第二消费方是对象信封域的 run_sync_rmw（整值写回丢字段，同根问题）。
禁止的收口方式：再新建一张私有锁表或与 wtxn 条带表并联的第二套锁源（见
task/ing/wtxn-lock-stripe-count-parity.md，那一条的正解同样是收敛到 windex 桶闩）。
验证：需补并发同键用例（多任务并发 INCR 断言终值 = 次数、并发 APPEND 断言尾段全在、并发
HSET 不同字段断言字段不丢），当前 wedb/wnode/tests 无并发同键读改写用例。

边界
与 task/ing/tiered-write-arm-concurrency.md 不同域（那条管 wcol/wbftree 分层树内多步写臂竞态，
本条管 hlog String 域与内存信封的命令层两步窗口）。
与 incr 旧值解析口径（前导零 / i64 严格解析）类票据不同问题。
与 task/ing/wtxn-lock-stripe-count-parity.md 共用「windex 桶闩是唯一锁源」这一收口结论，两单
分别落在事务锁面与 RMW 读改写面，实施时同一把锁、勿各造一套。

优先级
功能缺口（正确性：并发丢更新），高档；实施上排在锁源收敛（wtxn 那条）之后或与之一批。

盘点补记（qw13.invB string-rmw-key-bucket-lock）：dev e75716e 复核：桶锁唯一消费者仍只 ttl.rs:456/:504，无新原子入口；wtxn 条带锁表已重构为 store 注入 HashIndex loader（txn_lock_table.rs，粒度随索引扩容联动），「禁与 wtxn 条带表并联」的旧前提已变化，本票增量（user_read.rs 读侧无锁取证、bucket.rs 桶闩对位）仍须转录进收口单，与 rmw-atomic 并一棒的结论不变。

---
主代理补录（windex 2pl 收口棒 bfbd1e0 落地后，15:20）：本票若引用 `HashIndex::acquire_keys_lock_exclusive` / `lock_key_exclusive`（含 1024 轮 spin_loop+LockTimeout 中间态）一律失效——该族已连根删除，唯一锁源现为 `HashIndex::try_lock_key_exclusive`（windex/src/table.rs:601，同一把桶闩、无自旋、无超时，KeyLatch 见 lib.rs:17）。在途代码勿再造第二入口；wkv/src/ttl.rs 两处已改持新口。

---

## 判词（三棒，与收口单 rmw-atomic-read-modify-write-window 同批落地，分支 fix-rmw-atomic-window）

本票不单独改内核（票面与收口单均要求「择一实施、勿两处各改一次 RMW 内核」）：实施全部落在
收口单那份载荷里，本票按编排约定核「三项独占增量是否随择单丢失」并转录判词，判 **done**。
载荷 sha 与逐条验收证据见 `task/done/rmw-atomic-read-modify-write-window.md` 判词节；本处只登记本票独有项。

### 一、本票三项独占增量逐项核实（不随择单丢失）

1. **读侧取证仍成立、已被收口消灭**：`wnode/src/storage/session/common/user_read.rs:174 read_user_sync`
   （带前缀臂 `:188`）→ `wkv/src/session/mod.rs:285 with_session_consistent_read` 一致性读臂
   对键不加桶锁，原样。收口不改读臂本身，改的是**调用序**：命令臂先取窗再读
   （`resp/basic_commands/incr.rs:129→:134`、`set.rs:228→:231`/`:581→:584`、
   `bitmap/bitmap_commands.rs:77→:80`/`:459→:465`、`hyperloglog/hyper_log_log_commands.rs:379/:499`、
   `objects/rmw_helpers.rs:600`、`storage/session/txn_proc_view.rs:120→:121`），
   读落进闩内即本票所指缺口闭合；纯只读臂（GET/BITCOUNT/GETBIT）与 SET 条件族盲写
   （`set.rs:511 network_set_conditional`）不在射程。
2. **桶闩对位证据在位且为唯一底层**：`windex/src/bucket.rs:113 try_lock_exclusive`、
   `:241 unlock_exclusive`、`:254 is_latched_exclusive`、守卫 `:486 lock_exclusive_guard` →
   `:568 pub type KeyLatch<'a> = BucketExclusiveGuard<'a>`（别名不引入第二份锁实现，底层同字同 Drop），
   对位 C# `HashBucket.TryAcquireExclusiveLatch/Release/Promote` 与
   `ISessionLocker.cs:BasicSessionLocker.TryLockEphemeralExclusive`。
3. **禁令守住（不新建私有锁表、不与 wtxn 条带表并联）**：载荷 grep
   `Mutex<|RwLock<|LazyLock|OnceLock|striped` 零命中，无第四把锁；三消费面同址同闩已现刻核对——
   `wkv/src/session/rmw_window.rs:224/:230`、`wtxn/src/txn_lock_table.rs:117`、
   `wkv/src/ttl.rs:459/:511` 全部落到 `HashBucket::try_lock_exclusive`（`ttl.rs` 经
   `HashIndex::try_lock_key_exclusive`，`windex/src/table.rs:601`），
   `wtxn` 自述同锁同内存（`txn_lock_table.rs:17`）。窗口与 `wtxn::TxnKeyEntries::acquire_plan` 同款
   形态：钉 `Arc<HashIndex>` 版本 + 纯桶下标（`rmw_window.rs:101`），跨 split 扩容不串锁。

### 二、本票「验证」段判

要求「多任务并发 INCR 断言终值 = 次数、并发 APPEND 断言尾段全在、并发 HSET 不同字段断言字段不丢」：
`wnode/tests/rmw_key_concurrency.rs` 十二例覆盖并超出的做了冷化扇出五例，
摘闩反证 12/12 红（含「并发 HSET 同键不同字段被整值写回抹除：HLEN 376 ≠ 已回执字段数 1000」
「同键并发 INCR 丢更新：终值 506 ≠ 已回执自增数 1000」），收口前失败判据成立；
正式跑 12/12 绿、`-p wkv -p wnode` 全量 1332/1332 绿。

### 三、锁源单点（主代理 15:20 补录必改项）

本票文体引用 `acquire_keys_lock_exclusive` 的三处（现 ticket 正文 :21-22、:49-53 段）为立项期实况描述，
随该族在 `bfbd1e0` 连根删除而失效；全仓代码 grep 旧口零命中，
载荷从未引用旧口，散文旧口两处已在 968aab1 订正（见收口单判词第四节）。

### 四、门禁数字（同收口单，此处不复述全量）

`cargo check --workspace --all-targets` exit 0 / warning 0；`cargo nextest run -p wkv -p wnode`
1332 passed（1 leaky）/ 1 skipped / 0 failed；rustfmt 本票 14 枚载荷文件全 OK；
`bun js/check.js` 前后各 4701 字节、exit 0 双绿、零 ignore 回写、仅两枚行号漂移无映射增减。

---

## 主代理委派收口判词（收口子代理现刻复核，dev 尖 862c256；证据全取 HEAD，未读工作区）

本票不独立改内核，故复核对象是「三项独占增量在 dev 现尖是否仍成立」+「锁源单点补录项是否已落」，
逐条现刻取证如下。

### 一、落地 sha 链

9b199e2（wkv 窗口收口）→ 2121497（慢路径六臂接窗）→ 968aab1（回归 + 锁源注释订正）→ 0b868f2 →
**merge 2495601**（`Merge branch 'dev' into fix-rmw-atomic-window`，再回合 dev 至 18b0789），
该 merge 相对 dev 父净贡献 14 files +1226/−105。
两点簿记实况（详见收口单同段，此处只登记结论）：其一，2495601 不在 dev 第一亲链上，
经 `97b097f` 侧链汇入主链；其二，两票已在 `7999624` 完成 `task/ing → task/done` 的 `git mv`，
本代理现刻 `task/ing/` 零副本，故本棒无 mv 动作，仅追加本段。

### 二、三项独占增量现刻核（全在位）

1. **读侧取证**：`wnode/src/storage/session/common/user_read.rs:174 read_user_sync`
   （带前缀臂 `:188`）→ `wkv/src/session/mod.rs:285 with_session_consistent_read`
   一致性读臂仍不对键取桶锁，原样未动；本票所指缺口靠**调用序**消灭——命令臂一律先取窗再读
   （`incr.rs:129→134`、`set.rs:228→231`/`:581→584`、`bitmap_commands.rs:77→80`/`:459→465`、
   `hyper_log_log_commands.rs:379/:499`、`rmw_helpers.rs:600`、`txn_proc_view.rs:120→121`）。
   全仓写回面已无「无锁读 + 无窗写」两步式：`try_rmw_sync`/`upsert_rmw` 仅存
   `wkv/src/session/raw/write/rmw.rs:19 impl RmwWindow` 的 `:43`/`:81` 两枚定义，16 处调用点接收者全为 `window.`。
2. **桶闩对位为唯一底层**：`windex/src/bucket.rs:109 try_lock_exclusive`、`:236 unlock_exclusive`、
   `:249 is_latched_exclusive`、`:481 lock_exclusive_guard` → `:563 pub type KeyLatch<'a> =
   BucketExclusiveGuard<'a>`（别名，不引入第二份锁实现）；
   对位 C# `HashBucket.TryAcquireExclusiveLatch/Release` 与
   `ISessionLocker.cs:BasicSessionLocker.TryLockEphemeralExclusive` 的注释锚仍在
   `rmw_window.rs:1-11` 模块头。
3. **禁令守住**：载荷 `rmw_window.rs`/`raw/write/rmw.rs`/`slow.rs`/回归测试 grep
   `Mutex<|RwLock<|LazyLock|OnceLock|striped` **零命中**，无私有锁表、无与 `wtxn` 条带表并联；
   三面同址同闩现刻复点：`rmw_window.rs:217/:249`（自取）+ `wkv/src/ttl.rs:459/:511` +
   `wtxn/src/txn_lock_table.rs:116-117`（`self.pin().bucket(b).try_lock_exclusive()` 转发），
   窗口与 `TxnKeyEntries::acquire_plan` 同款形态（钉 `Arc<HashIndex>` + 纯桶下标 `rmw_window.rs:102`）。

### 三、锁源单点（15:20 补录必改项）现刻核

旧族 `acquire_keys_lock_exclusive` / 裸 `lock_key_exclusive` 全仓 grep（含 README、含注释）**0 命中**；
票面正文 `:21-22`、`:49-53` 的旧口引用属立项期实况，保留不改（散文历史态），
唯一现行入口 `windex/src/table.rs:601 HashIndex::try_lock_key_exclusive` 在位、
`windex/readme` 与 `windex/README` 已按「单次尝试、无自旋、无超时，重试预算由调用方承接」口径书写。
本窗口不自建守卫、以 `Drop`（`rmw_window.rs:120-128`）承载放闩，与补录要求不冲突。

### 四、票面「验证」段现刻核

要求 INCR 终值 = 次数、APPEND 尾段全在、HSET 字段不丢：`wnode/tests/rmw_key_concurrency.rs`
（HEAD 741 行 / 12 枚 `#[test]`）warm 四例 `:125/:156/:207/:243` 即为该三问加 PFADD 参照，
另有窗口层三例 `:370/:400/:433` 与冷化扇出五例 `:499/:553/:605/:653/:701`；
摘闩反证 12/12 红为三棒现场取证，本代理未复跑（不碰工作区、不改码）。

### 五、遗留项

- `windex/src/bucket.rs` 五枚锚点相对本票判词各下移 5 行（判词 113/241/254/486/568 → 现刻
  109/236/249/481/563），属后续 dev 棒插行，机制未变；本段数字为准。
- 本票消费面所依赖的 windex 签名在 `8fb4123`、`862c256` 两棒锁 API 重构后逐枚核到未改；
  workspace check / nextest 未在现刻复跑，归主代理门禁。
- 与 `wtxn-lock-stripe-count-parity` 共用的「windex 桶闩是唯一锁源」结论现刻仍成立；
  两基并存期（`user_key` 桶 vs 记录物理键桶）交错面按收口单第六节移交，本票不另立项。

结论：**三项独占增量与锁源单点补录全数在位，无缺口**；本票维持 **done**。
